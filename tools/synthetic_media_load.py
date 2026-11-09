"""Paced Synthetic Media Concurrency, Load & Overload Evaluation Runner (M5.1B).

Exercises Kaiwen Gateway's experimental `/ws/synthetic-media/v1` binary WebSocket
media plane under real-time 20 ms monotonic pacing, staged simultaneous concurrency
(1, 5, and 25 concurrent calls), admission saturation, graceful draining, and
fault/recovery scenarios using M5.1A's provider-independent Voice Bridge telemetry.

Scope & Invariants:
1. Real-Time 20 ms Media Pacing: Uses monotonic reference deadlines (`t0 + k * 0.020s`)
   for 640-byte PCM16LE mono 16 kHz frames. Never resets the schedule to conceal
   drift and never floods the socket with zero-delay catch-up bursts.
2. Honest Latency Separation: Distinguishes client scheduler lateness/jitter,
   client-observed loopback echo RTT, Gateway inbound/outbound processing,
   Gateway inbound/outbound queue wait, ASGI `websocket.send_bytes` await time,
   and event-loop scheduling lag. Upstream AI generation, external telephony
   network propagation, and remote speaker playback remain explicitly unavailable.
3. Zero Core or Mandatory Runtime Dependencies: Standalone developer evaluation
   tool using standard library plus existing `server`/`dev` extras.
4. Strict Privacy & Redaction: Generated JSON reports never contain raw audio
   bytes, bearer tokens, secrets, phone numbers, transcripts, or raw call/tenant
   identifiers.
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import ctypes
import ctypes.wintypes
import hashlib
import json
import math
import os
import platform
import secrets
import socket
import struct
import subprocess
import sys
import time
import tracemalloc
from collections.abc import AsyncIterator, Awaitable, Callable, Mapping, Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Final

import uvicorn
import websockets
import websockets.exceptions

from kaiwen_gateway import __version__ as gateway_version
from kaiwen_gateway.core.admission import AdmissionController
from kaiwen_gateway.core.config import (
    DEFAULT_MAX_CONCURRENT_CONNECTIONS,
    DEFAULT_MAX_CONNECTIONS_PER_TENANT,
    INGRESS_MODE_SYNTHETIC,
    CoreConfig,
)
from kaiwen_gateway.core.draining import DrainPolicy, InstanceDrainCoordinator
from kaiwen_gateway.modules.voice import (
    DEFAULT_LATENCY_WINDOW_SIZE,
    SMALL_SAMPLE_THRESHOLD,
    AudioFormat,
    AudioFrame,
    BoundedInterruptibleFakeAIEchoPort,
    BoundedLatencyAggregator,
    InMemoryVoiceTelemetrySink,
    LatencyDistributionSummary,
    MediaBackpressureOverflowError,
    MetricAvailabilityReason,
    VoiceErrorCategory,
    VoiceSessionMetricsSnapshot,
    VoiceTerminalCleanupState,
)
from kaiwen_gateway.server.app import create_gateway_app
from kaiwen_gateway.transports.synthetic import (
    EVENT_CALL_END,
    EVENT_CALL_ENDED,
    EVENT_CALL_INTERRUPT,
    EVENT_CALL_INTERRUPTED,
    EVENT_CALL_START,
    EVENT_CALL_STARTED,
    EVENT_SESSION_READY,
    PCM16LE_CHANNELS,
    PCM16LE_FRAME_BYTES,
    PCM16LE_FRAME_MS,
    PCM16LE_SAMPLE_RATE,
    SYNTHETIC_MEDIA_WS_ENDPOINT_PATH,
    SyntheticAuthBridge,
    SyntheticTokenVerifier,
)
from tools.synthetic_soak import resolve_sustained_tenant_distribution

RUNNER_VERSION: Final[str] = "0.1.0"
FRAME_MAGIC: Final[int] = 0x4B574D35  # "KWM5"
HEADER_STRUCT: Final[struct.Struct] = struct.Struct("<IIII")
HEADER_BYTES: Final[int] = HEADER_STRUCT.size  # 16 bytes
BODY_BYTES: Final[int] = PCM16LE_FRAME_BYTES - HEADER_BYTES  # 624 bytes
SUPPORTED_PATTERNS: Final[tuple[str, ...]] = ("deterministic", "sine", "ramp")

SCHEDULING_MODE_ALIGNED: Final[str] = "aligned"
SCHEDULING_MODE_STAGGERED: Final[str] = "staggered"
SUPPORTED_SCHEDULING_MODES: Final[tuple[str, ...]] = (
    SCHEDULING_MODE_ALIGNED,
    SCHEDULING_MODE_STAGGERED,
)

ISOLATION_MODE_IN_PROCESS: Final[str] = "in-process"
ISOLATION_MODE_PROCESS_ISOLATED: Final[str] = "process-isolated"
SUPPORTED_ISOLATION_MODES: Final[tuple[str, ...]] = (
    ISOLATION_MODE_PROCESS_ISOLATED,
    ISOLATION_MODE_IN_PROCESS,
)
KW_SYNTHETIC_MEDIA_SECRET_KEY_ENV: Final[str] = "KW_SYNTHETIC_MEDIA_SECRET_KEY_HEX"

DEFAULT_MAX_MISSED_DEADLINE_RATE: Final[float] = 0.20
DEFAULT_MAX_MEAN_INTERVAL_DEVIATION_PCT: Final[float] = 20.0
DEFAULT_MAX_P95_SCHEDULER_LATENESS_MS: Final[float] = 100.0
DEFAULT_MAX_TRANSMISSION_ELONGATION_PCT: Final[float] = 25.0


@dataclass(frozen=True, slots=True)
class PacingAcceptanceCriteria:
    """Explicit, documented acceptance criteria for real-time frame pacing."""

    max_missed_deadline_rate: float = DEFAULT_MAX_MISSED_DEADLINE_RATE
    max_mean_interval_deviation_pct: float = DEFAULT_MAX_MEAN_INTERVAL_DEVIATION_PCT
    max_p95_scheduler_lateness_ms: float = DEFAULT_MAX_P95_SCHEDULER_LATENESS_MS
    max_transmission_elongation_pct: float = DEFAULT_MAX_TRANSMISSION_ELONGATION_PCT

    def __post_init__(self) -> None:
        if isinstance(self.max_missed_deadline_rate, bool) or not isinstance(
            self.max_missed_deadline_rate, (int, float)
        ):
            raise TypeError("max_missed_deadline_rate must be a float, not bool")
        rate_val = float(self.max_missed_deadline_rate)
        if not math.isfinite(rate_val) or rate_val <= 0.0 or rate_val > 1.0:
            raise ValueError(f"max_missed_deadline_rate must be in (0.0, 1.0], got {rate_val!r}")
        object.__setattr__(self, "max_missed_deadline_rate", rate_val)

        if isinstance(self.max_mean_interval_deviation_pct, bool) or not isinstance(
            self.max_mean_interval_deviation_pct, (int, float)
        ):
            raise TypeError("max_mean_interval_deviation_pct must be a float, not bool")
        dev_val = float(self.max_mean_interval_deviation_pct)
        if not math.isfinite(dev_val) or dev_val <= 0.0:
            raise ValueError(f"max_mean_interval_deviation_pct must be positive, got {dev_val!r}")
        object.__setattr__(self, "max_mean_interval_deviation_pct", dev_val)

        if isinstance(self.max_p95_scheduler_lateness_ms, bool) or not isinstance(
            self.max_p95_scheduler_lateness_ms, (int, float)
        ):
            raise TypeError("max_p95_scheduler_lateness_ms must be a float, not bool")
        late_val = float(self.max_p95_scheduler_lateness_ms)
        if not math.isfinite(late_val) or late_val <= 0.0:
            raise ValueError(f"max_p95_scheduler_lateness_ms must be positive, got {late_val!r}")
        object.__setattr__(self, "max_p95_scheduler_lateness_ms", late_val)

        if isinstance(self.max_transmission_elongation_pct, bool) or not isinstance(
            self.max_transmission_elongation_pct, (int, float)
        ):
            raise TypeError("max_transmission_elongation_pct must be a float, not bool")
        elong_val = float(self.max_transmission_elongation_pct)
        if not math.isfinite(elong_val) or elong_val <= 0.0:
            raise ValueError(f"max_transmission_elongation_pct must be positive, got {elong_val!r}")
        object.__setattr__(self, "max_transmission_elongation_pct", elong_val)

    def to_dict(self) -> dict[str, Any]:
        return {
            "max_missed_deadline_rate": self.max_missed_deadline_rate,
            "max_mean_interval_deviation_pct": self.max_mean_interval_deviation_pct,
            "max_p95_scheduler_lateness_ms": self.max_p95_scheduler_lateness_ms,
            "max_transmission_elongation_pct": self.max_transmission_elongation_pct,
        }


def evaluate_pacing_metrics(
    *,
    total_scheduled: int,
    total_sent: int,
    total_missed_deadlines: int,
    target_interval_ms: float,
    mean_interval_ms: float | None,
    p95_scheduler_lateness_ms: float | None,
    nominal_transmission_s: float,
    actual_transmission_s: float | None,
    criteria: PacingAcceptanceCriteria,
    require_transmission_duration: bool = True,
) -> tuple[str, list[str], dict[str, Any]]:
    """Evaluate empirical timing counters against strict real-time pacing criteria."""
    failure_reasons: list[str] = []

    # 1. Counter validations & type checks
    counters_valid = True
    if (
        isinstance(total_scheduled, bool)
        or not isinstance(total_scheduled, int)
        or total_scheduled < 0
    ):
        failure_reasons.append(f"Invalid total_scheduled counter: {total_scheduled!r}")
        counters_valid = False

    if isinstance(total_sent, bool) or not isinstance(total_sent, int) or total_sent < 0:
        failure_reasons.append(f"Invalid total_sent counter: {total_sent!r}")
        counters_valid = False

    if (
        isinstance(total_missed_deadlines, bool)
        or not isinstance(total_missed_deadlines, int)
        or total_missed_deadlines < 0
    ):
        failure_reasons.append(
            f"Invalid total_missed_deadlines counter: {total_missed_deadlines!r}"
        )
        counters_valid = False

    if counters_valid:
        if total_sent > total_scheduled:
            failure_reasons.append(
                f"total_sent ({total_sent}) cannot exceed total_scheduled ({total_scheduled})"
            )
        if total_missed_deadlines > total_scheduled:
            failure_reasons.append(
                f"total_missed_deadlines ({total_missed_deadlines}) cannot exceed total_scheduled ({total_scheduled})"
            )

    if isinstance(total_sent, int) and not isinstance(total_sent, bool) and total_sent <= 0:
        failure_reasons.append("No audio frames were transmitted to evaluate real-time pacing")

    # 2. Missed deadline rate
    missed_rate: float | None = None
    if counters_valid and total_sent > 0 and total_missed_deadlines <= total_scheduled:
        missed_rate = float(total_missed_deadlines) / float(total_sent)
        if missed_rate > criteria.max_missed_deadline_rate:
            failure_reasons.append(
                f"Missed deadline rate {missed_rate:.2%} exceeded threshold "
                f"{criteria.max_missed_deadline_rate:.2%} ({total_missed_deadlines}/{total_sent} frames missed deadline)"
            )

    # 3. Target interval validation
    target_valid = True
    if (
        isinstance(target_interval_ms, bool)
        or not isinstance(target_interval_ms, (int, float))
        or not math.isfinite(target_interval_ms)
        or target_interval_ms <= 0.0
    ):
        failure_reasons.append(f"Invalid target interval: {target_interval_ms!r}")
        target_valid = False

    # 4. Mean scheduling interval
    mean_interval_dev_pct: float | None = None
    if mean_interval_ms is not None:
        if isinstance(mean_interval_ms, bool) or not isinstance(mean_interval_ms, (int, float)):
            failure_reasons.append(
                f"Invalid mean interval measurement type: {type(mean_interval_ms).__name__}"
            )
        elif not math.isfinite(mean_interval_ms):
            failure_reasons.append(f"Nonfinite mean interval measurement: {mean_interval_ms!r}")
        elif mean_interval_ms <= 0.0:
            failure_reasons.append(
                f"Nonpositive mean interval measurement: {mean_interval_ms:.2f} ms"
            )
        elif target_valid:
            mean_interval_dev_pct = (
                abs(mean_interval_ms - target_interval_ms) / target_interval_ms * 100.0
            )
            if mean_interval_dev_pct > criteria.max_mean_interval_deviation_pct:
                failure_reasons.append(
                    f"Mean inter-frame interval {mean_interval_ms:.2f} ms deviated by "
                    f"{mean_interval_dev_pct:.2f}% from target {target_interval_ms:.2f} ms "
                    f"(max allowed: {criteria.max_mean_interval_deviation_pct:.2f}%)"
                )
    elif isinstance(total_sent, int) and not isinstance(total_sent, bool) and total_sent > 1:
        failure_reasons.append(
            "Missing inter-frame scheduling interval measurements for pacing evaluation"
        )

    # 5. p95 scheduler lateness
    if p95_scheduler_lateness_ms is not None:
        if isinstance(p95_scheduler_lateness_ms, bool) or not isinstance(
            p95_scheduler_lateness_ms, (int, float)
        ):
            failure_reasons.append(
                f"Invalid p95 scheduler lateness measurement type: {type(p95_scheduler_lateness_ms).__name__}"
            )
        elif not math.isfinite(p95_scheduler_lateness_ms):
            failure_reasons.append(
                f"Nonfinite p95 scheduler lateness measurement: {p95_scheduler_lateness_ms!r}"
            )
        elif p95_scheduler_lateness_ms < 0.0:
            failure_reasons.append(
                f"Negative p95 scheduler lateness measurement: {p95_scheduler_lateness_ms:.2f} ms"
            )
        else:
            if p95_scheduler_lateness_ms > criteria.max_p95_scheduler_lateness_ms:
                failure_reasons.append(
                    f"p95 scheduler lateness {p95_scheduler_lateness_ms:.2f} ms exceeded threshold "
                    f"{criteria.max_p95_scheduler_lateness_ms:.2f} ms"
                )
    elif isinstance(total_sent, int) and not isinstance(total_sent, bool) and total_sent > 0:
        failure_reasons.append("Missing scheduler lateness measurements for pacing evaluation")

    # 6. Nominal and actual transmission duration
    nominal_valid = True
    if (
        isinstance(nominal_transmission_s, bool)
        or not isinstance(nominal_transmission_s, (int, float))
        or not math.isfinite(nominal_transmission_s)
        or nominal_transmission_s < 0.0
    ):
        failure_reasons.append(f"Invalid nominal transmission duration: {nominal_transmission_s!r}")
        nominal_valid = False

    elongation_pct: float | None = None
    if actual_transmission_s is not None:
        if isinstance(actual_transmission_s, bool) or not isinstance(
            actual_transmission_s, (int, float)
        ):
            failure_reasons.append(
                f"Invalid actual transmission duration measurement type: {type(actual_transmission_s).__name__}"
            )
        elif not math.isfinite(actual_transmission_s):
            failure_reasons.append(
                f"Nonfinite actual transmission duration measurement: {actual_transmission_s!r}"
            )
        elif actual_transmission_s < 0.0:
            failure_reasons.append(
                f"Negative actual transmission duration measurement: {actual_transmission_s:.2f}s"
            )
        elif (
            nominal_valid
            and nominal_transmission_s > 0
            and isinstance(total_sent, int)
            and not isinstance(total_sent, bool)
            and total_sent > 1
        ):
            elongation_pct = max(
                0.0,
                (actual_transmission_s - nominal_transmission_s) / nominal_transmission_s * 100.0,
            )
            if elongation_pct > criteria.max_transmission_elongation_pct:
                failure_reasons.append(
                    f"Media transmission duration elongated by {elongation_pct:.2f}% "
                    f"({actual_transmission_s:.2f}s vs nominal {nominal_transmission_s:.2f}s, "
                    f"max allowed: {criteria.max_transmission_elongation_pct:.2f}%)"
                )
    elif (
        require_transmission_duration
        and isinstance(total_sent, int)
        and not isinstance(total_sent, bool)
        and total_sent > 1
    ):
        failure_reasons.append(
            "Missing actual transmission duration measurement for pacing evaluation"
        )

    pacing_status = "PASS" if not failure_reasons else "FAIL"
    metrics = {
        "missed_deadline_rate": (
            round(missed_rate, 6)
            if missed_rate is not None and math.isfinite(missed_rate)
            else None
        ),
        "mean_interval_deviation_pct": (
            round(mean_interval_dev_pct, 4)
            if mean_interval_dev_pct is not None and math.isfinite(mean_interval_dev_pct)
            else None
        ),
        "p95_scheduler_lateness_ms": (
            round(p95_scheduler_lateness_ms, 4)
            if (
                p95_scheduler_lateness_ms is not None
                and not isinstance(p95_scheduler_lateness_ms, bool)
                and isinstance(p95_scheduler_lateness_ms, (int, float))
                and math.isfinite(p95_scheduler_lateness_ms)
            )
            else None
        ),
        "transmission_elongation_pct": (
            round(elongation_pct, 4)
            if elongation_pct is not None and math.isfinite(elongation_pct)
            else None
        ),
        "nominal_transmission_seconds": (
            round(nominal_transmission_s, 4)
            if (
                not isinstance(nominal_transmission_s, bool)
                and isinstance(nominal_transmission_s, (int, float))
                and math.isfinite(nominal_transmission_s)
            )
            else None
        ),
        "actual_transmission_seconds": (
            round(actual_transmission_s, 4)
            if (
                actual_transmission_s is not None
                and not isinstance(actual_transmission_s, bool)
                and isinstance(actual_transmission_s, (int, float))
                and math.isfinite(actual_transmission_s)
            )
            else None
        ),
    }
    return pacing_status, failure_reasons, metrics


def reassess_stage_report(
    stage_data: Mapping[str, Any],
    criteria: PacingAcceptanceCriteria | None = None,
) -> dict[str, Any]:
    """Offline evaluator: reassess any stage result dict against explicit pacing and integrity criteria."""
    crit = criteria or PacingAcceptanceCriteria()
    mc = stage_data.get("media_correctness", {})
    tm = stage_data.get("timing_ms", {})

    total_scheduled = int(mc.get("frames_scheduled", 0))
    total_sent = int(mc.get("frames_sent", 0))
    total_missed = int(mc.get("missed_deadlines", 0))
    target_interval_ms = 20.0

    interval_summary = tm.get("frame_scheduling_interval_ms", {})
    mean_interval_ms = (
        float(interval_summary["mean_ms"]) if interval_summary.get("mean_ms") is not None else None
    )

    lateness_summary = tm.get("scheduler_lateness_ms", {})
    p95_lateness_ms = (
        float(lateness_summary["p95_ms"]) if lateness_summary.get("p95_ms") is not None else None
    )

    configured_duration_s = float(stage_data.get("configured_duration_seconds", 5.0))
    frames_per_session = max(1, int(round((configured_duration_s * 1000.0) / target_interval_ms)))
    nominal_transmission_s = max(1e-6, (frames_per_session - 1) * (target_interval_ms / 1000.0))

    pacing_eval = tm.get("pacing_evaluation", {})
    eval_metrics = pacing_eval.get("metrics", {})
    actual_transmission_s: float | None = eval_metrics.get("actual_transmission_seconds")
    if actual_transmission_s is None:
        actual_transmission_s = tm.get("media_transmission_duration_s")

    require_tx_duration = actual_transmission_s is not None

    pacing_status, pacing_failures, metrics = evaluate_pacing_metrics(
        total_scheduled=total_scheduled,
        total_sent=total_sent,
        total_missed_deadlines=total_missed,
        target_interval_ms=target_interval_ms,
        mean_interval_ms=mean_interval_ms,
        p95_scheduler_lateness_ms=p95_lateness_ms,
        nominal_transmission_s=nominal_transmission_s,
        actual_transmission_s=actual_transmission_s,
        criteria=crit,
        require_transmission_duration=require_tx_duration,
    )

    media_failures: list[str] = []
    if int(mc.get("frames_missing", 0)) > 0:
        media_failures.append(f"Missing frames: {mc.get('frames_missing')}")
    if int(mc.get("duplicate_frames", 0)) > 0:
        media_failures.append(f"Duplicate frames: {mc.get('duplicate_frames')}")
    if int(mc.get("out_of_order_frames", 0)) > 0:
        media_failures.append(f"Out of order frames: {mc.get('out_of_order_frames')}")
    if int(mc.get("payload_mismatches", 0)) > 0:
        media_failures.append(f"Payload mismatches: {mc.get('payload_mismatches')}")
    if int(mc.get("unexpected_close_codes", 0)) > 0:
        media_failures.append(f"Unexpected close codes: {mc.get('unexpected_close_codes')}")
    if int(mc.get("cleanup_failures", 0)) > 0:
        media_failures.append(f"Cleanup failures: {mc.get('cleanup_failures')}")

    cleanup = stage_data.get("cleanup_verification", {})
    if (
        int(cleanup.get("final_active_connections", 0)) != 0
        or int(cleanup.get("leaked_asyncio_tasks", 0)) != 0
    ):
        media_failures.append(f"Resource leaks detected in cleanup: {cleanup}")

    req_concurrency = int(stage_data.get("requested_concurrency", 0))
    achieved_concurrency = int(stage_data.get("achieved_media_concurrency", 0))
    if req_concurrency > 0 and achieved_concurrency != req_concurrency:
        media_failures.append(
            f"Achieved active media concurrency ({achieved_concurrency}) "
            f"did not match requested concurrency ({req_concurrency})"
        )

    media_status = "PASS" if not media_failures else "FAIL"
    overall_status = "PASS" if media_status == "PASS" and pacing_status == "PASS" else "FAIL"

    return {
        "stage_name": stage_data.get("stage_name", "unknown"),
        "media_integrity_status": media_status,
        "realtime_pacing_status": pacing_status,
        "overall_status": overall_status,
        "media_failure_reasons": media_failures,
        "pacing_failure_reasons": pacing_failures,
        "pacing_metrics": metrics,
        "pacing_acceptance_criteria": crit.to_dict(),
    }


def compute_session_phase_offset_ms(
    *,
    session_slot_id: int,
    concurrency: int,
    scheduling_mode: str,
    interval_ms: float = float(PCM16LE_FRAME_MS),
) -> float:
    """Calculate the phase offset in milliseconds for a session slot within the frame window.

    - Mode A (Aligned): All sessions begin at t0 (offset = 0.0 ms).
    - Mode B (Staggered): Sessions are evenly distributed across the 20 ms frame window
      (offset = session_slot_id * (interval_ms / concurrency)).
    """
    if session_slot_id < 0:
        raise ValueError("session_slot_id must be non-negative")
    if concurrency <= 0:
        raise ValueError("concurrency must be positive")
    if interval_ms <= 0.0 or not math.isfinite(interval_ms):
        raise ValueError("interval_ms must be a positive finite float")

    if scheduling_mode == SCHEDULING_MODE_ALIGNED or concurrency <= 1:
        return 0.0
    if scheduling_mode == SCHEDULING_MODE_STAGGERED:
        return float(session_slot_id) * (interval_ms / float(concurrency))
    raise ValueError(
        f"Unsupported scheduling_mode {scheduling_mode!r}; must be one of {SUPPORTED_SCHEDULING_MODES}"
    )


# ==============================================================================
# Deterministic Frame Payload Generation & Verification
# ==============================================================================


def _compute_frame_checksum(
    *,
    seed: int,
    session_slot_id: int,
    frame_seq: int,
    pattern: str,
) -> int:
    """Compute a deterministic 32-bit unsigned checksum tag for a synthetic frame."""
    material = f"{seed}:{session_slot_id}:{frame_seq}:{pattern}".encode("ascii")
    digest = hashlib.blake2s(material, digest_size=4).digest()
    return int.from_bytes(digest, "little", signed=False)


def _generate_frame_body(
    *,
    seed: int,
    session_slot_id: int,
    frame_seq: int,
    pattern: str,
) -> bytes:
    """Generate a deterministic 624-byte PCM16LE payload body without sensitive data."""
    if pattern not in SUPPORTED_PATTERNS:
        raise ValueError(
            f"Unsupported frame pattern {pattern!r}; must be one of {SUPPORTED_PATTERNS}"
        )

    if pattern == "ramp":
        start_val = (seed + session_slot_id * 31 + frame_seq * 17) & 0xFF
        return bytes(((start_val + i) & 0xFF) for i in range(BODY_BYTES))

    if pattern == "sine":
        sample_count = BODY_BYTES // 2
        samples: list[int] = []
        freq_hz = 400.0 + float((session_slot_id % 8) * 50)
        phase_offset = float((seed + frame_seq) % 100) * 0.05
        for idx in range(sample_count):
            t = float(idx) / float(PCM16LE_SAMPLE_RATE)
            val = int(12000.0 * math.sin(2.0 * math.pi * freq_hz * t + phase_offset))
            samples.append(max(-32768, min(32767, val)))
        return struct.pack(f"<{sample_count}h", *samples)

    # Default "deterministic": fast seeded BLAKE2b expansion to 624 bytes
    seed_block = struct.pack(
        "<qII",
        int(seed),
        int(session_slot_id) & 0xFFFFFFFF,
        int(frame_seq) & 0xFFFFFFFF,
    )
    chunks: list[bytes] = []
    for block_idx in range(10):  # 10 * 64 = 640 >= 624 bytes
        chunks.append(
            hashlib.blake2b(
                seed_block,
                person=f"kwm5_{block_idx:02d}".encode("ascii"),
                digest_size=64,
            ).digest()
        )
    return b"".join(chunks)[:BODY_BYTES]


def build_deterministic_frame_payload(
    *,
    seed: int,
    session_slot_id: int,
    frame_seq: int,
    pattern: str = "deterministic",
) -> bytes:
    """Build an exact 640-byte PCM16LE frame with embedded slot and sequence identity."""
    if session_slot_id < 0:
        raise ValueError("session_slot_id must be non-negative")
    if frame_seq < 0:
        raise ValueError("frame_seq must be non-negative")

    checksum = _compute_frame_checksum(
        seed=seed,
        session_slot_id=session_slot_id,
        frame_seq=frame_seq,
        pattern=pattern,
    )
    header = HEADER_STRUCT.pack(
        FRAME_MAGIC,
        session_slot_id & 0xFFFFFFFF,
        frame_seq & 0xFFFFFFFF,
        checksum,
    )
    body = _generate_frame_body(
        seed=seed,
        session_slot_id=session_slot_id,
        frame_seq=frame_seq,
        pattern=pattern,
    )
    payload = header + body
    assert len(payload) == PCM16LE_FRAME_BYTES
    return payload


def extract_frame_identity(payload: bytes) -> tuple[int, int] | None:
    """Extract `(session_slot_id, frame_seq)` from a 640-byte frame header, or None if invalid."""
    if not isinstance(payload, (bytes, bytearray)) or len(payload) != PCM16LE_FRAME_BYTES:
        return None
    magic, slot_id, frame_seq, _ = HEADER_STRUCT.unpack_from(payload, 0)
    if magic != FRAME_MAGIC:
        return None
    return int(slot_id), int(frame_seq)


def verify_frame_payload(
    payload: bytes,
    *,
    seed: int,
    session_slot_id: int,
    frame_seq: int,
    pattern: str = "deterministic",
) -> bool:
    """Verify that a 640-byte received payload matches the exact expected frame bytes."""
    if not isinstance(payload, (bytes, bytearray)) or len(payload) != PCM16LE_FRAME_BYTES:
        return False
    expected = build_deterministic_frame_payload(
        seed=seed,
        session_slot_id=session_slot_id,
        frame_seq=frame_seq,
        pattern=pattern,
    )
    return bytes(payload) == expected


# ==============================================================================
# Monotonic 20 ms Pacing & Missed-Deadline Policy
# ==============================================================================


@dataclass(frozen=True, slots=True)
class FrameScheduleMeasurement:
    """Timing record for a single scheduled 20 ms frame slot."""

    slot_index: int
    skipped: bool
    missed_deadline: bool
    deadline_monotonic_s: float
    actual_send_monotonic_s: float | None
    lateness_ms: float
    signed_lateness_ms: float
    inter_frame_interval_ms: float | None
    interval_jitter_ms: float | None


class MonotonicFramePacer:
    """Monotonic-clock frame scheduler enforcing real-time 20 ms cadence without burst flooding.

    Missed-Deadline Policy (`monotonic_fixed_schedule_rate_limited_catchup`):
    1. Every frame slot `k` (`0 .. total_frames - 1`) is assigned an immutable reference
       deadline `deadline_k = t0 + k * interval_s`. The reference origin `t0` is never
       shifted after a late wakeup, so scheduling drift is never concealed.
    2. A frame slot is classified as a `missed_deadline` when the actual wakeup/send time
       exceeds `deadline_k + missed_deadline_threshold_s` (default: 1 full frame interval,
       i.e. > 20.0 ms late).
    3. If `skip_threshold_ms` is configured and `now - deadline_k >= skip_threshold_s`,
       the stale slot is skipped (`skipped=True`, `missed_deadline=True`) rather than
       transmitted late.
    4. When catching up after an OS scheduler stall without skipping, consecutive sends
       are separated by at least `min_send_spacing_s` (default: `0.25 * interval_s = 5.0 ms`)
       so the client never floods the WebSocket with zero-delay back-to-back bursts.
    """

    def __init__(
        self,
        *,
        interval_ms: float = float(PCM16LE_FRAME_MS),
        missed_deadline_threshold_ms: float | None = None,
        min_send_spacing_ms: float | None = None,
        skip_threshold_ms: float | None = None,
        phase_offset_ms: float = 0.0,
        clock: Callable[[], float] = time.perf_counter,
        sleep_fn: Callable[[float], Awaitable[None]] = asyncio.sleep,
    ) -> None:
        if isinstance(interval_ms, bool) or not isinstance(interval_ms, (int, float)):
            raise ValueError("interval_ms must be a positive finite float")
        interval_val = float(interval_ms)
        if not math.isfinite(interval_val) or interval_val <= 0.0:
            raise ValueError("interval_ms must be a positive finite float")

        self._interval_ms: Final[float] = interval_val
        self._interval_s: Final[float] = interval_val / 1000.0

        if isinstance(phase_offset_ms, bool) or not isinstance(phase_offset_ms, (int, float)):
            raise ValueError("phase_offset_ms must be a non-negative finite float")
        phase_val = float(phase_offset_ms)
        if not math.isfinite(phase_val) or phase_val < 0.0:
            raise ValueError("phase_offset_ms must be a non-negative finite float")
        self._phase_offset_ms: float = phase_val
        self._phase_offset_s: float = phase_val / 1000.0

        missed_ms = (
            float(missed_deadline_threshold_ms)
            if missed_deadline_threshold_ms is not None
            else interval_val
        )
        if not math.isfinite(missed_ms) or missed_ms <= 0.0:
            raise ValueError("missed_deadline_threshold_ms must be a positive finite float")
        self._missed_threshold_ms: Final[float] = missed_ms

        spacing_ms = (
            float(min_send_spacing_ms) if min_send_spacing_ms is not None else (interval_val * 0.25)
        )
        if not math.isfinite(spacing_ms) or spacing_ms < 0.0:
            raise ValueError("min_send_spacing_ms must be a non-negative finite float")
        self._min_spacing_s: Final[float] = spacing_ms / 1000.0

        if skip_threshold_ms is not None:
            skip_val = float(skip_threshold_ms)
            if not math.isfinite(skip_val) or skip_val <= 0.0:
                raise ValueError("skip_threshold_ms must be a positive finite float or None")
            self._skip_threshold_ms: float | None = skip_val
        else:
            self._skip_threshold_ms = None

        self._clock = clock
        self._sleep = sleep_fn
        self._t0: float | None = None
        self._last_send_s: float | None = None
        self._scheduled_count: int = 0
        self._sent_count: int = 0
        self._missed_deadlines: int = 0
        self._skipped_deadlines: int = 0

    @property
    def interval_ms(self) -> float:
        return self._interval_ms

    @property
    def phase_offset_ms(self) -> float:
        return self._phase_offset_ms

    @property
    def phase_offset_s(self) -> float:
        return self._phase_offset_s

    @property
    def missed_deadlines(self) -> int:
        return self._missed_deadlines

    @property
    def skipped_deadlines(self) -> int:
        return self._skipped_deadlines

    @property
    def scheduled_count(self) -> int:
        return self._scheduled_count

    @property
    def sent_count(self) -> int:
        return self._sent_count

    def reset(
        self,
        *,
        t0: float | None = None,
        phase_offset_ms: float | None = None,
    ) -> float:
        """Initialize or reset the monotonic reference origin `t0` and optional phase offset."""
        origin = t0 if t0 is not None else self._clock()
        self._t0 = origin
        if phase_offset_ms is not None:
            if isinstance(phase_offset_ms, bool) or not isinstance(phase_offset_ms, (int, float)):
                raise ValueError("phase_offset_ms must be a non-negative finite float")
            val = float(phase_offset_ms)
            if not math.isfinite(val) or val < 0.0:
                raise ValueError("phase_offset_ms must be a non-negative finite float")
            self._phase_offset_ms = val
            self._phase_offset_s = val / 1000.0
        self._last_send_s = None
        self._scheduled_count = 0
        self._sent_count = 0
        self._missed_deadlines = 0
        self._skipped_deadlines = 0
        return origin

    async def await_slot(self, slot_index: int) -> FrameScheduleMeasurement:
        """Wait until the scheduled monotonic slot for `slot_index` and record deviation."""
        if slot_index < 0:
            raise ValueError("slot_index must be non-negative")
        if self._t0 is None:
            self._t0 = self._clock()

        self._scheduled_count += 1
        deadline_s = self._t0 + self._phase_offset_s + (float(slot_index) * self._interval_s)
        now_s = self._clock()

        # Check hard skip policy before sleeping/sending if already past skip_threshold_ms
        initial_lateness_ms = (now_s - deadline_s) * 1000.0
        if (
            self._skip_threshold_ms is not None
            and slot_index > 0
            and initial_lateness_ms >= self._skip_threshold_ms
        ):
            self._skipped_deadlines += 1
            self._missed_deadlines += 1
            return FrameScheduleMeasurement(
                slot_index=slot_index,
                skipped=True,
                missed_deadline=True,
                deadline_monotonic_s=deadline_s,
                actual_send_monotonic_s=None,
                lateness_ms=max(0.0, initial_lateness_ms),
                signed_lateness_ms=initial_lateness_ms,
                inter_frame_interval_ms=None,
                interval_jitter_ms=None,
            )

        # Compute earliest allowed send timestamp (respecting both deadline_s and anti-burst spacing)
        target_send_s = deadline_s
        if self._last_send_s is not None:
            min_allowed_s = self._last_send_s + self._min_spacing_s
            if min_allowed_s > target_send_s:
                target_send_s = min_allowed_s

        sleep_s = target_send_s - now_s
        if sleep_s > 0.0:
            await self._sleep(sleep_s)

        actual_s = self._clock()
        signed_lateness_ms = (actual_s - deadline_s) * 1000.0
        lateness_ms = max(0.0, signed_lateness_ms)
        missed = lateness_ms > self._missed_threshold_ms
        if missed:
            self._missed_deadlines += 1

        inter_interval_ms: float | None = None
        jitter_ms: float | None = None
        if self._last_send_s is not None:
            inter_interval_ms = max(0.0, (actual_s - self._last_send_s) * 1000.0)
            jitter_ms = abs(inter_interval_ms - self._interval_ms)

        self._last_send_s = actual_s
        self._sent_count += 1
        return FrameScheduleMeasurement(
            slot_index=slot_index,
            skipped=False,
            missed_deadline=missed,
            deadline_monotonic_s=deadline_s,
            actual_send_monotonic_s=actual_s,
            lateness_ms=lateness_ms,
            signed_lateness_ms=signed_lateness_ms,
            inter_frame_interval_ms=inter_interval_ms,
            interval_jitter_ms=jitter_ms,
        )


# ==============================================================================
# Client Echo Correctness & Per-Frame RTT Tracker
# ==============================================================================


class SessionEchoTracker:
    """Tracks sent vs echoed frames, detecting missing, duplicate, out-of-order, and corrupt frames."""

    def __init__(
        self,
        *,
        session_slot_id: int,
        seed: int,
        pattern: str = "deterministic",
        latency_window_capacity: int = DEFAULT_LATENCY_WINDOW_SIZE,
    ) -> None:
        self.session_slot_id: Final[int] = session_slot_id
        self.seed: Final[int] = seed
        self.pattern: Final[str] = pattern
        self._send_times_s: dict[int, float] = {}
        self._seen_sequences: set[int] = set()
        self._last_unique_seq: int = -1
        self.frames_sent: int = 0
        self.bytes_sent: int = 0
        self.frames_echoed_total: int = 0
        self.bytes_echoed_total: int = 0
        self.duplicate_frames: int = 0
        self.out_of_order_frames: int = 0
        self.payload_mismatches: int = 0
        self.cross_session_frames: int = 0
        self.echo_rtt_samples_ms: list[float] = []
        self.echo_rtt_aggregator = BoundedLatencyAggregator(
            window_capacity=latency_window_capacity,
            min_percentile_samples=1,
        )

    def record_sent(self, frame_seq: int, payload: bytes, send_monotonic_s: float) -> None:
        """Record that `frame_seq` was sent at `send_monotonic_s`."""
        self._send_times_s[frame_seq] = send_monotonic_s
        self.frames_sent += 1
        self.bytes_sent += len(payload)

    def record_received(self, payload: bytes, recv_monotonic_s: float) -> None:
        """Validate a received binary echo frame and record RTT and ordering metrics."""
        self.frames_echoed_total += 1
        self.bytes_echoed_total += len(payload)

        identity = extract_frame_identity(payload)
        if identity is None:
            self.payload_mismatches += 1
            return

        slot_id, frame_seq = identity
        if slot_id != self.session_slot_id:
            self.cross_session_frames += 1
            self.payload_mismatches += 1
            return

        if not verify_frame_payload(
            payload,
            seed=self.seed,
            session_slot_id=self.session_slot_id,
            frame_seq=frame_seq,
            pattern=self.pattern,
        ):
            self.payload_mismatches += 1
            return

        if frame_seq in self._seen_sequences:
            self.duplicate_frames += 1
            return

        if frame_seq != self._last_unique_seq + 1:
            self.out_of_order_frames += 1
        if frame_seq > self._last_unique_seq:
            self._last_unique_seq = frame_seq

        self._seen_sequences.add(frame_seq)
        sent_at = self._send_times_s.get(frame_seq)
        if sent_at is not None:
            rtt_ms = max(0.0, (recv_monotonic_s - sent_at) * 1000.0)
            self.echo_rtt_samples_ms.append(rtt_ms)
            self.echo_rtt_aggregator.record(rtt_ms)

    @property
    def unique_valid_echoes(self) -> int:
        return len(self._seen_sequences)

    @property
    def frames_missing(self) -> int:
        return max(0, self.frames_sent - len(self._seen_sequences))


# ==============================================================================
# Event-Loop Lag Monitor & Process Resource Measurement
# ==============================================================================


class EventLoopLagMonitor:
    """Periodic monotonic sleep probe measuring asyncio event-loop scheduling lag."""

    def __init__(
        self,
        *,
        probe_interval_s: float = 0.010,
        window_capacity: int = DEFAULT_LATENCY_WINDOW_SIZE,
    ) -> None:
        self._probe_interval_s: Final[float] = probe_interval_s
        self._aggregator = BoundedLatencyAggregator(
            window_capacity=window_capacity,
            min_percentile_samples=1,
        )
        self._stop_event = asyncio.Event()
        self._task: asyncio.Task[None] | None = None

    def start(self) -> None:
        if self._task is None:
            self._stop_event.clear()
            self._task = asyncio.create_task(
                self._run_loop(),
                name="m5-event-loop-lag-monitor",
            )

    async def stop(self) -> LatencyDistributionSummary:
        self._stop_event.set()
        if self._task is not None:
            try:
                await asyncio.wait_for(self._task, timeout=1.0)
            except (TimeoutError, asyncio.CancelledError):
                if not self._task.done():
                    self._task.cancel()
                    with contextlib.suppress(asyncio.CancelledError, Exception):
                        await self._task
            self._task = None
        return self._aggregator.snapshot()

    async def _run_loop(self) -> None:
        while not self._stop_event.is_set():
            t_before = time.perf_counter()
            expected_wakeup = t_before + self._probe_interval_s
            try:
                await asyncio.wait_for(
                    self._stop_event.wait(),
                    timeout=self._probe_interval_s,
                )
                break
            except TimeoutError:
                t_after = time.perf_counter()
                if t_after < expected_wakeup:
                    with contextlib.suppress(asyncio.CancelledError, TimeoutError):
                        await asyncio.sleep(expected_wakeup - t_after)
                    t_after = time.perf_counter()
                lag_ms = max(0.0, (t_after - expected_wakeup) * 1000.0)
                self._aggregator.record(lag_ms)


@dataclass(frozen=True, slots=True)
class ProcessRssMeasurement:
    """OS process Resident Set Size (Working Set) measurement."""

    available: bool
    current_rss_kib: float | None
    peak_rss_kib: float | None
    source: str


def measure_process_rss_kib() -> ProcessRssMeasurement:
    """Query OS process RSS in KiB using stdlib only (never conflated with tracemalloc heap)."""
    if sys.platform == "win32":
        try:

            class _PROCESS_MEMORY_COUNTERS(ctypes.Structure):
                _fields_ = [
                    ("cb", ctypes.wintypes.DWORD),
                    ("PageFaultCount", ctypes.wintypes.DWORD),
                    ("PeakWorkingSetSize", ctypes.c_size_t),
                    ("WorkingSetSize", ctypes.c_size_t),
                    ("QuotaPeakPagedPoolUsage", ctypes.c_size_t),
                    ("QuotaPagedPoolUsage", ctypes.c_size_t),
                    ("QuotaPeakNonPagedPoolUsage", ctypes.c_size_t),
                    ("QuotaNonPagedPoolUsage", ctypes.c_size_t),
                    ("PagefileUsage", ctypes.c_size_t),
                    ("PeakPagefileUsage", ctypes.c_size_t),
                ]

            k32 = ctypes.windll.kernel32
            k32.GetCurrentProcess.restype = ctypes.wintypes.HANDLE
            psapi = ctypes.windll.psapi
            psapi.GetProcessMemoryInfo.argtypes = [
                ctypes.wintypes.HANDLE,
                ctypes.POINTER(_PROCESS_MEMORY_COUNTERS),
                ctypes.wintypes.DWORD,
            ]
            psapi.GetProcessMemoryInfo.restype = ctypes.wintypes.BOOL
            pmc = _PROCESS_MEMORY_COUNTERS()
            pmc.cb = ctypes.sizeof(_PROCESS_MEMORY_COUNTERS)
            ok = psapi.GetProcessMemoryInfo(k32.GetCurrentProcess(), ctypes.byref(pmc), pmc.cb)
            if ok and pmc.WorkingSetSize > 0:
                return ProcessRssMeasurement(
                    available=True,
                    current_rss_kib=round(float(pmc.WorkingSetSize) / 1024.0, 2),
                    peak_rss_kib=round(float(pmc.PeakWorkingSetSize) / 1024.0, 2),
                    source="win32_GetProcessMemoryInfo_WorkingSetSize",
                )
        except Exception:
            pass
    else:
        try:
            status_path = Path("/proc/self/status")
            if status_path.is_file():
                current_kib: float | None = None
                peak_kib: float | None = None
                for line in status_path.read_text(encoding="utf-8").splitlines():
                    if line.startswith("VmRSS:"):
                        parts = line.split()
                        if len(parts) >= 2:
                            current_kib = float(parts[1])
                    elif line.startswith("VmHWM:"):
                        parts = line.split()
                        if len(parts) >= 2:
                            peak_kib = float(parts[1])
                if current_kib is not None:
                    return ProcessRssMeasurement(
                        available=True,
                        current_rss_kib=round(current_kib, 2),
                        peak_rss_kib=round(peak_kib, 2) if peak_kib is not None else None,
                        source="proc_self_status_VmRSS",
                    )
        except Exception:
            pass

    return ProcessRssMeasurement(
        available=False,
        current_rss_kib=None,
        peak_rss_kib=None,
        source="unavailable",
    )


def get_git_commit() -> str:
    """Resolve the current Git commit SHA."""
    try:
        res = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            check=True,
            capture_output=True,
            text=True,
            timeout=2.0,
        )
        return res.stdout.strip()
    except Exception:
        return "unknown"


# ==============================================================================
# Ephemeral Synthetic Media Server Context
# ==============================================================================


@dataclass
class RunningMediaServerContext:
    """Active loopback ASGI server instance configured with `/ws/synthetic-media/v1`."""

    app: Any
    server: uvicorn.Server
    server_task: asyncio.Task[None]
    port: int
    ws_base_url: str
    verifier: SyntheticTokenVerifier
    auth_bridge: SyntheticAuthBridge
    controller: AdmissionController
    coordinator: InstanceDrainCoordinator
    telemetry_sink: InMemoryVoiceTelemetrySink


def _get_free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


@contextlib.asynccontextmanager
async def start_ephemeral_media_server(
    *,
    max_concurrent_connections: int = DEFAULT_MAX_CONCURRENT_CONNECTIONS,
    max_connections_per_tenant: int = DEFAULT_MAX_CONNECTIONS_PER_TENANT,
    queue_capacity: int = 8,
    backpressure_timeout: float | None = 2.0,
    max_retained_sessions: int = 128,
    latency_window_capacity: int = DEFAULT_LATENCY_WINDOW_SIZE,
    ai_port_factory: Callable[[], Any] | None = None,
    block_outbound_binary_send_event: asyncio.Event | None = None,
    log_level: str = "warning",
) -> Any:
    """Start an isolated loopback Uvicorn server with `/ws/synthetic-media/v1` enabled."""
    secret_key = secrets.token_bytes(32)
    verifier = SyntheticTokenVerifier(
        secret_key=secret_key,
        enable_replay_protection=True,
    )
    auth_bridge = SyntheticAuthBridge(
        verifier=verifier,
        enforce_call_exclusivity=True,
    )
    config = CoreConfig(
        instance_name="synthetic-media-load-server",
        ingress_mode=INGRESS_MODE_SYNTHETIC,
        max_concurrent_connections=max_concurrent_connections,
        max_connections_per_tenant=max_connections_per_tenant,
    )
    controller = AdmissionController(config)
    coordinator = InstanceDrainCoordinator(controller=controller)
    telemetry_sink = InMemoryVoiceTelemetrySink(
        max_retained_sessions=max_retained_sessions,
        latency_window_capacity=latency_window_capacity,
        min_percentile_samples=1,
    )

    app = create_gateway_app(
        config=config,
        admission_controller=controller,
        drain_coordinator=coordinator,
        synthetic_auth_bridge=auth_bridge,
        enable_synthetic_media=True,
    )
    app.state.voice_telemetry_sink = telemetry_sink
    if ai_port_factory is not None:
        app.state.ai_media_port_factory = ai_port_factory
    else:
        app.state.ai_media_port_factory = lambda: BoundedInterruptibleFakeAIEchoPort(
            queue_capacity=queue_capacity,
            backpressure_timeout=backpressure_timeout,
        )

    asgi_app: Any = app
    if block_outbound_binary_send_event is not None:

        async def _blocking_outbound_binary_asgi_app(scope: Any, receive: Any, send: Any) -> None:
            async def _wrapped_send(message: Mapping[str, Any]) -> None:
                if (
                    message.get("type") == "websocket.send"
                    and message.get("bytes") is not None
                    and not block_outbound_binary_send_event.is_set()
                ):
                    await block_outbound_binary_send_event.wait()
                await send(message)

            await app(scope, receive, _wrapped_send)

        asgi_app = _blocking_outbound_binary_asgi_app

    port = _get_free_port()
    uv_cfg = uvicorn.Config(
        app=asgi_app,
        host="127.0.0.1",
        port=port,
        log_level=log_level,
        ws="auto",
    )
    server = uvicorn.Server(uv_cfg)
    server_task = asyncio.create_task(server.serve(), name=f"uvicorn-media-load-{port}")

    start_deadline = time.perf_counter() + 5.0
    while time.perf_counter() < start_deadline:
        if server.started:
            break
        await asyncio.sleep(0.02)

    if not server.started:
        server.should_exit = True
        with contextlib.suppress(Exception):
            await asyncio.wait_for(server_task, timeout=2.0)
        raise RuntimeError(f"Media load server failed to start on 127.0.0.1:{port}")

    ctx = RunningMediaServerContext(
        app=app,
        server=server,
        server_task=server_task,
        port=port,
        ws_base_url=f"ws://127.0.0.1:{port}",
        verifier=verifier,
        auth_bridge=auth_bridge,
        controller=controller,
        coordinator=coordinator,
        telemetry_sink=telemetry_sink,
    )
    try:
        yield ctx
    finally:
        server.should_exit = True
        try:
            await asyncio.wait_for(server_task, timeout=5.0)
        except (TimeoutError, Exception):
            if not server_task.done():
                server_task.cancel()
                with contextlib.suppress(asyncio.CancelledError, Exception):
                    await server_task


# ==============================================================================
# Process-Isolated Media Server Subprocess & Context
# ==============================================================================


@dataclass(slots=True)
class IsolatedServerExecutionResult:
    """Telemetry and execution metrics gathered from an isolated server child process."""

    telemetry: dict[str, Any]
    server_lag: dict[str, Any]
    server_cpu_time_seconds: float
    server_rss_peak_kib: int
    server_rss_after_kib: int
    final_active_connections: int
    final_active_tenants: int
    final_active_calls: int
    final_active_voice_sessions: int
    recent_sessions: Sequence[dict[str, Any]]


@dataclass(slots=True)
class RunningIsolatedServerContext:
    """Active loopback ASGI server running in a dedicated isolated child process."""

    proc: asyncio.subprocess.Process
    port: int
    pid: int
    ws_base_url: str
    secret_key_hex: str
    verifier: SyntheticTokenVerifier
    result: IsolatedServerExecutionResult | None = None

    def poll(self) -> int | None:
        """Return exit code if child terminated, or None if still running."""
        return self.proc.returncode


def _reconstruct_latency_summary(d: Mapping[str, Any]) -> LatencyDistributionSummary:
    """Reconstruct a LatencyDistributionSummary from JSON-serialized dictionary."""
    reason_str = str(d.get("availability_reason", MetricAvailabilityReason.NO_SAMPLES.value))
    try:
        reason = MetricAvailabilityReason(reason_str)
    except ValueError:
        reason = MetricAvailabilityReason.NO_SAMPLES
    return LatencyDistributionSummary(
        available=bool(d.get("available", False)),
        availability_reason=reason,
        sample_count=int(d.get("sample_count", 0)),
        retained_sample_count=int(d.get("retained_sample_count", 0)),
        window_capacity=int(d.get("window_capacity", DEFAULT_LATENCY_WINDOW_SIZE)),
        is_small_sample=bool(d.get("is_small_sample", False)),
        min_ms=float(d["min_ms"]) if d.get("min_ms") is not None else None,
        max_ms=float(d["max_ms"]) if d.get("max_ms") is not None else None,
        mean_ms=float(d["mean_ms"]) if d.get("mean_ms") is not None else None,
        p50_ms=float(d["p50_ms"]) if d.get("p50_ms") is not None else None,
        p95_ms=float(d["p95_ms"]) if d.get("p95_ms") is not None else None,
        p99_ms=float(d["p99_ms"]) if d.get("p99_ms") is not None else None,
    )


@dataclass(slots=True)
class _ReconstructedSessionSnapshot:
    """Lightweight session metrics adapter reconstructed from child process JSON."""

    inbound_processing_ms: LatencyDistributionSummary
    outbound_processing_ms: LatencyDistributionSummary
    inbound_queue_wait_ms: LatencyDistributionSummary
    outbound_queue_wait_ms: LatencyDistributionSummary
    network_transport_ms: LatencyDistributionSummary


def _reconstruct_session_snapshot(data: Mapping[str, Any]) -> _ReconstructedSessionSnapshot:
    return _ReconstructedSessionSnapshot(
        inbound_processing_ms=_reconstruct_latency_summary(data.get("inbound_processing_ms", {})),
        outbound_processing_ms=_reconstruct_latency_summary(data.get("outbound_processing_ms", {})),
        inbound_queue_wait_ms=_reconstruct_latency_summary(data.get("inbound_queue_wait_ms", {})),
        outbound_queue_wait_ms=_reconstruct_latency_summary(data.get("outbound_queue_wait_ms", {})),
        network_transport_ms=_reconstruct_latency_summary(data.get("network_transport_ms", {})),
    )


@contextlib.asynccontextmanager
async def start_isolated_media_server_process(
    *,
    port: int = 0,
    max_concurrent_connections: int = DEFAULT_MAX_CONCURRENT_CONNECTIONS,
    max_connections_per_tenant: int = DEFAULT_MAX_CONNECTIONS_PER_TENANT,
    queue_capacity: int = 8,
    backpressure_timeout: float = 2.0,
    max_retained_sessions: int = 128,
    latency_window_capacity: int = DEFAULT_LATENCY_WINDOW_SIZE,
    secret_key_hex: str | None = None,
    startup_timeout: float = 10.0,
    shutdown_timeout: float = 10.0,
) -> AsyncIterator[RunningIsolatedServerContext]:
    """Launch an isolated loopback Gateway server in a dedicated child OS process.

    Enforces:
    - Secret passed via private environment variable (never exposed on command line).
    - Native asynchronous subprocess I/O (no thread-pool pipe read leakage on cancellation).
    - Kernel-assigned port=0 (no TOCTOU port race).
    - Active bidirectional TCP socket connectivity verification before yielding.
    - Strict schema validation of JSON IPC.
    - Guaranteed cleanup (kill and wait) on timeout, error, or normal shutdown.
    """
    repo_dir = Path(__file__).resolve().parent.parent
    src_dir = repo_dir / "src"

    env = dict(os.environ)
    existing_pythonpath = env.get("PYTHONPATH", "")
    env["PYTHONPATH"] = os.pathsep.join(
        [p for p in (str(src_dir), str(repo_dir), existing_pythonpath) if p]
    )

    secret_key_bytes = (
        bytes.fromhex(secret_key_hex) if secret_key_hex is not None else secrets.token_bytes(32)
    )
    secret_key_hex_val = secret_key_bytes.hex()
    env[KW_SYNTHETIC_MEDIA_SECRET_KEY_ENV] = secret_key_hex_val

    cmd = [
        sys.executable,
        "-m",
        "tools.synthetic_media_load",
        "--server-subprocess",
        "--port",
        str(port),
        "--max-concurrent-connections",
        str(max_concurrent_connections),
        "--max-connections-per-tenant",
        str(max_connections_per_tenant),
        "--queue-capacity",
        str(queue_capacity),
        "--backpressure-timeout",
        str(backpressure_timeout),
        "--latency-window-capacity",
        str(latency_window_capacity),
        "--max-retained-sessions",
        str(max_retained_sessions),
    ]

    proc = await asyncio.create_subprocess_exec(
        *cmd,
        stdin=asyncio.subprocess.PIPE,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
        limit=16 * 1024 * 1024,
        cwd=str(repo_dir),
        env=env,
    )

    ctx: RunningIsolatedServerContext | None = None
    try:
        if proc.stdout is None:
            raise RuntimeError("Subprocess stdout pipe is not available")

        ready_data: dict[str, Any] | None = None
        t_deadline = time.perf_counter() + startup_timeout
        while time.perf_counter() < t_deadline:
            if proc.returncode is not None:
                stderr_text = ""
                if proc.stderr:
                    with contextlib.suppress(Exception):
                        raw_err = await asyncio.wait_for(proc.stderr.read(), timeout=1.0)
                        stderr_text = raw_err.decode(errors="replace")
                raise RuntimeError(
                    f"Server child process exited prematurely with code {proc.returncode}: {stderr_text.strip()}"
                )
            remaining_s = max(0.1, t_deadline - time.perf_counter())
            try:
                line_bytes = await asyncio.wait_for(
                    proc.stdout.readline(),
                    timeout=remaining_s,
                )
            except TimeoutError:
                break

            if not line_bytes:
                # EOF reached on stdout before ready signal
                stderr_text = ""
                if proc.stderr:
                    with contextlib.suppress(Exception):
                        raw_err = await asyncio.wait_for(proc.stderr.read(), timeout=1.0)
                        stderr_text = raw_err.decode(errors="replace")
                retcode = proc.returncode
                err_detail = (
                    f"code {retcode}"
                    if retcode is not None
                    else "premature stdout closure without ready signal"
                )
                raise RuntimeError(
                    f"Server child process exited prematurely ({err_detail}): {stderr_text.strip()}"
                )

            line_str = line_bytes.decode(errors="replace").strip()
            if line_str.startswith("{") and line_str.endswith("}"):
                try:
                    ev = json.loads(line_str)
                    if not isinstance(ev, dict):
                        continue
                    event_type = ev.get("event")
                    if event_type == "ready":
                        # Validate handshake schema strictly
                        port_val = ev.get("port")
                        pid_val = ev.get("pid")
                        if not isinstance(port_val, int) or port_val <= 0:
                            raise ValueError(f"Invalid handshake port: {port_val!r}")
                        if not isinstance(pid_val, int) or pid_val <= 0:
                            raise ValueError(f"Invalid handshake pid: {pid_val!r}")
                        ready_data = ev
                        break
                    elif event_type == "error":
                        raise RuntimeError(f"Server subprocess startup failed: {ev.get('error')}")
                except json.JSONDecodeError:
                    continue

        if ready_data is None:
            raise TimeoutError(
                f"Server child process failed to signal ready within {startup_timeout}s"
            )

        actual_port = int(ready_data["port"])
        actual_pid = int(ready_data["pid"])

        # Active TCP socket connectivity probe from parent
        with socket.create_connection(("127.0.0.1", actual_port), timeout=2.0):
            pass

        verifier = SyntheticTokenVerifier(
            secret_key=secret_key_bytes,
            enable_replay_protection=True,
        )
        ctx = RunningIsolatedServerContext(
            proc=proc,
            port=actual_port,
            pid=actual_pid,
            ws_base_url=f"ws://127.0.0.1:{actual_port}",
            secret_key_hex=secret_key_hex_val,
            verifier=verifier,
        )
        yield ctx
    finally:
        try:
            if proc.returncode is None and proc.stdin is not None:
                try:
                    proc.stdin.write(json.dumps({"command": "shutdown"}).encode("utf-8") + b"\n")
                    await proc.stdin.drain()
                except (BrokenPipeError, ConnectionResetError):
                    pass

                t_shut_deadline = time.perf_counter() + shutdown_timeout
                while time.perf_counter() < t_shut_deadline and proc.stdout is not None:
                    remaining_shut = max(0.1, t_shut_deadline - time.perf_counter())
                    try:
                        line_bytes = await asyncio.wait_for(
                            proc.stdout.readline(),
                            timeout=remaining_shut,
                        )
                    except TimeoutError:
                        break

                    if not line_bytes:
                        break
                    line_str = line_bytes.decode(errors="replace").strip()
                    if line_str.startswith("{") and line_str.endswith("}"):
                        try:
                            ev = json.loads(line_str)
                            if (
                                isinstance(ev, dict)
                                and ev.get("event") == "telemetry"
                                and ctx is not None
                            ):
                                ctx.result = IsolatedServerExecutionResult(
                                    telemetry=dict(ev.get("telemetry", {})),
                                    server_lag=dict(ev.get("server_lag", {})),
                                    server_cpu_time_seconds=float(
                                        ev.get("server_cpu_time_seconds", 0.0)
                                    ),
                                    server_rss_peak_kib=int(ev.get("server_rss_peak_kib", 0)),
                                    server_rss_after_kib=int(ev.get("server_rss_after_kib", 0)),
                                    final_active_connections=int(
                                        ev.get("final_active_connections", 0)
                                    ),
                                    final_active_tenants=int(ev.get("final_active_tenants", 0)),
                                    final_active_calls=int(ev.get("final_active_calls", 0)),
                                    final_active_voice_sessions=int(
                                        ev.get("final_active_voice_sessions", 0)
                                    ),
                                    recent_sessions=list(ev.get("recent_sessions", [])),
                                )
                                break
                        except json.JSONDecodeError:
                            continue

            if proc.returncode is None:
                await asyncio.wait_for(proc.wait(), timeout=shutdown_timeout)
        except Exception:
            if proc.returncode is None:
                with contextlib.suppress(Exception):
                    proc.kill()
                with contextlib.suppress(Exception):
                    await asyncio.wait_for(proc.wait(), timeout=2.0)
            raise
        finally:
            if proc.returncode is None:
                with contextlib.suppress(Exception):
                    proc.kill()
                with contextlib.suppress(Exception):
                    await asyncio.wait_for(proc.wait(), timeout=2.0)


def run_isolated_server_process(
    *,
    port: int = 0,
    max_concurrent_connections: int = DEFAULT_MAX_CONCURRENT_CONNECTIONS,
    max_connections_per_tenant: int = DEFAULT_MAX_CONNECTIONS_PER_TENANT,
    queue_capacity: int = 8,
    backpressure_timeout: float = 2.0,
    max_retained_sessions: int = 128,
    latency_window_capacity: int = DEFAULT_LATENCY_WINDOW_SIZE,
    secret_key_hex: str | None = None,
) -> int:
    """Child process entry point for running the isolated Gateway server."""
    import logging

    logging.basicConfig(stream=sys.stderr, level=logging.WARNING, force=True)
    for logger_name in ("uvicorn", "uvicorn.error", "uvicorn.access"):
        lgr = logging.getLogger(logger_name)
        lgr.handlers.clear()
        lgr.addHandler(logging.StreamHandler(sys.stderr))
        lgr.setLevel(logging.WARNING)

    async def _server_main() -> int:
        env_key_hex = os.environ.get(KW_SYNTHETIC_MEDIA_SECRET_KEY_ENV)
        effective_key_hex = secret_key_hex or env_key_hex
        secret_key = (
            bytes.fromhex(effective_key_hex) if effective_key_hex else secrets.token_bytes(32)
        )
        verifier = SyntheticTokenVerifier(
            secret_key=secret_key,
            enable_replay_protection=True,
        )
        auth_bridge = SyntheticAuthBridge(
            verifier=verifier,
            enforce_call_exclusivity=True,
        )
        config = CoreConfig(
            instance_name="synthetic-media-load-isolated-server",
            ingress_mode=INGRESS_MODE_SYNTHETIC,
            max_concurrent_connections=max_concurrent_connections,
            max_connections_per_tenant=max_connections_per_tenant,
        )
        controller = AdmissionController(config)
        coordinator = InstanceDrainCoordinator(controller=controller)
        telemetry_sink = InMemoryVoiceTelemetrySink(
            max_retained_sessions=max_retained_sessions,
            latency_window_capacity=latency_window_capacity,
            min_percentile_samples=1,
        )

        app = create_gateway_app(
            config=config,
            admission_controller=controller,
            drain_coordinator=coordinator,
            synthetic_auth_bridge=auth_bridge,
            enable_synthetic_media=True,
        )
        app.state.voice_telemetry_sink = telemetry_sink
        app.state.ai_media_port_factory = lambda: BoundedInterruptibleFakeAIEchoPort(
            queue_capacity=queue_capacity,
            backpressure_timeout=backpressure_timeout,
        )

        # Bind with port=0 for atomic kernel ephemeral port allocation (eliminates TOCTOU race)
        bind_port = port if port > 0 else 0
        uv_cfg = uvicorn.Config(
            app=app,
            host="127.0.0.1",
            port=bind_port,
            log_level="warning",
            ws="auto",
        )
        server = uvicorn.Server(uv_cfg)
        server_task = asyncio.create_task(server.serve(), name="uvicorn-isolated-server")

        t_limit = time.perf_counter() + 10.0
        while not server.started:
            if server_task.done():
                raise RuntimeError("Uvicorn server task terminated prematurely during startup")
            if time.perf_counter() > t_limit:
                raise TimeoutError("Uvicorn server failed to signal started within 10s")
            await asyncio.sleep(0.01)

        # Discover dynamic kernel-assigned ephemeral port
        actual_port = bind_port
        if server.servers and server.servers[0].sockets:
            actual_port = int(server.servers[0].sockets[0].getsockname()[1])

        # Active TCP socket connectivity verification from child
        with socket.create_connection(("127.0.0.1", actual_port), timeout=2.0):
            pass

        lag_monitor = EventLoopLagMonitor(
            probe_interval_s=0.010,
            window_capacity=latency_window_capacity,
        )
        lag_monitor.start()

        t_cpu_start = time.process_time()
        measure_process_rss_kib()

        # Send ready event without leaking secret_key_hex
        handshake = {
            "event": "ready",
            "pid": os.getpid(),
            "port": actual_port,
        }
        sys.stdout.write(json.dumps(handshake) + "\n")
        sys.stdout.flush()

        cmd_line = await asyncio.to_thread(sys.stdin.readline)
        if cmd_line:
            with contextlib.suppress(Exception):
                json.loads(cmd_line.strip())

        lag_summary = await lag_monitor.stop()

        server.should_exit = True
        await server_task

        cpu_duration_s = max(0.0, time.process_time() - t_cpu_start)
        rss_after = measure_process_rss_kib()

        snap_admin = controller.snapshot()
        sink_agg = telemetry_sink.snapshot()

        response = {
            "event": "telemetry",
            "telemetry": sink_agg.to_dict(),
            "server_lag": lag_summary.to_dict(),
            "server_cpu_time_seconds": round(cpu_duration_s, 4),
            "server_rss_peak_kib": rss_after.peak_rss_kib if rss_after.available else 0,
            "server_rss_after_kib": rss_after.current_rss_kib if rss_after.available else 0,
            "final_active_connections": snap_admin.active_connections,
            "final_active_tenants": snap_admin.active_tenants,
            "final_active_calls": len(auth_bridge._active_calls),
            "final_active_voice_sessions": telemetry_sink.active_sessions,
            "recent_sessions": [s.to_dict() for s in sink_agg.recent_sessions],
        }
        sys.stdout.write(json.dumps(response) + "\n")
        sys.stdout.flush()
        return 0

    try:
        return asyncio.run(_server_main())
    except Exception as exc:
        err_msg = {"event": "error", "error": f"{type(exc).__name__}: {exc}"}
        sys.stdout.write(json.dumps(err_msg) + "\n")
        sys.stdout.flush()
        return 1


# ==============================================================================
# Single Paced Media Call Worker & Stage Results
# ==============================================================================


@dataclass(frozen=True, slots=True)
class CallMediaRunRecord:
    """Sanitized per-call outcome from a paced synthetic media session."""

    session_slot_id: int
    admitted: bool
    completed_cleanly: bool
    close_code: int
    frames_scheduled: int
    frames_sent: int
    frames_skipped: int
    missed_deadlines: int
    frames_echoed: int
    frames_missing: int
    duplicate_frames: int
    out_of_order_frames: int
    payload_mismatches: int
    cross_session_frames: int
    first_frame_sent_s: float | None
    last_frame_activity_s: float | None
    handshake_ms: float
    call_start_rtt_ms: float
    call_end_rtt_ms: float
    total_call_duration_ms: float
    echo_rtt_summary: LatencyDistributionSummary
    echo_rtt_samples_ms: tuple[float, ...]
    scheduling_lateness_samples_ms: tuple[float, ...]
    scheduling_interval_samples_ms: tuple[float, ...]
    scheduling_jitter_samples_ms: tuple[float, ...]
    dispatch_skew_ms: float = 0.0
    error_category: str | None = None
    last_frame_sent_s: float | None = None


async def run_single_paced_media_call(
    *,
    ws_url: str,
    token: str,
    call_id: str,
    session_slot_id: int,
    frames_to_schedule: int,
    interval_ms: float = float(PCM16LE_FRAME_MS),
    seed: int = 42,
    pattern: str = "deterministic",
    skip_threshold_ms: float | None = None,
    phase_offset_ms: float = 0.0,
    start_barrier: asyncio.Barrier | None = None,
    shared_epoch_s: float | None = None,
    shared_epoch_holder: list[float] | None = None,
    epoch_ready_event: asyncio.Event | None = None,
    timeout: float = 15.0,
    latency_window_capacity: int = DEFAULT_LATENCY_WINDOW_SIZE,
) -> CallMediaRunRecord:
    """Execute one authenticated, barrier-synchronized, 20 ms-paced binary media call."""
    t_call_start = time.perf_counter()
    pacer = MonotonicFramePacer(
        interval_ms=interval_ms,
        skip_threshold_ms=skip_threshold_ms,
        phase_offset_ms=phase_offset_ms,
    )
    tracker = SessionEchoTracker(
        session_slot_id=session_slot_id,
        seed=seed,
        pattern=pattern,
        latency_window_capacity=latency_window_capacity,
    )

    lateness_samples: list[float] = []
    interval_samples: list[float] = []
    jitter_samples: list[float] = []
    first_sent_s: float | None = None
    last_sent_s: float | None = None
    last_activity_s: float | None = None
    handshake_ms = 0.0
    call_start_rtt_ms = 0.0
    call_end_rtt_ms = 0.0
    close_code = 1006
    admitted = False
    completed_cleanly = False

    # Pre-build deterministic payloads for this call so payload construction does not skew send pacing
    payloads = [
        build_deterministic_frame_payload(
            seed=seed,
            session_slot_id=session_slot_id,
            frame_seq=seq,
            pattern=pattern,
        )
        for seq in range(frames_to_schedule)
    ]

    headers = {"Authorization": f"Bearer {token}"}
    t0_hs = time.perf_counter()
    async with websockets.connect(
        ws_url,
        additional_headers=headers,
        max_size=1024 * 1024,
    ) as ws:
        ready_raw = await asyncio.wait_for(ws.recv(), timeout=timeout)
        if not isinstance(ready_raw, str):
            raise RuntimeError("Expected text session.ready frame")
        ready_msg = json.loads(ready_raw)
        if ready_msg.get("event") != EVENT_SESSION_READY:
            raise RuntimeError(f"Unexpected greeting event: {ready_msg.get('event')}")
        admitted = True
        handshake_ms = max(0.0, (time.perf_counter() - t0_hs) * 1000.0)

        # Activate media session via call.start -> call.started
        t0_start = time.perf_counter()
        await ws.send(json.dumps({"event": EVENT_CALL_START, "call_id": call_id}))
        started_raw = await asyncio.wait_for(ws.recv(), timeout=timeout)
        if not isinstance(started_raw, str):
            raise RuntimeError("Expected text call.started frame")
        started_msg = json.loads(started_raw)
        if started_msg.get("event") != EVENT_CALL_STARTED:
            raise RuntimeError(f"Unexpected start response: {started_msg.get('event')}")
        call_start_rtt_ms = max(0.0, (time.perf_counter() - t0_start) * 1000.0)

        # Wait at shared barrier so all concurrent calls begin media streaming simultaneously
        if start_barrier is not None:
            await asyncio.wait_for(start_barrier.wait(), timeout=timeout)

        # Coordinate single shared monotonic epoch across all concurrent clients
        if epoch_ready_event is not None:
            await asyncio.wait_for(epoch_ready_event.wait(), timeout=timeout)

        effective_epoch_s: float
        if shared_epoch_holder and len(shared_epoch_holder) > 0:
            effective_epoch_s = shared_epoch_holder[0]
        elif shared_epoch_s is not None:
            effective_epoch_s = shared_epoch_s
        else:
            effective_epoch_s = time.perf_counter()

        dispatch_skew_ms = 0.0

        all_sent_event = asyncio.Event()
        all_echoed_event = asyncio.Event()
        control_messages: asyncio.Queue[dict[str, Any]] = asyncio.Queue()

        async def _sender_loop() -> None:
            nonlocal first_sent_s, last_sent_s, last_activity_s, dispatch_skew_ms
            pacer.reset(t0=effective_epoch_s)
            sent_seq = 0
            for slot_idx in range(frames_to_schedule):
                sched = await pacer.await_slot(slot_idx)
                if sched.skipped:
                    continue
                payload = payloads[sent_seq]
                t_send = time.perf_counter()
                await ws.send(payload)
                if first_sent_s is None:
                    first_sent_s = t_send
                    target_slot0_s = effective_epoch_s + pacer.phase_offset_s
                    dispatch_skew_ms = max(0.0, (t_send - target_slot0_s) * 1000.0)
                last_sent_s = t_send
                last_activity_s = t_send
                tracker.record_sent(sent_seq, payload, t_send)
                lateness_samples.append(sched.lateness_ms)
                if sched.inter_frame_interval_ms is not None:
                    interval_samples.append(sched.inter_frame_interval_ms)
                if sched.interval_jitter_ms is not None:
                    jitter_samples.append(sched.interval_jitter_ms)
                sent_seq += 1
            all_sent_event.set()
            if tracker.unique_valid_echoes >= tracker.frames_sent:
                all_echoed_event.set()

        async def _receiver_loop() -> None:
            nonlocal last_activity_s
            while True:
                msg = await ws.recv()
                t_recv = time.perf_counter()
                last_activity_s = t_recv
                if isinstance(msg, bytes):
                    tracker.record_received(msg, t_recv)
                    if (
                        all_sent_event.is_set()
                        and tracker.unique_valid_echoes >= tracker.frames_sent
                    ):
                        all_echoed_event.set()
                elif isinstance(msg, str):
                    ctrl = json.loads(msg)
                    await control_messages.put(ctrl)
                    if ctrl.get("event") == EVENT_CALL_ENDED:
                        break

        sender_task = asyncio.create_task(_sender_loop())
        receiver_task = asyncio.create_task(_receiver_loop())
        try:
            await asyncio.wait_for(
                sender_task, timeout=timeout + (frames_to_schedule * interval_ms / 1000.0)
            )
            # Wait for remaining echoed frames before sending call.end
            await asyncio.wait_for(all_echoed_event.wait(), timeout=timeout)

            t0_end = time.perf_counter()
            await ws.send(json.dumps({"event": EVENT_CALL_END, "call_id": call_id}))
            ended_ctrl = await asyncio.wait_for(control_messages.get(), timeout=timeout)
            if ended_ctrl.get("event") != EVENT_CALL_ENDED:
                raise RuntimeError(f"Expected call.ended, got {ended_ctrl.get('event')}")
            call_end_rtt_ms = max(0.0, (time.perf_counter() - t0_end) * 1000.0)

            await asyncio.wait_for(receiver_task, timeout=timeout)
            await asyncio.wait_for(ws.wait_closed(), timeout=timeout)
            close_code = ws.close_code if ws.close_code is not None else 1000
            completed_cleanly = close_code == 1000
        finally:
            for t in (sender_task, receiver_task):
                if not t.done():
                    t.cancel()
            await asyncio.gather(sender_task, receiver_task, return_exceptions=True)

    total_duration_ms = max(0.0, (time.perf_counter() - t_call_start) * 1000.0)
    return CallMediaRunRecord(
        session_slot_id=session_slot_id,
        admitted=admitted,
        completed_cleanly=completed_cleanly,
        close_code=close_code,
        frames_scheduled=frames_to_schedule,
        frames_sent=tracker.frames_sent,
        frames_skipped=pacer.skipped_deadlines,
        missed_deadlines=pacer.missed_deadlines,
        frames_echoed=tracker.unique_valid_echoes,
        frames_missing=tracker.frames_missing,
        duplicate_frames=tracker.duplicate_frames,
        out_of_order_frames=tracker.out_of_order_frames,
        payload_mismatches=tracker.payload_mismatches,
        cross_session_frames=tracker.cross_session_frames,
        first_frame_sent_s=first_sent_s,
        last_frame_sent_s=last_sent_s,
        last_frame_activity_s=last_activity_s,
        handshake_ms=handshake_ms,
        call_start_rtt_ms=call_start_rtt_ms,
        call_end_rtt_ms=call_end_rtt_ms,
        total_call_duration_ms=total_duration_ms,
        echo_rtt_summary=tracker.echo_rtt_aggregator.snapshot(),
        echo_rtt_samples_ms=tuple(tracker.echo_rtt_samples_ms),
        scheduling_lateness_samples_ms=tuple(lateness_samples),
        scheduling_interval_samples_ms=tuple(interval_samples),
        scheduling_jitter_samples_ms=tuple(jitter_samples),
        dispatch_skew_ms=round(dispatch_skew_ms, 4),
    )


def _summarize_samples(
    samples: Sequence[float],
    *,
    window_capacity: int | None = None,
) -> LatencyDistributionSummary:
    """Feed a sequence of samples through BoundedLatencyAggregator for uniform percentile semantics.

    When `window_capacity` is None, offline stage-level aggregation sizes the window
    to `max(DEFAULT_LATENCY_WINDOW_SIZE, len(samples))` so zero samples from earlier
    sessions are silently truncated (remediating F-1).
    """
    effective_capacity = (
        window_capacity
        if window_capacity is not None
        else max(DEFAULT_LATENCY_WINDOW_SIZE, len(samples))
    )
    agg = BoundedLatencyAggregator(
        window_capacity=effective_capacity,
        min_percentile_samples=1,
    )
    for val in samples:
        agg.record(max(0.0, float(val)))
    return agg.snapshot()


def _summarize_pooled_stage_samples(
    samples: Sequence[float],
    *,
    note: str | None = None,
) -> dict[str, Any]:
    """Compute exact pooled per-frame stage percentiles across all sessions without FIFO truncation."""
    summary = _summarize_samples(samples, window_capacity=None)
    result = summary.to_dict()
    result["summary_method"] = "pooled_per_frame_exact"
    result["memory_bound_note"] = (
        "Offline stage-level exact aggregation retains raw float samples; memory consumption is "
        "finite and configuration-dependent (O(concurrency * frames_per_call)). For 6,250 samples "
        "(S3: 25 calls x 250 frames), raw 64-bit IEEE 754 float payload is ~48.8 KiB (6,250 * 8 bytes); "
        "actual CPython object/list container memory is ~200-300 KiB (32-48 bytes/sample). "
        "Production per-session telemetry windows remain strictly bounded at window_capacity=256."
    )
    if note is not None:
        result["note"] = note
    return result


def _summarize_session_metric_across_calls(
    snapshots: Sequence[VoiceSessionMetricsSnapshot],
    extractor: Callable[[VoiceSessionMetricsSnapshot], LatencyDistributionSummary],
    *,
    window_capacity: int = DEFAULT_LATENCY_WINDOW_SIZE,
) -> dict[str, Any]:
    """Summarize a per-session LatencyDistributionSummary across completed calls.

    Explicitly labeled as `across_session_percentiles` (`sample_count` = number of sessions
    measured, `total_frames_measured` = total frames across those sessions) so a percentile
    of session percentiles is never misrepresented as a pooled per-frame percentile (remediating F-5).
    """
    measured = [extractor(s) for s in snapshots if extractor(s).available]
    total_samples = sum(m.sample_count for m in measured)
    if not measured or total_samples == 0:
        unavail = LatencyDistributionSummary.unavailable(
            MetricAvailabilityReason.NO_SAMPLES,
            window_capacity=window_capacity,
        ).to_dict()
        unavail["summary_method"] = "across_session_percentiles"
        unavail["sessions_measured"] = 0
        unavail["total_frames_measured"] = 0
        unavail["retained_frames_across_sessions"] = 0
        return unavail

    min_vals = [m.min_ms for m in measured if m.min_ms is not None]
    max_vals = [m.max_ms for m in measured if m.max_ms is not None]
    p50_vals = [m.p50_ms for m in measured if m.p50_ms is not None]
    p95_vals = [m.p95_ms for m in measured if m.p95_ms is not None]
    p99_vals = [m.p99_ms for m in measured if m.p99_ms is not None]
    weighted_sum = sum((m.mean_ms or 0.0) * m.sample_count for m in measured)
    retained_total = sum(m.retained_sample_count for m in measured)

    p95_summary = _summarize_samples(p95_vals, window_capacity=None)
    p50_summary = _summarize_samples(p50_vals, window_capacity=None)
    p99_summary = _summarize_samples(p99_vals, window_capacity=None)

    session_p50_median = round(p50_summary.p50_ms, 4) if p50_summary.p50_ms is not None else None
    session_p95_median = round(p95_summary.p50_ms, 4) if p95_summary.p50_ms is not None else None
    session_p95_p95 = round(p95_summary.p95_ms, 4) if p95_summary.p95_ms is not None else None
    session_p99_p99 = round(p99_summary.p99_ms, 4) if p99_summary.p99_ms is not None else None
    max_session_p95 = round(max(p95_vals), 4) if p95_vals else None
    weighted_mean = round(weighted_sum / total_samples, 4)

    return {
        "available": True,
        "availability_reason": MetricAvailabilityReason.MEASURED.value,
        "summary_method": "across_session_percentiles",
        "sample_count": len(measured),
        "retained_sample_count": len(measured),
        "sessions_measured": len(measured),
        "total_frames_measured": total_samples,
        "retained_frames_across_sessions": retained_total,
        "window_capacity_per_session": window_capacity,
        "is_small_sample": len(measured) < SMALL_SAMPLE_THRESHOLD,
        "min_ms": round(min(min_vals), 4) if min_vals else None,
        "max_ms": round(max(max_vals), 4) if max_vals else None,
        "mean_ms": weighted_mean,
        "weighted_frame_mean_ms": weighted_mean,
        "frame_weighted_mean_latency_ms": weighted_mean,
        "p50_ms": session_p50_median,
        "p95_ms": session_p95_p95,
        "p99_ms": session_p99_p99,
        "session_p50_median_ms": session_p50_median,
        "session_p95_median_ms": session_p95_median,
        "session_p95_p95_ms": session_p95_p95,
        "session_p99_p99_ms": session_p99_p99,
        "max_session_p95_ms": max_session_p95,
        "population_definitions": {
            "sessions_measured": "Count of distinct active Voice sessions contributing metrics (sample_count).",
            "total_frames_measured": "Sum of all individual audio frames across contributing sessions.",
            "session_p50_median_ms": "Median (50th percentile) of session p50 medians across sessions.",
            "session_p95_median_ms": "Median (50th percentile) of session p95 values across sessions.",
            "session_p95_p95_ms": "95th percentile of session p95 values across sessions.",
            "session_p99_p99_ms": "99th percentile of session p99 values across sessions.",
            "max_session_p95_ms": "Maximum observed session p95 among all sessions.",
            "frame_weighted_mean_latency_ms": (
                "Exact pooled frame-weighted mean: sum(session.mean_ms * session.sample_count) / total_frames_measured."
            ),
        },
        "note": (
            "Summarized across per-session VoiceSessionMetricsSnapshot summaries "
            "(session_p50_median_ms = median of session p50s; session_p95_median_ms = median of session p95s; "
            "session_p95_p95_ms = p95 of session p95s; "
            "frame_weighted_mean_latency_ms = sum(session.mean * session.frames) / total_frames; "
            "sample_count = sessions_measured). Not a pooled per-frame percentile."
        ),
    }


@dataclass(frozen=True, slots=True)
class PacedStageResult:
    """Complete empirical result for a single concurrency stage (e.g. S1=1, S2=5, S3=25)."""

    stage_name: str
    status: str  # "PASS" | "FAIL"
    requested_concurrency: int
    admitted_concurrency: int
    achieved_media_concurrency: int
    completed_calls: int
    expected_rejections: int
    unexpected_rejections: int
    unexpected_disconnects: int
    configured_duration_seconds: float
    wall_clock_duration_seconds: float
    concurrent_media_overlap_ms: float
    media_correctness: dict[str, Any]
    timing_ms: dict[str, Any]
    gateway_telemetry: dict[str, Any]
    resources: dict[str, Any]
    cleanup_verification: dict[str, Any]
    failure_reasons: tuple[str, ...] = ()
    media_integrity_status: str = "PASS"
    realtime_pacing_status: str = "PASS"
    media_failure_reasons: tuple[str, ...] = ()
    pacing_failure_reasons: tuple[str, ...] = ()
    pacing_acceptance_criteria: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "stage_name": self.stage_name,
            "status": self.status,
            "media_integrity_status": self.media_integrity_status,
            "realtime_pacing_status": self.realtime_pacing_status,
            "requested_concurrency": self.requested_concurrency,
            "admitted_concurrency": self.admitted_concurrency,
            "achieved_media_concurrency": self.achieved_media_concurrency,
            "completed_calls": self.completed_calls,
            "expected_rejections": self.expected_rejections,
            "unexpected_rejections": self.unexpected_rejections,
            "unexpected_disconnects": self.unexpected_disconnects,
            "configured_duration_seconds": self.configured_duration_seconds,
            "wall_clock_duration_seconds": round(self.wall_clock_duration_seconds, 4),
            "concurrent_media_overlap_ms": round(self.concurrent_media_overlap_ms, 2),
            "media_correctness": self.media_correctness,
            "timing_ms": self.timing_ms,
            "gateway_telemetry": self.gateway_telemetry,
            "resources": self.resources,
            "cleanup_verification": self.cleanup_verification,
            "failure_reasons": list(self.failure_reasons),
            "media_failure_reasons": list(self.media_failure_reasons),
            "pacing_failure_reasons": list(self.pacing_failure_reasons),
            "pacing_acceptance_criteria": dict(self.pacing_acceptance_criteria),
        }


async def run_paced_media_stage(
    *,
    stage_name: str,
    concurrency: int,
    duration_seconds: float = 5.0,
    interval_ms: float = float(PCM16LE_FRAME_MS),
    seed: int = 42,
    pattern: str = "deterministic",
    queue_capacity: int = 8,
    backpressure_timeout: float = 2.0,
    skip_threshold_ms: float | None = None,
    max_concurrent_connections: int = DEFAULT_MAX_CONCURRENT_CONNECTIONS,
    max_connections_per_tenant: int = DEFAULT_MAX_CONNECTIONS_PER_TENANT,
    latency_window_capacity: int = DEFAULT_LATENCY_WINDOW_SIZE,
    pacing_criteria: PacingAcceptanceCriteria | None = None,
    timeout: float = 20.0,
    isolation_mode: str = ISOLATION_MODE_PROCESS_ISOLATED,
    scheduling_mode: str = SCHEDULING_MODE_ALIGNED,
) -> PacedStageResult:
    """Run a synchronized, 20 ms-paced synthetic media stage at `concurrency` simultaneous calls."""
    if isolation_mode not in SUPPORTED_ISOLATION_MODES:
        raise ValueError(
            f"Unsupported isolation_mode {isolation_mode!r}; must be one of {SUPPORTED_ISOLATION_MODES}"
        )
    if scheduling_mode not in SUPPORTED_SCHEDULING_MODES:
        raise ValueError(
            f"Unsupported scheduling_mode {scheduling_mode!r}; must be one of {SUPPORTED_SCHEDULING_MODES}"
        )

    if pacing_criteria is not None and not isinstance(pacing_criteria, PacingAcceptanceCriteria):
        raise TypeError(
            f"pacing_criteria must be a PacingAcceptanceCriteria instance, got {type(pacing_criteria).__name__}"
        )
    effective_pacing_criteria = (
        pacing_criteria if pacing_criteria is not None else PacingAcceptanceCriteria()
    )

    if isinstance(max_concurrent_connections, bool) or not isinstance(
        max_concurrent_connections, int
    ):
        raise TypeError(
            f"max_concurrent_connections must be a positive integer, not bool or {type(max_concurrent_connections).__name__}"
        )
    if max_concurrent_connections <= 0:
        raise ValueError(
            f"max_concurrent_connections must be a positive integer, got {max_concurrent_connections!r}"
        )
    if isinstance(max_connections_per_tenant, bool) or not isinstance(
        max_connections_per_tenant, int
    ):
        raise TypeError(
            f"max_connections_per_tenant must be a positive integer, not bool or {type(max_connections_per_tenant).__name__}"
        )
    if max_connections_per_tenant <= 0:
        raise ValueError(
            f"max_connections_per_tenant must be a positive integer, got {max_connections_per_tenant!r}"
        )
    if max_connections_per_tenant > max_concurrent_connections:
        raise ValueError(
            f"max_connections_per_tenant ({max_connections_per_tenant}) cannot exceed max_concurrent_connections ({max_concurrent_connections})"
        )
    if concurrency > max_concurrent_connections:
        raise ValueError(
            f"Requested concurrency {concurrency} exceeds max capacity {max_concurrent_connections}"
        )

    frames_per_call = max(1, int(round((duration_seconds * 1000.0) / interval_ms)))
    tenant_dist = resolve_sustained_tenant_distribution(
        concurrency=concurrency,
        max_connections_per_tenant=max_connections_per_tenant,
        max_concurrent_connections=max_concurrent_connections,
    )

    was_tracing = tracemalloc.is_tracing()
    if not was_tracing:
        tracemalloc.start()
    tracemalloc.reset_peak()

    baseline_tasks = {
        t for t in asyncio.all_tasks() if t is not asyncio.current_task() and not t.done()
    }
    rss_before = measure_process_rss_kib()
    t_wall_start = time.perf_counter()
    t_cpu_start = time.process_time()

    failure_reasons: list[str] = []
    records: list[CallMediaRunRecord] = []
    unexpected_rejections = 0
    unexpected_disconnects = 0
    admitted_at_barrier = 0
    active_calls_at_barrier = 0
    active_voice_at_barrier = 0
    achieved_media_concurrency = 0

    final_active_connections = 0
    final_active_tenants = 0
    final_active_calls = 0
    final_active_voice_sessions = 0
    cleanup_failures = 0
    server_cpu_duration_s = 0.0
    server_rss_peak_kib = 0
    server_rss_after_kib = 0
    server_lag_dict: dict[str, Any] = {}
    gateway_telemetry: dict[str, Any] = {}
    recent_snaps: Sequence[Any] = ()

    server_cm = (
        start_isolated_media_server_process(
            max_concurrent_connections=max_concurrent_connections,
            max_connections_per_tenant=max_connections_per_tenant,
            queue_capacity=queue_capacity,
            backpressure_timeout=backpressure_timeout,
            max_retained_sessions=max(64, concurrency * 2),
            latency_window_capacity=latency_window_capacity,
        )
        if isolation_mode == ISOLATION_MODE_PROCESS_ISOLATED
        else start_ephemeral_media_server(
            max_concurrent_connections=max_concurrent_connections,
            max_connections_per_tenant=max_connections_per_tenant,
            queue_capacity=queue_capacity,
            backpressure_timeout=backpressure_timeout,
            max_retained_sessions=max(64, concurrency * 2),
            latency_window_capacity=latency_window_capacity,
        )
    )

    async with server_cm as server_ctx:
        ws_url = f"{server_ctx.ws_base_url}{SYNTHETIC_MEDIA_WS_ENDPOINT_PATH}"
        barrier = asyncio.Barrier(concurrency + 1)
        epoch_ready_event = asyncio.Event()
        shared_epoch_holder: list[float] = []
        lag_monitor = EventLoopLagMonitor(
            probe_interval_s=0.010,
            window_capacity=latency_window_capacity,
        )
        lag_monitor.start()

        # Issue per-call tokens across resolved tenant distribution
        call_specs: list[tuple[int, str, str, str, float]] = []
        slot_counter = 0
        for tenant_id, count in tenant_dist.items():
            for idx in range(count):
                call_id = f"paced_{stage_name}_{tenant_id}_{idx}"
                token = server_ctx.verifier.issue_token(
                    provider_id="m5_load_provider",
                    tenant_id=tenant_id,
                    call_id=call_id,
                    allowed_scopes=["voice.stream"],
                    ttl_seconds=max(60.0, duration_seconds + 30.0),
                )
                phase_ms = compute_session_phase_offset_ms(
                    session_slot_id=slot_counter,
                    concurrency=concurrency,
                    scheduling_mode=scheduling_mode,
                    interval_ms=interval_ms,
                )
                call_specs.append((slot_counter, tenant_id, call_id, token, phase_ms))
                slot_counter += 1

        async def _client_worker(slot_id: int, call_id: str, tok: str, phase_ms: float) -> None:
            nonlocal unexpected_rejections, unexpected_disconnects
            try:
                rec = await run_single_paced_media_call(
                    ws_url=ws_url,
                    token=tok,
                    call_id=call_id,
                    session_slot_id=slot_id,
                    frames_to_schedule=frames_per_call,
                    interval_ms=interval_ms,
                    seed=seed,
                    pattern=pattern,
                    skip_threshold_ms=skip_threshold_ms,
                    phase_offset_ms=phase_ms,
                    start_barrier=barrier,
                    shared_epoch_holder=shared_epoch_holder,
                    epoch_ready_event=epoch_ready_event,
                    timeout=timeout,
                    latency_window_capacity=latency_window_capacity,
                )
                records.append(rec)
            except websockets.exceptions.InvalidStatus as err:
                unexpected_rejections += 1
                with contextlib.suppress(Exception):
                    await barrier.abort()
                raise RuntimeError(f"Unexpected HTTP handshake rejection: {err}") from err
            except websockets.exceptions.ConnectionClosed as err:
                if err.rcvd is not None and err.rcvd.code == 1008:
                    unexpected_rejections += 1
                else:
                    unexpected_disconnects += 1
                with contextlib.suppress(Exception):
                    await barrier.abort()
                raise
            except Exception:
                unexpected_disconnects += 1
                with contextlib.suppress(Exception):
                    await barrier.abort()
                raise

        client_tasks = [
            asyncio.create_task(
                _client_worker(slot_id, call_id, tok, phase_ms),
                name=f"paced-client-{stage_name}-{slot_id}",
            )
            for slot_id, _, call_id, tok, phase_ms in call_specs
        ]

        try:
            await asyncio.wait_for(barrier.wait(), timeout=timeout)
            shared_epoch_holder.append(time.perf_counter())
            epoch_ready_event.set()
            if isinstance(server_ctx, RunningMediaServerContext):
                snap_mid = server_ctx.controller.snapshot()
                admitted_at_barrier = snap_mid.active_connections
                active_calls_at_barrier = len(server_ctx.auth_bridge._active_calls)
                active_voice_at_barrier = server_ctx.telemetry_sink.active_sessions
                achieved_media_concurrency = min(
                    admitted_at_barrier,
                    active_calls_at_barrier,
                    active_voice_at_barrier,
                )
            else:
                admitted_at_barrier = concurrency
                active_calls_at_barrier = concurrency
                active_voice_at_barrier = concurrency
                achieved_media_concurrency = concurrency

            if achieved_media_concurrency != concurrency:
                failure_reasons.append(
                    f"Achieved active media concurrency ({achieved_media_concurrency}) "
                    f"did not match requested concurrency ({concurrency})"
                )
        except (TimeoutError, asyncio.BrokenBarrierError) as err:
            failure_reasons.append(f"Concurrency start barrier failed: {type(err).__name__}")
            with contextlib.suppress(Exception):
                await barrier.abort()
            for t in client_tasks:
                if not t.done():
                    t.cancel()

        task_outcomes = await asyncio.gather(*client_tasks, return_exceptions=True)
        for outcome in task_outcomes:
            if isinstance(outcome, Exception):
                failure_reasons.append(f"Client session failed with {type(outcome).__name__}")

        lag_summary = await lag_monitor.stop()

        if isinstance(server_ctx, RunningMediaServerContext):
            await asyncio.sleep(0.02)
            snap_after = server_ctx.controller.snapshot()
            final_active_connections = snap_after.active_connections
            final_active_tenants = snap_after.active_tenants
            final_active_calls = len(server_ctx.auth_bridge._active_calls)
            final_active_voice_sessions = server_ctx.telemetry_sink.active_sessions
            sink_agg = server_ctx.telemetry_sink.snapshot()
            recent_snaps = sink_agg.recent_sessions
            server_lag_dict = lag_summary.to_dict()
            server_cpu_duration_s = 0.0
            server_rss_peak_kib = 0
            server_rss_after_kib = 0
            cleanup_failures = sum(
                1
                for s in sink_agg.recent_sessions
                if s.cleanup_state != VoiceTerminalCleanupState.COMPLETED_CLEAN
            )
            gateway_telemetry = {
                "total_sessions_started": sink_agg.total_sessions_started,
                "total_sessions_closed": sink_agg.total_sessions_closed,
                "total_sessions_completed_clean": sink_agg.total_sessions_completed_clean,
                "total_sessions_failed": sink_agg.total_sessions_failed,
                "peak_active_sessions": sink_agg.peak_active_sessions,
                "total_inbound_frames_received": sink_agg.total_inbound_frames_received,
                "total_inbound_frames_transferred": sink_agg.total_inbound_frames_transferred,
                "total_outbound_frames_received": sink_agg.total_outbound_frames_received,
                "total_outbound_frames_sent": sink_agg.total_outbound_frames_sent,
                "total_dropped_frames": sink_agg.total_dropped_frames,
                "max_inbound_queue_high_water_mark": sink_agg.max_inbound_queue_high_water_mark,
                "max_outbound_queue_high_water_mark": sink_agg.max_outbound_queue_high_water_mark,
                "error_counts": {k.value: v for k, v in sink_agg.error_counts.items()},
                "session_duration_ms": sink_agg.session_duration_ms.to_dict(),
                "cleanup_duration_ms": sink_agg.cleanup_duration_ms.to_dict(),
            }

    if isinstance(server_ctx, RunningIsolatedServerContext):
        iso_res = server_ctx.result
        if iso_res is not None:
            final_active_connections = iso_res.final_active_connections
            final_active_tenants = iso_res.final_active_tenants
            final_active_calls = iso_res.final_active_calls
            final_active_voice_sessions = iso_res.final_active_voice_sessions
            recent_snaps = [_reconstruct_session_snapshot(s) for s in iso_res.recent_sessions]
            server_lag_dict = iso_res.server_lag
            server_cpu_duration_s = iso_res.server_cpu_time_seconds
            server_rss_peak_kib = iso_res.server_rss_peak_kib
            server_rss_after_kib = iso_res.server_rss_after_kib
            cleanup_failures = sum(
                1
                for s in iso_res.recent_sessions
                if s.get("cleanup_state") != VoiceTerminalCleanupState.COMPLETED_CLEAN.value
            )
            telem_raw = iso_res.telemetry
            gateway_telemetry = {
                "total_sessions_started": int(telem_raw.get("total_sessions_started", 0)),
                "total_sessions_closed": int(telem_raw.get("total_sessions_closed", 0)),
                "total_sessions_completed_clean": int(
                    telem_raw.get("total_sessions_completed_clean", 0)
                ),
                "total_sessions_failed": int(telem_raw.get("total_sessions_failed", 0)),
                "peak_active_sessions": int(telem_raw.get("peak_active_sessions", 0)),
                "total_inbound_frames_received": int(
                    telem_raw.get("total_inbound_frames_received", 0)
                ),
                "total_inbound_frames_transferred": int(
                    telem_raw.get("total_inbound_frames_transferred", 0)
                ),
                "total_outbound_frames_received": int(
                    telem_raw.get("total_outbound_frames_received", 0)
                ),
                "total_outbound_frames_sent": int(telem_raw.get("total_outbound_frames_sent", 0)),
                "total_dropped_frames": int(telem_raw.get("total_dropped_frames", 0)),
                "max_inbound_queue_high_water_mark": int(
                    telem_raw.get("max_inbound_queue_high_water_mark", 0)
                ),
                "max_outbound_queue_high_water_mark": int(
                    telem_raw.get("max_outbound_queue_high_water_mark", 0)
                ),
                "error_counts": dict(telem_raw.get("error_counts", {})),
                "session_duration_ms": dict(telem_raw.get("session_duration_ms", {})),
                "cleanup_duration_ms": dict(telem_raw.get("cleanup_duration_ms", {})),
            }
            # Reconcile achieved_media_concurrency against independently observed server peak
            server_peak_sessions = int(telem_raw.get("peak_active_sessions", 0))
            achieved_media_concurrency = min(achieved_media_concurrency, server_peak_sessions)
        else:
            failure_reasons.append("Missing telemetry result from isolated server child process")

    t_wall_end = time.perf_counter()
    t_cpu_end = time.process_time()
    wall_duration_s = max(1e-6, t_wall_end - t_wall_start)
    cpu_duration_s = max(0.0, t_cpu_end - t_cpu_start)
    total_cpu_duration_s = (
        cpu_duration_s
        if isolation_mode == ISOLATION_MODE_IN_PROCESS
        else cpu_duration_s + server_cpu_duration_s
    )

    heap_current_bytes, heap_peak_bytes = tracemalloc.get_traced_memory()
    if not was_tracing:
        tracemalloc.stop()
    rss_after = measure_process_rss_kib()

    remaining_tasks = {
        t for t in asyncio.all_tasks() if t is not asyncio.current_task() and not t.done()
    }
    leaked_tasks = len(remaining_tasks - baseline_tasks)

    # Compute overlapping active media window across calls
    first_starts = [r.first_frame_sent_s for r in records if r.first_frame_sent_s is not None]
    last_ends = [r.last_frame_activity_s for r in records if r.last_frame_activity_s is not None]
    overlap_ms = 0.0
    if len(first_starts) == concurrency and len(last_ends) == concurrency:
        overlap_ms = max(0.0, (min(last_ends) - max(first_starts)) * 1000.0)

    # Reconcile client-side admission and verified active sessions
    client_admitted = sum(1 for r in records if r.admitted)
    admitted_concurrency = min(admitted_at_barrier, client_admitted)
    achieved_media_concurrency = min(achieved_media_concurrency, client_admitted)

    if achieved_media_concurrency != concurrency:
        failure_msg = (
            f"Achieved active media concurrency ({achieved_media_concurrency}) "
            f"did not match requested concurrency ({concurrency})"
        )
        if failure_msg not in failure_reasons:
            failure_reasons.append(failure_msg)
    if concurrency > 1 and overlap_ms <= 0.0:
        failure_reasons.append(
            f"No concurrent media streaming overlap observed across all {concurrency} sessions (overlap_ms={overlap_ms:.2f})"
        )

    # Aggregate client-side media correctness counters
    total_scheduled = sum(r.frames_scheduled for r in records)
    total_sent = sum(r.frames_sent for r in records)
    total_skipped = sum(r.frames_skipped for r in records)
    total_missed_deadlines = sum(r.missed_deadlines for r in records)
    total_echoed = sum(r.frames_echoed for r in records)
    total_missing = sum(r.frames_missing for r in records)
    total_duplicates = sum(r.duplicate_frames for r in records)
    total_out_of_order = sum(r.out_of_order_frames for r in records)
    total_mismatches = sum(r.payload_mismatches for r in records)
    total_cross_session = sum(r.cross_session_frames for r in records)
    unexpected_close_codes = sum(1 for r in records if r.close_code != 1000)
    completed_calls = sum(1 for r in records if r.completed_cleanly)

    # Frame conservation invariant: total_sent + total_skipped == total_scheduled
    if total_sent + total_skipped != total_scheduled:
        failure_reasons.append(
            f"Stage frame conservation violation: total_sent ({total_sent}) + "
            f"total_skipped ({total_skipped}) != total_scheduled ({total_scheduled})"
        )
    if skip_threshold_ms is None and total_skipped > 0:
        failure_reasons.append(
            f"Unexpected skipped frames ({total_skipped}) when skip_threshold_ms is None"
        )

    if completed_calls != concurrency:
        failure_reasons.append(
            f"Completed calls ({completed_calls}) != concurrency ({concurrency})"
        )
    if total_missing > 0:
        failure_reasons.append(f"Missing echoed frames detected: {total_missing}")
    if total_duplicates > 0:
        failure_reasons.append(f"Duplicate echoed frames detected: {total_duplicates}")
    if total_out_of_order > 0:
        failure_reasons.append(f"Out-of-order echoed frames detected: {total_out_of_order}")
    if total_mismatches > 0:
        failure_reasons.append(f"Payload byte mismatches detected: {total_mismatches}")
    if unexpected_close_codes > 0:
        failure_reasons.append(f"Unexpected WebSocket close codes: {unexpected_close_codes}")
    if cleanup_failures > 0:
        failure_reasons.append(f"Session cleanup failures detected: {cleanup_failures}")
    if (
        final_active_connections != 0
        or final_active_calls != 0
        or final_active_voice_sessions != 0
        or leaked_tasks != 0
    ):
        failure_reasons.append(
            f"Post-stage resource leak: connections={final_active_connections}, "
            f"calls={final_active_calls}, voice_sessions={final_active_voice_sessions}, "
            f"leaked_tasks={leaked_tasks}"
        )

    # Combine client timing samples across sessions (retaining 100% of stage samples)
    media_failure_reasons: list[str] = list(failure_reasons)
    all_lateness = [x for r in records for x in r.scheduling_lateness_samples_ms]
    all_intervals = [x for r in records for x in r.scheduling_interval_samples_ms]
    all_jitter = [x for r in records for x in r.scheduling_jitter_samples_ms]
    all_echo_rtts = [x for r in records for x in r.echo_rtt_samples_ms]
    all_handshakes = [r.handshake_ms for r in records]
    all_start_rtts = [r.call_start_rtt_ms for r in records]
    all_end_rtts = [r.call_end_rtt_ms for r in records]
    session_echo_p95s = [
        r.echo_rtt_summary.p95_ms
        for r in records
        if r.echo_rtt_summary.available and r.echo_rtt_summary.p95_ms is not None
    ]
    max_session_echo_p95 = max(session_echo_p95s) if session_echo_p95s else None

    nominal_transmission_s = (
        max(1e-6, (frames_per_call - 1) * (interval_ms / 1000.0))
        if frames_per_call > 1
        else (interval_ms / 1000.0)
    )
    session_tx_durations: list[float] = []
    for r in records:
        if r.first_frame_sent_s is not None:
            end_s = (
                r.last_frame_sent_s
                if getattr(r, "last_frame_sent_s", None) is not None
                else r.last_frame_activity_s
            )
            if end_s is not None:
                session_tx_durations.append(max(0.0, end_s - r.first_frame_sent_s))
    max_session_tx_duration_s = max(session_tx_durations) if session_tx_durations else None

    mean_interval = (sum(all_intervals) / len(all_intervals)) if all_intervals else None
    lateness_summary = _summarize_pooled_stage_samples(all_lateness)
    lateness_p95 = lateness_summary.get("p95_ms")

    pacing_status, pacing_failure_reasons, pacing_metrics = evaluate_pacing_metrics(
        total_scheduled=total_scheduled,
        total_sent=total_sent,
        total_missed_deadlines=total_missed_deadlines,
        target_interval_ms=interval_ms,
        mean_interval_ms=mean_interval,
        p95_scheduler_lateness_ms=lateness_p95,
        nominal_transmission_s=nominal_transmission_s,
        actual_transmission_s=max_session_tx_duration_s,
        criteria=effective_pacing_criteria,
        require_transmission_duration=True,
    )

    media_integrity_status = "PASS" if not media_failure_reasons else "FAIL"
    realtime_pacing_status = pacing_status
    stage_overall_status = (
        "PASS"
        if (media_integrity_status == "PASS" and realtime_pacing_status == "PASS")
        else "FAIL"
    )
    combined_failure_reasons = tuple(media_failure_reasons + pacing_failure_reasons)

    in_proc_dict = _summarize_session_metric_across_calls(
        recent_snaps,
        lambda s: s.inbound_processing_ms,
        window_capacity=latency_window_capacity,
    )
    out_proc_dict = _summarize_session_metric_across_calls(
        recent_snaps,
        lambda s: s.outbound_processing_ms,
        window_capacity=latency_window_capacity,
    )
    in_q_dict = _summarize_session_metric_across_calls(
        recent_snaps,
        lambda s: s.inbound_queue_wait_ms,
        window_capacity=latency_window_capacity,
    )
    out_q_dict = _summarize_session_metric_across_calls(
        recent_snaps,
        lambda s: s.outbound_queue_wait_ms,
        window_capacity=latency_window_capacity,
    )
    net_send_dict = _summarize_session_metric_across_calls(
        recent_snaps,
        lambda s: s.network_transport_ms,
        window_capacity=latency_window_capacity,
    )

    combined_session_p95s: list[float] = []
    combined_session_p50s: list[float] = []
    for snap in recent_snaps:
        if (
            snap.inbound_processing_ms.p95_ms is not None
            and snap.outbound_processing_ms.p95_ms is not None
            and snap.inbound_queue_wait_ms.p95_ms is not None
            and snap.outbound_queue_wait_ms.p95_ms is not None
        ):
            combined_session_p95s.append(
                snap.inbound_processing_ms.p95_ms
                + snap.outbound_processing_ms.p95_ms
                + snap.inbound_queue_wait_ms.p95_ms
                + snap.outbound_queue_wait_ms.p95_ms
            )
        if (
            snap.inbound_processing_ms.p50_ms is not None
            and snap.outbound_processing_ms.p50_ms is not None
            and snap.inbound_queue_wait_ms.p50_ms is not None
            and snap.outbound_queue_wait_ms.p50_ms is not None
        ):
            combined_session_p50s.append(
                snap.inbound_processing_ms.p50_ms
                + snap.outbound_processing_ms.p50_ms
                + snap.inbound_queue_wait_ms.p50_ms
                + snap.outbound_queue_wait_ms.p50_ms
            )

    logical_cpus = max(1, os.cpu_count() or 1)
    cpu_pct_single_core = (total_cpu_duration_s / wall_duration_s) * 100.0
    cpu_pct_all_cores = (total_cpu_duration_s / (wall_duration_s * float(logical_cpus))) * 100.0

    unavail_provider = LatencyDistributionSummary.unavailable(
        MetricAvailabilityReason.UNKNOWN_WITHOUT_PROVIDER_INSTRUMENTATION,
        window_capacity=latency_window_capacity,
    ).to_dict()
    unavail_playback = LatencyDistributionSummary.unavailable(
        MetricAvailabilityReason.UNKNOWN_WITHOUT_PLAYBACK_ACKNOWLEDGMENT,
        window_capacity=latency_window_capacity,
    ).to_dict()

    media_correctness = {
        "frames_scheduled": total_scheduled,
        "frames_sent": total_sent,
        "frames_skipped": total_skipped,
        "missed_deadlines": total_missed_deadlines,
        "missed_deadline_rate": pacing_metrics["missed_deadline_rate"],
        "frames_echoed": total_echoed,
        "frames_missing": total_missing,
        "duplicate_frames": total_duplicates,
        "out_of_order_frames": total_out_of_order,
        "payload_mismatches": total_mismatches,
        "cross_session_frames": total_cross_session,
        "unexpected_close_codes": unexpected_close_codes,
        "cleanup_failures": cleanup_failures,
        "frame_loss_rate": (
            round(float(total_missing) / float(total_sent), 6) if total_sent > 0 else 0.0
        ),
    }

    echo_rtt_dict = _summarize_pooled_stage_samples(
        all_echo_rtts,
        note=(
            "Exact pooled per-frame roundtrip time across all completed sessions in this stage; "
            "measures local loopback client-to-Gateway-to-FakeAI-to-client transfer on 127.0.0.1, "
            "NOT production voice latency or external provider network RTT."
        ),
    )
    echo_rtt_dict["max_session_p95_ms"] = max_session_echo_p95

    combined_p50_summary = _summarize_samples(combined_session_p50s, window_capacity=None)
    combined_p95_summary = _summarize_samples(combined_session_p95s, window_capacity=None)

    client_lag_samples = lag_summary.to_dict().get("raw_samples", [])
    client_lag_dict = (
        _summarize_pooled_stage_samples(client_lag_samples)
        if client_lag_samples
        else lag_summary.to_dict()
    )
    server_lag_samples = server_lag_dict.get("raw_samples", [])
    server_lag_summary_dict = (
        _summarize_pooled_stage_samples(server_lag_samples)
        if server_lag_samples
        else server_lag_dict
    )

    timing_ms = {
        "isolation_mode": isolation_mode,
        "scheduling_mode": scheduling_mode,
        "frame_scheduling_interval_ms": _summarize_pooled_stage_samples(all_intervals),
        "scheduler_lateness_ms": _summarize_pooled_stage_samples(all_lateness),
        "scheduler_jitter_ms": _summarize_pooled_stage_samples(all_jitter),
        "client_synthetic_echo_rtt_ms": echo_rtt_dict,
        "gateway_inbound_processing_ms": in_proc_dict,
        "gateway_outbound_processing_ms": out_proc_dict,
        "gateway_inbound_queue_wait_ms": in_q_dict,
        "gateway_outbound_queue_wait_ms": out_q_dict,
        "asgi_websocket_send_bytes_ms": net_send_dict,
        "gateway_added_overhead_estimate_ms": {
            "summary_method": "uncorrelated_component_percentile_sum_heuristic",
            "is_directly_measured_end_to_end_latency": False,
            "nfr_perf_004_qualified": False,
            "median_session_component_p50_sum_ms": (
                round(combined_p50_summary.p50_ms, 4)
                if combined_p50_summary.p50_ms is not None
                else None
            ),
            "p95_session_component_p95_sum_ms": (
                round(combined_p95_summary.p95_ms, 4)
                if combined_p95_summary.p95_ms is not None
                else None
            ),
            "max_session_component_p95_sum_ms": (
                round(max(combined_session_p95s), 4) if combined_session_p95s else None
            ),
            "nfr_perf_004_target_p95_ms": 50.0,
            "omitted_intervals": [
                "WebSocketTelephonyMediaAdapter._inbound_queue residence time",
                "producer queue.put backpressure wait (t_enqueued - t_pre_enqueue)",
                "BoundedInterruptibleFakeAIEchoPort._output_queue residence time",
                "local ASGI websocket.send_bytes await duration (network_transport_ms)",
                "unmeasured external telephony, provider network, and remote playback intervals",
            ],
            "omitted_gateway_owned_intervals": [
                "WebSocketTelephonyMediaAdapter._inbound_queue residence time before bridge pops it",
                "producer queue.put backpressure wait (t_enqueued - t_pre_enqueue) under queue saturation",
                "BoundedInterruptibleFakeAIEchoPort._output_queue residence time",
                "local ASGI websocket.send_bytes await duration (tracked separately as network_transport_ms)",
                "ASGI server framework demuxing, routing, and task-switch latency",
            ],
            "omitted_external_intervals": [
                "telephony carrier / PSTN / SIP network propagation wire delay",
                "client OS TCP network stack and kernel socket buffer queueing",
                "upstream AI provider network round-trip, model inference, and audio synthesis",
                "remote endpoint jitter buffering, audio hardware decoding, and physical acoustic playback",
            ],
            "note": (
                "Descriptive heuristic sum of marginal component percentiles across in_proc + out_proc + "
                "in_q_wait + out_q_wait. This sum of marginal percentiles is NOT the true p95 of the "
                "conjoint Gateway transit distribution, is NOT a statistically guaranteed upper bound "
                "(which is mathematically false when tail events are disjoint), and omits both Gateway-owned "
                "and external latency intervals. It cannot by itself certify compliance with NFR-PERF-004."
            ),
        },
        "client_dispatch_skew_ms": _summarize_samples(
            [r.dispatch_skew_ms for r in records], window_capacity=None
        ).to_dict(),
        "event_loop_lag_ms": client_lag_dict,
        "client_event_loop_lag_ms": client_lag_dict,
        "server_event_loop_lag_ms": server_lag_summary_dict,
        "media_transmission_duration_s": (
            round(max_session_tx_duration_s, 4) if max_session_tx_duration_s is not None else None
        ),
        "control_handshake_ms": _summarize_pooled_stage_samples(all_handshakes),
        "control_call_start_rtt_ms": _summarize_pooled_stage_samples(all_start_rtts),
        "control_call_end_rtt_ms": _summarize_pooled_stage_samples(all_end_rtts),
        "upstream_ai_generation_ms": unavail_provider,
        "external_telephony_network_ms": unavail_provider,
        "remote_playback_latency_ms": unavail_playback,
        "pacing_evaluation": {
            "status": realtime_pacing_status,
            "criteria": effective_pacing_criteria.to_dict(),
            "metrics": pacing_metrics,
            "failure_reasons": list(pacing_failure_reasons),
        },
    }

    resources = {
        "isolation_mode": isolation_mode,
        "scheduling_mode": scheduling_mode,
        "process_cpu_time_seconds": round(total_cpu_duration_s, 4),
        "client_process_cpu_time_seconds": round(cpu_duration_s, 4),
        "server_process_cpu_time_seconds": round(server_cpu_duration_s, 4),
        "wall_clock_duration_seconds": round(wall_duration_s, 4),
        "logical_cpu_count": logical_cpus,
        "cpu_utilization_pct_single_core": round(cpu_pct_single_core, 2),
        "cpu_utilization_pct_all_cores": round(cpu_pct_all_cores, 2),
        "cpu_normalization_note": (
            "cpu_utilization_pct_single_core = (process_cpu_time_seconds / wall_clock_duration_seconds) * 100 "
            "(100% = 1 logical core); cpu_utilization_pct_all_cores divides by logical_cpu_count."
        ),
        "python_heap_current_kib": round(float(heap_current_bytes) / 1024.0, 2),
        "python_heap_peak_kib": round(float(heap_peak_bytes) / 1024.0, 2),
        "python_heap_note": "Measured via stdlib tracemalloc in client runner process.",
        "process_rss_available": rss_after.available,
        "process_rss_before_kib": rss_before.current_rss_kib,
        "process_rss_after_kib": rss_after.current_rss_kib,
        "process_rss_peak_kib": (
            rss_after.peak_rss_kib
            if isolation_mode == ISOLATION_MODE_IN_PROCESS
            else max(rss_after.peak_rss_kib or 0, server_rss_peak_kib)
        ),
        "process_rss_source": rss_after.source,
        "client_rss_peak_kib": rss_after.peak_rss_kib,
        "server_rss_peak_kib": server_rss_peak_kib,
        "server_rss_after_kib": server_rss_after_kib,
    }

    cleanup_verification = {
        "final_active_connections": final_active_connections,
        "final_active_tenants": final_active_tenants,
        "final_active_calls": final_active_calls,
        "final_active_voice_sessions": final_active_voice_sessions,
        "leaked_asyncio_tasks": leaked_tasks,
    }

    return PacedStageResult(
        stage_name=stage_name,
        status=stage_overall_status,
        requested_concurrency=concurrency,
        admitted_concurrency=admitted_concurrency,
        achieved_media_concurrency=achieved_media_concurrency,
        completed_calls=completed_calls,
        expected_rejections=0,
        unexpected_rejections=unexpected_rejections,
        unexpected_disconnects=unexpected_disconnects,
        configured_duration_seconds=duration_seconds,
        wall_clock_duration_seconds=wall_duration_s,
        concurrent_media_overlap_ms=overlap_ms,
        media_correctness=media_correctness,
        timing_ms=timing_ms,
        gateway_telemetry=gateway_telemetry,
        resources=resources,
        cleanup_verification=cleanup_verification,
        failure_reasons=combined_failure_reasons,
        media_integrity_status=media_integrity_status,
        realtime_pacing_status=realtime_pacing_status,
        media_failure_reasons=tuple(media_failure_reasons),
        pacing_failure_reasons=tuple(pacing_failure_reasons),
        pacing_acceptance_criteria=effective_pacing_criteria.to_dict(),
    )


# ==============================================================================
# GATE F: Stability, Overload, Draining, Isolation & Recovery Suite
# ==============================================================================


@dataclass(frozen=True, slots=True)
class FaultScenarioResult:
    """Result of a narrowly scoped GATE F stability, overload, or recovery scenario."""

    scenario: str
    status: str  # "PASS" | "FAIL"
    expected_rejections: int
    unexpected_failures: int
    frames_sent: int
    frames_echoed: int
    final_active_connections: int
    final_active_calls: int
    final_active_voice_sessions: int
    leaked_tasks: int
    details: Mapping[str, Any]
    error: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "scenario": self.scenario,
            "status": self.status,
            "expected_rejections": self.expected_rejections,
            "unexpected_failures": self.unexpected_failures,
            "frames_sent": self.frames_sent,
            "frames_echoed": self.frames_echoed,
            "final_active_connections": self.final_active_connections,
            "final_active_calls": self.final_active_calls,
            "final_active_voice_sessions": self.final_active_voice_sessions,
            "leaked_tasks": self.leaked_tasks,
            "details": dict(self.details),
            "error": self.error,
        }


async def run_fault_and_overload_suite(*, seed: int = 42) -> list[FaultScenarioResult]:
    """Execute the 7 required GATE F stability, overload, draining, and recovery evaluations."""
    results: list[FaultScenarioResult] = []
    baseline_tasks = {
        t for t in asyncio.all_tasks() if t is not asyncio.current_task() and not t.done()
    }

    def _count_leaked_tasks() -> int:
        current = {
            t for t in asyncio.all_tasks() if t is not asyncio.current_task() and not t.done()
        }
        return len(current - baseline_tasks)

    # -------------------------------------------------------------------------
    # 1. Premature client disconnect during active audio
    # -------------------------------------------------------------------------
    async with start_ephemeral_media_server() as ctx:
        ws_url = f"{ctx.ws_base_url}{SYNTHETIC_MEDIA_WS_ENDPOINT_PATH}"
        tok = ctx.verifier.issue_token(
            provider_id="m5_fault",
            tenant_id="tenant_fault_disc",
            call_id="call_premature_disc",
            allowed_scopes=["voice.stream"],
        )
        sent_before_drop = 0
        echoed_before_drop = 0
        async with websockets.connect(
            ws_url, additional_headers={"Authorization": f"Bearer {tok}"}
        ) as ws:
            await ws.recv()  # session.ready
            await ws.send(json.dumps({"event": EVENT_CALL_START, "call_id": "call_premature_disc"}))
            await ws.recv()  # call.started
            for seq in range(5):
                payload = build_deterministic_frame_payload(
                    seed=seed, session_slot_id=0, frame_seq=seq
                )
                await ws.send(payload)
                sent_before_drop += 1
            msg = await asyncio.wait_for(ws.recv(), timeout=2.0)
            if isinstance(msg, bytes):
                echoed_before_drop += 1
            # Abrupt close without sending call.end
            await ws.close(code=1000, reason="Premature client hangup")

        await asyncio.sleep(0.05)
        snap = ctx.controller.snapshot()
        sink_snap = ctx.telemetry_sink.snapshot()
        active_calls = len(ctx.auth_bridge._active_calls)
        active_voice = ctx.telemetry_sink.active_sessions

    leaked = _count_leaked_tasks()
    ok = (
        snap.active_connections == 0
        and active_calls == 0
        and active_voice == 0
        and sink_snap.total_sessions_closed == 1
        and leaked == 0
    )
    results.append(
        FaultScenarioResult(
            scenario="premature_client_disconnect",
            status="PASS" if ok else "FAIL",
            expected_rejections=0,
            unexpected_failures=0 if ok else 1,
            frames_sent=sent_before_drop,
            frames_echoed=echoed_before_drop,
            final_active_connections=snap.active_connections,
            final_active_calls=active_calls,
            final_active_voice_sessions=active_voice,
            leaked_tasks=leaked,
            details={
                "gateway_inbound_frames_received": sink_snap.total_inbound_frames_received,
                "gateway_outbound_frames_sent": sink_snap.total_outbound_frames_sent,
                "gateway_dropped_frames": sink_snap.total_dropped_frames,
            },
        )
    )

    # -------------------------------------------------------------------------
    # 2. Blocked AI consumer & inbound backpressure overflow (R1-D / F-4)
    # -------------------------------------------------------------------------
    class _BlockedDownstreamAIPort(BoundedInterruptibleFakeAIEchoPort):
        def __init__(self) -> None:
            super().__init__(queue_capacity=2, backpressure_timeout=0.04)
            self._hold = asyncio.Event()

        async def receive_frame(self) -> AudioFrame | None:
            await self._hold.wait()
            return await super().receive_frame()

        async def close(self) -> None:
            self._hold.set()
            await super().close()

    async with start_ephemeral_media_server(
        ai_port_factory=_BlockedDownstreamAIPort,
    ) as ctx:
        ws_url = f"{ctx.ws_base_url}{SYNTHETIC_MEDIA_WS_ENDPOINT_PATH}"
        tok = ctx.verifier.issue_token(
            provider_id="m5_fault",
            tenant_id="tenant_fault_bp",
            call_id="call_slow_bp",
            allowed_scopes=["voice.stream"],
        )
        bp_sent = 0
        observed_close_code = 1000
        async with websockets.connect(
            ws_url, additional_headers={"Authorization": f"Bearer {tok}"}
        ) as ws:
            await ws.recv()  # session.ready
            await ws.send(json.dumps({"event": EVENT_CALL_START, "call_id": "call_slow_bp"}))
            await ws.recv()  # call.started
            try:
                for seq in range(16):
                    payload = build_deterministic_frame_payload(
                        seed=seed, session_slot_id=1, frame_seq=seq
                    )
                    await ws.send(payload)
                    bp_sent += 1
                    await asyncio.sleep(0.005)
                await asyncio.wait_for(ws.wait_closed(), timeout=2.0)
            except websockets.exceptions.ConnectionClosed as exc:
                if exc.rcvd is not None:
                    observed_close_code = exc.rcvd.code
            if ws.close_code is not None:
                observed_close_code = ws.close_code

        await asyncio.sleep(0.05)
        snap = ctx.controller.snapshot()
        sink_snap = ctx.telemetry_sink.snapshot()
        active_calls = len(ctx.auth_bridge._active_calls)
        active_voice = ctx.telemetry_sink.active_sessions

    leaked = _count_leaked_tasks()
    bp_errors = sink_snap.error_counts.get(VoiceErrorCategory.BACKPRESSURE_OVERFLOW, 0)
    ok = (
        observed_close_code == 1011
        and bp_errors >= 1
        and snap.active_connections == 0
        and active_calls == 0
        and active_voice == 0
        and leaked == 0
    )
    results.append(
        FaultScenarioResult(
            scenario="blocked_ai_consumer_inbound_backpressure",
            status="PASS" if ok else "FAIL",
            expected_rejections=0,
            unexpected_failures=0 if ok else 1,
            frames_sent=bp_sent,
            frames_echoed=0,
            final_active_connections=snap.active_connections,
            final_active_calls=active_calls,
            final_active_voice_sessions=active_voice,
            leaked_tasks=leaked,
            details={
                "observed_close_code": observed_close_code,
                "backpressure_overflow_errors": bp_errors,
                "max_inbound_queue_hwm": sink_snap.max_inbound_queue_high_water_mark,
                "unforwarded_inbound_frames": sink_snap.total_unforwarded_inbound_frames,
            },
        )
    )

    # -------------------------------------------------------------------------
    # 3. Blocked outbound telephony send backpressure (R1-D / F-4)
    # -------------------------------------------------------------------------
    outbound_block_event = asyncio.Event()

    class _OutboundOverflowingAIPort(BoundedInterruptibleFakeAIEchoPort):
        """AI port that enqueues 8 frames to saturate the outbound queue, then triggers backpressure overflow."""

        def __init__(self) -> None:
            super().__init__(queue_capacity=8, backpressure_timeout=0.05)
            self._emitted = 0

        async def send_frame(self, frame: AudioFrame) -> None:
            pass

        async def receive_frame(self) -> AudioFrame | None:
            if self._emitted <= 8:
                seq = self._emitted
                self._emitted += 1
                return AudioFrame(
                    payload=build_deterministic_frame_payload(
                        seed=seed, session_slot_id=2, frame_seq=seq
                    ),
                    format=AudioFormat.PCM16LE,
                    sample_rate=PCM16LE_SAMPLE_RATE,
                    channels=PCM16LE_CHANNELS,
                    sequence=seq,
                    timestamp_ms=seq * 20.0,
                    turn_id=1,
                )
            await asyncio.sleep(0.04)
            raise MediaBackpressureOverflowError(
                "Outbound queue capacity 8 exceeded and backpressure timed out after 0.04s"
            )

    async with start_ephemeral_media_server(
        queue_capacity=8,
        backpressure_timeout=0.05,
        ai_port_factory=_OutboundOverflowingAIPort,
        block_outbound_binary_send_event=outbound_block_event,
    ) as ctx:
        ws_url = f"{ctx.ws_base_url}{SYNTHETIC_MEDIA_WS_ENDPOINT_PATH}"
        tok = ctx.verifier.issue_token(
            provider_id="m5_fault",
            tenant_id="tenant_fault_out_bp",
            call_id="call_outbound_bp",
            allowed_scopes=["voice.stream"],
        )
        observed_out_close_code = 1000
        async with websockets.connect(
            ws_url, additional_headers={"Authorization": f"Bearer {tok}"}
        ) as ws:
            await ws.recv()  # session.ready
            await ws.send(json.dumps({"event": EVENT_CALL_START, "call_id": "call_outbound_bp"}))
            await ws.recv()  # call.started
            try:
                # Wait for the backpressure timeout to fire and close connection
                await asyncio.wait_for(ws.wait_closed(), timeout=3.0)
            except websockets.exceptions.ConnectionClosed as exc:
                if exc.rcvd is not None:
                    observed_out_close_code = exc.rcvd.code
            if ws.close_code is not None:
                observed_out_close_code = ws.close_code
            outbound_block_event.set()

        await asyncio.sleep(0.05)
        snap = ctx.controller.snapshot()
        sink_snap = ctx.telemetry_sink.snapshot()
        active_calls = len(ctx.auth_bridge._active_calls)
        active_voice = ctx.telemetry_sink.active_sessions

    outbound_block_event.set()
    leaked = _count_leaked_tasks()
    bp_errors_out = sink_snap.error_counts.get(VoiceErrorCategory.BACKPRESSURE_OVERFLOW, 0)
    ok_out = (
        observed_out_close_code == 1011
        and bp_errors_out >= 1
        and sink_snap.max_outbound_queue_high_water_mark == 8
        and snap.active_connections == 0
        and active_calls == 0
        and active_voice == 0
        and leaked == 0
    )
    results.append(
        FaultScenarioResult(
            scenario="blocked_outbound_send_backpressure",
            status="PASS" if ok_out else "FAIL",
            expected_rejections=0,
            unexpected_failures=0 if ok_out else 1,
            frames_sent=0,
            frames_echoed=0,
            final_active_connections=snap.active_connections,
            final_active_calls=active_calls,
            final_active_voice_sessions=active_voice,
            leaked_tasks=leaked,
            details={
                "observed_close_code": observed_out_close_code,
                "backpressure_overflow_errors": bp_errors_out,
                "max_outbound_queue_hwm": sink_snap.max_outbound_queue_high_water_mark,
                "unforwarded_outbound_frames": sink_snap.total_unforwarded_outbound_frames,
                "backpressure_evidence_type": "fault_injection_exception_handling_verification",
                "is_production_queue_timeout": False,
                "production_backpressure_source": "synthetic_ai_port_injected_overflow",
                "causal_mechanism": (
                    "Synthetic AI port enqueued 9 frames while ASGI send blocked, successfully "
                    "saturating VoiceBridgeSession._outbound_queue to high-water mark 8 (1 frame in flight "
                    "+ 8 buffered). The synthetic AI port then injected MediaBackpressureOverflowError "
                    "to verify Gateway error classification (VoiceErrorCategory.BACKPRESSURE_OVERFLOW), "
                    "WebSocket 1011 closure, and idempotent cleanup of the 9 unforwarded frames."
                ),
                "production_architecture_note": (
                    "In current Gateway production code (media_endpoint.py:276), VoiceBridgeSession is instantiated "
                    "without backpressure_timeout (defaulting to None). Therefore, outbound queue overflow in "
                    "media_endpoint.py blocks indefinitely rather than timing out. True VoiceBridgeSession "
                    "backpressure timeout is verified via direct unit tests with explicit backpressure_timeout."
                ),
                "limitation": (
                    "Controlled ASGI-send blocking tests VoiceBridgeSession outbound queue saturation and ASGI await "
                    "handling; it does not simulate physical OS kernel TCP socket buffer fullness."
                ),
            },
        )
    )

    # -------------------------------------------------------------------------
    # 3. Normal call.end during active audio (lossless in-flight drain)
    # -------------------------------------------------------------------------
    async with start_ephemeral_media_server() as ctx:
        ws_url = f"{ctx.ws_base_url}{SYNTHETIC_MEDIA_WS_ENDPOINT_PATH}"
        tok = ctx.verifier.issue_token(
            provider_id="m5_fault",
            tenant_id="tenant_drain",
            call_id="call_mid_stream_end",
            allowed_scopes=["voice.stream"],
        )
        rec = await run_single_paced_media_call(
            ws_url=ws_url,
            token=tok,
            call_id="call_mid_stream_end",
            session_slot_id=2,
            frames_to_schedule=12,
            interval_ms=20.0,
            seed=seed,
        )
        await asyncio.sleep(0.03)
        snap = ctx.controller.snapshot()
        active_calls = len(ctx.auth_bridge._active_calls)
        active_voice = ctx.telemetry_sink.active_sessions

    leaked = _count_leaked_tasks()
    ok = (
        rec.completed_cleanly
        and rec.frames_sent == 12
        and rec.frames_echoed == 12
        and rec.frames_missing == 0
        and snap.active_connections == 0
        and active_calls == 0
        and leaked == 0
    )
    results.append(
        FaultScenarioResult(
            scenario="normal_call_end_during_active_audio",
            status="PASS" if ok else "FAIL",
            expected_rejections=0,
            unexpected_failures=0 if ok else 1,
            frames_sent=rec.frames_sent,
            frames_echoed=rec.frames_echoed,
            final_active_connections=snap.active_connections,
            final_active_calls=active_calls,
            final_active_voice_sessions=active_voice,
            leaked_tasks=leaked,
            details={
                "close_code": rec.close_code,
                "call_end_rtt_ms": round(rec.call_end_rtt_ms, 3),
            },
        )
    )

    # -------------------------------------------------------------------------
    # 4. Server admission capacity saturation (per-tenant & global ceilings)
    # -------------------------------------------------------------------------
    ai_alloc_count = 0

    def _counting_ai_factory() -> BoundedInterruptibleFakeAIEchoPort:
        nonlocal ai_alloc_count
        ai_alloc_count += 1
        return BoundedInterruptibleFakeAIEchoPort()

    async with start_ephemeral_media_server(
        max_concurrent_connections=3,
        max_connections_per_tenant=2,
        ai_port_factory=_counting_ai_factory,
    ) as ctx:
        ws_url = f"{ctx.ws_base_url}{SYNTHETIC_MEDIA_WS_ENDPOINT_PATH}"
        hold_barrier = asyncio.Barrier(4)  # 3 holders + 1 coordinator
        release_holders = asyncio.Event()
        holder_sent = 0
        holder_echoed = 0

        async def _holder(tenant_id: str, call_id: str, slot_id: int) -> None:
            nonlocal holder_sent, holder_echoed
            tok = ctx.verifier.issue_token(
                provider_id="m5_sat",
                tenant_id=tenant_id,
                call_id=call_id,
                allowed_scopes=["voice.stream"],
            )
            async with websockets.connect(
                ws_url, additional_headers={"Authorization": f"Bearer {tok}"}
            ) as ws:
                await ws.recv()
                await ws.send(json.dumps({"event": EVENT_CALL_START, "call_id": call_id}))
                await ws.recv()
                await hold_barrier.wait()
                payload = build_deterministic_frame_payload(
                    seed=seed, session_slot_id=slot_id, frame_seq=0
                )
                await ws.send(payload)
                holder_sent += 1
                echo = await asyncio.wait_for(ws.recv(), timeout=2.0)
                if isinstance(echo, bytes) and echo == payload:
                    holder_echoed += 1
                await release_holders.wait()
                await ws.send(json.dumps({"event": EVENT_CALL_END, "call_id": call_id}))
                await ws.recv()
                await ws.wait_closed()

        h_tasks = [
            asyncio.create_task(_holder("tenant_sat_a", "call_sat_a1", 10)),
            asyncio.create_task(_holder("tenant_sat_a", "call_sat_a2", 11)),
            asyncio.create_task(_holder("tenant_sat_b", "call_sat_b1", 12)),
        ]
        await asyncio.wait_for(hold_barrier.wait(), timeout=5.0)
        alloc_before_rejections = ai_alloc_count
        expected_rej = 0

        # Attempt 1: Per-tenant overflow (tenant_sat_a is at 2/2, although global is also 3/3)
        # Attempt 2: Global overflow on tenant_sat_c (0/2 tenant, 3/3 global)
        for rej_tenant, rej_call in [
            ("tenant_sat_a", "call_sat_a_overflow"),
            ("tenant_sat_c", "call_sat_c_overflow"),
        ]:
            rej_tok = ctx.verifier.issue_token(
                provider_id="m5_sat",
                tenant_id=rej_tenant,
                call_id=rej_call,
                allowed_scopes=["voice.stream"],
            )
            try:
                async with websockets.connect(
                    ws_url, additional_headers={"Authorization": f"Bearer {rej_tok}"}
                ) as ws_rej:
                    await ws_rej.recv()
            except websockets.exceptions.InvalidStatus:
                expected_rej += 1
            except websockets.exceptions.ConnectionClosed as exc:
                if exc.rcvd is not None and exc.rcvd.code == 1008:
                    expected_rej += 1

        alloc_after_rejections = ai_alloc_count
        release_holders.set()
        await asyncio.gather(*h_tasks)
        await asyncio.sleep(0.03)

        snap = ctx.controller.snapshot()
        active_calls = len(ctx.auth_bridge._active_calls)
        active_voice = ctx.telemetry_sink.active_sessions

    leaked = _count_leaked_tasks()
    ai_allocated_for_rejected = alloc_after_rejections - alloc_before_rejections
    ok = (
        expected_rej == 2
        and ai_allocated_for_rejected == 0
        and holder_sent == 3
        and holder_echoed == 3
        and snap.active_connections == 0
        and active_calls == 0
        and leaked == 0
    )
    results.append(
        FaultScenarioResult(
            scenario="admission_capacity_saturation",
            status="PASS" if ok else "FAIL",
            expected_rejections=expected_rej,
            unexpected_failures=0 if ok else 1,
            frames_sent=holder_sent,
            frames_echoed=holder_echoed,
            final_active_connections=snap.active_connections,
            final_active_calls=active_calls,
            final_active_voice_sessions=active_voice,
            leaked_tasks=leaked,
            details={
                "ai_ports_allocated_for_admitted": alloc_before_rejections,
                "ai_ports_allocated_for_rejected": ai_allocated_for_rejected,
            },
        )
    )

    # -------------------------------------------------------------------------
    # 5. Server draining with active media calls
    # -------------------------------------------------------------------------
    async with start_ephemeral_media_server() as ctx:
        ws_url = f"{ctx.ws_base_url}{SYNTHETIC_MEDIA_WS_ENDPOINT_PATH}"
        drain_ready_barrier = asyncio.Barrier(3)  # 2 active calls + 1 coordinator
        continue_after_drain_start = asyncio.Event()
        drain_sent = 0
        drain_echoed = 0

        async def _active_call_during_drain(call_id: str, slot_id: int) -> None:
            nonlocal drain_sent, drain_echoed
            tok = ctx.verifier.issue_token(
                provider_id="m5_drain",
                tenant_id="tenant_draining",
                call_id=call_id,
                allowed_scopes=["voice.stream"],
            )
            async with websockets.connect(
                ws_url, additional_headers={"Authorization": f"Bearer {tok}"}
            ) as ws:
                await ws.recv()
                await ws.send(json.dumps({"event": EVENT_CALL_START, "call_id": call_id}))
                await ws.recv()
                await drain_ready_barrier.wait()
                await continue_after_drain_start.wait()
                for seq in range(4):
                    payload = build_deterministic_frame_payload(
                        seed=seed, session_slot_id=slot_id, frame_seq=seq
                    )
                    await ws.send(payload)
                    drain_sent += 1
                    echo = await asyncio.wait_for(ws.recv(), timeout=2.0)
                    if isinstance(echo, bytes) and echo == payload:
                        drain_echoed += 1
                    await asyncio.sleep(0.02)
                await ws.send(json.dumps({"event": EVENT_CALL_END, "call_id": call_id}))
                await ws.recv()
                await ws.wait_closed()

        d_tasks = [
            asyncio.create_task(_active_call_during_drain("call_drain_1", 20)),
            asyncio.create_task(_active_call_during_drain("call_drain_2", 21)),
        ]
        await asyncio.wait_for(drain_ready_barrier.wait(), timeout=5.0)

        # Initiate draining while 2 media calls are active
        drain_task = asyncio.create_task(
            ctx.coordinator.drain(DrainPolicy(grace_period=5.0, force_close_timeout=2.0))
        )
        await asyncio.sleep(0.02)

        # Attempt new connection while draining -> must be rejected with 1008
        drain_rejections = 0
        new_tok = ctx.verifier.issue_token(
            provider_id="m5_drain",
            tenant_id="tenant_draining",
            call_id="call_rejected_while_draining",
            allowed_scopes=["voice.stream"],
        )
        try:
            async with websockets.connect(
                ws_url, additional_headers={"Authorization": f"Bearer {new_tok}"}
            ) as ws_new:
                await ws_new.recv()
        except websockets.exceptions.InvalidStatus:
            drain_rejections += 1
        except websockets.exceptions.ConnectionClosed as exc:
            if exc.rcvd is not None and exc.rcvd.code == 1008:
                drain_rejections += 1

        continue_after_drain_start.set()
        await asyncio.gather(*d_tasks)
        drain_outcome = await asyncio.wait_for(drain_task, timeout=5.0)
        await asyncio.sleep(0.02)

        snap = ctx.controller.snapshot()
        active_calls = len(ctx.auth_bridge._active_calls)
        active_voice = ctx.telemetry_sink.active_sessions

    leaked = _count_leaked_tasks()
    drained_cleanly = (
        drain_outcome.unresolved_connections == 0
        and drain_outcome.active_leases_remaining == 0
        and not drain_outcome.deadline_exceeded
        and drain_outcome.graceful_completions == 2
    )
    ok = (
        drain_rejections == 1
        and drained_cleanly
        and drain_sent == 8
        and drain_echoed == 8
        and snap.active_connections == 0
        and active_calls == 0
        and leaked == 0
    )
    results.append(
        FaultScenarioResult(
            scenario="draining_with_active_media_calls",
            status="PASS" if ok else "FAIL",
            expected_rejections=drain_rejections,
            unexpected_failures=0 if ok else 1,
            frames_sent=drain_sent,
            frames_echoed=drain_echoed,
            final_active_connections=snap.active_connections,
            final_active_calls=active_calls,
            final_active_voice_sessions=active_voice,
            leaked_tasks=leaked,
            details={
                "drained_cleanly": drained_cleanly,
                "graceful_completions": drain_outcome.graceful_completions,
                "unresolved_connections": drain_outcome.unresolved_connections,
                "active_leases_remaining": drain_outcome.active_leases_remaining,
                "final_state": drain_outcome.final_state.value,
            },
        )
    )

    # -------------------------------------------------------------------------
    # 7. Recovery after an interrupted call and a failed call (R1-E / F-7)
    # -------------------------------------------------------------------------
    async with start_ephemeral_media_server() as ctx:
        ws_url = f"{ctx.ws_base_url}{SYNTHETIC_MEDIA_WS_ENDPOINT_PATH}"

        # 7a. Interrupted call (Turn 1 -> call.interrupt -> Turn 2 valid frame -> call.end)
        tok_intr = ctx.verifier.issue_token(
            provider_id="m5_rec",
            tenant_id="tenant_recovery",
            call_id="call_reusable_id",
            allowed_scopes=["voice.stream"],
        )
        turn2_echoed = False
        async with websockets.connect(
            ws_url, additional_headers={"Authorization": f"Bearer {tok_intr}"}
        ) as ws:
            await ws.recv()  # session.ready
            await ws.send(json.dumps({"event": EVENT_CALL_START, "call_id": "call_reusable_id"}))
            await ws.recv()  # call.started
            # Send Turn 1 frame
            await ws.send(
                build_deterministic_frame_payload(seed=seed, session_slot_id=30, frame_seq=0)
            )
            await ws.send(
                json.dumps(
                    {
                        "event": EVENT_CALL_INTERRUPT,
                        "call_id": "call_reusable_id",
                        "turn_id": 1,
                        "request_id": "req-rec-1",
                    }
                )
            )
            while True:
                msg = await asyncio.wait_for(ws.recv(), timeout=2.0)
                if isinstance(msg, str) and json.loads(msg).get("event") == EVENT_CALL_INTERRUPTED:
                    break
            # R1-E: Send Turn 2 valid audio frame after call.interrupted and verify valid echo
            t2_payload = build_deterministic_frame_payload(
                seed=seed, session_slot_id=30, frame_seq=1
            )
            await ws.send(t2_payload)
            while True:
                msg = await asyncio.wait_for(ws.recv(), timeout=2.0)
                if isinstance(msg, bytes):
                    if msg == t2_payload:
                        turn2_echoed = True
                        break
            await ws.send(json.dumps({"event": EVENT_CALL_END, "call_id": "call_reusable_id"}))
            while True:
                msg = await asyncio.wait_for(ws.recv(), timeout=2.0)
                if isinstance(msg, str) and json.loads(msg).get("event") == EVENT_CALL_ENDED:
                    break
            await ws.wait_closed()

        # 7b. Malformed frame call (100-byte invalid binary frame -> 1003 close)
        tok_bad = ctx.verifier.issue_token(
            provider_id="m5_rec",
            tenant_id="tenant_recovery",
            call_id="call_reusable_id",
            allowed_scopes=["voice.stream"],
        )
        bad_close_code: int | None = None
        async with websockets.connect(
            ws_url, additional_headers={"Authorization": f"Bearer {tok_bad}"}
        ) as ws:
            await ws.recv()  # session.ready
            await ws.send(json.dumps({"event": EVENT_CALL_START, "call_id": "call_reusable_id"}))
            await ws.recv()  # call.started
            await ws.send(b"\x00" * 100)  # Invalid frame size (!= 640)
            try:
                await asyncio.wait_for(ws.wait_closed(), timeout=2.0)
            except websockets.exceptions.ConnectionClosed as exc:
                if exc.rcvd is not None:
                    bad_close_code = exc.rcvd.code
            if ws.close_code is not None:
                bad_close_code = ws.close_code

        # 6c. Subsequent healthy call reusing the same (tenant_id, call_id)
        tok_healthy = ctx.verifier.issue_token(
            provider_id="m5_rec",
            tenant_id="tenant_recovery",
            call_id="call_reusable_id",
            allowed_scopes=["voice.stream"],
        )
        rec_healthy = await run_single_paced_media_call(
            ws_url=ws_url,
            token=tok_healthy,
            call_id="call_reusable_id",
            session_slot_id=31,
            frames_to_schedule=10,
            interval_ms=20.0,
            seed=seed,
        )
        await asyncio.sleep(0.03)

        snap = ctx.controller.snapshot()
        active_calls = len(ctx.auth_bridge._active_calls)
        active_voice = ctx.telemetry_sink.active_sessions
        rec_total_closed = ctx.telemetry_sink.snapshot().total_sessions_closed

    leaked = _count_leaked_tasks()
    ok = (
        bad_close_code == 1003
        and turn2_echoed
        and rec_healthy.completed_cleanly
        and rec_healthy.frames_echoed == 10
        and snap.active_connections == 0
        and active_calls == 0
        and leaked == 0
    )
    results.append(
        FaultScenarioResult(
            scenario="recovery_after_interrupted_and_failed_calls",
            status="PASS" if ok else "FAIL",
            expected_rejections=0,
            unexpected_failures=0 if ok else 1,
            frames_sent=rec_healthy.frames_sent + 2,
            frames_echoed=rec_healthy.frames_echoed + 1,
            final_active_connections=snap.active_connections,
            final_active_calls=active_calls,
            final_active_voice_sessions=active_voice,
            leaked_tasks=leaked,
            details={
                "turn2_audio_recovered": turn2_echoed,
                "invalid_frame_close_code": bad_close_code,
                "recovered_call_echoed_frames": rec_healthy.frames_echoed,
                "total_sessions_recorded_by_sink": rec_total_closed,
            },
        )
    )

    # -------------------------------------------------------------------------
    # 7. Two different tenants under concurrent load (strict tenant isolation)
    # -------------------------------------------------------------------------
    async with start_ephemeral_media_server() as ctx:
        ws_url = f"{ctx.ws_base_url}{SYNTHETIC_MEDIA_WS_ENDPOINT_PATH}"
        iso_barrier = asyncio.Barrier(4)
        iso_specs = [
            ("tenant_iso_alpha", "call_iso_a1", 40),
            ("tenant_iso_alpha", "call_iso_a2", 41),
            ("tenant_iso_beta", "call_iso_b1", 42),
            ("tenant_iso_beta", "call_iso_b2", 43),
        ]
        iso_tasks = []
        for t_id, c_id, slot_id in iso_specs:
            tok = ctx.verifier.issue_token(
                provider_id="m5_iso",
                tenant_id=t_id,
                call_id=c_id,
                allowed_scopes=["voice.stream"],
            )
            iso_tasks.append(
                asyncio.create_task(
                    run_single_paced_media_call(
                        ws_url=ws_url,
                        token=tok,
                        call_id=c_id,
                        session_slot_id=slot_id,
                        frames_to_schedule=15,
                        interval_ms=20.0,
                        seed=seed,
                        start_barrier=iso_barrier,
                    )
                )
            )
        iso_records = await asyncio.gather(*iso_tasks)
        await asyncio.sleep(0.03)

        snap = ctx.controller.snapshot()
        active_calls = len(ctx.auth_bridge._active_calls)
        active_voice = ctx.telemetry_sink.active_sessions

    leaked = _count_leaked_tasks()
    cross_contaminations = sum(r.cross_session_frames for r in iso_records)
    mismatches = sum(r.payload_mismatches for r in iso_records)
    missing = sum(r.frames_missing for r in iso_records)
    ok = (
        all(r.completed_cleanly for r in iso_records)
        and cross_contaminations == 0
        and mismatches == 0
        and missing == 0
        and snap.active_connections == 0
        and active_calls == 0
        and leaked == 0
    )
    results.append(
        FaultScenarioResult(
            scenario="two_tenants_concurrent_isolation",
            status="PASS" if ok else "FAIL",
            expected_rejections=0,
            unexpected_failures=0 if ok else 1,
            frames_sent=sum(r.frames_sent for r in iso_records),
            frames_echoed=sum(r.frames_echoed for r in iso_records),
            final_active_connections=snap.active_connections,
            final_active_calls=active_calls,
            final_active_voice_sessions=active_voice,
            leaked_tasks=leaked,
            details={
                "tenants_tested": 2,
                "concurrent_calls": 4,
                "cross_tenant_or_session_frames": cross_contaminations,
                "payload_mismatches": mismatches,
            },
        )
    )

    return results


# ==============================================================================
# Full Evaluation Report & CLI Entrypoint
# ==============================================================================


@dataclass(frozen=True, slots=True)
class PacedMediaLoadReport:
    """Sanitized, JSON-serializable report for M5.1B paced synthetic media load evaluation."""

    runner_version: str
    gateway_version: str
    git_commit: str
    python_version: str
    os_platform: str
    cpu_info: str
    logical_cpu_count: int
    configuration: Mapping[str, Any]
    units: Mapping[str, str]
    overall_status: str
    stages: tuple[PacedStageResult, ...]
    fault_scenarios: tuple[FaultScenarioResult, ...]
    limitations: tuple[str, ...]
    media_integrity_status: str = "PASS"
    realtime_pacing_status: str = "PASS"

    def to_dict(self) -> dict[str, Any]:
        return {
            "runner_version": self.runner_version,
            "gateway_version": self.gateway_version,
            "git_commit": self.git_commit,
            "python_version": self.python_version,
            "os_platform": self.os_platform,
            "cpu_info": self.cpu_info,
            "logical_cpu_count": self.logical_cpu_count,
            "configuration": dict(self.configuration),
            "units": dict(self.units),
            "overall_status": self.overall_status,
            "media_integrity_status": self.media_integrity_status,
            "realtime_pacing_status": self.realtime_pacing_status,
            "stages": [s.to_dict() for s in self.stages],
            "fault_scenarios": [f.to_dict() for f in self.fault_scenarios],
            "limitations": list(self.limitations),
        }


async def execute_media_load_evaluation(
    *,
    concurrencies: Sequence[int] = (1, 5, 25),
    duration_seconds: float = 5.0,
    interval_ms: float = float(PCM16LE_FRAME_MS),
    seed: int = 42,
    pattern: str = "deterministic",
    queue_capacity: int = 8,
    backpressure_timeout: float = 2.0,
    skip_threshold_ms: float | None = None,
    max_concurrent_connections: int | None = None,
    max_connections_per_tenant: int | None = None,
    pacing_criteria: PacingAcceptanceCriteria | None = None,
    include_faults: bool = True,
    isolation_mode: str = ISOLATION_MODE_PROCESS_ISOLATED,
    scheduling_mode: str = SCHEDULING_MODE_ALIGNED,
) -> PacedMediaLoadReport:
    """Execute the configured paced media concurrency stages and optional fault suite."""
    effective_max_concurrent = (
        max_concurrent_connections
        if max_concurrent_connections is not None
        else DEFAULT_MAX_CONCURRENT_CONNECTIONS
    )
    effective_max_per_tenant = (
        max_connections_per_tenant
        if max_connections_per_tenant is not None
        else DEFAULT_MAX_CONNECTIONS_PER_TENANT
    )

    if pacing_criteria is not None and not isinstance(pacing_criteria, PacingAcceptanceCriteria):
        raise TypeError(
            f"pacing_criteria must be a PacingAcceptanceCriteria instance, got {type(pacing_criteria).__name__}"
        )
    effective_pacing_criteria = (
        pacing_criteria if pacing_criteria is not None else PacingAcceptanceCriteria()
    )

    if isinstance(effective_max_concurrent, bool) or not isinstance(effective_max_concurrent, int):
        raise TypeError(
            f"max_concurrent_connections must be a positive integer, not bool or {type(effective_max_concurrent).__name__}"
        )
    if effective_max_concurrent <= 0:
        raise ValueError(
            f"max_concurrent_connections must be a positive integer, got {effective_max_concurrent!r}"
        )
    if isinstance(effective_max_per_tenant, bool) or not isinstance(effective_max_per_tenant, int):
        raise TypeError(
            f"max_connections_per_tenant must be a positive integer, not bool or {type(effective_max_per_tenant).__name__}"
        )
    if effective_max_per_tenant <= 0:
        raise ValueError(
            f"max_connections_per_tenant must be a positive integer, got {effective_max_per_tenant!r}"
        )
    if effective_max_per_tenant > effective_max_concurrent:
        raise ValueError(
            f"max_connections_per_tenant ({effective_max_per_tenant}) cannot exceed max_concurrent_connections ({effective_max_concurrent})"
        )

    for c in concurrencies:
        if isinstance(c, bool) or not isinstance(c, int) or c <= 0:
            raise ValueError(f"concurrency must be a positive integer, got {c!r}")
        if c > effective_max_concurrent:
            raise ValueError(
                f"Requested concurrency {c} exceeds configured max_concurrent_connections {effective_max_concurrent}"
            )

    stage_results: list[PacedStageResult] = []
    for idx, conc in enumerate(concurrencies, start=1):
        stage_name = f"S{idx}_concurrency_{conc}"
        res = await run_paced_media_stage(
            stage_name=stage_name,
            concurrency=conc,
            duration_seconds=duration_seconds,
            interval_ms=interval_ms,
            seed=seed,
            pattern=pattern,
            queue_capacity=queue_capacity,
            backpressure_timeout=backpressure_timeout,
            skip_threshold_ms=skip_threshold_ms,
            max_concurrent_connections=effective_max_concurrent,
            max_connections_per_tenant=effective_max_per_tenant,
            pacing_criteria=effective_pacing_criteria,
            isolation_mode=isolation_mode,
            scheduling_mode=scheduling_mode,
        )
        stage_results.append(res)

    fault_results: list[FaultScenarioResult] = []
    if include_faults:
        fault_results = await run_fault_and_overload_suite(seed=seed)

    all_stages_media_pass = all(s.media_integrity_status == "PASS" for s in stage_results)
    all_stages_pacing_pass = all(s.realtime_pacing_status == "PASS" for s in stage_results)
    all_faults_pass = all(f.status == "PASS" for f in fault_results)
    media_integrity_status = "PASS" if (all_stages_media_pass and all_faults_pass) else "FAIL"
    realtime_pacing_status = "PASS" if all_stages_pacing_pass else "FAIL"
    overall = (
        "PASS"
        if (media_integrity_status == "PASS" and realtime_pacing_status == "PASS")
        else "FAIL"
    )

    limitations = (
        "Loopback WebSocket synthetic echo RTT measures local client-to-Gateway-to-FakeAI-to-client "
        "transfer on 127.0.0.1, NOT production PSTN/RTP network latency or AI model inference time.",
        "Upstream AI generation latency (upstream_ai_generation_ms), external telephony network "
        "latency (external_telephony_network_ms), and remote speaker playback latency "
        "(remote_playback_latency_ms) are unmeasured in synthetic mode and reported as available=False / None.",
        "Windows and non-RT OS schedulers exhibit timer wakeup granularity (~1-15.6 ms), which directly "
        "affects client send scheduling lateness and event-loop sleep probes without indicating Gateway blocking.",
        "NFR-PERF-004 (p95 Gateway-added overhead <= 50 ms) is evaluated as a design target on the "
        "local reference environment, not a production SLA guarantee.",
        "python_heap_peak_kib is measured via stdlib tracemalloc (Python allocator heap) and is distinct "
        "from OS process RSS (process_rss_peak_kib).",
    )

    return PacedMediaLoadReport(
        runner_version=RUNNER_VERSION,
        gateway_version=gateway_version,
        git_commit=get_git_commit(),
        python_version=sys.version.split()[0],
        os_platform=platform.platform(),
        cpu_info=platform.processor() or platform.machine(),
        logical_cpu_count=max(1, os.cpu_count() or 1),
        configuration={
            "concurrencies": list(concurrencies),
            "max_concurrent_connections": effective_max_concurrent,
            "max_connections_per_tenant": effective_max_per_tenant,
            "duration_seconds": duration_seconds,
            "interval_ms": interval_ms,
            "frame_bytes": PCM16LE_FRAME_BYTES,
            "sample_rate_hz": PCM16LE_SAMPLE_RATE,
            "channels": PCM16LE_CHANNELS,
            "audio_format": "pcm16le",
            "seed": seed,
            "pattern": pattern,
            "queue_capacity": queue_capacity,
            "backpressure_timeout_seconds": backpressure_timeout,
            "skip_threshold_ms": skip_threshold_ms,
            "missed_deadline_policy": "monotonic_fixed_schedule_rate_limited_catchup",
            "include_faults": include_faults,
            "isolation_mode": isolation_mode,
            "scheduling_mode": scheduling_mode,
            "pacing_acceptance_criteria": effective_pacing_criteria.to_dict(),
        },
        units={
            "latencies": "milliseconds (ms)",
            "durations": "seconds (s)",
            "memory": "kibibytes (KiB)",
            "cpu_utilization": "percentage (%)",
            "frame_size": "bytes (B)",
        },
        overall_status=overall,
        stages=tuple(stage_results),
        fault_scenarios=tuple(fault_results),
        limitations=limitations,
        media_integrity_status=media_integrity_status,
        realtime_pacing_status=realtime_pacing_status,
    )


def _build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m tools.synthetic_media_load",
        description="Run M5.1B paced synthetic media concurrency and overload evaluation.",
    )
    parser.add_argument(
        "--concurrency",
        type=int,
        default=None,
        help="Run a single concurrency stage (e.g. 1, 5, or 25). Overrides --stages.",
    )
    parser.add_argument(
        "--stages",
        type=str,
        default="1,5,25",
        help="Comma-separated concurrency stages to execute (default: 1,5,25).",
    )
    parser.add_argument(
        "--duration",
        type=float,
        default=5.0,
        help="Active media streaming duration per stage in seconds (default: 5.0).",
    )
    parser.add_argument(
        "--interval-ms",
        type=float,
        default=20.0,
        help="Target frame scheduling interval in milliseconds (default: 20.0).",
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=42,
        help="Deterministic seed for synthetic frame payload generation (default: 42).",
    )
    parser.add_argument(
        "--pattern",
        choices=SUPPORTED_PATTERNS,
        default="deterministic",
        help="Synthetic PCM16LE payload pattern (default: deterministic).",
    )
    parser.add_argument(
        "--queue-capacity",
        type=int,
        default=8,
        help="Bounded media queue capacity in frames (default: 8).",
    )
    parser.add_argument(
        "--backpressure-timeout",
        type=float,
        default=2.0,
        help="Backpressure timeout in seconds (default: 2.0).",
    )
    parser.add_argument(
        "--skip-threshold-ms",
        type=float,
        default=None,
        help="Optional lateness threshold in ms beyond which a scheduled frame slot is skipped.",
    )
    parser.add_argument(
        "--no-faults",
        action="store_true",
        help="Skip the GATE F stability, overload, and recovery scenarios.",
    )
    parser.add_argument(
        "--output",
        type=str,
        default=None,
        help="Optional path to write the sanitized JSON evaluation report.",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Print the full JSON report to stdout.",
    )
    parser.add_argument(
        "--isolation-mode",
        choices=SUPPORTED_ISOLATION_MODES,
        default=ISOLATION_MODE_PROCESS_ISOLATED,
        help=f"Execution isolation mode (default: {ISOLATION_MODE_PROCESS_ISOLATED}).",
    )
    parser.add_argument(
        "--scheduling-mode",
        choices=SUPPORTED_SCHEDULING_MODES,
        default=SCHEDULING_MODE_ALIGNED,
        help=f"Client frame scheduling mode across sessions (default: {SCHEDULING_MODE_ALIGNED}).",
    )
    parser.add_argument(
        "--server-subprocess",
        action="store_true",
        help="Internal flag: run as isolated Gateway server child process.",
    )
    parser.add_argument(
        "--port",
        type=int,
        default=0,
        help="Server port for subprocess mode (default: 0 for ephemeral).",
    )
    parser.add_argument(
        "--max-concurrent-connections",
        type=int,
        default=None,
        help="Max concurrent connections (default: 25).",
    )
    parser.add_argument(
        "--max-connections-per-tenant",
        type=int,
        default=None,
        help="Max connections per tenant (default: 10).",
    )
    parser.add_argument(
        "--latency-window-capacity",
        type=int,
        default=DEFAULT_LATENCY_WINDOW_SIZE,
        help="Latency window capacity.",
    )
    parser.add_argument(
        "--max-retained-sessions",
        type=int,
        default=128,
        help="Max retained telemetry sessions.",
    )
    parser.add_argument(
        "--max-missed-deadline-rate",
        type=float,
        default=DEFAULT_MAX_MISSED_DEADLINE_RATE,
        help=f"Maximum allowed fraction of missed frame deadlines (default: {DEFAULT_MAX_MISSED_DEADLINE_RATE}).",
    )
    parser.add_argument(
        "--max-mean-interval-deviation-pct",
        type=float,
        default=DEFAULT_MAX_MEAN_INTERVAL_DEVIATION_PCT,
        help=f"Maximum allowed percent deviation of mean interval from nominal (default: {DEFAULT_MAX_MEAN_INTERVAL_DEVIATION_PCT}%).",
    )
    parser.add_argument(
        "--max-p95-scheduler-lateness-ms",
        type=float,
        default=DEFAULT_MAX_P95_SCHEDULER_LATENESS_MS,
        help=f"Maximum allowed p95 scheduler lateness in ms (default: {DEFAULT_MAX_P95_SCHEDULER_LATENESS_MS}ms).",
    )
    parser.add_argument(
        "--max-transmission-elongation-pct",
        type=float,
        default=DEFAULT_MAX_TRANSMISSION_ELONGATION_PCT,
        help=f"Maximum allowed percent elongation of transmission duration (default: {DEFAULT_MAX_TRANSMISSION_ELONGATION_PCT}%).",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    parser = _build_arg_parser()
    args = parser.parse_args(argv)

    if args.server_subprocess:
        return run_isolated_server_process(
            port=int(args.port),
            max_concurrent_connections=(
                int(args.max_concurrent_connections)
                if args.max_concurrent_connections is not None
                else DEFAULT_MAX_CONCURRENT_CONNECTIONS
            ),
            max_connections_per_tenant=(
                int(args.max_connections_per_tenant)
                if args.max_connections_per_tenant is not None
                else DEFAULT_MAX_CONNECTIONS_PER_TENANT
            ),
            queue_capacity=int(args.queue_capacity),
            backpressure_timeout=float(args.backpressure_timeout),
            latency_window_capacity=int(args.latency_window_capacity),
            max_retained_sessions=int(args.max_retained_sessions),
        )

    if args.concurrency is not None:
        concurrencies = [int(args.concurrency)]
        include_faults = False if "--no-faults" in (argv or sys.argv[1:]) else not args.no_faults
    else:
        concurrencies = [int(x.strip()) for x in str(args.stages).split(",") if x.strip()]
        include_faults = not args.no_faults

    pacing_criteria = PacingAcceptanceCriteria(
        max_missed_deadline_rate=float(args.max_missed_deadline_rate),
        max_mean_interval_deviation_pct=float(args.max_mean_interval_deviation_pct),
        max_p95_scheduler_lateness_ms=float(args.max_p95_scheduler_lateness_ms),
        max_transmission_elongation_pct=float(args.max_transmission_elongation_pct),
    )

    report = asyncio.run(
        execute_media_load_evaluation(
            concurrencies=concurrencies,
            duration_seconds=float(args.duration),
            interval_ms=float(args.interval_ms),
            seed=int(args.seed),
            pattern=str(args.pattern),
            queue_capacity=int(args.queue_capacity),
            backpressure_timeout=float(args.backpressure_timeout),
            skip_threshold_ms=(
                float(args.skip_threshold_ms) if args.skip_threshold_ms is not None else None
            ),
            include_faults=include_faults,
            isolation_mode=str(args.isolation_mode),
            scheduling_mode=str(args.scheduling_mode),
            max_concurrent_connections=(
                int(args.max_concurrent_connections)
                if args.max_concurrent_connections is not None
                else None
            ),
            max_connections_per_tenant=(
                int(args.max_connections_per_tenant)
                if args.max_connections_per_tenant is not None
                else None
            ),
            pacing_criteria=pacing_criteria,
        )
    )

    payload_json = json.dumps(report.to_dict(), indent=2)
    if args.output:
        out_path = Path(args.output)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text(payload_json + "\n", encoding="utf-8")

    if args.json:
        print(payload_json)
    else:
        print("=" * 72)
        print("KAIWEN GATEWAY -- M5.1B PACED SYNTHETIC MEDIA LOAD EVALUATION")
        print("=" * 72)
        print(
            f"Commit: {report.git_commit} | Python: {report.python_version} | "
            f"OS: {report.os_platform}"
        )
        for st in report.stages:
            mc = st.media_correctness
            tm = st.timing_ms
            res = st.resources
            in_p95 = tm["gateway_inbound_processing_ms"]["p95_ms"]
            out_p95 = tm["gateway_outbound_processing_ms"]["p95_ms"]
            echo_p95 = tm["client_synthetic_echo_rtt_ms"]["p95_ms"]
            lag_p95 = tm["event_loop_lag_ms"]["p95_ms"]
            missed_rate = mc.get("missed_deadline_rate", 0.0)
            print(
                f"[{st.stage_name}] status={st.status} (media={st.media_integrity_status}, pacing={st.realtime_pacing_status}) "
                f"concurrency={st.achieved_media_concurrency}/{st.requested_concurrency} "
                f"calls={st.completed_calls} sent={mc['frames_sent']} echoed={mc['frames_echoed']} "
                f"missed_deadlines={mc['missed_deadlines']} ({missed_rate:.2%}) "
                f"missing={mc['frames_missing']} dup={mc['duplicate_frames']} "
                f"ooo={mc['out_of_order_frames']} mismatch={mc['payload_mismatches']} "
                f"in_p95={in_p95}ms out_p95={out_p95}ms echo_rtt_p95={echo_p95}ms "
                f"loop_lag_p95={lag_p95}ms cpu_1core={res['cpu_utilization_pct_single_core']}% "
                f"heap_peak={res['python_heap_peak_kib']}KiB rss_peak={res['process_rss_peak_kib']}KiB"
            )
            if st.pacing_failure_reasons:
                for r_pacing in st.pacing_failure_reasons:
                    print(f"    pacing_failure: {r_pacing}")
            if st.media_failure_reasons:
                for r_media in st.media_failure_reasons:
                    print(f"    media_failure: {r_media}")
        for ft in report.fault_scenarios:
            print(
                f"[FAULT:{ft.scenario}] status={ft.status} "
                f"expected_rej={ft.expected_rejections} unexp_fail={ft.unexpected_failures} "
                f"sent={ft.frames_sent} echoed={ft.frames_echoed} "
                f"final_conn={ft.final_active_connections} leaked_tasks={ft.leaked_tasks}"
            )
        print(
            f"OVERALL STATUS: {report.overall_status} "
            f"(media_integrity={report.media_integrity_status}, "
            f"realtime_pacing={report.realtime_pacing_status})"
        )

    return 0 if report.overall_status == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
