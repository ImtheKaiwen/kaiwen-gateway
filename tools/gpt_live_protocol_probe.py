"""Isolated diagnostic protocol probe for OpenAI GPT-Live (wss://api.openai.com/v1/live/sessions).

IMPORTANT SCOPE BOUNDARY:
This module is a standalone diagnostic probe for verifying provider-level GPT-Live
WebSocket protocol interoperability (`session.start` -> `session.started` ->
`session.input_audio.append` -> `session.output_audio.delta`).
It is NOT a Kaiwen Gateway `AIRealtimeMediaPort` adapter
(`GPT_LIVE_GATEWAY_INTEGRATION_NOT_IMPLEMENTED`).
"""

from __future__ import annotations

import asyncio
import base64
import json
import math
import struct
import time
from dataclasses import dataclass, field
from typing import Any, Final

from kaiwen_gateway.adapters.openai_realtime.config import (
    sanitize_provider_message,
    validate_endpoint_url,
)

DEFAULT_GPT_LIVE_ENDPOINT_URL: Final[str] = "wss://api.openai.com/v1/live/sessions"
DEFAULT_GPT_LIVE_MODEL: Final[str] = "gpt-live-1"
DEFAULT_GPT_LIVE_VOICE: Final[str] = "marin"
GPT_LIVE_PCM_SAMPLE_RATE: Final[int] = 24_000
GPT_LIVE_CHUNK_BYTES_20MS: Final[int] = 960  # 24000 Hz * 0.020 s * 2 bytes

VERDICT_SMOKE_PASS: Final[str] = "GPT_LIVE_PROVIDER_SMOKE_PASS"
VERDICT_SMOKE_FAIL: Final[str] = "GPT_LIVE_PROVIDER_SMOKE_FAIL"
VERDICT_BLOCKED: Final[str] = "GPT_LIVE_BLOCKED"
GATEWAY_INTEGRATION_STATUS: Final[str] = "GPT_LIVE_GATEWAY_INTEGRATION_NOT_IMPLEMENTED"

_BLOCKED_ERROR_CODES: Final[frozenset[str]] = frozenset(
    {
        "insufficient_quota",
        "invalid_api_key",
        "model_not_found",
        "permission_denied",
        "unauthorized",
        "billing_hard_limit_reached",
    }
)


def compute_pcm16le_rms(payload: bytes) -> float:
    """Compute root-mean-square amplitude of a 16-bit little-endian mono PCM buffer."""
    sample_count = len(payload) // 2
    if sample_count == 0:
        return 0.0
    samples = struct.unpack(f"<{sample_count}h", payload[: sample_count * 2])
    return math.sqrt(sum(s * s for s in samples) / sample_count)


@dataclass(frozen=True, slots=True)
class GPTLiveProbeConfig:
    """Configuration for a bounded, single-session GPT-Live diagnostic probe."""

    api_key: str = field(repr=False)
    endpoint_url: str = DEFAULT_GPT_LIVE_ENDPOINT_URL
    model: str = DEFAULT_GPT_LIVE_MODEL
    voice: str = DEFAULT_GPT_LIVE_VOICE
    instructions: str = "Be brief. Reply with the single word ready."
    connect_timeout_seconds: float = 10.0
    session_start_timeout_seconds: float = 8.0
    audio_collect_timeout_seconds: float = 6.0
    close_timeout_seconds: float = 2.0

    def __post_init__(self) -> None:
        if not self.api_key or not self.api_key.strip():
            raise ValueError("api_key must be a non-empty string")
        if not self.model or not self.model.strip():
            raise ValueError("model must be a non-empty string")
        if not self.voice or not self.voice.strip():
            raise ValueError("voice must be a non-empty string")
        validate_endpoint_url(self.endpoint_url)
        if self.connect_timeout_seconds <= 0:
            raise ValueError("connect_timeout_seconds must be > 0")
        if self.session_start_timeout_seconds <= 0:
            raise ValueError("session_start_timeout_seconds must be > 0")
        if self.audio_collect_timeout_seconds <= 0:
            raise ValueError("audio_collect_timeout_seconds must be > 0")

    def build_session_start_event(self, event_id: str = "evt_start_001") -> dict[str, Any]:
        """Build the initial ``session.start`` event with client delegation (no backend LLM tools)."""
        return {
            "type": "session.start",
            "event_id": event_id,
            "session": {
                "model": self.model,
                "instructions": self.instructions,
                "audio": {
                    "format": {
                        "type": "audio/pcm",
                        "rate": GPT_LIVE_PCM_SAMPLE_RATE,
                    },
                    "output": {
                        "voice": self.voice,
                    },
                },
                "delegation": {
                    "type": "client",
                },
            },
        }


@dataclass(frozen=True, slots=True)
class GPTLiveProbeResult:
    """Sanitized summary of a single GPT-Live diagnostic session."""

    verdict: str
    gateway_integration_status: str
    endpoint_url: str
    model: str
    session_started: bool
    session_id: str | None
    connect_latency_ms: float | None
    duration_seconds: float
    input_chunks_sent: int
    output_audio_deltas_received: int
    output_audio_bytes: int
    output_audio_rms: float
    server_event_counts: dict[str, int]
    usage: dict[str, Any] | None
    error_message: str | None = None


async def run_gpt_live_probe(
    config: GPTLiveProbeConfig,
    input_pcm24k: bytes,
    *,
    chunk_pacing_seconds: float = 0.015,
) -> GPTLiveProbeResult:
    """Execute a single bounded GPT-Live diagnostic smoke session without retries."""
    validate_endpoint_url(config.endpoint_url)
    import websockets

    t0 = time.perf_counter()
    connect_latency_ms: float | None = None
    session_started = False
    session_id: str | None = None
    input_chunks_sent = 0
    audio_deltas: list[bytes] = []
    event_counts: dict[str, int] = {}
    observed_usage: dict[str, Any] | None = None
    error_code: str | None = None
    error_message: str | None = None

    started_evt = asyncio.Event()
    error_evt = asyncio.Event()

    try:
        async with websockets.connect(
            config.endpoint_url,
            additional_headers={"Authorization": f"Bearer {config.api_key}"},
            open_timeout=config.connect_timeout_seconds,
            close_timeout=config.close_timeout_seconds,
        ) as ws:
            connect_latency_ms = round((time.perf_counter() - t0) * 1000.0, 1)

            async def _receiver() -> None:
                nonlocal session_started, session_id, observed_usage, error_code, error_message
                async for raw in ws:
                    if not isinstance(raw, str):
                        continue
                    evt = json.loads(raw)
                    if not isinstance(evt, dict):
                        continue
                    etype = str(evt.get("type") or "unknown")
                    event_counts[etype] = event_counts.get(etype, 0) + 1

                    if etype == "session.started":
                        session_started = True
                        sess = evt.get("session")
                        if isinstance(sess, dict) and isinstance(sess.get("id"), str):
                            session_id = sess["id"]
                        started_evt.set()
                    elif etype == "session.output_audio.delta":
                        delta_b64 = evt.get("delta")
                        if isinstance(delta_b64, str) and delta_b64:
                            audio_deltas.append(base64.b64decode(delta_b64))
                    elif etype == "error":
                        nested_err = evt.get("error")
                        err_obj: dict[str, Any] = (
                            nested_err if isinstance(nested_err, dict) else evt
                        )
                        raw_code = err_obj.get("code") or err_obj.get("type")
                        if isinstance(raw_code, str):
                            error_code = raw_code
                        raw_msg = err_obj.get("message") or json.dumps(err_obj)
                        error_message = sanitize_provider_message(str(raw_msg), config.api_key)
                        error_evt.set()

                    if isinstance(evt.get("usage"), dict):
                        observed_usage = dict(evt["usage"])

            recv_task = asyncio.create_task(_receiver())
            try:
                await ws.send(json.dumps(config.build_session_start_event()))
                wait_tasks = [
                    asyncio.create_task(started_evt.wait()),
                    asyncio.create_task(error_evt.wait()),
                ]
                try:
                    await asyncio.wait(
                        wait_tasks,
                        timeout=config.session_start_timeout_seconds,
                        return_when=asyncio.FIRST_COMPLETED,
                    )
                finally:
                    for wt in wait_tasks:
                        if not wt.done():
                            wt.cancel()

                if session_started and not error_evt.is_set():
                    padded = input_pcm24k
                    rem = len(padded) % GPT_LIVE_CHUNK_BYTES_20MS
                    if rem:
                        padded += b"\x00" * (GPT_LIVE_CHUNK_BYTES_20MS - rem)

                    for offset in range(0, len(padded), GPT_LIVE_CHUNK_BYTES_20MS):
                        if error_evt.is_set():
                            break
                        chunk = padded[offset : offset + GPT_LIVE_CHUNK_BYTES_20MS]
                        await ws.send(
                            json.dumps(
                                {
                                    "type": "session.input_audio.append",
                                    "audio": base64.b64encode(chunk).decode("ascii"),
                                }
                            )
                        )
                        input_chunks_sent += 1
                        if chunk_pacing_seconds > 0:
                            await asyncio.sleep(chunk_pacing_seconds)

                    deadline = time.perf_counter() + config.audio_collect_timeout_seconds
                    last_count = 0
                    stable_ticks = 0
                    while time.perf_counter() < deadline and not error_evt.is_set():
                        await asyncio.sleep(0.1)
                        if len(audio_deltas) > 0:
                            if len(audio_deltas) == last_count:
                                stable_ticks += 1
                                if stable_ticks >= 4:
                                    break
                            else:
                                last_count = len(audio_deltas)
                                stable_ticks = 0
            finally:
                await ws.close()
                await asyncio.gather(recv_task, return_exceptions=True)

    except Exception as exc:
        safe_exc = sanitize_provider_message(f"{type(exc).__name__}: {exc}", config.api_key)
        error_message = error_message or safe_exc
        lower_exc = safe_exc.lower()
        if any(tok in lower_exc for tok in ("401", "403", "quota", "unauthorized", "permission")):
            error_code = error_code or "unauthorized"

    duration_s = round(time.perf_counter() - t0, 3)
    combined_audio = b"".join(audio_deltas)
    rms = round(compute_pcm16le_rms(combined_audio), 1) if combined_audio else 0.0

    if error_code and error_code.lower() in _BLOCKED_ERROR_CODES:
        verdict = VERDICT_BLOCKED
    elif session_started and len(audio_deltas) > 0 and error_message is None:
        verdict = VERDICT_SMOKE_PASS
    else:
        verdict = VERDICT_SMOKE_FAIL

    return GPTLiveProbeResult(
        verdict=verdict,
        gateway_integration_status=GATEWAY_INTEGRATION_STATUS,
        endpoint_url=config.endpoint_url,
        model=config.model,
        session_started=session_started,
        session_id=session_id,
        connect_latency_ms=connect_latency_ms,
        duration_seconds=duration_s,
        input_chunks_sent=input_chunks_sent,
        output_audio_deltas_received=len(audio_deltas),
        output_audio_bytes=len(combined_audio),
        output_audio_rms=rms,
        server_event_counts=event_counts,
        usage=observed_usage,
        error_message=error_message,
    )
