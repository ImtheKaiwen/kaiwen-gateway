"""Provider-independent, bounded, privacy-safe in-memory telemetry for the Voice Bridge (M5.1A).

Invariants:
- Provider-neutral: zero assumptions or dependencies on Kaiwen Agent, OpenAI, Verimor, or external SDKs.
- Bounded memory: latency distributions use a fixed-capacity sliding window (`deque(maxlen=...)`);
  aggregate sinks retain at most `max_retained_sessions` completed session snapshots and never hold
  references to live/closed session objects, media ports, queues, or audio frames.
- Privacy & security: never records raw audio bytes, transcripts, API keys, tokens, phone numbers,
  prompts, exception messages, or high-cardinality tenant/call identifiers as metric dimensions.
- Honest latency separation: explicitly separates Gateway receive/forward processing + adapter send
  await duration (`inbound_processing_ms`, `outbound_processing_ms`), internal queue waiting duration
  (`inbound_queue_wait_ms`, `outbound_queue_wait_ms`), and ASGI WebSocket `send_bytes` await duration
  (`network_transport_ms`, correlated per-frame via consume-once token); reports upstream AI generation
  duration and remote playback latency as unavailable/unknown rather than fabricating end-to-end metrics.
  Note: `websocket.send_bytes` await duration is an ASGI send-call duration, not proof of physical wire
  delivery, TCP socket flush, client reception, or speaker playback.
- Unavailable vs zero: a latency metric without samples reports `available=False` and `None` for
  duration fields (never `0.0`).
- Best-effort sink failure policy: telemetry sink callbacks must never corrupt an active call or
  prevent deterministic resource cleanup.
"""

from __future__ import annotations

import asyncio
import math
from collections import deque
from collections.abc import Mapping
from dataclasses import dataclass
from enum import StrEnum
from types import MappingProxyType
from typing import Any, Final, Protocol, runtime_checkable

from kaiwen_gateway.modules.voice.domain import (
    FutureTurnAudioFrameError,
    InvalidAudioFrameError,
    MediaBackpressureOverflowError,
    MediaPortClosedError,
    MediaPortError,
    OutOfOrderAudioFrameError,
    UnsupportedInterruptionCapabilityError,
    VoiceDomainError,
)

__all__ = [
    "DEFAULT_LATENCY_WINDOW_SIZE",
    "DEFAULT_MAX_RETAINED_SESSIONS",
    "SMALL_SAMPLE_THRESHOLD",
    "VoiceErrorCategory",
    "VoiceTerminalCleanupState",
    "MetricAvailabilityReason",
    "LatencyDistributionSummary",
    "BoundedLatencyAggregator",
    "VoiceSessionMetricsSnapshot",
    "VoiceAggregateMetricsSnapshot",
    "VoiceTelemetrySink",
    "InMemoryVoiceTelemetrySink",
    "classify_voice_error",
]

DEFAULT_LATENCY_WINDOW_SIZE: Final[int] = 256
DEFAULT_MAX_RETAINED_SESSIONS: Final[int] = 64
SMALL_SAMPLE_THRESHOLD: Final[int] = 20


class VoiceErrorCategory(StrEnum):
    """Safe, bounded error categories for Voice Bridge telemetry.

    Never includes raw exception messages, caller payloads, or tenant/call identifiers.
    """

    INVALID_FRAME = "invalid_frame"
    OUT_OF_ORDER_FRAME = "out_of_order_frame"
    FUTURE_TURN_FRAME = "future_turn_frame"
    BACKPRESSURE_OVERFLOW = "backpressure_overflow"
    PORT_CLOSED = "port_closed"
    PORT_IO_ERROR = "port_io_error"
    UNSUPPORTED_CAPABILITY = "unsupported_capability"
    INTERRUPTION_TIMEOUT = "interruption_timeout"
    INTERRUPTION_FAILED = "interruption_failed"
    SESSION_CANCELLED = "session_cancelled"
    CLEANUP_ERROR = "cleanup_error"
    INTERNAL_ERROR = "internal_error"


class VoiceTerminalCleanupState(StrEnum):
    """Lifecycle state of terminal resource cleanup for a VoiceBridgeSession."""

    NOT_STARTED = "not_started"
    IN_PROGRESS = "in_progress"
    COMPLETED_CLEAN = "completed_clean"
    COMPLETED_WITH_ERRORS = "completed_with_errors"


class MetricAvailabilityReason(StrEnum):
    """Canonical reason describing whether a latency metric is measured or unavailable."""

    MEASURED = "measured"
    NO_SAMPLES = "no_samples"
    INSUFFICIENT_SAMPLES_FOR_PERCENTILES = "insufficient_samples_for_percentiles"
    UNKNOWN_WITHOUT_PROVIDER_INSTRUMENTATION = "unknown_without_provider_instrumentation"
    UNKNOWN_WITHOUT_PLAYBACK_ACKNOWLEDGMENT = "unknown_without_playback_acknowledgment"


def classify_voice_error(
    exc: BaseException,
    *,
    during_interruption: bool = False,
    during_cleanup: bool = False,
) -> VoiceErrorCategory:
    """Classify an exception into a safe, bounded VoiceErrorCategory without inspecting message text."""
    if during_cleanup:
        return VoiceErrorCategory.CLEANUP_ERROR
    if isinstance(exc, FutureTurnAudioFrameError):
        return VoiceErrorCategory.FUTURE_TURN_FRAME
    if isinstance(exc, OutOfOrderAudioFrameError):
        return VoiceErrorCategory.OUT_OF_ORDER_FRAME
    if isinstance(exc, InvalidAudioFrameError):
        return VoiceErrorCategory.INVALID_FRAME
    if isinstance(exc, MediaBackpressureOverflowError):
        return VoiceErrorCategory.BACKPRESSURE_OVERFLOW
    if isinstance(exc, UnsupportedInterruptionCapabilityError):
        return VoiceErrorCategory.UNSUPPORTED_CAPABILITY
    if isinstance(exc, MediaPortClosedError):
        return VoiceErrorCategory.PORT_CLOSED
    if isinstance(exc, MediaPortError):
        return VoiceErrorCategory.PORT_IO_ERROR
    if during_interruption:
        if isinstance(exc, TimeoutError):
            return VoiceErrorCategory.INTERRUPTION_TIMEOUT
        if isinstance(exc, VoiceDomainError):
            return VoiceErrorCategory.INVALID_FRAME
        return VoiceErrorCategory.INTERRUPTION_FAILED
    if isinstance(exc, asyncio.CancelledError):
        return VoiceErrorCategory.SESSION_CANCELLED
    if isinstance(exc, TimeoutError):
        return VoiceErrorCategory.INTERRUPTION_TIMEOUT
    if isinstance(exc, VoiceDomainError):
        return VoiceErrorCategory.INVALID_FRAME
    return VoiceErrorCategory.INTERNAL_ERROR


def _compute_percentile(sorted_samples: list[float], percentile: float) -> float:
    """Compute linear-interpolated percentile over a non-empty sorted list of floats."""
    n = len(sorted_samples)
    if n == 1:
        return sorted_samples[0]
    k = (n - 1) * percentile
    f = math.floor(k)
    c = math.ceil(k)
    if f == c:
        return sorted_samples[int(k)]
    return sorted_samples[f] * (c - k) + sorted_samples[c] * (k - f)


@dataclass(frozen=True, slots=True)
class LatencyDistributionSummary:
    """Immutable bounded statistical summary of a duration metric in milliseconds.

    Invariants:
    - When `sample_count == 0`, `available` is `False` and all duration fields (`min_ms`,
      `max_ms`, `mean_ms`, `p50_ms`, `p95_ms`, `p99_ms`) are `None` (never `0.0`).
    - When `sample_count > 0` and `retained_sample_count < min_percentile_samples`,
      `available` is `True` with `min_ms`, `max_ms`, and `mean_ms` populated, while
      `p50_ms`, `p95_ms`, and `p99_ms` are `None`.
    - Percentiles are computed over the bounded sliding window of at most `window_capacity`
      most recent samples.
    """

    available: bool
    availability_reason: MetricAvailabilityReason
    sample_count: int
    retained_sample_count: int
    window_capacity: int
    is_small_sample: bool
    min_ms: float | None = None
    max_ms: float | None = None
    mean_ms: float | None = None
    p50_ms: float | None = None
    p95_ms: float | None = None
    p99_ms: float | None = None

    @classmethod
    def unavailable(
        cls,
        reason: MetricAvailabilityReason = MetricAvailabilityReason.NO_SAMPLES,
        *,
        window_capacity: int = DEFAULT_LATENCY_WINDOW_SIZE,
    ) -> LatencyDistributionSummary:
        """Construct an explicit unavailable latency summary with None duration values."""
        return cls(
            available=False,
            availability_reason=reason,
            sample_count=0,
            retained_sample_count=0,
            window_capacity=window_capacity,
            is_small_sample=False,
            min_ms=None,
            max_ms=None,
            mean_ms=None,
            p50_ms=None,
            p95_ms=None,
            p99_ms=None,
        )

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible dictionary representation with safe primitive values."""
        return {
            "available": self.available,
            "availability_reason": self.availability_reason.value,
            "sample_count": self.sample_count,
            "retained_sample_count": self.retained_sample_count,
            "window_capacity": self.window_capacity,
            "is_small_sample": self.is_small_sample,
            "min_ms": self.min_ms,
            "max_ms": self.max_ms,
            "mean_ms": self.mean_ms,
            "p50_ms": self.p50_ms,
            "p95_ms": self.p95_ms,
            "p99_ms": self.p99_ms,
        }


class BoundedLatencyAggregator:
    """Bounded O(1)-space latency aggregator using a fixed-size sliding sample window.

    Tracks exact running `sample_count`, `min_ms`, `max_ms`, and `mean_ms` across all
    observed samples, while bounding retained sample memory to `window_capacity` floats
    for `p50_ms`, `p95_ms`, and `p99_ms` estimation.
    """

    __slots__ = (
        "_window_capacity",
        "_min_percentile_samples",
        "_samples",
        "_sample_count",
        "_total_ms",
        "_min_ms",
        "_max_ms",
    )

    def __init__(
        self,
        *,
        window_capacity: int = DEFAULT_LATENCY_WINDOW_SIZE,
        min_percentile_samples: int = 1,
    ) -> None:
        if (
            isinstance(window_capacity, bool)
            or not isinstance(window_capacity, int)
            or window_capacity < 1
        ):
            raise VoiceDomainError("window_capacity must be an integer >= 1")
        if (
            isinstance(min_percentile_samples, bool)
            or not isinstance(min_percentile_samples, int)
            or min_percentile_samples < 1
        ):
            raise VoiceDomainError("min_percentile_samples must be an integer >= 1")

        self._window_capacity: Final[int] = window_capacity
        self._min_percentile_samples: Final[int] = min_percentile_samples
        self._samples: deque[float] = deque(maxlen=window_capacity)
        self._sample_count: int = 0
        self._total_ms: float = 0.0
        self._min_ms: float | None = None
        self._max_ms: float | None = None

    @property
    def window_capacity(self) -> int:
        """Maximum number of recent samples retained in memory."""
        return self._window_capacity

    @property
    def min_percentile_samples(self) -> int:
        """Minimum retained sample count required before reporting percentiles."""
        return self._min_percentile_samples

    @property
    def sample_count(self) -> int:
        """Total number of samples recorded since initialization."""
        return self._sample_count

    @property
    def retained_sample_count(self) -> int:
        """Current number of samples stored in the bounded window (`<= window_capacity`)."""
        return len(self._samples)

    def record(self, duration_ms: float) -> None:
        """Record a single non-negative finite duration measurement in milliseconds."""
        if isinstance(duration_ms, bool) or not isinstance(duration_ms, (int, float)):
            raise VoiceDomainError(
                f"duration_ms must be a finite non-negative float, got {type(duration_ms).__name__}"
            )
        val = float(duration_ms)
        if not math.isfinite(val) or val < 0.0:
            raise VoiceDomainError(f"duration_ms must be a finite non-negative float, got {val}")

        self._samples.append(val)
        self._sample_count += 1
        self._total_ms += val
        if self._min_ms is None or val < self._min_ms:
            self._min_ms = val
        if self._max_ms is None or val > self._max_ms:
            self._max_ms = val

    def snapshot(self) -> LatencyDistributionSummary:
        """Compute an immutable statistical summary of recorded latency samples."""
        if self._sample_count == 0:
            return LatencyDistributionSummary.unavailable(
                MetricAvailabilityReason.NO_SAMPLES,
                window_capacity=self._window_capacity,
            )

        sorted_window = sorted(self._samples)
        retained = len(sorted_window)
        mean_val = self._total_ms / self._sample_count
        is_small = retained < SMALL_SAMPLE_THRESHOLD

        if retained < self._min_percentile_samples:
            return LatencyDistributionSummary(
                available=True,
                availability_reason=MetricAvailabilityReason.INSUFFICIENT_SAMPLES_FOR_PERCENTILES,
                sample_count=self._sample_count,
                retained_sample_count=retained,
                window_capacity=self._window_capacity,
                is_small_sample=is_small,
                min_ms=self._min_ms,
                max_ms=self._max_ms,
                mean_ms=mean_val,
                p50_ms=None,
                p95_ms=None,
                p99_ms=None,
            )

        return LatencyDistributionSummary(
            available=True,
            availability_reason=MetricAvailabilityReason.MEASURED,
            sample_count=self._sample_count,
            retained_sample_count=retained,
            window_capacity=self._window_capacity,
            is_small_sample=is_small,
            min_ms=self._min_ms,
            max_ms=self._max_ms,
            mean_ms=mean_val,
            p50_ms=_compute_percentile(sorted_window, 0.50),
            p95_ms=_compute_percentile(sorted_window, 0.95),
            p99_ms=_compute_percentile(sorted_window, 0.99),
        )


@dataclass(frozen=True, slots=True)
class VoiceSessionMetricsSnapshot:
    """Privacy-safe, bounded telemetry snapshot for a single VoiceBridgeSession.

    Deliberately excludes `call_id`, `tenant_id`, raw audio payloads, transcripts,
    tokens, and raw exception strings to guarantee zero PII/secret/high-cardinality
    leakage in metric exports.
    """

    session_state: str
    cleanup_state: VoiceTerminalCleanupState
    session_duration_ms: float | None
    cleanup_duration_ms: float | None

    inbound_frames_received: int
    inbound_frames_transferred: int
    inbound_bytes_received: int
    inbound_bytes_transferred: int
    outbound_frames_received: int
    outbound_frames_sent: int
    outbound_bytes_received: int
    outbound_bytes_sent: int

    dropped_frames_total: int
    locally_discarded_frames: int
    unforwarded_inbound_frames: int
    unforwarded_outbound_frames: int
    stale_frames_suppressed: int

    current_turn_id: int
    fenced_turn_id: int
    interruptions_requested: int
    interruptions_completed: int
    interruptions_noop: int
    interruptions_failed: int

    queue_capacity: int
    inbound_queue_depth: int
    inbound_queue_high_water_mark: int
    outbound_queue_depth: int
    outbound_queue_high_water_mark: int

    inbound_processing_ms: LatencyDistributionSummary
    outbound_processing_ms: LatencyDistributionSummary
    inbound_queue_wait_ms: LatencyDistributionSummary
    outbound_queue_wait_ms: LatencyDistributionSummary
    network_transport_ms: LatencyDistributionSummary
    upstream_ai_generation_ms: LatencyDistributionSummary
    remote_playback_latency_ms: LatencyDistributionSummary

    error_counts: Mapping[VoiceErrorCategory, int]
    terminal_error_category: VoiceErrorCategory | None
    telemetry_sink_errors: int

    @property
    def total_errors(self) -> int:
        """Total count of categorized errors observed during the session."""
        return sum(self.error_counts.values())

    def error_count(self, category: VoiceErrorCategory) -> int:
        """Return the count of errors observed for a specific VoiceErrorCategory."""
        return self.error_counts.get(category, 0)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-serializable dictionary of safe session metrics."""
        return {
            "session_state": str(self.session_state),
            "cleanup_state": self.cleanup_state.value,
            "session_duration_ms": self.session_duration_ms,
            "cleanup_duration_ms": self.cleanup_duration_ms,
            "inbound_frames_received": self.inbound_frames_received,
            "inbound_frames_transferred": self.inbound_frames_transferred,
            "inbound_bytes_received": self.inbound_bytes_received,
            "inbound_bytes_transferred": self.inbound_bytes_transferred,
            "outbound_frames_received": self.outbound_frames_received,
            "outbound_frames_sent": self.outbound_frames_sent,
            "outbound_bytes_received": self.outbound_bytes_received,
            "outbound_bytes_sent": self.outbound_bytes_sent,
            "dropped_frames_total": self.dropped_frames_total,
            "locally_discarded_frames": self.locally_discarded_frames,
            "unforwarded_inbound_frames": self.unforwarded_inbound_frames,
            "unforwarded_outbound_frames": self.unforwarded_outbound_frames,
            "stale_frames_suppressed": self.stale_frames_suppressed,
            "current_turn_id": self.current_turn_id,
            "fenced_turn_id": self.fenced_turn_id,
            "interruptions_requested": self.interruptions_requested,
            "interruptions_completed": self.interruptions_completed,
            "interruptions_noop": self.interruptions_noop,
            "interruptions_failed": self.interruptions_failed,
            "queue_capacity": self.queue_capacity,
            "inbound_queue_depth": self.inbound_queue_depth,
            "inbound_queue_high_water_mark": self.inbound_queue_high_water_mark,
            "outbound_queue_depth": self.outbound_queue_depth,
            "outbound_queue_high_water_mark": self.outbound_queue_high_water_mark,
            "inbound_processing_ms": self.inbound_processing_ms.to_dict(),
            "outbound_processing_ms": self.outbound_processing_ms.to_dict(),
            "inbound_queue_wait_ms": self.inbound_queue_wait_ms.to_dict(),
            "outbound_queue_wait_ms": self.outbound_queue_wait_ms.to_dict(),
            "network_transport_ms": self.network_transport_ms.to_dict(),
            "upstream_ai_generation_ms": self.upstream_ai_generation_ms.to_dict(),
            "remote_playback_latency_ms": self.remote_playback_latency_ms.to_dict(),
            "error_counts": {k.value: v for k, v in self.error_counts.items()},
            "terminal_error_category": (
                self.terminal_error_category.value
                if self.terminal_error_category is not None
                else None
            ),
            "telemetry_sink_errors": self.telemetry_sink_errors,
        }


@dataclass(frozen=True, slots=True)
class VoiceAggregateMetricsSnapshot:
    """Immutable bounded aggregate telemetry snapshot across VoiceBridgeSession runs."""

    active_sessions: int
    peak_active_sessions: int
    total_sessions_started: int
    total_sessions_closed: int
    total_sessions_completed_clean: int
    total_sessions_failed: int

    total_inbound_frames_received: int
    total_inbound_frames_transferred: int
    total_inbound_bytes_received: int
    total_inbound_bytes_transferred: int
    total_outbound_frames_received: int
    total_outbound_frames_sent: int
    total_outbound_bytes_received: int
    total_outbound_bytes_sent: int

    total_dropped_frames: int
    total_locally_discarded_frames: int
    total_unforwarded_inbound_frames: int
    total_unforwarded_outbound_frames: int
    total_stale_frames_suppressed: int

    total_interruptions_requested: int
    total_interruptions_completed: int
    total_interruptions_noop: int
    total_interruptions_failed: int

    max_inbound_queue_high_water_mark: int
    max_outbound_queue_high_water_mark: int

    session_duration_ms: LatencyDistributionSummary
    cleanup_duration_ms: LatencyDistributionSummary

    error_counts: Mapping[VoiceErrorCategory, int]
    retained_session_count: int
    max_retained_sessions: int
    recent_sessions: tuple[VoiceSessionMetricsSnapshot, ...]

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-serializable dictionary of aggregate telemetry metrics."""
        return {
            "active_sessions": self.active_sessions,
            "peak_active_sessions": self.peak_active_sessions,
            "total_sessions_started": self.total_sessions_started,
            "total_sessions_closed": self.total_sessions_closed,
            "total_sessions_completed_clean": self.total_sessions_completed_clean,
            "total_sessions_failed": self.total_sessions_failed,
            "total_inbound_frames_received": self.total_inbound_frames_received,
            "total_inbound_frames_transferred": self.total_inbound_frames_transferred,
            "total_inbound_bytes_received": self.total_inbound_bytes_received,
            "total_inbound_bytes_transferred": self.total_inbound_bytes_transferred,
            "total_outbound_frames_received": self.total_outbound_frames_received,
            "total_outbound_frames_sent": self.total_outbound_frames_sent,
            "total_outbound_bytes_received": self.total_outbound_bytes_received,
            "total_outbound_bytes_sent": self.total_outbound_bytes_sent,
            "total_dropped_frames": self.total_dropped_frames,
            "total_locally_discarded_frames": self.total_locally_discarded_frames,
            "total_unforwarded_inbound_frames": self.total_unforwarded_inbound_frames,
            "total_unforwarded_outbound_frames": self.total_unforwarded_outbound_frames,
            "total_stale_frames_suppressed": self.total_stale_frames_suppressed,
            "total_interruptions_requested": self.total_interruptions_requested,
            "total_interruptions_completed": self.total_interruptions_completed,
            "total_interruptions_noop": self.total_interruptions_noop,
            "total_interruptions_failed": self.total_interruptions_failed,
            "max_inbound_queue_high_water_mark": self.max_inbound_queue_high_water_mark,
            "max_outbound_queue_high_water_mark": self.max_outbound_queue_high_water_mark,
            "session_duration_ms": self.session_duration_ms.to_dict(),
            "cleanup_duration_ms": self.cleanup_duration_ms.to_dict(),
            "error_counts": {k.value: v for k, v in self.error_counts.items()},
            "retained_session_count": self.retained_session_count,
            "max_retained_sessions": self.max_retained_sessions,
        }


@runtime_checkable
class VoiceTelemetrySink(Protocol):
    """Best-effort telemetry observer protocol for VoiceBridgeSession lifecycle and metrics.

    Failure Policy:
        Sink callbacks are invoked on a best-effort basis by VoiceBridgeSession.
        Exceptions raised by a sink are caught, counted in `telemetry_sink_errors`,
        and suppressed so they never corrupt media streaming or abort cleanup.
    """

    def on_session_started(self) -> None:
        """Called when a VoiceBridgeSession transitions to RUNNING."""
        ...

    def on_session_closed(self, snapshot: VoiceSessionMetricsSnapshot) -> None:
        """Called once when a VoiceBridgeSession completes terminal cleanup."""
        ...


class InMemoryVoiceTelemetrySink:
    """Bounded, privacy-safe in-memory telemetry sink for VoiceBridgeSession instances.

    Guarantees:
    - O(1) bounded memory: retains at most `max_retained_sessions` immutable
      `VoiceSessionMetricsSnapshot` objects in a ring buffer (`deque(maxlen=...)`).
    - Never holds references to `VoiceBridgeSession`, media ports, queues, or `AudioFrame`
      instances, preventing memory leaks across long-running processes.
    - Bounded cardinality: `error_counts` is keyed strictly by `VoiceErrorCategory`.
    """

    def __init__(
        self,
        *,
        max_retained_sessions: int = DEFAULT_MAX_RETAINED_SESSIONS,
        latency_window_capacity: int = DEFAULT_LATENCY_WINDOW_SIZE,
        min_percentile_samples: int = 1,
    ) -> None:
        if (
            isinstance(max_retained_sessions, bool)
            or not isinstance(max_retained_sessions, int)
            or max_retained_sessions < 1
        ):
            raise VoiceDomainError("max_retained_sessions must be an integer >= 1")

        self._max_retained_sessions: Final[int] = max_retained_sessions
        self._recent_sessions: deque[VoiceSessionMetricsSnapshot] = deque(
            maxlen=max_retained_sessions
        )
        self._session_duration_agg = BoundedLatencyAggregator(
            window_capacity=latency_window_capacity,
            min_percentile_samples=min_percentile_samples,
        )
        self._cleanup_duration_agg = BoundedLatencyAggregator(
            window_capacity=latency_window_capacity,
            min_percentile_samples=min_percentile_samples,
        )

        self._active_sessions: int = 0
        self._peak_active_sessions: int = 0
        self._total_sessions_started: int = 0
        self._total_sessions_closed: int = 0
        self._total_sessions_completed_clean: int = 0
        self._total_sessions_failed: int = 0

        self._total_inbound_frames_received: int = 0
        self._total_inbound_frames_transferred: int = 0
        self._total_inbound_bytes_received: int = 0
        self._total_inbound_bytes_transferred: int = 0
        self._total_outbound_frames_received: int = 0
        self._total_outbound_frames_sent: int = 0
        self._total_outbound_bytes_received: int = 0
        self._total_outbound_bytes_sent: int = 0

        self._total_dropped_frames: int = 0
        self._total_locally_discarded_frames: int = 0
        self._total_unforwarded_inbound_frames: int = 0
        self._total_unforwarded_outbound_frames: int = 0
        self._total_stale_frames_suppressed: int = 0

        self._total_interruptions_requested: int = 0
        self._total_interruptions_completed: int = 0
        self._total_interruptions_noop: int = 0
        self._total_interruptions_failed: int = 0

        self._max_inbound_queue_hwm: int = 0
        self._max_outbound_queue_hwm: int = 0
        self._error_counts: dict[VoiceErrorCategory, int] = {}

    @property
    def max_retained_sessions(self) -> int:
        """Maximum number of completed session snapshots retained in memory."""
        return self._max_retained_sessions

    @property
    def active_sessions(self) -> int:
        """Current number of active VoiceBridgeSession instances."""
        return self._active_sessions

    def on_session_started(self) -> None:
        """Record that a session has transitioned to RUNNING."""
        self._active_sessions += 1
        self._total_sessions_started += 1
        if self._active_sessions > self._peak_active_sessions:
            self._peak_active_sessions = self._active_sessions

    def on_session_closed(self, snapshot: VoiceSessionMetricsSnapshot) -> None:
        """Record terminal metrics from a completed or failed session."""
        if snapshot.session_duration_ms is not None and self._active_sessions > 0:
            self._active_sessions -= 1

        self._total_sessions_closed += 1
        if (
            snapshot.session_state == "closed"
            and snapshot.cleanup_state == VoiceTerminalCleanupState.COMPLETED_CLEAN
        ):
            self._total_sessions_completed_clean += 1
        else:
            self._total_sessions_failed += 1

        self._total_inbound_frames_received += snapshot.inbound_frames_received
        self._total_inbound_frames_transferred += snapshot.inbound_frames_transferred
        self._total_inbound_bytes_received += snapshot.inbound_bytes_received
        self._total_inbound_bytes_transferred += snapshot.inbound_bytes_transferred
        self._total_outbound_frames_received += snapshot.outbound_frames_received
        self._total_outbound_frames_sent += snapshot.outbound_frames_sent
        self._total_outbound_bytes_received += snapshot.outbound_bytes_received
        self._total_outbound_bytes_sent += snapshot.outbound_bytes_sent

        self._total_dropped_frames += snapshot.dropped_frames_total
        self._total_locally_discarded_frames += snapshot.locally_discarded_frames
        self._total_unforwarded_inbound_frames += snapshot.unforwarded_inbound_frames
        self._total_unforwarded_outbound_frames += snapshot.unforwarded_outbound_frames
        self._total_stale_frames_suppressed += snapshot.stale_frames_suppressed

        self._total_interruptions_requested += snapshot.interruptions_requested
        self._total_interruptions_completed += snapshot.interruptions_completed
        self._total_interruptions_noop += snapshot.interruptions_noop
        self._total_interruptions_failed += snapshot.interruptions_failed

        if snapshot.inbound_queue_high_water_mark > self._max_inbound_queue_hwm:
            self._max_inbound_queue_hwm = snapshot.inbound_queue_high_water_mark
        if snapshot.outbound_queue_high_water_mark > self._max_outbound_queue_hwm:
            self._max_outbound_queue_hwm = snapshot.outbound_queue_high_water_mark

        if snapshot.session_duration_ms is not None:
            self._session_duration_agg.record(snapshot.session_duration_ms)
        if snapshot.cleanup_duration_ms is not None:
            self._cleanup_duration_agg.record(snapshot.cleanup_duration_ms)

        for cat, count in snapshot.error_counts.items():
            if count > 0:
                self._error_counts[cat] = self._error_counts.get(cat, 0) + count

        self._recent_sessions.append(snapshot)

    def snapshot(self) -> VoiceAggregateMetricsSnapshot:
        """Return an immutable snapshot of aggregate metrics across sessions."""
        return VoiceAggregateMetricsSnapshot(
            active_sessions=self._active_sessions,
            peak_active_sessions=self._peak_active_sessions,
            total_sessions_started=self._total_sessions_started,
            total_sessions_closed=self._total_sessions_closed,
            total_sessions_completed_clean=self._total_sessions_completed_clean,
            total_sessions_failed=self._total_sessions_failed,
            total_inbound_frames_received=self._total_inbound_frames_received,
            total_inbound_frames_transferred=self._total_inbound_frames_transferred,
            total_inbound_bytes_received=self._total_inbound_bytes_received,
            total_inbound_bytes_transferred=self._total_inbound_bytes_transferred,
            total_outbound_frames_received=self._total_outbound_frames_received,
            total_outbound_frames_sent=self._total_outbound_frames_sent,
            total_outbound_bytes_received=self._total_outbound_bytes_received,
            total_outbound_bytes_sent=self._total_outbound_bytes_sent,
            total_dropped_frames=self._total_dropped_frames,
            total_locally_discarded_frames=self._total_locally_discarded_frames,
            total_unforwarded_inbound_frames=self._total_unforwarded_inbound_frames,
            total_unforwarded_outbound_frames=self._total_unforwarded_outbound_frames,
            total_stale_frames_suppressed=self._total_stale_frames_suppressed,
            total_interruptions_requested=self._total_interruptions_requested,
            total_interruptions_completed=self._total_interruptions_completed,
            total_interruptions_noop=self._total_interruptions_noop,
            total_interruptions_failed=self._total_interruptions_failed,
            max_inbound_queue_high_water_mark=self._max_inbound_queue_hwm,
            max_outbound_queue_high_water_mark=self._max_outbound_queue_hwm,
            session_duration_ms=self._session_duration_agg.snapshot(),
            cleanup_duration_ms=self._cleanup_duration_agg.snapshot(),
            error_counts=MappingProxyType(dict(self._error_counts)),
            retained_session_count=len(self._recent_sessions),
            max_retained_sessions=self._max_retained_sessions,
            recent_sessions=tuple(self._recent_sessions),
        )
