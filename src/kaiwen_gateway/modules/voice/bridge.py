"""Bidirectional in-process audio bridge for voice streaming sessions.

Invariants:
- One call, one isolated session: session holds private queues and tasks.
- Concurrently supervised pumps: inbound (telephony -> AI) and outbound (AI -> telephony).
- Strictly bounded media queues: backpressure applied on full queue; zero silent drops.
- Precise task ownership: supervisor manages pump lifecycles; cancels upon error or close.
- Idempotent termination: close() can be called multiple times safely.
- Zero conversion to JSON: raw AudioFrame instances are streamed directly.
"""

from __future__ import annotations

import asyncio
import math
import time
from collections import deque
from enum import StrEnum
from types import MappingProxyType, TracebackType
from typing import Final, Self

from kaiwen_gateway.modules.voice.domain import (
    AudioFrame,
    FutureTurnAudioFrameError,
    InvalidAudioFrameError,
    MediaBackpressureOverflowError,
    OutOfOrderAudioFrameError,
    UnsupportedInterruptionCapabilityError,
    VoiceBridgeSessionClosedError,
    VoiceDomainError,
)
from kaiwen_gateway.modules.voice.ports import (
    AIRealtimeMediaPort,
    InterruptibleAIPort,
    InterruptibleTelephonyPort,
    InterruptionOutcome,
    TelephonyMediaPort,
)
from kaiwen_gateway.modules.voice.telemetry import (
    DEFAULT_LATENCY_WINDOW_SIZE,
    BoundedLatencyAggregator,
    LatencyDistributionSummary,
    MetricAvailabilityReason,
    VoiceErrorCategory,
    VoiceSessionMetricsSnapshot,
    VoiceTelemetrySink,
    VoiceTerminalCleanupState,
    classify_voice_error,
)

__all__ = [
    "VoiceBridgeState",
    "VoiceBridgeSession",
]


class VoiceBridgeState(StrEnum):
    """Lifecycle state of a VoiceBridgeSession."""

    IDLE = "idle"
    RUNNING = "running"
    CLOSING = "closing"
    CLOSED = "closed"
    FAILED = "failed"


class VoiceBridgeSession:
    """Supervised bidirectional media bridge between a telephony port and an AI realtime port.

    Orchestrates two independent concurrent pumps:
    1. Inbound pump: TelephonyMediaPort -> bounded queue -> AIRealtimeMediaPort.
    2. Outbound pump: AIRealtimeMediaPort -> bounded queue -> TelephonyMediaPort.
    """

    def __init__(
        self,
        *,
        call_id: str,
        tenant_id: str,
        telephony_port: TelephonyMediaPort,
        ai_port: AIRealtimeMediaPort,
        queue_capacity: int = 8,
        backpressure_timeout: float | None = None,
        strict_sequence: bool = False,
        telemetry_sink: VoiceTelemetrySink | None = None,
        latency_window_capacity: int = DEFAULT_LATENCY_WINDOW_SIZE,
        min_percentile_samples: int = 1,
    ) -> None:
        if not isinstance(call_id, str) or not call_id.strip():
            raise VoiceDomainError("call_id must be a non-empty string")
        if not isinstance(tenant_id, str) or not tenant_id.strip():
            raise VoiceDomainError("tenant_id must be a non-empty string")
        if (
            isinstance(queue_capacity, bool)
            or not isinstance(queue_capacity, int)
            or queue_capacity < 1
        ):
            raise VoiceDomainError("queue_capacity must be an integer >= 1")
        if backpressure_timeout is not None:
            if isinstance(backpressure_timeout, bool) or not isinstance(
                backpressure_timeout, (int, float)
            ):
                raise VoiceDomainError(
                    "backpressure_timeout must be a positive finite float or None"
                )
            timeout_float = float(backpressure_timeout)
            if not math.isfinite(timeout_float) or timeout_float <= 0.0:
                raise VoiceDomainError(
                    "backpressure_timeout must be a positive finite float or None"
                )
        if telemetry_sink is not None and not isinstance(telemetry_sink, VoiceTelemetrySink):
            raise VoiceDomainError(
                "telemetry_sink must satisfy VoiceTelemetrySink protocol or be None"
            )

        self.call_id: Final[str] = call_id.strip()
        self.tenant_id: Final[str] = tenant_id.strip()
        self._telephony_port: Final[TelephonyMediaPort] = telephony_port
        self._ai_port: Final[AIRealtimeMediaPort] = ai_port
        self._queue_capacity: Final[int] = queue_capacity
        self._backpressure_timeout: Final[float | None] = (
            float(backpressure_timeout) if backpressure_timeout is not None else None
        )
        self._strict_sequence: Final[bool] = bool(strict_sequence)
        self._telemetry_sink: Final[VoiceTelemetrySink | None] = telemetry_sink
        self._latency_window_capacity: Final[int] = latency_window_capacity

        # Bounded latency aggregators (validate window_capacity & min_percentile_samples upfront)
        self._inbound_processing_latency = BoundedLatencyAggregator(
            window_capacity=latency_window_capacity,
            min_percentile_samples=min_percentile_samples,
        )
        self._outbound_processing_latency = BoundedLatencyAggregator(
            window_capacity=latency_window_capacity,
            min_percentile_samples=min_percentile_samples,
        )
        self._inbound_queue_wait_latency = BoundedLatencyAggregator(
            window_capacity=latency_window_capacity,
            min_percentile_samples=min_percentile_samples,
        )
        self._outbound_queue_wait_latency = BoundedLatencyAggregator(
            window_capacity=latency_window_capacity,
            min_percentile_samples=min_percentile_samples,
        )
        self._network_transport_latency = BoundedLatencyAggregator(
            window_capacity=latency_window_capacity,
            min_percentile_samples=min_percentile_samples,
        )

        # State management
        self._state: VoiceBridgeState = VoiceBridgeState.IDLE
        self._cleanup_state: VoiceTerminalCleanupState = VoiceTerminalCleanupState.NOT_STARTED
        self._error: BaseException | None = None
        self._closing_event: asyncio.Event = asyncio.Event()
        self._closed_event: asyncio.Event = asyncio.Event()
        self._cleanup_task: asyncio.Task[None] | None = None
        self._is_cleaned_up: bool = False
        self._started_monotonic: float | None = None
        self._session_duration_ms: float | None = None
        self._cleanup_duration_ms: float | None = None

        # Bounded per-direction queues
        self._inbound_queue: asyncio.Queue[AudioFrame | None] = asyncio.Queue(
            maxsize=self._queue_capacity
        )
        self._outbound_queue: asyncio.Queue[AudioFrame | None] = asyncio.Queue(
            maxsize=self._queue_capacity
        )
        self._inbound_timing_queue: deque[tuple[int, float, float, float]] = deque(
            maxlen=self._queue_capacity
        )
        self._outbound_timing_queue: deque[tuple[int, float, float, float]] = deque(
            maxlen=self._queue_capacity
        )
        self._inbound_queue_hwm: int = 0
        self._outbound_queue_hwm: int = 0
        self._sink_closed_notified: bool = False

        # Task tracking
        self._inbound_task: asyncio.Task[None] | None = None
        self._outbound_task: asyncio.Task[None] | None = None
        self._supervisor_task: asyncio.Task[None] | None = None

        # Counters and progression tracking
        self._inbound_frames_received: int = 0
        self._inbound_frames_transferred: int = 0
        self._inbound_bytes_received: int = 0
        self._inbound_bytes_transferred: int = 0
        self._outbound_frames_received: int = 0
        self._outbound_frames_transferred: int = 0
        self._outbound_bytes_received: int = 0
        self._outbound_bytes_sent: int = 0
        self._unforwarded_inbound_frames: int = 0
        self._unforwarded_outbound_frames: int = 0
        self._inbound_completed: bool = False
        self._outbound_completed: bool = False
        self._expected_inbound_seq: int = 0
        self._expected_outbound_seq: int = 0

        # Interruption and turn control tracking
        self._current_turn_id: int = 1
        self._interruptions_requested: int = 0
        self._interruption_count: int = 0
        self._interruptions_noop: int = 0
        self._interruptions_failed: int = 0
        self._stale_frames_suppressed: int = 0
        self._locally_discarded_frames: int = 0
        self._interruption_lock: asyncio.Lock = asyncio.Lock()

        # Interruption fence and partial provider state (Option A: retryable partial interruption)
        self._fenced_turn_id: int = 0
        self._pending_turn_id: int | None = None
        self._ai_cancelled_for_turn: int | None = None
        self._telephony_cleared_for_turn: int | None = None
        self._locally_discarded_for_turn: int = 0

        # Error classification & telemetry sink diagnostics
        self._error_counts: dict[VoiceErrorCategory, int] = {}
        self._telemetry_sink_errors: int = 0

    @property
    def state(self) -> VoiceBridgeState:
        """Current lifecycle state of the bridge session."""
        return self._state

    @property
    def cleanup_state(self) -> VoiceTerminalCleanupState:
        """Current or terminal resource cleanup state of the bridge session."""
        return self._cleanup_state

    @property
    def error(self) -> BaseException | None:
        """Exception that caused the bridge to fail, if any."""
        return self._error

    @property
    def inbound_queue_depth(self) -> int:
        """Current number of buffered audio frames in the inbound queue."""
        return self._inbound_queue.qsize()

    @property
    def outbound_queue_depth(self) -> int:
        """Current number of buffered audio frames in the outbound queue."""
        return self._outbound_queue.qsize()

    @property
    def inbound_queue_high_water_mark(self) -> int:
        """Peak number of buffered audio frames observed in the inbound queue."""
        self._update_inbound_hwm()
        return self._inbound_queue_hwm

    @property
    def outbound_queue_high_water_mark(self) -> int:
        """Peak number of buffered audio frames observed in the outbound queue."""
        self._update_outbound_hwm()
        return self._outbound_queue_hwm

    @property
    def inbound_frames_received(self) -> int:
        """Total audio frames received from telephony port."""
        return self._inbound_frames_received

    @property
    def inbound_frames_transferred(self) -> int:
        """Total audio frames successfully transferred from telephony to AI."""
        return self._inbound_frames_transferred

    @property
    def inbound_bytes_received(self) -> int:
        """Total audio payload bytes received from telephony port."""
        return self._inbound_bytes_received

    @property
    def inbound_bytes_transferred(self) -> int:
        """Total audio payload bytes transferred to AI port."""
        return self._inbound_bytes_transferred

    @property
    def outbound_frames_received(self) -> int:
        """Total audio frames received from AI port."""
        return self._outbound_frames_received

    @property
    def outbound_frames_transferred(self) -> int:
        """Total audio frames successfully transferred from AI to telephony."""
        return self._outbound_frames_transferred

    @property
    def outbound_bytes_received(self) -> int:
        """Total audio payload bytes received from AI port."""
        return self._outbound_bytes_received

    @property
    def outbound_bytes_sent(self) -> int:
        """Total audio payload bytes sent to telephony port."""
        return self._outbound_bytes_sent

    @property
    def dropped_frames_total(self) -> int:
        """Total audio frames dropped (interruption-discarded + unforwarded on close)."""
        return (
            self._locally_discarded_frames
            + self._unforwarded_inbound_frames
            + self._unforwarded_outbound_frames
        )

    @property
    def inbound_completed(self) -> bool:
        """Return True if the inbound media leg completed cleanly on EOF."""
        return self._inbound_completed

    @property
    def outbound_completed(self) -> bool:
        """Return True if the outbound media leg completed cleanly on EOF."""
        return self._outbound_completed

    @property
    def current_turn_id(self) -> int:
        """Current logical AI response generation / turn index (starts at 1)."""
        return self._current_turn_id

    @property
    def interruptions_requested(self) -> int:
        """Total number of interruption attempts issued on this active session."""
        return self._interruptions_requested

    @property
    def interruption_count(self) -> int:
        """Total number of successful interruptions performed during this session."""
        return self._interruption_count

    @property
    def interruptions_noop(self) -> int:
        """Total number of idempotent no-op interruptions for already-superseded turns."""
        return self._interruptions_noop

    @property
    def interruptions_failed(self) -> int:
        """Total number of failed interruption attempts during this session."""
        return self._interruptions_failed

    @property
    def stale_frames_suppressed(self) -> int:
        """Total frames from superseded turns suppressed by the outbound pump."""
        return self._stale_frames_suppressed

    @property
    def locally_discarded_frames(self) -> int:
        """Total buffered frames dropped from the outbound queue during interruptions."""
        return self._locally_discarded_frames

    @property
    def fenced_turn_id(self) -> int:
        """Highest turn index for which local output has been fenced and suppressed."""
        return self._fenced_turn_id

    @property
    def telemetry_sink_errors(self) -> int:
        """Count of best-effort telemetry sink callback errors caught and suppressed."""
        return self._telemetry_sink_errors

    def _record_error(self, category: VoiceErrorCategory) -> None:
        """Increment counter for a safe VoiceErrorCategory."""
        self._error_counts[category] = self._error_counts.get(category, 0) + 1

    @staticmethod
    def _count_queue_frames(queue: asyncio.Queue[AudioFrame | None]) -> int:
        """Return count of AudioFrame items currently buffered in queue (excluding None EOF)."""
        raw_q = getattr(queue, "_queue", None)
        if raw_q is not None:
            return sum(1 for item in raw_q if isinstance(item, AudioFrame))
        return queue.qsize()

    @staticmethod
    def _drain_queue_frames(queue: asyncio.Queue[AudioFrame | None]) -> int:
        """Drain all buffered items from queue during terminal cleanup, returning AudioFrame count."""
        frame_count = 0
        while not queue.empty():
            try:
                item = queue.get_nowait()
            except asyncio.QueueEmpty:
                break
            queue.task_done()
            if isinstance(item, AudioFrame):
                frame_count += 1
        return frame_count

    def _update_inbound_hwm(self) -> None:
        """Update inbound queue high-water mark from current buffered frame count."""
        depth = self._count_queue_frames(self._inbound_queue)
        if depth > self._inbound_queue_hwm:
            self._inbound_queue_hwm = depth

    def _update_outbound_hwm(self) -> None:
        """Update outbound queue high-water mark from current buffered frame count."""
        depth = self._count_queue_frames(self._outbound_queue)
        if depth > self._outbound_queue_hwm:
            self._outbound_queue_hwm = depth

    def metrics_snapshot(self) -> VoiceSessionMetricsSnapshot:
        """Return an immutable, privacy-safe snapshot of current session telemetry."""
        self._update_inbound_hwm()
        self._update_outbound_hwm()

        if self._session_duration_ms is not None:
            duration_ms: float | None = self._session_duration_ms
        elif self._started_monotonic is not None:
            duration_ms = max(0.0, (time.perf_counter() - self._started_monotonic) * 1000.0)
        else:
            duration_ms = None

        terminal_err_cat: VoiceErrorCategory | None = None
        if self._state == VoiceBridgeState.FAILED and self._error is not None:
            terminal_err_cat = classify_voice_error(
                self._error,
                during_cleanup=(
                    self._cleanup_state == VoiceTerminalCleanupState.COMPLETED_WITH_ERRORS
                    and not self._error_counts
                ),
            )

        return VoiceSessionMetricsSnapshot(
            session_state=self._state.value,
            cleanup_state=self._cleanup_state,
            session_duration_ms=duration_ms,
            cleanup_duration_ms=self._cleanup_duration_ms,
            inbound_frames_received=self._inbound_frames_received,
            inbound_frames_transferred=self._inbound_frames_transferred,
            inbound_bytes_received=self._inbound_bytes_received,
            inbound_bytes_transferred=self._inbound_bytes_transferred,
            outbound_frames_received=self._outbound_frames_received,
            outbound_frames_sent=self._outbound_frames_transferred,
            outbound_bytes_received=self._outbound_bytes_received,
            outbound_bytes_sent=self._outbound_bytes_sent,
            dropped_frames_total=self.dropped_frames_total,
            locally_discarded_frames=self._locally_discarded_frames,
            unforwarded_inbound_frames=self._unforwarded_inbound_frames,
            unforwarded_outbound_frames=self._unforwarded_outbound_frames,
            stale_frames_suppressed=self._stale_frames_suppressed,
            current_turn_id=self._current_turn_id,
            fenced_turn_id=self._fenced_turn_id,
            interruptions_requested=self._interruptions_requested,
            interruptions_completed=self._interruption_count,
            interruptions_noop=self._interruptions_noop,
            interruptions_failed=self._interruptions_failed,
            queue_capacity=self._queue_capacity,
            inbound_queue_depth=self._inbound_queue.qsize(),
            inbound_queue_high_water_mark=self._inbound_queue_hwm,
            outbound_queue_depth=self._outbound_queue.qsize(),
            outbound_queue_high_water_mark=self._outbound_queue_hwm,
            inbound_processing_ms=self._inbound_processing_latency.snapshot(),
            outbound_processing_ms=self._outbound_processing_latency.snapshot(),
            inbound_queue_wait_ms=self._inbound_queue_wait_latency.snapshot(),
            outbound_queue_wait_ms=self._outbound_queue_wait_latency.snapshot(),
            network_transport_ms=self._network_transport_latency.snapshot(),
            upstream_ai_generation_ms=LatencyDistributionSummary.unavailable(
                MetricAvailabilityReason.UNKNOWN_WITHOUT_PROVIDER_INSTRUMENTATION,
                window_capacity=self._latency_window_capacity,
            ),
            remote_playback_latency_ms=LatencyDistributionSummary.unavailable(
                MetricAvailabilityReason.UNKNOWN_WITHOUT_PLAYBACK_ACKNOWLEDGMENT,
                window_capacity=self._latency_window_capacity,
            ),
            error_counts=MappingProxyType(dict(self._error_counts)),
            terminal_error_category=terminal_err_cat,
            telemetry_sink_errors=self._telemetry_sink_errors,
        )

    def _is_outbound_frame_stale(self, frame: AudioFrame) -> bool:
        """Determine if an outbound audio frame belongs to a superseded or fenced turn."""
        if frame.turn_id is not None:
            return frame.turn_id <= self._fenced_turn_id or frame.turn_id < self._current_turn_id
        # Untagged frame: stale if any turn has been fenced or superseded
        return self._fenced_turn_id > 0 or self._current_turn_id > 1

    def _purge_outbound_queue(self, target_turn: int) -> int:
        """Purge buffered audio frames from the outbound queue matching or preceding target_turn.

        Preserves None EOF sentinels, FIFO timing correlation, and exact queue task accounting.
        """
        self._update_outbound_hwm()
        discarded_count = 0
        preserved_items: list[AudioFrame | None] = []
        preserved_timings: list[tuple[int, float, float, float]] = []
        while not self._outbound_queue.empty():
            try:
                item = self._outbound_queue.get_nowait()
            except asyncio.QueueEmpty:
                break
            self._outbound_queue.task_done()
            if item is None:
                preserved_items.append(None)
                continue

            matched_timing: tuple[int, float, float, float] | None = None
            if self._outbound_timing_queue and self._outbound_timing_queue[0][0] == id(item):
                matched_timing = self._outbound_timing_queue.popleft()

            if item.turn_id is not None and item.turn_id > target_turn:
                preserved_items.append(item)
                if matched_timing is not None:
                    preserved_timings.append(matched_timing)
            else:
                discarded_count += 1

        for item in preserved_items:
            self._outbound_queue.put_nowait(item)

        self._outbound_timing_queue = deque(preserved_timings, maxlen=self._queue_capacity)

        self._locally_discarded_frames += discarded_count
        self._locally_discarded_for_turn += discarded_count
        return discarded_count

    async def interrupt(
        self,
        *,
        turn_id: int | None = None,
        require_future_generation_cancellation: bool = False,
        require_playback_clear: bool = False,
        timeout: float = 2.0,
    ) -> InterruptionOutcome:
        """Interrupt current AI playback, purge unsent output, and advance turn identity.

        Args:
            turn_id: Optional turn index to interrupt. If specified and less than
                current_turn_id, the request is treated as an idempotent no-op.
            require_future_generation_cancellation: If True, raises
                UnsupportedInterruptionCapabilityError if the AI port cannot cancel generation,
                or raises provider exception / TimeoutError on cancellation failure.
            require_playback_clear: If True, raises UnsupportedInterruptionCapabilityError
                if the telephony port cannot clear remote provider playback, or raises
                provider exception / TimeoutError on clear failure.
            timeout: Maximum seconds to wait for provider cancellation and playback clear.
                Must be a positive finite float. Defaults to 2.0s.

        Returns:
            InterruptionOutcome detailing discarded frames and provider actions.

        Raises:
            VoiceBridgeSessionClosedError: If session is closed, closing, or failed.
            VoiceDomainError: If turn_id or timeout is invalid or refers to an unreached future turn.
            UnsupportedInterruptionCapabilityError: If required capabilities are unsupported.
            TimeoutError: If a required provider operation times out.
        """
        if (
            self._state
            in (
                VoiceBridgeState.CLOSING,
                VoiceBridgeState.CLOSED,
                VoiceBridgeState.FAILED,
            )
            or self._closed_event.is_set()
        ):
            self._interruptions_requested += 1
            self._interruptions_failed += 1
            self._record_error(VoiceErrorCategory.PORT_CLOSED)
            raise VoiceBridgeSessionClosedError(
                f"Cannot interrupt VoiceBridgeSession in state {self._state.value}"
            )

        if isinstance(timeout, bool) or not isinstance(timeout, (int, float)):
            self._interruptions_requested += 1
            self._interruptions_failed += 1
            self._record_error(VoiceErrorCategory.INVALID_FRAME)
            raise VoiceDomainError("timeout must be a positive finite float")
        timeout_float = float(timeout)
        if not math.isfinite(timeout_float) or timeout_float <= 0.0:
            self._interruptions_requested += 1
            self._interruptions_failed += 1
            self._record_error(VoiceErrorCategory.INVALID_FRAME)
            raise VoiceDomainError("timeout must be a positive finite float")

        if turn_id is not None:
            if not isinstance(turn_id, int) or isinstance(turn_id, bool):
                self._interruptions_requested += 1
                self._interruptions_failed += 1
                self._record_error(VoiceErrorCategory.INVALID_FRAME)
                raise VoiceDomainError(f"turn_id must be int or None, got {type(turn_id).__name__}")
            if turn_id < 0:
                self._interruptions_requested += 1
                self._interruptions_failed += 1
                self._record_error(VoiceErrorCategory.INVALID_FRAME)
                raise VoiceDomainError(f"turn_id must be non-negative, got {turn_id}")
            if turn_id > self._current_turn_id:
                self._interruptions_requested += 1
                self._interruptions_failed += 1
                self._record_error(VoiceErrorCategory.FUTURE_TURN_FRAME)
                raise VoiceDomainError(
                    f"Cannot interrupt future turn {turn_id} (current turn is {self._current_turn_id})"
                )
            if turn_id < self._current_turn_id:
                # Turn already interrupted / superseded: idempotent return
                self._interruptions_requested += 1
                self._interruptions_noop += 1
                return InterruptionOutcome(
                    call_id=self.call_id,
                    interrupted_turn_id=turn_id,
                    new_turn_id=self._current_turn_id,
                    locally_discarded_frames=0,
                    stale_frames_suppressed=self._stale_frames_suppressed,
                    future_generation_cancelled=False,
                    transport_pending_cancelled=False,
                    provider_playback_cleared=False,
                    playback_confirmed=False,
                )

        async with self._interruption_lock:
            if (
                self._state
                in (
                    VoiceBridgeState.CLOSING,
                    VoiceBridgeState.CLOSED,
                    VoiceBridgeState.FAILED,
                )
                or self._closed_event.is_set()
            ):
                self._interruptions_requested += 1
                self._interruptions_failed += 1
                self._record_error(VoiceErrorCategory.PORT_CLOSED)
                raise VoiceBridgeSessionClosedError(
                    f"Cannot interrupt VoiceBridgeSession in state {self._state.value}"
                )

            if turn_id is not None:
                if turn_id > self._current_turn_id:
                    self._interruptions_requested += 1
                    self._interruptions_failed += 1
                    self._record_error(VoiceErrorCategory.FUTURE_TURN_FRAME)
                    raise VoiceDomainError(
                        f"Cannot interrupt future turn {turn_id} (current turn is {self._current_turn_id})"
                    )
                if turn_id < self._current_turn_id:
                    self._interruptions_requested += 1
                    self._interruptions_noop += 1
                    return InterruptionOutcome(
                        call_id=self.call_id,
                        interrupted_turn_id=turn_id,
                        new_turn_id=self._current_turn_id,
                        locally_discarded_frames=0,
                        stale_frames_suppressed=self._stale_frames_suppressed,
                        future_generation_cancelled=False,
                        transport_pending_cancelled=False,
                        provider_playback_cleared=False,
                        playback_confirmed=False,
                    )

            self._interruptions_requested += 1

            # Check capability requirements before executing provider operations
            if require_future_generation_cancellation:
                if (
                    not isinstance(self._ai_port, InterruptibleAIPort)
                    or not self._ai_port.interruption_capabilities.can_cancel_future_generation
                ):
                    self._interruptions_failed += 1
                    self._record_error(VoiceErrorCategory.UNSUPPORTED_CAPABILITY)
                    raise UnsupportedInterruptionCapabilityError(
                        "AI port does not support future generation cancellation"
                    )

            if require_playback_clear:
                if (
                    not isinstance(self._telephony_port, InterruptibleTelephonyPort)
                    or not self._telephony_port.interruption_capabilities.can_clear_provider_playback
                ):
                    self._interruptions_failed += 1
                    self._record_error(VoiceErrorCategory.UNSUPPORTED_CAPABILITY)
                    raise UnsupportedInterruptionCapabilityError(
                        "Telephony port does not support clearing provider playback"
                    )

            target_turn = self._current_turn_id

            # Immediate local output fence and outbound queue purge
            self._fenced_turn_id = max(self._fenced_turn_id, target_turn)
            self._pending_turn_id = target_turn
            self._purge_outbound_queue(target_turn)

            deadline = asyncio.get_running_loop().time() + timeout_float

            # 1. Cancel upstream generation with bounded shared deadline and partial state reuse
            future_cancelled = False
            if isinstance(self._ai_port, InterruptibleAIPort):
                if self._ai_port.interruption_capabilities.can_cancel_future_generation:
                    if self._ai_cancelled_for_turn == target_turn:
                        future_cancelled = True
                    else:
                        t_rem = max(0.0, deadline - asyncio.get_running_loop().time())
                        if t_rem <= 0.0:
                            self._interruptions_failed += 1
                            self._record_error(VoiceErrorCategory.INTERRUPTION_TIMEOUT)
                            raise TimeoutError(
                                f"Interruption timed out after {timeout_float}s before AI cancellation"
                            )
                        try:
                            async with asyncio.timeout(t_rem):
                                await self._ai_port.cancel_turn(target_turn)
                            self._ai_cancelled_for_turn = target_turn
                            future_cancelled = True
                        except asyncio.CancelledError:
                            self._interruptions_failed += 1
                            self._record_error(VoiceErrorCategory.SESSION_CANCELLED)
                            raise
                        except Exception as exc:
                            self._record_error(classify_voice_error(exc, during_interruption=True))
                            if require_future_generation_cancellation:
                                self._interruptions_failed += 1
                                raise
                            future_cancelled = False

            # Check close race after AI cancellation
            if (
                self._state
                in (
                    VoiceBridgeState.CLOSING,
                    VoiceBridgeState.CLOSED,
                    VoiceBridgeState.FAILED,
                )
                or self._closing_event.is_set()
            ):
                self._interruptions_failed += 1
                self._record_error(VoiceErrorCategory.PORT_CLOSED)
                raise VoiceBridgeSessionClosedError(
                    f"Cannot interrupt VoiceBridgeSession in state {self._state.value}"
                )

            # 2. Clear transport / provider pending playback with bounded shared deadline and partial state reuse
            transport_pending_cancelled = False
            provider_playback_cleared = False
            if isinstance(self._telephony_port, InterruptibleTelephonyPort):
                caps = self._telephony_port.interruption_capabilities
                if self._telephony_cleared_for_turn == target_turn:
                    if caps.can_cancel_transport_pending:
                        transport_pending_cancelled = True
                    if caps.can_clear_provider_playback:
                        provider_playback_cleared = True
                else:
                    t_rem = max(0.0, deadline - asyncio.get_running_loop().time())
                    if t_rem <= 0.0:
                        self._interruptions_failed += 1
                        self._record_error(VoiceErrorCategory.INTERRUPTION_TIMEOUT)
                        raise TimeoutError(
                            f"Interruption timed out after {timeout_float}s before telephony playback clear"
                        )
                    try:
                        async with asyncio.timeout(t_rem):
                            await self._telephony_port.clear_pending_output()
                        self._telephony_cleared_for_turn = target_turn
                        if caps.can_cancel_transport_pending:
                            transport_pending_cancelled = True
                        if caps.can_clear_provider_playback:
                            provider_playback_cleared = True
                    except asyncio.CancelledError:
                        self._interruptions_failed += 1
                        self._record_error(VoiceErrorCategory.SESSION_CANCELLED)
                        raise
                    except Exception as exc:
                        self._record_error(classify_voice_error(exc, during_interruption=True))
                        if require_playback_clear:
                            self._interruptions_failed += 1
                            raise

            # Check close race after telephony clear
            if (
                self._state
                in (
                    VoiceBridgeState.CLOSING,
                    VoiceBridgeState.CLOSED,
                    VoiceBridgeState.FAILED,
                )
                or self._closing_event.is_set()
            ):
                self._interruptions_failed += 1
                self._record_error(VoiceErrorCategory.PORT_CLOSED)
                raise VoiceBridgeSessionClosedError(
                    f"Cannot interrupt VoiceBridgeSession in state {self._state.value}"
                )

            # 3. Commit turn progression and clear turn partial state
            self._current_turn_id += 1
            self._interruption_count += 1

            total_discarded = self._locally_discarded_for_turn
            self._locally_discarded_for_turn = 0
            self._pending_turn_id = None
            self._ai_cancelled_for_turn = None
            self._telephony_cleared_for_turn = None

            return InterruptionOutcome(
                call_id=self.call_id,
                interrupted_turn_id=target_turn,
                new_turn_id=self._current_turn_id,
                locally_discarded_frames=total_discarded,
                stale_frames_suppressed=self._stale_frames_suppressed,
                future_generation_cancelled=future_cancelled,
                transport_pending_cancelled=transport_pending_cancelled,
                provider_playback_cleared=provider_playback_cleared,
                playback_confirmed=False,
            )

    async def start(self) -> None:
        """Start the bidirectional media pumps under task supervision."""
        if self._state != VoiceBridgeState.IDLE:
            raise VoiceBridgeSessionClosedError(
                f"Cannot start VoiceBridgeSession in state {self._state.value}"
            )

        self._started_monotonic = time.perf_counter()
        self._state = VoiceBridgeState.RUNNING
        if self._telemetry_sink is not None:
            try:
                self._telemetry_sink.on_session_started()
            except Exception:
                self._telemetry_sink_errors += 1

        self._inbound_task = asyncio.create_task(
            self._run_inbound_pump(),
            name=f"voice-inbound-{self.call_id}",
        )
        self._outbound_task = asyncio.create_task(
            self._run_outbound_pump(),
            name=f"voice-outbound-{self.call_id}",
        )
        self._supervisor_task = asyncio.create_task(
            self._run_supervisor(),
            name=f"voice-supervisor-{self.call_id}",
        )
        self._supervisor_task.add_done_callback(lambda _t: self._ensure_cleanup_task())

    async def wait_until_closed(self, *, propagate_error: bool = True) -> None:
        """Wait until the bridge session completes, fails, or is closed."""
        await self._closed_event.wait()
        if propagate_error and self._error is not None:
            raise self._error

    async def run(self) -> None:
        """Start the bridge and wait for completion."""
        await self.start()
        try:
            await self.wait_until_closed()
        except asyncio.CancelledError:
            if not self._closing_event.is_set():
                self._record_error(VoiceErrorCategory.SESSION_CANCELLED)
            cleanup_task = self._ensure_cleanup_task()
            if not cleanup_task.done():
                try:
                    await asyncio.shield(cleanup_task)
                except asyncio.CancelledError:
                    while not cleanup_task.done():
                        try:
                            await asyncio.shield(cleanup_task)
                        except asyncio.CancelledError:
                            await asyncio.sleep(0)
            raise

    def _ensure_cleanup_task(self) -> asyncio.Task[None]:
        """Ensure the single terminal cleanup task is scheduled and tracked."""
        if self._state not in (VoiceBridgeState.FAILED, VoiceBridgeState.CLOSED):
            self._state = VoiceBridgeState.CLOSING
        self._closing_event.set()
        if self._cleanup_task is None:
            self._cleanup_task = asyncio.create_task(
                self._execute_cleanup(),
                name=f"voice-cleanup-{self.call_id}",
            )
        return self._cleanup_task

    async def close(self) -> None:
        """Idempotently close the session, cancel pump tasks, and release port resources.

        Guarantees:
        - Cancellation safety: If the calling task is cancelled while awaiting close(),
          CancelledError is propagated to the caller, but the underlying cleanup task
          continues running to completion in the background without leaking resources
          or leaving _closed_event unset.
        - Idempotence: Multiple concurrent or sequential calls await the same cleanup
          task and do not duplicate teardown.
        """
        if self._closed_event.is_set():
            return

        cleanup_task = self._ensure_cleanup_task()
        await asyncio.shield(cleanup_task)

        current = asyncio.current_task()
        if (
            self._supervisor_task is not None
            and self._supervisor_task is not current
            and not self._supervisor_task.done()
        ):
            try:
                await self._supervisor_task
            except (asyncio.CancelledError, Exception):
                pass

    async def __aenter__(self) -> Self:
        await self.start()
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        await self.close()

    async def _execute_cleanup(self) -> None:
        """Perform terminal cleanup of pumps and ports exactly once.

        Guarantees:
        - Cancellation resilience: runs as a dedicated session-owned task, so caller
          cancellation does not abort media port release or leave _closed_event unset.
        - Exhaustive port teardown: both telephony and AI ports are attempted even
          if the first port raises an exception.
        - Observable failure: port close exceptions mark the session FAILED and are
          recorded in self._error without silently overwriting prior pump failures.
        """
        t_cleanup_start = time.perf_counter()
        self._cleanup_state = VoiceTerminalCleanupState.IN_PROGRESS
        self._closing_event.set()

        # 1. Cancel pump tasks if still running and ensure all pump exceptions are retrieved
        all_pumps = [t for t in (self._inbound_task, self._outbound_task) if t is not None]
        for t in all_pumps:
            if not t.done():
                t.cancel()
        if all_pumps:
            pump_results = await asyncio.gather(*all_pumps, return_exceptions=True)
            if self._error is None:
                for res in pump_results:
                    if isinstance(res, BaseException) and not isinstance(
                        res, asyncio.CancelledError
                    ):
                        self._error = res
                        self._record_error(classify_voice_error(res))
                        break

        # 2. Await supervisor task so all pump supervision is terminated
        if self._supervisor_task is not None and not self._supervisor_task.done():
            try:
                await self._supervisor_task
            except (asyncio.CancelledError, Exception):
                pass

        # 2b. Record high-water marks and drain any remaining buffered frames so closed
        # sessions never retain AudioFrame payload bytes in memory.
        self._update_inbound_hwm()
        self._update_outbound_hwm()
        drained_inbound = self._drain_queue_frames(self._inbound_queue)
        drained_outbound = self._drain_queue_frames(self._outbound_queue)
        self._inbound_timing_queue.clear()
        self._outbound_timing_queue.clear()

        self._unforwarded_inbound_frames += drained_inbound
        self._unforwarded_outbound_frames += drained_outbound

        # 3. Media port teardown: both ports MUST be attempted
        close_errors: list[Exception] = []

        try:
            await self._telephony_port.close()
        except Exception as exc:
            close_errors.append(exc)
            self._record_error(VoiceErrorCategory.CLEANUP_ERROR)

        try:
            await self._ai_port.close()
        except Exception as exc:
            close_errors.append(exc)
            self._record_error(VoiceErrorCategory.CLEANUP_ERROR)

        # 4. Determine terminal state and error
        if self._error is not None:
            # Prior pump or supervisor failure is preserved; cleanup failure does not overwrite it
            self._state = VoiceBridgeState.FAILED
            for err in close_errors:
                if hasattr(err, "__context__"):
                    err.__context__ = self._error
        elif close_errors:
            # No prior failure, but port cleanup failed -> record as terminal failure
            self._state = VoiceBridgeState.FAILED
            if len(close_errors) == 1:
                self._error = close_errors[0]
            else:
                self._error = ExceptionGroup("Multiple media port cleanup failures", close_errors)
        elif self._state == VoiceBridgeState.FAILED:
            self._state = VoiceBridgeState.FAILED
        else:
            self._state = VoiceBridgeState.CLOSED

        t_cleanup_end = time.perf_counter()
        self._cleanup_duration_ms = max(0.0, (t_cleanup_end - t_cleanup_start) * 1000.0)
        if self._started_monotonic is not None:
            self._session_duration_ms = max(0.0, (t_cleanup_end - self._started_monotonic) * 1000.0)

        self._cleanup_state = (
            VoiceTerminalCleanupState.COMPLETED_WITH_ERRORS
            if close_errors
            else VoiceTerminalCleanupState.COMPLETED_CLEAN
        )
        try:
            if self._telemetry_sink is not None and not self._sink_closed_notified:
                self._sink_closed_notified = True
                try:
                    self._telemetry_sink.on_session_closed(self.metrics_snapshot())
                except Exception:
                    self._telemetry_sink_errors += 1
        finally:
            self._is_cleaned_up = True
            self._closed_event.set()

    async def _run_supervisor(self) -> None:
        """Supervise the two pumps, handle early failure, and drive termination."""
        try:
            assert self._inbound_task is not None
            assert self._outbound_task is not None

            supervised_tasks: tuple[asyncio.Task[None], asyncio.Task[None]] = (
                self._inbound_task,
                self._outbound_task,
            )
            done, pending = await asyncio.wait(
                supervised_tasks,
                return_when=asyncio.FIRST_EXCEPTION,
            )

            # Retrieve all completed task exceptions without early-break truncation
            task_exceptions: dict[asyncio.Task[None], BaseException | None] = {}
            for t in supervised_tasks:
                if t in done:
                    if t.cancelled():
                        task_exceptions[t] = asyncio.CancelledError()
                    else:
                        task_exceptions[t] = t.exception()

            # If any completed task failed or was cancelled, cancel and await pending siblings
            has_failure = any(exc is not None for exc in task_exceptions.values())
            if has_failure and pending:
                for t in pending:
                    t.cancel()
                pending_results = await asyncio.gather(*pending, return_exceptions=True)
                for t, res in zip(pending, pending_results, strict=True):
                    if isinstance(res, BaseException):
                        task_exceptions[t] = res
                    else:
                        task_exceptions[t] = None

            # Deterministic primary-error selection:
            # 1. Real exceptions take strict precedence over cancellation, evaluated in
            #    deterministic order (inbound pump before outbound pump).
            failed_exc: BaseException | None = None
            for t in supervised_tasks:
                exc = task_exceptions.get(t)
                if exc is not None and not isinstance(exc, asyncio.CancelledError):
                    failed_exc = exc
                    break

            # 2. If no real exception occurred, check for unexpected cancellation
            if failed_exc is None:
                for t in supervised_tasks:
                    exc = task_exceptions.get(t)
                    if isinstance(exc, asyncio.CancelledError):
                        if not self._closing_event.is_set():
                            failed_exc = exc
                            self._record_error(VoiceErrorCategory.SESSION_CANCELLED)
                        break

            if failed_exc is not None:
                self._error = failed_exc
                self._state = VoiceBridgeState.FAILED
            else:
                if self._state not in (VoiceBridgeState.CLOSING, VoiceBridgeState.CLOSED):
                    self._state = VoiceBridgeState.CLOSED

            self._ensure_cleanup_task()
        except asyncio.CancelledError:
            if not self._closing_event.is_set():
                self._record_error(VoiceErrorCategory.SESSION_CANCELLED)
            for pump_task in (self._inbound_task, self._outbound_task):
                if pump_task is not None and not pump_task.done():
                    pump_task.cancel()
            self._ensure_cleanup_task()
            raise
        except Exception as exc:
            self._error = exc
            self._state = VoiceBridgeState.FAILED
            self._record_error(classify_voice_error(exc))
            self._ensure_cleanup_task()

    async def _run_inbound_pump(self) -> None:
        """Run the inbound media pump (Telephony -> AI)."""

        async def receiver() -> None:
            while not self._closing_event.is_set():
                frame = await self._telephony_port.receive_frame()
                t_received = time.perf_counter()
                if frame is None:
                    # Clean EOF from telephony media stream
                    await self._inbound_queue.put(None)
                    break

                if not isinstance(frame, AudioFrame):
                    raise InvalidAudioFrameError(
                        f"Expected AudioFrame from telephony port, got {type(frame).__name__}"
                    )

                self._inbound_frames_received += 1
                self._inbound_bytes_received += len(frame.payload)

                if self._strict_sequence:
                    if frame.sequence != self._expected_inbound_seq:
                        self._unforwarded_inbound_frames += 1
                        raise OutOfOrderAudioFrameError(
                            f"Inbound frame sequence mismatch: expected {self._expected_inbound_seq}, "
                            f"got {frame.sequence}"
                        )
                    self._expected_inbound_seq += 1

                t_pre_enqueue = time.perf_counter()
                try:
                    if self._backpressure_timeout is not None:
                        try:
                            await asyncio.wait_for(
                                self._inbound_queue.put(frame),
                                timeout=self._backpressure_timeout,
                            )
                        except TimeoutError as err:
                            raise MediaBackpressureOverflowError(
                                f"Inbound queue capacity {self._queue_capacity} exceeded "
                                f"and backpressure timed out after {self._backpressure_timeout}s"
                            ) from err
                    else:
                        await self._inbound_queue.put(frame)
                except (asyncio.CancelledError, Exception):
                    self._unforwarded_inbound_frames += 1
                    raise

                t_enqueued = time.perf_counter()
                self._inbound_timing_queue.append(
                    (id(frame), t_received, t_pre_enqueue, t_enqueued)
                )
                self._update_inbound_hwm()

        async def forwarder() -> None:
            while not self._closing_event.is_set():
                self._update_inbound_hwm()
                item = await self._inbound_queue.get()
                try:
                    t_dequeued = time.perf_counter()
                    if item is None:
                        break

                    t_received = t_dequeued
                    t_pre_enqueue = t_dequeued
                    t_enqueued = t_dequeued
                    while self._inbound_timing_queue:
                        entry_id, entry_recv, entry_pre_enq, entry_enq = (
                            self._inbound_timing_queue.popleft()
                        )
                        if entry_id == id(item):
                            t_received = entry_recv
                            t_pre_enqueue = entry_pre_enq
                            t_enqueued = entry_enq
                            break

                    try:
                        await self._ai_port.send_frame(item)
                    except (asyncio.CancelledError, Exception):
                        self._unforwarded_inbound_frames += 1
                        raise

                    t_forwarded = time.perf_counter()
                    self._inbound_frames_transferred += 1
                    self._inbound_bytes_transferred += len(item.payload)
                    self._inbound_queue_wait_latency.record(
                        max(0.0, (t_dequeued - t_enqueued) * 1000.0)
                    )
                    self._inbound_processing_latency.record(
                        max(
                            0.0,
                            ((t_pre_enqueue - t_received) + (t_forwarded - t_dequeued)) * 1000.0,
                        )
                    )
                finally:
                    self._inbound_queue.task_done()

        rec_task = asyncio.create_task(receiver())
        fwd_task = asyncio.create_task(forwarder())
        pump_tasks = (rec_task, fwd_task)
        try:
            done, pending = await asyncio.wait(
                pump_tasks,
                return_when=asyncio.FIRST_EXCEPTION,
            )
            for t in pending:
                t.cancel()
            if pending:
                await asyncio.gather(*pending, return_exceptions=True)

            task_exceptions: dict[asyncio.Task[None], BaseException | None] = {}
            for t in pump_tasks:
                if t in done:
                    if t.cancelled():
                        task_exceptions[t] = asyncio.CancelledError()
                    else:
                        task_exceptions[t] = t.exception()

            first_exc: BaseException | None = None
            # Prioritize real exceptions over cancellation in deterministic order (receiver, then forwarder)
            for t in pump_tasks:
                exc = task_exceptions.get(t)
                if exc is not None and not isinstance(exc, asyncio.CancelledError):
                    first_exc = exc
                    break

            if first_exc is None:
                for t in pump_tasks:
                    exc = task_exceptions.get(t)
                    if isinstance(exc, asyncio.CancelledError):
                        first_exc = exc
                        break

            if first_exc is not None:
                if not isinstance(first_exc, asyncio.CancelledError):
                    self._record_error(classify_voice_error(first_exc))
                raise first_exc
            self._inbound_completed = True
        finally:
            for t in pump_tasks:
                if not t.done():
                    t.cancel()
            await asyncio.gather(*pump_tasks, return_exceptions=True)

    async def _run_outbound_pump(self) -> None:
        """Run the outbound media pump (AI -> Telephony)."""

        async def receiver() -> None:
            while not self._closing_event.is_set():
                frame = await self._ai_port.receive_frame()
                t_received = time.perf_counter()
                if frame is None:
                    # Clean EOF from AI realtime media stream
                    await self._outbound_queue.put(None)
                    break

                if not isinstance(frame, AudioFrame):
                    raise InvalidAudioFrameError(
                        f"Expected AudioFrame from AI port, got {type(frame).__name__}"
                    )

                self._outbound_frames_received += 1
                self._outbound_bytes_received += len(frame.payload)

                if self._strict_sequence:
                    if frame.sequence != self._expected_outbound_seq:
                        self._unforwarded_outbound_frames += 1
                        raise OutOfOrderAudioFrameError(
                            f"Outbound frame sequence mismatch: expected {self._expected_outbound_seq}, "
                            f"got {frame.sequence}"
                        )
                    self._expected_outbound_seq += 1

                # Future turn rejection: fail-closed if AI emits frames for an unreached future turn
                if frame.turn_id is not None and frame.turn_id > self._current_turn_id:
                    self._unforwarded_outbound_frames += 1
                    raise FutureTurnAudioFrameError(
                        f"Received audio frame for future turn {frame.turn_id} "
                        f"(current active turn is {self._current_turn_id})"
                    )

                # Stale frame suppression: drop frames belonging to older turns,
                # currently fenced turns, or untagged frames received after interruption
                if self._is_outbound_frame_stale(frame):
                    self._stale_frames_suppressed += 1
                    continue

                t_pre_enqueue = time.perf_counter()
                try:
                    if self._backpressure_timeout is not None:
                        try:
                            await asyncio.wait_for(
                                self._outbound_queue.put(frame),
                                timeout=self._backpressure_timeout,
                            )
                        except TimeoutError as err:
                            raise MediaBackpressureOverflowError(
                                f"Outbound queue capacity {self._queue_capacity} exceeded "
                                f"and backpressure timed out after {self._backpressure_timeout}s"
                            ) from err
                    else:
                        await self._outbound_queue.put(frame)
                except (asyncio.CancelledError, Exception):
                    self._unforwarded_outbound_frames += 1
                    raise

                t_enqueued = time.perf_counter()
                self._outbound_timing_queue.append(
                    (id(frame), t_received, t_pre_enqueue, t_enqueued)
                )
                self._update_outbound_hwm()

        async def forwarder() -> None:
            while not self._closing_event.is_set():
                self._update_outbound_hwm()
                item = await self._outbound_queue.get()
                try:
                    t_dequeued = time.perf_counter()
                    if item is None:
                        break

                    t_received = t_dequeued
                    t_pre_enqueue = t_dequeued
                    t_enqueued = t_dequeued
                    while self._outbound_timing_queue:
                        entry_id, entry_recv, entry_pre_enq, entry_enq = (
                            self._outbound_timing_queue.popleft()
                        )
                        if entry_id == id(item):
                            t_received = entry_recv
                            t_pre_enqueue = entry_pre_enq
                            t_enqueued = entry_enq
                            break

                    # Future turn rejection: fail-closed if queue item references an unreached future turn
                    if item.turn_id is not None and item.turn_id > self._current_turn_id:
                        self._unforwarded_outbound_frames += 1
                        raise FutureTurnAudioFrameError(
                            f"Outbound queue item references future turn {item.turn_id} "
                            f"(current active turn is {self._current_turn_id})"
                        )

                    # Stale frame suppression: drop frames belonging to older turns,
                    # currently fenced turns, or untagged frames popped after interruption
                    if self._is_outbound_frame_stale(item):
                        self._stale_frames_suppressed += 1
                        continue

                    try:
                        await self._telephony_port.send_frame(item)
                    except (asyncio.CancelledError, Exception):
                        self._unforwarded_outbound_frames += 1
                        raise

                    t_forwarded = time.perf_counter()
                    self._outbound_frames_transferred += 1
                    self._outbound_bytes_sent += len(item.payload)

                    net_ms: object = None
                    try:
                        consume_fn = getattr(
                            self._telephony_port, "consume_last_send_duration_ms", None
                        )
                        if callable(consume_fn):
                            net_ms = consume_fn(item)
                    except Exception:
                        self._telemetry_sink_errors += 1
                        net_ms = None

                    send_total_ms = max(0.0, (t_forwarded - t_dequeued) * 1000.0)
                    if (
                        isinstance(net_ms, (int, float))
                        and not isinstance(net_ms, bool)
                        and math.isfinite(net_ms)
                        and net_ms >= 0.0
                    ):
                        net_float = float(net_ms)
                        self._network_transport_latency.record(net_float)
                        send_processing_ms = max(0.0, send_total_ms - net_float)
                    else:
                        if net_ms is not None:
                            self._telemetry_sink_errors += 1
                        send_processing_ms = send_total_ms

                    self._outbound_queue_wait_latency.record(
                        max(0.0, (t_dequeued - t_enqueued) * 1000.0)
                    )
                    self._outbound_processing_latency.record(
                        max(0.0, (t_pre_enqueue - t_received) * 1000.0 + send_processing_ms)
                    )
                finally:
                    self._outbound_queue.task_done()

        rec_task = asyncio.create_task(receiver())
        fwd_task = asyncio.create_task(forwarder())
        pump_tasks = (rec_task, fwd_task)
        try:
            done, pending = await asyncio.wait(
                pump_tasks,
                return_when=asyncio.FIRST_EXCEPTION,
            )
            for t in pending:
                t.cancel()
            if pending:
                await asyncio.gather(*pending, return_exceptions=True)

            task_exceptions: dict[asyncio.Task[None], BaseException | None] = {}
            for t in pump_tasks:
                if t in done:
                    if t.cancelled():
                        task_exceptions[t] = asyncio.CancelledError()
                    else:
                        task_exceptions[t] = t.exception()

            first_exc: BaseException | None = None
            # Prioritize real exceptions over cancellation in deterministic order (receiver, then forwarder)
            for t in pump_tasks:
                exc = task_exceptions.get(t)
                if exc is not None and not isinstance(exc, asyncio.CancelledError):
                    first_exc = exc
                    break

            if first_exc is None:
                for t in pump_tasks:
                    exc = task_exceptions.get(t)
                    if isinstance(exc, asyncio.CancelledError):
                        first_exc = exc
                        break

            if first_exc is not None:
                if not isinstance(first_exc, asyncio.CancelledError):
                    self._record_error(classify_voice_error(first_exc))
                raise first_exc
            self._outbound_completed = True
        finally:
            for t in pump_tasks:
                if not t.done():
                    t.cancel()
            await asyncio.gather(*pump_tasks, return_exceptions=True)
