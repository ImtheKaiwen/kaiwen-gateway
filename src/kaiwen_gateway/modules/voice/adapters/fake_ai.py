"""Bounded Fake AI response adapter for voice streaming sessions.

Invariants:
- Provider-neutral: implements AIRealtimeMediaPort for voice bridge integration.
- Bounded buffering: output frames are buffered in a strictly bounded queue.
- Backpressure support: applies bounded wait on full queue; fails closed on timeout.
- Zero unlimited accumulation: does not store unbounded lists of frames in memory.
- Deterministic response: echoes received audio frames with matching format and sequence.
- Isolated instance lifecycle: each call creates an independent adapter instance.
"""

from __future__ import annotations

import asyncio
import math
from collections import deque
from typing import Final

from kaiwen_gateway.modules.voice.adapters.fake import FakeAIMediaAdapter
from kaiwen_gateway.modules.voice.domain import (
    AudioFormat,
    AudioFrame,
    MediaBackpressureOverflowError,
    MediaPortClosedError,
    VoiceDomainError,
)
from kaiwen_gateway.modules.voice.ports import (
    AIRealtimeMediaPort,
    InterruptibleAIPort,
    InterruptionCapabilities,
)

__all__ = [
    "BoundedFakeAIEchoPort",
    "BoundedInterruptibleFakeAIEchoPort",
    "InterruptibleFakeAIPort",
]

_CLOSE_SENTINEL: Final[object] = object()


class BoundedFakeAIEchoPort(AIRealtimeMediaPort):
    """Bounded, deterministic echo port implementing AIRealtimeMediaPort.

    Simulates an AI realtime backend by echoing inbound audio frames back to the
    outbound leg with strictly bounded queue depth and backpressure enforcement.
    """

    def __init__(
        self,
        *,
        queue_capacity: int = 8,
        backpressure_timeout: float | None = 2.0,
    ) -> None:
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

        self._queue_capacity: Final[int] = queue_capacity
        self._backpressure_timeout: Final[float | None] = (
            float(backpressure_timeout) if backpressure_timeout is not None else None
        )

        self._output_queue: asyncio.Queue[object] = asyncio.Queue(maxsize=self._queue_capacity)
        self._is_closed: bool = False
        self._close_event: asyncio.Event = asyncio.Event()

        # Bounded metrics (no raw payload storage)
        self._frames_received: int = 0
        self._frames_sent: int = 0
        self._bytes_received: int = 0
        self._bytes_sent: int = 0

    @property
    def is_closed(self) -> bool:
        """Return True if this AI realtime port is closed."""
        return self._is_closed

    @property
    def frames_received(self) -> int:
        """Total number of frames received via send_frame (inbound from telephony)."""
        return self._frames_received

    @property
    def frames_sent(self) -> int:
        """Total number of frames yielded via receive_frame (outbound to telephony)."""
        return self._frames_sent

    @property
    def bytes_received(self) -> int:
        """Total byte count received from telephony."""
        return self._bytes_received

    @property
    def bytes_sent(self) -> int:
        """Total byte count emitted to telephony."""
        return self._bytes_sent

    @property
    def queue_depth(self) -> int:
        """Current number of frames buffered in the bounded echo queue."""
        return self._output_queue.qsize()

    async def send_frame(self, frame: AudioFrame) -> None:
        """Receive an audio frame from the inbound bridge pump and enqueue an echo response.

        Args:
            frame: Validated AudioFrame received from telephony.

        Raises:
            MediaPortClosedError: If this port is closed.
            MediaBackpressureOverflowError: If the bounded output queue is full and timeout expires.
        """
        if self._is_closed:
            raise MediaPortClosedError("BoundedFakeAIEchoPort is closed")

        # Deterministically echo the frame
        echo_frame = AudioFrame(
            payload=frame.payload,
            format=frame.format
            if isinstance(frame.format, AudioFormat)
            else AudioFormat(str(frame.format)),
            sample_rate=frame.sample_rate,
            channels=frame.channels,
            sequence=frame.sequence,
            timestamp_ms=frame.timestamp_ms,
            turn_id=frame.turn_id,
        )

        # Enqueue with bounded backpressure
        try:
            if self._backpressure_timeout is not None:
                async with asyncio.timeout(self._backpressure_timeout):
                    await self._output_queue.put(echo_frame)
            else:
                await self._output_queue.put(echo_frame)
        except TimeoutError as exc:
            raise MediaBackpressureOverflowError(
                f"Echo output queue overflowed (capacity={self._queue_capacity}) "
                f"after {self._backpressure_timeout}s backpressure timeout"
            ) from exc

        self._frames_received += 1
        self._bytes_received += len(frame.payload)

    async def receive_frame(self) -> AudioFrame | None:
        """Receive the next echoed audio frame to be sent to telephony by the outbound pump.

        Returns:
            The next AudioFrame, or None on clean EOF.

        Raises:
            MediaPortClosedError: If this port is closed.
        """
        if self._is_closed and self._output_queue.empty():
            raise MediaPortClosedError("BoundedFakeAIEchoPort is closed")

        item = await self._output_queue.get()
        if item is _CLOSE_SENTINEL:
            raise MediaPortClosedError("BoundedFakeAIEchoPort is closed")

        if item is None:
            return None

        assert isinstance(item, AudioFrame)
        self._frames_sent += 1
        self._bytes_sent += len(item.payload)
        return item

    async def push_eof(self, *, timeout: float | None = 2.0) -> None:
        """Signal EOF to the outbound pump, waiting if the output queue is full.

        Guarantees:
        - FIFO order preserved: EOF is appended after all currently queued echo frames.
        - Zero frame loss: queued echo frames are preserved.
        - Bounded wait: fails closed with MediaBackpressureOverflowError on timeout.
        """
        if self._is_closed:
            return
        try:
            effective_timeout = timeout if timeout is not None else self._backpressure_timeout
            if effective_timeout is not None:
                async with asyncio.timeout(effective_timeout):
                    await self._output_queue.put(None)
            else:
                await self._output_queue.put(None)
        except TimeoutError as exc:
            raise MediaBackpressureOverflowError(
                f"Timed out after {effective_timeout}s waiting to enqueue AI output EOF into full queue"
            ) from exc

    def push_eof_nowait(self) -> bool:
        """Attempt to signal EOF immediately without blocking.

        Returns True if EOF was enqueued, False if the queue was full or closed.
        """
        if self._is_closed:
            return False
        try:
            self._output_queue.put_nowait(None)
            return True
        except asyncio.QueueFull:
            return False

    async def close(self) -> None:
        """Idempotently close the port and wake any pending receivers."""
        if not self._is_closed:
            self._is_closed = True
            self._close_event.set()
            # Unblock any receiver awaiting queue.get()
            try:
                self._output_queue.put_nowait(_CLOSE_SENTINEL)
            except asyncio.QueueFull:
                pass


class BoundedInterruptibleFakeAIEchoPort(BoundedFakeAIEchoPort, InterruptibleAIPort):
    """Bounded, deterministic echo port implementing both AIRealtimeMediaPort and InterruptibleAIPort.

    Combines the bounded queue and backpressure semantics of BoundedFakeAIEchoPort
    with turn cancellation and interruption capabilities for media endpoints.
    """

    def __init__(
        self,
        *,
        queue_capacity: int = 8,
        backpressure_timeout: float | None = 2.0,
        capabilities: InterruptionCapabilities | None = None,
        simulate_cancellation_error: Exception | None = None,
    ) -> None:
        super().__init__(
            queue_capacity=queue_capacity,
            backpressure_timeout=backpressure_timeout,
        )
        self._capabilities: Final[InterruptionCapabilities] = (
            capabilities
            if capabilities is not None
            else InterruptionCapabilities(
                can_cancel_future_generation=True,
                can_discard_local_output=True,
            )
        )
        self._simulate_cancellation_error: Exception | None = simulate_cancellation_error
        self._cancelled_high_water: int = 0
        self._recent_cancelled_turns: deque[int] = deque(maxlen=64)

    @property
    def interruption_capabilities(self) -> InterruptionCapabilities:
        """Return declared interruption capabilities for this AI port."""
        return self._capabilities

    @property
    def cancelled_turns(self) -> list[int]:
        """List of recent turn IDs that have been cancelled via cancel_turn (bounded)."""
        return list(self._recent_cancelled_turns)

    @property
    def cancelled_turn_high_water(self) -> int:
        """Monotonic high-water mark of cancelled turns."""
        return self._cancelled_high_water

    async def cancel_turn(self, turn_id: int) -> None:
        """Cancel future generation for the specified turn and purge unconsumed echo frames."""
        if self._is_closed:
            raise MediaPortClosedError("BoundedInterruptibleFakeAIEchoPort is closed")

        if self._simulate_cancellation_error is not None:
            err = self._simulate_cancellation_error
            self._simulate_cancellation_error = None
            raise err

        if turn_id > self._cancelled_high_water:
            self._cancelled_high_water = turn_id
        self._recent_cancelled_turns.append(turn_id)

        # Purge unconsumed echo frames for this turn (and prior cancelled turns) from the bounded output queue
        remaining: list[object] = []
        while not self._output_queue.empty():
            try:
                item = self._output_queue.get_nowait()
                if (
                    item is not None
                    and item is not _CLOSE_SENTINEL
                    and isinstance(item, AudioFrame)
                ):
                    if item.turn_id is not None and item.turn_id <= self._cancelled_high_water:
                        continue  # Discard frame matching cancelled turns
                remaining.append(item)
            except asyncio.QueueEmpty:
                break

        for item in remaining:
            self._output_queue.put_nowait(item)

    async def send_frame(self, frame: AudioFrame) -> None:
        """Receive an inbound frame from telephony and enqueue an echo response.

        Frames belonging to an already cancelled turn are dropped immediately.
        """
        if self._is_closed:
            raise MediaPortClosedError("BoundedInterruptibleFakeAIEchoPort is closed")

        if frame.turn_id is not None and frame.turn_id <= self._cancelled_high_water:
            return

        await super().send_frame(frame)

    async def receive_frame(self) -> AudioFrame | None:
        """Receive the next echoed audio frame, discarding any frames from cancelled turns.

        Guarantees that frames enqueued by producers unblocked concurrently during
        turn cancellation cannot escape to the consumer.
        """
        while True:
            frame = await super().receive_frame()
            if frame is None:
                return None
            if frame.turn_id is not None and frame.turn_id <= self._cancelled_high_water:
                continue
            return frame


class InterruptibleFakeAIPort(FakeAIMediaAdapter, InterruptibleAIPort):
    """Fake AI realtime media adapter supporting turn cancellation and tagged turn generation."""

    def __init__(
        self,
        *,
        name: str = "interruptible-fake-ai",
        capabilities: InterruptionCapabilities | None = None,
        simulate_cancellation_error: Exception | None = None,
        echo: bool = False,
        echo_turn_id: int = 1,
    ) -> None:
        super().__init__(name=name)
        self._capabilities: Final[InterruptionCapabilities] = (
            capabilities
            if capabilities is not None
            else InterruptionCapabilities(
                can_cancel_future_generation=True,
                can_discard_local_output=True,
            )
        )
        self._simulate_cancellation_error: Exception | None = simulate_cancellation_error
        self._cancelled_high_water: int = 0
        self._recent_cancelled_turns: deque[int] = deque(maxlen=64)
        self._echo: bool = bool(echo)
        self._echo_turn_id: int = int(echo_turn_id)

    @property
    def interruption_capabilities(self) -> InterruptionCapabilities:
        """Return declared interruption capabilities for this AI port."""
        return self._capabilities

    @property
    def cancelled_turns(self) -> list[int]:
        """List of recent turn IDs that have been cancelled via cancel_turn (bounded)."""
        return list(self._recent_cancelled_turns)

    @property
    def cancelled_turn_high_water(self) -> int:
        """Monotonic high-water mark of cancelled turns."""
        return self._cancelled_high_water

    def set_echo_turn_id(self, turn_id: int) -> None:
        """Configure turn_id for subsequent echoed frames when echo=True."""
        if not isinstance(turn_id, int) or isinstance(turn_id, bool) or turn_id < 0:
            raise VoiceDomainError(f"echo_turn_id must be a non-negative int, got {turn_id}")
        self._echo_turn_id = turn_id

    async def cancel_turn(self, turn_id: int) -> None:
        """Cancel future generation for the specified turn on the simulated AI backend."""
        if self._is_closed:
            raise MediaPortClosedError(f"{self.name} port is closed")

        if self._simulate_cancellation_error is not None:
            err = self._simulate_cancellation_error
            self._simulate_cancellation_error = None
            raise err

        if turn_id > self._cancelled_high_water:
            self._cancelled_high_water = turn_id
        self._recent_cancelled_turns.append(turn_id)

    async def send_frame(self, frame: AudioFrame) -> None:
        """Receive an inbound frame from telephony and optionally echo it tagged with turn_id."""
        await super().send_frame(frame)
        if self._echo and not self._is_closed:
            echo_frame = AudioFrame(
                payload=frame.payload,
                format=frame.format
                if isinstance(frame.format, AudioFormat)
                else AudioFormat(str(frame.format)),
                sample_rate=frame.sample_rate,
                channels=frame.channels,
                sequence=frame.sequence,
                timestamp_ms=frame.timestamp_ms,
                turn_id=self._echo_turn_id,
            )
            self.push_frame(echo_frame)
