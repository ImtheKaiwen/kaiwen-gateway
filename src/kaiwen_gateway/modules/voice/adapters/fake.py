"""Deterministic fake adapters for telephony and AI realtime media ports.

Invariants:
- Deterministic behavior: fully controlled via queue injection, delays, and error triggers.
- Safe termination: close() unblocks any pending receive and marks the adapter closed.
- Strict contracts: sending or receiving after close raises MediaPortClosedError.
- Observable state: tracks sent frames, close counts, and frame counters for tests.
"""

from __future__ import annotations

import asyncio
from typing import Final

from kaiwen_gateway.modules.voice.domain import (
    AudioFrame,
    MediaBackpressureOverflowError,
    MediaPortClosedError,
)
from kaiwen_gateway.modules.voice.ports import (
    AIRealtimeMediaPort,
    InterruptibleTelephonyPort,
    InterruptionCapabilities,
    TelephonyMediaPort,
)

__all__ = [
    "FakeTelephonyMediaAdapter",
    "FakeAIMediaAdapter",
    "InterruptibleFakeTelephonyAdapter",
]

_CLOSE_SENTINEL: Final[object] = object()


class _BaseFakeMediaAdapter:
    """Base implementation for deterministic fake media adapters."""

    def __init__(
        self,
        *,
        name: str,
        queue_capacity: int | None = None,
    ) -> None:
        self.name: Final[str] = name
        self._queue_capacity: Final[int | None] = queue_capacity
        self._inbound_queue: asyncio.Queue[object] = (
            asyncio.Queue(maxsize=queue_capacity)
            if queue_capacity is not None and queue_capacity > 0
            else asyncio.Queue()
        )
        self._pending_sent: asyncio.Queue[AudioFrame] = (
            asyncio.Queue(maxsize=queue_capacity)
            if queue_capacity is not None and queue_capacity > 0
            else asyncio.Queue()
        )
        self._sent_frames: list[AudioFrame] = []
        self._is_closed: bool = False
        self._close_count: int = 0
        self._close_event: asyncio.Event = asyncio.Event()

        # Delays for slow consumer/producer simulation
        self._send_delay: float = 0.0
        self._receive_delay: float = 0.0

        # Injected faults
        self._send_error: Exception | None = None
        self._send_error_after: int = 0
        self._receive_error: Exception | None = None
        self._receive_error_after: int = 0

        # Activity counters
        self._send_count: int = 0
        self._receive_count: int = 0

    @property
    def is_closed(self) -> bool:
        """Return True if this adapter has been closed."""
        return self._is_closed

    @property
    def close_count(self) -> int:
        """Number of times close() has been invoked."""
        return self._close_count

    @property
    def sent_frames(self) -> list[AudioFrame]:
        """List of frames sent through this adapter."""
        return list(self._sent_frames)

    @property
    def send_count(self) -> int:
        """Number of frames successfully sent."""
        return self._send_count

    @property
    def receive_count(self) -> int:
        """Number of frames received from receive_frame."""
        return self._receive_count

    @property
    def buffered_frame_count(self) -> int:
        """Return the count of currently buffered pending frames."""
        if self._queue_capacity is not None and self._queue_capacity > 0:
            return max(self._inbound_queue.qsize(), self._pending_sent.qsize())
        return self._inbound_queue.qsize()

    def push_frame(self, frame: AudioFrame) -> None:
        """Push an audio frame to be returned by receive_frame."""
        if self._is_closed:
            raise MediaPortClosedError(f"{self.name} port is closed")
        self._inbound_queue.put_nowait(frame)

    def push_eof(self) -> None:
        """Push EOF sentinel so receive_frame returns None."""
        if self._is_closed:
            raise MediaPortClosedError(f"{self.name} port is closed")
        self._inbound_queue.put_nowait(None)

    def set_send_delay(self, delay: float) -> None:
        """Configure delay in seconds during send_frame to simulate a slow consumer."""
        self._send_delay = max(0.0, delay)

    def set_receive_delay(self, delay: float) -> None:
        """Configure delay in seconds during receive_frame to simulate a slow producer."""
        self._receive_delay = max(0.0, delay)

    def inject_send_error(self, error: Exception, *, after_n_frames: int = 0) -> None:
        """Configure send_frame to raise error after after_n_frames frames."""
        self._send_error = error
        self._send_error_after = after_n_frames

    def inject_receive_error(self, error: Exception, *, after_n_frames: int = 0) -> None:
        """Configure receive_frame to raise error after after_n_frames frames."""
        self._receive_error = error
        self._receive_error_after = after_n_frames

    async def receive_frame(self) -> AudioFrame | None:
        """Receive the next frame, or None on EOF."""
        if self._is_closed:
            raise MediaPortClosedError(f"{self.name} port is closed")

        if self._receive_error is not None and self._receive_count >= self._receive_error_after:
            err = self._receive_error
            self._receive_error = None
            raise err

        if self._receive_delay > 0.0:
            await asyncio.sleep(self._receive_delay)

        item = await self._inbound_queue.get()
        if item is _CLOSE_SENTINEL:
            raise MediaPortClosedError(f"{self.name} port is closed")

        if item is None:
            return None

        self._receive_count += 1
        return item  # type: ignore[return-value]

    async def send_frame(self, frame: AudioFrame) -> None:
        """Send an audio frame through this adapter."""
        if self._is_closed:
            raise MediaPortClosedError(f"{self.name} port is closed")

        if self._send_error is not None and self._send_count >= self._send_error_after:
            err = self._send_error
            self._send_error = None
            raise err

        if self._queue_capacity is not None and self._queue_capacity > 0:
            if self._pending_sent.full():
                raise MediaBackpressureOverflowError(
                    f"{self.name} queue capacity exceeded ({self._queue_capacity})"
                )
            self._pending_sent.put_nowait(frame)

        if self._send_delay > 0.0:
            await asyncio.sleep(self._send_delay)

        self._sent_frames.append(frame)
        self._send_count += 1

    def drain_pending_sent(self) -> list[AudioFrame]:
        """Drain and return currently pending sent frames, freeing queue capacity."""
        drained: list[AudioFrame] = []
        while not self._pending_sent.empty():
            try:
                drained.append(self._pending_sent.get_nowait())
            except asyncio.QueueEmpty:
                break
        return drained

    async def close(self) -> None:
        """Close the adapter and unblock any waiting receivers."""
        self._close_count += 1
        if not self._is_closed:
            self._is_closed = True
            self._close_event.set()
            # Drain inbound and pending queues to guarantee room for sentinel
            while not self._inbound_queue.empty():
                try:
                    self._inbound_queue.get_nowait()
                except asyncio.QueueEmpty:
                    break
            while not self._pending_sent.empty():
                try:
                    self._pending_sent.get_nowait()
                except asyncio.QueueEmpty:
                    break
            try:
                self._inbound_queue.put_nowait(_CLOSE_SENTINEL)
            except asyncio.QueueFull:
                pass


class FakeTelephonyMediaAdapter(_BaseFakeMediaAdapter, TelephonyMediaPort):
    """Fake telephony media adapter for deterministic testing."""

    def __init__(self, *, name: str = "fake-telephony") -> None:
        super().__init__(name=name)


class FakeAIMediaAdapter(_BaseFakeMediaAdapter, AIRealtimeMediaPort):
    """Fake AI realtime media adapter for deterministic testing."""

    def __init__(
        self,
        *,
        name: str = "fake-ai",
        queue_capacity: int | None = None,
    ) -> None:
        super().__init__(name=name, queue_capacity=queue_capacity)


class InterruptibleFakeTelephonyAdapter(FakeTelephonyMediaAdapter, InterruptibleTelephonyPort):
    """Fake telephony adapter supporting interruption capabilities and clearing pending output."""

    def __init__(
        self,
        *,
        name: str = "interruptible-fake-telephony",
        capabilities: InterruptionCapabilities | None = None,
        simulate_clear_error: Exception | None = None,
    ) -> None:
        super().__init__(name=name)
        self._capabilities: Final[InterruptionCapabilities] = (
            capabilities
            if capabilities is not None
            else InterruptionCapabilities(
                can_cancel_transport_pending=True,
                can_clear_provider_playback=True,
                can_observe_playback_acks=False,
                cannot_confirm_playback=True,
            )
        )
        self._simulate_clear_error: Exception | None = simulate_clear_error
        self._clear_count: int = 0

    @property
    def interruption_capabilities(self) -> InterruptionCapabilities:
        """Return declared interruption capabilities for this telephony adapter."""
        return self._capabilities

    @property
    def clear_count(self) -> int:
        """Number of times clear_pending_output has been invoked."""
        return self._clear_count

    async def clear_pending_output(self) -> None:
        """Clear any buffered unplayed frames within the telephony transport adapter."""
        if self._is_closed:
            raise MediaPortClosedError(f"{self.name} port is closed")

        if self._simulate_clear_error is not None:
            err = self._simulate_clear_error
            self._simulate_clear_error = None
            raise err

        self._clear_count += 1
