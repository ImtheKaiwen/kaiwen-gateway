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
from typing import Final

from kaiwen_gateway.modules.voice.domain import (
    AudioFormat,
    AudioFrame,
    MediaBackpressureOverflowError,
    MediaPortClosedError,
    VoiceDomainError,
)
from kaiwen_gateway.modules.voice.ports import AIRealtimeMediaPort

__all__ = [
    "BoundedFakeAIEchoPort",
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

    def push_eof(self) -> None:
        """Signal EOF to the outbound pump so receive_frame() returns None."""
        if not self._is_closed:
            try:
                self._output_queue.put_nowait(None)
            except asyncio.QueueFull:
                pass

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
