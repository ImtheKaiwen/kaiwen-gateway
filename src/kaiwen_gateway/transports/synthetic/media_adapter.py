"""WebSocket telephony media adapter connecting ASGI WebSocket to TelephonyMediaPort.

Invariants:
- Implements TelephonyMediaPort: bridges WebSocket binary frames into AudioFrame instances.
- Single read-loop safety: frames are ingested from a dedicated WebSocket reader task.
- Serialized outbound transport: all writes (binary media and control text) use a shared send lock.
- Bounded buffering: inbound queue is strictly bounded with configurable backpressure timeout.
- Observable metrics: tracks frame/byte counters and queue depth without logging raw audio bytes.
- Idempotent termination: close() cleanly unblocks receivers and marks adapter terminal.
"""

from __future__ import annotations

import asyncio
import math
from typing import Any, Final

from kaiwen_gateway.core.identity import VerifiedConnectionContext
from kaiwen_gateway.modules.voice.domain import (
    AudioFrame,
    MediaBackpressureOverflowError,
    MediaPortClosedError,
    VoiceDomainError,
)
from kaiwen_gateway.modules.voice.ports import TelephonyMediaPort

__all__ = [
    "WebSocketTelephonyMediaAdapter",
]

_CLOSE_SENTINEL: Final[object] = object()


class WebSocketTelephonyMediaAdapter(TelephonyMediaPort):
    """TelephonyMediaPort adapter wrapping an admitted ASGI WebSocket connection."""

    def __init__(
        self,
        websocket: Any,
        context: VerifiedConnectionContext,
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

        self._websocket: Final[Any] = websocket
        self._context: Final[VerifiedConnectionContext] = context
        self._queue_capacity: Final[int] = queue_capacity
        self._backpressure_timeout: Final[float | None] = (
            float(backpressure_timeout) if backpressure_timeout is not None else None
        )

        self._inbound_queue: asyncio.Queue[object] = asyncio.Queue(maxsize=self._queue_capacity)
        self._send_lock: asyncio.Lock = asyncio.Lock()
        self._is_closed: bool = False
        self._close_event: asyncio.Event = asyncio.Event()

        # Observable telemetry counters
        self._frames_received: int = 0
        self._frames_sent: int = 0
        self._bytes_received: int = 0
        self._bytes_sent: int = 0
        self._reject_count: int = 0

    @property
    def is_closed(self) -> bool:
        """Return True if this telephony media adapter has been closed."""
        return self._is_closed

    @property
    def context(self) -> VerifiedConnectionContext:
        """Verified connection context associated with this leg."""
        return self._context

    @property
    def frames_received(self) -> int:
        """Total number of frames received via receive_frame (inbound from WebSocket)."""
        return self._frames_received

    @property
    def frames_sent(self) -> int:
        """Total number of frames transmitted to WebSocket via send_frame."""
        return self._frames_sent

    @property
    def bytes_received(self) -> int:
        """Total audio bytes ingested from the client."""
        return self._bytes_received

    @property
    def bytes_sent(self) -> int:
        """Total audio bytes transmitted to the client."""
        return self._bytes_sent

    @property
    def reject_count(self) -> int:
        """Number of rejected invalid frames or messages."""
        return self._reject_count

    @property
    def inbound_queue_depth(self) -> int:
        """Current number of audio frames waiting in the inbound buffer."""
        return self._inbound_queue.qsize()

    def record_reject(self) -> None:
        """Increment count of rejected frames or malformed messages."""
        self._reject_count += 1

    async def push_inbound_frame(self, frame: AudioFrame) -> None:
        """Push an inbound AudioFrame into the bounded queue from the ingress reader loop.

        Args:
            frame: Validated AudioFrame parsed from a binary WebSocket message.

        Raises:
            MediaPortClosedError: If this adapter is closed.
            MediaBackpressureOverflowError: If the inbound queue is full and backpressure expires.
        """
        if self._is_closed:
            raise MediaPortClosedError("WebSocketTelephonyMediaAdapter is closed")

        try:
            if self._backpressure_timeout is not None:
                async with asyncio.timeout(self._backpressure_timeout):
                    await self._inbound_queue.put(frame)
            else:
                await self._inbound_queue.put(frame)
        except TimeoutError as exc:
            raise MediaBackpressureOverflowError(
                f"Inbound media buffer overflowed (capacity={self._queue_capacity}) "
                f"after {self._backpressure_timeout}s backpressure timeout"
            ) from exc

    def push_inbound_eof(self) -> None:
        """Signal clean EOF to the inbound pump."""
        if not self._is_closed:
            try:
                self._inbound_queue.put_nowait(None)
            except asyncio.QueueFull:
                pass

    async def receive_frame(self) -> AudioFrame | None:
        """Receive the next audio frame from telephony (called by VoiceBridgeSession inbound pump).

        Returns:
            The next AudioFrame received, or None on clean EOF.

        Raises:
            MediaPortClosedError: If the port is closed.
        """
        if self._is_closed and self._inbound_queue.empty():
            raise MediaPortClosedError("WebSocketTelephonyMediaAdapter is closed")

        item = await self._inbound_queue.get()
        if item is _CLOSE_SENTINEL:
            raise MediaPortClosedError("WebSocketTelephonyMediaAdapter is closed")

        if item is None:
            return None

        assert isinstance(item, AudioFrame)
        self._frames_received += 1
        self._bytes_received += len(item.payload)
        return item

    async def send_frame(self, frame: AudioFrame) -> None:
        """Send an audio frame to the client over WebSocket as a binary message.

        Synchronized via send_lock to guarantee frame ordering and avoid interleaving.

        Args:
            frame: Validated AudioFrame from the outbound bridge pump.

        Raises:
            MediaPortClosedError: If the port is closed.
        """
        if self._is_closed:
            raise MediaPortClosedError("WebSocketTelephonyMediaAdapter is closed")

        async with self._send_lock:
            if self._is_closed:
                raise MediaPortClosedError("WebSocketTelephonyMediaAdapter is closed")
            try:
                await self._websocket.send_bytes(frame.payload)
            except Exception as exc:
                self._is_closed = True
                raise MediaPortClosedError("WebSocket connection closed or write failed") from exc

        self._frames_sent += 1
        self._bytes_sent += len(frame.payload)

    async def send_control_text(self, text: str) -> None:
        """Send a JSON control message over WebSocket synchronized with media writes.

        Args:
            text: UTF-8 JSON text string.

        Raises:
            MediaPortClosedError: If the port is closed.
        """
        if self._is_closed:
            raise MediaPortClosedError("WebSocketTelephonyMediaAdapter is closed")

        async with self._send_lock:
            if self._is_closed:
                raise MediaPortClosedError("WebSocketTelephonyMediaAdapter is closed")
            try:
                await self._websocket.send_text(text)
            except Exception as exc:
                self._is_closed = True
                raise MediaPortClosedError("WebSocket connection closed or write failed") from exc

    async def close(self) -> None:
        """Idempotently close the adapter and release resources."""
        if not self._is_closed:
            self._is_closed = True
            self._close_event.set()
            try:
                self._inbound_queue.put_nowait(_CLOSE_SENTINEL)
            except asyncio.QueueFull:
                pass
