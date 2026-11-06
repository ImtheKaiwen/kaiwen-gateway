"""Provider-neutral media port contracts for voice streaming.

Invariants:
- Provider-neutral: zero concrete provider assumptions (no Verimor, Twilio, OpenAI SDK).
- Protocol-agnostic: media ports operate purely on validated AudioFrame domain objects.
- Clean EOF semantics: receive_frame returns None when the media stream ends cleanly.
- Bounded async contracts: non-blocking or cooperative await with cancellation safety.
- Idempotent cleanup: close() must release all underlying resources safely.
"""

from __future__ import annotations

from typing import Protocol, runtime_checkable

from kaiwen_gateway.modules.voice.domain import AudioFrame

__all__ = [
    "TelephonyMediaPort",
    "AIRealtimeMediaPort",
]


@runtime_checkable
class TelephonyMediaPort(Protocol):
    """Port for receiving and sending telephony audio frames."""

    @property
    def is_closed(self) -> bool:
        """Return True if this telephony media port has been closed."""
        ...

    async def receive_frame(self) -> AudioFrame | None:
        """Receive the next audio frame from the telephony media leg.

        Returns:
            The next AudioFrame received from telephony, or None on clean EOF / stream termination.

        Raises:
            MediaPortClosedError: If the port is already closed.
            MediaPortError: If an underlying I/O or transport error occurs.
        """
        ...

    async def send_frame(self, frame: AudioFrame) -> None:
        """Send an audio frame to the telephony media leg.

        Args:
            frame: Validated AudioFrame to transmit.

        Raises:
            MediaPortClosedError: If the port is already closed.
            MediaPortError: If an underlying I/O or transport error occurs.
        """
        ...

    async def close(self) -> None:
        """Idempotently close the telephony media port and release resources."""
        ...


@runtime_checkable
class AIRealtimeMediaPort(Protocol):
    """Port for receiving and sending AI realtime audio frames."""

    @property
    def is_closed(self) -> bool:
        """Return True if this AI realtime media port has been closed."""
        ...

    async def receive_frame(self) -> AudioFrame | None:
        """Receive the next audio frame from the AI realtime backend.

        Returns:
            The next AudioFrame received from AI, or None on clean EOF / stream termination.

        Raises:
            MediaPortClosedError: If the port is already closed.
            MediaPortError: If an underlying I/O or transport error occurs.
        """
        ...

    async def send_frame(self, frame: AudioFrame) -> None:
        """Send an audio frame to the AI realtime backend.

        Args:
            frame: Validated AudioFrame to transmit.

        Raises:
            MediaPortClosedError: If the port is already closed.
            MediaPortError: If an underlying I/O or transport error occurs.
        """
        ...

    async def close(self) -> None:
        """Idempotently close the AI realtime media port and release resources."""
        ...
