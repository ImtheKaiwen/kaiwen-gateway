"""Provider-neutral media port contracts for voice streaming.

Invariants:
- Provider-neutral: zero concrete provider assumptions (no Verimor, Twilio, OpenAI SDK).
- Protocol-agnostic: media ports operate purely on validated AudioFrame domain objects.
- Clean EOF semantics: receive_frame returns None when the media stream ends cleanly.
- Bounded async contracts: non-blocking or cooperative await with cancellation safety.
- Idempotent cleanup: close() must release all underlying resources safely.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol, runtime_checkable

from kaiwen_gateway.modules.voice.domain import AudioFrame, VoiceDomainError

__all__ = [
    "TelephonyMediaPort",
    "AIRealtimeMediaPort",
    "InterruptionCapabilities",
    "InterruptionOutcome",
    "InterruptibleAIPort",
    "InterruptibleTelephonyPort",
]


@dataclass(frozen=True, slots=True)
class InterruptionCapabilities:
    """Interruption and playback control capabilities of a media port or provider adapter.

    Distinguishes independent capability boundaries across the voice streaming pipeline:
    - can_cancel_future_generation: Upstream AI backend can cancel in-flight or future response generation.
    - can_discard_local_output: Gateway can drop unsent audio frames from its in-memory queue.
    - can_cancel_transport_pending: Telephony transport adapter can cancel unwritten transport frames.
    - can_clear_provider_playback: Remote telephony/PSTN provider supports clearing its active playback buffer.
    - can_observe_playback_acks: Remote provider reports explicit frame playback acknowledgments.
    - cannot_confirm_playback: True if the adapter cannot verify whether transmitted audio was actually heard.
    """

    can_cancel_future_generation: bool = False
    can_discard_local_output: bool = True
    can_cancel_transport_pending: bool = False
    can_clear_provider_playback: bool = False
    can_observe_playback_acks: bool = False
    cannot_confirm_playback: bool = True

    def __post_init__(self) -> None:
        if self.can_observe_playback_acks and self.cannot_confirm_playback:
            raise VoiceDomainError(
                "cannot_confirm_playback cannot be True when can_observe_playback_acks is True"
            )
        if not self.can_observe_playback_acks and not self.cannot_confirm_playback:
            raise VoiceDomainError(
                "cannot_confirm_playback must be True when can_observe_playback_acks is False"
            )


@dataclass(frozen=True, slots=True)
class InterruptionOutcome:
    """Structured result of an interruption and turn-control request.

    Attributes:
        call_id: Identifier of the call session.
        interrupted_turn_id: The turn generation index that was interrupted.
        new_turn_id: The newly established turn generation index for subsequent output.
        locally_discarded_frames: Count of buffered frames dropped from Gateway-owned queues.
        stale_frames_suppressed: Count of frames from superseded turns suppressed by the pump.
        future_generation_cancelled: Whether the upstream AI backend confirmed generation cancellation.
        transport_pending_cancelled: Whether transport-level pending output was purged.
        provider_playback_cleared: Whether remote provider playback buffers were cleared.
        playback_confirmed: True only if provider observed explicit playback acknowledgments.
    """

    call_id: str
    interrupted_turn_id: int
    new_turn_id: int
    locally_discarded_frames: int
    stale_frames_suppressed: int
    future_generation_cancelled: bool
    transport_pending_cancelled: bool
    provider_playback_cleared: bool
    playback_confirmed: bool = False


@runtime_checkable
class InterruptibleAIPort(Protocol):
    """Optional protocol for AI realtime ports supporting turn cancellation."""

    @property
    def interruption_capabilities(self) -> InterruptionCapabilities:
        """Return the interruption capabilities of this AI port."""
        ...

    async def cancel_turn(self, turn_id: int) -> None:
        """Cancel the specified turn generation on the AI backend.

        Args:
            turn_id: Monotonically increasing generation index to cancel.
        """
        ...


@runtime_checkable
class InterruptibleTelephonyPort(Protocol):
    """Optional protocol for Telephony ports supporting playback buffer clearing."""

    @property
    def interruption_capabilities(self) -> InterruptionCapabilities:
        """Return the interruption capabilities of this telephony port."""
        ...

    async def clear_pending_output(self) -> None:
        """Clear any buffered unplayed frames within the telephony transport adapter."""
        ...


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
