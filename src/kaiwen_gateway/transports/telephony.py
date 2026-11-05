"""Provider-neutral telephony control contracts and domain event models.

Invariants:
- Provider-neutral: zero provider-specific wire framing assumptions (no Verimor, Twilio, etc.).
- Immutable identity: events are bound to server-verified tenant_id, provider_id, and call_id.
- Explicit call state: telephony call leg state is distinct from transport WSS state or Core lifecycle state.
- Bounded and typed: all control events are typed, immutable dataclasses.
- Capability honesty: unsupported telephony operations must fail closed with UnsupportedOperationError.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from enum import StrEnum
from types import MappingProxyType
from typing import Any, Protocol, runtime_checkable

__all__ = [
    "TelephonyCallState",
    "TelephonyControlEvent",
    "TelephonyCallStartedEvent",
    "TelephonyCallEndedEvent",
    "TelephonyControlPort",
    "TelephonyError",
    "InvalidCallStateTransitionError",
    "UnsupportedOperationError",
]


class TelephonyCallState(StrEnum):
    """Lifecycle state of the external telephony call leg."""

    IDLE = "idle"
    CONNECTED = "connected"
    DISCONNECTED = "disconnected"
    FAILED = "failed"


class TelephonyError(Exception):
    """Base exception for provider-neutral telephony domain errors."""


class InvalidCallStateTransitionError(TelephonyError):
    """Raised when an operation is requested that violates call state lifecycle invariants."""

    def __init__(self, current_state: TelephonyCallState, attempted_action: str) -> None:
        super().__init__(
            f"Invalid telephony call transition: cannot perform {attempted_action!r} "
            f"from state {current_state.value!r}"
        )
        self.current_state = current_state
        self.attempted_action = attempted_action


class UnsupportedOperationError(TelephonyError):
    """Raised when an unsupported telephony feature (e.g. transfer, dtmf) is requested."""

    def __init__(self, operation: str, *, provider: str = "synthetic") -> None:
        super().__init__(
            f"Telephony operation {operation!r} is not supported by {provider!r} adapter"
        )
        self.operation = operation
        self.provider = provider


@dataclass(frozen=True, slots=True)
class TelephonyControlEvent:
    """Base immutable provider-neutral telephony control event."""

    event_type: str
    tenant_id: str
    provider_id: str
    call_id: str
    timestamp: float


@dataclass(frozen=True, slots=True)
class TelephonyCallStartedEvent(TelephonyControlEvent):
    """Emitted when an inbound telephony call leg has started and connected."""

    stream_id: str | None = None
    metadata: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "metadata",
            MappingProxyType(dict(self.metadata)),
        )


@dataclass(frozen=True, slots=True)
class TelephonyCallEndedEvent(TelephonyControlEvent):
    """Emitted when an inbound telephony call leg has ended."""

    reason: str = "normal"
    code: int | None = None


@runtime_checkable
class TelephonyControlPort(Protocol):
    """Port for provider-neutral telephony control state and capabilities."""

    @property
    def call_state(self) -> TelephonyCallState:
        """Current lifecycle state of the logical telephony call leg."""
        ...

    @property
    def supported_capabilities(self) -> frozenset[str]:
        """Set of capabilities explicitly supported by this telephony adapter."""
        ...

    def is_capability_supported(self, capability: str) -> bool:
        """Check if a specific telephony capability is supported."""
        ...
