"""Transport adapters for Kaiwen Gateway."""

from __future__ import annotations

from kaiwen_gateway.transports.telephony import (
    InvalidCallStateTransitionError,
    TelephonyCallEndedEvent,
    TelephonyCallStartedEvent,
    TelephonyCallState,
    TelephonyControlEvent,
    TelephonyControlPort,
    TelephonyError,
    UnsupportedOperationError,
)

__all__ = [
    "InvalidCallStateTransitionError",
    "TelephonyCallEndedEvent",
    "TelephonyCallStartedEvent",
    "TelephonyCallState",
    "TelephonyControlEvent",
    "TelephonyControlPort",
    "TelephonyError",
    "UnsupportedOperationError",
]
