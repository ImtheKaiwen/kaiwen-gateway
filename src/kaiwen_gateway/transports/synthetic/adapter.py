"""Synthetic telephony control adapter implementation (v1).

Invariants:
- Provider-neutral translation: converts synthetic wire framing into normalized domain events.
- Immutable identity: binds verified tenant_id, provider_id, and call_id from Core security context.
- Verified isolation: caller wire payloads cannot modify or spoof verified identity fields.
- Explicit call state machine: validates and enforces strict IDLE -> CONNECTED -> DISCONNECTED transitions.
- Capability honesty: declares supported capabilities and strictly rejects unsupported operations (transfer, DTMF, hold).
- Zero media/AI dependencies: does not allocate AI sessions, audio buffers, or codecs.
"""

from __future__ import annotations

import time
from typing import Any, Final

from kaiwen_gateway.core.identity import VerifiedConnectionContext
from kaiwen_gateway.transports.synthetic.protocol import (
    EVENT_CALL_END,
    EVENT_CALL_ENDED,
    EVENT_CALL_START,
    EVENT_CALL_STARTED,
    InvalidMessageFormatError,
    UnsupportedEventError,
)
from kaiwen_gateway.transports.telephony import (
    InvalidCallStateTransitionError,
    TelephonyCallEndedEvent,
    TelephonyCallStartedEvent,
    TelephonyCallState,
    TelephonyControlEvent,
    TelephonyControlPort,
    UnsupportedOperationError,
)

__all__ = [
    "SyntheticTelephonyAdapter",
    "SUPPORTED_TELEPHONY_CAPABILITIES",
    "UNSUPPORTED_TELEPHONY_CAPABILITIES",
]

SUPPORTED_TELEPHONY_CAPABILITIES: Final[frozenset[str]] = frozenset(
    {
        "call_start",
        "call_end",
        "ping_pong",
    }
)

UNSUPPORTED_TELEPHONY_CAPABILITIES: Final[frozenset[str]] = frozenset(
    {
        "call_transfer",
        "dtmf",
        "hold",
        "audio_codec_negotiation",
        "barge_in_cancel",
        "voicemail",
        "conference",
    }
)


class SyntheticTelephonyAdapter(TelephonyControlPort):
    """Adapter translating synthetic WebSocket wire events into provider-neutral telephony control events."""

    __slots__ = (
        "_context",
        "_call_state",
        "_stream_id",
        "_last_event",
    )

    def __init__(self, context: VerifiedConnectionContext) -> None:
        self._context: Final[VerifiedConnectionContext] = context
        self._call_state: TelephonyCallState = TelephonyCallState.IDLE
        self._stream_id: str | None = None
        self._last_event: TelephonyControlEvent | None = None

    @property
    def context(self) -> VerifiedConnectionContext:
        """Verified security context bound to this call session."""
        return self._context

    @property
    def tenant_id(self) -> str:
        """Verified tenant identifier."""
        return self._context.tenant_id

    @property
    def provider_id(self) -> str:
        """Verified provider identifier."""
        return self._context.provider_id

    @property
    def call_id(self) -> str:
        """Verified call identifier."""
        return self._context.call_id

    @property
    def call_state(self) -> TelephonyCallState:
        """Current lifecycle state of the telephony call leg."""
        return self._call_state

    @property
    def stream_id(self) -> str | None:
        """Associated stream identifier once call has started, if provided."""
        return self._stream_id

    @property
    def last_event(self) -> TelephonyControlEvent | None:
        """Most recent normalized telephony control event emitted by this adapter."""
        return self._last_event

    @property
    def supported_capabilities(self) -> frozenset[str]:
        """Declared supported capabilities of this synthetic adapter."""
        return SUPPORTED_TELEPHONY_CAPABILITIES

    def is_capability_supported(self, capability: str) -> bool:
        """Return True if capability is supported, False otherwise."""
        return capability in SUPPORTED_TELEPHONY_CAPABILITIES

    def execute_unsupported_operation(self, operation: str) -> None:
        """Fail closed when an unsupported telephony operation is requested."""
        raise UnsupportedOperationError(operation, provider="synthetic")

    def _verify_untrusted_identity_claims(self, payload: dict[str, Any]) -> None:
        """Verify that caller wire payload does not attempt to spoof or override verified context."""
        if "call_id" in payload and payload["call_id"] != self.call_id:
            raise InvalidMessageFormatError(
                "Client-supplied call_id cannot override verified call context"
            )
        if "tenant_id" in payload and payload["tenant_id"] != self.tenant_id:
            raise InvalidMessageFormatError(
                "Client-supplied tenant_id cannot override verified tenant context"
            )
        if "provider_id" in payload and payload["provider_id"] != self.provider_id:
            raise InvalidMessageFormatError(
                "Client-supplied provider_id cannot override verified provider context"
            )
        if "allowed_scopes" in payload:
            raise InvalidMessageFormatError(
                "Client-supplied allowed_scopes cannot override verified context"
            )

    def terminate_call_on_transport_close(
        self, reason: str = "transport_closed"
    ) -> TelephonyCallEndedEvent | None:
        """Explicitly terminate any active telephony call when the transport connection closes."""
        if self._call_state == TelephonyCallState.CONNECTED:
            self._call_state = TelephonyCallState.DISCONNECTED
            event = TelephonyCallEndedEvent(
                event_type="call_ended",
                tenant_id=self.tenant_id,
                provider_id=self.provider_id,
                call_id=self.call_id,
                timestamp=time.time(),
                reason=reason,
                code=None,
            )
            self._last_event = event
            return event
        return None

    def handle_wire_event(
        self, payload: dict[str, Any]
    ) -> tuple[TelephonyControlEvent | None, dict[str, Any]]:
        """Translate an inbound wire payload into a domain control event and outbound response frame.

        Returns:
            Tuple of (domain_event_or_none, wire_response_dict). For duplicate idempotent events,
            domain_event is None to prevent duplicate terminal event dispatch.

        Raises:
            InvalidMessageFormatError: If caller attempts to spoof verified context.
            InvalidCallStateTransitionError: If the event violates lifecycle sequence.
            UnsupportedOperationError: If an unsupported telephony operation is requested.
            UnsupportedEventError: If the event name is not recognized.
        """
        self._verify_untrusted_identity_claims(payload)
        event_name = payload.get("event")

        if event_name in UNSUPPORTED_TELEPHONY_CAPABILITIES:
            self.execute_unsupported_operation(event_name)

        now = time.time()

        if event_name == EVENT_CALL_START:
            if self._call_state != TelephonyCallState.IDLE:
                raise InvalidCallStateTransitionError(self._call_state, EVENT_CALL_START)

            stream_id = payload.get("stream_id")
            metadata = payload.get("metadata", {})
            self._stream_id = stream_id
            self._call_state = TelephonyCallState.CONNECTED

            start_event = TelephonyCallStartedEvent(
                event_type="call_started",
                tenant_id=self.tenant_id,
                provider_id=self.provider_id,
                call_id=self.call_id,
                timestamp=now,
                stream_id=stream_id,
                metadata=dict(metadata),
            )
            self._last_event = start_event

            start_response: dict[str, Any] = {
                "event": EVENT_CALL_STARTED,
                "call_id": self.call_id,
                "state": TelephonyCallState.CONNECTED.value,
            }
            if stream_id is not None:
                start_response["stream_id"] = stream_id

            return start_event, start_response

        if event_name == EVENT_CALL_END:
            reason = payload.get("reason", "caller_hangup")
            code = payload.get("code")

            if self._call_state == TelephonyCallState.CONNECTED:
                self._call_state = TelephonyCallState.DISCONNECTED
                end_event = TelephonyCallEndedEvent(
                    event_type="call_ended",
                    tenant_id=self.tenant_id,
                    provider_id=self.provider_id,
                    call_id=self.call_id,
                    timestamp=now,
                    reason=reason,
                    code=code,
                )
                self._last_event = end_event
                end_response: dict[str, Any] = {
                    "event": EVENT_CALL_ENDED,
                    "call_id": self.call_id,
                    "state": TelephonyCallState.DISCONNECTED.value,
                    "reason": reason,
                }
                if code is not None:
                    end_response["code"] = code
                return end_event, end_response

            if self._call_state == TelephonyCallState.DISCONNECTED:
                # Idempotent terminal event: duplicate call.end is acknowledged without error,
                # but does NOT generate a new domain event (returns None) to preserve terminal event idempotency.
                initial_reason = reason
                initial_code = code
                if isinstance(self._last_event, TelephonyCallEndedEvent):
                    initial_reason = self._last_event.reason
                    initial_code = self._last_event.code
                idem_response: dict[str, Any] = {
                    "event": EVENT_CALL_ENDED,
                    "call_id": self.call_id,
                    "state": TelephonyCallState.DISCONNECTED.value,
                    "reason": initial_reason,
                    "idempotent": True,
                }
                if initial_code is not None:
                    idem_response["code"] = initial_code
                return None, idem_response

            # IDLE -> END is an invalid state transition (cannot end an unstarted call)
            raise InvalidCallStateTransitionError(self._call_state, EVENT_CALL_END)

        raise UnsupportedEventError(str(event_name) if event_name is not None else None)
