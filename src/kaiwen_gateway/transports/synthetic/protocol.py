"""Synthetic WebSocket control protocol framing and schema definitions (v1).

Invariants:
- Versioned: explicitly scoped to `v1`.
- Text-only: binary media frames are rejected in M3.2A (voice streaming deferred to M4).
- Bounded payload size: maximum frame size strictly enforced (64 KiB).
- Validated event vocabulary: unknown or malformed event schemas fail closed.
"""

from __future__ import annotations

import json
import math
from typing import Any, Final

SYNTHETIC_WS_ENDPOINT_PATH: Final[str] = "/ws/synthetic/v1"
SYNTHETIC_PROTOCOL_VERSION: Final[str] = "v1"
MAX_FRAME_SIZE_BYTES: Final[int] = 64 * 1024  # 64 KiB
MAX_PING_SEQ: Final[int] = 2_147_483_647  # Maximum 32-bit positive integer

EVENT_SESSION_READY: Final[str] = "session.ready"
EVENT_SESSION_PING: Final[str] = "session.ping"
EVENT_SESSION_PONG: Final[str] = "session.pong"
EVENT_SESSION_END: Final[str] = "session.end"

EVENT_CALL_START: Final[str] = "call.start"
EVENT_CALL_STARTED: Final[str] = "call.started"
EVENT_CALL_END: Final[str] = "call.end"
EVENT_CALL_ENDED: Final[str] = "call.ended"

SUPPORTED_CLIENT_EVENTS: Final[frozenset[str]] = frozenset(
    {
        EVENT_SESSION_PING,
        EVENT_SESSION_END,
        EVENT_CALL_START,
        EVENT_CALL_END,
    }
)


class SyntheticProtocolError(Exception):
    """Base exception for synthetic control protocol framing errors."""

    def __init__(self, message: str, *, close_code: int = 1008) -> None:
        super().__init__(message)
        self.close_code = close_code


class OversizedMessageError(SyntheticProtocolError):
    """Raised when an inbound message frame exceeds the maximum allowed byte length."""

    def __init__(self, size_bytes: int, max_bytes: int = MAX_FRAME_SIZE_BYTES) -> None:
        super().__init__(
            f"Message size ({size_bytes} bytes) exceeds maximum limit of {max_bytes} bytes",
            close_code=1009,
        )


class InvalidJsonError(SyntheticProtocolError):
    """Raised when an inbound message cannot be parsed as valid JSON."""

    def __init__(self, error: str) -> None:
        super().__init__(f"Invalid JSON payload: {error}", close_code=1007)


class InvalidMessageFormatError(SyntheticProtocolError):
    """Raised when an inbound message structure does not match expected control message schema."""

    def __init__(self, reason: str) -> None:
        super().__init__(f"Malformed control message: {reason}", close_code=1007)


class UnsupportedEventError(SyntheticProtocolError):
    """Raised when an inbound message specifies an unknown or unsupported event name."""

    def __init__(self, event_name: str | None = None) -> None:
        super().__init__("Unsupported event type", close_code=1008)
        self.event_name = event_name


def _reject_json_constant(constant: str) -> None:
    raise ValueError(f"Non-standard JSON constant {constant!r} not permitted")


def _strict_parse_float(value_str: str) -> float:
    val = float(value_str)
    if not math.isfinite(val):
        raise ValueError(f"Non-finite numeric value {value_str!r} not permitted")
    return val


def parse_and_validate_client_frame(raw_text: str) -> dict[str, Any]:
    """Parse and validate an inbound client control frame against v1 synthetic protocol rules.

    Raises:
        OversizedMessageError: If frame byte length exceeds 64 KiB.
        InvalidJsonError: If frame cannot be parsed as valid JSON (or contains NaN/Infinity/non-finite numbers).
        InvalidMessageFormatError: If frame is not a dictionary or lacks 'event' or contains invalid fields.
        UnsupportedEventError: If event name is not recognized in synthetic v1 protocol.
    """
    raw_bytes = raw_text.encode("utf-8")
    if len(raw_bytes) > MAX_FRAME_SIZE_BYTES:
        raise OversizedMessageError(len(raw_bytes))

    try:
        payload = json.loads(
            raw_text,
            parse_constant=_reject_json_constant,
            parse_float=_strict_parse_float,
        )
    except Exception as exc:
        raise InvalidJsonError(str(exc)) from exc

    if not isinstance(payload, dict):
        raise InvalidMessageFormatError("Frame payload must be a JSON object")

    event = payload.get("event")
    if not isinstance(event, str) or not event:
        raise InvalidMessageFormatError("Frame payload missing non-empty 'event' string")

    if event not in SUPPORTED_CLIENT_EVENTS:
        raise UnsupportedEventError(event)

    if event == EVENT_SESSION_PING and "seq" in payload:
        seq = payload["seq"]
        if isinstance(seq, bool) or not isinstance(seq, int) or seq < 0 or seq > MAX_PING_SEQ:
            raise InvalidMessageFormatError(
                f"'seq' must be a non-negative integer <= {MAX_PING_SEQ}"
            )

    if event == EVENT_CALL_START:
        if "stream_id" in payload:
            stream_id = payload["stream_id"]
            if not isinstance(stream_id, str) or not stream_id or len(stream_id) > 128:
                raise InvalidMessageFormatError(
                    "'stream_id' must be a non-empty string <= 128 characters"
                )
        if "metadata" in payload:
            meta = payload["metadata"]
            if not isinstance(meta, dict) or len(meta) > 32:
                raise InvalidMessageFormatError(
                    "'metadata' must be a JSON object with at most 32 entries"
                )
            for k, v in meta.items():
                if not isinstance(k, str) or not k or len(k) > 64:
                    raise InvalidMessageFormatError(
                        "Metadata keys must be non-empty strings <= 64 characters"
                    )
                if isinstance(v, bool) or isinstance(v, (int, float)):
                    pass
                elif isinstance(v, str):
                    if len(v) > 256:
                        raise InvalidMessageFormatError(
                            "Metadata string values must be <= 256 characters"
                        )
                else:
                    raise InvalidMessageFormatError(
                        "Metadata values must be primitive scalar types (string, int, float, bool)"
                    )

    if event == EVENT_CALL_END:
        if "reason" in payload:
            reason = payload["reason"]
            if not isinstance(reason, str) or not reason or len(reason) > 128:
                raise InvalidMessageFormatError(
                    "'reason' must be a non-empty string <= 128 characters"
                )
        if "code" in payload:
            code = payload["code"]
            if isinstance(code, bool) or not isinstance(code, int) or code < 0 or code > 65535:
                raise InvalidMessageFormatError("'code' must be an integer between 0 and 65535")

    return payload
