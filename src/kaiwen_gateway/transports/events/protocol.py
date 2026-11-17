"""Versioned v1 JSON WebSocket protocol parser and frame builders for `/ws/events/v1`."""

from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any, Final

from kaiwen_gateway.modules.events.domain import (
    DEFAULT_MAX_EVENT_BYTES,
    DEFAULT_MAX_JSON_DEPTH,
    EVENTS_PROTOCOL_VERSION,
    EventEnvelope,
)

EVENTS_WS_ENDPOINT_PATH: Final[str] = "/ws/events/v1"
EVENTS_WS_PROTOCOL_VERSION: Final[str] = EVENTS_PROTOCOL_VERSION
EVENTS_MAX_FRAME_BYTES: Final[int] = DEFAULT_MAX_EVENT_BYTES
EVENTS_MAX_JSON_DEPTH: Final[int] = DEFAULT_MAX_JSON_DEPTH

CMD_EVENTS_SUBSCRIBE: Final[str] = "events.subscribe"
CMD_EVENTS_UNSUBSCRIBE: Final[str] = "events.unsubscribe"
CMD_SESSION_PING: Final[str] = "session.ping"
CMD_EVENTS_PUBLISH: Final[str] = "events.publish"

EVENT_SESSION_READY: Final[str] = "session.ready"
EVENT_EVENTS_SUBSCRIBED: Final[str] = "events.subscribed"
EVENT_EVENTS_UNSUBSCRIBED: Final[str] = "events.unsubscribed"
EVENT_SESSION_PONG: Final[str] = "session.pong"
EVENT_EVENTS_DELIVERY: Final[str] = "events.delivery"
EVENT_EVENTS_ERROR: Final[str] = "events.error"

SUPPORTED_CLIENT_COMMANDS: Final[frozenset[str]] = frozenset(
    {
        CMD_EVENTS_SUBSCRIBE,
        CMD_EVENTS_UNSUBSCRIBE,
        CMD_SESSION_PING,
        CMD_EVENTS_PUBLISH,
    }
)


class EventsProtocolError(ValueError):
    """Base protocol error carrying deterministic WebSocket close code and safe error code."""

    def __init__(self, message: str, *, close_code: int, error_code: str) -> None:
        self.close_code: int = close_code
        self.error_code: str = error_code
        super().__init__(message)


class OversizedEventsFrameError(EventsProtocolError):
    """Raised when a WebSocket frame exceeds 65,536 UTF-8 bytes (close 1009)."""

    def __init__(self, message: str = "Frame exceeds maximum allowed size") -> None:
        super().__init__(message, close_code=1009, error_code="FRAME_TOO_LARGE")


class InvalidEventsFrameError(EventsProtocolError):
    """Raised on invalid UTF-8, malformed JSON, depth > 16, or invalid schema (close 1007)."""

    def __init__(
        self,
        message: str = "Invalid frame payload",
        *,
        error_code: str = "INVALID_FRAME",
    ) -> None:
        super().__init__(message, close_code=1007, error_code=error_code)


class EventsPolicyViolationError(EventsProtocolError):
    """Raised on policy violations such as forged identity or client publish (close 1008)."""

    def __init__(
        self,
        message: str = "Policy violation",
        *,
        error_code: str = "POLICY_VIOLATION",
    ) -> None:
        super().__init__(message, close_code=1008, error_code=error_code)


@dataclass(frozen=True, slots=True)
class ParsedEventsClientFrame:
    """Validated client WebSocket command frame."""

    command: str
    topic_pattern: str | None = None
    subscription_id: str | None = None
    request_id: str | None = None
    seq: int | str | None = None


def _reject_non_finite_constant(constant: str) -> Any:
    raise InvalidEventsFrameError(f"Non-finite JSON constant {constant!r} is forbidden")


def _check_json_depth(value: Any, *, depth: int, max_depth: int) -> None:
    if depth > max_depth:
        raise InvalidEventsFrameError(f"JSON nesting depth exceeds {max_depth}")
    if isinstance(value, Mapping):
        for v in value.values():
            _check_json_depth(v, depth=depth + 1, max_depth=max_depth)
    elif isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        for item in value:
            _check_json_depth(item, depth=depth + 1, max_depth=max_depth)


def parse_and_validate_events_client_frame(raw: str | bytes) -> ParsedEventsClientFrame:
    """Validate UTF-8, byte length, JSON depth, protocol_version, and command schema."""
    if isinstance(raw, bytes):
        if len(raw) > EVENTS_MAX_FRAME_BYTES:
            raise OversizedEventsFrameError()
        try:
            text = raw.decode("utf-8")
        except UnicodeDecodeError as exc:
            raise InvalidEventsFrameError("Frame is not valid UTF-8") from exc
    elif isinstance(raw, str):
        encoded = raw.encode("utf-8")
        if len(encoded) > EVENTS_MAX_FRAME_BYTES:
            raise OversizedEventsFrameError()
        text = raw
    else:
        raise InvalidEventsFrameError("Unsupported frame type")

    try:
        data = json.loads(text, parse_constant=_reject_non_finite_constant)
    except InvalidEventsFrameError:
        raise
    except (json.JSONDecodeError, ValueError) as exc:
        raise InvalidEventsFrameError("Malformed JSON frame") from exc

    _check_json_depth(data, depth=1, max_depth=EVENTS_MAX_JSON_DEPTH)

    if not isinstance(data, dict):
        raise InvalidEventsFrameError("Frame root must be a JSON object")

    if data.get("protocol_version") != EVENTS_WS_PROTOCOL_VERSION:
        raise EventsPolicyViolationError(
            "Unsupported or missing protocol_version",
            error_code="UNSUPPORTED_VERSION",
        )

    if "tenant_id" in data or "principal_id" in data:
        raise EventsPolicyViolationError(
            "Client frames must not specify tenant_id or principal_id",
            error_code="FORBIDDEN_IDENTITY_CLAIM",
        )

    command = data.get("type")
    if not isinstance(command, str) or command not in SUPPORTED_CLIENT_COMMANDS:
        raise InvalidEventsFrameError(
            "Unknown or invalid command type", error_code="UNKNOWN_COMMAND"
        )

    request_id = data.get("request_id")
    if request_id is not None and (not isinstance(request_id, str) or len(request_id) > 128):
        raise InvalidEventsFrameError("request_id must be a str (<= 128 chars)")

    if command == CMD_EVENTS_PUBLISH:
        raise EventsPolicyViolationError(
            "Client-side events.publish is disabled",
            error_code="FORBIDDEN_PUBLISH",
        )

    if command == CMD_EVENTS_SUBSCRIBE:
        pattern = data.get("topic_pattern", data.get("topic"))
        if not isinstance(pattern, str) or not pattern:
            raise InvalidEventsFrameError("events.subscribe requires non-empty topic_pattern")
        return ParsedEventsClientFrame(
            command=command,
            topic_pattern=pattern,
            request_id=request_id,
        )

    if command == CMD_EVENTS_UNSUBSCRIBE:
        sub_id = data.get("subscription_id")
        topic = data.get("topic", data.get("topic_pattern"))
        if sub_id is not None:
            if not isinstance(sub_id, str) or not sub_id:
                raise InvalidEventsFrameError(
                    "events.unsubscribe subscription_id must be a non-empty str"
                )
            return ParsedEventsClientFrame(
                command=command,
                subscription_id=sub_id,
                request_id=request_id,
            )
        if not isinstance(topic, str) or not topic:
            raise InvalidEventsFrameError(
                "events.unsubscribe requires non-empty topic or subscription_id"
            )
        return ParsedEventsClientFrame(
            command=command,
            topic_pattern=topic,
            request_id=request_id,
        )

    seq = data.get("seq")
    if seq is not None and (isinstance(seq, bool) or not isinstance(seq, (int, str))):
        raise InvalidEventsFrameError("session.ping seq must be an int or str")

    return ParsedEventsClientFrame(command=command, request_id=request_id, seq=seq)


def build_session_ready_frame(
    *,
    connection_id: str,
    tenant_id: str,
    principal_id: str,
    allowed_scopes: Sequence[str] = (),
) -> dict[str, Any]:
    return {
        "protocol_version": EVENTS_WS_PROTOCOL_VERSION,
        "type": EVENT_SESSION_READY,
        "connection_id": connection_id,
        "tenant_id": tenant_id,
        "principal_id": principal_id,
        "allowed_scopes": sorted(allowed_scopes),
    }


def build_subscribed_frame(
    *,
    subscription_id: str,
    topic_pattern: str,
    request_id: str | None = None,
) -> dict[str, Any]:
    frame: dict[str, Any] = {
        "protocol_version": EVENTS_WS_PROTOCOL_VERSION,
        "type": EVENT_EVENTS_SUBSCRIBED,
        "subscription_id": subscription_id,
        "topic_pattern": topic_pattern,
    }
    if request_id is not None:
        frame["request_id"] = request_id
    return frame


def build_unsubscribed_frame(
    *,
    removed: bool,
    subscription_id: str | None = None,
    topic: str | None = None,
    request_id: str | None = None,
) -> dict[str, Any]:
    frame: dict[str, Any] = {
        "protocol_version": EVENTS_WS_PROTOCOL_VERSION,
        "type": EVENT_EVENTS_UNSUBSCRIBED,
        "removed": removed,
    }
    if subscription_id is not None:
        frame["subscription_id"] = subscription_id
    if topic is not None:
        frame["topic"] = topic
    if request_id is not None:
        frame["request_id"] = request_id
    return frame


def build_session_pong_frame(
    *,
    request_id: str | None = None,
    seq: int | str | None = None,
) -> dict[str, Any]:
    frame: dict[str, Any] = {
        "protocol_version": EVENTS_WS_PROTOCOL_VERSION,
        "type": EVENT_SESSION_PONG,
    }
    if seq is not None:
        frame["seq"] = seq
    if request_id is not None:
        frame["request_id"] = request_id
    return frame


def build_delivery_frame(envelope: EventEnvelope) -> dict[str, Any]:
    frame: dict[str, Any] = {
        "protocol_version": EVENTS_WS_PROTOCOL_VERSION,
        "type": EVENT_EVENTS_DELIVERY,
        "event": envelope.to_dict(),
    }
    serialized = json.dumps(
        frame,
        allow_nan=False,
        separators=(",", ":"),
        ensure_ascii=False,
    ).encode("utf-8")
    if len(serialized) > EVENTS_MAX_FRAME_BYTES:
        raise OversizedEventsFrameError(
            f"Serialized events.delivery frame ({len(serialized)} bytes) exceeds "
            f"maximum {EVENTS_MAX_FRAME_BYTES} bytes"
        )
    return frame


def build_error_frame(
    *,
    code: str,
    message: str,
    request_id: str | None = None,
) -> dict[str, Any]:
    frame: dict[str, Any] = {
        "protocol_version": EVENTS_WS_PROTOCOL_VERSION,
        "type": EVENT_EVENTS_ERROR,
        "code": code,
        "message": message,
    }
    if request_id is not None:
        frame["request_id"] = request_id
    return frame


__all__ = [
    "CMD_EVENTS_PUBLISH",
    "CMD_EVENTS_SUBSCRIBE",
    "CMD_EVENTS_UNSUBSCRIBE",
    "CMD_SESSION_PING",
    "EVENTS_MAX_FRAME_BYTES",
    "EVENTS_MAX_JSON_DEPTH",
    "EVENTS_WS_ENDPOINT_PATH",
    "EVENTS_WS_PROTOCOL_VERSION",
    "EVENT_EVENTS_DELIVERY",
    "EVENT_EVENTS_ERROR",
    "EVENT_EVENTS_SUBSCRIBED",
    "EVENT_EVENTS_UNSUBSCRIBED",
    "EVENT_SESSION_PONG",
    "EVENT_SESSION_READY",
    "EventsPolicyViolationError",
    "EventsProtocolError",
    "InvalidEventsFrameError",
    "OversizedEventsFrameError",
    "ParsedEventsClientFrame",
    "SUPPORTED_CLIENT_COMMANDS",
    "build_delivery_frame",
    "build_error_frame",
    "build_session_pong_frame",
    "build_session_ready_frame",
    "build_subscribed_frame",
    "build_unsubscribed_frame",
    "parse_and_validate_events_client_frame",
]
