"""Domain models, validation, and bounded limits for the Kaiwen Gateway Events module."""

from __future__ import annotations

import json
import math
import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from enum import StrEnum
from types import MappingProxyType
from typing import Any, Final

from kaiwen_gateway.core.identity import validate_principal_id, validate_tenant_id

EVENTS_PROTOCOL_VERSION: Final[str] = "v1"

DEFAULT_MAX_EVENT_BYTES: Final[int] = 65_536
DEFAULT_MAX_JSON_DEPTH: Final[int] = 16
DEFAULT_MAX_SUBSCRIPTIONS_PER_CONNECTION: Final[int] = 32
DEFAULT_MAX_QUEUED_EVENTS_PER_CONNECTION: Final[int] = 128
DEFAULT_MAX_QUEUED_BYTES_PER_CONNECTION: Final[int] = 1_048_576
DEFAULT_GLOBAL_QUEUED_BYTES_BUDGET: Final[int] = 33_554_432

DEFAULT_MAX_EVENT_ID_LENGTH: Final[int] = 128
DEFAULT_MAX_EVENT_TYPE_LENGTH: Final[int] = 96
DEFAULT_MAX_TOPIC_LENGTH: Final[int] = 128
DEFAULT_MAX_CORRELATION_ID_LENGTH: Final[int] = 128

EVENT_ID_PATTERN: Final[re.Pattern[str]] = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$")
EVENT_TYPE_PATTERN: Final[re.Pattern[str]] = re.compile(
    r"^[a-z][a-z0-9_]*(\.[a-z][a-z0-9_]*){1,4}$"
)
TOPIC_PATTERN: Final[re.Pattern[str]] = re.compile(r"^[a-z0-9][a-z0-9._-]{0,127}$")
TOPIC_WILDCARD_PREFIX_PATTERN: Final[re.Pattern[str]] = re.compile(r"^[a-z0-9][a-z0-9._-]{0,125}$")
CORRELATION_ID_PATTERN: Final[re.Pattern[str]] = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:/-]{0,127}$")


class EventsError(Exception):
    """Base exception for all Kaiwen Gateway Events module errors."""


class InvalidEventError(EventsError, ValueError):
    """Raised when an event envelope, topic, or subscription input is malformed."""


class OversizedEventError(InvalidEventError):
    """Raised when a serialized event exceeds the configured maximum frame size."""


class UnsupportedEventVersionError(InvalidEventError):
    """Raised when an event envelope specifies an unsupported protocol version."""


class EventAuthorizationError(EventsError):
    """Raised when a principal lacks required authorization for an event operation."""


class EventRateLimitError(EventsError):
    """Raised when an event publish or subscribe operation exceeds rate limits."""


class EventBackpressureError(EventsError):
    """Raised when bounded event queues or global memory budgets are exhausted."""


class EventsModuleClosedError(EventsError):
    """Raised when operations are attempted on a stopped or unstarted Events module."""


class EventsBackendUnavailableError(EventsError):
    """Raised when a configured distributed Events backend (such as Redis) is unavailable."""


class EventsDependencyError(EventsError):
    """Raised when an optional Events extra (such as `kaiwen-gateway[redis]`) is missing."""


class SlowConsumerPolicy(StrEnum):
    """Policy applied when a subscriber connection's outbound queue overflows."""

    DISCONNECT_SLOW_CONSUMER = "disconnect_slow_consumer"
    DROP_OLDEST = "drop_oldest"


class EventDeliveryOutcome(StrEnum):
    """Outcome of attempting to enqueue an event to a single subscriber connection."""

    DELIVERED = "delivered"
    DROPPED_SLOW_CONSUMER = "dropped_slow_consumer"
    DISCONNECTED_SLOW_CONSUMER = "disconnected_slow_consumer"


def validate_event_id(value: object) -> str:
    """Validate canonical event identifier format."""
    if not isinstance(value, str):
        raise InvalidEventError(f"event_id must be a str, got {type(value).__name__}")
    if (
        not value
        or len(value) > DEFAULT_MAX_EVENT_ID_LENGTH
        or not EVENT_ID_PATTERN.fullmatch(value)
    ):
        raise InvalidEventError(
            "event_id must be 1-128 ASCII alphanumeric/._- characters starting with alphanumeric"
        )
    return value


def validate_event_type(value: object) -> str:
    """Validate canonical dot-delimited lowercase event_type."""
    if not isinstance(value, str):
        raise InvalidEventError(f"event_type must be a str, got {type(value).__name__}")
    if (
        not value
        or len(value) > DEFAULT_MAX_EVENT_TYPE_LENGTH
        or not EVENT_TYPE_PATTERN.fullmatch(value)
    ):
        raise InvalidEventError(
            "event_type must match ^[a-z][a-z0-9_]*(\\.[a-z][a-z0-9_]*){1,4}$ and be <= 96 chars"
        )
    return value


def validate_topic(value: object) -> str:
    """Validate an exact publish/subscription topic (wildcards forbidden)."""
    if not isinstance(value, str):
        raise InvalidEventError(f"topic must be a str, got {type(value).__name__}")
    if not value or len(value) > DEFAULT_MAX_TOPIC_LENGTH or not TOPIC_PATTERN.fullmatch(value):
        raise InvalidEventError(
            "topic must be 1-128 lowercase ASCII alphanumeric/._- characters starting with alphanumeric"
        )
    return value


def validate_topic_pattern(value: object) -> str:
    """Validate a subscription topic pattern (exact topic or `<prefix>.*`).

    Bare `*` and mid-string wildcards are rejected.
    """
    if not isinstance(value, str):
        raise InvalidEventError(f"topic_pattern must be a str, got {type(value).__name__}")
    if not value or len(value) > DEFAULT_MAX_TOPIC_LENGTH:
        raise InvalidEventError("topic_pattern must be 1-128 characters")
    if value.endswith(".*"):
        prefix = value[:-2]
        if not prefix or not TOPIC_WILDCARD_PREFIX_PATTERN.fullmatch(prefix):
            raise InvalidEventError(
                "wildcard topic_pattern must have a valid non-empty prefix before '.*'"
            )
        return value
    if "*" in value:
        raise InvalidEventError("topic_pattern only supports a single trailing '.*' wildcard")
    return validate_topic(value)


def topic_matches_pattern(topic: str, topic_pattern: str) -> bool:
    """Return True if a validated exact topic matches a validated subscription pattern."""
    if topic_pattern.endswith(".*"):
        prefix = topic_pattern[:-1]  # includes trailing '.'
        return topic.startswith(prefix)
    return topic == topic_pattern


def validate_correlation_id(value: object) -> str | None:
    """Validate optional correlation_id."""
    if value is None:
        return None
    if not isinstance(value, str):
        raise InvalidEventError(f"correlation_id must be a str or None, got {type(value).__name__}")
    if (
        not value
        or len(value) > DEFAULT_MAX_CORRELATION_ID_LENGTH
        or not CORRELATION_ID_PATTERN.fullmatch(value)
    ):
        raise InvalidEventError("correlation_id must be 1-128 ASCII alphanumeric/._:/- characters")
    return value


def validate_timestamp(value: object) -> str:
    """Validate UTC ISO-8601 timestamp string."""
    if not isinstance(value, str):
        raise InvalidEventError(f"timestamp must be a str, got {type(value).__name__}")
    if not value or ("T" not in value and "t" not in value):
        raise InvalidEventError("timestamp must be a valid UTC ISO-8601 datetime string")
    if not (value.endswith("Z") or value.endswith("+00:00")):
        raise InvalidEventError("timestamp must explicitly specify UTC ('Z' or '+00:00')")
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError as exc:
        raise InvalidEventError("timestamp is not a valid ISO-8601 datetime") from exc
    if parsed.tzinfo is None or parsed.utcoffset() != timedelta(0):
        raise InvalidEventError("timestamp must have zero UTC offset")
    return parsed.astimezone(UTC).isoformat().replace("+00:00", "Z")


def _freeze_and_validate_json_value(
    value: Any,
    *,
    depth: int,
    max_depth: int,
) -> Any:
    """Recursively validate JSON-compatibility, depth, finite floats, and freeze containers."""
    if depth > max_depth:
        raise InvalidEventError(f"payload exceeds maximum JSON nesting depth of {max_depth}")

    if value is None or isinstance(value, (bool, int, str)):
        return value

    if isinstance(value, float):
        if not math.isfinite(value):
            raise InvalidEventError("payload contains non-finite float (NaN or Infinity)")
        return value

    if isinstance(value, Mapping):
        frozen: dict[str, Any] = {}
        for k, v in value.items():
            if not isinstance(k, str):
                raise InvalidEventError(f"payload object keys must be str, got {type(k).__name__}")
            frozen[k] = _freeze_and_validate_json_value(
                v,
                depth=depth + 1,
                max_depth=max_depth,
            )
        return MappingProxyType(frozen)

    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        return tuple(
            _freeze_and_validate_json_value(
                item,
                depth=depth + 1,
                max_depth=max_depth,
            )
            for item in value
        )

    raise InvalidEventError(
        f"payload contains non-JSON-serializable value of type {type(value).__name__}"
    )


def _thaw_json_value(value: Any) -> Any:
    """Convert frozen MappingProxyType/tuple structure back to plain dict/list for JSON dumping."""
    if isinstance(value, Mapping):
        return {k: _thaw_json_value(v) for k, v in value.items()}
    if isinstance(value, tuple):
        return [_thaw_json_value(item) for item in value]
    return value


@dataclass(frozen=True, slots=True)
class EventEnvelope:
    """Immutable, size- and depth-validated canonical gateway event envelope."""

    protocol_version: str
    event_id: str
    event_type: str
    topic: str
    timestamp: str
    tenant_id: str
    principal_id: str
    payload: Mapping[str, Any]
    correlation_id: str | None = None
    max_event_bytes: int = field(default=DEFAULT_MAX_EVENT_BYTES, repr=False, compare=False)
    max_json_depth: int = field(default=DEFAULT_MAX_JSON_DEPTH, repr=False, compare=False)
    serialized_bytes: int = field(init=False, compare=False)
    _serialized_json: str = field(init=False, repr=False, compare=False)

    def __post_init__(self) -> None:
        if self.protocol_version != EVENTS_PROTOCOL_VERSION:
            raise UnsupportedEventVersionError(
                f"Unsupported protocol_version {self.protocol_version!r}; "
                f"expected {EVENTS_PROTOCOL_VERSION!r}"
            )
        if not isinstance(self.max_event_bytes, int) or isinstance(self.max_event_bytes, bool):
            raise InvalidEventError("max_event_bytes must be a positive int")
        if self.max_event_bytes <= 0:
            raise InvalidEventError("max_event_bytes must be positive")
        if self.max_event_bytes > DEFAULT_MAX_EVENT_BYTES:
            raise InvalidEventError(
                f"max_event_bytes ({self.max_event_bytes}) cannot exceed canonical v1 limit "
                f"{DEFAULT_MAX_EVENT_BYTES}"
            )
        if not isinstance(self.max_json_depth, int) or isinstance(self.max_json_depth, bool):
            raise InvalidEventError("max_json_depth must be a positive int")
        if self.max_json_depth <= 0:
            raise InvalidEventError("max_json_depth must be positive")
        if self.max_json_depth > DEFAULT_MAX_JSON_DEPTH:
            raise InvalidEventError(
                f"max_json_depth ({self.max_json_depth}) cannot exceed canonical v1 limit "
                f"{DEFAULT_MAX_JSON_DEPTH}"
            )

        object.__setattr__(self, "event_id", validate_event_id(self.event_id))
        object.__setattr__(self, "event_type", validate_event_type(self.event_type))
        object.__setattr__(self, "topic", validate_topic(self.topic))
        object.__setattr__(self, "timestamp", validate_timestamp(self.timestamp))
        object.__setattr__(self, "tenant_id", validate_tenant_id(self.tenant_id))
        object.__setattr__(self, "principal_id", validate_principal_id(self.principal_id))
        object.__setattr__(self, "correlation_id", validate_correlation_id(self.correlation_id))

        if not isinstance(self.payload, Mapping):
            raise InvalidEventError(f"payload must be a Mapping, got {type(self.payload).__name__}")

        frozen_payload = _freeze_and_validate_json_value(
            self.payload,
            depth=1,
            max_depth=self.max_json_depth,
        )
        object.__setattr__(self, "payload", frozen_payload)

        wire_dict = self._build_wire_dict()
        try:
            serialized = json.dumps(
                wire_dict,
                allow_nan=False,
                separators=(",", ":"),
                ensure_ascii=False,
            )
        except (TypeError, ValueError) as exc:
            raise InvalidEventError(f"EventEnvelope is not JSON serializable: {exc}") from exc

        byte_len = len(serialized.encode("utf-8"))
        if byte_len > self.max_event_bytes:
            raise OversizedEventError(
                f"Serialized event size {byte_len} bytes exceeds maximum {self.max_event_bytes} bytes"
            )

        object.__setattr__(self, "serialized_bytes", byte_len)
        object.__setattr__(self, "_serialized_json", serialized)

    def _build_wire_dict(self) -> dict[str, Any]:
        wire: dict[str, Any] = {
            "protocol_version": self.protocol_version,
            "event_id": self.event_id,
            "event_type": self.event_type,
            "topic": self.topic,
            "timestamp": self.timestamp,
            "tenant_id": self.tenant_id,
            "principal_id": self.principal_id,
            "payload": _thaw_json_value(self.payload),
        }
        if self.correlation_id is not None:
            wire["correlation_id"] = self.correlation_id
        return wire

    def to_dict(self) -> dict[str, Any]:
        """Return a fresh mutable JSON-serializable dictionary representation."""
        return self._build_wire_dict()

    def to_json(self) -> str:
        """Return canonical compact JSON string representation."""
        return self._serialized_json

    def to_json_bytes(self) -> bytes:
        """Return canonical compact UTF-8 encoded JSON bytes."""
        return self._serialized_json.encode("utf-8")

    def __repr__(self) -> str:
        return (
            f"EventEnvelope(protocol_version={self.protocol_version!r}, "
            f"event_id={self.event_id!r}, event_type={self.event_type!r}, "
            f"topic={self.topic!r}, serialized_bytes={self.serialized_bytes})"
        )


@dataclass(frozen=True, slots=True)
class PublishResult:
    """Bounded diagnostic summary of a single event publish operation.

    Note on ``accepted``:
        ``accepted=False`` indicates that one or more matched subscribers could not be
        enqueued (for example due to global queued-byte budget exhaustion). A failed or
        partially accepted fan-out (``accepted=False`` with ``delivered_subscribers > 0``)
        may already have delivered the event to other subscribers. Applications must not
        assume blind retries are duplicate-free.
    """

    event_id: str
    topic: str
    matched_subscribers: int
    delivered_subscribers: int
    dropped_subscribers: int
    disconnected_subscribers: int
    dropped_global_budget: int = 0
    accepted: bool = True
    durable_persisted: bool = False


@dataclass(frozen=True, slots=True)
class DurableEventRecord:
    """Interface record for future E4 durable event store adapters."""

    envelope: EventEnvelope
    sequence_id: str | None = None
    delivery_attempts: int | None = None


__all__ = [
    "CORRELATION_ID_PATTERN",
    "DEFAULT_GLOBAL_QUEUED_BYTES_BUDGET",
    "DEFAULT_MAX_CORRELATION_ID_LENGTH",
    "DEFAULT_MAX_EVENT_BYTES",
    "DEFAULT_MAX_EVENT_ID_LENGTH",
    "DEFAULT_MAX_EVENT_TYPE_LENGTH",
    "DEFAULT_MAX_JSON_DEPTH",
    "DEFAULT_MAX_QUEUED_BYTES_PER_CONNECTION",
    "DEFAULT_MAX_QUEUED_EVENTS_PER_CONNECTION",
    "DEFAULT_MAX_SUBSCRIPTIONS_PER_CONNECTION",
    "DEFAULT_MAX_TOPIC_LENGTH",
    "DurableEventRecord",
    "EVENTS_PROTOCOL_VERSION",
    "EVENT_ID_PATTERN",
    "EVENT_TYPE_PATTERN",
    "EventAuthorizationError",
    "EventBackpressureError",
    "EventDeliveryOutcome",
    "EventEnvelope",
    "EventsBackendUnavailableError",
    "EventsDependencyError",
    "EventsError",
    "EventsModuleClosedError",
    "EventRateLimitError",
    "InvalidEventError",
    "OversizedEventError",
    "PublishResult",
    "SlowConsumerPolicy",
    "TOPIC_PATTERN",
    "TOPIC_WILDCARD_PREFIX_PATTERN",
    "UnsupportedEventVersionError",
    "topic_matches_pattern",
    "validate_correlation_id",
    "validate_event_id",
    "validate_event_type",
    "validate_timestamp",
    "validate_topic",
    "validate_topic_pattern",
]
