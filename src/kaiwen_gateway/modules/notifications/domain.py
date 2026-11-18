"""Domain contracts, validation, and canonical recipient-topic derivation for Notifications (N1)."""

from __future__ import annotations

import hashlib
import json
import math
import re
import uuid
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from enum import StrEnum
from types import MappingProxyType
from typing import Any, Final

from kaiwen_gateway.core.identity import validate_principal_id, validate_tenant_id

NOTIFICATIONS_SCHEMA_VERSION: Final[str] = "notifications.v1"

# 48 KiB inner limit and depth 15 leave headroom for outer EventEnvelope (64 KiB / depth 16).
DEFAULT_MAX_NOTIFICATION_BYTES: Final[int] = 49_152
DEFAULT_MAX_JSON_DEPTH: Final[int] = 15
DEFAULT_MAX_NOTIFICATION_ID_LENGTH: Final[int] = 128
DEFAULT_MAX_CATEGORY_LENGTH: Final[int] = 64
DEFAULT_MAX_NOTIFICATION_TYPE_LENGTH: Final[int] = 96
DEFAULT_MAX_IDEMPOTENCY_KEY_LENGTH: Final[int] = 128
DEFAULT_MAX_CORRELATION_ID_LENGTH: Final[int] = 128

RECIPIENT_TOPIC_PREFIX: Final[str] = "notifications.r."
RECIPIENT_TOPIC_HASH_HEX_LENGTH: Final[int] = 24

NOTIFICATION_ID_PATTERN: Final[re.Pattern[str]] = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$")
CATEGORY_PATTERN: Final[re.Pattern[str]] = re.compile(r"^[a-z0-9][a-z0-9._-]{0,63}$")
NOTIFICATION_TYPE_PATTERN: Final[re.Pattern[str]] = re.compile(r"^[a-z0-9][a-z0-9._-]{0,95}$")
IDEMPOTENCY_KEY_PATTERN: Final[re.Pattern[str]] = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$")
CORRELATION_ID_PATTERN: Final[re.Pattern[str]] = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:/-]{0,127}$")


class NotificationsError(Exception):
    """Base exception for the Kaiwen Gateway Notifications module."""


class InvalidNotificationError(NotificationsError, ValueError):
    """Raised when a NotificationEnvelope or field fails validation."""


class OversizedNotificationError(InvalidNotificationError):
    """Raised when a NotificationEnvelope exceeds the bounded byte limit."""


class UnsupportedNotificationVersionError(InvalidNotificationError):
    """Raised when a notification carries an unsupported schema_version."""


class NotificationExpiredError(NotificationsError):
    """Raised when processing or dispatching an already-expired notification."""


class NotificationAuthorizationError(NotificationsError):
    """Raised when producer targeting or recipient subscription is unauthorized."""


class NotificationsPreferenceError(NotificationsError):
    """Raised when preference evaluation or preference-store lookup fails."""


class NotificationsModuleClosedError(NotificationsError):
    """Raised when using or restarting NotificationsModule after shutdown."""


class NotificationsBackendUnavailableError(NotificationsError):
    """Raised when a required Notifications backend dependency is unavailable."""


class NotificationPriority(StrEnum):
    """Ordered notification priority levels."""

    LOW = "low"
    NORMAL = "normal"
    HIGH = "high"
    URGENT = "urgent"


_PRIORITY_RANKS: Final[dict[NotificationPriority, int]] = {
    NotificationPriority.LOW: 0,
    NotificationPriority.NORMAL: 1,
    NotificationPriority.HIGH: 2,
    NotificationPriority.URGENT: 3,
}


def priority_rank(priority: NotificationPriority) -> int:
    """Return numeric rank for comparing NotificationPriority levels."""
    return _PRIORITY_RANKS[priority]


class NotificationStatus(StrEnum):
    """Explicit lifecycle states across the 6-stage notification delivery model."""

    CREATED = "created"
    DURABLE_ACCEPTED = "durable_accepted"
    PUBLISHED_LIVE = "published_live"
    DELIVERED_TO_TRANSPORT = "delivered_to_transport"
    CLIENT_ACKNOWLEDGED = "client_acknowledged"
    EXPIRED = "expired"
    FILTERED_BY_PREFERENCE = "filtered_by_preference"
    DEAD_LETTERED = "dead_lettered"


def generate_notification_id() -> str:
    """Generate a canonical synthetic notification identifier."""
    return f"notif_{uuid.uuid4().hex}"


def validate_notification_id(value: object) -> str:
    """Validate canonical notification_id."""
    if not isinstance(value, str):
        raise InvalidNotificationError(f"notification_id must be a str, got {type(value).__name__}")
    if (
        not value
        or len(value) > DEFAULT_MAX_NOTIFICATION_ID_LENGTH
        or not NOTIFICATION_ID_PATTERN.fullmatch(value)
    ):
        raise InvalidNotificationError(
            "notification_id must be 1-128 ASCII alphanumeric/._- characters"
        )
    return value


def validate_category(value: object) -> str:
    """Validate bounded lowercase notification category slug."""
    if not isinstance(value, str):
        raise InvalidNotificationError(f"category must be a str, got {type(value).__name__}")
    if (
        not value
        or len(value) > DEFAULT_MAX_CATEGORY_LENGTH
        or not CATEGORY_PATTERN.fullmatch(value)
    ):
        raise InvalidNotificationError(
            "category must be 1-64 lowercase ASCII alphanumeric/._- characters"
        )
    return value


def validate_notification_type(value: object) -> str:
    """Validate bounded lowercase notification_type."""
    if not isinstance(value, str):
        raise InvalidNotificationError(
            f"notification_type must be a str, got {type(value).__name__}"
        )
    if (
        not value
        or len(value) > DEFAULT_MAX_NOTIFICATION_TYPE_LENGTH
        or not NOTIFICATION_TYPE_PATTERN.fullmatch(value)
    ):
        raise InvalidNotificationError(
            "notification_type must be 1-96 lowercase ASCII alphanumeric/._- characters"
        )
    return value


def validate_idempotency_key(value: object) -> str | None:
    """Validate optional producer idempotency_key."""
    if value is None:
        return None
    if not isinstance(value, str):
        raise InvalidNotificationError(
            f"idempotency_key must be a str or None, got {type(value).__name__}"
        )
    if (
        not value
        or len(value) > DEFAULT_MAX_IDEMPOTENCY_KEY_LENGTH
        or not IDEMPOTENCY_KEY_PATTERN.fullmatch(value)
    ):
        raise InvalidNotificationError(
            "idempotency_key must be 1-128 ASCII alphanumeric/._:- characters"
        )
    return value


def validate_correlation_id(value: object) -> str | None:
    """Validate optional correlation_id."""
    if value is None:
        return None
    if not isinstance(value, str):
        raise InvalidNotificationError(
            f"correlation_id must be a str or None, got {type(value).__name__}"
        )
    if (
        not value
        or len(value) > DEFAULT_MAX_CORRELATION_ID_LENGTH
        or not CORRELATION_ID_PATTERN.fullmatch(value)
    ):
        raise InvalidNotificationError(
            "correlation_id must be 1-128 ASCII alphanumeric/._:/- characters"
        )
    return value


def validate_utc_timestamp(value: object, *, field_name: str = "timestamp") -> str:
    """Validate and normalize an explicit UTC ISO-8601 datetime string."""
    if not isinstance(value, str):
        raise InvalidNotificationError(f"{field_name} must be a str, got {type(value).__name__}")
    if not value or ("T" not in value and "t" not in value):
        raise InvalidNotificationError(f"{field_name} must be a valid UTC ISO-8601 datetime string")
    if not (value.endswith("Z") or value.endswith("+00:00")):
        raise InvalidNotificationError(
            f"{field_name} must explicitly specify UTC ('Z' or '+00:00')"
        )
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError as exc:
        raise InvalidNotificationError(f"{field_name} is not a valid ISO-8601 datetime") from exc
    if parsed.tzinfo is None or parsed.utcoffset() != timedelta(0):
        raise InvalidNotificationError(f"{field_name} must have zero UTC offset")
    return parsed.astimezone(UTC).isoformat().replace("+00:00", "Z")


def parse_utc_timestamp(value: str, *, field_name: str = "timestamp") -> datetime:
    """Parse a validated UTC ISO-8601 timestamp into an aware UTC datetime."""
    normalized = validate_utc_timestamp(value, field_name=field_name)
    return datetime.fromisoformat(normalized.replace("Z", "+00:00"))


def derive_recipient_topic(tenant_id: str, recipient_id: str) -> str:
    """Derive deterministic, tenant-separated Events topic for a recipient.

    Uses canonical length-prefixed UTF-8 encoding of `(tenant_id, recipient_id)`
    hashed with SHA-256 (24-char lowercase hex prefix) under `notifications.r.<hex>`.
    This provides Events `TOPIC_PATTERN` naming compatibility only and is NEVER an
    authentication credential or secret.
    """
    try:
        canonical_tenant = validate_tenant_id(tenant_id)
        canonical_recipient = validate_principal_id(recipient_id)
    except ValueError as exc:
        raise InvalidNotificationError(str(exc)) from exc

    tenant_bytes = canonical_tenant.encode("utf-8")
    recipient_bytes = canonical_recipient.encode("utf-8")
    canonical_input = (
        f"{len(tenant_bytes)}:".encode("ascii")
        + tenant_bytes
        + b":"
        + f"{len(recipient_bytes)}:".encode("ascii")
        + recipient_bytes
    )
    digest = hashlib.sha256(canonical_input).hexdigest()[:RECIPIENT_TOPIC_HASH_HEX_LENGTH]
    return f"{RECIPIENT_TOPIC_PREFIX}{digest}"


def _freeze_and_validate_json_value(
    value: Any,
    *,
    depth: int,
    max_depth: int,
    active_containers: set[int],
) -> Any:
    if depth > max_depth:
        raise InvalidNotificationError(f"payload exceeds maximum JSON nesting depth of {max_depth}")

    if value is None or isinstance(value, (bool, int, str)):
        return value

    if isinstance(value, float):
        if not math.isfinite(value):
            raise InvalidNotificationError("payload contains non-finite float (NaN or Infinity)")
        return value

    if isinstance(value, Mapping):
        obj_id = id(value)
        if obj_id in active_containers:
            raise InvalidNotificationError("payload contains cyclic reference")
        active_containers.add(obj_id)
        try:
            frozen: dict[str, Any] = {}
            for k, v in value.items():
                if not isinstance(k, str):
                    raise InvalidNotificationError(
                        f"payload object keys must be str, got {type(k).__name__}"
                    )
                frozen[k] = _freeze_and_validate_json_value(
                    v,
                    depth=depth + 1,
                    max_depth=max_depth,
                    active_containers=active_containers,
                )
            return MappingProxyType(frozen)
        finally:
            active_containers.discard(obj_id)

    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        obj_id = id(value)
        if obj_id in active_containers:
            raise InvalidNotificationError("payload contains cyclic reference")
        active_containers.add(obj_id)
        try:
            return tuple(
                _freeze_and_validate_json_value(
                    item,
                    depth=depth + 1,
                    max_depth=max_depth,
                    active_containers=active_containers,
                )
                for item in value
            )
        finally:
            active_containers.discard(obj_id)

    raise InvalidNotificationError(
        f"payload contains non-JSON-serializable value of type {type(value).__name__}"
    )


def _thaw_json_value(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {k: _thaw_json_value(v) for k, v in value.items()}
    if isinstance(value, tuple):
        return [_thaw_json_value(item) for item in value]
    return value


@dataclass(frozen=True, slots=True)
class NotificationEnvelope:
    """Immutable, size- and depth-validated canonical notification envelope (`notifications.v1`)."""

    notification_id: str
    schema_version: str
    tenant_id: str
    recipient_id: str
    category: str
    notification_type: str
    priority: NotificationPriority
    payload: Mapping[str, Any]
    created_at: str
    expires_at: str | None = None
    is_promotional: bool = False
    idempotency_key: str | None = None
    correlation_id: str | None = None
    max_notification_bytes: int = field(
        default=DEFAULT_MAX_NOTIFICATION_BYTES, repr=False, compare=False
    )
    max_json_depth: int = field(default=DEFAULT_MAX_JSON_DEPTH, repr=False, compare=False)
    serialized_bytes: int = field(init=False, compare=False)
    _serialized_json: str = field(init=False, repr=False, compare=False)

    def __post_init__(self) -> None:
        if self.schema_version != NOTIFICATIONS_SCHEMA_VERSION:
            raise UnsupportedNotificationVersionError(
                f"Unsupported schema_version {self.schema_version!r}; "
                f"expected {NOTIFICATIONS_SCHEMA_VERSION!r}"
            )
        if (
            isinstance(self.max_notification_bytes, bool)
            or not isinstance(self.max_notification_bytes, int)
            or self.max_notification_bytes <= 0
            or self.max_notification_bytes > DEFAULT_MAX_NOTIFICATION_BYTES
        ):
            raise InvalidNotificationError(
                f"max_notification_bytes must be between 1 and {DEFAULT_MAX_NOTIFICATION_BYTES}"
            )
        if (
            isinstance(self.max_json_depth, bool)
            or not isinstance(self.max_json_depth, int)
            or self.max_json_depth <= 0
            or self.max_json_depth > DEFAULT_MAX_JSON_DEPTH
        ):
            raise InvalidNotificationError(
                f"max_json_depth must be between 1 and {DEFAULT_MAX_JSON_DEPTH}"
            )
        if not isinstance(self.is_promotional, bool):
            raise InvalidNotificationError(
                f"is_promotional must be a bool, got {type(self.is_promotional).__name__}"
            )

        object.__setattr__(self, "notification_id", validate_notification_id(self.notification_id))
        try:
            object.__setattr__(self, "tenant_id", validate_tenant_id(self.tenant_id))
            object.__setattr__(self, "recipient_id", validate_principal_id(self.recipient_id))
        except ValueError as exc:
            raise InvalidNotificationError(str(exc)) from exc

        object.__setattr__(self, "category", validate_category(self.category))
        object.__setattr__(
            self, "notification_type", validate_notification_type(self.notification_type)
        )

        raw_priority: Any = self.priority
        if not isinstance(raw_priority, NotificationPriority):
            try:
                coerced_priority = NotificationPriority(str(raw_priority))
            except ValueError as exc:
                raise InvalidNotificationError(
                    f"Unsupported notification priority {raw_priority!r}"
                ) from exc
            object.__setattr__(self, "priority", coerced_priority)

        normalized_created = validate_utc_timestamp(self.created_at, field_name="created_at")
        object.__setattr__(self, "created_at", normalized_created)

        if self.expires_at is not None:
            normalized_expires = validate_utc_timestamp(self.expires_at, field_name="expires_at")
            created_dt = parse_utc_timestamp(normalized_created, field_name="created_at")
            expires_dt = parse_utc_timestamp(normalized_expires, field_name="expires_at")
            if expires_dt <= created_dt:
                raise InvalidNotificationError("expires_at must be strictly after created_at")
            object.__setattr__(self, "expires_at", normalized_expires)

        object.__setattr__(self, "idempotency_key", validate_idempotency_key(self.idempotency_key))
        object.__setattr__(self, "correlation_id", validate_correlation_id(self.correlation_id))

        if not isinstance(self.payload, Mapping):
            raise InvalidNotificationError(
                f"payload must be a mapping/JSON object, got {type(self.payload).__name__}"
            )
        frozen_payload = _freeze_and_validate_json_value(
            self.payload,
            depth=1,
            max_depth=self.max_json_depth,
            active_containers=set(),
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
            byte_len = len(serialized.encode("utf-8"))
        except (TypeError, ValueError, UnicodeEncodeError) as exc:
            raise InvalidNotificationError(
                f"NotificationEnvelope is not JSON serializable: {exc}"
            ) from exc
        if byte_len > self.max_notification_bytes:
            raise OversizedNotificationError(
                f"Serialized notification size {byte_len} bytes exceeds maximum "
                f"{self.max_notification_bytes} bytes"
            )

        object.__setattr__(self, "serialized_bytes", byte_len)
        object.__setattr__(self, "_serialized_json", serialized)

    def _build_wire_dict(self) -> dict[str, Any]:
        wire: dict[str, Any] = {
            "notification_id": self.notification_id,
            "schema_version": self.schema_version,
            "tenant_id": self.tenant_id,
            "recipient_id": self.recipient_id,
            "category": self.category,
            "notification_type": self.notification_type,
            "priority": self.priority.value,
            "payload": _thaw_json_value(self.payload),
            "created_at": self.created_at,
            "is_promotional": self.is_promotional,
        }
        if self.expires_at is not None:
            wire["expires_at"] = self.expires_at
        if self.idempotency_key is not None:
            wire["idempotency_key"] = self.idempotency_key
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
        """Return UTF-8 encoded compact JSON bytes."""
        return self._serialized_json.encode("utf-8")

    def recipient_topic(self) -> str:
        """Return deterministic Events topic derived from `(tenant_id, recipient_id)`."""
        return derive_recipient_topic(self.tenant_id, self.recipient_id)

    def is_expired(self, *, now: datetime | None = None) -> bool:
        """Return True if `expires_at` is set and `now >= expires_at`."""
        if self.expires_at is None:
            return False
        current = now.astimezone(UTC) if now is not None else datetime.now(UTC)
        expires_dt = parse_utc_timestamp(self.expires_at, field_name="expires_at")
        return current >= expires_dt

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> NotificationEnvelope:
        """Construct and validate a `NotificationEnvelope` from an untrusted mapping."""
        if not isinstance(data, Mapping):
            raise InvalidNotificationError(
                f"Notification data must be a mapping, got {type(data).__name__}"
            )
        required = (
            "notification_id",
            "schema_version",
            "tenant_id",
            "recipient_id",
            "category",
            "notification_type",
            "priority",
            "payload",
            "created_at",
        )
        missing = [k for k in required if k not in data]
        if missing:
            raise InvalidNotificationError(f"Missing required notification fields: {missing}")

        return cls(
            notification_id=data["notification_id"],
            schema_version=data["schema_version"],
            tenant_id=data["tenant_id"],
            recipient_id=data["recipient_id"],
            category=data["category"],
            notification_type=data["notification_type"],
            priority=data["priority"],
            payload=data["payload"],
            created_at=data["created_at"],
            expires_at=data.get("expires_at"),
            is_promotional=data.get("is_promotional", False),
            idempotency_key=data.get("idempotency_key"),
            correlation_id=data.get("correlation_id"),
        )


__all__ = [
    "CATEGORY_PATTERN",
    "CORRELATION_ID_PATTERN",
    "DEFAULT_MAX_CATEGORY_LENGTH",
    "DEFAULT_MAX_CORRELATION_ID_LENGTH",
    "DEFAULT_MAX_IDEMPOTENCY_KEY_LENGTH",
    "DEFAULT_MAX_JSON_DEPTH",
    "DEFAULT_MAX_NOTIFICATION_BYTES",
    "DEFAULT_MAX_NOTIFICATION_ID_LENGTH",
    "DEFAULT_MAX_NOTIFICATION_TYPE_LENGTH",
    "IDEMPOTENCY_KEY_PATTERN",
    "NOTIFICATIONS_SCHEMA_VERSION",
    "NOTIFICATION_ID_PATTERN",
    "NOTIFICATION_TYPE_PATTERN",
    "InvalidNotificationError",
    "NotificationAuthorizationError",
    "NotificationEnvelope",
    "NotificationExpiredError",
    "NotificationPriority",
    "NotificationStatus",
    "NotificationsBackendUnavailableError",
    "NotificationsError",
    "NotificationsModuleClosedError",
    "NotificationsPreferenceError",
    "OversizedNotificationError",
    "RECIPIENT_TOPIC_HASH_HEX_LENGTH",
    "RECIPIENT_TOPIC_PREFIX",
    "UnsupportedNotificationVersionError",
    "derive_recipient_topic",
    "generate_notification_id",
    "parse_utc_timestamp",
    "priority_rank",
    "validate_category",
    "validate_correlation_id",
    "validate_idempotency_key",
    "validate_notification_id",
    "validate_notification_type",
    "validate_utc_timestamp",
]
