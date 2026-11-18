"""Durable notification publisher, bounded Redis Streams worker, and crash recovery (N3)."""

from __future__ import annotations

import asyncio
import contextlib
import json
import math
import re
from collections import OrderedDict
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from types import MappingProxyType
from typing import Any, Final, Protocol, runtime_checkable

from kaiwen_gateway.core.identity import VerifiedPrincipalContext, validate_tenant_id
from kaiwen_gateway.modules.events.domain import (
    DEFAULT_MAX_EVENT_BYTES,
    EVENTS_PROTOCOL_VERSION,
    DurableEventRecord,
    EventBackpressureError,
    EventEnvelope,
    EventRateLimitError,
    EventsBackendUnavailableError,
    EventsError,
    EventsModuleClosedError,
    InvalidEventError,
    OversizedEventError,
    validate_topic,
)
from kaiwen_gateway.modules.events.ports import DurableEventStorePort, EventBrokerPort
from kaiwen_gateway.modules.notifications.domain import (
    NOTIFICATIONS_SCHEMA_VERSION,
    InvalidNotificationError,
    NotificationAuthorizationError,
    NotificationEnvelope,
    NotificationExpiredError,
    NotificationsBackendUnavailableError,
    NotificationsModuleClosedError,
    NotificationStatus,
    OversizedNotificationError,
    UnsupportedNotificationVersionError,
    validate_category,
)
from kaiwen_gateway.modules.notifications.ports import (
    DenyAllProducerAuthorizer,
    NotificationProducerAuthorizerPort,
    NotificationPublishResult,
)
from kaiwen_gateway.modules.notifications.preferences import (
    NotificationPreferencePort,
    evaluate_notification_preference,
)
from kaiwen_gateway.modules.notifications.telemetry import (
    InMemoryNotificationsTelemetrySink,
    NotificationsErrorCategory,
    NotificationsTelemetrySink,
)

DEFAULT_DURABLE_INGESTION_TOPIC: Final[str] = "notifications.ingest"
DEFAULT_DURABLE_GROUP: Final[str] = "notifications-workers"
DEFAULT_DURABLE_CONSUMER: Final[str] = "worker-1"
DEFAULT_DURABLE_READ_BATCH_SIZE: Final[int] = 32
DEFAULT_DURABLE_MAX_BATCH_SIZE: Final[int] = 256
DEFAULT_DURABLE_BLOCK_MS: Final[int] = 200
DEFAULT_DURABLE_MAX_BLOCK_MS: Final[int] = 1000
DEFAULT_DURABLE_DEDUP_MAX_ENTRIES: Final[int] = 10_000
_STREAM_IDENT_PATTERN: Final[re.Pattern[str]] = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,63}$")


def _validate_ident(value: object, name: str) -> str:
    if not isinstance(value, str) or not _STREAM_IDENT_PATTERN.fullmatch(value):
        raise InvalidNotificationError(
            f"{name} must be 1-64 ASCII alphanumeric/._:- characters starting with alphanumeric"
        )
    return value


def _validate_delivery_frame_size(event_envelope: EventEnvelope) -> None:
    delivery_frame = {
        "protocol_version": EVENTS_PROTOCOL_VERSION,
        "type": "events.delivery",
        "event": event_envelope.to_dict(),
    }
    try:
        serialized = json.dumps(
            delivery_frame,
            allow_nan=False,
            separators=(",", ":"),
            ensure_ascii=False,
        ).encode("utf-8")
    except (TypeError, ValueError, UnicodeEncodeError) as exc:
        raise InvalidNotificationError(
            f"Final notification delivery frame is not JSON serializable: {exc}"
        ) from exc
    if len(serialized) > DEFAULT_MAX_EVENT_BYTES:
        raise OversizedNotificationError(
            f"Final serialized events.delivery frame ({len(serialized)} bytes) "
            f"exceeds maximum {DEFAULT_MAX_EVENT_BYTES} bytes"
        )


@runtime_checkable
class DeadLetterCapableStore(Protocol):
    """Structural protocol for durable stores supporting explicit DLQ quarantine."""

    async def dead_letter(
        self,
        *,
        tenant_id: str,
        topic: str,
        group: str,
        record_id: str,
        reason: str,
        envelope: EventEnvelope | None = None,
        delivery_attempts: int = 1,
    ) -> Any: ...


@runtime_checkable
class RecoveryCapableStore(Protocol):
    """Structural protocol for durable stores supporting XPENDING/XAUTOCLAIM recovery."""

    async def reclaim_pending(
        self,
        *,
        tenant_id: str,
        topic: str,
        group: str,
        consumer: str,
        min_idle_ms: int | None = None,
        count: int | None = None,
        start_id: str = "0-0",
    ) -> Any: ...


@dataclass(frozen=True, slots=True)
class NotificationsDurableConfig:
    """Bounded configuration for `NotificationsDurableWorker`.

    `block_ms` is capped at `1000` ms so blocking `XREADGROUP` calls remain safely
    below `RedisClientConfig.socket_timeout_seconds` (default `2.0` s).
    """

    tenant_id: str
    topic: str = DEFAULT_DURABLE_INGESTION_TOPIC
    group: str = DEFAULT_DURABLE_GROUP
    consumer: str = DEFAULT_DURABLE_CONSUMER
    read_batch_size: int = DEFAULT_DURABLE_READ_BATCH_SIZE
    block_ms: int = DEFAULT_DURABLE_BLOCK_MS
    poll_interval_seconds: float = 0.05
    recovery_interval_seconds: float = 1.0
    claim_min_idle_ms: int | None = None
    max_dedup_entries: int = DEFAULT_DURABLE_DEDUP_MAX_ENTRIES
    authoritative_promotional_categories: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        try:
            object.__setattr__(self, "tenant_id", validate_tenant_id(self.tenant_id))
            object.__setattr__(self, "topic", validate_topic(self.topic))
        except ValueError as exc:
            raise InvalidNotificationError(str(exc)) from exc

        object.__setattr__(self, "group", _validate_ident(self.group, "group"))
        object.__setattr__(self, "consumer", _validate_ident(self.consumer, "consumer"))

        if (
            isinstance(self.read_batch_size, bool)
            or not isinstance(self.read_batch_size, int)
            or self.read_batch_size <= 0
            or self.read_batch_size > DEFAULT_DURABLE_MAX_BATCH_SIZE
        ):
            raise InvalidNotificationError(
                f"read_batch_size must be between 1 and {DEFAULT_DURABLE_MAX_BATCH_SIZE}"
            )
        if (
            isinstance(self.block_ms, bool)
            or not isinstance(self.block_ms, int)
            or self.block_ms < 0
            or self.block_ms > DEFAULT_DURABLE_MAX_BLOCK_MS
        ):
            raise InvalidNotificationError(
                f"block_ms must be between 0 and {DEFAULT_DURABLE_MAX_BLOCK_MS}"
            )
        if (
            isinstance(self.poll_interval_seconds, bool)
            or not isinstance(self.poll_interval_seconds, (int, float))
            or not math.isfinite(self.poll_interval_seconds)
            or self.poll_interval_seconds < 0.0
        ):
            raise InvalidNotificationError("poll_interval_seconds must be >= 0")
        if (
            isinstance(self.recovery_interval_seconds, bool)
            or not isinstance(self.recovery_interval_seconds, (int, float))
            or not math.isfinite(self.recovery_interval_seconds)
            or self.recovery_interval_seconds <= 0.0
        ):
            raise InvalidNotificationError("recovery_interval_seconds must be > 0")
        if self.claim_min_idle_ms is not None and (
            isinstance(self.claim_min_idle_ms, bool)
            or not isinstance(self.claim_min_idle_ms, int)
            or self.claim_min_idle_ms < 0
        ):
            raise InvalidNotificationError("claim_min_idle_ms must be a non-negative int or None")
        if (
            isinstance(self.max_dedup_entries, bool)
            or not isinstance(self.max_dedup_entries, int)
            or self.max_dedup_entries < 0
        ):
            raise InvalidNotificationError("max_dedup_entries must be a non-negative int")
        validated_cats = tuple(
            validate_category(cat) for cat in self.authoritative_promotional_categories
        )
        object.__setattr__(self, "authoritative_promotional_categories", validated_cats)


@dataclass(frozen=True, slots=True)
class NotificationsWorkerMetricsSnapshot:
    """Bounded-cardinality telemetry snapshot for N3 durable worker operations."""

    durable_accepted: int
    worker_processed: int
    worker_acked: int
    worker_reclaimed: int
    worker_deduplicated: int
    worker_dead_lettered: int
    worker_dlq_failed: int
    worker_retryable_failures: int
    redis_failures: int


@runtime_checkable
class NotificationsDurableTelemetrySink(Protocol):
    """Telemetry sink protocol for N3 durable worker counters."""

    def record_durable_accepted(self) -> None: ...

    def record_processed(self) -> None: ...

    def record_acked(self, count: int = 1) -> None: ...

    def record_reclaimed(self, count: int = 1) -> None: ...

    def record_deduplicated(self) -> None: ...

    def record_dead_lettered(self, count: int = 1) -> None: ...

    def record_dlq_failed(self) -> None: ...

    def record_retryable_failure(self) -> None: ...

    def record_redis_failure(self) -> None: ...

    def snapshot(self) -> NotificationsWorkerMetricsSnapshot: ...


class InMemoryNotificationsDurableTelemetrySink:
    """In-memory bounded-cardinality N3 worker telemetry sink."""

    __slots__ = (
        "_durable_accepted",
        "_worker_processed",
        "_worker_acked",
        "_worker_reclaimed",
        "_worker_deduplicated",
        "_worker_dead_lettered",
        "_worker_dlq_failed",
        "_worker_retryable_failures",
        "_redis_failures",
    )

    def __init__(self) -> None:
        self._durable_accepted: int = 0
        self._worker_processed: int = 0
        self._worker_acked: int = 0
        self._worker_reclaimed: int = 0
        self._worker_deduplicated: int = 0
        self._worker_dead_lettered: int = 0
        self._worker_dlq_failed: int = 0
        self._worker_retryable_failures: int = 0
        self._redis_failures: int = 0

    def record_durable_accepted(self) -> None:
        self._durable_accepted += 1

    def record_processed(self) -> None:
        self._worker_processed += 1

    def record_acked(self, count: int = 1) -> None:
        self._worker_acked += max(0, count)

    def record_reclaimed(self, count: int = 1) -> None:
        self._worker_reclaimed += max(0, count)

    def record_deduplicated(self) -> None:
        self._worker_deduplicated += 1

    def record_dead_lettered(self, count: int = 1) -> None:
        self._worker_dead_lettered += max(0, count)

    def record_dlq_failed(self) -> None:
        self._worker_dlq_failed += 1

    def record_retryable_failure(self) -> None:
        self._worker_retryable_failures += 1

    def record_redis_failure(self) -> None:
        self._redis_failures += 1

    def snapshot(self) -> NotificationsWorkerMetricsSnapshot:
        return NotificationsWorkerMetricsSnapshot(
            durable_accepted=self._durable_accepted,
            worker_processed=self._worker_processed,
            worker_acked=self._worker_acked,
            worker_reclaimed=self._worker_reclaimed,
            worker_deduplicated=self._worker_deduplicated,
            worker_dead_lettered=self._worker_dead_lettered,
            worker_dlq_failed=self._worker_dlq_failed,
            worker_retryable_failures=self._worker_retryable_failures,
            redis_failures=self._redis_failures,
        )


class NotificationsDurablePublisher:
    """Durable notification publisher (`NotificationPublisherPort`) backed by `DurableEventStorePort`.

    Persists validated, authorized notifications to a bounded ingestion stream via
    `DurableEventStorePort.append()` (`XADD`) and returns `DURABLE_ACCEPTED` only
    after Redis confirms an entry ID. Does not publish directly to `EventBrokerPort`,
    leaving live dispatch ownership to `NotificationsDurableWorker`.
    """

    __slots__ = (
        "_durable_store",
        "_producer_authorizer",
        "_telemetry",
        "_durable_telemetry",
        "_ingestion_topic",
        "_clock",
        "_closed",
    )

    def __init__(
        self,
        *,
        durable_store: DurableEventStorePort,
        producer_authorizer: NotificationProducerAuthorizerPort | None = None,
        telemetry: NotificationsTelemetrySink | None = None,
        durable_telemetry: NotificationsDurableTelemetrySink | None = None,
        ingestion_topic: str = DEFAULT_DURABLE_INGESTION_TOPIC,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        if not isinstance(durable_store, DurableEventStorePort):
            raise TypeError(
                "durable_store must implement DurableEventStorePort, "
                f"got {type(durable_store).__name__}"
            )
        effective_authorizer: NotificationProducerAuthorizerPort = (
            producer_authorizer if producer_authorizer is not None else DenyAllProducerAuthorizer()
        )
        if not isinstance(effective_authorizer, NotificationProducerAuthorizerPort):
            raise TypeError(
                "producer_authorizer must implement NotificationProducerAuthorizerPort, "
                f"got {type(effective_authorizer).__name__}"
            )
        effective_telemetry: NotificationsTelemetrySink = (
            telemetry if telemetry is not None else InMemoryNotificationsTelemetrySink()
        )
        effective_durable_telemetry: NotificationsDurableTelemetrySink = (
            durable_telemetry
            if durable_telemetry is not None
            else InMemoryNotificationsDurableTelemetrySink()
        )
        try:
            canonical_topic = validate_topic(ingestion_topic)
        except ValueError as exc:
            raise InvalidNotificationError(str(exc)) from exc

        self._durable_store: DurableEventStorePort = durable_store
        self._producer_authorizer: NotificationProducerAuthorizerPort = effective_authorizer
        self._telemetry: NotificationsTelemetrySink = effective_telemetry
        self._durable_telemetry: NotificationsDurableTelemetrySink = effective_durable_telemetry
        self._ingestion_topic: str = canonical_topic
        self._clock: Callable[[], datetime] = (
            clock if clock is not None else lambda: datetime.now(UTC)
        )
        self._closed: bool = False

    @property
    def is_closed(self) -> bool:
        return self._closed

    async def close(self) -> None:
        self._closed = True

    async def publish(
        self,
        notification: NotificationEnvelope,
        *,
        producer: VerifiedPrincipalContext | None = None,
    ) -> NotificationPublishResult:
        """Authorize, validate, and durably persist a notification via `DurableEventStorePort`."""
        if self._closed:
            self._telemetry.record_error(NotificationsErrorCategory.CLOSED)
            raise NotificationsModuleClosedError("NotificationsDurablePublisher is closed")

        raw_notification: Any = notification
        if not isinstance(raw_notification, NotificationEnvelope):
            self._telemetry.record_error(NotificationsErrorCategory.VALIDATION)
            raise InvalidNotificationError(
                f"notification must be a NotificationEnvelope, got {type(raw_notification).__name__}"
            )
        raw_producer: Any = producer
        if raw_producer is not None and not isinstance(raw_producer, VerifiedPrincipalContext):
            self._telemetry.record_error(NotificationsErrorCategory.AUTHORIZATION)
            raise NotificationAuthorizationError(
                f"producer must be VerifiedPrincipalContext or None, got {type(raw_producer).__name__}"
            )

        try:
            await self._producer_authorizer.authorize_publish(
                producer=producer,
                notification=notification,
            )
        except NotificationAuthorizationError:
            self._telemetry.record_error(NotificationsErrorCategory.AUTHORIZATION)
            raise
        except Exception as exc:
            self._telemetry.record_error(NotificationsErrorCategory.AUTHORIZATION)
            raise NotificationAuthorizationError("Producer authorization failed") from exc

        if producer is not None and producer.tenant_id != notification.tenant_id:
            self._telemetry.record_error(NotificationsErrorCategory.AUTHORIZATION)
            raise NotificationAuthorizationError(
                "Producer tenant_id does not match notification tenant_id"
            )

        if notification.is_expired(now=self._clock()):
            self._telemetry.record_expired()
            self._telemetry.record_error(NotificationsErrorCategory.EXPIRATION)
            raise NotificationExpiredError(
                f"Notification {notification.notification_id!r} has expired"
            )

        try:
            event_envelope = EventEnvelope(
                protocol_version=EVENTS_PROTOCOL_VERSION,
                event_id=notification.notification_id,
                event_type=NOTIFICATIONS_SCHEMA_VERSION,
                topic=self._ingestion_topic,
                timestamp=notification.created_at,
                tenant_id=notification.tenant_id,
                principal_id=notification.recipient_id,
                payload=notification.to_dict(),
                correlation_id=notification.correlation_id,
            )
        except OversizedEventError as exc:
            self._telemetry.record_error(NotificationsErrorCategory.VALIDATION)
            raise OversizedNotificationError(str(exc)) from exc
        except InvalidEventError as exc:
            self._telemetry.record_error(NotificationsErrorCategory.VALIDATION)
            raise InvalidNotificationError(str(exc)) from exc

        _validate_delivery_frame_size(event_envelope)

        try:
            sequence_id = await self._durable_store.append(event_envelope)
        except EventsModuleClosedError as exc:
            self._telemetry.record_error(NotificationsErrorCategory.CLOSED)
            raise NotificationsModuleClosedError(str(exc)) from exc
        except (EventsBackendUnavailableError, TimeoutError, OSError) as exc:
            self._telemetry.record_error(NotificationsErrorCategory.BACKEND_UNAVAILABLE)
            self._durable_telemetry.record_redis_failure()
            raise NotificationsBackendUnavailableError(
                "Durable store append failed or timed out; acceptance is ambiguous"
            ) from exc
        except EventsError as exc:
            self._telemetry.record_error(NotificationsErrorCategory.BACKEND_UNAVAILABLE)
            self._durable_telemetry.record_redis_failure()
            raise NotificationsBackendUnavailableError(str(exc)) from exc

        if not isinstance(sequence_id, str) or not sequence_id:
            self._telemetry.record_error(NotificationsErrorCategory.BACKEND_UNAVAILABLE)
            self._durable_telemetry.record_redis_failure()
            raise NotificationsBackendUnavailableError(
                "Durable store did not return a confirmed stream sequence_id"
            )

        self._durable_telemetry.record_durable_accepted()
        return NotificationPublishResult(
            notification_id=notification.notification_id,
            status=NotificationStatus.DURABLE_ACCEPTED,
            recipient_topic=notification.recipient_topic(),
            dispatched_connections=0,
            sequence_id=sequence_id,
        )


class NotificationsDurableWorker:
    """Bounded Redis Streams consumer & recovery worker for N3 Notifications.

    Processes a single `(tenant_id, topic, group, consumer)` stream without
    scanning Redis keys or spawning per-recipient workers. Maintains at-least-once
    processing within Redis stream retention; process-local bounded deduplication
    keyed by `(tenant_id, recipient_id, notification_id)` is best-effort only and
    is not crash-persistent.
    """

    __slots__ = (
        "_config",
        "_durable_store",
        "_broker",
        "_preference_port",
        "_telemetry",
        "_durable_telemetry",
        "_clock",
        "_dedup_seen",
        "_consume_task",
        "_recovery_task",
        "_closed",
    )

    def __init__(
        self,
        *,
        config: NotificationsDurableConfig,
        durable_store: DurableEventStorePort,
        broker: EventBrokerPort,
        preference_port: NotificationPreferencePort,
        telemetry: NotificationsTelemetrySink | None = None,
        durable_telemetry: NotificationsDurableTelemetrySink | None = None,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        if not isinstance(config, NotificationsDurableConfig):
            raise TypeError(
                f"config must be NotificationsDurableConfig, got {type(config).__name__}"
            )
        if not isinstance(durable_store, DurableEventStorePort):
            raise TypeError(
                "durable_store must implement DurableEventStorePort, "
                f"got {type(durable_store).__name__}"
            )
        if not isinstance(broker, EventBrokerPort):
            raise TypeError(f"broker must implement EventBrokerPort, got {type(broker).__name__}")
        if not isinstance(preference_port, NotificationPreferencePort):
            raise TypeError(
                "preference_port must implement NotificationPreferencePort, "
                f"got {type(preference_port).__name__}"
            )

        self._config: NotificationsDurableConfig = config
        self._durable_store: DurableEventStorePort = durable_store
        self._broker: EventBrokerPort = broker
        self._preference_port: NotificationPreferencePort = preference_port
        self._telemetry: NotificationsTelemetrySink = (
            telemetry if telemetry is not None else InMemoryNotificationsTelemetrySink()
        )
        self._durable_telemetry: NotificationsDurableTelemetrySink = (
            durable_telemetry
            if durable_telemetry is not None
            else InMemoryNotificationsDurableTelemetrySink()
        )
        self._clock: Callable[[], datetime] = (
            clock if clock is not None else lambda: datetime.now(UTC)
        )
        self._dedup_seen: OrderedDict[tuple[str, str, str], None] = OrderedDict()
        self._consume_task: asyncio.Task[None] | None = None
        self._recovery_task: asyncio.Task[None] | None = None
        self._closed: bool = False

    @property
    def config(self) -> NotificationsDurableConfig:
        return self._config

    @property
    def telemetry(self) -> NotificationsTelemetrySink:
        return self._telemetry

    @property
    def durable_telemetry(self) -> NotificationsDurableTelemetrySink:
        return self._durable_telemetry

    @property
    def is_running(self) -> bool:
        return (self._consume_task is not None and not self._consume_task.done()) or (
            self._recovery_task is not None and not self._recovery_task.done()
        )

    @property
    def is_closed(self) -> bool:
        return self._closed

    def _remember_dedup(self, key: tuple[str, str, str]) -> None:
        limit = self._config.max_dedup_entries
        if limit <= 0:
            return
        if key in self._dedup_seen:
            self._dedup_seen.move_to_end(key)
            return
        self._dedup_seen[key] = None
        while len(self._dedup_seen) > limit:
            self._dedup_seen.popitem(last=False)

    async def _ack_record(self, record_id: str) -> bool:
        try:
            acked = await self._durable_store.ack(
                tenant_id=self._config.tenant_id,
                topic=self._config.topic,
                group=self._config.group,
                record_ids=[record_id],
            )
        except (EventsBackendUnavailableError, EventsError, TimeoutError, OSError):
            self._telemetry.record_error(NotificationsErrorCategory.BACKEND_UNAVAILABLE)
            self._durable_telemetry.record_redis_failure()
            return False
        self._durable_telemetry.record_acked(acked)
        return acked > 0

    async def _quarantine_poison(
        self,
        *,
        record: DurableEventRecord,
        reason: str,
        delivery_attempts: int = 1,
    ) -> bool:
        record_id = record.sequence_id
        if not record_id:
            return False
        if isinstance(self._durable_store, DeadLetterCapableStore):
            try:
                await self._durable_store.dead_letter(
                    tenant_id=self._config.tenant_id,
                    topic=self._config.topic,
                    group=self._config.group,
                    record_id=record_id,
                    reason=reason,
                    envelope=record.envelope,
                    delivery_attempts=delivery_attempts,
                )
            except (EventsBackendUnavailableError, EventsError, TimeoutError, OSError):
                self._telemetry.record_error(NotificationsErrorCategory.BACKEND_UNAVAILABLE)
                self._durable_telemetry.record_dlq_failed()
                self._durable_telemetry.record_redis_failure()
                return False
            self._durable_telemetry.record_dead_lettered(1)
            return True

        # Fallback for stores without dead_letter(): do NOT silently ack without DLQ.
        self._durable_telemetry.record_dlq_failed()
        return False

    async def process_record(
        self,
        record: DurableEventRecord,
        *,
        delivery_attempts: int = 1,
    ) -> bool:
        """Process one stream record and XACK only after terminal outcome or DLQ write."""
        record_id = record.sequence_id
        if not record_id:
            return False

        self._durable_telemetry.record_processed()

        # 1. Tenant boundary check against configured worker tenant.
        if record.envelope.tenant_id != self._config.tenant_id:
            self._telemetry.record_error(NotificationsErrorCategory.AUTHORIZATION)
            return await self._quarantine_poison(
                record=record,
                reason="tenant_mismatch",
                delivery_attempts=delivery_attempts,
            )

        # 2. Parse & validate inner NotificationEnvelope (poison payloads go to DLQ).
        try:
            payload_data: Mapping[str, Any] = MappingProxyType(dict(record.envelope.payload))
            notif = NotificationEnvelope.from_dict(payload_data)
        except (
            InvalidNotificationError,
            UnsupportedNotificationVersionError,
            OversizedNotificationError,
            KeyError,
            TypeError,
            ValueError,
        ):
            self._telemetry.record_error(NotificationsErrorCategory.VALIDATION)
            return await self._quarantine_poison(
                record=record,
                reason="invalid_notification_envelope",
                delivery_attempts=delivery_attempts,
            )

        if notif.tenant_id != self._config.tenant_id:
            self._telemetry.record_error(NotificationsErrorCategory.AUTHORIZATION)
            return await self._quarantine_poison(
                record=record,
                reason="notification_tenant_mismatch",
                delivery_attempts=delivery_attempts,
            )

        dedup_key = (notif.tenant_id, notif.recipient_id, notif.notification_id)

        # 3. Bounded in-memory deduplication check (non-crash-persistent).
        if self._config.max_dedup_entries > 0 and dedup_key in self._dedup_seen:
            self._durable_telemetry.record_deduplicated()
            return await self._ack_record(record_id)

        # 4. Expiration check (terminally discarded and acknowledged).
        if notif.is_expired(now=self._clock()):
            self._telemetry.record_expired()
            return await self._ack_record(record_id)

        # 5. Preference lookup & evaluation (backend failure is retryable -> NO XACK).
        try:
            preference = await self._preference_port.get_preferences(
                tenant_id=notif.tenant_id,
                recipient_id=notif.recipient_id,
            )
            eval_result = evaluate_notification_preference(
                notif,
                preference,
                authoritative_promotional_categories=self._config.authoritative_promotional_categories,
                channel_id="in_app",
            )
        except Exception:
            self._telemetry.record_error(NotificationsErrorCategory.PREFERENCE)
            self._durable_telemetry.record_retryable_failure()
            return False

        if not eval_result.allowed:
            self._telemetry.record_filtered_by_preference()
            return await self._ack_record(record_id)

        # 6. Construct live Events envelope & validate final delivery frame size.
        recipient_topic = notif.recipient_topic()
        try:
            live_envelope = EventEnvelope(
                protocol_version=EVENTS_PROTOCOL_VERSION,
                event_id=notif.notification_id,
                event_type=NOTIFICATIONS_SCHEMA_VERSION,
                topic=recipient_topic,
                timestamp=notif.created_at,
                tenant_id=notif.tenant_id,
                principal_id=notif.recipient_id,
                payload=notif.to_dict(),
                correlation_id=notif.correlation_id,
            )
            _validate_delivery_frame_size(live_envelope)
        except (InvalidNotificationError, OversizedNotificationError, InvalidEventError):
            self._telemetry.record_error(NotificationsErrorCategory.VALIDATION)
            return await self._quarantine_poison(
                record=record,
                reason="oversized_or_invalid_delivery_frame",
                delivery_attempts=delivery_attempts,
            )

        # 7. Dispatch via N2 live broker (0 active connections is a valid terminal outcome).
        try:
            pub_res = await self._broker.publish(live_envelope)
        except (
            EventsBackendUnavailableError,
            EventBackpressureError,
            EventRateLimitError,
            EventsModuleClosedError,
            TimeoutError,
            OSError,
        ):
            self._telemetry.record_error(NotificationsErrorCategory.BACKEND_UNAVAILABLE)
            self._durable_telemetry.record_retryable_failure()
            self._durable_telemetry.record_redis_failure()
            return False

        self._remember_dedup(dedup_key)
        self._telemetry.record_published()
        if pub_res.delivered_subscribers > 0:
            self._telemetry.record_dispatched(matched_connections=pub_res.delivered_subscribers)

        # 8. Issue XACK strictly after Stage 2+3 terminal outcome.
        return await self._ack_record(record_id)

    async def run_once(self) -> int:
        """Read one bounded batch via `XREADGROUP` and process records."""
        try:
            records = await self._durable_store.read_group(
                tenant_id=self._config.tenant_id,
                topic=self._config.topic,
                group=self._config.group,
                consumer=self._config.consumer,
                count=self._config.read_batch_size,
                block_ms=self._config.block_ms,
            )
        except (EventsBackendUnavailableError, EventsError, TimeoutError, OSError) as exc:
            self._telemetry.record_error(NotificationsErrorCategory.BACKEND_UNAVAILABLE)
            self._durable_telemetry.record_redis_failure()
            raise NotificationsBackendUnavailableError(
                "Durable notification read_group failed"
            ) from exc

        acked = 0
        for rec in records:
            if await self.process_record(rec):
                acked += 1
        return acked

    async def recover_once(self, *, min_idle_ms: int | None = None) -> int:
        """Run one bounded `XPENDING`/`XAUTOCLAIM` recovery pass."""
        if not isinstance(self._durable_store, RecoveryCapableStore):
            return 0
        effective_idle = min_idle_ms if min_idle_ms is not None else self._config.claim_min_idle_ms
        try:
            recovery = await self._durable_store.reclaim_pending(
                tenant_id=self._config.tenant_id,
                topic=self._config.topic,
                group=self._config.group,
                consumer=self._config.consumer,
                min_idle_ms=effective_idle,
                count=self._config.read_batch_size,
            )
        except (EventsBackendUnavailableError, EventsError, TimeoutError, OSError) as exc:
            self._telemetry.record_error(NotificationsErrorCategory.BACKEND_UNAVAILABLE)
            self._durable_telemetry.record_redis_failure()
            raise NotificationsBackendUnavailableError(
                "Durable notification reclaim_pending failed"
            ) from exc

        reclaimed_records = getattr(recovery, "reclaimed", ())
        dead_lettered = getattr(recovery, "dead_lettered", ())
        if reclaimed_records:
            self._durable_telemetry.record_reclaimed(len(reclaimed_records))
        if dead_lettered:
            self._durable_telemetry.record_dead_lettered(len(dead_lettered))

        acked = 0
        for rec in reclaimed_records:
            if await self.process_record(rec):
                acked += 1
        return acked

    async def _consume_loop(self) -> None:
        while not self._closed:
            try:
                await self.run_once()
            except asyncio.CancelledError:
                raise
            except Exception:
                pass
            if self._config.poll_interval_seconds > 0.0:
                await asyncio.sleep(self._config.poll_interval_seconds)

    async def _recovery_loop(self) -> None:
        while not self._closed:
            await asyncio.sleep(self._config.recovery_interval_seconds)
            try:
                await self.recover_once()
            except asyncio.CancelledError:
                raise
            except Exception:
                pass

    def start(self) -> None:
        """Start bounded consumer and recovery tasks; rejects restart after shutdown."""
        if self._closed:
            raise NotificationsModuleClosedError(
                "Cannot restart NotificationsDurableWorker after shutdown"
            )
        if self._consume_task is None or self._consume_task.done():
            self._consume_task = asyncio.create_task(
                self._consume_loop(), name="gw-notif-durable-worker"
            )
        if isinstance(self._durable_store, RecoveryCapableStore) and (
            self._recovery_task is None or self._recovery_task.done()
        ):
            self._recovery_task = asyncio.create_task(
                self._recovery_loop(), name="gw-notif-durable-recovery"
            )

    async def stop(self) -> None:
        """Idempotently cancel and await worker tasks with zero leaked tasks."""
        self._closed = True
        tasks = [t for t in (self._consume_task, self._recovery_task) if t is not None]
        self._consume_task = None
        self._recovery_task = None
        for task in tasks:
            task.cancel()
        for task in tasks:
            with contextlib.suppress(asyncio.CancelledError):
                await task


__all__ = [
    "DEFAULT_DURABLE_BLOCK_MS",
    "DEFAULT_DURABLE_CONSUMER",
    "DEFAULT_DURABLE_DEDUP_MAX_ENTRIES",
    "DEFAULT_DURABLE_GROUP",
    "DEFAULT_DURABLE_INGESTION_TOPIC",
    "DEFAULT_DURABLE_MAX_BATCH_SIZE",
    "DEFAULT_DURABLE_MAX_BLOCK_MS",
    "DEFAULT_DURABLE_READ_BATCH_SIZE",
    "DeadLetterCapableStore",
    "InMemoryNotificationsDurableTelemetrySink",
    "NotificationsDurableConfig",
    "NotificationsDurablePublisher",
    "NotificationsDurableTelemetrySink",
    "NotificationsDurableWorker",
    "NotificationsWorkerMetricsSnapshot",
    "RecoveryCapableStore",
]
