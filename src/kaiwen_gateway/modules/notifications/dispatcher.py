"""Real-time NotificationsDispatcher publishing `notifications.v1` over `EventBrokerPort` (N2)."""

from __future__ import annotations

import json
from collections.abc import Callable, Collection
from datetime import UTC, datetime
from typing import Any

from kaiwen_gateway.core.identity import VerifiedPrincipalContext
from kaiwen_gateway.modules.events.domain import (
    DEFAULT_MAX_EVENT_BYTES,
    EVENTS_PROTOCOL_VERSION,
    EventAuthorizationError,
    EventBackpressureError,
    EventEnvelope,
    EventRateLimitError,
    EventsBackendUnavailableError,
    EventsError,
    EventsModuleClosedError,
    InvalidEventError,
    OversizedEventError,
)
from kaiwen_gateway.modules.events.ports import EventBrokerPort
from kaiwen_gateway.modules.notifications.domain import (
    NOTIFICATIONS_SCHEMA_VERSION,
    InvalidNotificationError,
    NotificationAuthorizationError,
    NotificationEnvelope,
    NotificationExpiredError,
    NotificationsBackendUnavailableError,
    NotificationsModuleClosedError,
    NotificationsPreferenceError,
    NotificationStatus,
    OversizedNotificationError,
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


class NotificationsDispatcher:
    """Real-time notification dispatcher implementing `NotificationPublisherPort` (N2).

    Enforces fail-closed producer authorization, tenant boundary checks,
    authoritative promotional classification, recipient preference filtering,
    expiration rejection, final serialized `events.delivery` frame-size bounds,
    and best-effort at-most-once live dispatch via `EventBrokerPort`.
    """

    __slots__ = (
        "_broker",
        "_preference_port",
        "_producer_authorizer",
        "_telemetry",
        "_authoritative_promotional_categories",
        "_clock",
        "_closed",
    )

    def __init__(
        self,
        *,
        broker: EventBrokerPort,
        preference_port: NotificationPreferencePort,
        producer_authorizer: NotificationProducerAuthorizerPort | None = None,
        telemetry: NotificationsTelemetrySink | None = None,
        authoritative_promotional_categories: Collection[str] = (),
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        if not isinstance(broker, EventBrokerPort):
            raise TypeError(f"broker must implement EventBrokerPort, got {type(broker).__name__}")
        if not isinstance(preference_port, NotificationPreferencePort):
            raise TypeError(
                "preference_port must implement NotificationPreferencePort, "
                f"got {type(preference_port).__name__}"
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
        if not isinstance(effective_telemetry, NotificationsTelemetrySink):
            raise TypeError(
                "telemetry must implement NotificationsTelemetrySink, "
                f"got {type(effective_telemetry).__name__}"
            )

        self._broker: EventBrokerPort = broker
        self._preference_port: NotificationPreferencePort = preference_port
        self._producer_authorizer: NotificationProducerAuthorizerPort = effective_authorizer
        self._telemetry: NotificationsTelemetrySink = effective_telemetry
        self._authoritative_promotional_categories: frozenset[str] = frozenset(
            validate_category(cat) for cat in authoritative_promotional_categories
        )
        self._clock: Callable[[], datetime] = (
            clock if clock is not None else lambda: datetime.now(UTC)
        )
        self._closed: bool = False

    @property
    def is_closed(self) -> bool:
        """Return True after `close()` has been called."""
        return self._closed

    @property
    def telemetry(self) -> NotificationsTelemetrySink:
        """Return bounded-cardinality Notifications telemetry sink."""
        return self._telemetry

    async def close(self) -> None:
        """Idempotently close the dispatcher against further publications."""
        self._closed = True

    async def publish(
        self,
        notification: NotificationEnvelope,
        *,
        producer: VerifiedPrincipalContext | None = None,
    ) -> NotificationPublishResult:
        """Validate, authorize, filter, and dispatch a live notification via `EventBrokerPort`."""
        if self._closed:
            self._telemetry.record_error(NotificationsErrorCategory.CLOSED)
            raise NotificationsModuleClosedError("NotificationsDispatcher is closed")

        # 1. Validate notification schema/type and producer context type.
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

        # 2. Verify producer authorization fail-closed before any side effect.
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

        # 3. Enforce producer/notification tenant boundary.
        if producer is not None and producer.tenant_id != notification.tenant_id:
            self._telemetry.record_error(NotificationsErrorCategory.AUTHORIZATION)
            raise NotificationAuthorizationError(
                "Producer tenant_id does not match notification tenant_id"
            )

        # 4 & 5. Query preferences and apply authoritative promotional classification.
        try:
            preference = await self._preference_port.get_preferences(
                tenant_id=notification.tenant_id,
                recipient_id=notification.recipient_id,
            )
        except NotificationsPreferenceError:
            self._telemetry.record_error(NotificationsErrorCategory.PREFERENCE)
            raise
        except Exception as exc:
            self._telemetry.record_error(NotificationsErrorCategory.PREFERENCE)
            raise NotificationsPreferenceError("Recipient preference lookup failed") from exc

        try:
            eval_result = evaluate_notification_preference(
                notification,
                preference,
                authoritative_promotional_categories=self._authoritative_promotional_categories,
                channel_id="in_app",
            )
        except NotificationsPreferenceError:
            self._telemetry.record_error(NotificationsErrorCategory.PREFERENCE)
            raise

        recipient_topic = notification.recipient_topic()

        if not eval_result.allowed:
            self._telemetry.record_filtered_by_preference()
            return NotificationPublishResult(
                notification_id=notification.notification_id,
                status=NotificationStatus.FILTERED_BY_PREFERENCE,
                recipient_topic=recipient_topic,
                dispatched_connections=0,
                sequence_id=None,
            )

        # 6. Reject expired notifications.
        if notification.is_expired(now=self._clock()):
            self._telemetry.record_expired()
            self._telemetry.record_error(NotificationsErrorCategory.EXPIRATION)
            raise NotificationExpiredError(
                f"Notification {notification.notification_id!r} has expired"
            )

        # 7. Construct compatible Events envelope preserving identifiers.
        try:
            event_envelope = EventEnvelope(
                protocol_version=EVENTS_PROTOCOL_VERSION,
                event_id=notification.notification_id,
                event_type=NOTIFICATIONS_SCHEMA_VERSION,
                topic=recipient_topic,
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

        # 8. Validate final serialized `events.delivery` WebSocket frame size.
        delivery_frame = {
            "protocol_version": EVENTS_PROTOCOL_VERSION,
            "type": "events.delivery",
            "event": event_envelope.to_dict(),
        }
        try:
            serialized_frame = json.dumps(
                delivery_frame,
                allow_nan=False,
                separators=(",", ":"),
                ensure_ascii=False,
            ).encode("utf-8")
        except (TypeError, ValueError, UnicodeEncodeError) as exc:
            self._telemetry.record_error(NotificationsErrorCategory.VALIDATION)
            raise InvalidNotificationError(
                f"Final notification delivery frame is not JSON serializable: {exc}"
            ) from exc

        if len(serialized_frame) > DEFAULT_MAX_EVENT_BYTES:
            self._telemetry.record_error(NotificationsErrorCategory.VALIDATION)
            raise OversizedNotificationError(
                f"Final serialized events.delivery frame ({len(serialized_frame)} bytes) "
                f"exceeds maximum {DEFAULT_MAX_EVENT_BYTES} bytes"
            )

        # 9. Publish through existing EventBrokerPort.
        try:
            publish_result = await self._broker.publish(event_envelope)
        except EventsModuleClosedError as exc:
            self._telemetry.record_error(NotificationsErrorCategory.CLOSED)
            raise NotificationsModuleClosedError(str(exc)) from exc
        except (
            EventsBackendUnavailableError,
            EventBackpressureError,
            EventRateLimitError,
        ) as exc:
            self._telemetry.record_error(NotificationsErrorCategory.BACKEND_UNAVAILABLE)
            raise NotificationsBackendUnavailableError(str(exc)) from exc
        except EventAuthorizationError as exc:
            self._telemetry.record_error(NotificationsErrorCategory.AUTHORIZATION)
            raise NotificationAuthorizationError(str(exc)) from exc
        except OversizedEventError as exc:
            self._telemetry.record_error(NotificationsErrorCategory.VALIDATION)
            raise OversizedNotificationError(str(exc)) from exc
        except InvalidEventError as exc:
            self._telemetry.record_error(NotificationsErrorCategory.VALIDATION)
            raise InvalidNotificationError(str(exc)) from exc
        except EventsError as exc:
            self._telemetry.record_error(NotificationsErrorCategory.INTERNAL)
            raise NotificationsBackendUnavailableError(str(exc)) from exc

        # 10. Record telemetry and return honest broker-dispatch result.
        dispatched_count = publish_result.delivered_subscribers
        self._telemetry.record_published()
        if dispatched_count > 0:
            self._telemetry.record_dispatched(matched_connections=dispatched_count)

        return NotificationPublishResult(
            notification_id=notification.notification_id,
            status=NotificationStatus.DISPATCHED_TO_BROKER,
            recipient_topic=recipient_topic,
            dispatched_connections=dispatched_count,
            sequence_id=None,
        )


__all__ = ["NotificationsDispatcher"]
