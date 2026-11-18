"""Unit and loopback `/ws/events/v1` integration tests for Notifications N2."""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from typing import Any

import pytest
from starlette.testclient import TestClient
from starlette.websockets import WebSocketDisconnect

from kaiwen_gateway.core import (
    CoreConfig,
    CoreConfigurationError,
    ProviderPrincipal,
    TenantPrincipalBinding,
    VerifiedPrincipalContext,
)
from kaiwen_gateway.modules.events import (
    EVENTS_PROTOCOL_VERSION,
    EventAuthorizationError,
    EventBrokerConfig,
    EventEnvelope,
    EventsBackendUnavailableError,
    EventsModule,
    InMemoryEventBroker,
    SlowConsumerPolicy,
)
from kaiwen_gateway.modules.events.redis import (
    RedisClientConfig,
    RedisPubSubBrokerConfig,
    RedisPubSubEventBroker,
)
from kaiwen_gateway.modules.notifications import (
    NOTIFICATIONS_SCHEMA_VERSION,
    DenyAllProducerAuthorizer,
    InMemoryNotificationPreferenceStore,
    InMemoryNotificationsTelemetrySink,
    InvalidNotificationError,
    NotificationAuthorizationError,
    NotificationEnvelope,
    NotificationExpiredError,
    NotificationPreference,
    NotificationPriority,
    NotificationsBackendUnavailableError,
    NotificationsDispatcher,
    NotificationsErrorCategory,
    NotificationsModule,
    NotificationsModuleClosedError,
    NotificationsPreferenceError,
    NotificationStatus,
    NotificationsTopicAuthorizer,
    OversizedNotificationError,
    derive_recipient_topic,
    generate_notification_id,
    is_protected_notification_pattern,
)
from kaiwen_gateway.server.app import create_gateway_app


class _TokenMapAuthenticator:
    def __init__(self, token_map: dict[str, tuple[str, str, frozenset[str]]]) -> None:
        self._token_map = token_map

    async def authenticate_bearer(self, *, token: str, connection_id: str) -> ProviderPrincipal:
        if token not in self._token_map:
            raise EventAuthorizationError("Invalid bearer token")
        return ProviderPrincipal(provider_id=f"idp:{token}")


class _TokenMapAuthorizer:
    def __init__(self, token_map: dict[str, tuple[str, str, frozenset[str]]]) -> None:
        self._by_provider = {f"idp:{tok}": spec for tok, spec in token_map.items()}

    async def authorize_principal(
        self, *, principal: ProviderPrincipal, connection_id: str
    ) -> TenantPrincipalBinding:
        spec = self._by_provider.get(principal.provider_id)
        if spec is None:
            raise EventAuthorizationError("Unknown principal")
        tenant_id, principal_id, scopes = spec
        return TenantPrincipalBinding(
            authorized_provider_id=principal.provider_id,
            tenant_id=tenant_id,
            principal_id=principal_id,
            allowed_scopes=scopes,
        )


class _AllowSameTenantAuthorizer:
    def __init__(self, *, allowed_recipients: set[str] | None = None) -> None:
        self._allowed_recipients = allowed_recipients

    async def authorize_publish(
        self,
        *,
        producer: VerifiedPrincipalContext | None,
        notification: NotificationEnvelope,
    ) -> None:
        if producer is not None and producer.tenant_id != notification.tenant_id:
            raise NotificationAuthorizationError("Cross-tenant publish denied")
        if (
            self._allowed_recipients is not None
            and notification.recipient_id not in self._allowed_recipients
        ):
            raise NotificationAuthorizationError("Producer cannot target recipient")


def _make_notification(
    *,
    tenant_id: str = "tenant-alpha",
    recipient_id: str = "user-a",
    category: str = "system",
    notification_type: str = "task.completed",
    priority: NotificationPriority = NotificationPriority.NORMAL,
    payload: dict[str, Any] | None = None,
    is_promotional: bool = False,
    created_at: str = "2026-11-18T12:00:00Z",
    expires_at: str | None = None,
    correlation_id: str | None = "corr-123",
    notification_id: str | None = None,
) -> NotificationEnvelope:
    return NotificationEnvelope(
        notification_id=notification_id or generate_notification_id(),
        schema_version=NOTIFICATIONS_SCHEMA_VERSION,
        tenant_id=tenant_id,
        recipient_id=recipient_id,
        category=category,
        notification_type=notification_type,
        priority=priority,
        payload=payload if payload is not None else {"title": "Task done"},
        created_at=created_at,
        expires_at=expires_at,
        is_promotional=is_promotional,
        correlation_id=correlation_id,
    )


def _make_producer(
    *,
    tenant_id: str = "tenant-alpha",
    principal_id: str = "svc-notifier",
    scopes: frozenset[str] = frozenset({"notifications:publish"}),
) -> VerifiedPrincipalContext:
    return VerifiedPrincipalContext(
        provider_id="svc-idp",
        tenant_id=tenant_id,
        connection_id="conn_prod_1",
        principal_id=principal_id,
        allowed_scopes=scopes,
    )


def _build_notifications_app(
    *,
    token_map: dict[str, tuple[str, str, frozenset[str]]],
    preference_store: InMemoryNotificationPreferenceStore | None = None,
    producer_authorizer: Any = None,
    authoritative_promotional_categories: tuple[str, ...] = ("marketing",),
    broker_config: EventBrokerConfig | None = None,
) -> Any:
    prefs = preference_store or InMemoryNotificationPreferenceStore()
    authz = producer_authorizer or _AllowSameTenantAuthorizer()
    topic_authorizer = NotificationsTopicAuthorizer()
    notif_module = NotificationsModule(
        preference_port=prefs,
        producer_authorizer=authz,
    )
    events_module = EventsModule(
        broker_config=broker_config,
        topic_authorizer=topic_authorizer,
    )
    return create_gateway_app(
        CoreConfig(events_ingress_mode="authenticated"),
        enable_events=True,
        events_authenticator=_TokenMapAuthenticator(token_map),
        events_authorizer=_TokenMapAuthorizer(token_map),
        events_module=events_module,
        enable_notifications=True,
        notifications_module=notif_module,
        events_topic_authorizer=topic_authorizer,
        notifications_authoritative_promotional_categories=authoritative_promotional_categories,
    )


def test_startup_fails_closed_without_mandatory_authorization_config() -> None:
    token_map = {"tok-a": ("tenant-alpha", "user-a", frozenset({"events:subscribe"}))}
    with pytest.raises(CoreConfigurationError):
        create_gateway_app(
            CoreConfig(events_ingress_mode="authenticated"),
            enable_events=True,
            events_authenticator=_TokenMapAuthenticator(token_map),
            events_authorizer=_TokenMapAuthorizer(token_map),
            enable_notifications=True,
        )

    with pytest.raises(CoreConfigurationError):
        create_gateway_app(
            CoreConfig(events_ingress_mode="disabled"),
            enable_events=False,
            enable_notifications=True,
            events_topic_authorizer=NotificationsTopicAuthorizer(),
        )

    with pytest.raises(CoreConfigurationError):
        create_gateway_app(
            CoreConfig(events_ingress_mode="authenticated"),
            enable_events=True,
            events_authenticator=_TokenMapAuthenticator(token_map),
            events_authorizer=_TokenMapAuthorizer(token_map),
            events_module=EventsModule(),
            enable_notifications=True,
            events_topic_authorizer=NotificationsTopicAuthorizer(),
        )

    # Auto-created EventsModule wires events_topic_authorizer into broker
    topic_auth = NotificationsTopicAuthorizer()
    auto_app = create_gateway_app(
        CoreConfig(events_ingress_mode="authenticated"),
        enable_events=True,
        events_authenticator=_TokenMapAuthenticator(token_map),
        events_authorizer=_TokenMapAuthorizer(token_map),
        enable_notifications=True,
        events_topic_authorizer=topic_auth,
    )
    assert auto_app.state.events_module.broker.topic_authorizer is topic_auth

    async def _check_direct_broker_guard() -> None:
        broker = auto_app.state.events_module.broker
        await broker.startup()
        try:
            ctx = VerifiedPrincipalContext(
                provider_id="idp",
                tenant_id="tenant-alpha",
                connection_id="conn_direct_1",
                principal_id="user-b",
                allowed_scopes=frozenset({"events:subscribe", "events:subscribe:wildcard"}),
            )
            with pytest.raises(EventAuthorizationError):
                await broker.subscribe(ctx, derive_recipient_topic("tenant-alpha", "user-a"))
            with pytest.raises(EventAuthorizationError):
                await broker.subscribe(ctx, "notifications.*")
        finally:
            await broker.shutdown()

    asyncio.run(_check_direct_broker_guard())


def test_is_protected_notification_pattern_cases() -> None:
    assert is_protected_notification_pattern("*") is True
    assert is_protected_notification_pattern("notifications.*") is True
    assert is_protected_notification_pattern("notifications.r.*") is True
    assert is_protected_notification_pattern("notifications.r.abcdef0123456789abcdef01") is True
    assert is_protected_notification_pattern("orders.created") is False
    assert is_protected_notification_pattern("orders.*") is False
    assert is_protected_notification_pattern("notifications.system") is False


def test_authorized_live_delivery_single_and_multi_session() -> None:
    token_map = {
        "tok-a1": ("tenant-alpha", "user-a", frozenset({"events:subscribe"})),
        "tok-a2": ("tenant-alpha", "user-a", frozenset({"events:subscribe"})),
    }
    app = _build_notifications_app(token_map=token_map)
    self_topic = derive_recipient_topic("tenant-alpha", "user-a")
    notif = _make_notification(
        tenant_id="tenant-alpha",
        recipient_id="user-a",
        payload={"message": "Build succeeded", "count": 3},
        correlation_id="corr-live-1",
    )
    producer = _make_producer(tenant_id="tenant-alpha")

    with TestClient(app) as client:
        assert client.get("/ready").status_code == 200
        with (
            client.websocket_connect(
                "/ws/events/v1", headers={"Authorization": "Bearer tok-a1"}
            ) as ws1,
            client.websocket_connect(
                "/ws/events/v1", headers={"Authorization": "Bearer tok-a2"}
            ) as ws2,
        ):
            assert ws1.receive_json()["type"] == "session.ready"
            assert ws2.receive_json()["type"] == "session.ready"

            ws1.send_json(
                {
                    "protocol_version": EVENTS_PROTOCOL_VERSION,
                    "type": "events.subscribe",
                    "topic_pattern": self_topic,
                    "request_id": "sub-1",
                }
            )
            assert ws1.receive_json()["type"] == "events.subscribed"

            ws2.send_json(
                {
                    "protocol_version": EVENTS_PROTOCOL_VERSION,
                    "type": "events.subscribe",
                    "topic_pattern": self_topic,
                    "request_id": "sub-2",
                }
            )
            assert ws2.receive_json()["type"] == "events.subscribed"

            dispatcher: NotificationsDispatcher = app.state.notifications_dispatcher
            result = asyncio.run(dispatcher.publish(notif, producer=producer))

            assert result.notification_id == notif.notification_id
            assert result.status == NotificationStatus.DISPATCHED_TO_BROKER
            assert result.recipient_topic == self_topic
            assert result.dispatched_connections == 2

            frame1 = ws1.receive_json()
            frame2 = ws2.receive_json()
            for frame in (frame1, frame2):
                assert frame["type"] == "events.delivery"
                evt = frame["event"]
                assert evt["event_type"] == NOTIFICATIONS_SCHEMA_VERSION
                assert evt["event_id"] == notif.notification_id
                assert evt["correlation_id"] == "corr-live-1"
                assert evt["topic"] == self_topic
                assert evt["payload"]["notification_id"] == notif.notification_id
                assert evt["payload"]["payload"] == {"message": "Build succeeded", "count": 3}


def test_recipient_isolation_and_wildcard_denial_over_ws() -> None:
    token_map = {
        "tok-a": ("tenant-alpha", "user-a", frozenset({"events:subscribe"})),
        "tok-b": (
            "tenant-alpha",
            "user-b",
            frozenset({"events:subscribe", "events:subscribe:wildcard"}),
        ),
        "tok-tenant-b-admin": (
            "tenant-beta",
            "admin-b",
            frozenset({"events:subscribe", "notifications:subscribe:tenant"}),
        ),
        "tok-tenant-a-admin": (
            "tenant-alpha",
            "admin-a",
            frozenset(
                {
                    "events:subscribe",
                    "events:subscribe:wildcard",
                    "notifications:subscribe:tenant",
                }
            ),
        ),
    }
    app = _build_notifications_app(token_map=token_map)
    topic_a = derive_recipient_topic("tenant-alpha", "user-a")
    notif = _make_notification(tenant_id="tenant-alpha", recipient_id="user-a")
    producer = _make_producer(tenant_id="tenant-alpha")

    with TestClient(app) as client:
        with (
            client.websocket_connect(
                "/ws/events/v1", headers={"Authorization": "Bearer tok-b"}
            ) as ws_b,
            client.websocket_connect(
                "/ws/events/v1", headers={"Authorization": "Bearer tok-tenant-b-admin"}
            ) as ws_b_admin,
            client.websocket_connect(
                "/ws/events/v1", headers={"Authorization": "Bearer tok-tenant-a-admin"}
            ) as ws_a_admin,
        ):
            assert ws_b.receive_json()["type"] == "session.ready"
            assert ws_b_admin.receive_json()["type"] == "session.ready"
            assert ws_a_admin.receive_json()["type"] == "session.ready"

            # User B with general events:subscribe + events:subscribe:wildcard is denied
            # for User A's topic, notifications.r.*, and notifications.*.
            for idx, bad_pattern in enumerate((topic_a, "notifications.r.*", "notifications.*")):
                ws_b.send_json(
                    {
                        "protocol_version": EVENTS_PROTOCOL_VERSION,
                        "type": "events.subscribe",
                        "topic_pattern": bad_pattern,
                        "request_id": f"bad-{idx}",
                    }
                )
                err = ws_b.receive_json()
                assert err["type"] == "events.error"
                assert err["code"] == "FORBIDDEN_SUBSCRIPTION"

            # Non-protected general Events topic still works for User B
            ws_b.send_json(
                {
                    "protocol_version": EVENTS_PROTOCOL_VERSION,
                    "type": "events.subscribe",
                    "topic_pattern": "orders.*",
                    "request_id": "ok-orders",
                }
            )
            assert ws_b.receive_json()["type"] == "events.subscribed"

            # Tenant B admin can subscribe within Tenant B, but never receives Tenant A events
            ws_b_admin.send_json(
                {
                    "protocol_version": EVENTS_PROTOCOL_VERSION,
                    "type": "events.subscribe",
                    "topic_pattern": topic_a,
                    "request_id": "admin-b-sub",
                }
            )
            assert ws_b_admin.receive_json()["type"] == "events.subscribed"

            # Tenant A admin can subscribe within Tenant A and receives Tenant A notification
            ws_a_admin.send_json(
                {
                    "protocol_version": EVENTS_PROTOCOL_VERSION,
                    "type": "events.subscribe",
                    "topic_pattern": "notifications.r.*",
                    "request_id": "admin-a-sub",
                }
            )
            assert ws_a_admin.receive_json()["type"] == "events.subscribed"

            dispatcher: NotificationsDispatcher = app.state.notifications_dispatcher
            result = asyncio.run(dispatcher.publish(notif, producer=producer))
            assert result.dispatched_connections == 1

            delivery = ws_a_admin.receive_json()
            assert delivery["type"] == "events.delivery"
            assert delivery["event"]["event_id"] == notif.notification_id

            # Verify Tenant B admin did not receive Tenant A's notification (ping returns pong directly)
            ws_b_admin.send_json(
                {
                    "protocol_version": EVENTS_PROTOCOL_VERSION,
                    "type": "session.ping",
                    "seq": 1,
                }
            )
            assert ws_b_admin.receive_json()["type"] == "session.pong"


def test_unauthenticated_and_forged_identity_rejected() -> None:
    token_map = {"tok-a": ("tenant-alpha", "user-a", frozenset({"events:subscribe"}))}
    app = _build_notifications_app(token_map=token_map)

    with TestClient(app) as client:
        with pytest.raises(WebSocketDisconnect) as exc_info:
            with client.websocket_connect("/ws/events/v1"):
                pass
        assert exc_info.value.code == 1008

        with client.websocket_connect(
            "/ws/events/v1", headers={"Authorization": "Bearer tok-a"}
        ) as ws:
            assert ws.receive_json()["type"] == "session.ready"
            ws.send_json(
                {
                    "protocol_version": EVENTS_PROTOCOL_VERSION,
                    "type": "events.subscribe",
                    "topic_pattern": derive_recipient_topic("tenant-alpha", "user-a"),
                    "tenant_id": "tenant-beta",
                    "principal_id": "admin",
                }
            )
            with pytest.raises(WebSocketDisconnect) as forged_exc:
                ws.receive_json()
            assert forged_exc.value.code == 1008


def test_topic_authorizer_denies_bare_star_and_invalid_context() -> None:
    authorizer = NotificationsTopicAuthorizer()
    ctx = VerifiedPrincipalContext(
        provider_id="idp",
        tenant_id="tenant-alpha",
        connection_id="conn_1",
        principal_id="user-a",
        allowed_scopes=frozenset({"events:subscribe", "events:subscribe:wildcard"}),
    )
    with pytest.raises(EventAuthorizationError):
        asyncio.run(authorizer.authorize_subscribe(context=ctx, topic_pattern="*"))

    with pytest.raises(EventAuthorizationError):
        asyncio.run(
            authorizer.authorize_subscribe(
                context="not-a-context",  # type: ignore[arg-type]
                topic_pattern="orders.created",
            )
        )


def test_producer_isolation_and_promotional_trust_boundary() -> None:
    async def _run() -> None:
        broker = InMemoryEventBroker(topic_authorizer=NotificationsTopicAuthorizer())
        await broker.startup()
        try:
            prefs = InMemoryNotificationPreferenceStore()
            telemetry = InMemoryNotificationsTelemetrySink()

            # 1. Default DenyAllProducerAuthorizer fails closed
            deny_dispatcher = NotificationsDispatcher(
                broker=broker,
                preference_port=prefs,
                producer_authorizer=DenyAllProducerAuthorizer(),
                telemetry=telemetry,
            )
            notif = _make_notification(tenant_id="tenant-alpha", recipient_id="user-a")
            with pytest.raises(NotificationAuthorizationError):
                await deny_dispatcher.publish(notif, producer=_make_producer())

            # 2. Target-restricted producer authorizer denies targeting user-b
            restricted_dispatcher = NotificationsDispatcher(
                broker=broker,
                preference_port=prefs,
                producer_authorizer=_AllowSameTenantAuthorizer(allowed_recipients={"user-a"}),
                telemetry=telemetry,
                authoritative_promotional_categories=("marketing",),
            )
            notif_b = _make_notification(tenant_id="tenant-alpha", recipient_id="user-b")
            with pytest.raises(NotificationAuthorizationError):
                await restricted_dispatcher.publish(notif_b, producer=_make_producer())

            # 3. Cross-tenant producer denied even if custom authorizer were permissive
            class _PermissiveAuthorizer:
                async def authorize_publish(
                    self,
                    *,
                    producer: VerifiedPrincipalContext | None,
                    notification: NotificationEnvelope,
                ) -> None:
                    return None

            cross_tenant_dispatcher = NotificationsDispatcher(
                broker=broker,
                preference_port=prefs,
                producer_authorizer=_PermissiveAuthorizer(),
                telemetry=telemetry,
                authoritative_promotional_categories=("marketing",),
            )
            with pytest.raises(NotificationAuthorizationError):
                await cross_tenant_dispatcher.publish(
                    notif,
                    producer=_make_producer(tenant_id="tenant-beta"),
                )

            # 4. Untrusted producer setting is_promotional=False on authoritative promo category
            spoofed_promo = _make_notification(
                tenant_id="tenant-alpha",
                recipient_id="user-a",
                category="marketing",
                is_promotional=False,
            )
            promo_res = await restricted_dispatcher.publish(
                spoofed_promo,
                producer=_make_producer(tenant_id="tenant-alpha"),
            )
            assert promo_res.status == NotificationStatus.FILTERED_BY_PREFERENCE
            assert promo_res.dispatched_connections == 0
            assert telemetry.snapshot().notifications_filtered_by_preference == 1
        finally:
            await broker.shutdown()

    asyncio.run(_run())


def test_expiration_preferences_offline_oversized_backend_failure_and_shutdown() -> None:
    async def _run() -> None:
        broker = InMemoryEventBroker(topic_authorizer=NotificationsTopicAuthorizer())
        await broker.startup()
        prefs = InMemoryNotificationPreferenceStore()
        telemetry = InMemoryNotificationsTelemetrySink()
        fixed_now = datetime(2026, 11, 18, 12, 30, 0, tzinfo=UTC)

        dispatcher = NotificationsDispatcher(
            broker=broker,
            preference_port=prefs,
            producer_authorizer=_AllowSameTenantAuthorizer(),
            telemetry=telemetry,
            authoritative_promotional_categories=("marketing",),
            clock=lambda: fixed_now,
        )

        # Offline recipient -> DISPATCHED_TO_BROKER with dispatched_connections == 0
        offline_notif = _make_notification(tenant_id="tenant-alpha", recipient_id="user-offline")
        res_offline = await dispatcher.publish(offline_notif, producer=_make_producer())
        assert res_offline.status == NotificationStatus.DISPATCHED_TO_BROKER
        assert res_offline.dispatched_connections == 0

        # Expired notification -> NotificationExpiredError, not published
        expired_notif = _make_notification(
            tenant_id="tenant-alpha",
            recipient_id="user-a",
            created_at="2026-11-18T12:00:00Z",
            expires_at="2026-11-18T12:15:00Z",
        )
        with pytest.raises(NotificationExpiredError):
            await dispatcher.publish(expired_notif, producer=_make_producer())
        assert telemetry.snapshot().notifications_expired == 1

        # Preference filters: muted category, min_priority, disabled in_app channel
        prefs.set_preference(
            NotificationPreference(
                tenant_id="tenant-alpha",
                recipient_id="user-pref",
                muted_categories=frozenset({"noisy"}),
                min_priority=NotificationPriority.HIGH,
                disabled_channels=frozenset(),
                allow_promotional=False,
            )
        )
        muted = _make_notification(
            tenant_id="tenant-alpha",
            recipient_id="user-pref",
            category="noisy",
            priority=NotificationPriority.URGENT,
        )
        assert (
            await dispatcher.publish(muted, producer=_make_producer())
        ).status == NotificationStatus.FILTERED_BY_PREFERENCE

        low_prio = _make_notification(
            tenant_id="tenant-alpha",
            recipient_id="user-pref",
            category="system",
            priority=NotificationPriority.NORMAL,
        )
        assert (
            await dispatcher.publish(low_prio, producer=_make_producer())
        ).status == NotificationStatus.FILTERED_BY_PREFERENCE

        prefs.set_preference(
            NotificationPreference(
                tenant_id="tenant-alpha",
                recipient_id="user-disabled",
                disabled_channels=frozenset({"in_app"}),
            )
        )
        disabled_ch = _make_notification(tenant_id="tenant-alpha", recipient_id="user-disabled")
        assert (
            await dispatcher.publish(disabled_ch, producer=_make_producer())
        ).status == NotificationStatus.FILTERED_BY_PREFERENCE

        # Preference-store failure fails closed
        prefs.set_fail_lookups(True)
        with pytest.raises(NotificationsPreferenceError):
            await dispatcher.publish(offline_notif, producer=_make_producer())
        prefs.set_fail_lookups(False)

        # Broker backend failure fails closed
        class _FailingBroker(InMemoryEventBroker):
            async def publish(self, envelope: EventEnvelope) -> Any:
                raise EventsBackendUnavailableError("Redis unavailable")

        failing_broker = _FailingBroker()
        await failing_broker.startup()
        failing_dispatcher = NotificationsDispatcher(
            broker=failing_broker,
            preference_port=prefs,
            producer_authorizer=_AllowSameTenantAuthorizer(),
            telemetry=telemetry,
        )
        with pytest.raises(NotificationsBackendUnavailableError):
            await failing_dispatcher.publish(offline_notif, producer=_make_producer())
        await failing_broker.shutdown()

        # Invalid notification input rejected
        with pytest.raises(InvalidNotificationError):
            await dispatcher.publish("invalid")  # type: ignore[arg-type]

        # Shutdown rejects further publishes
        await dispatcher.close()
        with pytest.raises(NotificationsModuleClosedError):
            await dispatcher.publish(offline_notif, producer=_make_producer())
        assert telemetry.snapshot().errors_by_category[NotificationsErrorCategory.CLOSED] >= 1
        await broker.shutdown()

    asyncio.run(_run())


def test_oversized_final_frame_rejected_before_broker_publish(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import kaiwen_gateway.modules.notifications.dispatcher as dispatcher_mod

    async def _run() -> None:
        broker = InMemoryEventBroker(topic_authorizer=NotificationsTopicAuthorizer())
        await broker.startup()
        try:
            prefs = InMemoryNotificationPreferenceStore()
            dispatcher = NotificationsDispatcher(
                broker=broker,
                preference_port=prefs,
                producer_authorizer=_AllowSameTenantAuthorizer(),
            )
            notif = _make_notification(
                tenant_id="tenant-alpha",
                recipient_id="user-a",
                payload={"data": "x" * 2048},
            )
            # Lower final-frame cap to verify Step 8 catches oversized final delivery frame
            # even when inner NotificationEnvelope was within its 48 KiB limit.
            monkeypatch.setattr(dispatcher_mod, "DEFAULT_MAX_EVENT_BYTES", 1024)
            with pytest.raises(OversizedNotificationError):
                await dispatcher.publish(notif, producer=_make_producer())
        finally:
            await broker.shutdown()

    asyncio.run(_run())


def test_slow_consumer_disconnect_isolated_from_healthy_recipient() -> None:
    token_map = {
        "tok-a": ("tenant-alpha", "user-a", frozenset({"events:subscribe"})),
        "tok-c": ("tenant-alpha", "user-c", frozenset({"events:subscribe"})),
    }
    app = _build_notifications_app(
        token_map=token_map,
        broker_config=EventBrokerConfig(
            max_queued_events_per_connection=2,
            slow_consumer_policy=SlowConsumerPolicy.DISCONNECT_SLOW_CONSUMER,
        ),
    )
    topic_a = derive_recipient_topic("tenant-alpha", "user-a")
    topic_c = derive_recipient_topic("tenant-alpha", "user-c")
    producer = _make_producer(tenant_id="tenant-alpha")

    with TestClient(app) as client:
        with (
            client.websocket_connect(
                "/ws/events/v1", headers={"Authorization": "Bearer tok-a"}
            ) as ws_slow,
            client.websocket_connect(
                "/ws/events/v1", headers={"Authorization": "Bearer tok-c"}
            ) as ws_healthy,
        ):
            assert ws_slow.receive_json()["type"] == "session.ready"
            assert ws_healthy.receive_json()["type"] == "session.ready"

            ws_slow.send_json(
                {
                    "protocol_version": EVENTS_PROTOCOL_VERSION,
                    "type": "events.subscribe",
                    "topic_pattern": topic_a,
                    "request_id": "sub-slow",
                }
            )
            assert ws_slow.receive_json()["type"] == "events.subscribed"

            ws_healthy.send_json(
                {
                    "protocol_version": EVENTS_PROTOCOL_VERSION,
                    "type": "events.subscribe",
                    "topic_pattern": topic_c,
                    "request_id": "sub-healthy",
                }
            )
            assert ws_healthy.receive_json()["type"] == "events.subscribed"

            dispatcher: NotificationsDispatcher = app.state.notifications_dispatcher
            # Flood slow consumer queue (> max_queued_events_per_connection=2)
            for idx in range(5):
                asyncio.run(
                    dispatcher.publish(
                        _make_notification(
                            tenant_id="tenant-alpha",
                            recipient_id="user-a",
                            payload={"seq": idx},
                        ),
                        producer=producer,
                    )
                )

            # Healthy recipient C still receives its own notification unaffected
            notif_c = _make_notification(
                tenant_id="tenant-alpha",
                recipient_id="user-c",
                payload={"msg": "healthy"},
            )
            res_c = asyncio.run(dispatcher.publish(notif_c, producer=producer))
            assert res_c.dispatched_connections == 1
            delivered = ws_healthy.receive_json()
            assert delivered["type"] == "events.delivery"
            assert delivered["event"]["event_id"] == notif_c.notification_id


class _PermissiveTopicAuthorizer:
    async def authorize_subscribe(
        self,
        *,
        context: VerifiedPrincipalContext,
        topic_pattern: str,
    ) -> None:
        return None


def test_permissive_topic_authorizer_cannot_bypass_recipient_isolation() -> None:
    token_map = {
        "tok-b": (
            "tenant-alpha",
            "user-b",
            frozenset({"events:subscribe", "events:subscribe:wildcard"}),
        ),
    }
    app = create_gateway_app(
        CoreConfig(events_ingress_mode="authenticated"),
        enable_events=True,
        events_authenticator=_TokenMapAuthenticator(token_map),
        events_authorizer=_TokenMapAuthorizer(token_map),
        enable_notifications=True,
        events_topic_authorizer=_PermissiveTopicAuthorizer(),
    )
    topic_a = derive_recipient_topic("tenant-alpha", "user-a")

    with TestClient(app) as client:
        with client.websocket_connect(
            "/ws/events/v1", headers={"Authorization": "Bearer tok-b"}
        ) as ws_b:
            assert ws_b.receive_json()["type"] == "session.ready"
            for idx, pattern in enumerate((topic_a, "notifications.r.*", "notifications.*")):
                ws_b.send_json(
                    {
                        "protocol_version": EVENTS_PROTOCOL_VERSION,
                        "type": "events.subscribe",
                        "topic_pattern": pattern,
                        "request_id": f"bypass-{idx}",
                    }
                )
                err = ws_b.receive_json()
                assert err["type"] == "events.error"
                assert err["code"] == "FORBIDDEN_SUBSCRIPTION"

    async def _check_direct_broker() -> None:
        app_direct = create_gateway_app(
            CoreConfig(events_ingress_mode="authenticated"),
            enable_events=True,
            events_authenticator=_TokenMapAuthenticator(token_map),
            events_authorizer=_TokenMapAuthorizer(token_map),
            enable_notifications=True,
            events_topic_authorizer=_PermissiveTopicAuthorizer(),
        )
        broker = app_direct.state.events_module.broker
        await broker.startup()
        try:
            ctx = VerifiedPrincipalContext(
                provider_id="idp",
                tenant_id="tenant-alpha",
                connection_id="conn_perm_1",
                principal_id="user-b",
                allowed_scopes=frozenset({"events:subscribe", "events:subscribe:wildcard"}),
            )
            with pytest.raises(EventAuthorizationError):
                await broker.subscribe(ctx, topic_a)
            with pytest.raises(EventAuthorizationError):
                await broker.subscribe(ctx, "notifications.*")
        finally:
            await broker.shutdown()

    asyncio.run(_check_direct_broker())

    # Explicit EventsModule with permissive topic_authorizer fails closed at startup
    with pytest.raises(CoreConfigurationError):
        create_gateway_app(
            CoreConfig(events_ingress_mode="authenticated"),
            enable_events=True,
            events_authenticator=_TokenMapAuthenticator(token_map),
            events_authorizer=_TokenMapAuthorizer(token_map),
            events_module=EventsModule(topic_authorizer=_PermissiveTopicAuthorizer()),
            enable_notifications=True,
            events_topic_authorizer=_PermissiveTopicAuthorizer(),
        )


def test_host_additional_policy_cannot_bypass_but_can_further_restrict() -> None:
    class _HostDenyOrdersSecret:
        async def authorize_subscribe(
            self,
            *,
            context: VerifiedPrincipalContext,
            topic_pattern: str,
        ) -> None:
            if topic_pattern == "orders.secret":
                raise EventAuthorizationError("Host policy denies orders.secret")

    token_map = {
        "tok-a": ("tenant-alpha", "user-a", frozenset({"events:subscribe"})),
    }
    app = create_gateway_app(
        CoreConfig(events_ingress_mode="authenticated"),
        enable_events=True,
        events_authenticator=_TokenMapAuthenticator(token_map),
        events_authorizer=_TokenMapAuthorizer(token_map),
        enable_notifications=True,
        events_topic_authorizer=_HostDenyOrdersSecret(),
    )
    self_topic = derive_recipient_topic("tenant-alpha", "user-a")
    other_topic = derive_recipient_topic("tenant-alpha", "user-b")

    with TestClient(app) as client:
        with client.websocket_connect(
            "/ws/events/v1", headers={"Authorization": "Bearer tok-a"}
        ) as ws:
            assert ws.receive_json()["type"] == "session.ready"
            # Self notification topic allowed
            ws.send_json(
                {
                    "protocol_version": EVENTS_PROTOCOL_VERSION,
                    "type": "events.subscribe",
                    "topic_pattern": self_topic,
                    "request_id": "self-ok",
                }
            )
            assert ws.receive_json()["type"] == "events.subscribed"

            # Cross-recipient denied by mandatory NotificationsTopicAuthorizer
            ws.send_json(
                {
                    "protocol_version": EVENTS_PROTOCOL_VERSION,
                    "type": "events.subscribe",
                    "topic_pattern": other_topic,
                    "request_id": "other-deny",
                }
            )
            assert ws.receive_json()["code"] == "FORBIDDEN_SUBSCRIPTION"

            # Additional host policy restriction enforced
            ws.send_json(
                {
                    "protocol_version": EVENTS_PROTOCOL_VERSION,
                    "type": "events.subscribe",
                    "topic_pattern": "orders.secret",
                    "request_id": "host-deny",
                }
            )
            assert ws.receive_json()["code"] == "FORBIDDEN_SUBSCRIPTION"


def test_subscription_scope_consistency_inmemory_and_redis_broker() -> None:
    async def _run() -> None:
        in_mem = InMemoryEventBroker(topic_authorizer=NotificationsTopicAuthorizer())
        await in_mem.startup()

        class _StubRedis:
            def pubsub(self) -> Any:
                raise AssertionError("pubsub not needed for auth-rejection tests")

            async def aclose(self) -> None:
                return None

        redis_broker = RedisPubSubEventBroker(
            config=RedisPubSubBrokerConfig(
                redis=RedisClientConfig(url="redis://127.0.0.1:6379/0"),
                broker=EventBrokerConfig(enable_rate_limiting=False),
            ),
            redis_client=_StubRedis(),
            topic_authorizer=NotificationsTopicAuthorizer(),
        )

        self_topic = derive_recipient_topic("tenant-alpha", "user-a")
        other_topic = derive_recipient_topic("tenant-alpha", "user-b")

        # 1. notifications:subscribe:self without events:subscribe is denied by broker
        self_only_ctx = VerifiedPrincipalContext(
            provider_id="idp",
            tenant_id="tenant-alpha",
            connection_id="conn_s1",
            principal_id="user-a",
            allowed_scopes=frozenset({"notifications:subscribe:self"}),
        )
        # 2. notifications:subscribe:tenant standalone: allowed for notifications.r.*,
        #    denied for non-notification topics
        tenant_admin_ctx = VerifiedPrincipalContext(
            provider_id="idp",
            tenant_id="tenant-alpha",
            connection_id="conn_t1",
            principal_id="admin-a",
            allowed_scopes=frozenset({"notifications:subscribe:tenant"}),
        )
        # 3. events:subscribe + events:subscribe:wildcard denied for cross-recipient & wildcards
        wildcard_ctx = VerifiedPrincipalContext(
            provider_id="idp",
            tenant_id="tenant-alpha",
            connection_id="conn_w1",
            principal_id="user-a",
            allowed_scopes=frozenset({"events:subscribe", "events:subscribe:wildcard"}),
        )

        try:
            for broker in (in_mem, redis_broker):
                with pytest.raises(EventAuthorizationError):
                    await broker.subscribe(self_only_ctx, self_topic)
                with pytest.raises(EventAuthorizationError):
                    await broker.subscribe(tenant_admin_ctx, "orders.created")
                with pytest.raises(EventAuthorizationError):
                    await broker.subscribe(wildcard_ctx, other_topic)
                with pytest.raises(EventAuthorizationError):
                    await broker.subscribe(wildcard_ctx, "notifications.r.*")
                with pytest.raises(EventAuthorizationError):
                    await broker.subscribe(wildcard_ctx, "notifications.*")

            # Standalone notifications:subscribe:tenant succeeds on InMemoryEventBroker
            sub = await in_mem.subscribe(tenant_admin_ctx, "notifications.r.*")
            assert sub.topic_pattern == "notifications.r.*"
        finally:
            await in_mem.shutdown()

    asyncio.run(_run())


def test_frame_size_serialization_parity_with_starlette_send_json() -> None:
    import json

    from kaiwen_gateway.transports.events.protocol import (
        EVENTS_MAX_FRAME_BYTES,
        build_delivery_frame,
    )

    notif = _make_notification(
        tenant_id="tenant-alpha",
        recipient_id="user-a",
        payload={"unicode": "こんにちは🚀", "padding": "a" * 40_000},
    )
    evt = EventEnvelope(
        protocol_version=EVENTS_PROTOCOL_VERSION,
        event_id=notif.notification_id,
        event_type=NOTIFICATIONS_SCHEMA_VERSION,
        topic=notif.recipient_topic(),
        timestamp=notif.created_at,
        tenant_id=notif.tenant_id,
        principal_id=notif.recipient_id,
        payload=notif.to_dict(),
        correlation_id=notif.correlation_id,
    )
    frame = build_delivery_frame(evt)
    # Match Starlette WebSocket.send_json() exact formatting:
    starlette_wire = json.dumps(frame, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
    dispatcher_wire = json.dumps(
        {
            "protocol_version": EVENTS_PROTOCOL_VERSION,
            "type": "events.delivery",
            "event": evt.to_dict(),
        },
        allow_nan=False,
        separators=(",", ":"),
        ensure_ascii=False,
    ).encode("utf-8")
    assert starlette_wire == dispatcher_wire
    assert len(starlette_wire) <= EVENTS_MAX_FRAME_BYTES
