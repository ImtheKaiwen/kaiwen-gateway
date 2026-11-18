"""Opt-in live-Redis multi-instance integration & failure-injection tests for Notifications N4.

All tests are skipped unless ``KAIWEN_TEST_REDIS_URL`` is set to a reachable Redis server.
"""

from __future__ import annotations

import asyncio
import os
import uuid
from typing import Any

import pytest
from starlette.testclient import TestClient

from kaiwen_gateway.core import (
    CoreConfig,
    ProviderPrincipal,
    TenantPrincipalBinding,
    VerifiedPrincipalContext,
)
from kaiwen_gateway.modules.events import (
    EVENTS_PROTOCOL_VERSION,
    EventAuthorizationError,
    EventBrokerConfig,
    EventEnvelope,
    EventsModule,
    SlowConsumerPolicy,
)
from kaiwen_gateway.modules.events.redis import (
    RedisClientConfig,
    RedisPubSubBrokerConfig,
    RedisPubSubEventBroker,
    RedisStreamsConfig,
    RedisStreamsDurableStore,
)
from kaiwen_gateway.modules.notifications import (
    NOTIFICATIONS_SCHEMA_VERSION,
    InMemoryNotificationPreferenceStore,
    InMemoryNotificationsDurableTelemetrySink,
    InMemoryNotificationsTelemetrySink,
    NotificationEnvelope,
    NotificationPriority,
    NotificationsDurableConfig,
    NotificationsDurablePublisher,
    NotificationsDurableWorker,
    NotificationsModule,
    NotificationsTopicAuthorizer,
    derive_recipient_topic,
    generate_notification_id,
)
from kaiwen_gateway.server.app import create_gateway_app

pytestmark = pytest.mark.skipif(
    not os.environ.get("KAIWEN_TEST_REDIS_URL"),
    reason="Opt-in live Redis N4 multi-instance tests require KAIWEN_TEST_REDIS_URL",
)


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


class _AllowAllProducerAuthorizer:
    async def authorize_publish(
        self,
        *,
        producer: VerifiedPrincipalContext | None,
        notification: NotificationEnvelope,
    ) -> None:
        return None


def _prefix() -> str:
    return f"kwn4{uuid.uuid4().hex[:10]}"


def _redis_url() -> str:
    return os.environ["KAIWEN_TEST_REDIS_URL"]


def _build_instance_app(
    *,
    prefix: str,
    token_map: dict[str, tuple[str, str, frozenset[str]]],
    broker_config: EventBrokerConfig | None = None,
) -> Any:
    redis_cfg = RedisClientConfig(url=_redis_url(), key_prefix=prefix)
    pubsub_cfg = RedisPubSubBrokerConfig(
        redis=redis_cfg,
        broker=broker_config or EventBrokerConfig(enable_rate_limiting=False),
    )
    topic_auth = NotificationsTopicAuthorizer()
    broker = RedisPubSubEventBroker(pubsub_cfg, topic_authorizer=topic_auth)
    events_mod = EventsModule(broker=broker)
    notif_mod = NotificationsModule(
        preference_port=InMemoryNotificationPreferenceStore(),
        producer_authorizer=_AllowAllProducerAuthorizer(),
    )
    return create_gateway_app(
        CoreConfig(events_ingress_mode="authenticated"),
        enable_events=True,
        events_authenticator=_TokenMapAuthenticator(token_map),
        events_authorizer=_TokenMapAuthorizer(token_map),
        events_module=events_mod,
        enable_notifications=True,
        notifications_module=notif_mod,
        events_topic_authorizer=topic_auth,
    )


def _make_notif(
    *,
    tenant_id: str = "tenant-alpha",
    recipient_id: str = "user-a",
    msg: str = "n4-live",
) -> NotificationEnvelope:
    return NotificationEnvelope(
        notification_id=generate_notification_id(),
        schema_version=NOTIFICATIONS_SCHEMA_VERSION,
        tenant_id=tenant_id,
        recipient_id=recipient_id,
        category="system",
        notification_type="task.completed",
        priority=NotificationPriority.NORMAL,
        payload={"msg": msg},
        created_at="2026-11-18T12:00:00Z",
        correlation_id="corr-n4",
    )


def test_n4_two_instance_durable_ingestion_pubsub_ws_delivery_and_isolation() -> None:
    prefix = _prefix()
    token_map = {
        "tok-a": ("tenant-alpha", "user-a", frozenset({"events:subscribe"})),
        "tok-b": (
            "tenant-alpha",
            "user-b",
            frozenset({"events:subscribe", "events:subscribe:wildcard"}),
        ),
        "tok-c": ("tenant-beta", "user-a", frozenset({"events:subscribe"})),
    }
    app1 = _build_instance_app(prefix=prefix, token_map=token_map)
    app2 = _build_instance_app(prefix=prefix, token_map=token_map)

    redis_cfg = RedisClientConfig(url=_redis_url(), key_prefix=prefix)
    streams_cfg = RedisStreamsConfig(redis=redis_cfg)
    store = RedisStreamsDurableStore(streams_cfg)
    topic_a_alpha = derive_recipient_topic("tenant-alpha", "user-a")
    topic_a_beta = derive_recipient_topic("tenant-beta", "user-a")
    notif = _make_notif(tenant_id="tenant-alpha", recipient_id="user-a", msg="cross-instance")

    with TestClient(app1) as c1, TestClient(app2) as c2:
        with (
            c2.websocket_connect(
                "/ws/events/v1", headers={"Authorization": "Bearer tok-a"}
            ) as ws_a_inst2,
            c1.websocket_connect(
                "/ws/events/v1", headers={"Authorization": "Bearer tok-b"}
            ) as ws_b_inst1,
            c2.websocket_connect(
                "/ws/events/v1", headers={"Authorization": "Bearer tok-c"}
            ) as ws_c_inst2,
        ):
            assert ws_a_inst2.receive_json()["type"] == "session.ready"
            assert ws_b_inst1.receive_json()["type"] == "session.ready"
            assert ws_c_inst2.receive_json()["type"] == "session.ready"

            ws_a_inst2.send_json(
                {
                    "protocol_version": EVENTS_PROTOCOL_VERSION,
                    "type": "events.subscribe",
                    "topic_pattern": topic_a_alpha,
                    "request_id": "sub-a",
                }
            )
            assert ws_a_inst2.receive_json()["type"] == "events.subscribed"

            # Same-tenant user-b cannot subscribe to user-a's topic on Instance 1
            ws_b_inst1.send_json(
                {
                    "protocol_version": EVENTS_PROTOCOL_VERSION,
                    "type": "events.subscribe",
                    "topic_pattern": topic_a_alpha,
                    "request_id": "sub-b-deny",
                }
            )
            assert ws_b_inst1.receive_json()["code"] == "FORBIDDEN_SUBSCRIPTION"

            # Cross-tenant user-a in tenant-beta subscribes to own tenant-beta topic
            ws_c_inst2.send_json(
                {
                    "protocol_version": EVENTS_PROTOCOL_VERSION,
                    "type": "events.subscribe",
                    "topic_pattern": topic_a_beta,
                    "request_id": "sub-c",
                }
            )
            assert ws_c_inst2.receive_json()["type"] == "events.subscribed"

            async def _publish_and_run_worker_on_inst1() -> None:
                await store.startup()
                try:
                    pub = NotificationsDurablePublisher(
                        durable_store=store,
                        producer_authorizer=_AllowAllProducerAuthorizer(),
                    )
                    await pub.publish(notif)

                    worker = NotificationsDurableWorker(
                        config=NotificationsDurableConfig(tenant_id="tenant-alpha"),
                        durable_store=store,
                        broker=app1.state.events_module.broker,
                        preference_port=InMemoryNotificationPreferenceStore(),
                    )
                    assert await worker.run_once() == 1
                finally:
                    await store.shutdown()

            assert c1.portal is not None
            c1.portal.call(_publish_and_run_worker_on_inst1)

            # Recipient A on Instance 2 receives notification dispatched by Worker on Instance 1
            delivery = ws_a_inst2.receive_json()
            assert delivery["type"] == "events.delivery"
            assert delivery["event"]["event_id"] == notif.notification_id
            assert delivery["event"]["payload"]["payload"] == {"msg": "cross-instance"}

            # Tenant-beta user did not receive tenant-alpha notification (ping -> pong)
            ws_c_inst2.send_json(
                {"protocol_version": EVENTS_PROTOCOL_VERSION, "type": "session.ping", "seq": 1}
            )
            assert ws_c_inst2.receive_json()["type"] == "session.pong"


def test_n4_competing_workers_and_crash_after_dispatch_before_xack() -> None:
    async def _run() -> None:
        prefix = _prefix()
        redis_cfg = RedisClientConfig(url=_redis_url(), key_prefix=prefix)
        streams_cfg = RedisStreamsConfig(redis=redis_cfg, claim_min_idle_ms=20)
        pubsub_cfg = RedisPubSubBrokerConfig(
            redis=redis_cfg,
            broker=EventBrokerConfig(enable_rate_limiting=False),
        )
        store = RedisStreamsDurableStore(streams_cfg)
        broker1 = RedisPubSubEventBroker(
            pubsub_cfg, topic_authorizer=NotificationsTopicAuthorizer()
        )
        broker2 = RedisPubSubEventBroker(
            pubsub_cfg, topic_authorizer=NotificationsTopicAuthorizer()
        )
        await store.startup()
        await broker1.startup()
        await broker2.startup()
        try:
            sub_ctx = VerifiedPrincipalContext(
                provider_id="idp",
                tenant_id="tenant-alpha",
                connection_id="conn_sub_2",
                principal_id="user-a",
                allowed_scopes=frozenset({"events:subscribe"}),
            )
            await broker2.subscribe(sub_ctx, derive_recipient_topic("tenant-alpha", "user-a"))
            consumer = broker2.consumer("conn_sub_2")

            prefs = InMemoryNotificationPreferenceStore()
            tel = InMemoryNotificationsTelemetrySink()
            dtel = InMemoryNotificationsDurableTelemetrySink()
            pub = NotificationsDurablePublisher(
                durable_store=store,
                producer_authorizer=_AllowAllProducerAuthorizer(),
                telemetry=tel,
                durable_telemetry=dtel,
            )

            # 1. Normal competing workers in one consumer group dispatch 6 items once total
            sent_ids: list[str] = []
            for idx in range(6):
                n = _make_notif(msg=f"batch-{idx}")
                sent_ids.append(n.notification_id)
                await pub.publish(n)

            w1 = NotificationsDurableWorker(
                config=NotificationsDurableConfig(
                    tenant_id="tenant-alpha", consumer="w-1", read_batch_size=3
                ),
                durable_store=store,
                broker=broker1,
                preference_port=prefs,
                telemetry=tel,
                durable_telemetry=dtel,
            )
            w2 = NotificationsDurableWorker(
                config=NotificationsDurableConfig(
                    tenant_id="tenant-alpha", consumer="w-2", read_batch_size=3
                ),
                durable_store=store,
                broker=broker2,
                preference_port=prefs,
                telemetry=tel,
                durable_telemetry=dtel,
            )
            assert (await w1.run_once()) + (await w2.run_once()) == 6

            received_ids: list[str] = []
            for _ in range(6):
                evt = await asyncio.wait_for(consumer.receive(), timeout=2.0)
                assert evt is not None
                received_ids.append(evt.event_id)
            assert sorted(received_ids) == sorted(sent_ids)

            # 2. Crash after broker dispatch before XACK -> duplicate delivery on recovery
            crash_notif = _make_notif(msg="crash-after-dispatch")
            await pub.publish(crash_notif)

            class _CrashBeforeAckStore:
                def __init__(self, inner: RedisStreamsDurableStore) -> None:
                    self._inner = inner

                async def append(self, event: EventEnvelope) -> str:
                    return await self._inner.append(event)

                async def read_group(self, **kwargs: Any) -> Any:
                    return await self._inner.read_group(**kwargs)

                async def ack(self, **kwargs: Any) -> int:
                    raise TimeoutError("Simulated crash before XACK")

                async def replay(self, **kwargs: Any) -> Any:
                    return await self._inner.replay(**kwargs)

            crash_worker = NotificationsDurableWorker(
                config=NotificationsDurableConfig(tenant_id="tenant-alpha", consumer="w-crash"),
                durable_store=_CrashBeforeAckStore(store),
                broker=broker1,
                preference_port=prefs,
            )
            assert await crash_worker.run_once() == 0  # dispatched, XACK failed
            first_evt = await asyncio.wait_for(consumer.receive(), timeout=2.0)
            assert first_evt is not None
            assert first_evt.event_id == crash_notif.notification_id

            await asyncio.sleep(0.05)
            assert await w2.recover_once(min_idle_ms=20) == 1
            second_evt = await asyncio.wait_for(consumer.receive(), timeout=2.0)
            assert second_evt is not None
            assert second_evt.event_id == crash_notif.notification_id

            pending = await store.pending_summary(
                tenant_id="tenant-alpha",
                topic="notifications.ingest",
                group="notifications-workers",
            )
            assert pending.pending_count == 0
        finally:
            await broker2.shutdown()
            await broker1.shutdown()
            await store.shutdown()

    asyncio.run(_run())


def test_n4_slow_ws_subscriber_and_poison_dlq_across_instances() -> None:
    prefix = _prefix()
    token_map = {
        "tok-slow": ("tenant-alpha", "user-slow", frozenset({"events:subscribe"})),
        "tok-fast": ("tenant-alpha", "user-fast", frozenset({"events:subscribe"})),
    }
    app1 = _build_instance_app(prefix=prefix, token_map=token_map)
    app2 = _build_instance_app(
        prefix=prefix,
        token_map=token_map,
        broker_config=EventBrokerConfig(
            max_queued_events_per_connection=2,
            slow_consumer_policy=SlowConsumerPolicy.DISCONNECT_SLOW_CONSUMER,
            enable_rate_limiting=False,
        ),
    )
    redis_cfg = RedisClientConfig(url=_redis_url(), key_prefix=prefix)
    store = RedisStreamsDurableStore(RedisStreamsConfig(redis=redis_cfg))
    topic_slow = derive_recipient_topic("tenant-alpha", "user-slow")
    topic_fast = derive_recipient_topic("tenant-alpha", "user-fast")

    with TestClient(app1) as c1, TestClient(app2) as c2:
        with (
            c2.websocket_connect(
                "/ws/events/v1", headers={"Authorization": "Bearer tok-slow"}
            ) as ws_slow,
            c1.websocket_connect(
                "/ws/events/v1", headers={"Authorization": "Bearer tok-fast"}
            ) as ws_fast,
        ):
            assert ws_slow.receive_json()["type"] == "session.ready"
            assert ws_fast.receive_json()["type"] == "session.ready"

            ws_slow.send_json(
                {
                    "protocol_version": EVENTS_PROTOCOL_VERSION,
                    "type": "events.subscribe",
                    "topic_pattern": topic_slow,
                    "request_id": "s-slow",
                }
            )
            assert ws_slow.receive_json()["type"] == "events.subscribed"

            ws_fast.send_json(
                {
                    "protocol_version": EVENTS_PROTOCOL_VERSION,
                    "type": "events.subscribe",
                    "topic_pattern": topic_fast,
                    "request_id": "s-fast",
                }
            )
            assert ws_fast.receive_json()["type"] == "events.subscribed"

            fast_notif = _make_notif(recipient_id="user-fast", msg="healthy")

            async def _drive() -> None:
                await store.startup()
                try:
                    pub = NotificationsDurablePublisher(
                        durable_store=store,
                        producer_authorizer=_AllowAllProducerAuthorizer(),
                    )
                    for i in range(5):
                        await pub.publish(_make_notif(recipient_id="user-slow", msg=f"burst-{i}"))
                    await pub.publish(fast_notif)

                    # Poison outer-mismatched envelope
                    await store.append(
                        EventEnvelope(
                            protocol_version=EVENTS_PROTOCOL_VERSION,
                            event_id="evt_n4_poison",
                            event_type="orders.created",
                            topic="notifications.ingest",
                            timestamp="2026-11-18T12:00:00Z",
                            tenant_id="tenant-alpha",
                            principal_id="user-fast",
                            payload=fast_notif.to_dict(),
                        )
                    )

                    worker = NotificationsDurableWorker(
                        config=NotificationsDurableConfig(tenant_id="tenant-alpha"),
                        durable_store=store,
                        broker=app1.state.events_module.broker,
                        preference_port=InMemoryNotificationPreferenceStore(),
                    )
                    assert await worker.run_once() == 7
                    dlq = await store.dead_letter_entries(
                        tenant_id="tenant-alpha", topic="notifications.ingest"
                    )
                    assert len(dlq) == 1
                    assert dlq[0].reason == "invalid_notification_event_type"
                finally:
                    await store.shutdown()

            assert c1.portal is not None
            c1.portal.call(_drive)

            delivered = ws_fast.receive_json()
            assert delivered["type"] == "events.delivery"
            assert delivered["event"]["event_id"] == fast_notif.notification_id
