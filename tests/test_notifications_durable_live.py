"""Opt-in live Redis integration tests for Notifications N3 durable delivery and recovery.

All tests are skipped unless ``KAIWEN_TEST_REDIS_URL`` is set to a reachable Redis server.
"""

from __future__ import annotations

import asyncio
import os
import uuid

import pytest

from kaiwen_gateway.core.identity import VerifiedPrincipalContext
from kaiwen_gateway.modules.events import (
    EVENTS_PROTOCOL_VERSION,
    EventEnvelope,
    InMemoryEventBroker,
)
from kaiwen_gateway.modules.events.redis import (
    RedisClientConfig,
    RedisStreamsConfig,
    RedisStreamsDurableStore,
    create_async_redis_client,
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
    NotificationStatus,
    NotificationsTopicAuthorizer,
    derive_recipient_topic,
    generate_notification_id,
)

pytestmark = pytest.mark.skipif(
    not os.environ.get("KAIWEN_TEST_REDIS_URL"),
    reason="Opt-in live Redis N3 integration tests require KAIWEN_TEST_REDIS_URL",
)


def _prefix() -> str:
    return f"kwn3{uuid.uuid4().hex[:10]}"


def _make_store(
    *,
    prefix: str,
    stream_maxlen: int = 1000,
    claim_min_idle_ms: int = 10,
    max_delivery_attempts: int = 3,
) -> tuple[RedisStreamsDurableStore, object]:
    url = os.environ["KAIWEN_TEST_REDIS_URL"]
    redis_cfg = RedisClientConfig(url=url, key_prefix=prefix)
    cfg = RedisStreamsConfig(
        redis=redis_cfg,
        maxlen=stream_maxlen,
        approximate_trim=False,
        claim_min_idle_ms=claim_min_idle_ms,
        max_delivery_attempts=max_delivery_attempts,
    )
    client = create_async_redis_client(redis_cfg)
    store = RedisStreamsDurableStore(config=cfg, redis_client=client)
    return store, client


def _make_notif(
    *,
    tenant_id: str = "tenant-alpha",
    recipient_id: str = "user-a",
) -> NotificationEnvelope:
    return NotificationEnvelope(
        notification_id=generate_notification_id(),
        schema_version=NOTIFICATIONS_SCHEMA_VERSION,
        tenant_id=tenant_id,
        recipient_id=recipient_id,
        category="system",
        notification_type="task.completed",
        priority=NotificationPriority.NORMAL,
        payload={"msg": "live-redis"},
        created_at="2026-11-18T12:00:00Z",
        correlation_id="corr-live-n3",
    )


class _AllowAllTenantAuthorizer:
    async def authorize_publish(
        self,
        *,
        producer: VerifiedPrincipalContext | None,
        notification: NotificationEnvelope,
    ) -> None:
        return None


def test_live_redis_xadd_xreadgroup_broker_dispatch_xack() -> None:
    async def _run() -> None:
        prefix = _prefix()
        store, client = _make_store(prefix=prefix)
        broker = InMemoryEventBroker(topic_authorizer=NotificationsTopicAuthorizer())
        await store.startup()
        await broker.startup()
        try:
            prefs = InMemoryNotificationPreferenceStore()
            telemetry = InMemoryNotificationsTelemetrySink()
            durable_tel = InMemoryNotificationsDurableTelemetrySink()
            publisher = NotificationsDurablePublisher(
                durable_store=store,
                producer_authorizer=_AllowAllTenantAuthorizer(),
                telemetry=telemetry,
                durable_telemetry=durable_tel,
            )
            worker = NotificationsDurableWorker(
                config=NotificationsDurableConfig(tenant_id="tenant-alpha"),
                durable_store=store,
                broker=broker,
                preference_port=prefs,
                telemetry=telemetry,
                durable_telemetry=durable_tel,
            )

            sub_ctx = VerifiedPrincipalContext(
                provider_id="idp",
                tenant_id="tenant-alpha",
                connection_id="conn_live_a",
                principal_id="user-a",
                allowed_scopes=frozenset({"events:subscribe"}),
            )
            await broker.subscribe(sub_ctx, derive_recipient_topic("tenant-alpha", "user-a"))
            consumer = broker.consumer("conn_live_a")

            notif = _make_notif()
            res = await publisher.publish(notif)
            assert res.status == NotificationStatus.DURABLE_ACCEPTED
            assert res.sequence_id is not None

            assert await worker.run_once() == 1
            delivered = await consumer.receive()
            assert delivered is not None
            assert delivered.event_id == notif.notification_id

            pending = await store.pending_summary(
                tenant_id="tenant-alpha",
                topic="notifications.ingest",
                group="notifications-workers",
            )
            assert pending.pending_count == 0
        finally:
            await broker.shutdown()
            await store.shutdown()
            await client.aclose()  # type: ignore[attr-defined]

    asyncio.run(_run())


def test_live_redis_crash_before_xack_and_xautoclaim_recovery() -> None:
    async def _run() -> None:
        prefix = _prefix()
        store, client = _make_store(prefix=prefix, claim_min_idle_ms=20)
        broker = InMemoryEventBroker(topic_authorizer=NotificationsTopicAuthorizer())
        await store.startup()
        await broker.startup()
        try:
            prefs = InMemoryNotificationPreferenceStore()
            durable_tel = InMemoryNotificationsDurableTelemetrySink()
            publisher = NotificationsDurablePublisher(
                durable_store=store,
                producer_authorizer=_AllowAllTenantAuthorizer(),
                durable_telemetry=durable_tel,
            )
            notif = _make_notif()
            await publisher.publish(notif)

            # Simulate worker-1 reading via XREADGROUP and crashing before XACK
            raw_records = await store.read_group(
                tenant_id="tenant-alpha",
                topic="notifications.ingest",
                group="notifications-workers",
                consumer="worker-crashed",
                count=10,
                block_ms=50,
            )
            assert len(raw_records) == 1

            await asyncio.sleep(0.05)

            worker2 = NotificationsDurableWorker(
                config=NotificationsDurableConfig(
                    tenant_id="tenant-alpha",
                    consumer="worker-2",
                    claim_min_idle_ms=20,
                ),
                durable_store=store,
                broker=broker,
                preference_port=prefs,
                durable_telemetry=durable_tel,
            )
            acked = await worker2.recover_once(min_idle_ms=20)
            assert acked == 1
            assert durable_tel.snapshot().worker_reclaimed == 1

            pending = await store.pending_summary(
                tenant_id="tenant-alpha",
                topic="notifications.ingest",
                group="notifications-workers",
            )
            assert pending.pending_count == 0
        finally:
            await broker.shutdown()
            await store.shutdown()
            await client.aclose()  # type: ignore[attr-defined]

    asyncio.run(_run())


def test_live_redis_multiple_competing_consumers() -> None:
    async def _run() -> None:
        prefix = _prefix()
        store, client = _make_store(prefix=prefix)
        broker = InMemoryEventBroker(topic_authorizer=NotificationsTopicAuthorizer())
        await store.startup()
        await broker.startup()
        try:
            prefs = InMemoryNotificationPreferenceStore()
            publisher = NotificationsDurablePublisher(
                durable_store=store,
                producer_authorizer=_AllowAllTenantAuthorizer(),
            )
            for _ in range(6):
                await publisher.publish(_make_notif())

            w1 = NotificationsDurableWorker(
                config=NotificationsDurableConfig(
                    tenant_id="tenant-alpha", consumer="worker-1", read_batch_size=3
                ),
                durable_store=store,
                broker=broker,
                preference_port=prefs,
            )
            w2 = NotificationsDurableWorker(
                config=NotificationsDurableConfig(
                    tenant_id="tenant-alpha", consumer="worker-2", read_batch_size=3
                ),
                durable_store=store,
                broker=broker,
                preference_port=prefs,
            )
            a1 = await w1.run_once()
            a2 = await w2.run_once()
            assert a1 + a2 == 6

            pending = await store.pending_summary(
                tenant_id="tenant-alpha",
                topic="notifications.ingest",
                group="notifications-workers",
            )
            assert pending.pending_count == 0
        finally:
            await broker.shutdown()
            await store.shutdown()
            await client.aclose()  # type: ignore[attr-defined]

    asyncio.run(_run())


def test_live_redis_poison_payload_dlq_quarantine() -> None:
    async def _run() -> None:
        prefix = _prefix()
        store, client = _make_store(prefix=prefix)
        broker = InMemoryEventBroker(topic_authorizer=NotificationsTopicAuthorizer())
        await store.startup()
        await broker.startup()
        try:
            durable_tel = InMemoryNotificationsDurableTelemetrySink()
            poison_env = EventEnvelope(
                protocol_version=EVENTS_PROTOCOL_VERSION,
                event_id="evt_live_poison",
                event_type=NOTIFICATIONS_SCHEMA_VERSION,
                topic="notifications.ingest",
                timestamp="2026-11-18T12:00:00Z",
                tenant_id="tenant-alpha",
                principal_id="user-a",
                payload={"invalid": True},
            )
            await store.append(poison_env)

            worker = NotificationsDurableWorker(
                config=NotificationsDurableConfig(tenant_id="tenant-alpha"),
                durable_store=store,
                broker=broker,
                preference_port=InMemoryNotificationPreferenceStore(),
                durable_telemetry=durable_tel,
            )
            assert await worker.run_once() == 1
            assert durable_tel.snapshot().worker_dead_lettered == 1

            dlq = await store.dead_letter_entries(
                tenant_id="tenant-alpha",
                topic="notifications.ingest",
            )
            assert len(dlq) == 1
            assert dlq[0].reason == "invalid_notification_envelope"

            pending = await store.pending_summary(
                tenant_id="tenant-alpha",
                topic="notifications.ingest",
                group="notifications-workers",
            )
            assert pending.pending_count == 0
        finally:
            await broker.shutdown()
            await store.shutdown()
            await client.aclose()  # type: ignore[attr-defined]

    asyncio.run(_run())


def test_live_redis_maxlen_retention_and_worker_resilience() -> None:
    async def _run() -> None:
        prefix = _prefix()
        store, client = _make_store(prefix=prefix, stream_maxlen=3)
        broker = InMemoryEventBroker(topic_authorizer=NotificationsTopicAuthorizer())
        await store.startup()
        await broker.startup()
        try:
            publisher = NotificationsDurablePublisher(
                durable_store=store,
                producer_authorizer=_AllowAllTenantAuthorizer(),
            )
            for _ in range(5):
                await publisher.publish(_make_notif())

            stats = await store.stream_stats(tenant_id="tenant-alpha", topic="notifications.ingest")
            assert stats.length == 3
        finally:
            await broker.shutdown()
            await store.shutdown()
            await client.aclose()  # type: ignore[attr-defined]

    asyncio.run(_run())
