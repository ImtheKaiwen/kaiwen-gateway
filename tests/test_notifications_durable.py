"""Deterministic unit tests for Notifications N3 durable delivery and recovery."""

from __future__ import annotations

import asyncio
from collections.abc import Sequence
from datetime import UTC, datetime

import pytest

from kaiwen_gateway.core.identity import VerifiedPrincipalContext
from kaiwen_gateway.modules.events import (
    EVENTS_PROTOCOL_VERSION,
    DurableEventRecord,
    EventEnvelope,
    EventsBackendUnavailableError,
    InMemoryEventBroker,
)
from kaiwen_gateway.modules.events.redis.streams import StreamDeadLetterRecord, StreamRecoveryResult
from kaiwen_gateway.modules.notifications import (
    NOTIFICATIONS_SCHEMA_VERSION,
    InMemoryNotificationPreferenceStore,
    InMemoryNotificationsDurableTelemetrySink,
    InMemoryNotificationsTelemetrySink,
    InvalidNotificationError,
    NotificationAuthorizationError,
    NotificationEnvelope,
    NotificationPriority,
    NotificationsBackendUnavailableError,
    NotificationsDurableConfig,
    NotificationsDurablePublisher,
    NotificationsDurableWorker,
    NotificationsModuleClosedError,
    NotificationStatus,
    NotificationsTopicAuthorizer,
    derive_recipient_topic,
    generate_notification_id,
)


class _AllowTenantProducerAuthorizer:
    async def authorize_publish(
        self,
        *,
        producer: VerifiedPrincipalContext | None,
        notification: NotificationEnvelope,
    ) -> None:
        if producer is not None and producer.tenant_id != notification.tenant_id:
            raise NotificationAuthorizationError("Cross-tenant publish denied")


class _FakeDurableStore:
    """Deterministic in-memory DurableEventStorePort + DeadLetter/Recovery fake."""

    def __init__(self) -> None:
        self.entries: list[tuple[str, EventEnvelope]] = []
        self.pending: dict[str, tuple[EventEnvelope, str, int]] = {}
        self.acked: list[str] = []
        self.dead_letters: list[tuple[str, str, EventEnvelope | None]] = []
        self.fail_append: bool = False
        self.return_empty_id: bool = False
        self.fail_ack: bool = False
        self.fail_dead_letter: bool = False
        self.fail_read: bool = False
        self._seq: int = 0

    async def append(self, event: EventEnvelope) -> str:
        if self.fail_append:
            raise TimeoutError("Redis XADD timed out")
        if self.return_empty_id:
            return ""
        self._seq += 1
        entry_id = f"{self._seq}-0"
        self.entries.append((entry_id, event))
        return entry_id

    async def read_group(
        self,
        *,
        tenant_id: str,
        topic: str,
        group: str,
        consumer: str,
        count: int,
        block_ms: int,
    ) -> list[DurableEventRecord]:
        if self.fail_read:
            raise EventsBackendUnavailableError("Redis unavailable")
        out: list[DurableEventRecord] = []
        remaining: list[tuple[str, EventEnvelope]] = []
        for entry_id, env in self.entries:
            if len(out) < count and env.tenant_id == tenant_id and env.topic == topic:
                self.pending[entry_id] = (env, consumer, 1)
                out.append(DurableEventRecord(envelope=env, sequence_id=entry_id))
            else:
                remaining.append((entry_id, env))
        self.entries = remaining
        return out

    async def ack(
        self,
        *,
        tenant_id: str,
        topic: str,
        group: str,
        record_ids: Sequence[str],
    ) -> int:
        if self.fail_ack:
            raise EventsBackendUnavailableError("Redis XACK failed")
        count = 0
        for rid in record_ids:
            if rid in self.pending:
                self.pending.pop(rid)
                self.acked.append(rid)
                count += 1
        return count

    async def replay(
        self,
        *,
        tenant_id: str,
        topic: str,
        from_id: str = "0-0",
        limit: int = 100,
    ) -> list[DurableEventRecord]:
        return [
            DurableEventRecord(envelope=env, sequence_id=eid)
            for eid, env in self.entries
            if env.tenant_id == tenant_id and env.topic == topic
        ][:limit]

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
    ) -> StreamDeadLetterRecord:
        if self.fail_dead_letter:
            raise EventsBackendUnavailableError("Redis DLQ XADD failed")
        self.pending.pop(record_id, None)
        self.dead_letters.append((record_id, reason, envelope))
        return StreamDeadLetterRecord(
            dlq_entry_id=f"dlq-{record_id}",
            original_entry_id=record_id,
            event_id=envelope.event_id if envelope is not None else None,
            group=group,
            delivery_attempts=delivery_attempts,
            reason=reason,
        )

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
    ) -> StreamRecoveryResult:
        reclaimed: list[DurableEventRecord] = []
        for eid, (env, _owner, attempts) in list(self.pending.items()):
            if env.tenant_id == tenant_id and env.topic == topic:
                self.pending[eid] = (env, consumer, attempts + 1)
                reclaimed.append(DurableEventRecord(envelope=env, sequence_id=eid))
                if count is not None and len(reclaimed) >= count:
                    break
        return StreamRecoveryResult(
            reclaimed=tuple(reclaimed),
            dead_lettered=(),
            next_start_id="0-0",
        )


def _make_notif(
    *,
    tenant_id: str = "tenant-alpha",
    recipient_id: str = "user-a",
    category: str = "system",
    priority: NotificationPriority = NotificationPriority.NORMAL,
    created_at: str = "2026-11-18T12:00:00Z",
    expires_at: str | None = None,
    is_promotional: bool = False,
    notification_id: str | None = None,
) -> NotificationEnvelope:
    return NotificationEnvelope(
        notification_id=notification_id or generate_notification_id(),
        schema_version=NOTIFICATIONS_SCHEMA_VERSION,
        tenant_id=tenant_id,
        recipient_id=recipient_id,
        category=category,
        notification_type="task.completed",
        priority=priority,
        payload={"msg": "hello"},
        created_at=created_at,
        expires_at=expires_at,
        is_promotional=is_promotional,
        correlation_id="corr-n3",
    )


def _make_producer(tenant_id: str = "tenant-alpha") -> VerifiedPrincipalContext:
    return VerifiedPrincipalContext(
        provider_id="svc-idp",
        tenant_id=tenant_id,
        connection_id="conn_prod_n3",
        principal_id="svc-notifier",
        allowed_scopes=frozenset({"notifications:publish"}),
    )


def test_durable_config_enforces_socket_timeout_and_bounds() -> None:
    with pytest.raises(InvalidNotificationError):
        NotificationsDurableConfig(tenant_id="tenant-alpha", block_ms=1001)
    with pytest.raises(InvalidNotificationError):
        NotificationsDurableConfig(tenant_id="tenant-alpha", read_batch_size=0)
    with pytest.raises(InvalidNotificationError):
        NotificationsDurableConfig(tenant_id="tenant-alpha", read_batch_size=257)
    cfg = NotificationsDurableConfig(tenant_id="tenant-alpha")
    assert cfg.block_ms == 200


def test_durable_xadd_processing_broker_dispatch_and_xack() -> None:
    async def _run() -> None:
        store = _FakeDurableStore()
        broker = InMemoryEventBroker(topic_authorizer=NotificationsTopicAuthorizer())
        await broker.startup()
        prefs = InMemoryNotificationPreferenceStore()
        telemetry = InMemoryNotificationsTelemetrySink()
        durable_tel = InMemoryNotificationsDurableTelemetrySink()

        publisher = NotificationsDurablePublisher(
            durable_store=store,
            producer_authorizer=_AllowTenantProducerAuthorizer(),
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

        # Subscribe recipient A on live broker
        sub_ctx = VerifiedPrincipalContext(
            provider_id="idp",
            tenant_id="tenant-alpha",
            connection_id="conn_user_a",
            principal_id="user-a",
            allowed_scopes=frozenset({"events:subscribe"}),
        )
        await broker.subscribe(sub_ctx, derive_recipient_topic("tenant-alpha", "user-a"))
        consumer = broker.consumer("conn_user_a")

        notif = _make_notif()
        pub_res = await publisher.publish(notif, producer=_make_producer())
        assert pub_res.status == NotificationStatus.DURABLE_ACCEPTED
        assert pub_res.sequence_id == "1-0"
        assert pub_res.dispatched_connections == 0
        # Not published to live broker prematurely
        assert telemetry.snapshot().notifications_published == 0

        acked = await worker.run_once()
        assert acked == 1
        assert store.acked == ["1-0"]
        assert not store.pending

        delivered = await consumer.receive()
        assert delivered is not None
        assert delivered.event_id == notif.notification_id
        assert delivered.event_type == NOTIFICATIONS_SCHEMA_VERSION
        assert delivered.topic == derive_recipient_topic("tenant-alpha", "user-a")

        assert telemetry.snapshot().notifications_published == 1
        assert telemetry.snapshot().notifications_dispatched == 1
        assert durable_tel.snapshot().durable_accepted == 1
        assert durable_tel.snapshot().worker_acked == 1

        await broker.shutdown()

    asyncio.run(_run())


def test_publisher_timeout_and_empty_sequence_id_fail_closed() -> None:
    async def _run() -> None:
        store = _FakeDurableStore()
        durable_tel = InMemoryNotificationsDurableTelemetrySink()
        publisher = NotificationsDurablePublisher(
            durable_store=store,
            producer_authorizer=_AllowTenantProducerAuthorizer(),
            durable_telemetry=durable_tel,
        )
        notif = _make_notif()

        store.fail_append = True
        with pytest.raises(NotificationsBackendUnavailableError):
            await publisher.publish(notif, producer=_make_producer())
        store.fail_append = False

        store.return_empty_id = True
        with pytest.raises(NotificationsBackendUnavailableError):
            await publisher.publish(notif, producer=_make_producer())
        assert durable_tel.snapshot().durable_accepted == 0
        assert durable_tel.snapshot().redis_failures == 2

    asyncio.run(_run())


def test_no_xack_before_terminal_processing_and_offline_recipient_acks() -> None:
    async def _run() -> None:
        store = _FakeDurableStore()
        broker = InMemoryEventBroker(topic_authorizer=NotificationsTopicAuthorizer())
        await broker.startup()
        prefs = InMemoryNotificationPreferenceStore()
        telemetry = InMemoryNotificationsTelemetrySink()
        durable_tel = InMemoryNotificationsDurableTelemetrySink()

        publisher = NotificationsDurablePublisher(
            durable_store=store,
            producer_authorizer=_AllowTenantProducerAuthorizer(),
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

        # 1. Retryable preference failure -> NO XACK (remains in PEL)
        notif = _make_notif()
        await publisher.publish(notif, producer=_make_producer())
        prefs.set_fail_lookups(True)
        acked = await worker.run_once()
        assert acked == 0
        assert "1-0" in store.pending
        assert store.acked == []
        assert durable_tel.snapshot().worker_retryable_failures == 1

        # 2. Recover via recover_once() when preference store recovers & recipient offline -> XACKed
        prefs.set_fail_lookups(False)
        recovered_acked = await worker.recover_once()
        assert recovered_acked == 1
        assert store.acked == ["1-0"]
        assert not store.pending
        assert durable_tel.snapshot().worker_reclaimed == 1
        assert telemetry.snapshot().notifications_published == 1
        assert telemetry.snapshot().notifications_dispatched == 0

        await broker.shutdown()

    asyncio.run(_run())


def test_expired_preference_filtered_and_dedup_redelivery() -> None:
    async def _run() -> None:
        store = _FakeDurableStore()
        broker = InMemoryEventBroker(topic_authorizer=NotificationsTopicAuthorizer())
        await broker.startup()
        prefs = InMemoryNotificationPreferenceStore()
        telemetry = InMemoryNotificationsTelemetrySink()
        durable_tel = InMemoryNotificationsDurableTelemetrySink()
        fixed_now = datetime(2026, 11, 18, 12, 30, 0, tzinfo=UTC)

        worker = NotificationsDurableWorker(
            config=NotificationsDurableConfig(
                tenant_id="tenant-alpha",
                authoritative_promotional_categories=("marketing",),
            ),
            durable_store=store,
            broker=broker,
            preference_port=prefs,
            telemetry=telemetry,
            durable_telemetry=durable_tel,
            clock=lambda: fixed_now,
        )

        # 1. Expired notification -> discarded & XACKed
        expired_notif = _make_notif(
            created_at="2026-11-18T12:00:00Z",
            expires_at="2026-11-18T12:15:00Z",
        )
        await store.append(
            EventEnvelope(
                protocol_version=EVENTS_PROTOCOL_VERSION,
                event_id=expired_notif.notification_id,
                event_type=NOTIFICATIONS_SCHEMA_VERSION,
                topic="notifications.ingest",
                timestamp=expired_notif.created_at,
                tenant_id="tenant-alpha",
                principal_id="user-a",
                payload=expired_notif.to_dict(),
            )
        )

        # 2. Muted/promotional notification -> filtered & XACKed
        promo_notif = _make_notif(category="marketing", is_promotional=False)
        await store.append(
            EventEnvelope(
                protocol_version=EVENTS_PROTOCOL_VERSION,
                event_id=promo_notif.notification_id,
                event_type=NOTIFICATIONS_SCHEMA_VERSION,
                topic="notifications.ingest",
                timestamp=promo_notif.created_at,
                tenant_id="tenant-alpha",
                principal_id="user-a",
                payload=promo_notif.to_dict(),
            )
        )

        # 3. Duplicate notification -> second copy deduplicated & XACKed
        dup_id = generate_notification_id()
        n_ok = _make_notif(notification_id=dup_id)
        for _ in range(2):
            await store.append(
                EventEnvelope(
                    protocol_version=EVENTS_PROTOCOL_VERSION,
                    event_id=n_ok.notification_id,
                    event_type=NOTIFICATIONS_SCHEMA_VERSION,
                    topic="notifications.ingest",
                    timestamp=n_ok.created_at,
                    tenant_id="tenant-alpha",
                    principal_id="user-a",
                    payload=n_ok.to_dict(),
                )
            )

        acked = await worker.run_once()
        assert acked == 4
        assert telemetry.snapshot().notifications_expired == 1
        assert telemetry.snapshot().notifications_filtered_by_preference == 1
        assert telemetry.snapshot().notifications_published == 1
        assert durable_tel.snapshot().worker_deduplicated == 1

        await broker.shutdown()

    asyncio.run(_run())


def test_poison_payload_tenant_mismatch_and_dlq_failure() -> None:
    async def _run() -> None:
        store = _FakeDurableStore()
        broker = InMemoryEventBroker(topic_authorizer=NotificationsTopicAuthorizer())
        await broker.startup()
        prefs = InMemoryNotificationPreferenceStore()
        telemetry = InMemoryNotificationsTelemetrySink()
        durable_tel = InMemoryNotificationsDurableTelemetrySink()

        worker = NotificationsDurableWorker(
            config=NotificationsDurableConfig(tenant_id="tenant-alpha"),
            durable_store=store,
            broker=broker,
            preference_port=prefs,
            telemetry=telemetry,
            durable_telemetry=durable_tel,
        )

        # 1. Poison inner NotificationEnvelope payload
        poison_env = EventEnvelope(
            protocol_version=EVENTS_PROTOCOL_VERSION,
            event_id="evt_poison_1",
            event_type=NOTIFICATIONS_SCHEMA_VERSION,
            topic="notifications.ingest",
            timestamp="2026-11-18T12:00:00Z",
            tenant_id="tenant-alpha",
            principal_id="user-a",
            payload={"bad": "missing_required_fields"},
        )
        await store.append(poison_env)

        # DLQ write failure must NOT ack
        store.fail_dead_letter = True
        assert await worker.run_once() == 0
        assert "1-0" in store.pending
        assert durable_tel.snapshot().worker_dlq_failed == 1
        assert durable_tel.snapshot().redis_failures == 1

        # DLQ recovers -> quarantined & removed from pending
        store.fail_dead_letter = False
        assert await worker.recover_once() == 1
        assert not store.pending
        assert len(store.dead_letters) == 1
        assert store.dead_letters[0][1] == "invalid_notification_envelope"

        # 2. Cross-tenant inner notification quarantined
        cross_env = EventEnvelope(
            protocol_version=EVENTS_PROTOCOL_VERSION,
            event_id="evt_cross_1",
            event_type=NOTIFICATIONS_SCHEMA_VERSION,
            topic="notifications.ingest",
            timestamp="2026-11-18T12:00:00Z",
            tenant_id="tenant-alpha",
            principal_id="user-a",
            payload=_make_notif(tenant_id="tenant-beta").to_dict(),
        )
        await store.append(cross_env)
        assert await worker.run_once() == 1
        assert len(store.dead_letters) == 2
        assert store.dead_letters[1][1] == "notification_tenant_mismatch"

        await broker.shutdown()

    asyncio.run(_run())


def test_worker_cancellation_and_clean_shutdown() -> None:
    async def _run() -> None:
        store = _FakeDurableStore()
        broker = InMemoryEventBroker(topic_authorizer=NotificationsTopicAuthorizer())
        await broker.startup()
        worker = NotificationsDurableWorker(
            config=NotificationsDurableConfig(
                tenant_id="tenant-alpha",
                poll_interval_seconds=0.01,
                recovery_interval_seconds=0.02,
            ),
            durable_store=store,
            broker=broker,
            preference_port=InMemoryNotificationPreferenceStore(),
        )
        baseline_tasks = len(asyncio.all_tasks())
        worker.start()
        assert worker.is_running is True
        await asyncio.sleep(0.03)
        await worker.stop()
        assert len(asyncio.all_tasks()) == baseline_tasks
        with pytest.raises(NotificationsModuleClosedError):
            worker.start()
        await broker.shutdown()

    asyncio.run(_run())
