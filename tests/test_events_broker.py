"""Sections B, C, D tests: InMemoryEventBroker routing, budgets, slow consumers, and cleanup."""

from __future__ import annotations

import asyncio
from collections.abc import Mapping

import pytest

from kaiwen_gateway.core.identity import VerifiedPrincipalContext
from kaiwen_gateway.modules.events import (
    EVENTS_PROTOCOL_VERSION,
    EventAuthorizationError,
    EventBackpressureError,
    EventBrokerConfig,
    EventEnvelope,
    InMemoryEventBroker,
    InMemoryEventsTelemetrySink,
    SlowConsumerPolicy,
)


def _ctx(
    *,
    provider_id: str = "provider-1",
    tenant_id: str = "tenant-a",
    principal_id: str = "principal-1",
    connection_id: str = "conn-1",
    scopes: tuple[str, ...] = ("events:subscribe", "events:subscribe:wildcard"),
) -> VerifiedPrincipalContext:
    return VerifiedPrincipalContext(
        provider_id=provider_id,
        tenant_id=tenant_id,
        principal_id=principal_id,
        connection_id=connection_id,
        allowed_scopes=frozenset(scopes),
    )


def _env(
    *,
    event_id: str = "evt-1",
    topic: str = "call.status",
    tenant_id: str = "tenant-a",
    principal_id: str = "publisher-1",
    payload: Mapping[str, object] | None = None,
) -> EventEnvelope:
    return EventEnvelope(
        protocol_version=EVENTS_PROTOCOL_VERSION,
        event_id=event_id,
        event_type="call.status.updated",
        topic=topic,
        timestamp="2026-11-18T00:00:00Z",
        tenant_id=tenant_id,
        principal_id=principal_id,
        payload=payload if payload is not None else {"ok": True},
    )


def test_broker_tenant_isolation_and_scope_enforcement() -> None:
    async def scenario() -> None:
        broker = InMemoryEventBroker()
        await broker.startup()

        with pytest.raises(EventAuthorizationError):
            await broker.subscribe(_ctx(scopes=("voice:connect",)), "call.status")
        with pytest.raises(EventAuthorizationError):
            await broker.subscribe(_ctx(scopes=("events:subscribe",)), "call.*")

        await broker.subscribe(_ctx(tenant_id="tenant-a", connection_id="conn-a"), "call.*")
        await broker.subscribe(_ctx(tenant_id="tenant-b", connection_id="conn-b"), "call.*")
        cons_a = broker.consumer("conn-a")
        cons_b = broker.consumer("conn-b")

        result = await broker.publish(_env(tenant_id="tenant-a", event_id="evt-a1"))
        assert result.matched_subscribers == 1
        assert result.delivered_subscribers == 1

        received = await asyncio.wait_for(cons_a.receive(), timeout=1.0)
        assert received is not None
        assert received.event_id == "evt-a1"

        with pytest.raises(TimeoutError):
            await asyncio.wait_for(cons_b.receive(), timeout=0.05)

        await broker.shutdown()
        assert broker.stats().queued_bytes == 0
        assert broker.stats().active_connections == 0

    asyncio.run(scenario())


def test_broker_overlapping_and_disjoint_patterns_multiplexed_consumer() -> None:
    async def scenario() -> None:
        broker = InMemoryEventBroker()
        await broker.startup()
        ctx = _ctx(connection_id="conn-multi")
        sub_wild = await broker.subscribe(ctx, "call.*")
        sub_exact = await broker.subscribe(ctx, "call.status")
        sub_disjoint = await broker.subscribe(ctx, "agent.status")
        consumer = broker.consumer(ctx.connection_id)

        # Overlapping subscriptions deliver once
        res = await broker.publish(_env(event_id="evt-dedup", topic="call.status"))
        assert res.matched_subscribers == 1
        assert res.delivered_subscribers == 1
        assert broker.stats().queued_events == 1

        # Disjoint pattern delivers onto the same multiplexed queue
        await broker.publish(_env(event_id="evt-agent", topic="agent.status"))

        got1 = await asyncio.wait_for(consumer.receive(), timeout=1.0)
        got2 = await asyncio.wait_for(consumer.receive(), timeout=1.0)
        assert got1 is not None and got1.event_id == "evt-dedup"
        assert got2 is not None and got2.event_id == "evt-agent"
        assert broker.stats().queued_events == 0
        assert broker.stats().queued_bytes == 0

        # Unsubscribing one pattern preserves the connection until the last subscription closes
        await sub_wild.close()
        await sub_disjoint.close()
        assert broker.stats().active_connections == 1
        assert broker.stats().active_subscriptions == 1

        await broker.publish(_env(event_id="evt-2", topic="call.status"))
        got3 = await asyncio.wait_for(consumer.receive(), timeout=1.0)
        assert got3 is not None and got3.event_id == "evt-2"

        await sub_exact.close()
        assert await consumer.receive() is None
        assert broker.stats().active_connections == 0
        assert broker.stats().active_subscriptions == 0

    asyncio.run(scenario())


def test_broker_subscription_limit_and_multiplexed_byte_budget() -> None:
    async def scenario() -> None:
        broker = InMemoryEventBroker(
            EventBrokerConfig(
                max_subscriptions_per_connection=32,
                max_queued_events_per_connection=128,
                max_queued_bytes_per_connection=1_048_576,
                global_queued_bytes_budget=33_554_432,
                enable_rate_limiting=False,
            )
        )
        await broker.startup()
        ctx = _ctx(connection_id="conn-32")
        subs = [await broker.subscribe(ctx, f"topic.{i}.*") for i in range(32)]
        assert len(subs) == 32
        consumer = broker.consumer(ctx.connection_id)

        with pytest.raises(EventBackpressureError):
            await broker.subscribe(ctx, "topic.32.*")

        # Even with 32 subscriptions on one connection, per-connection queue is capped at 1 MiB.
        big_payload = {"blob": "x" * 60_000}
        disconnected_at = None
        for idx in range(32):
            res = await broker.publish(
                _env(event_id=f"evt-{idx}", topic=f"topic.{idx}.ping", payload=big_payload)
            )
            if res.disconnected_subscribers == 1:
                disconnected_at = idx
                break

        assert disconnected_at is not None
        assert broker.stats().active_connections == 0
        assert broker.stats().queued_bytes == 0
        assert broker.stats().queued_events == 0
        assert await consumer.receive() is None

    asyncio.run(scenario())


def test_broker_count_limit_disconnects_slow_consumer_and_clears_counters() -> None:
    async def scenario() -> None:
        telemetry = InMemoryEventsTelemetrySink()
        broker = InMemoryEventBroker(
            EventBrokerConfig(
                max_queued_events_per_connection=4,
                max_queued_bytes_per_connection=65_536,
                global_queued_bytes_budget=131_072,
            ),
            telemetry=telemetry,
        )
        await broker.startup()
        await broker.subscribe(_ctx(connection_id="slow-conn"), "call.*")
        consumer = broker.consumer("slow-conn")

        for idx in range(4):
            res = await broker.publish(_env(event_id=f"evt-{idx}", payload={"i": idx}))
            assert res.delivered_subscribers == 1

        assert broker.stats().queued_events == 4
        overflow = await broker.publish(_env(event_id="evt-overflow", payload={"i": 4}))
        assert overflow.disconnected_subscribers == 1
        assert broker.stats().active_connections == 0
        assert broker.stats().queued_events == 0
        assert broker.stats().queued_bytes == 0

        snap = telemetry.snapshot()
        assert snap.slow_consumers_disconnected == 1
        assert snap.queued_bytes == 0
        assert snap.active_connections == 0
        assert await consumer.receive() is None

    asyncio.run(scenario())


def test_broker_global_byte_budget_across_connections() -> None:
    async def scenario() -> None:
        small_env = _env(event_id="e1", payload={"data": "x" * 400})
        budget = small_env.serialized_bytes * 3
        broker = InMemoryEventBroker(
            EventBrokerConfig(
                max_queued_events_per_connection=10,
                max_queued_bytes_per_connection=budget,
                global_queued_bytes_budget=budget,
            )
        )
        await broker.startup()
        sub1 = await broker.subscribe(_ctx(connection_id="c1"), "call.*")
        sub2 = await broker.subscribe(_ctx(connection_id="c2"), "call.*")

        r1 = await broker.publish(_env(event_id="e1", payload={"data": "x" * 400}))
        assert r1.delivered_subscribers == 2
        assert broker.stats().queued_bytes == small_env.serialized_bytes * 2

        # Second publish: c1 fits (3 * size == budget), c2 drops on global budget without disconnecting
        r2 = await broker.publish(_env(event_id="e2", payload={"data": "x" * 400}))
        assert r2.delivered_subscribers == 1
        assert r2.disconnected_subscribers == 0
        assert r2.dropped_global_budget == 1
        assert broker.stats().queued_bytes <= budget
        assert sub2.is_active is True

        await sub1.close()
        await sub2.close()
        assert broker.stats().queued_bytes == 0

    asyncio.run(scenario())


def test_broker_drop_oldest_policy_maintains_exact_byte_accounting() -> None:
    async def scenario() -> None:
        broker = InMemoryEventBroker(
            EventBrokerConfig(
                max_queued_events_per_connection=2,
                slow_consumer_policy=SlowConsumerPolicy.DROP_OLDEST,
            )
        )
        await broker.startup()
        await broker.subscribe(_ctx(connection_id="drop-conn"), "call.*")
        consumer = broker.consumer("drop-conn")

        await broker.publish(_env(event_id="e1", payload={"n": 1}))
        await broker.publish(_env(event_id="e2", payload={"n": 2}))
        await broker.publish(_env(event_id="e3", payload={"n": 3}))

        assert broker.stats().queued_events == 2
        got1 = await consumer.receive()
        got2 = await consumer.receive()
        assert got1 is not None and got2 is not None
        assert [got1.event_id, got2.event_id] == ["e2", "e3"]
        assert broker.stats().queued_events == 0
        assert broker.stats().queued_bytes == 0

    asyncio.run(scenario())


def test_broker_cancellation_and_concurrent_publish_subscribe() -> None:
    async def scenario() -> None:
        broker = InMemoryEventBroker()
        await broker.startup()
        await broker.subscribe(_ctx(connection_id="c-async"), "call.*")
        consumer = broker.consumer("c-async")

        # Cancel a pending receive waiter and verify broker stays healthy
        waiter = asyncio.create_task(consumer.receive())
        await asyncio.sleep(0)
        waiter.cancel()
        with pytest.raises(asyncio.CancelledError):
            await waiter

        received: list[str] = []

        async def consume_task_fn() -> None:
            async for env in consumer:
                received.append(env.event_id)
                if len(received) == 20:
                    await consumer.close()

        consumer_task = asyncio.create_task(consume_task_fn())
        await asyncio.gather(*(broker.publish(_env(event_id=f"c-{i}")) for i in range(20)))
        await asyncio.wait_for(consumer_task, timeout=2.0)

        assert len(received) == 20
        assert broker.stats().active_connections == 0
        assert broker.stats().queued_bytes == 0

    asyncio.run(scenario())
