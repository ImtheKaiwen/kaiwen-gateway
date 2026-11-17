"""Deterministic regression tests reproducing E1 independent review findings R1-R7."""

from __future__ import annotations

import asyncio

import pytest

from kaiwen_gateway.core import (
    AdmissionController,
    AdmissionLease,
    CoreConfig,
    PrincipalAdmissionLease,
    VerifiedConnectionContext,
    VerifiedPrincipalContext,
)
from kaiwen_gateway.modules.events import (
    DEFAULT_MAX_EVENT_BYTES,
    EVENTS_PROTOCOL_VERSION,
    EventAuthorizationError,
    EventBrokerConfig,
    EventEnvelope,
    EventRateLimitError,
    EventsModule,
    EventsModuleClosedError,
    InMemoryEventBroker,
    InvalidEventError,
    OversizedEventError,
)


def _ctx(
    *,
    connection_id: str = "conn-1",
    tenant_id: str = "tenant-a",
    principal_id: str = "principal-1",
    allowed_scopes: tuple[str, ...] = ("events:subscribe",),
) -> VerifiedPrincipalContext:
    return VerifiedPrincipalContext(
        provider_id="provider-1",
        tenant_id=tenant_id,
        principal_id=principal_id,
        connection_id=connection_id,
        allowed_scopes=frozenset(allowed_scopes),
    )


def _env(
    *,
    event_id: str = "evt-1",
    topic: str = "call.status",
    tenant_id: str = "tenant-a",
    principal_id: str = "publisher-1",
    payload: dict[str, object] | None = None,
    **extra: object,
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
        **extra,  # type: ignore[arg-type]
    )


def test_r1_wildcard_subscription_denied_without_explicit_wildcard_scope() -> None:
    async def scenario() -> None:
        broker = InMemoryEventBroker()
        await broker.startup()
        # General events:subscribe must NOT allow wildcard subscriptions
        with pytest.raises(EventAuthorizationError):
            await broker.subscribe(_ctx(allowed_scopes=("events:subscribe",)), "call.*")

        # Topic-scoped wildcard allows matching prefix only
        sub = await broker.subscribe(
            _ctx(allowed_scopes=("events:subscribe:call.wildcard",)),
            "call.*",
        )
        assert sub.is_active is True
        with pytest.raises(EventAuthorizationError):
            await broker.subscribe(
                _ctx(allowed_scopes=("events:subscribe:call.wildcard",)),
                "agent.*",
            )
        await broker.shutdown()

    asyncio.run(scenario())


def test_r2_subscription_handle_has_no_cross_topic_receive_and_uses_connection_consumer() -> None:
    async def scenario() -> None:
        broker = InMemoryEventBroker()
        await broker.startup()
        ctx = _ctx(allowed_scopes=("events:subscribe", "events:subscribe:wildcard"))
        sub_a = await broker.subscribe(ctx, "call.status")
        sub_b = await broker.subscribe(ctx, "agent.status")

        # Subscription handles manage registration/lifecycle only, not queue consumption
        assert not hasattr(sub_a, "receive")
        assert not hasattr(sub_b, "receive")

        consumer = broker.consumer(ctx.connection_id)
        await broker.publish(_env(event_id="evt-b", topic="agent.status"))
        got = await asyncio.wait_for(consumer.receive(), timeout=1.0)
        assert got is not None
        assert got.event_id == "evt-b"
        await broker.shutdown()

    asyncio.run(scenario())


def test_r3_canonical_max_event_bytes_cannot_be_raised_above_65536() -> None:
    with pytest.raises(InvalidEventError):
        _env(max_event_bytes=DEFAULT_MAX_EVENT_BYTES + 1)

    # Unicode UTF-8 byte accounting: 4-byte emoji exceeds 65,536 bytes even with < 20,000 chars
    with pytest.raises(OversizedEventError):
        _env(payload={"text": "\U0001f600" * 17_000})


def test_r4_accepted_port_contracts_and_overloaded_acquire() -> None:
    async def scenario() -> None:
        broker = InMemoryEventBroker()
        assert hasattr(broker, "startup")
        assert hasattr(broker, "shutdown")

        from kaiwen_gateway.modules.events import DurableEventStorePort

        for method_name in ("append", "read_group", "ack", "replay"):
            assert hasattr(DurableEventStorePort, method_name)

        controller = AdmissionController(CoreConfig())
        voice_lease = await controller.acquire(
            VerifiedConnectionContext(
                provider_id="provider-1",
                tenant_id="tenant-a",
                call_id="call-1",
                connection_id="conn-v",
                allowed_scopes=frozenset({"voice:connect"}),
            )
        )
        evt_lease = await controller.acquire_principal(
            _ctx(connection_id="conn-e", allowed_scopes=("events:subscribe",))
        )
        assert isinstance(voice_lease, AdmissionLease)
        assert isinstance(evt_lease, PrincipalAdmissionLease)
        await voice_lease.release()
        await evt_lease.release()

    asyncio.run(scenario())


def test_r5_global_byte_budget_exhaustion_does_not_disconnect_healthy_connection() -> None:
    async def scenario() -> None:
        probe = _env(event_id="e1", payload={"data": "x" * 400})
        budget = probe.serialized_bytes * 3
        broker = InMemoryEventBroker(
            EventBrokerConfig(
                max_queued_events_per_connection=10,
                max_queued_bytes_per_connection=budget,
                global_queued_bytes_budget=budget,
            )
        )
        await broker.startup()
        sub1 = await broker.subscribe(_ctx(connection_id="c1"), "call.status")
        sub2 = await broker.subscribe(_ctx(connection_id="c2"), "call.status")

        r1 = await broker.publish(_env(event_id="e1", payload={"data": "x" * 400}))
        assert r1.delivered_subscribers == 2

        # c1 fits (3 * size == budget), c2 hits global budget (NOT per-connection slow consumer!)
        r2 = await broker.publish(_env(event_id="e2", payload={"data": "x" * 400}))
        assert r2.delivered_subscribers == 1
        assert r2.disconnected_subscribers == 0
        assert r2.dropped_subscribers == 1
        assert sub2.is_active is True

        await sub1.close()
        await sub2.close()
        assert broker.stats().queued_bytes == 0
        await broker.shutdown()

    asyncio.run(scenario())


def _is_started(mod: EventsModule) -> bool:
    return mod.is_started


def test_r6_events_module_rejects_restart_after_terminal_shutdown() -> None:
    async def scenario() -> None:
        module = EventsModule()
        await module.startup()
        assert _is_started(module) is True

        await module.shutdown()
        assert _is_started(module) is False

        with pytest.raises(EventsModuleClosedError):
            await module.startup()
        assert _is_started(module) is False

    asyncio.run(scenario())


def test_r7_default_broker_enforces_provisional_rate_limits(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr("kaiwen_gateway.modules.events.ratelimit.time.monotonic", lambda: 100.0)

    async def scenario() -> None:
        broker = InMemoryEventBroker()
        await broker.startup()
        ctx = _ctx(connection_id="c-rl")
        # Provisional subscribe burst is 20 -> 21st immediate subscribe must be rate-limited
        for i in range(20):
            if i < 20:
                await broker.subscribe(_ctx(connection_id=f"c-rl-{i}"), "call.status")
        with pytest.raises(EventRateLimitError):
            await broker.subscribe(ctx, "call.status")
        await broker.shutdown()

    asyncio.run(scenario())


def test_cross_principal_connection_id_reuse_rejected() -> None:
    async def scenario() -> None:
        broker = InMemoryEventBroker()
        await broker.startup()
        await broker.subscribe(
            _ctx(connection_id="shared-conn", principal_id="principal-1"),
            "call.status",
        )
        with pytest.raises(EventAuthorizationError):
            await broker.subscribe(
                _ctx(connection_id="shared-conn", principal_id="principal-2"),
                "call.status",
            )
        await broker.shutdown()

    asyncio.run(scenario())


def test_default_publish_rate_limit_burst_of_400(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("kaiwen_gateway.modules.events.ratelimit.time.monotonic", lambda: 100.0)

    async def scenario() -> None:
        broker = InMemoryEventBroker()
        await broker.startup()
        envelopes = [_env(event_id=f"evt-{i}", principal_id="pub-burst-1") for i in range(401)]
        for env in envelopes[:400]:
            res = await broker.publish(env)
            assert res.accepted is True
        with pytest.raises(EventRateLimitError):
            await broker.publish(envelopes[400])
        await broker.shutdown()

    asyncio.run(scenario())


def test_partial_fanout_under_global_byte_budget_pressure() -> None:
    async def scenario() -> None:
        probe = _env(event_id="e1", payload={"data": "x" * 400})
        budget = probe.serialized_bytes * 3
        broker = InMemoryEventBroker(
            EventBrokerConfig(
                max_queued_events_per_connection=10,
                max_queued_bytes_per_connection=budget,
                global_queued_bytes_budget=budget,
            )
        )
        await broker.startup()
        sub1 = await broker.subscribe(_ctx(connection_id="c1"), "call.status")
        sub2 = await broker.subscribe(_ctx(connection_id="c2"), "call.status")
        cons1 = broker.consumer("c1")
        cons2 = broker.consumer("c2")

        r1 = await broker.publish(_env(event_id="e1", payload={"data": "x" * 400}))
        assert r1.accepted is True
        assert r1.delivered_subscribers == 2

        # Partial fan-out: one subscriber receives e2, the second drops due to global budget
        r2 = await broker.publish(_env(event_id="e2", payload={"data": "x" * 400}))
        assert r2.accepted is False
        assert r2.delivered_subscribers == 1
        assert r2.dropped_global_budget == 1
        assert r2.disconnected_subscribers == 0
        assert sub1.is_active is True
        assert sub2.is_active is True

        assert await cons1.receive() is not None
        assert await cons2.receive() is not None
        await broker.shutdown()
        assert broker.stats().queued_bytes == 0

    asyncio.run(scenario())
