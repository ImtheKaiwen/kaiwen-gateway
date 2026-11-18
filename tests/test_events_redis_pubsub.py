"""Offline deterministic tests for E3 RedisPubSubEventBroker (Sections A, B, C, F)."""

from __future__ import annotations

import asyncio
import fnmatch
import subprocess
import sys

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
    EventsBackendUnavailableError,
    EventsModule,
    SlowConsumerPolicy,
)
from kaiwen_gateway.modules.events.redis import (
    RedisClientConfig,
    RedisPubSubBrokerConfig,
    RedisPubSubEventBroker,
)
from kaiwen_gateway.server.app import create_gateway_app


class _FakeRedisBus:
    def __init__(self) -> None:
        self.subscribers: list[_FakePubSub] = []
        self.fail_ping: bool = False
        self.fail_publish: bool = False
        self.fail_subscribe: bool = False

    async def publish(self, channel: str, data: str) -> int:
        if self.fail_publish:
            raise ConnectionError("Redis publish failed")
        delivered = 0
        for sub in list(self.subscribers):
            if sub.closed:
                continue
            if channel in sub.channels:
                await sub.incoming.put({"type": "message", "channel": channel, "data": data})
                delivered += 1
            for pat in list(sub.patterns):
                if fnmatch.fnmatchcase(channel, pat):
                    await sub.incoming.put(
                        {"type": "pmessage", "pattern": pat, "channel": channel, "data": data}
                    )
                    delivered += 1
        return delivered


class _FakePubSub:
    def __init__(self, bus: _FakeRedisBus) -> None:
        self.bus = bus
        self.channels: set[str] = set()
        self.patterns: set[str] = set()
        self.incoming: asyncio.Queue[dict[str, str]] = asyncio.Queue()
        self.closed: bool = False
        self.fail_get_message: bool = False
        bus.subscribers.append(self)

    async def subscribe(self, *channels: str) -> None:
        if self.bus.fail_subscribe:
            raise ConnectionError("Redis subscribe failed")
        self.channels.update(channels)

    async def unsubscribe(self, *channels: str) -> None:
        for ch in channels:
            self.channels.discard(ch)

    async def psubscribe(self, *patterns: str) -> None:
        if self.bus.fail_subscribe:
            raise ConnectionError("Redis psubscribe failed")
        self.patterns.update(patterns)

    async def punsubscribe(self, *patterns: str) -> None:
        for p in patterns:
            self.patterns.discard(p)

    async def get_message(
        self, *, ignore_subscribe_messages: bool = True, timeout: float = 0.5
    ) -> dict[str, str] | None:
        if self.fail_get_message:
            raise ConnectionError("Redis listener disconnected")
        try:
            return await asyncio.wait_for(self.incoming.get(), timeout=min(timeout, 0.05))
        except TimeoutError:
            return None

    async def aclose(self) -> None:
        self.closed = True


class _FakeRedisClient:
    def __init__(self, bus: _FakeRedisBus) -> None:
        self.bus = bus
        self.pubsubs_created: int = 0

    async def ping(self) -> bool:
        if self.bus.fail_ping:
            raise ConnectionError("Redis ping failed")
        return True

    def pubsub(self) -> _FakePubSub:
        self.pubsubs_created += 1
        return _FakePubSub(self.bus)

    async def publish(self, channel: str, data: str) -> int:
        return await self.bus.publish(channel, data)


def _ctx(
    *,
    connection_id: str = "conn-1",
    tenant_id: str = "tenant-a",
    principal_id: str = "user-1",
    scopes: tuple[str, ...] = ("events:subscribe", "events:subscribe:wildcard"),
) -> VerifiedPrincipalContext:
    return VerifiedPrincipalContext(
        provider_id="idp",
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
) -> EventEnvelope:
    return EventEnvelope(
        protocol_version=EVENTS_PROTOCOL_VERSION,
        event_id=event_id,
        event_type="call.status.updated",
        topic=topic,
        timestamp="2026-11-18T00:00:00Z",
        tenant_id=tenant_id,
        principal_id="pub-1",
        payload={"ok": True},
    )


def test_cross_instance_pubsub_tenant_isolation_and_dedup() -> None:
    async def scenario() -> None:
        bus = _FakeRedisBus()
        client_a = _FakeRedisClient(bus)
        client_b = _FakeRedisClient(bus)
        cfg = RedisPubSubBrokerConfig(
            broker=EventBrokerConfig(enable_rate_limiting=False),
        )
        broker_a = RedisPubSubEventBroker(cfg, redis_client=client_a)
        broker_b = RedisPubSubEventBroker(cfg, redis_client=client_b)

        await broker_a.startup()
        await broker_b.startup()

        # Unauthorized wildcard denied
        with pytest.raises(EventAuthorizationError):
            await broker_b.subscribe(
                _ctx(connection_id="c-no-wild", scopes=("events:subscribe",)),
                "call.*",
            )

        # Instance B subscribes with overlapping patterns + another tenant + colon tenant
        await broker_b.subscribe(_ctx(connection_id="c-b1"), "call.*")
        await broker_b.subscribe(_ctx(connection_id="c-b1"), "call.status")
        await broker_b.subscribe(_ctx(connection_id="c-b-other", tenant_id="tenant-b"), "call.*")
        await broker_b.subscribe(
            _ctx(connection_id="c-b-colon", tenant_id="org:acme"), "call.status"
        )
        cons_b1 = broker_b.consumer("c-b1")
        cons_b_other = broker_b.consumer("c-b-other")
        cons_b_colon = broker_b.consumer("c-b-colon")

        # Instance A publishes -> Instance B receives exactly once (no duplicate, no tenant leak)
        await broker_a.publish(_env(event_id="evt-cross-1", tenant_id="tenant-a"))
        got = await asyncio.wait_for(cons_b1.receive(), timeout=1.0)
        assert got is not None and got.event_id == "evt-cross-1"

        # Tenant with colon routes cleanly
        await broker_a.publish(_env(event_id="evt-colon-1", tenant_id="org:acme"))
        got_colon = await asyncio.wait_for(cons_b_colon.receive(), timeout=1.0)
        assert got_colon is not None and got_colon.event_id == "evt-colon-1"

        with pytest.raises(TimeoutError):
            await asyncio.wait_for(cons_b1.receive(), timeout=0.05)
        with pytest.raises(TimeoutError):
            await asyncio.wait_for(cons_b_other.receive(), timeout=0.05)

        # Single PubSub connection per instance despite 3 subscriptions
        assert client_b.pubsubs_created == 1

        await broker_a.shutdown()
        await broker_b.shutdown()
        assert broker_b.stats().active_connections == 0
        assert broker_b.stats().queued_bytes == 0

    asyncio.run(scenario())


def _is_healthy(broker: RedisPubSubEventBroker) -> bool:
    return broker.is_healthy


def test_redis_unavailability_reconnect_and_shutdown_while_reconnecting() -> None:
    async def scenario() -> None:
        bus = _FakeRedisBus()
        bus.fail_ping = True
        client = _FakeRedisClient(bus)
        cfg = RedisPubSubBrokerConfig(
            redis=RedisClientConfig(url="redis://localhost:6379/0"),
            broker=EventBrokerConfig(enable_rate_limiting=False),
            initial_reconnect_backoff_seconds=0.02,
            max_reconnect_backoff_seconds=0.08,
        )
        broker = RedisPubSubEventBroker(cfg, redis_client=client)

        # Startup fails closed when Redis is unreachable
        with pytest.raises(EventsBackendUnavailableError):
            await broker.startup()

        # Recover ping and start cleanly
        bus.fail_ping = False
        await broker.startup()
        assert _is_healthy(broker) is True

        await broker.subscribe(_ctx(connection_id="c1"), "call.status")
        cons = broker.consumer("c1")

        # Publish failure marks unhealthy and fails closed (never silently downgrades)
        bus.fail_publish = True
        with pytest.raises(EventsBackendUnavailableError):
            await broker.publish(_env(event_id="evt-fail"))
        assert _is_healthy(broker) is False

        with pytest.raises(EventsBackendUnavailableError):
            await broker.subscribe(_ctx(connection_id="c2"), "call.status")
        # Failed subscribe must not leave an orphaned connection entry
        assert "c2" not in broker._connections

        # Listener reconnects automatically when Redis recovers
        bus.fail_publish = False
        assert broker._pubsub is not None
        broker._pubsub.fail_get_message = True
        for _ in range(25):
            if _is_healthy(broker) and client.pubsubs_created >= 2:
                break
            await asyncio.sleep(0.02)
        assert _is_healthy(broker) is True

        # Active subscription is restored after reconnect
        await broker.publish(_env(event_id="evt-recovered"))
        got = await asyncio.wait_for(cons.receive(), timeout=1.0)
        assert got is not None and got.event_id == "evt-recovered"

        # Shutdown while reconnecting exits cleanly with zero leaked tasks
        bus.fail_ping = True
        assert broker._pubsub is not None
        broker._pubsub.fail_get_message = True
        await asyncio.sleep(0.03)
        await broker.shutdown()
        assert broker.is_closed is True
        assert broker.stats().active_connections == 0

    asyncio.run(scenario())


def test_readiness_reports_503_when_redis_broker_unhealthy() -> None:
    class _Auth:
        async def authenticate_bearer(self, *, token: str, connection_id: str) -> ProviderPrincipal:
            return ProviderPrincipal(provider_id="idp")

    class _Authz:
        async def authorize_principal(
            self, *, principal: ProviderPrincipal, connection_id: str
        ) -> TenantPrincipalBinding:
            return TenantPrincipalBinding(
                authorized_provider_id="idp",
                tenant_id="tenant-a",
                principal_id="u1",
                allowed_scopes=frozenset({"events:subscribe"}),
            )

    bus = _FakeRedisBus()
    client = _FakeRedisClient(bus)
    broker = RedisPubSubEventBroker(
        RedisPubSubBrokerConfig(broker=EventBrokerConfig(enable_rate_limiting=False)),
        redis_client=client,
    )
    app = create_gateway_app(
        CoreConfig(ingress_mode="disabled", events_ingress_mode="authenticated"),
        enable_events=True,
        events_authenticator=_Auth(),
        events_authorizer=_Authz(),
        events_module=EventsModule(broker=broker),
    )
    with TestClient(app) as client_http:
        assert client_http.get("/ready").status_code == 200
        broker._healthy = False
        r = client_http.get("/ready")
        assert r.status_code == 503
        assert r.json()["reason"] == "events_backend_unavailable"


def test_base_and_core_imports_succeed_without_redis_installed() -> None:
    script = """
import importlib.abc
import sys

class _BlockRedis(importlib.abc.MetaPathFinder):
    def find_spec(self, fullname, path=None, target=None):
        if fullname.split(".")[0] == "redis":
            raise ModuleNotFoundError("Blocked redis")
        return None

sys.meta_path.insert(0, _BlockRedis())
import kaiwen_gateway
import kaiwen_gateway.core
import kaiwen_gateway.modules.events
print("NO_REDIS_OK")
"""
    proc = subprocess.run(
        [sys.executable, "-c", script],
        capture_output=True,
        text=True,
        check=True,
    )
    assert proc.stdout.strip() == "NO_REDIS_OK"


def test_transient_publish_error_self_heals_health_and_slow_consumer_cleans_refcounts() -> None:
    async def scenario() -> None:
        bus = _FakeRedisBus()
        client = _FakeRedisClient(bus)
        broker = RedisPubSubEventBroker(
            RedisPubSubBrokerConfig(
                broker=EventBrokerConfig(
                    enable_rate_limiting=False,
                    max_queued_events_per_connection=1,
                    slow_consumer_policy=SlowConsumerPolicy.DISCONNECT_SLOW_CONSUMER,
                ),
                initial_reconnect_backoff_seconds=0.01,
                max_reconnect_backoff_seconds=0.05,
            ),
            redis_client=client,
        )
        await broker.startup()
        try:
            # 1. Transient publish failure while idle sets _healthy=False, then self-heals
            bus.fail_publish = True
            with pytest.raises(EventsBackendUnavailableError):
                await broker.publish(_env(event_id="e-fail"))
            unhealthy_now = broker.is_healthy
            assert unhealthy_now is False
            bus.fail_publish = False
            for _ in range(40):
                if broker.is_healthy:
                    break
                await asyncio.sleep(0.02)
            assert broker.is_healthy is True

            # 2. Slow-consumer disconnect cleans up refcounts and Redis pubsub subscriptions
            await broker.subscribe(_ctx(connection_id="c-slow"), "call.status")
            assert len(broker._exact_refcounts) == 1
            await broker.publish(_env(event_id="e-1"))
            await broker.publish(_env(event_id="e-2"))
            for _ in range(40):
                if not broker._exact_refcounts and not broker._connections:
                    break
                await asyncio.sleep(0.02)
            assert broker._exact_refcounts == {}
            assert broker._connections == {}
            assert broker._pubsub is not None
            assert broker._pubsub.channels == set()

            # 3. Concurrent unsubscribe + subscribe preserves Redis subscription consistency
            sub1 = await broker.subscribe(_ctx(connection_id="c-race-1"), "race.topic")
            await asyncio.gather(
                broker.unsubscribe(sub1.subscription_id),
                broker.subscribe(_ctx(connection_id="c-race-2"), "race.topic"),
            )
            assert broker._exact_refcounts.get("kw:evt:v1:tenant-a:race.topic") == 1
            assert "kw:evt:v1:tenant-a:race.topic" in broker._pubsub.channels
        finally:
            await broker.shutdown()

    asyncio.run(scenario())
