"""Live Redis integration tests for E3 multi-instance Pub/Sub, rate limiting, and replay.

All tests are skipped unless ``KAIWEN_TEST_REDIS_URL`` is set to a reachable Redis server.
"""

from __future__ import annotations

import asyncio
import logging
import os
import time
import uuid
from urllib.parse import urlparse

import httpx
import pytest
from starlette.testclient import TestClient
from starlette.websockets import WebSocketDisconnect

from kaiwen_gateway.core import CoreConfig, VerifiedPrincipalContext
from kaiwen_gateway.modules.events import (
    EVENTS_PROTOCOL_VERSION,
    EVENTS_TICKET_AUDIENCE,
    EventAuthorizationError,
    EventBrokerConfig,
    EventEnvelope,
    EventsBackendUnavailableError,
    EventsModule,
    EventsTicketClaims,
    InvalidEventError,
    RateLimitConfig,
    SlowConsumerPolicy,
)
from kaiwen_gateway.modules.events.redis import (
    RedisClientConfig,
    RedisDistributedRateLimiter,
    RedisPubSubBrokerConfig,
    RedisPubSubEventBroker,
    RedisTicketReplayCache,
    create_async_redis_client,
)
from kaiwen_gateway.server.app import create_gateway_app

pytestmark = pytest.mark.skipif(
    not os.environ.get("KAIWEN_TEST_REDIS_URL"),
    reason="Opt-in live Redis integration tests require KAIWEN_TEST_REDIS_URL",
)


def _prefix() -> str:
    return f"kwl{uuid.uuid4().hex[:10]}"


def _redis_url() -> str:
    return os.environ["KAIWEN_TEST_REDIS_URL"]


def _ctx(
    *,
    tenant_id: str = "tenant-a",
    principal_id: str = "user-1",
    connection_id: str = "conn-1",
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
    event_id: str,
    topic: str = "call.status",
    tenant_id: str = "tenant-a",
    payload: dict[str, object] | None = None,
) -> EventEnvelope:
    return EventEnvelope(
        protocol_version=EVENTS_PROTOCOL_VERSION,
        event_id=event_id,
        event_type="call.status.updated",
        topic=topic,
        timestamp="2026-11-18T12:00:00Z",
        tenant_id=tenant_id,
        principal_id="pub-1",
        payload=payload if payload is not None else {"ok": True},
    )


async def _wait_until_subscribed(seconds: float = 0.08) -> None:
    await asyncio.sleep(seconds)


async def _wait_for_health(
    broker: RedisPubSubEventBroker, *, target: bool, timeout: float = 5.0
) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if broker.is_healthy is target:
            return
        await asyncio.sleep(0.05)
    raise AssertionError(f"Timed out waiting for broker.is_healthy={target}")


class _TcpChaosProxy:
    """Localhost-only async TCP proxy to inject real Redis disconnects and recovery."""

    def __init__(self, target_host: str, target_port: int) -> None:
        self._target_host = target_host
        self._target_port = target_port
        self._server: asyncio.Server | None = None
        self._accepting = True
        self._active_writers: set[asyncio.StreamWriter] = set()
        self._tasks: set[asyncio.Task[None]] = set()
        self.port: int = 0

    async def start(self) -> None:
        self._server = await asyncio.start_server(self._handle_client, "127.0.0.1", 0)
        sock = self._server.sockets[0]
        self.port = int(sock.getsockname()[1])

    async def _handle_client(
        self, client_reader: asyncio.StreamReader, client_writer: asyncio.StreamWriter
    ) -> None:
        if not self._accepting:
            client_writer.close()
            return
        try:
            upstream_reader, upstream_writer = await asyncio.open_connection(
                self._target_host, self._target_port
            )
        except Exception:
            client_writer.close()
            return
        self._active_writers.add(client_writer)
        self._active_writers.add(upstream_writer)

        async def pipe(src: asyncio.StreamReader, dst: asyncio.StreamWriter) -> None:
            try:
                while True:
                    chunk = await src.read(4096)
                    if not chunk:
                        break
                    dst.write(chunk)
                    await dst.drain()
            except Exception:
                pass
            finally:
                dst.close()

        t1 = asyncio.create_task(pipe(client_reader, upstream_writer))
        t2 = asyncio.create_task(pipe(upstream_reader, client_writer))
        self._tasks.update((t1, t2))
        await asyncio.gather(t1, t2, return_exceptions=True)
        self._active_writers.discard(client_writer)
        self._active_writers.discard(upstream_writer)
        self._tasks.discard(t1)
        self._tasks.discard(t2)

    async def drop_and_reject(self) -> None:
        self._accepting = False
        for writer in list(self._active_writers):
            writer.close()
        await asyncio.sleep(0.05)

    def resume(self) -> None:
        self._accepting = True

    async def stop(self) -> None:
        self._accepting = False
        if self._server is not None:
            self._server.close()
            await self._server.wait_closed()
        for writer in list(self._active_writers):
            writer.close()
        for task in list(self._tasks):
            task.cancel()
        if self._tasks:
            await asyncio.gather(*self._tasks, return_exceptions=True)


def test_live_redis_multi_instance_pubsub_isolation_lifecycle_and_bounded_memory() -> None:
    async def scenario() -> None:
        prefix = _prefix()
        cfg_a = RedisClientConfig(url=_redis_url(), key_prefix=prefix)
        cfg_b = RedisClientConfig(url=_redis_url(), key_prefix=prefix)
        client_a = create_async_redis_client(cfg_a)
        client_b = create_async_redis_client(cfg_b)
        broker_cfg_a = RedisPubSubBrokerConfig(
            redis=cfg_a,
            broker=EventBrokerConfig(
                enable_rate_limiting=False,
                max_queued_events_per_connection=3,
                max_queued_bytes_per_connection=4096,
                slow_consumer_policy=SlowConsumerPolicy.DROP_OLDEST,
            ),
        )
        broker_cfg_b = RedisPubSubBrokerConfig(
            redis=cfg_b,
            broker=EventBrokerConfig(
                enable_rate_limiting=False,
                max_queued_events_per_connection=3,
                max_queued_bytes_per_connection=4096,
                slow_consumer_policy=SlowConsumerPolicy.DROP_OLDEST,
            ),
        )
        broker_a = RedisPubSubEventBroker(broker_cfg_a, redis_client=client_a)
        broker_b = RedisPubSubEventBroker(broker_cfg_b, redis_client=client_b)

        await asyncio.wait_for(broker_a.startup(), timeout=5.0)
        await asyncio.wait_for(broker_b.startup(), timeout=5.0)
        assert broker_a.is_healthy is True
        assert broker_b.is_healthy is True

        try:
            # TEST 1, 3, 4: Cross-instance + local+remote + overlapping patterns (no double delivery)
            sub_local = await broker_a.subscribe(_ctx(connection_id="c-a1"), "call.status")
            sub_b_wild = await broker_b.subscribe(_ctx(connection_id="c-b1"), "call.*")
            sub_b_exact = await broker_b.subscribe(_ctx(connection_id="c-b1"), "call.status")
            # TEST 2: Tenant isolation including colon-containing tenant IDs
            sub_colon_a = await broker_b.subscribe(
                _ctx(tenant_id="tenant:alpha:eu", connection_id="c-colon-a"),
                "call.*",
            )
            sub_colon_b = await broker_b.subscribe(
                _ctx(tenant_id="tenant:beta:us", connection_id="c-colon-b"),
                "call.*",
            )
            await _wait_until_subscribed()

            cons_a = broker_a.consumer("c-a1")
            cons_b = broker_b.consumer("c-b1")
            cons_colon_a = broker_b.consumer("c-colon-a")
            cons_colon_b = broker_b.consumer("c-colon-b")

            await broker_a.publish(_env(event_id="e-shared", tenant_id="tenant-a"))
            got_a = await asyncio.wait_for(cons_a.receive(), timeout=5.0)
            got_b = await asyncio.wait_for(cons_b.receive(), timeout=5.0)
            assert got_a is not None and got_a.event_id == "e-shared"
            assert got_b is not None and got_b.event_id == "e-shared"
            # Verify no second duplicate arrives on c-a1 or c-b1
            with pytest.raises(asyncio.TimeoutError):
                await asyncio.wait_for(cons_a.receive(), timeout=0.15)
            with pytest.raises(asyncio.TimeoutError):
                await asyncio.wait_for(cons_b.receive(), timeout=0.15)

            # Colon-containing tenant isolation
            await broker_a.publish(
                _env(event_id="e-colon-a", tenant_id="tenant:alpha:eu", topic="call.status")
            )
            got_ca = await asyncio.wait_for(cons_colon_a.receive(), timeout=5.0)
            assert got_ca is not None and got_ca.event_id == "e-colon-a"
            assert got_ca.tenant_id == "tenant:alpha:eu"
            with pytest.raises(asyncio.TimeoutError):
                await asyncio.wait_for(cons_colon_b.receive(), timeout=0.15)

            # TEST 5: Subscription lifecycle (unsubscribe, disconnect, reconnect)
            assert await broker_b.unsubscribe(sub_b_wild.subscription_id) is True
            assert await broker_b.unsubscribe(sub_b_exact.subscription_id) is True
            assert await broker_b.unsubscribe(sub_colon_a.subscription_id) is True
            assert await broker_b.unsubscribe(sub_colon_b.subscription_id) is True
            assert await broker_a.unsubscribe(sub_local.subscription_id) is True
            await _wait_until_subscribed(0.05)

            # Re-subscribe on c-b1 and verify stale subscriptions do not fire
            sub_re = await broker_b.subscribe(_ctx(connection_id="c-b2"), "order.created")
            await _wait_until_subscribed()
            await broker_a.publish(
                _env(event_id="e-stale", topic="call.status", tenant_id="tenant-a")
            )
            await broker_a.publish(
                _env(event_id="e-fresh", topic="order.created", tenant_id="tenant-a")
            )
            cons_b2 = broker_b.consumer("c-b2")
            got_b2 = await asyncio.wait_for(cons_b2.receive(), timeout=5.0)
            assert got_b2 is not None and got_b2.event_id == "e-fresh"
            await broker_b.close_connection("c-b2")
            assert not sub_re.is_active

            # TEST 7: Bounded memory (slow consumer drop-oldest does not block fast consumer)
            await broker_b.subscribe(_ctx(connection_id="c-slow"), "burst.topic")
            await broker_b.subscribe(_ctx(connection_id="c-fast"), "burst.topic")
            await _wait_until_subscribed()
            cons_fast = broker_b.consumer("c-fast")
            cons_slow = broker_b.consumer("c-slow")
            for idx in range(6):
                await broker_a.publish(
                    _env(event_id=f"b-{idx}", topic="burst.topic", tenant_id="tenant-a")
                )
            # Slow consumer has max_connection_queue_events=3 -> keeps latest 3
            await asyncio.sleep(0.15)
            slow_ids = []
            for _ in range(3):
                item = await asyncio.wait_for(cons_slow.receive(), timeout=5.0)
                assert item is not None
                slow_ids.append(item.event_id)
            assert slow_ids == ["b-3", "b-4", "b-5"]
            fast_item = await asyncio.wait_for(cons_fast.receive(), timeout=5.0)
            assert fast_item is not None
        finally:
            # TEST 6: Clean shutdown closes Redis clients and listener tasks
            await asyncio.wait_for(broker_a.shutdown(), timeout=5.0)
            await asyncio.wait_for(broker_b.shutdown(), timeout=5.0)
            await client_a.aclose()
            await client_b.aclose()
            assert broker_a.is_closed is True
            assert broker_b.is_closed is True
            assert broker_a._listener_task is None or broker_a._listener_task.done()
            assert broker_b._listener_task is None or broker_b._listener_task.done()

    asyncio.run(scenario())


def test_live_redis_disconnect_readiness_and_reconnect_recovery() -> None:
    async def scenario() -> None:
        parsed = urlparse(_redis_url())
        proxy = _TcpChaosProxy(parsed.hostname or "127.0.0.1", parsed.port or 6379)
        await proxy.start()
        prefix = _prefix()
        cfg = RedisClientConfig(
            url=f"redis://127.0.0.1:{proxy.port}/0",
            key_prefix=prefix,
            connect_timeout_seconds=0.4,
            socket_timeout_seconds=0.4,
        )
        client = create_async_redis_client(cfg)
        broker = RedisPubSubEventBroker(
            RedisPubSubBrokerConfig(
                redis=cfg,
                broker=EventBrokerConfig(enable_rate_limiting=False),
                initial_reconnect_backoff_seconds=0.05,
                max_reconnect_backoff_seconds=0.2,
            ),
            redis_client=client,
        )

        class _ReadyVerifier:
            async def verify_ticket(
                self, *, ticket: str, origin: str, connection_id: str
            ) -> EventsTicketClaims:
                raise EventAuthorizationError("unused")

        app = create_gateway_app(
            CoreConfig(ingress_mode="disabled", events_ingress_mode="authenticated"),
            enable_events=True,
            events_module=EventsModule(broker=broker),
            events_ticket_verifier=_ReadyVerifier(),
            events_allowed_origins=("https://app.example.com",),
        )
        try:
            async with app.router.lifespan_context(app):
                transport = httpx.ASGITransport(app=app)
                async with httpx.AsyncClient(
                    transport=transport, base_url="http://testserver"
                ) as http_client:
                    assert (await http_client.get("/ready")).status_code == 200
                    await broker.subscribe(_ctx(connection_id="c-rec"), "rec.topic")
                    await _wait_until_subscribed()
                    cons = broker.consumer("c-rec")

                    # Inject real TCP outage against Redis
                    await proxy.drop_and_reject()
                    with pytest.raises(EventsBackendUnavailableError):
                        await broker.publish(_env(event_id="e-down", topic="rec.topic"))
                    await _wait_for_health(broker, target=False, timeout=5.0)
                    ready_down = await http_client.get("/ready")
                    assert ready_down.status_code == 503
                    assert ready_down.json()["reason"] == "events_backend_unavailable"

                    # Resume connectivity and verify automatic reconnect + resubscribe
                    proxy.resume()
                    await _wait_for_health(broker, target=True, timeout=5.0)
                    assert (await http_client.get("/ready")).status_code == 200
                    await _wait_until_subscribed(0.1)

                    await broker.publish(_env(event_id="e-recovered", topic="rec.topic"))
                    got = await asyncio.wait_for(cons.receive(), timeout=5.0)
                    assert got is not None and got.event_id == "e-recovered"
        finally:
            await asyncio.wait_for(broker.shutdown(), timeout=5.0)
            await client.aclose()
            await proxy.stop()

    asyncio.run(scenario())


def test_live_redis_distributed_rate_limiter_concurrency_replenishment_and_outage() -> None:
    async def scenario() -> None:
        prefix = _prefix()
        cfg = RedisClientConfig(url=_redis_url(), key_prefix=prefix)
        c1 = create_async_redis_client(cfg)
        c2 = create_async_redis_client(cfg)
        limiter_a = RedisDistributedRateLimiter(
            redis_client=c1,
            config=RateLimitConfig(rate_per_second=10.0, burst=5.0, bucket_ttl_seconds=15.0),
            scope="publish",
            key_prefix=prefix,
        )
        limiter_b = RedisDistributedRateLimiter(
            redis_client=c2,
            config=RateLimitConfig(rate_per_second=10.0, burst=5.0, bucket_ttl_seconds=15.0),
            scope="publish",
            key_prefix=prefix,
        )
        try:
            key = f"tenant-rl:{uuid.uuid4().hex[:8]}"
            # Concurrent burst across two independent limiter instances -> exactly 5 allowed
            decisions = await asyncio.wait_for(
                asyncio.gather(
                    *(
                        (limiter_a if i % 2 == 0 else limiter_b).check(key, cost=1.0)
                        for i in range(16)
                    )
                ),
                timeout=5.0,
            )
            allowed_count = sum(1 for d in decisions if d.allowed)
            assert allowed_count == 5

            # Verify PEXPIRE TTL was set in Redis
            pttl = await c1.pttl(limiter_a._redis_key(key))
            assert 0 < pttl <= 15_000

            # Replenishment: at 10 tokens/s, sleeping 0.25s restores >= 2 tokens
            await asyncio.sleep(0.25)
            d_after = await asyncio.wait_for(limiter_b.check(key, cost=1.0), timeout=5.0)
            assert d_after.allowed is True

            # Input validation
            for bad_cost in (True, False, 0.0, -2.0, float("nan"), float("inf")):
                with pytest.raises(InvalidEventError):
                    await limiter_a.check(key, cost=bad_cost)
            with pytest.raises(InvalidEventError):
                await limiter_a.check("", cost=1.0)
        finally:
            await c1.aclose()
            await c2.aclose()

        # Outage fail-closed (no local fallback)
        bad_cfg = RedisClientConfig(
            url="redis://127.0.0.1:1/0",
            key_prefix=prefix,
            connect_timeout_seconds=0.2,
            socket_timeout_seconds=0.2,
        )
        bad_client = create_async_redis_client(bad_cfg)
        down_limiter = RedisDistributedRateLimiter(
            redis_client=bad_client,
            config=RateLimitConfig(rate_per_second=10.0, burst=5.0),
            key_prefix=prefix,
        )
        try:
            with pytest.raises(EventsBackendUnavailableError):
                await asyncio.wait_for(down_limiter.check("tenant-down", cost=1.0), timeout=5.0)
        finally:
            await bad_client.aclose()

    asyncio.run(scenario())


def test_live_redis_ticket_replay_concurrency_e2_ws_and_secret_redaction(
    caplog: pytest.LogCaptureFixture,
) -> None:
    secret_ticket = "super-secret-live-ticket-999"

    class _Verifier:
        async def verify_ticket(
            self, *, ticket: str, origin: str, connection_id: str
        ) -> EventsTicketClaims:
            if ticket != secret_ticket:
                raise EventAuthorizationError("invalid ticket")
            return EventsTicketClaims(
                jti="jti-live-ws-1",
                tenant_id="tenant:live:1",
                principal_id="user-live",
                allowed_scopes=frozenset({"events:subscribe"}),
                origin=origin,
                audience=EVENTS_TICKET_AUDIENCE,
                expires_at=time.time() + 25.0,
            )

    async def scenario() -> None:
        prefix = _prefix()
        cfg = RedisClientConfig(
            url=_redis_url().replace("redis://", "redis://user:supersecretpass@")
            if "@" not in _redis_url()
            else _redis_url(),
            key_prefix=prefix,
        )
        assert "supersecretpass" not in repr(cfg)

        real_cfg = RedisClientConfig(url=_redis_url(), key_prefix=prefix)
        c1 = create_async_redis_client(real_cfg)
        c2 = create_async_redis_client(real_cfg)
        replay_a = RedisTicketReplayCache(redis_client=c1, key_prefix=prefix)
        replay_b = RedisTicketReplayCache(redis_client=c2, key_prefix=prefix)
        try:
            # 1 & 2: Instance A consumes, Instance B rejects
            jti_seq = f"jti-seq-{uuid.uuid4().hex[:8]}"
            assert await replay_a.consume(jti_seq, expires_at=time.time() + 30.0) is True
            assert await replay_b.consume(jti_seq, expires_at=time.time() + 30.0) is False

            # 3 & 4: Concurrent consumption -> exactly one succeeds
            jti_race = f"jti-race-{uuid.uuid4().hex[:8]}"
            exp = time.time() + 30.0
            res = await asyncio.wait_for(
                asyncio.gather(
                    replay_a.consume(jti_race, expires_at=exp),
                    replay_b.consume(jti_race, expires_at=exp),
                ),
                timeout=5.0,
            )
            assert sorted(res) == [False, True]

            # 5 & 6: Expired and malformed rejected
            assert await replay_a.consume("jti-expired", expires_at=time.time() - 1.0) is False
            assert await replay_a.consume("", expires_at=time.time() + 30.0) is False
        finally:
            await c1.aclose()
            await c2.aclose()

        # 7: Redis outage fails closed
        bad_cfg = RedisClientConfig(
            url="redis://127.0.0.1:1/0",
            key_prefix=prefix,
            connect_timeout_seconds=0.2,
            socket_timeout_seconds=0.2,
        )
        bad_client = create_async_redis_client(bad_cfg)
        replay_down = RedisTicketReplayCache(redis_client=bad_client, key_prefix=prefix)
        try:
            with pytest.raises(EventsBackendUnavailableError):
                await asyncio.wait_for(
                    replay_down.consume("jti-outage", expires_at=time.time() + 30.0),
                    timeout=5.0,
                )
        finally:
            await bad_client.aclose()

    with caplog.at_level(logging.DEBUG):
        asyncio.run(scenario())

    # End-to-end /ws/events/v1 with two Gateway apps backed by real Redis
    prefix_ws = _prefix()
    ws_cfg = RedisClientConfig(url=_redis_url(), key_prefix=prefix_ws)
    ws_c1 = create_async_redis_client(ws_cfg)
    ws_c2 = create_async_redis_client(ws_cfg)
    app_a = create_gateway_app(
        CoreConfig(ingress_mode="disabled", events_ingress_mode="authenticated"),
        enable_events=True,
        events_ticket_verifier=_Verifier(),
        events_allowed_origins=("https://app.example.com",),
        events_replay_cache=RedisTicketReplayCache(redis_client=ws_c1, key_prefix=prefix_ws),
    )
    app_b = create_gateway_app(
        CoreConfig(ingress_mode="disabled", events_ingress_mode="authenticated"),
        enable_events=True,
        events_ticket_verifier=_Verifier(),
        events_allowed_origins=("https://app.example.com",),
        events_replay_cache=RedisTicketReplayCache(redis_client=ws_c2, key_prefix=prefix_ws),
    )
    try:
        with TestClient(app_a) as client_a, TestClient(app_b) as client_b:
            # Origin allowlist enforced
            with pytest.raises(WebSocketDisconnect) as bad_origin:
                with client_a.websocket_connect(
                    f"/ws/events/v1?ticket={secret_ticket}",
                    headers={"Origin": "https://evil.example.com"},
                ):
                    pass
            assert bad_origin.value.code == 1008

            # First connection succeeds on Instance A
            with client_a.websocket_connect(
                f"/ws/events/v1?ticket={secret_ticket}",
                headers={"Origin": "https://app.example.com"},
            ) as ws_a:
                assert ws_a.receive_json()["type"] == "session.ready"

            # Replay rejected on Instance B
            with pytest.raises(WebSocketDisconnect) as replay_exc:
                with client_b.websocket_connect(
                    f"/ws/events/v1?ticket={secret_ticket}",
                    headers={"Origin": "https://app.example.com"},
                ):
                    pass
            assert replay_exc.value.code == 1008
    finally:
        import contextlib

        with contextlib.suppress(RuntimeError):
            asyncio.run(ws_c1.aclose())
        with contextlib.suppress(RuntimeError):
            asyncio.run(ws_c2.aclose())

    assert secret_ticket not in caplog.text
    assert "supersecretpass" not in caplog.text
