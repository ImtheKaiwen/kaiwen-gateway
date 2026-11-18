"""Offline deterministic + opt-in Redis tests for E3 distributed rate limiting & replay."""

from __future__ import annotations

import asyncio
import os

import pytest
from starlette.testclient import TestClient
from starlette.websockets import WebSocketDisconnect

from kaiwen_gateway.core import CoreConfig
from kaiwen_gateway.modules.events import (
    EVENTS_TICKET_AUDIENCE,
    EventAuthorizationError,
    EventsBackendUnavailableError,
    EventsTicketClaims,
    InvalidEventError,
    RateLimitConfig,
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


class _FakeRedisKV:
    """In-memory atomic simulator for Redis Lua token-bucket `EVAL` and `SET NX PX`."""

    def __init__(self) -> None:
        self.kv: dict[str, tuple[str, float]] = {}
        self.buckets: dict[str, tuple[float, int, float]] = {}
        self.now_ms: int = 1_000_000
        self.fail: bool = False
        self._lock = asyncio.Lock()

    async def set(
        self,
        key: str,
        value: str,
        *,
        px: int | None = None,
        nx: bool = False,
    ) -> bool:
        if self.fail:
            raise ConnectionError("Redis unavailable")
        async with self._lock:
            existing = self.kv.get(key)
            if existing is not None and existing[1] <= self.now_ms:
                self.kv.pop(key, None)
                existing = None
            if nx and existing is not None:
                return False
            exp_ms = float(self.now_ms + (px or 0)) if px is not None else float("inf")
            self.kv[key] = (value, exp_ms)
            return True

    async def eval(
        self,
        script: str,
        numkeys: int,
        key: str,
        rate_s: str,
        burst_s: str,
        cost_s: str,
        ttl_ms_s: str,
        now_ms_s: str,
    ) -> list[object]:
        if self.fail:
            raise ConnectionError("Redis unavailable")
        async with self._lock:
            rate = float(rate_s)
            burst = float(burst_s)
            cost = float(cost_s)
            ttl_ms = int(ttl_ms_s)
            now_ms = int(now_ms_s)
            if now_ms < 0:
                now_ms = self.now_ms

            entry = self.buckets.get(key)
            if entry is not None and entry[2] <= now_ms:
                self.buckets.pop(key, None)
                entry = None

            if entry is None:
                tokens = burst
                last_ms = now_ms
            else:
                prev_tokens, prev_last, _ = entry
                elapsed_ms = max(0, now_ms - prev_last)
                tokens = min(burst, prev_tokens + (elapsed_ms / 1000.0) * rate)
                last_ms = max(prev_last, now_ms)

            if tokens >= cost:
                tokens = max(0.0, tokens - cost)
                allowed = 1
                retry_ms = 0
            else:
                allowed = 0
                retry_ms = int(-(-(cost - tokens) / rate * 1000.0 // 1))

            self.buckets[key] = (tokens, last_ms, float(now_ms + ttl_ms))
            return [allowed, f"{tokens:.6f}", str(retry_ms)]


def test_distributed_rate_limiter_shared_across_instances_and_fail_closed() -> None:
    async def scenario() -> None:
        store = _FakeRedisKV()
        clock_now = 1000.0
        cfg = RateLimitConfig(rate_per_second=2.0, burst=3.0, bucket_ttl_seconds=10.0)
        lim_a = RedisDistributedRateLimiter(
            redis_client=store, config=cfg, scope="pub", clock=lambda: clock_now
        )
        lim_b = RedisDistributedRateLimiter(
            redis_client=store, config=cfg, scope="pub", clock=lambda: clock_now
        )

        # Shared burst=3 across Instance A and Instance B
        assert (await lim_a.check("tenant-a:pub-1")).allowed is True
        assert (await lim_b.check("tenant-a:pub-1")).allowed is True
        assert (await lim_a.check("tenant-a:pub-1")).allowed is True
        d4 = await lim_b.check("tenant-a:pub-1")
        assert d4.allowed is False
        assert d4.retry_after_seconds >= 0.5

        # Concurrent checks never exceed burst
        clock_now = 1015.0  # past TTL -> fresh burst=3
        decisions = await asyncio.gather(
            *(lim_a.check("k-conc") for _ in range(5)),
            *(lim_b.check("k-conc") for _ in range(5)),
        )
        assert sum(1 for d in decisions if d.allowed) == 3

        # Redis failure fails closed (never bypasses)
        store.fail = True
        with pytest.raises(EventsBackendUnavailableError):
            await lim_a.check("tenant-a:pub-1")

    asyncio.run(scenario())


def test_distributed_ticket_replay_across_instances_and_websocket() -> None:
    async def scenario() -> None:
        store = _FakeRedisKV()
        now = 1000.0
        cache_a = RedisTicketReplayCache(redis_client=store, clock=lambda: now)
        cache_b = RedisTicketReplayCache(redis_client=store, clock=lambda: now)

        # Consumed on Instance A -> rejected on Instance B
        assert await cache_a.consume("jti-cross-1", expires_at=1020.0) is True
        assert await cache_b.consume("jti-cross-1", expires_at=1020.0) is False

        # Concurrent consumption across A & B has exact 1 winner
        results = await asyncio.gather(
            cache_a.consume("jti-race", expires_at=1020.0),
            cache_b.consume("jti-race", expires_at=1020.0),
            cache_a.consume("jti-race", expires_at=1020.0),
            cache_b.consume("jti-race", expires_at=1020.0),
        )
        assert sum(1 for r in results if r) == 1

        # Redis failure fails closed
        store.fail = True
        with pytest.raises(EventsBackendUnavailableError):
            await cache_a.consume("jti-fail", expires_at=1020.0)

    asyncio.run(scenario())


def test_websocket_cross_instance_ticket_replay_and_fail_closed() -> None:
    import time

    store = _FakeRedisKV()
    store.now_ms = int(time.time() * 1000)

    class _Verifier:
        async def verify_ticket(
            self, *, ticket: str, origin: str, connection_id: str
        ) -> EventsTicketClaims:
            if ticket != "t-shared":
                raise EventAuthorizationError("bad ticket")
            return EventsTicketClaims(
                jti="jti-ws-shared",
                tenant_id="tenant-a",
                principal_id="u1",
                allowed_scopes=frozenset({"events:subscribe"}),
                origin=origin,
                audience=EVENTS_TICKET_AUDIENCE,
                expires_at=time.time() + 20.0,
            )

    app_a = create_gateway_app(
        CoreConfig(ingress_mode="disabled", events_ingress_mode="authenticated"),
        enable_events=True,
        events_ticket_verifier=_Verifier(),
        events_allowed_origins=("https://app.example.com",),
        events_replay_cache=RedisTicketReplayCache(redis_client=store),
    )
    app_b = create_gateway_app(
        CoreConfig(ingress_mode="disabled", events_ingress_mode="authenticated"),
        enable_events=True,
        events_ticket_verifier=_Verifier(),
        events_allowed_origins=("https://app.example.com",),
        events_replay_cache=RedisTicketReplayCache(redis_client=store),
    )
    with TestClient(app_a) as client_a, TestClient(app_b) as client_b:
        with client_a.websocket_connect(
            "/ws/events/v1?ticket=t-shared",
            headers={"Origin": "https://app.example.com"},
        ) as ws_a:
            assert ws_a.receive_json()["type"] == "session.ready"

        # Instance B rejects replay
        with pytest.raises(WebSocketDisconnect) as exc_b:
            with client_b.websocket_connect(
                "/ws/events/v1?ticket=t-shared",
                headers={"Origin": "https://app.example.com"},
            ):
                pass
        assert exc_b.value.code == 1008

        # Redis failure fails closed (1011)
        store.fail = True
        with pytest.raises(WebSocketDisconnect) as exc_down:
            with client_b.websocket_connect(
                "/ws/events/v1?ticket=t-shared",
                headers={"Origin": "https://app.example.com"},
            ):
                pass
        assert exc_down.value.code == 1011


@pytest.mark.skipif(
    not os.environ.get("KAIWEN_TEST_REDIS_URL"),
    reason="Opt-in live Redis integration test requires KAIWEN_TEST_REDIS_URL",
)
def test_opt_in_live_redis_cross_instance_integration() -> None:
    async def scenario() -> None:
        url = os.environ["KAIWEN_TEST_REDIS_URL"]
        cfg = RedisClientConfig(url=url, key_prefix="kwtest")
        c1 = create_async_redis_client(cfg)
        c2 = create_async_redis_client(cfg)
        broker = RedisPubSubEventBroker(
            RedisPubSubBrokerConfig(redis=cfg),
            redis_client=c1,
        )
        replay = RedisTicketReplayCache(redis_client=c2, key_prefix="kwtest")
        await broker.startup()
        assert broker.is_healthy is True
        assert await replay.consume("optin-jti-1", expires_at=9_999_999_999.0) in (True, False)
        await broker.shutdown()

    asyncio.run(scenario())


def test_p2_direct_instantiation_validation_and_app_replay_cache_type_check() -> None:
    store = _FakeRedisKV()
    for bad_prefix in ("", "BAD", "bad:prefix", "a" * 33):
        with pytest.raises(InvalidEventError):
            RedisDistributedRateLimiter(
                redis_client=store,
                config=RateLimitConfig(rate_per_second=10.0, burst=5.0),
                key_prefix=bad_prefix,
            )
        with pytest.raises(InvalidEventError):
            RedisTicketReplayCache(redis_client=store, key_prefix=bad_prefix)

    limiter = RedisDistributedRateLimiter(
        redis_client=store,
        config=RateLimitConfig(rate_per_second=10.0, burst=5.0),
        key_prefix="kw_p2",
    )
    for bad_cost in (True, False, 0.0, -1.0, float("nan"), float("inf")):
        with pytest.raises(InvalidEventError):
            asyncio.run(limiter.check("tenant-1:principal-1", cost=bad_cost))

    class _P2Verifier:
        async def verify_ticket(
            self, *, ticket: str, origin: str, connection_id: str
        ) -> EventsTicketClaims:
            raise EventAuthorizationError("unused")

    with pytest.raises(TypeError, match="events_replay_cache"):
        create_gateway_app(
            CoreConfig(ingress_mode="disabled", events_ingress_mode="authenticated"),
            enable_events=True,
            events_ticket_verifier=_P2Verifier(),
            events_allowed_origins=("https://app.example.com",),
            events_replay_cache="invalid-cache",  # type: ignore[arg-type]
        )
