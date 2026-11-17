"""Section E tests: TokenBucketRateLimiter deterministic clock, concurrency, and eviction."""

from __future__ import annotations

import asyncio

from kaiwen_gateway.modules.events import RateLimitConfig, TokenBucketRateLimiter


class _FakeClock:
    def __init__(self, initial: float = 100.0) -> None:
        self.now: float = initial

    def __call__(self) -> float:
        return self.now


def test_rate_limiter_burst_refill_and_clock_safety() -> None:
    async def scenario() -> None:
        clock = _FakeClock(100.0)
        limiter = TokenBucketRateLimiter(
            RateLimitConfig(rate_per_second=2.0, burst=3.0, bucket_ttl_seconds=10.0),
            clock=clock,
        )

        # Burst of 3 succeeds
        d1 = await limiter.check("t1:p1")
        d2 = await limiter.check("t1:p1")
        d3 = await limiter.check("t1:p1")
        assert (d1.allowed, d2.allowed, d3.allowed) == (True, True, True)
        assert d3.remaining_tokens == 0.0

        # 4th rejected with deterministic retry_after = 0.5s
        d4 = await limiter.check("t1:p1")
        assert d4.allowed is False
        assert d4.retry_after_seconds == 0.5
        assert d4.remaining_tokens == 0.0

        # Negative clock delta never yields negative tokens or corrupts state
        clock.now = 99.0
        d_back = await limiter.check("t1:p1")
        assert d_back.allowed is False
        assert d_back.remaining_tokens == 0.0

        # Advance 0.5s -> exact 1.0 token available
        clock.now = 100.5
        d5 = await limiter.check("t1:p1")
        assert d5.allowed is True
        assert d5.remaining_tokens == 0.0

    asyncio.run(scenario())


def test_rate_limiter_concurrency_never_over_admits() -> None:
    async def scenario() -> None:
        clock = _FakeClock(0.0)
        limiter = TokenBucketRateLimiter(
            RateLimitConfig(rate_per_second=1.0, burst=5.0),
            clock=clock,
        )
        decisions = await asyncio.gather(*(limiter.check("shared-key") for _ in range(20)))
        allowed_count = sum(1 for d in decisions if d.allowed)
        assert allowed_count == 5

    asyncio.run(scenario())


def test_rate_limiter_ttl_and_lru_eviction() -> None:
    async def scenario() -> None:
        clock = _FakeClock(0.0)
        limiter = TokenBucketRateLimiter(
            RateLimitConfig(
                rate_per_second=1.0,
                burst=1.0,
                bucket_ttl_seconds=5.0,
                max_buckets=3,
            ),
            clock=clock,
        )

        await limiter.check("k1")
        await limiter.check("k2")
        await limiter.check("k3")
        assert limiter.active_bucket_count == 3

        # Adding k4 evicts LRU k1
        await limiter.check("k4")
        assert limiter.active_bucket_count == 3

        # Advancing past TTL expires abandoned buckets
        clock.now = 6.0
        removed = limiter.prune_expired()
        assert removed == 3
        assert limiter.active_bucket_count == 0

    asyncio.run(scenario())
