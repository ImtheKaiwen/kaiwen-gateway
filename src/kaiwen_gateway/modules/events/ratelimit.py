"""Deterministic, bounded in-memory token-bucket rate limiter for Events."""

from __future__ import annotations

import asyncio
import math
import time
from collections import OrderedDict
from collections.abc import Callable
from dataclasses import dataclass

from kaiwen_gateway.modules.events.domain import InvalidEventError


@dataclass(frozen=True, slots=True)
class RateLimitConfig:
    """Validated token-bucket rate limit configuration."""

    rate_per_second: float
    burst: float
    bucket_ttl_seconds: float = 300.0
    max_buckets: int = 10_000

    def __post_init__(self) -> None:
        if not math.isfinite(self.rate_per_second) or self.rate_per_second <= 0.0:
            raise InvalidEventError("rate_per_second must be a finite positive number")
        if not math.isfinite(self.burst) or self.burst < 1.0:
            raise InvalidEventError("burst must be a finite number >= 1.0")
        if not math.isfinite(self.bucket_ttl_seconds) or self.bucket_ttl_seconds <= 0.0:
            raise InvalidEventError("bucket_ttl_seconds must be a finite positive number")
        if isinstance(self.max_buckets, bool) or self.max_buckets <= 0:
            raise InvalidEventError("max_buckets must be a positive int")


@dataclass(frozen=True, slots=True)
class RateLimitDecision:
    """Result of a single token-bucket admission check."""

    allowed: bool
    remaining_tokens: float
    retry_after_seconds: float


@dataclass(slots=True)
class _BucketState:
    tokens: float
    last_refill: float
    last_seen: float


class TokenBucketRateLimiter:
    """Bounded, monotonic-clock token-bucket rate limiter with TTL and LRU eviction."""

    __slots__ = ("_config", "_clock", "_buckets", "_lock")

    def __init__(
        self,
        config: RateLimitConfig,
        *,
        clock: Callable[[], float] | None = None,
    ) -> None:
        if not isinstance(config, RateLimitConfig):
            raise TypeError(f"config must be a RateLimitConfig, got {type(config).__name__}")
        self._config: RateLimitConfig = config
        self._clock: Callable[[], float] = clock if clock is not None else time.monotonic
        self._buckets: OrderedDict[str, _BucketState] = OrderedDict()
        self._lock: asyncio.Lock = asyncio.Lock()

    @property
    def config(self) -> RateLimitConfig:
        """Return the immutable rate limit configuration."""
        return self._config

    @property
    def active_bucket_count(self) -> int:
        """Return the current number of tracked buckets."""
        return len(self._buckets)

    def _evict_expired(self, now: float) -> None:
        ttl = self._config.bucket_ttl_seconds
        expired_keys = [
            key for key, bucket in self._buckets.items() if (now - bucket.last_seen) >= ttl
        ]
        for key in expired_keys:
            self._buckets.pop(key, None)

    def prune_expired(self) -> int:
        """Synchronously evict expired buckets and return the number removed."""
        now = float(self._clock())
        before = len(self._buckets)
        self._evict_expired(now)
        return before - len(self._buckets)

    def _evaluate_unlocked(self, key: str, *, cost: float) -> RateLimitDecision:
        if not isinstance(key, str) or not key:
            raise InvalidEventError("rate limit key must be a non-empty str")
        if not math.isfinite(cost) or cost <= 0.0:
            raise InvalidEventError("cost must be a finite positive number")
        if cost > self._config.burst:
            return RateLimitDecision(
                allowed=False,
                remaining_tokens=0.0,
                retry_after_seconds=(cost - self._config.burst) / self._config.rate_per_second,
            )

        now = float(self._clock())
        self._evict_expired(now)

        bucket = self._buckets.get(key)
        if bucket is None:
            while len(self._buckets) >= self._config.max_buckets:
                self._buckets.popitem(last=False)
            bucket = _BucketState(
                tokens=self._config.burst,
                last_refill=now,
                last_seen=now,
            )
            self._buckets[key] = bucket
        else:
            self._buckets.move_to_end(key)
            elapsed = max(0.0, now - bucket.last_refill)
            bucket.tokens = min(
                self._config.burst,
                bucket.tokens + elapsed * self._config.rate_per_second,
            )
            bucket.last_refill = max(bucket.last_refill, now)
            bucket.last_seen = max(bucket.last_seen, now)

        if bucket.tokens >= cost:
            bucket.tokens = max(0.0, bucket.tokens - cost)
            return RateLimitDecision(
                allowed=True,
                remaining_tokens=bucket.tokens,
                retry_after_seconds=0.0,
            )

        deficit = cost - bucket.tokens
        retry_after = max(0.0, deficit / self._config.rate_per_second)
        return RateLimitDecision(
            allowed=False,
            remaining_tokens=max(0.0, bucket.tokens),
            retry_after_seconds=retry_after,
        )

    async def check(self, key: str, *, cost: float = 1.0) -> RateLimitDecision:
        """Atomically check and consume tokens for `key` under the async lock."""
        async with self._lock:
            return self._evaluate_unlocked(key, cost=cost)


__all__ = [
    "RateLimitConfig",
    "RateLimitDecision",
    "TokenBucketRateLimiter",
]
