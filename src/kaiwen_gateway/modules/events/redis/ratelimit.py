"""Atomic Redis Lua token-bucket distributed rate limiter for E3."""

from __future__ import annotations

import hashlib
import math
from collections.abc import Callable
from typing import Any, Final

from kaiwen_gateway.modules.events.domain import EventsBackendUnavailableError, InvalidEventError
from kaiwen_gateway.modules.events.ratelimit import RateLimitConfig, RateLimitDecision

_LUA_TOKEN_BUCKET_SCRIPT: Final[str] = """
local key = KEYS[1]
local rate = tonumber(ARGV[1])
local burst = tonumber(ARGV[2])
local cost = tonumber(ARGV[3])
local ttl_ms = tonumber(ARGV[4])
local now_ms = tonumber(ARGV[5])

if now_ms < 0 then
    local t = redis.call('TIME')
    now_ms = (tonumber(t[1]) * 1000) + math.floor(tonumber(t[2]) / 1000)
end

local state = redis.call('HMGET', key, 'tokens', 'last_ms')
local tokens = tonumber(state[1])
local last_ms = tonumber(state[2])

if tokens == nil or last_ms == nil then
    tokens = burst
    last_ms = now_ms
else
    local elapsed_ms = math.max(0, now_ms - last_ms)
    tokens = math.min(burst, tokens + (elapsed_ms / 1000.0) * rate)
    last_ms = math.max(last_ms, now_ms)
end

local allowed = 0
local retry_ms = 0
if tokens >= cost then
    tokens = math.max(0.0, tokens - cost)
    allowed = 1
else
    local deficit = cost - tokens
    retry_ms = math.ceil((deficit / rate) * 1000.0)
end

redis.call('HSET', key, 'tokens', string.format('%.6f', tokens), 'last_ms', tostring(last_ms))
redis.call('PEXPIRE', key, ttl_ms)
return {allowed, string.format('%.6f', tokens), tostring(retry_ms)}
"""


class RedisDistributedRateLimiter:
    """Atomic Redis Lua token-bucket rate limiter implementing `RateLimiterPort`.

    Fails closed (`EventsBackendUnavailableError`) on Redis errors so distributed
    limits are never silently bypassed during backend outages.
    """

    __slots__ = ("_client", "_config", "_scope", "_key_prefix", "_clock")

    def __init__(
        self,
        *,
        redis_client: Any,
        config: RateLimitConfig,
        scope: str = "events",
        key_prefix: str = "kw",
        clock: Callable[[], float] | None = None,
    ) -> None:
        if not isinstance(config, RateLimitConfig):
            raise TypeError(f"config must be a RateLimitConfig, got {type(config).__name__}")
        if not isinstance(scope, str) or not scope:
            raise InvalidEventError("scope must be a non-empty str")
        self._client: Any = redis_client
        self._config: RateLimitConfig = config
        self._scope: str = scope
        self._key_prefix: str = key_prefix
        self._clock: Callable[[], float] | None = clock

    @property
    def config(self) -> RateLimitConfig:
        """Return immutable rate-limit configuration."""
        return self._config

    def _redis_key(self, key: str) -> str:
        digest = hashlib.sha256(key.encode("utf-8")).hexdigest()[:32]
        return f"{self._key_prefix}:rl:v1:{self._scope}:{digest}"

    async def check(self, key: str, *, cost: float = 1.0) -> RateLimitDecision:
        """Atomically evaluate and consume `cost` tokens in Redis via Lua `EVAL`."""
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

        redis_key = self._redis_key(key)
        ttl_ms = max(1000, int(math.ceil(self._config.bucket_ttl_seconds * 1000.0)))
        now_ms = int(float(self._clock()) * 1000.0) if self._clock is not None else -1

        try:
            raw = await self._client.eval(
                _LUA_TOKEN_BUCKET_SCRIPT,
                1,
                redis_key,
                str(self._config.rate_per_second),
                str(self._config.burst),
                str(cost),
                str(ttl_ms),
                str(now_ms),
            )
            allowed = int(raw[0]) == 1
            remaining = max(0.0, float(raw[1]))
            retry_after_sec = max(0.0, int(raw[2]) / 1000.0)
            return RateLimitDecision(
                allowed=allowed,
                remaining_tokens=remaining,
                retry_after_seconds=retry_after_sec,
            )
        except EventsBackendUnavailableError:
            raise
        except Exception as exc:
            raise EventsBackendUnavailableError(
                "Redis distributed rate limiter unavailable"
            ) from exc


__all__ = ["RedisDistributedRateLimiter"]
