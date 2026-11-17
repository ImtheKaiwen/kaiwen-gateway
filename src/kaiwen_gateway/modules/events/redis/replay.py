"""Cross-instance atomic Redis WebSocket ticket replay cache (`SET NX PX`)."""

from __future__ import annotations

import hashlib
import math
import time
from collections.abc import Callable
from typing import Any

from kaiwen_gateway.modules.events.domain import EventsBackendUnavailableError


class RedisTicketReplayCache:
    """Atomic cross-instance single-use `jti` replay cache backed by Redis `SET NX PX`.

    Implements `TicketReplayCachePort`. Fails closed (`EventsBackendUnavailableError`)
    on Redis connectivity or command errors so replayed or unverified tickets are never accepted.
    """

    __slots__ = ("_client", "_key_prefix", "_clock")

    def __init__(
        self,
        *,
        redis_client: Any,
        key_prefix: str = "kw",
        clock: Callable[[], float] | None = None,
    ) -> None:
        self._client: Any = redis_client
        self._key_prefix: str = key_prefix
        self._clock: Callable[[], float] = clock if clock is not None else time.time

    def _redis_key(self, jti: str) -> str:
        digest = hashlib.sha256(jti.encode("utf-8")).hexdigest()
        return f"{self._key_prefix}:ws:jti:v1:{digest}"

    async def consume(self, jti: str, *, expires_at: float) -> bool:
        """Atomically consume `jti` across all Gateway instances; False if already seen/expired."""
        if not isinstance(jti, str) or not jti:
            return False
        now = float(self._clock())
        if not math.isfinite(expires_at) or expires_at <= now:
            return False

        ttl_ms = max(1, int(math.ceil((expires_at - now) * 1000.0)))
        redis_key = self._redis_key(jti)
        try:
            result = await self._client.set(redis_key, "1", px=ttl_ms, nx=True)
            return bool(result)
        except Exception as exc:
            raise EventsBackendUnavailableError("Redis ticket replay backend unavailable") from exc


__all__ = ["RedisTicketReplayCache"]
