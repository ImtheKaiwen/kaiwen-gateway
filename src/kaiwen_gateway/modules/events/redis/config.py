"""Configuration and lazy `redis.asyncio` client factory for optional E3 Redis adapters."""

from __future__ import annotations

import math
import re
from dataclasses import dataclass
from typing import Any, Final
from urllib.parse import urlparse

from kaiwen_gateway.modules.events.domain import EventsDependencyError, InvalidEventError

DEFAULT_REDIS_CONNECT_TIMEOUT_SECONDS: Final[float] = 2.0
DEFAULT_REDIS_SOCKET_TIMEOUT_SECONDS: Final[float] = 2.0
DEFAULT_INITIAL_RECONNECT_BACKOFF_SECONDS: Final[float] = 0.1
DEFAULT_MAX_RECONNECT_BACKOFF_SECONDS: Final[float] = 5.0
KEY_PREFIX_PATTERN: Final[re.Pattern[str]] = re.compile(r"^[a-z0-9][a-z0-9_-]{0,31}$")


def _redact_redis_url(url: str) -> str:
    parsed = urlparse(url)
    host = parsed.hostname or "unknown"
    port = f":{parsed.port}" if parsed.port else ""
    db = parsed.path or ""
    return f"{parsed.scheme}://{host}{port}{db}"


@dataclass(frozen=True, slots=True)
class RedisClientConfig:
    """Validated configuration for optional Redis client connections."""

    url: str = "redis://localhost:6379/0"
    key_prefix: str = "kw"
    connect_timeout_seconds: float = DEFAULT_REDIS_CONNECT_TIMEOUT_SECONDS
    socket_timeout_seconds: float = DEFAULT_REDIS_SOCKET_TIMEOUT_SECONDS
    tls_ca_certs: str | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.url, str) or not self.url:
            raise InvalidEventError("Redis url must be a non-empty str")
        parsed = urlparse(self.url)
        if parsed.scheme not in ("redis", "rediss", "unix"):
            raise InvalidEventError("Redis url scheme must be 'redis', 'rediss', or 'unix'")
        if not isinstance(self.key_prefix, str) or not KEY_PREFIX_PATTERN.fullmatch(
            self.key_prefix
        ):
            raise InvalidEventError("key_prefix must match ^[a-z0-9][a-z0-9_-]{0,31}$")
        for name, val in (
            ("connect_timeout_seconds", self.connect_timeout_seconds),
            ("socket_timeout_seconds", self.socket_timeout_seconds),
        ):
            if (
                isinstance(val, bool)
                or not isinstance(val, (int, float))
                or not math.isfinite(val)
                or val <= 0.0
            ):
                raise InvalidEventError(f"{name} must be a finite positive number")

    def __repr__(self) -> str:
        return (
            f"RedisClientConfig(url={_redact_redis_url(self.url)!r}, "
            f"key_prefix={self.key_prefix!r})"
        )


def create_async_redis_client(config: RedisClientConfig) -> Any:
    """Lazily import `redis.asyncio` and construct an async Redis client."""
    try:
        import redis.asyncio as aioredis
    except ImportError as exc:  # pragma: no cover
        raise EventsDependencyError(
            "Redis Events adapters require 'kaiwen-gateway[redis]' (`redis>=5.0`) to be installed."
        ) from exc

    kwargs: dict[str, Any] = {
        "socket_connect_timeout": config.connect_timeout_seconds,
        "socket_timeout": config.socket_timeout_seconds,
        "decode_responses": True,
    }
    if config.tls_ca_certs is not None:
        kwargs["ssl_ca_certs"] = config.tls_ca_certs
    return aioredis.Redis.from_url(config.url, **kwargs)


__all__ = [
    "DEFAULT_INITIAL_RECONNECT_BACKOFF_SECONDS",
    "DEFAULT_MAX_RECONNECT_BACKOFF_SECONDS",
    "DEFAULT_REDIS_CONNECT_TIMEOUT_SECONDS",
    "DEFAULT_REDIS_SOCKET_TIMEOUT_SECONDS",
    "KEY_PREFIX_PATTERN",
    "RedisClientConfig",
    "create_async_redis_client",
]
