"""Optional Redis Pub/Sub, Streams durability, distributed rate limiting, and ticket replay (E3/E4)."""

from __future__ import annotations

from kaiwen_gateway.modules.events.redis.broker import (
    RedisEventConsumer,
    RedisEventSubscription,
    RedisPubSubBrokerConfig,
    RedisPubSubEventBroker,
)
from kaiwen_gateway.modules.events.redis.config import (
    DEFAULT_INITIAL_RECONNECT_BACKOFF_SECONDS,
    DEFAULT_MAX_RECONNECT_BACKOFF_SECONDS,
    DEFAULT_REDIS_CONNECT_TIMEOUT_SECONDS,
    DEFAULT_REDIS_SOCKET_TIMEOUT_SECONDS,
    KEY_PREFIX_PATTERN,
    RedisClientConfig,
    create_async_redis_client,
)
from kaiwen_gateway.modules.events.redis.ratelimit import RedisDistributedRateLimiter
from kaiwen_gateway.modules.events.redis.replay import RedisTicketReplayCache
from kaiwen_gateway.modules.events.redis.streams import (
    DEFAULT_CLAIM_MIN_IDLE_MS,
    DEFAULT_DEAD_LETTER_MAXLEN,
    DEFAULT_MAX_DELIVERY_ATTEMPTS,
    DEFAULT_RECOVERY_BATCH_SIZE,
    DEFAULT_RECOVERY_INTERVAL_SECONDS,
    DEFAULT_STREAM_MAX_READ_BATCH_SIZE,
    DEFAULT_STREAM_MAX_REPLAY_LIMIT,
    DEFAULT_STREAM_MAXLEN,
    RedisStreamsConfig,
    RedisStreamsDurableStore,
    StreamDeadLetterRecord,
    StreamPendingSummary,
    StreamRecoveryResult,
    StreamRecoveryWorker,
    StreamStats,
)

__all__ = [
    "DEFAULT_CLAIM_MIN_IDLE_MS",
    "DEFAULT_DEAD_LETTER_MAXLEN",
    "DEFAULT_INITIAL_RECONNECT_BACKOFF_SECONDS",
    "DEFAULT_MAX_DELIVERY_ATTEMPTS",
    "DEFAULT_MAX_RECONNECT_BACKOFF_SECONDS",
    "DEFAULT_RECOVERY_BATCH_SIZE",
    "DEFAULT_RECOVERY_INTERVAL_SECONDS",
    "DEFAULT_REDIS_CONNECT_TIMEOUT_SECONDS",
    "DEFAULT_REDIS_SOCKET_TIMEOUT_SECONDS",
    "DEFAULT_STREAM_MAXLEN",
    "DEFAULT_STREAM_MAX_READ_BATCH_SIZE",
    "DEFAULT_STREAM_MAX_REPLAY_LIMIT",
    "KEY_PREFIX_PATTERN",
    "RedisClientConfig",
    "RedisDistributedRateLimiter",
    "RedisEventConsumer",
    "RedisEventSubscription",
    "RedisPubSubBrokerConfig",
    "RedisPubSubEventBroker",
    "RedisStreamsConfig",
    "RedisStreamsDurableStore",
    "RedisTicketReplayCache",
    "StreamDeadLetterRecord",
    "StreamPendingSummary",
    "StreamRecoveryResult",
    "StreamRecoveryWorker",
    "StreamStats",
    "create_async_redis_client",
]
