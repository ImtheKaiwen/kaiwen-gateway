"""Optional Redis Pub/Sub cross-instance live event broker for E3."""

from __future__ import annotations

import asyncio
import contextlib
import json
import uuid
from collections import deque
from dataclasses import dataclass, field
from typing import Any

from kaiwen_gateway.core.identity import VerifiedPrincipalContext, validate_tenant_id
from kaiwen_gateway.modules.events.auth import EventTopicAuthorizerPort
from kaiwen_gateway.modules.events.broker import (
    DEFAULT_PUBLISH_BURST,
    DEFAULT_PUBLISH_RATE_PER_SECOND,
    DEFAULT_SUBSCRIBE_BURST,
    DEFAULT_SUBSCRIBE_RATE_PER_SECOND,
    EventBrokerConfig,
    EventBrokerStats,
    _is_subscription_authorized,
)
from kaiwen_gateway.modules.events.domain import (
    EventAuthorizationError,
    EventBackpressureError,
    EventEnvelope,
    EventRateLimitError,
    EventsBackendUnavailableError,
    EventsError,
    EventsModuleClosedError,
    InvalidEventError,
    PublishResult,
    SlowConsumerPolicy,
    topic_matches_pattern,
    validate_topic,
    validate_topic_pattern,
)
from kaiwen_gateway.modules.events.ports import EventConnectionConsumer
from kaiwen_gateway.modules.events.ratelimit import RateLimitConfig, RateLimiterPort
from kaiwen_gateway.modules.events.redis.config import (
    DEFAULT_INITIAL_RECONNECT_BACKOFF_SECONDS,
    DEFAULT_MAX_RECONNECT_BACKOFF_SECONDS,
    RedisClientConfig,
    create_async_redis_client,
)
from kaiwen_gateway.modules.events.redis.ratelimit import RedisDistributedRateLimiter
from kaiwen_gateway.modules.events.telemetry import (
    EventsErrorCategory,
    EventsTelemetrySink,
    InMemoryEventsTelemetrySink,
)


@dataclass(frozen=True, slots=True)
class RedisPubSubBrokerConfig:
    """Configuration for `RedisPubSubEventBroker`."""

    redis: RedisClientConfig = field(default_factory=RedisClientConfig)
    broker: EventBrokerConfig = field(default_factory=EventBrokerConfig)
    initial_reconnect_backoff_seconds: float = DEFAULT_INITIAL_RECONNECT_BACKOFF_SECONDS
    max_reconnect_backoff_seconds: float = DEFAULT_MAX_RECONNECT_BACKOFF_SECONDS

    def __post_init__(self) -> None:
        if self.initial_reconnect_backoff_seconds <= 0.0:
            raise InvalidEventError("initial_reconnect_backoff_seconds must be positive")
        if self.max_reconnect_backoff_seconds < self.initial_reconnect_backoff_seconds:
            raise InvalidEventError(
                "max_reconnect_backoff_seconds must be >= initial_reconnect_backoff_seconds"
            )


class _RedisConnectionQueueState:
    """Single multiplexed bounded outbound queue for one local subscriber connection."""

    __slots__ = (
        "connection_id",
        "tenant_id",
        "principal_id",
        "queue",
        "queued_bytes",
        "subscriptions",
        "closed",
        "cond",
        "consumer",
    )

    def __init__(
        self,
        *,
        connection_id: str,
        tenant_id: str,
        principal_id: str,
        lock: asyncio.Lock,
    ) -> None:
        self.connection_id: str = connection_id
        self.tenant_id: str = tenant_id
        self.principal_id: str = principal_id
        self.queue: deque[EventEnvelope] = deque()
        self.queued_bytes: int = 0
        self.subscriptions: dict[str, RedisEventSubscription] = {}
        self.closed: bool = False
        self.cond: asyncio.Condition = asyncio.Condition(lock)
        self.consumer: RedisEventConsumer | None = None


class RedisEventSubscription:
    """Registration and lifecycle handle for a Redis-backed topic subscription."""

    __slots__ = (
        "_broker",
        "_subscription_id",
        "_connection_id",
        "_tenant_id",
        "_topic_pattern",
        "_closed",
    )

    def __init__(
        self,
        *,
        broker: RedisPubSubEventBroker,
        subscription_id: str,
        connection_id: str,
        tenant_id: str,
        topic_pattern: str,
    ) -> None:
        self._broker: RedisPubSubEventBroker = broker
        self._subscription_id: str = subscription_id
        self._connection_id: str = connection_id
        self._tenant_id: str = tenant_id
        self._topic_pattern: str = topic_pattern
        self._closed: bool = False

    @property
    def subscription_id(self) -> str:
        return self._subscription_id

    @property
    def connection_id(self) -> str:
        return self._connection_id

    @property
    def tenant_id(self) -> str:
        return self._tenant_id

    @property
    def topic_pattern(self) -> str:
        return self._topic_pattern

    @property
    def is_active(self) -> bool:
        return not self._closed and self._broker._is_subscription_active(self._subscription_id)

    async def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        await self._broker.unsubscribe(self._subscription_id)


class RedisEventConsumer:
    """Single connection-level consumer draining a connection's multiplexed outbound queue."""

    __slots__ = ("_broker", "_connection_id")

    def __init__(self, *, broker: RedisPubSubEventBroker, connection_id: str) -> None:
        self._broker: RedisPubSubEventBroker = broker
        self._connection_id: str = connection_id

    @property
    def connection_id(self) -> str:
        return self._connection_id

    @property
    def is_active(self) -> bool:
        return self._broker._is_connection_active(self._connection_id)

    async def receive(self) -> EventEnvelope | None:
        return await self._broker._receive_for_connection(self._connection_id)

    async def close(self) -> None:
        await self._broker.close_connection(self._connection_id)

    def __aiter__(self) -> RedisEventConsumer:
        return self

    async def __anext__(self) -> EventEnvelope:
        envelope = await self.receive()
        if envelope is None:
            raise StopAsyncIteration
        return envelope


class RedisPubSubEventBroker:
    """Cross-instance at-most-once live event broker backed by `redis.asyncio` Pub/Sub.

    Architecture invariants:
    - Channel namespace: `{key_prefix}:evt:v1:{tenant_id}:{topic}` (inputs validated).
    - Single dedicated background listener per Gateway instance (never per-subscription).
    - Zero local+Redis duplication: `publish()` writes to Redis only; local subscribers
      receive events via the Redis Pub/Sub listener.
    - Fail-closed on Redis outage (`EventsBackendUnavailableError`): never silently
      downgrades to process-local delivery.
    - Bounded exponential reconnect backoff (`100 ms` -> `5 s` cap) with clean
      cancellation-safe `shutdown()`.
    """

    __slots__ = (
        "_config",
        "_telemetry",
        "_client",
        "_owns_client",
        "_pubsub",
        "_publish_rate_limiter",
        "_subscribe_rate_limiter",
        "_topic_authorizer",
        "_lock",
        "_pubsub_op_lock",
        "_connections",
        "_subscriptions",
        "_tenant_connections",
        "_exact_refcounts",
        "_pattern_refcounts",
        "_pending_unsubs",
        "_global_queued_bytes",
        "_global_queued_events",
        "_listener_task",
        "_started",
        "_closed",
        "_healthy",
    )

    def __init__(
        self,
        config: RedisPubSubBrokerConfig | None = None,
        *,
        redis_client: Any | None = None,
        telemetry: EventsTelemetrySink | None = None,
        publish_rate_limiter: RateLimiterPort | None = None,
        subscribe_rate_limiter: RateLimiterPort | None = None,
        topic_authorizer: EventTopicAuthorizerPort | None = None,
    ) -> None:
        self._config: RedisPubSubBrokerConfig = (
            config if config is not None else RedisPubSubBrokerConfig()
        )
        if topic_authorizer is not None and not isinstance(
            topic_authorizer, EventTopicAuthorizerPort
        ):
            raise TypeError("topic_authorizer must implement EventTopicAuthorizerPort")
        self._topic_authorizer: EventTopicAuthorizerPort | None = topic_authorizer
        self._telemetry: EventsTelemetrySink = (
            telemetry if telemetry is not None else InMemoryEventsTelemetrySink()
        )
        if redis_client is not None:
            self._client: Any = redis_client
            self._owns_client: bool = False
        else:
            self._client = create_async_redis_client(self._config.redis)
            self._owns_client = True

        self._pubsub: Any | None = None

        broker_cfg = self._config.broker
        prefix = self._config.redis.key_prefix
        if publish_rate_limiter is not None:
            self._publish_rate_limiter: RateLimiterPort | None = publish_rate_limiter
        elif broker_cfg.enable_rate_limiting:
            pub_cfg = broker_cfg.publish_rate_limit or RateLimitConfig(
                rate_per_second=DEFAULT_PUBLISH_RATE_PER_SECOND,
                burst=DEFAULT_PUBLISH_BURST,
            )
            self._publish_rate_limiter = RedisDistributedRateLimiter(
                redis_client=self._client,
                config=pub_cfg,
                scope="pub",
                key_prefix=prefix,
            )
        else:
            self._publish_rate_limiter = None

        if subscribe_rate_limiter is not None:
            self._subscribe_rate_limiter: RateLimiterPort | None = subscribe_rate_limiter
        elif broker_cfg.enable_rate_limiting:
            sub_cfg = broker_cfg.subscribe_rate_limit or RateLimitConfig(
                rate_per_second=DEFAULT_SUBSCRIBE_RATE_PER_SECOND,
                burst=DEFAULT_SUBSCRIBE_BURST,
            )
            self._subscribe_rate_limiter = RedisDistributedRateLimiter(
                redis_client=self._client,
                config=sub_cfg,
                scope="sub",
                key_prefix=prefix,
            )
        else:
            self._subscribe_rate_limiter = None

        self._lock: asyncio.Lock = asyncio.Lock()
        self._pubsub_op_lock: asyncio.Lock = asyncio.Lock()
        self._connections: dict[str, _RedisConnectionQueueState] = {}
        self._subscriptions: dict[str, RedisEventSubscription] = {}
        self._tenant_connections: dict[str, set[str]] = {}
        self._exact_refcounts: dict[str, int] = {}
        self._pattern_refcounts: dict[str, int] = {}
        self._pending_unsubs: list[tuple[bool, str]] = []
        self._global_queued_bytes: int = 0
        self._global_queued_events: int = 0
        self._listener_task: asyncio.Task[None] | None = None
        self._started: bool = False
        self._closed: bool = False
        self._healthy: bool = False

    def _is_closed_state(self) -> bool:
        return self._closed

    @property
    def is_closed(self) -> bool:
        return self._closed

    @property
    def is_healthy(self) -> bool:
        return self._healthy and not self._closed

    @property
    def is_started(self) -> bool:
        return self._started and not self._closed

    @property
    def topic_authorizer(self) -> EventTopicAuthorizerPort | None:
        """Return optional per-subscription topic authorizer hook."""
        return self._topic_authorizer

    def stats(self) -> EventBrokerStats:
        return EventBrokerStats(
            active_connections=len(self._connections),
            active_subscriptions=len(self._subscriptions),
            queued_events=self._global_queued_events,
            queued_bytes=self._global_queued_bytes,
            global_queued_bytes_budget=self._config.broker.global_queued_bytes_budget,
        )

    def _channel_name(self, tenant_id: str, topic: str) -> str:
        t_id = validate_tenant_id(tenant_id)
        t_topic = validate_topic(topic)
        return f"{self._config.redis.key_prefix}:evt:v1:{t_id}:{t_topic}"

    def _pattern_name(self, tenant_id: str, topic_pattern: str) -> tuple[bool, str]:
        t_id = validate_tenant_id(tenant_id)
        pat = validate_topic_pattern(topic_pattern)
        is_wild = pat.endswith(".*")
        return is_wild, f"{self._config.redis.key_prefix}:evt:v1:{t_id}:{pat}"

    def _sync_gauges_unlocked(self) -> None:
        self._telemetry.update_gauges(
            active_connections=len(self._connections),
            active_subscriptions=len(self._subscriptions),
            queued_events=self._global_queued_events,
            queued_bytes=self._global_queued_bytes,
        )

    def _is_subscription_active(self, subscription_id: str) -> bool:
        return not self._closed and subscription_id in self._subscriptions

    def _is_connection_active(self, connection_id: str) -> bool:
        if self._closed:
            return False
        conn = self._connections.get(connection_id)
        return conn is not None and not conn.closed

    def consumer(self, connection_id: str) -> EventConnectionConsumer:
        conn = self._connections.get(connection_id)
        if conn is None or conn.closed:
            raise EventsModuleClosedError(f"No active subscriber connection {connection_id!r}")
        if conn.consumer is None:
            conn.consumer = RedisEventConsumer(broker=self, connection_id=connection_id)
        return conn.consumer

    async def startup(self) -> None:
        """Connect to Redis, verify health, and start the single background Pub/Sub listener."""
        async with self._lock:
            if self._closed:
                raise EventsModuleClosedError(
                    "Cannot restart RedisPubSubEventBroker after terminal shutdown"
                )
            if self._started:
                return
            try:
                await self._client.ping()
                pubsub = self._client.pubsub()
                self._pubsub = pubsub
            except Exception as exc:
                self._healthy = False
                self._telemetry.record_error(EventsErrorCategory.BACKEND_UNAVAILABLE)
                raise EventsBackendUnavailableError(
                    "Redis Pub/Sub backend unavailable during startup"
                ) from exc

            self._started = True
            self._healthy = True
            self._listener_task = asyncio.create_task(
                self._listener_loop(),
                name="gw-redis-events-listener",
            )

    async def shutdown(self) -> None:
        """Cancel background listener, close Redis Pub/Sub & local queues cleanly."""
        async with self._lock:
            if self._closed:
                return
            self._closed = True
            self._started = False
            self._healthy = False
            listener = self._listener_task
            self._listener_task = None
            pubsub = self._pubsub
            self._pubsub = None
            for conn_id in list(self._connections):
                self._close_connection_unlocked(conn_id)
            self._exact_refcounts.clear()
            self._pattern_refcounts.clear()
            self._global_queued_bytes = 0
            self._global_queued_events = 0
            self._sync_gauges_unlocked()

        if listener is not None:
            listener.cancel()
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await listener

        if pubsub is not None:
            with contextlib.suppress(Exception):
                await pubsub.aclose() if hasattr(pubsub, "aclose") else await pubsub.close()

        if self._owns_client:
            with contextlib.suppress(Exception):
                await (
                    self._client.aclose()
                    if hasattr(self._client, "aclose")
                    else self._client.close()
                )

    async def _resubscribe_all(self, pubsub: Any) -> None:
        async with self._lock:
            exact = list(self._exact_refcounts.keys())
            patterns = list(self._pattern_refcounts.keys())
        if exact:
            await pubsub.subscribe(*exact)
        if patterns:
            await pubsub.psubscribe(*patterns)

    async def _listener_loop(self) -> None:
        backoff = self._config.initial_reconnect_backoff_seconds
        max_backoff = self._config.max_reconnect_backoff_seconds
        while not self._closed:
            pubsub = self._pubsub
            if pubsub is None:
                candidate_pubsub: Any | None = None
                try:
                    await self._client.ping()
                    candidate_pubsub = self._client.pubsub()
                    await self._resubscribe_all(candidate_pubsub)
                    self._pubsub = candidate_pubsub
                    self._healthy = True
                    backoff = self._config.initial_reconnect_backoff_seconds
                    pubsub = candidate_pubsub
                except asyncio.CancelledError:
                    if candidate_pubsub is not None:
                        with contextlib.suppress(Exception):
                            await (
                                candidate_pubsub.aclose()
                                if hasattr(candidate_pubsub, "aclose")
                                else candidate_pubsub.close()
                            )
                    raise
                except Exception:
                    self._healthy = False
                    self._telemetry.record_error(EventsErrorCategory.BACKEND_UNAVAILABLE)
                    if candidate_pubsub is not None:
                        with contextlib.suppress(Exception):
                            await (
                                candidate_pubsub.aclose()
                                if hasattr(candidate_pubsub, "aclose")
                                else candidate_pubsub.close()
                            )
                    await asyncio.sleep(backoff)
                    backoff = min(max_backoff, backoff * 2.0)
                    continue

            if not self._healthy:
                try:
                    await self._client.ping()
                    self._healthy = True
                    backoff = self._config.initial_reconnect_backoff_seconds
                except asyncio.CancelledError:
                    raise
                except Exception:
                    self._healthy = False
                    self._telemetry.record_error(EventsErrorCategory.BACKEND_UNAVAILABLE)
                    self._pubsub = None
                    with contextlib.suppress(Exception):
                        await pubsub.aclose() if hasattr(pubsub, "aclose") else await pubsub.close()
                    await asyncio.sleep(backoff)
                    backoff = min(max_backoff, backoff * 2.0)
                    continue

            async with self._lock:
                has_subs = bool(self._exact_refcounts or self._pattern_refcounts)
            if not has_subs:
                await asyncio.sleep(0.05)
                continue

            try:
                msg = await pubsub.get_message(ignore_subscribe_messages=True, timeout=0.5)
                if msg is None:
                    await asyncio.sleep(0)
                    continue
                self._healthy = True
                backoff = self._config.initial_reconnect_backoff_seconds
                await self._handle_redis_message(msg)
            except asyncio.CancelledError:
                raise
            except Exception:
                self._healthy = False
                self._telemetry.record_error(EventsErrorCategory.BACKEND_UNAVAILABLE)
                self._pubsub = None
                with contextlib.suppress(Exception):
                    await pubsub.aclose() if hasattr(pubsub, "aclose") else await pubsub.close()
                await asyncio.sleep(backoff)
                backoff = min(max_backoff, backoff * 2.0)

    async def _handle_redis_message(self, msg: dict[str, Any]) -> None:
        msg_type = msg.get("type")
        if msg_type not in ("message", "pmessage"):
            return
        raw_channel = msg.get("channel")
        raw_data = msg.get("data")
        if isinstance(raw_channel, bytes):
            raw_channel = raw_channel.decode("utf-8", errors="ignore")
        if isinstance(raw_data, bytes):
            raw_data = raw_data.decode("utf-8", errors="ignore")
        if not isinstance(raw_channel, str) or not isinstance(raw_data, str):
            return

        expected_prefix = f"{self._config.redis.key_prefix}:evt:v1:"
        if not raw_channel.startswith(expected_prefix):
            return
        rest = raw_channel[len(expected_prefix) :]
        parts = rest.rsplit(":", 1)
        if len(parts) != 2:
            return
        channel_tenant, channel_topic = parts

        channel_pat_topic: str | None = None
        if msg_type == "pmessage":
            raw_pattern = msg.get("pattern")
            if isinstance(raw_pattern, bytes):
                raw_pattern = raw_pattern.decode("utf-8", errors="ignore")
            if not isinstance(raw_pattern, str) or not raw_pattern.startswith(expected_prefix):
                return
            pat_parts = raw_pattern[len(expected_prefix) :].rsplit(":", 1)
            if len(pat_parts) != 2:
                return
            pat_tenant, channel_pat_topic = pat_parts
            if pat_tenant != channel_tenant:
                self._telemetry.record_error(EventsErrorCategory.AUTHORIZATION)
                return

        try:
            parsed = json.loads(raw_data)
            envelope = EventEnvelope(
                protocol_version=parsed["protocol_version"],
                event_id=parsed["event_id"],
                event_type=parsed["event_type"],
                topic=parsed["topic"],
                timestamp=parsed["timestamp"],
                tenant_id=parsed["tenant_id"],
                principal_id=parsed["principal_id"],
                payload=parsed["payload"],
                correlation_id=parsed.get("correlation_id"),
            )
        except Exception:
            self._telemetry.record_error(EventsErrorCategory.VALIDATION)
            return

        # Defense-in-depth: channel tenant/topic must match verified envelope fields
        if envelope.tenant_id != channel_tenant or envelope.topic != channel_topic:
            self._telemetry.record_error(EventsErrorCategory.AUTHORIZATION)
            return

        async with self._lock:
            if self._closed:
                return
            tenant_conn_ids = list(self._tenant_connections.get(envelope.tenant_id, ()))
            for conn_id in tenant_conn_ids:
                conn = self._connections.get(conn_id)
                if conn is None or conn.closed:
                    continue
                has_exact = any(
                    sub.topic_pattern == envelope.topic for sub in conn.subscriptions.values()
                )
                if msg_type == "message":
                    if has_exact:
                        self._enqueue_to_connection_unlocked(conn, envelope)
                    continue
                if has_exact:
                    continue
                matching_wilds = [
                    sub.topic_pattern
                    for sub in conn.subscriptions.values()
                    if sub.topic_pattern.endswith(".*")
                    and topic_matches_pattern(envelope.topic, sub.topic_pattern)
                ]
                if matching_wilds and channel_pat_topic == min(matching_wilds):
                    self._enqueue_to_connection_unlocked(conn, envelope)
            self._sync_gauges_unlocked()
        await self._flush_pending_unsubs()

    def _enqueue_to_connection_unlocked(
        self,
        conn: _RedisConnectionQueueState,
        envelope: EventEnvelope,
    ) -> None:
        event_size = envelope.serialized_bytes
        broker_cfg = self._config.broker

        if event_size > broker_cfg.max_queued_bytes_per_connection:
            self._close_connection_unlocked(conn.connection_id)
            return

        per_conn_overflow = (
            len(conn.queue) >= broker_cfg.max_queued_events_per_connection
            or conn.queued_bytes + event_size > broker_cfg.max_queued_bytes_per_connection
        )
        if per_conn_overflow:
            if broker_cfg.slow_consumer_policy == SlowConsumerPolicy.DROP_OLDEST:
                while conn.queue and (
                    len(conn.queue) >= broker_cfg.max_queued_events_per_connection
                    or conn.queued_bytes + event_size > broker_cfg.max_queued_bytes_per_connection
                ):
                    dropped = conn.queue.popleft()
                    conn.queued_bytes = max(0, conn.queued_bytes - dropped.serialized_bytes)
                    self._global_queued_events = max(0, self._global_queued_events - 1)
                    self._global_queued_bytes = max(
                        0, self._global_queued_bytes - dropped.serialized_bytes
                    )
            else:
                self._close_connection_unlocked(conn.connection_id)
                return

        if self._global_queued_bytes + event_size > broker_cfg.global_queued_bytes_budget:
            self._telemetry.record_error(EventsErrorCategory.BACKPRESSURE)
            return

        conn.queue.append(envelope)
        conn.queued_bytes += event_size
        self._global_queued_events += 1
        self._global_queued_bytes += event_size
        conn.cond.notify(1)

    def _close_connection_unlocked(self, connection_id: str) -> int:
        conn = self._connections.pop(connection_id, None)
        if conn is None:
            return 0
        conn.closed = True
        removed_subs = len(conn.subscriptions)
        for sub_id, sub in list(conn.subscriptions.items()):
            sub._closed = True
            self._subscriptions.pop(sub_id, None)
            is_wild, redis_target = self._pattern_name(sub.tenant_id, sub.topic_pattern)
            ref_map = self._pattern_refcounts if is_wild else self._exact_refcounts
            count = ref_map.get(redis_target, 0)
            if count <= 1:
                ref_map.pop(redis_target, None)
                self._pending_unsubs.append((is_wild, redis_target))
            else:
                ref_map[redis_target] = count - 1
        conn.subscriptions.clear()

        if conn.queue:
            self._global_queued_events = max(0, self._global_queued_events - len(conn.queue))
            self._global_queued_bytes = max(0, self._global_queued_bytes - conn.queued_bytes)
            conn.queue.clear()
            conn.queued_bytes = 0

        tenant_set = self._tenant_connections.get(conn.tenant_id)
        if tenant_set is not None:
            tenant_set.discard(connection_id)
            if not tenant_set:
                self._tenant_connections.pop(conn.tenant_id, None)

        conn.cond.notify_all()
        self._sync_gauges_unlocked()
        return removed_subs

    async def _flush_pending_unsubs(self) -> None:
        async with self._pubsub_op_lock:
            async with self._lock:
                if not self._pending_unsubs:
                    return
                pending = list(self._pending_unsubs)
                self._pending_unsubs.clear()
                pubsub = self._pubsub
                if pubsub is None:
                    return
                to_unsub = [
                    (is_wild, target)
                    for is_wild, target in pending
                    if (self._pattern_refcounts if is_wild else self._exact_refcounts).get(
                        target, 0
                    )
                    == 0
                ]
            for is_wild, target in to_unsub:
                with contextlib.suppress(Exception):
                    if is_wild:
                        await pubsub.punsubscribe(target)
                    else:
                        await pubsub.unsubscribe(target)

    async def subscribe(
        self,
        context: VerifiedPrincipalContext,
        topic_pattern: str,
    ) -> RedisEventSubscription:
        if not isinstance(context, VerifiedPrincipalContext):
            raise TypeError(
                f"context must be a VerifiedPrincipalContext, got {type(context).__name__}"
            )
        try:
            canonical_pattern = validate_topic_pattern(topic_pattern)
        except EventsError:
            self._telemetry.record_error(EventsErrorCategory.VALIDATION)
            raise

        if not _is_subscription_authorized(context, canonical_pattern):
            self._telemetry.record_error(EventsErrorCategory.AUTHORIZATION)
            raise EventAuthorizationError(
                "Principal lacks required scope for requested topic pattern"
            )

        if self._topic_authorizer is not None:
            try:
                await self._topic_authorizer.authorize_subscribe(
                    context=context,
                    topic_pattern=canonical_pattern,
                )
            except EventAuthorizationError:
                self._telemetry.record_error(EventsErrorCategory.AUTHORIZATION)
                raise
            except Exception as exc:
                self._telemetry.record_error(EventsErrorCategory.AUTHORIZATION)
                raise EventAuthorizationError("Topic authorization failed") from exc

        if self._subscribe_rate_limiter is not None:
            try:
                decision = await self._subscribe_rate_limiter.check(
                    f"sub:{context.tenant_id}:{context.principal_id}"
                )
            except EventsBackendUnavailableError:
                self._healthy = False
                self._telemetry.record_error(EventsErrorCategory.BACKEND_UNAVAILABLE)
                raise
            if not decision.allowed:
                self._telemetry.record_rate_limit_rejection()
                self._telemetry.record_error(EventsErrorCategory.RATE_LIMIT)
                raise EventRateLimitError(
                    f"Subscription rate limit exceeded; retry after {decision.retry_after_seconds:.3f}s"
                )

        is_wild, redis_target = self._pattern_name(context.tenant_id, canonical_pattern)

        async with self._pubsub_op_lock:
            async with self._lock:
                if self._is_closed_state():
                    self._telemetry.record_error(EventsErrorCategory.CLOSED)
                    raise EventsModuleClosedError("Redis Event broker is closed")
                if not self._healthy or self._pubsub is None:
                    self._telemetry.record_error(EventsErrorCategory.BACKEND_UNAVAILABLE)
                    raise EventsBackendUnavailableError("Redis Pub/Sub backend is unavailable")

                existing_conn = self._connections.get(context.connection_id)
                if existing_conn is not None:
                    if (
                        existing_conn.tenant_id != context.tenant_id
                        or existing_conn.principal_id != context.principal_id
                    ):
                        self._telemetry.record_error(EventsErrorCategory.AUTHORIZATION)
                        raise EventAuthorizationError(
                            "Connection is already bound to a different tenant or principal"
                        )
                    if (
                        len(existing_conn.subscriptions)
                        >= self._config.broker.max_subscriptions_per_connection
                    ):
                        self._telemetry.record_error(EventsErrorCategory.BACKPRESSURE)
                        raise EventBackpressureError(
                            "Connection exceeded max_subscriptions_per_connection"
                        )

                ref_map = self._pattern_refcounts if is_wild else self._exact_refcounts
                need_redis_sub = ref_map.get(redis_target, 0) == 0
                pubsub = self._pubsub

            if need_redis_sub:
                try:
                    if is_wild:
                        await pubsub.psubscribe(redis_target)
                    else:
                        await pubsub.subscribe(redis_target)
                except Exception as exc:
                    self._healthy = False
                    self._telemetry.record_error(EventsErrorCategory.BACKEND_UNAVAILABLE)
                    raise EventsBackendUnavailableError("Redis Pub/Sub subscribe failed") from exc

            async with self._lock:
                if self._is_closed_state():
                    self._telemetry.record_error(EventsErrorCategory.CLOSED)
                    raise EventsModuleClosedError("Redis Event broker is closed")
                if not self._healthy or self._pubsub is None:
                    self._telemetry.record_error(EventsErrorCategory.BACKEND_UNAVAILABLE)
                    raise EventsBackendUnavailableError("Redis Pub/Sub backend is unavailable")

                if need_redis_sub and self._pubsub is not pubsub:
                    try:
                        if is_wild:
                            await self._pubsub.psubscribe(redis_target)
                        else:
                            await self._pubsub.subscribe(redis_target)
                    except Exception as exc:
                        self._healthy = False
                        self._telemetry.record_error(EventsErrorCategory.BACKEND_UNAVAILABLE)
                        raise EventsBackendUnavailableError(
                            "Redis Pub/Sub subscribe failed after reconnect"
                        ) from exc

                conn = self._connections.get(context.connection_id)
                if conn is None:
                    conn = _RedisConnectionQueueState(
                        connection_id=context.connection_id,
                        tenant_id=context.tenant_id,
                        principal_id=context.principal_id,
                        lock=self._lock,
                    )
                    self._connections[context.connection_id] = conn
                    self._tenant_connections.setdefault(context.tenant_id, set()).add(
                        context.connection_id
                    )
                elif (
                    len(conn.subscriptions) >= self._config.broker.max_subscriptions_per_connection
                ):
                    self._telemetry.record_error(EventsErrorCategory.BACKPRESSURE)
                    raise EventBackpressureError(
                        "Connection exceeded max_subscriptions_per_connection"
                    )

                ref_map = self._pattern_refcounts if is_wild else self._exact_refcounts
                ref_map[redis_target] = ref_map.get(redis_target, 0) + 1
                sub_id = f"sub_{uuid.uuid4().hex[:16]}"
                sub = RedisEventSubscription(
                    broker=self,
                    subscription_id=sub_id,
                    connection_id=context.connection_id,
                    tenant_id=context.tenant_id,
                    topic_pattern=canonical_pattern,
                )
                conn.subscriptions[sub_id] = sub
                self._subscriptions[sub_id] = sub
                self._sync_gauges_unlocked()
                return sub

    async def unsubscribe(self, subscription_id: str) -> bool:
        async with self._pubsub_op_lock:
            need_redis_unsub: tuple[bool, str] | None = None
            async with self._lock:
                sub = self._subscriptions.pop(subscription_id, None)
                if sub is None:
                    return False
                sub._closed = True
                is_wild, redis_target = self._pattern_name(sub.tenant_id, sub.topic_pattern)
                ref_map = self._pattern_refcounts if is_wild else self._exact_refcounts
                count = ref_map.get(redis_target, 0)
                if count <= 1:
                    ref_map.pop(redis_target, None)
                    need_redis_unsub = (is_wild, redis_target)
                else:
                    ref_map[redis_target] = count - 1

                conn = self._connections.get(sub.connection_id)
                if conn is not None:
                    conn.subscriptions.pop(subscription_id, None)
                    conn.cond.notify_all()
                    if not conn.subscriptions:
                        self._close_connection_unlocked(sub.connection_id)
                self._sync_gauges_unlocked()
                pubsub = self._pubsub

            if need_redis_unsub is not None and pubsub is not None:
                is_wild, redis_target = need_redis_unsub
                with contextlib.suppress(Exception):
                    if is_wild:
                        await pubsub.punsubscribe(redis_target)
                    else:
                        await pubsub.unsubscribe(redis_target)
            return True

    async def close_connection(self, connection_id: str) -> int:
        sub_ids: list[str]
        async with self._lock:
            conn = self._connections.get(connection_id)
            if conn is None:
                return 0
            sub_ids = list(conn.subscriptions.keys())
        for sub_id in sub_ids:
            await self.unsubscribe(sub_id)
        async with self._lock:
            self._close_connection_unlocked(connection_id)
        return len(sub_ids)

    async def _receive_for_connection(self, connection_id: str) -> EventEnvelope | None:
        async with self._lock:
            while True:
                if self._closed:
                    return None
                conn = self._connections.get(connection_id)
                if conn is None or conn.closed:
                    return None
                if conn.queue:
                    envelope = conn.queue.popleft()
                    conn.queued_bytes = max(0, conn.queued_bytes - envelope.serialized_bytes)
                    self._global_queued_events = max(0, self._global_queued_events - 1)
                    self._global_queued_bytes = max(
                        0, self._global_queued_bytes - envelope.serialized_bytes
                    )
                    self._sync_gauges_unlocked()
                    return envelope
                await conn.cond.wait()

    async def publish(self, envelope: EventEnvelope) -> PublishResult:
        """Publish an EventEnvelope to Redis Pub/Sub only (zero local+Redis duplication)."""
        if not isinstance(envelope, EventEnvelope):
            raise TypeError(f"envelope must be an EventEnvelope, got {type(envelope).__name__}")

        if self._publish_rate_limiter is not None:
            try:
                decision = await self._publish_rate_limiter.check(
                    f"pub:{envelope.tenant_id}:{envelope.principal_id}"
                )
            except EventsBackendUnavailableError:
                self._healthy = False
                self._telemetry.record_error(EventsErrorCategory.BACKEND_UNAVAILABLE)
                raise
            if not decision.allowed:
                self._telemetry.record_rate_limit_rejection()
                self._telemetry.record_error(EventsErrorCategory.RATE_LIMIT)
                raise EventRateLimitError(
                    f"Publish rate limit exceeded; retry after {decision.retry_after_seconds:.3f}s"
                )

        if self._closed:
            self._telemetry.record_error(EventsErrorCategory.CLOSED)
            raise EventsModuleClosedError("Redis Event broker is closed")
        if not self._healthy:
            self._telemetry.record_error(EventsErrorCategory.BACKEND_UNAVAILABLE)
            raise EventsBackendUnavailableError("Redis Pub/Sub backend is unavailable")

        channel = self._channel_name(envelope.tenant_id, envelope.topic)
        try:
            receivers = int(await self._client.publish(channel, envelope.to_json()))
        except Exception as exc:
            self._healthy = False
            self._telemetry.record_error(EventsErrorCategory.BACKEND_UNAVAILABLE)
            raise EventsBackendUnavailableError("Redis Pub/Sub publish failed") from exc

        self._telemetry.record_publish(
            delivered=receivers,
            dropped_slow_consumer=0,
            disconnected_slow_consumer=0,
        )
        return PublishResult(
            event_id=envelope.event_id,
            topic=envelope.topic,
            matched_subscribers=receivers,
            delivered_subscribers=receivers,
            dropped_subscribers=0,
            disconnected_subscribers=0,
            accepted=True,
            durable_persisted=False,
        )


__all__ = [
    "RedisEventConsumer",
    "RedisEventSubscription",
    "RedisPubSubBrokerConfig",
    "RedisPubSubEventBroker",
]
