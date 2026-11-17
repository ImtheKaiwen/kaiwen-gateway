"""In-memory tenant-scoped, bounded-memory event broker for E1."""

from __future__ import annotations

import asyncio
import uuid
from collections import deque
from collections.abc import AsyncIterator
from dataclasses import dataclass

from kaiwen_gateway.core.identity import VerifiedPrincipalContext, validate_tenant_id
from kaiwen_gateway.modules.events.domain import (
    DEFAULT_GLOBAL_QUEUED_BYTES_BUDGET,
    DEFAULT_MAX_QUEUED_BYTES_PER_CONNECTION,
    DEFAULT_MAX_QUEUED_EVENTS_PER_CONNECTION,
    DEFAULT_MAX_SUBSCRIPTIONS_PER_CONNECTION,
    EventAuthorizationError,
    EventBackpressureError,
    EventEnvelope,
    EventRateLimitError,
    EventsError,
    EventsModuleClosedError,
    InvalidEventError,
    PublishResult,
    SlowConsumerPolicy,
    topic_matches_pattern,
    validate_topic_pattern,
)
from kaiwen_gateway.modules.events.ratelimit import TokenBucketRateLimiter
from kaiwen_gateway.modules.events.telemetry import (
    EventsErrorCategory,
    EventsTelemetrySink,
    InMemoryEventsTelemetrySink,
)


@dataclass(frozen=True, slots=True)
class EventBrokerConfig:
    """Validated bounded limits and policies for InMemoryEventBroker."""

    max_subscriptions_per_connection: int = DEFAULT_MAX_SUBSCRIPTIONS_PER_CONNECTION
    max_queued_events_per_connection: int = DEFAULT_MAX_QUEUED_EVENTS_PER_CONNECTION
    max_queued_bytes_per_connection: int = DEFAULT_MAX_QUEUED_BYTES_PER_CONNECTION
    global_queued_bytes_budget: int = DEFAULT_GLOBAL_QUEUED_BYTES_BUDGET
    slow_consumer_policy: SlowConsumerPolicy = SlowConsumerPolicy.DISCONNECT_SLOW_CONSUMER
    required_subscribe_scopes: tuple[str, ...] = ("events:subscribe", "events:admin")

    def __post_init__(self) -> None:
        for name, value in (
            ("max_subscriptions_per_connection", self.max_subscriptions_per_connection),
            ("max_queued_events_per_connection", self.max_queued_events_per_connection),
            ("max_queued_bytes_per_connection", self.max_queued_bytes_per_connection),
            ("global_queued_bytes_budget", self.global_queued_bytes_budget),
        ):
            if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
                raise InvalidEventError(f"{name} must be a positive int")
        if not isinstance(self.slow_consumer_policy, SlowConsumerPolicy):
            raise InvalidEventError(
                f"slow_consumer_policy must be SlowConsumerPolicy, "
                f"got {type(self.slow_consumer_policy).__name__}"
            )
        if self.max_queued_bytes_per_connection > self.global_queued_bytes_budget:
            raise InvalidEventError(
                "max_queued_bytes_per_connection cannot exceed global_queued_bytes_budget"
            )


@dataclass(frozen=True, slots=True)
class EventBrokerStats:
    """Bounded operational counters for InMemoryEventBroker."""

    active_connections: int
    active_subscriptions: int
    queued_events: int
    queued_bytes: int
    global_queued_bytes_budget: int


class _ConnectionQueueState:
    """Single multiplexed bounded outbound queue for one logical subscriber connection."""

    __slots__ = (
        "connection_id",
        "tenant_id",
        "queue",
        "queued_bytes",
        "subscriptions",
        "closed",
        "cond",
    )

    def __init__(self, *, connection_id: str, tenant_id: str, lock: asyncio.Lock) -> None:
        self.connection_id: str = connection_id
        self.tenant_id: str = tenant_id
        self.queue: deque[EventEnvelope] = deque()
        self.queued_bytes: int = 0
        self.subscriptions: dict[str, InMemoryEventSubscription] = {}
        self.closed: bool = False
        self.cond: asyncio.Condition = asyncio.Condition(lock)


class InMemoryEventSubscription:
    """Active subscription handle backed by a connection's multiplexed outbound queue."""

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
        broker: InMemoryEventBroker,
        subscription_id: str,
        connection_id: str,
        tenant_id: str,
        topic_pattern: str,
    ) -> None:
        self._broker: InMemoryEventBroker = broker
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

    async def receive(self) -> EventEnvelope:
        """Await the next event delivered to this subscription's connection queue."""
        return await self._broker._receive_for_subscription(self)

    async def close(self) -> None:
        """Unsubscribe cleanly and idempotently."""
        if self._closed:
            return
        self._closed = True
        await self._broker.unsubscribe(self._subscription_id)

    def __aiter__(self) -> AsyncIterator[EventEnvelope]:
        return self

    async def __anext__(self) -> EventEnvelope:
        try:
            return await self.receive()
        except EventsModuleClosedError as exc:
            raise StopAsyncIteration from exc

    def __repr__(self) -> str:
        return (
            f"InMemoryEventSubscription(subscription_id={self._subscription_id!r}, "
            f"active={self.is_active})"
        )


class InMemoryEventBroker:
    """Single-instance in-memory tenant-scoped event broker with strict byte budgets."""

    __slots__ = (
        "_config",
        "_telemetry",
        "_publish_rate_limiter",
        "_subscribe_rate_limiter",
        "_lock",
        "_connections",
        "_subscriptions",
        "_tenant_connections",
        "_global_queued_bytes",
        "_global_queued_events",
        "_closed",
    )

    def __init__(
        self,
        config: EventBrokerConfig | None = None,
        *,
        telemetry: EventsTelemetrySink | None = None,
        publish_rate_limiter: TokenBucketRateLimiter | None = None,
        subscribe_rate_limiter: TokenBucketRateLimiter | None = None,
    ) -> None:
        self._config: EventBrokerConfig = config if config is not None else EventBrokerConfig()
        self._telemetry: EventsTelemetrySink = (
            telemetry if telemetry is not None else InMemoryEventsTelemetrySink()
        )
        self._publish_rate_limiter: TokenBucketRateLimiter | None = publish_rate_limiter
        self._subscribe_rate_limiter: TokenBucketRateLimiter | None = subscribe_rate_limiter
        self._lock: asyncio.Lock = asyncio.Lock()
        self._connections: dict[str, _ConnectionQueueState] = {}
        self._subscriptions: dict[str, InMemoryEventSubscription] = {}
        self._tenant_connections: dict[str, set[str]] = {}
        self._global_queued_bytes: int = 0
        self._global_queued_events: int = 0
        self._closed: bool = False

    @property
    def config(self) -> EventBrokerConfig:
        """Return immutable broker configuration."""
        return self._config

    @property
    def telemetry(self) -> EventsTelemetrySink:
        """Return attached telemetry sink."""
        return self._telemetry

    @property
    def is_closed(self) -> bool:
        """Return True if the broker has been closed."""
        return self._closed

    def stats(self) -> EventBrokerStats:
        """Return current bounded connection, subscription, and queued byte counters."""
        return EventBrokerStats(
            active_connections=len(self._connections),
            active_subscriptions=len(self._subscriptions),
            queued_events=self._global_queued_events,
            queued_bytes=self._global_queued_bytes,
            global_queued_bytes_budget=self._config.global_queued_bytes_budget,
        )

    def _sync_gauges_unlocked(self) -> None:
        self._telemetry.update_gauges(
            active_connections=len(self._connections),
            active_subscriptions=len(self._subscriptions),
            queued_events=self._global_queued_events,
            queued_bytes=self._global_queued_bytes,
        )

    def _is_subscription_active(self, subscription_id: str) -> bool:
        return not self._closed and subscription_id in self._subscriptions

    def _close_connection_unlocked(self, connection_id: str) -> int:
        conn = self._connections.pop(connection_id, None)
        if conn is None:
            return 0

        conn.closed = True
        removed_subs = len(conn.subscriptions)
        for sub_id, sub in list(conn.subscriptions.items()):
            sub._closed = True
            self._subscriptions.pop(sub_id, None)
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

    async def subscribe(
        self,
        context: VerifiedPrincipalContext,
        topic_pattern: str,
    ) -> InMemoryEventSubscription:
        """Register a validated topic subscription for a verified principal connection."""
        if not isinstance(context, VerifiedPrincipalContext):
            raise TypeError(
                f"context must be a VerifiedPrincipalContext, got {type(context).__name__}"
            )

        if self._config.required_subscribe_scopes and not any(
            context.has_scope(scope) for scope in self._config.required_subscribe_scopes
        ):
            self._telemetry.record_error(EventsErrorCategory.AUTHORIZATION)
            raise EventAuthorizationError("Principal lacks required scope for event subscription")

        try:
            canonical_pattern = validate_topic_pattern(topic_pattern)
        except EventsError:
            self._telemetry.record_error(EventsErrorCategory.VALIDATION)
            raise

        if self._subscribe_rate_limiter is not None:
            decision = await self._subscribe_rate_limiter.check(
                f"sub:{context.tenant_id}:{context.principal_id}"
            )
            if not decision.allowed:
                self._telemetry.record_rate_limit_rejection()
                self._telemetry.record_error(EventsErrorCategory.RATE_LIMIT)
                raise EventRateLimitError(
                    f"Subscription rate limit exceeded; retry after {decision.retry_after_seconds:.3f}s"
                )

        async with self._lock:
            if self._closed:
                self._telemetry.record_error(EventsErrorCategory.CLOSED)
                raise EventsModuleClosedError("Event broker is closed")

            conn = self._connections.get(context.connection_id)
            if conn is None:
                conn = _ConnectionQueueState(
                    connection_id=context.connection_id,
                    tenant_id=context.tenant_id,
                    lock=self._lock,
                )
                self._connections[context.connection_id] = conn
                self._tenant_connections.setdefault(context.tenant_id, set()).add(
                    context.connection_id
                )
            elif conn.tenant_id != context.tenant_id:
                self._telemetry.record_error(EventsErrorCategory.AUTHORIZATION)
                raise EventAuthorizationError("Connection is already bound to a different tenant")

            if len(conn.subscriptions) >= self._config.max_subscriptions_per_connection:
                self._telemetry.record_error(EventsErrorCategory.BACKPRESSURE)
                raise EventBackpressureError(
                    f"Connection exceeded max_subscriptions_per_connection "
                    f"({self._config.max_subscriptions_per_connection})"
                )

            sub_id = f"sub_{uuid.uuid4().hex[:16]}"
            sub = InMemoryEventSubscription(
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
        """Remove a single subscription; closes its connection queue if none remain."""
        async with self._lock:
            sub = self._subscriptions.pop(subscription_id, None)
            if sub is None:
                return False

            sub._closed = True
            conn = self._connections.get(sub.connection_id)
            if conn is not None:
                conn.subscriptions.pop(subscription_id, None)
                conn.cond.notify_all()
                if not conn.subscriptions:
                    self._close_connection_unlocked(sub.connection_id)
                    return True
            self._sync_gauges_unlocked()
            return True

    async def close_connection(self, connection_id: str) -> int:
        """Close a subscriber connection, unsubscribing all patterns and releasing queued bytes."""
        async with self._lock:
            return self._close_connection_unlocked(connection_id)

    async def _receive_for_subscription(self, sub: InMemoryEventSubscription) -> EventEnvelope:
        async with self._lock:
            while True:
                if self._closed or sub._closed:
                    raise EventsModuleClosedError("Event subscription is closed")
                conn = self._connections.get(sub.connection_id)
                if conn is None or conn.closed or sub.subscription_id not in conn.subscriptions:
                    raise EventsModuleClosedError("Subscriber connection is closed")

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

    def _enqueue_to_connection_unlocked(
        self,
        conn: _ConnectionQueueState,
        envelope: EventEnvelope,
    ) -> str:
        event_size = envelope.serialized_bytes

        if (
            event_size > self._config.max_queued_bytes_per_connection
            or event_size > self._config.global_queued_bytes_budget
        ):
            self._close_connection_unlocked(conn.connection_id)
            return "disconnected"

        if self._config.slow_consumer_policy == SlowConsumerPolicy.DROP_OLDEST:
            while conn.queue and (
                len(conn.queue) >= self._config.max_queued_events_per_connection
                or conn.queued_bytes + event_size > self._config.max_queued_bytes_per_connection
                or self._global_queued_bytes + event_size > self._config.global_queued_bytes_budget
            ):
                dropped = conn.queue.popleft()
                conn.queued_bytes = max(0, conn.queued_bytes - dropped.serialized_bytes)
                self._global_queued_events = max(0, self._global_queued_events - 1)
                self._global_queued_bytes = max(
                    0, self._global_queued_bytes - dropped.serialized_bytes
                )
            if self._global_queued_bytes + event_size > self._config.global_queued_bytes_budget:
                return "dropped"
            conn.queue.append(envelope)
            conn.queued_bytes += event_size
            self._global_queued_events += 1
            self._global_queued_bytes += event_size
            conn.cond.notify(1)
            return "delivered"

        # Default: DISCONNECT_SLOW_CONSUMER
        if (
            len(conn.queue) >= self._config.max_queued_events_per_connection
            or conn.queued_bytes + event_size > self._config.max_queued_bytes_per_connection
            or self._global_queued_bytes + event_size > self._config.global_queued_bytes_budget
        ):
            self._close_connection_unlocked(conn.connection_id)
            return "disconnected"

        conn.queue.append(envelope)
        conn.queued_bytes += event_size
        self._global_queued_events += 1
        self._global_queued_bytes += event_size
        conn.cond.notify(1)
        return "delivered"

    async def publish(self, envelope: EventEnvelope) -> PublishResult:
        """Publish a validated EventEnvelope to matching tenant-scoped connections."""
        if not isinstance(envelope, EventEnvelope):
            raise TypeError(f"envelope must be an EventEnvelope, got {type(envelope).__name__}")

        if self._publish_rate_limiter is not None:
            decision = await self._publish_rate_limiter.check(
                f"pub:{envelope.tenant_id}:{envelope.principal_id}"
            )
            if not decision.allowed:
                self._telemetry.record_rate_limit_rejection()
                self._telemetry.record_error(EventsErrorCategory.RATE_LIMIT)
                raise EventRateLimitError(
                    f"Publish rate limit exceeded; retry after {decision.retry_after_seconds:.3f}s"
                )

        async with self._lock:
            if self._closed:
                self._telemetry.record_error(EventsErrorCategory.CLOSED)
                raise EventsModuleClosedError("Event broker is closed")

            tenant_conn_ids = list(self._tenant_connections.get(envelope.tenant_id, ()))
            matched_connections: list[_ConnectionQueueState] = []

            for conn_id in tenant_conn_ids:
                conn = self._connections.get(conn_id)
                if conn is None or conn.closed:
                    continue
                if any(
                    topic_matches_pattern(envelope.topic, sub.topic_pattern)
                    for sub in conn.subscriptions.values()
                ):
                    matched_connections.append(conn)

            delivered = 0
            dropped = 0
            disconnected = 0

            for conn in matched_connections:
                outcome = self._enqueue_to_connection_unlocked(conn, envelope)
                if outcome == "delivered":
                    delivered += 1
                elif outcome == "dropped":
                    dropped += 1
                else:
                    disconnected += 1

            self._sync_gauges_unlocked()
            self._telemetry.record_publish(
                delivered=delivered,
                dropped_slow_consumer=dropped,
                disconnected_slow_consumer=disconnected,
            )
            return PublishResult(
                event_id=envelope.event_id,
                topic=envelope.topic,
                matched_subscribers=len(matched_connections),
                delivered_subscribers=delivered,
                dropped_subscribers=dropped,
                disconnected_subscribers=disconnected,
                durable_persisted=False,
            )

    async def publish_batch(
        self,
        envelopes: list[EventEnvelope] | tuple[EventEnvelope, ...],
    ) -> list[PublishResult]:
        """Publish a sequence of validated envelopes in order."""
        results: list[PublishResult] = []
        for envelope in envelopes:
            results.append(await self.publish(envelope))
        return results

    async def active_connections_for_tenant(self, tenant_id: str) -> int:
        """Return the number of active subscriber connections for a tenant."""
        canonical = validate_tenant_id(tenant_id)
        async with self._lock:
            return len(self._tenant_connections.get(canonical, ()))

    async def close(self) -> None:
        """Idempotently close all subscriber connections and release all memory budgets."""
        async with self._lock:
            if self._closed:
                return
            self._closed = True
            for conn_id in list(self._connections):
                self._close_connection_unlocked(conn_id)
            self._global_queued_bytes = 0
            self._global_queued_events = 0
            self._sync_gauges_unlocked()


__all__ = [
    "EventBrokerConfig",
    "EventBrokerStats",
    "InMemoryEventBroker",
    "InMemoryEventSubscription",
]
