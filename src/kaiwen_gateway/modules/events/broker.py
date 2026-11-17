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
from kaiwen_gateway.modules.events.ratelimit import RateLimitConfig, TokenBucketRateLimiter
from kaiwen_gateway.modules.events.telemetry import (
    EventsErrorCategory,
    EventsTelemetrySink,
    InMemoryEventsTelemetrySink,
)

DEFAULT_PUBLISH_RATE_PER_SECOND: float = 200.0
DEFAULT_PUBLISH_BURST: float = 400.0
DEFAULT_SUBSCRIBE_RATE_PER_SECOND: float = 10.0
DEFAULT_SUBSCRIBE_BURST: float = 20.0


def _is_subscription_authorized(
    context: VerifiedPrincipalContext,
    canonical_pattern: str,
) -> bool:
    """Enforce fail-closed topic and wildcard authorization on verified principal scopes.

    Canonical scopes (constrained by Core's `^[a-z][a-z0-9_.:-]{0,63}$`):
    - Exact topic `<topic>`: requires `events:subscribe`, `events:admin`, or
      `events:subscribe:<topic>`.
    - Wildcard `<prefix>.*`: denied by default for `events:subscribe`; requires
      `events:subscribe:wildcard`, `events:admin`, or prefix-scoped
      `events:subscribe:<scope_prefix>.wildcard` where `<prefix>` equals or is a
      dot-delimited sub-prefix of `<scope_prefix>`.
    """
    scopes = context.allowed_scopes
    if "events:admin" in scopes:
        return True

    if canonical_pattern.endswith(".*"):
        if "events:subscribe:wildcard" in scopes:
            return True
        req_prefix = canonical_pattern[:-2]
        for scope in scopes:
            if scope.startswith("events:subscribe:") and scope.endswith(".wildcard"):
                scope_prefix = scope[len("events:subscribe:") : -len(".wildcard")]
                if scope_prefix and (
                    req_prefix == scope_prefix or req_prefix.startswith(f"{scope_prefix}.")
                ):
                    return True
        return False

    return "events:subscribe" in scopes or f"events:subscribe:{canonical_pattern}" in scopes


@dataclass(frozen=True, slots=True)
class EventBrokerConfig:
    """Validated bounded limits and policies for InMemoryEventBroker.

    Rate limits are process-local (single-instance) unless a distributed E3 adapter is wired.
    """

    max_subscriptions_per_connection: int = DEFAULT_MAX_SUBSCRIPTIONS_PER_CONNECTION
    max_queued_events_per_connection: int = DEFAULT_MAX_QUEUED_EVENTS_PER_CONNECTION
    max_queued_bytes_per_connection: int = DEFAULT_MAX_QUEUED_BYTES_PER_CONNECTION
    global_queued_bytes_budget: int = DEFAULT_GLOBAL_QUEUED_BYTES_BUDGET
    slow_consumer_policy: SlowConsumerPolicy = SlowConsumerPolicy.DISCONNECT_SLOW_CONSUMER
    enable_rate_limiting: bool = True
    publish_rate_limit: RateLimitConfig | None = None
    subscribe_rate_limit: RateLimitConfig | None = None

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
        self.subscriptions: dict[str, InMemoryEventSubscription] = {}
        self.closed: bool = False
        self.cond: asyncio.Condition = asyncio.Condition(lock)
        self.consumer: InMemoryEventConsumer | None = None


class InMemoryEventSubscription:
    """Registration and lifecycle handle for a topic subscription.

    Consumption is owned by `InMemoryEventConsumer` (`broker.consumer(connection_id)`)
    so events from unrelated topics on the same multiplexed queue cannot be read through
    an incompatible subscription-specific handle.
    """

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

    async def close(self) -> None:
        """Unsubscribe this handle cleanly and idempotently."""
        if self._closed:
            return
        self._closed = True
        await self._broker.unsubscribe(self._subscription_id)

    def __repr__(self) -> str:
        return (
            f"InMemoryEventSubscription(subscription_id={self._subscription_id!r}, "
            f"active={self.is_active})"
        )


class InMemoryEventConsumer:
    """Single connection-level consumer draining a connection's multiplexed outbound queue."""

    __slots__ = ("_broker", "_connection_id")

    def __init__(self, *, broker: InMemoryEventBroker, connection_id: str) -> None:
        self._broker: InMemoryEventBroker = broker
        self._connection_id: str = connection_id

    @property
    def connection_id(self) -> str:
        """Return canonical connection identifier bound to this consumer."""
        return self._connection_id

    @property
    def is_active(self) -> bool:
        """Return True while the connection remains open in the broker."""
        return self._broker._is_connection_active(self._connection_id)

    async def receive(self) -> EventEnvelope | None:
        """Await the next multiplexed event for this connection, or None when closed."""
        return await self._broker._receive_for_connection(self._connection_id)

    async def close(self) -> None:
        """Close the subscriber connection and release all subscriptions and queued bytes."""
        await self._broker.close_connection(self._connection_id)

    def __aiter__(self) -> AsyncIterator[EventEnvelope]:
        return self

    async def __anext__(self) -> EventEnvelope:
        envelope = await self.receive()
        if envelope is None:
            raise StopAsyncIteration
        return envelope

    def __repr__(self) -> str:
        return (
            f"InMemoryEventConsumer(connection_id={self._connection_id!r}, active={self.is_active})"
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
        "_started",
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
        if publish_rate_limiter is not None:
            self._publish_rate_limiter: TokenBucketRateLimiter | None = publish_rate_limiter
        elif self._config.enable_rate_limiting:
            pub_cfg = self._config.publish_rate_limit or RateLimitConfig(
                rate_per_second=DEFAULT_PUBLISH_RATE_PER_SECOND,
                burst=DEFAULT_PUBLISH_BURST,
            )
            self._publish_rate_limiter = TokenBucketRateLimiter(pub_cfg)
        else:
            self._publish_rate_limiter = None

        if subscribe_rate_limiter is not None:
            self._subscribe_rate_limiter: TokenBucketRateLimiter | None = subscribe_rate_limiter
        elif self._config.enable_rate_limiting:
            sub_cfg = self._config.subscribe_rate_limit or RateLimitConfig(
                rate_per_second=DEFAULT_SUBSCRIBE_RATE_PER_SECOND,
                burst=DEFAULT_SUBSCRIBE_BURST,
            )
            self._subscribe_rate_limiter = TokenBucketRateLimiter(sub_cfg)
        else:
            self._subscribe_rate_limiter = None

        self._lock: asyncio.Lock = asyncio.Lock()
        self._connections: dict[str, _ConnectionQueueState] = {}
        self._subscriptions: dict[str, InMemoryEventSubscription] = {}
        self._tenant_connections: dict[str, set[str]] = {}
        self._global_queued_bytes: int = 0
        self._global_queued_events: int = 0
        self._started: bool = False
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
    def is_started(self) -> bool:
        """Return True if startup() completed and shutdown() has not run."""
        return self._started and not self._closed

    @property
    def is_closed(self) -> bool:
        """Return True if the broker has been shut down."""
        return self._closed

    async def startup(self) -> None:
        """Initialize broker state; rejects restart after terminal shutdown."""
        async with self._lock:
            if self._closed:
                raise EventsModuleClosedError(
                    "Cannot restart InMemoryEventBroker after terminal shutdown"
                )
            self._started = True

    async def shutdown(self) -> None:
        """Idempotently shut down the broker and release all connections and byte budgets."""
        async with self._lock:
            if self._closed:
                return
            self._closed = True
            self._started = False
            for conn_id in list(self._connections):
                self._close_connection_unlocked(conn_id)
            self._global_queued_bytes = 0
            self._global_queued_events = 0
            self._sync_gauges_unlocked()

    async def close(self) -> None:
        """Alias for shutdown()."""
        await self.shutdown()

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

    def _is_connection_active(self, connection_id: str) -> bool:
        if self._closed:
            return False
        conn = self._connections.get(connection_id)
        return conn is not None and not conn.closed

    def consumer(self, connection_id: str) -> InMemoryEventConsumer:
        """Return the single connection-level consumer for `connection_id`."""
        conn = self._connections.get(connection_id)
        if conn is None or conn.closed:
            raise EventsModuleClosedError(f"No active subscriber connection {connection_id!r}")
        if conn.consumer is None:
            conn.consumer = InMemoryEventConsumer(broker=self, connection_id=connection_id)
        return conn.consumer

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
        """Register a validated, scope-authorized topic subscription for a principal connection."""
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
                    principal_id=context.principal_id,
                    lock=self._lock,
                )
                self._connections[context.connection_id] = conn
                self._tenant_connections.setdefault(context.tenant_id, set()).add(
                    context.connection_id
                )
            elif conn.tenant_id != context.tenant_id or conn.principal_id != context.principal_id:
                self._telemetry.record_error(EventsErrorCategory.AUTHORIZATION)
                raise EventAuthorizationError(
                    "Connection is already bound to a different tenant or principal"
                )

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

    def _enqueue_to_connection_unlocked(
        self,
        conn: _ConnectionQueueState,
        envelope: EventEnvelope,
    ) -> str:
        event_size = envelope.serialized_bytes

        # 1. Per-connection limits (count & bytes) -> slow-consumer policy
        if event_size > self._config.max_queued_bytes_per_connection:
            self._close_connection_unlocked(conn.connection_id)
            return "disconnected"

        per_conn_overflow = (
            len(conn.queue) >= self._config.max_queued_events_per_connection
            or conn.queued_bytes + event_size > self._config.max_queued_bytes_per_connection
        )
        if per_conn_overflow:
            if self._config.slow_consumer_policy == SlowConsumerPolicy.DROP_OLDEST:
                while conn.queue and (
                    len(conn.queue) >= self._config.max_queued_events_per_connection
                    or conn.queued_bytes + event_size > self._config.max_queued_bytes_per_connection
                ):
                    dropped = conn.queue.popleft()
                    conn.queued_bytes = max(0, conn.queued_bytes - dropped.serialized_bytes)
                    self._global_queued_events = max(0, self._global_queued_events - 1)
                    self._global_queued_bytes = max(
                        0, self._global_queued_bytes - dropped.serialized_bytes
                    )
            else:
                self._close_connection_unlocked(conn.connection_id)
                return "disconnected"

        # 2. Global queued-byte budget -> reject/drop enqueue WITHOUT disconnecting healthy conn
        if self._global_queued_bytes + event_size > self._config.global_queued_bytes_budget:
            self._telemetry.record_error(EventsErrorCategory.BACKPRESSURE)
            return "dropped_global"

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
            dropped_global = 0

            for conn in matched_connections:
                outcome = self._enqueue_to_connection_unlocked(conn, envelope)
                if outcome == "delivered":
                    delivered += 1
                elif outcome == "dropped_global":
                    dropped += 1
                    dropped_global += 1
                elif outcome == "dropped":
                    dropped += 1
                else:
                    disconnected += 1

            self._sync_gauges_unlocked()
            self._telemetry.record_publish(
                delivered=delivered,
                dropped_slow_consumer=dropped - dropped_global,
                disconnected_slow_consumer=disconnected,
            )
            return PublishResult(
                event_id=envelope.event_id,
                topic=envelope.topic,
                matched_subscribers=len(matched_connections),
                delivered_subscribers=delivered,
                dropped_subscribers=dropped,
                disconnected_subscribers=disconnected,
                dropped_global_budget=dropped_global,
                accepted=dropped_global == 0,
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


__all__ = [
    "DEFAULT_PUBLISH_BURST",
    "DEFAULT_PUBLISH_RATE_PER_SECOND",
    "DEFAULT_SUBSCRIBE_BURST",
    "DEFAULT_SUBSCRIBE_RATE_PER_SECOND",
    "EventBrokerConfig",
    "EventBrokerStats",
    "InMemoryEventBroker",
    "InMemoryEventConsumer",
    "InMemoryEventSubscription",
]
