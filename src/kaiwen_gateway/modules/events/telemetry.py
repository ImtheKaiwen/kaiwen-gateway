"""Privacy-safe telemetry contracts and in-memory sink for the Events module."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from enum import StrEnum
from types import MappingProxyType
from typing import Protocol, runtime_checkable


class EventsErrorCategory(StrEnum):
    """Low-cardinality error categories for Events module telemetry."""

    VALIDATION = "validation"
    AUTHORIZATION = "authorization"
    RATE_LIMIT = "rate_limit"
    BACKPRESSURE = "backpressure"
    BACKEND_UNAVAILABLE = "backend_unavailable"
    CLOSED = "closed"
    INTERNAL = "internal"


@dataclass(frozen=True, slots=True)
class EventsMetricsSnapshot:
    """Immutable snapshot of bounded Events module operational metrics."""

    events_published: int
    events_delivered: int
    events_dropped_slow_consumer: int
    slow_consumers_disconnected: int
    rate_limit_rejections: int
    active_connections: int
    active_subscriptions: int
    queued_events: int
    queued_bytes: int
    errors_by_category: Mapping[EventsErrorCategory, int]


@runtime_checkable
class EventsTelemetrySink(Protocol):
    """Telemetry sink protocol for Events module counters and bounded gauges.

    Implementations must never record raw payloads, topics, event IDs, or credentials.
    """

    def record_publish(
        self,
        *,
        delivered: int,
        dropped_slow_consumer: int,
        disconnected_slow_consumer: int,
    ) -> None:
        """Record the outcome counts of a single publish operation."""
        ...

    def record_rate_limit_rejection(self) -> None:
        """Record a token-bucket rate limit rejection."""
        ...

    def record_error(self, category: EventsErrorCategory) -> None:
        """Record a categorized Events error."""
        ...

    def update_gauges(
        self,
        *,
        active_connections: int,
        active_subscriptions: int,
        queued_events: int,
        queued_bytes: int,
    ) -> None:
        """Update current bounded connection, subscription, and queue byte gauges."""
        ...

    def snapshot(self) -> EventsMetricsSnapshot:
        """Return an immutable snapshot of current metrics."""
        ...


class InMemoryEventsTelemetrySink:
    """Thread-safe/single-loop-safe in-memory Events telemetry sink for tests and diagnostics."""

    __slots__ = (
        "_events_published",
        "_events_delivered",
        "_events_dropped_slow_consumer",
        "_slow_consumers_disconnected",
        "_rate_limit_rejections",
        "_active_connections",
        "_active_subscriptions",
        "_queued_events",
        "_queued_bytes",
        "_errors_by_category",
    )

    def __init__(self) -> None:
        self._events_published: int = 0
        self._events_delivered: int = 0
        self._events_dropped_slow_consumer: int = 0
        self._slow_consumers_disconnected: int = 0
        self._rate_limit_rejections: int = 0
        self._active_connections: int = 0
        self._active_subscriptions: int = 0
        self._queued_events: int = 0
        self._queued_bytes: int = 0
        self._errors_by_category: dict[EventsErrorCategory, int] = {
            category: 0 for category in EventsErrorCategory
        }

    def record_publish(
        self,
        *,
        delivered: int,
        dropped_slow_consumer: int,
        disconnected_slow_consumer: int,
    ) -> None:
        self._events_published += 1
        self._events_delivered += max(0, delivered)
        self._events_dropped_slow_consumer += max(0, dropped_slow_consumer)
        self._slow_consumers_disconnected += max(0, disconnected_slow_consumer)

    def record_rate_limit_rejection(self) -> None:
        self._rate_limit_rejections += 1

    def record_error(self, category: EventsErrorCategory) -> None:
        self._errors_by_category[category] = self._errors_by_category.get(category, 0) + 1

    def update_gauges(
        self,
        *,
        active_connections: int,
        active_subscriptions: int,
        queued_events: int,
        queued_bytes: int,
    ) -> None:
        self._active_connections = max(0, active_connections)
        self._active_subscriptions = max(0, active_subscriptions)
        self._queued_events = max(0, queued_events)
        self._queued_bytes = max(0, queued_bytes)

    def snapshot(self) -> EventsMetricsSnapshot:
        return EventsMetricsSnapshot(
            events_published=self._events_published,
            events_delivered=self._events_delivered,
            events_dropped_slow_consumer=self._events_dropped_slow_consumer,
            slow_consumers_disconnected=self._slow_consumers_disconnected,
            rate_limit_rejections=self._rate_limit_rejections,
            active_connections=self._active_connections,
            active_subscriptions=self._active_subscriptions,
            queued_events=self._queued_events,
            queued_bytes=self._queued_bytes,
            errors_by_category=MappingProxyType(dict(self._errors_by_category)),
        )


@dataclass(frozen=True, slots=True)
class DurableStreamMetricsSnapshot:
    """Immutable bounded-cardinality telemetry snapshot for E4 Redis Streams durability."""

    append_succeeded: int
    append_failed: int
    append_ambiguous: int
    read_succeeded: int
    read_failed: int
    records_read: int
    ack_succeeded: int
    ack_failed: int
    records_acked: int
    recovery_attempts: int
    records_reclaimed: int
    recovery_failed: int
    retry_exhausted: int
    dead_lettered: int
    dead_letter_failed: int
    retention_trimmed_losses: int
    malformed_records: int
    redis_errors: int
    consumer_lag: int
    delivery_latency_ms_sum: float
    delivery_latency_count: int
    delivery_latency_max_ms: float


@runtime_checkable
class DurableStreamTelemetrySink(Protocol):
    """Telemetry sink protocol for E4 Redis Streams operations.

    Implementations must never record unbounded event IDs, raw payloads,
    authentication tokens, WebSocket tickets, or Redis credentials.
    """

    def record_append(self, *, succeeded: bool, ambiguous: bool = False) -> None: ...

    def record_read(self, *, succeeded: bool, record_count: int = 0) -> None: ...

    def record_ack(self, *, succeeded: bool, acked_count: int = 0) -> None: ...

    def record_recovery(self, *, succeeded: bool, reclaimed_count: int = 0) -> None: ...

    def record_retry_exhausted(self) -> None: ...

    def record_dead_letter(self, *, succeeded: bool) -> None: ...

    def record_retention_loss(self, *, count: int = 1) -> None: ...

    def record_malformed_record(self) -> None: ...

    def record_redis_error(self) -> None: ...

    def update_consumer_lag(self, lag: int) -> None: ...

    def record_delivery_latency_ms(self, latency_ms: float) -> None: ...

    def snapshot(self) -> DurableStreamMetricsSnapshot: ...


class InMemoryDurableStreamTelemetrySink:
    """In-memory bounded-cardinality telemetry sink for E4 Redis Streams."""

    __slots__ = (
        "_append_succeeded",
        "_append_failed",
        "_append_ambiguous",
        "_read_succeeded",
        "_read_failed",
        "_records_read",
        "_ack_succeeded",
        "_ack_failed",
        "_records_acked",
        "_recovery_attempts",
        "_records_reclaimed",
        "_recovery_failed",
        "_retry_exhausted",
        "_dead_lettered",
        "_dead_letter_failed",
        "_retention_trimmed_losses",
        "_malformed_records",
        "_redis_errors",
        "_consumer_lag",
        "_delivery_latency_ms_sum",
        "_delivery_latency_count",
        "_delivery_latency_max_ms",
    )

    def __init__(self) -> None:
        self._append_succeeded: int = 0
        self._append_failed: int = 0
        self._append_ambiguous: int = 0
        self._read_succeeded: int = 0
        self._read_failed: int = 0
        self._records_read: int = 0
        self._ack_succeeded: int = 0
        self._ack_failed: int = 0
        self._records_acked: int = 0
        self._recovery_attempts: int = 0
        self._records_reclaimed: int = 0
        self._recovery_failed: int = 0
        self._retry_exhausted: int = 0
        self._dead_lettered: int = 0
        self._dead_letter_failed: int = 0
        self._retention_trimmed_losses: int = 0
        self._malformed_records: int = 0
        self._redis_errors: int = 0
        self._consumer_lag: int = 0
        self._delivery_latency_ms_sum: float = 0.0
        self._delivery_latency_count: int = 0
        self._delivery_latency_max_ms: float = 0.0

    def record_append(self, *, succeeded: bool, ambiguous: bool = False) -> None:
        if succeeded:
            self._append_succeeded += 1
        elif ambiguous:
            self._append_ambiguous += 1
        else:
            self._append_failed += 1

    def record_read(self, *, succeeded: bool, record_count: int = 0) -> None:
        if succeeded:
            self._read_succeeded += 1
            self._records_read += max(0, record_count)
        else:
            self._read_failed += 1

    def record_ack(self, *, succeeded: bool, acked_count: int = 0) -> None:
        if succeeded:
            self._ack_succeeded += 1
            self._records_acked += max(0, acked_count)
        else:
            self._ack_failed += 1

    def record_recovery(self, *, succeeded: bool, reclaimed_count: int = 0) -> None:
        self._recovery_attempts += 1
        if succeeded:
            self._records_reclaimed += max(0, reclaimed_count)
        else:
            self._recovery_failed += 1

    def record_retry_exhausted(self) -> None:
        self._retry_exhausted += 1

    def record_dead_letter(self, *, succeeded: bool) -> None:
        if succeeded:
            self._dead_lettered += 1
        else:
            self._dead_letter_failed += 1

    def record_retention_loss(self, *, count: int = 1) -> None:
        self._retention_trimmed_losses += max(0, count)

    def record_malformed_record(self) -> None:
        self._malformed_records += 1

    def record_redis_error(self) -> None:
        self._redis_errors += 1

    def update_consumer_lag(self, lag: int) -> None:
        self._consumer_lag = max(0, lag)

    def record_delivery_latency_ms(self, latency_ms: float) -> None:
        clamped = max(0.0, float(latency_ms))
        self._delivery_latency_ms_sum += clamped
        self._delivery_latency_count += 1
        if clamped > self._delivery_latency_max_ms:
            self._delivery_latency_max_ms = clamped

    def snapshot(self) -> DurableStreamMetricsSnapshot:
        return DurableStreamMetricsSnapshot(
            append_succeeded=self._append_succeeded,
            append_failed=self._append_failed,
            append_ambiguous=self._append_ambiguous,
            read_succeeded=self._read_succeeded,
            read_failed=self._read_failed,
            records_read=self._records_read,
            ack_succeeded=self._ack_succeeded,
            ack_failed=self._ack_failed,
            records_acked=self._records_acked,
            recovery_attempts=self._recovery_attempts,
            records_reclaimed=self._records_reclaimed,
            recovery_failed=self._recovery_failed,
            retry_exhausted=self._retry_exhausted,
            dead_lettered=self._dead_lettered,
            dead_letter_failed=self._dead_letter_failed,
            retention_trimmed_losses=self._retention_trimmed_losses,
            malformed_records=self._malformed_records,
            redis_errors=self._redis_errors,
            consumer_lag=self._consumer_lag,
            delivery_latency_ms_sum=self._delivery_latency_ms_sum,
            delivery_latency_count=self._delivery_latency_count,
            delivery_latency_max_ms=self._delivery_latency_max_ms,
        )


__all__ = [
    "DurableStreamMetricsSnapshot",
    "DurableStreamTelemetrySink",
    "EventsErrorCategory",
    "EventsMetricsSnapshot",
    "EventsTelemetrySink",
    "InMemoryDurableStreamTelemetrySink",
    "InMemoryEventsTelemetrySink",
]
