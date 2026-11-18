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


__all__ = [
    "EventsErrorCategory",
    "EventsMetricsSnapshot",
    "EventsTelemetrySink",
    "InMemoryEventsTelemetrySink",
]
