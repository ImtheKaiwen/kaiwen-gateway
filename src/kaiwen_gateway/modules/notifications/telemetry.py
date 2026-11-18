"""Bounded-cardinality, privacy-safe telemetry contracts for Notifications (N1)."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from enum import StrEnum
from types import MappingProxyType
from typing import Protocol, runtime_checkable


class NotificationsErrorCategory(StrEnum):
    """Low-cardinality error categories for Notifications telemetry."""

    VALIDATION = "validation"
    AUTHORIZATION = "authorization"
    EXPIRATION = "expiration"
    PREFERENCE = "preference"
    BACKEND_UNAVAILABLE = "backend_unavailable"
    CLOSED = "closed"
    INTERNAL = "internal"


@dataclass(frozen=True, slots=True)
class NotificationsMetricsSnapshot:
    """Immutable bounded-cardinality telemetry snapshot for Notifications."""

    notifications_published: int
    notifications_dispatched: int
    notifications_expired: int
    notifications_filtered_by_preference: int
    notifications_channel_failed: int
    errors_by_category: Mapping[NotificationsErrorCategory, int]


@runtime_checkable
class NotificationsTelemetrySink(Protocol):
    """Telemetry sink protocol for Notifications counters.

    Implementations must never record notification IDs, recipient IDs, unbounded
    tenant labels, raw payloads, access tokens, or authentication secrets.
    """

    def record_published(self) -> None: ...

    def record_dispatched(self, *, matched_connections: int = 1) -> None:
        """Record live broker dispatch acceptance (never implies client receipt)."""
        ...

    def record_expired(self) -> None: ...

    def record_filtered_by_preference(self) -> None: ...

    def record_channel_failure(self) -> None: ...

    def record_error(self, category: NotificationsErrorCategory) -> None: ...

    def snapshot(self) -> NotificationsMetricsSnapshot: ...


class InMemoryNotificationsTelemetrySink:
    """In-memory bounded-cardinality Notifications telemetry sink."""

    __slots__ = (
        "_notifications_published",
        "_notifications_dispatched",
        "_notifications_expired",
        "_notifications_filtered_by_preference",
        "_notifications_channel_failed",
        "_errors_by_category",
    )

    def __init__(self) -> None:
        self._notifications_published: int = 0
        self._notifications_dispatched: int = 0
        self._notifications_expired: int = 0
        self._notifications_filtered_by_preference: int = 0
        self._notifications_channel_failed: int = 0
        self._errors_by_category: dict[NotificationsErrorCategory, int] = {
            category: 0 for category in NotificationsErrorCategory
        }

    def record_published(self) -> None:
        self._notifications_published += 1

    def record_dispatched(self, *, matched_connections: int = 1) -> None:
        self._notifications_dispatched += max(0, matched_connections)

    def record_expired(self) -> None:
        self._notifications_expired += 1

    def record_filtered_by_preference(self) -> None:
        self._notifications_filtered_by_preference += 1

    def record_channel_failure(self) -> None:
        self._notifications_channel_failed += 1

    def record_error(self, category: NotificationsErrorCategory) -> None:
        self._errors_by_category[category] = self._errors_by_category.get(category, 0) + 1

    def snapshot(self) -> NotificationsMetricsSnapshot:
        return NotificationsMetricsSnapshot(
            notifications_published=self._notifications_published,
            notifications_dispatched=self._notifications_dispatched,
            notifications_expired=self._notifications_expired,
            notifications_filtered_by_preference=self._notifications_filtered_by_preference,
            notifications_channel_failed=self._notifications_channel_failed,
            errors_by_category=MappingProxyType(dict(self._errors_by_category)),
        )


__all__ = [
    "InMemoryNotificationsTelemetrySink",
    "NotificationsErrorCategory",
    "NotificationsMetricsSnapshot",
    "NotificationsTelemetrySink",
]
