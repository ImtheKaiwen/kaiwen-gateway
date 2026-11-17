"""Framework-independent ports and protocols for the Kaiwen Gateway Events module."""

from __future__ import annotations

from collections.abc import AsyncIterator, Sequence
from typing import Protocol, runtime_checkable

from kaiwen_gateway.core.identity import VerifiedPrincipalContext
from kaiwen_gateway.modules.events.domain import (
    DurableEventRecord,
    EventEnvelope,
    PublishResult,
)


@runtime_checkable
class EventSubscription(Protocol):
    """Handle representing an active topic subscription on a subscriber connection."""

    @property
    def subscription_id(self) -> str:
        """Return unique synthetic subscription identifier."""
        ...

    @property
    def connection_id(self) -> str:
        """Return canonical connection identifier owning this subscription."""
        ...

    @property
    def tenant_id(self) -> str:
        """Return canonical tenant identifier owning this subscription."""
        ...

    @property
    def topic_pattern(self) -> str:
        """Return validated topic pattern bound to this subscription."""
        ...

    @property
    def is_active(self) -> bool:
        """Return True while this subscription and its underlying connection remain active."""
        ...

    async def receive(self) -> EventEnvelope:
        """Await the next delivered event from the connection's bounded queue."""
        ...

    async def close(self) -> None:
        """Unsubscribe this handle cleanly and idempotently."""
        ...

    def __aiter__(self) -> AsyncIterator[EventEnvelope]: ...

    async def __anext__(self) -> EventEnvelope: ...


@runtime_checkable
class EventPublisherPort(Protocol):
    """Port for publishing validated events to matching tenant-scoped subscribers."""

    async def publish(self, envelope: EventEnvelope) -> PublishResult:
        """Publish an EventEnvelope and return a bounded delivery summary."""
        ...


@runtime_checkable
class EventSubscriptionPort(Protocol):
    """Port for managing principal-authenticated event subscriptions."""

    async def subscribe(
        self,
        context: VerifiedPrincipalContext,
        topic_pattern: str,
    ) -> EventSubscription:
        """Register a validated topic subscription for a verified principal connection."""
        ...

    async def unsubscribe(self, subscription_id: str) -> bool:
        """Remove a subscription by identifier, returning True if it was active."""
        ...


@runtime_checkable
class EventBrokerPort(EventPublisherPort, EventSubscriptionPort, Protocol):
    """Combined broker port for E1 in-memory and future E3 distributed implementations."""

    async def close_connection(self, connection_id: str) -> int:
        """Close all subscriptions and release queued bytes for a subscriber connection."""
        ...

    async def close(self) -> None:
        """Shut down the broker and release all connections and memory budgets."""
        ...


@runtime_checkable
class DurableEventStorePort(Protocol):
    """Interface-only durability port reserved for future E4 replay/streaming."""

    async def append(self, envelope: EventEnvelope) -> DurableEventRecord:
        """Persist an event envelope and return its durable sequence record."""
        ...

    async def read_since(
        self,
        tenant_id: str,
        topic_pattern: str,
        after_sequence_id: str | None = None,
        *,
        limit: int = 100,
    ) -> Sequence[DurableEventRecord]:
        """Read persisted events for a tenant/pattern after an optional sequence cursor."""
        ...


__all__ = [
    "DurableEventStorePort",
    "EventBrokerPort",
    "EventPublisherPort",
    "EventSubscription",
    "EventSubscriptionPort",
]
