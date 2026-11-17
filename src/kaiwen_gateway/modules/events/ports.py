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
    """Registration and lifecycle handle for a topic subscription on a connection.

    Subscriptions do not expose per-subscription `receive()` methods because all
    subscriptions on a connection share a single multiplexed bounded outbound queue.
    """

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

    async def close(self) -> None:
        """Unsubscribe this handle cleanly and idempotently."""
        ...


@runtime_checkable
class EventConnectionConsumer(Protocol):
    """Single connection-level consumer draining the multiplexed outbound queue."""

    @property
    def connection_id(self) -> str:
        """Return canonical connection identifier bound to this consumer."""
        ...

    @property
    def is_active(self) -> bool:
        """Return True while the underlying subscriber connection remains open."""
        ...

    async def receive(self) -> EventEnvelope | None:
        """Await the next multiplexed event for this connection, or None on close."""
        ...

    async def close(self) -> None:
        """Close the subscriber connection and release all queued bytes."""
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
    """Unified live broker port for E1 in-memory and future E3 distributed adapters."""

    async def startup(self) -> None:
        """Initialize broker state for accepting subscriptions and publishes."""
        ...

    async def shutdown(self) -> None:
        """Shut down the broker and release all connections and memory budgets."""
        ...

    def consumer(self, connection_id: str) -> EventConnectionConsumer:
        """Return the connection-level consumer for `connection_id`."""
        ...

    async def close_connection(self, connection_id: str) -> int:
        """Close all subscriptions and release queued bytes for a subscriber connection."""
        ...


@runtime_checkable
class DurableEventStorePort(Protocol):
    """Interface-only durability port reserved for future E4 Redis Streams adapters."""

    async def append(self, event: EventEnvelope) -> str:
        """Append an event envelope and return its stream entry identifier."""
        ...

    async def read_group(
        self,
        *,
        tenant_id: str,
        topic: str,
        group: str,
        consumer: str,
        count: int,
        block_ms: int,
    ) -> list[DurableEventRecord]:
        """Read unacknowledged/new records for a consumer group."""
        ...

    async def ack(
        self,
        *,
        tenant_id: str,
        topic: str,
        group: str,
        record_ids: Sequence[str],
    ) -> int:
        """Acknowledge processed stream records and return the count acknowledged."""
        ...

    async def replay(
        self,
        *,
        tenant_id: str,
        topic: str,
        from_id: str = "0-0",
        limit: int = 100,
    ) -> list[DurableEventRecord]:
        """Replay retained events from a stream cursor within retention bounds."""
        ...


__all__ = [
    "DurableEventStorePort",
    "EventBrokerPort",
    "EventConnectionConsumer",
    "EventPublisherPort",
    "EventSubscription",
    "EventSubscriptionPort",
]
