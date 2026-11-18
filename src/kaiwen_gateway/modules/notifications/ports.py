"""Framework-independent ports and fail-closed authorization contracts for Notifications (N1)."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol, runtime_checkable

from kaiwen_gateway.core.identity import VerifiedPrincipalContext
from kaiwen_gateway.modules.notifications.domain import (
    NotificationAuthorizationError,
    NotificationEnvelope,
    NotificationStatus,
)


@dataclass(frozen=True, slots=True)
class NotificationPublishResult:
    """Bounded outcome summary for a notification publish operation."""

    notification_id: str
    status: NotificationStatus
    recipient_topic: str
    delivered_count: int = 0
    sequence_id: str | None = None


@dataclass(frozen=True, slots=True)
class ChannelDeliveryResult:
    """Outcome of dispatching a notification to an external channel adapter."""

    channel_id: str
    accepted: bool
    provider_ref: str | None = None


@runtime_checkable
class NotificationProducerAuthorizerPort(Protocol):
    """Host-application policy enforcing whether a producer may target a recipient."""

    async def authorize_publish(
        self,
        *,
        producer: VerifiedPrincipalContext | None,
        notification: NotificationEnvelope,
    ) -> None:
        """Raise `NotificationAuthorizationError` if targeting is unauthorized."""
        ...


@runtime_checkable
class NotificationPublisherPort(Protocol):
    """Port for publishing validated notifications."""

    async def publish(
        self,
        notification: NotificationEnvelope,
        *,
        producer: VerifiedPrincipalContext | None = None,
    ) -> NotificationPublishResult:
        """Validate, authorize, and publish a notification."""
        ...


@runtime_checkable
class NotificationChannelPort(Protocol):
    """Reserved interface for future external notification channels (Email/SMS/Push/Webhook)."""

    @property
    def channel_id(self) -> str:
        """Return canonical channel identifier (e.g. 'email', 'sms', 'push', 'webhook')."""
        ...

    async def send(self, notification: NotificationEnvelope) -> ChannelDeliveryResult:
        """Dispatch a notification through the external channel."""
        ...


class DenyAllProducerAuthorizer:
    """Fail-closed default `NotificationProducerAuthorizerPort` denying all publishes."""

    async def authorize_publish(
        self,
        *,
        producer: VerifiedPrincipalContext | None,
        notification: NotificationEnvelope,
    ) -> None:
        raise NotificationAuthorizationError(
            "No NotificationProducerAuthorizerPort policy configured; publish denied fail-closed"
        )


__all__ = [
    "ChannelDeliveryResult",
    "DenyAllProducerAuthorizer",
    "NotificationChannelPort",
    "NotificationProducerAuthorizerPort",
    "NotificationPublishResult",
    "NotificationPublisherPort",
]
