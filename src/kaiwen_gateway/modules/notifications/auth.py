"""Fail-closed recipient subscription authorization for Notifications (N2)."""

from __future__ import annotations

from typing import Final

from kaiwen_gateway.core.identity import VerifiedPrincipalContext
from kaiwen_gateway.modules.events.auth import EventTopicAuthorizerPort
from kaiwen_gateway.modules.events.domain import (
    EventAuthorizationError,
    topic_matches_pattern,
    validate_topic_pattern,
)
from kaiwen_gateway.modules.notifications.domain import (
    RECIPIENT_TOPIC_HASH_HEX_LENGTH,
    RECIPIENT_TOPIC_PREFIX,
    derive_recipient_topic,
)

ADMIN_NOTIFICATION_SUBSCRIPTION_SCOPES: Final[frozenset[str]] = frozenset(
    {"events:admin", "notifications:subscribe:tenant"}
)
_PROBE_RECIPIENT_TOPIC: Final[str] = (
    f"{RECIPIENT_TOPIC_PREFIX}{'0' * RECIPIENT_TOPIC_HASH_HEX_LENGTH}"
)


def is_protected_notification_pattern(topic_pattern: str) -> bool:
    """Return True if `topic_pattern` targets or can match `notifications.r.*`."""
    if not isinstance(topic_pattern, str) or not topic_pattern:
        return True
    if topic_pattern == "*":
        return True
    if topic_pattern.startswith(RECIPIENT_TOPIC_PREFIX):
        return True
    if topic_pattern.endswith(".*"):
        return topic_matches_pattern(_PROBE_RECIPIENT_TOPIC, topic_pattern)
    return "*" in topic_pattern


class NotificationsTopicAuthorizer:
    """Mandatory N2 fail-closed `EventTopicAuthorizerPort` protecting `notifications.r.*`.

    Rules enforced:
    - General `events:subscribe` and `events:subscribe:wildcard` never grant access to
      another recipient's `notifications.r.<token>` topic or any wildcard capable of
      matching `notifications.r.*` (including `*`, `notifications.*`, `notifications.r.*`).
    - Exact `notifications.r.<token>` subscriptions are permitted only when `<token>`
      matches the canonical hash of `(context.tenant_id, context.principal_id)` and the
      principal holds an explicit subscription scope (`events:subscribe` or
      `events:subscribe:<self_topic>`; `notifications:subscribe:self` /
      `notifications:subscribe` must be paired with `events:subscribe` or
      `events:subscribe:<self_topic>` at the Events broker layer), OR when the principal
      holds an explicit tenant-scoped administrative scope
      (`notifications:subscribe:tenant` or `events:admin`).
    - `notifications:subscribe:tenant` is supported directly by Events brokers for
      `notifications.r.*` and `notifications.r.<token>` within `context.tenant_id`.
    - Tenant isolation is always enforced via immutable `VerifiedPrincipalContext.tenant_id`.
    """

    async def authorize_subscribe(
        self,
        *,
        context: VerifiedPrincipalContext,
        topic_pattern: str,
    ) -> None:
        """Raise `EventAuthorizationError` if subscription to `topic_pattern` is denied."""
        if not isinstance(context, VerifiedPrincipalContext):
            raise EventAuthorizationError("VerifiedPrincipalContext is required")

        if not isinstance(topic_pattern, str) or not topic_pattern:
            raise EventAuthorizationError("topic_pattern must be a non-empty string")

        if topic_pattern == "*":
            raise EventAuthorizationError(
                "Broad wildcard '*' subscription to protected notification namespace is denied"
            )

        canonical_pattern = validate_topic_pattern(topic_pattern)

        if not is_protected_notification_pattern(canonical_pattern):
            return

        if context.allowed_scopes & ADMIN_NOTIFICATION_SUBSCRIPTION_SCOPES:
            return

        if canonical_pattern.endswith(".*"):
            raise EventAuthorizationError(
                "Wildcard subscription to protected notification namespace is denied"
            )

        expected_self_topic = derive_recipient_topic(context.tenant_id, context.principal_id)
        if canonical_pattern != expected_self_topic:
            raise EventAuthorizationError("Cross-recipient notification subscription is denied")

        allowed_self_scopes = frozenset(
            {
                "events:subscribe",
                f"events:subscribe:{expected_self_topic}",
                "notifications:subscribe",
                "notifications:subscribe:self",
            }
        )
        if not (context.allowed_scopes & allowed_self_scopes):
            raise EventAuthorizationError(
                "Principal lacks required scope for self notification subscription"
            )


class ChainedNotificationsTopicAuthorizer:
    """Enforces mandatory `NotificationsTopicAuthorizer` plus an additional host policy.

    Additional host-defined `EventTopicAuthorizerPort` hooks can only restrict
    subscriptions further; they can never bypass mandatory recipient isolation.
    """

    __slots__ = ("_mandatory", "_additional")

    def __init__(
        self,
        *,
        additional: EventTopicAuthorizerPort | None = None,
    ) -> None:
        if additional is not None and not isinstance(additional, EventTopicAuthorizerPort):
            raise TypeError(
                "additional must implement EventTopicAuthorizerPort, "
                f"got {type(additional).__name__}"
            )
        self._mandatory: NotificationsTopicAuthorizer = NotificationsTopicAuthorizer()
        self._additional: EventTopicAuthorizerPort | None = additional

    @property
    def additional(self) -> EventTopicAuthorizerPort | None:
        """Return optional additional host-defined topic authorizer."""
        return self._additional

    async def authorize_subscribe(
        self,
        *,
        context: VerifiedPrincipalContext,
        topic_pattern: str,
    ) -> None:
        """Enforce mandatory recipient isolation first, then optional host policy."""
        await self._mandatory.authorize_subscribe(
            context=context,
            topic_pattern=topic_pattern,
        )
        if self._additional is not None:
            await self._additional.authorize_subscribe(
                context=context,
                topic_pattern=topic_pattern,
            )


def enforces_mandatory_recipient_isolation(authorizer: object) -> bool:
    """Return True only for exact non-overridden mandatory recipient authorizer types."""
    return type(authorizer) in (
        NotificationsTopicAuthorizer,
        ChainedNotificationsTopicAuthorizer,
    )


__all__ = [
    "ADMIN_NOTIFICATION_SUBSCRIPTION_SCOPES",
    "ChainedNotificationsTopicAuthorizer",
    "NotificationsTopicAuthorizer",
    "enforces_mandatory_recipient_isolation",
    "is_protected_notification_pattern",
]
