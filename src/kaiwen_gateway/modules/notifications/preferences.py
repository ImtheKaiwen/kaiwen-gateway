"""Lightweight notification preference contracts and in-memory test store (N1)."""

from __future__ import annotations

from collections.abc import Collection
from dataclasses import dataclass, field
from typing import Any, Protocol, runtime_checkable

from kaiwen_gateway.core.identity import validate_principal_id, validate_tenant_id
from kaiwen_gateway.modules.notifications.domain import (
    InvalidNotificationError,
    NotificationEnvelope,
    NotificationPriority,
    NotificationsPreferenceError,
    priority_rank,
    validate_category,
)


@dataclass(frozen=True, slots=True)
class NotificationPreference:
    """Per-recipient notification preference record."""

    tenant_id: str
    recipient_id: str
    muted_categories: frozenset[str] = field(default_factory=frozenset)
    min_priority: NotificationPriority = NotificationPriority.LOW
    disabled_channels: frozenset[str] = field(default_factory=frozenset)
    allow_promotional: bool = False

    def __post_init__(self) -> None:
        try:
            object.__setattr__(self, "tenant_id", validate_tenant_id(self.tenant_id))
            object.__setattr__(self, "recipient_id", validate_principal_id(self.recipient_id))
        except ValueError as exc:
            raise InvalidNotificationError(str(exc)) from exc

        if not isinstance(self.allow_promotional, bool):
            raise InvalidNotificationError("allow_promotional must be a bool")
        raw_min_priority: Any = self.min_priority
        if not isinstance(raw_min_priority, NotificationPriority):
            try:
                coerced = NotificationPriority(str(raw_min_priority))
            except ValueError as exc:
                raise InvalidNotificationError(
                    f"Unsupported min_priority {raw_min_priority!r}"
                ) from exc
            object.__setattr__(self, "min_priority", coerced)

        validated_cats = frozenset(validate_category(c) for c in self.muted_categories)
        object.__setattr__(self, "muted_categories", validated_cats)
        cleaned_channels: list[str] = []
        for ch in self.disabled_channels:
            if not isinstance(ch, str) or not ch.strip():
                raise InvalidNotificationError("disabled_channels must contain non-empty strings")
            cleaned_channels.append(ch.strip())
        object.__setattr__(self, "disabled_channels", frozenset(cleaned_channels))


@dataclass(frozen=True, slots=True)
class PreferenceEvaluationResult:
    """Outcome of evaluating a notification against recipient preferences."""

    allowed: bool
    reason: str


@runtime_checkable
class NotificationPreferencePort(Protocol):
    """Port for looking up recipient notification preferences.

    Returning `None` indicates no preference record exists for the recipient.
    Backend lookup failures must raise `NotificationsPreferenceError` (never return `None`).
    """

    async def get_preferences(
        self,
        *,
        tenant_id: str,
        recipient_id: str,
    ) -> NotificationPreference | None:
        """Return stored preferences for `(tenant_id, recipient_id)`, or `None` if unset."""
        ...


def evaluate_notification_preference(
    notification: NotificationEnvelope,
    preference: NotificationPreference | None,
    *,
    authoritative_promotional_categories: Collection[str],
    channel_id: str = "in_app",
) -> PreferenceEvaluationResult:
    """Evaluate delivery eligibility preserving ADR-0007 preference policy.

    - `authoritative_promotional_categories` is a required host-configured collection
      so N2 callers cannot silently omit authoritative promotional classification
      and trust an unverified producer's `is_promotional=False` flag.
    - Missing preferences (`None`) allow only non-promotional `in_app` notifications.
    """
    canonical_promo_cats = frozenset(
        validate_category(cat) for cat in authoritative_promotional_categories
    )
    effective_promotional = notification.is_promotional or (
        notification.category in canonical_promo_cats
    )

    if preference is None:
        if effective_promotional:
            return PreferenceEvaluationResult(allowed=False, reason="promotional_opt_in_required")
        if channel_id != "in_app":
            return PreferenceEvaluationResult(
                allowed=False, reason="non_in_app_requires_explicit_preference"
            )
        return PreferenceEvaluationResult(allowed=True, reason="default_non_promotional_in_app")

    if (
        preference.tenant_id != notification.tenant_id
        or preference.recipient_id != notification.recipient_id
    ):
        raise NotificationsPreferenceError("Preference record tenant_id/recipient_id mismatch")

    if channel_id in preference.disabled_channels:
        return PreferenceEvaluationResult(allowed=False, reason="channel_disabled")
    if effective_promotional and not preference.allow_promotional:
        return PreferenceEvaluationResult(allowed=False, reason="promotional_opt_in_required")
    if notification.category in preference.muted_categories:
        return PreferenceEvaluationResult(allowed=False, reason="category_muted")
    if priority_rank(notification.priority) < priority_rank(preference.min_priority):
        return PreferenceEvaluationResult(allowed=False, reason="below_min_priority")
    return PreferenceEvaluationResult(allowed=True, reason="allowed_by_preference")


class InMemoryNotificationPreferenceStore:
    """In-memory `NotificationPreferencePort` implementation for unit tests."""

    __slots__ = ("_records", "_fail_lookups")

    def __init__(self, *, fail_lookups: bool = False) -> None:
        self._records: dict[tuple[str, str], NotificationPreference] = {}
        self._fail_lookups: bool = fail_lookups

    def set_preference(self, preference: NotificationPreference) -> None:
        self._records[(preference.tenant_id, preference.recipient_id)] = preference

    def set_fail_lookups(self, fail: bool) -> None:
        self._fail_lookups = fail

    async def get_preferences(
        self,
        *,
        tenant_id: str,
        recipient_id: str,
    ) -> NotificationPreference | None:
        if self._fail_lookups:
            raise NotificationsPreferenceError("Notification preference backend unavailable")
        return self._records.get(
            (validate_tenant_id(tenant_id), validate_principal_id(recipient_id))
        )


__all__ = [
    "InMemoryNotificationPreferenceStore",
    "NotificationPreference",
    "NotificationPreferencePort",
    "PreferenceEvaluationResult",
    "evaluate_notification_preference",
]
