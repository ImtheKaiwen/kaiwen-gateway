"""Deterministic unit tests for Notifications N1 Core."""

from __future__ import annotations

import asyncio
import json
import subprocess
import sys
from datetime import UTC, datetime
from typing import Any

import pytest

from kaiwen_gateway.core import (
    GatewayModule,
    VerifiedPrincipalContext,
    compose_gateway_core,
)
from kaiwen_gateway.modules.events import (
    EVENTS_PROTOCOL_VERSION,
    EventEnvelope,
    validate_topic,
)
from kaiwen_gateway.modules.notifications import (
    DEFAULT_MAX_NOTIFICATION_BYTES,
    NOTIFICATIONS_SCHEMA_VERSION,
    RECIPIENT_TOPIC_HASH_HEX_LENGTH,
    RECIPIENT_TOPIC_PREFIX,
    ChannelDeliveryResult,
    DenyAllProducerAuthorizer,
    InMemoryNotificationPreferenceStore,
    InMemoryNotificationsTelemetrySink,
    InvalidNotificationError,
    NotificationAuthorizationError,
    NotificationChannelPort,
    NotificationEnvelope,
    NotificationPreference,
    NotificationPreferencePort,
    NotificationPriority,
    NotificationProducerAuthorizerPort,
    NotificationsErrorCategory,
    NotificationsModule,
    NotificationsModuleClosedError,
    NotificationsPreferenceError,
    NotificationStatus,
    OversizedNotificationError,
    UnsupportedNotificationVersionError,
    derive_recipient_topic,
    evaluate_notification_preference,
    generate_notification_id,
)
from kaiwen_gateway.transports.events.protocol import EVENTS_MAX_FRAME_BYTES, build_delivery_frame


def _make_notification(**overrides: Any) -> NotificationEnvelope:
    data: dict[str, Any] = {
        "notification_id": "notif_01",
        "schema_version": NOTIFICATIONS_SCHEMA_VERSION,
        "tenant_id": "tenant:acme",
        "recipient_id": "user:alice",
        "category": "system",
        "notification_type": "quota.warning",
        "priority": NotificationPriority.NORMAL,
        "payload": {"usage_pct": 85},
        "created_at": "2026-11-18T12:00:00Z",
        "expires_at": "2026-11-18T13:00:00Z",
        "is_promotional": False,
        "idempotency_key": "idem-1",
        "correlation_id": "corr-1",
    }
    data.update(overrides)
    return NotificationEnvelope(**data)


def test_valid_notification_construction_and_roundtrip() -> None:
    nid = generate_notification_id()
    assert nid.startswith("notif_")
    env = _make_notification(notification_id=nid)
    assert env.schema_version == NOTIFICATIONS_SCHEMA_VERSION
    assert env.serialized_bytes <= DEFAULT_MAX_NOTIFICATION_BYTES
    as_dict = env.to_dict()
    rebuilt = NotificationEnvelope.from_dict(as_dict)
    assert rebuilt == env
    assert json.loads(env.to_json()) == as_dict
    assert env.to_json_bytes() == env.to_json().encode("utf-8")


def test_invalid_schema_version_and_identities() -> None:
    with pytest.raises(UnsupportedNotificationVersionError):
        _make_notification(schema_version="notifications.v2")

    for bad_tenant in ("", "tenant bad", "tenant\n1"):
        with pytest.raises(InvalidNotificationError):
            _make_notification(tenant_id=bad_tenant)

    for bad_recipient in ("", "user bad", "user\t1"):
        with pytest.raises(InvalidNotificationError):
            _make_notification(recipient_id=bad_recipient)

    with pytest.raises(InvalidNotificationError):
        _make_notification(priority="critical")

    with pytest.raises(InvalidNotificationError):
        NotificationEnvelope.from_dict({"notification_id": "notif_1"})


def test_malformed_non_json_non_finite_deep_and_cyclic_payloads() -> None:
    with pytest.raises(InvalidNotificationError):
        _make_notification(payload=["not", "a", "dict"])

    with pytest.raises(InvalidNotificationError):
        _make_notification(payload={1: "non-str-key"})

    for bad_val in (b"bytes", {1, 2}, object()):
        with pytest.raises(InvalidNotificationError):
            _make_notification(payload={"bad": bad_val})

    for bad_float in (float("nan"), float("inf"), float("-inf")):
        with pytest.raises(InvalidNotificationError):
            _make_notification(payload={"score": bad_float})

    # Deeply nested (> 15) rejected; depth 15 wraps cleanly in EventEnvelope (depth <= 16)
    max_depth_payload: dict[str, Any] = {"leaf": 1}
    for _ in range(13):
        max_depth_payload = {"child": max_depth_payload}
    max_depth_env = _make_notification(payload=max_depth_payload)
    wrapped = EventEnvelope(
        protocol_version=EVENTS_PROTOCOL_VERSION,
        event_id="evt_depth_compat",
        event_type="notifications.v1",
        topic=max_depth_env.recipient_topic(),
        timestamp=max_depth_env.created_at,
        tenant_id=max_depth_env.tenant_id,
        principal_id="svc-notifications",
        payload=max_depth_env.to_dict(),
    )
    assert wrapped.serialized_bytes > 0

    too_deep: dict[str, Any] = {"child": max_depth_payload}
    with pytest.raises(InvalidNotificationError, match="depth"):
        _make_notification(payload=too_deep)

    # Lone UTF-16 surrogate raises InvalidNotificationError
    with pytest.raises(InvalidNotificationError):
        _make_notification(payload={"bad": "\ud800"})

    # Upper-bound enforcement on max_notification_bytes / max_json_depth
    with pytest.raises(InvalidNotificationError):
        _make_notification(max_notification_bytes=DEFAULT_MAX_NOTIFICATION_BYTES + 1)
    with pytest.raises(InvalidNotificationError):
        _make_notification(max_json_depth=16)

    # Cyclic payload
    cyclic: dict[str, Any] = {}
    cyclic["self"] = cyclic
    with pytest.raises(InvalidNotificationError, match="cyclic"):
        _make_notification(payload=cyclic)


def test_size_limit_boundary_and_n2_events_frame_compatibility() -> None:
    base = _make_notification(payload={"pad": ""})
    overhead = base.serialized_bytes
    max_pad_len = DEFAULT_MAX_NOTIFICATION_BYTES - overhead

    exact_env = _make_notification(payload={"pad": "a" * max_pad_len})
    assert exact_env.serialized_bytes == DEFAULT_MAX_NOTIFICATION_BYTES

    with pytest.raises(OversizedNotificationError):
        _make_notification(payload={"pad": "a" * (max_pad_len + 1)})

    # Verify N2 compatibility: max-size 48 KiB NotificationEnvelope fits inside EventEnvelope
    # and build_delivery_frame() within EVENTS_MAX_FRAME_BYTES (65_536)
    outer = EventEnvelope(
        protocol_version=EVENTS_PROTOCOL_VERSION,
        event_id="evt_n2_compat",
        event_type="notifications.v1",
        topic=exact_env.recipient_topic(),
        timestamp=exact_env.created_at,
        tenant_id=exact_env.tenant_id,
        principal_id="svc-notifications",
        payload=exact_env.to_dict(),
    )
    frame = build_delivery_frame(outer)
    assert len(json.dumps(frame).encode("utf-8")) <= EVENTS_MAX_FRAME_BYTES


def test_utc_timestamps_expiration_and_idempotency_key() -> None:
    with pytest.raises(InvalidNotificationError):
        _make_notification(created_at="2026-11-18T12:00:00")
    with pytest.raises(InvalidNotificationError):
        _make_notification(created_at="2026-11-18T12:00:00+02:00")
    with pytest.raises(InvalidNotificationError):
        _make_notification(expires_at="2026-11-18T12:00:00Z")  # not strictly after created_at

    env = _make_notification(
        created_at="2026-11-18T12:00:00+00:00",
        expires_at="2026-11-18T12:05:00Z",
    )
    assert env.created_at == "2026-11-18T12:00:00Z"
    assert env.is_expired(now=datetime(2026, 11, 18, 12, 4, 59, tzinfo=UTC)) is False
    assert env.is_expired(now=datetime(2026, 11, 18, 12, 5, 0, tzinfo=UTC)) is True

    no_exp = _make_notification(expires_at=None)
    assert no_exp.is_expired(now=datetime(2099, 1, 1, tzinfo=UTC)) is False

    for bad_idem in ("", "bad key", "a" * 129):
        with pytest.raises(InvalidNotificationError):
            _make_notification(idempotency_key=bad_idem)


def test_deterministic_recipient_topic_derivation_and_collision_resistance() -> None:
    t1 = derive_recipient_topic("tenant:1", "user:alice")
    t2 = derive_recipient_topic("tenant:1", "user:alice")
    assert t1 == t2
    assert t1.startswith(RECIPIENT_TOPIC_PREFIX)
    assert len(t1) == len(RECIPIENT_TOPIC_PREFIX) + RECIPIENT_TOPIC_HASH_HEX_LENGTH
    assert validate_topic(t1) == t1

    # Tenant separation
    assert derive_recipient_topic("tenant:1", "user:alice") != derive_recipient_topic(
        "tenant:2", "user:alice"
    )
    # Length-prefixed encoding prevents delimiter boundary collisions
    assert derive_recipient_topic("a:b", "c") != derive_recipient_topic("a", "b:c")


def test_preferences_policy_authoritative_promo_and_failure() -> None:
    async def scenario() -> None:
        store = InMemoryNotificationPreferenceStore()
        assert isinstance(store, NotificationPreferencePort)

        # Missing preferences: non-promotional in-app allowed, promotional denied, non-in-app denied
        non_promo = _make_notification(category="system", is_promotional=False)
        assert (
            evaluate_notification_preference(
                non_promo,
                await store.get_preferences(tenant_id="tenant:acme", recipient_id="user:alice"),
            ).allowed
            is True
        )
        assert (
            evaluate_notification_preference(non_promo, None, channel_id="email").allowed is False
        )
        promo = _make_notification(
            category="marketing", is_promotional=True, priority=NotificationPriority.HIGH
        )
        assert evaluate_notification_preference(promo, None).allowed is False

        # Host authoritative promotional category blocks untrusted producer setting is_promotional=False
        spoofed_promo = _make_notification(category="marketing", is_promotional=False)
        res_auth_promo = evaluate_notification_preference(
            spoofed_promo,
            None,
            authoritative_promotional_categories=("marketing",),
        )
        assert res_auth_promo.allowed is False
        assert res_auth_promo.reason == "promotional_opt_in_required"

        # Explicit preference filtering (muted category, min priority, disabled channel)
        pref = NotificationPreference(
            tenant_id="tenant:acme",
            recipient_id="user:alice",
            muted_categories=frozenset({"noisy"}),
            min_priority=NotificationPriority.HIGH,
            disabled_channels=frozenset({"sms", " sms "}),
            allow_promotional=True,
        )
        store.set_preference(pref)
        loaded = await store.get_preferences(tenant_id="tenant:acme", recipient_id="user:alice")
        assert loaded == pref
        assert evaluate_notification_preference(promo, loaded).allowed is True
        assert (
            evaluate_notification_preference(
                _make_notification(category="noisy", priority=NotificationPriority.HIGH), loaded
            ).allowed
            is False
        )
        assert (
            evaluate_notification_preference(
                _make_notification(priority=NotificationPriority.NORMAL), loaded
            ).allowed
            is False
        )
        assert (
            evaluate_notification_preference(
                _make_notification(priority=NotificationPriority.HIGH), loaded, channel_id="sms"
            ).allowed
            is False
        )

        # Preference backend failure fails closed
        store.set_fail_lookups(True)
        with pytest.raises(NotificationsPreferenceError):
            await store.get_preferences(tenant_id="tenant:acme", recipient_id="user:alice")

    asyncio.run(scenario())


def test_producer_authorizer_channel_port_lifecycle_and_telemetry() -> None:
    async def scenario() -> None:
        env = _make_notification()
        deny_all = DenyAllProducerAuthorizer()
        assert isinstance(deny_all, NotificationProducerAuthorizerPort)
        producer = VerifiedPrincipalContext(
            provider_id="idp",
            tenant_id="tenant:acme",
            principal_id="svc-1",
            connection_id="conn-1",
            allowed_scopes=frozenset({"notifications:publish"}),
        )
        with pytest.raises(NotificationAuthorizationError):
            await deny_all.authorize_publish(producer=producer, notification=env)

        class _StubChannel:
            @property
            def channel_id(self) -> str:
                return "webhook"

            async def send(self, notification: NotificationEnvelope) -> ChannelDeliveryResult:
                return ChannelDeliveryResult(channel_id="webhook", accepted=True)

        assert isinstance(_StubChannel(), NotificationChannelPort)

        # NotificationsModule GatewayModule lifecycle
        sink = InMemoryNotificationsTelemetrySink()
        mod = NotificationsModule(telemetry=sink)
        assert isinstance(mod, GatewayModule)
        assert mod.descriptor.module_id == "notifications"

        core = compose_gateway_core(modules=[mod])
        assert core.get_module("notifications") is mod
        await mod.startup()
        await mod.startup()  # idempotent
        assert mod.is_started is True
        await mod.shutdown()
        await mod.shutdown()  # idempotent
        assert mod.is_closed is True
        with pytest.raises(NotificationsModuleClosedError):
            await mod.startup()

        # Bounded-cardinality telemetry
        sink.record_published()
        sink.record_delivered(count=1)
        sink.record_expired()
        sink.record_filtered_by_preference()
        sink.record_channel_failure()
        sink.record_error(NotificationsErrorCategory.AUTHORIZATION)
        snap = sink.snapshot()
        assert snap.notifications_published == 1
        assert snap.errors_by_category[NotificationsErrorCategory.AUTHORIZATION] == 1
        assert set(snap.errors_by_category.keys()) == set(NotificationsErrorCategory)
        assert NotificationStatus.CREATED.value == "created"

    asyncio.run(scenario())


def test_notifications_core_imports_without_optional_dependencies() -> None:
    script = """
import importlib.abc
import sys

class _BlockOptional(importlib.abc.MetaPathFinder):
    def find_spec(self, fullname, path=None, target=None):
        if fullname.split(".")[0] in ("redis", "websockets", "starlette"):
            raise ModuleNotFoundError(f"Blocked {fullname}")
        return None

sys.meta_path.insert(0, _BlockOptional())
import kaiwen_gateway
import kaiwen_gateway.core
import kaiwen_gateway.modules.notifications
print("NOTIFICATIONS_CORE_ISOLATED_OK")
"""
    proc = subprocess.run(
        [sys.executable, "-c", script],
        capture_output=True,
        text=True,
        check=True,
    )
    assert proc.stdout.strip() == "NOTIFICATIONS_CORE_ISOLATED_OK"
