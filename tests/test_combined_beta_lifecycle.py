"""Combined Beta Integration Acceptance tests (Phases 2, 4, 5 — deterministic in-process).

Validates combined Core + Voice + Events + Notifications lifecycle, readiness reporting,
fail-closed security configuration, shared admission limits, concurrent Voice/Events/Notifications
execution, slow-consumer isolation, and graceful shutdown under active mixed workloads.
"""

from __future__ import annotations

import asyncio
import time
from typing import Any

import pytest
from starlette.testclient import TestClient
from starlette.websockets import WebSocketDisconnect

from kaiwen_gateway.core import (
    CORE_CONTRACT_VERSION,
    AdmissionController,
    CoreConfig,
    CoreConfigurationError,
    GatewayCore,
    ModuleDescriptor,
    ProviderPrincipal,
    TenantPrincipalBinding,
    VerifiedPrincipalContext,
    compose_gateway_core,
)
from kaiwen_gateway.modules.events import (
    EventAuthorizationError,
    EventBrokerConfig,
    EventTopicAuthorizerPort,
    SlowConsumerPolicy,
)
from kaiwen_gateway.modules.notifications import (
    NOTIFICATIONS_SCHEMA_VERSION,
    InMemoryNotificationPreferenceStore,
    InMemoryNotificationsTelemetrySink,
    NotificationEnvelope,
    NotificationPriority,
    NotificationsModule,
    NotificationsTopicAuthorizer,
    derive_recipient_topic,
    generate_notification_id,
)
from kaiwen_gateway.modules.voice import BoundedInterruptibleFakeAIEchoPort
from kaiwen_gateway.server import ServerLifecycleError, create_gateway_app
from kaiwen_gateway.transports.synthetic import (
    EVENT_CALL_END,
    EVENT_CALL_ENDED,
    EVENT_CALL_INTERRUPT,
    EVENT_CALL_INTERRUPTED,
    EVENT_CALL_START,
    EVENT_CALL_STARTED,
    EVENT_SESSION_READY,
    PCM16LE_FRAME_BYTES,
    SYNTHETIC_MEDIA_WS_ENDPOINT_PATH,
    SYNTHETIC_WS_ENDPOINT_PATH,
    SyntheticAuthBridge,
    SyntheticTokenVerifier,
)

VOICE_SECRET = b"combined_beta_voice_secret_32b!!"


class _TokenMapEventsAuth:
    """Deterministic token authenticator for Events WebSocket tests."""

    def __init__(self, principals: dict[str, VerifiedPrincipalContext]) -> None:
        self._principals = principals

    async def authenticate_bearer(self, *, token: str, connection_id: str) -> ProviderPrincipal:
        if token not in self._principals:
            raise EventAuthorizationError("Invalid or unknown token")
        return ProviderPrincipal(provider_id=f"idp:{token}")


class _TokenMapEventsAuthorizer:
    """Tenant + scope authorizer for Events WebSocket tests."""

    def __init__(self, principals: dict[str, VerifiedPrincipalContext]) -> None:
        self._by_provider = {f"idp:{tok}": ctx for tok, ctx in principals.items()}

    async def authorize_principal(
        self, *, principal: ProviderPrincipal, connection_id: str
    ) -> TenantPrincipalBinding:
        ctx = self._by_provider.get(principal.provider_id)
        if ctx is None:
            raise EventAuthorizationError("Unknown principal")
        return TenantPrincipalBinding(
            authorized_provider_id=principal.provider_id,
            tenant_id=ctx.tenant_id,
            principal_id=ctx.principal_id,
            allowed_scopes=ctx.allowed_scopes,
        )


class _AllowAllProducerAuth:
    """Authorizes configured producer principals to publish notifications."""

    async def authorize_publish(
        self,
        *,
        producer: VerifiedPrincipalContext | None,
        notification: NotificationEnvelope,
    ) -> None:
        if producer is not None and producer.tenant_id != notification.tenant_id:
            raise EventAuthorizationError("Cross-tenant producer publish denied")


class _PermissiveTopicAuthorizer:
    """Intentionally permissive authorizer to verify mandatory recipient isolation wrapping."""

    async def authorize_subscribe(
        self,
        *,
        context: VerifiedPrincipalContext,
        topic_pattern: str,
    ) -> None:
        return None


class _RecordingModule:
    """Helper GatewayModule recording startup/shutdown ordering and optional failures."""

    def __init__(
        self,
        module_id: str,
        events: list[str],
        *,
        fail_on_startup: bool = False,
    ) -> None:
        self._descriptor = ModuleDescriptor(
            module_id=module_id,
            contract_version=CORE_CONTRACT_VERSION,
            description=f"Test module {module_id}",
        )
        self._events = events
        self._fail_on_startup = fail_on_startup

    @property
    def descriptor(self) -> ModuleDescriptor:
        return self._descriptor

    async def startup(self) -> None:
        self._events.append(f"start:{self._descriptor.module_id}")
        if self._fail_on_startup:
            raise RuntimeError(f"Simulated startup failure in {self._descriptor.module_id}")

    async def shutdown(self) -> None:
        self._events.append(f"stop:{self._descriptor.module_id}")


def _make_principal(
    tenant_id: str,
    principal_id: str,
    scopes: frozenset[str] = frozenset({"events:subscribe"}),
) -> VerifiedPrincipalContext:
    return VerifiedPrincipalContext(
        connection_id=f"conn-{tenant_id}-{principal_id}",
        provider_id="synthetic-events",
        tenant_id=tenant_id,
        principal_id=principal_id,
        allowed_scopes=scopes,
    )


def _make_voice_bridge(secret: bytes = VOICE_SECRET) -> SyntheticAuthBridge:
    verifier = SyntheticTokenVerifier(
        secret_key=secret,
        enable_replay_protection=True,
    )
    return SyntheticAuthBridge(
        verifier=verifier,
        enforce_call_exclusivity=True,
    )


def _make_voice_token(
    bridge: SyntheticAuthBridge,
    *,
    tenant_id: str,
    call_id: str,
    scopes: tuple[str, ...] = ("voice.stream",),
) -> str:
    return bridge.verifier.issue_token(
        provider_id="synthetic",
        tenant_id=tenant_id,
        call_id=call_id,
        allowed_scopes=scopes,
        ttl_seconds=60.0,
    )


def _build_full_stack_app(
    *,
    core_config: CoreConfig | None = None,
    enable_voice: bool = True,
    enable_events: bool = True,
    enable_notifications: bool = True,
    events_broker_config: EventBrokerConfig | None = None,
    events_topic_authorizer: EventTopicAuthorizerPort | None = None,
    principals: dict[str, VerifiedPrincipalContext] | None = None,
) -> tuple[Any, SyntheticAuthBridge | None]:
    cfg = core_config or CoreConfig(
        instance_name="combined-beta-test",
        ingress_mode="synthetic" if enable_voice else "disabled",
        events_ingress_mode="authenticated" if enable_events else "disabled",
    )
    bridge = _make_voice_bridge() if enable_voice else None
    p_map = principals or {
        "tok-t1-u1": _make_principal("tenant-a", "user-1"),
        "tok-t1-u2": _make_principal("tenant-a", "user-2"),
        "tok-t2-u1": _make_principal("tenant-b", "user-1"),
    }
    notif_mod = (
        NotificationsModule(
            preference_port=InMemoryNotificationPreferenceStore(),
            producer_authorizer=_AllowAllProducerAuth(),
            telemetry=InMemoryNotificationsTelemetrySink(),
        )
        if enable_notifications
        else None
    )
    app = create_gateway_app(
        cfg,
        synthetic_auth_bridge=bridge,
        enable_synthetic_media=enable_voice,
        ai_media_port_factory=(
            (lambda: BoundedInterruptibleFakeAIEchoPort()) if enable_voice else None
        ),
        enable_events=enable_events,
        events_authenticator=_TokenMapEventsAuth(p_map) if enable_events else None,
        events_authorizer=_TokenMapEventsAuthorizer(p_map) if enable_events else None,
        events_broker_config=events_broker_config,
        events_topic_authorizer=(
            events_topic_authorizer
            if events_topic_authorizer is not None
            else (NotificationsTopicAuthorizer() if enable_notifications else None)
        ),
        enable_notifications=enable_notifications,
        notifications_module=notif_mod,
    )
    return app, bridge


# ---------------------------------------------------------------------------
# Phase 2: Combined Application Lifecycle
# ---------------------------------------------------------------------------


def test_phase2_core_plus_voice_only_lifecycle() -> None:
    app, bridge = _build_full_stack_app(
        enable_voice=True,
        enable_events=False,
        enable_notifications=False,
    )
    assert bridge is not None
    with TestClient(app) as client:
        assert client.get("/health").status_code == 200
        ready = client.get("/ready")
        assert ready.status_code == 200
        assert ready.json()["status"] == "ready"
        assert ready.json()["ingress_mode"] == "synthetic"
        assert "events_ingress_mode" not in ready.json()

        # Synthetic Voice WS works
        tok = _make_voice_token(bridge, tenant_id="tenant-a", call_id="call-v1")
        with client.websocket_connect(
            SYNTHETIC_WS_ENDPOINT_PATH, headers={"Authorization": f"Bearer {tok}"}
        ) as ws:
            ready_frame = ws.receive_json()
            assert ready_frame["event"] == EVENT_SESSION_READY
            ws.send_json({"event": EVENT_CALL_START, "call_id": "call-v1"})
            assert ws.receive_json()["event"] == EVENT_CALL_STARTED
            ws.send_json({"event": EVENT_CALL_END, "call_id": "call-v1"})
            assert ws.receive_json()["event"] == EVENT_CALL_ENDED

        # Disabled Events WS rejects fail-closed
        with pytest.raises(WebSocketDisconnect):
            with client.websocket_connect(
                "/ws/events/v1", headers={"Authorization": "Bearer tok-t1-u1"}
            ):
                pass


def test_phase2_core_plus_events_only_lifecycle() -> None:
    app, _ = _build_full_stack_app(
        enable_voice=False,
        enable_events=True,
        enable_notifications=False,
    )
    with TestClient(app) as client:
        assert client.get("/health").status_code == 200
        ready = client.get("/ready")
        assert ready.status_code == 200
        assert ready.json()["status"] == "ready"
        assert ready.json()["ingress_mode"] == "disabled"
        assert ready.json()["events_ingress_mode"] == "authenticated"

        with client.websocket_connect(
            "/ws/events/v1", headers={"Authorization": "Bearer tok-t1-u1"}
        ) as ws:
            frame = ws.receive_json()
            assert frame["type"] == "session.ready"
            assert frame["tenant_id"] == "tenant-a"

        # Disabled Voice WS rejects fail-closed
        with pytest.raises(WebSocketDisconnect):
            with client.websocket_connect(
                SYNTHETIC_WS_ENDPOINT_PATH, headers={"Authorization": "Bearer bad"}
            ):
                pass


def test_phase2_core_events_notifications_lifecycle_and_mandatory_isolation() -> None:
    # Even if host supplies a permissive topic authorizer, mandatory recipient isolation wraps it.
    app, _ = _build_full_stack_app(
        enable_voice=False,
        enable_events=True,
        enable_notifications=True,
        events_topic_authorizer=_PermissiveTopicAuthorizer(),
    )
    assert [m.descriptor.module_id for m in app.state.core.modules()] == [
        "events",
        "notifications",
    ]
    with TestClient(app) as client:
        ready = client.get("/ready")
        assert ready.status_code == 200
        assert ready.json()["events_ingress_mode"] == "authenticated"

        u2_topic = derive_recipient_topic("tenant-a", "user-2")
        with client.websocket_connect(
            "/ws/events/v1", headers={"Authorization": "Bearer tok-t1-u1"}
        ) as ws:
            assert ws.receive_json()["type"] == "session.ready"
            # Cross-recipient subscription must be denied fail-closed
            ws.send_json(
                {
                    "type": "events.subscribe",
                    "protocol_version": "v1",
                    "topic": u2_topic,
                    "request_id": "r1",
                }
            )
            err = ws.receive_json()
            assert err["type"] == "events.error"
            assert err["request_id"] == "r1"
            assert err["code"] == "FORBIDDEN_SUBSCRIPTION"

            # Own recipient topic succeeds
            u1_topic = derive_recipient_topic("tenant-a", "user-1")
            ws.send_json(
                {
                    "type": "events.subscribe",
                    "protocol_version": "v1",
                    "topic": u1_topic,
                    "request_id": "r2",
                }
            )
            sub = ws.receive_json()
            assert sub["type"] == "events.subscribed"
            assert sub["request_id"] == "r2"

    assert app.state.stopped is True
    assert app.state.events_module.broker.is_closed is True
    assert app.state.notifications_module.is_closed is True


def test_phase2_full_combined_voice_events_notifications_lifecycle() -> None:
    app, bridge = _build_full_stack_app(
        enable_voice=True,
        enable_events=True,
        enable_notifications=True,
    )
    assert bridge is not None
    assert [m.descriptor.module_id for m in app.state.core.modules()] == [
        "events",
        "notifications",
    ]
    with TestClient(app) as client:
        ready = client.get("/ready")
        assert ready.status_code == 200
        payload = ready.json()
        assert payload["status"] == "ready"
        assert payload["ingress_mode"] == "synthetic"
        assert payload["events_ingress_mode"] == "authenticated"

        # Simultaneously connect Voice media and Events WS
        v_tok = _make_voice_token(bridge, tenant_id="tenant-a", call_id="call-combo-1")
        u1_topic = derive_recipient_topic("tenant-a", "user-1")
        with (
            client.websocket_connect(
                SYNTHETIC_MEDIA_WS_ENDPOINT_PATH,
                headers={"Authorization": f"Bearer {v_tok}"},
            ) as vws,
            client.websocket_connect(
                "/ws/events/v1", headers={"Authorization": "Bearer tok-t1-u1"}
            ) as ews,
        ):
            assert vws.receive_json()["event"] == EVENT_SESSION_READY
            assert ews.receive_json()["type"] == "session.ready"

            ews.send_json(
                {
                    "type": "events.subscribe",
                    "protocol_version": "v1",
                    "topic": u1_topic,
                    "request_id": "s1",
                }
            )
            assert ews.receive_json()["type"] == "events.subscribed"

            vws.send_json({"event": EVENT_CALL_START, "call_id": "call-combo-1"})
            assert vws.receive_json()["event"] == EVENT_CALL_STARTED

            # Publish a notification via app.state.notifications_dispatcher
            producer = _make_principal(
                "tenant-a", "svc-notifier", scopes=frozenset({"notifications:publish"})
            )
            notif = NotificationEnvelope(
                notification_id=generate_notification_id(),
                schema_version=NOTIFICATIONS_SCHEMA_VERSION,
                tenant_id="tenant-a",
                recipient_id="user-1",
                category="system.alert",
                notification_type="alert.combined",
                priority=NotificationPriority.HIGH,
                payload={"title": "Combined check", "body": "Voice and Events active together"},
                created_at="2026-11-18T20:00:00Z",
            )
            pub_res = asyncio.run(
                app.state.notifications_dispatcher.publish(notif, producer=producer)
            )
            assert pub_res.dispatched_connections == 1

            delivery = ews.receive_json()
            assert delivery["type"] == "events.delivery"
            assert delivery["event"]["payload"]["notification_id"] == notif.notification_id

            vws.send_json({"event": EVENT_CALL_END, "call_id": "call-combo-1"})
            assert vws.receive_json()["event"] == EVENT_CALL_ENDED

    assert app.state.stopped is True


def test_phase2_fail_closed_security_config_validation() -> None:
    with pytest.raises(CoreConfigurationError):
        create_gateway_app(
            CoreConfig(events_ingress_mode="authenticated"),
            enable_events=True,
            enable_notifications=True,
            events_authenticator=_TokenMapEventsAuth({}),
            events_authorizer=_TokenMapEventsAuthorizer({}),
            events_topic_authorizer=None,  # Missing mandatory topic authorizer
        )

    with pytest.raises(CoreConfigurationError):
        create_gateway_app(
            CoreConfig(events_ingress_mode="disabled"),
            enable_events=False,
            enable_notifications=True,
            events_topic_authorizer=NotificationsTopicAuthorizer(),
        )


def test_phase2_optional_module_startup_failure_rolls_back_and_fails_closed() -> None:
    lifecycle_log: list[str] = []
    mod_ok = _RecordingModule("events_pre", lifecycle_log, fail_on_startup=False)
    mod_bad = _RecordingModule("failing_mod", lifecycle_log, fail_on_startup=True)
    core: GatewayCore = compose_gateway_core(
        config=CoreConfig(ingress_mode="disabled", events_ingress_mode="disabled"),
        modules=[mod_ok, mod_bad],
    )
    app = create_gateway_app(core.config, core=core)

    with pytest.raises(ServerLifecycleError):
        with TestClient(app):
            pass

    assert lifecycle_log == ["start:events_pre", "start:failing_mod", "stop:events_pre"]
    assert app.state.startup_failed is True
    assert app.state.started is False
    assert app.state.stopped is True


# ---------------------------------------------------------------------------
# Phase 4 & 5 (In-Process): Shared Admission, Voice Interruption + Events, Slow Consumer Isolation
# ---------------------------------------------------------------------------


def test_phase4_shared_admission_ceiling_across_voice_and_events() -> None:
    cfg = CoreConfig(
        ingress_mode="synthetic",
        events_ingress_mode="authenticated",
        max_concurrent_connections=2,
        max_connections_per_tenant=2,
    )
    app, bridge = _build_full_stack_app(
        core_config=cfg,
        enable_voice=True,
        enable_events=True,
        enable_notifications=True,
    )
    assert bridge is not None
    controller: AdmissionController = app.state.admission_controller

    with TestClient(app) as client:
        v_tok = _make_voice_token(bridge, tenant_id="tenant-a", call_id="call-adm-1")
        with (
            client.websocket_connect(
                SYNTHETIC_MEDIA_WS_ENDPOINT_PATH,
                headers={"Authorization": f"Bearer {v_tok}"},
            ) as vws,
            client.websocket_connect(
                "/ws/events/v1", headers={"Authorization": "Bearer tok-t1-u1"}
            ) as ews,
        ):
            assert vws.receive_json()["event"] == EVENT_SESSION_READY
            assert ews.receive_json()["type"] == "session.ready"
            assert controller.snapshot().available_capacity == 0

            # /ready must report 503 capacity_exceeded when shared admission ceiling is full
            ready = client.get("/ready")
            assert ready.status_code == 503
            assert ready.json()["reason"] == "capacity_exceeded"

            # A 3rd connection (whether Events or Voice) must be rejected fail-closed
            with pytest.raises(WebSocketDisconnect):
                with client.websocket_connect(
                    "/ws/events/v1", headers={"Authorization": "Bearer tok-t1-u2"}
                ):
                    pass

    assert controller.snapshot().active_connections == 0


def test_phase4_voice_barge_in_interruption_with_concurrent_events_and_notifications() -> None:
    app, bridge = _build_full_stack_app(
        enable_voice=True,
        enable_events=True,
        enable_notifications=True,
    )
    assert bridge is not None

    with TestClient(app) as client:
        v_tok = _make_voice_token(bridge, tenant_id="tenant-a", call_id="call-barge-1")
        u1_topic = derive_recipient_topic("tenant-a", "user-1")
        with (
            client.websocket_connect(
                SYNTHETIC_MEDIA_WS_ENDPOINT_PATH,
                headers={"Authorization": f"Bearer {v_tok}"},
            ) as vws,
            client.websocket_connect(
                "/ws/events/v1", headers={"Authorization": "Bearer tok-t1-u1"}
            ) as ews,
        ):
            assert vws.receive_json()["event"] == EVENT_SESSION_READY
            assert ews.receive_json()["type"] == "session.ready"
            ews.send_json(
                {
                    "type": "events.subscribe",
                    "protocol_version": "v1",
                    "topic": u1_topic,
                    "request_id": "sub1",
                }
            )
            assert ews.receive_json()["type"] == "events.subscribed"

            vws.send_json({"event": EVENT_CALL_START, "call_id": "call-barge-1"})
            assert vws.receive_json()["event"] == EVENT_CALL_STARTED

            # Send 3 binary PCM16LE audio frames for Turn 1 and receive echoed audio
            pcm_frame = b"\x01\x02" * (PCM16LE_FRAME_BYTES // 2)
            for _ in range(3):
                vws.send_bytes(pcm_frame)
            for _ in range(3):
                echoed = vws.receive_bytes()
                assert len(echoed) == PCM16LE_FRAME_BYTES

            # Trigger barge-in interruption while publishing a notification concurrently
            producer = _make_principal(
                "tenant-a", "svc-notifier", scopes=frozenset({"notifications:publish"})
            )
            barge_notif = NotificationEnvelope(
                notification_id=generate_notification_id(),
                schema_version=NOTIFICATIONS_SCHEMA_VERSION,
                tenant_id="tenant-a",
                recipient_id="user-1",
                category="system.alert",
                notification_type="alert.barge",
                priority=NotificationPriority.HIGH,
                payload={"during": "barge_in"},
                created_at="2026-11-18T20:00:00Z",
            )
            vws.send_json(
                {
                    "event": EVENT_CALL_INTERRUPT,
                    "call_id": "call-barge-1",
                    "turn_id": 1,
                    "request_id": "req-barge-1",
                }
            )
            pub_res = asyncio.run(
                app.state.notifications_dispatcher.publish(barge_notif, producer=producer)
            )
            assert pub_res.dispatched_connections == 1
            interrupted = vws.receive_json()
            assert interrupted["event"] == EVENT_CALL_INTERRUPTED
            assert interrupted["interrupted_turn_id"] == 1
            assert interrupted["new_turn_id"] == 2
            delivery = ews.receive_json()
            assert delivery["type"] == "events.delivery"
            assert delivery["event"]["payload"]["notification_id"] == barge_notif.notification_id

            # Turn 2 audio continues cleanly
            vws.send_bytes(pcm_frame)
            assert len(vws.receive_bytes()) == PCM16LE_FRAME_BYTES

            vws.send_json({"event": EVENT_CALL_END, "call_id": "call-barge-1"})
            assert vws.receive_json()["event"] == EVENT_CALL_ENDED


def test_phase5_slow_events_subscriber_isolated_from_healthy_and_voice() -> None:
    broker_cfg = EventBrokerConfig(
        max_queued_events_per_connection=4,
        max_queued_bytes_per_connection=16_384,
        slow_consumer_policy=SlowConsumerPolicy.DISCONNECT_SLOW_CONSUMER,
    )
    app, bridge = _build_full_stack_app(
        enable_voice=True,
        enable_events=True,
        enable_notifications=True,
        events_broker_config=broker_cfg,
    )
    assert bridge is not None

    with TestClient(app) as client:
        v_tok = _make_voice_token(bridge, tenant_id="tenant-a", call_id="call-slow-iso")
        u1_topic = derive_recipient_topic("tenant-a", "user-1")
        u2_topic = derive_recipient_topic("tenant-a", "user-2")

        with (
            client.websocket_connect(
                SYNTHETIC_MEDIA_WS_ENDPOINT_PATH,
                headers={"Authorization": f"Bearer {v_tok}"},
            ) as vws,
            client.websocket_connect(
                "/ws/events/v1", headers={"Authorization": "Bearer tok-t1-u1"}
            ) as slow_ws,
            client.websocket_connect(
                "/ws/events/v1", headers={"Authorization": "Bearer tok-t1-u2"}
            ) as healthy_ws,
        ):
            assert vws.receive_json()["event"] == EVENT_SESSION_READY
            assert slow_ws.receive_json()["type"] == "session.ready"
            assert healthy_ws.receive_json()["type"] == "session.ready"

            slow_ws.send_json(
                {
                    "type": "events.subscribe",
                    "protocol_version": "v1",
                    "topic": u1_topic,
                    "request_id": "s1",
                }
            )
            assert slow_ws.receive_json()["type"] == "events.subscribed"

            healthy_ws.send_json(
                {
                    "type": "events.subscribe",
                    "protocol_version": "v1",
                    "topic": u2_topic,
                    "request_id": "s2",
                }
            )
            assert healthy_ws.receive_json()["type"] == "events.subscribed"

            vws.send_json({"event": EVENT_CALL_START, "call_id": "call-slow-iso"})
            assert vws.receive_json()["event"] == EVENT_CALL_STARTED

            # Overflow slow_ws's 4-event outbound queue without reading from slow_ws
            producer = _make_principal(
                "tenant-a", "svc-notifier", scopes=frozenset({"notifications:publish"})
            )
            for idx in range(12):
                notif = NotificationEnvelope(
                    notification_id=generate_notification_id(),
                    schema_version=NOTIFICATIONS_SCHEMA_VERSION,
                    tenant_id="tenant-a",
                    recipient_id="user-1",
                    category="system.alert",
                    notification_type="alert.flood",
                    priority=NotificationPriority.NORMAL,
                    payload={"title": f"Flood {idx}", "body": "overflow slow consumer"},
                    created_at="2026-11-18T20:00:00Z",
                )
                asyncio.run(app.state.notifications_dispatcher.publish(notif, producer=producer))

            # Healthy recipient user-2 still receives notifications normally
            healthy_notif = NotificationEnvelope(
                notification_id=generate_notification_id(),
                schema_version=NOTIFICATIONS_SCHEMA_VERSION,
                tenant_id="tenant-a",
                recipient_id="user-2",
                category="system.alert",
                notification_type="alert.healthy",
                priority=NotificationPriority.HIGH,
                payload={"title": "Healthy", "body": "still delivered"},
                created_at="2026-11-18T20:00:00Z",
            )
            res = asyncio.run(
                app.state.notifications_dispatcher.publish(healthy_notif, producer=producer)
            )
            assert res.dispatched_connections == 1
            delivered = healthy_ws.receive_json()
            assert delivered["event"]["payload"]["notification_id"] == healthy_notif.notification_id

            # Active Voice session continues unaffected
            pcm = b"\x05\x06" * (PCM16LE_FRAME_BYTES // 2)
            vws.send_bytes(pcm)
            assert len(vws.receive_bytes()) == PCM16LE_FRAME_BYTES

            vws.send_json({"event": EVENT_CALL_END, "call_id": "call-slow-iso"})
            assert vws.receive_json()["event"] == EVENT_CALL_ENDED

            # Allow broker to reflect slow consumer disconnection (only healthy_ws remains active)
            deadline = time.monotonic() + 2.0
            while (
                app.state.events_module.broker.stats().active_connections > 1
                and time.monotonic() < deadline
            ):
                time.sleep(0.02)
            assert app.state.events_module.broker.stats().active_connections == 1
