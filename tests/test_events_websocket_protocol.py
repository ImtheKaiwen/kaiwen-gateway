"""Offline ASGI/WebSocket tests for E2 authorization, protocol, rate limits, and lifecycle."""

from __future__ import annotations

import asyncio
import json

import pytest
from starlette.testclient import TestClient
from starlette.websockets import WebSocketDisconnect

from kaiwen_gateway.core import (
    CoreConfig,
    ProviderPrincipal,
    TenantPrincipalBinding,
)
from kaiwen_gateway.modules.events import (
    EVENTS_PROTOCOL_VERSION,
    EventAuthorizationError,
    EventBrokerConfig,
    EventEnvelope,
    EventsModule,
)
from kaiwen_gateway.server.app import create_gateway_app
from kaiwen_gateway.transports.synthetic import SyntheticAuthBridge, SyntheticTokenVerifier


class _ScopedBearerAuth:
    async def authenticate_bearer(self, *, token: str, connection_id: str) -> ProviderPrincipal:
        if token in {"tenant-a-exact", "tenant-a-wild", "tenant-b-wild", "no-events"}:
            return ProviderPrincipal(provider_id=token)
        raise EventAuthorizationError("Invalid token")


class _ScopedAuthorizer:
    async def authorize_principal(
        self, *, principal: ProviderPrincipal, connection_id: str
    ) -> TenantPrincipalBinding:
        pid = principal.provider_id
        if pid == "tenant-a-exact":
            return TenantPrincipalBinding(
                authorized_provider_id=pid,
                tenant_id="tenant-a",
                principal_id="user-a1",
                allowed_scopes=frozenset({"events:subscribe"}),
            )
        if pid == "tenant-a-wild":
            return TenantPrincipalBinding(
                authorized_provider_id=pid,
                tenant_id="tenant-a",
                principal_id="user-a2",
                allowed_scopes=frozenset({"events:subscribe", "events:subscribe:wildcard"}),
            )
        if pid == "tenant-b-wild":
            return TenantPrincipalBinding(
                authorized_provider_id=pid,
                tenant_id="tenant-b",
                principal_id="user-b1",
                allowed_scopes=frozenset({"events:subscribe", "events:subscribe:wildcard"}),
            )
        return TenantPrincipalBinding(
            authorized_provider_id=pid,
            tenant_id="tenant-a",
            principal_id="user-no",
            allowed_scopes=frozenset({"voice:connect"}),
        )


def _env(
    *,
    event_id: str = "evt-1",
    topic: str = "call.status",
    tenant_id: str = "tenant-a",
    payload: dict[str, object] | None = None,
) -> EventEnvelope:
    return EventEnvelope(
        protocol_version=EVENTS_PROTOCOL_VERSION,
        event_id=event_id,
        event_type="call.status.updated",
        topic=topic,
        timestamp="2026-11-18T00:00:00Z",
        tenant_id=tenant_id,
        principal_id="server-pub",
        payload=payload if payload is not None else {"ok": True},
    )


def _make_events_app(
    *,
    config: CoreConfig | None = None,
    broker_config: EventBrokerConfig | None = None,
    synthetic_auth_bridge: SyntheticAuthBridge | None = None,
) -> tuple[object, EventsModule]:
    events_mod = EventsModule(broker_config=broker_config)
    app = create_gateway_app(
        config or CoreConfig(ingress_mode="disabled", events_ingress_mode="authenticated"),
        synthetic_auth_bridge=synthetic_auth_bridge,
        enable_events=True,
        events_authenticator=_ScopedBearerAuth(),
        events_authorizer=_ScopedAuthorizer(),
        events_module=events_mod,
    )
    return app, events_mod


def test_authorization_forged_identity_wildcards_publish_and_tenant_isolation() -> None:
    app, events_mod = _make_events_app()
    with TestClient(app) as client:  # type: ignore[arg-type]
        # Unauthorized topic & wildcard
        with client.websocket_connect(
            "/ws/events/v1", headers={"Authorization": "Bearer no-events"}
        ) as ws_no:
            ws_no.receive_json()
            ws_no.send_json(
                {
                    "protocol_version": "v1",
                    "type": "events.subscribe",
                    "topic_pattern": "call.status",
                    "request_id": "r1",
                }
            )
            err = ws_no.receive_json()
            assert err["type"] == "events.error"
            assert err["code"] == "FORBIDDEN_SUBSCRIPTION"

        with client.websocket_connect(
            "/ws/events/v1", headers={"Authorization": "Bearer tenant-a-exact"}
        ) as ws_exact:
            ws_exact.receive_json()
            ws_exact.send_json(
                {
                    "protocol_version": "v1",
                    "type": "events.subscribe",
                    "topic_pattern": "call.*",
                    "request_id": "r2",
                }
            )
            err_w = ws_exact.receive_json()
            assert err_w["type"] == "events.error"
            assert err_w["code"] == "FORBIDDEN_SUBSCRIPTION"

        # Forged tenant_id closes 1008
        with client.websocket_connect(
            "/ws/events/v1", headers={"Authorization": "Bearer tenant-a-exact"}
        ) as ws_forge:
            ws_forge.receive_json()
            ws_forge.send_json(
                {
                    "protocol_version": "v1",
                    "type": "session.ping",
                    "tenant_id": "tenant-b",
                }
            )
            with pytest.raises(WebSocketDisconnect) as exc_forge:
                ws_forge.receive_json()
            assert exc_forge.value.code == 1008

        # Client publish closes 1008
        with client.websocket_connect(
            "/ws/events/v1", headers={"Authorization": "Bearer tenant-a-exact"}
        ) as ws_pub:
            ws_pub.receive_json()
            ws_pub.send_json(
                {
                    "protocol_version": "v1",
                    "type": "events.publish",
                    "topic": "call.status",
                }
            )
            with pytest.raises(WebSocketDisconnect) as exc_pub:
                ws_pub.receive_json()
            assert exc_pub.value.code == 1008

        # Cross-tenant isolation + deduplicated overlapping delivery
        with (
            client.websocket_connect(
                "/ws/events/v1", headers={"Authorization": "Bearer tenant-a-wild"}
            ) as ws_a,
            client.websocket_connect(
                "/ws/events/v1", headers={"Authorization": "Bearer tenant-b-wild"}
            ) as ws_b,
        ):
            ws_a.receive_json()
            ws_b.receive_json()
            ws_a.send_json(
                {"protocol_version": "v1", "type": "events.subscribe", "topic_pattern": "call.*"}
            )
            sub_a1 = ws_a.receive_json()
            assert sub_a1["type"] == "events.subscribed"

            ws_a.send_json(
                {
                    "protocol_version": "v1",
                    "type": "events.subscribe",
                    "topic_pattern": "call.status",
                }
            )
            assert ws_a.receive_json()["type"] == "events.subscribed"

            ws_b.send_json(
                {"protocol_version": "v1", "type": "events.subscribe", "topic_pattern": "call.*"}
            )
            assert ws_b.receive_json()["type"] == "events.subscribed"

            # Publish to tenant-a: ws_a gets exactly 1 delivery, ws_b gets none
            res = asyncio.run(events_mod.broker.publish(_env(event_id="evt-iso-1")))
            assert res.delivered_subscribers == 1
            delivered = ws_a.receive_json()
            assert delivered["type"] == "events.delivery"
            assert delivered["event"]["event_id"] == "evt-iso-1"

            # Unsubscribe & ping
            ws_a.send_json(
                {
                    "protocol_version": "v1",
                    "type": "events.unsubscribe",
                    "subscription_id": sub_a1["subscription_id"],
                }
            )
            unsub = ws_a.receive_json()
            assert unsub["type"] == "events.unsubscribed"
            assert unsub["removed"] is True

            ws_b.send_json({"protocol_version": "v1", "type": "session.ping", "request_id": "p1"})
            pong = ws_b.receive_json()
            assert pong["type"] == "session.pong"
            assert pong["request_id"] == "p1"

    assert events_mod.broker.stats().active_connections == 0
    assert events_mod.broker.stats().queued_bytes == 0


def test_protocol_validation_close_codes() -> None:
    app, _ = _make_events_app()
    with TestClient(app) as client:  # type: ignore[arg-type]
        bad_cases: list[tuple[str | bytes, int]] = [
            ("not json {{{", 1007),
            (b"\xff\xfe\xfd", 1007),
            (json.dumps({"protocol_version": "v2", "type": "session.ping"}), 1008),
            (json.dumps({"protocol_version": "v1", "type": "unknown.cmd"}), 1007),
            (
                json.dumps({"protocol_version": "v1", "type": "session.ping", "x": float("nan")})
                if False
                else '{"protocol_version":"v1","type":"session.ping","x":NaN}',
                1007,
            ),
            ("a" * 70_000, 1009),
        ]
        for payload, expected_code in bad_cases:
            with client.websocket_connect(
                "/ws/events/v1", headers={"Authorization": "Bearer tenant-a-exact"}
            ) as ws:
                ws.receive_json()
                if isinstance(payload, bytes):
                    ws.send_bytes(payload)
                else:
                    ws.send_text(payload)
                with pytest.raises(WebSocketDisconnect) as exc:
                    ws.receive_json()
                assert exc.value.code == expected_code


def test_control_rate_limit_abuse_and_slow_consumer_disconnect() -> None:
    app, events_mod = _make_events_app(
        broker_config=EventBrokerConfig(max_queued_events_per_connection=2)
    )
    app.state.events_control_rate_per_second = 1.0  # type: ignore[attr-defined]
    app.state.events_control_burst = 2.0  # type: ignore[attr-defined]

    with TestClient(app) as client:  # type: ignore[arg-type]
        # Control rate-limit burst 2 -> repeated abuse closes 1008
        with client.websocket_connect(
            "/ws/events/v1", headers={"Authorization": "Bearer tenant-a-exact"}
        ) as ws:
            ws.receive_json()
            ws.send_json({"protocol_version": "v1", "type": "session.ping"})
            assert ws.receive_json()["type"] == "session.pong"
            ws.send_json({"protocol_version": "v1", "type": "session.ping"})
            assert ws.receive_json()["type"] == "session.pong"

            # Exceed burst 3 times -> 2 RATE_LIMITED errors, 3rd closes 1008
            for _ in range(2):
                ws.send_json({"protocol_version": "v1", "type": "session.ping"})
                assert ws.receive_json()["code"] == "RATE_LIMITED"
            ws.send_json({"protocol_version": "v1", "type": "session.ping"})
            ws.receive_json()
            with pytest.raises(WebSocketDisconnect) as exc_rl:
                ws.receive_json()
            assert exc_rl.value.code == 1008

    assert events_mod.broker.stats().active_connections == 0
    assert events_mod.broker.stats().queued_bytes == 0


def test_slow_consumer_disconnects_websocket_1008_and_cleans_counters() -> None:
    app, events_mod = _make_events_app(
        broker_config=EventBrokerConfig(max_queued_events_per_connection=2)
    )
    with TestClient(app) as client:  # type: ignore[arg-type]
        with client.websocket_connect(
            "/ws/events/v1", headers={"Authorization": "Bearer tenant-a-exact"}
        ) as ws:
            ws.receive_json()
            ws.send_json(
                {
                    "protocol_version": "v1",
                    "type": "events.subscribe",
                    "topic_pattern": "call.status",
                }
            )
            assert ws.receive_json()["type"] == "events.subscribed"

            # Publish 3 events without reading from ws (max_queued_events_per_connection=2)
            for idx in range(3):
                asyncio.run(events_mod.broker.publish(_env(event_id=f"slow-{idx}")))

            # Slow consumer overflow disconnects with 1008
            with pytest.raises(WebSocketDisconnect) as exc_slow:
                while True:
                    ws.receive_json()
            assert exc_slow.value.code == 1008

    assert events_mod.broker.stats().active_connections == 0
    assert events_mod.broker.stats().queued_bytes == 0
    assert app.state.admission_controller.snapshot().active_connections == 0  # type: ignore[attr-defined]


def test_voice_and_events_shared_admission_capacity() -> None:
    bridge = SyntheticAuthBridge(
        verifier=SyntheticTokenVerifier(b"test-secret-key-0123456789abcdef")
    )
    token_v = bridge.verifier.issue_token(
        provider_id="synthetic-provider",
        tenant_id="tenant-a",
        call_id="call-voice-1",
        allowed_scopes=("voice.stream",),
    )
    config = CoreConfig(
        ingress_mode="synthetic",
        events_ingress_mode="authenticated",
        max_concurrent_connections=2,
        max_connections_per_tenant=2,
    )
    app, _ = _make_events_app(config=config, synthetic_auth_bridge=bridge)

    with TestClient(app) as client:  # type: ignore[arg-type]
        with (
            client.websocket_connect(
                "/ws/synthetic/v1", headers={"Authorization": f"Bearer {token_v}"}
            ) as ws_voice,
            client.websocket_connect(
                "/ws/events/v1", headers={"Authorization": "Bearer tenant-a-exact"}
            ) as ws_evt,
        ):
            assert ws_voice.receive_json()["event"] == "session.ready"
            assert ws_evt.receive_json()["type"] == "session.ready"

            # 3rd connection exceeds shared max_concurrent_connections=2
            with pytest.raises(WebSocketDisconnect) as exc_cap:
                with client.websocket_connect(
                    "/ws/events/v1", headers={"Authorization": "Bearer tenant-b-wild"}
                ) as w3:
                    w3.receive_json()
            assert exc_cap.value.code == 1008

        assert app.state.admission_controller.snapshot().active_connections == 0  # type: ignore[attr-defined]
