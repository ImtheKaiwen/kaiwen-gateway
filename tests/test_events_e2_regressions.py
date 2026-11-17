"""Deterministic regression tests reproducing E2 R1-R4 security and contract issues."""

from __future__ import annotations

import asyncio
import json

import pytest
from starlette.testclient import TestClient
from starlette.websockets import WebSocketDisconnect

from kaiwen_gateway.core import (
    AdmissionController,
    CoreConfig,
    CoreConfigurationError,
    InstanceDrainCoordinator,
    ProviderPrincipal,
    TenantPrincipalBinding,
)
from kaiwen_gateway.modules.events import (
    DEFAULT_MAX_EVENT_BYTES,
    EVENTS_PROTOCOL_VERSION,
    BoundedTicketReplayCache,
    EventEnvelope,
    EventsModule,
)
from kaiwen_gateway.server.app import create_gateway_app
from kaiwen_gateway.transports.events import events_websocket_endpoint


class _Auth:
    async def authenticate_bearer(self, *, token: str, connection_id: str) -> ProviderPrincipal:
        return ProviderPrincipal(provider_id="idp")


class _Authz:
    async def authorize_principal(
        self, *, principal: ProviderPrincipal, connection_id: str
    ) -> TenantPrincipalBinding:
        return TenantPrincipalBinding(
            authorized_provider_id=principal.provider_id,
            tenant_id="tenant-a",
            principal_id="user-1",
            allowed_scopes=frozenset({"events:subscribe"}),
        )


def test_r1_replay_cache_never_evicts_unexpired_and_fails_closed() -> None:
    async def scenario() -> None:
        now = 100.0
        cache = BoundedTicketReplayCache(max_entries=1, clock=lambda: now)

        assert await cache.consume("jti-A", expires_at=120.0) is True
        # Capacity exhausted by unexpired jti-A -> jti-B must fail closed
        assert await cache.consume("jti-B", expires_at=120.0) is False
        # Retrying jti-A must still be rejected (was NOT evicted!)
        assert await cache.consume("jti-A", expires_at=120.0) is False

        # Advance clock past jti-A expiry -> jti-C succeeds
        now = 121.0
        assert await cache.consume("jti-C", expires_at=140.0) is True

    asyncio.run(scenario())


def test_r2_accepted_wire_protocol_fields_and_unsupported_version_1008() -> None:
    app = create_gateway_app(
        CoreConfig(ingress_mode="disabled", events_ingress_mode="authenticated"),
        enable_events=True,
        events_authenticator=_Auth(),
        events_authorizer=_Authz(),
    )
    with TestClient(app) as client:
        with client.websocket_connect(
            "/ws/events/v1", headers={"Authorization": "Bearer ok"}
        ) as ws:
            ready = ws.receive_json()
            assert ready["type"] == "session.ready"
            assert ready["allowed_scopes"] == ["events:subscribe"]

            ws.send_json({"protocol_version": "v1", "type": "session.ping", "seq": 7})
            pong = ws.receive_json()
            assert pong["type"] == "session.pong"
            assert pong["seq"] == 7

            ws.send_json(
                {
                    "protocol_version": "v1",
                    "type": "events.subscribe",
                    "request_id": "req-1",
                    "topic": "call.status",
                }
            )
            sub = ws.receive_json()
            assert sub["type"] == "events.subscribed"
            assert sub["request_id"] == "req-1"

            ws.send_json(
                {
                    "protocol_version": "v1",
                    "type": "events.unsubscribe",
                    "request_id": "req-2",
                    "topic": "call.status",
                }
            )
            unsub = ws.receive_json()
            assert unsub["type"] == "events.unsubscribed"
            assert unsub["request_id"] == "req-2"
            assert unsub["removed"] is True

        # Unsupported protocol_version closes with 1008 (FR-EVT-003)
        with client.websocket_connect(
            "/ws/events/v1", headers={"Authorization": "Bearer ok"}
        ) as ws_ver:
            ws_ver.receive_json()
            ws_ver.send_json({"protocol_version": "v99", "type": "session.ping"})
            with pytest.raises(WebSocketDisconnect) as exc_ver:
                ws_ver.receive_json()
            assert exc_ver.value.code == 1008


def test_r3_stalled_send_times_out_and_releases_lease() -> None:
    class _StalledWebSocket:
        def __init__(self, app_obj: object) -> None:
            self.app = app_obj
            self.headers = {"authorization": "Bearer ok"}
            self.query_params: dict[str, str] = {}
            self.client_state = None
            self.closed_codes: list[int] = []

        async def accept(self) -> None:
            pass

        async def send_json(self, data: object) -> None:
            await asyncio.sleep(5.0)

        async def receive(self) -> dict[str, str]:
            await asyncio.sleep(5.0)
            return {"type": "websocket.disconnect"}

        async def close(self, code: int = 1000, reason: str = "") -> None:
            self.closed_codes.append(code)

    async def scenario() -> None:
        app = create_gateway_app(
            CoreConfig(ingress_mode="disabled", events_ingress_mode="authenticated"),
            enable_events=True,
            events_authenticator=_Auth(),
            events_authorizer=_Authz(),
            events_send_timeout_seconds=0.05,
        )
        await app.state.module_lifecycle.startup()
        controller: AdmissionController = app.state.admission_controller
        coordinator: InstanceDrainCoordinator = app.state.drain_coordinator
        ws = _StalledWebSocket(app)

        await asyncio.wait_for(events_websocket_endpoint(ws), timeout=1.0)  # type: ignore[arg-type]
        assert controller.snapshot().active_connections == 0
        await coordinator.drain()
        await app.state.module_lifecycle.shutdown()

    asyncio.run(scenario())


def test_r4_outbound_delivery_frame_respects_65536_byte_limit() -> None:
    # Build an EventEnvelope right at 65,536 bytes
    probe = EventEnvelope(
        protocol_version=EVENTS_PROTOCOL_VERSION,
        event_id="evt-max-1",
        event_type="call.status.updated",
        topic="call.status",
        timestamp="2026-11-18T00:00:00Z",
        tenant_id="tenant-a",
        principal_id="pub-1",
        payload={"data": ""},
    )
    max_env = EventEnvelope(
        protocol_version=EVENTS_PROTOCOL_VERSION,
        event_id="evt-max-1",
        event_type="call.status.updated",
        topic="call.status",
        timestamp="2026-11-18T00:00:00Z",
        tenant_id="tenant-a",
        principal_id="pub-1",
        payload={"data": "a" * (DEFAULT_MAX_EVENT_BYTES - probe.serialized_bytes)},
    )
    assert max_env.serialized_bytes == DEFAULT_MAX_EVENT_BYTES

    events_mod = EventsModule()
    app = create_gateway_app(
        CoreConfig(ingress_mode="disabled", events_ingress_mode="authenticated"),
        enable_events=True,
        events_authenticator=_Auth(),
        events_authorizer=_Authz(),
        events_module=events_mod,
    )
    with TestClient(app) as client:
        with client.websocket_connect(
            "/ws/events/v1", headers={"Authorization": "Bearer ok"}
        ) as ws:
            ws.receive_json()
            ws.send_json(
                {
                    "protocol_version": "v1",
                    "type": "events.subscribe",
                    "request_id": "r1",
                    "topic": "call.status",
                }
            )
            assert ws.receive_json()["type"] == "events.subscribed"

            asyncio.run(events_mod.broker.publish(max_env))
            msg = ws.receive_json()
            encoded = json.dumps(msg, separators=(",", ":")).encode("utf-8")
            assert len(encoded) <= DEFAULT_MAX_EVENT_BYTES
            assert msg["type"] == "events.error"
            assert msg["code"] == "FRAME_TOO_LARGE"


@pytest.mark.parametrize(
    "bad_timeout",
    [
        float("nan"),
        float("inf"),
        float("-inf"),
        0,
        0.0,
        -1.0,
        True,
        False,
        "5",
        None,
    ],
)
def test_events_send_timeout_seconds_rejects_invalid_values(bad_timeout: object) -> None:
    with pytest.raises(CoreConfigurationError):
        create_gateway_app(
            CoreConfig(ingress_mode="disabled", events_ingress_mode="authenticated"),
            enable_events=True,
            events_authenticator=_Auth(),
            events_authorizer=_Authz(),
            events_send_timeout_seconds=bad_timeout,  # type: ignore[arg-type]
        )


def test_events_send_timeout_seconds_accepts_finite_positive_numbers() -> None:
    for good_timeout in (5, 0.25):
        app = create_gateway_app(
            CoreConfig(ingress_mode="disabled", events_ingress_mode="authenticated"),
            enable_events=True,
            events_authenticator=_Auth(),
            events_authorizer=_Authz(),
            events_send_timeout_seconds=good_timeout,
        )
        assert app.state.events_send_timeout_seconds == float(good_timeout)
