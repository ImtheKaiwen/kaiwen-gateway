"""Offline ASGI/WebSocket tests for E2 fail-closed config, readiness, and authentication."""

from __future__ import annotations

import asyncio
import time
import uuid

import pytest
from starlette.testclient import TestClient
from starlette.websockets import WebSocketDisconnect

from kaiwen_gateway.core import (
    CoreConfig,
    CoreConfigurationError,
    ProviderPrincipal,
    TenantPrincipalBinding,
)
from kaiwen_gateway.modules.events import (
    EVENTS_TICKET_AUDIENCE,
    BoundedTicketReplayCache,
    EventAuthorizationError,
    EventsModule,
    EventsTicketClaims,
)
from kaiwen_gateway.server.app import create_gateway_app
from kaiwen_gateway.server.exceptions import ServerLifecycleError


class _StubBearerAuth:
    async def authenticate_bearer(self, *, token: str, connection_id: str) -> ProviderPrincipal:
        if token == "valid-token":
            return ProviderPrincipal(provider_id="stub-idp")
        raise EventAuthorizationError("Invalid bearer token")


class _StubAuthorizer:
    async def authorize_principal(
        self, *, principal: ProviderPrincipal, connection_id: str
    ) -> TenantPrincipalBinding:
        return TenantPrincipalBinding(
            authorized_provider_id=principal.provider_id,
            tenant_id="tenant-alpha",
            principal_id="user-1",
            allowed_scopes=frozenset({"events:subscribe", "events:subscribe:wildcard"}),
        )


class _StubTicketVerifier:
    def __init__(self, claims_by_ticket: dict[str, EventsTicketClaims]) -> None:
        self._claims = claims_by_ticket

    async def verify_ticket(
        self, *, ticket: str, origin: str, connection_id: str
    ) -> EventsTicketClaims:
        claims = self._claims.get(ticket)
        if claims is None:
            raise EventAuthorizationError("Unknown ticket")
        return claims


def _make_ticket_claims(
    *,
    jti: str | None = None,
    origin: str = "https://app.example.com",
    audience: str = EVENTS_TICKET_AUDIENCE,
    ttl_seconds: float = 20.0,
) -> EventsTicketClaims:
    return EventsTicketClaims(
        jti=jti or f"jti-{uuid.uuid4().hex[:12]}",
        tenant_id="tenant-browser",
        principal_id="browser-user-1",
        allowed_scopes=frozenset({"events:subscribe"}),
        origin=origin,
        audience=audience,
        expires_at=time.time() + ttl_seconds,
    )


def test_fail_closed_events_app_configuration() -> None:
    with pytest.raises(CoreConfigurationError):
        create_gateway_app(enable_events=True)

    with pytest.raises(CoreConfigurationError):
        create_gateway_app(CoreConfig(events_ingress_mode="authenticated"), enable_events=False)

    with pytest.raises(CoreConfigurationError):
        create_gateway_app(CoreConfig(events_ingress_mode="authenticated"), enable_events=True)

    with pytest.raises(CoreConfigurationError):
        create_gateway_app(
            CoreConfig(events_ingress_mode="authenticated"),
            enable_events=True,
            events_authenticator=_StubBearerAuth(),
        )

    with pytest.raises(CoreConfigurationError):
        create_gateway_app(
            CoreConfig(events_ingress_mode="authenticated"),
            enable_events=True,
            events_ticket_verifier=_StubTicketVerifier({}),
            events_allowed_origins=(),
        )


def test_events_only_readiness_and_failed_startup() -> None:
    app = create_gateway_app(
        CoreConfig(ingress_mode="disabled", events_ingress_mode="authenticated"),
        enable_events=True,
        events_authenticator=_StubBearerAuth(),
        events_authorizer=_StubAuthorizer(),
    )
    with TestClient(app) as client:
        ready = client.get("/ready")
        assert ready.status_code == 200
        body = ready.json()
        assert body["status"] == "ready"
        assert body["events_ingress_mode"] == "authenticated"
        assert body["ingress_mode"] == "disabled"

        # Voice endpoint remains unavailable
        with pytest.raises(WebSocketDisconnect):
            with client.websocket_connect("/ws/synthetic/v1"):
                pass

    # Failed Events startup keeps readiness unavailable
    broken_module = EventsModule()
    asyncio.run(broken_module.shutdown())
    app_broken = create_gateway_app(
        CoreConfig(ingress_mode="disabled", events_ingress_mode="authenticated"),
        enable_events=True,
        events_authenticator=_StubBearerAuth(),
        events_authorizer=_StubAuthorizer(),
        events_module=broken_module,
    )
    with pytest.raises(ServerLifecycleError):
        with TestClient(app_broken):
            pass


def test_bearer_authentication_paths() -> None:
    app = create_gateway_app(
        CoreConfig(ingress_mode="disabled", events_ingress_mode="authenticated"),
        enable_events=True,
        events_authenticator=_StubBearerAuth(),
        events_authorizer=_StubAuthorizer(),
    )
    with TestClient(app) as client:
        # Missing credentials
        with pytest.raises(WebSocketDisconnect) as exc_missing:
            with client.websocket_connect("/ws/events/v1"):
                pass
        assert exc_missing.value.code == 1008

        # Invalid bearer token
        with pytest.raises(WebSocketDisconnect) as exc_bad:
            with client.websocket_connect(
                "/ws/events/v1", headers={"Authorization": "Bearer wrong-token"}
            ):
                pass
        assert exc_bad.value.code == 1008

        # Valid bearer token
        with client.websocket_connect(
            "/ws/events/v1", headers={"Authorization": "Bearer valid-token"}
        ) as ws:
            ready = ws.receive_json()
            assert ready["type"] == "session.ready"
            assert ready["tenant_id"] == "tenant-alpha"
            assert ready["principal_id"] == "user-1"


def test_browser_ticket_authentication_origin_ttl_replay_and_query_token_rejection() -> None:
    claims_valid = _make_ticket_claims(jti="jti-once")
    claims_expired = _make_ticket_claims(jti="jti-exp", ttl_seconds=-5.0)
    claims_long_ttl = _make_ticket_claims(jti="jti-long", ttl_seconds=120.0)
    claims_wrong_aud = _make_ticket_claims(jti="jti-aud", audience="https://other.example")

    verifier = _StubTicketVerifier(
        {
            "valid": claims_valid,
            "expired": claims_expired,
            "long": claims_long_ttl,
            "wrong_aud": claims_wrong_aud,
        }
    )
    app = create_gateway_app(
        CoreConfig(ingress_mode="disabled", events_ingress_mode="authenticated"),
        enable_events=True,
        events_authenticator=_StubBearerAuth(),
        events_authorizer=_StubAuthorizer(),
        events_ticket_verifier=verifier,
        events_allowed_origins=("https://app.example.com",),
    )

    with TestClient(app) as client:
        # Query token parameters rejected even with valid bearer header
        for param in ("token", "access_token", "authorization"):
            with pytest.raises(WebSocketDisconnect) as exc_q:
                with client.websocket_connect(
                    f"/ws/events/v1?{param}=secret",
                    headers={"Authorization": "Bearer valid-token"},
                ):
                    pass
            assert exc_q.value.code == 1008

        # Disallowed origin rejected
        with pytest.raises(WebSocketDisconnect) as exc_orig:
            with client.websocket_connect(
                "/ws/events/v1?ticket=valid",
                headers={"Origin": "https://evil.example.com"},
            ):
                pass
        assert exc_orig.value.code == 1008

        # Expired ticket rejected
        with pytest.raises(WebSocketDisconnect) as exc_exp:
            with client.websocket_connect(
                "/ws/events/v1?ticket=expired",
                headers={"Origin": "https://app.example.com"},
            ):
                pass
        assert exc_exp.value.code == 1008

        # TTL > 30s rejected
        with pytest.raises(WebSocketDisconnect) as exc_ttl:
            with client.websocket_connect(
                "/ws/events/v1?ticket=long",
                headers={"Origin": "https://app.example.com"},
            ):
                pass
        assert exc_ttl.value.code == 1008

        # Wrong audience rejected
        with pytest.raises(WebSocketDisconnect) as exc_aud:
            with client.websocket_connect(
                "/ws/events/v1?ticket=wrong_aud",
                headers={"Origin": "https://app.example.com"},
            ):
                pass
        assert exc_aud.value.code == 1008

        # Valid ticket connects once
        with client.websocket_connect(
            "/ws/events/v1?ticket=valid",
            headers={"Origin": "https://app.example.com"},
        ) as ws:
            ready = ws.receive_json()
            assert ready["type"] == "session.ready"
            assert ready["tenant_id"] == "tenant-browser"
            assert ready["principal_id"] == "browser-user-1"

        # Replaying same ticket jti is rejected pre-accept
        with pytest.raises(WebSocketDisconnect) as exc_replay:
            with client.websocket_connect(
                "/ws/events/v1?ticket=valid",
                headers={"Origin": "https://app.example.com"},
            ):
                pass
        assert exc_replay.value.code == 1008


def test_concurrent_replay_cache_atomicity() -> None:
    async def scenario() -> None:
        cache = BoundedTicketReplayCache(clock=lambda: 100.0)
        results = await asyncio.gather(
            *(cache.consume("same-jti", expires_at=120.0) for _ in range(10))
        )
        assert sum(1 for r in results if r) == 1

    asyncio.run(scenario())


def test_handshake_cancellation_releases_admission_lease() -> None:
    from kaiwen_gateway.core import AdmissionController, InstanceDrainCoordinator
    from kaiwen_gateway.transports.events import events_websocket_endpoint

    class _CancellingWebSocket:
        def __init__(self, app_obj: object) -> None:
            self.app = app_obj
            self.headers = {"authorization": "Bearer valid-token"}
            self.query_params: dict[str, str] = {}
            self.client_state = None

        async def accept(self) -> None:
            raise asyncio.CancelledError()

        async def close(self, code: int = 1000, reason: str = "") -> None:
            pass

    async def scenario() -> None:
        app = create_gateway_app(
            CoreConfig(ingress_mode="disabled", events_ingress_mode="authenticated"),
            enable_events=True,
            events_authenticator=_StubBearerAuth(),
            events_authorizer=_StubAuthorizer(),
        )
        await app.state.module_lifecycle.startup()
        controller: AdmissionController = app.state.admission_controller
        coordinator: InstanceDrainCoordinator = app.state.drain_coordinator
        assert controller.snapshot().active_connections == 0

        with pytest.raises(asyncio.CancelledError):
            await events_websocket_endpoint(_CancellingWebSocket(app))  # type: ignore[arg-type]

        assert controller.snapshot().active_connections == 0
        await coordinator.drain()
        await app.state.module_lifecycle.shutdown()

    asyncio.run(scenario())
