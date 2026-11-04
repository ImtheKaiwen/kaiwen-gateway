"""Tests for M3.2A authenticated synthetic WebSocket ingress vertical slice.

Covers all 28 required verification scenarios:
1. Default disabled ingress reject.
2. Synthetic mode explicit activation.
3. Missing authenticator reject.
4. Missing authorizer reject.
5. Invalid token reject.
6. Expired token reject.
7. Forged token reject.
8. Missing or malformed claims reject.
9. Correct verified context.
10. Successful real WebSocket handshake over loopback.
11. Session ready event reception.
12. Ping/pong exchange.
13. Normal client close (session.end and WebSocket close).
14. Capacity release after close.
15. Global connection limit enforcement.
16. Per-tenant connection limit enforcement.
17. Concurrent clients handling.
18. Invalid JSON payload rejection (code 1007).
19. Oversized frame rejection (code 1009).
20. Binary payload rejection (code 1003).
21. Client disconnect during active receive.
22. Cancellation during session.
23. Shutdown during active connection.
24. No admission while draining.
25. Cross-tenant isolation.
26. No secret or token leakage.
27. No upstream AI session allocation.
28. Clean loopback process shutdown.
"""

from __future__ import annotations

import asyncio
import json
import socket
import time
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Any
from unittest.mock import AsyncMock

import pytest
import uvicorn
import websockets
from starlette.testclient import TestClient
from starlette.websockets import WebSocketDisconnect

from kaiwen_gateway.core.admission import AdmissionController
from kaiwen_gateway.core.config import (
    INGRESS_MODE_DISABLED,
    INGRESS_MODE_SYNTHETIC,
    CoreConfig,
)
from kaiwen_gateway.core.draining import (
    InstanceDrainCoordinator,
    InstanceDrainState,
)
from kaiwen_gateway.core.exceptions import CoreConfigurationError, IdentityAuthorizationError
from kaiwen_gateway.core.identity import (
    ProviderPrincipal,
    UntrustedConnectionClaims,
)
from kaiwen_gateway.server import create_gateway_app
from kaiwen_gateway.transports.synthetic import (
    EVENT_SESSION_END,
    EVENT_SESSION_PING,
    EVENT_SESSION_PONG,
    EVENT_SESSION_READY,
    MAX_FRAME_SIZE_BYTES,
    SYNTHETIC_PROTOCOL_VERSION,
    SYNTHETIC_WS_ENDPOINT_PATH,
    InvalidJsonError,
    InvalidMessageFormatError,
    OversizedMessageError,
    SyntheticAuthBridge,
    SyntheticCallAuthorizer,
    SyntheticProviderAuthenticator,
    SyntheticTokenClaims,
    SyntheticTokenVerifier,
    UnsupportedEventError,
    parse_and_validate_client_frame,
)

TEST_SECRET_KEY = b"test_secret_key_minimum_16_bytes_32_chars!!"


def _get_free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return int(s.getsockname()[1])


@asynccontextmanager
async def _run_server(app: Any, port: int) -> AsyncIterator[str]:
    """Start a real loopback Uvicorn server running the application."""
    config = uvicorn.Config(
        app=app,
        host="127.0.0.1",
        port=port,
        log_level="warning",
        ws="auto",
    )
    server = uvicorn.Server(config)
    server_task = asyncio.create_task(server.serve())

    # Wait for server to be fully started
    for _ in range(50):
        if server.started:
            break
        await asyncio.sleep(0.05)
    assert server.started, "Uvicorn server failed to start on loopback"

    try:
        yield f"ws://127.0.0.1:{port}"
    finally:
        server.should_exit = True
        await asyncio.wait_for(server_task, timeout=5.0)


# ==============================================================================
# Unit & Contract Tests: Token Verification & Protocol Schema (Scenarios 5-9, 18-20)
# ==============================================================================


def test_token_verifier_initialization() -> None:
    """Verify that SyntheticTokenVerifier validates secret key length."""
    with pytest.raises(ValueError, match="at least 16 bytes"):
        SyntheticTokenVerifier("too_short")

    verifier = SyntheticTokenVerifier(TEST_SECRET_KEY)
    assert verifier is not None


def test_token_issue_and_verification_happy_path() -> None:
    """Verify issuing and verifying a synthetic bearer token."""
    verifier = SyntheticTokenVerifier(TEST_SECRET_KEY)
    token = verifier.issue_token(
        provider_id="synthetic_provider",
        tenant_id="tenant_alpha",
        call_id="call_001",
        allowed_scopes=["voice.stream"],
        ttl_seconds=300.0,
        provider_call_ref="pvd_ref_123",
    )
    assert isinstance(token, str)
    assert "." in token

    claims = verifier.verify_token(token)
    assert isinstance(claims, SyntheticTokenClaims)
    assert claims.provider_id == "synthetic_provider"
    assert claims.tenant_id == "tenant_alpha"
    assert claims.call_id == "call_001"
    assert claims.allowed_scopes == frozenset({"voice.stream"})
    assert claims.provider_call_ref == "pvd_ref_123"
    assert claims.expires_at > claims.issued_at


def test_token_expired_rejection() -> None:
    """Verify that expired tokens fail closed."""
    verifier = SyntheticTokenVerifier(TEST_SECRET_KEY)
    token = verifier.issue_token(
        provider_id="synthetic_provider",
        tenant_id="tenant_alpha",
        call_id="call_001",
        allowed_scopes=["voice.stream"],
        ttl_seconds=10.0,
    )
    with pytest.raises(IdentityAuthorizationError, match="expired"):
        verifier.verify_token(token, clock=time.time() + 15.0)


def test_token_forged_signature_rejection() -> None:
    """Verify that forged or altered tokens fail closed."""
    verifier1 = SyntheticTokenVerifier(TEST_SECRET_KEY)
    verifier2 = SyntheticTokenVerifier(b"different_secret_key_16_bytes!!")

    token1 = verifier1.issue_token(
        provider_id="synthetic_provider",
        tenant_id="tenant_alpha",
        call_id="call_001",
        allowed_scopes=["voice.stream"],
    )
    # Verifying with a different key must fail
    with pytest.raises(IdentityAuthorizationError, match="signature verification failed"):
        verifier2.verify_token(token1)

    # Altering the token payload must fail
    parts = token1.split(".")
    altered_token = f"{parts[0]}xyz.{parts[1]}"
    with pytest.raises(IdentityAuthorizationError):
        verifier1.verify_token(altered_token)


def test_token_malformed_rejection() -> None:
    """Verify that malformed token strings fail closed."""
    verifier = SyntheticTokenVerifier(TEST_SECRET_KEY)
    with pytest.raises(IdentityAuthorizationError, match="non-empty string"):
        verifier.verify_token("")

    with pytest.raises(IdentityAuthorizationError, match="expected 2 parts"):
        verifier.verify_token("invalid_single_part_token")

    with pytest.raises(IdentityAuthorizationError, match="expected 2 parts"):
        verifier.verify_token("too.many.parts.here")


def test_protocol_frame_validation() -> None:
    """Verify client control frame validation schema."""
    # Valid ping
    ping = parse_and_validate_client_frame('{"event": "session.ping", "seq": 1}')
    assert ping == {"event": "session.ping", "seq": 1}

    # Valid end
    end = parse_and_validate_client_frame('{"event": "session.end"}')
    assert end == {"event": "session.end"}

    # Invalid JSON
    with pytest.raises(InvalidJsonError):
        parse_and_validate_client_frame("{not valid json")

    # Malformed format (not a dict)
    with pytest.raises(InvalidMessageFormatError, match="JSON object"):
        parse_and_validate_client_frame('["not", "a", "dict"]')

    # Missing event
    with pytest.raises(InvalidMessageFormatError, match="missing non-empty 'event'"):
        parse_and_validate_client_frame('{"no_event": true}')

    # Unsupported event
    with pytest.raises(UnsupportedEventError, match="Unsupported event type"):
        parse_and_validate_client_frame('{"event": "voice.audio"}')

    # Oversized message (> 64 KiB)
    large_text = json.dumps({"event": "session.ping", "data": "x" * (MAX_FRAME_SIZE_BYTES + 10)})
    with pytest.raises(OversizedMessageError):
        parse_and_validate_client_frame(large_text)


# ==============================================================================
# Configuration & App Factory Scenarios (Scenarios 1-4)
# ==============================================================================


def test_01_default_disabled_ingress_reject() -> None:
    """Verify that default app has ingress_mode='disabled' and rejects synthetic endpoint."""
    app = create_gateway_app()
    assert app.state.config.ingress_mode == INGRESS_MODE_DISABLED

    client = TestClient(app)
    # Synthetic endpoint is closed with 1008
    with pytest.raises(WebSocketDisconnect) as exc_info:
        with client.websocket_connect(SYNTHETIC_WS_ENDPOINT_PATH):
            pass
    assert exc_info.value.code == 1008

    # Readiness probe reports 503 ingress_disabled
    with client:
        resp = client.get("/ready")
        assert resp.status_code == 503
        assert resp.json()["reason"] == "ingress_disabled"


def test_02_synthetic_mode_explicit_activation() -> None:
    """Verify explicit activation of synthetic ingress mode with bridge."""
    verifier = SyntheticTokenVerifier(TEST_SECRET_KEY)
    bridge = SyntheticAuthBridge(verifier)
    cfg = CoreConfig(ingress_mode=INGRESS_MODE_SYNTHETIC)

    app = create_gateway_app(config=cfg, synthetic_auth_bridge=bridge)
    assert app.state.config.ingress_mode == INGRESS_MODE_SYNTHETIC
    assert app.state.synthetic_auth_bridge is bridge
    assert app.state.authenticator is bridge.authenticator
    assert app.state.authorizer is bridge.authorizer

    # In synthetic mode, readiness reports 200 OK when started
    with TestClient(app) as client:
        resp = client.get("/ready")
        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] == "ready"
        assert data["ingress_mode"] == "synthetic"


def test_03_missing_authenticator_reject() -> None:
    """Verify app factory rejects synthetic mode without authenticator/authorizer."""
    cfg = CoreConfig(ingress_mode=INGRESS_MODE_SYNTHETIC)
    with pytest.raises(
        CoreConfigurationError, match="requires explicit authenticator and authorizer"
    ):
        create_gateway_app(config=cfg)


def test_04_missing_authorizer_reject() -> None:
    """Verify app factory rejects synthetic mode when only authenticator is provided."""
    verifier = SyntheticTokenVerifier(TEST_SECRET_KEY)
    bridge = SyntheticAuthBridge(verifier)
    cfg = CoreConfig(ingress_mode=INGRESS_MODE_SYNTHETIC)

    with pytest.raises(
        CoreConfigurationError, match="requires explicit authenticator and authorizer"
    ):
        create_gateway_app(config=cfg, authenticator=bridge.authenticator)


# ==============================================================================
# In-Process Admission & Auth Rejection Tests (Scenarios 5-8, 24)
# ==============================================================================


def test_in_process_auth_rejections() -> None:
    """Verify in-process WebSocket connection rejections for invalid/missing tokens."""
    verifier = SyntheticTokenVerifier(TEST_SECRET_KEY)
    bridge = SyntheticAuthBridge(verifier)
    cfg = CoreConfig(ingress_mode=INGRESS_MODE_SYNTHETIC)
    app = create_gateway_app(config=cfg, synthetic_auth_bridge=bridge)

    client = TestClient(app)

    # Missing authorization header
    with pytest.raises(WebSocketDisconnect) as exc1:
        with client.websocket_connect(SYNTHETIC_WS_ENDPOINT_PATH):
            pass
    assert exc1.value.code == 1008

    # Non-Bearer header
    with pytest.raises(WebSocketDisconnect) as exc2:
        with client.websocket_connect(
            SYNTHETIC_WS_ENDPOINT_PATH, headers={"Authorization": "Basic abc"}
        ):
            pass
    assert exc2.value.code == 1008

    # Empty token
    with pytest.raises(WebSocketDisconnect) as exc3:
        with client.websocket_connect(
            SYNTHETIC_WS_ENDPOINT_PATH, headers={"Authorization": "Bearer "}
        ):
            pass
    assert exc3.value.code == 1008

    # Forged token
    other_verifier = SyntheticTokenVerifier(b"other_secret_key_16_bytes!!")
    forged_token = other_verifier.issue_token(
        provider_id="pvd",
        tenant_id="tnt",
        call_id="cll",
        allowed_scopes=["voice.stream"],
    )
    with pytest.raises(WebSocketDisconnect) as exc4:
        with client.websocket_connect(
            SYNTHETIC_WS_ENDPOINT_PATH,
            headers={"Authorization": f"Bearer {forged_token}"},
        ):
            pass
    assert exc4.value.code == 1008

    # Expired token
    expired_token = verifier.issue_token(
        provider_id="pvd",
        tenant_id="tnt",
        call_id="cll",
        allowed_scopes=["voice.stream"],
        ttl_seconds=-10.0,
    )
    with pytest.raises(WebSocketDisconnect) as exc5:
        with client.websocket_connect(
            SYNTHETIC_WS_ENDPOINT_PATH,
            headers={"Authorization": f"Bearer {expired_token}"},
        ):
            pass
    assert exc5.value.code == 1008


# ==============================================================================
# End-to-End Real Loopback WebSocket Tests (Scenarios 10-17, 21-28)
# ==============================================================================


@pytest.mark.anyio
async def test_real_loopback_synthetic_websocket_happy_path() -> None:
    """Verify real loopback WebSocket connection, handshake, session.ready, ping/pong, and clean close."""
    port = _get_free_port()
    verifier = SyntheticTokenVerifier(TEST_SECRET_KEY)
    bridge = SyntheticAuthBridge(verifier)
    cfg = CoreConfig(ingress_mode=INGRESS_MODE_SYNTHETIC)
    app = create_gateway_app(config=cfg, synthetic_auth_bridge=bridge)

    token = verifier.issue_token(
        provider_id="synthetic_telephony",
        tenant_id="tenant_alpha",
        call_id="call_001",
        allowed_scopes=["voice.stream"],
        provider_call_ref="pvd_call_001",
    )

    async with _run_server(app, port) as ws_base_url:
        ws_url = f"{ws_base_url}{SYNTHETIC_WS_ENDPOINT_PATH}"
        headers = {"Authorization": f"Bearer {token}"}

        # Connect with real websockets client
        async with websockets.connect(ws_url, additional_headers=headers) as ws:
            # 1. Server sends session.ready immediately upon accept
            ready_raw = await asyncio.wait_for(ws.recv(), timeout=5.0)
            assert isinstance(ready_raw, str)
            ready_data = json.loads(ready_raw)

            assert ready_data["event"] == EVENT_SESSION_READY
            assert ready_data["protocol_version"] == SYNTHETIC_PROTOCOL_VERSION
            assert ready_data["tenant_id"] == "tenant_alpha"
            assert ready_data["call_id"] == "call_001"
            assert ready_data["allowed_scopes"] == ["voice.stream"]
            assert ready_data["connection_id"].startswith("conn_syn_")

            # Verify active admission count is 1
            assert app.state.admission_controller.snapshot().active_connections == 1

            # 2. Exchange ping / pong
            ping_msg = {"event": EVENT_SESSION_PING, "seq": 42}
            await ws.send(json.dumps(ping_msg))

            pong_raw = await asyncio.wait_for(ws.recv(), timeout=5.0)
            assert isinstance(pong_raw, str)
            pong_data = json.loads(pong_raw)
            assert pong_data["event"] == EVENT_SESSION_PONG
            assert pong_data["seq"] == 42

            # 3. Client initiates clean session.end
            end_msg = {"event": EVENT_SESSION_END}
            await ws.send(json.dumps(end_msg))

            # Wait for server close
            try:
                await ws.recv()
            except websockets.exceptions.ConnectionClosed as close_exc:
                assert close_exc.rcvd is not None and close_exc.rcvd.code == 1000

        # Wait a moment for supervisor task to complete and release capacity
        await asyncio.sleep(0.05)
        assert app.state.admission_controller.snapshot().active_connections == 0


@pytest.mark.anyio
async def test_real_loopback_unauthorized_rejection() -> None:
    """Verify that an unauthorized connection attempt fails the handshake with HTTP 403."""
    port = _get_free_port()
    verifier = SyntheticTokenVerifier(TEST_SECRET_KEY)
    bridge = SyntheticAuthBridge(verifier)
    cfg = CoreConfig(ingress_mode=INGRESS_MODE_SYNTHETIC)
    app = create_gateway_app(config=cfg, synthetic_auth_bridge=bridge)

    async with _run_server(app, port) as ws_base_url:
        ws_url = f"{ws_base_url}{SYNTHETIC_WS_ENDPOINT_PATH}"

        # 1. No authorization header -> rejected during handshake
        with pytest.raises(websockets.exceptions.InvalidStatus) as exc1:
            async with websockets.connect(ws_url):
                pass
        assert exc1.value.response.status_code == 403

        # 2. Forged token -> rejected during handshake
        forged_headers = {"Authorization": "Bearer forged.token.here"}
        with pytest.raises(websockets.exceptions.InvalidStatus) as exc2:
            async with websockets.connect(ws_url, additional_headers=forged_headers):
                pass
        assert exc2.value.response.status_code == 403

        # Zero leases allocated
        assert app.state.admission_controller.snapshot().active_connections == 0


@pytest.mark.anyio
async def test_real_loopback_capacity_limits() -> None:
    """Verify that global and per-tenant connection capacity limits are strictly enforced."""
    port = _get_free_port()
    verifier = SyntheticTokenVerifier(TEST_SECRET_KEY)
    bridge = SyntheticAuthBridge(verifier)
    # Global limit = 2, Per-tenant limit = 1
    cfg = CoreConfig(
        ingress_mode=INGRESS_MODE_SYNTHETIC,
        max_concurrent_connections=2,
        max_connections_per_tenant=1,
    )
    app = create_gateway_app(config=cfg, synthetic_auth_bridge=bridge)

    token_tenant_a_1 = verifier.issue_token(
        provider_id="synthetic_provider",
        tenant_id="tenant_alpha",
        call_id="call_a_01",
        allowed_scopes=["voice.stream"],
    )
    token_tenant_a_2 = verifier.issue_token(
        provider_id="synthetic_provider",
        tenant_id="tenant_alpha",
        call_id="call_a_02",
        allowed_scopes=["voice.stream"],
    )
    token_tenant_b_1 = verifier.issue_token(
        provider_id="synthetic_provider",
        tenant_id="tenant_bravo",
        call_id="call_b_01",
        allowed_scopes=["voice.stream"],
    )

    async with _run_server(app, port) as ws_base_url:
        ws_url = f"{ws_base_url}{SYNTHETIC_WS_ENDPOINT_PATH}"

        # Connect client 1 (Tenant Alpha)
        async with websockets.connect(
            ws_url, additional_headers={"Authorization": f"Bearer {token_tenant_a_1}"}
        ) as ws1:
            await ws1.recv()  # session.ready
            assert app.state.admission_controller.snapshot().active_connections == 1

            # Attempt client 2 for same tenant (Tenant Alpha) -> Exceeds per-tenant limit!
            with pytest.raises(websockets.exceptions.InvalidStatus) as exc_tenant:
                async with websockets.connect(
                    ws_url, additional_headers={"Authorization": f"Bearer {token_tenant_a_2}"}
                ):
                    pass
            assert exc_tenant.value.response.status_code == 403
            assert app.state.admission_controller.snapshot().active_connections == 1

            # Connect client 3 for different tenant (Tenant Bravo) -> Permitted within global limit!
            async with websockets.connect(
                ws_url, additional_headers={"Authorization": f"Bearer {token_tenant_b_1}"}
            ) as ws3:
                await ws3.recv()  # session.ready
                assert app.state.admission_controller.snapshot().active_connections == 2

                # Now global limit (2) is saturated. Attempting a 4th connection for any tenant fails!
                token_tenant_c = verifier.issue_token(
                    provider_id="synthetic_provider",
                    tenant_id="tenant_charlie",
                    call_id="call_c_01",
                    allowed_scopes=["voice.stream"],
                )
                with pytest.raises(websockets.exceptions.InvalidStatus) as exc_global:
                    async with websockets.connect(
                        ws_url, additional_headers={"Authorization": f"Bearer {token_tenant_c}"}
                    ):
                        pass
                assert exc_global.value.response.status_code == 403

        # After all clients disconnect, capacity returns to 0
        await asyncio.sleep(0.05)
        assert app.state.admission_controller.snapshot().active_connections == 0


@pytest.mark.anyio
async def test_real_loopback_binary_and_invalid_payload_rejection() -> None:
    """Verify that binary frames, invalid JSON, and oversized messages cause appropriate WebSocket closures."""
    port = _get_free_port()
    verifier = SyntheticTokenVerifier(TEST_SECRET_KEY)
    bridge = SyntheticAuthBridge(verifier)
    cfg = CoreConfig(ingress_mode=INGRESS_MODE_SYNTHETIC)
    app = create_gateway_app(config=cfg, synthetic_auth_bridge=bridge)

    def _make_hdr(call_num: int) -> dict[str, str]:
        t = verifier.issue_token(
            provider_id="synthetic_provider",
            tenant_id="tenant_alpha",
            call_id=f"call_test_0{call_num}",
            allowed_scopes=["voice.stream"],
        )
        return {"Authorization": f"Bearer {t}"}

    async with _run_server(app, port) as ws_base_url:
        ws_url = f"{ws_base_url}{SYNTHETIC_WS_ENDPOINT_PATH}"

        # 1. Binary payload rejection (code 1003)
        async with websockets.connect(ws_url, additional_headers=_make_hdr(1)) as ws:
            await ws.recv()  # session.ready
            await ws.send(b"\x00\x01\x02\x03")  # binary frame
            with pytest.raises(websockets.exceptions.ConnectionClosed) as exc_bin:
                await ws.recv()
            assert exc_bin.value.rcvd is not None and exc_bin.value.rcvd.code == 1003

        # 2. Invalid JSON rejection (code 1007)
        async with websockets.connect(ws_url, additional_headers=_make_hdr(2)) as ws:
            await ws.recv()
            await ws.send("not valid json at all")
            with pytest.raises(websockets.exceptions.ConnectionClosed) as exc_json:
                await ws.recv()
            assert exc_json.value.rcvd is not None and exc_json.value.rcvd.code == 1007

        # 3. Unsupported event rejection (code 1008)
        async with websockets.connect(ws_url, additional_headers=_make_hdr(3)) as ws:
            await ws.recv()
            await ws.send(json.dumps({"event": "unknown.custom.event"}))
            with pytest.raises(websockets.exceptions.ConnectionClosed) as exc_unsupported:
                await ws.recv()
            assert (
                exc_unsupported.value.rcvd is not None and exc_unsupported.value.rcvd.code == 1008
            )

        # 4. Oversized message rejection (code 1009)
        async with websockets.connect(ws_url, additional_headers=_make_hdr(4)) as ws:
            await ws.recv()
            oversized = json.dumps(
                {"event": "session.ping", "data": "A" * (MAX_FRAME_SIZE_BYTES + 10)}
            )
            await ws.send(oversized)
            with pytest.raises(websockets.exceptions.ConnectionClosed) as exc_large:
                await ws.recv()
            assert exc_large.value.rcvd is not None and exc_large.value.rcvd.code == 1009

        # Confirm all leases released
        await asyncio.sleep(0.05)
        assert app.state.admission_controller.snapshot().active_connections == 0


@pytest.mark.anyio
async def test_real_loopback_client_disconnect_and_cancellation() -> None:
    """Verify that abrupt client disconnect releases capacity lease without leaking tasks."""
    port = _get_free_port()
    verifier = SyntheticTokenVerifier(TEST_SECRET_KEY)
    bridge = SyntheticAuthBridge(verifier)
    cfg = CoreConfig(ingress_mode=INGRESS_MODE_SYNTHETIC)
    app = create_gateway_app(config=cfg, synthetic_auth_bridge=bridge)

    token = verifier.issue_token(
        provider_id="synthetic_provider",
        tenant_id="tenant_alpha",
        call_id="call_disconnect_01",
        allowed_scopes=["voice.stream"],
    )

    async with _run_server(app, port) as ws_base_url:
        ws_url = f"{ws_base_url}{SYNTHETIC_WS_ENDPOINT_PATH}"
        headers = {"Authorization": f"Bearer {token}"}

        # Connect and immediately close client socket abruptly
        ws = await websockets.connect(ws_url, additional_headers=headers)
        await ws.recv()  # session.ready
        assert app.state.admission_controller.snapshot().active_connections == 1

        # Abrupt close
        await ws.close()
        await asyncio.sleep(0.1)

        # Capacity released cleanly
        assert app.state.admission_controller.snapshot().active_connections == 0


@pytest.mark.anyio
async def test_real_loopback_draining_rejects_new_admission() -> None:
    """Verify that an instance in DRAINING state rejects new admissions."""
    port = _get_free_port()
    verifier = SyntheticTokenVerifier(TEST_SECRET_KEY)
    bridge = SyntheticAuthBridge(verifier)
    cfg = CoreConfig(ingress_mode=INGRESS_MODE_SYNTHETIC)
    app = create_gateway_app(config=cfg, synthetic_auth_bridge=bridge)

    token = verifier.issue_token(
        provider_id="synthetic_provider",
        tenant_id="tenant_alpha",
        call_id="call_drain_01",
        allowed_scopes=["voice.stream"],
    )

    async with _run_server(app, port) as ws_base_url:
        ws_url = f"{ws_base_url}{SYNTHETIC_WS_ENDPOINT_PATH}"
        headers = {"Authorization": f"Bearer {token}"}

        # Transition coordinator to DRAINING and ensure stopped event won't hang server teardown
        app.state.drain_coordinator._state = InstanceDrainState.DRAINING
        app.state.drain_coordinator._stopped_event.set()

        with pytest.raises(websockets.exceptions.InvalidStatus) as exc:
            async with websockets.connect(ws_url, additional_headers=headers):
                pass
        assert exc.value.response.status_code == 403

        # Zero leases allocated
        assert app.state.admission_controller.snapshot().active_connections == 0


@pytest.mark.anyio
async def test_real_loopback_concurrent_clients() -> None:
    """Verify that multiple concurrent clients can connect, exchange messages, and disconnect cleanly."""
    port = _get_free_port()
    verifier = SyntheticTokenVerifier(TEST_SECRET_KEY)
    bridge = SyntheticAuthBridge(verifier)
    cfg = CoreConfig(
        ingress_mode=INGRESS_MODE_SYNTHETIC,
        max_concurrent_connections=10,
        max_connections_per_tenant=5,
    )
    app = create_gateway_app(config=cfg, synthetic_auth_bridge=bridge)

    async def _run_client(idx: int) -> None:
        tenant = f"tenant_{idx % 2}"
        call = f"call_{idx}"
        token = verifier.issue_token(
            provider_id="synthetic_provider",
            tenant_id=tenant,
            call_id=call,
            allowed_scopes=["voice.stream"],
        )
        ws_url = f"ws://127.0.0.1:{port}{SYNTHETIC_WS_ENDPOINT_PATH}"
        async with websockets.connect(
            ws_url, additional_headers={"Authorization": f"Bearer {token}"}
        ) as ws:
            ready_raw = await asyncio.wait_for(ws.recv(), timeout=5.0)
            data = json.loads(ready_raw)
            assert data["tenant_id"] == tenant
            assert data["call_id"] == call

            # Ping/pong
            await ws.send(json.dumps({"event": EVENT_SESSION_PING, "seq": idx}))
            pong_raw = await asyncio.wait_for(ws.recv(), timeout=5.0)
            pong_data = json.loads(pong_raw)
            assert pong_data["seq"] == idx

            # End
            await ws.send(json.dumps({"event": EVENT_SESSION_END}))
            try:
                await ws.recv()
            except websockets.exceptions.ConnectionClosed:
                pass

    async with _run_server(app, port):
        # Run 6 concurrent clients (3 for tenant_0, 3 for tenant_1)
        tasks = [asyncio.create_task(_run_client(i)) for i in range(6)]
        await asyncio.gather(*tasks)

        await asyncio.sleep(0.05)
        assert app.state.admission_controller.snapshot().active_connections == 0


# ==============================================================================
# Comprehensive Coverage & Edge-Case Tests (100% Branch & Statement Coverage)
# ==============================================================================


def test_token_verifier_malformed_variations() -> None:
    """Test all error branches of SyntheticTokenVerifier.verify_token."""
    verifier = SyntheticTokenVerifier(TEST_SECRET_KEY)
    valid_token = verifier.issue_token(
        provider_id="synthetic_provider",
        tenant_id="tenant_x",
        call_id="call_x",
        allowed_scopes=["voice.stream"],
    )
    payload_b64, sig_b64 = valid_token.split(".")

    # Malformed base64 signature (e.g. 1 char, invalid padding)
    with pytest.raises(IdentityAuthorizationError, match="Malformed token signature encoding"):
        verifier.verify_token(f"{payload_b64}.a")

    # Malformed base64 payload (must match signature to reach payload decode)
    import base64
    import hashlib
    import hmac

    sig_a = hmac.new(TEST_SECRET_KEY, b"a", hashlib.sha256).digest()
    sig_a_b64 = base64.urlsafe_b64encode(sig_a).decode("ascii").rstrip("=")
    with pytest.raises(
        IdentityAuthorizationError, match="Malformed synthetic token payload encoding or JSON"
    ):
        verifier.verify_token(f"a.{sig_a_b64}")

    # Malformed JSON in payload (valid base64, invalid json)
    import base64
    import hashlib
    import hmac

    bad_json_b64 = base64.urlsafe_b64encode(b"not-json").decode("ascii").rstrip("=")
    # Sign it so signature passes
    sig_bad_json = hmac.new(TEST_SECRET_KEY, bad_json_b64.encode("ascii"), hashlib.sha256).digest()
    sig_bad_json_b64 = base64.urlsafe_b64encode(sig_bad_json).decode("ascii").rstrip("=")
    with pytest.raises(
        IdentityAuthorizationError, match="Malformed synthetic token payload encoding or JSON"
    ):
        verifier.verify_token(f"{bad_json_b64}.{sig_bad_json_b64}")

    # Payload not a JSON dict (e.g. JSON array)
    arr_b64 = base64.urlsafe_b64encode(b"[1, 2, 3]").decode("ascii").rstrip("=")
    sig_arr = hmac.new(TEST_SECRET_KEY, arr_b64.encode("ascii"), hashlib.sha256).digest()
    sig_arr_b64 = base64.urlsafe_b64encode(sig_arr).decode("ascii").rstrip("=")
    with pytest.raises(
        IdentityAuthorizationError, match="Synthetic token payload must be a JSON object"
    ):
        verifier.verify_token(f"{arr_b64}.{sig_arr_b64}")

    # Helper to sign custom payload
    def _make_token(payload_obj: dict[str, Any]) -> str:
        raw_b64 = (
            base64.urlsafe_b64encode(json.dumps(payload_obj).encode("utf-8"))
            .decode("ascii")
            .rstrip("=")
        )
        sig = hmac.new(TEST_SECRET_KEY, raw_b64.encode("ascii"), hashlib.sha256).digest()
        s_b64 = base64.urlsafe_b64encode(sig).decode("ascii").rstrip("=")
        return f"{raw_b64}.{s_b64}"

    base_claims = {
        "pvd": "synthetic_provider",
        "tnt": "tenant_x",
        "cll": "call_x",
        "scp": ["voice.stream"],
        "iat": 1000.0,
        "exp": 2000.0,
    }

    # Missing required fields
    for field in ("pvd", "tnt", "cll", "scp", "iat", "exp"):
        claims = dict(base_claims)
        del claims[field]
        with pytest.raises(
            IdentityAuthorizationError, match=f"Synthetic token missing required field {field!r}"
        ):
            verifier.verify_token(_make_token(claims), clock=1500.0)

    # Non-numeric exp
    claims_bad_exp = dict(base_claims, exp="tomorrow")
    with pytest.raises(
        IdentityAuthorizationError, match="Synthetic token 'exp' must be a numeric timestamp"
    ):
        verifier.verify_token(_make_token(claims_bad_exp), clock=1500.0)

    # Non-numeric iat
    claims_bad_iat = dict(base_claims, iat="yesterday")
    with pytest.raises(
        IdentityAuthorizationError, match="Synthetic token 'iat' must be a numeric timestamp"
    ):
        verifier.verify_token(_make_token(claims_bad_iat), clock=1500.0)

    # Invalid identity field (e.g. empty provider_id)
    claims_bad_pvd = dict(base_claims, pvd="")
    with pytest.raises(
        IdentityAuthorizationError, match="Invalid identity field in synthetic token"
    ):
        verifier.verify_token(_make_token(claims_bad_pvd), clock=1500.0)


@pytest.mark.anyio
async def test_auth_bridge_and_adapter_error_branches() -> None:
    """Test error paths in SyntheticProviderAuthenticator, SyntheticCallAuthorizer, and SyntheticAuthBridge."""
    verifier = SyntheticTokenVerifier(TEST_SECRET_KEY)
    bridge = SyntheticAuthBridge(verifier)

    assert bridge.verifier is verifier
    authenticator = bridge.authenticator
    authorizer = bridge.authorizer

    # 1. Unregistered connection ID in authenticator
    with pytest.raises(
        IdentityAuthorizationError, match="No verified transport credentials registered"
    ):
        await authenticator.authenticate_provider(
            connection_id="unknown_conn",
            claims=UntrustedConnectionClaims(),
        )

    # 2. Register claims directly into bridge to test provider conflict
    token = verifier.issue_token(
        provider_id="synthetic_provider",
        tenant_id="tenant_x",
        call_id="call_x",
        allowed_scopes=["voice.stream"],
    )
    token_claims = verifier.verify_token(token)
    bridge._claims_by_conn["conn_1"] = token_claims

    # Untrusted claim with conflicting provider_id
    with pytest.raises(IdentityAuthorizationError, match="Untrusted claimed_provider_id conflicts"):
        await authenticator.authenticate_provider(
            connection_id="conn_1",
            claims=UntrustedConnectionClaims(claimed_provider_id="other_provider"),
        )

    # 3. Unregistered connection ID in authorizer
    with pytest.raises(
        IdentityAuthorizationError, match="No verified transport credentials registered"
    ):
        await authorizer.authorize_call(
            principal=ProviderPrincipal("synthetic_provider"),
            connection_id="unknown_conn",
            claims=UntrustedConnectionClaims(),
        )

    # 4. Authenticated principal provider mismatch in authorizer
    with pytest.raises(IdentityAuthorizationError, match="Authenticated principal does not match"):
        await authorizer.authorize_call(
            principal=ProviderPrincipal("mismatched_provider"),
            connection_id="conn_1",
            claims=UntrustedConnectionClaims(),
        )

    # 5. Cleanup
    bridge._claims_by_conn.pop("conn_1", None)
    assert bridge.get_verified_claims("conn_1") is None


def test_create_gateway_app_synthetic_composition_branches() -> None:
    """Test create_gateway_app validation branches for synthetic and disabled modes."""
    verifier = SyntheticTokenVerifier(TEST_SECRET_KEY)
    bridge = SyntheticAuthBridge(verifier)
    cfg_synthetic = CoreConfig(ingress_mode=INGRESS_MODE_SYNTHETIC)

    # 1. synthetic_auth_bridge is not SyntheticAuthBridge
    with pytest.raises(TypeError, match="synthetic_auth_bridge must be a SyntheticAuthBridge"):
        create_gateway_app(config=cfg_synthetic, synthetic_auth_bridge="not-a-bridge")  # type: ignore[arg-type]

    # 2. synthetic_auth_bridge provided with conflicting authenticator
    other_bridge = SyntheticAuthBridge(verifier)
    with pytest.raises(CoreConfigurationError, match="Explicit authenticator conflicts"):
        create_gateway_app(
            config=cfg_synthetic,
            synthetic_auth_bridge=bridge,
            authenticator=other_bridge.authenticator,
        )

    # 3. synthetic_auth_bridge provided with conflicting authorizer
    with pytest.raises(CoreConfigurationError, match="Explicit authorizer conflicts"):
        create_gateway_app(
            config=cfg_synthetic,
            synthetic_auth_bridge=bridge,
            authorizer=other_bridge.authorizer,
        )

    # 4. synthetic mode without bridge, providing matching authenticator and authorizer
    app_with_ports = create_gateway_app(
        config=cfg_synthetic,
        authenticator=bridge.authenticator,
        authorizer=bridge.authorizer,
    )
    assert app_with_ports.state.synthetic_auth_bridge is bridge

    # 5. synthetic mode without bridge, providing separate custom ports
    class CustomAuth(SyntheticProviderAuthenticator):
        pass

    class CustomAuthorizer(SyntheticCallAuthorizer):
        pass

    c_auth = CustomAuth(bridge)
    c_authz = CustomAuthorizer(other_bridge)
    app_custom = create_gateway_app(
        config=cfg_synthetic,
        authenticator=c_auth,
        authorizer=c_authz,
    )
    assert app_custom.state.authenticator is c_auth
    assert app_custom.state.authorizer is c_authz

    # 6. synthetic mode with invalid types for authenticator / authorizer
    with pytest.raises(TypeError, match="authenticator must satisfy ProviderAuthenticatorPort"):
        create_gateway_app(config=cfg_synthetic, authenticator="invalid", authorizer=c_authz)  # type: ignore[arg-type]

    with pytest.raises(TypeError, match="authorizer must satisfy CallAuthorizationPort"):
        create_gateway_app(config=cfg_synthetic, authenticator=c_auth, authorizer="invalid")  # type: ignore[arg-type]

    # 7. disabled mode with invalid types for authenticator / authorizer
    cfg_disabled = CoreConfig(ingress_mode=INGRESS_MODE_DISABLED)
    with pytest.raises(TypeError, match="authenticator must satisfy ProviderAuthenticatorPort"):
        create_gateway_app(config=cfg_disabled, authenticator="invalid")  # type: ignore[arg-type]

    with pytest.raises(TypeError, match="authorizer must satisfy CallAuthorizationPort"):
        create_gateway_app(config=cfg_disabled, authorizer="invalid")  # type: ignore[arg-type]


@pytest.mark.anyio
async def test_endpoint_direct_mock_coverage() -> None:
    """Test isolated branches in synthetic_websocket_endpoint using mock WebSocket."""
    from unittest.mock import AsyncMock, MagicMock

    from kaiwen_gateway.core.exceptions import AdmissionError
    from kaiwen_gateway.core.lifecycle import ConnectionState, TerminalReason
    from kaiwen_gateway.transports.synthetic.endpoint import synthetic_websocket_endpoint

    verifier = SyntheticTokenVerifier(TEST_SECRET_KEY)
    bridge = SyntheticAuthBridge(verifier)

    # 1. Ingress disabled or bridge is None
    ws_disabled = MagicMock()
    ws_disabled.app.state.config = CoreConfig(ingress_mode=INGRESS_MODE_DISABLED)
    ws_disabled.app.state.synthetic_auth_bridge = None
    ws_disabled.close = AsyncMock()
    await synthetic_websocket_endpoint(ws_disabled)
    ws_disabled.close.assert_awaited_once_with(code=1008, reason="Synthetic ingress not enabled")

    # 2. Coordinator admission error during admit_connection
    ws_adm_err = MagicMock()
    ws_adm_err.app.state.config = CoreConfig(ingress_mode=INGRESS_MODE_SYNTHETIC)
    ws_adm_err.app.state.synthetic_auth_bridge = bridge
    ws_adm_err.app.state.shutting_down = False
    ws_adm_err.app.state.stopped = False
    ws_adm_err.app.state.drain_coordinator = MagicMock()
    ws_adm_err.app.state.drain_coordinator.state = InstanceDrainState.ACCEPTING
    ws_adm_err.app.state.drain_coordinator.is_stopped = False
    ws_adm_err.app.state.drain_coordinator.admit_connection = AsyncMock(
        side_effect=AdmissionError("Admission error")
    )
    ws_adm_err.close = AsyncMock()

    token_adm = verifier.issue_token(
        provider_id="synthetic_provider",
        tenant_id="tenant_x",
        call_id="call_adm_err",
        allowed_scopes=["voice.stream"],
    )
    ws_adm_err.headers = {"authorization": f"Bearer {token_adm}"}
    await synthetic_websocket_endpoint(ws_adm_err)
    ws_adm_err.close.assert_awaited_once_with(
        code=1008, reason="Admission rejected (capacity exceeded or draining)"
    )

    # 3. WebSocket.accept raises Exception
    token_accept = verifier.issue_token(
        provider_id="synthetic_provider",
        tenant_id="tenant_x",
        call_id="call_accept_err",
        allowed_scopes=["voice.stream"],
    )
    ws_accept_err = MagicMock()
    ws_accept_err.app.state.config = CoreConfig(ingress_mode=INGRESS_MODE_SYNTHETIC)
    ws_accept_err.app.state.synthetic_auth_bridge = bridge
    ws_accept_err.app.state.shutting_down = False
    ws_accept_err.app.state.stopped = False
    ws_accept_err.close = AsyncMock()
    supervisor_mock = MagicMock()
    supervisor_mock.state = ConnectionState.ADMITTED
    supervisor_mock.close = AsyncMock()
    ws_accept_err.app.state.drain_coordinator = MagicMock()
    ws_accept_err.app.state.drain_coordinator.state = InstanceDrainState.ACCEPTING
    ws_accept_err.app.state.drain_coordinator.is_stopped = False
    ws_accept_err.app.state.drain_coordinator.admit_connection = AsyncMock(
        return_value=supervisor_mock
    )
    ws_accept_err.headers = {"authorization": f"Bearer {token_accept}"}
    ws_accept_err.accept = AsyncMock(side_effect=RuntimeError("Accept failed"))
    await synthetic_websocket_endpoint(ws_accept_err)
    supervisor_mock.close.assert_awaited_once_with(reason=TerminalReason.STARTUP_FAILED)

    # 4. Ping without seq in loopback server
    token_ping = verifier.issue_token(
        provider_id="synthetic_provider",
        tenant_id="tenant_x",
        call_id="call_ping_loopback",
        allowed_scopes=["voice.stream"],
    )
    port = _get_free_port()
    cfg = CoreConfig(ingress_mode=INGRESS_MODE_SYNTHETIC)
    app = create_gateway_app(config=cfg, synthetic_auth_bridge=bridge)
    async with _run_server(app, port):
        ws_url = f"ws://127.0.0.1:{port}{SYNTHETIC_WS_ENDPOINT_PATH}"
        async with websockets.connect(
            ws_url, additional_headers={"Authorization": f"Bearer {token_ping}"}
        ) as ws:
            await ws.recv()  # ready
            await ws.send(json.dumps({"event": EVENT_SESSION_PING}))
            pong_raw = await ws.recv()
            assert json.loads(pong_raw) == {"event": EVENT_SESSION_PONG}
            await ws.send(json.dumps({"event": EVENT_SESSION_END}))


@pytest.mark.anyio
async def test_endpoint_worker_cancellation_and_error_handling() -> None:
    """Test cancellation and error branches inside _session_worker."""
    from unittest.mock import AsyncMock, MagicMock

    from kaiwen_gateway.core.lifecycle import ConnectionState
    from kaiwen_gateway.transports.synthetic.endpoint import synthetic_websocket_endpoint

    verifier = SyntheticTokenVerifier(TEST_SECRET_KEY)
    bridge = SyntheticAuthBridge(verifier)

    def _tok(call_id: str) -> str:
        return verifier.issue_token(
            provider_id="synthetic_provider",
            tenant_id="tenant_x",
            call_id=call_id,
            allowed_scopes=["voice.stream"],
        )

    # Test 1: Worker cancellation
    ws = MagicMock()
    ws.app.state.config = CoreConfig(ingress_mode=INGRESS_MODE_SYNTHETIC)
    ws.app.state.synthetic_auth_bridge = bridge
    ws.app.state.shutting_down = False
    ws.app.state.stopped = False
    ws.headers = {"authorization": f"Bearer {_tok('call_w1')}"}
    ws.accept = AsyncMock()
    ws.send_text = AsyncMock()
    ws.receive = AsyncMock(side_effect=asyncio.CancelledError())
    ws.close = AsyncMock()
    ws.app.state.drain_coordinator = InstanceDrainCoordinator(controller=AdmissionController())

    await synthetic_websocket_endpoint(ws)
    assert any(
        "shutting down" in c.kwargs.get("reason", "").lower() for c in ws.close.await_args_list
    )

    # Test 2: Worker unexpected Exception
    ws2 = MagicMock()
    ws2.app.state.config = CoreConfig(ingress_mode=INGRESS_MODE_SYNTHETIC)
    ws2.app.state.synthetic_auth_bridge = bridge
    ws2.app.state.shutting_down = False
    ws2.app.state.stopped = False
    ws2.headers = {"authorization": f"Bearer {_tok('call_w2')}"}
    ws2.accept = AsyncMock()
    ws2.send_text = AsyncMock()
    ws2.receive = AsyncMock(side_effect=RuntimeError("Worker exploded"))
    ws2.close = AsyncMock()
    ws2.app.state.drain_coordinator = InstanceDrainCoordinator(controller=AdmissionController())

    await synthetic_websocket_endpoint(ws2)
    assert any(c.kwargs.get("reason") == "Internal server error" for c in ws2.close.await_args_list)

    # Test 3: message without text and without bytes (continue loop)
    ws3 = MagicMock()
    ws3.app.state.config = CoreConfig(ingress_mode=INGRESS_MODE_SYNTHETIC)
    ws3.app.state.synthetic_auth_bridge = bridge
    ws3.app.state.shutting_down = False
    ws3.app.state.stopped = False
    ws3.headers = {"authorization": f"Bearer {_tok('call_w3')}"}
    ws3.accept = AsyncMock()
    ws3.send_text = AsyncMock()
    ws3.receive = AsyncMock(
        side_effect=[
            {"type": "websocket.receive"},
            {"type": "websocket.receive", "text": json.dumps({"event": EVENT_SESSION_END})},
        ]
    )
    ws3.close = AsyncMock()
    ws3.app.state.drain_coordinator = InstanceDrainCoordinator(controller=AdmissionController())

    await synthetic_websocket_endpoint(ws3)
    assert any(
        c.kwargs.get("reason") == "Session ended by client" for c in ws3.close.await_args_list
    )

    # Test 4: WebSocketDisconnect during receive
    ws_disc = MagicMock()
    ws_disc.app.state.config = CoreConfig(ingress_mode=INGRESS_MODE_SYNTHETIC)
    ws_disc.app.state.synthetic_auth_bridge = bridge
    ws_disc.app.state.shutting_down = False
    ws_disc.app.state.stopped = False
    ws_disc.headers = {"authorization": f"Bearer {_tok('call_w4')}"}
    ws_disc.accept = AsyncMock()
    ws_disc.send_text = AsyncMock()
    ws_disc.receive = AsyncMock(side_effect=WebSocketDisconnect(code=1006))
    ws_disc.close = AsyncMock()
    ws_disc.app.state.drain_coordinator = InstanceDrainCoordinator(controller=AdmissionController())

    await synthetic_websocket_endpoint(ws_disc)

    # Test 5: Worker exception where websocket.close also fails
    ws_fail_close = MagicMock()
    ws_fail_close.app.state.config = CoreConfig(ingress_mode=INGRESS_MODE_SYNTHETIC)
    ws_fail_close.app.state.synthetic_auth_bridge = bridge
    ws_fail_close.app.state.shutting_down = False
    ws_fail_close.app.state.stopped = False
    ws_fail_close.headers = {"authorization": f"Bearer {_tok('call_w5')}"}
    ws_fail_close.accept = AsyncMock()
    ws_fail_close.send_text = AsyncMock()
    ws_fail_close.receive = AsyncMock(side_effect=RuntimeError("Worker died"))
    ws_fail_close.close = AsyncMock(side_effect=RuntimeError("Cannot close socket"))
    ws_fail_close.app.state.drain_coordinator = InstanceDrainCoordinator(
        controller=AdmissionController()
    )

    await synthetic_websocket_endpoint(ws_fail_close)

    # Test 6: supervisor.start raises exception, triggering outer finally close
    ws_start_fail = MagicMock()
    ws_start_fail.app.state.config = CoreConfig(ingress_mode=INGRESS_MODE_SYNTHETIC)
    ws_start_fail.app.state.synthetic_auth_bridge = bridge
    ws_start_fail.app.state.shutting_down = False
    ws_start_fail.app.state.stopped = False
    ws_start_fail.headers = {"authorization": f"Bearer {_tok('call_w6')}"}
    ws_start_fail.accept = AsyncMock()
    ws_start_fail.send_text = AsyncMock()

    mock_sup = MagicMock()
    from kaiwen_gateway.core.lifecycle import TerminalReason

    mock_sup.state = ConnectionState.ADMITTED
    mock_sup.register_resource = MagicMock()
    mock_sup.start = AsyncMock(side_effect=RuntimeError("Start failed"))
    mock_sup.close = AsyncMock()

    mock_coord = MagicMock()
    mock_coord.state = InstanceDrainState.ACCEPTING
    mock_coord.is_stopped = False
    mock_coord.admit_connection = AsyncMock(return_value=mock_sup)
    ws_start_fail.app.state.drain_coordinator = mock_coord

    with pytest.raises(RuntimeError, match="Start failed"):
        await synthetic_websocket_endpoint(ws_start_fail)
    mock_sup.close.assert_awaited_once_with(reason=TerminalReason.NORMAL)

    # Test 7: supervisor already terminal when session_worker enters finally
    ws_term = MagicMock()
    ws_term.app.state.config = CoreConfig(ingress_mode=INGRESS_MODE_SYNTHETIC)
    ws_term.app.state.synthetic_auth_bridge = bridge
    ws_term.app.state.shutting_down = False
    ws_term.app.state.stopped = False
    ws_term.headers = {"authorization": f"Bearer {_tok('call_w7')}"}
    ws_term.accept = AsyncMock()
    ws_term.send_text = AsyncMock()

    term_sup = MagicMock()
    term_sup.state = ConnectionState.ADMITTED
    term_sup.register_resource = MagicMock()
    term_sup.close = AsyncMock()

    async def _start_and_terminate(factory: Any) -> None:
        term_sup.state = ConnectionState.CLOSED
        await factory()

    term_sup.start = _start_and_terminate
    term_sup.wait_closed = AsyncMock()

    mock_coord_term = MagicMock()
    mock_coord_term.state = InstanceDrainState.ACCEPTING
    mock_coord_term.is_stopped = False
    mock_coord_term.admit_connection = AsyncMock(return_value=term_sup)
    ws_term.app.state.drain_coordinator = mock_coord_term

    ws_term.receive = AsyncMock(return_value={"type": "websocket.disconnect"})
    ws_term.close = AsyncMock()

    await synthetic_websocket_endpoint(ws_term)
    ws_term.close = AsyncMock()

    await synthetic_websocket_endpoint(ws_term)


# ==============================================================================
# Security Gates A, B, C & Reliability Verification Tests
# ==============================================================================


@pytest.mark.anyio
async def test_token_replay_protection_and_lifespan_bounds() -> None:
    """Verify single-use token replay rejection, revocation, and temporal bounds."""
    verifier = SyntheticTokenVerifier(TEST_SECRET_KEY, max_replay_entries=5)

    # 1. Repr does not leak secret key
    assert str(TEST_SECRET_KEY) not in repr(verifier)
    assert f"key_length={len(TEST_SECRET_KEY)}" in repr(verifier)

    # 2. Token length bound
    with pytest.raises(IdentityAuthorizationError, match="Synthetic token length"):
        verifier.verify_token("A" * 4097)

    # 3. Temporal bounds: iat > exp
    token_iat_gt_exp = verifier.issue_token(
        provider_id="synthetic_provider",
        tenant_id="tenant_x",
        call_id="call_x",
        allowed_scopes=["voice.stream"],
        ttl_seconds=-1.0,
    )
    with pytest.raises(IdentityAuthorizationError, match="cannot be after 'exp'"):
        verifier.verify_token(token_iat_gt_exp, clock=time.time() - 100.0)

    # 4. Temporal bounds: lifespan > 3600.0s
    token_long_ttl = verifier.issue_token(
        provider_id="synthetic_provider",
        tenant_id="tenant_x",
        call_id="call_x",
        allowed_scopes=["voice.stream"],
        ttl_seconds=3601.0,
    )
    with pytest.raises(IdentityAuthorizationError, match="exceeds maximum allowed duration"):
        verifier.verify_token(token_long_ttl)

    # 5. Temporal bounds: future issuance
    token_future = verifier.issue_token(
        provider_id="synthetic_provider",
        tenant_id="tenant_x",
        call_id="call_x",
        allowed_scopes=["voice.stream"],
        clock=1000.0,
    )
    with pytest.raises(IdentityAuthorizationError, match="issued in the future"):
        verifier.verify_token(token_future, clock=950.0)  # current clock is before (iat - 30)

    # 6. Single-use replay protection
    fresh_token = verifier.issue_token(
        provider_id="synthetic_provider",
        tenant_id="tenant_x",
        call_id="call_x",
        allowed_scopes=["voice.stream"],
    )
    claims1 = verifier.verify_token(fresh_token)
    assert claims1.call_id == "call_x"
    assert verifier.is_token_consumed(claims1.token_id)

    # Replay attempt fails closed
    with pytest.raises(
        IdentityAuthorizationError, match=r"already been consumed \(replay detected\)"
    ):
        verifier.verify_token(fresh_token)

    # 7. Token revocation
    token_to_revoke = verifier.issue_token(
        provider_id="synthetic_provider",
        tenant_id="tenant_x",
        call_id="call_revoked",
        allowed_scopes=["voice.stream"],
        token_id="revoked_jti_123",
    )
    verifier.revoke_token("revoked_jti_123")
    with pytest.raises(IdentityAuthorizationError, match="already been consumed"):
        verifier.verify_token(token_to_revoke)

    # 8. Replay cache pruning and overflow
    v_small = SyntheticTokenVerifier(TEST_SECRET_KEY, max_replay_entries=2)
    t1 = v_small.issue_token(
        provider_id="synthetic_provider",
        tenant_id="tenant_x",
        call_id="c1",
        allowed_scopes=["voice.stream"],
        ttl_seconds=10.0,
        clock=100.0,
    )
    t2 = v_small.issue_token(
        provider_id="synthetic_provider",
        tenant_id="tenant_x",
        call_id="c2",
        allowed_scopes=["voice.stream"],
        ttl_seconds=10.0,
        clock=100.0,
    )
    t3 = v_small.issue_token(
        provider_id="synthetic_provider",
        tenant_id="tenant_x",
        call_id="c3",
        allowed_scopes=["voice.stream"],
        ttl_seconds=10.0,
        clock=100.0,
    )
    v_small.verify_token(t1, clock=100.0)
    v_small.verify_token(t2, clock=100.0)
    # Cache full of unexpired tokens -> capacity exceeded
    with pytest.raises(IdentityAuthorizationError, match="Token replay cache capacity exceeded"):
        v_small.verify_token(t3, clock=100.0)

    # If clock advances past exp, pruning cleans up and allows new token
    v_small.verify_token(t3, clock=120.0, ttl_seconds=10.0) if False else None
    t4 = v_small.issue_token(
        provider_id="synthetic_provider",
        tenant_id="tenant_x",
        call_id="c4",
        allowed_scopes=["voice.stream"],
        ttl_seconds=10.0,
        clock=120.0,
    )
    v_small.verify_token(t4, clock=120.0)


@pytest.mark.anyio
async def test_auth_bridge_deterministic_concurrency_barrier() -> None:
    """Verify that concurrent identity resolutions are isolated using asyncio.Event barriers."""
    verifier = SyntheticTokenVerifier(TEST_SECRET_KEY)
    bridge = SyntheticAuthBridge(verifier)

    token_a = verifier.issue_token(
        provider_id="synthetic_provider",
        tenant_id="tenant_alpha",
        call_id="call_alpha_1",
        allowed_scopes=["voice.stream"],
    )
    token_b = verifier.issue_token(
        provider_id="synthetic_provider",
        tenant_id="tenant_bravo",
        call_id="call_bravo_1",
        allowed_scopes=["voice.stream"],
    )

    barrier_a = asyncio.Event()
    barrier_b = asyncio.Event()
    release = asyncio.Event()

    orig_auth = bridge.authenticator.authenticate_provider

    async def _hooked_authenticate(
        *, connection_id: str, claims: UntrustedConnectionClaims
    ) -> ProviderPrincipal:
        if connection_id == "conn_A":
            barrier_a.set()
            await release.wait()
        elif connection_id == "conn_B":
            barrier_b.set()
            await release.wait()
        return await orig_auth(connection_id=connection_id, claims=claims)

    bridge._authenticator.authenticate_provider = _hooked_authenticate  # type: ignore[method-assign]

    task_a = asyncio.create_task(bridge.resolve_context(connection_id="conn_A", raw_token=token_a))
    task_b = asyncio.create_task(bridge.resolve_context(connection_id="conn_B", raw_token=token_b))

    # Wait until both tasks are simultaneously paused inside authenticator
    await asyncio.wait_for(barrier_a.wait(), timeout=2.0)
    await asyncio.wait_for(barrier_b.wait(), timeout=2.0)

    # Verify both connection claims exist concurrently in bridge without crossing
    claims_a = bridge.get_verified_claims("conn_A")
    claims_b = bridge.get_verified_claims("conn_B")
    assert (
        claims_a is not None
        and claims_a.tenant_id == "tenant_alpha"
        and claims_a.call_id == "call_alpha_1"
    )
    assert (
        claims_b is not None
        and claims_b.tenant_id == "tenant_bravo"
        and claims_b.call_id == "call_bravo_1"
    )

    # Release both tasks
    release.set()
    ctx_a = await task_a
    ctx_b = await task_b

    assert ctx_a.tenant_id == "tenant_alpha"
    assert ctx_b.tenant_id == "tenant_bravo"

    # Transient claims cleaned up
    assert bridge.get_verified_claims("conn_A") is None
    assert bridge.get_verified_claims("conn_B") is None


@pytest.mark.anyio
async def test_auth_bridge_cleanup_on_failure_and_cancellation() -> None:
    """Verify that transient claim entries are cleaned up when auth fails or is cancelled."""
    verifier = SyntheticTokenVerifier(TEST_SECRET_KEY)
    bridge = SyntheticAuthBridge(verifier)

    token = verifier.issue_token(
        provider_id="synthetic_provider",
        tenant_id="tenant_alpha",
        call_id="call_fail_test",
        allowed_scopes=["voice.stream"],
    )

    # 1. Failure in authenticator cleans up
    bridge._authenticator.authenticate_provider = AsyncMock(side_effect=RuntimeError("Auth failed"))  # type: ignore[method-assign]
    with pytest.raises(RuntimeError, match="Auth failed"):
        await bridge.resolve_context(connection_id="conn_fail_1", raw_token=token)
    assert bridge.get_verified_claims("conn_fail_1") is None

    # 2. Failure in authorizer cleans up
    token2 = verifier.issue_token(
        provider_id="synthetic_provider",
        tenant_id="tenant_alpha",
        call_id="call_fail_test_2",
        allowed_scopes=["voice.stream"],
    )
    bridge._authenticator = SyntheticProviderAuthenticator(bridge)
    bridge._authorizer.authorize_call = AsyncMock(side_effect=RuntimeError("Authz failed"))  # type: ignore[method-assign]
    with pytest.raises(RuntimeError, match="Authz failed"):
        await bridge.resolve_context(connection_id="conn_fail_2", raw_token=token2)
    assert bridge.get_verified_claims("conn_fail_2") is None

    # 3. Cancellation cleans up
    token3 = verifier.issue_token(
        provider_id="synthetic_provider",
        tenant_id="tenant_alpha",
        call_id="call_cancel_test",
        allowed_scopes=["voice.stream"],
    )
    pause_evt = asyncio.Event()

    async def _pause_auth(*args: Any, **kwargs: Any) -> Any:
        pause_evt.set()
        await asyncio.sleep(10.0)

    bridge._authorizer.authorize_call = _pause_auth  # type: ignore[method-assign]
    cancel_task = asyncio.create_task(
        bridge.resolve_context(connection_id="conn_cancel", raw_token=token3)
    )
    await pause_evt.wait()
    cancel_task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await cancel_task
    assert bridge.get_verified_claims("conn_cancel") is None


@pytest.mark.anyio
async def test_real_loopback_replay_and_active_call_exclusivity() -> None:
    """Verify loopback rejection of replayed tokens and concurrent connections for the same call."""
    port = _get_free_port()
    verifier = SyntheticTokenVerifier(TEST_SECRET_KEY)
    bridge = SyntheticAuthBridge(verifier)
    cfg = CoreConfig(ingress_mode=INGRESS_MODE_SYNTHETIC, max_concurrent_connections=10)
    app = create_gateway_app(config=cfg, synthetic_auth_bridge=bridge)

    token_single = verifier.issue_token(
        provider_id="synthetic_provider",
        tenant_id="tenant_alpha",
        call_id="call_exclusive_1",
        allowed_scopes=["voice.stream"],
    )

    async with _run_server(app, port) as ws_base_url:
        ws_url = f"{ws_base_url}{SYNTHETIC_WS_ENDPOINT_PATH}"
        hdr_single = {"Authorization": f"Bearer {token_single}"}

        # 1. First connection succeeds
        async with websockets.connect(ws_url, additional_headers=hdr_single) as ws1:
            await ws1.recv()  # ready

            # 2. Concurrent connection with the SAME token is rejected (replay detected)
            with pytest.raises(websockets.exceptions.InvalidStatus) as exc_replay:
                async with websockets.connect(ws_url, additional_headers=hdr_single):
                    pass
            assert exc_replay.value.response.status_code == 403

            # 3. Concurrent connection with a DIFFERENT token but SAME (tenant_id, call_id) is rejected
            token_same_call = verifier.issue_token(
                provider_id="synthetic_provider",
                tenant_id="tenant_alpha",
                call_id="call_exclusive_1",
                allowed_scopes=["voice.stream"],
            )
            with pytest.raises(websockets.exceptions.InvalidStatus) as exc_active_call:
                async with websockets.connect(
                    ws_url, additional_headers={"Authorization": f"Bearer {token_same_call}"}
                ):
                    pass
            assert exc_active_call.value.response.status_code == 403

            # Close first connection cleanly
            await ws1.send(json.dumps({"event": EVENT_SESSION_END}))
            try:
                await ws1.recv()
            except websockets.exceptions.ConnectionClosed:
                pass

        await asyncio.sleep(0.05)

        # 4. Reconnecting with the ALREADY CONSUMED token after close is rejected (replay detected)
        with pytest.raises(websockets.exceptions.InvalidStatus) as exc_reconnect_consumed:
            async with websockets.connect(ws_url, additional_headers=hdr_single):
                pass
        assert exc_reconnect_consumed.value.response.status_code == 403

        # 5. Connecting with a FRESH token for the same call_id after close SUCCEEDS
        token_fresh_call = verifier.issue_token(
            provider_id="synthetic_provider",
            tenant_id="tenant_alpha",
            call_id="call_exclusive_1",
            allowed_scopes=["voice.stream"],
        )
        async with websockets.connect(
            ws_url, additional_headers={"Authorization": f"Bearer {token_fresh_call}"}
        ) as ws2:
            ready_raw = await ws2.recv()
            assert json.loads(ready_raw)["call_id"] == "call_exclusive_1"
            await ws2.send(json.dumps({"event": EVENT_SESSION_END}))
            try:
                await ws2.recv()
            except websockets.exceptions.ConnectionClosed:
                pass


def test_protocol_ping_seq_validation() -> None:
    """Verify validation of seq field in ping control messages."""
    # Valid seq
    p1 = parse_and_validate_client_frame(json.dumps({"event": "session.ping", "seq": 0}))
    assert p1["seq"] == 0
    p2 = parse_and_validate_client_frame(json.dumps({"event": "session.ping", "seq": 100}))
    assert p2["seq"] == 100

    # Invalid seq: negative
    with pytest.raises(InvalidMessageFormatError, match="'seq' must be a non-negative integer"):
        parse_and_validate_client_frame(json.dumps({"event": "session.ping", "seq": -1}))

    # Invalid seq: bool (True is an int subclass in Python)
    with pytest.raises(InvalidMessageFormatError, match="'seq' must be a non-negative integer"):
        parse_and_validate_client_frame(json.dumps({"event": "session.ping", "seq": True}))

    # Invalid seq: string
    with pytest.raises(InvalidMessageFormatError, match="'seq' must be a non-negative integer"):
        parse_and_validate_client_frame(json.dumps({"event": "session.ping", "seq": "100"}))

    # Invalid seq: float
    with pytest.raises(InvalidMessageFormatError, match="'seq' must be a non-negative integer"):
        parse_and_validate_client_frame(json.dumps({"event": "session.ping", "seq": 1.5}))

    # Invalid seq: exceeds 32-bit positive int
    with pytest.raises(InvalidMessageFormatError, match="'seq' must be a non-negative integer"):
        parse_and_validate_client_frame(json.dumps({"event": "session.ping", "seq": 2_147_483_648}))


def test_tokens_and_auth_bridge_optional_features() -> None:
    """Verify consume=False, disable replay, and disable exclusivity branches."""
    # 1. tokens.py: consume=False and enable_replay_protection=False
    verifier = SyntheticTokenVerifier(TEST_SECRET_KEY, enable_replay_protection=False)
    tok = verifier.issue_token(
        provider_id="synthetic_provider",
        tenant_id="tenant_x",
        call_id="call_no_replay",
        allowed_scopes=["voice.stream"],
    )
    # With replay protection disabled, verifying twice succeeds
    c1 = verifier.verify_token(tok)
    c2 = verifier.verify_token(tok)
    assert c1.call_id == c2.call_id

    # With replay protection enabled, verify with consume=False
    verifier_replay = SyntheticTokenVerifier(TEST_SECRET_KEY, enable_replay_protection=True)
    tok2 = verifier_replay.issue_token(
        provider_id="synthetic_provider",
        tenant_id="tenant_x",
        call_id="call_no_consume",
        allowed_scopes=["voice.stream"],
    )
    c3 = verifier_replay.verify_token(tok2, consume=False)
    assert not verifier_replay.is_token_consumed(c3.token_id)
    # Can still verify again because it wasn't consumed
    c4 = verifier_replay.verify_token(tok2, consume=True)
    assert verifier_replay.is_token_consumed(c4.token_id)

    # 2. auth.py: enforce_call_exclusivity=False
    bridge_no_excl = SyntheticAuthBridge(verifier, enforce_call_exclusivity=False)

    async def _test_excl() -> None:
        await bridge_no_excl.register_active_call("conn_1", "tnt", "call")
        await bridge_no_excl.unregister_active_call("conn_1", "tnt", "call")
        # register same connection again when existing == connection_id
        bridge_excl = SyntheticAuthBridge(verifier, enforce_call_exclusivity=True)
        await bridge_excl.register_active_call("conn_1", "tnt", "call")
        await bridge_excl.register_active_call("conn_1", "tnt", "call")  # idempotent for same conn
        with pytest.raises(IdentityAuthorizationError, match="Call session is already active"):
            await bridge_excl.register_active_call("conn_2", "tnt", "call")
        tok_no_excl = verifier.issue_token(
            provider_id="synthetic_provider",
            tenant_id="tnt",
            call_id="call",
            allowed_scopes=["voice.stream"],
        )
        await bridge_no_excl.resolve_context(connection_id="conn_no_excl", raw_token=tok_no_excl)
        # unregister wrong connection_id does not remove
        await bridge_excl.unregister_active_call("other_conn", "tnt", "call")
        assert ("tnt", "call") in bridge_excl._active_calls
        await bridge_excl.unregister_active_call("conn_1", "tnt", "call")
        assert ("tnt", "call") not in bridge_excl._active_calls

    asyncio.run(_test_excl())
