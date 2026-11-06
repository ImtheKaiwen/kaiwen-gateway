"""Comprehensive verification suite for M4.2A synthetic binary WebSocket media slice.

Covers:
- Gate A: Synthetic media wire contract, framing, constants, existing endpoint regression.
- Gate B: Authentication, HMAC verification, replay protection, voice.stream scope, admission.
- Gate C: Telephony media adapter, single read-loop demux, serialized outbound writes.
- Gate D: Bounded Fake AI echo port, backpressure support, zero unbounded memory.
- Gate E: Real loopback Uvicorn server and WebSocket binary audio round-trip.
- Gate F: Security and negative fault matrix (tampered token, expired, replay, bad frame size, etc.).
- Gate G: Unified lifecycle ownership, complete capacity recovery, zero leaked leases/calls.
"""

from __future__ import annotations

import asyncio
import json
import socket
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Any, Final

import pytest
import uvicorn
import websockets
from starlette.testclient import TestClient
from starlette.websockets import WebSocketDisconnect

from kaiwen_gateway.core.admission import AdmissionController
from kaiwen_gateway.core.config import (
    INGRESS_MODE_SYNTHETIC,
    CoreConfig,
)
from kaiwen_gateway.core.draining import InstanceDrainCoordinator
from kaiwen_gateway.core.identity import (
    VerifiedConnectionContext,
)
from kaiwen_gateway.modules.voice.adapters.fake_ai import BoundedFakeAIEchoPort
from kaiwen_gateway.modules.voice.domain import (
    AudioFormat,
    AudioFrame,
    MediaBackpressureOverflowError,
    MediaPortClosedError,
    VoiceDomainError,
)
from kaiwen_gateway.server import create_gateway_app
from kaiwen_gateway.transports.synthetic import (
    EVENT_CALL_END,
    EVENT_CALL_ENDED,
    EVENT_CALL_START,
    EVENT_CALL_STARTED,
    EVENT_SESSION_END,
    EVENT_SESSION_PING,
    EVENT_SESSION_PONG,
    EVENT_SESSION_READY,
    PCM16LE_CHANNELS,
    PCM16LE_FRAME_BYTES,
    PCM16LE_FRAME_MS,
    PCM16LE_SAMPLE_RATE,
    SYNTHETIC_MEDIA_PROTOCOL_VERSION,
    SYNTHETIC_MEDIA_WS_ENDPOINT_PATH,
    SYNTHETIC_WS_ENDPOINT_PATH,
    InvalidAudioFrameSizeError,
    SyntheticAuthBridge,
    SyntheticTokenVerifier,
    WebSocketTelephonyMediaAdapter,
    validate_pcm16le_binary_frame,
)

TEST_SECRET_KEY: Final[bytes] = b"synthetic_media_test_secret_32bytes!"


def _get_free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return int(s.getsockname()[1])


@asynccontextmanager
async def _run_test_server(app: Any, port: int) -> AsyncIterator[str]:
    """Start a real loopback Uvicorn server for network integration tests."""
    config = uvicorn.Config(
        app=app,
        host="127.0.0.1",
        port=port,
        log_level="warning",
        ws="auto",
    )
    server = uvicorn.Server(config)
    server_task = asyncio.create_task(server.serve())

    for _ in range(100):
        if server.started:
            break
        await asyncio.sleep(0.02)
    assert server.started, "Uvicorn server failed to start on loopback"

    try:
        yield f"ws://127.0.0.1:{port}"
    finally:
        server.should_exit = True
        try:
            await asyncio.wait_for(server_task, timeout=5.0)
        except (TimeoutError, asyncio.CancelledError):
            pass


def _make_verified_context(
    connection_id: str = "conn_test_01",
    tenant_id: str = "tenant_test",
    call_id: str = "call_test",
    scopes: tuple[str, ...] = ("voice.stream",),
) -> VerifiedConnectionContext:
    return VerifiedConnectionContext(
        provider_id="smoke_provider",
        tenant_id=tenant_id,
        connection_id=connection_id,
        call_id=call_id,
        allowed_scopes=frozenset(scopes),
    )


# =============================================================================
# GATE A: Wire Contract, Protocol Constants, and Validators
# =============================================================================


def test_pcm16le_wire_constants() -> None:
    """Verify standard PCM16LE 16kHz mono 20ms framing constants."""
    assert PCM16LE_SAMPLE_RATE == 16_000
    assert PCM16LE_CHANNELS == 1
    assert PCM16LE_FRAME_MS == 20
    assert PCM16LE_FRAME_BYTES == 640  # 16000 * 1 * 2 * 0.020
    assert SYNTHETIC_MEDIA_PROTOCOL_VERSION == "v1"
    assert SYNTHETIC_MEDIA_WS_ENDPOINT_PATH == "/ws/synthetic-media/v1"


def test_validate_pcm16le_binary_frame() -> None:
    """Verify strict validation of PCM16LE binary payloads."""
    # Valid 640-byte frame passes without error
    valid_payload = b"\x00" * 640
    validate_pcm16le_binary_frame(valid_payload)

    # Undersized frame raises InvalidAudioFrameSizeError
    with pytest.raises(InvalidAudioFrameSizeError) as exc_under:
        validate_pcm16le_binary_frame(b"\x00" * 639)
    assert exc_under.value.actual_size == 639
    assert exc_under.value.expected_size == 640

    # Oversized frame raises InvalidAudioFrameSizeError
    with pytest.raises(InvalidAudioFrameSizeError) as exc_over:
        validate_pcm16le_binary_frame(b"\x00" * 641)
    assert exc_over.value.actual_size == 641

    # Non-bytes raises TypeError
    with pytest.raises(TypeError):
        validate_pcm16le_binary_frame("not_bytes")  # type: ignore[arg-type]


def test_synthetic_media_endpoint_disabled_by_default() -> None:
    """Verify /ws/synthetic-media/v1 is disabled by default and rejects connections."""
    verifier = SyntheticTokenVerifier(secret_key=TEST_SECRET_KEY)
    auth_bridge = SyntheticAuthBridge(verifier=verifier)
    app = create_gateway_app(
        config=CoreConfig(ingress_mode=INGRESS_MODE_SYNTHETIC),
        synthetic_auth_bridge=auth_bridge,
        enable_synthetic_media=False,  # default
    )
    client = TestClient(app)

    token = verifier.issue_token(
        provider_id="smoke_provider",
        tenant_id="tenant_01",
        call_id="call_01",
        allowed_scopes=["voice.stream"],
    )

    # Route is not present, falls through to catch-all reject (WebSocketDisconnect 1008)
    with pytest.raises(WebSocketDisconnect) as exc:
        with client.websocket_connect(
            SYNTHETIC_MEDIA_WS_ENDPOINT_PATH,
            headers={"Authorization": f"Bearer {token}"},
        ):
            pass
    assert exc.value.code == 1008


def test_create_gateway_app_synthetic_media_parameter_validation() -> None:
    """Verify create_gateway_app type checks for enable_synthetic_media and ai_media_port_factory."""
    with pytest.raises(TypeError, match="enable_synthetic_media must be a bool"):
        create_gateway_app(enable_synthetic_media="invalid")  # type: ignore[arg-type]

    with pytest.raises(TypeError, match="ai_media_port_factory must be callable"):
        create_gateway_app(ai_media_port_factory="not_callable")  # type: ignore[arg-type]


def test_existing_synthetic_text_endpoint_still_rejects_binary() -> None:
    """Verify existing text-only /ws/synthetic/v1 is completely unaffected and still rejects binary."""
    verifier = SyntheticTokenVerifier(secret_key=TEST_SECRET_KEY)
    auth_bridge = SyntheticAuthBridge(verifier=verifier)
    app = create_gateway_app(
        config=CoreConfig(ingress_mode=INGRESS_MODE_SYNTHETIC),
        synthetic_auth_bridge=auth_bridge,
        enable_synthetic_media=True,
    )
    client = TestClient(app)

    token = verifier.issue_token(
        provider_id="smoke_provider",
        tenant_id="tenant_01",
        call_id="call_01",
        allowed_scopes=["voice.stream"],
    )

    with client.websocket_connect(
        SYNTHETIC_WS_ENDPOINT_PATH,
        headers={"Authorization": f"Bearer {token}"},
    ) as ws:
        ready = ws.receive_json()
        assert ready["event"] == EVENT_SESSION_READY

        # Send binary data to the text endpoint -> must be closed with 1003
        ws.send_bytes(b"\x00" * 640)
        with pytest.raises(WebSocketDisconnect) as exc:
            ws.receive_json()
        assert exc.value.code == 1003


# =============================================================================
# GATE B: Authentication, Scope, and Admission Pre-Requisites
# =============================================================================


def test_auth_missing_header_rejected() -> None:
    """Verify connection without Authorization header is rejected with 1008."""
    verifier = SyntheticTokenVerifier(secret_key=TEST_SECRET_KEY)
    auth_bridge = SyntheticAuthBridge(verifier=verifier)
    app = create_gateway_app(
        config=CoreConfig(ingress_mode=INGRESS_MODE_SYNTHETIC),
        synthetic_auth_bridge=auth_bridge,
        enable_synthetic_media=True,
    )
    client = TestClient(app)

    with pytest.raises(WebSocketDisconnect) as exc:
        with client.websocket_connect(SYNTHETIC_MEDIA_WS_ENDPOINT_PATH):
            pass
    assert exc.value.code == 1008


def test_auth_invalid_bearer_scheme_rejected() -> None:
    """Verify connection with non-Bearer Authorization header is rejected with 1008."""
    verifier = SyntheticTokenVerifier(secret_key=TEST_SECRET_KEY)
    auth_bridge = SyntheticAuthBridge(verifier=verifier)
    app = create_gateway_app(
        config=CoreConfig(ingress_mode=INGRESS_MODE_SYNTHETIC),
        synthetic_auth_bridge=auth_bridge,
        enable_synthetic_media=True,
    )
    client = TestClient(app)

    with pytest.raises(WebSocketDisconnect) as exc:
        with client.websocket_connect(
            SYNTHETIC_MEDIA_WS_ENDPOINT_PATH,
            headers={"Authorization": "Basic dXNlcjpwYXNz"},
        ):
            pass
    assert exc.value.code == 1008


def test_auth_empty_bearer_token_rejected() -> None:
    """Verify empty bearer token is rejected with 1008."""
    verifier = SyntheticTokenVerifier(secret_key=TEST_SECRET_KEY)
    auth_bridge = SyntheticAuthBridge(verifier=verifier)
    app = create_gateway_app(
        config=CoreConfig(ingress_mode=INGRESS_MODE_SYNTHETIC),
        synthetic_auth_bridge=auth_bridge,
        enable_synthetic_media=True,
    )
    client = TestClient(app)

    with pytest.raises(WebSocketDisconnect) as exc:
        with client.websocket_connect(
            SYNTHETIC_MEDIA_WS_ENDPOINT_PATH,
            headers={"Authorization": "Bearer   "},
        ):
            pass
    assert exc.value.code == 1008


def test_auth_tampered_hmac_rejected() -> None:
    """Verify tampered token signature is rejected fail-closed with 1008."""
    verifier = SyntheticTokenVerifier(secret_key=TEST_SECRET_KEY)
    auth_bridge = SyntheticAuthBridge(verifier=verifier)
    app = create_gateway_app(
        config=CoreConfig(ingress_mode=INGRESS_MODE_SYNTHETIC),
        synthetic_auth_bridge=auth_bridge,
        enable_synthetic_media=True,
    )
    client = TestClient(app)

    valid_token = verifier.issue_token(
        provider_id="smoke_provider",
        tenant_id="tenant_01",
        call_id="call_01",
        allowed_scopes=["voice.stream"],
    )
    tampered_token = valid_token[:-4] + "AAAA"

    with pytest.raises(WebSocketDisconnect) as exc:
        with client.websocket_connect(
            SYNTHETIC_MEDIA_WS_ENDPOINT_PATH,
            headers={"Authorization": f"Bearer {tampered_token}"},
        ):
            pass
    assert exc.value.code == 1008


def test_auth_missing_voice_stream_scope_rejected() -> None:
    """Verify token missing required 'voice.stream' scope is rejected with 1008."""
    verifier = SyntheticTokenVerifier(secret_key=TEST_SECRET_KEY)
    auth_bridge = SyntheticAuthBridge(verifier=verifier)
    app = create_gateway_app(
        config=CoreConfig(ingress_mode=INGRESS_MODE_SYNTHETIC),
        synthetic_auth_bridge=auth_bridge,
        enable_synthetic_media=True,
    )
    client = TestClient(app)

    token = verifier.issue_token(
        provider_id="smoke_provider",
        tenant_id="tenant_01",
        call_id="call_01",
        allowed_scopes=["voice.control"],  # Missing voice.stream!
    )

    with pytest.raises(WebSocketDisconnect) as exc:
        with client.websocket_connect(
            SYNTHETIC_MEDIA_WS_ENDPOINT_PATH,
            headers={"Authorization": f"Bearer {token}"},
        ):
            pass
    assert exc.value.code == 1008


def test_auth_replayed_nonce_token_rejected() -> None:
    """Verify token replay protection rejects reused token with 1008."""
    verifier = SyntheticTokenVerifier(secret_key=TEST_SECRET_KEY, enable_replay_protection=True)
    auth_bridge = SyntheticAuthBridge(verifier=verifier)
    app = create_gateway_app(
        config=CoreConfig(ingress_mode=INGRESS_MODE_SYNTHETIC),
        synthetic_auth_bridge=auth_bridge,
        enable_synthetic_media=True,
    )
    client = TestClient(app)

    token = verifier.issue_token(
        provider_id="smoke_provider",
        tenant_id="tenant_01",
        call_id="call_01",
        allowed_scopes=["voice.stream"],
    )

    # First connection consumes the token
    with client.websocket_connect(
        SYNTHETIC_MEDIA_WS_ENDPOINT_PATH,
        headers={"Authorization": f"Bearer {token}"},
    ) as ws:
        ready = ws.receive_json()
        assert ready["event"] == EVENT_SESSION_READY
        ws.send_json({"event": EVENT_SESSION_END})

    # Second connection using identical token must fail replay protection
    with pytest.raises(WebSocketDisconnect) as exc:
        with client.websocket_connect(
            SYNTHETIC_MEDIA_WS_ENDPOINT_PATH,
            headers={"Authorization": f"Bearer {token}"},
        ):
            pass
    assert exc.value.code == 1008


def test_admission_global_capacity_limit_rejected() -> None:
    """Verify that when admission limit is exhausted, connection is rejected with 1008."""
    config = CoreConfig(
        ingress_mode=INGRESS_MODE_SYNTHETIC,
        max_concurrent_connections=1,
        max_connections_per_tenant=1,
    )
    admission = AdmissionController(config)
    verifier = SyntheticTokenVerifier(secret_key=TEST_SECRET_KEY)
    auth_bridge = SyntheticAuthBridge(verifier=verifier)
    app = create_gateway_app(
        config=config,
        admission_controller=admission,
        synthetic_auth_bridge=auth_bridge,
        enable_synthetic_media=True,
    )
    client = TestClient(app)

    token1 = verifier.issue_token(
        provider_id="smoke_provider",
        tenant_id="tenant_01",
        call_id="call_01",
        allowed_scopes=["voice.stream"],
    )
    token2 = verifier.issue_token(
        provider_id="smoke_provider",
        tenant_id="tenant_02",
        call_id="call_02",
        allowed_scopes=["voice.stream"],
    )

    with client.websocket_connect(
        SYNTHETIC_MEDIA_WS_ENDPOINT_PATH,
        headers={"Authorization": f"Bearer {token1}"},
    ) as ws1:
        ready1 = ws1.receive_json()
        assert ready1["event"] == EVENT_SESSION_READY

        # Second connection exceeds global limit of 1 -> rejected with 1008
        with pytest.raises(WebSocketDisconnect) as exc:
            with client.websocket_connect(
                SYNTHETIC_MEDIA_WS_ENDPOINT_PATH,
                headers={"Authorization": f"Bearer {token2}"},
            ):
                pass
        assert exc.value.code == 1008

        ws1.send_json({"event": EVENT_SESSION_END})

    # After close, capacity is fully recovered
    assert admission.snapshot().active_connections == 0


def test_admission_tenant_capacity_limit_rejected() -> None:
    """Verify that when tenant limit is exhausted, connection is rejected with 1008."""
    config = CoreConfig(
        ingress_mode=INGRESS_MODE_SYNTHETIC,
        max_concurrent_connections=10,
        max_connections_per_tenant=1,
    )
    admission = AdmissionController(config)
    verifier = SyntheticTokenVerifier(secret_key=TEST_SECRET_KEY)
    auth_bridge = SyntheticAuthBridge(verifier=verifier)
    app = create_gateway_app(
        config=config,
        admission_controller=admission,
        synthetic_auth_bridge=auth_bridge,
        enable_synthetic_media=True,
    )
    client = TestClient(app)

    token1 = verifier.issue_token(
        provider_id="smoke_provider",
        tenant_id="tenant_same",
        call_id="call_01",
        allowed_scopes=["voice.stream"],
    )
    token2 = verifier.issue_token(
        provider_id="smoke_provider",
        tenant_id="tenant_same",
        call_id="call_02",
        allowed_scopes=["voice.stream"],
    )

    with client.websocket_connect(
        SYNTHETIC_MEDIA_WS_ENDPOINT_PATH,
        headers={"Authorization": f"Bearer {token1}"},
    ) as ws1:
        ready1 = ws1.receive_json()
        assert ready1["event"] == EVENT_SESSION_READY

        # Second connection for same tenant exceeds tenant limit of 1 -> rejected
        with pytest.raises(WebSocketDisconnect) as exc:
            with client.websocket_connect(
                SYNTHETIC_MEDIA_WS_ENDPOINT_PATH,
                headers={"Authorization": f"Bearer {token2}"},
            ):
                pass
        assert exc.value.code == 1008

        ws1.send_json({"event": EVENT_SESSION_END})


def test_call_exclusivity_collision_rejected() -> None:
    """Verify concurrent connections attempting the same call_id in the same tenant collide."""
    verifier = SyntheticTokenVerifier(secret_key=TEST_SECRET_KEY)
    auth_bridge = SyntheticAuthBridge(verifier=verifier, enforce_call_exclusivity=True)
    app = create_gateway_app(
        config=CoreConfig(ingress_mode=INGRESS_MODE_SYNTHETIC),
        synthetic_auth_bridge=auth_bridge,
        enable_synthetic_media=True,
    )
    client = TestClient(app)

    token1 = verifier.issue_token(
        provider_id="smoke_provider",
        tenant_id="tenant_01",
        call_id="colliding_call",
        allowed_scopes=["voice.stream"],
    )
    token2 = verifier.issue_token(
        provider_id="smoke_provider",
        tenant_id="tenant_01",
        call_id="colliding_call",
        allowed_scopes=["voice.stream"],
    )

    with client.websocket_connect(
        SYNTHETIC_MEDIA_WS_ENDPOINT_PATH,
        headers={"Authorization": f"Bearer {token1}"},
    ) as ws1:
        ready1 = ws1.receive_json()
        assert ready1["event"] == EVENT_SESSION_READY

        # Second connection for the same active call_id -> rejected with 1008
        with pytest.raises(WebSocketDisconnect) as exc:
            with client.websocket_connect(
                SYNTHETIC_MEDIA_WS_ENDPOINT_PATH,
                headers={"Authorization": f"Bearer {token2}"},
            ):
                pass
        assert exc.value.code == 1008

        ws1.send_json({"event": EVENT_SESSION_END})


# =============================================================================
# GATE C & D: Media Framing, Ingress Demuxing & Adapter Invariants
# =============================================================================


def test_binary_frame_before_call_start_rejected() -> None:
    """Verify sending binary audio before call.start closes the connection with 1008."""
    verifier = SyntheticTokenVerifier(secret_key=TEST_SECRET_KEY)
    auth_bridge = SyntheticAuthBridge(verifier=verifier)
    app = create_gateway_app(
        config=CoreConfig(ingress_mode=INGRESS_MODE_SYNTHETIC),
        synthetic_auth_bridge=auth_bridge,
        enable_synthetic_media=True,
    )
    client = TestClient(app)

    token = verifier.issue_token(
        provider_id="smoke_provider",
        tenant_id="tenant_01",
        call_id="call_01",
        allowed_scopes=["voice.stream"],
    )

    with client.websocket_connect(
        SYNTHETIC_MEDIA_WS_ENDPOINT_PATH,
        headers={"Authorization": f"Bearer {token}"},
    ) as ws:
        ready = ws.receive_json()
        assert ready["event"] == EVENT_SESSION_READY

        # Send binary media BEFORE call.start
        ws.send_bytes(b"\x00" * PCM16LE_FRAME_BYTES)

        with pytest.raises(WebSocketDisconnect) as exc:
            ws.receive_json()
        assert exc.value.code == 1008


def test_invalid_frame_size_rejected() -> None:
    """Verify sending non-640-byte binary frame closes connection with 1008."""
    verifier = SyntheticTokenVerifier(secret_key=TEST_SECRET_KEY)
    auth_bridge = SyntheticAuthBridge(verifier=verifier)
    app = create_gateway_app(
        config=CoreConfig(ingress_mode=INGRESS_MODE_SYNTHETIC),
        synthetic_auth_bridge=auth_bridge,
        enable_synthetic_media=True,
    )
    client = TestClient(app)

    token = verifier.issue_token(
        provider_id="smoke_provider",
        tenant_id="tenant_01",
        call_id="call_01",
        allowed_scopes=["voice.stream"],
    )

    with client.websocket_connect(
        SYNTHETIC_MEDIA_WS_ENDPOINT_PATH,
        headers={"Authorization": f"Bearer {token}"},
    ) as ws:
        ws.receive_json()  # session.ready
        ws.send_json({"event": EVENT_CALL_START, "call_id": "call_01"})
        started = ws.receive_json()
        assert started["event"] == EVENT_CALL_STARTED

        # Send invalid size: 200 bytes instead of 640
        ws.send_bytes(b"\x00" * 200)

        with pytest.raises(WebSocketDisconnect) as exc:
            ws.receive_bytes()
        assert exc.value.code == 1008


def test_duplicate_call_start_rejected() -> None:
    """Verify sending call.start twice closes connection with 1008."""
    verifier = SyntheticTokenVerifier(secret_key=TEST_SECRET_KEY)
    auth_bridge = SyntheticAuthBridge(verifier=verifier)
    app = create_gateway_app(
        config=CoreConfig(ingress_mode=INGRESS_MODE_SYNTHETIC),
        synthetic_auth_bridge=auth_bridge,
        enable_synthetic_media=True,
    )
    client = TestClient(app)

    token = verifier.issue_token(
        provider_id="smoke_provider",
        tenant_id="tenant_01",
        call_id="call_01",
        allowed_scopes=["voice.stream"],
    )

    with client.websocket_connect(
        SYNTHETIC_MEDIA_WS_ENDPOINT_PATH,
        headers={"Authorization": f"Bearer {token}"},
    ) as ws:
        ws.receive_json()  # session.ready
        ws.send_json({"event": EVENT_CALL_START, "call_id": "call_01"})
        started = ws.receive_json()
        assert started["event"] == EVENT_CALL_STARTED

        # Second call.start
        ws.send_json({"event": EVENT_CALL_START, "call_id": "call_01"})

        with pytest.raises(WebSocketDisconnect) as exc:
            ws.receive_json()
        assert exc.value.code == 1008


def test_call_start_mismatched_call_id_rejected() -> None:
    """Verify call.start containing wrong call_id is rejected with 1008."""
    verifier = SyntheticTokenVerifier(secret_key=TEST_SECRET_KEY)
    auth_bridge = SyntheticAuthBridge(verifier=verifier)
    app = create_gateway_app(
        config=CoreConfig(ingress_mode=INGRESS_MODE_SYNTHETIC),
        synthetic_auth_bridge=auth_bridge,
        enable_synthetic_media=True,
    )
    client = TestClient(app)

    token = verifier.issue_token(
        provider_id="smoke_provider",
        tenant_id="tenant_01",
        call_id="legitimate_call",
        allowed_scopes=["voice.stream"],
    )

    with client.websocket_connect(
        SYNTHETIC_MEDIA_WS_ENDPOINT_PATH,
        headers={"Authorization": f"Bearer {token}"},
    ) as ws:
        ws.receive_json()  # session.ready
        ws.send_json({"event": EVENT_CALL_START, "call_id": "spoofed_call"})

        with pytest.raises(WebSocketDisconnect) as exc:
            ws.receive_json()
        assert exc.value.code == 1008


def test_call_end_when_not_active_rejected() -> None:
    """Verify call.end before call.start is rejected with 1008."""
    verifier = SyntheticTokenVerifier(secret_key=TEST_SECRET_KEY)
    auth_bridge = SyntheticAuthBridge(verifier=verifier)
    app = create_gateway_app(
        config=CoreConfig(ingress_mode=INGRESS_MODE_SYNTHETIC),
        synthetic_auth_bridge=auth_bridge,
        enable_synthetic_media=True,
    )
    client = TestClient(app)

    token = verifier.issue_token(
        provider_id="smoke_provider",
        tenant_id="tenant_01",
        call_id="call_01",
        allowed_scopes=["voice.stream"],
    )

    with client.websocket_connect(
        SYNTHETIC_MEDIA_WS_ENDPOINT_PATH,
        headers={"Authorization": f"Bearer {token}"},
    ) as ws:
        ws.receive_json()  # session.ready
        ws.send_json({"event": EVENT_CALL_END, "call_id": "call_01"})

        with pytest.raises(WebSocketDisconnect) as exc:
            ws.receive_json()
        assert exc.value.code == 1008


def test_malformed_json_control_rejected() -> None:
    """Verify malformed JSON control message is rejected with close code 1007."""
    verifier = SyntheticTokenVerifier(secret_key=TEST_SECRET_KEY)
    auth_bridge = SyntheticAuthBridge(verifier=verifier)
    app = create_gateway_app(
        config=CoreConfig(ingress_mode=INGRESS_MODE_SYNTHETIC),
        synthetic_auth_bridge=auth_bridge,
        enable_synthetic_media=True,
    )
    client = TestClient(app)

    token = verifier.issue_token(
        provider_id="smoke_provider",
        tenant_id="tenant_01",
        call_id="call_01",
        allowed_scopes=["voice.stream"],
    )

    with client.websocket_connect(
        SYNTHETIC_MEDIA_WS_ENDPOINT_PATH,
        headers={"Authorization": f"Bearer {token}"},
    ) as ws:
        ws.receive_json()  # session.ready
        ws.send_text("{bad_json")

        with pytest.raises(WebSocketDisconnect) as exc:
            ws.receive_json()
        assert exc.value.code == 1007


def test_session_ping_pong_exchange() -> None:
    """Verify session.ping receives session.pong with matching seq."""
    verifier = SyntheticTokenVerifier(secret_key=TEST_SECRET_KEY)
    auth_bridge = SyntheticAuthBridge(verifier=verifier)
    app = create_gateway_app(
        config=CoreConfig(ingress_mode=INGRESS_MODE_SYNTHETIC),
        synthetic_auth_bridge=auth_bridge,
        enable_synthetic_media=True,
    )
    client = TestClient(app)

    token = verifier.issue_token(
        provider_id="smoke_provider",
        tenant_id="tenant_01",
        call_id="call_01",
        allowed_scopes=["voice.stream"],
    )

    with client.websocket_connect(
        SYNTHETIC_MEDIA_WS_ENDPOINT_PATH,
        headers={"Authorization": f"Bearer {token}"},
    ) as ws:
        ready = ws.receive_json()
        assert ready["event"] == EVENT_SESSION_READY

        ws.send_json({"event": EVENT_SESSION_PING, "seq": 42})
        pong = ws.receive_json()
        assert pong["event"] == EVENT_SESSION_PONG
        assert pong["seq"] == 42

        ws.send_json({"event": EVENT_SESSION_END})


# =============================================================================
# GATE D: BoundedFakeAIEchoPort Unit Tests
# =============================================================================


def test_bounded_fake_ai_constructor_validation() -> None:
    """Verify BoundedFakeAIEchoPort constructor parameter validation."""
    with pytest.raises(VoiceDomainError):
        BoundedFakeAIEchoPort(queue_capacity=0)
    with pytest.raises(VoiceDomainError):
        BoundedFakeAIEchoPort(queue_capacity=True)
    with pytest.raises(VoiceDomainError):
        BoundedFakeAIEchoPort(backpressure_timeout=0.0)
    with pytest.raises(VoiceDomainError):
        BoundedFakeAIEchoPort(backpressure_timeout=-1.0)
    with pytest.raises(VoiceDomainError):
        BoundedFakeAIEchoPort(backpressure_timeout=float("nan"))


def test_bounded_fake_ai_roundtrip_and_close() -> None:
    """Verify BoundedFakeAIEchoPort echo behavior, metrics, and closed behavior."""

    async def _test() -> None:
        port = BoundedFakeAIEchoPort(queue_capacity=4, backpressure_timeout=1.0)
        port_closed = bool(port.is_closed)
        assert port_closed is False
        assert port.frames_received == 0
        assert port.frames_sent == 0

        frame = AudioFrame(
            payload=b"\x11" * 640,
            format=AudioFormat.PCM16LE,
            sample_rate=16000,
            channels=1,
            sequence=0,
            timestamp_ms=0.0,
        )
        await port.send_frame(frame)
        assert port.frames_received == 1
        assert port.bytes_received == 640
        assert port.queue_depth == 1

        echoed = await port.receive_frame()
        assert echoed is not None
        assert echoed.payload == b"\x11" * 640
        assert echoed.sequence == 0
        assert port.frames_sent == 1
        assert port.bytes_sent == 640
        assert port.queue_depth == 0

        # Push EOF
        port.push_eof()
        eof_recv = await port.receive_frame()
        assert eof_recv is None

        # Close
        await port.close()
        port_closed = bool(port.is_closed)
        assert port_closed is True

        with pytest.raises(MediaPortClosedError):
            await port.send_frame(frame)

        with pytest.raises(MediaPortClosedError):
            await port.receive_frame()

    asyncio.run(_test())


def test_bounded_fake_ai_backpressure_overflow() -> None:
    """Verify BoundedFakeAIEchoPort raises MediaBackpressureOverflowError when queue fills."""

    async def _test() -> None:
        port = BoundedFakeAIEchoPort(queue_capacity=2, backpressure_timeout=0.05)

        for i in range(2):
            frame = AudioFrame(
                payload=f"F{i}".encode("ascii") + b"\x00" * 638,
                format=AudioFormat.PCM16LE,
                sample_rate=16000,
                channels=1,
                sequence=i,
                timestamp_ms=float(i * 20),
            )
            await port.send_frame(frame)

        # Third frame exceeds queue_capacity of 2 and times out after 0.05s
        overflow_frame = AudioFrame(
            payload=b"\xff" * 640,
            format=AudioFormat.PCM16LE,
            sample_rate=16000,
            channels=1,
            sequence=2,
            timestamp_ms=40.0,
        )
        with pytest.raises(MediaBackpressureOverflowError, match="overflowed"):
            await port.send_frame(overflow_frame)

        await port.close()

    asyncio.run(_test())


# =============================================================================
# GATE C: WebSocketTelephonyMediaAdapter Unit Tests
# =============================================================================


def test_websocket_telephony_adapter_constructor_validation() -> None:
    """Verify WebSocketTelephonyMediaAdapter constructor parameter validation."""
    ctx = _make_verified_context()
    dummy_ws = object()

    with pytest.raises(VoiceDomainError):
        WebSocketTelephonyMediaAdapter(dummy_ws, ctx, queue_capacity=0)
    with pytest.raises(VoiceDomainError):
        WebSocketTelephonyMediaAdapter(dummy_ws, ctx, backpressure_timeout=-0.5)


def test_websocket_telephony_adapter_inbound_and_outbound() -> None:
    """Verify WebSocketTelephonyMediaAdapter push_inbound, receive_frame, send_frame, and metrics."""

    class MockWebSocket:
        def __init__(self) -> None:
            self.sent_bytes: list[bytes] = []
            self.sent_texts: list[str] = []

        async def send_bytes(self, data: bytes) -> None:
            self.sent_bytes.append(data)

        async def send_text(self, text: str) -> None:
            self.sent_texts.append(text)

    async def _test() -> None:
        mock_ws = MockWebSocket()
        ctx = _make_verified_context()
        adapter = WebSocketTelephonyMediaAdapter(mock_ws, ctx, queue_capacity=4)

        tel_closed = bool(adapter.is_closed)
        assert tel_closed is False
        assert adapter.context is ctx
        assert adapter.reject_count == 0
        adapter.record_reject()
        assert adapter.reject_count == 1

        frame = AudioFrame(
            payload=b"\xab" * 640,
            format=AudioFormat.PCM16LE,
            sample_rate=16000,
            channels=1,
            sequence=0,
            timestamp_ms=0.0,
        )

        # Inbound leg
        await adapter.push_inbound_frame(frame)
        assert adapter.inbound_queue_depth == 1

        recv = await adapter.receive_frame()
        assert recv is not None
        assert recv.payload == b"\xab" * 640
        assert adapter.frames_received == 1
        assert adapter.bytes_received == 640

        # Outbound leg
        await adapter.send_frame(frame)
        assert len(mock_ws.sent_bytes) == 1
        assert mock_ws.sent_bytes[0] == b"\xab" * 640
        assert adapter.frames_sent == 1
        assert adapter.bytes_sent == 640

        # Control text leg
        await adapter.send_control_text('{"event":"pong"}')
        assert len(mock_ws.sent_texts) == 1

        # EOF
        adapter.push_inbound_eof()
        eof = await adapter.receive_frame()
        assert eof is None

        # Close
        await adapter.close()
        is_closed = bool(adapter.is_closed)
        assert is_closed is True

        with pytest.raises(MediaPortClosedError):
            await adapter.receive_frame()

        with pytest.raises(MediaPortClosedError):
            await adapter.send_frame(frame)

        with pytest.raises(MediaPortClosedError):
            await adapter.send_control_text("test")

        with pytest.raises(MediaPortClosedError):
            await adapter.push_inbound_frame(frame)

    asyncio.run(_test())


# =============================================================================
# GATE E: In-Process & Loopback End-to-End Streaming & Isolation
# =============================================================================


def test_in_process_streaming_roundtrip() -> None:
    """Verify in-process WebSocket client bidirectional audio streaming round-trip."""
    verifier = SyntheticTokenVerifier(secret_key=TEST_SECRET_KEY)
    auth_bridge = SyntheticAuthBridge(verifier=verifier)
    app = create_gateway_app(
        config=CoreConfig(ingress_mode=INGRESS_MODE_SYNTHETIC),
        synthetic_auth_bridge=auth_bridge,
        enable_synthetic_media=True,
    )
    client = TestClient(app)

    token = verifier.issue_token(
        provider_id="smoke_provider",
        tenant_id="tenant_01",
        call_id="call_roundtrip_01",
        allowed_scopes=["voice.stream"],
    )

    with client.websocket_connect(
        SYNTHETIC_MEDIA_WS_ENDPOINT_PATH,
        headers={"Authorization": f"Bearer {token}"},
    ) as ws:
        ready = ws.receive_json()
        assert ready["event"] == EVENT_SESSION_READY
        assert ready["call_id"] == "call_roundtrip_01"

        ws.send_json({"event": EVENT_CALL_START, "call_id": "call_roundtrip_01"})
        started = ws.receive_json()
        assert started["event"] == EVENT_CALL_STARTED

        # Stream 5 audio frames and receive 5 echoed frames
        sent_frames: list[bytes] = []
        for i in range(5):
            payload = f"FRAME_{i}_".encode("ascii") + b"\x00" * (640 - len(f"FRAME_{i}_"))
            sent_frames.append(payload)
            ws.send_bytes(payload)
            echoed = ws.receive_bytes()
            assert echoed == payload

        ws.send_json({"event": EVENT_CALL_END, "call_id": "call_roundtrip_01"})
        ended = ws.receive_json()
        assert ended["event"] == EVENT_CALL_ENDED


def test_loopback_two_independent_calls_isolated() -> None:
    """Verify two concurrent independent calls on loopback have full isolation and zero cross-talk."""

    async def _test() -> None:
        port = _get_free_port()
        config = CoreConfig(
            ingress_mode=INGRESS_MODE_SYNTHETIC,
            max_concurrent_connections=10,
            max_connections_per_tenant=5,
        )
        admission = AdmissionController(config)
        verifier = SyntheticTokenVerifier(secret_key=TEST_SECRET_KEY)
        auth_bridge = SyntheticAuthBridge(verifier=verifier)

        app = create_gateway_app(
            config=config,
            admission_controller=admission,
            synthetic_auth_bridge=auth_bridge,
            enable_synthetic_media=True,
        )

        async with _run_test_server(app, port) as base_ws_url:
            uri = f"{base_ws_url}{SYNTHETIC_MEDIA_WS_ENDPOINT_PATH}"

            token1 = verifier.issue_token(
                provider_id="smoke_provider",
                tenant_id="tenant_alpha",
                call_id="call_alpha_01",
                allowed_scopes=["voice.stream"],
            )
            token2 = verifier.issue_token(
                provider_id="smoke_provider",
                tenant_id="tenant_beta",
                call_id="call_beta_02",
                allowed_scopes=["voice.stream"],
            )

            async with (
                websockets.connect(
                    uri, additional_headers={"Authorization": f"Bearer {token1}"}
                ) as ws1,
                websockets.connect(
                    uri, additional_headers={"Authorization": f"Bearer {token2}"}
                ) as ws2,
            ):
                # Receive ready
                r1 = json.loads(await ws1.recv())
                r2 = json.loads(await ws2.recv())
                assert r1["call_id"] == "call_alpha_01"
                assert r2["call_id"] == "call_beta_02"

                # Start calls
                await ws1.send(json.dumps({"event": EVENT_CALL_START, "call_id": "call_alpha_01"}))
                await ws2.send(json.dumps({"event": EVENT_CALL_START, "call_id": "call_beta_02"}))

                s1 = json.loads(await ws1.recv())
                s2 = json.loads(await ws2.recv())
                assert s1["event"] == EVENT_CALL_STARTED
                assert s2["event"] == EVENT_CALL_STARTED

                # Send distinctive payloads
                payload_alpha = b"ALPHA_" + b"\xaa" * 634
                payload_beta = b"BETAA_" + b"\xbb" * 634

                await ws1.send(payload_alpha)
                await ws2.send(payload_beta)

                echo_alpha = await ws1.recv()
                echo_beta = await ws2.recv()

                # Verify strict isolation
                assert echo_alpha == payload_alpha
                assert echo_beta == payload_beta

                # End calls
                await ws1.send(json.dumps({"event": EVENT_CALL_END, "call_id": "call_alpha_01"}))
                await ws2.send(json.dumps({"event": EVENT_CALL_END, "call_id": "call_beta_02"}))

                await ws1.recv()
                await ws2.recv()

        # Capacity completely recovered
        await asyncio.sleep(0.05)
        assert admission.snapshot().active_connections == 0
        assert len(auth_bridge._active_calls) == 0

    asyncio.run(_test())


def test_sudden_client_disconnect_capacity_recovered() -> None:
    """Verify that an abrupt client disconnect mid-session cleanly releases all capacity and active calls."""

    async def _test() -> None:
        port = _get_free_port()
        config = CoreConfig(
            ingress_mode=INGRESS_MODE_SYNTHETIC,
            max_concurrent_connections=10,
            max_connections_per_tenant=5,
        )
        admission = AdmissionController(config)
        verifier = SyntheticTokenVerifier(secret_key=TEST_SECRET_KEY)
        auth_bridge = SyntheticAuthBridge(verifier=verifier)

        app = create_gateway_app(
            config=config,
            admission_controller=admission,
            synthetic_auth_bridge=auth_bridge,
            enable_synthetic_media=True,
        )

        async with _run_test_server(app, port) as base_ws_url:
            uri = f"{base_ws_url}{SYNTHETIC_MEDIA_WS_ENDPOINT_PATH}"

            token = verifier.issue_token(
                provider_id="smoke_provider",
                tenant_id="tenant_disconnect",
                call_id="call_disconnect_01",
                allowed_scopes=["voice.stream"],
            )

            ws = await websockets.connect(
                uri, additional_headers={"Authorization": f"Bearer {token}"}
            )
            await ws.recv()  # session.ready
            await ws.send(json.dumps({"event": EVENT_CALL_START, "call_id": "call_disconnect_01"}))
            await ws.recv()  # call.started

            # Send 2 frames
            await ws.send(b"\x01" * 640)
            await ws.recv()

            assert admission.snapshot().active_connections == 1
            assert ("tenant_disconnect", "call_disconnect_01") in auth_bridge._active_calls

            # Abruptly close connection without call.end
            await ws.close()

            # Wait briefly for supervisor to observe disconnect and execute cleanup
            for _ in range(50):
                if admission.snapshot().active_connections == 0:
                    break
                await asyncio.sleep(0.05)

            assert admission.snapshot().active_connections == 0
            assert ("tenant_disconnect", "call_disconnect_01") not in auth_bridge._active_calls

    asyncio.run(_test())


def test_server_draining_rejects_synthetic_media() -> None:
    """Verify that when the server is in draining state, connection is rejected with 1008."""
    config = CoreConfig(ingress_mode=INGRESS_MODE_SYNTHETIC)
    admission = AdmissionController(config)
    coordinator = InstanceDrainCoordinator(controller=admission)

    verifier = SyntheticTokenVerifier(secret_key=TEST_SECRET_KEY)
    auth_bridge = SyntheticAuthBridge(verifier=verifier)
    app = create_gateway_app(
        config=config,
        admission_controller=admission,
        drain_coordinator=coordinator,
        synthetic_auth_bridge=auth_bridge,
        enable_synthetic_media=True,
    )
    app.state.shutting_down = True
    client = TestClient(app)

    token = verifier.issue_token(
        provider_id="smoke_provider",
        tenant_id="tenant_01",
        call_id="call_01",
        allowed_scopes=["voice.stream"],
    )

    with pytest.raises(WebSocketDisconnect) as exc:
        with client.websocket_connect(
            SYNTHETIC_MEDIA_WS_ENDPOINT_PATH,
            headers={"Authorization": f"Bearer {token}"},
        ):
            pass
    assert exc.value.code == 1008


def test_misconfigured_auth_bridge_policy_rejects_synthetic_media() -> None:
    """Verify connection is rejected if exclusivity or replay protection is disabled on auth_bridge."""
    from kaiwen_gateway.core.exceptions import CoreConfigurationError

    # Factory level check
    verifier = SyntheticTokenVerifier(secret_key=TEST_SECRET_KEY, enable_replay_protection=False)
    auth_bridge = SyntheticAuthBridge(verifier=verifier, enforce_call_exclusivity=False)

    with pytest.raises(
        CoreConfigurationError, match="Synthetic ingress requires active call exclusivity"
    ):
        create_gateway_app(
            config=CoreConfig(ingress_mode=INGRESS_MODE_SYNTHETIC),
            synthetic_auth_bridge=auth_bridge,
            enable_synthetic_media=True,
        )

    # Runtime defense-in-depth check in endpoint
    valid_verifier = SyntheticTokenVerifier(
        secret_key=TEST_SECRET_KEY, enable_replay_protection=True
    )
    valid_bridge = SyntheticAuthBridge(verifier=valid_verifier, enforce_call_exclusivity=True)
    app = create_gateway_app(
        config=CoreConfig(ingress_mode=INGRESS_MODE_SYNTHETIC),
        synthetic_auth_bridge=valid_bridge,
        enable_synthetic_media=True,
    )
    valid_bridge._enforce_call_exclusivity = False
    client = TestClient(app)

    token = valid_verifier.issue_token(
        provider_id="smoke_provider",
        tenant_id="tenant_01",
        call_id="call_01",
        allowed_scopes=["voice.stream"],
    )

    with pytest.raises(WebSocketDisconnect) as exc:
        with client.websocket_connect(
            SYNTHETIC_MEDIA_WS_ENDPOINT_PATH,
            headers={"Authorization": f"Bearer {token}"},
        ):
            pass
    assert exc.value.code == 1008


def test_synthetic_media_protocol_errors_instantiation() -> None:
    """Verify direct error classes for synthetic media."""
    from kaiwen_gateway.transports.synthetic.media_protocol import (
        MediaAfterTerminationError,
        MediaBeforeActivationError,
    )

    err1 = MediaBeforeActivationError()
    assert err1.close_code == 1008
    assert "before call.start" in str(err1)

    err2 = MediaAfterTerminationError()
    assert err2.close_code == 1008
    assert "after call.end" in str(err2)


def test_call_end_mismatched_call_id_rejected() -> None:
    """Verify call.end containing wrong call_id is rejected with 1008."""
    verifier = SyntheticTokenVerifier(secret_key=TEST_SECRET_KEY)
    auth_bridge = SyntheticAuthBridge(verifier=verifier)
    app = create_gateway_app(
        config=CoreConfig(ingress_mode=INGRESS_MODE_SYNTHETIC),
        synthetic_auth_bridge=auth_bridge,
        enable_synthetic_media=True,
    )
    client = TestClient(app)

    token = verifier.issue_token(
        provider_id="smoke_provider",
        tenant_id="tenant_01",
        call_id="call_legit_01",
        allowed_scopes=["voice.stream"],
    )

    with client.websocket_connect(
        SYNTHETIC_MEDIA_WS_ENDPOINT_PATH,
        headers={"Authorization": f"Bearer {token}"},
    ) as ws:
        ws.receive_json()  # session.ready
        ws.send_json({"event": EVENT_CALL_START, "call_id": "call_legit_01"})
        ws.receive_json()  # call.started

        # Send call.end with wrong call_id
        ws.send_json({"event": EVENT_CALL_END, "call_id": "call_wrong_99"})

        with pytest.raises(WebSocketDisconnect) as exc:
            ws.receive_json()
        assert exc.value.code == 1008


def test_custom_ai_media_port_factory_used() -> None:
    """Verify custom ai_media_port_factory in create_gateway_app is invoked per call."""
    custom_created = 0

    def _factory() -> BoundedFakeAIEchoPort:
        nonlocal custom_created
        custom_created += 1
        return BoundedFakeAIEchoPort(queue_capacity=10)

    verifier = SyntheticTokenVerifier(secret_key=TEST_SECRET_KEY)
    auth_bridge = SyntheticAuthBridge(verifier=verifier)
    app = create_gateway_app(
        config=CoreConfig(ingress_mode=INGRESS_MODE_SYNTHETIC),
        synthetic_auth_bridge=auth_bridge,
        enable_synthetic_media=True,
        ai_media_port_factory=_factory,
    )
    client = TestClient(app)

    token = verifier.issue_token(
        provider_id="smoke_provider",
        tenant_id="tenant_01",
        call_id="call_custom_01",
        allowed_scopes=["voice.stream"],
    )

    with client.websocket_connect(
        SYNTHETIC_MEDIA_WS_ENDPOINT_PATH,
        headers={"Authorization": f"Bearer {token}"},
    ) as ws:
        ws.receive_json()
        ws.send_json({"event": EVENT_SESSION_END})

    assert custom_created == 1


def test_binary_frame_after_call_end_rejected() -> None:
    """Verify sending binary audio after call.end closes connection with code 1008 and specific reason."""
    verifier = SyntheticTokenVerifier(secret_key=TEST_SECRET_KEY)
    auth_bridge = SyntheticAuthBridge(verifier=verifier)
    app = create_gateway_app(
        config=CoreConfig(ingress_mode=INGRESS_MODE_SYNTHETIC),
        synthetic_auth_bridge=auth_bridge,
        enable_synthetic_media=True,
    )
    client = TestClient(app)

    token = verifier.issue_token(
        provider_id="smoke_provider",
        tenant_id="tenant_01",
        call_id="call_01",
        allowed_scopes=["voice.stream"],
    )

    with client.websocket_connect(
        SYNTHETIC_MEDIA_WS_ENDPOINT_PATH,
        headers={"Authorization": f"Bearer {token}"},
    ) as ws:
        ws.receive_json()  # session.ready
        ws.send_json({"event": EVENT_CALL_START, "call_id": "call_01"})
        started = ws.receive_json()
        assert started["event"] == EVENT_CALL_STARTED

        # Send valid audio frame
        ws.send_bytes(b"\x00" * PCM16LE_FRAME_BYTES)
        echo = ws.receive_bytes()
        assert len(echo) == PCM16LE_FRAME_BYTES

        # End call
        ws.send_json({"event": EVENT_CALL_END, "call_id": "call_01"})
        ended = ws.receive_json()
        assert ended["event"] == EVENT_CALL_ENDED

        # Now send binary frame AFTER call.end
        ws.send_bytes(b"\x00" * PCM16LE_FRAME_BYTES)

        with pytest.raises(WebSocketDisconnect) as exc:
            ws.receive_bytes()
        assert exc.value.code == 1008


def test_call_end_media_drain_race_with_barrier() -> None:
    """Gate D: Deterministic race test between call.end and in-flight Fake AI responses using asyncio.Event barrier."""

    async def _test() -> None:
        port = _get_free_port()
        config = CoreConfig(ingress_mode=INGRESS_MODE_SYNTHETIC)
        admission = AdmissionController(config)
        verifier = SyntheticTokenVerifier(secret_key=TEST_SECRET_KEY)
        auth_bridge = SyntheticAuthBridge(verifier=verifier)

        # AI port with an explicit barrier: pauses after receiving first frame
        first_frame_barrier = asyncio.Event()
        release_barrier = asyncio.Event()

        class BarrierFakeAIPort(BoundedFakeAIEchoPort):
            async def send_frame(self, frame: AudioFrame) -> None:
                await super().send_frame(frame)
                first_frame_barrier.set()
                # Wait until test releases barrier
                await release_barrier.wait()

        app = create_gateway_app(
            config=config,
            admission_controller=admission,
            synthetic_auth_bridge=auth_bridge,
            enable_synthetic_media=True,
            ai_media_port_factory=BarrierFakeAIPort,
        )

        async with _run_test_server(app, port) as base_ws_url:
            uri = f"{base_ws_url}{SYNTHETIC_MEDIA_WS_ENDPOINT_PATH}"
            token = verifier.issue_token(
                provider_id="smoke_provider",
                tenant_id="tenant_drain_race",
                call_id="call_drain_race_01",
                allowed_scopes=["voice.stream"],
            )

            async with websockets.connect(
                uri, additional_headers={"Authorization": f"Bearer {token}"}
            ) as ws:
                await ws.recv()  # session.ready
                await ws.send(
                    json.dumps({"event": EVENT_CALL_START, "call_id": "call_drain_race_01"})
                )
                await ws.recv()  # call.started

                # Send 1 frame
                await ws.send(b"\x12" * PCM16LE_FRAME_BYTES)

                # Wait deterministically until AI port has ingested the frame
                await asyncio.wait_for(first_frame_barrier.wait(), timeout=2.0)

                # Send call.end while AI port is held in barrier
                await ws.send(
                    json.dumps({"event": EVENT_CALL_END, "call_id": "call_drain_race_01"})
                )

                # Now release the barrier so AI can process and output queue drains
                release_barrier.set()

                # Read remaining responses: client receives call.ended or echoed frame + call.ended
                received_items: list[str | bytes] = []
                while True:
                    try:
                        msg = await asyncio.wait_for(ws.recv(), timeout=2.0)
                        received_items.append(msg)
                    except (websockets.exceptions.ConnectionClosed, TimeoutError):
                        break

                # Confirm call.ended was delivered
                has_ended = any(isinstance(m, str) and "call.ended" in m for m in received_items)
                assert has_ended is True

        # Verify 100% capacity and call recovery
        await asyncio.sleep(0.05)
        assert admission.snapshot().active_connections == 0
        assert len(auth_bridge._active_calls) == 0

    asyncio.run(_test())


def test_session_idle_timeout_closes_connection() -> None:
    """Verify idle timeout triggers clean close (code 1000)."""
    verifier = SyntheticTokenVerifier(secret_key=TEST_SECRET_KEY)
    auth_bridge = SyntheticAuthBridge(verifier=verifier)
    app = create_gateway_app(
        config=CoreConfig(ingress_mode=INGRESS_MODE_SYNTHETIC),
        synthetic_auth_bridge=auth_bridge,
        enable_synthetic_media=True,
    )
    app.state.synthetic_idle_timeout = 0.05
    client = TestClient(app)

    token = verifier.issue_token(
        provider_id="smoke_provider",
        tenant_id="tenant_01",
        call_id="call_idle_01",
        allowed_scopes=["voice.stream"],
    )

    with client.websocket_connect(
        SYNTHETIC_MEDIA_WS_ENDPOINT_PATH,
        headers={"Authorization": f"Bearer {token}"},
    ) as ws:
        ws.receive_json()  # session.ready
        with pytest.raises(WebSocketDisconnect) as exc:
            ws.receive_json()
        assert exc.value.code == 1000
