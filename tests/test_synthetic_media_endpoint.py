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
from kaiwen_gateway.core.lifecycle import (
    ConnectionState,
    ConnectionSupervisor,
    TerminalReason,
)
from kaiwen_gateway.modules.voice.adapters.fake_ai import BoundedFakeAIEchoPort
from kaiwen_gateway.modules.voice.bridge import VoiceBridgeSession
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
from kaiwen_gateway.transports.synthetic.media_endpoint import _resolve_positive_float

TEST_SECRET_KEY: Final[bytes] = b"synthetic_media_test_secret_32bytes!"


class CapturingCoordinator(InstanceDrainCoordinator):
    """Subclass capturing supervisors admitted through the coordinator."""

    def __init__(self, controller: AdmissionController) -> None:
        super().__init__(controller=controller)
        self.captured: list[ConnectionSupervisor] = []

    async def admit_connection(
        self,
        context: VerifiedConnectionContext,
        *,
        supervisor_policy: Any = None,
    ) -> ConnectionSupervisor:
        sup = await super().admit_connection(context, supervisor_policy=supervisor_policy)
        self.captured.append(sup)
        return sup


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
        assert exc.value.code == 1003


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
        await port.push_eof()
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
        await adapter.push_inbound_eof()
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


def test_call_end_server_initiated_normal_closure_and_capacity_recovery() -> None:
    """Verify call.end triggers lossless drain, call.ended, and server-initiated normal closure (code 1000)."""
    verifier = SyntheticTokenVerifier(secret_key=TEST_SECRET_KEY)
    auth_bridge = SyntheticAuthBridge(verifier=verifier)
    admission = AdmissionController(CoreConfig(ingress_mode=INGRESS_MODE_SYNTHETIC))
    app = create_gateway_app(
        config=CoreConfig(ingress_mode=INGRESS_MODE_SYNTHETIC),
        admission_controller=admission,
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

        # End call: server executes lossless drain and closes connection
        ws.send_json({"event": EVENT_CALL_END, "call_id": "call_01"})
        ended = ws.receive_json()
        assert ended["event"] == EVENT_CALL_ENDED

        # Server normally closes socket with code 1000
        with pytest.raises(WebSocketDisconnect) as exc:
            ws.receive_bytes()
        assert exc.value.code == 1000

    # Capacity and call registry immediately recovered
    assert admission.snapshot().active_connections == 0
    assert len(auth_bridge._active_calls) == 0


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

                # Confirm exact byte delivery and frame count invariants
                binary_frames = [m for m in received_items if isinstance(m, bytes)]
                text_frames = [m for m in received_items if isinstance(m, str)]

                assert len(binary_frames) == 1
                assert binary_frames[0] == b"\x12" * PCM16LE_FRAME_BYTES
                assert len(text_frames) == 1
                ended_payload = json.loads(text_frames[0])
                assert ended_payload["event"] == EVENT_CALL_ENDED
                assert ended_payload["call_id"] == "call_drain_race_01"

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


# =============================================================================
# GATE A-D REMEDIATION REGRESSION TESTS (PR #21 AUDIT)
# =============================================================================


def test_gate_a_server_initiated_drain_and_capacity_recovery_no_client_close() -> None:
    """Gate A: Verify server terminates connection with 1000 after drain, recovering all capacity without client ws.close()."""

    async def _test() -> None:
        port = _get_free_port()
        config = CoreConfig(ingress_mode=INGRESS_MODE_SYNTHETIC)
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
                tenant_id="tenant_gate_a",
                call_id="call_gate_a_01",
                allowed_scopes=["voice.stream"],
            )

            async with websockets.connect(
                uri, additional_headers={"Authorization": f"Bearer {token}"}
            ) as ws:
                await ws.recv()  # session.ready
                await ws.send(json.dumps({"event": EVENT_CALL_START, "call_id": "call_gate_a_01"}))
                await ws.recv()  # call.started

                # Send 3 audio frames
                for _i in range(3):
                    await ws.send(b"\x33" * PCM16LE_FRAME_BYTES)
                    echo = await ws.recv()
                    assert echo == b"\x33" * PCM16LE_FRAME_BYTES

                # Client sends call.end and NEVER closes socket itself
                await ws.send(json.dumps({"event": EVENT_CALL_END, "call_id": "call_gate_a_01"}))

                # Client receives call.ended
                ended_msg = await ws.recv()
                assert isinstance(ended_msg, str)
                ended_data = json.loads(ended_msg)
                assert ended_data["event"] == EVENT_CALL_ENDED

                # Server closes socket with code 1000
                with pytest.raises(websockets.exceptions.ConnectionClosedOK) as exc:
                    await ws.recv()
                assert exc.value.rcvd is not None
                assert exc.value.rcvd.code == 1000

        # Verify admission active connections = 0, active calls = 0
        await asyncio.sleep(0.05)
        assert admission.snapshot().active_connections == 0
        assert len(auth_bridge._active_calls) == 0

    asyncio.run(_test())


def test_gate_b_full_inbound_queue_eof_preservation() -> None:
    """Gate B: Verify push_inbound_eof does not drop EOF marker when inbound queue is full."""

    async def _test() -> None:
        class MockWS:
            async def send_bytes(self, data: bytes) -> None:
                pass

            async def send_text(self, text: str) -> None:
                pass

        mock_ws = MockWS()
        context = _make_verified_context()
        adapter = WebSocketTelephonyMediaAdapter(
            websocket=mock_ws, context=context, queue_capacity=2
        )

        frame1 = AudioFrame(
            payload=b"\x01" * 640,
            format=AudioFormat.PCM16LE,
            sample_rate=16000,
            channels=1,
            sequence=0,
            timestamp_ms=0.0,
        )
        frame2 = AudioFrame(
            payload=b"\x02" * 640,
            format=AudioFormat.PCM16LE,
            sample_rate=16000,
            channels=1,
            sequence=1,
            timestamp_ms=20.0,
        )

        await adapter.push_inbound_frame(frame1)
        await adapter.push_inbound_frame(frame2)
        assert adapter.inbound_queue_depth == 2

        # In concurrent task, push EOF into full queue
        eof_task = asyncio.create_task(adapter.push_inbound_eof(timeout=2.0))
        await asyncio.sleep(0.01)
        assert not eof_task.done()

        # Read first frame: frees queue space, allowing EOF to be enqueued
        recv1 = await adapter.receive_frame()
        assert recv1 is not None
        assert recv1.payload == b"\x01" * 640

        await eof_task
        assert eof_task.done()

        # Read second frame
        recv2 = await adapter.receive_frame()
        assert recv2 is not None
        assert recv2.payload == b"\x02" * 640

        # Read clean EOF (None)
        recv3 = await adapter.receive_frame()
        assert recv3 is None

    asyncio.run(_test())


def test_gate_b_full_ai_output_queue_eof_preservation() -> None:
    """Gate B: Verify BoundedFakeAIEchoPort push_eof does not drop EOF when output queue is full."""

    async def _test() -> None:
        port = BoundedFakeAIEchoPort(queue_capacity=2, backpressure_timeout=2.0)

        frame1 = AudioFrame(
            payload=b"\x01" * 640,
            format=AudioFormat.PCM16LE,
            sample_rate=16000,
            channels=1,
            sequence=0,
            timestamp_ms=0.0,
        )
        frame2 = AudioFrame(
            payload=b"\x02" * 640,
            format=AudioFormat.PCM16LE,
            sample_rate=16000,
            channels=1,
            sequence=1,
            timestamp_ms=20.0,
        )

        await port.send_frame(frame1)
        await port.send_frame(frame2)
        assert port.queue_depth == 2

        # In concurrent task, push EOF into full queue
        eof_task = asyncio.create_task(port.push_eof(timeout=2.0))
        await asyncio.sleep(0.01)
        assert not eof_task.done()

        # Drain frame 1
        recv1 = await port.receive_frame()
        assert recv1 is not None
        assert recv1.payload == b"\x01" * 640

        await eof_task
        assert eof_task.done()

        # Drain frame 2
        recv2 = await port.receive_frame()
        assert recv2 is not None
        assert recv2.payload == b"\x02" * 640

        # Drain clean EOF
        recv3 = await port.receive_frame()
        assert recv3 is None

    asyncio.run(_test())


def test_gate_b_slow_ai_exact_frame_count_and_ordering() -> None:
    """Gate B: Verify slow AI produces exact frame count and ordering during early call.end."""

    async def _test() -> None:
        port_num = _get_free_port()
        config = CoreConfig(ingress_mode=INGRESS_MODE_SYNTHETIC)
        admission = AdmissionController(config)
        verifier = SyntheticTokenVerifier(secret_key=TEST_SECRET_KEY)
        auth_bridge = SyntheticAuthBridge(verifier=verifier)

        class SlowFakeAIPort(BoundedFakeAIEchoPort):
            async def send_frame(self, frame: AudioFrame) -> None:
                await asyncio.sleep(0.02)  # artificial delay
                await super().send_frame(frame)

        app = create_gateway_app(
            config=config,
            admission_controller=admission,
            synthetic_auth_bridge=auth_bridge,
            enable_synthetic_media=True,
            ai_media_port_factory=SlowFakeAIPort,
        )

        async with _run_test_server(app, port_num) as base_ws_url:
            uri = f"{base_ws_url}{SYNTHETIC_MEDIA_WS_ENDPOINT_PATH}"
            token = verifier.issue_token(
                provider_id="smoke_provider",
                tenant_id="tenant_slow_ai",
                call_id="call_slow_ai_01",
                allowed_scopes=["voice.stream"],
            )

            async with websockets.connect(
                uri, additional_headers={"Authorization": f"Bearer {token}"}
            ) as ws:
                await ws.recv()  # session.ready
                await ws.send(json.dumps({"event": EVENT_CALL_START, "call_id": "call_slow_ai_01"}))
                await ws.recv()  # call.started

                # Send 6 frames
                expected_payloads: list[bytes] = []
                for i in range(6):
                    payload = f"FRAME_{i:02d}_".encode("ascii") + b"\x55" * (
                        640 - len(f"FRAME_{i:02d}_")
                    )
                    expected_payloads.append(payload)
                    await ws.send(payload)

                # Send call.end immediately
                await ws.send(json.dumps({"event": EVENT_CALL_END, "call_id": "call_slow_ai_01"}))

                # Collect all received items
                received_items: list[str | bytes] = []
                while True:
                    try:
                        msg = await asyncio.wait_for(ws.recv(), timeout=3.0)
                        received_items.append(msg)
                    except (websockets.exceptions.ConnectionClosed, TimeoutError):
                        break

                binary_frames = [m for m in received_items if isinstance(m, bytes)]
                text_frames = [m for m in received_items if isinstance(m, str)]

                assert len(binary_frames) == 6
                assert binary_frames == expected_payloads
                assert len(text_frames) == 1
                ended_json = json.loads(text_frames[0])
                assert ended_json["event"] == EVENT_CALL_ENDED

        await asyncio.sleep(0.05)
        assert admission.snapshot().active_connections == 0
        assert len(auth_bridge._active_calls) == 0

    asyncio.run(_test())


def test_gate_b_drain_timeout_closes_with_1011_no_call_ended() -> None:
    """Gate B & D: Verify drain timeout closes with 1011 and never sends call.ended."""

    async def _test() -> None:
        port_num = _get_free_port()
        config = CoreConfig(ingress_mode=INGRESS_MODE_SYNTHETIC)
        admission = AdmissionController(config)
        verifier = SyntheticTokenVerifier(secret_key=TEST_SECRET_KEY)
        auth_bridge = SyntheticAuthBridge(verifier=verifier)

        # AI port that hangs indefinitely in receive_frame
        class HangingFakeAIPort(BoundedFakeAIEchoPort):
            async def receive_frame(self) -> AudioFrame | None:
                await asyncio.sleep(10.0)
                return None

        app = create_gateway_app(
            config=config,
            admission_controller=admission,
            synthetic_auth_bridge=auth_bridge,
            enable_synthetic_media=True,
            ai_media_port_factory=HangingFakeAIPort,
        )
        app.state.synthetic_drain_timeout = 0.1  # 100ms drain deadline

        async with _run_test_server(app, port_num) as base_ws_url:
            uri = f"{base_ws_url}{SYNTHETIC_MEDIA_WS_ENDPOINT_PATH}"
            token = verifier.issue_token(
                provider_id="smoke_provider",
                tenant_id="tenant_hang",
                call_id="call_hang_01",
                allowed_scopes=["voice.stream"],
            )

            async with websockets.connect(
                uri, additional_headers={"Authorization": f"Bearer {token}"}
            ) as ws:
                await ws.recv()  # session.ready
                await ws.send(json.dumps({"event": EVENT_CALL_START, "call_id": "call_hang_01"}))
                await ws.recv()  # call.started

                # Send 1 frame
                await ws.send(b"\x00" * PCM16LE_FRAME_BYTES)

                # Send call.end
                await ws.send(json.dumps({"event": EVENT_CALL_END, "call_id": "call_hang_01"}))

                received_items: list[str | bytes] = []
                close_code: int | None = None
                while True:
                    try:
                        msg = await asyncio.wait_for(ws.recv(), timeout=2.0)
                        received_items.append(msg)
                    except websockets.exceptions.ConnectionClosed as exc:
                        close_code = exc.rcvd.code if exc.rcvd else exc.code
                        break
                    except TimeoutError:
                        break

                # call.ended must NOT have been sent on drain failure
                has_ended = any(isinstance(m, str) and "call.ended" in m for m in received_items)
                assert has_ended is False
                assert close_code == 1011

        await asyncio.sleep(0.05)
        assert admission.snapshot().active_connections == 0
        assert len(auth_bridge._active_calls) == 0

    asyncio.run(_test())


def test_gate_c_factory_failure_releases_admission_and_ports() -> None:
    """Gate C: Verify ai_media_port_factory failure releases admission lease and closes cleanly with 1011."""
    verifier = SyntheticTokenVerifier(secret_key=TEST_SECRET_KEY)
    auth_bridge = SyntheticAuthBridge(verifier=verifier)
    admission = AdmissionController(CoreConfig(ingress_mode=INGRESS_MODE_SYNTHETIC))

    def _failing_ai_factory() -> Any:
        raise RuntimeError("Synchronous AI factory failure")

    app = create_gateway_app(
        config=CoreConfig(ingress_mode=INGRESS_MODE_SYNTHETIC),
        admission_controller=admission,
        synthetic_auth_bridge=auth_bridge,
        enable_synthetic_media=True,
        ai_media_port_factory=_failing_ai_factory,
    )
    client = TestClient(app)

    token = verifier.issue_token(
        provider_id="smoke_provider",
        tenant_id="tenant_crash",
        call_id="call_crash_01",
        allowed_scopes=["voice.stream"],
    )

    with pytest.raises(WebSocketDisconnect) as exc:
        with client.websocket_connect(
            SYNTHETIC_MEDIA_WS_ENDPOINT_PATH,
            headers={"Authorization": f"Bearer {token}"},
        ):
            pass

    assert exc.value.code == 1011
    # Admission lease must be recovered immediately
    assert admission.snapshot().active_connections == 0
    assert len(auth_bridge._active_calls) == 0


def test_gate_c_bridge_construction_failure_releases_admission_and_closes_ports(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Gate C: Verify VoiceBridgeSession constructor failure releases admission lease and closes created ports."""
    verifier = SyntheticTokenVerifier(secret_key=TEST_SECRET_KEY)
    auth_bridge = SyntheticAuthBridge(verifier=verifier)
    admission = AdmissionController(CoreConfig(ingress_mode=INGRESS_MODE_SYNTHETIC))

    created_ai_port: BoundedFakeAIEchoPort | None = None

    def _tracking_factory() -> BoundedFakeAIEchoPort:
        nonlocal created_ai_port
        created_ai_port = BoundedFakeAIEchoPort()
        return created_ai_port

    def _exploding_bridge_init(*args: Any, **kwargs: Any) -> None:
        raise RuntimeError("Simulated bridge constructor crash")

    monkeypatch.setattr(VoiceBridgeSession, "__init__", _exploding_bridge_init)

    app = create_gateway_app(
        config=CoreConfig(ingress_mode=INGRESS_MODE_SYNTHETIC),
        admission_controller=admission,
        synthetic_auth_bridge=auth_bridge,
        enable_synthetic_media=True,
        ai_media_port_factory=_tracking_factory,
    )
    client = TestClient(app)

    token = verifier.issue_token(
        provider_id="smoke_provider",
        tenant_id="tenant_crash2",
        call_id="call_crash_02",
        allowed_scopes=["voice.stream"],
    )

    with pytest.raises(WebSocketDisconnect) as exc:
        with client.websocket_connect(
            SYNTHETIC_MEDIA_WS_ENDPOINT_PATH,
            headers={"Authorization": f"Bearer {token}"},
        ):
            pass

    assert exc.value.code == 1011
    assert admission.snapshot().active_connections == 0
    assert len(auth_bridge._active_calls) == 0
    assert created_ai_port is not None
    assert created_ai_port.is_closed is True


def test_gate_c_resource_registration_failure_releases_admission(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Gate C: Verify ConnectionSupervisor.register_resource failure releases admission lease."""
    verifier = SyntheticTokenVerifier(secret_key=TEST_SECRET_KEY)
    auth_bridge = SyntheticAuthBridge(verifier=verifier)
    admission = AdmissionController(CoreConfig(ingress_mode=INGRESS_MODE_SYNTHETIC))

    def _exploding_register_resource(self: Any, callback: Any) -> None:
        raise RuntimeError("Supervisor registration exploded")

    monkeypatch.setattr(ConnectionSupervisor, "register_resource", _exploding_register_resource)

    app = create_gateway_app(
        config=CoreConfig(ingress_mode=INGRESS_MODE_SYNTHETIC),
        admission_controller=admission,
        synthetic_auth_bridge=auth_bridge,
        enable_synthetic_media=True,
    )
    client = TestClient(app)

    token = verifier.issue_token(
        provider_id="smoke_provider",
        tenant_id="tenant_crash3",
        call_id="call_crash_03",
        allowed_scopes=["voice.stream"],
    )

    with pytest.raises(WebSocketDisconnect) as exc:
        with client.websocket_connect(
            SYNTHETIC_MEDIA_WS_ENDPOINT_PATH,
            headers={"Authorization": f"Bearer {token}"},
        ):
            pass

    assert exc.value.code == 1011
    assert admission.snapshot().active_connections == 0
    assert len(auth_bridge._active_calls) == 0


def test_gate_c_cleanup_exception_in_one_port_does_not_block_others() -> None:
    """Gate C: Verify an exception in one port's close() does not prevent remaining cleanups."""
    verifier = SyntheticTokenVerifier(secret_key=TEST_SECRET_KEY)
    auth_bridge = SyntheticAuthBridge(verifier=verifier)
    admission = AdmissionController(CoreConfig(ingress_mode=INGRESS_MODE_SYNTHETIC))

    class ExplodingCloseAIPort(BoundedFakeAIEchoPort):
        async def close(self) -> None:
            await super().close()
            raise RuntimeError("Port close exploded intentionally")

    app = create_gateway_app(
        config=CoreConfig(ingress_mode=INGRESS_MODE_SYNTHETIC),
        admission_controller=admission,
        synthetic_auth_bridge=auth_bridge,
        enable_synthetic_media=True,
        ai_media_port_factory=ExplodingCloseAIPort,
    )
    client = TestClient(app)

    token = verifier.issue_token(
        provider_id="smoke_provider",
        tenant_id="tenant_cleanup_fail",
        call_id="call_clean_01",
        allowed_scopes=["voice.stream"],
    )

    with client.websocket_connect(
        SYNTHETIC_MEDIA_WS_ENDPOINT_PATH,
        headers={"Authorization": f"Bearer {token}"},
    ) as ws:
        ws.receive_json()  # session.ready
        ws.send_json({"event": EVENT_CALL_START, "call_id": "call_clean_01"})
        ws.receive_json()  # call.started
        ws.send_json({"event": EVENT_CALL_END, "call_id": "call_clean_01"})
        # Because ExplodingCloseAIPort explodes during close, bridge fails
        # and server terminates with code 1011 (no call.ended sent!)
        with pytest.raises(WebSocketDisconnect) as exc:
            ws.receive_json()
        assert exc.value.code == 1011

    # All resources and leases must still be recovered
    assert admission.snapshot().active_connections == 0
    assert len(auth_bridge._active_calls) == 0


def test_gate_c_unauthenticated_connection_never_invokes_ai_factory() -> None:
    """Gate C: Verify unauthenticated connection is rejected before invoking AI port factory."""
    verifier = SyntheticTokenVerifier(secret_key=TEST_SECRET_KEY)
    auth_bridge = SyntheticAuthBridge(verifier=verifier)
    factory_calls = 0

    def _factory() -> BoundedFakeAIEchoPort:
        nonlocal factory_calls
        factory_calls += 1
        return BoundedFakeAIEchoPort()

    app = create_gateway_app(
        config=CoreConfig(ingress_mode=INGRESS_MODE_SYNTHETIC),
        synthetic_auth_bridge=auth_bridge,
        enable_synthetic_media=True,
        ai_media_port_factory=_factory,
    )
    client = TestClient(app)

    # 1. No Authorization header
    with pytest.raises(WebSocketDisconnect) as exc1:
        with client.websocket_connect(SYNTHETIC_MEDIA_WS_ENDPOINT_PATH):
            pass
    assert exc1.value.code == 1008
    assert factory_calls == 0

    # 2. Invalid/tampered token
    with pytest.raises(WebSocketDisconnect) as exc2:
        with client.websocket_connect(
            SYNTHETIC_MEDIA_WS_ENDPOINT_PATH,
            headers={"Authorization": "Bearer totally_bogus_token"},
        ):
            pass
    assert exc2.value.code == 1008
    assert factory_calls == 0


def test_gate_d_voice_bridge_failure_closes_with_1011_no_call_ended() -> None:
    """Gate D: Verify voice bridge pump failure causes close code 1011 and never sends call.ended."""

    async def _test() -> None:
        port_num = _get_free_port()
        config = CoreConfig(ingress_mode=INGRESS_MODE_SYNTHETIC)
        admission = AdmissionController(config)
        verifier = SyntheticTokenVerifier(secret_key=TEST_SECRET_KEY)
        auth_bridge = SyntheticAuthBridge(verifier=verifier)

        class ExplodingSendAIPort(BoundedFakeAIEchoPort):
            async def send_frame(self, frame: AudioFrame) -> None:
                raise VoiceDomainError("Simulated upstream backend crash")

        app = create_gateway_app(
            config=config,
            admission_controller=admission,
            synthetic_auth_bridge=auth_bridge,
            enable_synthetic_media=True,
            ai_media_port_factory=ExplodingSendAIPort,
        )

        async with _run_test_server(app, port_num) as base_ws_url:
            uri = f"{base_ws_url}{SYNTHETIC_MEDIA_WS_ENDPOINT_PATH}"
            token = verifier.issue_token(
                provider_id="smoke_provider",
                tenant_id="tenant_bridge_err",
                call_id="call_bridge_err_01",
                allowed_scopes=["voice.stream"],
            )

            async with websockets.connect(
                uri, additional_headers={"Authorization": f"Bearer {token}"}
            ) as ws:
                await ws.recv()  # session.ready
                await ws.send(
                    json.dumps({"event": EVENT_CALL_START, "call_id": "call_bridge_err_01"})
                )
                await ws.recv()  # call.started

                # Send frame which will trigger ExplodingSendAIPort
                await ws.send(b"\x00" * PCM16LE_FRAME_BYTES)

                # Send call.end
                await ws.send(
                    json.dumps({"event": EVENT_CALL_END, "call_id": "call_bridge_err_01"})
                )

                received_items: list[str | bytes] = []
                close_code: int | None = None
                while True:
                    try:
                        msg = await asyncio.wait_for(ws.recv(), timeout=2.0)
                        received_items.append(msg)
                    except websockets.exceptions.ConnectionClosed as exc:
                        close_code = exc.rcvd.code if exc.rcvd else exc.code
                        break
                    except TimeoutError:
                        break

                has_ended = any(isinstance(m, str) and "call.ended" in m for m in received_items)
                assert has_ended is False
                assert close_code == 1011

        await asyncio.sleep(0.05)
        assert admission.snapshot().active_connections == 0
        assert len(auth_bridge._active_calls) == 0

    asyncio.run(_test())


def test_gate_a_upstream_bridge_failure_proactively_terminates_without_client_call_end() -> None:
    """Gate A: Upstream bridge failure during streaming terminates transport with 1011 without client call.end."""

    async def _test() -> None:
        port_num = _get_free_port()
        config = CoreConfig(ingress_mode=INGRESS_MODE_SYNTHETIC)
        admission = AdmissionController(config)
        verifier = SyntheticTokenVerifier(secret_key=TEST_SECRET_KEY)
        auth_bridge = SyntheticAuthBridge(verifier=verifier)

        class ExplodingSendAIPort(BoundedFakeAIEchoPort):
            async def send_frame(self, frame: AudioFrame) -> None:
                raise VoiceDomainError("Simulated upstream backend crash")

        coordinator = CapturingCoordinator(admission)
        app = create_gateway_app(
            config=config,
            admission_controller=admission,
            drain_coordinator=coordinator,
            synthetic_auth_bridge=auth_bridge,
            enable_synthetic_media=True,
            ai_media_port_factory=ExplodingSendAIPort,
        )

        async with _run_test_server(app, port_num) as base_ws_url:
            uri = f"{base_ws_url}{SYNTHETIC_MEDIA_WS_ENDPOINT_PATH}"
            token = verifier.issue_token(
                provider_id="smoke_provider",
                tenant_id="tenant_gate_a",
                call_id="call_gate_a_01",
                allowed_scopes=["voice.stream"],
            )

            async with websockets.connect(
                uri, additional_headers={"Authorization": f"Bearer {token}"}
            ) as ws:
                await ws.recv()  # session.ready
                await ws.send(json.dumps({"event": EVENT_CALL_START, "call_id": "call_gate_a_01"}))
                await ws.recv()  # call.started

                # Send 1 audio frame to trigger the AI backend crash
                await ws.send(b"\x00" * PCM16LE_FRAME_BYTES)

                # Client does NOT send call.end! Server must proactively terminate.
                received_items: list[str | bytes] = []
                close_code: int | None = None
                while True:
                    try:
                        msg = await asyncio.wait_for(ws.recv(), timeout=2.0)
                        received_items.append(msg)
                    except websockets.exceptions.ConnectionClosed as exc:
                        close_code = exc.rcvd.code if exc.rcvd else exc.code
                        break

                has_ended = any(isinstance(m, str) and "call.ended" in m for m in received_items)
                assert has_ended is False
                assert close_code == 1011

        await asyncio.sleep(0.05)
        # All capacity and active call registration must be recovered
        assert admission.snapshot().active_connections == 0
        assert len(auth_bridge._active_calls) == 0

        # Supervisor must report FAILED / CLEANUP_FAILED
        assert len(coordinator.captured) == 1
        sup = coordinator.captured[0]
        assert sup.outcome is not None
        assert sup.outcome.state == ConnectionState.FAILED
        assert sup.outcome.reason == TerminalReason.CLEANUP_FAILED

    asyncio.run(_test())


def test_gate_a_session_ping_rejected_after_bridge_failure_no_pong() -> None:
    """Gate A: Client sends session.ping after bridge failure; server rejects with 1011 and never sends pong."""

    async def _test() -> None:
        port_num = _get_free_port()
        config = CoreConfig(ingress_mode=INGRESS_MODE_SYNTHETIC)
        admission = AdmissionController(config)
        verifier = SyntheticTokenVerifier(secret_key=TEST_SECRET_KEY)
        auth_bridge = SyntheticAuthBridge(verifier=verifier)

        class ExplodingSendAIPort(BoundedFakeAIEchoPort):
            async def send_frame(self, frame: AudioFrame) -> None:
                raise VoiceDomainError("Simulated upstream backend crash")

        app = create_gateway_app(
            config=config,
            admission_controller=admission,
            synthetic_auth_bridge=auth_bridge,
            enable_synthetic_media=True,
            ai_media_port_factory=ExplodingSendAIPort,
        )

        async with _run_test_server(app, port_num) as base_ws_url:
            uri = f"{base_ws_url}{SYNTHETIC_MEDIA_WS_ENDPOINT_PATH}"
            token = verifier.issue_token(
                provider_id="smoke_provider",
                tenant_id="tenant_gate_a_ping",
                call_id="call_gate_a_ping_01",
                allowed_scopes=["voice.stream"],
            )

            async with websockets.connect(
                uri, additional_headers={"Authorization": f"Bearer {token}"}
            ) as ws:
                await ws.recv()  # session.ready
                await ws.send(
                    json.dumps({"event": EVENT_CALL_START, "call_id": "call_gate_a_ping_01"})
                )
                await ws.recv()  # call.started

                # Send frame causing bridge failure
                await ws.send(b"\x00" * PCM16LE_FRAME_BYTES)

                # Wait 50ms for failure to propagate in bridge
                await asyncio.sleep(0.05)

                # Attempt to keep session alive by sending session.ping
                try:
                    await ws.send(json.dumps({"event": EVENT_SESSION_PING, "seq": 999}))
                except websockets.exceptions.ConnectionClosed:
                    pass

                received_items: list[str | bytes] = []
                close_code: int | None = None
                while True:
                    try:
                        msg = await asyncio.wait_for(ws.recv(), timeout=2.0)
                        received_items.append(msg)
                    except websockets.exceptions.ConnectionClosed as exc:
                        close_code = exc.rcvd.code if exc.rcvd else exc.code
                        break
                    except TimeoutError:
                        break

                has_pong = any(isinstance(m, str) and "session.pong" in m for m in received_items)
                has_ended = any(isinstance(m, str) and "call.ended" in m for m in received_items)
                assert has_pong is False
                assert has_ended is False
                assert close_code == 1011

        await asyncio.sleep(0.05)
        assert admission.snapshot().active_connections == 0
        assert len(auth_bridge._active_calls) == 0

    asyncio.run(_test())


def test_gate_b_ai_port_cleanup_error_propagates_to_core_supervisor() -> None:
    """Gate B: Exception in AI port close() propagates to Core supervisor as CLEANUP_FAILED."""
    verifier = SyntheticTokenVerifier(secret_key=TEST_SECRET_KEY)
    auth_bridge = SyntheticAuthBridge(verifier=verifier)
    admission = AdmissionController(CoreConfig(ingress_mode=INGRESS_MODE_SYNTHETIC))

    class ExplodingCloseAIPort(BoundedFakeAIEchoPort):
        async def close(self) -> None:
            await super().close()
            raise RuntimeError("AI port close exploded intentionally")

    coordinator = CapturingCoordinator(admission)
    app = create_gateway_app(
        config=CoreConfig(ingress_mode=INGRESS_MODE_SYNTHETIC),
        admission_controller=admission,
        drain_coordinator=coordinator,
        synthetic_auth_bridge=auth_bridge,
        enable_synthetic_media=True,
        ai_media_port_factory=ExplodingCloseAIPort,
    )

    client = TestClient(app)
    token = verifier.issue_token(
        provider_id="smoke_provider",
        tenant_id="tenant_cleanup_ai_err",
        call_id="call_clean_ai_01",
        allowed_scopes=["voice.stream"],
    )

    with client.websocket_connect(
        SYNTHETIC_MEDIA_WS_ENDPOINT_PATH,
        headers={"Authorization": f"Bearer {token}"},
    ) as ws:
        ws.receive_json()  # session.ready
        ws.send_json({"event": EVENT_CALL_START, "call_id": "call_clean_ai_01"})
        ws.receive_json()  # call.started
        ws.send_json({"event": EVENT_CALL_END, "call_id": "call_clean_ai_01"})
        with pytest.raises(WebSocketDisconnect) as exc:
            ws.receive_json()
        assert exc.value.code == 1011

    assert admission.snapshot().active_connections == 0
    assert len(auth_bridge._active_calls) == 0

    assert len(coordinator.captured) == 1
    sup = coordinator.captured[0]
    assert sup.outcome is not None
    assert sup.outcome.state == ConnectionState.FAILED
    assert sup.outcome.reason == TerminalReason.CLEANUP_FAILED


def test_gate_b_telephony_adapter_cleanup_error_propagates_to_core_supervisor() -> None:
    """Gate B: Exception in telephony adapter close() propagates to Core supervisor as CLEANUP_FAILED."""
    verifier = SyntheticTokenVerifier(secret_key=TEST_SECRET_KEY)
    auth_bridge = SyntheticAuthBridge(verifier=verifier)
    admission = AdmissionController(CoreConfig(ingress_mode=INGRESS_MODE_SYNTHETIC))

    coordinator = CapturingCoordinator(admission)
    app = create_gateway_app(
        config=CoreConfig(ingress_mode=INGRESS_MODE_SYNTHETIC),
        admission_controller=admission,
        drain_coordinator=coordinator,
        synthetic_auth_bridge=auth_bridge,
        enable_synthetic_media=True,
    )

    orig_adapter_close = WebSocketTelephonyMediaAdapter.close

    async def _exploding_close(self: WebSocketTelephonyMediaAdapter) -> None:
        await orig_adapter_close(self)
        raise RuntimeError("Telephony adapter close exploded intentionally")

    WebSocketTelephonyMediaAdapter.close = _exploding_close  # type: ignore[method-assign]

    try:
        client = TestClient(app)
        token = verifier.issue_token(
            provider_id="smoke_provider",
            tenant_id="tenant_cleanup_tel_err",
            call_id="call_clean_tel_01",
            allowed_scopes=["voice.stream"],
        )

        with client.websocket_connect(
            SYNTHETIC_MEDIA_WS_ENDPOINT_PATH,
            headers={"Authorization": f"Bearer {token}"},
        ) as ws:
            ws.receive_json()  # session.ready
            ws.send_json({"event": EVENT_CALL_START, "call_id": "call_clean_tel_01"})
            ws.receive_json()  # call.started
            ws.send_json({"event": EVENT_CALL_END, "call_id": "call_clean_tel_01"})
            with pytest.raises(WebSocketDisconnect) as exc:
                ws.receive_json()
            assert exc.value.code == 1011
    finally:
        WebSocketTelephonyMediaAdapter.close = orig_adapter_close  # type: ignore[method-assign]

    assert admission.snapshot().active_connections == 0
    assert len(auth_bridge._active_calls) == 0

    assert len(coordinator.captured) == 1
    sup = coordinator.captured[0]
    assert sup.outcome is not None
    assert sup.outcome.state == ConnectionState.FAILED
    assert sup.outcome.reason == TerminalReason.CLEANUP_FAILED


def test_gate_d_timeout_sanitization_helper() -> None:
    """Gate D: _resolve_positive_float sanitizes non-finite, non-positive, or invalid values."""
    default = 10.0
    assert _resolve_positive_float(float("nan"), default) == default
    assert _resolve_positive_float(float("inf"), default) == default
    assert _resolve_positive_float(float("-inf"), default) == default
    assert _resolve_positive_float(-5.0, default) == default
    assert _resolve_positive_float(0.0, default) == default
    assert _resolve_positive_float(-0.0, default) == default
    assert _resolve_positive_float(None, default) == default
    assert _resolve_positive_float(True, default) == default
    assert _resolve_positive_float(False, default) == default
    assert _resolve_positive_float("invalid", default) == default
    assert _resolve_positive_float(25.5, default) == 25.5
    assert _resolve_positive_float(1, default) == 1.0


def test_gate_d_invalid_timeouts_fall_back_safely_in_endpoint() -> None:
    """Gate D: Non-finite / non-positive timeouts fall back to defaults and allow normal completion."""
    verifier = SyntheticTokenVerifier(secret_key=TEST_SECRET_KEY)
    auth_bridge = SyntheticAuthBridge(verifier=verifier)
    admission = AdmissionController(CoreConfig(ingress_mode=INGRESS_MODE_SYNTHETIC))

    app = create_gateway_app(
        config=CoreConfig(ingress_mode=INGRESS_MODE_SYNTHETIC),
        admission_controller=admission,
        synthetic_auth_bridge=auth_bridge,
        enable_synthetic_media=True,
    )
    # Inject invalid timeouts on app.state
    app.state.synthetic_drain_timeout = float("nan")
    app.state.synthetic_idle_timeout = -15.0
    app.state.synthetic_backpressure_timeout = -15.0
    app.state.synthetic_send_timeout = -15.0

    client = TestClient(app)
    token = verifier.issue_token(
        provider_id="smoke_provider",
        tenant_id="tenant_timeout_nan",
        call_id="call_timeout_nan_01",
        allowed_scopes=["voice.stream"],
    )

    with client.websocket_connect(
        SYNTHETIC_MEDIA_WS_ENDPOINT_PATH,
        headers={"Authorization": f"Bearer {token}"},
    ) as ws:
        ready = ws.receive_json()
        assert ready["event"] == EVENT_SESSION_READY
        ws.send_json({"event": EVENT_CALL_START, "call_id": "call_timeout_nan_01"})
        started = ws.receive_json()
        assert started["event"] == EVENT_CALL_STARTED
        ws.send_bytes(b"\x00" * PCM16LE_FRAME_BYTES)
        echoed = ws.receive_bytes()
        assert len(echoed) == PCM16LE_FRAME_BYTES
        ws.send_json({"event": EVENT_CALL_END, "call_id": "call_timeout_nan_01"})
        ended = ws.receive_json()
        assert ended["event"] == EVENT_CALL_ENDED

    assert admission.snapshot().active_connections == 0
    assert len(auth_bridge._active_calls) == 0


def test_gate_b_synthetic_send_timeout_constructor_validation() -> None:
    """Gate B: Verify WebSocketTelephonyMediaAdapter validates send_timeout parameter."""
    dummy_ws = object()
    ctx = _make_verified_context()

    # Valid configurations
    adapter1 = WebSocketTelephonyMediaAdapter(dummy_ws, ctx, send_timeout=None)
    assert adapter1.send_timeout is None
    adapter2 = WebSocketTelephonyMediaAdapter(dummy_ws, ctx, send_timeout=1.5)
    assert adapter2.send_timeout == 1.5

    # Invalid configurations
    with pytest.raises(VoiceDomainError, match="send_timeout"):
        WebSocketTelephonyMediaAdapter(dummy_ws, ctx, send_timeout=0.0)
    with pytest.raises(VoiceDomainError, match="send_timeout"):
        WebSocketTelephonyMediaAdapter(dummy_ws, ctx, send_timeout=-1.0)
    with pytest.raises(VoiceDomainError, match="send_timeout"):
        WebSocketTelephonyMediaAdapter(dummy_ws, ctx, send_timeout=float("nan"))
    with pytest.raises(VoiceDomainError, match="send_timeout"):
        WebSocketTelephonyMediaAdapter(dummy_ws, ctx, send_timeout=True)
    with pytest.raises(VoiceDomainError, match="send_timeout"):
        WebSocketTelephonyMediaAdapter(dummy_ws, ctx, send_timeout="invalid")  # type: ignore[arg-type]


def test_gate_b_synthetic_send_timeout_resolved_and_applied() -> None:
    """Gate B: Verify custom synthetic_send_timeout is passed to adapter."""
    verifier = SyntheticTokenVerifier(secret_key=TEST_SECRET_KEY)
    auth_bridge = SyntheticAuthBridge(verifier=verifier)
    admission = AdmissionController(CoreConfig(ingress_mode=INGRESS_MODE_SYNTHETIC))

    app = create_gateway_app(
        config=CoreConfig(ingress_mode=INGRESS_MODE_SYNTHETIC),
        admission_controller=admission,
        synthetic_auth_bridge=auth_bridge,
        enable_synthetic_media=True,
    )
    app.state.synthetic_send_timeout = 0.05

    client = TestClient(app)
    token = verifier.issue_token(
        provider_id="smoke_provider",
        tenant_id="tenant_send_custom",
        call_id="call_send_custom_01",
        allowed_scopes=["voice.stream"],
    )

    with client.websocket_connect(
        SYNTHETIC_MEDIA_WS_ENDPOINT_PATH,
        headers={"Authorization": f"Bearer {token}"},
    ) as ws:
        ready = ws.receive_json()
        assert ready["event"] == EVENT_SESSION_READY
        ws.send_json({"event": EVENT_CALL_START, "call_id": "call_send_custom_01"})
        started = ws.receive_json()
        assert started["event"] == EVENT_CALL_STARTED
        ws.send_bytes(b"\x00" * PCM16LE_FRAME_BYTES)
        echoed = ws.receive_bytes()
        assert len(echoed) == PCM16LE_FRAME_BYTES
        ws.send_json({"event": EVENT_CALL_END, "call_id": "call_send_custom_01"})
        ended = ws.receive_json()
        assert ended["event"] == EVENT_CALL_ENDED

    assert admission.snapshot().active_connections == 0
    assert len(auth_bridge._active_calls) == 0
