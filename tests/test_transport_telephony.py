"""Comprehensive tests for provider-neutral telephony control contracts and synthetic adapter (M3.2B).

Covers:
- Provider-neutral domain contracts: TelephonyCallState, TelephonyControlEvent, etc.
- SyntheticTelephonyAdapter unit tests: state transitions, capabilities, identity binding, and spoofing prevention.
- Protocol schema validation for call.start and call.end frames.
- Real loopback WebSocket end-to-end telephony lifecycle:
  1. Handshake and session.ready
  2. Call start and acknowledgment
  3. Ping/pong during active call
  4. Call end and acknowledgment
  5. Clean termination and capacity reclamation
- Concurrency, tenant isolation, and negative edge cases (invalid transitions, forged identity, unsupported ops).
"""

from __future__ import annotations

import asyncio
import json
import socket
import uuid
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Any, cast

import pytest
import uvicorn
import websockets

from kaiwen_gateway.core.admission import AdmissionController
from kaiwen_gateway.core.config import INGRESS_MODE_SYNTHETIC, CoreConfig
from kaiwen_gateway.core.draining import InstanceDrainCoordinator
from kaiwen_gateway.core.identity import VerifiedConnectionContext
from kaiwen_gateway.server.app import create_gateway_app
from kaiwen_gateway.transports.synthetic.adapter import (
    SUPPORTED_TELEPHONY_CAPABILITIES,
    UNSUPPORTED_TELEPHONY_CAPABILITIES,
    SyntheticTelephonyAdapter,
)
from kaiwen_gateway.transports.synthetic.auth import SyntheticAuthBridge
from kaiwen_gateway.transports.synthetic.protocol import (
    EVENT_CALL_END,
    EVENT_CALL_ENDED,
    EVENT_CALL_START,
    EVENT_CALL_STARTED,
    EVENT_SESSION_END,
    EVENT_SESSION_PING,
    EVENT_SESSION_PONG,
    EVENT_SESSION_READY,
    SYNTHETIC_WS_ENDPOINT_PATH,
    InvalidJsonError,
    InvalidMessageFormatError,
    UnsupportedEventError,
    parse_and_validate_client_frame,
)
from kaiwen_gateway.transports.synthetic.tokens import SyntheticTokenVerifier
from kaiwen_gateway.transports.telephony import (
    InvalidCallStateTransitionError,
    TelephonyCallEndedEvent,
    TelephonyCallStartedEvent,
    TelephonyCallState,
    TelephonyControlPort,
    UnsupportedOperationError,
)

TEST_SECRET_KEY = b"telephony_secret_key_minimum_16_bytes_32_chars!!"


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


def _make_context(
    *,
    tenant_id: str = "tenant_1",
    call_id: str = "call_1",
    provider_id: str = "synthetic",
) -> VerifiedConnectionContext:
    return VerifiedConnectionContext(
        connection_id=f"conn_{uuid.uuid4().hex[:12]}",
        provider_id=provider_id,
        tenant_id=tenant_id,
        call_id=call_id,
        allowed_scopes=frozenset({"voice.stream"}),
    )


def _get_call_state(adapter: SyntheticTelephonyAdapter) -> TelephonyCallState:
    return adapter.call_state


# ==============================================================================
# 1. Provider-Neutral Contracts & Unit Tests
# ==============================================================================


def test_telephony_call_states_and_events() -> None:
    """Verify TelephonyCallState enum and dataclass events."""
    assert TelephonyCallState.IDLE.value == "idle"
    assert TelephonyCallState.CONNECTED.value == "connected"
    assert TelephonyCallState.DISCONNECTED.value == "disconnected"
    assert TelephonyCallState.FAILED.value == "failed"

    evt_start = TelephonyCallStartedEvent(
        event_type="call_started",
        tenant_id="t1",
        provider_id="p1",
        call_id="c1",
        timestamp=123.45,
        stream_id="str_1",
        metadata={"codec": "synthetic_control_only"},
    )
    assert evt_start.event_type == "call_started"
    assert evt_start.tenant_id == "t1"
    assert evt_start.provider_id == "p1"
    assert evt_start.call_id == "c1"
    assert evt_start.timestamp == 123.45
    assert evt_start.stream_id == "str_1"
    assert evt_start.metadata == {"codec": "synthetic_control_only"}

    # Metadata mapping is strictly immutable (MappingProxyType)
    with pytest.raises(TypeError):
        cast(Any, evt_start.metadata)["new_key"] = "forbidden"

    evt_end = TelephonyCallEndedEvent(
        event_type="call_ended",
        tenant_id="t1",
        provider_id="p1",
        call_id="c1",
        timestamp=150.0,
        reason="caller_hangup",
        code=16,
    )
    assert evt_end.event_type == "call_ended"
    assert evt_end.reason == "caller_hangup"
    assert evt_end.code == 16


def test_telephony_exceptions_formatting() -> None:
    """Verify Telephony domain exceptions and formatted messages."""
    exc1 = InvalidCallStateTransitionError(TelephonyCallState.CONNECTED, "call.start")
    assert "Invalid telephony call transition" in str(exc1)
    assert exc1.current_state == TelephonyCallState.CONNECTED
    assert exc1.attempted_action == "call.start"

    exc2 = UnsupportedOperationError("call_transfer", provider="synthetic")
    assert "not supported by 'synthetic'" in str(exc2)
    assert exc2.operation == "call_transfer"
    assert exc2.provider == "synthetic"


# ==============================================================================
# 2. SyntheticTelephonyAdapter Unit Tests
# ==============================================================================


def test_synthetic_telephony_adapter_initialization_and_capabilities() -> None:
    """Verify adapter initialization, capability declaration, and protocol conformance."""
    ctx = _make_context(tenant_id="t_alpha", call_id="c_100", provider_id="syn_pvd")
    adapter = SyntheticTelephonyAdapter(ctx)

    assert isinstance(adapter, TelephonyControlPort)
    assert adapter.context is ctx
    assert adapter.tenant_id == "t_alpha"
    assert adapter.provider_id == "syn_pvd"
    assert adapter.call_id == "c_100"
    assert adapter.call_state == TelephonyCallState.IDLE
    assert adapter.stream_id is None

    # Capabilities
    assert adapter.supported_capabilities == SUPPORTED_TELEPHONY_CAPABILITIES
    assert adapter.is_capability_supported("call_start") is True
    assert adapter.is_capability_supported("call_end") is True
    assert adapter.is_capability_supported("ping_pong") is True
    assert adapter.is_capability_supported("call_transfer") is False
    assert adapter.is_capability_supported("dtmf") is False

    assert "call_transfer" in UNSUPPORTED_TELEPHONY_CAPABILITIES
    with pytest.raises(UnsupportedOperationError, match="call_transfer"):
        adapter.execute_unsupported_operation("call_transfer")


def test_synthetic_telephony_adapter_happy_path_transitions() -> None:
    """Verify normal IDLE -> CONNECTED -> DISCONNECTED lifecycle transitions."""
    ctx = _make_context(call_id="c_lifecycle")
    adapter = SyntheticTelephonyAdapter(ctx)

    # 1. Start call
    start_payload = {
        "event": EVENT_CALL_START,
        "stream_id": "stream_abc",
        "metadata": {"custom_tag": "test_tag"},
    }
    evt1, resp1 = adapter.handle_wire_event(start_payload)
    assert isinstance(evt1, TelephonyCallStartedEvent)
    assert evt1.call_id == "c_lifecycle"
    assert evt1.stream_id == "stream_abc"
    assert evt1.metadata == {"custom_tag": "test_tag"}
    assert _get_call_state(adapter) == TelephonyCallState.CONNECTED
    assert adapter.stream_id == "stream_abc"

    assert resp1["event"] == EVENT_CALL_STARTED
    assert resp1["call_id"] == "c_lifecycle"
    assert resp1["state"] == "connected"
    assert resp1["stream_id"] == "stream_abc"

    # 2. End call
    end_payload = {
        "event": EVENT_CALL_END,
        "reason": "customer_hangup",
        "code": 200,
    }
    evt2, resp2 = adapter.handle_wire_event(end_payload)
    assert isinstance(evt2, TelephonyCallEndedEvent)
    assert evt2.call_id == "c_lifecycle"
    assert evt2.reason == "customer_hangup"
    assert evt2.code == 200
    assert _get_call_state(adapter) == TelephonyCallState.DISCONNECTED

    assert resp2["event"] == EVENT_CALL_ENDED
    assert resp2["call_id"] == "c_lifecycle"
    assert resp2["state"] == "disconnected"
    assert resp2["reason"] == "customer_hangup"

    # 3. Duplicate call.end is idempotent
    evt3, resp3 = adapter.handle_wire_event(end_payload)
    assert isinstance(evt3, TelephonyCallEndedEvent)
    assert _get_call_state(adapter) == TelephonyCallState.DISCONNECTED
    assert resp3["idempotent"] is True


def test_synthetic_telephony_adapter_invalid_transitions() -> None:
    """Verify state machine rejects invalid call transitions fail closed."""
    ctx = _make_context(call_id="c_invalid_trans")
    adapter = SyntheticTelephonyAdapter(ctx)

    # Cannot end call before starting
    with pytest.raises(InvalidCallStateTransitionError, match="call.end"):
        adapter.handle_wire_event({"event": EVENT_CALL_END})
    assert _get_call_state(adapter) == TelephonyCallState.IDLE

    # Start call
    adapter.handle_wire_event({"event": EVENT_CALL_START})
    assert _get_call_state(adapter) == TelephonyCallState.CONNECTED

    # Cannot start call again (duplicate start is rejected)
    with pytest.raises(InvalidCallStateTransitionError, match="call.start"):
        adapter.handle_wire_event({"event": EVENT_CALL_START})

    # End call
    adapter.handle_wire_event({"event": EVENT_CALL_END})
    assert _get_call_state(adapter) == TelephonyCallState.DISCONNECTED

    # Cannot restart an ended call
    with pytest.raises(InvalidCallStateTransitionError, match="call.start"):
        adapter.handle_wire_event({"event": EVENT_CALL_START})


def test_synthetic_telephony_adapter_identity_spoofing_prevention() -> None:
    """Verify that client wire payloads cannot override or spoof verified context identity."""
    ctx = _make_context(tenant_id="tenant_legit", call_id="call_legit", provider_id="pvd_legit")
    adapter = SyntheticTelephonyAdapter(ctx)

    # Forged call_id
    with pytest.raises(InvalidMessageFormatError, match="call_id cannot override"):
        adapter.handle_wire_event({"event": EVENT_CALL_START, "call_id": "forged_call_id"})

    # Forged tenant_id
    with pytest.raises(InvalidMessageFormatError, match="tenant_id cannot override"):
        adapter.handle_wire_event({"event": EVENT_CALL_START, "tenant_id": "forged_tenant_id"})

    # Forged provider_id
    with pytest.raises(InvalidMessageFormatError, match="provider_id cannot override"):
        adapter.handle_wire_event({"event": EVENT_CALL_START, "provider_id": "forged_provider_id"})

    # Client-supplied allowed_scopes rejection
    with pytest.raises(InvalidMessageFormatError, match="allowed_scopes cannot override"):
        adapter.handle_wire_event({"event": EVENT_CALL_START, "allowed_scopes": ["voice.admin"]})

    # Legitimate matching identity in payload is accepted
    evt, resp = adapter.handle_wire_event(
        {
            "event": EVENT_CALL_START,
            "call_id": "call_legit",
            "tenant_id": "tenant_legit",
            "provider_id": "pvd_legit",
        }
    )
    assert evt.call_id == "call_legit"
    assert resp["call_id"] == "call_legit"


def test_synthetic_telephony_adapter_transport_teardown() -> None:
    """Verify adapter deterministically transitions active call to DISCONNECTED on transport teardown."""
    ctx = _make_context(call_id="c_teardown")
    adapter = SyntheticTelephonyAdapter(ctx)

    # 1. Idle call teardown does nothing
    assert adapter.terminate_call_on_transport_close(reason="transport_closed") is None
    assert _get_call_state(adapter) == TelephonyCallState.IDLE

    # 2. Start call
    adapter.handle_wire_event({"event": EVENT_CALL_START})
    assert _get_call_state(adapter) == TelephonyCallState.CONNECTED

    # 3. Teardown transitions to DISCONNECTED
    evt = adapter.terminate_call_on_transport_close(reason="session_ended")
    assert evt is not None
    assert evt.reason == "session_ended"
    assert _get_call_state(adapter) == TelephonyCallState.DISCONNECTED

    # 4. Subsequent teardown does nothing
    assert adapter.terminate_call_on_transport_close(reason="transport_closed") is None
    assert _get_call_state(adapter) == TelephonyCallState.DISCONNECTED


def test_synthetic_telephony_adapter_unsupported_operation_rejection() -> None:
    """Verify adapter explicitly rejects unsupported telephony operations."""
    ctx = _make_context()
    adapter = SyntheticTelephonyAdapter(ctx)

    for op in ("call_transfer", "dtmf", "hold", "conference"):
        with pytest.raises(UnsupportedOperationError, match=op):
            adapter.handle_wire_event({"event": op})

    with pytest.raises(UnsupportedEventError, match="unknown_custom_op"):
        adapter.handle_wire_event({"event": "unknown_custom_op"})


# ==============================================================================
# 3. Protocol Framing & Schema Validation Unit Tests
# ==============================================================================


def test_protocol_framing_call_start_and_end() -> None:
    """Verify parse_and_validate_client_frame on call.start and call.end schemas."""
    # Valid call.start
    f1 = parse_and_validate_client_frame(
        json.dumps(
            {"event": "call.start", "stream_id": "s1", "metadata": {"key": "val", "num": 10}}
        )
    )
    assert f1["event"] == "call.start"
    assert f1["stream_id"] == "s1"
    assert f1["metadata"]["key"] == "val"

    # Valid call.end
    f2 = parse_and_validate_client_frame(
        json.dumps({"event": "call.end", "reason": "normal", "code": 100})
    )
    assert f2["event"] == "call.end"
    assert f2["reason"] == "normal"
    assert f2["code"] == 100

    # Malformed call.start stream_id
    with pytest.raises(InvalidMessageFormatError, match="'stream_id' must be a non-empty string"):
        parse_and_validate_client_frame(json.dumps({"event": "call.start", "stream_id": ""}))
    with pytest.raises(InvalidMessageFormatError, match="'stream_id' must be a non-empty string"):
        parse_and_validate_client_frame(json.dumps({"event": "call.start", "stream_id": 123}))

    # Malformed metadata
    with pytest.raises(InvalidMessageFormatError, match="'metadata' must be a JSON object"):
        parse_and_validate_client_frame(json.dumps({"event": "call.start", "metadata": "bad"}))
    with pytest.raises(InvalidMessageFormatError, match="Metadata keys must be non-empty strings"):
        parse_and_validate_client_frame(
            json.dumps({"event": "call.start", "metadata": {"": "val"}})
        )
    with pytest.raises(InvalidMessageFormatError, match="Metadata values must be primitive"):
        parse_and_validate_client_frame(
            json.dumps({"event": "call.start", "metadata": {"key": {"nested": "obj"}}})
        )

    # Malformed call.end
    with pytest.raises(InvalidMessageFormatError, match="'reason' must be a non-empty string"):
        parse_and_validate_client_frame(json.dumps({"event": "call.end", "reason": ""}))
    with pytest.raises(InvalidMessageFormatError, match="'code' must be an integer"):
        parse_and_validate_client_frame(json.dumps({"event": "call.end", "code": "not_int"}))
    with pytest.raises(InvalidMessageFormatError, match="'code' must be an integer"):
        parse_and_validate_client_frame(json.dumps({"event": "call.end", "code": True}))
    with pytest.raises(InvalidMessageFormatError, match="'code' must be an integer"):
        parse_and_validate_client_frame(json.dumps({"event": "call.end", "code": -1}))
    with pytest.raises(InvalidMessageFormatError, match="'code' must be an integer"):
        parse_and_validate_client_frame(json.dumps({"event": "call.end", "code": 70000}))

    # Strict JSON: Rejection of NaN, Infinity, -Infinity
    with pytest.raises(InvalidJsonError, match="Non-standard JSON constant"):
        parse_and_validate_client_frame('{"event": "call.start", "metadata": {"val": NaN}}')
    with pytest.raises(InvalidJsonError, match="Non-standard JSON constant"):
        parse_and_validate_client_frame('{"event": "call.start", "metadata": {"val": Infinity}}')
    with pytest.raises(InvalidJsonError, match="Non-standard JSON constant"):
        parse_and_validate_client_frame('{"event": "call.start", "metadata": {"val": -Infinity}}')
    with pytest.raises(InvalidJsonError, match="Non-standard JSON constant"):
        parse_and_validate_client_frame(
            json.dumps({"event": "call.start", "metadata": {"num": float("nan")}})
        )
    with pytest.raises(InvalidJsonError, match="Non-standard JSON constant"):
        parse_and_validate_client_frame(
            json.dumps({"event": "call.start", "metadata": {"num": float("inf")}})
        )
    with pytest.raises(InvalidMessageFormatError, match="Metadata string values must be <= 256"):
        parse_and_validate_client_frame(
            json.dumps({"event": "call.start", "metadata": {"long": "x" * 257}})
        )


# ==============================================================================
# 4. End-to-End Real Loopback WebSocket Tests (Section 8 & 9)
# ==============================================================================


@pytest.mark.anyio
async def test_real_loopback_telephony_full_lifecycle_scenario() -> None:
    """Section 8: First complete end-to-end real loopback WebSocket telephony lifecycle.

    Sequence:
    1. Connect with valid synthetic token.
    2. Receive session.ready.
    3. Send call.start -> receive call.started.
    4. Send session.ping -> receive session.pong.
    5. Send call.end -> receive call.ended.
    6. Send session.end -> clean termination with code 1000.
    7. All capacity leases and active calls returned to zero.
    """
    port = _get_free_port()
    verifier = SyntheticTokenVerifier(TEST_SECRET_KEY)
    bridge = SyntheticAuthBridge(verifier)
    ctrl = AdmissionController()
    coord = InstanceDrainCoordinator(controller=ctrl)
    cfg = CoreConfig(ingress_mode=INGRESS_MODE_SYNTHETIC)
    app = create_gateway_app(config=cfg, drain_coordinator=coord, synthetic_auth_bridge=bridge)

    token = verifier.issue_token(
        provider_id="synthetic_carrier",
        tenant_id="tenant_omega",
        call_id="call_tel_001",
        allowed_scopes=["voice.stream"],
    )

    async with _run_server(app, port) as ws_base_url:
        ws_url = f"{ws_base_url}{SYNTHETIC_WS_ENDPOINT_PATH}"
        headers = {"Authorization": f"Bearer {token}"}

        async with websockets.connect(ws_url, additional_headers=headers) as ws:
            # 1. Server emits session.ready
            ready_raw = await asyncio.wait_for(ws.recv(), timeout=5.0)
            ready_data = json.loads(ready_raw)
            assert ready_data["event"] == EVENT_SESSION_READY
            assert ready_data["call_id"] == "call_tel_001"
            assert ready_data["tenant_id"] == "tenant_omega"
            assert ctrl.snapshot().active_connections == 1

            # 2. Client initiates telephony call
            start_frame = {
                "event": EVENT_CALL_START,
                "stream_id": "stream_carrier_42",
                "metadata": {"caller": "+10005550100", "ani": "anon"},
            }
            await ws.send(json.dumps(start_frame))

            # 3. Server acknowledges call start
            started_raw = await asyncio.wait_for(ws.recv(), timeout=5.0)
            started_data = json.loads(started_raw)
            assert started_data["event"] == EVENT_CALL_STARTED
            assert started_data["call_id"] == "call_tel_001"
            assert started_data["state"] == "connected"
            assert started_data["stream_id"] == "stream_carrier_42"

            # 4. Ping/pong during active call
            await ws.send(json.dumps({"event": EVENT_SESSION_PING, "seq": 101}))
            pong_raw = await asyncio.wait_for(ws.recv(), timeout=5.0)
            pong_data = json.loads(pong_raw)
            assert pong_data["event"] == EVENT_SESSION_PONG
            assert pong_data["seq"] == 101

            # 5. Client ends telephony call
            end_frame = {
                "event": EVENT_CALL_END,
                "reason": "completed",
                "code": 16,
            }
            await ws.send(json.dumps(end_frame))

            # 6. Server acknowledges call end
            ended_raw = await asyncio.wait_for(ws.recv(), timeout=5.0)
            ended_data = json.loads(ended_raw)
            assert ended_data["event"] == EVENT_CALL_ENDED
            assert ended_data["call_id"] == "call_tel_001"
            assert ended_data["state"] == "disconnected"
            assert ended_data["reason"] == "completed"

            # 7. Graceful session close
            await ws.send(json.dumps({"event": EVENT_SESSION_END}))
            try:
                await ws.recv()
            except websockets.exceptions.ConnectionClosedOK as close_exc:
                assert close_exc.rcvd is not None
                assert close_exc.rcvd.code == 1000
                assert "Session ended by client" in close_exc.rcvd.reason

    # Verify zero capacity leaks
    assert ctrl.snapshot().active_connections == 0
    assert coord.active_connections_count == 0
    assert len(bridge._active_calls) == 0


@pytest.mark.anyio
async def test_real_loopback_telephony_idempotent_duplicate_end() -> None:
    """Verify that duplicate call.end is accepted idempotently without crashing or dropping connection."""
    port = _get_free_port()
    verifier = SyntheticTokenVerifier(TEST_SECRET_KEY)
    bridge = SyntheticAuthBridge(verifier)
    ctrl = AdmissionController()
    coord = InstanceDrainCoordinator(controller=ctrl)
    cfg = CoreConfig(ingress_mode=INGRESS_MODE_SYNTHETIC)
    app = create_gateway_app(config=cfg, drain_coordinator=coord, synthetic_auth_bridge=bridge)

    token = verifier.issue_token(
        provider_id="carrier_b",
        tenant_id="tenant_b",
        call_id="call_dup_end",
        allowed_scopes=["voice.stream"],
    )

    async with _run_server(app, port) as ws_base_url:
        ws_url = f"{ws_base_url}{SYNTHETIC_WS_ENDPOINT_PATH}"
        headers = {"Authorization": f"Bearer {token}"}

        async with websockets.connect(ws_url, additional_headers=headers) as ws:
            await ws.recv()  # session.ready

            # Start call
            await ws.send(json.dumps({"event": EVENT_CALL_START}))
            resp1 = json.loads(await ws.recv())
            assert resp1["event"] == EVENT_CALL_STARTED

            # End call (first time)
            await ws.send(json.dumps({"event": EVENT_CALL_END, "reason": "hangup_1"}))
            resp2 = json.loads(await ws.recv())
            assert resp2["event"] == EVENT_CALL_ENDED
            assert resp2.get("idempotent") is None

            # End call (second time - duplicate)
            await ws.send(json.dumps({"event": EVENT_CALL_END, "reason": "hangup_2"}))
            resp3 = json.loads(await ws.recv())
            assert resp3["event"] == EVENT_CALL_ENDED
            assert resp3.get("idempotent") is True

            # Connection remains open and clean; terminate normally
            await ws.send(json.dumps({"event": EVENT_SESSION_END}))
            try:
                await ws.recv()
            except websockets.exceptions.ConnectionClosedOK:
                pass

    assert ctrl.snapshot().active_connections == 0


@pytest.mark.anyio
async def test_real_loopback_telephony_negative_forged_identity_fails_closed() -> None:
    """Section 9: Forged tenant or call identity in wire frame causes immediate fail-closed termination."""
    port = _get_free_port()
    verifier = SyntheticTokenVerifier(TEST_SECRET_KEY)
    bridge = SyntheticAuthBridge(verifier)
    ctrl = AdmissionController()
    coord = InstanceDrainCoordinator(controller=ctrl)
    cfg = CoreConfig(ingress_mode=INGRESS_MODE_SYNTHETIC)
    app = create_gateway_app(config=cfg, drain_coordinator=coord, synthetic_auth_bridge=bridge)

    token = verifier.issue_token(
        provider_id="pvd_safe",
        tenant_id="tenant_safe",
        call_id="call_safe_01",
        allowed_scopes=["voice.stream"],
    )

    async with _run_server(app, port) as ws_base_url:
        ws_url = f"{ws_base_url}{SYNTHETIC_WS_ENDPOINT_PATH}"
        headers = {"Authorization": f"Bearer {token}"}

        async with websockets.connect(ws_url, additional_headers=headers) as ws:
            await ws.recv()  # session.ready

            # Attempt to spoof tenant_id inside call.start frame
            bad_start = {
                "event": EVENT_CALL_START,
                "tenant_id": "victim_tenant_id",
            }
            await ws.send(json.dumps(bad_start))

            with pytest.raises(websockets.exceptions.ConnectionClosed) as exc:
                await ws.recv()

            assert exc.value.rcvd is not None
            assert exc.value.rcvd.code == 1007
            assert "tenant_id cannot override" in exc.value.rcvd.reason

    assert ctrl.snapshot().active_connections == 0
    assert len(bridge._active_calls) == 0


@pytest.mark.anyio
async def test_real_loopback_telephony_negative_invalid_transition_fails_closed() -> None:
    """Section 9: Sending call.end before call.start triggers 1008 close and lease release."""
    port = _get_free_port()
    verifier = SyntheticTokenVerifier(TEST_SECRET_KEY)
    bridge = SyntheticAuthBridge(verifier)
    ctrl = AdmissionController()
    coord = InstanceDrainCoordinator(controller=ctrl)
    cfg = CoreConfig(ingress_mode=INGRESS_MODE_SYNTHETIC)
    app = create_gateway_app(config=cfg, drain_coordinator=coord, synthetic_auth_bridge=bridge)

    token = verifier.issue_token(
        provider_id="pvd_trans",
        tenant_id="tenant_trans",
        call_id="call_trans_01",
        allowed_scopes=["voice.stream"],
    )

    async with _run_server(app, port) as ws_base_url:
        ws_url = f"{ws_base_url}{SYNTHETIC_WS_ENDPOINT_PATH}"
        headers = {"Authorization": f"Bearer {token}"}

        async with websockets.connect(ws_url, additional_headers=headers) as ws:
            await ws.recv()  # session.ready

            # Send call.end while in IDLE state (invalid!)
            await ws.send(json.dumps({"event": EVENT_CALL_END}))

            with pytest.raises(websockets.exceptions.ConnectionClosed) as exc:
                await ws.recv()

            assert exc.value.rcvd is not None
            assert exc.value.rcvd.code == 1008
            assert "Invalid telephony call transition" in exc.value.rcvd.reason

    assert ctrl.snapshot().active_connections == 0
    assert len(bridge._active_calls) == 0


@pytest.mark.anyio
async def test_real_loopback_telephony_concurrent_isolated_tenants() -> None:
    """Section 9: Two simultaneous calls for different tenants maintain strict independent isolation."""
    port = _get_free_port()
    verifier = SyntheticTokenVerifier(TEST_SECRET_KEY)
    bridge = SyntheticAuthBridge(verifier)
    ctrl = AdmissionController()
    coord = InstanceDrainCoordinator(controller=ctrl)
    cfg = CoreConfig(ingress_mode=INGRESS_MODE_SYNTHETIC)
    app = create_gateway_app(config=cfg, drain_coordinator=coord, synthetic_auth_bridge=bridge)

    tok_a = verifier.issue_token(
        provider_id="carrier_1",
        tenant_id="tenant_AAA",
        call_id="call_A_10",
        allowed_scopes=["voice.stream"],
    )
    tok_b = verifier.issue_token(
        provider_id="carrier_2",
        tenant_id="tenant_BBB",
        call_id="call_B_20",
        allowed_scopes=["voice.stream"],
    )

    barrier_a = asyncio.Event()
    barrier_b = asyncio.Event()

    async with _run_server(app, port) as ws_base_url:
        ws_url = f"{ws_base_url}{SYNTHETIC_WS_ENDPOINT_PATH}"

        async def _client_a() -> None:
            async with websockets.connect(
                ws_url, additional_headers={"Authorization": f"Bearer {tok_a}"}
            ) as ws:
                ready_a = json.loads(await ws.recv())
                assert ready_a["tenant_id"] == "tenant_AAA"
                assert ready_a["call_id"] == "call_A_10"

                await ws.send(json.dumps({"event": EVENT_CALL_START, "stream_id": "str_A"}))
                start_a = json.loads(await ws.recv())
                assert start_a["call_id"] == "call_A_10"

                barrier_a.set()
                await barrier_b.wait()

                await ws.send(json.dumps({"event": EVENT_CALL_END}))
                end_a = json.loads(await ws.recv())
                assert end_a["call_id"] == "call_A_10"
                await ws.send(json.dumps({"event": EVENT_SESSION_END}))
                try:
                    await ws.recv()
                except websockets.exceptions.ConnectionClosedOK:
                    pass

        async def _client_b() -> None:
            await barrier_a.wait()
            async with websockets.connect(
                ws_url, additional_headers={"Authorization": f"Bearer {tok_b}"}
            ) as ws:
                ready_b = json.loads(await ws.recv())
                assert ready_b["tenant_id"] == "tenant_BBB"
                assert ready_b["call_id"] == "call_B_20"

                # Verify both are admitted concurrently
                assert ctrl.snapshot().active_connections == 2

                await ws.send(json.dumps({"event": EVENT_CALL_START, "stream_id": "str_B"}))
                start_b = json.loads(await ws.recv())
                assert start_b["call_id"] == "call_B_20"

                barrier_b.set()

                # Client A will end call while Client B remains connected
                await asyncio.sleep(0.1)
                assert ctrl.snapshot().active_connections >= 1

                await ws.send(json.dumps({"event": EVENT_CALL_END}))
                end_b = json.loads(await ws.recv())
                assert end_b["call_id"] == "call_B_20"
                await ws.send(json.dumps({"event": EVENT_SESSION_END}))
                try:
                    await ws.recv()
                except websockets.exceptions.ConnectionClosedOK:
                    pass

        await asyncio.gather(_client_a(), _client_b())

    assert ctrl.snapshot().active_connections == 0
    assert len(bridge._active_calls) == 0


@pytest.mark.anyio
async def test_real_loopback_telephony_transport_closure_while_call_active() -> None:
    """Section 8: Closing WebSocket while call is active cleans up call and leases safely."""
    port = _get_free_port()
    verifier = SyntheticTokenVerifier(TEST_SECRET_KEY)
    bridge = SyntheticAuthBridge(verifier)
    ctrl = AdmissionController()
    coord = InstanceDrainCoordinator(controller=ctrl)
    cfg = CoreConfig(ingress_mode=INGRESS_MODE_SYNTHETIC)
    app = create_gateway_app(config=cfg, drain_coordinator=coord, synthetic_auth_bridge=bridge)

    token = verifier.issue_token(
        provider_id="synthetic_carrier",
        tenant_id="tenant_teardown",
        call_id="call_tel_teardown",
        allowed_scopes=["voice.stream"],
    )

    async with _run_server(app, port) as ws_base_url:
        ws_url = f"{ws_base_url}{SYNTHETIC_WS_ENDPOINT_PATH}"
        headers = {"Authorization": f"Bearer {token}"}

        async with websockets.connect(ws_url, additional_headers=headers) as ws:
            await ws.recv()  # session.ready

            # Start call
            await ws.send(json.dumps({"event": EVENT_CALL_START, "stream_id": "str_active"}))
            start_data = json.loads(await ws.recv())
            assert start_data["state"] == "connected"

            # Directly request session.end without explicit call.end
            await ws.send(json.dumps({"event": EVENT_SESSION_END}))
            try:
                await ws.recv()
            except websockets.exceptions.ConnectionClosedOK as close_exc:
                assert close_exc.rcvd is not None
                assert close_exc.rcvd.code == 1000

    # Capacity and active call are completely cleaned up
    assert ctrl.snapshot().active_connections == 0
    assert len(bridge._active_calls) == 0
