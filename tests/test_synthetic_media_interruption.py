"""Comprehensive verification suite for M4.2B.2B synthetic WebSocket media interruption.

Covers:
- Gate A: Protocol isolation between text-only /ws/synthetic/v1 and media /ws/synthetic-media/v1.
- Gate B: Ingress boundary turn tagging preventing stale suppression across turns.
- Gate C: Bounded interruptible Fake AI echo port with turn cancellation and queue purging.
- Gate D: Outbound response ordering barrier ensuring call.interrupted arrives before Turn 2 audio.
- Gate E: Wire schema validation, future turn rejection, duplicate turn idempotence, close codes.
- Gate F: Concurrent lifecycle handling (interrupt + call.end, client disconnect, provider failure).
- Gate G: Real loopback Uvicorn server and WebSocket acceptance matrix.
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

from kaiwen_gateway.core.admission import AdmissionController
from kaiwen_gateway.core.config import INGRESS_MODE_SYNTHETIC, CoreConfig
from kaiwen_gateway.core.draining import InstanceDrainCoordinator
from kaiwen_gateway.core.identity import VerifiedConnectionContext
from kaiwen_gateway.modules.voice import (
    AudioFormat,
    AudioFrame,
    BoundedFakeAIEchoPort,
    BoundedInterruptibleFakeAIEchoPort,
    InterruptionCapabilities,
    MediaPortClosedError,
)
from kaiwen_gateway.server import create_gateway_app
from kaiwen_gateway.transports.synthetic import (
    EVENT_CALL_END,
    EVENT_CALL_ENDED,
    EVENT_CALL_INTERRUPT,
    EVENT_CALL_INTERRUPTED,
    EVENT_CALL_START,
    EVENT_CALL_STARTED,
    EVENT_SESSION_READY,
    PCM16LE_CHANNELS,
    PCM16LE_FRAME_BYTES,
    PCM16LE_FRAME_MS,
    PCM16LE_SAMPLE_RATE,
    SUPPORTED_CLIENT_EVENTS,
    SUPPORTED_MEDIA_CLIENT_EVENTS,
    SYNTHETIC_MEDIA_WS_ENDPOINT_PATH,
    SYNTHETIC_WS_ENDPOINT_PATH,
    InvalidMessageFormatError,
    SyntheticAuthBridge,
    SyntheticTokenVerifier,
    UnsupportedEventError,
    WebSocketTelephonyMediaAdapter,
    parse_and_validate_client_frame,
    parse_and_validate_media_control_frame,
)

TEST_SECRET_KEY: Final[bytes] = b"synthetic_interruption_key_32b!"


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


def _make_pcm16le_frame(
    payload_byte: int = 0xAA, sequence: int = 0, turn_id: int = 1
) -> AudioFrame:
    return AudioFrame(
        payload=bytes([payload_byte]) * PCM16LE_FRAME_BYTES,
        format=AudioFormat.PCM16LE,
        sample_rate=PCM16LE_SAMPLE_RATE,
        channels=PCM16LE_CHANNELS,
        sequence=sequence,
        timestamp_ms=float(sequence * PCM16LE_FRAME_MS),
        turn_id=turn_id,
    )


# =============================================================================
# GATE A: Protocol Isolation & Parser Unit Tests
# =============================================================================


def test_gate_a_protocol_isolation_constants() -> None:
    """Verify that call.interrupt is part of media protocol but NOT text protocol."""
    assert EVENT_CALL_INTERRUPT == "call.interrupt"
    assert EVENT_CALL_INTERRUPTED == "call.interrupted"
    assert EVENT_CALL_INTERRUPT in SUPPORTED_MEDIA_CLIENT_EVENTS
    assert EVENT_CALL_INTERRUPT not in SUPPORTED_CLIENT_EVENTS


def test_gate_a_text_protocol_rejects_call_interrupt() -> None:
    """Verify that default parse_and_validate_client_frame rejects call.interrupt with 1008."""
    raw = json.dumps({"event": "call.interrupt", "call_id": "call_1", "turn_id": 1})
    with pytest.raises(UnsupportedEventError) as exc:
        parse_and_validate_client_frame(raw)
    assert exc.value.close_code == 1008


def test_gate_a_media_protocol_accepts_call_interrupt() -> None:
    """Verify that parse_and_validate_media_control_frame accepts valid call.interrupt."""
    raw = json.dumps(
        {
            "event": "call.interrupt",
            "call_id": "call_1",
            "turn_id": 1,
            "request_id": "req-123",
        }
    )
    parsed = parse_and_validate_media_control_frame(raw)
    assert parsed["event"] == "call.interrupt"
    assert parsed["call_id"] == "call_1"
    assert parsed["turn_id"] == 1
    assert parsed["request_id"] == "req-123"


def test_gate_e_media_control_frame_schema_validation() -> None:
    """Verify strict schema validation for call.interrupt control frames."""
    # Unknown field -> 1007
    with pytest.raises(InvalidMessageFormatError) as exc:
        parse_and_validate_media_control_frame(
            json.dumps({"event": "call.interrupt", "call_id": "c1", "turn_id": 1, "unknown": "val"})
        )
    assert exc.value.close_code == 1007
    assert "Unknown field" in str(exc.value)

    # Missing call_id -> 1007
    with pytest.raises(InvalidMessageFormatError) as exc:
        parse_and_validate_media_control_frame(
            json.dumps({"event": "call.interrupt", "turn_id": 1})
        )
    assert exc.value.close_code == 1007

    # Empty call_id -> 1007
    with pytest.raises(InvalidMessageFormatError) as exc:
        parse_and_validate_media_control_frame(
            json.dumps({"event": "call.interrupt", "call_id": "", "turn_id": 1})
        )
    assert exc.value.close_code == 1007

    # Missing turn_id -> 1007
    with pytest.raises(InvalidMessageFormatError) as exc:
        parse_and_validate_media_control_frame(
            json.dumps({"event": "call.interrupt", "call_id": "c1"})
        )
    assert exc.value.close_code == 1007

    # Non-integer turn_id (string) -> 1007
    with pytest.raises(InvalidMessageFormatError) as exc:
        parse_and_validate_media_control_frame(
            json.dumps({"event": "call.interrupt", "call_id": "c1", "turn_id": "1"})
        )
    assert exc.value.close_code == 1007

    # Non-integer turn_id (bool) -> 1007
    with pytest.raises(InvalidMessageFormatError) as exc:
        parse_and_validate_media_control_frame(
            json.dumps({"event": "call.interrupt", "call_id": "c1", "turn_id": True})
        )
    assert exc.value.close_code == 1007

    # Non-positive turn_id (0) -> 1007
    with pytest.raises(InvalidMessageFormatError) as exc:
        parse_and_validate_media_control_frame(
            json.dumps({"event": "call.interrupt", "call_id": "c1", "turn_id": 0})
        )
    assert exc.value.close_code == 1007

    # Invalid request_id (special chars / spaces) -> 1007
    with pytest.raises(InvalidMessageFormatError) as exc:
        parse_and_validate_media_control_frame(
            json.dumps(
                {
                    "event": "call.interrupt",
                    "call_id": "c1",
                    "turn_id": 1,
                    "request_id": "bad id with spaces!",
                }
            )
        )
    assert exc.value.close_code == 1007


# =============================================================================
# GATE C: Bounded Interruptible AI Echo Port Unit Tests
# =============================================================================


def test_gate_c_bounded_interruptible_ai_echo_port() -> None:
    """Verify BoundedInterruptibleFakeAIEchoPort capabilities, cancellation, and purging."""

    async def _test() -> None:
        port = BoundedInterruptibleFakeAIEchoPort(queue_capacity=8)
        assert isinstance(port.interruption_capabilities, InterruptionCapabilities)
        assert port.interruption_capabilities.can_cancel_future_generation is True
        assert port.interruption_capabilities.can_discard_local_output is True

        # Send 3 frames for turn 1
        f1 = _make_pcm16le_frame(0x11, sequence=0, turn_id=1)
        f2 = _make_pcm16le_frame(0x12, sequence=1, turn_id=1)
        f3 = _make_pcm16le_frame(0x13, sequence=2, turn_id=1)
        await port.send_frame(f1)
        await port.send_frame(f2)
        await port.send_frame(f3)
        assert port.queue_depth == 3

        # Cancel turn 1 -> unconsumed frames matching turn 1 are purged
        await port.cancel_turn(1)
        assert port.cancelled_turns == [1]
        assert port.queue_depth == 0

        # Sending another frame for turn 1 is dropped immediately
        late_turn1 = _make_pcm16le_frame(0x14, sequence=3, turn_id=1)
        await port.send_frame(late_turn1)
        assert port.queue_depth == 0

        # Sending a frame for turn 2 is enqueued and can be received
        turn2_frame = _make_pcm16le_frame(0x21, sequence=4, turn_id=2)
        await port.send_frame(turn2_frame)
        assert port.queue_depth == 1

        received = await port.receive_frame()
        assert received is not None
        assert received.turn_id == 2
        assert received.payload == turn2_frame.payload

        await port.close()
        assert port.is_closed is True
        with pytest.raises(MediaPortClosedError):
            await port.cancel_turn(2)

    asyncio.run(_test())


# =============================================================================
# GATE D: Outbound Turn Ordering Barrier Unit Test
# =============================================================================


def test_gate_d_telephony_adapter_outbound_barrier() -> None:
    """Verify WebSocketTelephonyMediaAdapter holds Turn 2 audio until barrier release."""

    async def _test() -> None:
        sent_items: list[bytes] = []

        class FakeWS:
            async def send_bytes(self, data: bytes) -> None:
                sent_items.append(data)

            async def send_text(self, text: str) -> None:
                sent_items.append(text.encode("utf-8"))

        context = VerifiedConnectionContext(
            provider_id="prov_test",
            tenant_id="tenant_test",
            connection_id="conn_test",
            call_id="call_test",
            allowed_scopes=frozenset({"voice.stream"}),
        )

        adapter = WebSocketTelephonyMediaAdapter(websocket=FakeWS(), context=context)

        # Turn 1 frame passes immediately
        f1 = _make_pcm16le_frame(0x01, sequence=0, turn_id=1)
        await adapter.send_frame(f1)
        assert len(sent_items) == 1

        # Engage barrier for turn interruption
        adapter.hold_outbound_barrier()

        # Sending Turn 2 frame will block on the barrier
        f2 = _make_pcm16le_frame(0x02, sequence=1, turn_id=2)
        send_task = asyncio.create_task(adapter.send_frame(f2))

        await asyncio.sleep(0.02)
        assert not send_task.done()
        assert len(sent_items) == 1  # Turn 2 has NOT been sent yet

        # Send call.interrupted control text
        await adapter.send_control_text('{"event":"call.interrupted"}')
        assert len(sent_items) == 2
        assert sent_items[1] == b'{"event":"call.interrupted"}'

        # Release barrier for Turn 2
        adapter.release_outbound_barrier(2)
        await asyncio.wait_for(send_task, timeout=1.0)

        # Now Turn 2 frame is sent AFTER call.interrupted
        assert len(sent_items) == 3
        assert sent_items[2] == f2.payload

        await adapter.close()

    asyncio.run(_test())


# =============================================================================
# GATE C & D: Deterministic Race and Concurrency Tests
# =============================================================================


def test_deterministic_race_a_new_turn_ack_ordering() -> None:
    """Race Test A: Block control response before send, make Turn T+1 available,
    verify no Turn T+1 frame arrives before call.interrupted, release control response,
    verify correct serialized delivery order.
    """

    async def _test() -> None:
        sent_items: list[tuple[str, Any]] = []

        class FakeWS:
            async def send_bytes(self, data: bytes) -> None:
                sent_items.append(("bytes", data))

            async def send_text(self, text: str) -> None:
                sent_items.append(("text", text))

        context = VerifiedConnectionContext(
            provider_id="prov_test",
            tenant_id="tenant_test",
            connection_id="conn_test",
            call_id="call_test",
            allowed_scopes=frozenset({"voice.stream"}),
        )

        adapter = WebSocketTelephonyMediaAdapter(websocket=FakeWS(), context=context)

        # Send Turn 1 frame
        f1 = _make_pcm16le_frame(0x11, sequence=0, turn_id=1)
        await adapter.send_frame(f1)
        assert len(sent_items) == 1
        assert sent_items[0] == ("bytes", f1.payload)

        # Interruption happens: engage barrier
        adapter.hold_outbound_barrier()

        # Turn 2 frame becomes available concurrently before call.interrupted is sent
        f2 = _make_pcm16le_frame(0x22, sequence=1, turn_id=2)
        send_task = asyncio.create_task(adapter.send_frame(f2))

        # Give loop several ticks; verify Turn 2 is NOT sent yet
        for _ in range(5):
            await asyncio.sleep(0.01)
        assert not send_task.done()
        assert len(sent_items) == 1  # Still only Turn 1

        # Now send control response call.interrupted
        control_msg = json.dumps(
            {
                "event": EVENT_CALL_INTERRUPTED,
                "call_id": "call_test",
                "interrupted_turn_id": 1,
                "new_turn_id": 2,
            }
        )
        await adapter.send_control_text(control_msg)
        assert len(sent_items) == 2
        assert sent_items[1] == ("text", control_msg)

        # Release barrier for Turn 2
        adapter.release_outbound_barrier(2)
        await asyncio.wait_for(send_task, timeout=1.0)

        # Verify exact order: Turn 1 frame -> call.interrupted -> Turn 2 frame
        assert len(sent_items) == 3
        assert sent_items[2] == ("bytes", f2.payload)

        await adapter.close()

    asyncio.run(_test())


def test_deterministic_race_b_multiple_sequential_barriers() -> None:
    """Race Test B: Turn 1 -> Turn 2 -> Turn 3. Verify Turn 3 audio cannot bypass
    the second barrier even after the first barrier was released.
    """

    async def _test() -> None:
        sent_items: list[tuple[str, Any]] = []

        class FakeWS:
            async def send_bytes(self, data: bytes) -> None:
                sent_items.append(("bytes", data))

            async def send_text(self, text: str) -> None:
                sent_items.append(("text", text))

        context = VerifiedConnectionContext(
            provider_id="prov_test",
            tenant_id="tenant_test",
            connection_id="conn_test",
            call_id="call_test",
            allowed_scopes=frozenset({"voice.stream"}),
        )

        adapter = WebSocketTelephonyMediaAdapter(websocket=FakeWS(), context=context)

        # Turn 1
        f1 = _make_pcm16le_frame(0x11, sequence=0, turn_id=1)
        await adapter.send_frame(f1)

        # Interrupt 1 -> Turn 2
        adapter.hold_outbound_barrier()
        msg1 = json.dumps({"event": EVENT_CALL_INTERRUPTED, "new_turn_id": 2})
        await adapter.send_control_text(msg1)
        adapter.release_outbound_barrier(2)

        f2 = _make_pcm16le_frame(0x22, sequence=1, turn_id=2)
        await adapter.send_frame(f2)

        # Now Interrupt 2 -> Turn 3: engage second barrier
        adapter.hold_outbound_barrier()

        # Try sending Turn 3 frame
        f3 = _make_pcm16le_frame(0x33, sequence=2, turn_id=3)
        send_task_3 = asyncio.create_task(adapter.send_frame(f3))

        for _ in range(5):
            await asyncio.sleep(0.01)
        assert not send_task_3.done()
        # Verify Turn 3 frame was held back and NOT sent
        assert ("bytes", f3.payload) not in sent_items

        # Send control text for Turn 3
        msg2 = json.dumps({"event": EVENT_CALL_INTERRUPTED, "new_turn_id": 3})
        await adapter.send_control_text(msg2)

        # Release barrier for Turn 3
        adapter.release_outbound_barrier(3)
        await asyncio.wait_for(send_task_3, timeout=1.0)

        # Verify Turn 3 was sent only after msg2
        assert sent_items[-1] == ("bytes", f3.payload)
        assert sent_items[-2] == ("text", msg2)

        await adapter.close()

    asyncio.run(_test())


def test_deterministic_race_c_send_lock_race_and_honesty() -> None:
    """Race Test C: Hold an old-turn send under _send_lock, start interruption,
    release old send: verify no deadlock, old frame committed to transport,
    and control text is serialized safely.
    """

    async def _test() -> None:
        in_send_event = asyncio.Event()
        release_send_event = asyncio.Event()
        sent_items: list[tuple[str, Any]] = []

        class SlowWS:
            async def send_bytes(self, data: bytes) -> None:
                in_send_event.set()
                await release_send_event.wait()
                sent_items.append(("bytes", data))

            async def send_text(self, text: str) -> None:
                sent_items.append(("text", text))

        context = VerifiedConnectionContext(
            provider_id="prov_test",
            tenant_id="tenant_test",
            connection_id="conn_test",
            call_id="call_test",
            allowed_scopes=frozenset({"voice.stream"}),
        )

        adapter = WebSocketTelephonyMediaAdapter(websocket=SlowWS(), context=context)

        # Task 1: Start sending Turn 1 frame which pauses inside send_bytes holding _send_lock
        f1 = _make_pcm16le_frame(0x11, sequence=0, turn_id=1)
        send_old_task = asyncio.create_task(adapter.send_frame(f1))
        await in_send_event.wait()

        # Interruption starts concurrently: engage barrier and attempt to send control text
        adapter.hold_outbound_barrier()
        ctrl_msg = json.dumps({"event": EVENT_CALL_INTERRUPTED, "new_turn_id": 2})

        # Control text send will wait for _send_lock (currently held by send_old_task)
        ctrl_task = asyncio.create_task(adapter.send_control_text(ctrl_msg))

        await asyncio.sleep(0.02)
        assert not ctrl_task.done()
        assert not send_old_task.done()
        assert len(sent_items) == 0

        # Release the slow send_bytes
        release_send_event.set()
        await asyncio.wait_for(send_old_task, timeout=1.0)
        await asyncio.wait_for(ctrl_task, timeout=1.0)

        # Verify no deadlock, and serialized order: old frame committed, then control text
        assert len(sent_items) == 2
        assert sent_items[0] == ("bytes", f1.payload)
        assert sent_items[1] == ("text", ctrl_msg)

        await adapter.close()

    asyncio.run(_test())


def test_deterministic_race_d_barrier_unblocks_on_close_and_failure() -> None:
    """Race Test D: When client disconnects or adapter closes while frames await gate,
    waiting tasks unblock cleanly with MediaPortClosedError and do not hang.
    """

    async def _test() -> None:
        sent_items: list[bytes] = []

        class FakeWS:
            async def send_bytes(self, data: bytes) -> None:
                sent_items.append(data)

            async def send_text(self, text: str) -> None:
                pass

        context = VerifiedConnectionContext(
            provider_id="prov_test",
            tenant_id="tenant_test",
            connection_id="conn_test",
            call_id="call_test",
            allowed_scopes=frozenset({"voice.stream"}),
        )

        adapter = WebSocketTelephonyMediaAdapter(websocket=FakeWS(), context=context)

        # Engage barrier
        adapter.hold_outbound_barrier()

        # Enqueue send of Turn 2 frame
        f2 = _make_pcm16le_frame(0x22, sequence=1, turn_id=2)
        send_task = asyncio.create_task(adapter.send_frame(f2))

        await asyncio.sleep(0.02)
        assert not send_task.done()

        # Simulate disconnect/closure
        await adapter.close()

        # send_task must unblock and raise MediaPortClosedError immediately
        with pytest.raises(MediaPortClosedError):
            await asyncio.wait_for(send_task, timeout=0.5)

        assert len(sent_items) == 0

    asyncio.run(_test())


# =============================================================================
# GATE G: Real Loopback Acceptance Matrix (18 Scenarios)
# =============================================================================


def _create_server_app(
    ai_port_factory: Any = None,
) -> tuple[Any, SyntheticTokenVerifier, SyntheticAuthBridge, InstanceDrainCoordinator]:
    verifier = SyntheticTokenVerifier(secret_key=TEST_SECRET_KEY, enable_replay_protection=True)
    auth_bridge = SyntheticAuthBridge(verifier=verifier, enforce_call_exclusivity=True)
    admission = AdmissionController(
        CoreConfig(ingress_mode=INGRESS_MODE_SYNTHETIC, max_concurrent_connections=10)
    )
    coordinator = InstanceDrainCoordinator(controller=admission)

    app = create_gateway_app(
        config=CoreConfig(ingress_mode=INGRESS_MODE_SYNTHETIC),
        synthetic_auth_bridge=auth_bridge,
        enable_synthetic_media=True,
    )
    app.state.drain_coordinator = coordinator
    if ai_port_factory is not None:
        app.state.ai_media_port_factory = ai_port_factory

    return app, verifier, auth_bridge, coordinator


def test_acceptance_01_interruption_happy_path_two_turns() -> None:
    """Scenario 1: Happy path turn 1 streaming -> interrupt -> turn 2 streaming -> clean end."""

    async def _test() -> None:
        app, verifier, auth_bridge, coordinator = _create_server_app()
        port = _get_free_port()

        token = verifier.issue_token(
            provider_id="smoke_provider",
            tenant_id="tenant_01",
            call_id="call_acc_01",
            allowed_scopes=["voice.stream"],
        )

        async with _run_test_server(app, port) as server_url:
            uri = f"{server_url}{SYNTHETIC_MEDIA_WS_ENDPOINT_PATH}"
            headers = {"Authorization": f"Bearer {token}"}

            async with websockets.connect(uri, additional_headers=headers) as ws:
                # 1. Greeting
                ready = json.loads(await ws.recv())
                assert ready["event"] == EVENT_SESSION_READY

                # 2. Start call
                await ws.send(json.dumps({"event": EVENT_CALL_START, "call_id": "call_acc_01"}))
                started = json.loads(await ws.recv())
                assert started["event"] == EVENT_CALL_STARTED

                # 3. Stream 3 frames for Turn 1
                for i in range(3):
                    frame_bytes = bytes([0x10 + i]) * PCM16LE_FRAME_BYTES
                    await ws.send(frame_bytes)

                for i in range(3):
                    echoed = await ws.recv()
                    assert isinstance(echoed, bytes)
                    assert echoed == bytes([0x10 + i]) * PCM16LE_FRAME_BYTES

                # 4. Interruption
                await ws.send(
                    json.dumps(
                        {
                            "event": EVENT_CALL_INTERRUPT,
                            "call_id": "call_acc_01",
                            "turn_id": 1,
                            "request_id": "req-turn1",
                        }
                    )
                )

                interrupted = json.loads(await ws.recv())
                assert interrupted["event"] == EVENT_CALL_INTERRUPTED
                assert interrupted["call_id"] == "call_acc_01"
                assert interrupted["interrupted_turn_id"] == 1
                assert interrupted["new_turn_id"] == 2
                assert interrupted["request_id"] == "req-turn1"
                assert interrupted["playback_confirmed"] is False

                # 5. Stream 3 frames for Turn 2
                for i in range(3):
                    frame_bytes = bytes([0x20 + i]) * PCM16LE_FRAME_BYTES
                    await ws.send(frame_bytes)

                for i in range(3):
                    echoed = await ws.recv()
                    assert isinstance(echoed, bytes)
                    assert echoed == bytes([0x20 + i]) * PCM16LE_FRAME_BYTES

                # 6. End call
                await ws.send(json.dumps({"event": EVENT_CALL_END, "call_id": "call_acc_01"}))
                ended = json.loads(await ws.recv())
                assert ended["event"] == EVENT_CALL_ENDED

                # Server closes normally with 1000
                await ws.wait_closed()
                assert ws.close_code == 1000

        # Verification of zero leaked leases
        assert len(auth_bridge._active_calls) == 0
        assert coordinator.controller.snapshot().active_connections == 0

    asyncio.run(_test())


def test_acceptance_02_ingress_turn_tagging_prevents_stale_suppression() -> None:
    """Scenario 2: Turn 2 frames must not be dropped as stale frames after interruption."""

    async def _test() -> None:
        app, verifier, auth_bridge, coordinator = _create_server_app()
        port = _get_free_port()

        token = verifier.issue_token(
            provider_id="smoke_provider",
            tenant_id="tenant_01",
            call_id="call_acc_02",
            allowed_scopes=["voice.stream"],
        )

        async with _run_test_server(app, port) as server_url:
            uri = f"{server_url}{SYNTHETIC_MEDIA_WS_ENDPOINT_PATH}"
            async with websockets.connect(
                uri, additional_headers={"Authorization": f"Bearer {token}"}
            ) as ws:
                await ws.recv()  # session.ready
                await ws.send(json.dumps({"event": EVENT_CALL_START, "call_id": "call_acc_02"}))
                await ws.recv()  # call.started

                # Send 1 frame turn 1 and receive it
                await ws.send(bytes([0x01]) * PCM16LE_FRAME_BYTES)
                await ws.recv()

                # Interrupt turn 1 -> advance to turn 2
                await ws.send(
                    json.dumps(
                        {"event": EVENT_CALL_INTERRUPT, "call_id": "call_acc_02", "turn_id": 1}
                    )
                )
                res = json.loads(await ws.recv())
                assert res["new_turn_id"] == 2

                # Now send turn 2 frame: verify it is NOT suppressed as stale and is echoed back!
                turn2_payload = bytes([0x02]) * PCM16LE_FRAME_BYTES
                await ws.send(turn2_payload)

                echoed = await ws.recv()
                assert echoed == turn2_payload

                await ws.send(json.dumps({"event": EVENT_CALL_END, "call_id": "call_acc_02"}))
                await ws.recv()  # call.ended

    asyncio.run(_test())


def test_acceptance_03_outbound_ordering_barrier_with_autonomous_ai() -> None:
    """Scenario 3: Barrier ensures call.interrupted is received BEFORE Turn 2 audio arrives."""

    async def _test() -> None:
        class AutonomousTurn2AI(BoundedInterruptibleFakeAIEchoPort):
            async def cancel_turn(self, turn_id: int) -> None:
                await super().cancel_turn(turn_id)
                # Immediately generate Turn 2 output frame into output queue
                turn2_frame = _make_pcm16le_frame(0x99, sequence=99, turn_id=turn_id + 1)
                await self._output_queue.put(turn2_frame)

        app, verifier, auth_bridge, coordinator = _create_server_app(
            ai_port_factory=AutonomousTurn2AI
        )
        port = _get_free_port()

        token = verifier.issue_token(
            provider_id="smoke_provider",
            tenant_id="tenant_01",
            call_id="call_acc_03",
            allowed_scopes=["voice.stream"],
        )

        async with _run_test_server(app, port) as server_url:
            uri = f"{server_url}{SYNTHETIC_MEDIA_WS_ENDPOINT_PATH}"
            async with websockets.connect(
                uri, additional_headers={"Authorization": f"Bearer {token}"}
            ) as ws:
                await ws.recv()
                await ws.send(json.dumps({"event": EVENT_CALL_START, "call_id": "call_acc_03"}))
                await ws.recv()

                # Trigger interrupt
                await ws.send(
                    json.dumps(
                        {"event": EVENT_CALL_INTERRUPT, "call_id": "call_acc_03", "turn_id": 1}
                    )
                )

                # FIRST message received MUST be text control message 'call.interrupted'
                first_msg = await ws.recv()
                assert isinstance(first_msg, str)
                payload = json.loads(first_msg)
                assert payload["event"] == EVENT_CALL_INTERRUPTED

                # SECOND message received is the autonomous Turn 2 binary frame
                second_msg = await ws.recv()
                assert isinstance(second_msg, bytes)
                assert second_msg == bytes([0x99]) * PCM16LE_FRAME_BYTES

                await ws.send(json.dumps({"event": EVENT_CALL_END, "call_id": "call_acc_03"}))
                await ws.recv()

    asyncio.run(_test())


def test_acceptance_04_idempotent_duplicate_interruption() -> None:
    """Scenario 4: Re-sending call.interrupt for already-interrupted turn returns idempotent success."""

    async def _test() -> None:
        app, verifier, auth_bridge, coordinator = _create_server_app()
        port = _get_free_port()

        token = verifier.issue_token(
            provider_id="smoke_provider",
            tenant_id="tenant_01",
            call_id="call_acc_04",
            allowed_scopes=["voice.stream"],
        )

        async with _run_test_server(app, port) as server_url:
            uri = f"{server_url}{SYNTHETIC_MEDIA_WS_ENDPOINT_PATH}"
            async with websockets.connect(
                uri, additional_headers={"Authorization": f"Bearer {token}"}
            ) as ws:
                await ws.recv()
                await ws.send(json.dumps({"event": EVENT_CALL_START, "call_id": "call_acc_04"}))
                await ws.recv()

                # First interrupt turn 1 -> new turn 2
                await ws.send(
                    json.dumps(
                        {"event": EVENT_CALL_INTERRUPT, "call_id": "call_acc_04", "turn_id": 1}
                    )
                )
                res1 = json.loads(await ws.recv())
                assert res1["event"] == EVENT_CALL_INTERRUPTED
                assert res1["new_turn_id"] == 2

                # Duplicate interrupt turn 1 -> idempotent success
                await ws.send(
                    json.dumps(
                        {"event": EVENT_CALL_INTERRUPT, "call_id": "call_acc_04", "turn_id": 1}
                    )
                )
                res2 = json.loads(await ws.recv())
                assert res2["event"] == EVENT_CALL_INTERRUPTED
                assert res2["interrupted_turn_id"] == 1
                assert res2["new_turn_id"] == 2
                assert res2["locally_discarded_frames"] == 0

                # Session remains active and can transfer Turn 2 audio
                t2 = bytes([0x55]) * PCM16LE_FRAME_BYTES
                await ws.send(t2)
                assert await ws.recv() == t2

                await ws.send(json.dumps({"event": EVENT_CALL_END, "call_id": "call_acc_04"}))
                await ws.recv()

    asyncio.run(_test())


def test_acceptance_05_future_turn_interruption_fails_closed_1008() -> None:
    """Scenario 5: Attempting to interrupt a future unreached turn fails closed with close code 1008."""

    async def _test() -> None:
        app, verifier, auth_bridge, coordinator = _create_server_app()
        port = _get_free_port()

        token = verifier.issue_token(
            provider_id="smoke_provider",
            tenant_id="tenant_01",
            call_id="call_acc_05",
            allowed_scopes=["voice.stream"],
        )

        async with _run_test_server(app, port) as server_url:
            uri = f"{server_url}{SYNTHETIC_MEDIA_WS_ENDPOINT_PATH}"
            async with websockets.connect(
                uri, additional_headers={"Authorization": f"Bearer {token}"}
            ) as ws:
                await ws.recv()
                await ws.send(json.dumps({"event": EVENT_CALL_START, "call_id": "call_acc_05"}))
                await ws.recv()

                # Attempt to interrupt turn 99 when current turn is 1
                await ws.send(
                    json.dumps(
                        {"event": EVENT_CALL_INTERRUPT, "call_id": "call_acc_05", "turn_id": 99}
                    )
                )

                await ws.wait_closed()
                assert ws.close_code == 1008
                assert ws.close_reason is not None
                assert "Cannot interrupt future turn" in ws.close_reason

    asyncio.run(_test())


def test_acceptance_06_stale_turn_after_multiple_interruptions() -> None:
    """Scenario 6: Stale turn interruption after advancing to Turn 3 returns idempotent success."""

    async def _test() -> None:
        app, verifier, auth_bridge, coordinator = _create_server_app()
        port = _get_free_port()

        token = verifier.issue_token(
            provider_id="smoke_provider",
            tenant_id="tenant_01",
            call_id="call_acc_06",
            allowed_scopes=["voice.stream"],
        )

        async with _run_test_server(app, port) as server_url:
            uri = f"{server_url}{SYNTHETIC_MEDIA_WS_ENDPOINT_PATH}"
            async with websockets.connect(
                uri, additional_headers={"Authorization": f"Bearer {token}"}
            ) as ws:
                await ws.recv()
                await ws.send(json.dumps({"event": EVENT_CALL_START, "call_id": "call_acc_06"}))
                await ws.recv()

                # Interrupt turn 1 -> 2
                await ws.send(
                    json.dumps(
                        {"event": EVENT_CALL_INTERRUPT, "call_id": "call_acc_06", "turn_id": 1}
                    )
                )
                await ws.recv()

                # Interrupt turn 2 -> 3
                await ws.send(
                    json.dumps(
                        {"event": EVENT_CALL_INTERRUPT, "call_id": "call_acc_06", "turn_id": 2}
                    )
                )
                await ws.recv()

                # Now send interrupt for Turn 1
                await ws.send(
                    json.dumps(
                        {"event": EVENT_CALL_INTERRUPT, "call_id": "call_acc_06", "turn_id": 1}
                    )
                )
                res = json.loads(await ws.recv())
                assert res["interrupted_turn_id"] == 1
                assert res["new_turn_id"] == 3
                assert res["locally_discarded_frames"] == 0

                await ws.send(json.dumps({"event": EVENT_CALL_END, "call_id": "call_acc_06"}))
                await ws.recv()

    asyncio.run(_test())


def test_acceptance_07_interrupt_before_call_start_fails_closed_1008() -> None:
    """Scenario 7: call.interrupt before call.start fails closed with 1008."""

    async def _test() -> None:
        app, verifier, auth_bridge, coordinator = _create_server_app()
        port = _get_free_port()

        token = verifier.issue_token(
            provider_id="smoke_provider",
            tenant_id="tenant_01",
            call_id="call_acc_07",
            allowed_scopes=["voice.stream"],
        )

        async with _run_test_server(app, port) as server_url:
            uri = f"{server_url}{SYNTHETIC_MEDIA_WS_ENDPOINT_PATH}"
            async with websockets.connect(
                uri, additional_headers={"Authorization": f"Bearer {token}"}
            ) as ws:
                await ws.recv()  # session.ready
                await ws.send(
                    json.dumps(
                        {"event": EVENT_CALL_INTERRUPT, "call_id": "call_acc_07", "turn_id": 1}
                    )
                )

                await ws.wait_closed()
                assert ws.close_code == 1008
                assert ws.close_reason is not None
                assert "call is not active" in ws.close_reason

    asyncio.run(_test())


def test_acceptance_08_interrupt_call_id_mismatch_fails_closed_1008() -> None:
    """Scenario 8: call.interrupt with mismatched call_id fails closed with 1008."""

    async def _test() -> None:
        app, verifier, auth_bridge, coordinator = _create_server_app()
        port = _get_free_port()

        token = verifier.issue_token(
            provider_id="smoke_provider",
            tenant_id="tenant_01",
            call_id="call_acc_08",
            allowed_scopes=["voice.stream"],
        )

        async with _run_test_server(app, port) as server_url:
            uri = f"{server_url}{SYNTHETIC_MEDIA_WS_ENDPOINT_PATH}"
            async with websockets.connect(
                uri, additional_headers={"Authorization": f"Bearer {token}"}
            ) as ws:
                await ws.recv()
                await ws.send(json.dumps({"event": EVENT_CALL_START, "call_id": "call_acc_08"}))
                await ws.recv()

                # Mismatched call_id
                await ws.send(
                    json.dumps(
                        {"event": EVENT_CALL_INTERRUPT, "call_id": "call_FORGED", "turn_id": 1}
                    )
                )

                await ws.wait_closed()
                assert ws.close_code == 1008
                assert ws.close_reason is not None
                assert "Call ID in payload does not match" in ws.close_reason

    asyncio.run(_test())


def test_acceptance_09_interrupt_missing_or_invalid_fields_fails_closed_1007() -> None:
    """Scenario 9: Missing fields, wrong types, or unknown fields fail closed with 1007."""

    async def _test() -> None:
        app, verifier, auth_bridge, coordinator = _create_server_app()
        port = _get_free_port()

        token = verifier.issue_token(
            provider_id="smoke_provider",
            tenant_id="tenant_01",
            call_id="call_acc_09",
            allowed_scopes=["voice.stream"],
        )

        async with _run_test_server(app, port) as server_url:
            uri = f"{server_url}{SYNTHETIC_MEDIA_WS_ENDPOINT_PATH}"
            async with websockets.connect(
                uri, additional_headers={"Authorization": f"Bearer {token}"}
            ) as ws:
                await ws.recv()
                await ws.send(json.dumps({"event": EVENT_CALL_START, "call_id": "call_acc_09"}))
                await ws.recv()

                # Send payload with unknown field
                await ws.send(
                    json.dumps(
                        {
                            "event": EVENT_CALL_INTERRUPT,
                            "call_id": "call_acc_09",
                            "turn_id": 1,
                            "extra_bad_field": 123,
                        }
                    )
                )

                await ws.wait_closed()
                assert ws.close_code == 1007

    asyncio.run(_test())


def test_acceptance_10_interrupt_oversized_payload_fails_closed_1009() -> None:
    """Scenario 10: Oversized control frame (>64 KiB) fails closed with 1009."""

    async def _test() -> None:
        app, verifier, auth_bridge, coordinator = _create_server_app()
        port = _get_free_port()

        token = verifier.issue_token(
            provider_id="smoke_provider",
            tenant_id="tenant_01",
            call_id="call_acc_10",
            allowed_scopes=["voice.stream"],
        )

        async with _run_test_server(app, port) as server_url:
            uri = f"{server_url}{SYNTHETIC_MEDIA_WS_ENDPOINT_PATH}"
            async with websockets.connect(
                uri, additional_headers={"Authorization": f"Bearer {token}"}
            ) as ws:
                await ws.recv()
                await ws.send(json.dumps({"event": EVENT_CALL_START, "call_id": "call_acc_10"}))
                await ws.recv()

                # Padded oversized message > 64 KiB
                oversized_text = json.dumps(
                    {
                        "event": EVENT_CALL_INTERRUPT,
                        "call_id": "call_acc_10",
                        "turn_id": 1,
                    }
                ) + (" " * (65 * 1024))

                await ws.send(oversized_text)
                await ws.wait_closed()
                assert ws.close_code == 1009

    asyncio.run(_test())


def test_acceptance_11_text_endpoint_strictly_rejects_call_interrupt_1008() -> None:
    """Scenario 11: Gate A isolation - text endpoint /ws/synthetic/v1 rejects call.interrupt with 1008."""

    async def _test() -> None:
        app, verifier, auth_bridge, coordinator = _create_server_app()
        port = _get_free_port()

        token = verifier.issue_token(
            provider_id="smoke_provider",
            tenant_id="tenant_01",
            call_id="call_acc_11",
            allowed_scopes=["voice.stream"],
        )

        async with _run_test_server(app, port) as server_url:
            uri = f"{server_url}{SYNTHETIC_WS_ENDPOINT_PATH}"
            async with websockets.connect(
                uri, additional_headers={"Authorization": f"Bearer {token}"}
            ) as ws:
                await ws.recv()  # session.ready
                await ws.send(json.dumps({"event": EVENT_CALL_START, "call_id": "call_acc_11"}))
                await ws.recv()  # call.started

                # Attempt call.interrupt on text-only endpoint
                await ws.send(
                    json.dumps(
                        {"event": EVENT_CALL_INTERRUPT, "call_id": "call_acc_11", "turn_id": 1}
                    )
                )

                await ws.wait_closed()
                assert ws.close_code == 1008
                assert ws.close_reason is not None
                assert "Unsupported event type" in ws.close_reason

    asyncio.run(_test())


def test_acceptance_12_non_interruptible_ai_port_fails_closed_1011() -> None:
    """Scenario 12: Custom non-interruptible AI port causes call.interrupt to fail closed with 1011."""

    async def _test() -> None:
        # BoundedFakeAIEchoPort does NOT implement InterruptibleAIPort
        app, verifier, auth_bridge, coordinator = _create_server_app(
            ai_port_factory=BoundedFakeAIEchoPort
        )
        port = _get_free_port()

        token = verifier.issue_token(
            provider_id="smoke_provider",
            tenant_id="tenant_01",
            call_id="call_acc_12",
            allowed_scopes=["voice.stream"],
        )

        async with _run_test_server(app, port) as server_url:
            uri = f"{server_url}{SYNTHETIC_MEDIA_WS_ENDPOINT_PATH}"
            async with websockets.connect(
                uri, additional_headers={"Authorization": f"Bearer {token}"}
            ) as ws:
                await ws.recv()
                await ws.send(json.dumps({"event": EVENT_CALL_START, "call_id": "call_acc_12"}))
                await ws.recv()

                await ws.send(
                    json.dumps(
                        {"event": EVENT_CALL_INTERRUPT, "call_id": "call_acc_12", "turn_id": 1}
                    )
                )

                await ws.wait_closed()
                assert ws.close_code == 1011
                assert ws.close_reason is not None
                assert "Upstream media interruption failed" in ws.close_reason

    asyncio.run(_test())


def test_acceptance_13_ai_cancellation_error_fails_closed_1011() -> None:
    """Scenario 13: Upstream cancellation exception causes call.interrupt to fail closed with 1011."""

    async def _test() -> None:
        class FailingCancelAI(BoundedInterruptibleFakeAIEchoPort):
            async def cancel_turn(self, turn_id: int) -> None:
                raise RuntimeError("Upstream provider AI cancel error")

        app, verifier, auth_bridge, coordinator = _create_server_app(
            ai_port_factory=FailingCancelAI
        )
        port = _get_free_port()

        token = verifier.issue_token(
            provider_id="smoke_provider",
            tenant_id="tenant_01",
            call_id="call_acc_13",
            allowed_scopes=["voice.stream"],
        )

        async with _run_test_server(app, port) as server_url:
            uri = f"{server_url}{SYNTHETIC_MEDIA_WS_ENDPOINT_PATH}"
            async with websockets.connect(
                uri, additional_headers={"Authorization": f"Bearer {token}"}
            ) as ws:
                await ws.recv()
                await ws.send(json.dumps({"event": EVENT_CALL_START, "call_id": "call_acc_13"}))
                await ws.recv()

                await ws.send(
                    json.dumps(
                        {"event": EVENT_CALL_INTERRUPT, "call_id": "call_acc_13", "turn_id": 1}
                    )
                )

                await ws.wait_closed()
                assert ws.close_code == 1011
                assert ws.close_reason is not None
                assert "Upstream media interruption failed" in ws.close_reason

    asyncio.run(_test())


def test_acceptance_14_concurrent_interrupt_and_call_end_drains_cleanly() -> None:
    """Scenario 14: call.interrupt immediately followed by call.end executes clean drain and closes 1000."""

    async def _test() -> None:
        app, verifier, auth_bridge, coordinator = _create_server_app()
        port = _get_free_port()

        token = verifier.issue_token(
            provider_id="smoke_provider",
            tenant_id="tenant_01",
            call_id="call_acc_14",
            allowed_scopes=["voice.stream"],
        )

        async with _run_test_server(app, port) as server_url:
            uri = f"{server_url}{SYNTHETIC_MEDIA_WS_ENDPOINT_PATH}"
            async with websockets.connect(
                uri, additional_headers={"Authorization": f"Bearer {token}"}
            ) as ws:
                await ws.recv()
                await ws.send(json.dumps({"event": EVENT_CALL_START, "call_id": "call_acc_14"}))
                await ws.recv()

                # Send interrupt and immediately call.end
                await ws.send(
                    json.dumps(
                        {"event": EVENT_CALL_INTERRUPT, "call_id": "call_acc_14", "turn_id": 1}
                    )
                )
                await ws.send(json.dumps({"event": EVENT_CALL_END, "call_id": "call_acc_14"}))

                # Receive call.interrupted
                interrupted = json.loads(await ws.recv())
                assert interrupted["event"] == EVENT_CALL_INTERRUPTED

                # Receive call.ended
                ended = json.loads(await ws.recv())
                assert ended["event"] == EVENT_CALL_ENDED

                await ws.wait_closed()
                assert ws.close_code == 1000

        assert len(auth_bridge._active_calls) == 0

    asyncio.run(_test())


def test_acceptance_15_client_disconnect_after_interrupt_recovers_capacity() -> None:
    """Scenario 15: Client disconnects abruptly after interrupt; leases and admission are recovered."""

    async def _test() -> None:
        app, verifier, auth_bridge, coordinator = _create_server_app()
        port = _get_free_port()

        token = verifier.issue_token(
            provider_id="smoke_provider",
            tenant_id="tenant_01",
            call_id="call_acc_15",
            allowed_scopes=["voice.stream"],
        )

        async with _run_test_server(app, port) as server_url:
            uri = f"{server_url}{SYNTHETIC_MEDIA_WS_ENDPOINT_PATH}"
            ws = await websockets.connect(
                uri, additional_headers={"Authorization": f"Bearer {token}"}
            )
            await ws.recv()
            await ws.send(json.dumps({"event": EVENT_CALL_START, "call_id": "call_acc_15"}))
            await ws.recv()

            # Send interrupt
            await ws.send(
                json.dumps({"event": EVENT_CALL_INTERRUPT, "call_id": "call_acc_15", "turn_id": 1})
            )
            await ws.recv()

            # Abrupt client close without call.end
            await ws.close(code=1000)

            # Allow server supervisor to finish cleanup
            for _ in range(50):
                if (
                    len(auth_bridge._active_calls) == 0
                    and coordinator.controller.snapshot().active_connections == 0
                ):
                    break
                await asyncio.sleep(0.02)

        assert len(auth_bridge._active_calls) == 0
        assert coordinator.controller.snapshot().active_connections == 0

    asyncio.run(_test())


def test_acceptance_16_interruption_response_contains_all_10_required_fields() -> None:
    """Scenario 16: Verify all 10 required fields in call.interrupted response schema."""

    async def _test() -> None:
        app, verifier, auth_bridge, coordinator = _create_server_app()
        port = _get_free_port()

        token = verifier.issue_token(
            provider_id="smoke_provider",
            tenant_id="tenant_01",
            call_id="call_acc_16",
            allowed_scopes=["voice.stream"],
        )

        async with _run_test_server(app, port) as server_url:
            uri = f"{server_url}{SYNTHETIC_MEDIA_WS_ENDPOINT_PATH}"
            async with websockets.connect(
                uri, additional_headers={"Authorization": f"Bearer {token}"}
            ) as ws:
                await ws.recv()
                await ws.send(json.dumps({"event": EVENT_CALL_START, "call_id": "call_acc_16"}))
                await ws.recv()

                await ws.send(
                    json.dumps(
                        {
                            "event": EVENT_CALL_INTERRUPT,
                            "call_id": "call_acc_16",
                            "turn_id": 1,
                            "request_id": "client-req-99",
                        }
                    )
                )

                res = json.loads(await ws.recv())

                required_fields = {
                    "event",
                    "call_id",
                    "interrupted_turn_id",
                    "new_turn_id",
                    "locally_discarded_frames",
                    "stale_frames_suppressed",
                    "future_generation_cancelled",
                    "transport_pending_cancelled",
                    "provider_playback_cleared",
                    "playback_confirmed",
                    "request_id",
                }
                assert set(res.keys()) == required_fields
                assert res["event"] == EVENT_CALL_INTERRUPTED
                assert res["call_id"] == "call_acc_16"
                assert res["interrupted_turn_id"] == 1
                assert res["new_turn_id"] == 2
                assert res["playback_confirmed"] is False
                assert res["request_id"] == "client-req-99"

                await ws.send(json.dumps({"event": EVENT_CALL_END, "call_id": "call_acc_16"}))
                await ws.recv()

    asyncio.run(_test())


def test_acceptance_17_invalid_json_fails_closed_1007() -> None:
    """Scenario 17: Non-JSON payload on media endpoint fails closed with 1007."""

    async def _test() -> None:
        app, verifier, auth_bridge, coordinator = _create_server_app()
        port = _get_free_port()

        token = verifier.issue_token(
            provider_id="smoke_provider",
            tenant_id="tenant_01",
            call_id="call_acc_17",
            allowed_scopes=["voice.stream"],
        )

        async with _run_test_server(app, port) as server_url:
            uri = f"{server_url}{SYNTHETIC_MEDIA_WS_ENDPOINT_PATH}"
            async with websockets.connect(
                uri, additional_headers={"Authorization": f"Bearer {token}"}
            ) as ws:
                await ws.recv()
                await ws.send("{broken_json: true,")

                await ws.wait_closed()
                assert ws.close_code == 1007

    asyncio.run(_test())


def test_acceptance_18_multiple_sequential_turns_and_interruptions() -> None:
    """Scenario 18: Multiple turn progressions (Turn 1 -> Turn 2 -> Turn 3 -> Turn 4) in sequence."""

    async def _test() -> None:
        app, verifier, auth_bridge, coordinator = _create_server_app()
        port = _get_free_port()

        token = verifier.issue_token(
            provider_id="smoke_provider",
            tenant_id="tenant_01",
            call_id="call_acc_18",
            allowed_scopes=["voice.stream"],
        )

        async with _run_test_server(app, port) as server_url:
            uri = f"{server_url}{SYNTHETIC_MEDIA_WS_ENDPOINT_PATH}"
            async with websockets.connect(
                uri, additional_headers={"Authorization": f"Bearer {token}"}
            ) as ws:
                await ws.recv()
                await ws.send(json.dumps({"event": EVENT_CALL_START, "call_id": "call_acc_18"}))
                await ws.recv()

                for expected_current in range(1, 4):
                    # Send frame for current turn
                    frame = bytes([expected_current]) * PCM16LE_FRAME_BYTES
                    await ws.send(frame)
                    echoed = await ws.recv()
                    assert echoed == frame

                    # Interrupt current turn
                    await ws.send(
                        json.dumps(
                            {
                                "event": EVENT_CALL_INTERRUPT,
                                "call_id": "call_acc_18",
                                "turn_id": expected_current,
                            }
                        )
                    )
                    res = json.loads(await ws.recv())
                    assert res["interrupted_turn_id"] == expected_current
                    assert res["new_turn_id"] == expected_current + 1

                # In Turn 4, send final frame
                frame4 = bytes([4]) * PCM16LE_FRAME_BYTES
                await ws.send(frame4)
                assert await ws.recv() == frame4

                await ws.send(json.dumps({"event": EVENT_CALL_END, "call_id": "call_acc_18"}))
                await ws.recv()
                await ws.wait_closed()
                assert ws.close_code == 1000

    asyncio.run(_test())
