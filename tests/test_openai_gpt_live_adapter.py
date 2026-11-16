"""Comprehensive offline integration and reliability tests for OpenAIGPTLiveMediaAdapter."""

from __future__ import annotations

import asyncio
import base64
import json
import time
import traceback
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Any

import pytest
import uvicorn
import websockets

from kaiwen_gateway.adapters.openai_gpt_live import (
    OpenAIGPTLiveConfig,
    OpenAIGPTLiveError,
    OpenAIGPTLiveMediaAdapter,
    OpenAIGPTLiveTransport,
    create_openai_gpt_live_adapter_factory,
)
from kaiwen_gateway.adapters.openai_realtime.audio import GATEWAY_FRAME_BYTES
from kaiwen_gateway.core import CoreConfig
from kaiwen_gateway.modules.voice import (
    AudioFormat,
    AudioFrame,
    MediaBackpressureOverflowError,
    UnsupportedInterruptionCapabilityError,
    VoiceBridgeSession,
)
from kaiwen_gateway.modules.voice.adapters.fake import FakeTelephonyMediaAdapter
from kaiwen_gateway.server import create_gateway_app
from kaiwen_gateway.transports.synthetic import SyntheticAuthBridge, SyntheticTokenVerifier


@asynccontextmanager
async def _mock_gpt_live_ws_server(
    handler: Any,
) -> AsyncIterator[tuple[str, list[dict[str, Any]]]]:
    """Run an ephemeral local WebSocket server simulating wss://api.openai.com/v1/live/sessions."""
    connections_log: list[dict[str, Any]] = []

    async def _wrapped(ws: Any) -> None:
        req = getattr(ws, "request", None)
        path = getattr(req, "path", getattr(ws, "path", ""))
        headers = dict(getattr(req, "headers", {}))
        connections_log.append({"path": path, "headers": headers})
        await handler(ws)

    server = await websockets.serve(_wrapped, "127.0.0.1", 0)
    port = next(iter(server.sockets)).getsockname()[1]
    try:
        yield f"ws://127.0.0.1:{port}/v1/live/sessions", connections_log
    finally:
        server.close()
        await server.wait_closed()


def test_01_session_start_gating_and_single_upstream_session() -> None:
    """Verify session.start schema, session.started gating before audio append, and single session."""

    async def _run() -> None:
        received_client_events: list[tuple[float, dict[str, Any]]] = []
        session_started_sent_at = 0.0

        async def _handler(ws: Any) -> None:
            nonlocal session_started_sent_at
            async for raw in ws:
                evt = json.loads(raw)
                received_client_events.append((time.perf_counter(), evt))
                if evt.get("type") == "session.start":
                    await asyncio.sleep(0.05)
                    session_started_sent_at = time.perf_counter()
                    await ws.send(
                        json.dumps(
                            {
                                "type": "session.started",
                                "event_id": "evt_started_1",
                                "session": {
                                    "id": "live_sess_01",
                                    "model": "gpt-live-1",
                                    "status": "active",
                                },
                            }
                        )
                    )
                elif evt.get("type") == "session.input_audio.append":
                    pcm_24k = b"\x05\x00" * 480
                    await ws.send(
                        json.dumps(
                            {
                                "type": "session.output_audio.delta",
                                "delta": base64.b64encode(pcm_24k).decode("ascii"),
                                "start_ms": 0,
                                "end_ms": 20,
                            }
                        )
                    )
                    await ws.close()
                    return

        async with _mock_gpt_live_ws_server(_handler) as (ws_url, conn_log):
            cfg = OpenAIGPTLiveConfig(
                api_key="sk-test-gpt-live-secret",
                model="gpt-live-1",
                voice="marin",
                instructions="Reply concisely.",
                endpoint_url=ws_url,
            )
            assert "sk-test-gpt-live-secret" not in repr(cfg)
            assert "?model=" not in cfg.build_websocket_url()

            adapter = OpenAIGPTLiveMediaAdapter(
                call_id="call_live_init",
                tenant_id="tenant_a",
                config=cfg,
            )
            frame_in = AudioFrame(
                payload=b"\x01\x00" * 320,
                format=AudioFormat.PCM16LE,
                sample_rate=16000,
                channels=1,
                sequence=0,
                timestamp_ms=0.0,
            )
            recv_task = asyncio.create_task(adapter.receive_frame())
            await adapter.send_frame(frame_in)
            out_frame = await asyncio.wait_for(recv_task, timeout=2.0)

            assert out_frame is not None
            assert len(out_frame.payload) == GATEWAY_FRAME_BYTES
            assert out_frame.sample_rate == 16000
            assert out_frame.sequence == 0
            assert out_frame.timestamp_ms == 0.0
            assert adapter.provider_session_id == "live_sess_01"
            assert await asyncio.wait_for(adapter.receive_frame(), timeout=2.0) is None
            await adapter.close()

            assert len(conn_log) == 1
            assert conn_log[0]["path"] == "/v1/live/sessions"
            assert received_client_events[0][1]["type"] == "session.start"
            assert received_client_events[0][1]["session"]["model"] == "gpt-live-1"
            assert received_client_events[0][1]["session"]["delegation"] == {"type": "client"}
            assert received_client_events[1][1]["type"] == "session.input_audio.append"
            # Audio append was strictly gated on session.started!
            assert received_client_events[1][0] >= session_started_sent_at

    asyncio.run(_run())


def test_02_session_start_timeout_and_premature_close_fail_closed() -> None:
    """Verify timeout waiting for session.started or premature EOF raises OpenAIGPTLiveError."""

    async def _run_timeout() -> None:
        async def _silent_handler(ws: Any) -> None:
            async for _raw in ws:
                await asyncio.sleep(5.0)

        async with _mock_gpt_live_ws_server(_silent_handler) as (ws_url, _):
            cfg = OpenAIGPTLiveConfig(
                api_key="sk-test-timeout",
                endpoint_url=ws_url,
                session_start_timeout_seconds=0.1,
            )
            adapter = OpenAIGPTLiveMediaAdapter(
                call_id="call_timeout", tenant_id="tenant_a", config=cfg
            )
            await adapter.send_frame(
                AudioFrame(
                    payload=b"\x01\x00" * 320,
                    format=AudioFormat.PCM16LE,
                    sample_rate=16000,
                    channels=1,
                    sequence=0,
                    timestamp_ms=0.0,
                )
            )
            with pytest.raises(OpenAIGPTLiveError, match="session.started"):
                await asyncio.wait_for(adapter.receive_frame(), timeout=2.0)
            assert adapter.is_closed
            await adapter.close()

    async def _run_premature_eof() -> None:
        async def _eof_handler(ws: Any) -> None:
            await ws.recv()
            await ws.close()

        async with _mock_gpt_live_ws_server(_eof_handler) as (ws_url, _):
            cfg = OpenAIGPTLiveConfig(api_key="sk-test-eof", endpoint_url=ws_url)
            adapter = OpenAIGPTLiveMediaAdapter(
                call_id="call_eof", tenant_id="tenant_a", config=cfg
            )
            with pytest.raises(OpenAIGPTLiveError, match="before session.started"):
                await asyncio.wait_for(adapter.receive_frame(), timeout=2.0)
            assert adapter.is_closed
            await adapter.close()

    async def _run_delta_before_started() -> None:
        async def _bad_order_handler(ws: Any) -> None:
            await ws.recv()
            await ws.send(
                json.dumps(
                    {
                        "type": "session.output_audio.delta",
                        "delta": base64.b64encode(b"\x01\x00" * 480).decode("ascii"),
                    }
                )
            )
            await asyncio.sleep(0.2)

        async with _mock_gpt_live_ws_server(_bad_order_handler) as (ws_url, _):
            cfg = OpenAIGPTLiveConfig(api_key="sk-test-order", endpoint_url=ws_url)
            adapter = OpenAIGPTLiveMediaAdapter(
                call_id="call_order", tenant_id="tenant_a", config=cfg
            )
            with pytest.raises(OpenAIGPTLiveError, match="before session.started"):
                await asyncio.wait_for(adapter.receive_frame(), timeout=2.0)
            assert adapter.is_closed
            await adapter.close()

    asyncio.run(_run_timeout())
    asyncio.run(_run_premature_eof())
    asyncio.run(_run_delta_before_started())


def test_03_interruption_capabilities_and_voice_bridge_turn_control() -> None:
    """Verify truthful capabilities, local output discard, residual delta suppression, and VoiceBridge integration."""

    async def _run() -> None:
        server_events: asyncio.Queue[dict[str, Any] | None] = asyncio.Queue()
        client_events: list[dict[str, Any]] = []
        started_enqueued = asyncio.Event()

        class _ControlledTransport:
            async def send_json(self, payload: dict[str, Any]) -> None:
                client_events.append(payload)
                if payload.get("type") == "session.start":
                    await server_events.put(
                        {
                            "type": "session.started",
                            "session": {"id": "live_int_1", "model": "gpt-live-1"},
                        }
                    )
                    started_enqueued.set()

            async def receive_json(self) -> dict[str, Any] | None:
                return await server_events.get()

            async def close(self) -> None:
                await server_events.put(None)

        async def _connector(_cfg: OpenAIGPTLiveConfig) -> OpenAIGPTLiveTransport:
            return _ControlledTransport()

        cfg = OpenAIGPTLiveConfig(
            api_key="sk-test-int",
            endpoint_url="wss://mock.openai.local/v1/live/sessions",
        )
        adapter = OpenAIGPTLiveMediaAdapter(
            call_id="call_live_int",
            tenant_id="tenant_a",
            config=cfg,
            transport_connector=_connector,
        )
        caps = adapter.interruption_capabilities
        assert caps.can_cancel_future_generation is False
        assert caps.can_discard_local_output is True
        assert caps.can_clear_provider_playback is False
        assert caps.cannot_confirm_playback is True

        tel_port = FakeTelephonyMediaAdapter(name="tel-gpt-live")
        session = VoiceBridgeSession(
            call_id="call_live_int",
            tenant_id="tenant_a",
            telephony_port=tel_port,
            ai_port=adapter,
            queue_capacity=8,
        )
        await session.start()
        await asyncio.wait_for(started_enqueued.wait(), timeout=2.0)

        # Requiring upstream generation cancellation must raise UnsupportedInterruptionCapabilityError!
        with pytest.raises(UnsupportedInterruptionCapabilityError):
            await session.interrupt(require_future_generation_cancellation=True)

        # Deliver Turn 1 audio delta
        await server_events.put(
            {
                "type": "session.output_audio.delta",
                "delta": base64.b64encode(b"\x02\x00" * 480).decode("ascii"),
            }
        )
        deadline = time.perf_counter() + 2.0
        while len(tel_port.sent_frames) < 1 and time.perf_counter() < deadline:
            await asyncio.sleep(0.01)
        assert len(tel_port.sent_frames) == 1
        assert tel_port.sent_frames[0].turn_id == 1

        # Interrupt Turn 1 (without require_future_generation_cancellation)
        outcome = await session.interrupt(turn_id=1)
        assert outcome.interrupted_turn_id == 1
        assert outcome.new_turn_id == 2
        assert outcome.future_generation_cancelled is False

        # Duplicate interruption is idempotent
        outcome_dup = await session.interrupt(turn_id=1)
        assert outcome_dup.new_turn_id == 2

        # Residual in-flight Turn 1 delta arriving before Turn 2 caller speech must be suppressed!
        await server_events.put(
            {
                "type": "session.output_audio.delta",
                "delta": base64.b64encode(b"\x09\x00" * 480).decode("ascii"),
            }
        )
        await asyncio.sleep(0.05)
        assert len(tel_port.sent_frames) == 1

        # Caller sends Turn 2 speech -> unlocks Turn 2 output deltas with turn_id == 2!
        tel_port.push_frame(
            AudioFrame(
                payload=b"\x03\x00" * 320,
                format=AudioFormat.PCM16LE,
                sample_rate=16000,
                channels=1,
                sequence=0,
                timestamp_ms=0.0,
                turn_id=2,
            )
        )
        await asyncio.sleep(0.05)
        await server_events.put(
            {
                "type": "session.output_audio.delta",
                "delta": base64.b64encode(b"\x07\x00" * 480).decode("ascii"),
            }
        )
        deadline = time.perf_counter() + 2.0
        while len(tel_port.sent_frames) < 2 and time.perf_counter() < deadline:
            await asyncio.sleep(0.01)
        assert len(tel_port.sent_frames) == 2
        assert tel_port.sent_frames[1].turn_id == 2
        assert tel_port.sent_frames[1].payload == b"\x07\x00" * 320

        # No response.cancel wire event was ever sent to GPT-Live
        assert not any(e.get("type") == "response.cancel" for e in client_events)
        await session.close()

    asyncio.run(_run())


def test_04_error_sanitization_and_plaintext_ws_rejection(
    tmp_path: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Verify credential scrubbing, .env loading, and plaintext remote ws:// rejection."""
    secret = "sk-gpt-live-super-secret-777"

    with pytest.raises(ValueError, match="wss://"):
        OpenAIGPTLiveConfig(api_key=secret, endpoint_url="ws://api.openai.com/v1/live/sessions")

    dotenv = tmp_path / ".env"
    dotenv.write_text(f"TEST_GPT_LIVE_KEY={secret}\n", encoding="utf-8")
    monkeypatch.delenv("TEST_GPT_LIVE_KEY", raising=False)
    cfg = OpenAIGPTLiveConfig.from_env(env_var="TEST_GPT_LIVE_KEY", dotenv_path=dotenv)
    assert cfg.api_key == secret
    assert secret not in repr(cfg)

    async def _run_err() -> None:
        async def _reject_connector(c: OpenAIGPTLiveConfig) -> OpenAIGPTLiveTransport:
            raise ConnectionRefusedError(f"Rejected key {c.api_key}")

        adapter = OpenAIGPTLiveMediaAdapter(
            call_id="call_err",
            tenant_id="tenant_a",
            config=cfg,
            transport_connector=_reject_connector,
        )
        with pytest.raises(OpenAIGPTLiveError) as exc_info:
            await adapter.receive_frame()
        formatted = "".join(
            traceback.format_exception(type(exc_info.value), exc_info.value, exc_info.tb)
        )
        assert secret not in formatted
        assert "[REDACTED]" in str(exc_info.value)
        await adapter.close()

    asyncio.run(_run_err())


def test_05_backpressure_and_idempotent_close_with_zero_task_leaks() -> None:
    """Verify slow-consumer backpressure overflow and idempotent concurrent close."""

    async def _run() -> None:
        tasks_before = {t for t in asyncio.all_tasks() if not t.done()}

        class _FloodTransport:
            def __init__(self) -> None:
                self._started = False

            async def send_json(self, payload: dict[str, Any]) -> None:
                return None

            async def receive_json(self) -> dict[str, Any] | None:
                await asyncio.sleep(0)
                if not self._started:
                    self._started = True
                    return {
                        "type": "session.started",
                        "session": {"id": "live_flood", "model": "gpt-live-1"},
                    }
                return {
                    "type": "session.output_audio.delta",
                    "delta": base64.b64encode(b"\x01\x00" * 480).decode("ascii"),
                }

            async def close(self) -> None:
                return None

        async def _connector(_cfg: OpenAIGPTLiveConfig) -> OpenAIGPTLiveTransport:
            return _FloodTransport()

        cfg = OpenAIGPTLiveConfig(
            api_key="sk-test-flood",
            endpoint_url="wss://mock.openai.local/v1/live/sessions",
            outbound_queue_capacity=2,
            backpressure_timeout_seconds=0.02,
        )
        adapter = OpenAIGPTLiveMediaAdapter(
            call_id="call_flood",
            tenant_id="tenant_a",
            config=cfg,
            transport_connector=_connector,
        )
        await adapter._ensure_connected()
        await asyncio.sleep(0.08)
        assert adapter.is_closed
        with pytest.raises(MediaBackpressureOverflowError):
            await adapter.receive_frame()

        await asyncio.gather(adapter.close(), adapter.close(), adapter.close())
        await asyncio.sleep(0)
        tasks_after = {t for t in asyncio.all_tasks() if not t.done()}
        assert tasks_after - tasks_before == set()

    asyncio.run(_run())


def test_06_synthetic_media_websocket_end_to_end_with_gpt_live_factory() -> None:
    """Verify end-to-end /ws/synthetic-media/v1 -> VoiceBridge -> OpenAIGPTLiveMediaAdapter flow."""

    async def _run() -> None:
        async def _handler(ws: Any) -> None:
            async for raw in ws:
                evt = json.loads(raw)
                if evt.get("type") == "session.start":
                    await ws.send(
                        json.dumps(
                            {
                                "type": "session.started",
                                "session": {"id": "live_e2e_1", "model": "gpt-live-1"},
                            }
                        )
                    )
                elif evt.get("type") == "session.input_audio.append":
                    await ws.send(
                        json.dumps(
                            {
                                "type": "session.output_audio.delta",
                                "delta": evt["audio"],
                            }
                        )
                    )

        async with _mock_gpt_live_ws_server(_handler) as (ws_url, _):
            gpt_live_cfg = OpenAIGPTLiveConfig(
                api_key="sk-test-e2e-gpt-live",
                endpoint_url=ws_url,
            )
            secret = b"synthetic-hmac-secret-gpt-live-e2e-32bytes"
            verifier = SyntheticTokenVerifier(secret_key=secret)
            token = verifier.issue_token(
                provider_id="synthetic",
                tenant_id="tenant_live",
                call_id="call_live_e2e",
                allowed_scopes={"voice.stream"},
            )
            app = create_gateway_app(
                CoreConfig(instance_name="gw_gpt_live_e2e", ingress_mode="synthetic"),
                synthetic_auth_bridge=SyntheticAuthBridge(verifier),
                enable_synthetic_media=True,
                ai_media_port_factory=create_openai_gpt_live_adapter_factory(gpt_live_cfg),
            )
            import socket

            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.bind(("127.0.0.1", 0))
                gw_port = int(s.getsockname()[1])

            server = uvicorn.Server(
                uvicorn.Config(app=app, host="127.0.0.1", port=gw_port, log_level="warning")
            )
            srv_task = asyncio.create_task(server.serve())
            for _ in range(100):
                if server.started:
                    break
                await asyncio.sleep(0.02)
            assert server.started

            try:
                # 1. Normal bidirectional call over /ws/synthetic-media/v1
                async with websockets.connect(
                    f"ws://127.0.0.1:{gw_port}/ws/synthetic-media/v1",
                    additional_headers={"Authorization": f"Bearer {token}"},
                ) as ws:
                    ready = json.loads(await ws.recv())
                    assert ready["event"] == "session.ready"
                    await ws.send(
                        json.dumps(
                            {
                                "event": "call.start",
                                "call_id": "call_live_e2e",
                            }
                        )
                    )
                    started = json.loads(await ws.recv())
                    assert started["event"] == "call.started"

                    await ws.send(b"\x04\x00" * 320)
                    audio_out = await asyncio.wait_for(ws.recv(), timeout=2.0)
                    assert isinstance(audio_out, bytes)
                    assert len(audio_out) == 640

                    await ws.send(
                        json.dumps(
                            {
                                "event": "call.end",
                                "call_id": "call_live_e2e",
                            }
                        )
                    )
                    ended = json.loads(await asyncio.wait_for(ws.recv(), timeout=2.0))
                    assert ended["event"] == "call.ended"

                # 2. Sending call.interrupt succeeds via local output purge/fencing
                # and truthfully reports future_generation_cancelled=False.
                token_2 = verifier.issue_token(
                    provider_id="synthetic",
                    tenant_id="tenant_live",
                    call_id="call_live_e2e_int",
                    allowed_scopes={"voice.stream"},
                )
                async with websockets.connect(
                    f"ws://127.0.0.1:{gw_port}/ws/synthetic-media/v1",
                    additional_headers={"Authorization": f"Bearer {token_2}"},
                ) as ws2:
                    await ws2.recv()  # session.ready
                    await ws2.send(
                        json.dumps({"event": "call.start", "call_id": "call_live_e2e_int"})
                    )
                    await ws2.recv()  # call.started
                    await ws2.send(
                        json.dumps(
                            {
                                "event": "call.interrupt",
                                "call_id": "call_live_e2e_int",
                                "turn_id": 1,
                                "request_id": "req-gpt-live-int-1",
                            }
                        )
                    )
                    interrupted = json.loads(await asyncio.wait_for(ws2.recv(), timeout=2.0))
                    assert interrupted["event"] == "call.interrupted"
                    assert interrupted["interrupted_turn_id"] == 1
                    assert interrupted["new_turn_id"] == 2
                    assert interrupted["future_generation_cancelled"] is False
                    assert interrupted["request_id"] == "req-gpt-live-int-1"

                    await ws2.send(
                        json.dumps({"event": "call.end", "call_id": "call_live_e2e_int"})
                    )
                    ended2 = json.loads(await asyncio.wait_for(ws2.recv(), timeout=2.0))
                    assert ended2["event"] == "call.ended"
                    await asyncio.wait_for(ws2.wait_closed(), timeout=2.0)
                    assert ws2.close_code == 1000
            finally:
                server.should_exit = True
                await asyncio.wait_for(srv_task, timeout=5.0)

    asyncio.run(_run())


def test_07_simultaneous_send_receive_frame_continuity_and_multi_tenant_isolation() -> None:
    """Verify full-duplex simultaneous streaming, frame continuity across irregular chunks, and multi-tenant isolation."""

    async def _run() -> None:
        async def _handler(ws: Any) -> None:
            async for raw in ws:
                evt = json.loads(raw)
                if evt.get("type") == "session.start":
                    await ws.send(
                        json.dumps(
                            {
                                "type": "session.started",
                                "session": {"id": "live_iso", "model": "gpt-live-1"},
                            }
                        )
                    )
                elif evt.get("type") == "session.input_audio.append":
                    raw_24k = base64.b64decode(evt["audio"])
                    # Split into two irregular sub-frame deltas to exercise OutboundFramePacker continuity
                    mid = len(raw_24k) // 3
                    await ws.send(
                        json.dumps(
                            {
                                "type": "session.output_audio.delta",
                                "delta": base64.b64encode(raw_24k[:mid]).decode("ascii"),
                            }
                        )
                    )
                    await ws.send(
                        json.dumps(
                            {
                                "type": "session.output_audio.delta",
                                "delta": base64.b64encode(raw_24k[mid:]).decode("ascii"),
                            }
                        )
                    )

        async with _mock_gpt_live_ws_server(_handler) as (ws_url, conn_log):
            cfg = OpenAIGPTLiveConfig(api_key="sk-test-iso", endpoint_url=ws_url)
            factory = create_openai_gpt_live_adapter_factory(cfg)
            tel_a = FakeTelephonyMediaAdapter(name="tel-a")
            tel_b = FakeTelephonyMediaAdapter(name="tel-b")
            sess_a = VoiceBridgeSession(
                call_id="call_a",
                tenant_id="tenant_a",
                telephony_port=tel_a,
                ai_port=factory(call_id="call_a", tenant_id="tenant_a"),
                queue_capacity=16,
            )
            sess_b = VoiceBridgeSession(
                call_id="call_b",
                tenant_id="tenant_b",
                telephony_port=tel_b,
                ai_port=factory(call_id="call_b", tenant_id="tenant_b"),
                queue_capacity=16,
            )
            await asyncio.gather(sess_a.start(), sess_b.start())

            for seq in range(5):
                tel_a.push_frame(
                    AudioFrame(
                        payload=b"\x10\x00" * 320,
                        format=AudioFormat.PCM16LE,
                        sample_rate=16000,
                        channels=1,
                        sequence=seq,
                        timestamp_ms=seq * 20.0,
                    )
                )
                tel_b.push_frame(
                    AudioFrame(
                        payload=b"\x20\x00" * 320,
                        format=AudioFormat.PCM16LE,
                        sample_rate=16000,
                        channels=1,
                        sequence=seq,
                        timestamp_ms=seq * 20.0,
                    )
                )

            deadline = time.perf_counter() + 3.0
            while (len(tel_a.sent_frames) < 5 or len(tel_b.sent_frames) < 5) and (
                time.perf_counter() < deadline
            ):
                await asyncio.sleep(0.01)

            await asyncio.gather(sess_a.close(), sess_b.close())
            assert len(conn_log) == 2
            assert len(tel_a.sent_frames) == 5
            assert len(tel_b.sent_frames) == 5
            assert [f.sequence for f in tel_a.sent_frames] == [0, 1, 2, 3, 4]
            assert [f.timestamp_ms for f in tel_a.sent_frames] == [0.0, 20.0, 40.0, 60.0, 80.0]
            assert all(f.payload == b"\x10\x00" * 320 for f in tel_a.sent_frames)
            assert all(f.payload == b"\x20\x00" * 320 for f in tel_b.sent_frames)

    asyncio.run(_run())


def test_08_provider_error_disconnect_and_connection_cancellation() -> None:
    """Verify provider error event, mid-stream disconnect, and cancellation during handshake."""

    async def _run_provider_error() -> None:
        async def _err_handler(ws: Any) -> None:
            async for raw in ws:
                evt = json.loads(raw)
                if evt.get("type") == "session.start":
                    await ws.send(
                        json.dumps(
                            {
                                "type": "error",
                                "error": {
                                    "code": "rate_limit_exceeded",
                                    "message": "Too many GPT-Live sessions",
                                },
                            }
                        )
                    )
                    return

        async with _mock_gpt_live_ws_server(_err_handler) as (ws_url, _):
            cfg = OpenAIGPTLiveConfig(api_key="sk-test-err-evt", endpoint_url=ws_url)
            adapter = OpenAIGPTLiveMediaAdapter(
                call_id="call_err_evt", tenant_id="tenant_a", config=cfg
            )
            with pytest.raises(OpenAIGPTLiveError, match="rate_limit_exceeded"):
                await asyncio.wait_for(adapter.receive_frame(), timeout=2.0)
            assert adapter.is_closed
            await adapter.close()

    async def _run_mid_stream_disconnect() -> None:
        class _DropTransport:
            def __init__(self) -> None:
                self._count = 0

            async def send_json(self, payload: dict[str, Any]) -> None:
                return None

            async def receive_json(self) -> dict[str, Any] | None:
                self._count += 1
                if self._count == 1:
                    return {
                        "type": "session.started",
                        "session": {"id": "live_drop", "model": "gpt-live-1"},
                    }
                raise ConnectionResetError("Peer dropped GPT-Live socket")

            async def close(self) -> None:
                return None

        async def _connector(_cfg: OpenAIGPTLiveConfig) -> OpenAIGPTLiveTransport:
            return _DropTransport()

        cfg = OpenAIGPTLiveConfig(
            api_key="sk-test-drop",
            endpoint_url="wss://mock.openai.local/v1/live/sessions",
        )
        adapter = OpenAIGPTLiveMediaAdapter(
            call_id="call_drop",
            tenant_id="tenant_a",
            config=cfg,
            transport_connector=_connector,
        )
        with pytest.raises(OpenAIGPTLiveError, match="Peer dropped GPT-Live socket"):
            await asyncio.wait_for(adapter.receive_frame(), timeout=2.0)
        assert adapter.is_closed
        await adapter.close()

    async def _run_connect_cancellation() -> None:
        entered = asyncio.Event()
        closed = False

        class _SlowTransport:
            async def send_json(self, payload: dict[str, Any]) -> None:
                entered.set()
                await asyncio.sleep(10.0)

            async def receive_json(self) -> dict[str, Any] | None:
                return None

            async def close(self) -> None:
                nonlocal closed
                closed = True

        async def _connector(_cfg: OpenAIGPTLiveConfig) -> OpenAIGPTLiveTransport:
            return _SlowTransport()

        cfg = OpenAIGPTLiveConfig(
            api_key="sk-test-cancel-conn",
            endpoint_url="wss://mock.openai.local/v1/live/sessions",
        )
        adapter = OpenAIGPTLiveMediaAdapter(
            call_id="call_cancel_conn",
            tenant_id="tenant_a",
            config=cfg,
            transport_connector=_connector,
        )
        t = asyncio.create_task(adapter.receive_frame())
        await asyncio.wait_for(entered.wait(), timeout=2.0)
        t.cancel()
        with pytest.raises(asyncio.CancelledError):
            await t
        assert closed is True
        assert adapter.is_closed is True
        await adapter.close()

    asyncio.run(_run_provider_error())
    asyncio.run(_run_mid_stream_disconnect())
    asyncio.run(_run_connect_cancellation())


def test_09_multi_frame_delta_burst_interrupt_preserves_contiguous_sequence_in_bridge() -> None:
    """Verify mid-burst local turn purge preserves contiguous outbound sequence numbers for VoiceBridgeSession."""
    from kaiwen_gateway.modules.voice import (
        InterruptibleFakeTelephonyAdapter,
        VoiceBridgeState,
    )

    async def _run() -> None:
        rx_q: asyncio.Queue[dict[str, Any] | None] = asyncio.Queue()

        class _BurstTransport:
            def __init__(self) -> None:
                self._appends = 0

            async def send_json(self, payload: dict[str, Any]) -> None:
                if payload.get("type") == "session.start":
                    await rx_q.put(
                        {
                            "type": "session.started",
                            "session": {"id": "live_burst_seq", "model": "gpt-live-1"},
                        }
                    )
                elif payload.get("type") == "session.input_audio.append":
                    self._appends += 1
                    if self._appends == 1:
                        burst_b64 = base64.b64encode(b"\x11\x00" * 9600).decode("ascii")
                        await rx_q.put({"type": "session.output_audio.delta", "delta": burst_b64})
                    elif self._appends == 2:
                        turn2_b64 = base64.b64encode(b"\x22\x00" * 960).decode("ascii")
                        await rx_q.put({"type": "session.output_audio.delta", "delta": turn2_b64})

            async def receive_json(self) -> dict[str, Any] | None:
                return await rx_q.get()

            async def close(self) -> None:
                await rx_q.put(None)

        async def _connector(_cfg: OpenAIGPTLiveConfig) -> OpenAIGPTLiveTransport:
            return _BurstTransport()

        cfg = OpenAIGPTLiveConfig(
            api_key="sk-test-burst-seq",
            endpoint_url="wss://mock.openai.local/v1/live/sessions",
        )
        adapter = OpenAIGPTLiveMediaAdapter(
            call_id="call_burst_seq",
            tenant_id="tenant_a",
            config=cfg,
            transport_connector=_connector,
        )

        class _SlowSendTelephony(InterruptibleFakeTelephonyAdapter):
            async def send_frame(self, frame: AudioFrame) -> None:
                await super().send_frame(frame)
                await asyncio.sleep(0.008)

        tel = _SlowSendTelephony()
        bridge = VoiceBridgeSession(
            call_id="call_burst_seq",
            tenant_id="tenant_a",
            telephony_port=tel,
            ai_port=adapter,
        )

        async with bridge:
            # Trigger connection and queue a 10-frame (9600 bytes @ 24kHz) delta burst for Turn 1
            tel.push_frame(
                AudioFrame(
                    payload=b"\x01\x00" * 320,
                    format=AudioFormat.PCM16LE,
                    sample_rate=16_000,
                    channels=1,
                    sequence=0,
                    timestamp_ms=0.0,
                    turn_id=1,
                )
            )

            # Wait until at least 2 frames are received by telephony, leaving ~8 frames in adapter/bridge queue
            for _ in range(100):
                if len(tel.sent_frames) >= 2:
                    break
                await asyncio.sleep(0.005)
            assert 2 <= len(tel.sent_frames) < 10

            # Interrupt Turn 1 -> purges remaining queued Turn 1 frames inside adapter and bridge
            outcome = await bridge.interrupt(
                turn_id=1,
                require_future_generation_cancellation=False,
            )
            assert outcome.interrupted_turn_id == 1
            assert outcome.new_turn_id == 2
            assert outcome.future_generation_cancelled is False

            # Send Turn 2 caller audio to lift suppression and trigger Turn 2 delta
            tel.push_frame(
                AudioFrame(
                    payload=b"\x02\x00" * 320,
                    format=AudioFormat.PCM16LE,
                    sample_rate=16_000,
                    channels=1,
                    sequence=1,
                    timestamp_ms=20.0,
                    turn_id=2,
                )
            )

            for _ in range(100):
                if sum(1 for f in tel.sent_frames if f.turn_id == 2) >= 2:
                    break
                await asyncio.sleep(0.01)

            turn1_frames = [f for f in tel.sent_frames if f.turn_id == 1]
            turn2_frames = [f for f in tel.sent_frames if f.turn_id == 2]
            assert len(turn2_frames) >= 2
            expected_turn2_start = (
                len(turn1_frames)
                + outcome.locally_discarded_frames
                + bridge.stale_frames_suppressed
            )
            assert [f.sequence for f in turn2_frames] == [
                expected_turn2_start + i for i in range(len(turn2_frames))
            ]
            assert turn2_frames[0].sequence < 20
            assert bridge.state != VoiceBridgeState.FAILED

    asyncio.run(_run())
