"""Comprehensive integration tests for OpenAIRealtimeMediaAdapter (mocked local server, zero API keys)."""

from __future__ import annotations

import asyncio
import base64
import json
import math
import struct
import time
import traceback
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Any

import pytest
import websockets
from starlette.testclient import TestClient
from starlette.websockets import WebSocketDisconnect

from kaiwen_gateway.adapters.openai_realtime import (
    OpenAIRealtimeConfig,
    OpenAIRealtimeError,
    OpenAIRealtimeMediaAdapter,
    OpenAIRealtimeTransport,
    create_openai_realtime_adapter_factory,
)
from kaiwen_gateway.adapters.openai_realtime.audio import (
    GATEWAY_FRAME_BYTES,
    OutboundFramePacker,
    PCM16Resampler16kTo24k,
    PCM16Resampler24kTo16k,
)
from kaiwen_gateway.core import CoreConfig
from kaiwen_gateway.modules.voice import (
    AudioFormat,
    AudioFrame,
    MediaBackpressureOverflowError,
    MediaPortClosedError,
    VoiceBridgeSession,
)
from kaiwen_gateway.modules.voice.adapters.fake import FakeTelephonyMediaAdapter
from kaiwen_gateway.server import create_gateway_app
from kaiwen_gateway.transports.synthetic import SyntheticAuthBridge, SyntheticTokenVerifier


def _make_sine_pcm16le(
    sample_rate: int, freq_hz: float, num_samples: int, amp: int = 8000
) -> bytes:
    samples = [
        int(amp * math.sin(2.0 * math.pi * freq_hz * i / sample_rate)) for i in range(num_samples)
    ]
    return struct.pack(f"<{len(samples)}h", *samples)


@asynccontextmanager
async def _mock_openai_ws_server(
    handler: Any,
) -> AsyncIterator[tuple[str, list[dict[str, Any]]]]:
    """Run an ephemeral local WebSocket server simulating wss://api.openai.com/v1/realtime."""
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
        yield f"ws://127.0.0.1:{port}/v1/realtime", connections_log
    finally:
        server.close()
        await server.wait_closed()


def test_01_successful_session_initialization_and_single_upstream_session() -> None:
    """Verify lazy connect, GA session.update schema, auth header, and single session under concurrency."""

    async def _run() -> None:
        received_client_events: list[dict[str, Any]] = []

        async def _handler(ws: Any) -> None:
            await ws.send(json.dumps({"type": "session.created", "session": {"id": "sess_1"}}))
            async for raw in ws:
                evt = json.loads(raw)
                received_client_events.append(evt)
                if evt.get("type") == "session.update":
                    await ws.send(
                        json.dumps({"type": "session.updated", "session": evt["session"]})
                    )
                elif evt.get("type") == "input_audio_buffer.append":
                    await ws.send(
                        json.dumps(
                            {
                                "type": "response.created",
                                "response": {"id": "resp_1", "status": "in_progress"},
                            }
                        )
                    )
                    pcm_24k = b"\x05\x00" * 480
                    await ws.send(
                        json.dumps(
                            {
                                "type": "response.output_audio.delta",
                                "response_id": "resp_1",
                                "delta": base64.b64encode(pcm_24k).decode("ascii"),
                            }
                        )
                    )
                    await ws.send(
                        json.dumps({"type": "response.done", "response": {"id": "resp_1"}})
                    )
                    await ws.close()
                    return

        async with _mock_openai_ws_server(_handler) as (ws_url, conn_log):
            cfg = OpenAIRealtimeConfig(
                api_key="sk-test-secret-key-123",
                model="gpt-realtime",
                voice="alloy",
                instructions="Speak clearly.",
                endpoint_url=ws_url,
            )
            assert "sk-test-secret-key-123" not in repr(cfg)

            adapter = OpenAIRealtimeMediaAdapter(
                call_id="call_init_1",
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
            # Trigger concurrent send_frame and receive_frame on uninitialized adapter
            recv_task = asyncio.create_task(adapter.receive_frame())
            await adapter.send_frame(frame_in)
            out_frame = await asyncio.wait_for(recv_task, timeout=2.0)

            assert out_frame is not None
            assert len(out_frame.payload) == GATEWAY_FRAME_BYTES
            assert out_frame.sample_rate == 16000
            assert await asyncio.wait_for(adapter.receive_frame(), timeout=2.0) is None
            await adapter.close()

        assert len(conn_log) == 1
        assert "model=gpt-realtime" in conn_log[0]["path"]
        auth_val = conn_log[0]["headers"].get("Authorization") or conn_log[0]["headers"].get(
            "authorization"
        )
        assert auth_val == "Bearer sk-test-secret-key-123"

        session_updates = [e for e in received_client_events if e["type"] == "session.update"]
        assert len(session_updates) == 1
        sess = session_updates[0]["session"]
        assert sess["type"] == "realtime"
        assert sess["output_modalities"] == ["audio"]
        assert sess["audio"]["input"]["format"] == {"type": "audio/pcm", "rate": 24000}
        assert sess["audio"]["output"]["format"] == {"type": "audio/pcm", "rate": 24000}
        assert sess["audio"]["output"]["voice"] == "alloy"
        assert sess["instructions"] == "Speak clearly."

    asyncio.run(_run())


def test_02_concurrent_audio_send_and_receive() -> None:
    """Verify simultaneous bidirectional audio streaming over local WebSocket."""

    async def _run() -> None:
        async def _handler(ws: Any) -> None:
            await ws.send(json.dumps({"type": "session.created"}))
            append_count = 0
            async for raw in ws:
                evt = json.loads(raw)
                if evt.get("type") == "input_audio_buffer.append":
                    append_count += 1
                    await ws.send(
                        json.dumps(
                            {
                                "type": "response.output_audio.delta",
                                "response_id": "resp_conc",
                                "delta": evt["audio"],
                            }
                        )
                    )
                    if append_count == 5:
                        await ws.close()
                        return

        async with _mock_openai_ws_server(_handler) as (ws_url, _):
            cfg = OpenAIRealtimeConfig(api_key="sk-test-conc", endpoint_url=ws_url)
            adapter = OpenAIRealtimeMediaAdapter(
                call_id="call_conc", tenant_id="tenant_a", config=cfg
            )

            async def _sender() -> None:
                for seq in range(5):
                    await adapter.send_frame(
                        AudioFrame(
                            payload=b"\x02\x00" * 320,
                            format=AudioFormat.PCM16LE,
                            sample_rate=16000,
                            channels=1,
                            sequence=seq,
                            timestamp_ms=seq * 20.0,
                        )
                    )

            async def _receiver() -> list[AudioFrame]:
                frames: list[AudioFrame] = []
                while True:
                    f = await asyncio.wait_for(adapter.receive_frame(), timeout=2.0)
                    if f is None:
                        return frames
                    frames.append(f)

            _, received = await asyncio.gather(_sender(), _receiver())
            assert len(received) == 5
            assert [f.sequence for f in received] == [0, 1, 2, 3, 4]
            await adapter.close()

    asyncio.run(_run())


def test_03_audio_conversion_correctness_and_overhead() -> None:
    """Verify 16k<->24k resampling, arbitrary delta chunking, frame boundaries, and low overhead."""
    r_up = PCM16Resampler16kTo24k()
    r_down = PCM16Resampler24kTo16k()
    packer = OutboundFramePacker()

    # 10 frames of 440 Hz sine wave at 16 kHz (200 ms)
    src_16k = _make_sine_pcm16le(16_000, 440.0, 320 * 10)
    up_24k = bytearray()
    t0 = time.perf_counter()
    for i in range(10):
        chunk_16k = src_16k[i * 640 : (i + 1) * 640]
        resampled_24k = r_up.resample(chunk_16k)
        assert len(resampled_24k) == 960  # 20 ms @ 24 kHz = 480 samples = 960 bytes
        up_24k.extend(resampled_24k)

    # Feed 24 kHz bytes in irregular/odd chunk sizes (e.g. 333 bytes)
    out_frames: list[AudioFrame] = []
    pos = 0
    while pos < len(up_24k):
        piece = bytes(up_24k[pos : pos + 333])
        pos += 333
        down_16k = r_down.resample(piece)
        out_frames.extend(packer.push_bytes(down_16k, turn_id=0))
    elapsed_ms = (time.perf_counter() - t0) * 1000.0

    # At least 9 complete 640-byte frames (10th may have 1-sample tail before flush)
    assert len(out_frames) >= 9
    for idx, frame in enumerate(out_frames):
        assert len(frame.payload) == 640
        assert frame.sample_rate == 16000
        assert frame.channels == 1
        assert frame.sequence == idx
        assert frame.timestamp_ms == idx * 20.0

    # Verify non-zero signal energy preserved (not zeroed or corrupted)
    first_samples = struct.unpack("<320h", out_frames[0].payload)
    rms = math.sqrt(sum(s * s for s in first_samples) / len(first_samples))
    assert rms > 4000.0
    # Overhead per 20ms frame should be well under 5 ms in Python
    assert elapsed_ms / 10.0 < 5.0


@pytest.mark.parametrize(
    "bad_event",
    [
        {"no_type": True},
        {"type": "response.output_audio.delta", "delta": "!!!not-valid-base64!!!"},
        {"type": "response.output_audio.delta", "delta": 12345},
    ],
)
def test_04_invalid_provider_event_handling(bad_event: dict[str, Any]) -> None:
    """Verify invalid or malformed provider events fail closed with OpenAIRealtimeError."""

    async def _run() -> None:
        async def _handler(ws: Any) -> None:
            await ws.send(json.dumps(bad_event))
            await asyncio.sleep(0.2)

        async with _mock_openai_ws_server(_handler) as (ws_url, _):
            cfg = OpenAIRealtimeConfig(api_key="sk-test-bad-evt", endpoint_url=ws_url)
            adapter = OpenAIRealtimeMediaAdapter(
                call_id="call_bad_evt", tenant_id="tenant_a", config=cfg
            )
            with pytest.raises(OpenAIRealtimeError):
                await asyncio.wait_for(adapter.receive_frame(), timeout=2.0)
            assert adapter.is_closed
            await adapter.close()

    asyncio.run(_run())


def test_05_upstream_connection_rejection_sanitized() -> None:
    """Verify upstream handshake failure is sanitized and never leaks API key."""

    async def _run() -> None:
        secret_key = "sk-super-secret-do-not-leak"

        async def _rejecting_connector(cfg: OpenAIRealtimeConfig) -> OpenAIRealtimeTransport:
            raise ConnectionRefusedError(f"401 Unauthorized for key {cfg.api_key}")

        cfg = OpenAIRealtimeConfig(
            api_key=secret_key, endpoint_url="wss://mock.openai.local/v1/realtime"
        )
        adapter = OpenAIRealtimeMediaAdapter(
            call_id="call_reject",
            tenant_id="tenant_a",
            config=cfg,
            transport_connector=_rejecting_connector,
        )
        with pytest.raises(OpenAIRealtimeError) as exc_info:
            await adapter.receive_frame()
        assert secret_key not in str(exc_info.value)
        assert "[REDACTED]" in str(exc_info.value)
        assert adapter.is_closed
        await adapter.close()

    asyncio.run(_run())


def test_06_mid_stream_disconnect() -> None:
    """Verify unexpected mid-stream transport failure closes adapter and unblocks receiver."""

    async def _run() -> None:
        class _DropTransport:
            async def send_json(self, payload: dict[str, Any]) -> None:
                return None

            async def receive_json(self) -> dict[str, Any] | None:
                raise ConnectionResetError("TCP RST from peer")

            async def close(self) -> None:
                return None

        async def _connector(_cfg: OpenAIRealtimeConfig) -> OpenAIRealtimeTransport:
            return _DropTransport()

        cfg = OpenAIRealtimeConfig(
            api_key="sk-test-drop", endpoint_url="wss://mock.openai.local/v1/realtime"
        )
        adapter = OpenAIRealtimeMediaAdapter(
            call_id="call_drop",
            tenant_id="tenant_a",
            config=cfg,
            transport_connector=_connector,
        )
        with pytest.raises(OpenAIRealtimeError, match="TCP RST"):
            await asyncio.wait_for(adapter.receive_frame(), timeout=2.0)
        assert adapter.is_closed
        await adapter.close()

    asyncio.run(_run())


def test_07_and_08_rate_limit_and_provider_error_during_output() -> None:
    """Verify rate-limit and mid-output provider error events propagate cleanly."""

    async def _run(err_code: str, err_msg: str) -> None:
        send_error = asyncio.Event()

        async def _handler(ws: Any) -> None:
            await ws.send(
                json.dumps(
                    {
                        "type": "response.output_audio.delta",
                        "response_id": "resp_err",
                        "delta": base64.b64encode(b"\x03\x00" * 480).decode("ascii"),
                    }
                )
            )
            await send_error.wait()
            await ws.send(
                json.dumps(
                    {
                        "type": "error",
                        "error": {"type": "rate_limit_error", "code": err_code, "message": err_msg},
                    }
                )
            )
            await asyncio.sleep(0.2)

        async with _mock_openai_ws_server(_handler) as (ws_url, _):
            cfg = OpenAIRealtimeConfig(api_key="sk-test-err", endpoint_url=ws_url)
            adapter = OpenAIRealtimeMediaAdapter(
                call_id="call_err", tenant_id="tenant_a", config=cfg
            )
            first = await asyncio.wait_for(adapter.receive_frame(), timeout=2.0)
            assert first is not None
            send_error.set()
            with pytest.raises(OpenAIRealtimeError, match=err_code):
                await asyncio.wait_for(adapter.receive_frame(), timeout=2.0)
            assert adapter.is_closed
            await adapter.close()

    asyncio.run(_run("rate_limit_exceeded", "Too many requests"))
    asyncio.run(_run("server_error", "Internal upstream error"))


def test_09_and_10_interruption_and_stale_output_suppression() -> None:
    """Verify cancel_turn sends response.cancel, purges buffers, and drops delayed stale deltas."""

    async def _run() -> None:
        sent_to_server: list[dict[str, Any]] = []
        cancel_seen = asyncio.Event()

        async def _handler(ws: Any) -> None:
            await ws.send(json.dumps({"type": "response.created", "response": {"id": "resp_t0"}}))
            await ws.send(
                json.dumps(
                    {
                        "type": "response.output_audio.delta",
                        "response_id": "resp_t0",
                        "delta": base64.b64encode(b"\x01\x00" * 480).decode("ascii"),
                    }
                )
            )
            async for raw in ws:
                evt = json.loads(raw)
                sent_to_server.append(evt)
                if evt.get("type") == "response.cancel":
                    cancel_seen.set()
                    # Late delta from cancelled turn 0 (must be suppressed!)
                    await ws.send(
                        json.dumps(
                            {
                                "type": "response.output_audio.delta",
                                "response_id": "resp_t0",
                                "delta": base64.b64encode(b"\x09\x00" * 480).decode("ascii"),
                            }
                        )
                    )
                    # New turn 1 response (must be delivered!)
                    await ws.send(
                        json.dumps({"type": "response.created", "response": {"id": "resp_t1"}})
                    )
                    await ws.send(
                        json.dumps(
                            {
                                "type": "response.output_audio.delta",
                                "response_id": "resp_t1",
                                "delta": base64.b64encode(b"\x07\x00" * 480).decode("ascii"),
                            }
                        )
                    )
                    await ws.close()
                    return

        async with _mock_openai_ws_server(_handler) as (ws_url, _):
            cfg = OpenAIRealtimeConfig(api_key="sk-test-int", endpoint_url=ws_url)
            adapter = OpenAIRealtimeMediaAdapter(
                call_id="call_int", tenant_id="tenant_a", config=cfg
            )
            caps = adapter.interruption_capabilities
            assert caps.can_cancel_future_generation is True
            assert caps.can_discard_local_output is True
            assert caps.can_clear_provider_playback is False
            assert caps.cannot_confirm_playback is True

            f0 = await asyncio.wait_for(adapter.receive_frame(), timeout=2.0)
            assert f0 is not None and f0.turn_id == 1

            await adapter.cancel_turn(1)
            await asyncio.wait_for(cancel_seen.wait(), timeout=2.0)

            f1 = await asyncio.wait_for(adapter.receive_frame(), timeout=2.0)
            assert f1 is not None
            assert f1.turn_id == 2
            assert f1.payload == b"\x07\x00" * 320
            assert await asyncio.wait_for(adapter.receive_frame(), timeout=2.0) is None
            await adapter.close()

        assert any(e["type"] == "response.cancel" for e in sent_to_server)

    asyncio.run(_run())


def test_11_and_12_slow_consumer_and_queue_saturation() -> None:
    """Verify bounded outbound and inbound queues raise MediaBackpressureOverflowError on saturation."""

    async def _run_slow_consumer() -> None:
        class _FloodTransport:
            def __init__(self) -> None:
                self._count = 0

            async def send_json(self, payload: dict[str, Any]) -> None:
                return None

            async def receive_json(self) -> dict[str, Any] | None:
                self._count += 1
                await asyncio.sleep(0)
                return {
                    "type": "response.output_audio.delta",
                    "response_id": "resp_flood",
                    "delta": base64.b64encode(b"\x01\x00" * 480).decode("ascii"),
                }

            async def close(self) -> None:
                return None

        async def _connector(_cfg: OpenAIRealtimeConfig) -> OpenAIRealtimeTransport:
            return _FloodTransport()

        cfg = OpenAIRealtimeConfig(
            api_key="sk-test-slow",
            endpoint_url="wss://mock.openai.local/v1/realtime",
            outbound_queue_capacity=2,
            backpressure_timeout_seconds=0.02,
        )
        adapter = OpenAIRealtimeMediaAdapter(
            call_id="call_slow", tenant_id="tenant_a", config=cfg, transport_connector=_connector
        )
        await adapter._ensure_connected()
        await asyncio.sleep(0.08)
        assert adapter.is_closed
        with pytest.raises(MediaBackpressureOverflowError):
            await adapter.receive_frame()
        await adapter.close()

    async def _run_inbound_saturation() -> None:
        gate = asyncio.Event()

        class _BlockedSendTransport:
            async def send_json(self, payload: dict[str, Any]) -> None:
                if payload.get("type") == "input_audio_buffer.append":
                    await gate.wait()

            async def receive_json(self) -> dict[str, Any] | None:
                await gate.wait()
                return None

            async def close(self) -> None:
                gate.set()

        async def _connector(_cfg: OpenAIRealtimeConfig) -> OpenAIRealtimeTransport:
            return _BlockedSendTransport()

        cfg = OpenAIRealtimeConfig(
            api_key="sk-test-sat",
            endpoint_url="wss://mock.openai.local/v1/realtime",
            inbound_queue_capacity=1,
            backpressure_timeout_seconds=0.02,
        )
        adapter = OpenAIRealtimeMediaAdapter(
            call_id="call_sat", tenant_id="tenant_a", config=cfg, transport_connector=_connector
        )
        with pytest.raises(MediaBackpressureOverflowError):
            for seq in range(5):
                await adapter.send_frame(
                    AudioFrame(
                        payload=b"\x01\x00" * 320,
                        format=AudioFormat.PCM16LE,
                        sample_rate=16000,
                        channels=1,
                        sequence=seq,
                        timestamp_ms=seq * 20.0,
                    )
                )
        await adapter.close()

    asyncio.run(_run_slow_consumer())
    asyncio.run(_run_inbound_saturation())


def test_13_and_14_close_during_pending_tasks_and_repeated_close() -> None:
    """Verify idempotent/concurrent close cancels all background tasks with zero task leaks."""

    async def _run() -> None:
        tasks_before = {t for t in asyncio.all_tasks() if not t.done()}

        async def _handler(ws: Any) -> None:
            await asyncio.sleep(30)

        async with _mock_openai_ws_server(_handler) as (ws_url, _):
            cfg = OpenAIRealtimeConfig(api_key="sk-test-close", endpoint_url=ws_url)
            adapter = OpenAIRealtimeMediaAdapter(
                call_id="call_close", tenant_id="tenant_a", config=cfg
            )
            await adapter._ensure_connected()
            await asyncio.gather(adapter.close(), adapter.close(), adapter.close())
            assert adapter.is_closed
            with pytest.raises(MediaPortClosedError):
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

        await asyncio.sleep(0)
        tasks_after = {t for t in asyncio.all_tasks() if not t.done()}
        assert tasks_after - tasks_before == set()

    asyncio.run(_run())


def test_15_multi_call_and_multi_tenant_isolation_via_voice_bridge() -> None:
    """Verify concurrent VoiceBridgeSession calls across two tenants remain completely isolated."""

    async def _run() -> None:
        async def _handler(ws: Any) -> None:
            async for raw in ws:
                evt = json.loads(raw)
                if evt.get("type") == "input_audio_buffer.append":
                    await ws.send(
                        json.dumps(
                            {
                                "type": "response.output_audio.delta",
                                "response_id": "resp_iso",
                                "delta": evt["audio"],
                            }
                        )
                    )

        async with _mock_openai_ws_server(_handler) as (ws_url, conn_log):
            cfg = OpenAIRealtimeConfig(api_key="sk-test-iso", endpoint_url=ws_url)
            factory = create_openai_realtime_adapter_factory(cfg)

            tel_a = FakeTelephonyMediaAdapter(name="tel_a")
            tel_b = FakeTelephonyMediaAdapter(name="tel_b")
            ai_a = factory(call_id="call_a", tenant_id="tenant_alpha")
            ai_b = factory(call_id="call_b", tenant_id="tenant_beta")

            session_a = VoiceBridgeSession(
                call_id="call_a", tenant_id="tenant_alpha", telephony_port=tel_a, ai_port=ai_a
            )
            session_b = VoiceBridgeSession(
                call_id="call_b", tenant_id="tenant_beta", telephony_port=tel_b, ai_port=ai_b
            )
            await asyncio.gather(session_a.start(), session_b.start())

            tel_a.push_frame(
                AudioFrame(
                    payload=b"\x11\x00" * 320,
                    format=AudioFormat.PCM16LE,
                    sample_rate=16000,
                    channels=1,
                    sequence=0,
                    timestamp_ms=0.0,
                )
            )
            tel_b.push_frame(
                AudioFrame(
                    payload=b"\x22\x00" * 320,
                    format=AudioFormat.PCM16LE,
                    sample_rate=16000,
                    channels=1,
                    sequence=0,
                    timestamp_ms=0.0,
                )
            )
            deadline = time.monotonic() + 2.0
            while time.monotonic() < deadline:
                if tel_a.sent_frames and tel_b.sent_frames:
                    break
                await asyncio.sleep(0.01)
            await asyncio.gather(session_a.close(), session_b.close())

            assert len(conn_log) == 2
            assert any(f.payload == b"\x11\x00" * 320 for f in tel_a.sent_frames)
            assert all(f.payload != b"\x22\x00" * 320 for f in tel_a.sent_frames)
            assert any(f.payload == b"\x22\x00" * 320 for f in tel_b.sent_frames)
            assert all(f.payload != b"\x11\x00" * 320 for f in tel_b.sent_frames)

    asyncio.run(_run())


def test_16_no_session_creation_before_trusted_admission() -> None:
    """Verify unauthenticated ASGI WebSocket requests never invoke the OpenAI adapter factory."""
    factory_calls: list[tuple[str, str]] = []
    cfg = OpenAIRealtimeConfig(
        api_key="sk-test-auth-gate", endpoint_url="wss://mock.openai.local/v1/realtime"
    )

    def _recording_factory(
        *, call_id: str, tenant_id: str, **kwargs: Any
    ) -> OpenAIRealtimeMediaAdapter:
        factory_calls.append((call_id, tenant_id))
        return OpenAIRealtimeMediaAdapter(call_id=call_id, tenant_id=tenant_id, config=cfg)

    core_cfg = CoreConfig(instance_name="openai_auth_gate", ingress_mode="synthetic")
    app = create_gateway_app(
        core_cfg,
        synthetic_auth_bridge=SyntheticAuthBridge(
            SyntheticTokenVerifier(secret_key=b"test-secret-key-for-auth-gate-32b")
        ),
        enable_synthetic_media=True,
        ai_media_port_factory=_recording_factory,
    )

    with TestClient(app) as client:
        with pytest.raises(WebSocketDisconnect):
            with client.websocket_connect("/ws/synthetic-media/v1"):
                pass

    assert factory_calls == []


def test_17_eof_preserved_when_outbound_queue_full() -> None:
    """Verify clean EOF is not lost if _outbound_queue is full when provider closes."""

    async def _run() -> None:
        class _FullQueueEOFTransport:
            def __init__(self) -> None:
                self._step = 0

            async def send_json(self, payload: dict[str, Any]) -> None:
                return None

            async def receive_json(self) -> dict[str, Any] | None:
                self._step += 1
                if self._step == 1:
                    return {
                        "type": "response.output_audio.delta",
                        "response_id": "resp_full_eof",
                        "delta": base64.b64encode(b"\x01\x00" * 480).decode("ascii"),
                    }
                return None

            async def close(self) -> None:
                return None

        async def _connector(_cfg: OpenAIRealtimeConfig) -> OpenAIRealtimeTransport:
            return _FullQueueEOFTransport()

        cfg = OpenAIRealtimeConfig(
            api_key="sk-test-eof-full",
            endpoint_url="wss://mock.openai.local/v1/realtime",
            outbound_queue_capacity=1,
        )
        adapter = OpenAIRealtimeMediaAdapter(
            call_id="call_eof_full",
            tenant_id="tenant_a",
            config=cfg,
            transport_connector=_connector,
        )
        await adapter._ensure_connected()
        await asyncio.sleep(0.05)

        frame = await asyncio.wait_for(adapter.receive_frame(), timeout=1.0)
        assert frame is not None
        eof = await asyncio.wait_for(adapter.receive_frame(), timeout=1.0)
        assert eof is None
        await adapter.close()

    asyncio.run(_run())


def test_18_transport_close_awaited_without_self_cancellation_on_loop_error() -> None:
    """Verify failing background loop awaits transport.close() without cancelling itself."""

    async def _run() -> None:
        close_completed = asyncio.Event()

        class _AsyncCloseTransport:
            async def send_json(self, payload: dict[str, Any]) -> None:
                return None

            async def receive_json(self) -> dict[str, Any] | None:
                raise ConnectionResetError("Simulated drop")

            async def close(self) -> None:
                await asyncio.sleep(0.01)
                close_completed.set()

        async def _connector(_cfg: OpenAIRealtimeConfig) -> OpenAIRealtimeTransport:
            return _AsyncCloseTransport()

        cfg = OpenAIRealtimeConfig(
            api_key="sk-test-self-cancel",
            endpoint_url="wss://mock.openai.local/v1/realtime",
        )
        adapter = OpenAIRealtimeMediaAdapter(
            call_id="call_self_cancel",
            tenant_id="tenant_a",
            config=cfg,
            transport_connector=_connector,
        )
        with pytest.raises(OpenAIRealtimeError):
            await asyncio.wait_for(adapter.receive_frame(), timeout=1.0)
        await asyncio.wait_for(close_completed.wait(), timeout=1.0)
        await adapter.close()

    asyncio.run(_run())


def test_19_close_during_connect_disposes_new_transport() -> None:
    """Verify close() racing with _ensure_connected() disposes the new transport safely."""

    async def _run() -> None:
        connect_entered = asyncio.Event()
        release_connect = asyncio.Event()
        transport_closed = False

        class _SlowConnectTransport:
            async def send_json(self, payload: dict[str, Any]) -> None:
                return None

            async def receive_json(self) -> dict[str, Any] | None:
                return None

            async def close(self) -> None:
                nonlocal transport_closed
                transport_closed = True

        async def _connector(_cfg: OpenAIRealtimeConfig) -> OpenAIRealtimeTransport:
            connect_entered.set()
            await release_connect.wait()
            return _SlowConnectTransport()

        cfg = OpenAIRealtimeConfig(
            api_key="sk-test-race",
            endpoint_url="wss://mock.openai.local/v1/realtime",
        )
        adapter = OpenAIRealtimeMediaAdapter(
            call_id="call_race",
            tenant_id="tenant_a",
            config=cfg,
            transport_connector=_connector,
        )
        recv_task = asyncio.create_task(adapter.receive_frame())
        await connect_entered.wait()
        close_task = asyncio.create_task(adapter.close())
        await asyncio.sleep(0.01)
        release_connect.set()

        await close_task
        with pytest.raises(MediaPortClosedError):
            await recv_task
        assert transport_closed is True

    asyncio.run(_run())


def test_20_close_completes_cleanup_when_caller_is_cancelled() -> None:
    """Verify cancelling a close() caller does not abort or skip underlying transport cleanup."""

    async def _run() -> None:
        close_entered = asyncio.Event()
        release_close = asyncio.Event()
        transport_closed = False

        class _SlowCloseTransport:
            async def send_json(self, payload: dict[str, Any]) -> None:
                return None

            async def receive_json(self) -> dict[str, Any] | None:
                await asyncio.sleep(10.0)
                return None

            async def close(self) -> None:
                nonlocal transport_closed
                close_entered.set()
                await release_close.wait()
                transport_closed = True

        async def _connector(_cfg: OpenAIRealtimeConfig) -> OpenAIRealtimeTransport:
            return _SlowCloseTransport()

        cfg = OpenAIRealtimeConfig(
            api_key="sk-test-close-cancel",
            endpoint_url="wss://mock.openai.local/v1/realtime",
            close_timeout_seconds=1.0,
        )
        adapter = OpenAIRealtimeMediaAdapter(
            call_id="call_close_cancel",
            tenant_id="tenant_a",
            config=cfg,
            transport_connector=_connector,
        )
        await adapter.send_frame(
            AudioFrame(
                payload=b"\x01\x00" * 320,
                format=AudioFormat.PCM16LE,
                sample_rate=16000,
                channels=1,
                sequence=1,
                timestamp_ms=20,
                turn_id=1,
            )
        )

        first_close = asyncio.create_task(adapter.close())
        await close_entered.wait()
        first_close.cancel()
        with pytest.raises(asyncio.CancelledError):
            await first_close

        release_close.set()
        await asyncio.wait_for(adapter.close(), timeout=1.0)
        assert transport_closed is True

    asyncio.run(_run())


def test_21_delayed_response_created_after_cancel_turn_is_fenced() -> None:
    """Verify a delayed response.created from an interrupted generation cannot acquire the new turn ID."""

    async def _run() -> None:
        server_events: asyncio.Queue[dict[str, Any] | None] = asyncio.Queue()

        class _RaceTransport:
            async def send_json(self, payload: dict[str, Any]) -> None:
                if payload.get("type") == "response.cancel":
                    # Simulate response.created + delta from the interrupted generation
                    # arriving after cancel_turn() was called, followed by response.done(cancelled),
                    # and finally the new turn's response.created + delta.
                    await server_events.put(
                        {"type": "response.created", "response": {"id": "resp_old"}}
                    )
                    await server_events.put(
                        {
                            "type": "response.output_audio.delta",
                            "response_id": "resp_old",
                            "delta": base64.b64encode(b"\x09\x00" * 480).decode("ascii"),
                        }
                    )
                    await server_events.put(
                        {
                            "type": "response.done",
                            "response": {"id": "resp_old", "status": "cancelled"},
                        }
                    )
                    await server_events.put(
                        {"type": "response.created", "response": {"id": "resp_new"}}
                    )
                    await server_events.put(
                        {
                            "type": "response.output_audio.delta",
                            "response_id": "resp_new",
                            "delta": base64.b64encode(b"\x07\x00" * 480).decode("ascii"),
                        }
                    )
                    await server_events.put(None)

            async def receive_json(self) -> dict[str, Any] | None:
                return await server_events.get()

            async def close(self) -> None:
                await server_events.put(None)

        async def _connector(_cfg: OpenAIRealtimeConfig) -> OpenAIRealtimeTransport:
            return _RaceTransport()

        cfg = OpenAIRealtimeConfig(
            api_key="sk-test-race-fence",
            endpoint_url="wss://mock.openai.local/v1/realtime",
        )
        adapter = OpenAIRealtimeMediaAdapter(
            call_id="call_race_fence",
            tenant_id="tenant_a",
            config=cfg,
            transport_connector=_connector,
        )
        await adapter.send_frame(
            AudioFrame(
                payload=b"\x01\x00" * 320,
                format=AudioFormat.PCM16LE,
                sample_rate=16000,
                channels=1,
                sequence=1,
                timestamp_ms=20,
                turn_id=1,
            )
        )
        await adapter.cancel_turn(1)

        frame = await asyncio.wait_for(adapter.receive_frame(), timeout=2.0)
        assert frame is not None
        assert frame.turn_id == 2
        assert frame.payload == b"\x07\x00" * 320
        assert await asyncio.wait_for(adapter.receive_frame(), timeout=2.0) is None
        await adapter.close()

    asyncio.run(_run())


def test_22_eof_not_published_before_tail_frame_enqueued() -> None:
    """Verify clean EOF is not visible to receive_frame() while a partial tail frame is still pending."""

    async def _run() -> None:
        server_events: asyncio.Queue[dict[str, Any] | None] = asyncio.Queue()
        # First delta produces 1 full 640-byte frame + 320-byte partial tail in packer.
        await server_events.put({"type": "response.created", "response": {"id": "resp_tail"}})
        await server_events.put(
            {
                "type": "response.output_audio.delta",
                "response_id": "resp_tail",
                "delta": base64.b64encode(b"\x03\x00" * 720).decode("ascii"),
            }
        )
        await server_events.put(None)

        class _TailTransport:
            async def send_json(self, payload: dict[str, Any]) -> None:
                return None

            async def receive_json(self) -> dict[str, Any] | None:
                return await server_events.get()

            async def close(self) -> None:
                return None

        async def _connector(_cfg: OpenAIRealtimeConfig) -> OpenAIRealtimeTransport:
            return _TailTransport()

        cfg = OpenAIRealtimeConfig(
            api_key="sk-test-tail-eof",
            endpoint_url="wss://mock.openai.local/v1/realtime",
            outbound_queue_capacity=1,
            backpressure_timeout_seconds=1.0,
        )
        adapter = OpenAIRealtimeMediaAdapter(
            call_id="call_tail_eof",
            tenant_id="tenant_a",
            config=cfg,
            queue_capacity=1,
            backpressure_timeout=1.0,
            transport_connector=_connector,
        )

        f1 = await asyncio.wait_for(adapter.receive_frame(), timeout=2.0)
        assert f1 is not None
        f2 = await asyncio.wait_for(adapter.receive_frame(), timeout=2.0)
        assert f2 is not None
        assert len(f2.payload) == 640
        assert await asyncio.wait_for(adapter.receive_frame(), timeout=2.0) is None
        await adapter.close()

    asyncio.run(_run())


def test_23_cancel_after_output_audio_done_allows_response_cancel_not_active() -> None:
    """Verify normal response.done(completed) does not consume the response_cancel_not_active allowance."""

    async def _run() -> None:
        server_events: asyncio.Queue[dict[str, Any] | None] = asyncio.Queue()

        class _AckTransport:
            async def send_json(self, payload: dict[str, Any]) -> None:
                if payload.get("type") == "response.cancel":
                    await server_events.put(
                        {
                            "type": "response.done",
                            "response": {"id": "resp_done", "status": "completed"},
                        }
                    )
                    await server_events.put(
                        {
                            "type": "error",
                            "error": {
                                "code": "response_cancel_not_active",
                                "message": "no active response",
                            },
                        }
                    )
                    await server_events.put(
                        {"type": "response.created", "response": {"id": "resp_next"}}
                    )
                    await server_events.put(
                        {
                            "type": "response.output_audio.delta",
                            "response_id": "resp_next",
                            "delta": base64.b64encode(b"\x05\x00" * 480).decode("ascii"),
                        }
                    )
                    await server_events.put(None)

            async def receive_json(self) -> dict[str, Any] | None:
                return await server_events.get()

            async def close(self) -> None:
                await server_events.put(None)

        async def _connector(_cfg: OpenAIRealtimeConfig) -> OpenAIRealtimeTransport:
            return _AckTransport()

        await server_events.put({"type": "response.created", "response": {"id": "resp_done"}})
        await server_events.put(
            {
                "type": "response.output_audio.delta",
                "response_id": "resp_done",
                "delta": base64.b64encode(b"\x01\x00" * 480).decode("ascii"),
            }
        )
        await server_events.put({"type": "response.output_audio.done", "response_id": "resp_done"})

        cfg = OpenAIRealtimeConfig(
            api_key="sk-test-cancel-ack",
            endpoint_url="wss://mock.openai.local/v1/realtime",
        )
        adapter = OpenAIRealtimeMediaAdapter(
            call_id="call_cancel_ack",
            tenant_id="tenant_a",
            config=cfg,
            transport_connector=_connector,
        )
        first = await asyncio.wait_for(adapter.receive_frame(), timeout=2.0)
        assert first is not None and first.turn_id == 1

        # Simulate cancel_turn() after response.output_audio.done but before response.done(completed).
        await asyncio.sleep(0.02)
        await adapter.cancel_turn(1)

        nxt = await asyncio.wait_for(adapter.receive_frame(), timeout=2.0)
        assert nxt is not None
        assert nxt.turn_id == 2
        assert nxt.payload == b"\x05\x00" * 320
        assert await asyncio.wait_for(adapter.receive_frame(), timeout=2.0) is None
        await adapter.close()

    asyncio.run(_run())


def test_24_in_progress_failure_transport_close_survives_adapter_close() -> None:
    """Verify explicit adapter.close() does not cancel an in-progress failure transport.close()."""

    async def _run() -> None:
        close_entered = asyncio.Event()
        release_close = asyncio.Event()
        close_completed = False
        close_calls = 0

        class _FailureCloseTransport:
            async def send_json(self, payload: dict[str, Any]) -> None:
                return None

            async def receive_json(self) -> dict[str, Any] | None:
                raise RuntimeError("boom")

            async def close(self) -> None:
                nonlocal close_completed, close_calls
                close_calls += 1
                if close_calls > 1:
                    return
                close_entered.set()
                await release_close.wait()
                close_completed = True

        async def _connector(_cfg: OpenAIRealtimeConfig) -> OpenAIRealtimeTransport:
            return _FailureCloseTransport()

        cfg = OpenAIRealtimeConfig(
            api_key="sk-test-close-race",
            endpoint_url="wss://mock.openai.local/v1/realtime",
            close_timeout_seconds=1.0,
        )
        adapter = OpenAIRealtimeMediaAdapter(
            call_id="call_fail_close",
            tenant_id="tenant_a",
            config=cfg,
            transport_connector=_connector,
        )
        await adapter._ensure_connected()
        await asyncio.wait_for(close_entered.wait(), timeout=1.0)

        adapter_close = asyncio.create_task(adapter.close())
        await asyncio.sleep(0.02)
        release_close.set()
        await asyncio.wait_for(adapter_close, timeout=1.0)
        assert close_completed is True

    asyncio.run(_run())


def test_25_inbound_resampler_preserves_fractional_phase_across_odd_chunks() -> None:
    """Verify 16k->24k resampler preserves exact sample count regardless of chunking."""
    src_16k = _make_sine_pcm16le(16_000, 440.0, 320)
    r_bulk = PCM16Resampler16kTo24k()
    bulk_out = r_bulk.resample(src_16k)
    assert len(bulk_out) == 960

    r_chunked = PCM16Resampler16kTo24k()
    chunked_out = bytearray()
    for i in range(0, len(src_16k), 2):
        chunked_out.extend(r_chunked.resample(src_16k[i : i + 2]))
    assert len(chunked_out) == 960


def test_26_formatted_traceback_redacts_api_key() -> None:
    """Verify formatted exception tracebacks do not leak raw API keys via chained causes."""

    async def _run() -> None:
        secret_key = "sk-super-secret-traceback-leak-test"

        async def _leaking_connector(_cfg: OpenAIRealtimeConfig) -> OpenAIRealtimeTransport:
            raise RuntimeError(f"upstream rejected Bearer {secret_key}")

        cfg = OpenAIRealtimeConfig(
            api_key=secret_key,
            endpoint_url="wss://mock.openai.local/v1/realtime",
        )
        adapter = OpenAIRealtimeMediaAdapter(
            call_id="call_tb",
            tenant_id="tenant_a",
            config=cfg,
            transport_connector=_leaking_connector,
        )
        with pytest.raises(OpenAIRealtimeError) as exc_info:
            await adapter.receive_frame()

        formatted = "".join(traceback.format_exception(exc_info.value))
        assert secret_key not in formatted
        assert "[REDACTED]" in formatted
        await adapter.close()

    asyncio.run(_run())


def test_27_pending_inbound_enqueue_fails_closed_and_leaves_queue_empty_on_close() -> None:
    """Verify a blocked send_frame() fails closed on close() and cannot repopulate the queue."""

    async def _run() -> None:
        send_blocked = asyncio.Event()
        release_send = asyncio.Event()

        class _BlockedSendTransport:
            async def send_json(self, payload: dict[str, Any]) -> None:
                if payload.get("type") == "input_audio_buffer.append":
                    send_blocked.set()
                    await release_send.wait()

            async def receive_json(self) -> dict[str, Any] | None:
                await asyncio.sleep(10.0)
                return None

            async def close(self) -> None:
                release_send.set()

        async def _connector(_cfg: OpenAIRealtimeConfig) -> OpenAIRealtimeTransport:
            return _BlockedSendTransport()

        cfg = OpenAIRealtimeConfig(
            api_key="sk-test-send-close",
            endpoint_url="wss://mock.openai.local/v1/realtime",
            inbound_queue_capacity=1,
            backpressure_timeout_seconds=1.0,
        )
        adapter = OpenAIRealtimeMediaAdapter(
            call_id="call_send_close",
            tenant_id="tenant_a",
            config=cfg,
            queue_capacity=1,
            backpressure_timeout=1.0,
            transport_connector=_connector,
        )

        def _mk(seq: int) -> AudioFrame:
            return AudioFrame(
                payload=b"\x01\x00" * 320,
                format=AudioFormat.PCM16LE,
                sample_rate=16000,
                channels=1,
                sequence=seq,
                timestamp_ms=seq * 20.0,
                turn_id=1,
            )

        # Frame 0 enters sender_loop and blocks on send_json.
        await adapter.send_frame(_mk(0))
        await asyncio.wait_for(send_blocked.wait(), timeout=1.0)
        # Frame 1 fills capacity-1 inbound queue.
        await adapter.send_frame(_mk(1))

        # Frame 2 blocks in send_frame() awaiting queue space.
        pending_send = asyncio.create_task(adapter.send_frame(_mk(2)))
        await asyncio.sleep(0.02)

        await adapter.close()
        with pytest.raises(MediaPortClosedError):
            await asyncio.wait_for(pending_send, timeout=1.0)

        assert adapter._inbound_queue.empty()

    asyncio.run(_run())


def test_28_response_turn_map_is_bounded_and_cleared_on_close() -> None:
    """Verify _response_turn_map stays bounded over long calls and is cleared on close()."""

    async def _run() -> None:
        cfg = OpenAIRealtimeConfig(
            api_key="sk-test-bounded-history",
            endpoint_url="wss://mock.openai.local/v1/realtime",
        )
        adapter = OpenAIRealtimeMediaAdapter(
            call_id="call_bounded_hist",
            tenant_id="tenant_a",
            config=cfg,
        )
        for i in range(500):
            await adapter._handle_server_event(
                {"type": "response.created", "response": {"id": f"resp_{i}"}}
            )
            await adapter._handle_server_event(
                {"type": "response.done", "response": {"id": f"resp_{i}", "status": "completed"}}
            )

        assert len(adapter._response_turn_map) <= 64
        await adapter.close()
        assert len(adapter._response_turn_map) == 0

    asyncio.run(_run())


def test_29_failed_response_done_surfaces_sanitized_error_and_closes() -> None:
    """Verify response.done with status='failed' raises sanitized OpenAIRealtimeError and closes."""

    async def _run() -> None:
        secret_key = "sk-test-failed-response-key"
        server_events: asyncio.Queue[dict[str, Any] | None] = asyncio.Queue()
        await server_events.put({"type": "response.created", "response": {"id": "resp_fail"}})
        await server_events.put(
            {
                "type": "response.done",
                "response": {
                    "id": "resp_fail",
                    "status": "failed",
                    "status_details": {
                        "type": "failed",
                        "error": {
                            "type": "server_error",
                            "code": "rate_limit_exceeded",
                            "message": f"quota exhausted for Bearer {secret_key}",
                        },
                    },
                },
            }
        )

        class _FailedResponseTransport:
            async def send_json(self, payload: dict[str, Any]) -> None:
                return None

            async def receive_json(self) -> dict[str, Any] | None:
                return await server_events.get()

            async def close(self) -> None:
                await server_events.put(None)

        async def _connector(_cfg: OpenAIRealtimeConfig) -> OpenAIRealtimeTransport:
            return _FailedResponseTransport()

        cfg = OpenAIRealtimeConfig(
            api_key=secret_key,
            endpoint_url="wss://mock.openai.local/v1/realtime",
        )
        adapter = OpenAIRealtimeMediaAdapter(
            call_id="call_resp_fail",
            tenant_id="tenant_a",
            config=cfg,
            transport_connector=_connector,
        )
        with pytest.raises(OpenAIRealtimeError) as exc_info:
            await asyncio.wait_for(adapter.receive_frame(), timeout=2.0)

        msg = str(exc_info.value)
        assert "rate_limit_exceeded" in msg
        assert secret_key not in msg
        assert "[REDACTED]" in msg
        assert adapter.is_closed is True

    asyncio.run(_run())


def test_30_interruption_blocks_future_turn_audio_until_cancel_turn_commits() -> None:
    """Verify no future-turn audio reaches VoiceBridgeSession while response.cancel is in flight."""

    async def _run() -> None:
        server_events: asyncio.Queue[dict[str, Any] | None] = asyncio.Queue()
        cancel_entered = asyncio.Event()
        release_cancel = asyncio.Event()

        class _BlockingCancelTransport:
            async def send_json(self, payload: dict[str, Any]) -> None:
                if payload.get("type") == "response.cancel":
                    cancel_entered.set()
                    await release_cancel.wait()

            async def receive_json(self) -> dict[str, Any] | None:
                return await server_events.get()

            async def close(self) -> None:
                release_cancel.set()
                await server_events.put(None)

        async def _connector(_cfg: OpenAIRealtimeConfig) -> OpenAIRealtimeTransport:
            return _BlockingCancelTransport()

        cfg = OpenAIRealtimeConfig(
            api_key="sk-test-int-race",
            endpoint_url="wss://mock.openai.local/v1/realtime",
            cancel_timeout_seconds=2.0,
        )
        adapter = OpenAIRealtimeMediaAdapter(
            call_id="call_int_race",
            tenant_id="tenant_a",
            config=cfg,
            transport_connector=_connector,
        )
        tel_port = FakeTelephonyMediaAdapter(name="tel-int-race")
        session = VoiceBridgeSession(
            call_id="call_int_race",
            tenant_id="tenant_a",
            telephony_port=tel_port,
            ai_port=adapter,
            queue_capacity=8,
        )
        await session.start()

        async def _wait_sent_count(count: int) -> AudioFrame:
            deadline = asyncio.get_running_loop().time() + 2.0
            while len(tel_port.sent_frames) < count:
                if asyncio.get_running_loop().time() > deadline:
                    raise TimeoutError(f"Timed out waiting for {count} sent frames")
                await asyncio.sleep(0.01)
            return tel_port.sent_frames[count - 1]

        # Start Turn 1 so adapter is connected and delivering Turn 1 audio.
        await server_events.put({"type": "response.created", "response": {"id": "resp_t1"}})
        await server_events.put(
            {
                "type": "response.output_audio.delta",
                "response_id": "resp_t1",
                "delta": base64.b64encode(b"\x01\x00" * 480).decode("ascii"),
            }
        )
        t1_frame = await _wait_sent_count(1)
        assert t1_frame.turn_id == 1

        # Begin interruption; response.cancel blocks inside transport.send_json.
        interrupt_task = asyncio.create_task(session.interrupt(turn_id=1))
        await asyncio.wait_for(cancel_entered.wait(), timeout=2.0)

        # Inject subsequent response.created + delta while response.cancel is still blocked.
        await server_events.put({"type": "response.created", "response": {"id": "resp_t2"}})
        await server_events.put(
            {
                "type": "response.output_audio.delta",
                "response_id": "resp_t2",
                "delta": base64.b64encode(b"\x07\x00" * 480).decode("ascii"),
            }
        )
        await asyncio.sleep(0.05)

        # Future-turn audio must NOT have been forwarded to telephony before interrupt() commits.
        assert len(tel_port.sent_frames) == 1

        # Unblock response.cancel and verify VoiceBridgeSession commits cleanly and forwards Turn 2.
        release_cancel.set()
        outcome = await asyncio.wait_for(interrupt_task, timeout=2.0)
        assert outcome.new_turn_id == 2

        t2_frame = await _wait_sent_count(2)
        assert t2_frame.turn_id == 2
        assert t2_frame.payload == b"\x07\x00" * 320
        assert session.error is None
        await session.close()

    asyncio.run(_run())


def test_31_plaintext_remote_ws_rejected_while_loopback_ws_and_wss_allowed() -> None:
    """Verify plaintext remote ws:// URLs are rejected while loopback ws:// and wss:// work."""
    from kaiwen_gateway.adapters.openai_realtime.transport import (
        connect_openai_realtime_websocket,
    )

    with pytest.raises(ValueError, match="wss://"):
        OpenAIRealtimeConfig(api_key="sk-test", endpoint_url="ws://api.openai.com/v1/realtime")

    with pytest.raises(ValueError, match="wss://"):
        OpenAIRealtimeConfig(api_key="sk-test", endpoint_url="ws://10.0.0.5:8080/v1/realtime")

    # Loopback ws:// and remote wss:// remain permitted.
    cfg_local = OpenAIRealtimeConfig(
        api_key="sk-test", endpoint_url="ws://127.0.0.1:8000/v1/realtime"
    )
    assert cfg_local.build_websocket_url().startswith("ws://127.0.0.1:8000")
    cfg_localhost = OpenAIRealtimeConfig(
        api_key="sk-test", endpoint_url="ws://localhost:8000/v1/realtime"
    )
    assert cfg_localhost.build_websocket_url().startswith("ws://localhost:8000")
    cfg_tls = OpenAIRealtimeConfig(
        api_key="sk-test", endpoint_url="wss://api.openai.com/v1/realtime"
    )
    assert cfg_tls.build_websocket_url().startswith("wss://api.openai.com")

    # Defense-in-depth at transport connect time even if config validation were bypassed.
    bypassed = object.__new__(OpenAIRealtimeConfig)
    object.__setattr__(bypassed, "api_key", "sk-test-secret")
    object.__setattr__(bypassed, "model", "gpt-realtime")
    object.__setattr__(bypassed, "voice", "alloy")
    object.__setattr__(bypassed, "instructions", None)
    object.__setattr__(bypassed, "endpoint_url", "ws://remote.example.com/v1/realtime")
    object.__setattr__(bypassed, "connect_timeout_seconds", 1.0)
    object.__setattr__(bypassed, "close_timeout_seconds", 1.0)

    async def _check_transport() -> None:
        with pytest.raises(OpenAIRealtimeError, match="wss://"):
            await connect_openai_realtime_websocket(bypassed)

    asyncio.run(_check_transport())


def test_32_incomplete_response_done_content_filter_surfaces_sanitized_error() -> None:
    """Verify response.done with status='incomplete' raises sanitized OpenAIRealtimeError."""

    async def _run() -> None:
        secret_key = "sk-test-incomplete-key"
        server_events: asyncio.Queue[dict[str, Any] | None] = asyncio.Queue()
        await server_events.put({"type": "response.created", "response": {"id": "resp_inc"}})
        await server_events.put(
            {
                "type": "response.done",
                "response": {
                    "id": "resp_inc",
                    "status": "incomplete",
                    "status_details": {
                        "type": "incomplete",
                        "reason": "content_filter",
                    },
                },
            }
        )

        class _IncompleteTransport:
            async def send_json(self, payload: dict[str, Any]) -> None:
                return None

            async def receive_json(self) -> dict[str, Any] | None:
                return await server_events.get()

            async def close(self) -> None:
                await server_events.put(None)

        async def _connector(_cfg: OpenAIRealtimeConfig) -> OpenAIRealtimeTransport:
            return _IncompleteTransport()

        cfg = OpenAIRealtimeConfig(
            api_key=secret_key,
            endpoint_url="wss://mock.openai.local/v1/realtime",
        )
        adapter = OpenAIRealtimeMediaAdapter(
            call_id="call_resp_inc",
            tenant_id="tenant_a",
            config=cfg,
            transport_connector=_connector,
        )
        with pytest.raises(OpenAIRealtimeError) as exc_info:
            await asyncio.wait_for(adapter.receive_frame(), timeout=2.0)

        msg = str(exc_info.value)
        assert "incomplete" in msg
        assert "content_filter" in msg
        assert secret_key not in msg
        assert adapter.is_closed is True

    asyncio.run(_run())
