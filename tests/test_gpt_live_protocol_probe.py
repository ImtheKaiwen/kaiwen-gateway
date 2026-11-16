"""Offline deterministic tests for tools/gpt_live_protocol_probe.py."""

from __future__ import annotations

import asyncio
import base64
import json
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Any

import pytest
import websockets
from tools.gpt_live_protocol_probe import (
    GATEWAY_INTEGRATION_STATUS,
    VERDICT_BLOCKED,
    VERDICT_SMOKE_FAIL,
    VERDICT_SMOKE_PASS,
    GPTLiveProbeConfig,
    run_gpt_live_probe,
)


@asynccontextmanager
async def _mock_gpt_live_server(handler: Any) -> AsyncIterator[str]:
    server = await websockets.serve(handler, "127.0.0.1", 0)
    port = next(iter(server.sockets)).getsockname()[1]
    try:
        yield f"ws://127.0.0.1:{port}/v1/live/sessions"
    finally:
        server.close()
        await server.wait_closed()


def test_gpt_live_probe_config_rejects_remote_plaintext_ws_and_hides_key() -> None:
    """GPTLiveProbeConfig rejects remote ws:// and redacts api_key from repr."""
    secret = "sk-live-probe-secret-999"
    with pytest.raises(ValueError, match="wss://"):
        GPTLiveProbeConfig(api_key=secret, endpoint_url="ws://api.openai.com/v1/live/sessions")

    cfg = GPTLiveProbeConfig(api_key=secret)
    assert secret not in repr(cfg)
    evt = cfg.build_session_start_event()
    assert evt["type"] == "session.start"
    assert evt["session"]["model"] == "gpt-live-1"
    assert evt["session"]["delegation"] == {"type": "client"}


def test_gpt_live_probe_smoke_pass() -> None:
    """Verify full session.start -> session.started -> audio append -> audio delta -> close flow."""

    async def _run() -> None:
        received_client_events: list[dict[str, Any]] = []

        async def _handler(ws: Any) -> None:
            async for raw in ws:
                evt = json.loads(raw)
                received_client_events.append(evt)
                if evt.get("type") == "session.start":
                    await ws.send(
                        json.dumps(
                            {
                                "type": "session.started",
                                "event_id": "evt_started_1",
                                "session": {
                                    "id": "live_mock_123",
                                    "model": "gpt-live-1",
                                    "status": "active",
                                },
                            }
                        )
                    )
                elif evt.get("type") == "session.input_audio.append":
                    pcm_out = b"\x10\x08" * 480
                    await ws.send(
                        json.dumps(
                            {
                                "type": "session.output_audio.delta",
                                "delta": base64.b64encode(pcm_out).decode("ascii"),
                                "start_ms": 0,
                                "end_ms": 20,
                            }
                        )
                    )

        async with _mock_gpt_live_server(_handler) as ws_url:
            cfg = GPTLiveProbeConfig(
                api_key="sk-mock-live-pass",
                endpoint_url=ws_url,
                audio_collect_timeout_seconds=1.0,
            )
            res = await run_gpt_live_probe(
                cfg,
                b"\x05\x00" * 960,
                chunk_pacing_seconds=0.0,
            )
            assert res.verdict == VERDICT_SMOKE_PASS
            assert res.gateway_integration_status == GATEWAY_INTEGRATION_STATUS
            assert res.session_started is True
            assert res.session_id == "live_mock_123"
            assert res.input_chunks_sent == 2
            assert res.output_audio_deltas_received == 2
            assert res.output_audio_bytes == 1920
            assert res.output_audio_rms > 0
            assert res.error_message is None

    asyncio.run(_run())


def test_gpt_live_probe_blocked_on_quota_or_permission_error() -> None:
    """Verify provider quota/permission errors classify as GPT_LIVE_BLOCKED and scrub api_key."""

    async def _run() -> None:
        secret = "sk-live-secret-to-scrub"

        async def _handler(ws: Any) -> None:
            async for _raw in ws:
                await ws.send(
                    json.dumps(
                        {
                            "type": "error",
                            "error": {
                                "code": "insufficient_quota",
                                "message": f"Quota exceeded for key {secret}",
                            },
                        }
                    )
                )
                return

        async with _mock_gpt_live_server(_handler) as ws_url:
            cfg = GPTLiveProbeConfig(api_key=secret, endpoint_url=ws_url)
            res = await run_gpt_live_probe(cfg, b"\x01\x00" * 480, chunk_pacing_seconds=0.0)
            assert res.verdict == VERDICT_BLOCKED
            assert res.gateway_integration_status == GATEWAY_INTEGRATION_STATUS
            assert res.error_message is not None
            assert secret not in res.error_message
            assert "[REDACTED]" in res.error_message

    asyncio.run(_run())


def test_gpt_live_probe_smoke_fail_when_no_audio_returned() -> None:
    """Verify session that starts but produces no audio delta classifies as GPT_LIVE_PROVIDER_SMOKE_FAIL."""

    async def _run() -> None:
        async def _handler(ws: Any) -> None:
            async for raw in ws:
                evt = json.loads(raw)
                if evt.get("type") == "session.start":
                    await ws.send(
                        json.dumps(
                            {
                                "type": "session.started",
                                "session": {"id": "live_no_audio", "model": "gpt-live-1"},
                            }
                        )
                    )

        async with _mock_gpt_live_server(_handler) as ws_url:
            cfg = GPTLiveProbeConfig(
                api_key="sk-mock-no-audio",
                endpoint_url=ws_url,
                audio_collect_timeout_seconds=0.2,
            )
            res = await run_gpt_live_probe(cfg, b"\x01\x00" * 480, chunk_pacing_seconds=0.0)
            assert res.verdict == VERDICT_SMOKE_FAIL
            assert res.session_started is True
            assert res.output_audio_deltas_received == 0

    asyncio.run(_run())
