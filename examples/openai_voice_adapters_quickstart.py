"""Quickstart example: OpenAI Realtime and OpenAI GPT-Live Gateway voice adapters.

Demonstrates:
1. Configuring `OpenAIRealtimeConfig` (`wss://api.openai.com/v1/realtime`, `gpt-realtime`)
   and `OpenAIGPTLiveConfig` (`wss://api.openai.com/v1/live/sessions`, `gpt-live-1`).
2. Wiring `create_openai_realtime_adapter_factory` and `create_openai_gpt_live_adapter_factory`
   into `create_gateway_app(enable_synthetic_media=True, ai_media_port_factory=...)`.
3. Streaming 16 kHz PCM16LE audio frames through `VoiceBridgeSession` and exercising
   turn interruption (`can_cancel_future_generation=True` for Realtime vs.
   `can_cancel_future_generation=False, can_discard_local_output=True` for GPT-Live).

By default, this script uses in-memory mock transports so it runs offline without
API keys or paid calls. To connect to the live OpenAI endpoints, set `OPENAI_API_KEY`
in your server environment (or `.env`) and pass `OpenAIRealtimeConfig.from_env()` /
`OpenAIGPTLiveConfig.from_env()` without `transport_connector`.

Requires:
    pip install "kaiwen-gateway[server,openai]"

Usage:
    python examples/openai_voice_adapters_quickstart.py
"""

from __future__ import annotations

import asyncio
import base64
from typing import Any

from kaiwen_gateway.adapters.openai_gpt_live import (
    OpenAIGPTLiveConfig,
    OpenAIGPTLiveMediaAdapter,
    OpenAIGPTLiveTransport,
)
from kaiwen_gateway.adapters.openai_realtime import (
    OpenAIRealtimeConfig,
    OpenAIRealtimeMediaAdapter,
    OpenAIRealtimeTransport,
)
from kaiwen_gateway.modules.voice import (
    AudioFormat,
    AudioFrame,
    InterruptibleFakeTelephonyAdapter,
    VoiceBridgeSession,
)


class _MockRealtimeTransport:
    def __init__(self) -> None:
        self._rx: asyncio.Queue[dict[str, Any] | None] = asyncio.Queue()
        self._rx.put_nowait({"type": "session.created", "session": {"id": "sess_rt_demo"}})

    async def send_json(self, payload: dict[str, Any]) -> None:
        msg_type = payload.get("type")
        if msg_type == "session.update":
            await self._rx.put({"type": "session.updated", "session": payload.get("session", {})})
        elif msg_type == "input_audio_buffer.append":
            await self._rx.put({"type": "response.created", "response": {"id": "resp_rt_1"}})
            pcm24k = b"\x10\x00" * 960
            await self._rx.put(
                {
                    "type": "response.output_audio.delta",
                    "response_id": "resp_rt_1",
                    "delta": base64.b64encode(pcm24k).decode("ascii"),
                }
            )
        elif msg_type == "response.cancel":
            await self._rx.put(
                {
                    "type": "response.done",
                    "response": {"id": "resp_rt_1", "status": "cancelled"},
                }
            )

    async def receive_json(self) -> dict[str, Any] | None:
        return await self._rx.get()

    async def close(self) -> None:
        await self._rx.put(None)


class _MockGPTLiveTransport:
    def __init__(self) -> None:
        self._rx: asyncio.Queue[dict[str, Any] | None] = asyncio.Queue()

    async def send_json(self, payload: dict[str, Any]) -> None:
        msg_type = payload.get("type")
        if msg_type == "session.start":
            await self._rx.put(
                {
                    "type": "session.started",
                    "session": {"id": "live_demo_001", "model": "gpt-live-1"},
                }
            )
        elif msg_type == "session.input_audio.append":
            pcm24k = b"\x20\x00" * 960
            await self._rx.put(
                {
                    "type": "session.output_audio.delta",
                    "delta": base64.b64encode(pcm24k).decode("ascii"),
                }
            )

    async def receive_json(self) -> dict[str, Any] | None:
        return await self._rx.get()

    async def close(self) -> None:
        await self._rx.put(None)


async def _run_realtime_demo() -> None:
    async def _connector(_cfg: OpenAIRealtimeConfig) -> OpenAIRealtimeTransport:
        return _MockRealtimeTransport()

    # For live OpenAI Realtime, use:
    # config = OpenAIRealtimeConfig.from_env(model="gpt-realtime", voice="alloy")
    config = OpenAIRealtimeConfig(api_key="<OPENAI_API_KEY_PLACEHOLDER>")
    adapter = OpenAIRealtimeMediaAdapter(
        call_id="demo-rt-call",
        tenant_id="demo-tenant",
        config=config,
        transport_connector=_connector,
    )
    tel = InterruptibleFakeTelephonyAdapter()
    async with VoiceBridgeSession(
        call_id="demo-rt-call",
        tenant_id="demo-tenant",
        telephony_port=tel,
        ai_port=adapter,
    ) as bridge:
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
        for _ in range(50):
            if len(tel.sent_frames) >= 2:
                break
            await asyncio.sleep(0.01)
        outcome = await bridge.interrupt(turn_id=1)
        print(
            f"[OpenAI Realtime] frames={len(tel.sent_frames)} "
            f"future_cancelled={outcome.future_generation_cancelled} "
            f"new_turn_id={outcome.new_turn_id}"
        )


async def _run_gpt_live_demo() -> None:
    async def _connector(_cfg: OpenAIGPTLiveConfig) -> OpenAIGPTLiveTransport:
        return _MockGPTLiveTransport()

    # For live OpenAI GPT-Live, use:
    # config = OpenAIGPTLiveConfig.from_env(model="gpt-live-1", voice="marin")
    config = OpenAIGPTLiveConfig(api_key="<OPENAI_API_KEY_PLACEHOLDER>")
    adapter = OpenAIGPTLiveMediaAdapter(
        call_id="demo-gpt-live-call",
        tenant_id="demo-tenant",
        config=config,
        transport_connector=_connector,
    )
    tel = InterruptibleFakeTelephonyAdapter()
    async with VoiceBridgeSession(
        call_id="demo-gpt-live-call",
        tenant_id="demo-tenant",
        telephony_port=tel,
        ai_port=adapter,
    ) as bridge:
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
        for _ in range(50):
            if len(tel.sent_frames) >= 2:
                break
            await asyncio.sleep(0.01)
        outcome = await bridge.interrupt(
            turn_id=1,
            require_future_generation_cancellation=False,
        )
        print(
            f"[OpenAI GPT-Live] frames={len(tel.sent_frames)} "
            f"future_cancelled={outcome.future_generation_cancelled} "
            f"new_turn_id={outcome.new_turn_id}"
        )


async def main() -> None:
    await _run_realtime_demo()
    await _run_gpt_live_demo()


if __name__ == "__main__":
    asyncio.run(main())
