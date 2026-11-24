# OpenAI Realtime Adapter Guide (M4-R2)

> **Status:** `LIVE_VERIFIED` (`gpt-realtime` via `wss://api.openai.com/v1/realtime`) & `MOCK_VERIFIED` (32 offline adapter tests + 8 conformance dimensions, 0 skips).
> **Companion Adapter (`GPT-Live`):** OpenAI's `wss://api.openai.com/v1/live/sessions` (`gpt-live-1`) protocol is supported via the dedicated [`kaiwen_gateway.adapters.openai_gpt_live`](OPENAI_GPT_LIVE_ADAPTER.md) adapter (`LIVE_VERIFIED` & `MOCK_VERIFIED`).

---

## 1. Installation & Optional Dependencies

Core-only installations (`kaiwen-gateway` with `dependencies = []`) remain free of external runtime dependencies.

To enable the OpenAI Realtime WebSocket transport (`websockets`):

```bash
pip install "kaiwen-gateway[openai]"
# or with the ASGI server stack:
pip install "kaiwen-gateway[server]"
```

---

## 2. Environment & Server Configuration

API credentials are strictly **server-owned** and must never be supplied by client WebSockets:

```python
from kaiwen_gateway.adapters.openai_realtime import (
    OpenAIRealtimeConfig,
    create_openai_realtime_adapter_factory,
)
from kaiwen_gateway.core import CoreConfig
from kaiwen_gateway.server import create_gateway_app

# Reads OPENAI_API_KEY from server environment (fails closed if missing)
openai_config = OpenAIRealtimeConfig.from_env(
    model="gpt-realtime",
    voice="alloy",
    instructions="You are a concise voice assistant.",
)

app = create_gateway_app(
    CoreConfig(instance_name="gateway-openai"),
    enable_synthetic_media=True,
    ai_media_port_factory=create_openai_realtime_adapter_factory(openai_config),
)
```

---

## 3. Official Provider Contract & Audio Format Conversion

- **Endpoint:** `wss://api.openai.com/v1/realtime?model=gpt-realtime`
- **Authentication:** `Authorization: Bearer <OPENAI_API_KEY>` (header; scrubbed from all exception messages and `repr(config)`)
- **GA Session Configuration (`session.update`):**
  - `session.type = "realtime"`
  - `session.output_modalities = ["audio"]`
  - `session.audio.input.format = {"type": "audio/pcm", "rate": 24000}`
  - `session.audio.output.format = {"type": "audio/pcm", "rate": 24000}`
  - `session.audio.input.turn_detection` (`server_vad` by default, configurable)
- **Audio Resampling Pipeline (`kaiwen_gateway.adapters.openai_realtime.audio`):**
  - **Inbound (Gateway -> OpenAI):** Canonical `16,000 Hz` mono `pcm16le` (`640` bytes / 20 ms) is resampled to `24,000 Hz` mono `pcm16le` (`960` bytes / 20 ms) via `PCM16Resampler16kTo24k`, base64-encoded, and sent as `input_audio_buffer.append`.
  - **Outbound (OpenAI -> Gateway):** Base64 `response.output_audio.delta` payloads (`24,000 Hz` mono `pcm16le`) are decoded, resampled to `16,000 Hz` via stateful `PCM16Resampler24kTo16k`, and repacked into canonical `640`-byte `AudioFrame`s by `OutboundFramePacker` with monotonic `sequence` and `timestamp_ms`.

---

## 4. Connection Lifecycle, Backpressure & Failure Semantics

- **Synchronous Factory / Lazy Async Connect:** `OpenAIRealtimeMediaAdapter.__init__` performs zero network I/O. The WebSocket handshake and `session.update` run lazily on the first `send_frame()` or `receive_frame()` under an `asyncio.Lock`, bounded by `connect_timeout_seconds`.
- **Single Session Guarantee:** Exactly one upstream WebSocket session is created per admitted call. No automatic reconnect loop is attempted (since reconnecting does not restore provider conversation state); upstream disconnects fail closed.
- **Bounded Buffering & Backpressure:** Both `_inbound_queue` (`inbound_queue_capacity`) and `_outbound_queue` (`outbound_queue_capacity`) are strictly bounded. Saturation beyond `backpressure_timeout_seconds` raises `MediaBackpressureOverflowError`.
- **Idempotent Cleanup:** `await adapter.close()` is idempotent, cancels and awaits all background sender/receiver tasks within `close_timeout_seconds`, closes the WebSocket transport, and drains internal queues.

---

## 5. Capability Matrix

| Capability | Status | Notes |
| --- | --- | --- |
| `AIRealtimeMediaPort` (`send_frame` / `receive_frame` / `close`) | `LIVE_VERIFIED` & `MOCK_VERIFIED` | Verified offline via `RealtimeAdapterConformanceSuite` (8/8 dimensions, 0 skips) + 32 mock server tests, and live against `wss://api.openai.com/v1/realtime?model=gpt-realtime` (2026-11-16) |
| `InterruptibleAIPort` (`response.cancel` + local purge) | `LIVE_VERIFIED` & `MOCK_VERIFIED` | Sends `response.cancel`, fences superseded `turn_id`s, resets `OutboundFramePacker`, purges `_outbound_queue`; verified live with mid-response `status="cancelled"` |
| `PCM16LE` 16 kHz <-> 24 kHz Resampling | `LIVE_VERIFIED` & `MOCK_VERIFIED` | Stateful 2:3 and 3:2 linear interpolation; verified offline with 440 Hz sine wave and live with bidirectional 16 kHz <-> 24 kHz speech |
| Provider Playback Buffer Clear (`can_clear_provider_playback`) | `UNSUPPORTED` | WebSocket client cannot clear audio already transmitted to telephony leg |
| Playback Confirmation (`can_observe_playback_acks`) | `UNSUPPORTED` | `cannot_confirm_playback = True` |
| Live OpenAI Realtime API (`wss://api.openai.com/v1/realtime`) | `LIVE_VERIFIED` | Verified on `gpt-realtime` (`session.created`, `session.updated`, `input_audio_buffer.append`, `response.output_audio.delta`, `response.done` `completed` & `cancelled`, clean close) |
| OpenAI GPT-Live Adapter (`wss://api.openai.com/v1/live/sessions`) | `LIVE_VERIFIED` & `MOCK_VERIFIED` | Dedicated [`kaiwen_gateway.adapters.openai_gpt_live`](OPENAI_GPT_LIVE_ADAPTER.md) adapter (`OpenAIGPTLiveMediaAdapter`) verified offline and live on `gpt-live-1` (`M4-R4`) |

---

## 6. Semantic & Tool-Result Side Channel (`ADR-0008`, opt-in, `MOCK_VERIFIED`)

```python
from kaiwen_gateway.modules.voice import ToolDeclaration, ToolResult, ToolCallRequested

cfg = OpenAIRealtimeConfig(
    api_key=server_owned_key,
    enable_semantic_events=True,
    tools=(ToolDeclaration(name="tasks_add", description="Add a task",
                           parameters={"type": "object", "properties": {"title": {"type": "string"}}}),),
    input_transcription_model="gpt-4o-transcribe",  # optional
)
adapter = OpenAIRealtimeMediaAdapter(call_id=call_id, tenant_id=tenant_id, config=cfg)
# Media flows through VoiceBridgeSession as before. In a separate task:
while (event := await adapter.receive_semantic_event()) is not None:
    if isinstance(event, ToolCallRequested):
        output = await my_policy_checked_executor(event)  # Gateway never authorizes
        await adapter.submit_tool_result(ToolResult(call_id=event.call_id, output=output))
```

| Server event | Semantic event |
| --- | --- |
| `session.updated` (first) | `SessionLifecycleEvent(phase="ready")` |
| `response.created` / `response.done` | `ResponseLifecycleEvent` (`started` / `completed` / `cancelled` / `failed` / `incomplete`) |
| `response.function_call_arguments.done`, `response.output_item.done`, `response.done` `function_call` items | one `ToolCallRequested` per `call_id` |
| `response.output_audio_transcript.delta` / `.done` | `TranscriptEvent(role="assistant")` |
| `conversation.item.input_audio_transcription.delta` / `.completed` | `TranscriptEvent(role="user")` (only with `input_transcription_model`) |

Tool results are sent as `conversation.item.create` (`function_call_output`) followed by `response.create`. `cancel_turn()` and close/failure cancel pending calls (`ToolCallCancelled`); results for them raise `StaleToolCallError`. Live verification of this channel is **NOT_VERIFIED**.

## 7. Mock Testing Instructions

Run the conformance and integration suites offline with zero API keys or external network:

```bash
uv run --extra server pytest tests/test_openai_realtime_conformance.py tests/test_openai_realtime_adapter.py tests/test_openai_realtime_semantic.py -v
```
