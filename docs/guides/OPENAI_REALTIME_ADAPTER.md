# OpenAI Realtime Adapter Guide (M4-R2)

> **Status:** `MOCK_VERIFIED` (`main` unreleased post-`v0.1.0a1`, PR #49; `MAINTENANCE — INTEGRATION PENDING`) — Verified against a local mocked OpenAI Realtime WebSocket server and all 8 dimensions of `RealtimeAdapterConformanceSuite` (38 tests, 0 skips). **Not `LIVE_VERIFIED`**; live OpenAI API interoperability requires maintainer authorization and an `OPENAI_API_KEY`.

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
| `AIRealtimeMediaPort` (`send_frame` / `receive_frame` / `close`) | `MOCK_VERIFIED` | Verified via `RealtimeAdapterConformanceSuite` (8/8 dimensions, 0 skips) and local `websockets` mock server |
| `InterruptibleAIPort` (`response.cancel` + local purge) | `MOCK_VERIFIED` | Sends `response.cancel`, fences superseded `turn_id`s, resets `OutboundFramePacker`, purges `_outbound_queue` |
| `PCM16LE` 16 kHz <-> 24 kHz Resampling | `MOCK_VERIFIED` | Stateful 2:3 and 3:2 linear interpolation; verified with 440 Hz sine wave and irregular chunk sizes |
| Provider Playback Buffer Clear (`can_clear_provider_playback`) | `UNSUPPORTED` | WebSocket client cannot clear audio already transmitted to telephony leg |
| Playback Confirmation (`can_observe_playback_acks`) | `UNSUPPORTED` | `cannot_confirm_playback = True` |
| Live OpenAI Realtime API Interoperability | `UNVERIFIED` | Requires maintainer-authorized live run with `OPENAI_API_KEY` (`LIVE_VERIFIED` pending) |

---

## 6. Mock Testing Instructions

Run the conformance and integration suites offline with zero API keys or external network:

```bash
uv run --extra server pytest tests/test_openai_realtime_conformance.py tests/test_openai_realtime_adapter.py -v
```
