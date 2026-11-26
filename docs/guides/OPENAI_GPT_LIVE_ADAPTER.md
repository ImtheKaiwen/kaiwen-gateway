# OpenAI GPT-Live Adapter Guide (M4-R4)

> **Status:** `LIVE_VERIFIED` (`gpt-live-1` via `wss://api.openai.com/v1/live/sessions`) & `MOCK_VERIFIED` (9 offline adapter integration tests + 8 conformance dimensions, 0 skips).
> **Companion Adapter:** For OpenAI's standard Realtime API (`wss://api.openai.com/v1/realtime`, `gpt-realtime`), see [`OPENAI_REALTIME_ADAPTER.md`](OPENAI_REALTIME_ADAPTER.md).

---

## 1. Installation & Optional Dependencies

Core-only installations (`kaiwen-gateway` with `dependencies = []`) remain free of external runtime dependencies.

To enable the OpenAI GPT-Live WebSocket transport (`websockets`):

```bash
pip install "kaiwen-gateway[openai]"
# or with the ASGI server stack:
pip install "kaiwen-gateway[server]"
```

---

## 2. Environment & Server Configuration

API credentials are strictly **server-owned** (`OPENAI_API_KEY` environment variable or local git-ignored `.env`) and must never be supplied by client WebSockets:

```python
from kaiwen_gateway.adapters.openai_gpt_live import (
    OpenAIGPTLiveConfig,
    create_openai_gpt_live_adapter_factory,
)
from kaiwen_gateway.core import CoreConfig
from kaiwen_gateway.server import create_gateway_app

# Reads OPENAI_API_KEY from server environment or local .env (fails closed if missing)
gpt_live_config = OpenAIGPTLiveConfig.from_env(
    model="gpt-live-1",
    voice="marin",
    instructions="You are a concise voice assistant.",
)

app = create_gateway_app(
    CoreConfig(instance_name="gateway-openai-gpt-live"),
    enable_synthetic_media=True,
    ai_media_port_factory=create_openai_gpt_live_adapter_factory(gpt_live_config),
)
```

---

## 3. Official Provider Contract & Protocol Comparison

OpenAI GPT-Live (`wss://api.openai.com/v1/live/sessions`) uses a distinct wire protocol from OpenAI Realtime (`wss://api.openai.com/v1/realtime`):

| Dimension | OpenAI Realtime (`openai_realtime`) | OpenAI GPT-Live (`openai_gpt_live`) |
| --- | --- | --- |
| **Endpoint** | `wss://api.openai.com/v1/realtime?model=gpt-realtime` | `wss://api.openai.com/v1/live/sessions` |
| **Default Model / Voice** | `gpt-realtime` / `alloy` | `gpt-live-1` / `marin` |
| **Session Initialization** | Client sends `session.update`; server replies `session.created` / `session.updated` | Client sends `session.start` (`delegation: {"type": "client"}`); server replies `session.started` |
| **Pre-Ready Audio Rule** | Audio buffered locally until connected | **Strict:** No `session.input_audio.append` may be sent before `session.started` is received |
| **Client Audio Event** | `input_audio_buffer.append` (24 kHz PCM16LE base64) | `session.input_audio.append` (24 kHz PCM16LE base64) |
| **Server Audio Event** | `response.output_audio.delta` (24 kHz PCM16LE base64) | `session.output_audio.delta` (24 kHz PCM16LE base64) |
| **Upstream Cancel Event** | `response.cancel` (`can_cancel_future_generation = True`) | **Full-duplex native turn handling** (no `response.cancel` command; `can_cancel_future_generation = False`, `can_discard_local_output = True`) |

### Audio Resampling Pipeline (`kaiwen_gateway.adapters.openai_realtime.audio`)
- **Inbound (Gateway -> OpenAI GPT-Live):** Canonical `16,000 Hz` mono `pcm16le` (`640` bytes / 20 ms) is resampled to `24,000 Hz` mono `pcm16le` (`960` bytes / 20 ms) via `PCM16Resampler16kTo24k`, base64-encoded, and sent as `session.input_audio.append` only after `session.started` is confirmed.
- **Outbound (OpenAI GPT-Live -> Gateway):** Base64 `session.output_audio.delta` payloads (`24,000 Hz` mono `pcm16le`) are decoded, resampled to `16,000 Hz` via stateful `PCM16Resampler24kTo16k`, and repacked into canonical `640`-byte `AudioFrame`s by `OutboundFramePacker` with contiguous monotonic `sequence` and `timestamp_ms`.

---

## 4. Interruption & Turn Control Semantics

Because GPT-Live handles overlapping speech natively in full-duplex and does not expose a client-side `response.cancel` wire event:

- `OpenAIGPTLiveMediaAdapter.interruption_capabilities` truthfully declares:
  - `can_cancel_future_generation = False`
  - `can_discard_local_output = True`
  - `can_clear_provider_playback = False`
  - `cannot_confirm_playback = True`
- When `VoiceBridgeSession.interrupt()` (or synthetic WebSocket `call.interrupt`) is invoked:
  1. `VoiceBridgeSession` immediately fences the interrupted turn and purges its own outbound queue.
  2. `VoiceBridgeSession` calls `await adapter.cancel_turn(turn_id)`.
  3. `OpenAIGPTLiveMediaAdapter.cancel_turn(turn_id)` purges all buffered frames for the superseded turn from `_outbound_queue`, resets `_packer` and `_resampler_out`, enables `_suppress_until_next_input = True` so trailing provider deltas from the interrupted turn are suppressed until new caller input arrives, and advances `_current_turn_id`.
  4. `receive_frame()` maintains contiguous delivery sequence numbers across purged turns so `VoiceBridgeSession` strict sequence validation remains satisfied.

---

## 5. Capability Matrix

| Capability | Status | Notes |
| --- | --- | --- |
| `AIRealtimeMediaPort` (`send_frame` / `receive_frame` / `close`) | `LIVE_VERIFIED` & `MOCK_VERIFIED` | Verified offline via `RealtimeAdapterConformanceSuite` (8/8 dimensions, 0 skips) + 9 integration tests, and live end-to-end through `VoiceBridgeSession` against `wss://api.openai.com/v1/live/sessions` (`gpt-live-1`, 2026-11-16) |
| `InterruptibleAIPort` (local output purge + turn fencing) | `LIVE_VERIFIED` & `MOCK_VERIFIED` | `can_cancel_future_generation=False, can_discard_local_output=True`; purges local buffers, suppresses trailing deltas until next caller input, and advances turn ID; verified live and in mock tests |
| `PCM16LE` 16 kHz <-> 24 kHz Resampling | `LIVE_VERIFIED` & `MOCK_VERIFIED` | Shared stateful 2:3 and 3:2 linear interpolation resamplers |
| Remote `ws://` Credential Protection | `MOCK_VERIFIED` | Rejects plaintext `ws://` for non-loopback hosts while permitting loopback `ws://127.0.0.1` for local mock testing |

---

## 6. Semantic Side Channel (`ADR-0008`, opt-in, `MOCK_VERIFIED`)

With `enable_semantic_events=True` the adapter exposes `session.input_transcript.delta` / `session.output_transcript.delta` as non-final `TranscriptEvent`s and `session.started` / `session.closed` as `SessionLifecycleEvent`s. Current OpenAI docs describe no transcript completion event and no client-delegation function-call contract, so `tool_calls`, `*_transcript_final` and `response_lifecycle` stay `False` and `submit_tool_result()` raises `UnsupportedSemanticCapabilityError`. That applies to client delegation (the default). For Responses delegation set `responses_delegation=ResponsesDelegationConfig(...)`: tool capabilities are then declared and calls follow `ADR-0009` (`MOCK_VERIFIED` only; no live provider run). Backend model tokens are billed separately from the Live session.

## 7. Mock Testing Instructions

Run the conformance and integration suites offline with zero API keys or external network:

```bash
uv run --extra server pytest tests/test_openai_gpt_live_conformance.py tests/test_openai_gpt_live_adapter.py -v
```
