# AI Provider Realtime Adapter Integration Guide (M4-R1)

> **Audience:** External AI provider adapter authors and contributors (e.g. OpenAI Realtime / Live API developers).  
> **Status:** Active integration specification for Kaiwen Gateway.  
> **Conformance:** All adapters must satisfy `kaiwen_gateway.testing.conformance.RealtimeAdapterConformanceSuite`.

---

## 1. Overview & Architecture

Kaiwen Gateway uses Hexagonal Architecture (Ports and Adapters) to bridge telephony sessions to real-time AI backends. The Gateway core and voice media bridge are completely provider-agnostic: they operate exclusively on canonical audio frames and abstract asynchronous ports.

```
+-------------------------------------------------------------------------------+
|                             Kaiwen Gateway                                    |
|                                                                               |
|  [Telephony Leg]          [VoiceBridgeSession]             [AI Provider Leg]  |
|  WebSocket / PSTN <====> TelephonyMediaPort  <====>  AIRealtimeMediaPort <===> External AI
|                              (AudioFrame)               (AudioFrame)        (e.g. OpenAI)
+-------------------------------------------------------------------------------+
```

A third-party developer can implement an AI provider adapter (such as an OpenAI Realtime WebSocket client) without modifying any internal Gateway logic or telephony transports.

---

## 2. Core Port Interfaces

The provider-neutral interfaces are defined in `kaiwen_gateway.modules.voice.ports`.

### 2.1 `AIRealtimeMediaPort` (alias: `AIRealtimePort`)

Every AI realtime adapter must implement the `AIRealtimeMediaPort` protocol:

```python
from typing import Protocol, runtime_checkable
from kaiwen_gateway.modules.voice.domain import AudioFrame

@runtime_checkable
class AIRealtimeMediaPort(Protocol):
    @property
    def is_closed(self) -> bool:
        """Return True if this AI realtime media port has been closed."""
        ...

    async def receive_frame(self) -> AudioFrame | None:
        """Receive the next audio frame from the AI backend.

        Returns:
            The next AudioFrame received from AI, or None on clean stream EOF.

        Raises:
            MediaPortClosedError: If the port is already closed.
            MediaPortError: If an underlying I/O or transport error occurs.
        """
        ...

    async def send_frame(self, frame: AudioFrame) -> None:
        """Send an audio frame to the AI realtime backend.

        Args:
            frame: Validated AudioFrame to transmit.

        Raises:
            MediaPortClosedError: If the port is already closed.
            MediaPortError: If an underlying I/O or transport error occurs.
        """
        ...

    async def close(self) -> None:
        """Idempotently close the AI realtime media port and release resources."""
        ...
```

### 2.2 `InterruptibleAIPort` (Barge-in Support)

If the AI provider supports cancelling in-flight response generation when the user interrupts (barges in), the adapter should also implement `InterruptibleAIPort`:

```python
@runtime_checkable
class InterruptibleAIPort(Protocol):
    @property
    def interruption_capabilities(self) -> InterruptionCapabilities:
        """Return the interruption capabilities of this AI port."""
        ...

    async def cancel_turn(self, turn_id: int) -> None:
        """Cancel the specified turn generation on the AI backend.

        Args:
            turn_id: Monotonically increasing turn generation index to cancel.
        """
        ...
```

#### `InterruptionCapabilities`

Declare capabilities accurately:
- `can_cancel_future_generation: bool = True` (if the provider API supports canceling active generation, e.g. via `response.cancel`).
- `can_discard_local_output: bool = True` (local queues can be purged).
- `cannot_confirm_playback: bool = True` (unless explicit per-frame playback ACKs are received from the remote leg).

---

## 3. Canonical Audio Framing

All frames exchanged across `AIRealtimeMediaPort` are instances of `kaiwen_gateway.modules.voice.domain.AudioFrame`.

- **Encoding:** Linear PCM 16-bit little-endian (`pcm16le`).
- **Sample Rate:** `16000` Hz (16 kHz).
- **Channels:** `1` (mono).
- **Frame Duration:** `20` ms (320 samples = `640` bytes payload).
- **Domain Envelope:**
  - `payload: bytes`: Exactly 640 bytes for standard 20ms frames (max allowable 65,536 bytes).
  - `format: AudioFormat.PCM16LE`: `"pcm16le"`.
  - `sequence: int`: Monotonically increasing sequence number per stream.
  - `timestamp_ms: float`: Milliseconds from call start.
  - `turn_id: int | None`: Active turn index, assigned and fenced by Gateway.

> [!IMPORTANT]
> Any encoding, decoding, resampling, or protocol framing (e.g. converting PCM16LE to G.711 or base64 JSON payloads) must be performed **strictly at the adapter boundary**. Core Gateway logic operates purely on canonical `AudioFrame` instances.

---

## 4. Session Lifecycle & Context-Aware Factory

Adapters are instantiated per call session via an `ai_media_port_factory` registered on the ASGI application:

```python
from kaiwen_gateway.server.app import create_gateway_app

def my_openai_adapter_factory(*, call_id: str, tenant_id: str, **kwargs) -> AIRealtimeMediaPort:
    return OpenAILiveMediaAdapter(
        call_id=call_id,
        tenant_id=tenant_id,
        api_key=os.environ["OPENAI_API_KEY"],
    )

app = create_gateway_app(
    enable_synthetic_media=True,
    ai_media_port_factory=my_openai_adapter_factory,
)
```

### Factory Resolution Semantics
- **Context-Aware:** If the factory accepts `call_id` and/or `tenant_id` (or `**kwargs`), the Gateway passes the verified call context upon session initialization.
- **Backwards-Compatible:** 0-argument factories (e.g. `lambda: BoundedFakeAIEchoPort()`) remain fully supported for tests.

---

## 5. Barge-in & Interruption Handling

When a user speaks while the AI is outputting audio:
1. Telephony transport receives speech and notifies Gateway via `call.interrupt`.
2. `VoiceBridgeSession` immediately acquires `_interruption_lock` and sets an output fence (`_fenced_turn_id`).
3. Outbound queues are purged of stale frames belonging to superseded turns (`<= turn_id`).
4. Gateway calls `await adapter.cancel_turn(target_turn)` on the AI port.
5. In response to `cancel_turn`, the adapter must:
   - Send the provider-specific cancellation message (e.g. `response.cancel`).
   - Discard any pending buffered frames for that turn within the adapter.
   - Return cleanly without blocking (timeout bounded).
6. Gateway advances `current_turn_id += 1`.
7. Any delayed frames arriving from the upstream provider for old turns are automatically dropped by the Gateway's stale output suppression filters.

---

## 6. Secret & Credential Handling

- **Server-Owned Secrets:** All provider API keys (e.g. `OPENAI_API_KEY`), tokens, and credentials must be owned by the server process (via environment variables, secret managers, or server configuration).
- **Client Prohibition:** Client WebSockets, synthetic media connections, and telephony SIP/WSS messages must **never** contain provider credentials.
- **Zero Logging of Secrets:** Adapters must never log raw API keys, bearer tokens, or audio payloads in plain text.

---

## 7. Reusable Conformance Test Suite

The Gateway provides a pre-packaged conformance test suite in `kaiwen_gateway.testing.conformance`:

```python
import pytest
from kaiwen_gateway.testing.conformance import RealtimeAdapterConformanceSuite
from my_package.adapters.openai import OpenAILiveMediaAdapter

class TestOpenAILiveAdapterConformance(RealtimeAdapterConformanceSuite):
    def create_adapter(self, *, call_id: str, tenant_id: str, queue_capacity: int = 8, **kwargs):
        # Return adapter configured with synthetic/mock backend for deterministic CI
        return OpenAILiveMediaAdapter(
            call_id=call_id,
            tenant_id=tenant_id,
            queue_capacity=queue_capacity,
            endpoint_url="ws://127.0.0.1:8999/mock-openai",
        )
```

The suite deterministically verifies 8 critical dimensions:
1. `test_bi_directional_audio_routing_and_delivery`
2. `test_two_concurrent_calls_isolation`
3. `test_turn_interruption_and_cancellation`
4. `test_stale_output_suppression_under_voice_bridge`
5. `test_media_format_validation`
6. `test_bounded_queue_and_backpressure_enforcement`
7. `test_upstream_error_propagation_and_state_safety`
8. `test_idempotent_close_and_resource_teardown`

---

## 8. Local Live Smoke Testing & Boundaries

### Running Local Live Tests
- For local manual smoke testing with real OpenAI endpoints, write a standalone runner in `tools/` or examples.
- **Strict Rule:** Zero paid API calls or external network dependencies are permitted in automated CI runs. All CI checks must run with mock/synthetic fixtures.
- Measure latency across processing, queuing, and transport separately using the telemetry hooks in `VoiceBridgeSession`.

### Non-Claims & Gate Status
- Implementing an adapter is tracked under milestone `M4`.
- Until an OpenAI Live adapter is implemented and verified against the live provider contract, `FR-INT-005` remains deferred under the synthetic alpha Gate B exception.
