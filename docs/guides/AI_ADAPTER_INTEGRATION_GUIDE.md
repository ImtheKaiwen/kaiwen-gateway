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
import os
from collections.abc import Callable
from kaiwen_gateway.modules.voice.ports import AIRealtimeMediaPort
from kaiwen_gateway.server.app import create_gateway_app

# Example: Provider adapter factory (pseudocode implementation for external adapter)
def my_openai_adapter_factory(*, call_id: str, tenant_id: str, **kwargs) -> AIRealtimeMediaPort:
    # external_adapter = OpenAILiveMediaAdapter(call_id=call_id, tenant_id=tenant_id, api_key=os.environ["OPENAI_API_KEY"])
    # return external_adapter
    raise NotImplementedError("Implement provider-specific adapter")

app = create_gateway_app(
    enable_synthetic_media=True,
    ai_media_port_factory=my_openai_adapter_factory,
)
```

### Factory Invocation Guarantees & Context Propagation Contract

1. **Signature Inspection Before Invocation:**
   - The Gateway inspects the callable's signature (`inspect.signature`) and pre-binds parameters before calling the factory.
   - Context parameters (`call_id`, `tenant_id`, and `context`/`verified_context`) are bound according to parameter names and kinds.
   - If the factory accepts `**kwargs`, `call_id` and `tenant_id` (and `context`) are supplied in keyword arguments.
   - Factories with optional identity parameters (e.g. `call_id: str = "default"`) are passed the verified session context, never allowed to silently default.

2. **Single-Invocation Guarantee:**
   - The selected factory is invoked **EXACTLY ONCE**.
   - If the factory raises an exception during initialization (e.g. `TypeError`, `ValueError`, network error), the Gateway does **NOT** retry with omitted arguments or fall back to an unverified default factory.

3. **Fail-Closed Session Cleanup:**
   - Any exception during factory instantiation propagates immediately into the Gateway's safe session cleanup path (`_cleanup_media_session`).
   - The session terminates fail-closed: the active call registration is cleared, any initialized resources are closed, and the client WebSocket is closed with code `1011` (`Initialization failed`).

4. **Backwards Compatibility:**
   - Valid zero-argument synthetic factories (e.g. `lambda: BoundedFakeAIEchoPort()`) remain fully supported for testing.
   - If a factory defines required parameters that cannot be satisfied from verified context, the Gateway fails closed with `VoiceDomainError` without calling the factory.

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
from kaiwen_gateway.modules.voice.ports import AIRealtimeMediaPort

class TestMyProviderAdapterConformance(RealtimeAdapterConformanceSuite):
    def create_adapter(self, *, call_id: str, tenant_id: str, queue_capacity: int = 8, **kwargs) -> AIRealtimeMediaPort:
        # Return adapter configured with a mock/synthetic upstream backend for deterministic CI:
        # return MyProviderMediaAdapter(call_id=call_id, tenant_id=tenant_id, queue_capacity=queue_capacity)
        raise NotImplementedError("Provide mock-backed adapter instance")
```

### 7.1 Conformance Test Harness Hooks

The suite defines explicit test harness hooks that external adapter test suites can override:

| Hook Method | Signature | Purpose |
|---|---|---|
| `inject_upstream_frame` | `(adapter, frame: AudioFrame) -> bool` | Injects synthetic upstream response audio into the mock adapter/transport. Default checks `push_frame()`. |
| `inject_upstream_eof` | `async (adapter) -> bool` | Injects clean EOF into the mock adapter/transport. Default checks `push_eof()`. |
| `get_received_inbound_frames` | `(adapter) -> list[AudioFrame] \| None` | Returns recorded inbound frames sent by the caller via `send_frame()`. |
| `get_buffered_frame_count` | `(adapter) -> int \| None` | Returns the count of buffered frames without inspecting private attributes (`_output_queue`). Default checks `buffered_frame_count`, `queue_depth`, or `buffer_size`. |
| `inject_upstream_failure` | `(adapter, error: Exception) -> bool` | Injects transport fault into the actual adapter instance under test. Default checks `inject_receive_error()` or `trigger_upstream_failure()`. |

### 7.2 Independent Bidirectional Audio Semantics

In real AI realtime adapters, `send_frame()` and `receive_frame()` represent independent audio channels:
- `send_frame()` streams the caller's microphone audio upstream to the model.
- `receive_frame()` receives synthesized speech generated by the model.
The test suite validates byte integrity, sequence ordering, and framing independently in each direction and does **not** assume or require echo behavior.

### 7.3 Eight Core Verification Dimensions

The suite deterministically verifies 8 critical dimensions:
1. `test_bi_directional_audio_routing_and_delivery`: Independent routing, delivery, 640-byte PCM16LE framing, sequence monotonic advance, and clean EOF.
2. `test_two_concurrent_calls_isolation`: Strict tenant and call isolation with zero cross-talk or state bleeding between concurrent calls.
3. `test_turn_interruption_and_cancellation`: Upstream turn cancellation, local buffer purging, and turn high-water tracking.
4. `test_stale_output_suppression`: Automatic suppression and dropping of delayed/superseded turn frames.
5. `test_media_format_validation`: Rejection of malformed, oversized, or unsupported frame parameters.
6. `test_bounded_queue_and_backpressure_enforcement`: Bounded memory verification, overload policies, and backpressure stall/overflow handling.
7. `test_upstream_error_propagation_and_state_safety`: Fault injection into the adapter under test, transition to closed state, unblocking of receivers, and zero leaked background tasks.
8. `test_idempotent_close_and_resource_teardown`: Idempotent closure, post-close error raising, and zero asyncio task leaks.

---

## 8. What the Conformance Suite Proves (and Does NOT Prove)

### What Conformance Proves:
- **Contract & Protocol Compliance:** The adapter strictly satisfies `AIRealtimeMediaPort` and `InterruptibleAIPort` specifications.
- **Audio Framing & Domain Invariants:** The adapter exchanges valid 16 kHz PCM16LE mono 20 ms `AudioFrame` value objects.
- **Multi-Tenant Isolation:** Concurrent sessions never bleed audio or shared mutable state.
- **Memory Safety & Backpressure:** Queues are finite and bounded; backpressure overflow is handled deterministically.
- **Lifecycle & Resource Discipline:** Idempotent `close()`, proper cancellation, unblocked receivers, and zero asyncio task leaks.

### What Conformance Does NOT Prove:
- **Real-World Latency & Jitter:** Synthetic tests do not measure public Internet network jitter, Wi-Fi latency, or packet reordering.
- **Model Speech Quality & Accuracy:** Conformance does not evaluate transcription accuracy, voice naturalness, or model hallucinations.
- **Vendor Rate Limits & Quotas:** Conformance runs against mock transports and does not test external API billing, rate limits, or concurrency quotas.
- **Production Media Gateway Round-Trip Time:** Total round-trip latency depends on telephony transcoding, upstream model inference speed, and network hops.

---

## 9. Local Live Smoke Testing & Boundaries

### Running Local Live Tests
- For local manual smoke testing with real OpenAI endpoints, write a standalone runner in `tools/` or examples.
- **Strict Rule:** Zero paid API calls or external network dependencies are permitted in automated CI runs. All CI checks must run with mock/synthetic fixtures.
- Measure latency across processing, queuing, and transport separately using the telemetry hooks in `VoiceBridgeSession`.

### Non-Claims & Gate Status
- Implementing an adapter is tracked under milestone `M4`.
- Until an OpenAI Live adapter is implemented and verified against the live provider contract, `FR-INT-005` remains deferred under the synthetic alpha Gate B exception.
