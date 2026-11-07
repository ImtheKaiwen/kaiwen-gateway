# Voice Media Bridge, Turn Interruption, Observability & Audio Contracts (M4.1 / M4.2 / M5.1A)

> **Status:** M4.1 Complete (PR #20 / `577e6b9`); M4.2A Complete (PR #21 / `df28327`); M4.2B.1 Complete (PR #22 / `e457f8f`); M4.2B.2 Complete (PR #23 / `a3528ea`); M5.1A In progress (provider-independent Voice Bridge observability foundation)
> **Document version:** 1.4
> **Last updated:** 2026-11-07
> **Scope:** Voice-local media domain contracts, provider-neutral media ports, bounded bidirectional bridge, turn identity, stale frame suppression, interruption capability protocol, synthetic binary WebSocket transport, and provider-independent in-memory telemetry.

---

## 1. Overview and Architecture Boundary

Kaiwen Gateway V1 architecture separates the **control plane** (call signaling, lifecycle, authentication, admission) from the **media plane** (high-frequency streaming audio frames).

In milestones M4.1–M4.2 and M5.1A, the voice media subsystem is organized under `kaiwen_gateway.modules.voice`:
```text
src/kaiwen_gateway/modules/voice/
  ├── __init__.py           # Public exports (AudioFrame, VoiceBridgeSession, ports, adapters, telemetry)
  ├── domain.py             # Pure immutable AudioFrame contract and domain error types
  ├── ports.py              # Provider-neutral async media port protocols
  ├── bridge.py             # Supervised bidirectional VoiceBridgeSession with bounded queues & telemetry
  ├── telemetry.py          # Bounded, privacy-safe in-memory session & aggregate telemetry (M5.1A)
  └── adapters/
      ├── __init__.py
      ├── fake.py           # Deterministic fake adapters for telephony & AI media
      └── fake_ai.py        # Bounded interruptible fake AI echo ports
```

### Architectural Invariants:
1. **Core Independence:** Core modules do not import or depend on Voice.
2. **Provider Neutrality:** The media domain and bridge do not import or assume WebSocket, Uvicorn, Verimor, or OpenAI SDKs.
3. **No Unnecessary Conversion:** Media bytes are streamed directly as raw immutable frames; they are never converted to JSON strings or durable message bus events.
4. **Trusted Identity:** `call_id` and `tenant_id` are supplied via server-verified call context; untrusted media frame metadata cannot alter tenant or call authority.
5. **Zero Mandatory Runtime Dependencies:** The base package maintains zero mandatory runtime dependencies.

---

## 2. Voice Media Contract (`AudioFrame`)

The `AudioFrame` domain object (`domain.py`) is an immutable, frozen, slotted dataclass encapsulating a single unit of streaming audio:

```python
@dataclass(frozen=True, slots=True)
class AudioFrame:
    payload: bytes
    format: str | AudioFormat
    sample_rate: int
    channels: int
    sequence: int
    timestamp_ms: float
    turn_id: int | None = None
```

### Validation Invariants:
- **Payload:** Must be `bytes`, non-empty (`len(payload) > 0`), and bounded by `DEFAULT_MAX_PAYLOAD_SIZE = 65_536` bytes (64 KiB). Oversized payloads raise `OversizedAudioFrameError`.
- **Format:** Non-empty string or `AudioFormat` enum (e.g. `AudioFormat.PCM16LE = "pcm16le"`).
- **Sample Rate:** Positive integer strictly between 8,000 and 192,000 Hz.
- **Channels:** Positive integer strictly between 1 and 8 (e.g. 1 for mono, 2 for stereo).
- **Sequence:** Monotonically non-negative integer (`sequence >= 0`).
- **Timing:** Non-negative finite float (`timestamp_ms >= 0.0` and `math.isfinite(timestamp_ms)`). Non-finite values (`NaN`, `Inf`, `-Inf`) or negative numbers raise `InvalidAudioFrameError`.

> **Note on Audio Formats:** Initial synthetic fixtures use standard 20ms PCM16LE mono at 16 kHz (640 bytes). This is a test fixture configuration and does not represent a commitment or claim regarding real Verimor or OpenAI audio wire formats. Gateway does not implement codec converters, resamplers, or audio transcoders in M4.1.

---

## 3. Provider-Neutral Media Ports

Media ports define the asynchronous communication boundary between the bridge and concrete transport/AI adapters:

### `TelephonyMediaPort`
- `receive_frame() -> AudioFrame | None`: Asynchronously reads the next audio frame from telephony. Returns `None` on clean EOF / end-of-stream.
- `send_frame(frame: AudioFrame) -> None`: Transmits an audio frame to telephony.
- `close() -> None`: Idempotently closes the port and unblocks any waiting tasks.

### `AIRealtimeMediaPort`
- `receive_frame() -> AudioFrame | None`: Asynchronously reads the next audio frame from AI realtime backend. Returns `None` on clean EOF / end-of-stream.
- `send_frame(frame: AudioFrame) -> None`: Transmits an audio frame to the AI realtime backend.
- `close() -> None`: Idempotently closes the port and unblocks any waiting tasks.

Both protocols are `@runtime_checkable` and raise `MediaPortClosedError` if I/O is attempted on a closed port.

---

## 4. Supervised Bidirectional Bridge (`VoiceBridgeSession`)

`VoiceBridgeSession` manages the concurrent lifecycle of two independent media pumps:
1. **Inbound Pump:** `TelephonyMediaPort` $\rightarrow$ bounded inbound queue $\rightarrow$ `AIRealtimeMediaPort`.
2. **Outbound Pump:** `AIRealtimeMediaPort` $\rightarrow$ bounded outbound queue $\rightarrow$ `TelephonyMediaPort`.

```text
               ┌──────────────────────────────────────────────┐
               │              VoiceBridgeSession              │
               │                                              │
Telephony ────>│ [Inbound Receiver] ──> [Inbound Queue (8)] ──>│────> AI Backend
Media Port     │                             │                │     Media Port
               │                             v                │
Telephony <────│<── [Outbound Queue (8)] <── [Outbound Recv] <│<──── AI Backend
Media Port     │                             │                │     Media Port
               │                     [Supervisor Task]        │
               └──────────────────────────────────────────────┘
```

### Bounded Buffering & Backpressure Policy:
- Each pump maintains a private, bounded `asyncio.Queue` (default capacity: 8 frames).
- **Normal Flow:** The receiver pushes frames to the queue; the forwarder pops and sends them.
- **Slow Consumer Backpressure:** When a consumer (e.g. AI backend or telephony leg) delays processing:
  1. The queue fills up to its maximum capacity (e.g. 8 frames).
  2. The receiver's `queue.put(frame)` pauses, pausing reads from the source port.
  3. No frames are dropped silently; FIFO order and byte integrity are strictly preserved.
- **Backpressure Timeout (Fail-Close):** If `backpressure_timeout` is configured and the consumer remains blocked past the deadline, `MediaBackpressureOverflowError` is raised, triggering a controlled abort and session closure.

### Memory & Latency Budget Considerations:
- **Theoretical Audio Backlog:** An 8-frame buffer holding standard 20ms audio frames represents $8 \times 20\text{ ms} = 160\text{ ms}$ of maximum buffered speech backlog per direction. This is a buffer depth boundary under slow consumers, **not** a measurement of end-to-end processing latency. Milestone M4.1 does not change queue capacity based on speculative latency targets; an empirical latency budget will be evaluated in subsequent benchmarks.
- **Theoretical Memory Ceiling:** With `DEFAULT_MAX_PAYLOAD_SIZE = 65_536` bytes (64 KiB) and two 8-frame queues, the theoretical upper bound for in-queue payload memory is:
  $$2 \times 8 \times 64\text{ KiB} = 1\text{ MiB per active call}$$
  This accounts solely for queued `AudioFrame` payloads and excludes in-flight OS socket buffers, adapter states, or external runtime memory.
- **Test Adapter Boundary:** Note that `FakeTelephonyMediaAdapter` and `FakeAIMediaAdapter` utilize unbounded `asyncio.Queue` internally for harness frame pre-staging. This is a deterministic testing mechanism and must not be confused with the strictly bounded queue guarantees enforced within `VoiceBridgeSession`.

### Failure, Cancellation & Lifecycle Guarantees:
- **Parent `run()` Cancellation:** When a parent task running `session.run()` is cancelled, the session immediately triggers terminal cleanup and shields it to completion. All background pumps and the supervisor task are terminated, underlying ports are closed, `_closed_event` is set, and `asyncio.CancelledError` is cleanly re-raised. Zero orphan tasks or open ports remain.
- **Single-Owner Terminal Cleanup Task:** Cleanup is owned by a single, session-tracked task (`_cleanup_task`). If a caller awaiting `session.close()` is cancelled, `CancelledError` propagates to the caller immediately, while the underlying cleanup task continues executing in the background until ports are released and `_closed_event` is set. Subsequent or concurrent `close()` calls await the same task.
- **Exhaustive Port Teardown & Observable Failures:** During terminal cleanup, both telephony and AI media ports are attempted. If an exception occurs on either port (or both), it is recorded as a terminal failure (`FAILED` state, never false `CLOSED`). If a prior pump failure already occurred, that original error is preserved in `session.error` and not silently overwritten by cleanup exceptions.
- **Terminal State Preservation:** A failed session (`FAILED` state caused by an unhandled pump error, invalid frame, backpressure overflow, or port close failure) cannot be overwritten by concurrent or subsequent `close()` calls to `CLOSING` or `CLOSED`. The terminal state strictly remains `FAILED` with `session.error` intact.
- **Pump Failure Isolation:** If either pump encounters an exception, the supervisor immediately cancels the sibling pump, records the error, and schedules terminal cleanup.
- **Clean EOF:** When a source signals EOF (`receive_frame() -> None`), the receiver queues a sentinel. The forwarder drains all buffered frames in FIFO order, then terminates cleanly.
- **Idempotent Teardown:** `session.close()` can be invoked repeatedly and concurrently without deadlocks, errors, or duplicate port teardown.

---

## 5. Turn Identity, Interruption & Playback Control (M4.2B.1)

In conversational voice streaming, user speech interruptions (barge-in) require immediate cessation of stale AI audio output and synchronization between the client, Gateway, and upstream AI models.

### Decoupled Turn Identity vs. Transport Sequence:
- **`AudioFrame.sequence` (Transport Level):** Monotonically increasing counter per transport leg verifying that frames arrive without transport loss or reordering.
- **`AudioFrame.turn_id` / `session.current_turn_id` (Logical Turn Generation):** Monotonically increasing identifier distinguishing successive AI responses. Starting at turn `1`, each interruption advances `current_turn_id` by 1.

### Two-Level Stale Frame Defense:
1. **Immediate Outbound Queue Purge:** When `session.interrupt()` is called, all unwritten frames currently residing in `_outbound_queue` are instantly dequeued and dropped, releasing backpressure and preventing outdated frames from reaching the telephony forwarder. EOF sentinels are strictly preserved.
2. **Ingress & Forwarder Stale Frame Suppression:**
   - In `_run_outbound_pump.receiver`: Any frame yielded by the AI port with `frame.turn_id < current_turn_id` is dropped prior to queueing and increments `stale_frames_suppressed`.
   - In `_run_outbound_pump.forwarder`: Any frame popped with `frame.turn_id < current_turn_id` is dropped and increments `stale_frames_suppressed`.

### Playback Honesty & Capability Protocols:
The subsystem defines explicit provider capability protocols (`ports.py`):
- `InterruptibleAIPort`: Declares `interruption_capabilities` and async `cancel_turn(turn_id: int)`.
- `InterruptibleTelephonyPort`: Declares `interruption_capabilities` and async `clear_pending_output()`.
- `InterruptionCapabilities`:
  - `can_cancel_future_generation`: Whether backend supports cancelling in-flight generation.
  - `can_discard_local_output`: Always True for Gateway's internal queue.
  - `can_cancel_transport_pending`: Whether transport driver can clear buffered socket bytes.
  - `can_clear_provider_playback`: Whether remote PSTN/telephony provider supports clearing playback.
  - `can_observe_playback_acks`: Whether provider sends frame-level playback confirmation.
  - `cannot_confirm_playback`: True by default. Enforces that Gateway **never claims audio was played** unless actual provider acknowledgments exist.

### Interruption Outcome Contract:
`VoiceBridgeSession.interrupt()` returns an immutable `InterruptionOutcome`:
```python
@dataclass(frozen=True, slots=True)
class InterruptionOutcome:
    call_id: str
    interrupted_turn_id: int
    new_turn_id: int
    locally_discarded_frames: int
    stale_frames_suppressed: int
    future_generation_cancelled: bool
    transport_pending_cancelled: bool
    provider_playback_cleared: bool
    playback_confirmed: bool = False
```

---

## 6. Provider-Independent Voice Bridge Observability (M5.1A)

`kaiwen_gateway.modules.voice.telemetry` provides a bounded, privacy-safe, in-memory telemetry layer for `VoiceBridgeSession` and the synthetic binary WebSocket media path without coupling to Kaiwen Agent, OpenAI, Verimor, or external telemetry SDKs.

### Separated Latency Dimensions & Unavailable Semantics:
Each `VoiceSessionMetricsSnapshot` reports separated latency distributions (`LatencyDistributionSummary`):
- `inbound_processing_ms`: Monotonic per-frame duration covering Gateway inbound receiver validation plus forwarder dispatch and `AIRealtimeMediaPort.send_frame()` await time (excluding `_inbound_queue` waiting time and producer queue-put backpressure wait).
- `outbound_processing_ms`: Monotonic per-frame duration covering Gateway outbound receiver validation/turn-fencing plus forwarder dispatch and `TelephonyMediaPort.send_frame()` await time (excluding `_outbound_queue` waiting time, producer queue-put backpressure wait, and any consumed `network_transport_ms`).
- `inbound_queue_wait_ms` & `outbound_queue_wait_ms`: Time each transferred/sent frame waits inside the bounded internal media queues (`_inbound_queue` and `_outbound_queue`).
- `network_transport_ms`: Per-frame ASGI `websocket.send_bytes(frame.payload)` await duration when using `WebSocketTelephonyMediaAdapter`, correlated to the exact frame via `consume_last_send_duration_ms(frame)`; explicitly `available=False` (`MetricAvailabilityReason.NO_SAMPLES`) for in-process ports. **Important:** `websocket.send_bytes` await duration measures the local ASGI send call only and is not proof of physical wire propagation, socket flush, client reception, or remote speaker playback.
- `upstream_ai_generation_ms`: Always `available=False` (`MetricAvailabilityReason.UNKNOWN_WITHOUT_PROVIDER_INSTRUMENTATION`) until an instrumented AI provider adapter is attached.
- `remote_playback_latency_ms`: Always `available=False` (`MetricAvailabilityReason.UNKNOWN_WITHOUT_PLAYBACK_ACKNOWLEDGMENT`) when the telephony provider does not emit playback acknowledgments.

When `sample_count == 0`, `LatencyDistributionSummary.available` is `False` and all duration fields (`min_ms`, `max_ms`, `mean_ms`, `p50_ms`, `p95_ms`, `p99_ms`) are `None` (never fabricated as `0.0`). Running `sample_count`, `min_ms`, `max_ms`, and `mean_ms` are tracked over the lifetime of the aggregator, whereas `p50_ms`, `p95_ms`, and `p99_ms` use linear interpolation over the bounded sliding window (`window_capacity = 256`).

### Frame & Interruption Accounting Identities:
For frames received through the bridge media ports, terminal session snapshots satisfy strict conservation identities:
- **Inbound Conservation:** `inbound_frames_received == inbound_frames_transferred + unforwarded_inbound_frames`
- **Outbound Conservation:** `outbound_frames_received == outbound_frames_sent + locally_discarded_frames + stale_frames_suppressed + unforwarded_outbound_frames`
- **Dropped Frame Total:** `dropped_frames_total == locally_discarded_frames + unforwarded_inbound_frames + unforwarded_outbound_frames` (`stale_frames_suppressed` is tracked as a separate, mutually exclusive counter for frames belonging to already-superseded turns).
- **Interruption Accounting:** `interruptions_requested == interruptions_completed + interruptions_noop + interruptions_failed`

### Synthetic Baseline Workload Classification:
Synthetic 25-frame baseline tests (25 frames $\times$ 20 ms = 500 ms of represented audio at 16 kHz mono PCM16LE, 640 B/frame) execute in **unpaced burst mode** (~0.5–5 ms wall-clock runtime) to deterministically exercise queue, turn-barrier, and loopback WebSocket paths. Burst-mode runtime must not be interpreted as real-time 20 ms pacing, production throughput capacity, or end-to-end conversational SLA compliance.

### Bounded Memory & Privacy Guarantees:
- **Bounded Latency Windows:** `BoundedLatencyAggregator` retains at most `DEFAULT_LATENCY_WINDOW_SIZE = 256` recent samples in a `deque(maxlen=256)` while tracking exact running `sample_count`, `min_ms`, `max_ms`, and `mean_ms`.
- **Bounded Session Retention:** `InMemoryVoiceTelemetrySink` retains at most `DEFAULT_MAX_RETAINED_SESSIONS = 64` immutable `VoiceSessionMetricsSnapshot` objects in a ring buffer and never holds references to `VoiceBridgeSession`, ports, queues, or `AudioFrame` instances.
- **Terminal Queue Draining:** On session closure, `VoiceBridgeSession` drains any unforwarded `AudioFrame` items from its internal queues and counts them under `unforwarded_inbound_frames` / `unforwarded_outbound_frames` (`dropped_frames_total`), ensuring zero retained audio frames after cleanup.
- **Strict Redaction:** Telemetry snapshots never include `call_id`, `tenant_id`, raw audio bytes, transcripts, tokens, phone numbers, prompts, or raw exception messages. Errors are aggregated strictly by the bounded `VoiceErrorCategory` enum.
- **Best-Effort Sink Isolation:** Exceptions raised by a `VoiceTelemetrySink` are caught and counted in `telemetry_sink_errors` so a faulty sink can never disrupt active media streaming or abort terminal cleanup.

---

## 7. Verification and Deterministic Testing

The test suite validates:
- **Byte Order & Integrity:** 100% identical byte matching between transmitted and received frames.
- **Two-Call Isolation:** Parallel calls (Call A and Call B) with distinct marker bytes show zero cross-call contamination; interrupting Call A has zero impact on Call B.
- **Cross-Tenant Isolation:** Sessions strictly fence tenant and call contexts.
- **Deterministic Fakes:** `InterruptibleFakeTelephonyAdapter`, `BoundedInterruptibleFakeAIEchoPort`, and `InterruptibleFakeAIPort` support delay simulation, fault injection, and EOF signaling without fragile sleep timing.
- **Interruption & WebSocket Conformance:** In-process (`tests/test_voice_interruption.py`) and real loopback WebSocket (`tests/test_synthetic_media_interruption.py`) verification of turn interruption, outbound ordering barriers, and stale frame suppression.
- **Observability & Privacy Verification:** `tests/test_voice_observability.py` verifies bounded latency windows, unavailable/None semantics, zero PII/audio leakage, sink failure isolation, and reproducible synthetic baseline latency measurements.
- **Runnable Smoke Scripts:** Executable via `python -m tools.voice_smoke` and `python -m tools.synthetic_media_smoke`.

---

## 8. Scope & Milestone Status

### Current Project Milestone Status:
- **M4.1:** Complete (Merged in PR #20 / `577e6b9`).
- **M4.2A:** Complete (Protected squash merged in PR #21 / `df28327`).
- **M4.2B.1:** Complete (Protected squash merged in PR #22 / `e457f8f`).
- **M4.2B.2:** Complete (Protected squash merged in PR #23 / `a3528ea`).
- **M4 External AI Integration:** Pending (`KaiwenAgentRealtimeAdapter` deferred while `kaiwen-agent` public realtime API is under active upstream development; OpenAI Live unverified).
- **M5.1A:** In progress (Provider-independent Voice Bridge observability foundation).

### Explicit Boundaries & Non-Goals:
- **No Real Verimor Integration:** Fictional synthetic test adapters do not constitute Verimor protocol compatibility (`OPEN-001`).
- **No OpenAI Live Integration:** Does not connect to live OpenAI Live API endpoints (`OPEN-003`).
- **No Audio Codecs:** Opus, G.711, AAC, resampling, or audio encoding algorithms are not implemented.
- **No End-to-End Latency Guarantee:** In-process sub-millisecond memory queue transfers do not demonstrate network latency or production performance.
- **No Production Deployment:** This library slice is strictly pre-alpha foundation code.

