# Voice Media Bridge, Turn Interruption & Audio Contracts (M4.1 / M4.2)

> **Status:** M4.1 Complete (PR #20 / `577e6b9`); M4.2A Complete (PR #21 / `df28327`); M4.2B.1 In progress (turn interruption & in-process playback control)
> **Document version:** 1.2
> **Last updated:** 2026-11-07
> **Scope:** Voice-local media domain contracts, provider-neutral media ports, bounded bidirectional bridge, turn identity, stale frame suppression, interruption capability protocol, and synthetic binary WebSocket transport.

---

## 1. Overview and Architecture Boundary

Kaiwen Gateway V1 architecture separates the **control plane** (call signaling, lifecycle, authentication, admission) from the **media plane** (high-frequency streaming audio frames).

In milestone M4.1, the voice media subsystem is introduced under `kaiwen_gateway.modules.voice`:
```text
src/kaiwen_gateway/modules/voice/
  ├── __init__.py           # Public exports (AudioFrame, VoiceBridgeSession, ports, adapters)
  ├── domain.py             # Pure immutable AudioFrame contract and domain error types
  ├── ports.py              # Provider-neutral async media port protocols
  ├── bridge.py             # Supervised bidirectional VoiceBridgeSession with bounded queues
  └── adapters/
      ├── __init__.py
      └── fake.py           # Deterministic fake adapters for telephony & AI media
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

## 6. Verification and Deterministic Testing

The test suite validates:
- **Byte Order & Integrity:** 100% identical byte matching between transmitted and received frames.
- **Two-Call Isolation:** Parallel calls (Call A and Call B) with distinct marker bytes show zero cross-call contamination; interrupting Call A has zero impact on Call B.
- **Cross-Tenant Isolation:** Sessions strictly fence tenant and call contexts.
- **Deterministic Fakes:** `InterruptibleFakeTelephonyAdapter` and `InterruptibleFakeAIPort` support delay simulation, fault injection, and EOF signaling without fragile sleep timing.
- **15 Required Interruption Scenarios:** All edge cases covered, including queue backpressure relief, duplicate/idempotent interrupts, race conditions with close, provider failure degradation, and task leak absence.
- **Runnable In-Process Smoke:** Executable via `python -m tools.voice_smoke` or `python tools/voice_smoke.py`.

---

## 7. Scope & Milestone Status

### Current Project Milestone Status:
- **M4.1:** Complete (Merged in PR #20 / `577e6b9`).
- **M4.2A:** Complete (Protected squash merged in PR #21 / `df28327`).
- **M4.2B.1:** In progress (Turn interruption contracts, in-process turn control, stale suppression).
- **M4.2B.2:** Not started (WebSocket wire event integration for interruption).
- **M5:** Not started (Resilience, fault simulation, soak evidence).

### Explicit Boundaries & Non-Goals:
- **No Real Verimor Integration:** Fictional synthetic test adapters do not constitute Verimor protocol compatibility (`OPEN-001`).
- **No OpenAI Live Integration:** Does not connect to live OpenAI Live API endpoints (`OPEN-003`).
- **No Audio Codecs:** Opus, G.711, AAC, resampling, or audio encoding algorithms are not implemented.
- **No End-to-End Latency Guarantee:** In-process sub-millisecond memory queue transfers do not demonstrate network latency or production performance.
- **No Production Deployment:** This library slice is strictly pre-alpha foundation code.

