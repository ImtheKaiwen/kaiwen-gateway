# Voice Media Bridge, Turn Interruption, Observability & Paced Concurrency Load Evaluation (M4.1 / M4.2 / M5.1A / M5.1B)

> **Status:** M4.1 Complete (PR #20 / `577e6b9`); M4.2A Complete (PR #21 / `df28327`); M4.2B.1 Complete (PR #22 / `e457f8f`); M4.2B.2 Complete (PR #23 / `a3528ea`); M5.1A Complete (PR #24 / `aca13cc`); M5.1B In progress (paced synthetic media concurrency & load evaluation)
> **Document version:** 1.5
> **Last updated:** 2026-11-08
> **Scope:** Voice-local media domain contracts, provider-neutral media ports, bounded bidirectional bridge, turn identity, stale frame suppression, interruption capability protocol, synthetic binary WebSocket transport, provider-independent in-memory telemetry, and real-time 20 ms paced synthetic media concurrency evaluation.

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

## 7. Paced Synthetic Media Concurrency & Overload Evaluation (M5.1B)

Milestone M5.1B (`tools/synthetic_media_load.py`) evaluates the **media plane** (`/ws/synthetic-media/v1`) under real-time 20 ms frame pacing at **1, 5, and 25 simultaneous calls**, reusing M5.1A's `InMemoryVoiceTelemetrySink`, `VoiceSessionMetricsSnapshot`, and `BoundedLatencyAggregator`.

### Why M5.1B Is Distinct from M3.3B (`tools/synthetic_soak.py`):
- **M3.3B (`tools/synthetic_soak.py`)** exercised the **control plane** (`/ws/synthetic/v1` JSON `call.start`, `ping`/`pong`, `call.end`), admission ceilings, and draining without streaming binary audio frames or instantiating `VoiceBridgeSession`.
- **M5.1B (`tools/synthetic_media_load.py`)** exercises the **media plane** (`/ws/synthetic-media/v1`) with continuous, barrier-synchronized 20 ms binary PCM16LE audio streaming (`640 B/frame` at 16 kHz mono = `50 frames/s` inbound + `50 frames/s` outbound per call), verifying frame byte integrity, FIFO ordering, bounded queue high-water marks, internal Gateway processing/queue-wait latencies, and resource cleanup across concurrent sessions.

### Monotonic 20 ms Pacing Algorithm & Missed-Deadline Policy:
- **Reference Schedule (`MonotonicFramePacer`):** Each call establishes a monotonic reference origin $t_0$ after all concurrent calls pass the `call.started` barrier. Frame slot $k \in \{0, \dots, N-1\}$ is assigned an immutable deadline:
  $$t_{\text{deadline}}(k) = t_0 + k \times 0.020\text{ s}$$
  The reference origin $t_0$ is **never** reset after a late wakeup, ensuring scheduling drift is measured rather than concealed.
- **Missed-Deadline Policy (`monotonic_fixed_schedule_rate_limited_catchup`):**
  1. A slot is recorded in `missed_deadlines` whenever actual wakeup/send time exceeds $t_{\text{deadline}}(k) + 20.0\text{ ms}$.
  2. If `--skip-threshold-ms` is configured and lateness reaches that threshold, the stale slot is skipped (`frames_skipped`) rather than sent late.
  3. When catching up after an OS timer or event-loop stall, consecutive sends enforce a minimum anti-burst spacing (`min_send_spacing_ms = 5.0 ms` default, i.e. $0.25 \times \text{interval}$) so the client never floods the WebSocket with zero-delay back-to-back bursts.
- **Deterministic 640-Byte Payloads:** Every 640-byte frame carries a 16-byte little-endian header (`magic=0x4B574D35`, `session_slot_id`, `frame_seq`, `blake2s_checksum`) followed by 624 deterministic PCM16LE body bytes (`"deterministic"`, `"sine"`, or `"ramp"`), enabling exact detection of missing, duplicate, out-of-order, corrupt, or cross-session frames.

### Measurement Definitions, Percentile Semantics & Heuristic Bounds:
| Metric | Definition & Boundary | Aggregation Method |
| --- | --- | --- |
| `frame_scheduling_interval_ms` / `scheduler_lateness_ms` / `scheduler_jitter_ms` | Client-side monotonic send cadence, wakeup lateness $\max(0, t_{\text{actual}} - t_{\text{deadline}})$, and $\|(\Delta t_{\text{send}}) - 20\text{ ms}\|$. | `pooled_per_frame_exact`: 100% sample retention across calls. Raw IEEE 754 64-bit float storage is ~48.8 KiB for 6,250 samples ($6,250 \times 8\text{ B}$ at S3); CPython object/list memory is ~200–300 KiB (finite, $\mathcal{O}(N_{\text{calls}} \times N_{\text{frames}})$). |
| `client_synthetic_echo_rtt_ms` | Client monotonic round-trip from `ws.send(frame)` to receiving matching 640-byte echoed frame on loopback. Includes client scheduling, loopback transport, Gateway queues, and AI echo. | `pooled_per_frame_exact` across all echoed frames, plus `max_session_p95_ms` across sessions. |
| `gateway_inbound_processing_ms` / `gateway_outbound_processing_ms` | Gateway internal per-frame receiver validation + forwarder dispatch + port `send_frame` await time from `VoiceSessionMetricsSnapshot`. | `across_session_percentiles`: Distribution of per-session p95s across active sessions (`session_p50_median_ms`, `session_p95_p95_ms`, `max_session_p95_ms`, `weighted_frame_mean_ms`). |
| `gateway_inbound_queue_wait_ms` / `gateway_outbound_queue_wait_ms` | Per-frame residence time inside `_inbound_queue` and `_outbound_queue` (capacity 8). | `across_session_percentiles` over per-session p95 metrics snapshots. |
| `asgi_websocket_send_bytes_ms` | Server `websocket.send_bytes(frame.payload)` await time (`network_transport_ms`). Local ASGI socket write only, not wire propagation or speaker playback. | `across_session_percentiles` over per-session p95 metrics snapshots. |
| `gateway_added_overhead_estimate_ms` | Descriptive heuristic summing session p95s of inbound queue wait, inbound processing, outbound queue wait, and outbound processing. Labeled `summary_method: "uncorrelated_component_percentile_sum_heuristic"`, `is_directly_measured_end_to_end_latency: False`, `nfr_perf_004_qualified: False`. | Sum of marginal component p95s (descriptive heuristic; NOT a guaranteed upper bound, does not measure end-to-end transit, does not qualify `NFR-PERF-004`). |
| `event_loop_lag_ms` | Periodic 10 ms monotonic sleep probe measuring asyncio event-loop wakeup delay. | `pooled_per_frame_exact` over loopback evaluation duration. |
| `python_heap_peak_kib` vs `process_rss_peak_kib` | `tracemalloc` Python allocator heap peak vs OS process Working Set / VmRSS peak (reported separately, never conflated). | Min/Max/Current/Peak tracked via platform stdlib (`psapi` on Windows, `/proc/self/status` on Linux). |
| `upstream_ai_generation_ms`, `external_telephony_network_ms`, `remote_playback_latency_ms` | Explicitly `available=False` and `None` in synthetic mode (`unknown_without_provider_instrumentation` / `unknown_without_playback_acknowledgment`). | Unavailable without provider instrumentation. |

### Overhead Heuristic Interpretation & Omitted Intervals:
Summing the 95th percentiles of internal Gateway components (`inbound_queue_wait_p95 + inbound_proc_p95 + outbound_queue_wait_p95 + outbound_proc_p95`) is an **uncalibrated descriptive heuristic**, NOT a valid mathematical upper bound. In general, $p95(\sum X_i) \le \sum p95(X_i)$ is mathematically false whenever upper-tail events are disjoint or negatively dependent (as proven in regression test `test_percentile_sum_inequality_counterexample_and_conjoint_latency_distinction`, where $p95(X_1 + X_2) = 100\text{ ms} > 10\text{ ms} = p95(X_1) + p95(X_2)$).

Furthermore, this component sum is NOT an empirical measurement of end-to-end frame transit latency, and omits multiple critical latency intervals partitioned by ownership:

**Category A: Gateway-Owned Latency Intervals (Unmeasured or Excluded from Component Sum):**
1. Telephony ingress adapter queue residence time (`WebSocketTelephonyMediaAdapter._inbound_queue` wait before bridge pops it).
2. Producer-side `queue.put()` backpressure waiting under queue saturation (`t_enqueued - t_pre_enqueue`).
3. Outbound transport send duration (`websocket.send_bytes` await duration, tracked separately as `network_transport_ms` but excluded from the 4-component sum).
4. Local ASGI server framework demuxing, routing, and task-switch latency.

**Category B: Genuinely External Latency Intervals (Outside Gateway Boundary):**
1. Telephony carrier / PSTN / SIP network propagation wire delay.
2. Client OS TCP network stack and kernel socket buffer queueing.
3. Upstream AI provider network round-trip, model inference, and audio synthesis.
4. Remote endpoint jitter buffering, audio hardware decoding, and physical acoustic playback.

Consequently, `nfr_perf_004_qualified` remains `False` until end-to-end hardware/provider instrumentation is integrated in M5.2.

### OS Timer Granularity & Single-Loop Loopback Limitations:
- Non-real-time OS schedulers (especially Windows with ~1–15.6 ms timer resolution) introduce wakeup jitter on `asyncio.sleep(0.020)`.
- In single-process loopback execution, the Uvicorn server, `VoiceBridgeSession` tasks, and all $N$ WebSocket client tasks share a single Python event loop thread. At $N=25$ (`1,250 frames/s` inbound + `1,250 frames/s` outbound = `2,500 WebSocket frames/s`), single-core CPU utilization reaches ~92–94%, inflating client-side loopback echo RTT (`p95 ~689.7 ms`) while Gateway internal per-session added overhead remains bounded (`p95 ~22.9 ms` $\le 50\text{ ms}$ target, max queue HWM `1`).

### Expected Rejection vs. Unexpected Failure:
- **Expected Rejections (`expected_rejections`):** Policy-enforced admission denials (`1008` / HTTP rejection) during capacity saturation (`max_concurrent_connections`, `max_connections_per_tenant`) or instance draining. Verified to allocate **zero** upstream AI media ports.
- **Unexpected Failures (`unexpected_failures`):** Unplanned disconnects, missing/corrupt/out-of-order frames, unhandled exceptions, or post-run resource leaks (`final_active_connections != 0` or `leaked_tasks != 0`).

### Reproduction Commands:
```bash
# Full S1=1, S2=5, S3=25 evaluation (5.0s per stage) + 8 GATE F fault/overload scenarios
uv run --locked --group dev --extra server python -m tools.synthetic_media_load --duration 5 --seed 42

# Single concurrency stage without fault suite, writing sanitized JSON report
uv run --locked --group dev --extra server python -m tools.synthetic_media_load --concurrency 5 --duration 5 --seed 42 --no-faults --output m5_s2.json
```

### Empirical Reference Summary (Windows 11, Python 3.12.13, 12 Logical CPUs, 5.0 s / 250 Frames per Call):
| Stage | Achieved Concurrency | Sent / Echoed | Loss / Dup / OOO / Mismatch | Gateway In / Out Proc p95 (ms) | Gateway In / Out Queue Wait p95 (ms) | Gateway Added Overhead p95 (ms) | Client Echo RTT p95 (ms) | Max Queue HWM (In / Out) | CPU (1-Core / All-Core) | Heap Peak / RSS Peak (KiB) | Cleanup (Conn / Calls / Tasks) |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| **S1 (`1`)** | `1 / 1` | `250 / 250` | `0 / 0 / 0 / 0` | `0.0699 / 0.0321` | `0.0527 / 0.0354` | `0.1901` | `1.8774` | `1 / 1` | `9.19% / 0.77%` | `2,072.57 / 41,192.0` | `0 / 0 / 0` |
| **S2 (`5`)** | `5 / 5` | `1,250 / 1,250` | `0 / 0 / 0 / 0` | `0.0612 / 0.0347` | `0.9168 / 1.6961` | `2.5664` | `10.0850` | `1 / 1` | `20.61% / 1.72%` | `3,091.31 / 44,104.0` | `0 / 0 / 0` |
| **S3 (`25`)** | `25 / 25` | `6,250 / 6,250` | `0 / 0 / 0 / 0` | `0.0782 / 0.0378` | `11.2876 / 12.0455` | `22.9426` | `689.6707` | `1 / 1` | `92.66% / 7.72%` | `15,255.28 / 66,536.0` | `0 / 0 / 0` |

All 8 GATE F fault and overload scenarios pass with `0` leaked connections, `0` leaked calls, `0` leaked Voice sessions, and `0` leaked asyncio tasks:
1. `premature_client_disconnect`: Client disconnects during streaming; bridge aborts cleanly without unhandled exceptions.
2. `blocked_ai_consumer_inbound_backpressure`: AI port blocks inbound consumption; `_inbound_queue` saturates (HWM=8), backpressure timeout fires, WebSocket closes with `1011`, and `BACKPRESSURE_OVERFLOW` error is recorded.
3. `blocked_outbound_send_backpressure`: Outbound ASGI send blocks; `_outbound_queue` saturates (HWM=8, 1 frame in flight + 8 buffered); synthetic AI port injects `MediaBackpressureOverflowError` verifying Gateway exception handling, WebSocket closure with `1011`, and clean drainage of 9 unforwarded frames (genuine `VoiceBridgeSession` internal queue timeout is independently verified via direct unit tests).
4. `normal_call_end_during_active_audio`: Client ends call cleanly with `call.end`; bridge flushes and closes with `1000`.
5. `admission_capacity_saturation`: Global connection limit rejects excess calls with `1008`; zero AI ports allocated for rejected calls.
6. `draining_with_active_media_calls`: Server initiates graceful drain; active calls complete streaming while new arrivals are rejected.
7. `recovery_after_interrupted_and_failed_calls`: After an interrupted call (Turn 1 $\to$ Turn 2 with audio recovery) and an invalid frame failure (`1003`), a subsequent call streams normally.
8. `two_tenants_concurrent_isolation`: Two tenants run concurrent media calls simultaneously with zero cross-tenant state or audio leakage.

---


### Process-Isolated Media Concurrency & Load Validation (M5.1B-R2 & R2.1)

To resolve whether the severe 25-call loopback echo RTT latency observed in the single-event-loop architecture was a Gateway bottleneck or client load-generation contention, M5.1B-R2 and R2.1 introduce **process-isolated concurrency testing** via `start_isolated_media_server_process` in `tools/synthetic_media_load.py`.

#### 1. Process Isolation Architecture & Hardening:
- **Server Process:** Dedicated child OS process running Uvicorn + ASGI Gateway app, `create_gateway_app`, `InMemoryVoiceTelemetrySink`, and an independent server `EventLoopLagMonitor`. Communication uses line-delimited JSON over `stdin`/`stdout`.
- **Client Runner Process:** Independent parent OS process running $N$ concurrent client sender/receiver tasks, `MonotonicFramePacer`, client `EventLoopLagMonitor`, and collecting RTT/lateness/jitter samples.
- **Shared Monotonic Scheduling Epoch (Finding 1):** A single shared epoch origin $t_0 = \text{time.perf\_counter()}$ is recorded immediately upon barrier release and passed to all $N$ client pacers. Every client session schedules its $k$-th frame against the deterministic deadline:
  $$\text{deadline}_k = t_0 + \frac{\text{phase\_offset\_ms}}{1000.0} + (k \times \text{interval\_s})$$
  Client dispatch skew (the delta between target deadline and actual send attempt of frame 0) is measured and reported separately as `client_dispatch_skew_ms`, never conflated with configured phase offsets.
- **Deterministic Asynchronous IPC Lifecycle (Finding 2):** Replaced synchronous thread-pool pipe reads with native `asyncio.create_subprocess_exec` and `asyncio.StreamReader` (with a 16 MiB line limit). Timeouts and cancellations terminate cleanly without abandoning blocked OS worker threads in `concurrent.futures.ThreadPoolExecutor`.
- **Atomic Kernel Ephemeral Port Binding (Finding 3):** Uvicorn binds with `port=0`, allowing the OS kernel to allocate an unused ephemeral port atomically (eliminating TOCTOU port race conditions). Both child and parent perform active bidirectional TCP socket connectivity probes (`socket.create_connection`) before WebSocket streaming begins.
- **Secret Key Environment Isolation (Finding 5):** Secrets are passed exclusively via the private environment variable `KW_SYNTHETIC_MEDIA_SECRET_KEY_HEX`, eliminating `--secret-key-hex` from the command-line argument vector. Secret material is stripped from the child stdout handshake JSON, and IPC schemas are strictly validated.
- **Subprocess Failure-Path Coverage (Finding 6 & B2):** Explicit deterministic mocked-subprocess regression tests in `tests/test_synthetic_media_load.py` verify startup failures, startup timeouts, child error events, malformed IPC schemas, shutdown timeouts with forced process termination (`proc.kill()`), and cancellation. Real-child post-start abnormal termination and >16 MiB stream overflow remain documented nonblocking runtime limitations.
- **Subprocess Stderr Handling (Finding B1):** The child process configures standard library logging at `WARNING` level to `sys.stderr`. The parent inspects and decodes `proc.stderr` on startup failure and process exit. Continuous asynchronous draining of `stderr` during active streaming is deliberately omitted to prevent background task leaks and keep test tooling focused.
- **Server Event-Loop Lag Sample Accounting (Finding B3):** The lag monitor probes event loop latency at a configured interval of 10 ms (`0.010 s`). On Windows, `BaseEventLoop._clock_resolution` defaults to 15.625 ms (`0.015625 s`), which permits scheduled timers to evaluate early when high-frequency IOCP network packets (1,250 frames/s across 25 calls) wake the event loop. The internal `BoundedLatencyAggregator` strictly enforces `window_capacity=256` to bound memory usage regardless of total probe iterations. A server lag p95 of 0.00 ms confirms the server event loop was unblocked, though tail p99 and max capture scheduling quantization.

#### 2. Phase-Aligned (Mode A) vs. Phase-Staggered (Mode B) Scheduling:
- **Mode A (Aligned, `--scheduling-mode aligned`):** All $N$ sessions schedule frame 0 at $t_0$ (`phase_offset_ms = 0.0`), creating simultaneous burst wakeups.
- **Mode B (Staggered, `--scheduling-mode staggered`):** Sessions are evenly staggered across the 20 ms frame interval ($\text{offset} = \text{slot\_id} \times \frac{20.0}{N}\text{ ms}$), distributing send wakeups evenly across time.

#### 3. Empirical Reference Matrix (Windows 11, Python 3.12.13, 12 Logical CPUs, 20 ms / 640-byte Frames):
| Stage & Mode | Isolation | Concurrency / Duration | Sent / Echoed | Missing / Dup / OOO | Client Echo RTT p95 (ms) | Client Dispatch Skew p95 (ms) | Gateway Overhead Est. p95 Sum (ms) | Server Event Loop Lag p95 (ms) | Client Event Loop Lag p95 (ms) | CPU Time (Client / Server) |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| **S1 (1 call)** | Isolated (Aligned) | 1 / 5.0 s | 250 / 250 | 0 / 0 / 0 | **1.94** | 0.25 | 0.10 | **0.00** | 5.85 | 0.25 s / 0.14 s |
| **S2 (5 calls)** | Isolated (Aligned) | 5 / 5.0 s | 1,250 / 1,250 | 0 / 0 / 0 | **7.31** | 4.31 | 0.32 | **0.00** | 15.06 | 1.25 s / 0.84 s |
| **S3 Baseline** | In-Process (Single Loop) | 25 / 5.0 s | 6,250 / 6,250 | 0 / 0 / 0 | **2,582.27** | 6.10 | 22.10 | 25.08 | 25.08 | 6.12 s (shared single loop) |
| **S3 Run 1 (Mode A)** | Isolated (Aligned) | 25 / 5.0 s | 6,250 / 6,250 | 0 / 0 / 0 | **28.15** | 11.33 | 1.00 | **0.00** | 21.32 | 3.20 s / 1.25 s |
| **S3 Run 2 (Mode A)** | Isolated (Aligned) | 25 / 5.0 s | 6,250 / 6,250 | 0 / 0 / 0 | **23.19** | 11.99 | 0.85 | **0.00** | 18.45 | 2.85 s / 1.40 s |
| **S3 Run 3 (Mode A)** | Isolated (Aligned) | 25 / 5.0 s | 6,250 / 6,250 | 0 / 0 / 0 | **26.73** | 9.91 | 0.99 | **0.00** | 19.80 | 2.95 s / 1.20 s |
| **S3 Run 1 (Mode B)** | Isolated (Staggered) | 25 / 5.0 s | 6,250 / 6,250 | 0 / 0 / 0 | **26.39** | 3.82 | 0.99 | **0.00** | 16.50 | 3.12 s / 1.10 s |
| **S3 Run 2 (Mode B)** | Isolated (Staggered) | 25 / 5.0 s | 6,250 / 6,250 | 0 / 0 / 0 | **28.71** | 5.92 | 1.00 | **0.00** | 17.20 | 3.10 s / 1.05 s |
| **S3 Run 3 (Mode B)** | Isolated (Staggered) | 25 / 5.0 s | 6,250 / 6,250 | 0 / 0 / 0 | **26.51** | 4.70 | 0.94 | **0.00** | 16.90 | 3.00 s / 1.15 s |
| **S3 Sustained (15s)** | Isolated (Staggered) | 25 / 15.0 s | 18,750 / 18,750 | 0 / 0 / 0 | **25.01** | 4.50 | 0.95 | **0.00** | 17.52 | 8.75 s / 3.10 s |
| **GATE F Faults (8)** | In-Process / Isolated | Varied | Varied | 0 / 0 / 0 | N/A | N/A | N/A | N/A | N/A | All 8 scenarios PASS |

#### 4. Engineering Takeaways & Objective Boundary Qualifications:
1. **The single-loop latency spike was an artifact of client load generator resource starvation:** When running 25 client loops and the Uvicorn Gateway on the same event loop thread, the single thread saturated, inflating loopback echo RTT to ~2,582 ms. Under process isolation, client echo RTT collapsed to **23.2–28.7 ms p95 (>90x reduction)**, while Gateway internal processing overhead remained below **1.0 ms** and server event-loop lag stayed at **0.0 ms**.
2. **Phase alignment vs. staggering:** Mode B (staggered) exhibits lower client dispatch skew (p95 ~4–6 ms vs ~10–12 ms in Mode A) by smoothing out barrier wakeups, though both modes achieve comparable ~26 ms loopback echo RTT.
3. **15-Second Sustained Robustness:** In a 15-second run with 18,750 frames across 25 concurrent calls, zero frames were lost, skipped, or duplicated, server event-loop lag remained 0.0 ms, and all 25 calls cleaned up with zero resource leaks.
4. **Boundary & Non-Certification Qualification:** While Gateway internal added overhead remains small ($< 1.0\text{ ms}$ p95), loopback evaluation on localhost measures only in-memory and OS socket loopback transfer. It does not measure external telephony carrier jitter, SIP trunking propagation delay, physical acoustics, or upstream AI inference and audio generation time. It cannot by itself certify compliance with end-to-end NFR-PERF-004 in live production environments.

## 8. Verification and Deterministic Testing

The test suite validates:
- **Byte Order & Integrity:** 100% identical byte matching between transmitted and received frames.
- **Two-Call Isolation:** Parallel calls (Call A and Call B) with distinct marker bytes show zero cross-call contamination; interrupting Call A has zero impact on Call B.
- **Cross-Tenant Isolation:** Sessions strictly fence tenant and call contexts.
- **Deterministic Fakes:** `InterruptibleFakeTelephonyAdapter`, `BoundedInterruptibleFakeAIEchoPort`, and `InterruptibleFakeAIPort` support delay simulation, fault injection, and EOF signaling without fragile sleep timing.
- **Interruption & WebSocket Conformance:** In-process (`tests/test_voice_interruption.py`) and real loopback WebSocket (`tests/test_synthetic_media_interruption.py`) verification of turn interruption, outbound ordering barriers, and stale frame suppression.
- **Observability & Privacy Verification:** `tests/test_voice_observability.py` verifies bounded latency windows, unavailable/None semantics, zero PII/audio leakage, sink failure isolation, and reproducible synthetic baseline latency measurements.
- **Paced Media Concurrency & Fault Verification:** `tests/test_synthetic_media_load.py` verifies 20 ms monotonic pacing, drift and missed-deadline handling, deterministic 640-byte frame verification, echo anomaly detection, admission saturation, draining, and privacy-safe report serialization.
- **Runnable Smoke & Evaluation Scripts:** Executable via `python -m tools.voice_smoke`, `python -m tools.synthetic_media_smoke`, and `python -m tools.synthetic_media_load`.

---

## 9. Scope & Milestone Status

### Current Project Milestone Status:
- **M4.1:** Complete (Merged in PR #20 / `577e6b9`).
- **M4.2A:** Complete (Protected squash merged in PR #21 / `df28327`).
- **M4.2B.1:** Complete (Protected squash merged in PR #22 / `e457f8f`).
- **M4.2B.2:** Complete (Protected squash merged in PR #23 / `a3528ea`).
- **M4 External AI Integration:** Pending (`KaiwenAgentRealtimeAdapter` deferred while `kaiwen-agent` public realtime API is under active upstream development; OpenAI Live unverified).
- **M5.1A:** Complete (Protected squash merged in PR #24 / `aca13cc`).
- **M5.1B:** In progress (Paced synthetic media concurrency and overload evaluation at 1, 5, and 25 simultaneous calls).

### Explicit Boundaries & Non-Goals:
- **No Real Verimor Integration:** Fictional synthetic test adapters do not constitute Verimor protocol compatibility (`OPEN-001`).
- **No OpenAI Live Integration:** Does not connect to live OpenAI Live API endpoints (`OPEN-003`).
- **No Audio Codecs:** Opus, G.711, AAC, resampling, or audio encoding algorithms are not implemented.
- **No End-to-End Latency Guarantee:** In-process sub-millisecond memory queue transfers do not demonstrate network latency or production performance.
- **No Production Deployment:** This library slice is strictly pre-alpha foundation code.

