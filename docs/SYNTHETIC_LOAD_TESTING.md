# Synthetic Load, Soak & Fault Testing Specification (M3.3B)

> **Status:** Reference documentation for synthetic load, soak, and fault simulation (M3.3B)<br>
> **Tool:** `tools/synthetic_soak.py`<br>
> **Target Release:** `0.1.0a1`<br>
> **Requirement IDs:** `NFR-PERF-001`, `NFR-PERF-002`, `NFR-PERF-003`, `NFR-PERF-004`, `AT-003`, `AT-005`<br>
> **Important:** Measurements obtained with this tool reflect **local control-event round-trip times (RTT) over loopback WebSockets**. They do **not** measure voice media frame transport or audio transcoding latency, and must **never** be cited as satisfying the `NFR-PERF-004` 50 ms voice media target (which applies to the future M4 audio bridge).

---

## 1. Scope & Design Philosophy

`tools/synthetic_soak.py` is a standalone, opt-in developer tool designed to exercise Kaiwen Gateway's synthetic telephony control plane under staged concurrency, capacity saturation, protocol fault injection, and sustained or repeated soak cycles.

### 1.1 Architectural Isolation & Zero Runtime Dependencies

- **Pure Developer Tool:** `tools/synthetic_soak.py` lives outside the published package root (`src/kaiwen_gateway/`) and is explicitly excluded from wheel and sdist distributions via `pyproject.toml`.
- **Base Package Integrity:** The base library (`dependencies = []`) remains strictly zero-dependency. The runner requires the optional `server` and `dev` extras (`websockets`, `starlette`, `uvicorn`, `pytest`).
- **Ephemeral In-Process Server:** Each scenario stage starts an isolated loopback ASGI server bound to an atomic OS-assigned port (`port=0`), avoiding port conflicts, test pollution, or shared global state.
- **Resource Lease Verification:** Before and after each stage, the runner snapshots the server's `AdmissionController` and `SyntheticAuthBridge` to verify zero connection leaks (`active_connections == 0`, `available_capacity == max_concurrent_connections`, `active_calls == 0`, `managed == 0`).

---

## 2. CLI Options and Configuration

The runner provides a flexible CLI with strict parameter validation:

```bash
uv run --group dev --extra server python tools/synthetic_soak.py [OPTIONS]
```

### 2.1 Available CLI Parameters

| Option | Type | Default | Description |
| --- | --- | --- | --- |
| `--concurrency` | `int` (`1`, `5`, `25`) | `1` | Target concurrent active call volume to exercise. |
| `--scenario` | `str` (`concurrency`, `saturation`, `fault`, `soak`, `sustained`, `all`) | `concurrency` | Test scenario or suite to execute. |
| `--duration` | `float` | `10.0` | Maximum duration in seconds for the run or individual stage (must be positive and $\le$ 3600s). |
| `--iterations` | `int` | `1` | Number of repeated cycles for soak mode (1 to 10000). |
| `--port` | `int` | `0` | Loopback port to bind (default `0` for atomic OS socket allocation). |
| `--output` | `str` | `None` | Path to export machine-readable JSON report. |
| `--seed` | `int` | `None` | Integer seed for timing and jitter determinism. |
| `--log-level` | `str` | `warning` | Uvicorn server log level (`critical`, `error`, `warning`, `info`, `debug`). |

### 2.2 Workload Duration Semantics

To prevent ambiguity between time-bounded and iteration-bounded runs, the runner enforces strict semantic definitions:

1. **Configured Maximum Duration (`--duration`):** The configured wall-clock time limit for the run or individual stage.
2. **Actual Elapsed Duration (`duration_seconds`):** The measured wall-clock time taken by the stage from start to complete teardown.
3. **Completed Iterations (`completed_iterations`):** In repeated soak mode, the stage stops as soon as `--iterations` cycles complete OR elapsed time reaches `--duration`, whichever happens first.
4. **Sustained Holding Duration (`sustained_holding_seconds`):** In sustained soak mode (`--scenario sustained`), all connections are held actively concurrent for the target duration (e.g. 60.0s), with periodic keepalive pings and periodic active connection sampling.
5. **Active Concurrency Sampling:** During sustained soak, active connections are sampled periodically (e.g. every 1.0s). The report records `min_active_connections` and `max_active_connections`. A result of `min_active == max_active == target` confirms that active connections remained at target capacity at all sampled inspection intervals during the holding window. Note that discrete sampling provides evidence at sample instants rather than continuous mathematical guarantees for unobserved microseconds.

---

## 3. Test Scenarios and Methodology

### 3.1 Concurrency Stages (1, 5, 25 Concurrent Calls)

Concurrency testing exercises Gateway's ability to handle simultaneous active synthetic calls without cross-tenant context leaks or race conditions.

- **Stage A (1 Call):** Baseline single-call latency profile and verification of end-to-end synthetic protocol lifecycle.
- **Stage B (5 Concurrent Calls):** Moderate concurrency across multiple tenants.
- **Stage C (25 Concurrent Calls):** Maximum supported single-instance concurrency distributed across three tenants:
  - Tenant A: 10 calls (at tenant capacity limit)
  - Tenant B: 10 calls (at tenant capacity limit)
  - Tenant C: 5 calls (remaining global capacity)

#### Active Overlap Synchronization (`asyncio.Barrier`)

Simply launching 25 connections asynchronously does not guarantee concurrent active overlap. To verify true concurrency:
1. Each client connection completes the handshake (`session.ready`) and starts the call (`call.start` $\rightarrow$ `call.started` in state `connected`).
2. All 25 client coroutines wait at an `asyncio.Barrier(26)` (25 clients + 1 coordinator inspector).
3. If any client encounters an error before reaching the barrier, it invokes `await barrier.abort()`, cleanly breaking the barrier and waking up waiting tasks without deadlocks or hangs.
4. The coordinator inspects the live `AdmissionController` and `SyntheticAuthBridge` state:
   - `snapshot().active_connections == 25`
   - `len(bridge._active_calls) == 25`
   - Tenant allocations: `tenant_a == 10`, `tenant_b == 10`, `tenant_c == 5`.
5. Only after verified active overlap does the coordinator release the barrier to execute ping/pong keepalives and teardown.

---

### 3.2 Capacity Saturation Testing

Validates that Gateway enforces both per-tenant and global admission quotas fail-closed during the HTTP WebSocket upgrade handshake:

1. **Per-Tenant Saturation:**
   - 10 connections for `tenant_alpha` are established and held active.
   - An 11th connection attempt for `tenant_alpha` is initiated.
   - **Expected reaction:** Handshake is rejected with **HTTP 403 Forbidden** prior to WebSocket upgrade acceptance. Active connections remain undisturbed.
   - A subsequent connection for `tenant_beta` is attempted and successfully admitted (since global capacity 10/25 has room).
2. **Global Saturation:**
   - All 25 global connections are filled across three tenants (10 on A, 10 on B, 5 on C).
   - A 26th connection attempt for `tenant_c` (which only has 5/10 tenant usage) is initiated.
   - **Expected reaction:** Handshake is rejected with **HTTP 403 Forbidden** due to global capacity exhaustion (`active_connections == max_concurrent_connections`).
3. **Full Teardown:**
   - All held connections complete clean disconnection (`call.end` $\rightarrow$ `session.end` $\rightarrow$ code 1000).
   - Zero-leak verification ensures `available_capacity == 25`, `active_connections == 0`, and `active_calls == 0`.

---

### 3.3 Fault Injection Matrix

The fault injection stage executes 8 distinct protocol, security, and lifecycle fault classes, distinguishing clean client closes from genuine transport drops:

| Fault ID | Scenario Description | Injection Method | Expected Reaction | Verified Code / Status |
| --- | --- | --- | --- | --- |
| **FLT-01** | Forged Token Signature | Corrupted signature suffix | Handshake rejected before accept | HTTP 403 Forbidden |
| **FLT-02** | Replayed Token | Reused token ID with active replay cache | First use succeeds; second use rejected | HTTP 403 Forbidden |
| **FLT-03** | Missing Required Scope | Token has `voice.read` instead of `voice.stream` | Scope check fails before accept | HTTP 403 Forbidden |
| **FLT-04** | Malformed JSON Payload | Unparseable syntax `not-valid-json-{{{` | Protocol parser rejects invalid payload | Close code `1007` (Invalid Frame) |
| **FLT-05** | Client-Initiated WebSocket Close | Client closes socket mid-call with code 1001 | Supervisor detects close and releases lease | Lease released; active count decremented |
| **FLT-06** | Genuine Transport Abort | TCP transport socket aborted without close handshake | Server catches EOF/1006; lease released; concurrent tenant call unaffected | Lease released; zero leak; cross-tenant isolated |
| **FLT-07** | Duplicate `call.start` | Second `call.start` sent on active session | Telephony adapter detects invalid state transition | Close code `1008` (Policy Violation) |
| **FLT-08** | Server Instance Draining | Coordinator begins drain while call is active | New connections rejected during draining; active call finishes | Handshake rejected / HTTP 403; zero leaks |

---

### 3.4 Sustained Soak Testing (Opt-In 60-Second Run)

Designed for long-duration stability and resource endurance:
- Exercises 25 simultaneous calls distributed across 3 tenants (10 + 10 + 5).
- Connections are held actively concurrent for the full target duration (e.g. 60.0 seconds).
- Active connections are sampled every 1.0 second during the sustained window.
- Periodic keepalive pings are exchanged on all connections every 5.0 seconds.
- At expiration, all calls execute orderly teardown and complete clean closure.
- Resource verification confirms zero lingering leases or memory leaks.

---

## 4. Measurement Methodology and Metrics

### 4.1 Counters and Metric Accounting

To maintain absolute accounting integrity, all result metrics are rigorously distinguished:
- `connection_attempts`: Total connection attempts initiated.
- `successful_admissions`: Connections that completed the WebSocket handshake and were admitted.
- `expected_rejections`: Connections rejected as expected under protocol security rules (e.g. HTTP 403 for forged/replayed/overflow tokens). **Never counted as successful calls.**
- `unexpected_failures`: Any unplanned connection drop or unhandled error.
- `completed_calls`: Calls that completed orderly start-ping-end lifecycle (`call.start` through `call.ended`). Connections that perform only session handshake and termination without starting a telephone call are counted as admissions, not completed calls.
- `peak_active_connections`: Maximum active concurrent connections observed directly from `AdmissionController` capacity snapshots during holding/barrier phases.
- `final_active_connections`: Active connections remaining at stage end (must be 0).
- `active_leases_after_cleanup`: Remaining managed connection leases in coordinator (must be 0).

### 4.2 Latency Percentile Computation

For each measured stage, the runner computes:
- Minimum (`min_ms`)
- 50th percentile / Median (`p50_ms`)
- 95th percentile (`p95_ms`)
- 99th percentile (`p99_ms`)
- Maximum (`max_ms`)
- Arithmetic Mean (`mean_ms`)

#### Small Sample Indicator (`*`)
When the sample count $N < 20$, higher percentiles (p95, p99) cannot be statistically estimated with high confidence. The console renderer displays an asterisk (`*`) and an explanatory footnote:
```text
Stage              Metric          Count  p50      p95      p99      Max      Mean
--------------------------------------------------------------------------------
concurrency_1      handshake       1      31.96    31.96   * 31.96   * 31.96    31.96
```

### 4.3 Resource Tracking

- **Heap Memory Peak:** Measured via Python's standard `tracemalloc` module (`tracemalloc.get_traced_memory()`). This measures Python heap allocation overhead rather than operating system Resident Set Size (RSS).
- **Process CPU Time:** Measured via `time.process_time()`, tracking combined user and kernel CPU execution seconds.
- **Events Per Second:** Calculated as $\frac{\text{observed\_protocol\_events}}{\text{duration\_seconds}}$, derived from actual protocol frames (handshake, call control, ping/pong, session termination) counted directly on client transports rather than fixed estimates.

---

## 5. Reference Benchmark Results

### 5.1 60-Second Sustained Soak Run (25 Concurrent Calls)

Executed on Windows 11, Python 3.12.13 (Intel Core i7-12700H reference platform):

```text
================================================================================
  KAIWEN GATEWAY — SYNTHETIC LOAD & SOAK TEST REPORT (M3.3B)
================================================================================
Runner Version    : 0.1.0
Gateway Version   : 0.1.0a1 (commit: d347eaed36)
Environment       : Python 3.12.13 on Windows-11-10.0.26200-SP0
Processor / CPU   : Intel64 Family 6 Model 154 Stepping 3, GenuineIntel
Workload Config   : {'scenario': 'sustained', 'concurrency': 25, 'duration_seconds': 60.0, 'iterations': 1, 'port': 0}
Wall Duration     : 60.615 s
CPU Process Time  : 0.578 s
Heap Peak Memory  : 8740.1 KiB (tracemalloc)
Overall Result    : PASS
================================================================================

STAGES SUMMARY:
--------------------------------------------------------------------------------
Scenario               Req  Peak  Attempts  Admitted  Rej   Calls  Status  Duration
--------------------------------------------------------------------------------
sustained_soak_25      25   25    25        25        0     25     PASS    60.214  s
   [Sustained hold: 60.01s | Min sampled: 25 | Max sampled: 25]
--------------------------------------------------------------------------------

LATENCY PERCENTILES (milliseconds):
--------------------------------------------------------------------------------
Stage              Metric          Count  p50      p95      p99      Max      Mean
--------------------------------------------------------------------------------
sustained_soak_25  handshake       25     86.37    118.38   118.50   118.52   86.76
sustained_soak_25  call_start_rtt  25     17.88    19.90    20.02    20.06    15.46
sustained_soak_25  ping_rtt        25     8.89     9.15     9.19     9.20     8.88
sustained_soak_25  call_end_rtt    25     10.55    11.23    11.27    11.28    10.54
sustained_soak_25  cleanup         25     20.02    20.36    20.38    20.39    20.03
sustained_soak_25  total_duration  25     60171.35 60177.03 60202.01 60209.88 60172.30
--------------------------------------------------------------------------------
  (*) Note: Sample size < 20; p95/p99 values are indicative only.

RESOURCE RECOVERY:
--------------------------------------------------------------------------------
[sustained_soak_25] Active Connections: 0 | Available Capacity: 25 / 25 | Active Tenants: 0
--------------------------------------------------------------------------------
```

### 5.2 Fast Suite (Burst Concurrency, Saturation, Faults, Repeated Soak)

```text
================================================================================
  KAIWEN GATEWAY — SYNTHETIC LOAD & SOAK TEST REPORT (M3.3B)
================================================================================
Runner Version    : 0.1.0
Gateway Version   : 0.1.0a1 (commit: d347eaed36)
Environment       : Python 3.12.13 on Windows-11-10.0.26200-SP0
Processor / CPU   : Intel64 Family 6 Model 154 Stepping 3, GenuineIntel
Workload Config   : {'scenario': 'all', 'concurrency': 1, 'duration_seconds': 10.0, 'iterations': 1, 'port': 0}
Wall Duration     : 2.174 s
CPU Process Time  : 0.641 s
Heap Peak Memory  : 14560.8 KiB (tracemalloc)
Overall Result    : PASS
================================================================================

STAGES SUMMARY:
--------------------------------------------------------------------------------
Scenario               Req  Peak  Attempts  Admitted  Rej   Calls  Status  Duration
--------------------------------------------------------------------------------
concurrency_1          1    1     1         1         0     1      PASS    0.036   s
concurrency_5          5    5     5         5         0     5      PASS    0.038   s
concurrency_25         25   25    25        25        0     25     PASS    0.169   s
capacity_saturation    25   25    38        36        2     35     PASS    0.229   s
fault_injection        1    2     11        7         4     3      PASS    0.316   s
repeated_soak_1        1    1     3         3         0     3      PASS    0.021   s
--------------------------------------------------------------------------------

LATENCY PERCENTILES (milliseconds):
--------------------------------------------------------------------------------
Stage              Metric          Count  p50      p95      p99      Max      Mean
--------------------------------------------------------------------------------
concurrency_1      handshake       1      31.96    31.96   * 31.96   * 31.96    31.96
concurrency_1      call_start_rtt  1      0.71     0.71    * 0.71    * 0.71     0.71
concurrency_1      ping_rtt        1      0.59     0.59    * 0.59    * 0.59     0.59
concurrency_1      call_end_rtt    1      0.50     0.50    * 0.50    * 0.50     0.50
concurrency_1      cleanup         1      1.41     1.41    * 1.41    * 1.41     1.41
concurrency_1      total_duration  1      35.31    35.31   * 35.31   * 35.31    35.31
concurrency_5      handshake       5      19.70    20.30   * 20.34   * 20.34    19.54
concurrency_5      call_start_rtt  5      3.78     4.75    * 4.80    * 4.81     3.41
concurrency_5      ping_rtt        5      2.41     2.46    * 2.47    * 2.47     2.38
concurrency_5      call_end_rtt    5      3.25     3.27    * 3.27    * 3.27     3.22
concurrency_5      cleanup         5      6.35     6.40    * 6.40    * 6.41     6.33
concurrency_5      total_duration  5      35.37    37.47   * 37.65   * 37.70    35.65
concurrency_25     handshake       25     71.76    98.72    98.98    99.03    71.16
concurrency_25     call_start_rtt  25     15.13    16.57    16.63    16.65    12.90
concurrency_25     ping_rtt        25     12.43    12.84    12.88    12.89    12.07
concurrency_25     call_end_rtt    25     12.54    13.05    13.42    13.53    12.65
concurrency_25     cleanup         25     24.74    25.20    25.38    25.43    24.66
concurrency_25     total_duration  25     158.24   164.91   165.62   165.83   158.62
--------------------------------------------------------------------------------
  (*) Note: Sample size < 20; p95/p99 values are indicative only.

RESOURCE RECOVERY:
--------------------------------------------------------------------------------
[concurrency_1] Active Connections: 0 | Available Capacity: 25 / 25 | Active Tenants: 0
[concurrency_5] Active Connections: 0 | Available Capacity: 25 / 25 | Active Tenants: 0
[concurrency_25] Active Connections: 0 | Available Capacity: 25 / 25 | Active Tenants: 0
[capacity_saturation] Active Connections: 0 | Available Capacity: 25 / 25 | Active Tenants: 0
[fault_injection] Active Connections: 0 | Available Capacity: 25 / 25 | Active Tenants: 0
[repeated_soak_1] Active Connections: 0 | Available Capacity: 25 / 25 | Active Tenants: 0
--------------------------------------------------------------------------------
```

---

## 6. How to Reproduce

### 6.1 Fast Suite Scenarios

```bash
# Baseline 1 call
uv run --group dev --extra server python tools/synthetic_soak.py --concurrency 1

# Moderate 5 calls
uv run --group dev --extra server python tools/synthetic_soak.py --concurrency 5

# Max single-instance 25 calls (burst)
uv run --group dev --extra server python tools/synthetic_soak.py --concurrency 25

# Capacity saturation (global 25 and tenant 10 rejections)
uv run --group dev --extra server python tools/synthetic_soak.py --scenario saturation

# Fault injection (8 fault classes)
uv run --group dev --extra server python tools/synthetic_soak.py --scenario fault

# Complete fast suite with machine-readable JSON export
uv run --group dev --extra server python tools/synthetic_soak.py --scenario all --output soak_report.json
```

### 6.2 Sustained 60-Second Soak Run (Manual / Opt-In)

```bash
uv run --group dev --extra server python tools/synthetic_soak.py --scenario sustained --duration 60 --concurrency 25
```
