# Kaiwen Gateway

**A modular, provider-agnostic communication gateway for real-time AI applications.**

> [!IMPORTANT]
> **Project Status: Synthetic Alpha Release Candidate (`v0.1.0a1`).**
> This repository provides an installable, verified, synthetic-environment alpha release candidate. It enables developers to test, benchmark, and integrate against the Gateway using fully reproducible in-process and loopback synthetic environments without requiring external credentials or proprietary telephony hardware.
>
> **Production use is not yet supported.** Live Verimor PSTN/WSS interoperability and external AI provider adapters (e.g. OpenAI Realtime / Gemini Live) are **not** included in this release candidate and remain subject to official vendor specifications and contract validation.

Kaiwen Gateway is designed to be both a **reusable Python library** and a **deployable ASGI service**. It accepts connections from external systems, authenticates and isolates them, translates provider-specific protocols into well-defined internal contracts, and bridges live communication to application and AI backends.

The first implementation focuses on **bidirectional telephone audio over secure WebSockets (WSS)**. The core is decoupled from telephony so future communication modules can be introduced without rewriting the platform.

---

## Quickstart

### 1. Installation

Kaiwen Gateway is designed with **zero mandatory runtime dependencies** for its core contracts. The optional `[server]` extra provides the ASGI server and WebSocket runtime.

```bash
# Install the core library and ASGI server runtime
pip install "kaiwen-gateway[server]"

# Or using uv:
uv pip install "kaiwen-gateway[server]"
```

To install the minimal core library only (contracts, admission control, identity, lifecycle supervision):

```bash
pip install kaiwen-gateway
```

### 2. Verify Installation & Configuration

The CLI runs without external dependencies:

```bash
# Check version
kaiwen-gateway --version

# Verify configuration and environment variables
kaiwen-gateway check-config
```

### 3. Run Executable Demos

The Gateway includes self-contained, automated loopback demos that allocate an ephemeral port, verify token authentication, execute a complete call lifecycle, and confirm zero capacity leakage.

```bash
# Control plane synthetic telephony call demo:
kaiwen-gateway demo

# Full media plane demo (streams 25 binary PCM16LE audio frames, verifies echo, exercises barge-in interruption, and verifies zero leak):
kaiwen-gateway demo --media
```

### 4. Developer Quickstart Script

See [`examples/quickstart_synthetic_media.py`](examples/quickstart_synthetic_media.py) for a complete, runnable script demonstrating how to:
1. Initialize Gateway Core with admission control and lifecycle supervision.
2. Spin up an ephemeral Uvicorn server with `/ws/synthetic-media/v1`.
3. Mint an HMAC-SHA256 authenticated bearer token.
4. Stream binary PCM16LE frames, receive echo audio, trigger barge-in interruption, and cleanly terminate.

```bash
uv run python examples/quickstart_synthetic_media.py
```

---

## Capability Matrix (v0.1.0a1)

The following matrix distinguishes verified capabilities in this synthetic alpha from deferred or unsupported features:

| Capability | Status in v0.1.0a1 | Notes / Limitations |
| --- | --- | --- |
| **Gateway Core Architecture** | **Supported (Verified)** | Framework-independent modular core (`compose_gateway_core`), typed module registry, immutable configuration (`CoreConfig`). |
| **Identity & Scoped Authorization** | **Supported (Verified)** | Strong identity tokens (`ProviderPrincipal`, `TenantCallBinding`), HMAC-SHA256 validation, scope checking (`voice.stream`, `voice.admin`), control-character rejection. |
| **Admission Control** | **Supported (Verified)** | Process-local atomic capacity reservations against global and per-tenant ceilings, duplicate connection prevention, idempotent lease release. |
| **Supervised Connection Lifecycle** | **Supported (Verified)** | Strict state machine (`NEW` → `ADMITTED` → `ACTIVE` → `CLOSED`/`FAILED`), cancellation shielding, timeout enforcement, zero orphan tasks. |
| **Graceful Instance Draining** | **Supported (Verified)** | Multi-phase draining coordinator (`InstanceDrainCoordinator`), admission rejection during drain, bounded grace period, cooperative termination. |
| **ASGI Server & Health Endpoints** | **Supported (Verified)** | Starlette/Uvicorn ASGI host, `GET /health` (liveness), `GET /ready` (readiness with drain detection). Restricted to loopback (`127.0.0.1`) by default. |
| **Synthetic Telephony Transport** | **Supported (Verified)** | `/ws/synthetic-telephony/v1` authenticated WSS endpoint, structured JSON control events (`call.start`, `call.started`, `call.end`, `call.ended`, ping/pong). |
| **Synthetic Media Transport** | **Supported (Verified)** | `/ws/synthetic-media/v1` binary WebSocket transport, 16kHz 16-bit linear PCM mono framing (20ms frames), single-read demux, control event interleaving. |
| **Voice Media Bridge** | **Supported (Verified)** | Asynchronous bidirectional media bridge (`VoiceBridgeSession`), isolated inbound/outbound audio pumps, turn tracking, backpressure bounds. |
| **Barge-in Interruption** | **Supported (Verified)** | User speech interruption triggers downstream AI cancellation, outbound buffer flushing, and synchronized turn incrementing. |
| **Fake AI Echo Adapter** | **Supported (Verified)** | In-process `BoundedFakeAIEchoPort` for deterministic loopback audio testing and benchmarking. |
| **Privacy & Zero Audio Persistence** | **Supported (Verified)** | **Zero raw audio or transcript persistence by default.** Sensitive identifiers redacted from logs and error summaries. |
| **Live Verimor Telephony** | **Unsupported (Deferred)** | `FR-INT-007` / `OPEN-001`. Blocked pending official vendor WebSocket specification. Wire format is not guessed. |
| **External Realtime AI Backends** | **Unsupported (Deferred)** | `FR-INT-003` / `OPEN-003`. External adapters for OpenAI Realtime or Gemini Live are deferred; loopback testing uses fake echo adapter. |
| **Distributed / Multi-Instance State** | **Unsupported (Deferred)** | Atomic replay store and distributed multi-worker coordination deferred to Gate C/D (M8). Current alpha is single-instance only. |
| **Low-Latency Voice at $\ge 100$ Calls** | **Documented Limitation** | 100 concurrent calls achieve 100% media integrity (0% loss) and pass pacing criteria, but median p95 echo RTT is ~2,045 ms due to single-threaded event loop queuing. See Performance section. |

---

## CLI Reference

`kaiwen-gateway` provides an `argparse`-driven command-line interface:

| Command | Arguments | Dependencies | Description |
| --- | --- | --- | --- |
| `kaiwen-gateway --version` | None | Python stdlib only | Displays package and contract version. |
| `kaiwen-gateway --help` | None | Python stdlib only | Displays CLI usage instructions and available subcommands. |
| `kaiwen-gateway check-config` | None | Python stdlib only | Validates `CoreConfig` and environment variables without starting a server. |
| `kaiwen-gateway serve` | `--host` (default `127.0.0.1`), `--port` (default `8000`), `--log-level` | `kaiwen-gateway[server]` | Starts the local development ASGI server. Defaults to loopback with `ingress_mode="disabled"`. |
| `kaiwen-gateway demo` | `--timeout` (default `10.0`), `--port` (default `0`) | `kaiwen-gateway[server]` | Executes self-contained control plane synthetic call demo with ephemeral HMAC token. |
| `kaiwen-gateway demo --media` | `--timeout` (default `15.0`), `--port` (default `0`) | `kaiwen-gateway[server]` | Executes full media plane demo: streams 25 PCM16LE audio frames, verifies echo, exercises barge-in interruption, and verifies zero capacity leak. |

---

## Architecture & Design Principles

```text
┌─────────────────────────────────────────────────────┐
│ External systems: telephony / clients / webhooks   │
└────────────────────────┬────────────────────────────┘
                         │
              HTTP / WSS (loopback)
                         │
┌────────────────────────▼────────────────────────────┐
│ Kaiwen Gateway                                     │
│                                                     │
│  Server / transport adapters                        │
│    ├─ /ws/synthetic-telephony/v1 (Control)          │
│    └─ /ws/synthetic-media/v1 (Audio PCM16LE)        │
│        │                                            │
│  Core: identity · admission · routing · lifecycle   │
│    ├─ AdmissionController                           │
│    ├─ ConnectionSupervisor                          │
│    └─ InstanceDrainCoordinator                      │
│        │                                            │
│  Modules: Voice (v1)                                │
│    ├─ VoiceBridgeSession (Inbound/Outbound pumps)   │
│    ├─ Barge-in interruption & turn control          │
│    └─ BoundedFakeAIEchoPort                         │
└─────────────────────────────────────────────────────┘
```

### Design Principles

1. **A small core.** Zero external dependencies in core. No duplicate agent runtimes, schedulers, or speculative distributed buses.
2. **Explicit adapters.** External provider details belong behind ports with contract tests.
3. **Minimal audio hot path.** Zero JSON serialization on the raw binary audio path. Non-blocking queue operations (`put_nowait()`) on media pumps.
4. **Isolation by default.** All authorization decisions require cryptographic verification; caller ID or client-supplied IDs are never trusted blindly.
5. **Fail safely.** Timeouts, cancellation, bounded queues, and guaranteed resource cleanup across all error and cancellation paths.
6. **Measured performance.** Performance claims must be supported by multi-trial, repeatable evidence under documented reference conditions.

---

## Security & Privacy Boundaries

Security and privacy are core architectural requirements:

- **Zero Audio & Transcript Persistence:** By default, Kaiwen Gateway **never** writes raw audio frames, audio payloads, or conversation transcripts to disk, database, or logs. Any audio recording requires an explicit, application-owned legal basis and custom adapter.
- **Cryptographic Authentication:** Inbound WebSocket connections require an HTTP `Authorization: Bearer <token>` header verified via HMAC-SHA256 with a 60-second TTL before the WebSocket upgrade is accepted. Unauthorized connections receive HTTP 403 or close code 1008 without allocating Gateway resources.
- **Strict Scope Boundaries:** Token permissions are scoped (e.g. `voice.stream`, `voice.admin`). Media endpoints require `voice.stream`; administrative actions require `voice.admin`.
- **Bounded Resources & Admission:** Global and per-tenant connection ceilings are strictly enforced. Requests exceeding limits are rejected immediately (`GlobalCapacityExceededError`, `TenantCapacityExceededError`).
- **Loopback-Only Default:** The ASGI server binds strictly to `127.0.0.1` by default. Never expose the development server to public interfaces (`0.0.0.0`) without an upstream TLS termination proxy.
- **Redaction:** Sensitive identifiers, tokens, and raw exception messages are redacted from public logs, error summaries, and lease string representations.

For vulnerability reporting, see [`SECURITY.md`](SECURITY.md).

---

## Performance & Concurrency (M5 Evidence Summary)

Repeatable synthetic performance qualification was conducted across 18 independent trials using the multi-process benchmark harness (`tools/synthetic_media_load.py` and `tools/aggregate_benchmark_trials.py`):

| Concurrency (Calls) | Client Workers | Frame Integrity | Missed Deadlines | Scheduler Lateness (p95) | Echo RTT (p95) | Gateway Inbound Wait | Verdict |
| :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: |
| **1** | 1 | 100.0% | 0.00% | 4.3 ms | ~30 ms | < 0.1 ms | `REPEATABLY_PASSING` |
| **5** | 1 | 100.0% | 0.00% | 10.9 ms | ~35 ms | < 0.2 ms | `REPEATABLY_PASSING` |
| **25** | 2 | 100.0% | 0.32% | 10.9 ms | ~55 ms | ~0.5 ms | `REPEATABLY_PASSING` |
| **50** | 2 | 100.0% | 0.00% | 10.0 ms | ~86 ms | ~1.2 ms | `REPEATABLY_PASSING` |
| **100** | 4 | 100.0% | 0.02% | 8.4 ms | ~2,045 ms | ~26 ms | `REPEATABLY_PASSING`* |

*\*Important Qualification Note on 100 Concurrency:*
- **What passed:** All three 100-session qualification trials passed 100% media frame integrity (zero dropped frames), 100% session cleanup, and met client sender pacing criteria (0.02% missed deadlines).
- **Explicit Latency Limitation:** At 100 concurrent sessions (5,000 frames/sec total throughput), synthetic echo round-trip time (RTT) p95 reached **~2,045 ms** (ranging from 1,732 ms to 2,705 ms). Diagnostic bottleneck isolation identified that this latency is caused by **single-threaded asyncio event loop transit queue residence** on the Gateway server under high aggregate event volumes, rather than network stalls or CPU starvation.
- **Operational Reality:** Kaiwen Gateway does **NOT** claim sub-150ms interactive voice latency at 100 concurrent sessions on a single asyncio event loop. For interactive low-latency voice, single-instance concurrency should be capped at **50 calls** (p95 RTT ~86 ms), or scaled horizontally across multiple worker processes.

Full benchmark reports and methodology are available in `reports/m5_repeatable_qualification_report.md` and `reports/m5_qualification_manifest.json`.

---

## Local Development & Verification

From a local clone with Python 3.11+ and `uv` installed:

```bash
# Sync dependencies
uv sync --locked --group dev --extra server

# Code formatting & linting
uv run ruff check .
uv run ruff format --check .

# Strict typechecking
uv run mypy src tests tools examples

# Run unit & integration test suite with coverage
uv run pytest -q --cov=kaiwen_gateway --cov-report=term-missing

# Dependency vulnerability audit
uv audit --locked --preview-features audit-command

# Build distributions
uv build
```

---

## Documentation Map

| Document | Purpose |
| --- | --- |
| [`docs/REQUIREMENTS.md`](docs/REQUIREMENTS.md) | V1 requirements, P0 acceptance criteria, and status. |
| [`ROADMAP.md`](ROADMAP.md) | Milestone delivery status and deferred work packages. |
| [`CHANGELOG.md`](CHANGELOG.md) | Record of user-visible changes and version releases. |
| [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md) | Hexagonal architecture, ports, adapters, and data flow. |
| [`docs/decisions/`](docs/decisions/) | Numbered Architecture Decision Records (`ADR-0001` through `ADR-0004`). |
| [`SECURITY.md`](SECURITY.md) | Security policy and vulnerability disclosure instructions. |
| [`CONTRIBUTING.md`](CONTRIBUTING.md) | Development workflow and PR submission standards. |
| [`AGENTS.md`](AGENTS.md) | Rules and verification protocols for autonomous coding agents. |

---

## License

This project is licensed under the [MIT License](LICENSE) (`Copyright (c) 2026 Kaiwen`).
