# Kaiwen Gateway

**A modular, provider-agnostic communication gateway for real-time AI applications.**

> [!IMPORTANT]
> **Project Status: Synthetic Alpha Release Candidate (`v0.1.0a1` — Unreleased).**
> This repository provides an installable, verified, synthetic-environment alpha release candidate. It enables developers to test, benchmark, and integrate against the Gateway using fully reproducible in-process and loopback synthetic environments without requiring external credentials or proprietary telephony hardware.
>
> **Production use is not yet supported.** The package is **not yet published to PyPI**. Live Verimor PSTN/WSS interoperability (`FR-INT-007`) and external AI provider adapters (`FR-INT-003`, e.g. OpenAI Realtime / Gemini Live) remain deferred pending official vendor specifications and contract validation.

Kaiwen Gateway is designed to be both a **reusable Python library** and a **deployable ASGI service**. It accepts connections from external systems, authenticates and isolates them, translates provider-specific protocols into well-defined internal contracts, and bridges live communication to application and AI backends.

The first implementation focuses on **bidirectional telephone audio over secure WebSockets (WSS)**. The core is decoupled from telephony so future communication modules can be introduced without rewriting the platform.

---

## Quickstart

### 1. Installation

#### A. Evaluating Locally from Source or Built Wheel (Available Now)

Kaiwen Gateway is currently in release candidate status. To install from a local checkout or locally built wheel:

```bash
# Clone the repository
git clone https://github.com/ImtheKaiwen/kaiwen-gateway.git
cd kaiwen-gateway

# Option 1: Install development dependencies and ASGI runtime using uv
uv sync --extra server

# Option 2: Build and install local wheel into an isolated virtual environment
uv build
pip install dist/kaiwen_gateway-0.1.0a1-py3-none-any.whl[server]
```

To install only the minimal core library (contracts, admission control, identity, lifecycle supervision) without server dependencies:

```bash
pip install dist/kaiwen_gateway-0.1.0a1-py3-none-any.whl
```

#### B. Installing from PyPI (Post-Publication)

*Note: The package is not yet published to PyPI. Publishing is protected by strict release safeguards (`.github/workflows/release.yml`): `workflow_dispatch` is restricted to verification and dry-run testing only; publication to PyPI requires an authorized, non-draft GitHub Release event with a matching version tag and explicit maintainer approval via `vars.PYPI_PUBLISH_AUTHORIZED=true`. The commands below will be available once the maintainer officially authorizes and completes publication to PyPI.*

```bash
# Install core package + ASGI runtime once published:
pip install "kaiwen-gateway[server]"

# Install minimal core library only:
pip install kaiwen-gateway
```

---

### 2. Verify Installation & Configuration

The CLI runs with zero external runtime dependencies:

```bash
# Check version
kaiwen-gateway --version

# Verify configuration and environment variables
kaiwen-gateway check-config
```

---

### 3. Run Executable Demos

The Gateway includes self-contained, automated loopback demos that allocate an ephemeral port, verify token authentication, execute a complete call lifecycle, and confirm zero capacity leakage:

```bash
# Control plane synthetic telephony call demo:
kaiwen-gateway demo

# Full media plane demo (streams 25 binary PCM16LE audio frames, verifies echo, exercises barge-in interruption, and verifies zero capacity leak):
kaiwen-gateway demo --media
```

---

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

| Capability | Requirement Reference | Status in v0.1.0a1 | Notes / Limitations |
| --- | --- | --- | --- |
| **Gateway Core Architecture** | `FR-CORE-001`–`007` | **Supported (Verified)** | Framework-independent modular core (`compose_gateway_core`), typed module registry, immutable configuration (`CoreConfig`). |
| **Identity & Scoped Authorization** | `FR-CONN-002`, `SEC-002`–`003` | **Supported (Verified)** | Strong identity tokens (`ProviderPrincipal`, `TenantCallBinding`), HMAC-SHA256 validation, scope checking (`voice.stream`, `voice.admin`), control-character rejection. |
| **Admission Control** | `FR-CONN-005`, `NFR-REL-002` | **Supported (Verified)** | Process-local atomic capacity reservations against global and per-tenant ceilings, duplicate connection prevention, idempotent lease release. |
| **Supervised Connection Lifecycle** | `FR-CONN-004`, `REL-001`–`003` | **Supported (Verified)** | Strict state machine (`NEW` → `ADMITTED` → `ACTIVE` → `CLOSED`/`FAILED`), cancellation shielding, timeout enforcement, zero orphan tasks. |
| **Graceful Instance Draining** | `FR-CONN-006`, `REL-004` | **Supported (Verified)** | Multi-phase draining coordinator (`InstanceDrainCoordinator`), admission rejection during drain, bounded grace period, cooperative termination. |
| **ASGI Server & Health Endpoints** | `FR-CORE-002`, `OBS-001` | **Supported (Verified)** | Starlette/Uvicorn ASGI host, `GET /health` (liveness), `GET /ready` (readiness with drain detection). Restricted to loopback (`127.0.0.1`) by default. |
| **Synthetic Telephony Transport** | `FR-CONN-001`, `FR-INT-006` | **Supported (Verified)** | `/ws/synthetic-telephony/v1` authenticated WSS endpoint, structured JSON control events (`call.start`, `call.started`, `call.end`, `call.ended`, ping/pong). |
| **Synthetic Media Transport** | `FR-VOICE-002`–`004` | **Supported (Verified)** | `/ws/synthetic-media/v1` binary WebSocket transport, 16kHz 16-bit linear PCM mono framing (20ms frames), single-read demux, control event interleaving. |
| **Voice Media Bridge** | `FR-VOICE-001`, `FR-VOICE-005` | **Supported (Verified)** | Asynchronous bidirectional media bridge (`VoiceBridgeSession`), isolated inbound/outbound audio pumps, turn tracking, backpressure bounds. |
| **Barge-in Interruption** | `FR-VOICE-006` | **Supported (Verified)** | User speech interruption triggers downstream AI cancellation, outbound buffer flushing, and synchronized turn incrementing. |
| **Fake AI Echo Adapter** | `FR-INT-002` | **Supported (Verified)** | In-process `BoundedFakeAIEchoPort` for deterministic loopback audio testing and benchmarking. |
| **Server-Owned Credentials** | `FR-INT-003`, `SEC-005` | **Supported (Verified)** | Credentials and secrets stored only in server-controlled configuration/secret providers; redacted from logs and error summaries; never exposed to telephony clients. |
| **Privacy & Zero Audio Persistence** | `SEC-006` | **Supported (Verified)** | **Zero raw audio or transcript persistence by default.** No recording or transcript storage occurs without application-owned explicit opt-in adapters. |
| **Outbound Egress & SSRF Protection** | `SEC-008` | **Supported (Verified)** | In-process synthetic adapters; no arbitrary user-supplied egress URLs accepted; connections restricted to loopback endpoints. |
| **Gateway Transport Overhead** | `NFR-PERF-004` (P1) | **Design Goal (Observed)** | Design goal is p95 Gateway-added transport processing overhead ≤ 50 ms under reference workload (excluding external network/model delays; not an established guarantee). Observed Gateway queue wait + bridge compute overhead was ~2.5 ms at 50 sessions and ~26 ms at 100 sessions under reference conditions. |
| **Live Verimor Telephony** | `FR-INT-007` (P1) | **Deferred (Pending Vendor Spec)** | `OPEN-001` / `OPEN-002`. Blocked pending official vendor WebSocket specification. Wire format is strictly not guessed. |
| **OpenAI Live Contract Verification** | `FR-INT-005` | **Deferred (Exception Pending)** | `OPEN-003`. Official sandbox verification unverified; requires production credentials. Loopback testing uses fake echo adapter. Gate B exception pending maintainer approval. |
| **Agent Realtime Context Mapping** | `FR-INT-004` | **Deferred (Exception Pending)** | Integration adapter deferred while `kaiwen-agent` public realtime interfaces are under active parallel development. Gate B exception pending maintainer approval. |
| **Tool Authorization Delegation** | `SEC-007` | **Deferred (Exception Pending)** | External agent tool authorization, approvals, and audit are owned by consuming application/agent, out of scope for synthetic audio gateway alpha. Gate B exception pending maintainer approval. |
| **Distributed / Multi-Instance State** | `Gate C/D`, `M8` | **Deferred (Out of Alpha Scope)** | Atomic replay store and distributed multi-worker coordination deferred to Gate C/D (M8). Current alpha is single-instance only. |
| **Interactive Voice at $\ge 100$ Calls** | `NFR-PERF-006` (P1) | **Documented Limitation** | 100 concurrent calls achieve 100% media integrity (0% loss) and pass pacing criteria, but median p95 echo RTT reaches ~2,045 ms due to single-threaded event loop transit backlog. |

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

## Security & Privacy Boundaries

Security and privacy are core architectural requirements:

- **Zero Audio & Transcript Persistence (`SEC-006`):** By default, Kaiwen Gateway **never** writes raw audio frames, audio payloads, or conversation transcripts to disk, database, or logs. Any audio recording requires an explicit, application-owned legal basis and custom adapter.
- **Cryptographic Authentication (`SEC-002`):** Inbound WebSocket connections require an HTTP `Authorization: Bearer <token>` header verified via HMAC-SHA256 with a 60-second TTL before the WebSocket upgrade is accepted. Unauthorized connections receive HTTP 403 or close code 1008 without allocating Gateway resources.
- **Strict Scope Boundaries (`SEC-003`):** Token permissions are scoped (e.g. `voice.stream`, `voice.admin`). Media endpoints require `voice.stream`; administrative actions require `voice.admin`.
- **Server-Controlled Credentials (`SEC-005`, `FR-INT-003`):** API keys and server credentials remain on the server, redacted from logs, traces, and error summaries, and are never exposed to telephony clients.
- **Bounded Resources & Admission (`SEC-004`):** Global and per-tenant connection ceilings are strictly enforced. Requests exceeding limits are rejected immediately (`GlobalCapacityExceededError`, `TenantCapacityExceededError`).
- **Loopback-Only Default (`SEC-001`):** The ASGI server binds strictly to `127.0.0.1` by default. Never expose the development server to public interfaces (`0.0.0.0`) without an upstream TLS termination proxy.
- **SSRF & Egress Protection (`SEC-008`):** Restricts connections to verified loopback endpoints; rejects arbitrary user-supplied egress URLs.

For vulnerability reporting, see [`SECURITY.md`](SECURITY.md).

---

## Performance & Concurrency (Empirical Evidence)

Repeatable synthetic performance qualification was conducted across 18 independent trials using the multi-process benchmark harness (`tools/synthetic_media_load.py` and `tools/aggregate_benchmark_trials.py`), with targeted fast-path queue optimizations and echo bottleneck isolation evaluated in PR #36:

### Key Metrics Distinction

It is critical to distinguish between the following five performance metrics:

1. **Media Integrity:** Frame completeness and byte correctness (100% frames delivered, 0 missing, 0 duplicates, 0 payload mismatches).
2. **Client Pacing:** Timeliness of frame transmission by client load-generator workers (missed deadline rate $\le 20\%$, scheduler lateness $\le 100$ ms).
3. **Synthetic Echo RTT:** Full loopback round-trip time through client send $\rightarrow$ socket $\rightarrow$ Gateway server event loop $\rightarrow$ inbound queue $\rightarrow$ bridge $\rightarrow$ fake AI echo $\rightarrow$ outbound queue $\rightarrow$ socket $\rightarrow$ client receive.
4. **Gateway-Added Processing Overhead:** Pure CPU compute duration within the Gateway Voice Bridge (`inbound_processing_ms` + `outbound_processing_ms`), which remains sub-millisecond (< 0.05 ms p95 at 100 sessions).
5. **Interactive Conversational Latency:** Real human conversational latency, which includes external carrier network jitter, real AI generation latency, and client audio devices. **Synthetic echo RTT does not represent conversational latency or guarantee an interactive SLA.**

### Measured Concurrency Results

| Concurrency (Calls) | Client Workers | Frame Integrity | Missed Deadlines | Scheduler Lateness (p95) | Synthetic Echo RTT (p95 Median) | Synthetic Echo RTT (p95 Worst) | Gateway Inbound Wait | Verdict |
| :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: |
| **1** | 1 | 100.0% | 0.00% | 4.3 ms | ~30 ms | ~33 ms | < 0.1 ms | `REPEATABLY_PASSING` |
| **5** | 1 | 100.0% | 0.00% | 10.9 ms | ~35 ms | ~38 ms | < 0.2 ms | `REPEATABLY_PASSING` |
| **25** | 2 | 100.0% | 0.32% | 10.9 ms | ~55 ms | ~62 ms | ~0.5 ms | `REPEATABLY_PASSING` |
| **50** | 2 | 100.0% | 0.00% | 10.0 ms | **127.3 ms** | **255.7 ms** | ~1.2 ms | `REPEATABLY_PASSING`* |
| **100** | 4 | 100.0% | 0.02% | 8.4 ms | **~2,045 ms** | **~2,705 ms** | ~26 ms | `REPEATABLY_PASSING`* |

*\*Important Qualification Notes:*
- **50 Concurrent Sessions:** All trials passed 100% media integrity and client pacing. Median trial p95 echo RTT was **127.3 ms** (with worst trial p95 reaching **255.7 ms** under high event loop density). This establishes successful synthetic qualification under test conditions, but does **not** constitute a universal guarantee of sub-150ms production voice capacity under variable network or host conditions.
- **100 Concurrent Sessions:** All trials passed 100% media integrity (zero frame loss) and 100% session cleanup. However, synthetic echo RTT p95 reached **~2,045 ms** (worst trial ~2,705 ms) due to **single-threaded asyncio event loop transit backlog** under 5,000 frames/sec aggregate throughput.
- **Explicit Capacity Limitation:** Kaiwen Gateway does **NOT** claim sub-150ms interactive voice latency at 100 concurrent sessions on a single asyncio event loop. Scaling to 100+ concurrent interactive voice calls requires horizontal multi-worker or multi-process server distribution.

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
| [`docs/decisions/`](docs/decisions/) | Numbered Architecture Decision Records (`ADR-0001` through `ADR-0005`). |
| [`SECURITY.md`](SECURITY.md) | Security policy and vulnerability disclosure instructions. |
| [`CONTRIBUTING.md`](CONTRIBUTING.md) | Development workflow and PR submission standards. |
| [`AGENTS.md`](AGENTS.md) | Rules and verification protocols for autonomous coding agents. |

---

## License

This project is licensed under the [MIT License](LICENSE) (`Copyright (c) 2026 Kaiwen`).
