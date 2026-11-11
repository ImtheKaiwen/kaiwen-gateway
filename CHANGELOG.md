# Changelog

All notable **user-facing changes** to Kaiwen Gateway are recorded in this file.

This project follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and intends to use [Semantic Versioning](https://semver.org/spec/v2.0.0.html) for its Python distribution. The **Python package version**, any **external Gateway wire/API protocol version**, and **provider adapter compatibility** are separate concerns; a package release does not automatically change a wire protocol version.

> **Project status (2026-11-12): Synthetic Alpha Release Candidate (v0.1.0a1 - Unreleased).**
> Kaiwen Gateway `v0.1.0a1` is prepared as a verified, installable, documented synthetic-environment alpha release candidate. It provides a complete, runnable synthetic communication gateway (core identity, admission control, connection lifecycle, graceful draining, ASGI server, synthetic telephony control, synthetic PCM16LE audio streaming, voice media bridge, and barge-in interruption) without requiring external credentials or proprietary telephony hardware.
>
> **Production use is not yet supported and no package has been published to PyPI.** Live Verimor interoperability (`FR-INT-007`) and external AI provider backends (`FR-INT-003`, OpenAI Realtime / Gemini Live) remain deferred pending vendor specifications.

## [Unreleased]

### Added
- None yet.

---

## [0.1.0a1] - Unreleased (Release Candidate)

### Added

#### Gateway Core & Composition
- **Framework-Independent Core:** Added `GatewayCore` and `compose_gateway_core` with typed module registration, bounded module collection (`MAX_COMPOSED_MODULES = 64`), transactional module registration, and process-local instance isolation (`ADR-0001`, `ADR-0002`).
- **Immutable Configuration:** Added `CoreConfig` with fail-closed ingress validation (`ingress_mode="disabled"` by default, rejecting public ingress without secure configuration).
- **Strong Identity & Scopes:** Added immutable identity value objects (`ProviderPrincipal`, `TenantCallBinding`, `VerifiedConnectionContext`), HMAC-SHA256 cryptographic token verification with a 60-second TTL, scope enforcement (`voice.stream`, `voice.admin`), control character rejection, and scope denial redaction.
- **Process-Local Admission Control:** Added `AdmissionController` with atomic capacity reservation against global (default 25) and per-tenant (default 10) ceilings, duplicate active connection rejection, idempotent exactly-once lease release, and zero-count tenant pruning.
- **Supervised Connection Lifecycle:** Added `ConnectionSupervisor` with strict state transitions (`NEW` → `ADMITTED` → `ACTIVE` → `CLOSED`/`FAILED`), worker task supervision, sibling task cancellation, shielded background cleanup with timeout protection, LIFO registered async teardown, bounded auto-close fallbacks, and zero orphan tasks.
- **Graceful Multi-Phase Draining:** Added `InstanceDrainCoordinator` with atomic admission gating, linearized race protection between admissions and shutdown, bounded grace periods, cooperative termination (`TerminalReason.CANCELLED`), and conservative unresolved cohort accounting.

#### Server & Transports
- **ASGI Application Runtime:** Added `create_gateway_app` and `gateway_lifespan` in `kaiwen_gateway.server` with Starlette/Uvicorn hosting, loopback-only default binding (`127.0.0.1:8000`), HTTP liveness endpoint (`GET /health`), and HTTP readiness endpoint (`GET /ready` reflecting drain and stopped states).
- **Synthetic Telephony Transport:** Added `/ws/synthetic-telephony/v1` authenticated WebSocket transport with structured JSON control frames (`call.start`, `call.started`, `call.end`, `call.ended`, ping/pong), frame size limits (64 KiB), and pre-upgrade authentication rejection.
- **Synthetic Media Audio Transport:** Added `/ws/synthetic-media/v1` binary WebSocket transport supporting bidirectional 16kHz 16-bit linear PCM mono audio framing (20ms frames / 640 bytes), single-read demux loop, and interleaved JSON control events.

#### Voice Media Bridge & Interruption
- **Voice Media Bridge:** Added `VoiceBridgeSession` in `kaiwen_gateway.modules.voice` with lifetime-bound, concurrent inbound and outbound audio pumps, turn tracking, backpressure bounds, and deadline-protected sentinel insertion.
- **Barge-in Interruption:** Implemented user barge-in handling: incoming audio interrupts active downstream output, flushes unplayed audio buffers, increments turn IDs, and signals interruption events across the media transport.
- **Fake AI Echo Adapter:** Added in-process `BoundedFakeAIEchoPort` providing deterministic audio loopback, configurable frame transformation, and turn-aware echo response for synthetic testing and benchmarking.

#### Developer CLI & Demos
- **Zero-Dependency CLI:** Added `kaiwen-gateway` entrypoint with standard library `argparse` supporting `kaiwen-gateway --version` and `kaiwen-gateway check-config` with zero external dependencies.
- **Local Development Server:** Added `kaiwen-gateway serve` to launch local ASGI host on loopback interfaces with optional `[server]` extra.
- **Synthetic Control Demo:** Added `kaiwen-gateway demo` to execute an automated, end-to-end synthetic telephony call lifecycle with ephemeral port allocation, HMAC token authentication, and verified zero capacity leakage.
- **Synthetic Media Audio Demo:** Added `kaiwen-gateway demo --media` to stream 25 binary PCM16LE audio frames, verify bidirectional byte echo, exercise barge-in interruption (Turn 1 → Turn 2), stream follow-up audio, cleanly terminate the call, and verify zero active capacity leaks.
- **Developer Quickstart Example:** Added `examples/quickstart_synthetic_media.py` demonstrating programmatic setup, token generation, streaming, interruption, and shutdown.

#### Release Automation & CI
- **Release Automation Workflow:** Added `.github/workflows/release.yml` with `workflow_dispatch` (dry-run by default) and `release: [published]` triggers, clean virtualenv smoke tests, SHA256 checksum generation, and PyPI Trusted Publishing via OIDC (`pypa/gh-action-pypi-publish@release/v1`).
- **Comprehensive Fast CI:** Hardened `.github/workflows/ci.yml` running across Python 3.11, 3.12, and 3.13 with Ruff, strict mypy, pytest with coverage, `uv audit --locked` vulnerability scanning, and clean wheel/sdist smoke testing including `demo --media`.

#### Benchmarking & Performance
- **Synthetic Media Load Evaluation Suite:** Added `tools/synthetic_media_load.py` supporting 1, 5, 25, 50, and 100 simultaneous synthetic calls under 20ms PCM16LE frame pacing.
- **Multi-Process Client Workers:** Partitioned client load generation across independent worker subprocesses (`--client-workers N`), eliminating client event loop starvation at high concurrency.
- **Multi-Trial Aggregator:** Added `tools/aggregate_benchmark_trials.py` to aggregate multi-trial benchmark reports, enforce trial comparability, calculate cross-trial tail metrics, and classify configurations (`REPEATABLY_PASSING`, `MIXED_RESULTS`, `REPEATABLY_FAILING`).
- **Repeatable Qualification Results:** Established verified synthetic concurrency qualification across 18 independent trials with PR #36 queue fast-path and bottleneck isolation: 1 call (3/3 PASS), 5 calls (3/3 PASS), 25 calls (3/3 PASS with 2 workers), 50 calls (3/3 PASS with 2 workers; median trial p95 echo RTT 127.3 ms, worst trial p95 255.7 ms), and 100 calls (3/3 PASS with 4 workers; 100% media integrity, 0% frame loss, 0.02% missed deadlines, median trial p95 echo RTT ~2,045 ms due to single-threaded event loop queuing). These synthetic metrics confirm frame integrity and client pacing under test conditions but do not constitute a universal production capacity or interactive voice SLA.

### Fixed
- **Fast-Path Queue Non-Blocking Puts:** Optimized media and adapter queue insertion by trying `put_nowait()` on non-full queues before falling back to `asyncio.wait_for(put())`, reducing event loop task switching under high frame volume.
- **Constrained Diagnostic Instrumentation:** Replaced per-frame memory dictionaries with fixed-window circular buffers and rate-limited sampling to avoid memory growth during high-concurrency benchmarks.
- **Voice Bridge Task Exception Retrieval:** Corrected retrieval of simultaneously failed pump task exceptions in `VoiceBridgeSession`, eliminating unobserved task exception warnings in Python 3.12+ and establishing deterministic primary-error precedence.
- **Terminal Cleanup Safety:** Attached a done-callback to the supervisor task, ensuring terminal teardown, buffer drainage, and port closure execute even if the supervisor task is cancelled prior to running.
- **Outbound Backpressure & Send Deadlines:** Added dedicated write deadlines via `send_timeout` on `WebSocketTelephonyMediaAdapter` and bounded queue insertion via `synthetic_backpressure_timeout`, preventing producer deadlocks when downstream media delivery stalls.
- **Accurate Manifest CLI Generation:** Corrected benchmark aggregator manifest generation to emit actual CLI options (`--concurrency`, `--output`, `--no-faults`) rather than deprecated flags.

### Security
- **Zero Raw Audio Persistence (`SEC-006`):** Enforced architectural rule ensuring no raw audio frames, audio payloads, or transcripts are persisted to disk, databases, or public logs by default.
- **Server-Controlled Credentials & Redaction (`SEC-005`, `FR-INT-003`):** Credentials and secrets remain strictly server-side, redacted from logs, traces, and error summaries, and are never exposed to telephony clients.
- **Cryptographic Token Verification (`SEC-002`):** Implemented HMAC-SHA256 signature verification with strict 60-second validity windows and scope verification (`SEC-003`).
- **Egress & SSRF Protection (`SEC-008`):** Restricted connections to verified loopback endpoints; rejects arbitrary user-supplied egress URLs.
- **Admission Ceilings (`SEC-004`, `FR-CONN-005`):** Strictly enforced global and per-tenant connection ceilings, failing closed before allocating upstream or server resources.
