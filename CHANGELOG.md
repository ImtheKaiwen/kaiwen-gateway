# Changelog

All notable **user-facing changes** to Kaiwen Gateway are recorded in this file.

This project follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and intends to use [Semantic Versioning](https://semver.org/spec/v2.0.0.html) for its Python distribution. The **Python package version**, any **external Gateway wire/API protocol version**, and **provider adapter compatibility** are separate concerns; a package release does not automatically change a wire protocol version.

> **Project status (2026-11-16): `v0.1.0b1` Public Beta Released (Gateway Core + OpenAI Voice Adapters Beta; `MAINTENANCE — INTEGRATION PENDING`).**
> Kaiwen Gateway `v0.1.0b1` is published on PyPI and GitHub Releases (superseding synthetic alphas `v0.1.0a2` and `v0.1.0a1`), covering Gateway Core, ASGI server hosting, synthetic telephony/media transports, `VoiceBridgeSession`, `RealtimeAdapterConformanceSuite`, and both `LIVE_VERIFIED` OpenAI voice adapters (`kaiwen_gateway.adapters.openai_realtime` and `kaiwen_gateway.adapters.openai_gpt_live`).
>
> **Experimental beta — production use is not supported.** Kaiwen Agent, Kaiwen Speech, and live Verimor PSTN/WSS interoperability (`FR-INT-007`) remain deferred external integrations.

## [Unreleased]

### Added
- **WebSocket Events & Security (`E2`, `/ws/events/v1`):** Added `CoreConfig.events_ingress_mode` (`"disabled"` default, `"authenticated"`), fail-closed `create_gateway_app()` Events wiring (`enable_events`, `events_authenticator`, `events_authorizer`, `events_ticket_verifier`, `events_allowed_origins`, `events_send_timeout_seconds`), `EventsAuthenticatorPort` / `EventsAuthorizerPort` / `EventsTicketVerifierPort` / `BoundedTicketReplayCache` (single-instance process-local `jti` replay protection that never evicts unexpired entries and fails closed when exhausted; cross-instance replay protection deferred to `E3`), `kaiwen_gateway.transports.events` (`/ws/events/v1` protocol v1 with `session.ready` `allowed_scopes`, `events.subscribe` `topic`/`topic_pattern`, `events.unsubscribe` `topic`/`subscription_id`, `session.ping`/`session.pong` `seq`, `events.delivery` `<= 65_536` byte wire frame enforcement, `events.error`, bounded send timeout `5.0s`, control rate limit `20/s` burst `40`, `1000`/`1007`/`1008`/`1009`/`1011` close codes), and Events-only `/ready` readiness support. Browser deployments require `WSS`/`TLS`, explicit `events_allowed_origins`, and reverse-proxy/CDN `?ticket=` query-string log redaction.
- **Core Additive Principal Identity & Admission (`E1`):** Added `PrincipalId`, `validate_principal_id`, `TenantPrincipalBinding`, `VerifiedPrincipalContext`, `PrincipalAuthorizationPort`, `bind_verified_principal_context`, `resolve_verified_principal_context`, `VerifiedConnectionIdentity`, `PrincipalAdmissionLease`, `AdmissionController.acquire_principal` / `admit_principal`, `PrincipalConnectionSupervisor`, and `InstanceDrainCoordinator.admit_principal_connection()` while preserving all Voice `call_id: str` public APIs unchanged.
- **Events Foundation Module (`E1`, `kaiwen_gateway.modules.events`):** Added immutable `EventEnvelope` (`v1`, `64 KiB` max frame, max JSON depth `16`, finite floats), `PublishResult`, `EventConnectionConsumer` / `EventSubscription` (explicit connection-level consumer owning the single multiplexed outbound queue while subscription handles manage registration/lifecycle), `EventPublisherPort` / `EventSubscriptionPort` / `EventBrokerPort` / `DurableEventStorePort`, `EventsTelemetrySink` / `InMemoryEventsTelemetrySink`, `TokenBucketRateLimiter` (provisional process-local defaults `200/s` burst `400` publish, `10/s` burst `20` subscribe active by default), `InMemoryEventBroker` (wildcard scope enforcement, `128` events, `1 MiB`/conn, `32 MiB` global budget, `32` subscriptions/conn, `disconnect_slow_consumer` default, global budget backpressure without falsely disconnecting healthy connections), and `EventsModule` (`GatewayModule` lifecycle adapter rejecting restart after terminal shutdown).

---

## [0.1.0b1] - 2026-11-16

### Added
- **OpenAI GPT-Live Media Adapter (`M4-R4`, `LIVE_VERIFIED` & `MOCK_VERIFIED`):** Added `kaiwen_gateway.adapters.openai_gpt_live` (`OpenAIGPTLiveConfig`, `OpenAIGPTLiveMediaAdapter`, `OpenAIGPTLiveTransport`, `create_openai_gpt_live_adapter_factory`, `connect_openai_gpt_live_websocket`) implementing `AIRealtimeMediaPort` and `InterruptibleAIPort` against `wss://api.openai.com/v1/live/sessions` (`gpt-live-1`). Enforces strict `session.start` -> `session.started` ordering before `session.input_audio.append`, stateful 16 kHz <-> 24 kHz PCM16LE resampling, bounded queues, local output purge and turn fencing (`can_cancel_future_generation=False`, `can_discard_local_output=True`), and secret-free error handling. Verified against `RealtimeAdapterConformanceSuite` (8/8 dimensions), 9 offline integration tests, and live end-to-end through `VoiceBridgeSession` and `/ws/synthetic-media/v1`.
- **OpenAI GPT-Live Guide & Dual-Adapter Quickstart (`M4-R4`):** Added `docs/guides/OPENAI_GPT_LIVE_ADAPTER.md` and `examples/openai_voice_adapters_quickstart.py`.
- **OpenAI Realtime Live API Verification (`M4-R3`, `LIVE_VERIFIED`):** Verified `kaiwen_gateway.adapters.openai_realtime` against the live OpenAI Realtime API (`wss://api.openai.com/v1/realtime?model=gpt-realtime`) across full bidirectional 16 kHz <-> 24 kHz PCM16LE audio streaming, `session.update`, completed response delivery, mid-response `response.cancel` turn interruption, and clean session teardown.
- **GPT-Live Standalone Diagnostic Protocol Probe (`M4-R3`, `GPT_LIVE_PROVIDER_SMOKE_PASS`):** Added `tools/gpt_live_protocol_probe.py` (with offline tests in `tests/test_gpt_live_protocol_probe.py`) and verified provider-level interoperability with `wss://api.openai.com/v1/live/sessions` (`gpt-live-1`).
- **Optional `.env` Loading in `OpenAIRealtimeConfig.from_env()` & `OpenAIGPTLiveConfig.from_env()`:** Added safe local `.env` fallback parsing (`dotenv_path=".env"`) when `OPENAI_API_KEY` is not already present in `os.environ`, without mutating process environment variables or exposing secrets.

### Fixed
- **Local-Output-Discard Interruption & Contiguous Sequence Stamping (`M4-R4`):** Updated `VoiceBridgeSession.interrupt()` and `/ws/synthetic-media/v1` `call.interrupt` handling to invoke `cancel_turn()` on `InterruptibleAIPort` implementations that declare `can_discard_local_output=True` with `can_cancel_future_generation=False` (truthfully reporting `future_generation_cancelled=False`), and updated `OpenAIGPTLiveMediaAdapter.receive_frame()` and `OpenAIRealtimeMediaAdapter.receive_frame()` to stamp contiguous delivery sequence numbers across mid-burst turn purges.
- **Server-Cancelled Response Partial Frame Reset:** Updated `OpenAIRealtimeMediaAdapter` to reset `_resampler_out` and `_packer` when `response.done` reports `status="cancelled"`, preventing residual sub-frame audio bytes from a server-VAD cancelled response from bleeding into subsequent responses.

---

## [0.1.0a2] - 2026-11-15

### Added
- **OpenAI Realtime Media Adapter (M4-R2, PR #49, `MOCK_VERIFIED`):** Added isolated `kaiwen_gateway.adapters.openai_realtime` package (`OpenAIRealtimeConfig`, `OpenAIRealtimeMediaAdapter`, `OpenAIRealtimeTransport`, `create_openai_realtime_adapter_factory`) and optional `openai` extra in `pyproject.toml`. Implements `AIRealtimeMediaPort` and `InterruptibleAIPort` with GA `session.update` (`audio/pcm`, `rate: 24000`), stateful 16 kHz <-> 24 kHz PCM16LE resampling, bounded queues, turn-transition fencing, loopback-only `ws://` / `wss://` transport validation, sanitized incomplete-response error propagation, and conformance testing. Verified against all 8 dimensions of `RealtimeAdapterConformanceSuite` (zero skips) and 30 mocked WebSocket integration tests (38 total); live OpenAI API verification (`LIVE_VERIFIED`) remains pending maintainer authorization.
- **Maintenance Freeze & Closeout Documentation:** Documented `MAINTENANCE — INTEGRATION PENDING` status, `v0.1.0a1` vs. `main` capability boundaries, PyPI publication verification, GitHub protection audit, and deferred Dependabot backlog across repository documentation.
- **Host-Owned Module Lifecycle Orchestration (S1-A):** Added `ModuleLifecycleManager`, `ModuleLifecyclePolicy`, `ModuleLifecyclePhase`, `ModuleFailure`, `ModuleStartupError`, and `ModuleShutdownError` in `kaiwen_gateway.core` to execute registered `GatewayModule` `startup()` hooks in registration order and `shutdown()` hooks in reverse order with bounded timeouts, startup rollback, aggregate shutdown errors, and secret-free error reporting. Wired `gateway_lifespan` in `kaiwen_gateway.server` to start registered modules before marking readiness and drain active connections before stopping modules.
- **Public API & Compatibility Policy (S1-C):** Published `docs/PUBLIC_API_AND_COMPATIBILITY.md` documenting alpha-stage compatibility expectations, version separation (package vs. Core contract vs. synthetic protocol), internal/private boundaries, and provider adapter ownership rules, backed by regression tests in `tests/test_public_api_compatibility.py`.
- **AI Realtime Adapter Conformance Suite:** Added `RealtimeAdapterConformanceSuite` and `SyntheticUpstreamFailingAIAdapter` in `kaiwen_gateway.testing.conformance` (packaged with wheel distributions) allowing third-party AI provider adapter authors to verify 8 critical isolation, audio delivery, backpressure, turn cancellation, and failure dimensions deterministically with zero paid API calls.

### Fixed
- **Release Workflow Dynamic Version Inspection:** Replaced hardcoded `Version: 0.1.0a1` assertion in `.github/workflows/release.yml` distribution inspection with dynamic verification against `pyproject.toml` (`steps.version.outputs.version`), backed by regression coverage in `tests/test_release_workflow.py`.
- **Fail-Closed Lifespan Drain Verification & Module Resource Safety (S1-R2, PR #48):** Updated `gateway_lifespan` in `kaiwen_gateway.server` to inspect `InstanceDrainCoordinator.report` (`unresolved_connections` and `active_leases_remaining`). If draining completes with unresolved connections or active admission leases (or fails to complete), `gateway_lifespan` skips `ModuleLifecycleManager.shutdown()` so module resources are not closed underneath still-active workers, sets `app.state.shutdown_failed = True` (`app.state.stopped = False`), and raises `ServerLifecycleError`.
- **Conformance Factory Resolution Safety (S1-B):** Replaced `TypeError`-retry cascading in `RealtimeAdapterConformanceSuite._resolve_adapter()` with pre-invocation signature inspection (`_resolve_adapter_binding`), guaranteeing exact single invocation and preserving internal initialization `TypeError` exceptions without stripping verified context.
- **Context-Aware AI Port Factory Resolution:** Enhanced `create_gateway_app` and synthetic media endpoints to pass `call_id` and `tenant_id` to `ai_media_port_factory` while maintaining 100% backwards compatibility with zero-argument factories.
- **AI Realtime Port Protocol Alias:** Exported `AIRealtimePort = AIRealtimeMediaPort` in `kaiwen_gateway.modules.voice.ports` for spec and naming compatibility.
- **AI Provider Adapter Integration Guide:** Published `docs/guides/AI_ADAPTER_INTEGRATION_GUIDE.md` detailing interface contracts, 16kHz PCM16LE framing rules, barge-in turn cancellation, server-side secret management, and conformance test execution.

---

## [0.1.0a1] - 2026-11-12

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
- **Release Automation Workflow:** Added `.github/workflows/release.yml` with `workflow_dispatch` (strictly verification and dry-run only; never publishes to PyPI) and `release: [published]` triggers, clean virtualenv smoke tests, SHA256 checksum generation, exact tag-to-version validation, failure on distribution conflict (`skip-existing: false`), fail-closed maintainer authorization guard (`vars.PYPI_PUBLISH_AUTHORIZED=true`), and PyPI Trusted Publishing via OIDC (`pypa/gh-action-pypi-publish`).
- **Comprehensive Fast CI:** Hardened `.github/workflows/ci.yml` running across Python 3.11, 3.12, and 3.13 with Ruff, strict mypy, pytest with coverage, `uv audit --locked` vulnerability scanning, and clean wheel/sdist smoke testing including `demo --media`.

#### Benchmarking & Performance
- **Synthetic Media Load Evaluation Suite:** Added `tools/synthetic_media_load.py` supporting 1, 5, 25, 50, and 100 simultaneous synthetic calls under 20ms PCM16LE frame pacing.
- **Multi-Process Client Workers:** Partitioned client load generation across independent worker subprocesses (`--client-workers N`), eliminating client event loop starvation at high concurrency.
- **Multi-Trial Aggregator:** Added `tools/aggregate_benchmark_trials.py` to aggregate multi-trial benchmark reports, enforce trial comparability, calculate cross-trial tail metrics, and classify configurations (`REPEATABLY_PASSING`, `MIXED_RESULTS`, `REPEATABLY_FAILING`).
- **Repeatable Qualification Results:** Established verified synthetic concurrency qualification across 18 independent trials with PR #36 queue fast-path and bottleneck isolation: 1 call (3/3 PASS), 5 calls (3/3 PASS), 25 calls (3/3 PASS with 2 workers), 50 calls (3/3 PASS with 2 workers; median trial p95 echo RTT 127.3 ms, worst trial p95 255.7 ms), and 100 calls (3/3 PASS with 4 workers; 100% media integrity, 0% frame loss, 0.02% missed deadlines, median trial p95 echo RTT ~2,045 ms due to single-threaded event loop queuing). In-bridge compute latency remained sub-millisecond (< 0.05 ms p95 at 100 calls), and Gateway-added transport overhead design goal (`NFR-PERF-004`, ≤ 50 ms) was observed at ~2.5 ms under 50 sessions and ~26 ms under 100 sessions. These synthetic metrics confirm frame integrity and client pacing under test conditions but do not constitute a universal production capacity or interactive voice SLA.

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
