# Kaiwen Gateway — Combined Beta Integration Acceptance Report (R1)

> **Repository:** `ImtheKaiwen/kaiwen-gateway`
> **Verified Baseline (`main`):** `20b390789647552afd738ae5de7d2e2c38baa26a`
> **Published Beta Package:** `v0.1.0b1` (PyPI & GitHub Releases — Gateway Core + OpenAI Voice Adapters Beta)
> **Scope:** Combined cross-module integration, real-process multi-instance behavior, concurrent Voice + Events + Notifications workloads, active graceful shutdown, failure isolation, and compatibility review across Core, Voice, Events (`E1`–`E4`), and Notifications (`N0`–`N4`).
> **Non-Goal Disclaimer:** This report evaluates **experimental beta integration readiness** on synthetic telephony/AI adapters and local disposable Redis. It does **not** certify production readiness, SLA compliance, exactly-once delivery, or live Kaiwen Agent / Kaiwen Speech / Verimor interoperability.

---

## 1. Baseline & Published-Beta vs. Unreleased-Main Gap

### 1.1 Exact Baseline
- **Base commit on `origin/main`:** `20b390789647552afd738ae5de7d2e2c38baa26a` (`docs(roadmap): mark Notifications N4 complete (PR #72 / 3a33413) (#73)`).
- **Published release:** `v0.1.0b1` (`2026-11-16`, Gateway Core + ASGI Server + Synthetic Voice + OpenAI Realtime & GPT-Live Voice Adapters).
- **Unreleased changes on `main` (`v0.1.0b1..20b3907`, 17 commits, 74 files changed, +18,931 / -181 lines):**
  1. **Core additive principal identity & admission (`E1`, PR #59):** `PrincipalId`, `VerifiedPrincipalContext`, `PrincipalAuthorizationPort`, `AdmissionController.admit_principal`, `PrincipalConnectionSupervisor`, `InstanceDrainCoordinator.admit_principal_connection()` (all Voice `call_id` APIs preserved unchanged).
  2. **Events module & authenticated `/ws/events/v1` transport (`E1`–`E2`, PR #59–#60):** `EventsModule`, `InMemoryEventBroker`, `EventEnvelope`, `TokenBucketRateLimiter`, `BoundedTicketReplayCache`, `/ws/events/v1` protocol v1.
  3. **Optional Redis Events backend (`E3`–`E4`, PR #61–#62):** `kaiwen-gateway[redis]` (`redis>=5.0,<7`), `RedisPubSubEventBroker`, `RedisDistributedRateLimiter`, `RedisTicketReplayCache`, `RedisStreamsDurableStore`, `StreamRecoveryWorker`, `tools/benchmark_events_streams.py`.
  4. **Notifications module (`N0`–`N4`, PR #64–#73):** `NotificationsModule`, `NotificationEnvelope`, `derive_recipient_topic()`, `NotificationsDispatcher`, `NotificationsTopicAuthorizer`, `ChainedNotificationsTopicAuthorizer`, `NotificationsDurablePublisher`, `NotificationsDurableWorker`, `tools/benchmark_notifications.py`, `docs/NOTIFICATIONS_N4_ACCEPTANCE_REPORT.md`.

### 1.2 Requirements for a Future Combined Beta Release (Not Published Here)
- Bump package version in `pyproject.toml` and `kaiwen_gateway.__version__` (e.g., `0.1.0b2` or `0.2.0b1`) and move `CHANGELOG.md` `[Unreleased]` entries into a dated release section.
- Update `AGENTS.md`, `README.md`, `docs/ARCHITECTURE.md`, and `docs/PUBLIC_API_AND_COMPATIBILITY.md` release headers from `v0.1.0b1` to the new beta version.
- Run a maintainer-approved release workflow (`.github/workflows/release.yml`) after all CI and acceptance gates pass.
- Deferred external integrations (Kaiwen Agent, Kaiwen Speech, live Verimor PSTN/WSS) remain explicitly excluded from this beta milestone.

---

## 2. Acceptance Scenario Matrix (R1 Evidence Closure)

All scenarios explicitly distinguish **broker acceptance**, **durable `XACK`**, **server-side WebSocket dispatch**, and **confirmed client receipt**. Test-only `/_test/*` HTTP routes in `tests/combined_beta_process_app.py` exist strictly for test-harness control/telemetry and are excluded from wheel builds and public APIs.

| Scenario | Phase | Status | Topology & Network Boundary | Verification Boundary | Test Evidence |
|---|---|---|---|---|---|
| Core + Voice only lifecycle (`/health`, `/ready`, `/ws/synthetic/v1`, `/ws/events/v1` rejected) | 2 | **PASS** | In-process ASGI (`TestClient`) | Confirmed WS client receipt & fail-closed rejection | `tests/test_combined_beta_lifecycle.py::test_phase2_core_plus_voice_only_lifecycle` |
| Core + Events only lifecycle (`/ready`, `/ws/events/v1`, `/ws/synthetic/v1` rejected) | 2 | **PASS** | In-process ASGI (`TestClient`) | Confirmed WS client receipt & fail-closed rejection | `tests/test_combined_beta_lifecycle.py::test_phase2_core_plus_events_only_lifecycle` |
| Core + Events + Notifications lifecycle & mandatory recipient isolation wrapping permissive authorizer | 2 | **PASS** | In-process ASGI (`TestClient`) | Confirmed WS `events.error` (`FORBIDDEN_SUBSCRIPTION`) & `events.subscribed` | `tests/test_combined_beta_lifecycle.py::test_phase2_core_events_notifications_lifecycle_and_mandatory_isolation` |
| Core + Voice + Events + Notifications full combined lifecycle | 2 | **PASS** | In-process ASGI (`TestClient`) | Confirmed WS client receipt on `/ws/synthetic-media/v1` & `/ws/events/v1` | `tests/test_combined_beta_lifecycle.py::test_phase2_full_combined_voice_events_notifications_lifecycle` |
| Fail-closed security configuration validation (`enable_notifications` without topic authorizer / `enable_events`) | 2 | **PASS** | In-process factory validation | Synchronous `CoreConfigurationError` before app creation | `tests/test_combined_beta_lifecycle.py::test_phase2_fail_closed_security_config_validation` |
| Optional module startup failure rollback & fail-closed state | 2 | **PASS** | In-process ASGI (`TestClient`) | `ServerLifecycleError`, rollback of started modules, `startup_failed=True`, `stopped=True` | `tests/test_combined_beta_lifecycle.py::test_phase2_optional_module_startup_failure_rolls_back_and_fails_closed` |
| Shared `AdmissionController` ceiling across Voice + Events + `/ready` `503 capacity_exceeded` | 4 | **PASS** | In-process ASGI (`TestClient`) | Shared global/tenant capacity accounting & fail-closed 3rd WS rejection | `tests/test_combined_beta_lifecycle.py::test_phase4_shared_admission_ceiling_across_voice_and_events` |
| Voice barge-in interruption concurrent with notification delivery | 4 | **PASS** | In-process ASGI (`TestClient`) | Confirmed PCM16LE echo, `call.interrupted`, & WS notification receipt | `tests/test_combined_beta_lifecycle.py::test_phase4_voice_barge_in_interruption_with_concurrent_events_and_notifications` |
| Unauthorized notification publication rejection (default `DenyAllProducerAuthorizer` & cross-tenant producer) | R1-5 | **PASS** | In-process ASGI (`TestClient`) | `NotificationAuthorizationError` / `EventAuthorizationError` fail-closed | `tests/test_combined_beta_lifecycle.py::test_r1_phase5_unauthorized_notification_publication_rejected` |
| Slow Events/Notifications subscriber (`disconnect_slow_consumer`) isolated from healthy recipient & active Voice | 5 | **PASS** | In-process ASGI (`TestClient`) | Slow WS evicted (`active_connections: 2 -> 1`), healthy WS & Voice unaffected | `tests/test_combined_beta_lifecycle.py::test_phase5_slow_events_subscriber_isolated_from_healthy_and_voice` |
| Active mixed-workload graceful ASGI shutdown (`uvicorn.Server.should_exit = True` while Voice + Events + worker active) | R1-1 | **PASS** | Real loopback `uvicorn.Server` (`127.0.0.1`) + real `websockets` clients | `DrainReport(unresolved_connections=0, active_leases_remaining=0)`, `stopped=True`, `shutdown_failed=False`, worker cancelled, zero surviving tasks, `< 5s` | `tests/test_combined_beta_lifecycle.py::test_r1_phase1_graceful_asgi_shutdown_under_active_mixed_workload` |
| Separate-OS-process `SIGTERM` graceful shutdown on Windows (`win32`) | R1-1 | **UNVERIFIED** | Windows `subprocess.Popen.terminate()` | On Windows, `terminate()` invokes `TerminateProcess` (hard kill), not graceful ASGI shutdown; genuine graceful shutdown is verified via loopback `uvicorn.Server` above | Platform limitation on `win32` |
| Real two-OS-process cross-instance Events, Notifications, tenant & recipient isolation, and process replacement (`gw-2` → `gw-3`) | 3 & 4 | **PASS** | Two independent Uvicorn OS processes (`127.0.0.1:<port1>`, `<port2>`) + Docker `redis:7-alpine` (`127.0.0.1:16379`) + real `websockets` clients | Confirmed end-to-end TCP WebSocket client receipt (`30/30` unique durable notifications, `worker_acked=30`, `FORBIDDEN_SUBSCRIPTION` denials, tenant isolation) | `tests/test_combined_beta_live_redis.py::test_phase3_and_phase4_real_multiprocess_concurrent_workloads` |
| Competing durable workers: deterministic individual processing (`gw-1` & `gw-2`), shared consumer group `XINFO CONSUMERS`, bounded worker task count, `pending_count == 0` | R1-2 | **PASS** | Two independent Uvicorn OS processes + Docker `redis:7-alpine` | `gw-1` acked `5/5` alone, `gw-2` acked `5/5` alone, both acked `20/20` concurrently, both registered in `XINFO CONSUMERS`, `worker_task_count <= 2`, `pending_count == 0` | `tests/test_combined_beta_live_redis.py::test_r1_phase2_competing_workers_individual_processing_and_bounded_loops` |
| Voice 20 ms paced PCM16LE frame ordering, controlled in-flight burst interruption (turn fencing), concurrent Events/Notifications, & server-side event-loop lag | R1-3 & R1-6 | **PASS** | Two independent Uvicorn OS processes + Docker `redis:7-alpine` + real `websockets` clients | 2 simultaneous Voice sessions (`10` paced frames each, `6`-frame in-flight burst + `call.interrupt`, zero Turn-1 leak after `call.interrupted`, `5` Turn-2 frames in order), `20/20` confirmed WS notifications, server-side loop lag recorded | `tests/test_combined_beta_live_redis.py::test_r1_phase3_voice_20ms_pacing_and_inflight_interruption_with_events` |
| Live Redis TCP disconnection during active Voice + Events session & automatic recovery | 5 | **PASS** | Independent Uvicorn OS process behind local asyncio TCP proxy → Docker `redis:7-alpine` + real `websockets` clients | `/ready` transitions `200 -> 503 (events_backend_unavailable) -> 200`; active Voice PCM16LE & `call.interrupt` unaffected; Events delivery resumes post-recovery | `tests/test_combined_beta_live_redis.py::test_phase5_redis_disconnection_isolates_active_voice_and_recovers` |
| Idle `RedisPubSubEventBroker` readiness characterization (zero active subscriptions on outage) | R1-4 | **PASS** (characterized P2) | Independent Uvicorn OS process behind local asyncio TCP proxy → Docker `redis:7-alpine` | `/ready` remains `200` while idle with zero subscriptions; first Redis publish fails closed (`503`) and immediately flips `/ready` to `503 events_backend_unavailable`; recovers to `200` after reconnect | `tests/test_combined_beta_live_redis.py::test_r1_phase4_idle_redis_readiness_characterization` |
| Durable worker crash before broker dispatch & after broker dispatch before `XACK` | 5 | **PASS** | Live Docker `redis:7-alpine` Streams (`XREADGROUP`, `XPENDING`/`XAUTOCLAIM`) | `worker_reclaimed=2`, `worker_acked=2`, `pending_count=0` (at-least-once semantics verified) | `tests/test_combined_beta_live_redis.py::test_phase5_worker_crash_before_and_after_dispatch_before_xack` |
| Clean wheel build & isolated virtualenv smoke tests (base wheel zero deps + `[server,redis]` extras) | 6 & 7 | **PASS** | Isolated `uv venv` (Python 3.12.13, Windows) | `BASE_WHEEL_OK 0.1.0b1` (1 pkg installed), `FULL_EXTRAS_WHEEL_OK` (10 pkgs installed) | `uv build` + isolated `uv pip install` |

---

## 3. Real-Process Deployment Topology & Measured Performance (Measurement Integrity)

### 3.1 Environment & Percentile Methodology
- **Host environment:** Windows 11 (`win32`), Python `3.12.13`, `uvicorn 0.54.0`, `websockets 15.0.1`, `redis 6.4.0`, disposable Docker container `redis:7-alpine` listening on `127.0.0.1:16379`.
- **Process topology:** Two independent Uvicorn OS processes (`gw-1`, `gw-2`) on distinct loopback TCP ports (`127.0.0.1:<port1>`, `127.0.0.1:<port2>`), sharing Redis Pub/Sub and Redis Streams, exercised by external `websockets` TCP clients.
- **Percentile method:** Nearest-rank index `sorted_vals[round((pct / 100.0) * (N - 1))]`.
- **Memory metric boundary:** Measured via Windows `GetProcessMemoryInfo().WorkingSetSize` (**Windows Working Set**, not Linux RSS/PSS).

### 3.2 Measured Results (Real Multi-Process + Live Redis)
1. **Concurrent batch (`test_phase3_and_phase4_real_multiprocess_concurrent_workloads`, `n=30` durable notifications + 2 active Voice sessions):**
   - Confirmed WebSocket client receipt: `30 / 30` unique notifications in `0.358 s` (observed throughput over batch interval: `83.73` notifications/s; `worker_acked=30`).
   - End-to-end notification client-receipt latency (HTTP enqueue on `gw-1` → WS client receipt on `gw-2`, `n=30`): **p50 = `26.98 ms`**, **p95 = `38.03 ms`**, **p99 = `42.58 ms`**.
   - Voice synthetic PCM16LE echo RTT (`n=10`): **p50 = `0.26 ms`**, **p95 = `0.91 ms`**.
   - Test-runner (client-side) asyncio event-loop lag (10 ms probe): **p95 = `7.28 ms`**.
   - Windows Working Set: `gw-1 = 42.4 MB`, `gw-2 = 41.3 MB`.
2. **20 ms paced Voice + in-flight interruption + concurrent Notifications (`test_r1_phase3_voice_20ms_pacing_and_inflight_interruption_with_events`, `n=20` durable notifications + 2 simultaneous 20 ms paced Voice sessions):**
   - Confirmed WebSocket client receipt: `20 / 20` unique notifications in `0.261 s` (**p50 = `32.21 ms`**, **p95 = `42.79 ms`**).
   - Voice 20 ms paced send interval (`n=18` inter-frame intervals across 2 sessions): **p50 = `23.84 ms`**, **p95 = `35.30 ms`** (reflects Windows timer resolution jitter on 20 ms `asyncio.sleep`).
   - Voice synthetic PCM16LE echo RTT (`n=20` paced frames): **p50 = `1.31 ms`**, **p95 = `4.51 ms`**.
   - **Gateway server-side event-loop lag** (in-server 20 ms probe task inside `gw-1` and `gw-2`): **`gw-1` p95 = `11.94 ms`**, **`gw-2` p95 = `12.27 ms`**.
   - Windows Working Set: `gw-1 = 42.2 MB`, `gw-2 = 41.5 MB`.
- **Limitations:** Local loopback on a single workstation with synthetic AI echo adapters and bounded sample sizes (`n=20..30`); excludes WAN jitter, TLS termination, and real AI provider latency. No production SLA is claimed.

---

## 4. Categorized Findings & Maintainer Decisions

### 4.1 Confirmed Product Defects
- **P0:** `0`
- **P1:** `0`

### 4.2 Acceptance Test Coverage Gaps (R1 Disposition)
- **Active mixed-workload graceful shutdown:** Closed via real loopback `uvicorn.Server.should_exit = True` test (`PASS`). Separate-OS-process `SIGTERM` graceful shutdown remains explicitly **`UNVERIFIED`** on Windows (`win32`) because `Popen.terminate()` is a hard process kill.
- **Competing durable worker individual processing:** Closed (`PASS` — deterministic individual ack verification for both `gw-1` and `gw-2` + Redis `XINFO CONSUMERS` verification + bounded worker task count).
- **Voice 20 ms pacing & in-flight interruption:** Closed (`PASS` — 20 ms paced unique frames + 6-frame in-flight burst + turn-fencing verification).
- **Server-side vs. client-side event-loop lag & Windows Working Set labeling:** Closed (`PASS` — server-side probe added inside `gw-1`/`gw-2` test harness and labeled separately from client-side lag).
- **Unauthorized notification publication rejection:** Closed (`PASS` — both default `DenyAllProducerAuthorizer` and cross-tenant producer rejection verified).

### 4.3 Operational Limitations (P2 — Requesting Maintainer Direction Where Applicable)
1. **Idle `RedisPubSubEventBroker` readiness (`P2`, maintainer policy decision requested):**
   - *Observed behavior:* In `RedisPubSubEventBroker._listener_loop()`, when `has_subs == False`, the loop sleeps `0.05 s` without issuing Redis `PING` commands. If Redis disconnects while an instance has zero active Pub/Sub subscriptions, `/ready` remains `200` until a `publish()` or `subscribe()` operation fails closed (`EventsBackendUnavailableError`), at which point `/ready` immediately flips to `503 events_backend_unavailable` and auto-recovers to `200` when Redis returns.
   - *Why not changed autonomously:* Adding periodic background `PING` traffic when idle changes idle Redis command load and readiness-flap policy.
   - *Maintainer options:* (A) Retain passive zero-idle-traffic behavior as documented; (B) Add an opt-in `idle_health_check_interval_seconds: float | None` setting to `RedisPubSubBrokerConfig`; or (C) Probe Redis lazily inside `/ready` with a short cached TTL.
2. **At-least-once crash recovery window:** As documented in `ADR-0007` and `docs/NOTIFICATIONS_N4_ACCEPTANCE_REPORT.md`, a worker crash *after* broker dispatch and *before* `XACK` causes redelivery after `claim_min_idle_ms`.
3. **Windows timer-flake issue (#68):** Remains separately tracked; not implicated in R1.

### 4.4 Future Production-Readiness Requirements (Out of Beta Scope)
- Real TLS/WSS termination, reverse-proxy `?ticket=` query-string log redaction, multi-host deployment load/soak testing, Linux `SIGTERM` process-signal soak verification, and deferred external integrations (Kaiwen Agent, Kaiwen Speech, live Verimor PSTN/WSS).

---

## 5. Quality Gates & Verification Evidence

### 5.1 Local Checks (Windows, Python 3.12.13)
- `uv lock --check`: **PASS** (`Resolved 27 packages in 1ms`)
- `uv run --no-sync ruff check .`: **PASS** (`All checks passed!`)
- `uv run --no-sync ruff format --check .`: **PASS** (`144 files already formatted`)
- `uv run --no-sync mypy src tests tools`: **PASS** (`Success: no issues found in 142 source files`)
- Combined + Live-Redis integration suite (`KAIWEN_TEST_REDIS_URL=redis://127.0.0.1:16379/0`):
  - Command: `uv run --no-sync pytest -q tests/test_combined_beta_lifecycle.py tests/test_combined_beta_live_redis.py tests/test_events_redis_live_integration.py tests/test_events_redis_streams_live.py tests/test_notifications_durable_live.py tests/test_notifications_n4_multi_instance_live.py`
  - Result: **32 passed in 23.33s** (plus targeted re-verification of `test_r1_phase2_competing_workers_individual_processing_and_bounded_loops` after tightening `worker_task_count == 2`).
- Full offline `pytest -q`: **1199 passed, 24 skipped in 149.47s** (0 failures; 22 skips are opt-in `KAIWEN_TEST_REDIS_URL` tests verified live above, 2 skips are synthetic echo adapter conformance upstream-failure hooks).
- Clean wheel & sdist build + isolated venv smoke tests: **PASS** (`BASE_WHEEL_OK 0.1.0b1`, `FULL_EXTRAS_WHEEL_OK`).

### 5.2 Independent R1 Review (Internal Subagent — Not a GitHub Maintainer Approval)
- **Reviewer:** Fresh independent `research` subagent (`45497d95-0e01-49e0-ab2f-642b362156fc`).
- **Findings:** `0` P0, `1` P1 (test false positive in `worker_task_count` task-name filter `"notifications"` vs. actual `"gw-notif-durable"` — **resolved** by filtering `"gw-notif-durable"` and asserting exact `worker_task_count == 2`), `4` P2 (report placeholders populated; Windows Working Set & simulated offline worker accurately documented; server loop-lag warmup samples noted).
- **Product Defects:** `0`.
- **Disposition:** Ready for maintainer review once R1 fixes and CI checks are green (note: internal subagent review is not a GitHub maintainer approval).

### 5.3 GitHub Actions CI (Draft PR #74)
- Verified on Draft PR #74 (`feat/combined-beta-acceptance`) across `Quality & Tests (Python 3.11)`, `Quality & Tests (Python 3.12)`, `Quality & Tests (Python 3.13)`, and `Package Build & Clean Wheel Smoke Test`.
