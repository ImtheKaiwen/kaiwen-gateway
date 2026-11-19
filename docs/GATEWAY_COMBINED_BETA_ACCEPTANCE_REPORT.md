# Kaiwen Gateway — Combined Beta Integration Acceptance Report

> **Repository:** `ImtheKaiwen/kaiwen-gateway`
> **Verified Baseline (`main`):** `20b390789647552afd738ae5de7d2e2c38baa26a`
> **Published Beta Package:** `v0.1.0b1` (PyPI & GitHub Releases — Gateway Core + OpenAI Voice Adapters Beta)
> **Scope:** Combined cross-module integration, real-process multi-instance behavior, concurrent Voice + Events + Notifications workloads, failure isolation, and compatibility review across Core, Voice, Events (`E1`–`E4`), and Notifications (`N0`–`N4`).
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
- Note: Kaiwen Agent integration, Kaiwen Speech integration, and live Verimor PSTN/WSS interoperability remain deferred external integrations and are **not** blockers for a combined Core + Voice + Events + Notifications beta release, provided release notes keep those boundaries explicit.

---

## 2. Acceptance Scenario Matrix

All scenarios below explicitly distinguish **broker acceptance**, **server-side WebSocket dispatch**, and **confirmed client receipt**.

| Scenario | Phase | Status | Topology & Network Boundary | Verification Boundary | Test Evidence |
|---|---|---|---|---|---|
| Core + Voice only lifecycle (`/health`, `/ready`, `/ws/synthetic/v1`, `/ws/events/v1` rejected) | 2 | **PASS** | In-process ASGI (`TestClient`) | Confirmed WS client receipt & fail-closed rejection | `tests/test_combined_beta_lifecycle.py::test_phase2_core_plus_voice_only_lifecycle` |
| Core + Events only lifecycle (`/ready`, `/ws/events/v1`, `/ws/synthetic/v1` rejected) | 2 | **PASS** | In-process ASGI (`TestClient`) | Confirmed WS client receipt & fail-closed rejection | `tests/test_combined_beta_lifecycle.py::test_phase2_core_plus_events_only_lifecycle` |
| Core + Events + Notifications lifecycle & mandatory recipient isolation wrapping permissive authorizer | 2 | **PASS** | In-process ASGI (`TestClient`) | Confirmed WS `events.error` (`FORBIDDEN_SUBSCRIPTION`) & `events.subscribed` | `tests/test_combined_beta_lifecycle.py::test_phase2_core_events_notifications_lifecycle_and_mandatory_isolation` |
| Core + Voice + Events + Notifications full combined lifecycle | 2 | **PASS** | In-process ASGI (`TestClient`) | Confirmed WS client receipt on both `/ws/synthetic-media/v1` and `/ws/events/v1` | `tests/test_combined_beta_lifecycle.py::test_phase2_full_combined_voice_events_notifications_lifecycle` |
| Fail-closed security configuration validation (`enable_notifications` without topic authorizer / `enable_events`) | 2 | **PASS** | In-process factory validation | Synchronous `CoreConfigurationError` before app creation | `tests/test_combined_beta_lifecycle.py::test_phase2_fail_closed_security_config_validation` |
| Optional module startup failure rollback & fail-closed state | 2 | **PASS** | In-process ASGI (`TestClient`) | `ServerLifecycleError`, rollback of started modules, `startup_failed=True`, `stopped=True` | `tests/test_combined_beta_lifecycle.py::test_phase2_optional_module_startup_failure_rolls_back_and_fails_closed` |
| Shared `AdmissionController` ceiling across Voice + Events + `/ready` `503 capacity_exceeded` | 4 | **PASS** | In-process ASGI (`TestClient`) | Shared global/tenant capacity accounting & fail-closed 3rd WS rejection | `tests/test_combined_beta_lifecycle.py::test_phase4_shared_admission_ceiling_across_voice_and_events` |
| Voice barge-in interruption (`call.interrupt` → `call.interrupted`, Turn 1 → Turn 2) with concurrent Events/Notifications | 4 | **PASS** | In-process ASGI (`TestClient`) | Confirmed binary PCM16LE echo & JSON control frame ordering | `tests/test_combined_beta_lifecycle.py::test_phase4_voice_barge_in_interruption_with_concurrent_events_and_notifications` |
| Slow Events/Notifications subscriber (`disconnect_slow_consumer`) isolated from healthy recipient & active Voice | 5 | **PASS** | In-process ASGI (`TestClient`) | Slow WS evicted (`active_connections: 2 -> 1`), healthy WS & Voice unaffected | `tests/test_combined_beta_lifecycle.py::test_phase5_slow_events_subscriber_isolated_from_healthy_and_voice` |
| Real two-OS-process cross-instance Events, competing durable Notifications workers, tenant & recipient isolation, concurrent Voice sessions, and process replacement (`gw-2` → `gw-3`) | 3 & 4 | **PASS** | Two independent Uvicorn OS processes on separate `127.0.0.1` TCP ports + Docker `redis:7-alpine` (`127.0.0.1:16379`) + real `websockets` clients | Confirmed end-to-end TCP WebSocket client receipt (`30/30` unique durable notifications, `worker_acked=30`, `FORBIDDEN_SUBSCRIPTION` denials, tenant isolation, PCM16LE echo + barge-in) | `tests/test_combined_beta_live_redis.py::test_phase3_and_phase4_real_multiprocess_concurrent_workloads` |
| Live Redis TCP disconnection during active Voice + Events session & automatic recovery | 5 | **PASS** | Independent Uvicorn OS process behind local asyncio TCP proxy → Docker `redis:7-alpine` + real `websockets` clients | `/ready` transitions `200 -> 503 (events_backend_unavailable) -> 200`; active Voice PCM16LE & `call.interrupt` unaffected; Events delivery resumes post-recovery | `tests/test_combined_beta_live_redis.py::test_phase5_redis_disconnection_isolates_active_voice_and_recovers` |
| Durable worker crash before broker dispatch & after broker dispatch before `XACK` | 5 | **PASS** | Live Docker `redis:7-alpine` Streams (`XREADGROUP`, `XPENDING`/`XAUTOCLAIM`) | `worker_reclaimed=2`, `worker_acked=2`, `pending_count=0` (at-least-once semantics verified) | `tests/test_combined_beta_live_redis.py::test_phase5_worker_crash_before_and_after_dispatch_before_xack` |
| Clean wheel build & isolated virtualenv smoke tests (base wheel zero deps + `[server,redis]` extras) | 6 & 7 | **PASS** | Isolated `uv venv` (Python 3.12.13, Windows) | `BASE_WHEEL_OK 0.1.0b1` (1 pkg installed), `FULL_EXTRAS_WHEEL_OK` (10 pkgs installed) | `uv build` + isolated `uv pip install` |

---

## 3. Real-Process Deployment Topology & Measured Performance

### 3.1 Real-Process Topology
- **Host environment:** Windows 11 (`win32`), Python `3.12.13`, `uvicorn 0.54.0`, `websockets 15.0.1`, `redis 6.4.0`, disposable Docker container `redis:7-alpine` listening on `127.0.0.1:16379`.
- **Gateway OS processes:**
  - `gw-1` (`uvicorn tests.combined_beta_process_app:build_process_app --factory --host 127.0.0.1 --port <port1>`)
  - `gw-2` (`uvicorn tests.combined_beta_process_app:build_process_app --factory --host 127.0.0.1 --port <port2>`)
  - Replacement process `gw-3` started on `<port3>` after terminating `gw-2` to verify disconnect/reconnect continuity.
- **Shared Redis state:** Pub/Sub fan-out (`RedisPubSubEventBroker`) and durable Notifications stream (`RedisStreamsDurableStore`, consumer group `<prefix>-group`, competing `NotificationsDurableWorker` instances in `gw-1` and `gw-2`).

### 3.2 Measured Performance & Boundaries
Captured from `test_phase3_and_phase4_real_multiprocess_concurrent_workloads` (`KAIWEN_TEST_REDIS_URL=redis://127.0.0.1:16379/0`):
- **Workload:** 30 durable notifications published via `gw-1` → Redis Streams → competing workers (`gw-1`, `gw-2`) → Redis Pub/Sub → real `websockets` client on `gw-2`, executing concurrently with 2 active `/ws/synthetic-media/v1` Voice sessions (one on `gw-1`, one on `gw-2`, 5× 640-byte PCM16LE frames + barge-in `call.interrupt` + Turn 2 audio each).
- **Observed metrics:**
  - Unique durable notifications confirmed received by WebSocket client: `30 / 30` (`worker_processed=30`, `worker_acked=30`, `0` duplicates under normal operation).
  - Batch elapsed wall-clock time: `0.469 s` (observed throughput: `63.99` notifications/s).
  - End-to-end notification client-receipt latency (HTTP enqueue on `gw-1` → WebSocket client receipt on `gw-2`): **p50 = `27.42 ms`**, **p95 = `40.82 ms`**, **p99 = `44.15 ms`**.
  - Voice synthetic PCM16LE echo round-trip latency over loopback WebSocket: **p50 = `0.72 ms`**, **p95 = `1.77 ms`**.
  - Test-runner asyncio event-loop lag (10 ms sleep probe): **p95 = `9.46 ms`**.
  - Independent OS process RSS (`GetProcessMemoryInfo` `WorkingSetSize`): **`gw-1 = 42.2 MB`**, **`gw-2 = 41.4 MB`**.
- **Measurement boundaries & limitations:**
  - Local loopback TCP (`127.0.0.1`) on a single developer workstation; does not include WAN network jitter, TLS termination overhead, or external AI provider latency.
  - Sample size (`n=30` durable notifications, `n=10` Voice echo frames) is a bounded acceptance concurrency check, not a long-duration soak or production capacity benchmark. No production SLA is claimed.

---

## 4. Failure Isolation & Operational Findings

- **P0 Findings:** `0`
- **P1 Findings:** `0`
- **P2 Operational Observations (Documented Limitations):**
  1. **Idle `RedisPubSubEventBroker` health detection:** During Phase 5 live TCP-proxy outage testing, we observed that when `RedisPubSubEventBroker` has **zero** active subscriptions, its idle Pub/Sub listener does not actively ping Redis; `/ready` therefore remains `200` until a subscription is active or a Redis command (`publish`/`subscribe`) executes and fails. When at least one Events subscription is active (or a publish/subscribe call runs), TCP disconnection is detected promptly and `/ready` transitions to `503 events_backend_unavailable`, then recovers automatically to `200` on reconnect.
  2. **At-least-once crash recovery window:** As documented in `ADR-0007` and `docs/NOTIFICATIONS_N4_ACCEPTANCE_REPORT.md`, a worker crash *after* broker dispatch and *before* `XACK` results in redelivery after `claim_min_idle_ms`, which can produce a duplicate delivery to connected clients.
  3. **Known Windows timer-flake issue (#68):** Tracked separately; not modified in this acceptance pass.

---

## 5. Quality Gates & Verification Evidence

### 5.1 Local Checks (Windows, Python 3.12.13)
- `uv lock --check`: **PASS** (`Resolved 27 packages in 1ms`)
- `uv run --no-sync ruff check .`: **PASS** (`All checks passed!`)
- `uv run --no-sync ruff format --check .`: **PASS** (`144 files already formatted`)
- `uv run --no-sync mypy src tests tools`: **PASS** (`Success: no issues found in 142 source files`)
- Combined + Live-Redis integration suite (`KAIWEN_TEST_REDIS_URL=redis://127.0.0.1:16379/0`):
  - Command: `uv run --no-sync pytest -q tests/test_combined_beta_lifecycle.py tests/test_combined_beta_live_redis.py tests/test_events_redis_live_integration.py tests/test_events_redis_streams_live.py tests/test_notifications_durable_live.py tests/test_notifications_n4_multi_instance_live.py`
  - Result: **27 passed in 12.63s**
- Clean wheel & sdist build + isolated venv smoke tests: **PASS** (`BASE_WHEEL_OK 0.1.0b1`, `FULL_EXTRAS_WHEEL_OK`)
- Full offline `pytest -q`: **PASS** (`1197 passed, 21 skipped in 151.36s` — 19 opt-in `KAIWEN_TEST_REDIS_URL` tests separately verified live above; 2 synthetic echo adapter conformance upstream-failure hooks).

### 5.2 Independent Security, Reliability & Compatibility Review
- **Reviewer:** Independent `research` subagent (`55d8d915-3d73-4e1f-ba88-5fef648734cb`).
- **Findings:** `0` P0, `0` P1, `4` P2 (3 non-blocking test-hygiene observations — publishing a notification during in-process barge-in, removing unused `KAIWEN_BETA_STREAM_KEY`, and capturing subprocess stdout/stderr on startup timeout — resolved prior to PR push; 1 operational observation regarding idle `RedisPubSubEventBroker` health detection documented in §4).
- **Verdict:** **APPROVE**.

### 5.3 GitHub Actions CI
- Verified via Draft PR Fast CI matrix (`Quality & Tests` on Python 3.11, 3.12, 3.13 + `Package Build & Clean Wheel Smoke Test`).
