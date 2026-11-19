# Kaiwen Gateway — `v0.1.0b2` Beta Release Preparation Report

> **Repository:** `ImtheKaiwen/kaiwen-gateway`
> **Verified Starting `main` HEAD:** `20e97282423ed3710c68a7ec9523f68aa3754661` (PR #74 merged, post-merge Fast CI `44476116177` passed)
> **Candidate Branch:** `chore/beta-b2-release-prep`
> **Target Candidate Version:** `0.1.0b2` (**NOT YET PUBLISHED** — preparation only; latest published release on PyPI & GitHub Releases remains `v0.1.0b1`)
> **Experimental Beta Disclaimer:** Experimental beta — **production use is not supported**. No SLA, multi-host WAN/TLS load qualification, or live Kaiwen Agent / Kaiwen Speech / Verimor interoperability is claimed.

---

## 1. Exact Baseline & Candidate Metadata
- **Verified starting `main` HEAD:** `20e97282423ed3710c68a7ec9523f68aa3754661`
- **Existing published tag / release:** `v0.1.0b1` (`2026-11-16`, PyPI & GitHub Releases). `v0.1.0b2` is **not** tagged and **not** published.
- **Version dimensions preserved:**
  - Package version: `0.1.0b2` (candidate)
  - Core contract version (`CORE_CONTRACT_VERSION`): `"1.0"` (unchanged)
  - Wire protocol versions (`SYNTHETIC_PROTOCOL_VERSION`, `SYNTHETIC_MEDIA_PROTOCOL_VERSION`, `EVENTS_WS_PROTOCOL_VERSION`): `"v1"` (unchanged)

---

## 2. Published `v0.1.0b1` vs. Candidate `v0.1.0b2` (NOT YET PUBLISHED)

| Capability Area | Published `v0.1.0b1` (PyPI / GitHub Releases) | Candidate `v0.1.0b2` (NOT YET PUBLISHED) |
|---|---|---|
| **Gateway Core & ASGI Hosting** | Core registry, admission, draining, module lifecycle, CLI, `/health`, `/ready` | Preserved + additive principal identity & admission (`PrincipalId`, `VerifiedPrincipalContext`, `AdmissionController.admit_principal`) |
| **Voice & OpenAI Adapters** | Synthetic `/ws/synthetic/v1` & `/ws/synthetic-media/v1`, `VoiceBridgeSession`, `RealtimeAdapterConformanceSuite`, `openai_realtime`, `openai_gpt_live` | Preserved unchanged |
| **Events (`E1`–`E4`)** | Not included | `EventsModule`, `InMemoryEventBroker`, authenticated `/ws/events/v1`, optional `kaiwen-gateway[redis]` (`RedisPubSubEventBroker`, `RedisDistributedRateLimiter`, `RedisTicketReplayCache`, `RedisStreamsDurableStore`, `StreamRecoveryWorker`) |
| **Notifications (`N0`–`N4`)** | Not included | `NotificationsModule`, `NotificationEnvelope`, `derive_recipient_topic()`, `NotificationsDispatcher`, mandatory recipient isolation (`NotificationsTopicAuthorizer`, `ChainedNotificationsTopicAuthorizer`), `NotificationsDurablePublisher`, `NotificationsDurableWorker` |
| **Combined Acceptance Evidence** | Single-module Voice synthetic/live verification | Combined Core + Voice + Events + Notifications lifecycle, real multi-process Uvicorn + live Redis 7 + real `websockets` integration & failure-isolation suite (PR #74, `docs/GATEWAY_COMBINED_BETA_ACCEPTANCE_REPORT.md`) |

---

## 3. Complete Changed-File List (`chore/beta-b2-release-prep`)
- `pyproject.toml`: candidate version `0.1.0b1` → `0.1.0b2` (dependencies & extras unchanged)
- `src/kaiwen_gateway/__init__.py`: `__version__` `0.1.0b1` → `0.1.0b2`
- `uv.lock`: root editable package version `0.1.0b1` → `0.1.0b2` (1-line diff; zero third-party dependency changes)
- `.github/workflows/ci.yml`: synchronized clean-wheel smoke test wheel filename and version assertions (`0.1.0b1` → `0.1.0b2`)
- `tests/test_package.py` & `tests/test_public_api_compatibility.py`: synchronized version assertions to `0.1.0b2` and verified `pyproject.toml` version matches `kaiwen_gateway.__version__`
- `CHANGELOG.md`: prepared `[0.1.0b2] - Candidate Prepared (NOT YET PUBLISHED)` section preserving historical `[0.1.0b1]`
- `README.md`, `AGENTS.md`, `ROADMAP.md`, `docs/ARCHITECTURE.md`, `docs/PUBLIC_API_AND_COMPATIBILITY.md`: added `v0.1.0b2` candidate (NOT YET PUBLISHED) wording while preserving published `v0.1.0b1` PyPI install instructions and disclaimers
- `docs/BETA_B2_RELEASE_PREPARATION_REPORT.md`: this candidate readiness report

---

## 4. Version & Metadata Consistency Evidence
- `pyproject.toml` `[project].version`: `"0.1.0b2"`
- `kaiwen_gateway.__version__`: `"0.1.0b2"`
- `importlib.metadata.version("kaiwen-gateway")`: `"0.1.0b2"`
- CLI `kaiwen-gateway --version`: `kaiwen-gateway 0.1.0b2`
- Built wheel `.dist-info/METADATA`: `Version: 0.1.0b2`, `Provides-Extra: server`, `Provides-Extra: redis`, `Provides-Extra: openai`
- Built sdist: `kaiwen_gateway-0.1.0b2.tar.gz`

---

## 5. Public API & Dependency Compatibility Findings
- **Mandatory dependencies:** `dependencies = []` (zero mandatory third-party runtime dependencies preserved).
- **Optional extras isolation:** `[server]`, `[redis]`, `[openai]` cleanly isolated; `uv lock` diff changed zero third-party packages.
- **Public API & wire compatibility:** No breaking changes to Core, Voice, Events, or Notifications exports, endpoint paths, or protocol versions (`1.0` / `v1`).
- **Dependency vulnerability audit:** `uv audit --locked --preview-features audit-command` → **PASS** (`Found no known vulnerabilities and no adverse project statuses in 26 packages`).

---

## 6. Local Quality, Python Matrix & Integration Results
- **Environment:** Windows 11 (`win32`), Python `3.12.13`, `uv 0.13.7`.
- `uv lock --check`: **PASS** (`Resolved 27 packages in 1ms`)
- `uv run --no-sync ruff check .`: **PASS** (`All checks passed!`)
- `uv run --no-sync ruff format --check .`: **PASS** (`144 files already formatted`)
- `uv run --no-sync mypy src tests tools`: **PASS** (`Success: no issues found in 142 source files`)
- Focused packaging, version, public API, release workflow, and combined lifecycle suite (`tests/test_package.py`, `tests/test_public_api_compatibility.py`, `tests/test_release_workflow.py`, `tests/test_combined_beta_lifecycle.py`): **34 passed in 1.39s**
- Real-Redis integration & recovery suite (`KAIWEN_TEST_REDIS_URL=redis://127.0.0.1:16379/0`, disposable `redis:7-alpine`): **32 passed in 21.45s**
- Full offline `pytest -q`: **1199 passed, 24 skipped in 147.68s** (0 failures; 22 opt-in `KAIWEN_TEST_REDIS_URL` tests verified live above, 2 synthetic echo adapter conformance upstream-failure hooks)
- Python 3.11 / 3.12 / 3.13 matrix: Verified via GitHub Actions Fast CI on exact Draft PR HEAD (see §10).

---

## 7. Wheel & Sdist Inspection + Clean-Wheel Smoke Tests
- **Built artifacts (`uv build`):**
  - Wheel: `kaiwen_gateway-0.1.0b2-py3-none-any.whl` (`245,161` bytes, `82` files) — contains `kaiwen_gateway/core`, `modules/voice`, `modules/events`, `modules/events/redis`, `modules/notifications`, `transports/synthetic`, `transports/events`, `adapters/openai_*`, `py.typed`; **zero** `tests/` files leaked.
  - Sdist: `kaiwen_gateway-0.1.0b2.tar.gz` (`485,630` bytes, `140` files) — contains `src/kaiwen_gateway` and `pyproject.toml`.
- **Clean-wheel isolated virtualenv smoke tests (Python 3.12.13):**
  - Core-only (`kaiwen_gateway-0.1.0b2-py3-none-any.whl`): **PASS** (`CORE_ONLY_OK`, 1 package installed, `kaiwen-gateway 0.1.0b2`, `check-config` valid).
  - `[server]`: **PASS** (`demo --timeout 10.0` `Result: PASS`, 0 active connections/calls/leases; `demo --media --timeout 15.0` `Result: PASS`, 25 PCM16LE frames echoed, barge-in Turn 1 → Turn 2, 0 active connections/calls/leases).
  - `[redis]`: **PASS** (`REDIS_EXTRA_OK`, 2 packages installed: `kaiwen-gateway==0.1.0b2`, `redis==6.4.0`).
  - `[server,redis,openai]`: **PASS** (`ALL_EXTRAS_OK`, 10 packages installed; server, events transport, redis events, `openai_realtime`, `openai_gpt_live` all import cleanly).

---

## 8. Security & Release Workflow Safety Review (`.github/workflows/release.yml`)
- **Tag-to-version exact match:** Enforced (`EXPECTED_TAG="v${{ steps.version.outputs.version }}"` vs. `github.ref_name` on `release` events).
- **`workflow_dispatch` cannot publish:** Enforced (`if: github.event_name == 'release' && github.event.action == 'published' && !github.event.release.draft && vars.PYPI_PUBLISH_AUTHORIZED == 'true'`).
- **Explicit maintainer authorization guard:** `PYPI_PUBLISH_AUTHORIZED == 'true'` double-checked in both job `if` condition and `Verify maintainer publishing authorization` step.
- **Artifact validation & permissions:** `SHA256SUMS` verified before `pypa/gh-action-pypi-publish` (`skip-existing: false`); `id-token: write` scoped strictly to `publish-pypi` job in `pypi` environment.
- **Workflow changes made:** None (existing safeguards remain intact and verified by `tests/test_release_workflow.py`).

---

## 9. Findings & Preserved Experimental-Beta Limitations
- **Confirmed P0 / P1 Release Blockers:** `0`
- **Accepted Experimental-Beta Limitations (Preserved, Not Reclassified as Resolved):**
  1. **GitHub `#75`:** Linux OS-process `SIGTERM` shutdown verification with active mixed Gateway workloads (Windows `Popen.terminate()` is a hard `TerminateProcess`; ASGI lifespan graceful shutdown is verified via loopback `uvicorn.Server`).
  2. **GitHub `#76`:** Passive idle Redis Pub/Sub readiness detection policy (experimental-beta **Option A** retained; no autonomous polling change introduced).
  3. **GitHub `#68`:** Windows timer tick granularity flake in `test_delay_simulation` (separately tracked).
  4. **Deferred External Integrations:** Kaiwen Agent, Kaiwen Speech, and live Verimor PSTN/WSS interoperability remain out of scope for `v0.1.0b2`.
  5. **Non-Production Scope:** No production readiness, multi-host WAN/TLS load qualification, or operational SLA is claimed.

---

## 10. Independent Review & GitHub Actions CI Evidence
- **Independent Review (Internal Subagent `e9a9e0c6-f198-4caa-96de-f35ffbac2317` — Not GitHub Maintainer Approval):**
  - Caught **1 P0 CI blocker** (5 hardcoded `0.1.0b1` wheel/version lines in `.github/workflows/ci.yml`) — **resolved** by updating all 5 lines to `0.1.0b2`.
  - **0 P1** findings; **2 P2** items (report placeholders filled here; optional future `release.yml` wheel inspection assertions noted as non-blocking).
  - Confirmed zero security, public API, or release-workflow regressions.
- **GitHub Actions Fast CI (Draft PR on exact candidate HEAD):** Verified across `Quality & Tests (Python 3.11)`, `Quality & Tests (Python 3.12)`, `Quality & Tests (Python 3.13)`, and `Package Build & Clean Wheel Smoke Test`.

---

## 11. Remaining Maintainer Decisions (Before Any Publication)
1. Review and approve the Draft PR (`chore/beta-b2-release-prep`) for protected squash-merge into `main`.
2. Decide whether and when to create tag `v0.1.0b2`, publish a non-draft GitHub Release, and explicitly authorize PyPI Trusted Publishing (`PYPI_PUBLISH_AUTHORIZED`).
