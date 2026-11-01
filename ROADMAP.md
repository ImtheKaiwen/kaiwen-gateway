# Kaiwen Gateway — Development Roadmap

> **Status:** Active roadmap — M0 Complete (2026-11-01); M1 Not started (awaiting separate authorization)
> **Project phase:** Design / pre-implementation
> **Initial target release:** `0.1.0a1` (synthetic-environment alpha)
> **Last reviewed:** 2026-11-01
> **Repository:** `ImtheKaiwen/kaiwen-gateway`
> **Primary scope:** Reusable Python library and optional ASGI service; **Voice is the only V1 functional module**.

This document describes **the order in which Kaiwen Gateway will be built**, the evidence required to close each milestone, and the dependencies that can block the next stage. It is not an implementation report or a substitute for GitHub Issues and Pull Requests.

**Important status distinction:** The initial documentation baseline was published in commit `d4223509f50b67f556420138c669ca1f33ba0abd` and ADR-0001..0003 were accepted on 2026-11-01; **this approval covers the V1 architectural design only and must not be represented as an implemented package, tested integration, release, or deployment**. Update status only from verified repository and test evidence.

## 1. Sources of truth and decision authority

Read the following before acting on a milestone:

| Document | Authority / responsibility |
| --- | --- |
| [`docs/REQUIREMENTS.md`](docs/REQUIREMENTS.md) | **What** V1 must deliver; requirement IDs, P0/P1 priorities and acceptance gates. |
| [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md) | **How** the approved design separates Core, transports, Voice, provider adapters and Kaiwen Agent. |
| [`docs/decisions/`](docs/decisions/) | **Why** material architecture decisions were chosen; proposal/acceptance/supersession history. |
| [`AGENTS.md`](AGENTS.md) | Rules governing Antigravity, Codex and other coding agents, including permission boundaries and reporting. |
| **`ROADMAP.md`** | **When** approved work is attempted; milestones, status, dependencies and exit evidence. |
| [`CHANGELOG.md`](CHANGELOG.md) | User-visible changes under `Unreleased` and historical releases, **not** future plans. |
| [`SECURITY.md`](SECURITY.md) | Vulnerability disclosure, supported versions and security contact policy. |
| [`CONTRIBUTING.md`](CONTRIBUTING.md) | Development workflow, PR review, testing and contribution process. |
| [`README.md`](README.md) | Public-facing description and *verified* install/use instructions. |

An approved user task and applicable security restrictions take precedence over speculative plans. If documents disagree, report the conflicting passages and obtain a resolution; do not silently rewrite the requirements to fit an easier implementation. ADRs initially marked `Proposed` **require explicit acceptance** before treating their trade-offs as settled project policy.

## 2. Delivery principles

1. **One milestone at a time.** Complete the requested stage, present evidence, and stop for maintainer review. Starting the next milestone is a separate authorization.
2. **Make progress verifiable.** A milestone is complete only when its deliverables exist and its required tests/reviews pass. A checklist is not evidence.
3. **Build the smallest useful foundation.** Do not create future modules, dashboards, service meshes, distributed schedulers or an elaborate plugin system before there is a real consumer.
4. **Keep media separate from control.** Stream audio using bounded, low-overhead paths; represent lifecycle and safety events separately. No unnecessary persistent queues, event buses or JSON conversion for raw media.
5. **Protect trust boundaries.** Verify provider identity before allocating paid AI resources. Derive tenant, call and tool authority from server-controlled policy, never caller audio or untrusted message fields.
6. **Reuse Kaiwen Agent.** Prefer its versioned public realtime interfaces and policy/tool infrastructure. Avoid duplicating an agent loop or logical session manager in Gateway.
7. **Use synthetic data first.** No real customer audio, calls or confidential provider specifications in an open repository, public issue, CI artifact or default telemetry.
8. **Distinguish verification levels.** Synthetic simulator success is not verified Verimor interoperability; an alpha demo is not a live pilot; a pilot is not production readiness.
9. **Reserve releases and deployment for explicit approval.** Publishing to PyPI, pushing a public container, creating release tags or modifying live infrastructure require separate authorization.
10. **Respect measured limits.** Concurrency stages and latency figures are test goals or observations, never capacity promises without repeatable measurement.

## 3. Status vocabulary and initial snapshot

Use only these milestone status values:

| Status | Meaning |
| --- | --- |
| `Not started` | No implementation work has been verified for this milestone. |
| `In progress` | Specifically authorized work has begun; deliverables or evidence are incomplete. |
| `Blocked` | A documented external dependency or decision prevents required completion. |
| `Ready for review` | Proposed deliverables and evidence exist; maintainer acceptance is pending. |
| `Complete` | Maintainer accepted the milestone and the required evidence is linked. |

**Initial milestone status:**

| ID | Milestone | Initial status | Depends on |
| --- | --- | --- | --- |
| **M0** | Documentation baseline and approval | `Complete` (closed 2026-11-01; commits `d422350`, `27ab0f9`, `4a8fe82`) | Maintainer review |
| **M1** | Python package and CI foundation | `In progress` (M1.1 merged in `#1`; M1.2 local quality/test foundation implemented; M1.3–M1.4 pending) | M0 |
| **M2** | Core contracts, identity and security/admission | `Not started` | M1 |
| **M3** | ASGI/WSS ingress and synthetic telephony adapter | `Not started` | M2 |
| **M4** | Voice audio bridge and Kaiwen Agent integration | `Not started` | M3 |
| **M5** | Resilience, simulation, security and load evidence | `Not started` | M4 |
| **M6** | Alpha packaging and release-readiness review | `Not started` | M5 |
| **M7** | Conditional Verimor pilot | `Blocked` (official protocol and authorizations) | M6 + `OPEN-*` dependencies |
| **M8** | Production-readiness decision (separate track) | `Not started` | M7 evidence and separately approved operations plan |

Milestone IDs are stable; do not renumber completed milestones. Update the status table and evidence links after a reviewed change. If a stage is partially complete, keep it `In progress` or `Ready for review` rather than marking it `Complete`.

## 4. Milestone plans and exit criteria

### M0 — Documentation baseline and scope approval

**Goal:** Establish a consistent, reviewable agreement before implementation.

**Deliverables**

- [x] `README.md` updated to accurately describe the *planned*, not-yet-implemented product.
- [x] `docs/REQUIREMENTS.md` defines requirements, priorities, `AT-*` scenarios and Gates A–D.
- [x] `docs/ARCHITECTURE.md` describes boundaries, trust flow, lifecycle, media/control paths and deployment assumptions.
- [x] `docs/decisions/0001-modular-monolith.md` reviewed and accepted or revised (`Accepted`, 2026-11-01).
- [x] `docs/decisions/0002-hexagonal-architecture.md` reviewed and accepted or revised (`Accepted`, 2026-11-01).
- [x] `docs/decisions/0003-library-and-server.md` reviewed and accepted or revised (`Accepted`, 2026-11-01).
- [x] `AGENTS.md` defines agent permissions, quality checks, documentation rules and stop conditions.
- [x] `ROADMAP.md`, `CHANGELOG.md`, `SECURITY.md`, `CONTRIBUTING.md`, and `LICENSE` (MIT) created with accurate statuses/policies.
- [x] Links, scope exclusions, dependency direction, proposed package/API status and terminology cross-checked (117 relative links, 7 anchors, 106 unique IDs verified).
- [x] Maintainer explicitly approves the documentation baseline and enables/verifies GitHub Private Vulnerability Reporting ([`SECURITY.md §8`](SECURITY.md#8-security-verification-and-release-gates)); M1 authorization remains a separate step (Gate A and ADR-0001..0003 accepted 2026-11-01; Private Vulnerability Reporting verified as `{"enabled": true}` via GitHub API on 2026-11-01; M1 remains `Not started`).

**Exit evidence:** Baseline published in commit `d4223509f50b67f556420138c669ca1f33ba0abd` and updated with accepted ADRs and MIT `LICENSE` in commit `27ab0f96c42ad150ca7b1191fa94e78bdf38e452`; reviewed file inventory (11 Markdown files + MIT `LICENSE`), automated relative link/anchor verification (117 links, 7 anchors), requirement traceability verification (106 unique IDs), **Gate A** acceptance in [`docs/REQUIREMENTS.md`](docs/REQUIREMENTS.md), `ADR-0001`..`0003` marked `Accepted`, and GitHub Private Vulnerability Reporting verified as `{"enabled": true}` (`2026-11-01`). Closing M0 does **not** automatically authorize M1; M1 remains `Not started` until separately authorized.

**Prohibited during M0:** Implementing transport/Voice code; adding production keys; launching deployments; publishing releases; treating `Proposed` ADRs as accepted without approval.

### M1 — Reproducible package and engineering foundation

**Goal:** Create the smallest importable, installable Python package with dependable local and CI checks.

**Deliverables**

- [x] `src/kaiwen_gateway/` package with intentionally minimal public exports and no import side effects (M1.1: `src/kaiwen_gateway/__init__.py` and `src/kaiwen_gateway/py.typed`, merged in `#1` / `b0a666e`).
- [x] `pyproject.toml` with Hatchling, Python 3.11+, metadata, typed-package marker where appropriate, and justified dependencies/extras (M1.1–M1.2: Hatchling, Python `>=3.11`, `py.typed`, zero runtime dependencies, and `dependency-groups.dev` for Ruff, mypy, pytest, and pytest-cov).
- [ ] `uv` project/lock setup and documented local commands; resolve supported Python version policy (M1.1–M1.2 `uv.lock`, `uv sync --locked --group dev`, and local lint/typecheck/test/build commands are verified; CI multi-version Python matrix policy remains pending in M1.3–M1.4).
- [x] Ruff formatting/lint, strict mypy and pytest configuration; small unit-test harness (M1.2: configured in `pyproject.toml` and verified on `src` and `tests`).
- [x] `tests/` layout and a clean-environment wheel build/install/import smoke test (M1.1–M1.2: `tests/test_package.py` unit/subprocess tests and isolated wheel/sdist smoke verification).
- [ ] GitHub Actions **fast CI** for checks, tests and wheel validation, with minimal permissions.
- [ ] Basic security/dependency/workflow hygiene (e.g., least-privilege Actions, dependency update policy and checks as available).
- [x] README developer setup instructions updated **only after commands are verified** (M1.1–M1.2 verified local `uv` quality/test/build commands documented in `README.md`, `CONTRIBUTING.md`, and `AGENTS.md`).

**Exit evidence:** Passing formatter/linter/typecheck/tests/build in documented environment; workflow run link or explicit explanation why no remote run exists; installed-wheel smoke result. Package install must not start a server or contact any provider.

**Relevant IDs:** `FR-CORE-001`, `FR-CORE-005`, `ENG-*`, `CI-001` through `CI-003`, `AT-001` (package portion).

**Not part of M1:** WSS endpoint, fake Verimor traffic, AI provider connectivity, speculative plugin architecture or automatic publishing.

### M2 — Core composition, trusted identity, admission and lifecycle

**Goal:** Implement a small, framework-neutral core that can be embedded in more than one host instance.

**Deliverables**

- [ ] Typed module registration and lifecycle contracts; duplicate/incompatible registration rejection.
- [ ] Explicit composition/app construction with independent instances; no shared mutable process-wide call registry.
- [ ] Validated, typed configuration; reject insecure or incomplete ingress configuration before accepting traffic.
- [ ] Trusted, immutable connection/call identity and tenant policy boundaries.
- [ ] Connection state machine, admission limits and correctly released capacity reservations.
- [ ] Timeouts, task/lifecycle ownership and safe termination contracts with deterministic fake transports.
- [ ] Core log/metric interfaces that do not require production monitoring infrastructure at import time.
- [ ] Contract, concurrency, negative-auth, cancellation and cleanup tests for these components.

**Exit evidence:** Two-instance isolation test; forged identity cannot change tenant/call scope; duplicate and over-capacity admission tests; no resource leak in cancellation/closure fixtures; no `core` import of Voice, FastAPI, Verimor or OpenAI SDKs.

**Relevant IDs:** `FR-CORE-002` through `FR-CORE-006`, `FR-CONN-002` through `FR-CONN-006`, `SEC-002` through `SEC-005`, `OBS-001` through `OBS-003` (interfaces/base coverage).

**Architecture check:** ADR-0001 and ADR-0002 import boundaries are enforced or verified explicitly; agent domain concerns remain outside Gateway Core.

### M3 — ASGI/WSS server and synthetic telephony boundary

**Goal:** Expose a safe testable inbound connection path **without inventing Verimor's actual protocol**.

**Deliverables**

- [ ] Optional ASGI host and CLI configuration/start path; CLI names selected and verified in tests.
- [ ] Versioned synthetic WSS endpoint contract and telephony adapter port.
- [ ] **Project-owned fake telephony wire protocol**, explicitly named synthetic in code, examples and tests.
- [ ] Authenticated WSS handshake, per-connection identity binding, input schema/frame-size validation and rejection before upstream allocation.
- [ ] Connection close/error mapping, request/resource limits, idle handling as relevant to current scope.
- [ ] Synthetic WSS test client and basic handshake/security/integration tests.
- [ ] Documented boundary between public WSS/TLS deployment requirements and local in-process test transport.

**Exit evidence:** Unauthorized handshakes fail safely; fake transport connects; malformed/oversized input is rejected; no AI provider session is created before authorization; generated/written synthetic contract matches tests; clean-wheel CLI/app smoke test.

**Relevant IDs:** `FR-CORE-002`, `FR-CORE-007`, `FR-CONN-001` through `FR-CONN-006`, `FR-INT-001`, `FR-INT-006`, `SEC-001` through `SEC-004`, `AT-003`, `AT-005`.

**Explicit boundary:** The synthetic handshake and event format must **not** be called Verimor-compatible or described as tested against a real Verimor service.

### M4 — Voice media bridge, realtime adapter and interruption

**Goal:** Complete a bidirectional, single-call and multi-call **synthetic** voice bridge using the shared Gateway Core.

**Deliverables**

- [ ] Voice-local media/domain contracts and provider-neutral telephony/AI adapter interfaces.
- [ ] Two lifetime-bound concurrent audio pumps: telephony → AI and AI → telephony.
- [ ] Provider-specific codec negotiation/conversion **only when evidence requires it**; validate frame formats/order/timing.
- [ ] Bounded media queues/backpressure, precise cancellation and idempotent cleanup.
- [ ] Barge-in/unplayed-output handling according to **verified adapter capabilities**.
- [ ] Fake AI adapter and independent per-call session state; concurrency/isolation fixtures.
- [ ] Kaiwen Agent integration adapter using **public, compatible APIs**; no duplicate agent runtime.
- [ ] OpenAI Live contract review against current official docs; an authorized sandbox smoke test if credentials/approval are available.
- [ ] Clear capability matrix distinguishing synthetic operation, reviewed contract and real provider-verified behavior.

**Exit evidence:** Deterministic bidirectional audio test; two tenants/calls never cross frames or context; interruption test demonstrates correct unplayed-output policy for the mock; forced close cleans upstream and both tasks; documented evidence or unresolved limitation for OpenAI Live real smoke test.

**Relevant IDs:** `FR-VOICE-001` through `FR-VOICE-008`, `FR-INT-002` through `FR-INT-005`, `SEC-003`, `SEC-007`, `SEC-008`, `AT-002`, `AT-004`, `AT-006`, `AT-007`.

**Release caveat:** If a P0 external smoke test is unauthorized or unavailable, record the precise exception/blocker under Gate B; do **not** claim live OpenAI interoperability based solely on mock tests.

### M5 — Simulator, failure handling, security and measured quality

**Goal:** Demonstrate predictable behavior under disconnects, malformed frames, slow consumers and concurrent calls.

**Deliverables**

- [ ] Reproducible simulator with synthetic speech/frame fixtures and explicit scenario inputs/seeds.
- [ ] Failure matrix covering provider rejection, rate limiting, timeout, shutdown, invalid frames, repeated events and overload.
- [ ] Controlled fallback or clean termination. Human transfer only if actually supported and separately verified.
- [ ] Replay/duplicate-start handling appropriate to the initial process model; defer distributed atomic storage to the live pilot gate where allowed.
- [ ] No-storage-by-default and redaction tests for logs, traces, errors and CI artifacts.
- [ ] In-process performance instrumentation (queue duration, bridge processing latency, connection counts, rejects, provider errors).
- [ ] Reproducible synthetic load tests at **1 → 5 → 25 → 50 → 100** concurrent calls as environment/capacity permits, measuring p50/p95/p99, CPU, memory, queue growth and failure rate.
- [ ] Scheduled/manual benchmark workflow when stable; security and dependency scans without leaking test data.

**Exit evidence:** `AT-001` through `AT-008`, `AT-010` and `AT-011` test reports where applicable; leak/cleanup evidence; environment and traffic profile included in benchmark artifact; regressions documented rather than masked by generous averages.

**Relevant IDs:** `FR-OPS-002` through `FR-OPS-004`, `FR-INT-008`, `SEC-*`, `NFR-*`, `CI-004`, `CI-005`, `AT-005` through `AT-011`.

**Performance truthfulness:** Load stages are **targets for evaluation**, not verified throughput or service guarantees. Do not impose an arbitrary 100-call release threshold if measurements show infrastructure/provider limits; report actual sustainable capacity and obtain explicit approval for changed acceptance expectations.

### M6 — Alpha release-readiness (`0.1.0a1`)

**Goal:** Prepare a public, clearly labeled **synthetic-environment alpha** that another developer can install and test without private credentials.

**Deliverables**

- [ ] Gate B checked against every P0 requirement, with documented and approved exceptions for unavailable external verification.
- [ ] Wheel and sdist built, metadata checked, installed in clean environment; dependency extras validated.
- [ ] Working, verified minimal demo and CLI reference; examples use synthetic data and fake telephony.
- [ ] Container build and documented startup/readiness/draining behavior **to the extent required by the selected alpha/pilot boundary**; P1 production operations may remain deferred and explicitly identified.
- [ ] Public API/wire contract docs distinguish mock behavior from real Verimor/OpenAI support.
- [ ] Release automation uses least-privilege credentials, provenance/trusted publishing where supported, reviewed version and immutable artifacts.
- [ ] `CHANGELOG.md` has a reviewed `Unreleased` entry and an accurate alpha release section **only once a release actually occurs**.
- [ ] Maintainer approves the actual publishing/tagging/container-push action separately.

**Exit evidence:** Gate B acceptance record; CI run/test links; package install/build/CLI output; artifact digests; release notes and risks. **A release candidate passing checks is not the same as publishing a release.**

**Relevant IDs:** `AT-001` through `AT-011` as required by Gate B, `CI-006`, `CI-007` where applicable, `FR-OPS-001`, `FR-OPS-006` (deployment readiness where in scope).

**Definition:** `0.1.0a1` is an **intent**, not a promise that a specific public PyPI artifact exists.

### M7 — Conditional real Verimor pilot (not a prerequisite for synthetic alpha)

**Goal:** Prove an authorized, narrow real-world integration **only after** vendor and data-processing prerequisites are satisfied.

**Prerequisites / external blockers**

- [ ] Official Verimor WSS handshake and direction, authentication, binary/JSON framing, codecs, sample rate/timestamps, call control, playback/cancel and close semantics (`OPEN-001`, `OPEN-002`).
- [ ] Current OpenAI Live contract confirmed against live sandbox (`OPEN-003`).
- [ ] Legal/customer permission and data-processing/privacy review, including international transfer if relevant (`OPEN-004`).
- [ ] Deployment region, network restrictions, approved provider budgets/quotas and expected concurrency (`OPEN-005`).
- [ ] Real call handoff/transfer capability either confirmed or explicitly documented as unsupported (`OPEN-006`).

**Deliverables**

- [ ] Real Verimor adapter implements **only documented, verified** capabilities; sanitized fixtures only when permitted.
- [ ] Production-style authentication, replay protection, key rotation and network restrictions for the chosen deployment.
- [ ] Health/readiness/draining and per-call failure policy exercised in staging.
- [ ] Logs, traces, metrics, alert thresholds and operator runbooks available without recording confidential call content.
- [ ] Limited authorized sandbox/phone-call pilot with documented results, quota controls and responsible operators.
- [ ] Explicit pilot go/no-go decision, remaining risks and remediation plan.

**Exit evidence:** `AT-009`, `AT-012`, `AT-013`, applicable P1 requirements and **Gate C** reviewed with actual provider and legal/operations evidence. If technical specs never arrive, M7 stays `Blocked` while the generic Gateway alpha can still exist.

**No automatic advancement:** A successful synthetic test, public package, vendor marketing page or an internal agreement is not authorization to place real calls or use customer recordings.

### M8 — Production readiness (separate decision and future work)

**Goal:** Decide whether a specific deployment meets a real organization's reliability, security, legal and on-call requirements.

**Possible work, subject to measured need:**

- Deployment-specific SLOs/error budgets and capacity testing.
- Multi-instance admission coordination and durable replay/lease semantics when horizontal scaling is justified.
- Safe rolling deployments, backup/recovery expectations, secret rotation and controlled rollback drills.
- Incident response, monitoring/on-call ownership, access review and data retention/deletion verification.
- Documented tenancy and operational responsibilities between Gateway, Kaiwen Agent and the consuming organization.

**Exit evidence:** Separate production readiness review (**Gate D**) with recorded approvals and measurable operational evidence. M8 does **not** authorize Kubernetes, a control dashboard, broad provider support or microservices by default.

## 5. Milestone dependencies and review rhythm

```text
M0 Documentation acceptance
  └── M1 Package / CI foundation
        └── M2 Core identity, admission and lifecycle
              └── M3 ASGI / WSS + synthetic telephony boundary
                    └── M4 Voice bridge + Kaiwen Agent adapter
                          └── M5 Simulation, hardening and evidence
                                └── M6 Synthetic alpha readiness
                                      └── M7 Real Verimor pilot (conditional)
                                            └── M8 Production review (separate)
```

Implementation may split a milestone into small PRs, but a PR is not a new release gate. The maintainer explicitly authorizes the next milestone. If work is blocked, the agent may report a smaller safe alternative **within the current authorized scope**; it may not silently skip a security requirement or claim a higher release gate.

**Required handoff after each milestone** (see [`AGENTS.md`](AGENTS.md)):

1. Scope attempted and final status (`Complete`, `Ready for review`, `Blocked`, etc.).
2. Files changed and why; public API/contract changes highlighted.
3. Tests/commands actually run, including exact successes, failures and unavailable checks.
4. Security, privacy and performance considerations; any observed metrics with environment details.
5. Decision records added/updated, and requirement IDs or acceptance scenarios addressed.
6. Commit/PR reference **only if actually created**; no fabricated links or status claims.
7. Open risks, unresolved vendor dependencies and the recommended *next proposed* milestone (without starting it).

## 6. Acceptance and release-gate matrix

The canonical pass criteria live in [`docs/REQUIREMENTS.md`](docs/REQUIREMENTS.md#7-acceptance-scenarios-and-release-gates). This table only maps them to the development sequence; it must not redefine their content.

| Gate | First assessed | Meaning | Critical boundary |
| --- | --- | --- | --- |
| **Gate A** | M0 | Approved docs and implementation authorization | No implementation without a coherent baseline and explicit permission. |
| **Gate B** | M6 | `0.1.0a1` synthetic alpha eligibility | `AT-001`–`AT-008`, `AT-010`, `AT-011` plus the documented `AT-009` local-safety treatment and P0 requirements. |
| **Gate C** | M7 | Limited authorized Verimor pilot | Real provider interoperability, privacy authorization, replay protection and operational readiness. |
| **Gate D** | M8 | Deployment-specific production readiness | Separate decision; passing Gate C is not enough. |

**Cross-cutting verifications:**

| Topic | Key scenarios | Main milestone(s) |
| --- | --- | --- |
| Clean installation / CLI | `AT-001` | M1, M3, M6 |
| Authorized synthetic calls | `AT-002`, `AT-003` | M3, M4 |
| Tenant/call isolation | `AT-004` | M2, M4, M5 |
| Invalid frames, overload, interruption | `AT-005`, `AT-006` | M3, M4, M5 |
| Cancellation / cleanup | `AT-007` | M2, M4, M5 |
| Provider error/fallback | `AT-008` | M5 |
| Duplicate call start | `AT-009` | M5 local safeguard, M7 durable/atomic pilot requirement |
| Synthetic concurrency metrics | `AT-010` | M5, M6 |
| No sensitive data leakage | `AT-011` | M2–M6 |
| Draining | `AT-012` | M7, or earlier if available |
| Real Verimor media contract | `AT-013` | M7 **only after official specification** |

## 7. Open dependencies and risk register

These items are carried from [`docs/REQUIREMENTS.md`](docs/REQUIREMENTS.md#8-dependencies-assumptions-and-unresolved-questions), not assumed resolved:

| Item | Risk if guessed | Required evidence / decision | Blocking scope |
| --- | --- | --- | --- |
| `OPEN-001` — Verimor handshake/auth/direction | Unauthorized or incompatible WSS integration | Official vendor contract or approved capture | M7 |
| `OPEN-002` — media/codec/control semantics | Corrupt audio, failed interruption, dropped calls | Verified wire examples/capability matrix | M7 |
| `OPEN-003` — OpenAI Live current protocol | Fake success misrepresented as live integration | Current official spec + authorized sandbox smoke | Live interoperability claim |
| `OPEN-004` — data/consent/legal responsibilities | Personal data misuse or unlawful transfer | Documented organization approval and legal/privacy assessment | Any real customer processing |
| `OPEN-005` — deployment, quotas and traffic | Unsupportable capacity/cost promises | Target region, provider quotas, benchmark profile | Pilot capacity/SLO |
| `OPEN-006` — call handoff/transfer | Impossible fallback advertised as functional | Real vendor capability validation or graceful-close fallback | Transfer feature claim |
| `OPEN-007` — final public API/CLI/extras | Unstable/inaccurate docs | Verified implementation and compatibility tests | Final alpha Quickstart |

A blocker is closed only with a verifiable source, test or accepted decision. External vendor uncertainty does not justify a speculative implementation in Core.

## 8. CI/CD and observability rollout

Introduce tooling when it provides a real quality gate. Workflow names are **proposals**, not claims that files already exist.

| Stage | Continuous quality work | Operational visibility |
| --- | --- | --- |
| M1 | Fast CI: format, lint, strict types, unit tests, build/smoke | Test runner output and machine-readable results |
| M2–M3 | Core/security/ASGI contract tests; workflow/secret hygiene | Structured lifecycle logs and safe correlation fields |
| M4 | Voice adapter and interruption tests; compatibility checks | Queue/bridge timing, active sessions, provider errors |
| M5 | Security scanning and manual/scheduled fault/load workflow | p50/p95/p99, resource use, error-rate and leak reports |
| M6 | Reproducible release validation, artifact provenance if available | Release health and documented known limitations |
| M7 | Staging smoke/deploy approval and rollback tests | Tracing, alerts, operator runbooks, readiness/draining |

Do not run paid live provider tests from untrusted public PRs. Do not grant publishing tokens to ordinary CI jobs. Benchmark runners differ: track environment, traffic profile and repeated measurements instead of treating a single noisy timing number as a hard performance guarantee.

## 9. Deferred ideas — deliberately **not** V1 implementation commitments

These may be explored in future planning only with a specific use case, review and an ADR if boundaries change:

- Messaging, Events/Webhooks, generalized AI Provider Proxy, Tool/MCP Gateway and additional communication modules.
- Visual Gateway Control Center, tenant self-service management, SaaS billing or plugin marketplace.
- Local Kaiwen Voice inference or a separate speech training pipeline.
- SIP/RTP bridging where the confirmed provider contract calls for it; not a disguised replacement for verified WSS.
- Microservice decomposition, Kubernetes, Kafka, always-on Redis/PostgreSQL or automatic migration of active calls between workers.
- Broad multi-vendor telephony support, outbound call campaigns, arbitrary tool execution or customer-specific CRM logic.

A future idea becomes committed scope **only after** the maintainer approves an updated requirement/roadmap and, if architectural, an ADR. Do not create empty placeholder packages for these ideas.

## 10. Roadmap maintenance protocol

**At each reviewed milestone or significant scope change:**

1. Update its `Status` in the milestone table using actual evidence; never mark a milestone done solely because code was generated.
2. Check off individual deliverables only when verified in the repository and tests; note exceptions in the relevant PR/issue rather than silently reinterpreting a checkbox.
3. Record the corresponding GitHub Issue/PR and CI links when available. Use Issues/PRs for task breakdown; **do not duplicate a daily to-do list here**.
4. Update [`CHANGELOG.md`](CHANGELOG.md) under `Unreleased` for meaningful completed user-facing changes, not for intentions or design-only tasks unless they are public documentation changes worth noting.
5. When public protocols, trust boundaries, deployment assumptions or module responsibilities change, update requirements/architecture and add or supersede an ADR in [`docs/decisions/`](docs/decisions/).
6. Reassess `OPEN-*` blockers with cited evidence. Keep unverified provider details explicitly unresolved.
7. Record the next milestone as **proposed**. Do not push, tag, publish, deploy or start the next stage without the task-specific authorization required by `AGENTS.md`.

### Change history (this roadmap only)

| Date | Change | Status |
| --- | --- | --- |
| 2026-10-31 | Drafted M0–M8 plan and synthetic-alpha/pilot/production boundaries | Proposed; not a record of completed implementation |
| 2026-11-01 | Published M0 baseline (`d422350`), accepted ADR-0001..0003 and Gate A (`27ab0f9`), added MIT `LICENSE`, verified GitHub Private Vulnerability Reporting (`{"enabled": true}`), and closed M0 | `Complete` (M0 closed; M1 remains `Not started`) |

---

**V1 success means:** A reviewer can install and run the same reusable Gateway components in library/server form, exercise a secure synthetic bidirectional Voice session, observe failure/cancellation behavior, and reproduce the tests. **Real Verimor interoperability and production readiness remain separate, evidence-driven decisions.**
