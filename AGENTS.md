# Kaiwen Gateway — Coding Agent Instructions

> **Repository:** `ImtheKaiwen/kaiwen-gateway`
> **Package:** `kaiwen-gateway` / import `kaiwen_gateway`
> **Product phase:** Design / pre-implementation
> **Target first alpha:** `0.1.0a1`
> **Document status:** Active operating rules (M0 baseline accepted 2026-11-01)
> **Last reviewed:** 2026-11-01

This file governs autonomous and assisted coding work in the Kaiwen Gateway repository, including work performed by Antigravity, Codex, and other coding agents. It is a **repository-level working contract**, not permission to expand the project, deploy a system, or claim that unimplemented features exist.

**Primary operating principle:** Deliver the smallest verified change that satisfies the assigned milestone while preserving architectural boundaries, user privacy, and the maintainability of the public library.

## 1. Read before modifying anything

At the beginning of each task, inspect the relevant source files and read these documents in this order:

1. [`docs/REQUIREMENTS.md`](docs/REQUIREMENTS.md) — authoritative V1 scope, requirement IDs, priorities, verification and release gates.
2. [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md) — components, dependency direction, state, transport and security boundaries.
3. Applicable records under [`docs/decisions/`](docs/decisions/) — architectural decisions, current status, trade-offs and supersession history.
4. [`ROADMAP.md`](ROADMAP.md) — planned milestone, actual progress, dependencies and blockers.
5. [`CONTRIBUTING.md`](CONTRIBUTING.md) and [`SECURITY.md`](SECURITY.md) — contribution and disclosure practices.
6. [`README.md`](README.md) and [`CHANGELOG.md`](CHANGELOG.md) — public-facing project description and user-visible changes.

If a referenced document does not exist yet, **report the missing dependency**; do not invent its content or claim it was reviewed. Do not use external or historical design proposals to override checked-in decisions without explicitly raising the conflict.

**Authority precedence for ordinary implementation:** user-approved task scope and repository policy; V1 requirements/acceptance gates; accepted ADRs and architecture; approved roadmap milestone; implementation and tests. If two sources disagree, stop the conflicting work, identify the exact sections and propose a resolution. Do not silently choose the easier interpretation. Security requirements and explicit user restrictions cannot be bypassed by another document.

ADRs marked `Proposed` describe intended direction, **not approval by themselves**. Their implementation boundaries are the current planning baseline, but the maintainer must accept any material change or final public contract.

## 2. Scope discipline and agent autonomy

### 2.1 Work one reviewed milestone at a time

- The maintainer will provide a **bounded milestone**. Implement only that milestone and its directly necessary tests, documentation and configuration.
- Stop at the end of the assigned stage and report evidence. **Do not automatically begin the next stage.**
- Within the approved stage, make ordinary local implementation choices independently if they preserve established contracts.
- Ask for a decision **before** expanding V1 scope, changing module boundaries, selecting a different architecture, changing security ownership, introducing mandatory infrastructure, exposing a new external protocol, or adding significant dependencies.
- If requirements or provider documentation are missing, implement an explicitly labeled fake/test boundary or report a blocker; do not improvise production behavior.
- Avoid unrelated refactors, cosmetic churn, unnecessary generated scaffolding, and speculative future modules.

### 2.2 Explicit authorization required

Never perform these actions merely because they appear convenient:

- Publish a Python package, push a container image, tag a release, deploy to production, change DNS, or provision paid resources.
- Modify live infrastructure, production credentials, provider account settings, or customer-facing telephone routing.
- Collect, upload, record, replay, or transmit real customer audio, telephone numbers, transcripts, or other personal data.
- Run live provider tests that incur cost or process non-synthetic data without explicit authorization and approved test credentials.
- Delete repositories, remove project history, rewrite shared Git history, force-push, or make destructive repository changes.
- Add a new class of agent tools, expand tool permissions, disable safety controls, or bypass required approvals.
- Push commits or merge PRs unless the current task explicitly authorizes those actions. A code change request is not, by itself, permission to release or deploy.

Use only authorized accounts and environments. Never bypass provider terms, consent requirements, or access controls.

### 2.3 Current hard scope

**V1:** reusable Python library plus optional ASGI service/CLI; a small explicit module registry; authenticated WSS; one Voice module; synthetic telephony adapter/simulator; Kaiwen Agent-backed realtime AI integration; call isolation; bounded audio streaming; interruption; cleanup; safe telemetry; packaging and verification.

**Conditional:** real Verimor adapter and live telephone interoperability only after the authoritative provider contract and authorized sandbox tests are available.

**Not V1:** Messaging/Event/MCP gateways, general AI proxy, control dashboard, local Kaiwen Voice inference, SIP bridge without a separate approved specification, Kubernetes, Kafka, microservice decomposition, distributed call migration, plugin marketplace, CRM/billing system, or large-scale outbound-calling automation.

Do not create empty future-module directories simply to make the repository look complete.

## 3. Non-negotiable architecture rules

### 3.1 Three accepted design directions to review

- **ADR-0001:** Modular Monolith — one codebase and deployable service; independently bounded internal modules.
- **ADR-0002:** Hexagonal Architecture — inward dependencies, typed ports, replaceable adapters and explicit composition.
- **ADR-0003:** Library and Server — one Python distribution; minimal importable library core; optional hosting and CLI, not two implementations.

ADR-0001 through ADR-0003 were accepted by the maintainer on 2026-11-01 as the V1 architectural design baseline (not as proof of runtime implementation). Always read the **actual ADR status** in `docs/decisions/` before describing any future ADR as formally accepted.

### 3.2 Dependency direction

```text
core                      -> standard library + minimal common types
voice.domain              -> voice-local types + allowed core contracts
voice.application         -> voice domain/ports + core contracts
voice.adapters            -> corresponding voice ports + optional provider clients
transports                -> core ports + optional ASGI framework
server / CLI              -> explicit composition of ports, adapters and modules
```

**Forbidden:**

- `core` importing Voice, FastAPI, Starlette, Uvicorn, Verimor, OpenAI SDKs, or a product application.
- Voice domain/application code using ASGI `WebSocket` or SDK connection objects directly.
- An adapter importing another provider's concrete adapter or setting global process state.
- `kaiwen-agent` importing or depending on `kaiwen-gateway`.
- Reimplementing Kaiwen Agent's agent loop, realtime session manager, tool execution, general authorization pipeline, or scheduler inside Gateway.
- Global mutable call registries shared by independently created Gateway instances.
- Starting a server, contacting a provider, or spawning background tasks as a side effect of `import kaiwen_gateway`.

Gateway **may depend on documented public APIs** of `kaiwen-agent` through an adapter. Inspect the version actually selected, not assumptions from a separate repository. When a required capability is absent, report the gap or propose a small upstream change; never import unstable private internals as a shortcut.

### 3.3 Minimal, meaningful contracts

- Use typed, explicit module registration and application factories. Reject duplicate modules and incompatible configurations deterministically.
- Design ports around **required behaviors**, not a large generic interface assembled for hypothetical providers.
- Keep provider framing, authentication peculiarities, codec negotiation and proprietary event names inside adapters.
- Model each call's trusted identity and state explicitly. Domain types must not allow external payloads to silently replace verified identity.
- Keep the **media plane** (audio bytes/frames) separate from the **control plane** (start, stop, interruptions, errors, audit events).
- Do not send high-frequency raw media through a general event bus, database, persistent queue or JSON serialization layer without a demonstrated requirement and approved ADR.
- Minimize copies/transcoding where safe, but never trade away validation, isolation or predictable overload behavior.

Public API names, CLI commands, endpoint paths and optional dependency extras are **not final merely because they appear as pseudocode in design documents**. Define them in implementation, test them and document only verified behavior.

## 4. Voice and realtime invariants

A correct successful call is insufficient. The implementation must behave safely during concurrency, malformed data and failures.

1. **One call, one isolated logical session:** per-call state, buffers, task ownership, tenant context and upstream AI session. No cross-call audio or context leaks.
2. **Verify before allocating:** authenticate and authorize the inbound connection, then apply admission limits, **before** creating a paid/upstream AI session.
3. **Trusted immutable identity:** `tenant_id`, `provider_id`, call identity and permitted scopes come from verified server policy, not caller speech, phone number or untrusted WSS frame fields.
4. **Bidirectional streaming:** telephony→AI and AI→telephony pumps may run concurrently but must share a controlled lifetime, cancellation and cleanup policy.
5. **Bounded resources:** maximum frame sizes, per-call buffer limits, active-call limits, timeouts and defined slow-consumer policies. Never use an unbounded audio queue.
6. **Async safety:** do not block the event loop with synchronous network I/O, disk operations, expensive transcoding or inference work in the media hot path.
7. **Interruption correctness:** barge-in must stop/cancel or clear **unplayed** output according to the verified capabilities of both providers; no invented acknowledgement semantics.
8. **Terminal correctness:** normal close, invalid frame, timeout, provider failure and cancellation all release the two pumps, media buffers, upstream connection and admission reservation exactly once (idempotent cleanup).
9. **Transport honesty:** a WSS disconnect alone does not prove a PSTN call has ended; do not assume reconnect can recover dropped frames or provider state.
10. **Provider capability honesty:** unsupported codecs, call transfer, DTMF, playback cancellation, session resume and other actions must fail clearly instead of pretending they work.

**Protocol uncertainty:** The actual Verimor inbound WSS handshake, media framing, codec, timestamping, authentication and control-event rules are unresolved. A synthetic adapter must have a clearly fictional, project-owned wire contract; never call synthetic success “Verimor integration verified.” Real OpenAI Live endpoint/event/codec compatibility must also be checked against authoritative current documentation and demonstrated in an authorized sandbox before certification.

## 5. Security, privacy and data handling

Treat all inbound network data, caller speech, transcripts, provider metadata and AI-generated text/tool proposals as **untrusted** until specifically verified for their intended use.

- Public ingress must use TLS/WSS, an authenticated connection scheme actually supported by the provider, and fail-closed policy defaults.
- Validate message/frame size, schema, format, origin as applicable, auth expiry/replay, connection admission and resource budgets.
- Bind trusted tenant, call, and authorized user/application scopes server-side; verify scope again at each sensitive tool/data boundary.
- A model's requested tool call is **not authorization**. Delegate business permissions and approval requirements to the consuming application and supported Kaiwen Agent policy/tool mechanisms.
- Limit secrets to controlled server-side configuration/secret providers. Never commit `.env`, API keys, auth headers, provider tokens, private keys or customer credentials.
- By default **do not persist raw audio or transcripts**, and never include them in application logs, exception text, trace attributes, screenshots, GitHub issues, fixtures or CI artifacts.
- Use synthetic data for tests, examples, benchmarks, documentation and public PRs. Anonymization requires review; merely replacing a name is not sufficient for sensitive recordings.
- Avoid logging telephone numbers and other direct identifiers. Redact or use non-reversible scoped pseudonyms when correlation is needed.
- Enforce explicit retention, access and deletion policies if any personal data storage is later approved.
- Do not claim end-to-end encryption when the Gateway or AI provider must access plaintext audio.
- A real-company pilot requires an approved data-handling/consent/legal review, including relevant KVKK and international-transfer considerations; this repository alone does not create legal permission.

If a credential or sensitive dataset is discovered in the repository or tool output, **do not repeat it in a response or commit**. Stop potentially exposing operations and report the incident through an approved private channel.

## 6. Coding conventions

### 6.1 Python and package structure

- Python **3.11+**, `src/` layout, `pyproject.toml`, Hatchling and a locked development environment via `uv` (subject to checked-in configuration).
- `ruff format`, `ruff check`, and **strict mypy** for production package code.
- Type every exported function/method; use `Protocol`, `TypedDict`, dataclasses or Pydantic where appropriate. Prefer simple typed data rather than redundant wrappers.
- Prefer small cohesive modules and explicit constructor injection; keep imports acyclic and avoid internal service locators or uncontrolled plugin auto-discovery.
- Raise meaningful typed exceptions for programmer/configuration errors and map them to sanitized transport errors at the boundary.
- Never use bare `except:` or silently swallow exceptions. Cancellation must propagate or be handled in a documented terminal path.
- Avoid hidden retry loops; make retryability, deadlines, jitter, idempotency and maximum attempts explicit when retries are safe.
- No `print()` for production diagnostics; use the defined structured logger interface. CLI output may use stdout/stderr intentionally.
- Keep comments/docstrings about **why** a non-obvious invariant exists, not narration of self-explanatory code.
- Avoid adding dependencies without explaining their purpose, licensing, maintenance and import/runtime cost.

### 6.2 Public compatibility and semantics

- Public interfaces must have focused documentation and unit/contract tests.
- Keep Python distribution version, external Gateway protocol version and adapter compatibility distinct.
- Preserve backward compatibility unless a deliberate, documented breaking change is approved.
- Use explicit capability negotiation/detection; do not infer that all telephony or AI providers have the same feature set.
- Any wire-contract change updates schemas/examples, sanitized fixtures, conformance tests and migration guidance together.
- Avoid network, filesystem or credential dependencies in unit-test module import paths.

### 6.3 Async and concurrency practice

- Use managed task lifecycles (`TaskGroup` or equivalent controlled supervision) and cancel/await child tasks on shutdown.
- Keep session-state transitions and admission bookkeeping atomic within the chosen deployment topology.
- Be explicit about the difference between **single-process correctness** and multi-instance/distributed correctness; in-memory locks do not provide distributed safety.
- Timeouts must use monotonic timing where appropriate; cleanup must remain bounded and idempotent.
- Capture backpressure and memory behavior in tests rather than assuming a queue limit means the entire process is bounded.

## 7. Testing and evidence policy

### 7.1 Minimum tests for affected code

| Change | Required evidence |
| --- | --- |
| Core contracts and registry | Unit tests, bad-config/duplicate tests, architectural import checks |
| WSS/identity | Valid/invalid handshake, forged/replayed token, cross-tenant admission tests |
| Voice bridge | Fake bidirectional audio, frame validation, interruption, slow-reader/backpressure and cross-call isolation tests |
| Provider adapters | Shared conformance tests, explicit unsupported capabilities, sanitized fixtures |
| Session lifecycle | Connect/close, cancellation, timeout, double-close, provider failure and orphan-task/resource tests |
| Public API/CLI/package | Build wheel, install in clean environment, import and CLI smoke tests |
| Observability | Log schema, correlation, redaction, bounded metric cardinality tests |
| Security-sensitive change | Negative tests and review of exposure/permissions/default policy |
| Performance-sensitive change | Baseline vs changed measurement on documented reference conditions |

Default test I/O must be synthetic and deterministic. Stub external provider requests; do not use paid API sessions or real phone calls in ordinary PR CI.

### 7.2 Quality checks (when tooling is installed)

Use repository-defined commands and versions from `pyproject.toml`, the lockfile and CI. The examples below are **intended checks**, not evidence that the repository has working tooling today:

```bash
uv sync --extra dev
uv run ruff format --check .
uv run ruff check .
uv run mypy src/kaiwen_gateway
uv run pytest
uv build
```

The exact extras, dependency groups and commands **must be adjusted to the actual implemented `pyproject.toml`**. Do not mark a check “passed” unless it was executed successfully and its exit status/result was observed.

When a check cannot run, state `NOT RUN` or `BLOCKED`, name the blocker and explain the resulting validation gap. Never invent coverage percentages, latency data, capacity limits, benchmark numbers, smoke-test results or live-provider compatibility.

### 7.3 Performance evaluation

- Use staged **synthetic** sessions at 1, 5 and 25 concurrency before attempting 50/100.
- Record reference environment, Python and package version, codec/frame sizes, test duration, active sessions, errors, CPU, memory, event-loop lag, buffer depth and p50/p95/p99 processing delay.
- Distinguish Gateway overhead from provider/network/model response time.
- The proposed p95 Gateway-added overhead ≤50 ms is a **target**, not a verified service guarantee.
- Keep expensive soak/load/fault tests out of the fast PR workflow; execute on documented scheduled/manual or release gates.

## 8. GitHub workflow, dependency and release discipline

- Prefer small feature branches and focused Pull Requests. Do not bypass required checks on `main`.
- A typical PR should run format, lint, strict typing, unit tests, deterministic contract/integration tests and clean package smoke tests.
- Use pinned/reviewed external GitHub Actions and least-privilege `permissions`; publish and deploy jobs receive elevated credentials only within protected environments.
- Separate CI, security, integration, performance, compatibility, release and deploy **when the work justifies separate workflows**. Do not create empty decorative YAML files.
- Use dependency/secret/static-analysis/workflow-security scans appropriate to the actual repo and available GitHub security features.
- Do not run production deploys, publish to PyPI, upload releases or push images from ordinary development commits.
- Use versioned artifacts and explicit approval for external publication. Favor short-lived OIDC trusted publishing over long-lived registry tokens when supported.
- Do not assert CodeQL, branch protection, GitHub rulesets or other hosted features are configured without verifying repository settings.
- Record the exact commit SHA and relevant job/test URLs **only when available and verified**.

Recommended Conventional Commit prefixes: `feat`, `fix`, `perf`, `refactor`, `test`, `docs`, `build`, `ci`, `chore`; use a useful scope such as `voice`, `core`, `security` or `server`.

## 9. Logs, metrics, tracing and operation

- Emit structured JSON-compatible diagnostic events with timestamp, severity, stable event name, safe error class, optional trace ID and sanitized session/tenant correlation identifiers.
- Never log every audio frame by default. Track throughput, queue age, latency, active-call count, admission rejection and provider failures as metrics.
- Metric labels must have bounded cardinality; do not add raw call IDs, phone numbers or transcripts as Prometheus labels.
- Keep OpenTelemetry exporters optional; the minimal library must not require a running observability backend.
- Expose liveness and readiness separately when the server exists. Draining rejects new calls while managing active calls under an explicit policy.
- Capture operational failure states in actionable, safe error codes and runbooks; no secrets or raw media in incident reports.

## 10. Documentation, decisions and traceability

### 10.1 Each document has exactly one job

| Document | Purpose / when to update |
| --- | --- |
| `README.md` | Public description and *tested* installation/quickstart information; reflect real status |
| `docs/REQUIREMENTS.md` | Requirement IDs, acceptance gates, V1 scope; update when approved scope changes |
| `docs/ARCHITECTURE.md` | Components, boundaries, media/control flow; update when the design materially changes |
| `docs/decisions/NNNN-*.md` | Significant technical choices, alternatives, consequences and supersession |
| `ROADMAP.md` | Milestones and evidence-backed progress, not granular issue duplication |
| `CHANGELOG.md` | User-facing added/changed/fixed/security items under `Unreleased`, then versioned releases |
| `AGENTS.md` | Coding-agent operating rules and required verification evidence |
| `CONTRIBUTING.md` | Human contribution workflow, setup, code review and PR standards |
| `SECURITY.md` | Responsible disclosure, versions and security reporting channel |

### 10.2 ADR policy

Create a numbered ADR when a decision materially affects one or more of:

- Dependency/module boundaries or an accepted architecture principle.
- Public Python API, Gateway wire contract or provider compatibility strategy.
- Trusted identity, authorization, privacy, retention or security ownership.
- Streaming hot path, codec strategy, buffering or call/session lifecycle invariants.
- Persistence, distributed coordination, deployment topology or runtime ownership.
- A new mandatory service/dependency with durable maintenance consequences.

**Do not** create an ADR for each function, minor refactor, typo, test assertion or routine library upgrade. ADRs are **reviewable engineering rationale**, never private chain-of-thought dumps, prompts or transcripts of an agent's entire decision process.

Use this template, adapting sections to the decision:

```markdown
# ADR-0004: Concise decision title

- Status: Proposed
- Date: YYYY-MM-DD
- Decision owners: Maintainer review pending
- Related requirements: FR-..., SEC-..., NFR-...
- Supersedes: None

## Context and problem
## Decision
## Alternatives considered
## Consequences and trade-offs
## Validation / acceptance evidence
## Reconsideration triggers
```

Start an ADR as `Proposed`. Only mark it `Accepted` when the maintainer explicitly approves. Never delete or rewrite the history of a superseded decision; link the replacement and set the previous record's status to `Superseded` when authorized.

### 10.3 Roadmap and changelog policy

- Roadmap status can change to `Completed` only with linked code and test/acceptance evidence. Documentation completion must not be mistaken for runtime implementation.
- Track implementation detail in GitHub Issues/PRs; keep `ROADMAP.md` focused on phases, deliverables, blockers and acceptance gates.
- Add user-facing, verified changes to `CHANGELOG.md` under `Unreleased` in `Added`, `Changed`, `Fixed`, `Security`, etc., as appropriate.
- Do not list a planned feature as shipped, create fictional releases, claim production readiness, or mark a live provider as verified from simulator results.
- When a change affects a requirement, reference the relevant stable ID in the PR summary/test report; keep requirement ID meanings stable.

## 11. Required workflow for every assigned milestone

1. **Inspect:** Read relevant documents and actual repository state, including branch/worktree status and files involved.
2. **Restate scope:** Briefly identify requested deliverables, requirement IDs, constraints, known unknowns and exclusions.
3. **Plan:** Propose a small implementation path and tests. If the task is documentation-only, **do not modify application code**.
4. **Implement:** Change only files justified by this milestone. Prefer a focused, reviewable diff.
5. **Validate:** Run all available checks relevant to the change; capture actual results. Distinguish successful execution from merely intended checks.
6. **Document:** Update architecture, requirement status, ADR, roadmap and changelog **only when their defined triggers apply**.
7. **Review:** Inspect the diff for secrets, unsupported claims, architectural violations, dependency drift and unwanted scope expansion.
8. **Report and stop:** Give the maintainer the evidence summary below and wait for explicit authorization before proceeding to the next phase.

### 11.1 Final handoff format

Conclude every task with a concise report using these headings:

```text
Milestone: <name / phase>
Status: COMPLETE | PARTIAL | BLOCKED
Summary: <what was actually changed>
Files changed: <paths>
Decisions: <ADR created/updated, or "none">
Requirements addressed: <stable IDs>
Verification:
  PASS: <exact commands / tests and observed outcome>
  FAIL: <exact failures>
  NOT RUN: <checks omitted and why>
Security/privacy review: <brief outcome>
Open risks/blockers: <specific items>
Git state: <branch, commit SHA, pushed/not pushed — only verified facts>
Next proposed milestone: <one bounded next step; do not start it>
```

Do not state that a commit was pushed, a workflow passed, a deployment is active, a file was uploaded, or an endpoint works unless it has actually been verified. If no implementation was performed, state **documentation-only**.

## 12. First-stage instruction (repository bootstrap)

The first project phase is **documentation only**. Prepare and review the documents incrementally, **only when individually assigned by the maintainer**: `README.md`, `docs/REQUIREMENTS.md`, `docs/ARCHITECTURE.md`, ADR-0001 through ADR-0003, this `AGENTS.md`, `ROADMAP.md`, `CHANGELOG.md`, `SECURITY.md` and `CONTRIBUTING.md`. Do not infer that an instruction to work on one document authorizes editing the others.

- Keep proposed designs explicitly labeled and internally consistent.
- Do not create application modules, install production dependencies, invent a Verimor frame schema, add unverified quickstarts, or claim running CI at this stage.
- Do not accept ADRs on the maintainer's behalf.
- Make a documentation cross-link/status review, then stop for approval before Phase 2 (tooling/repository bootstrap).

---

**Definition of done for an agent task:** Small approved scope, truthful implementation state, architecture preserved, security/privacy reviewed, relevant verification executed, documentation traceable, and a concrete handoff report. Nothing is “done” merely because an agent says it is.
