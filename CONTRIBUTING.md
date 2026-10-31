# Contributing to Kaiwen Gateway

> **Project:** [Kaiwen Gateway](https://github.com/ImtheKaiwen/kaiwen-gateway)
> **Repository:** `ImtheKaiwen/kaiwen-gateway`
> **Phase:** Design / pre-implementation; initial documentation is under review
> **Planned first alpha:** `0.1.0a1` — **not released**
> **Document status:** Proposed contributor guide; pending maintainer approval
> **Last reviewed:** 2026-11-01

Thank you for your interest in Kaiwen Gateway. This guide describes how to propose, develop, test, review, and document changes to the project. It applies to human contributors and complements the [coding-agent instructions](AGENTS.md). It does **not** grant permission to use production systems, customer data, paid AI services, or release credentials.

**Current state:** The repository is still in its documentation/design phase. The intended Python package, CLI, WebSocket server, provider adapters, tests, and GitHub Actions may not exist yet. Commands in this guide that depend on those files are **planned for milestone M1 and later**; do not report them as working until they have been implemented and executed successfully.

## 1. Before you contribute

Read the project's sources of truth in this order:

1. [V1 requirements and acceptance gates](docs/REQUIREMENTS.md) — what must be delivered and what is explicitly out of scope.
2. [Architecture](docs/ARCHITECTURE.md) — components, dependencies, trust boundaries, and realtime design.
3. [Architecture decisions](docs/decisions/) — why significant decisions were proposed or accepted; check each ADR's **actual status**.
4. [Roadmap](ROADMAP.md) — the currently approved milestone, dependencies, and blockers.
5. [Security policy](SECURITY.md) — vulnerability reporting, data handling, and security expectations.
6. [Coding-agent rules](AGENTS.md), when using Antigravity, Codex, or another coding agent.

If two documents conflict, describe the conflict in the issue/PR and request clarification before implementing the contradictory part. **A proposed ADR is not an accepted ADR.** A roadmap item is not an authorization to start it.

### 1.1 What belongs in V1

Kaiwen Gateway is intended to be a reusable, provider-neutral Python library **and** an optional ASGI server/CLI. V1 implements one communication module, **Voice**, which bridges authenticated telephony WebSocket audio to realtime AI through Kaiwen Agent. The design uses a modular monolith with Hexagonal Architecture (Ports & Adapters).

The V1 work includes a synthetic telephony simulator, an explicitly defined test protocol, bounded streaming, per-call isolation, security controls, observability interfaces, tests, and packaging.

**Real Verimor compatibility is conditional:** the official provider contract and authorized tests must be available before anyone claims interoperability. Synthetic fixtures must not be represented as Verimor's real wire format. Live OpenAI interoperability also requires verification against the current provider contract.

**Out of scope for V1:** Messaging/Event/MCP gateways, a generalized AI proxy, plugin marketplace, dashboards, Kubernetes, Kafka, speculative multi-node call migration, local Kaiwen Voice inference, and unapproved production telephony work. Do not add placeholder modules for these simply to fill directories.

### 1.2 What kinds of contributions help

- Clear documentation fixes, reproducible bug reports, targeted tests, small approved features, and verified performance improvements.
- Security hardening and negative tests that preserve existing contracts.
- Provider-independent adapter contract improvements backed by a real use case and tests.
- Focused changes to the current authorized milestone rather than unrelated refactoring.

Before undertaking a major interface, security, wire-protocol, or architectural change, open an issue or discussion and agree on the scope with the maintainer. For security vulnerabilities, use the **private process in [SECURITY.md](SECURITY.md)**, **not** a public issue.

## 2. Development environment

### 2.1 Prerequisites

- Git and a supported desktop/server development environment.
- Python **3.11 or newer**; the exact tested version matrix will be defined by CI.
- [`uv`](https://docs.astral.sh/uv/) for reproducible Python dependency management, once the package scaffold exists.
- A terminal and an editor supporting Python type checking.
- Docker only for tasks that explicitly require container validation; it is **not** required for ordinary documentation or Core work.

No production account, real telephone number, customer recording, or paid-provider API key is required for a normal contribution. Ordinary CI should run with fakes and synthetic data.

### 2.2 Clone and create a focused branch

```bash
# Clone the public repository
git clone https://github.com/ImtheKaiwen/kaiwen-gateway.git
cd kaiwen-gateway

# Work on a branch (choose a descriptive name)
git switch -c docs/contributing-guide
```

For external contributors, fork the repository first and clone your fork. The maintainer may choose a different branch/PR workflow while repository protections are being configured; **do not assume branch protection is already enabled**.

Suggested branch prefixes: `docs/`, `feat/`, `fix/`, `test/`, `security/`, `perf/`, and `chore/`.

### 2.3 Python setup — only after M1 creates `pyproject.toml`

The following commands are **proposed**, not presently verified. Use the actual dependency groups/extras and commands defined by the committed `pyproject.toml` and CI when they exist:

```bash
uv sync --extra dev
uv run ruff format --check .
uv run ruff check .
uv run mypy src/kaiwen_gateway
uv run pytest
uv build
```

The `dev` extra is illustrative: the project may ultimately use a `dependency-groups.dev` group, in which case the setup command will differ. **Never invent a passing result** for a command that could not run because the scaffold is absent. Mark it `NOT RUN` or `BLOCKED` and state why.

Avoid manually modifying the generated lockfile. When dependencies change, regenerate it using the selected package manager, include the updated lockfile when required by repository policy, and explain the dependency's purpose.

## 3. Architecture and coding conventions

### 3.1 Dependency direction

Keep the Gateway Core small and framework-neutral:

```text
core                 -> Python standard library and minimal common contracts
voice.domain         -> voice-owned types and permitted core contracts
voice.application    -> voice domain/ports and core contracts
voice.adapters       -> corresponding ports and optional provider clients
transports           -> core transport contracts and optional ASGI framework
server / CLI         -> explicit composition of modules and adapters
```

Do **not** import FastAPI, Starlette, Verimor, or OpenAI provider SDKs from `core`. Do **not** expose framework-specific WebSocket objects inside the Voice domain/application layer. Do **not** make `kaiwen-agent` depend on `kaiwen-gateway`, reproduce Kaiwen Agent's agent loop/session management, or call Agent private implementation APIs.

The **media plane** (raw audio) is intentionally distinct from the **control plane** (session and policy events). Do not route frame-by-frame audio through a general event bus, database, Redis, or JSON serialization without a documented, approved reason. Preserve bounded memory and isolation even when optimizing for fewer copies.

### 3.2 Python style and type safety

- Follow repository-pinned **Ruff formatting and linting** rules.
- Follow **strict mypy** checks for the typed package; annotate public interfaces and key state transitions.
- Use descriptive names, small explicit interfaces, and typed configuration. Avoid speculative abstractions and unnecessary global singletons.
- Prefer small, purposeful modules to files built around vague utilities or enormous manager classes.
- Keep import side effects minimal: importing `kaiwen_gateway` must not start a server, create tasks, read production secrets, or contact providers.
- Prefer documented, compatible public APIs over copying or monkey-patching dependencies.
- Handle errors with typed, actionable exceptions/events; do not suppress failures or print provider credentials.
- Update public contract documentation and tests whenever changing published behavior.

The repository's own configuration, once committed, is authoritative for line length, Python targets, dependency groups, tool versions, and exact check commands. Do not claim a convention is enforced by CI until it actually is.

### 3.3 Async and realtime implementation rules

- Never block the event loop with synchronous I/O or long CPU-bound work on a live media path.
- Use managed, bounded task lifetimes (`asyncio.TaskGroup` or an equivalent supervised pattern); cancel **and await** child tasks during teardown.
- Authenticate and authorize external connections **before** allocating an upstream paid AI session.
- Use server-verified, immutable call/tenant identity; untrusted audio, caller ID, or inbound frame metadata must not grant permissions.
- Enforce connection, frame-size, queue, time, concurrency, and per-tenant limits. An unbounded queue is not an acceptable overflow strategy.
- Make cleanup idempotent and test double-close, upstream failure, disconnects, cancellation, slow peers, and interruption/barge-in behavior.
- Distinguish single-process guarantees from distributed guarantees. In-memory registries and locks do not coordinate multiple server instances.
- Never claim support for a media codec, provider event, handoff feature, or interruption semantics without contract evidence and tests.

## 4. Tests and acceptance evidence

Every behavior change should include focused verification. Prefer deterministic synthetic fixtures and fake upstream/downstream adapters over actual telephony or paid AI traffic.

| Change area | Minimum expected checks |
| --- | --- |
| Core, module registry, configuration | Unit tests, invalid/duplicate configuration tests, architecture/import boundary checks |
| WSS and authentication | Allowed/denied handshake, invalid or replayed credential, over-capacity and cross-tenant tests |
| Voice streaming | Bidirectional frames, codec/frame validation, isolation, bounded queues, slow peer, interruption and cancellation |
| Adapters | Common contract/conformance tests, unsupported-capability behavior, sanitized fixtures |
| Session management | Connect, disconnect, timeout, failure, repeated closure, orphan-task/resource checks |
| Package, public API, CLI | Wheel/sdist build, clean-environment install, import smoke test and verified CLI help/behavior |
| Logging, metrics, tracing | Structured event schema, correlation, redaction and bounded-cardinality checks |
| Security fixes | Regression tests for the exact trust-boundary failure plus negative/abuse cases |
| Performance changes | Comparable baseline and changed measurement, environment details, p50/p95/p99 and resource impact |

Coverage is **supporting evidence**, not a replacement for behavioral and negative testing. Follow any threshold once it is agreed and enforced by CI; avoid fabricated coverage figures or unsupported capacity claims.

### 4.1 Test levels and execution frequency

- **Each relevant PR:** formatting, linting, type checks, unit tests, deterministic integration/contract tests, and package smoke tests where applicable.
- **Security-sensitive PR:** focused adversarial/negative tests and applicable code/dependency/workflow scans.
- **Scheduled or manually approved runs:** concurrency/load tests, fault injection, longer soak tests, compatibility matrix, and expensive provider smoke tests.
- **Provider live/sandbox tests:** only with explicit approval, safe test credentials, cost limits, and provider/data-processing prerequisites. Never run automatically from untrusted pull requests.

A simulated call passing tests demonstrates the **simulator contract**, not live Verimor compatibility. A package building successfully does **not** demonstrate production reliability.

## 5. Issues, branches, commits, and pull requests

### 5.1 Reporting an issue

For a non-sensitive bug, include the observed behavior, expected behavior, minimal reproduction, sanitized logs, package/commit version, environment, and the impact. Include the relevant requirement ID (`FR-*`, `SEC-*`, `ENG-*`, `AT-*`, etc.) where known.

Do not paste authentication material, real audio, personal data, customer transcripts, proprietary telephone wire documentation, or private provider endpoints into public issues.

Before proposing a large feature, check [ROADMAP.md](ROADMAP.md) and the V1 exclusions. A new feature idea does **not** automatically become an approved milestone.

### 5.2 Commits

Use descriptive, focused commits. Conventional Commit prefixes are recommended, not yet asserted as an enforced GitHub rule:

```text
docs(contributing): define contributor workflow
feat(core): add explicit module registration
fix(voice): clean up audio tasks after disconnect
test(security): reject replayed call credentials
perf(audio): reduce buffer copies without changing frame semantics
ci(test): run deterministic websocket contract tests
```

Avoid unrelated modifications and generated data in a commit. Never rewrite shared history or force-push without explicit maintainer approval.

### 5.3 Pull request requirements

A PR should have **one coherent purpose** and contain:

1. **Summary:** the problem, intended behavior, and affected layer or module.
2. **Scope:** included work, intentionally excluded work, and any changed public API/wire contract.
3. **Traceability:** requirement IDs, relevant issue or milestone, and applicable ADRs.
4. **Verification:** exact commands run, results, environments, and relevant CI/job links if available.
5. **Security/privacy review:** trust boundaries, authorization, logging/PII, provider costs, secrets and operational risks as applicable.
6. **Compatibility/migration:** any breaking change, new configuration, optional dependency, or migration instruction.
7. **Documentation:** README, architecture, contract docs, ADR, roadmap, and changelog **only when their update criteria apply**.
8. **Known limitations:** blocked tests, provider contract unknowns, and why the remaining risk is acceptable for the **current** milestone.

A PR that could not run a required test must disclose the missing test and blocker. Do not describe tests as passed based on their presence in code or a planned workflow. For agent-generated changes, retain a human-reviewable diff and the same evidence requirements.

### 5.4 Reviewer checklist

Reviewers should verify correctness **and** containment of the change:

- Does it meet the current authorized milestone without introducing speculative features?
- Does it preserve Core/Voice/transport/provider boundaries and the Kaiwen Agent ownership boundary?
- Are public APIs, WSS events, and error behavior explicit, versioned or documented appropriately?
- Are cross-call identity, authorization, resource bounds, cancellation, failure paths and cleanup tested?
- Are logs, traces, metrics, test data and artifacts free of secrets and real customer content?
- Are test results reproducible, and are unavailable checks honestly labeled?
- Are new runtime/development dependencies justified, versioned and reviewed?
- Is documentation updated without marking proposed features as shipped or ADRs as accepted?

The maintainer decides when to merge. A green CI status is necessary when required checks are configured, but it is **not** a substitute for scope and security review.

## 6. Documentation and design decisions

Use each document for one purpose:

| Document | Update when... |
| --- | --- |
| [`README.md`](README.md) | Public behavior, verified installation steps, or supported entry points change. |
| [`docs/REQUIREMENTS.md`](docs/REQUIREMENTS.md) | Approved scope, requirement IDs, or acceptance criteria change. |
| [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md) | Component contracts, dependency boundaries, trust boundaries, or flow materially change. |
| [`docs/decisions/`](docs/decisions/) | A meaningful architecture/security/protocol/runtime trade-off is selected or superseded. |
| [`ROADMAP.md`](ROADMAP.md) | A milestone's evidence-backed progress, blockers, or approval changes. |
| [`CHANGELOG.md`](CHANGELOG.md) | A verified, user-visible behavior or relevant documentation baseline changes; record under `Unreleased` until published. |
| [`SECURITY.md`](SECURITY.md) | Disclosure contact, supported-version policy, or relevant security process changes. |
| [`AGENTS.md`](AGENTS.md) | Coding-agent permissions, architecture guardrails, and reporting obligations change. |

### 6.1 Decision records (ADRs)

Create a numbered `docs/decisions/NNNN-topic.md` only when a durable choice affects architecture, public contracts, security policy, provider integration strategy, critical audio behavior, mandatory infrastructure, or release topology. Explain **context, selected option, alternatives, trade-offs, tests, and reconsideration conditions**.

- Start a new decision at `Proposed`.
- Record maintainer acceptance explicitly before marking it `Accepted`.
- Supersede an older ADR by referencing it from a new ADR; do not erase decision history.
- Do not capture private chain-of-thought, raw agent prompts, or every micro-decision as an ADR.
- Reference changed ADRs in PRs; update related architecture and requirements only when justified.

Human contributors and coding agents follow the same decision rules. A coding agent must stop at the assigned milestone boundary and request authorization before significant scope, security, architecture, publishing, or deployment changes; see [`AGENTS.md`](AGENTS.md).

## 7. Security, privacy, and responsible disclosure

Kaiwen Gateway will process potentially sensitive telephone conversations. Treat all untrusted provider frames, speech transcripts, caller metadata, and model/tool outputs as untrusted inputs until validated against server-owned policy.

- **Never commit** API keys, `.env` files containing secrets, tokens, private certificates, personal data, real customer audio/transcripts, or confidential provider documents.
- Prefer **synthetic or sanitized** fixtures. Do not include reconstructable real caller information in sample data, GitHub Actions artifacts, traces, or screenshots.
- Do not log raw audio frames or full transcripts by default; use sanitized correlation fields and bounded metric labels.
- Do not connect to real telephone providers, chargeable AI services, or production infrastructure without explicit authorization and approved test data.
- Keep tool execution deny-by-default and tenant scoped; a spoken instruction cannot grant an agent extra permissions.
- Report vulnerabilities according to [SECURITY.md](SECURITY.md). **Do not post exploit details in public issues.** Its preferred reporting channel must be checked for availability; never assume a private reporting feature is enabled.

If confidential material is accidentally committed, stop, inform the maintainer through an appropriate private channel, and prioritize credential revocation or incident containment. Merely deleting a later commit does not make exposed secrets safe again.

## 8. CI/CD, releases, and deployment

The eventual GitHub Actions setup should use least-privilege permissions, reviewed/pinned third-party actions, appropriately scoped workflow triggers, safe secrets management, and explicit publication approval. Separate fast PR checks from slower security, performance, compatibility, release, and deployment checks when each has real work to do; do **not** add ornamental empty workflows.

**A normal contribution does not authorize:**

- Publishing to PyPI or a container registry; creating release tags or public GitHub releases.
- Deploying a Gateway instance or changing telephone routing/provider account settings.
- Running paid live tests, capturing customer media, or contacting real customers.
- Changing repository protections, secret stores, or privileged GitHub settings.

Releases require approved quality gates, known artifacts, and a separate maintainer decision. `0.1.0a1` is **a planned alpha**, not an existing release. Follow [ROADMAP.md](ROADMAP.md) for the difference between documentation approval, synthetic alpha, real-provider pilot, and production readiness.

## 9. Completion and handoff format

At the end of any approved task, including an Antigravity milestone, provide a brief evidence-based handoff:

```text
Milestone / task:
Scope delivered:
Files changed:
Relevant requirement IDs / ADRs:
Checks PASSED (exact commands and results):
Checks FAILED (with relevant output):
Checks NOT RUN / BLOCKED (with reason):
Security / privacy review:
Known limitations / open questions:
Commit / PR / CI references (only verified ones):
Next recommended step (not started without authorization):
```

Do not claim a milestone is completed merely because files were written; required reviews and acceptance criteria must be satisfied. Never claim a commit, push, release, provider handshake, or production deployment occurred without supporting evidence.

## 10. Participation and project stewardship

Keep contributions respectful, technical, and reviewable. Offer clear reasons when disagreeing and focus reviews on behavior, risks, maintainability, and measured results rather than personal judgments. The maintainer may defer an otherwise valuable contribution to preserve the V1 scope.

**License:** The intended repository license is MIT; verify the actual committed `LICENSE` file before contributing or making redistribution claims. If the license file is missing, ask the maintainer to resolve licensing before submitting substantive reusable code. Do not assume third-party code, provider specifications, sample audio, or model weights are automatically redistributable.

**Questions and non-sensitive proposals:** Use repository Issues or Discussions **if enabled**. For private security matters, follow [SECURITY.md](SECURITY.md).

---

**Contributor principle:** Small, well-tested, explicitly authorized changes are more valuable than impressive-looking scaffolding. Preserve the separation between the design, verified implementation, and production operation of Kaiwen Gateway.
