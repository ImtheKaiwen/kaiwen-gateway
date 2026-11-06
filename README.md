# Kaiwen Gateway

**A modular, provider-agnostic communication gateway for real-time AI applications.**

> **Project status: Design / pre-implementation.** This repository is being prepared for its first alpha release. The architecture and scope below describe intended behavior, **not** features that are already implemented or verified. Production use is not yet supported.

Kaiwen Gateway is designed to be both a **reusable Python library** and a **deployable ASGI service**. It accepts connections from external systems, authenticates and isolates them, translates provider-specific protocols into well-defined internal contracts, and bridges live communication to application and AI backends.

The first implementation focuses on **bidirectional telephone audio over secure WebSockets (WSS)**. The core will be kept independent of telephony so future communication modules can be introduced without rewriting the platform.

## Why Kaiwen Gateway?

External providers and AI backends rarely share the same message schema, authentication model, audio format, or connection lifecycle. Applications should not have to rewrite this integration logic for every provider.

Kaiwen Gateway aims to provide:

- **One reusable integration boundary** for external, real-time communication.
- **Independent sessions** for concurrent connections, with trusted tenant identity and scoped authorization.
- **Provider adapters** that contain protocol-specific behavior instead of leaking it into the core.
- **Streaming-first handling** with bounded buffers, cancellation, and controlled backpressure.
- **Operational visibility** through structured logs, metrics, and distributed traces.
- **Library and service entry points** without forcing every consumer to run a standalone server.

These are **design goals** and will be validated incrementally.

## Initial use case: Voice Gateway

The first target integration is a telephony provider sending live audio to a Gateway-controlled WSS endpoint and receiving synthesized audio back over the connection. Kaiwen Gateway will bridge that stream to a real-time AI provider while keeping call state and authorized agent operations isolated.

```text
Caller
  ↕
Telephony provider (initial target: Verimor)
  ↕  bidirectional audio / WSS (subject to provider protocol)
Kaiwen Gateway
  ├─ Transport: connection and authentication
  ├─ Voice: frames, buffering, interruption, call lifecycle
  ├─ Adapter: provider-specific media and control messages
  └─ Integration: Kaiwen Agent session and tool context
  ↕
Real-time AI backend (initial target: OpenAI Live)
```

**Integration dependency:** The exact Verimor WebSocket handshake, authentication scheme, media encoding, frame structure, and call-control events have **not yet been verified**. Development will start with a documented mock provider and conformance tests; a production adapter must not guess these details.

A provider that offers **SIP only**, instead of a compatible bidirectional WSS media stream, requires a separate SIP/media bridge or a different integration mode. WSS and SIP are not interchangeable.

## Architecture

**Architecture style:** Modular Monolith with **Hexagonal Architecture (Ports & Adapters)** and dependency inversion.

```text
┌─────────────────────────────────────────────────────┐
│ External systems: telephony / clients / webhooks   │
└────────────────────────┬────────────────────────────┘
                         │
              HTTP / WSS / future transports
                         │
┌────────────────────────▼────────────────────────────┐
│ Kaiwen Gateway                                     │
│                                                     │
│  Server / transport adapters                        │
│        │                                            │
│  Core: identity · admission · routing · lifecycle   │
│        │                                            │
│  Modules: Voice (v1) · future optional modules      │
│        │                                            │
│  Provider / application adapters                    │
└────────────────────────┬────────────────────────────┘
                         │
       ┌─────────────────┴─────────────────┐
       ▼                                   ▼
  Kaiwen Agent                       AI providers
  (agent runtime,                    (e.g. OpenAI Live)
   context, tools)
```

### Ownership boundaries

| Component | Owns | Does not own |
| --- | --- | --- |
| **Gateway Core** | Connection identity, authenticated context, admission, module registration, routing contracts, lifecycle hooks | Agent reasoning, business decisions, audio codec semantics |
| **Transport adapters** | HTTP/WSS handling, transport authentication hooks, framing, disconnect signaling | Tenant policy decisions, agent execution |
| **Voice module** | Call/media lifecycle, audio bridge, codec negotiation, barge-in, audio queue policy | Generic gateway routing for all modules |
| **Provider adapters** | External wire formats, event mapping, upstream connectivity | Cross-tenant authorization or business tools |
| **Kaiwen Agent** | Agent execution, conversation context, tool permissions, approvals, agent-side realtime lifecycle | Public Gateway endpoints or provider-specific WSS hosting |
| **Consuming application** | Tenant provisioning, business policy, consent, allowed tools, records and retention rules | Shared Gateway infrastructure internals |

**Dependency rule:** `kaiwen-gateway` may depend on `kaiwen-agent`; `kaiwen-agent` must not depend on `kaiwen-gateway`. Existing Kaiwen Agent runtime and session logic must be reused rather than duplicated.

### Design principles

1. **A small core.** Do not implement a second agent runtime, scheduler, or universal event bus.
2. **Explicit adapters.** External provider details belong behind ports with contract tests.
3. **Minimal audio hot path.** Avoid unnecessary JSON conversion, database calls, synchronous I/O, and persistence of audio frames.
4. **Isolation by default.** No authorization decision may be based solely on caller ID or user-supplied tenant/session identifiers.
5. **Fail safely.** Timeouts, cancellation, bounded queues, cleanup, and appropriate call fallback are first-class behaviors.
6. **Measured performance.** Concurrency and latency claims require reproducible tests, not configuration defaults.
7. **Incremental extensibility.** Future modules should not require implementations or dependencies before they are needed.

## Planned modules

| Module | Scope | V1 status |
| --- | --- | --- |
| `voice` | Bidirectional streaming audio, phone sessions and real-time AI bridge | **In scope; not yet implemented** |
| `messaging` | Chat-platform ingress and normalized messaging events | Future; out of scope |
| `events` | Validated external events and webhooks | Future; out of scope |
| `streaming` | General realtime application streams | Future; out of scope |
| `ai` | Gateway-level provider access and request mediation | Future; out of scope |
| `tools` | Controlled external tool / MCP access | Future; out of scope |

The core will provide a **small module registration contract**. A dynamic plugin marketplace, distributed microservices, advanced admin UI, and speculative modules are **not part of V1**.

## V1 delivery scope

The first alpha targets:

- A reusable, typed Gateway Core and **explicit** module registration.
- A runnable ASGI application with an authenticated WSS endpoint and basic HTTP health endpoints.
- A Voice module with independent call sessions and bidirectional media pumps.
- A mock telephony adapter and a real-time AI adapter integration path.
- Audio-frame validation, backpressure, timeouts, cancellation, and controlled disconnection.
- Per-tenant and per-call identity boundaries; tool authority remains in Kaiwen Agent / the consuming application.
- A simulator and automated unit, contract, integration, security, and load tests.
- Structured logging, metrics/tracing hooks, packaging, CLI, and Docker deployment support.
- Documentation of protocol versions, lifecycle semantics, and operational limitations.

**Not in V1:** a production-ready Verimor adapter without the provider's actual protocol specification; local Kaiwen Voice inference; a generic multi-channel SaaS control panel; Kubernetes; a dynamic plugin marketplace.

## Security and privacy expectations

Security is an architectural requirement, not an optional transport feature.

- All external connections require explicit authentication and authorization **before** upstream AI sessions are allocated.
- Connections must be bound to trusted provider, tenant, and call identities; credentials should be short-lived and replay-resistant where supported by the provider protocol.
- Enforce per-tenant admission limits, bounded frame sizes, rate controls, and resource budgets.
- Separate audio sessions, conversation context, permitted tools, logs, and stored records across tenants.
- Do not persist raw audio or transcripts by default. Any recording and retention require an explicit application-owned legal basis and policy.
- Never put provider credentials, raw customer audio, full transcripts, or sensitive phone numbers in normal logs or public test fixtures.
- All agent tool calls remain subject to application-side permissions, policy, approvals, and audit controls.
- Cloud AI processing and international data transfers require a separate legal and operational assessment; WSS alone does not provide end-to-end confidentiality from processing services.

See [`SECURITY.md`](SECURITY.md) for the security policy and vulnerability reporting process. Do not send vulnerability reports containing sensitive data through public issues.

## Engineering and release standards

The project intends to use:

| Area | Planned approach |
| --- | --- |
| Python | Python 3.11+; asynchronous I/O |
| Application server | ASGI; FastAPI/Starlette + Uvicorn |
| Packaging | `pyproject.toml`, Hatchling, `uv` |
| Code quality | Ruff formatting/linting, mypy strict |
| Testing | pytest, async tests, provider contract tests, fault injection, load tests |
| CI | GitHub Actions for quality, compatibility, security and release checks |
| Observability | Structured JSON logs, OpenTelemetry-compatible tracing and metrics |
| Release | Semantic Versioning, changelog, reproducible artifacts, controlled publishing |
| Deployment | Container image and explicit configuration / secrets |

A passing unit suite does **not** prove real telephony interoperability, security readiness, or production concurrency capacity. Release criteria will distinguish simulation, sandbox integration, and real-provider validation.

## Getting started

**The package is not yet released on PyPI. The Core layer (M1, M2), runnable ASGI runtime foundation (M3.1, merged in PR #15 / `224284f`), authenticated synthetic WebSocket ingress (M3.2A, merged in PR #16 / `558bd95`), synthetic telephony adapter foundation (M3.2B, merged in PR #17 / `7a28c40`), and CLI with executable synthetic call demo (M3.3A) are implemented. Voice audio media streaming (M4) and real provider adapters remain pre-alpha and unimplemented.** Public endpoint paths, server commands, and configuration examples will be published after they are merged and released. Please do not assume an example endpoint or command from design discussions is available today.

### Verified local package, quality, and CI foundation (M1)

From a local clone with Python 3.11+ and [`uv`](https://docs.astral.sh/uv/) installed, the minimal package skeleton and development quality checks can be synced, verified, and built locally:

```bash
uv sync --locked --group dev --extra server
uv audit --locked --preview-features audit-command
uv run --locked --group dev --extra server ruff check .
uv run --locked --group dev --extra server ruff format --check .
uv run --locked --group dev --extra server mypy src tests
uv run --locked --group dev --extra server pytest -q --cov=kaiwen_gateway --cov-report=term-missing
uv build
```

### Local development CLI and ASGI host runtime (M3.1, M3.2 & M3.3A)

Kaiwen Gateway provides an `argparse`-driven command-line interface via `kaiwen-gateway`:

```bash
# Display CLI version and help (zero external runtime dependencies)
uv run kaiwen-gateway --version
uv run kaiwen-gateway --help

# Validate CoreConfig and KAIWEN_GATEWAY_* environment variables without running a server
uv run kaiwen-gateway check-config

# Execute automated loopback synthetic call demo (allocates free port, verifies full lifecycle, zero leaks)
uv run --locked --group dev --extra server kaiwen-gateway demo

# Run local development ASGI server on 127.0.0.1:8000 (ingress_mode="disabled" by default)
uv run --locked --group dev --extra server kaiwen-gateway serve

# Backward-compatible python -m module execution:
uv run --locked --group dev --extra server python -m kaiwen_gateway.server
```

**Operational and security boundaries:**
- **Zero base dependencies:** `kaiwen-gateway --help`, `--version`, and `check-config` run using only Python standard library. Optional extras (`kaiwen-gateway[server]`) are required for `serve` and `demo`.
- **Loopback binding only:** Binds strictly to `127.0.0.1:8000` (or specified loopback port); never bind to public `0.0.0.0` or deploy without a production reverse proxy and TLS boundary.
- **Liveness (`GET /health`):** Returns `200 OK` (`{"status": "ok", "instance": "<name>"}`) when the ASGI host process is viable; returns `503 Service Unavailable` if queried before lifespan startup completes.
- **Readiness (`GET /ready`):** In default disabled mode (`ingress_mode="disabled"`), returns `503 Service Unavailable` (`{"status": "unavailable", "reason": "ingress_disabled"}`). When configured with `ingress_mode="synthetic"` and active admission capacity, returns `200 OK` (`{"status": "ready", "ingress_mode": "synthetic", "instance": "<name>"}`). Returns `503` with `reason="draining"` during instance draining, and `reason="stopped"` once stopped.
- **Authenticated synthetic WebSocket ingress (`/ws/synthetic/v1` - M3.2A & M3.2B):**
  - Explicit activation via `CoreConfig(ingress_mode="synthetic")` paired with a server-configured `SyntheticAuthBridge` (or matching authenticator and authorizer ports).
  - Inbound connections require an HTTP `Authorization: Bearer <token>` header verified cryptographically (HMAC-SHA256 signature, expiry, and canonical claims) before upgrade acceptance.
  - Handshakes lacking valid credentials, forged tokens, or exceeding admission capacity are closed prior to acceptance (`code 1008`), allocating zero capacity leases and zero upstream sessions.
  - Established connections exchange structured text JSON control frames: `session.ready` (server -> client), `call.start` / `call.started`, `call.end` / `call.ended`, `session.ping` / `session.pong`, and `session.end` (graceful termination).
  - Binary frames are strictly rejected (`code 1003`) in M3.2; voice audio streaming pipelines remain deferred to M4.
  - Frames exceeding 64 KiB are rejected with `code 1009`; malformed JSON frames are rejected with `code 1007`.

For architecture and milestone tracking:

1. Read [`ROADMAP.md`](ROADMAP.md) and [`docs/REQUIREMENTS.md`](docs/REQUIREMENTS.md) for the V1 scope, milestones, and acceptance gates.
2. Review [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md) and [`docs/decisions/`](docs/decisions/) for boundaries and accepted V1 architectural decisions.
3. Track implementation through issues and pull requests rather than treating this README as an implementation checklist.

A fast GitHub Actions CI workflow ([`.github/workflows/ci.yml`](.github/workflows/ci.yml)) runs on pull requests and pushes to `main` across Python 3.11, 3.12, and 3.13, plus isolated wheel/sdist smoke validation:

- **M1.3 baseline CI verification (`main`):** Verified on `main` commit `1355b0c2093bfac4d0520ffb3ffb06ccc5f2b937` in [GitHub Actions run `41263309448`](https://github.com/ImtheKaiwen/kaiwen-gateway/actions/runs/41263309448) across Python 3.11, 3.12, 3.13, and the package smoke job (prior to adding `uv audit`).
- **M1.4A security hardening verification (`main`):** Merged in [PR #4](https://github.com/ImtheKaiwen/kaiwen-gateway/pull/4) (`32111a402b289a5608cd70effdb70fa0fe53d877`) and verified on `push -> main` in [GitHub Actions run `41264905664`](https://github.com/ImtheKaiwen/kaiwen-gateway/actions/runs/41264905664) with all 4 CI jobs and the `uv audit --locked` step (on Python 3.12) succeeding, alongside initial Dependabot version-update checks ([run `41264912866`](https://github.com/ImtheKaiwen/kaiwen-gateway/actions/runs/41264912866) for `uv` and [run `41264912883`](https://github.com/ImtheKaiwen/kaiwen-gateway/actions/runs/41264912883) for `github-actions`).
- **M1.4B.1 protected `main` branch:** Classic GitHub branch protection is enabled on `main` (`protected: true`, pull request required, `strict: true` up-to-date branch enforcement, `enforce_admins: true`, `required_linear_history: true`, `allow_force_pushes: false`, `allow_deletions: false`, and `required_approving_review_count: 0` for single-maintainer operation) with four required GitHub Actions status checks bound to `app_id: 15368`: `Quality & Tests (Python 3.11)`, `Quality & Tests (Python 3.12)`, `Quality & Tests (Python 3.13)`, and `Package Build & Clean Wheel Smoke Test`.

**Note:** The M0 documentation baseline and initial ADRs (`ADR-0001`–`0003`) have been accepted as the V1 design baseline (`2026-11-01`), the M1 engineering foundation is complete, M2 (Core identity, admission control, connection lifecycle, and instance draining) is complete, M3.1 (ASGI host runtime foundation) is merged in [PR #15](https://github.com/ImtheKaiwen/kaiwen-gateway/pull/15) (`224284f`), M3.2A (authenticated synthetic WebSocket ingress) is merged in [PR #16](https://github.com/ImtheKaiwen/kaiwen-gateway/pull/16) (`558bd95`), M3.2B (synthetic telephony control adapter foundation) is merged in [PR #17](https://github.com/ImtheKaiwen/kaiwen-gateway/pull/17) (`7a28c40`), and M3.3A (CLI and executable synthetic call demo) is in review on Draft [PR #18](https://github.com/ImtheKaiwen/kaiwen-gateway/pull/18) (`0fdfc3c` / remote CI verified / merge pending). These contracts enforce in-process structural contracts, cryptographic HMAC-SHA256 token verification (with 60-second TTL), and loopback simulation only, **not** live PSTN telephony interoperability. The test coverage reported applies **only to the currently implemented package, Core, ASGI, and synthetic transport modules**, not to the unimplemented Gateway application. **Voice audio media streaming (M4), real Verimor integration, and PyPI release are not yet implemented.**

## Documentation map

The repository documentation baseline is organized as follows:

| Document | Purpose |
| --- | --- |
| [`AGENTS.md`](AGENTS.md) | Rules and verification expectations for coding agents |
| [`ROADMAP.md`](ROADMAP.md) | Milestones, delivery status, deferred work |
| [`CHANGELOG.md`](CHANGELOG.md) | User-visible changes, beginning with `Unreleased` |
| [`CONTRIBUTING.md`](CONTRIBUTING.md) | Development workflow, coding and PR standards |
| [`SECURITY.md`](SECURITY.md) | Security policy and private vulnerability reporting |
| [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md) | Architecture, ports, boundaries, data paths |
| [`docs/REQUIREMENTS.md`](docs/REQUIREMENTS.md) | V1 requirements, non-goals and acceptance criteria |
| [`docs/decisions/`](docs/decisions/) | Numbered architecture decision records (ADRs) |

Decisions document **what was decided and why**, not a coding agent's private reasoning or an unfiltered action log. Significant changes should reference the corresponding issue, PR, and ADR where appropriate.

## Contribution and licensing

The repository is public to support transparent development and reusable infrastructure. Contributions should follow [`CONTRIBUTING.md`](CONTRIBUTING.md). Public issues and pull requests must use synthetic or sanitized data, never real customer recordings or private provider credentials.

**License: [MIT](LICENSE).** This repository is licensed under the MIT License (`Copyright (c) 2026 Kaiwen`). The project is in the pre-implementation phase and is not yet a published PyPI package or production-supported service.
