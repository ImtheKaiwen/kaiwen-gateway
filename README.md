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

**The package is not yet released on PyPI. While the Core layer (M1, M2) and runnable ASGI runtime foundation (M3.1) are implemented, the synthetic WSS ingress (M3.2), CLI commands (M3.3), and Voice audio runtime (M4) are not yet implemented.** Public endpoint paths, server commands, and configuration examples will be published after they are implemented and covered by tests. Please do not assume an example endpoint or command from design discussions is available today.

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

### Local development ASGI host runtime (M3.1)

With optional server dependencies installed (`uv sync --locked --group dev --extra server`), the instance-scoped ASGI server foundation can be run locally on the loopback interface:

```bash
# Run local development ASGI server on 127.0.0.1:8000
uv run --locked --group dev --extra server python -m kaiwen_gateway.server

# Alternatively, using uvicorn factory invocation:
uv run --locked --group dev --extra server uvicorn kaiwen_gateway.server:create_gateway_app --factory --host 127.0.0.1 --port 8000
```

**Operational and security boundaries:**
- **Loopback binding only:** Binds strictly to `127.0.0.1:8000`; never bind to public `0.0.0.0` or deploy without a production reverse proxy and TLS boundary.
- **Liveness (`GET /health`):** Returns `200 OK` (`{"status": "ok", "instance": "<name>"}`) when the ASGI host process is viable; returns `503 Service Unavailable` if queried before lifespan startup completes.
- **Readiness (`GET /ready`):** Returns `503 Service Unavailable` (`{"status": "unavailable", "reason": "ingress_disabled"}`) because external media ingress remains disabled fail-closed by default in `CoreConfig` (`ingress_mode="disabled"`). Returns `503` with `reason="draining"` during instance draining, and `reason="stopped"` once stopped.
- **WebSocket upgrade refusal:** Connection attempts to WebSocket routes are immediately closed prior to upgrade acceptance (`code 1008` in-process ASGI close; HTTP 403 Forbidden on the network). No WebSocket connection is accepted, and no upstream AI session or media pipeline is allocated. Concrete synthetic telephony ingress is deferred to M3.2+.

For architecture and milestone tracking:

1. Read [`ROADMAP.md`](ROADMAP.md) and [`docs/REQUIREMENTS.md`](docs/REQUIREMENTS.md) for the V1 scope, milestones, and acceptance gates.
2. Review [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md) and [`docs/decisions/`](docs/decisions/) for boundaries and accepted V1 architectural decisions.
3. Track implementation through issues and pull requests rather than treating this README as an implementation checklist.

A fast GitHub Actions CI workflow ([`.github/workflows/ci.yml`](.github/workflows/ci.yml)) runs on pull requests and pushes to `main` across Python 3.11, 3.12, and 3.13, plus isolated wheel/sdist smoke validation:

- **M1.3 baseline CI verification (`main`):** Verified on `main` commit `1355b0c2093bfac4d0520ffb3ffb06ccc5f2b937` in [GitHub Actions run `41263309448`](https://github.com/ImtheKaiwen/kaiwen-gateway/actions/runs/41263309448) across Python 3.11, 3.12, 3.13, and the package smoke job (prior to adding `uv audit`).
- **M1.4A security hardening verification (`main`):** Merged in [PR #4](https://github.com/ImtheKaiwen/kaiwen-gateway/pull/4) (`32111a402b289a5608cd70effdb70fa0fe53d877`) and verified on `push -> main` in [GitHub Actions run `41264905664`](https://github.com/ImtheKaiwen/kaiwen-gateway/actions/runs/41264905664) with all 4 CI jobs and the `uv audit --locked` step (on Python 3.12) succeeding, alongside initial Dependabot version-update checks ([run `41264912866`](https://github.com/ImtheKaiwen/kaiwen-gateway/actions/runs/41264912866) for `uv` and [run `41264912883`](https://github.com/ImtheKaiwen/kaiwen-gateway/actions/runs/41264912883) for `github-actions`).
- **M1.4B.1 protected `main` branch:** Classic GitHub branch protection is enabled on `main` (`protected: true`, pull request required, `strict: true` up-to-date branch enforcement, `enforce_admins: true`, `required_linear_history: true`, `allow_force_pushes: false`, `allow_deletions: false`, and `required_approving_review_count: 0` for single-maintainer operation) with four required GitHub Actions status checks bound to `app_id: 15368`: `Quality & Tests (Python 3.11)`, `Quality & Tests (Python 3.12)`, `Quality & Tests (Python 3.13)`, and `Package Build & Clean Wheel Smoke Test`.

**Note:** The M0 documentation baseline and initial ADRs (`ADR-0001`–`0003`) have been accepted as the V1 design baseline (`2026-11-01`), the M1 engineering foundation (`src/kaiwen_gateway` minimal importable package skeleton, Ruff, strict mypy, pytest, Python 3.11/3.12/3.13 fast CI, wheel/sdist smoke validation, `uv audit`, weekly Dependabot version updates, and `main` branch protection) is complete, M2.1 ([PR #9](https://github.com/ImtheKaiwen/kaiwen-gateway/pull/9) / `68030a0`, [run `41518757425`](https://github.com/ImtheKaiwen/kaiwen-gateway/actions/runs/41518757425)) added the framework-independent Core module contracts (`GatewayModule`, `ModuleLifecycle`, `ModuleDescriptor`) and explicit instance-scoped `ModuleRegistry`, M2.2A ([PR #10](https://github.com/ImtheKaiwen/kaiwen-gateway/pull/10) / `f65ad6d`, [run `41705325629`](https://github.com/ImtheKaiwen/kaiwen-gateway/actions/runs/41705325629)) added immutable fail-closed Core configuration (`CoreConfig`, `validate_core_config`, `CoreConfigurationError`) and bounded instance-scoped Core composition (`GatewayCore`, `compose_gateway_core`, capped at `64` modules per instance via `MAX_COMPOSED_MODULES`), and M2.2B adds pure Core identity, tenant/call scope contracts, and authenticator/authorizer ports (`ProviderId`, `TenantId`, `ConnectionId`, `CallId`, `PermissionScope`, `UntrustedConnectionClaims`, `ProviderPrincipal`, `TenantCallBinding`, `VerifiedConnectionContext`, `ProviderAuthenticatorPort`, `CallAuthorizationPort`, `bind_verified_connection_context`, and `resolve_verified_connection_context`) in `kaiwen_gateway.core`. These Python dataclasses and validators enforce in-process structural contracts only, **not** cryptographic provider authentication or a capability sandbox against arbitrary in-process code. Configured connection ceilings in `CoreConfig` (`max_concurrent_connections`, `max_connections_per_tenant`) are not yet enforced by an active admission controller, and in-process instance isolation does not provide multi-process or distributed admission coordination. The `100%` coverage reported by pytest applies **only to the currently implemented package and `kaiwen_gateway.core` modules**, not to the unimplemented Gateway application. **Concrete transport WSS ingress, Voice audio bridge, CLI commands, real Verimor integration, and PyPI release are not yet implemented.**

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
