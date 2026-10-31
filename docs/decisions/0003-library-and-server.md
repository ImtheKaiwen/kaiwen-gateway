# ADR-0003: Distribute One Python Library with an Optional ASGI Server and CLI

- **Status:** Accepted
- **Date:** 2026-10-31 (accepted 2026-11-01)
- **Decision owners:** Kaiwen Gateway maintainers
- **Scope:** V1 packaging, public API, host composition, application lifecycle, CLI, configuration, and distribution
- **Related decisions:** [ADR-0001 — Modular Monolith](0001-modular-monolith.md), [ADR-0002 — Hexagonal Architecture](0002-hexagonal-architecture.md)
- **Related documents:** [Architecture](../ARCHITECTURE.md), [Requirements](../REQUIREMENTS.md), [Project overview](../../README.md), [Roadmap](../../ROADMAP.md)
- **Primary requirements:** `FR-CORE-001` through `FR-CORE-008`, `FR-CONN-003` through `FR-CONN-006`, `FR-CONN-009`, `SEC-*`, `ENG-*`

> **Implementation status:** This accepted ADR (approved 2026-11-01) specifies the **approved V1 packaging and hosting design**, not a completed package, executable, API, protocol, release, or deployment. Names, commands, imports, extras, and paths shown as examples are **illustrative** and are not frozen until implemented, exercised, and documented. The GitHub repository being public does not mean a PyPI package has been published.

## 1. Context

Kaiwen Gateway serves two distinct consumers:

1. **Application developers** who want to embed Gateway functionality and choose their own transport, authentication, configuration, telemetry, and process lifecycle.
2. **Operators** who want a runnable, independently deployed WebSocket/HTTP service for a real-time telephony-to-AI bridge.

A library-only project would force every operator to write its own ASGI hosting, startup/shutdown, connection admission, and deployment glue. A server-only project would force application developers to make network calls to infrastructure that could otherwise run in their own process. Maintaining unrelated library and server implementations would encourage duplicated security and media behavior.

At the same time, one oversized mandatory dependency set would make simple imports expensive, require unrelated provider SDKs, and complicate installation, testing, and upgrades. Server startup at import time, implicit environment access, or globally shared mutable session registries would make embedded use unsafe and difficult to test.

[ADR-0001](0001-modular-monolith.md) selects one Modular Monolith and deployment unit for V1. [ADR-0002](0002-hexagonal-architecture.md) selects explicit ports/adapters and composition. This ADR defines **how the same codebase is consumed and operated**, without weakening those decisions.

## 2. Decision

**Publish one versioned Python distribution named `kaiwen-gateway`, exposing the import package `kaiwen_gateway`, with a small reusable library core and an optional runnable ASGI server/CLI.**

The following rules are normative:

1. **One implementation, two consumption modes.** Embedded library usage and standalone service usage share the same Core contracts, Voice application behavior, security rules, and tested adapters; neither creates a parallel runtime.
2. **One-way dependencies.** `kaiwen-gateway` may integrate with the documented public APIs of `kaiwen-agent`, particularly for realtime AI sessions. `kaiwen-agent` must never import or require Gateway.
3. **Minimal default import.** Importing the base package must not import FastAPI, Uvicorn, provider SDKs, initialize an agent session, require production credentials, start a network listener, or spawn background tasks.
4. **Optional feature dependencies.** Server hosting, Voice integrations, and provider-specific clients are installed only as required by an explicit supported feature group, subject to an implementation-verified extras design.
5. **Explicit instance composition.** Applications and the reference server create independent Gateway/app instances through a documented constructor or factory. No implicit process-wide singleton owns active calls, authentication state, or mutable configuration.
6. **ASGI is a hosting boundary, not the domain.** The runnable server is an adapter around shared application contracts; the library's Core and Voice application layers cannot depend on ASGI or Uvicorn types.
7. **A CLI is convenience, not the sole API.** CLI operations delegate to the same validated composition and application interfaces as programmatic embedding.
8. **Fail closed on unsafe configuration.** A server with enabled external ingress must not become publicly accessible without an explicit, valid authentication/admission policy. Missing secrets, invalid provider bindings, or unsupported configuration fail before accepting calls.
9. **Consistent lifecycle.** Startup, readiness, draining, task cancellation, and shutdown are defined once and exercised in both embedded and hosted forms.
10. **No premature framework.** V1 implements only Voice. No auto-discovered plugins, alternative server product, admin SaaS, distributed process scheduler, or per-module deployment framework is part of this decision.

## 3. Distribution versus runtime boundaries

```text
                   Python distribution: kaiwen-gateway
          ┌───────────────────────────────────────────────┐
          │ Importable package: kaiwen_gateway            │
          │                                               │
          │  Core contracts + explicit module registry    │
          │           │                                   │
          │     Voice application + ports                 │
          │           │                                   │
          │     Optional provider adapters                │
          │           │                                   │
          │  Optional ASGI composition + CLI              │
          └───────────┬──────────────────────┬────────────┘
                      │                      │
             Embedded Python host     Standalone service
             controls lifecycle       CLI/ASGI owns lifespan
                      │                      │
                      └──────────┬───────────┘
                                 ▼
                   Same approved adapters/behavior
                                 │
                          Kaiwen Agent
                                 │
                           AI provider
```

- **Distribution:** one source tree and one library release, potentially with different optional dependency groups.
- **Runtime:** one *logical* Gateway instance per composition; one standalone ASGI process is the starting deployment model.
- **Transport:** HTTP/WSS are optional server adapters; they are not universal assumptions in the Core API.
- **Scaling:** loading the wheel in multiple processes does not produce shared connection state, distributed rate limiting, seamless call migration, or automatic service discovery.

A Docker image, when introduced, packages the reference server from a **known wheel/commit**. It is a deployment artifact, **not** a separate implementation or independently versioned gateway engine.

## 4. Package and import boundary

The conceptual source layout is:

```text
src/kaiwen_gateway/
  __init__.py              # Small, intentional public exports
  core/                    # Framework-independent contracts and policies
  modules/
    voice/
      domain/              # Call/media types and state transitions
      application/         # Call orchestration and audio pumps
      ports/               # Narrow media / AI contracts
      adapters/            # Opt-in concrete integrations
  transports/
    http/                  # ASGI-specific HTTP endpoints
    websocket/             # ASGI/WSS protocol handling
  server/
    app.py                 # Explicit ASGI app factory / wiring
    lifespan.py            # Reference server lifecycle
  cli.py                   # Optional installed command entry point
```

This is **a proposed organization**, not a requirement to create empty directories. Actual files emerge with functionality; the invariant is the **dependency boundary**, not the exact naming of every directory.

### 4.1 Import discipline

- `import kaiwen_gateway` should remain cheap, side-effect-free, and safe without Voice, web server, or AI extras installed.
- Framework-specific imports belong under `transports/` or `server/` and load when those features are explicitly used.
- Agent and AI-provider imports belong at the Voice integration boundary, not in Core or the top-level package initializer.
- The base distribution may use small validation/typing dependencies where justified, but the declared minimal dependency set must be verified from a clean installation.
- Clear errors must explain missing extras **at the feature activation boundary**. Missing optional packages must not break unrelated imports.
- No import-time reading of credentials, dynamic plugin scanning, network sockets, asyncio tasks, logging configuration overrides, or global event-loop creation.

### 4.2 Public versus private interfaces

Only deliberately documented imports and entry points are part of the supported public API. Internal module paths are not automatically stable just because Python allows importing them. Prefer a small public facade and explicit versioned contracts over exporting every class.

Changes to public APIs require tests, documentation, and an intentional compatibility/release note. The project is initially alpha; it must still identify breaking changes rather than silently changing them.

## 5. Installation and optional features

Use `pyproject.toml` and **Hatchling** to build an installable Python wheel and source distribution. Use **uv** for contributor dependency management and a checked-in development lockfile; the package's published dependency metadata remains authoritative for consumer installations.

A *candidate*, not yet approved, feature split is:

| Capability | Candidate install group | Expected contents | Constraint |
| --- | --- | --- | --- |
| Minimal typed core | Base install | Pure contracts, registry, configuration contracts | No ASGI or AI provider SDK imports at package import time |
| ASGI hosting | `server` | Starlette/FastAPI, Uvicorn as selected | No AI credentials needed for importing server tools |
| Voice orchestration | `voice` | Voice-specific runtime dependencies | Still no mandatory concrete telephony integration |
| Kaiwen Agent/OpenAI Live | `openai-live` or `voice-openai` | Compatible Agent and provider transport dependencies | Compatible versions verified against Agent public APIs |
| Developer tooling | `dev` (local only) | Ruff, mypy, pytest, packaging/security tools | Not part of default runtime installation |

**Do not treat these names as released extras.** Final names, dependency ranges and install examples are to be resolved with package smoke tests (`OPEN-007` in [Architecture](../ARCHITECTURE.md#12-open-architecture-questions-and-decisions-needed-before-pilot)). Avoid an optional-dependency matrix that is more complicated than the real V1 features.

### 5.1 Compatibility rules

- Require Python **3.11+** for V1 unless an explicit review changes the baseline.
- Declare upper/lower bounds where tested incompatibilities are known; do not use unconstrained prerelease provider versions in production distributions.
- Test at least the chosen supported Python version(s), package build/install, and a compatible pinned or constrained Kaiwen Agent integration set.
- Do not import undocumented `kaiwen_agent` private members or copy its `RealtimeSessionManager`, authorization pipeline, or provider session logic.
- A release must state **which integration combinations were tested**; the presence of a dependency does not prove that real OpenAI audio sessions work.

## 6. Library composition contract

The embedded API must support **more than one independent instance in one Python process**. Each instance owns its own module registry, active-connection state, admission state, configuration, and registered lifecycle resources. Share only immutable, explicitly injected resources when safe.

A conceptual usage example (pseudocode; **not currently implemented**):

```python
# The names below are illustrative and may change before release.
settings = GatewaySettings(...)  # Validated, non-secret representation
identity = TrustedIdentityResolver(...)
voice = VoiceModule(media_adapter=..., ai_adapter=...)

gateway = Gateway(settings=settings, identity_resolver=identity)
gateway.register(voice)

# Only a host with the optional ASGI feature installed asks for an ASGI app.
app = create_asgi_app(gateway)
```

The public contract, once chosen, must specify:

1. How modules are registered, validated, and rejected on duplicate/unsupported IDs.
2. When settings are evaluated and immutable trusted identity is attached.
3. Whether construction is synchronous and which phase performs async initialization.
4. Who owns sockets, task supervisors, stores and outbound AI connections.
5. How startup and shutdown failures are propagated without leaking resources.
6. How multiple app instances stay isolated, even within the same event loop.
7. What happens if a consumer tries to use an optional feature without its installed extra.

### 6.1 Host-ownership rule

An **embedded host** controls the parent process, ASGI lifespan if applicable, authentication integration, and environment/secret source. Gateway controls **its own registered resources and per-call operations**. Neither may silently close an unrelated resource owned by the other.

Adapters receiving externally managed clients must document whether they *borrow* or *own* those clients, especially during shutdown. Shared outbound pools, if ever introduced, require explicit reference/lifecycle ownership and tenant isolation tests.

## 7. Reference ASGI server and CLI

The reference server composes Core + Voice + selected adapters and exposes only **documented, explicitly enabled** HTTP/WSS routes. Endpoint paths, event types and CLI command strings are finalized only when implementation and contract tests are available.

### 7.1 Server responsibilities

- Configure the ASGI framework, routes, authentication hooks, admission checks, and dependency injection.
- Verify the **upstream caller/provider** before allocating a downstream AI session; reject when configuration cannot establish trusted tenant/call authority.
- Allocate and release per-call resources through the shared lifecycle contracts.
- Expose liveness and readiness checks separately; readiness must be false during draining or unsafe initialization.
- Propagate cancellation and shutdown without logging secrets, customer media, or transcripts by default.
- Keep framework request/WebSocket objects outside Core and Voice application contracts.

### 7.2 CLI responsibilities

The CLI should eventually support the minimal equivalent of:

| Intent | Expected behavior |
| --- | --- |
| Serve | Validate configuration, compose the selected module/adapters and start ASGI hosting |
| Config check | Parse and validate configuration **without opening provider sessions or exposing secrets** |
| Version/help | Report installed package/compatible capabilities clearly |
| Diagnostics | Optional later command for safe, local runtime checks; not a precondition for alpha |

**Illustrative only:** earlier design discussions used command shapes such as `kaiwen-gateway serve` and `kaiwen-gateway config check`. They are **not promises of a currently available executable**. The chosen entry point must be smoke-tested from the built wheel in a fresh environment.

### 7.3 Server process model

Start with **one Uvicorn/ASGI worker process** per instance to keep call-scoped tasks and admission observable. Do **not** silently enable multiple workers or assume local registries/locks enforce global limits across processes. Replicas require separately approved coordination for trusted call identity, idempotency, quotas and session ownership.

The CLI must not create hidden additional worker processes for live calls. A deployment's process manager may restart crashed processes, but a restarted process cannot inherit an active WebSocket or upstream AI session from a dead process.

## 8. Configuration, secrets, and trust

Configuration must be explicit, typed, validated, and suitable for both embedding and serving.

### 8.1 Config sources and precedence

- An embedding application may supply already-validated settings and secret providers directly.
- The reference server may read an explicitly documented configuration source (e.g., environment variables, validated config file, CLI overrides).
- The actual **precedence order must be specified and covered by tests** before release. Do not silently merge unrelated tenants or provider settings.
- Secrets should be handled through a narrow provider boundary and must not appear in exception strings, command-line echo, debug dumps, process titles, or structured logs.
- Unsupported options, malformed limits and missing required auth configuration must produce actionable **sanitized** errors.

### 8.2 Security ownership

The **deployment/consuming application** establishes which telephony provider and tenant are authorized, how verified caller identity maps to permitted actions, what tools may execute, and whether personal data may be retained. Gateway enforces its own ingress/admission and session isolation boundaries. Kaiwen Agent enforces the application-configured tool permissions, approvals, and context policy.

Caller-provided phone numbers, provider metadata and client-supplied tenant IDs are never sufficient authority. If Verimor's available WSS authentication method is not yet known, the real adapter **cannot be declared production-compatible**. Mock authentication is allowed only in explicitly identified local tests.

## 9. Startup, readiness, draining, and shutdown

For both embedding and the reference server, lifecycle behavior must be deterministic and idempotent where appropriate:

```text
CREATED
   │ validate configuration and adapters
   ▼
STARTING ── failure ──> FAILED / cleanup
   │
   ▼
READY ── request to stop ──> DRAINING
   │                        │ reject new calls
   │                        │ allow active calls to finish
   │                        ▼
   └───────────────────> STOPPING
                                │ cancel/close remaining owned resources
                                ▼
                             STOPPED
```

- **Startup:** validate mandatory settings and adapter capabilities before readiness; fail closed if ingress authentication is unavailable.
- **Health:** liveness can report that the process is running while readiness reports it is **not accepting new calls**.
- **Admission:** draining prevents new calls; it does not automatically terminate established ones.
- **Drain:** wait a bounded, configured interval; upon expiry follow explicit provider-supported fallback/termination rules.
- **Shutdown:** cancel/join owned audio pumps; close call-specific upstream sessions; release admission reservations; close *owned* resources; emit sanitized terminal metrics/events.
- **Repeated stop:** must not double-release resources or corrupt state.
- **Failure handling:** partial startup should unwind already acquired resources and not leave an apparently healthy, unauthenticated listener.

A WebSocket cannot simply be migrated to another process during deployment. Connection draining is a graceful shutdown strategy, **not** live session migration or a guaranteed uninterrupted call.

## 10. Error model and operational behavior

- **Library:** raise documented, typed errors for configuration, missing optional capability, authorization/admission failure, invalid state transition, provider negotiation and lifecycle failure. Avoid broad `except: pass` or unbounded retries.
- **CLI:** report a safe, concise explanation and nonzero exit status on failure; detailed diagnostics may be emitted to a protected sink after sanitization.
- **ASGI:** map allowed failure categories to a documented HTTP/WSS rejection or close behavior. Do not leak provider response bodies, secrets or internal stack traces to clients.
- **Voice stream:** isolate failures to the affected call when possible, and apply bounded retries only where protocol semantics allow. Safe termination is preferable to an invented transparent failover.
- **Metrics:** use safe, bounded-cardinality labels. Do not emit per-frame text logs or include raw recordings in exception traces.

No generic `GatewayError` may be used as a pretext to discard meaningful error categories or prevent applications from taking safe recovery actions.

## 11. Alternatives considered

| Alternative | Benefit | Cost / risk | Outcome |
| --- | --- | --- | --- |
| **A. One package + optional ASGI server and CLI** | Shared implementation; easy embedding and service deployment; single release lineage | Requires disciplined optional imports and lifecycle ownership | **Selected** |
| **B. Server-only application** | Simpler initial startup story | Difficult in-process use; forces network hop for embedded consumers | Rejected |
| **C. Library-only package** | Small install surface | Every deployment recreates auth, ASGI lifecycle and startup/ops glue | Rejected |
| **D. Separate library and service repositories** | Independent publishing cadence | Version skew, duplicated tests and integrations, cross-repo change overhead in V1 | Deferred |
| **E. Embed Gateway server inside Kaiwen Agent** | Fewer packages to install | Reverses ownership and ties telephony hosting to agent-runtime release lifecycle | Rejected |
| **F. Import-time singleton/server auto-start** | Short example code | Test contamination, secret leakage, unowned tasks, impossible independent instances | Rejected |
| **G. Everything as mandatory dependency** | Single installation command | Pulls unrelated server, AI and telephony dependencies into all consumers | Rejected |

## 12. Consequences and mitigations

### 12.1 Benefits

- Embedded applications can reuse the same call/audio behavior as the reference service.
- Operators have one supported server composition rather than rebuilding ASGI hosting for each deployment.
- Core-only consumers avoid server and provider SDK costs.
- Testing can create multiple independent instances with fake providers and deterministic lifecycle ownership.
- Build provenance, release notes, and security fixes can be tracked through one distribution lineage.

### 12.2 Trade-offs

- Optional dependency combinations require careful documentation and compatibility testing.
- A library update can affect server deployments, so release gates must validate both modes.
- Python's permissive import system means the documented public surface must be deliberately maintained.
- Single-process hosting has a shared failure domain and provides no inherent distributed coordination.
- Two host modes increase the number of lifecycle edge cases compared with a server-only script.

### 12.3 Required mitigations

- Automated clean-install tests for minimal and supported feature combinations.
- App-factory tests using **two independent instances** in one process.
- Import-without-optional-extras and import-without-credentials tests.
- Explicit configuration/auth failure tests before WSS acceptance.
- Simulated startup failure, graceful draining, double-shutdown, cancellation and partial-cleanup tests.
- CLI entry-point smoke tests from the **built wheel**, not only editable installs.
- A package/public-API compatibility check and coordinated changelog updates for breaking changes.
- No production-readiness claim based solely on unit tests, simulator output or package publication.

## 13. Verification and acceptance evidence

| Requirement | Evidence needed before declaring implemented |
| --- | --- |
| `FR-CORE-001` | Install wheel in a clean environment; `import kaiwen_gateway` succeeds. |
| `FR-CORE-002` | Create two independent apps; configuration, registries and sessions do not leak across instances. |
| `FR-CORE-003/004` | Fake module contract tests, deterministic registration, duplicate/version rejection. |
| `FR-CORE-005` | Minimal dependency graph and import tests without ASGI/Agent/provider packages. |
| `FR-CORE-006` | Typed config tests, explicit source precedence and fail-closed invalid auth. |
| `FR-CORE-007` | Console entry point installed and executed from a noneditable wheel. |
| `FR-CORE-008` | Distinct health/readiness tests, including startup failure and draining. |
| `FR-CONN-003/005` | Unauthorized WSS call creates no AI session; admission limit is enforced. |
| `FR-CONN-006/009` | Fault injection verifies cleanup, bounded drain and no newly admitted calls. |
| `ENG-*` | Ruff, strict type checks, pytest, package builds and applicable security checks in CI. |
| External interoperability | Separate authorized live/sandbox tests confirm actual Verimor and OpenAI media contracts; mock tests do **not** count. |

The acceptance status must distinguish **planned**, **implemented**, **mock-verified**, **provider-verified**, and **production-approved**. Evidence includes test commands/results, linked PR/commit and the environment used. A library and server that both launch do not automatically satisfy security, load or privacy obligations.

## 14. When to reconsider

Review this decision if:

1. Independent library/server releases become necessary because of demonstrated external consumers or separate security/support cadences.
2. Multi-process deployment is required by measured load/availability needs, with verified distributed admission and session ownership.
3. The actual telephony provider contract requires a fundamentally different transport or runtime than the ASGI/WSS assumption.
4. The optional dependency graph becomes materially harder to maintain than a small number of separately versioned deliverables.
5. A documented supported use case cannot be expressed through the public composition API without importing private internals.

Reconsideration requires evidence, migration and compatibility impact, operational/security trade-offs, and a **new numbered ADR** superseding or amending this one. Do not silently change the package/distribution boundary.

## 15. Implementation order and governance

1. Maintain alignment between accepted ADR-0001, ADR-0002, and this ADR across requirements and architecture links.
2. Establish `pyproject.toml`, minimal package, clean build/import tests, developer lockfile and code-quality tools.
3. Define only the small public contracts needed for explicit module registration, configuration and independent composition.
4. Add the optional ASGI factory, CLI and safe startup/readiness/draining behavior with fake adapters.
5. Implement and test Voice media orchestration, fake telephony and fake AI paths.
6. Integrate Kaiwen Agent using tested public APIs, then verify the selected live AI protocol separately.
7. Add the real Verimor adapter only after its authoritative WSS protocol and authentication are known.
8. Validate the built distribution and reference container, document supported installation commands, and run the release gates.

A coding agent may propose implementation details within these boundaries; **material changes to trust, public API, dependency direction, deployment or release model require explicit review**. Record decisions and evidence in ADRs/issues/PRs, not private agent deliberation logs or a separate duplicate task tracker.

### Decision record management

- **Proposed → Accepted:** project owner reviews public consumption modes, dependency split, security ownership and tests.
- **Accepted → Superseded:** a later numbered ADR records the replacement and migration; retain this document as history.
- **Rejected:** state why; do not delete the record.
- Update [Architecture](../ARCHITECTURE.md), [Requirements](../REQUIREMENTS.md) and [Roadmap](../../ROADMAP.md) when the reviewed decision materially changes those documents.

---

**Summary:** Kaiwen Gateway V1 is intended to ship as **one reusable, importable Python distribution with an optional ASGI application and CLI**. The library and server must share behavior, remain explicitly composed and secure by default, and be independently verifiable. The precise API, extras, commands and real provider compatibility remain **unimplemented proposals** until tests and documentation prove otherwise.
