# ADR-0001: Adopt a Modular Monolith for Kaiwen Gateway V1

- **Status:** Proposed — pending owner review
- **Date:** 2026-10-31
- **Decision owners:** Kaiwen Gateway project maintainers
- **Scope:** V1 application structure, module ownership, packaging, and deployment
- **Related:** [Architecture](../ARCHITECTURE.md) · [Requirements](../REQUIREMENTS.md) · [Project overview](../../README.md)
- **Related decisions:** [ADR-0002 — Hexagonal Architecture](0002-hexagonal-architecture.md) · [ADR-0003 — Library and Server](0003-library-and-server.md)

> **Decision status note:** This ADR records the agreed design direction for review. It does **not** imply the repository already contains a running Gateway, a tested Verimor adapter, or a production deployment. Change the status to **Accepted** only when the project owner explicitly approves the document and its constraints.

## 1. Context

Kaiwen Gateway is intended to be a **reusable, provider-neutral Python communication gateway** that can be imported as a library and run as an ASGI server. Its first implementation is a bidirectional, real-time **Voice Gateway**, connecting a telephony provider's media stream to an AI realtime provider through existing [Kaiwen Agent](https://github.com/ImtheKaiwen/kaiwen-agent) capabilities.

V1 must solve concrete engineering problems:

1. Accept and authenticate inbound WebSocket connections without trusting caller-controlled metadata.
2. Associate every accepted call with an immutable, authorized tenant/call context.
3. Bridge inbound and outbound audio with predictable lifecycle, low overhead, bounded buffering, interruption handling, and cleanup.
4. Reuse Kaiwen Agent for existing realtime/provider and agent/tool responsibilities rather than implementing another agent framework.
5. Make integrations testable without depending on live telephony, paid AI calls, or private customer recordings.
6. Deliver a package that can be imported by applications **and** launched as one managed service.
7. Retain a clear path for future communication modules without implementing them prematurely.

Known constraints and uncertainty:

- **Only Voice is in V1.** Messaging, general Events/Webhooks, streaming, AI proxy, and Tool/MCP gateways are future candidates, not V1 deliverables.
- Verimor's authoritative WSS handshake, media frames, audio formats, control messages, and authentication capabilities have not yet been supplied. A simulator may test Gateway behavior but must not claim actual Verimor compatibility.
- Real OpenAI Live interoperability and current media/control semantics require verification against the live provider contract.
- There is no measured evidence yet that a specific host, worker count, or session configuration supports 25, 50, or 100 real simultaneous calls.
- V1 is initially intended to run as **one ASGI process**, so in-memory session and capacity management have **process-local** scope.

The design must be simple enough to implement, review, and operate now, while preserving boundaries that make later extraction possible *if justified by production evidence*.

## 2. Decision

**Build Kaiwen Gateway V1 as a Modular Monolith:** one cohesive Python codebase with explicit module boundaries, one composition root, and one runnable ASGI application, while exposing reusable library components.

More precisely:

1. **Single product boundary:** one `kaiwen-gateway` repository and one distributable Python project named `kaiwen-gateway` (`kaiwen_gateway` import namespace).
2. **Single initial deployable:** an ASGI app that composes Gateway Core, the Voice module, and selected adapters. The reference deployment begins with one worker/process; scaling is a later, separately validated decision.
3. **Shared, minimal Core:** module registration, validated configuration, trusted connection context, admission/lifecycle primitives, and observability interfaces. Core stays unaware of telephony wire formats and AI provider details.
4. **Feature-owned modules:** Voice owns call/audio-specific types, state machines, media-flow orchestration, interruption semantics, and voice-level fallback policy. Future communication modules must have independently testable boundaries.
5. **Explicit integration adapters:** Verimor (when verified), simulated telephony, and Kaiwen Agent-backed AI are assembled at the application composition root, not globally discovered at runtime.
6. **No inter-module private imports:** modules share only stable Core contracts or narrowly documented public application interfaces; no direct access to another module's internal state, buffers, or stores.
7. **No second Agent Runtime:** the dependency direction is `kaiwen-gateway → kaiwen-agent`, never the reverse. Gateway owns transport/integration concerns; Agent owns its existing realtime and intelligence-related abstractions.
8. **A separate media hot path:** high-frequency media is handled as ephemeral, call-scoped streaming, not serialized into a generic persistent event bus for every frame. Low-frequency lifecycle events may use standard event/telemetry interfaces.
9. **No unnecessary distributed infrastructure in V1:** Redis, Kafka, service mesh, Kubernetes, cross-service RPC, and a generalized plugin platform are not requirements for the first working release.

**Important distinction:** This ADR selects the **module and deployment shape**. It does not replace the dependency-design decision in ADR-0002 (Hexagonal Architecture) or the public packaging/API decision in ADR-0003 (Library + Server). A Modular Monolith can be internally poorly coupled; therefore those later records are still necessary.

## 3. Architectural boundaries

The target structure is conceptual; directories are created as needed, not all at once:

```text
kaiwen_gateway/
├── core/                    # shared contracts, identity, admission, lifecycle
├── transports/              # ASGI HTTP/WebSocket-facing adapters
├── modules/
│   └── voice/
│       ├── domain/          # call/media types and invariants
│       ├── application/     # call supervisor and streaming orchestration
│       ├── ports/           # telephony and realtime AI interfaces
│       └── adapters/        # simulated provider; Agent/OpenAI; later Verimor
├── server/                  # explicit ASGI composition and lifespan
└── cli.py                   # optional executable entry point
```

### 3.1 Ownership matrix

| Boundary | Owns | Must not own |
| --- | --- | --- |
| **Core** | Trusted connection identity, module registration, admission, shared lifecycle/telemetry contracts | Codec parsing, provider frame JSON, LLM instructions, specific business tools |
| **Transport** | HTTP/WSS protocol I/O, handshake, close signaling, transport backpressure | Business authorization based on untrusted headers, Agent reasoning |
| **Voice** | One call's media pumps, bounded queues, interruption, voice errors/fallback, cleanup | Generic Gateway registration, scheduling, durable Agent memory |
| **Provider adapters** | Provider-specific framing, controls, codecs, feature/handshake mapping | Arbitrary tenant assignment, bypassing security checks |
| **Kaiwen Agent integration** | Connect Voice to existing Agent realtime/session/permission interfaces | Duplicated Agent internals in the Gateway repository |
| **Consuming application** | Tenant onboarding, business authorization, privacy/retention policies, approved tools | Private Gateway queue or socket state |

### 3.2 Dependency and state invariants

- `core` does **not** import `modules.voice`, ASGI frameworks, Verimor SDKs, OpenAI SDKs, or the consuming product.
- Module application logic depends on declared ports, not concrete network/SDK clients.
- `server` is the composition root that wires concrete implementations; no import-time network sessions or hidden mutable global registries.
- Every call has its own authorized security context, AI session, buffers, task ownership, and close/cleanup state.
- Untrusted media or control payloads cannot change verified tenant/permissions after admission.
- Process-local limits and locks must **never** be advertised as cluster-wide guarantees.
- Provider adapters may have optional dependencies; importing the lightweight Core must not import every integration.

## 4. Alternatives considered

| Option | Benefits | Costs and risks | Outcome |
| --- | --- | --- | --- |
| **A. Modular Monolith (selected)** | Simple local development, testability, one deployment, direct in-process calls, predictable tracing and audio overhead, clear future extraction points | Requires discipline to prevent module coupling; one deployable has shared failure domain; coordinated releases | **Select for V1** |
| **B. Immediate microservices** | Independent deployment and technology stacks; isolated scaling/fault domains when engineered correctly | Network hops in real-time paths, distributed identity/session coordination, tracing and incident complexity, more infrastructure and operational cost before validated demand | **Reject for V1** |
| **C. Unstructured/classic monolith** | Fastest initial scaffolding; few concepts | Provider and transport concerns tend to mix; modules are hard to replace/test; business rules can leak into Core | **Reject** |
| **D. Gateway inside `kaiwen-agent` repository/runtime** | Reuses code directly; fewer repos | Couples a deployable, telephony-facing server to a general-purpose Agent library; broadens Agent's public responsibilities and release lifecycle | **Reject** |
| **E. Generic plugin/microkernel system from day one** | Potentially dynamic integrations and third-party extensions | Loading/versioning/security complexity; unclear contract stability; no current requirement for untrusted plugins or marketplace | **Defer** |
| **F. Serverless function per media interaction** | Operational simplicity for short-lived HTTP events | Long-lived bidirectional WSS media and call-scoped state are a poor default fit for short-duration/stateless request models | **Reject as default media runtime** |

Microservices are not inherently slower or less secure. They are simply **not justified by V1's known workload, team needs, and unverified capacity requirements**.

## 5. Consequences

### 5.1 Positive

- Shorter path to a working **synthetic** end-to-end demonstration, without a distributed deployment stack.
- A single process can maintain local call-scoped tasks and media buffers with clear ownership and less inter-service serialization.
- Modules and adapters can be unit-, contract-, and integration-tested using fakes.
- Shared authentication, admission, telemetry, and lifecycle rules stay consistent across present/future communication modules.
- A library consumer can embed the supported API independently of the reference server, subject to ADR-0003's final public contract.
- Reusing Kaiwen Agent prevents duplicate conversation/session/tool systems and divergent authorization behavior.
- A future split remains possible *when* component boundaries and operational requirements warrant it.

### 5.2 Negative / trade-offs

- One process is one failure domain: a process crash may affect all active calls on that instance.
- Slow or blocking Python work can still disrupt other calls on the same event loop; isolation requires disciplined async programming, task supervision, bounded work, and testing.
- In-memory admission/session registries do not coordinate multiple replicas. Merely adding Uvicorn workers is not a correct distributed capacity strategy.
- Independent deployment of one module is unavailable initially; Core and Voice ship together.
- Maintaining boundaries requires tooling and code review, not just directory naming.
- The selected structure does not establish real capacity, security compliance, or live provider interoperability on its own.

### 5.3 Required mitigations

- Enforce import boundaries where practical and test module contracts independently.
- Own every call's async tasks under a call supervisor; ensure timeouts, cancellation, bounded queues, and idempotent cleanup.
- Test cross-call and cross-tenant isolation, malformed frames, replay/duplicate starts, slow peers, interrupted playback, and graceful draining.
- Run a deterministic simulated telephony ↔ Gateway ↔ fake AI integration suite in CI.
- Add structured redacted logs, safe metrics, and correlation IDs without recording raw media by default.
- Benchmark staged concurrency and actual hardware; publish results as measured observations, not generic throughput promises.
- Keep external provider interfaces versioned/contract-tested; do not infer Verimor protocol details from the generic adapter.

## 6. Decision guardrails and non-goals

This ADR **does not authorize**:

- Implementing Messaging, Events, MCP, AI proxy, or other future modules in V1.
- Creating separate per-module services merely because modules have distinct folders.
- Adding Kafka, Redis, Kubernetes, distributed tracing backends, a service mesh, or dynamic plugin discovery without a measured/approved requirement.
- Storing customer audio/transcripts by default or embedding company-specific policies in Core.
- Claiming that a simulator validates the actual Verimor WSS protocol.
- Promising horizontal scaling, transparent call migration, or uninterrupted AI-provider failover before those behaviors are proven.

Lightweight **ports and configuration hooks** for future requirements are allowed; speculative implementations and placeholder directory trees are not necessary.

## 7. Validation and acceptance evidence

The architecture decision is only implemented when the following evidence exists:

| Check | Required evidence |
| --- | --- |
| **Boundary enforcement** | Static import checks/code review plus tests showing Core runs without Voice/provider optional dependencies. |
| **One deliverable** | An installable Python wheel and a runnable ASGI app built from the same source tree. |
| **Explicit composition** | Tests demonstrate deterministic module registration, duplicate rejection, startup/shutdown, and configurable adapters. |
| **Replaceable adapters** | The same contract/integration tests run against simulated provider implementations. |
| **Call isolation** | Concurrent synthetic calls show no shared buffers, tenant context, agent tools, or upstream session leakage. |
| **Streaming reliability** | Bounded buffering, two-way media flow, controlled failure/interrupt/cleanup, and graceful draining are tested. |
| **Scope discipline** | Only Voice is implemented as a functional communication module; future modules remain documented candidates. |
| **Operations honesty** | Single-process limits, actual benchmark conditions, and unresolved provider contracts are visible in release documentation. |

Reference detailed acceptance scenarios **AT-001 through AT-013** in [REQUIREMENTS.md](../REQUIREMENTS.md#71-required-acceptance-scenarios). The actual Verimor scenario **AT-013** cannot pass until its authoritative protocol is obtained and live/sandbox tested.

## 8. When to reconsider this decision

Review, **do not automatically reverse**, the decision when evidence shows one or more of the following:

1. One module requires a materially different scaling or availability model that cannot be met by process replicas and explicit coordination.
2. Resource-heavy CPU/transcoding work demonstrably harms Voice's real-time service levels despite bounded worker/process isolation.
3. Independent release cadence or different security/compliance trust zones require hard process or service separation.
4. Measured call concurrency exceeds an economically reasonable single-instance plan and distributed ownership becomes necessary.
5. Organizational boundaries or provider constraints require separate operational ownership and incident isolation.

Before extracting a service, require a short design review with: benchmark evidence, expected latency/availability effect, API contract, authentication/authorization model, ownership of connection state, distributed admission/idempotency, failure behavior, observability, rollback, and cost comparison.

A new, numbered ADR must **supersede or amend ADR-0001**; do not silently alter the accepted architecture. A move to multiple replicas does **not** by itself imply a move to microservices: a modular monolith may be replicated once distributed session/coordination requirements are addressed.

## 9. Implementation sequencing

1. Complete and review the initial design documents and ADRs.
2. Bootstrap the package, strict typing/linting/testing, CI, and import-boundary checks.
3. Build minimal Core contracts, explicit composition, authentication/admission seam, and lifecycle.
4. Build Voice around simulated telephony and fake AI, verifying two-way streaming and failure behavior.
5. Wire Kaiwen Agent through public APIs; independently verify the selected live AI provider contract.
6. Add the real Verimor adapter **only after** its protocol and authorization requirements are confirmed.
7. Run security/load tests, operational reviews, and the separately approved pilot gates.

Each milestone should be independently reviewable. A coding agent should record substantive deviations in a proposed ADR and request owner approval rather than silently expanding scope.

## 10. Review and record maintenance

- **Proposed → Accepted:** project owner reviews this ADR, linked architecture/requirements, and known uncertainties.
- **Accepted → Superseded:** a new ADR describes the replacement, migration plan, consequences, and links back to this record.
- **Rejected:** state the rejected rationale; do not delete the historical file.
- Do not store an agent's private deliberations, every commit, or temporary debugging notes in `docs/decisions/`. Store reviewable decisions, options, evidence, and trade-offs.
- On acceptance or substantive change, update [Architecture](../ARCHITECTURE.md), [Requirements](../REQUIREMENTS.md), and [Roadmap](../../ROADMAP.md) as appropriate.

---

**Summary:** Kaiwen Gateway V1 will be a **modular, provider-neutral Python library with one runnable ASGI service**, implementing only Voice in its initial functional scope. The modular boundary is a maintainability and testability decision; real-time capacity, security, and third-party protocol compatibility still require implementation and proof.
