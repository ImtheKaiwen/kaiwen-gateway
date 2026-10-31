# ADR-0002: Adopt Hexagonal Architecture (Ports & Adapters)

- **Status:** Accepted
- **Date:** 2026-10-31 (accepted 2026-11-01)
- **Decision owners:** Kaiwen Gateway maintainers
- **Scope:** V1 internal dependency boundaries, Voice integrations, and application composition
- **Related decisions:** [ADR-0001 — Modular Monolith](0001-modular-monolith.md), [ADR-0003 — Library and Server](0003-library-and-server.md)
- **Related documents:** [Architecture](../ARCHITECTURE.md), [Requirements](../REQUIREMENTS.md), [Project overview](../../README.md)

> **Implementation status:** This is an accepted architectural decision for V1 design boundaries (approved 2026-11-01), **not** an assertion that code, adapters, public interfaces, or production integrations exist. Example names in this document are illustrative until implemented and tested.

## 1. Context

Kaiwen Gateway will be both an importable Python library and a deployable ASGI service. Its first functional module, **Voice**, must bridge a telephony provider's bidirectional media connection to an AI realtime session, reusing the public functionality of **Kaiwen Agent**.

The system has multiple independent reasons to change:

- A telephony provider may change its authentication method, wire events, or audio encoding.
- An AI realtime provider or a compatible Kaiwen Agent public interface may change.
- The server may move between ASGI frameworks, deployment environments, or telemetry backends.
- New communication modules may be added without changing the Voice media path.
- Tests must replace network services with deterministic simulated integrations.

If framework objects, provider SDK types, and agent implementation details spread into the domain/application logic, these changes become expensive, difficult to verify, and dangerous to call isolation. Merely selecting a Modular Monolith in [ADR-0001](0001-modular-monolith.md) does not prevent this coupling.

The media path also has tight latency and memory requirements; architectural abstraction must **not** mean passing every audio frame through generic event serialization, dynamic plugins, persistence, or additional network hops.

## 2. Decision

**Adopt Hexagonal Architecture (Ports & Adapters), with dependency inversion and explicit composition, inside the Modular Monolith.**

1. **Core owns only provider-neutral shared contracts and infrastructure-neutral policies**, including authenticated connection identity, admission, module registration, lifecycle and diagnostic interfaces.
2. **Voice owns its media domain and application orchestration.** It defines the capabilities required from telephony media endpoints and AI realtime sessions as narrow, typed **outbound ports**.
3. **Inbound adapters** translate external triggers into application-facing operations. For V1, an ASGI/WSS transport adapter receives connection attempts and provider frames, but it does not own Voice application policy.
4. **Outbound adapters** implement Voice ports using a synthetic telephony endpoint, provider-specific framing, and a Kaiwen Agent-backed realtime integration as applicable. Provider details remain local to those adapters.
5. **The composition root** (`server/`, a public app factory, or an embedding host) constructs, validates, and injects concrete adapters. Domain and application code must never select providers by importing concrete implementations or reading global secrets.
6. **Dependency direction points inward:** domain and application behavior must not depend on FastAPI/Starlette, Uvicorn, OpenAI SDK types, Verimor wire formats, Redis, or concrete observability exporters.
7. **The Gateway → Kaiwen Agent dependency is one-way.** The Agent's runtime, tools, durable conversation system, and authorization controls are **reused**, not reimplemented. Integration is via supported public interfaces, isolated behind a Gateway-side adapter.
8. **Only Voice is implemented for V1.** No hypothetical Messaging, Events, MCP or plugin framework code is needed to demonstrate the boundary.

This decision applies to **source dependency structure**, not the number of services or processes. V1 remains one deployable ASGI process as defined in ADR-0001.

## 3. Architecture and dependency direction

```text
       External telephony client / provider        Embedded Python consumer
                      │                                      │
                WSS / HTTP                                Library API
                      │                                      │
           ┌──────────▼────────────┐             ┌───────────▼──────────┐
           │ Inbound adapters      │             │ Public composition    │
           │ ASGI, WSS, HTTP       │             │ / app factory         │
           └──────────┬────────────┘             └───────────┬──────────┘
                      └───────────────────┬──────────────────┘
                                          ▼
                          ┌─────────────────────────────┐
                          │ Gateway Core contracts      │
                          │ trusted identity / admission│
                          └──────────────┬──────────────┘
                                         ▼
                          ┌─────────────────────────────┐
                          │ Voice application layer     │
                          │ call lifecycle / audio      │
                          │ pumps / interruption        │
                          └───────────┬─────────┬───────┘
                                      │         │
                        outbound ports│         │outbound ports
                                      ▼         ▼
                        ┌─────────────────┐ ┌────────────────────┐
                        │ Telephony media │ │ AI realtime        │
                        │ adapter(s)      │ │ integration adapter│
                        └────────┬────────┘ └─────────┬──────────┘
                                 │                    │
                            Provider wire         Kaiwen Agent
                            or simulator          public APIs
                                                       │
                                                 AI provider

       Concrete construction/injection happens at the composition root.
       Arrows indicate calls/flow, not permission to reverse import boundaries.
```

**Important distinction:** The inbound ASGI adapter may need to participate directly in reading/writing provider frames. This does **not** require duplicate connections or an extra message queue. A narrowly scoped, call-local media endpoint can implement a Voice port while remaining owned by the transport/provider adapter. Do not introduce artificial copies or hops solely to match a diagram.

### 3.1 Ownership matrix

| Component | May define/use | Must not own or import |
| --- | --- | --- |
| `core` | Typed contracts, trusted context, admission/lifecycle interfaces, diagnostic ports | `modules.voice`, web frameworks, provider SDKs, OpenAI, Verimor, consuming apps |
| `modules.voice.domain` | Pure media/call types, capability data, validated state transitions | ASGI WebSocket, global registries, persistence drivers, provider SDKs |
| `modules.voice.ports` | Required abstract media and AI capabilities | Concrete provider session/client classes |
| `modules.voice.application` | Call supervisor, bounded audio pumps, cancellation, orchestration, Voice ports | FastAPI request objects, SDK client instantiation, environment reads, Agent private internals |
| `modules.voice.adapters` | Port implementations; translation of provider framing/events; integration with documented Kaiwen Agent public APIs | Direct coupling to sibling adapter implementations; changing trusted context based on caller claims |
| `transports` | ASGI HTTP/WSS handshakes, frame I/O, connection-level controls, adapter dispatch | Agent reasoning, business tool authorization, generic Voice domain implementation |
| `server` / composition root | Configuration, secrets injection, route/app construction, startup/shutdown, adapter selection | Business policy hidden in generic registry, network activity at import time |
| Consuming product | Tenant provisioning, identity resolution, business policy, tool permissions, retention/legal decisions | Direct dependence on Gateway's private implementation details |

Shared contracts belong in `core` **only if** more than one module genuinely requires them. Voice-specific media types should remain in `modules.voice` instead of expanding Core preemptively.

### 3.2 Allowed imports

```text
core                    -> Python stdlib + minimal typing/validation dependencies
voice.domain            -> core contracts as needed + voice-local pure types
voice.ports             -> core + voice.domain
voice.application       -> core + voice.domain + voice.ports
voice.adapters.*        -> voice.ports + voice.domain + explicitly needed SDKs
transports.*            -> core contracts + transport/framework dependencies
server / CLI            -> all layers needed for explicit wiring
```

The precise module paths may change during implementation, but the **direction and ownership rules are normative**. Exceptions require architectural review, not an undocumented shortcut.

### 3.3 Forbidden dependencies and behaviors

- `core` importing any concrete module, ASGI implementation, Kaiwen Agent, or provider integration.
- Voice application accepting an `ASGI WebSocket`, OpenAI SDK event, or raw Verimor message as its business-facing contract.
- Provider-specific string event names, sample rates, credentials, or retry rules hard-coded into Core.
- Provider adapters importing or modifying another adapter's private state.
- Global mutable session registries or module auto-discovery with hidden startup side effects.
- Import-time network connections, background tasks, or access to mandatory credentials.
- Kaiwen Agent importing Gateway, or Gateway copying Agent's realtime session manager, tool executor, policies, or scheduler.
- Circular dependencies disguised through dynamic imports or service-locator lookups.
- Generic event buses, durable queues, or per-frame JSON conversion in the live media hot path without a measured, accepted requirement.

## 4. Port design principles

Ports express **capabilities required by an application use case**, not a copy of a third-party SDK.

1. **Small and cohesive:** separate audio streaming, lifecycle/control operations, authenticated context, and diagnostics when their lifecycles differ.
2. **Typed and explicit:** define input/output types, error classes, cancellation semantics, capability negotiation, and ownership of resources.
3. **Async where I/O occurs:** use asynchronous contracts for connection, send, receive, and close. Pure validation or configuration can remain synchronous.
4. **Streaming-first:** transfer binary media as ephemeral frames (`bytes`, `memoryview`, or a carefully measured immutable frame abstraction), with minimal transformation.
5. **No implicit retries:** live media cannot be blindly replayed; retry only explicitly safe and idempotent control operations.
6. **Capabilities over assumptions:** interruption, playback acknowledgment, DTMF, transfer, codecs, reconnect, and cancellation must be declared as supported/unsupported rather than assumed for all providers.
7. **Ownership is explicit:** exactly one call supervisor owns the call-local tasks, queues, upstream session and final cleanup.
8. **Errors are useful:** distinguish authentication failure, unsupported capability/format, provider refusal, timeout, overload and clean disconnect; do not leak secrets or raw sensitive payloads.

### 4.1 Illustrative contract sketch (not a frozen public API)

```python
from collections.abc import AsyncIterator
from dataclasses import dataclass
from typing import Protocol


@dataclass(frozen=True, slots=True)
class AudioFrame:
    payload: bytes
    codec: str
    # Additional timing/sequence data is defined only when required.


class MediaEndpoint(Protocol):
    def incoming_audio(self) -> AsyncIterator[AudioFrame]: ...

    async def send_audio(self, frame: AudioFrame) -> None: ...

    async def close(self) -> None: ...


class AIRealtimeSession(Protocol):
    async def send_audio(self, frame: AudioFrame) -> None: ...

    def outgoing_audio(self) -> AsyncIterator[AudioFrame]: ...

    async def close(self) -> None: ...
```

**This example omits essential production concerns deliberately:** control events, authentication, trust binding, pause/interruption, playback completion, frame timestamps, backpressure, errors and cancellation. These must be specified in reviewed contracts and their conformance tests before implementation is considered complete. In particular, an abstract `close()` does not imply that the telephony call itself has been hung up.

Do not add generic `execute(command: dict)` or `handle(event: Any)` escape hatches when narrow typed contracts can express the needed operation. Likewise, do not require every audio frame to allocate a complex domain object if profiling shows it is unnecessary.

### 4.2 Transport and business policy boundary

A WSS adapter may terminate a TLS connection (or receive a verified connection from a trusted ingress), parse frames, and present an authenticated transport identity. It **must not** infer a tenant's business permissions from an unverified `tenant_id`, caller ID, SIP header, or speech content.

The host application or a trusted configured resolver must authorize the provider identity, bind an **immutable per-call security context**, enforce admission policy, and then permit creation of the AI session. Authorization and resource admission occur **before** paid upstream allocation. Any missing security-critical configuration fails closed.

The Gateway is responsible for transport identity and call isolation. **Kaiwen Agent and the consuming application** remain authoritative for business-tool permissions, human approval, and external side effects. Model output is never authorization.

### 4.3 Telephony and AI adapter boundaries

**Telephony adapter** translates a confirmed provider's handshake, audio framing, codec metadata, and supported call-control events into the Voice ports. The synthetic V1 adapter uses a **clearly fictional project-owned test protocol**. A real Verimor adapter requires authoritative verification of handshake, authentication, direction, media frames, codec, sequence/timestamps, interruption/playback semantics, call lifecycle, and error handling.

**AI adapter** creates and manages the AI-facing realtime session through supported **Kaiwen Agent public APIs** where suitable. It maps supported media/control behavior and observes Agent's existing policy/sideband semantics. It does not bypass Agent tool authorization or import private provider helpers merely because doing so shortens implementation. Validate actual OpenAI Live API interoperability separately before claiming a live integration.

## 5. Realtime media, state, and cancellation

Hexagonal design must preserve the real-time properties of Voice:

- **Two call-owned pumps:** telephony → AI and AI → telephony may run concurrently, but both are tied to the same supervisor and cancellation scope.
- **Bounded buffering:** slow consumers must not produce unbounded memory usage or silently accumulate seconds of delayed speech. Define limits in bytes *and/or* buffered audio duration.
- **No generic serialization:** raw audio remains an ephemeral binary stream; high-level session/control events can use separate structured messages.
- **Codec decisions at boundaries:** negotiate/validate compatibility and transform only when required. Do not claim zero-copy without measurement.
- **Interruptions are capability-aware:** when barge-in occurs, cancel/stop AI output where supported, clear unplayed output locally, and keep conversation state coherent. Unsupported playback acknowledgment must be reported, not simulated.
- **Distinct state machines:** transport connection, telephony call, and Agent realtime session can terminate or recover differently. Do not merge them into one misleading enum.
- **Idempotent cleanup:** any task failure, disconnect, timeout or cancellation initiates one bounded cleanup sequence, releasing queues and provider sessions without leaking tasks or credentials.
- **No fake resume:** a replaced WSS connection does not automatically restore the telephony call, AI state, or lost audio. Reconnect behavior needs an explicit verified contract.

Architecture abstraction must be evaluated against **p95/p99 added gateway latency, allocation rates, memory per active call, queue age and event-loop lag**. Complexity that imposes measurable hot-path overhead without corresponding isolation or maintainability benefits should be rejected.

## 6. Construction and lifecycle

The deployment application (or importing host) acts as the composition root:

1. Parse and validate typed configuration, including required security settings.
2. Create narrow identity/authentication resolvers and admission controls.
3. Construct explicitly selected transport, Voice and Agent-backed adapters.
4. Register the one V1 Voice module; reject invalid, duplicate or unsupported registration.
5. Assemble an ASGI app with separate health/readiness semantics; no hidden mutable global app singleton.
6. On a verified incoming call, bind a trusted call context, enforce limits, then open the upstream session and two call-owned media pumps.
7. On application shutdown, stop accepting new calls, drain under a defined policy, and close outstanding resources.

The library must also be embeddable: constructing two independently configured app instances must not make them share a session registry, secrets, or provider state by accident. Scope resource ownership to the explicit application instance or call.

**Optional dependency rule:** importing the minimal `kaiwen_gateway` package must not require FastAPI, a telephony SDK, an OpenAI SDK, or an already-running provider. Load optional integrations only when the selected server/module adapter is constructed.

## 7. Alternatives considered

| Option | Benefits | Trade-offs | Outcome |
| --- | --- | --- | --- |
| **Hexagonal / Ports & Adapters (selected)** | Provider-neutral logic, deterministic fake adapters, replaceable integrations, enforceable dependency rules | Requires disciplined interface design; too many tiny abstractions can create overhead | **Selected** |
| Direct FastAPI route → OpenAI SDK | Quick first prototype, fewer files | Hard-codes wire/provider choices, weak mockability, difficult call isolation and replacement | Rejected as system architecture |
| Traditional layered design without explicit ports | Familiar and simple initially | Infrastructure details can leak into services; provider swapping relies on convention | Rejected for integration boundaries |
| Generic event-driven/event-bus architecture | Flexible for asynchronous business events | Per-frame serialization/queue overhead, latency and backpressure complexity for voice | Rejected for live audio hot path; allowed for low-frequency events |
| Dynamic plugin microkernel | Potential external extension marketplace | Complex discovery, versioning, lifecycle and sandboxing without a V1 need | Deferred |
| Distributed microservices | Independent scaling of components | Higher operational complexity and network latency before demonstrated need | Governed separately by ADR-0001; not selected for V1 |

## 8. Consequences and risks

### 8.1 Positive consequences

- The Core remains independent of telephony and AI SDK details.
- Fake telephony and AI adapters allow offline simulation, CI, fault injection and reproducible tests.
- Caller/session identity rules can be tested without placing real calls or exposing customer data.
- Provider swaps are localized where contracts are truly compatible; incompatible capabilities are surfaced explicitly.
- The same package can be embedded in an application or hosted as an ASGI service without duplicating Voice orchestration.
- Other communication modules could be added later without forcing Voice-specific types into Core.

### 8.2 Costs and risks

- Ports may proliferate if interfaces are created for hypothetical future products.
- Abstraction can hide expensive copies, codec conversions, or unnecessary synchronization if not benchmarked.
- Provider APIs rarely match exactly; least-common-denominator ports can conceal useful or critical capabilities.
- The Gateway and Kaiwen Agent may drift if upstream public interfaces change without compatibility tests.
- One process still has a shared failure domain; Hexagonal Architecture does **not** itself deliver security, availability, throughput or distributed coordination.

### 8.3 Mitigations

- Define ports from **current Voice use cases and tested fakes**, not speculative integrations.
- Model optional capabilities explicitly; fail clearly when unsupported.
- Profile hot-path transforms and avoid buffering/copying unless justified.
- Prefer documented Kaiwen Agent public APIs; version the supported dependency range and test it.
- Treat immutable verified identity, per-call ownership, cancellation and backpressure as acceptance requirements, not stylistic choices.
- Require an ADR and review for exceptions that alter core/module ownership or provider trust boundaries.

## 9. Verification and enforcement

The following are required **evidence**, not claims about current implementation:

| Check | Test / evidence | Related requirement |
| --- | --- | --- |
| Core has no forbidden imports | Static import-boundary checks and code review | `ENG-006` |
| Minimal library import works | Clean environment without voice/server provider extras | `FR-CORE-005` |
| Independent composition | Build two app instances; confirm separate settings, registries, sessions | `FR-CORE-002` |
| Adapter interchangeability | Same contract tests with fake telephony and fake AI providers | `FR-INT-001`, `FR-INT-002` |
| Trust isolation | Invalid identity, forged tenant, replay and cross-call audio tests | `FR-CONN-002`, `SEC-003`, `FR-VOICE-007` |
| Cancellation and cleanup | Fault-inject send/receive, timeout, disconnect, cancel; assert no leaked tasks | `FR-CONN-006`, `FR-VOICE-008` |
| Buffer safety | Slow-reader simulation; bounded queues and defined overload behavior | `FR-VOICE-005` |
| Capability reporting | Unsupported transfer, codec, interruption or resume must fail explicitly | `FR-INT-009`, `FR-VOICE-006` |
| Protocol honesty | Simulator labeled synthetic; real Verimor unverified until approved sandbox test | `FR-INT-006`, `FR-INT-007` |
| AI dependency compatibility | Test documented Kaiwen Agent API, separately validate actual OpenAI Live wire contract | `FR-INT-004`, `FR-INT-005` |
| Performance regression | Synthetic load tests for latency, memory, copies and event-loop lag | `NFR-PERF-*` |

CI should reject **provable** import-boundary violations and failing conformance/security tests. Performance thresholds are defined from a controlled baseline, avoiding flaky unconditional CI failures on shared runners.

## 10. When to reconsider

Revisit this ADR if **measured evidence** establishes one or more of the following:

1. Voice hot-path overhead from port boundaries causes meaningful latency or allocation regressions that cannot be resolved with narrower interfaces or optimized implementations.
2. Multiple communication modules require stable shared capabilities that justify moving a specific contract from Voice into Core.
3. A provider requires semantics the current port cannot express without unsafe behavior; evolve the port or introduce a capability-specific extension, rather than hiding the difference.
4. A supported Kaiwen Agent public API changes or becomes unsuitable, requiring a revised integration contract or an upstream proposal.
5. A distinct trust boundary or operational constraint requires process/service separation; address deployment architecture in a **new ADR**, not through ad hoc imports.

A superseding ADR must explain the trigger, alternatives, consequences, migration plan and verification evidence. Do not silently rewrite an accepted historical decision.

## 11. Decision governance and implementation sequence

1. Treat this accepted ADR as a binding V1 dependency-boundary rule; any material boundary change requires maintainer review and a superseding ADR.
2. Bootstrap the minimal package, app factory, strict tooling and import checks.
3. Implement call context, Voice ports and deterministic fake adapters with conformance tests.
4. Implement the WSS/ASGI adapter and Voice supervisor with safe cancellation, flow control and cleanup.
5. Integrate through documented Kaiwen Agent public APIs and validate the selected live provider independently.
6. Implement real Verimor wire mapping **only after** authoritative provider documentation and permission for testing are available.
7. Update [Architecture](../ARCHITECTURE.md), [Requirements](../REQUIREMENTS.md), the roadmap and changelog when accepted decisions result in code changes. Keep release status separate from document status.

### Review record

- **Decision state:** Accepted (2026-11-01)
- **Approved by:** Maintainer
- **Implementation evidence:** Not yet recorded (pre-implementation phase)
- **Next review trigger:** Before first Gateway Core / Voice port implementation

---

**Decision summary:** Keep business/application orchestration independent of incoming transport and external AI/telephony SDKs. Use narrow typed ports, explicit adapter construction, and strict dependency direction. Preserve a direct, bounded, call-local media path while reusing Kaiwen Agent and keeping all unverified Verimor wire details outside Core.
