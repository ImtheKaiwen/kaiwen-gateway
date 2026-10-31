# Kaiwen Gateway — V1 Architecture

> **Status:** Approved M0 V1 design baseline (ADR-0001..0003 accepted 2026-11-01) — not an implementation report
> **Document version:** 1.0
> **Initial release target:** `0.1.0a1` (synthetic alpha)
> **Last updated:** 2026-11-01
> **Scope:** Reusable Python library + deployable ASGI server; **Voice** is the only V1 communication module.

## 1. Purpose, authority, and implementation status

This document describes **how** the requirements in [REQUIREMENTS.md](REQUIREMENTS.md) should be implemented: component boundaries, dependency direction, trust boundaries, media and control flows, failure handling, deployment topology, and verification strategy. The public product overview lives in [../README.md](../README.md); delivery sequence belongs in [../ROADMAP.md](../ROADMAP.md); enduring decisions belong in [decisions/](decisions/).

**This is a design, not an implementation report.** Names, layouts, contracts, endpoints, and examples here are *proposals*. They become an implemented public API only after source code, tests, and documentation agree. Do not mark the system production-ready or Verimor-compatible based on this document.

Key constraints:

1. One repository and one deployable application for V1; independently importable Python library components.
2. **Modular Monolith** for packaging/deployment, **Hexagonal Architecture (Ports & Adapters)** for dependency boundaries, and dependency inversion for integrations.
3. One implemented business module: **Voice**. Messaging, Events, Streaming, AI API proxy, and Tool/MCP Gateway remain potential later modules, not V1 implementations.
4. **Reuse Kaiwen Agent.** The Gateway does not implement a second agent executor, tool-authorization pipeline, task scheduler, or durable conversation system.
5. **No provider-specific assumptions in Core.** The provider's actual WSS protocol must be obtained before the real adapter is claimed to work.
6. The live audio path must avoid unnecessary persistence, JSON conversion, shared message buses, and blocking operations.

### 1.1 Related requirement families

- Library and module contracts: `FR-CORE-*`, `ENG-001/006`.
- Connection and trusted identity: `FR-CONN-*`, `SEC-001/004`.
- Media flows: `FR-VOICE-*`, `NFR-PERF-*`, `NFR-REL-*`.
- Telephony and AI adapters: `FR-INT-*`.
- Observability and operations: `FR-OPS-*`, `OBS-*`, `CI-*`.
- Pilot and release gates: [REQUIREMENTS.md §7](REQUIREMENTS.md#7-acceptance-scenarios-and-release-gates).

## 2. System context and architectural boundaries

```text
                  Untrusted external network
  Caller ── PSTN ── Telephony provider (target: Verimor)
                           ↕
                 Bidirectional media WSS
                 (wire contract pending)
                           ↕
┌──────────────────────────┴──────────────────────────────┐
│ Kaiwen Gateway deployment (ASGI process)               │
│                                                         │
│ [ASGI/WSS transport adapter]                            │
│     │                                                   │
│ [Authentication + trusted call context]                 │
│     │                                                   │
│ [Admission / limits / connection lifecycle]              │
│     │                                                   │
│ [Voice module: media bridge + control + cancellation]    │
│     │                     │                             │
│ [Telephony media port]   [AI realtime port]              │
└──────────────────────────┼──────────────────────────────┘
                           ↕
              Kaiwen Agent library integration
              ├─ realtime sessions and providers
              ├─ conversation/context bridge as needed
              └─ tools, permission/approval/audit policies
                           ↕
                    OpenAI Live (initial AI)
```

This diagram shows **logical ownership**, not an assumption that every byte must flow through the agent's durable conversation or tool-execution subsystem. Raw media is streamed ephemerally; stable semantic conversation events may be forwarded to the application/Agent only when needed.

### 2.1 Ownership matrix

| Component | Responsible for | Explicitly not responsible for |
| --- | --- | --- |
| **Gateway Core** | Module registration; authenticated connection context; admission; connection lifetime; common diagnostics and extension points | Decoding audio; Verimor wire rules; LLM reasoning; application business policy |
| **ASGI/transport** | HTTP/WSS handshake, frame I/O, transport-level backpressure, close signalling | Arbitrary tenant trust or agent/tool authorization |
| **Voice module** | Call-specific state; two media pumps; audio validation, framing/codec decisions, interruption; cleanup and voice-specific fallback | General HTTP routing, universal events, second agent loop |
| **Telephony adapter** | Provider handshake/control/audio mappings, provider capability declarations | Assigning trusted tenant permissions based only on caller-supplied values |
| **AI adapter** | Opening/closing provider realtime sessions; sending/receiving audio/control events, translating provider errors | Authorizing external callers or executing tools without Agent policy |
| **Kaiwen Agent** | Existing runtime, realtime/session contracts, context, tools, policies, audit, task delegation | Public Gateway WSS listener, provider-specific media server |
| **Consuming organization** | Tenant onboarding, legitimate call routing, prompts/tools, legal basis, data retention, operator approval policy | Core transport internals |

**Dependency law:** `kaiwen-gateway → kaiwen-agent` is allowed; `kaiwen-agent → kaiwen-gateway` is forbidden. The Gateway must use documented Agent public APIs, pin compatible versions, and propose upstream general-purpose changes rather than importing Agent private helpers or copying subsystems.

### 2.2 Three distinct identities

Do not collapse these identities:

- **Transport connection:** short-lived connection and its verified provider (`connection_id`).
- **Call / session:** one authorized call (`call_id`, tenant-bound) with one or more transient transport attempts (`gateway_session_id`); provider IDs are metadata.
- **Conversation:** application-owned `conversation_id` associated with a trusted tenant/subject; it may outlive one transport connection.

The authenticated caller is **not necessarily** a known application user. Telephone number/caller ID is not proof of identity. If an Agent `user_id` is required, the consuming application maps an authenticated principal or an explicitly scoped service/call identity; it must not silently equate a phone number with a fully authorized account.

## 3. Proposed package organization

```text
kaiwen-gateway/
├── src/kaiwen_gateway/
│   ├── __init__.py                    # intentionally small public exports
│   ├── core/
│   │   ├── contracts/                 # typed module/connection ports
│   │   ├── identity/                  # verified immutable context
│   │   ├── admission/                 # admission/limits policies
│   │   ├── registry/                  # explicit module registration
│   │   ├── lifecycle/                 # connection/draining state
│   │   └── observability/             # safe telemetry ports
│   ├── transports/
│   │   ├── websocket/                 # ASGI websocket adapter
│   │   └── http/                      # health/readiness/webhook transport
│   ├── modules/
│   │   └── voice/
│   │       ├── domain/                # call/media types and invariants
│   │       ├── application/           # session orchestration and audio bridge
│   │       ├── ports/                 # telephony/AI/media contracts
│   │       └── adapters/
│   │           ├── simulated/         # fictional test provider
│   │           ├── openai_live/       # Kaiwen Agent-backed integration
│   │           └── verimor/           # only after authoritative spec
│   ├── server/                        # ASGI composition root and lifespan
│   └── cli.py                         # launch/config validation entry point
├── tests/
│   ├── unit/
│   ├── contract/
│   ├── integration/
│   ├── security/
│   └── performance/
├── examples/                         # synthetic only
├── docs/
│   ├── REQUIREMENTS.md
│   ├── ARCHITECTURE.md
│   └── decisions/
├── deploy/                             # container and operations examples
├── pyproject.toml
├── README.md
└── ...
```

**This is a target layout, not a create-every-file directive.** Create only the subpackages required for the current implementation milestone. A future `messaging/` directory is not needed until that module exists.

### 3.1 Import/dependency rules

```text
core                  → standard library + small typed dependencies
voice.domain          → core contracts / voice-local pure types
voice.application     → core + voice.domain + voice.ports
voice.adapters        → corresponding ports + optional provider SDK/library
transports            → core ports + optional ASGI framework
server / CLI          → composition of core, transports, modules, adapters
```

- **Forbidden:** Core importing `voice`, FastAPI, Verimor, OpenAI, the deployed application, or runtime SDK objects.
- **Forbidden:** A provider adapter importing concrete sibling adapters.
- **Forbidden:** Voice application service accessing ASGI `WebSocket` objects directly; use a transport/media port.
- **Forbidden:** Copying Kaiwen Agent's `RealtimeSessionManager`, `ToolExecutor`, or agent loop into Gateway.
- **Required:** Provider-dependent optional imports occur at the adapter boundary; a minimal import must not load all optional dependencies.
- **Required:** Construction is explicit in the composition root; no hidden global mutable registry or startup network side effects.

Import/layer violations should be statically checked where practical and exercised in CI.

## 4. Public composition and module contract

V1 ships a **small, explicit** module registry. The interface is intentionally narrower than a general plugin runtime.

Illustrative typed contracts (names are conceptual, not shipped code):

```python
from typing import Protocol

class GatewayModule(Protocol):
    @property
    def name(self) -> str: ...

    async def startup(self) -> None: ...
    async def shutdown(self) -> None: ...

class AdmissionPolicy(Protocol):
    async def admit(self, context: "VerifiedConnectionContext") -> "AdmissionLease": ...

class GatewayTelemetry(Protocol):
    def connection_event(self, event: "SafeConnectionEvent") -> None: ...
```

Contracts must specify lifecycle and failure semantics, not merely method signatures. Module registration is explicit (`register(...)`-style), rejects duplicates and invalid configurations, and cannot bypass core authentication. No runtime discovery, execution of downloaded plugins, or module marketplace is required for V1.

The library provides an **ASGI app factory** so independent applications can compose separate Gateway instances; the same package may include an optional CLI/server extra. Configuration is typed and validated; insecure or missing production credentials fail closed. CLI command strings and public endpoint names are **not frozen** until implemented and tested (`OPEN-007`).

## 5. Voice module: ports, formats, and flow

### 5.1 Provider-neutral contracts

Keep at least the following conceptual boundaries:

| Port | Responsibility | Example implementation |
| --- | --- | --- |
| **TelephonyMediaPort** | Provider connection and close; receive decoded control/media envelope; send audio and supported playback controls | Fictional simulator; later actual Verimor adapter |
| **AIRealtimePort** | Create an authorized provider session; send input audio; receive output audio and semantic events; interrupt/close where supported | Kaiwen Agent-backed OpenAI Live adapter; fake AI |
| **CallAuthorizer** | Resolve inbound verified provider credentials/claims to immutable tenant/call context | Application-owned adapter |
| **AdmissionPort** | Reserve and release connection capacity atomically at the current deployment scope | Process-local V1; distributed implementation later |
| **CallEvents / Telemetry** | Observe safe lifecycle events and aggregate metrics | In-memory test sink; optional exporter |

These are conceptual seams; implementations may consolidate interfaces if doing so simplifies V1 without losing testability or isolation. Do **not** build an interface for every one-line helper.

### 5.2 Media and control planes

Two paths have different requirements:

```text
MEDIA (high-rate, ephemeral; do not publish to generic event bus)
provider media frame → validate → (decode/reframe only if necessary)
                    → bounded per-call buffer → AI input
AI audio output      → validate → (encode/reframe only if necessary)
                    → bounded per-call buffer → provider playback

CONTROL (low-rate, semantic)
call started / connected / interrupted / failed / ended
                    → call state + safe metrics + optional application event
```

Never assume provider frames are PCM or JSON; frame parsing/encoding is adapter-owned. Establish **negotiated or configured codec, sample rate, channel count, packet size, sequencing, and timing policy** before allocating media buffers. The gateway does not fabricate negotiation support if the provider is fixed-format.

A provider's media timestamp is not globally trusted wall-clock time; use monotonic local timing for latency and duration measurements. Use a shared session epoch/generation to fence stale audio during interruption or reconnect.

### 5.3 One call lifecycle — reference sequence

```text
1. Provider opens proposed WSS connection / sends start event
2. ASGI transport accepts request for authentication processing
3. Authenticator validates credentials and provider constraints
4. Application resolves trusted tenant + call authorization
5. Admission grants bounded capacity lease; otherwise reject
6. Voice builds isolated CallSession and associates conversation context
7. AI adapter opens one call-scoped upstream realtime session
8. Run inbound and outbound audio pumps with supervised tasks
9. Process interruption, errors, and low-rate lifecycle signals
10. On termination: stop pumps, clear audio, close provider session,
    release admission lease, emit safe terminal diagnostics
```

**Handshake nuance:** Step 2 must not imply a successful WebSocket upgrade before authorization; where credentials are available in the HTTP upgrade request, reject **before upgrading**. If the provider requires a post-upgrade authentication exchange, permit only a constrained unauthenticated handshake state with strict timeout/frame/byte limits and allocate no upstream AI resources until verification passes.

`CallSession` retains the immutable verified security context and references to per-call audio queues, media tasks, AI session, and counters. A caller payload must never mutate `tenant_id`, permitted tools, or upstream endpoint.

### 5.4 Connection and call state

Proposed gateway transport states:

```text
NEW → AUTHENTICATING → ADMITTED → ACTIVE → CLOSING → CLOSED
  └→ REJECTED            └→ FAILED  └→ FAILED
                          ACTIVE → DRAINING → CLOSING
```

Actual state transitions and terminal behavior must be guarded and tested. `DRAINING` means **the instance rejects new calls** while existing sessions continue under a bounded deadline; it is primarily a deployment/admission state, not necessarily a per-call protocol message.

Keep distinct the **Gateway connection state**, **telephony call state**, and **Kaiwen Agent logical realtime session state**. Do not force them into one enum: for example, a WSS transport failure may terminate the media leg without proving the caller's PSTN call has ended.

### 5.5 Concurrency and task ownership

For every authorized call:

```text
CallSupervisor (one lifecycle owner)
├─ inbound_pump: telephony → AI
├─ outbound_pump: AI → telephony
└─ control/liveness handling (as required)
```

- Audio pumps are async and independently scheduled, but owned by one supervisor (`TaskGroup` or equivalent).
- A normal termination, one pump's failure, or cancellation results in deterministic shutdown of related tasks.
- Every external operation has appropriate timeout/cancellation semantics.
- Do not make blocking HTTP/SDK or expensive codec work on the event loop; isolate CPU-heavy codecs into bounded workers when measured necessary.
- Keep bounded queues per call, not one shared queue for all customers.
- **Never** silently accumulate unbounded voice data. Under sustained overload, apply protocol-supported flow control or a defined degradation/termination policy; arbitrary loss of spoken content is not an acceptable default.
- Cleanup is idempotent; terminal events and capacity release occur at most once semantically even if disconnect/error callbacks race.

### 5.6 Barge-in, queued output, and reconnect

When the caller interrupts AI speech:

1. Receive a verified provider interruption or validated VAD/turn signal.
2. Cancel or pause AI generation through the supported provider operation.
3. Invalidate **unplayed** outbound frames by playback generation/epoch, where permitted.
4. Synchronize the conversation timeline only using documented provider playback acknowledgment and truncation semantics.
5. Preserve already-heard content and permitted context; never pretend queued or discarded output was spoken.

If a provider cannot acknowledge playback/cancel/truncate, document and test the limitation; do not promise perfect barge-in. Connection reconnects must distinguish resuming a **logical conversation** from resuming an opaque provider audio session. Never replay audio or side-effecting tools blindly after reconnect.

## 6. Integration with Kaiwen Agent

The existing `kaiwen_agent.realtime` library provides logical realtime lifecycle, provider abstraction, per-tenant/user session limits, and audio/control entry points. The Agent also owns tool safety, context/memory bridges, and policy-controlled task delegation. **Gateway reuses these at the appropriate seams.**

Initial integration design:

1. Authenticate the provider and resolve trusted tenant/call metadata inside the host application.
2. Construct a call-scoped Agent context and `conversation_id` from trusted application records; never from untrusted speech or caller ID alone.
3. Use public Agent realtime APIs (or a narrow, explicitly reviewed adapter) to create an upstream AI session.
4. Send transient audio into the provider connection and consume its output/event stream.
5. Map only stable semantic transcript/call events into durable conversation logic when explicitly configured; do not persist raw audio by default.
6. For tool calls, reuse Agent's central tool executor, permissions, approval, idempotency and audit composition. An LLM's suggested tool call is **not** authorization.

**Important verification:** Existing Agent realtime code and tests do not prove that the current OpenAI Live wire protocol and audio formats are compatible with a live provider session. The OpenAI transport endpoint, message schema, codec support, interruption semantics and limits must be verified against the official API and tested (`OPEN-003`, `FR-INT-005`). Avoid private Agent implementation imports even if they appear convenient.

The gateway shall not automatically share one upstream AI session among unrelated calls. Provider connection state, instructions, tenant tools and usage accounting are call-scoped, even if immutable provider configurations are shared.

## 7. Security and data privacy architecture

### 7.1 Trust boundaries

```text
[Public network / provider payload]
       │ untrusted
       ▼
[Transport handshake + Authenticator]
       │ verified provider identity
       ▼
[Application call/tenant authorization + Admission]
       │ immutable VerifiedConnectionContext
       ▼
[Voice application session]
       │ scoped AI session and tool permissions
       ▼
[Kaiwen Agent / AI provider]
```

Only authenticated, authorized, admitted calls may create upstream AI connections. The provider's phone number, presented call ID, WSS JSON fields or SIP headers do not by themselves grant tenant/user authority. If supported by the confirmed provider contract, prefer mTLS or signed short-lived, replay-resistant credentials, but do not hard-code an unverified Verimor scheme.

### 7.2 Isolation and abuse resistance

- Every operation is scoped to **verified tenant + call + permission context**.
- Per-call memory, queues, tasks, AI sessions, trace context, and cleanup ownership are independent.
- Cross-tenant access is checked again at persistent stores and tools; a call ID alone is insufficient authorization.
- Reject malformed frames, unsupported media types, oversized payloads, invalid control transitions, excessive admission rates and resource exhaustion.
- Restrict AI egress to approved destinations; never accept arbitrary upstream URLs from caller payloads (SSRF defense).
- Secrets remain server-side; do not include raw tokens in WebSocket URLs or normal logs when avoidable. Use a provider-compatible secure handshake and redact request metadata.
- Limit browser origins if browser clients are later supported, but an Origin header alone is **not** a provider authentication mechanism.

### 7.3 Data handling

**Default:** process live audio and temporary transcript deltas in memory; do not durably record media or full transcripts. Any recording, transcript retention, evaluation export or model training requires separately configured application policy, justified access and data protection review. Public test assets must be synthetic/sanitized; never commit customer audio, phone numbers, API credentials, or private provider documents.

WSS/TLS protects audio **in transit** to the Gateway, not from the Gateway or external AI processing provider. Location of processing, processors/subprocessors, international transfer, lawful basis, retention and incident handling require a separate documented decision before a real-company pilot. See `SEC-*` and `OPEN-004` in requirements.

## 8. Capacity, observability, and service health

### 8.1 Single-process V1 topology

```text
Telephony WSS ──TLS termination / reverse proxy──► ASGI process (1 worker)
                                                     ├─ Gateway Core
                                                     ├─ Voice sessions + bounded queues
                                                     ├─ Kaiwen Agent adapter
                                                     └─ outbound WSS to AI provider
```

The V1 reference deployment is **single-process**. Do not imply that an in-process lock or active-session dictionary is a distributed connection manager. Two Uvicorn workers do not share in-memory live connections or session limits. Multi-instance deployment requires explicit admission coordination, duplicate-event protection, ownership/routing strategy and draining semantics; do not add Redis/Kubernetes merely for appearance.

The reverse proxy must support WebSocket upgrades and configured idle/read/write limits. Health (`alive`) is distinct from readiness (`accepts new calls`); a draining instance is alive but not ready for admission. There must be a configured maximum time for draining, after which the operator policy controls transfer/fallback/termination rather than promising seamless live socket migration.

### 8.2 Media hot-path performance

- Minimize copies and format conversions; profile before optimizing.
- Validate frames once at the boundary; avoid repeated JSON encoding of media.
- Track per-direction receive→send processing latency, bounded queue duration, aggregate packet throughput and event-loop lag.
- Measure p50/p95/p99 under documented load, codec, runner hardware, network/AI fixtures and failure injection.
- The **≤ 50 ms p95 Gateway-added overhead** from `NFR-PERF-004` is a **design target**, not a measured guarantee. Keep upstream model inference/network latency separate.
- Synthetic benchmarks progress 1 → 5 → 25 calls; only then attempt 50 and 100. Configured session caps are not throughput evidence.

### 8.3 Logs, traces, and metrics

Emit **structured, redacted lifecycle logs**, not per-frame logs. Example schema (not an existing implementation):

```json
{
  "event": "voice.session.closed",
  "level": "INFO",
  "trace_id": "opaque-trace-id",
  "tenant_key": "safe-internal-id",
  "session_key": "opaque-session-id",
  "duration_ms": 73421,
  "close_reason": "normal"
}
```

Metrics should include active sessions, rejected admissions, provider failures, queue age/occupancy, bridge latency, unexpected disconnect rate, and cleanup errors. **Never** use call IDs, phone numbers, transcripts, or other unbounded sensitive values as metric labels. OpenTelemetry exporters are optional adapters; importing the core must not require Grafana/Prometheus/Tempo/Loki services.

Trace correlation may join Gateway and Agent stages only through explicitly permitted safe IDs. Observability must not become a new source of raw-audio storage.

## 9. Errors, recovery, and operational policy

| Failure | Required V1 behavior | What not to claim |
| --- | --- | --- |
| Unauthenticated/invalid WSS attempt | Reject early; bounded diagnostics; allocate no AI session | Client has any tenant authority |
| Unsupported/oversized media | Reject according to documented protocol; close if unsafe; release resources | All corrupt streams recover without loss |
| Slow AI or telephony peer | Bounded queues, backpressure, timeout/degradation policy | Unlimited buffering is safe |
| AI provider fails/rate-limits | Isolate affected call; safe fallback or controlled close; classify error | Transparent seamless provider failover |
| Telephone provider disconnects | Stop audio and clean upstream; record safe terminal event | PSTN call definitely ended solely from WSS close |
| Duplicate start/control events | Idempotent handling with deployment-appropriate atomicity | Local in-memory check protects multi-node races |
| Shutdown while calls are active | Stop admission; drain until deadline; terminal policy | Existing WebSockets magically migrate to another process |
| Tool call suggested by LLM | Enforce application/Agent permission, approval and audit | Model output authorizes an action |

An operator runbook must describe provisioning, configuration validation, credential rotation, provider outage response, draining, deployment rollback, privacy escalation and incident review before a live pilot. Calls requiring a human handoff must check telephony provider transfer capability. When no transfer facility exists, fail safely and do not claim handoff is implemented.

## 10. Development, testing, and release architecture

### 10.1 Deterministic test stack

- **Unit:** pure core contracts, state transitions, codecs/frame validation, admission and policy boundaries.
- **Contract:** each provider adapter against the same fake media/control conformance scenarios; unsupported capabilities fail explicitly.
- **Integration:** simulated telephony ↔ Gateway ↔ fake AI end-to-end audio, authorization and lifecycle.
- **Security:** forged identity, cross-tenant audio/context leakage, replay, secret redaction, malformed frames, egress restrictions.
- **Fault injection:** slow consumer, upstream failure, task cancellation, double-close, timeout, draining, reconnect and interrupted playback.
- **Load/soak:** reference hardware, 1/5/25 stages, later 50/100; capture latencies and resource use.
- **Live provider:** separately authorized sandbox smoke checks for OpenAI and real Verimor; no secret or customer data in CI by default.

The simulated telephony provider **must be explicitly labeled fictional**. Until the official Verimor contract arrives, the test harness validates Gateway behavior, *not* Verimor wire compatibility.

### 10.2 Quality gates

CI on Pull Requests should require Ruff format/lint, strict typing, unit/contract/mock integration tests, package build and clean installation smoke. Security scanning and workflow checks are additional gates. Scheduled/manual stress tests and authorized external-provider tests must remain separate from fast deterministic PR CI.

A release pipeline verifies its artifact before publication, uses least-privilege workflow permissions, and separates package release from deployment. Version the Python package, public Gateway wire protocol and adapter compatibility intentionally; none should silently change without corresponding tests and release notes.

### 10.3 Document and decision workflow

- **Requirements** state *what* and acceptance criteria.
- **Architecture** states *how* and dependency rules.
- **ADRs** state significant decisions, alternatives and consequences; never record private agent chain-of-thought or every code edit.
- **Roadmap** states milestones and observed status, not a second issue tracker.
- **Changelog** records user-visible changes under `Unreleased`; do not announce unimplemented work as released.
- **AGENTS.md** instructs coding agents to preserve these boundaries, run evidence-producing tests and request review for material scope changes.

Architecture changes affecting module boundaries, security ownership, realtime media path, public protocols, storage or deployment should update this document, requirements/tests and a numbered ADR in one reviewable change when feasible.

## 11. Accepted V1 ADRs and decision boundaries

The initial three V1 architecture decision records under [`decisions/`](decisions/) have been reviewed and accepted (`2026-11-01`) as the V1 design baseline (not as proof of runtime implementation):

| Accepted ADR | Status | Subject | Main decision |
| --- | --- | --- | --- |
| [`0001-modular-monolith.md`](decisions/0001-modular-monolith.md) | `Accepted` | Packaging and deployment | One modular library + runnable app; no premature microservices |
| [`0002-hexagonal-architecture.md`](decisions/0002-hexagonal-architecture.md) | `Accepted` | Dependency direction and replaceable ports | Core owns interfaces; integrations implement them |
| [`0003-library-and-server.md`](decisions/0003-library-and-server.md) | `Accepted` | Reuse + ASGI hosting | Importable package, explicit app factory, optional CLI/server |

Future ADRs only when justified: audio bypasses general event bus; verified identity versus provider metadata; deployment model changes; persistent stores; additional providers. An ADR records accepted rationale and its alternatives, not an agent's unfiltered internal deliberations.

## 12. Open architecture questions and decisions needed before pilot

| ID | Unresolved contract/decision | Consequence |
| --- | --- | --- |
| `OPEN-001` | Does Verimor initiate WSS to us, and what are its handshake and auth rules? | Ingress transport/auth adapter cannot be finalized. |
| `OPEN-002` | What are media/control framing, codec, sample rate, sequencing, playback cancel/ack, and terminal events? | Real audio codec/interrupt interoperability cannot be certified. |
| `OPEN-003` | Which OpenAI Live endpoints, event payloads and supported codecs apply to the selected account/version? | Live AI adapter requires versioned sandbox verification. |
| `OPEN-004` | Who authorizes calls/tools/recordings, and what privacy/data-transfer arrangements apply? | Real customer call processing must not begin. |
| `OPEN-005` | Actual infrastructure, AI quota, target peak calls and operating budget? | Capacity and SLO remain uncommitted. |
| `OPEN-006` | Is transfer/handoff supported and how is failure signaled? | Do not advertise human handoff without provider proof. |
| `OPEN-007` | Actual package extras, CLI commands and public endpoint names? | Finalize through implementation + compatibility tests. |

**Exit criteria for architecture review:** ownership boundaries are accepted, each trust boundary has an owner, no unverified provider wire format has entered Core, V1 non-goals remain excluded, and every P0 acceptance scenario can be mapped to a component and test. Record approval and relevant ADRs before expanding implementation scope.

---

**Architecture summary:** Keep the Core small, keep high-frequency media transient and call-scoped, reuse Kaiwen Agent rather than duplicating it, and ship a testable single-process library/server before considering distributed deployment. A functioning simulator is not evidence of real Verimor or OpenAI live-media interoperability.
