# Kaiwen Gateway — V1 Requirements

> **Status:** Proposed baseline for review — not an implementation report
> **Document version:** 1.0-draft
> **Initial target:** `0.1.0a1` (alpha)
> **Last updated:** 2026-10-31
> **Scope:** Reusable Python library + deployable ASGI service; Voice is the only implemented communication module in V1.

## 1. Purpose and document rules

This document defines **what Kaiwen Gateway V1 must do**, **how acceptance will be verified**, and **what will not be built**. It is the scope and acceptance baseline for developers and coding agents. The implementation architecture belongs in [`ARCHITECTURE.md`](ARCHITECTURE.md); architectural trade-offs belong in [`decisions/`](decisions/); task sequencing belongs in [`../ROADMAP.md`](../ROADMAP.md).

The project is currently in the **design/pre-implementation phase**. Requirements marked *Required* are commitments for the intended V1, **not claims of existing functionality**. Do not write an implementation status, feature badge, quickstart, capacity claim, or release announcement that suggests these requirements are already fulfilled.

### 1.1 Requirement vocabulary

| Term | Meaning |
| --- | --- |
| **MUST** | Required for the stated release gate. |
| **SHOULD** | Expected unless an explicit, documented exception is accepted. |
| **MAY** | Optional and not a reason to delay V1. |
| **P0** | Essential for an alpha implementation or its basic security. |
| **P1** | Required before a limited live-company pilot, or as explicitly noted. |
| **P2** | Deferred improvement; not a V1 acceptance requirement. |
| **Mock-verified** | Passed using synthetic/provider-independent fixtures; not real interoperability. |
| **Provider-verified** | Tested against an authorized sandbox/real integration. |
| **Production-ready** | Explicit operational, privacy, security, reliability and load gates have been met; never implied by alpha status. |

IDs such as `FR-VOICE-003` are stable. Do not silently reuse an ID for a different requirement. If scope changes, update this document and the corresponding roadmap, tests, and ADR where appropriate.

## 2. Product definition and boundaries

### 2.1 Product goal

Kaiwen Gateway is a **provider-agnostic communication gateway** that can be imported as a Python library or deployed as an ASGI service. It authenticates external connections, binds trusted identity, manages transport lifecycles, and delegates protocol-specific behavior to opt-in modules. The initial **Voice** module connects a telephony provider's bidirectional live audio stream to a real-time AI backend through existing Kaiwen Agent capabilities.

The first **intended** telephony integration is **Verimor**; the first AI backend is **OpenAI Live**. Neither their exact wire interoperability nor the Verimor audio protocol has been verified as part of this document.

### 2.2 System context

```text
Caller
   ↕
Telephony provider (initial integration target: Verimor)
   ↕  bidirectional WSS media, if confirmed by its technical contract
Kaiwen Gateway (WSS/HTTP server, authentication, connection lifecycle)
   ├── Core (identity, admission, registry, observability)
   └── Voice module (audio bridge, media/control events, interruption)
           ↕
     Kaiwen Agent library (realtime session, context, tools and policies)
           ↕
     Real-time AI provider (initially OpenAI Live)
```

**Protocol distinction:** SIP/RTP and WSS media are not interchangeable. If the official provider contract requires SIP or another media path, a separately scoped media adapter/bridge or a revised integration plan will be necessary. Do not pretend a generic WSS endpoint accepts SIP media.

### 2.3 Ownership and dependency rules

| Owner | Owns | Must not duplicate |
| --- | --- | --- |
| Gateway Core | Transport admission, trusted connection identity, registration, transport lifecycle, shared diagnostics | AI reasoning, general task scheduling, audio codec business logic |
| Voice module | Call/media lifecycle, format adapters, audio pumps, queues, interruption and media-specific policies | Generic Gateway Core contracts |
| Transport adapters | WSS/HTTP framing, protocol-specific handshake and I/O | Global tenant policy or agent execution |
| Kaiwen Agent | Agent execution, context, tool policy/approvals, logical realtime session management | Gateway's public WSS server or Verimor-specific wire protocol |
| Consuming application | Customer/tenant provisioning, business tools, caller authorization, lawful processing decisions, retention policies | Gateway transport and reusable protocol primitives |

`kaiwen-gateway` MAY depend on `kaiwen-agent`. The reverse dependency is **forbidden**. Do not copy the Agent's runtime, scheduler, authorization pipeline or realtime session manager into Gateway. Prefer public, versioned Agent interfaces; propose upstream changes only when a general-purpose contract is truly missing.

### 2.4 V1 scope

**In scope:** A typed Python package; small explicit module registry; ASGI app factory and CLI; authenticated bidirectional WSS; a Voice module; a mock telephony provider; OpenAI Live bridge integration; per-call lifecycle/identity; bounded audio buffering, cancellation and interruptions; operational signals; security boundaries; simulator, automated tests, packaging and controlled release infrastructure.

**Conditionally in scope:** A functioning **Verimor adapter** only after its authoritative handshake, authentication, frame schema, codecs, playback/cancel semantics and lifecycle events are available. An invented protocol is not an acceptable substitute.

**Out of scope for V1:** General messaging/events/AI/MCP gateways; visual admin dashboard; SaaS billing; distributed microservice orchestration; Kafka/Kubernetes; automatic call migration across worker processes; local Kaiwen Voice model inference/training; call-center CRM implementation; mass outbound calling; a plugin marketplace; autonomous tool execution beyond the existing authorized Kaiwen Agent interfaces.

Future modules may be documented as **extension possibilities**, but their code, dependencies and speculative configuration MUST NOT be added merely to make the repository look extensive.

## 3. Functional requirements

### 3.1 Library, composition and configuration

| ID | Priority | Requirement | Verification |
| --- | --- | --- | --- |
| `FR-CORE-001` | P0 | Provide a `kaiwen_gateway` importable Python package with an explicitly documented public surface. | Clean-environment wheel installation and import test. |
| `FR-CORE-002` | P0 | Provide an app factory that composes a runnable ASGI server without requiring a global singleton. | Two independent app instances in a unit test. |
| `FR-CORE-003` | P0 | Define minimal typed contracts for module registration, startup/shutdown, connection context and transport dispatch. | Fake module passes contract tests. |
| `FR-CORE-004` | P0 | Register modules explicitly and reject duplicate IDs, unsupported versions and invalid configurations. | Deterministic negative tests. |
| `FR-CORE-005` | P0 | Keep optional voice/OpenAI/server dependencies out of the minimal library install where practical. | Dependency inspection and minimal import test. |
| `FR-CORE-006` | P0 | Load configuration through typed, validated settings. Missing security-critical settings MUST fail closed. | Configuration matrix and invalid-input tests. |
| `FR-CORE-007` | P0 | Provide a CLI entry point to start the app and validate configuration; actual command names must be documented only after implementation. | Built wheel CLI smoke test. |
| `FR-CORE-008` | P1 | Expose health and readiness status separately: a process may be alive while refusing new calls. | Health/readiness state tests, including draining. |

**Non-goal:** The module contract is not a generic event bus. It must not force raw audio frames through JSON events or persistent queues.

### 3.2 Connection handling and identity

| ID | Priority | Requirement | Verification |
| --- | --- | --- | --- |
| `FR-CONN-001` | P0 | Accept authenticated WSS connections through a documented, versioned endpoint. | Authorized/unauthorized handshake tests. |
| `FR-CONN-002` | P0 | Bind immutable, **server-verified** `provider_id`, `tenant_id` and unique `call_id` / `connection_id` to each connection. | Forged identity and cross-tenant tests. |
| `FR-CONN-003` | P0 | Reject unauthenticated connections before allocating an upstream AI session. | Provider-create spy remains uncalled. |
| `FR-CONN-004` | P0 | Track explicit connection states (`connecting`, `active`, `draining`, `closing`, `closed` or equivalents) with guarded transitions. | State-machine tests. |
| `FR-CONN-005` | P0 | Enforce admission controls and configurable global/per-tenant concurrency ceilings. | Concurrent create/reject tests. |
| `FR-CONN-006` | P0 | Handle normal disconnect, invalid frames, transport loss, timeout and task cancellation without orphaning upstream sessions or tasks. | Fault-injection and leak tests. |
| `FR-CONN-007` | P1 | Define heartbeat, idle timeout, maximum session lifetime and server-side close reason mapping. | Simulated timeouts and lifecycle tests. |
| `FR-CONN-008` | P1 | Reject duplicate/replayed call-start events using an atomic store appropriate to the deployment model. | Concurrent duplicate-event test. |
| `FR-CONN-009` | P1 | Enter *draining* mode on deployment shutdown; reject new calls while allowing existing calls to finish within policy. | Deployment lifecycle test. |

Caller-supplied phone numbers, SIP headers and WebSocket payload fields are **untrusted metadata**, not identity or authorization authority.

### 3.3 Voice transport and audio bridge

| ID | Priority | Requirement | Verification |
| --- | --- | --- | --- |
| `FR-VOICE-001` | P0 | Keep each call's inbound and outbound audio streams independent, with one logical session context per call. | Parallel multi-call isolation test. |
| `FR-VOICE-002` | P0 | Implement two concurrently running, lifecycle-bound pumps: telephony→AI and AI→telephony. | Bidirectional mock audio fixture test. |
| `FR-VOICE-003` | P0 | Validate frame type, size, format, ordering assumptions and timing metadata before passing frames into the bridge. | Malformed/oversized frame tests. |
| `FR-VOICE-004` | P0 | Support a narrowly defined initial media format set based on confirmed provider and AI capabilities; avoid unnecessary transcoding. | Codec fixture and round-trip tests. |
| `FR-VOICE-005` | P0 | Use bounded media buffers and explicit policies for overload, slow upstream/downstream and resource exhaustion. | Slow-reader and backpressure tests. |
| `FR-VOICE-006` | P0 | Implement interruption/barge-in handling, including stopping or clearing audio that has not been played when the wire protocol supports this. | Interrupted playback timeline test. |
| `FR-VOICE-007` | P0 | Ensure no frames from call A are emitted on call B, including during concurrent connect, reconnect and disconnect. | Isolation tests with distinct audio markers. |
| `FR-VOICE-008` | P0 | Clean up the upstream connection, audio tasks and buffers after call termination or failure. | Forced-error cleanup test. |
| `FR-VOICE-009` | P1 | Apply a documented, controllable fallback on upstream error; human handoff is required only when the provider and deployment actually support transfer. | Failure-matrix tests; no false transfer claims. |
| `FR-VOICE-010` | P1 | Define safe reconnect behavior; never assume that lost audio or a provider session can be resumed transparently. | Reconnect/failure scenario test. |

Media buffers should be ephemeral. Do not write individual audio frames to a database, durable event stream or default application logs.

### 3.4 AI and telephony integrations

| ID | Priority | Requirement | Verification |
| --- | --- | --- | --- |
| `FR-INT-001` | P0 | Provide an internal `TelephonyMediaAdapter`-style port independent of Verimor wire formats. | Mock provider conformance suite. |
| `FR-INT-002` | P0 | Provide an `AIRealtimeAdapter`-style port, reusing Kaiwen Agent's public realtime capabilities where suitable. | Fake AI provider conformance suite. |
| `FR-INT-003` | P0 | Support server-owned credentials and provider configuration; never expose API keys to telephony clients. | Secret redaction and boundary tests. |
| `FR-INT-004` | P0 | Map validated call identity to Kaiwen Agent's trusted tenant/user/conversation context; no provider-supplied identity may elevate permissions. | Context mapping tests. |
| `FR-INT-005` | P0 | Validate current OpenAI Live handshake, events and audio contract against current official specifications before claiming live interoperability. | Version-pinned sandbox smoke test; result recorded. |
| `FR-INT-006` | P0 | Supply a **mock Verimor-compatible test provider** with an explicitly fictional, project-owned protocol until genuine specs arrive. | Deterministic simulator tests. |
| `FR-INT-007` | P1 | Implement real Verimor protocol mapping **only from provided authoritative specifications**; preserve sample messages as sanitized test fixtures when permitted. | Authorized provider sandbox/end-to-end test. |
| `FR-INT-008` | P1 | Detect/handle upstream rate limits, provider rejection, retries and disconnects without reusing one call's AI state for another. | Fake failure and quota tests. |
| `FR-INT-009` | P1 | Document exactly which call actions (e.g. accept, hang up, transfer, DTMF) are available; unsupported actions must fail explicitly. | Capability/negative tests. |

**Open question:** Verimor is reported to intend bidirectional live audio over WSS, but the actual **handshake, connection initiator, authentication mechanism, control-event schema, packet format, codec, sample rate and playback acknowledgment/cancellation behavior remain unverified**. The implementation must not lock those assumptions into the Gateway Core.

### 3.5 Operations and developer experience

| ID | Priority | Requirement | Verification |
| --- | --- | --- | --- |
| `FR-OPS-001` | P0 | Provide reproducible local development commands and a synthetic single-call demo. | CI smoke test in clean checkout. |
| `FR-OPS-002` | P0 | Provide a load/fault simulator without real customer speech or paid AI calls. | Simulator deterministic suite. |
| `FR-OPS-003` | P0 | Emit structured lifecycle/error logs with consistent correlation fields and redaction rules. | Log schema and secret-leak tests. |
| `FR-OPS-004` | P0 | Expose metrics hooks for active connections, rejects, disconnects, media queue time and bridge latency. | In-memory sink and assertions. |
| `FR-OPS-005` | P1 | Support trace propagation/correlation through Gateway → Agent → AI integration where safe and supported. | End-to-end trace correlation test. |
| `FR-OPS-006` | P1 | Provide a container image, environment configuration template, readiness checks and deployment runbook. | Container smoke test. |
| `FR-OPS-007` | P1 | Provide actionable alerts/runbook guidance for saturation, high error rates, excessive latency and unavailable upstream providers. | Document review and alert-rule validation. |
| `FR-OPS-008` | P1 | Publish versioned API/wire contract documentation and a compatibility policy. | Generated/spec validation and docs review. |

## 4. Security, privacy and compliance requirements

| ID | Priority | Requirement | Verification |
| --- | --- | --- | --- |
| `SEC-001` | P0 | External communication uses WSS/TLS in non-local deployment; no plaintext public media endpoint. | Deployment configuration review and TLS smoke test. |
| `SEC-002` | P0 | Authenticate provider connections with a verified scheme actually supported by the provider (e.g. mTLS or cryptographically validated short-lived tokens). | Negative-auth and replay tests. |
| `SEC-003` | P0 | Authorization is server-side and scoped to tenant, call and tool; untrusted media/control frames cannot change that scope. | Cross-tenant and forged-claim tests. |
| `SEC-004` | P0 | Validate inbound frame size, message schema, content type, connection rate, idle time, aggregate resource budgets and concurrent admissions. | Abuse/fuzz/fault tests. |
| `SEC-005` | P0 | Store credentials only in server-controlled configuration/secret providers and redact them from logs, traces, errors and fixtures. | Static checks and seeded-secret tests. |
| `SEC-006` | P0 | Do not persist raw audio or full transcripts by default; define explicit opt-in recording/retention interfaces owned by applications. | No-storage-on-default-path test. |
| `SEC-007` | P0 | Delegate tool permissions, approvals, idempotency and audit to trusted Kaiwen Agent/application policy components; never treat model output as authorization. | Tool-denial and policy tests. |
| `SEC-008` | P0 | Restrict outbound connections to approved AI endpoints and protect against arbitrary user-supplied upstream URLs. | Egress validation/SSRF negative tests. |
| `SEC-009` | P1 | Use atomic replay protection for provider call-start webhooks/events and document key rotation/revocation. | Duplicate-event/race tests and runbook. |
| `SEC-010` | P1 | Produce a deployment-specific privacy/data-flow assessment before processing real customer calls; address notice, legal basis, retention, access, processors and international transfers as applicable. | Written approval; not replaceable by code tests. |
| `SEC-011` | P1 | Establish private vulnerability reporting and dependency/workflow scanning before public release. | SECURITY.md and CI scan evidence. |

**Release blocker:** No real company or customer calls may be routed through an unverified integration or without the consuming organization's authorized security/privacy review. The Gateway's technical security controls do not independently establish compliance with KVKK or any other law.

## 5. Performance and reliability requirements

Requirements must distinguish **Gateway-added latency** from network delay and AI model inference delay. No configuration value proves throughput or capacity.

| ID | Priority | Requirement / initial target | Verification |
| --- | --- | --- | --- |
| `NFR-PERF-001` | P0 | No blocking network, file or expensive CPU work on the event loop's media hot path. | Code review, event-loop-lag instrumented test. |
| `NFR-PERF-002` | P0 | Audio streaming must use bounded per-call queues and bounded maximum frame sizes. | Slow-consumer and memory-limit tests. |
| `NFR-PERF-003` | P0 | Record p50/p95/p99 latency for Gateway receive→upstream send and upstream receive→Gateway send separately. | Synthetic timed frame test. |
| `NFR-PERF-004` | P1 | **Design goal:** p95 Gateway-added transport processing overhead ≤ 50 ms under the documented reference workload, excluding external network/model delays. This is not an established guarantee. | Repeatable benchmark with hardware, codec, duration, load and instrumentation documented. |
| `NFR-PERF-005` | P0 | Run staged synthetic load tests at 1, 5 and 25 concurrent calls; record pass/fail and resource use. | CI/manual load report. |
| `NFR-PERF-006` | P1 | Attempt 50 and 100 concurrent calls only after the earlier stages are stable; publish observed capacity, not promised capacity. | Reference-environment load report with p95/p99, CPU, memory, errors. |
| `NFR-REL-001` | P0 | All connection tasks and upstream resources must be released after success, error, cancellation and timeout. | Soak and fault-injection tests. |
| `NFR-REL-002` | P0 | Reject or degrade safely when capacity is exhausted rather than allowing unbounded allocation. | Saturation tests. |
| `NFR-REL-003` | P1 | Support bounded graceful draining for deploy/restart, with documented behavior for calls exceeding the drain deadline. | Controlled shutdown test. |
| `NFR-REL-004` | P1 | Define policy and visible outcome for AI provider outage, call-transfer failure and interrupted media. | Outage/fallback scenario suite. |
| `NFR-REL-005` | P1 | Set an explicit pilot error budget/SLO only **after** sandbox baseline and operational ownership are established. | Signed pilot acceptance record. |

A test passes only when the workload and environment are identified. Do not claim “supports 100 calls” from an in-memory default limit of 100 or 500; capacity is an empirical result.

## 6. Engineering, testing and delivery standards

### 6.1 Source quality

| ID | Priority | Requirement |
| --- | --- | --- |
| `ENG-001` | P0 | Python 3.11+, `pyproject.toml`, locked development environment, Hatchling build and clean package installation. |
| `ENG-002` | P0 | Ruff formatter/linter and strict type checking on package code; explicit public API annotations. |
| `ENG-003` | P0 | pytest unit, async integration and provider contract tests; deterministic fakes for external I/O. |
| `ENG-004` | P0 | Test cancellation, race conditions, duplicate events, malformed frames and cross-tenant isolation, not just happy paths. |
| `ENG-005` | P0 | Require a failing regression test for a reproduced defect when practical. |
| `ENG-006` | P0 | Keep transport/framework/provider dependencies behind adapters; prohibit circular imports and prohibited Core→Voice/provider imports. |
| `ENG-007` | P1 | Set and enforce a meaningful coverage floor after baseline; critical security and lifecycle branches require explicit tests regardless of percentage. |
| `ENG-008` | P1 | Use performance-regression checks on stable reference runners or report-only checks until reproducibility is established. |

### 6.2 GitHub Actions and quality gates

| ID | Priority | Requirement |
| --- | --- | --- |
| `CI-001` | P0 | Every PR runs format, lint, types, unit tests, mock integration tests and package smoke tests. |
| `CI-002` | P0 | The protected main branch requires the agreed checks; avoid publishing from an ordinary feature push. |
| `CI-003` | P0 | Pin and review CI dependencies/actions; grant workflows least-privilege permissions, with isolated publishing authority. |
| `CI-004` | P1 | Add dependency, secret, static-analysis and GitHub Actions security scanning. |
| `CI-005` | P1 | Run provider-independent stress/fault tests on a separate scheduled or manual workflow; avoid expensive load tests on every edit. |
| `CI-006` | P1 | Release from a reviewed, versioned artifact with build verification, release notes, provenance where available and manual/approved publish controls. |
| `CI-007` | P1 | Publish a container image and wheel through documented, separately authorized release/deploy workflows. |

### 6.3 Observability and diagnostic policy

| ID | Priority | Requirement |
| --- | --- | --- |
| `OBS-001` | P0 | Use structured logs (`event`, `level`, timestamp, trace/correlation ID, safe tenant/session identifiers, error class). |
| `OBS-002` | P0 | Record aggregate frame throughput, queue duration, latency, active connections, admission denials and provider failures without logging media content. |
| `OBS-003` | P0 | Ensure metrics label cardinality is bounded; avoid telephone numbers, raw IDs and transcripts as metric labels. |
| `OBS-004` | P1 | Expose optional OpenTelemetry-compatible trace and metrics exporters; no mandatory Grafana/Prometheus/Tempo/Loki runtime dependency for package import. |
| `OBS-005` | P1 | Provide documented operator actions for high error rate, upstream outage, draining and exhausted capacity. |

### 6.4 Documentation and project history

Maintain these documents as **separate sources of truth**:

| Artifact | Responsibility |
| --- | --- |
| `README.md` | Public project overview and *verified* getting started instructions. |
| `docs/REQUIREMENTS.md` | Requirements and acceptance criteria (this file). |
| `docs/ARCHITECTURE.md` | Component boundaries, ports, state/data flows and deployment topology. |
| `AGENTS.md` | Coding-agent instructions, prohibited actions and verification steps. |
| `ROADMAP.md` | Milestones and project status, not a second issue tracker. |
| `CHANGELOG.md` | User-visible changes under `Unreleased` and released versions. |
| `SECURITY.md` | Supported versions and private vulnerability disclosure channel. |
| `CONTRIBUTING.md` | Local development, PR, review, style and test workflow. |
| `docs/decisions/NNNN-*.md` | Important accepted/superseded architectural and technical decisions, alternatives and consequences. |

An ADR records a **concise, reviewable decision rationale**, not a coding agent's hidden reasoning or every intermediate command. Architectural changes require an ADR when they affect public contracts, module boundaries, security ownership, persistence, audio path or deployment model. User approval is required for material V1 scope expansions.

## 7. Acceptance scenarios and release gates

### 7.1 Required acceptance scenarios

| Scenario ID | Situation | Pass condition | Related requirements |
| --- | --- | --- | --- |
| `AT-001` | Install built wheel in clean Python environment. | Import, app factory and CLI work without developer checkout assumptions. | FR-CORE-001/002/007, ENG-001 |
| `AT-002` | Open valid authenticated call. | One independent, correctly scoped AI session; synthetic audio flows both directions. | FR-CONN-001/002, FR-VOICE-001/002 |
| `AT-003` | Try invalid, expired, forged or replayed credentials. | Rejected before any upstream allocation. | FR-CONN-003, SEC-002/003 |
| `AT-004` | Run simultaneous calls for different tenants. | No audio, context, secrets or tool authority cross tenant boundaries. | FR-VOICE-007, SEC-003/007 |
| `AT-005` | Send malformed/oversized frames and flood connections. | Controlled rejection/limits; no unbounded memory or crash. | FR-VOICE-003/005, SEC-004 |
| `AT-006` | Interrupt AI playback mid-response. | Queued/unplayed outbound audio handled according to negotiated provider semantics. | FR-VOICE-006 |
| `AT-007` | Disconnect, timeout or cancel during active media. | Both pumps finish and all resources are released. | FR-CONN-006, FR-VOICE-008, NFR-REL-001 |
| `AT-008` | Simulate upstream outage or rate limit. | Deterministic fallback/close, safe resource cleanup and meaningful telemetry. | FR-INT-008, FR-VOICE-009, NFR-REL-004 |
| `AT-009` | Repeated provider call-start event. | At most one accepted logical call; duplicate has defined outcome. | FR-CONN-008, SEC-009 |
| `AT-010` | Run synthetic concurrency stages. | Benchmark artifacts include load/environment, p95/p99 latency, errors, CPU and memory. | NFR-PERF-003/005/006 |
| `AT-011` | Inspect logs/traces/metrics and repository fixtures. | No raw audio, transcripts, API secrets or identifiable phone numbers leaked. | SEC-005/006, OBS-001/003 |
| `AT-012` | Drain a running instance during active calls. | New calls rejected; existing calls finish or end under documented policy. | FR-CONN-009, NFR-REL-003 |
| `AT-013` | Validate real Verimor WSS integration. | Authorized real/sandbox media session satisfies official protocol, measured and recorded. | FR-INT-007; **blocked pending official contract** |

### 7.2 Gate A — Documentation / implementation authorization

- [ ] README, requirements, architecture, roadmap and initial ADRs agree on ownership and V1 boundaries.
- [ ] Known unknowns are tracked; unknown Verimor protocol fields have not been fabricated.
- [ ] Agent work is broken into separately reviewable increments and requires explicit scope approval for major changes.

### 7.3 Gate B — Alpha (`0.1.0a1`, synthetic environment)

- [ ] P0 requirements are implemented or an approved, explicit exception is recorded.
- [ ] `AT-001` through `AT-008`, `AT-010` and `AT-011` pass in synthetic tests; where `AT-009` is deferred to pilot, duplicate starts must at least fail safely locally.
- [ ] CI quality gates pass; built wheel/container demo uses synthetic data only.
- [ ] README labels the package **alpha** and clearly states that live Verimor compatibility and production readiness are **not verified**.
- [ ] No real customer data, calls or confidential provider documents are used in tests.

### 7.4 Gate C — Limited live-company pilot

- [ ] Complete the real provider contract and verify `AT-013` against its official version.
- [ ] Implement P1 security/operational requirements relevant to the chosen deployment, including atomic replay protection, draining and provider failure behavior.
- [ ] Obtain documented tenant/customer authorization and privacy/data-transfer review.
- [ ] Complete sandbox smoke, security, concurrency and failover tests; document observed limits and open risks.
- [ ] Confirm operational contacts, incident runbooks, secret rotation, rollback and approved retention policy.

### 7.5 Gate D — Production readiness (separate decision)

A successful pilot **does not automatically grant production status**. Production requires a separately reviewed SLO, capacity plan, incident response process, verified provider terms, monitoring coverage, deployment/recovery testing, secrets/data governance, and operational ownership. These must be recorded in release-readiness evidence.

## 8. Dependencies, assumptions and unresolved questions

| ID | Open item | Required action | Blocks |
| --- | --- | --- | --- |
| `OPEN-001` | Verimor WSS handshake, connection initiator and authentication | Obtain authoritative technical documentation / controlled sample. | Real provider adapter and live pilot. |
| `OPEN-002` | Verimor media/control frames, codecs, sample rate, timestamps, cancellation, end-of-call | Obtain versioned message examples and capabilities. | Real end-to-end voice interoperability. |
| `OPEN-003` | Applicable OpenAI Live endpoint, event and codec contract | Check official specification and exercise a current sandbox connection. | Live OpenAI interoperability claim. |
| `OPEN-004` | Customer/tenant authorization and call content privacy rules | Agree responsibilities with the consuming company and qualified privacy/legal stakeholders. | Real customer data processing. |
| `OPEN-005` | Deployment location, permitted egress, provider quotas, expected call concurrency | Document infrastructure and budget constraints before pilot. | Capacity/SLO commitments. |
| `OPEN-006` | Call transfer/human handoff support | Confirm provider capabilities and decide an explicit failure message/close fallback. | Human-transfer feature claim. |
| `OPEN-007` | Exact public API names, package extras and CLI commands | Decide during implementation and freeze with tests. | Publication of reliable Quickstart. |

Do not resolve any `OPEN-*` item by an unsupported assumption. For each closure, record evidence or a link to the reviewed issue/PR/ADR.

## 9. Change management

1. Changes to a P0 requirement, public protocol, identity model, core/module ownership or scope exclusion require an explicit review and, where architectural, an ADR.
2. Update requirement IDs, acceptance tests, documentation and roadmap in the same PR where practical.
3. Add meaningful user-visible functionality and breaking changes to `CHANGELOG.md` under `Unreleased`; released versions are immutable historical records.
4. Track implementation tasks and bugs in GitHub Issues/PRs. Do not turn this document into a daily activity log.
5. Never claim a feature is working, safe, compliant or production-ready merely because the requirement is written here.

---

**Baseline summary:** Deliver a **small, secure, provider-neutral library and runnable server** with one working **Voice** module; verify it with synthetic bidirectional streaming before attempting a real Verimor integration. Maintain explicit separation between reusable Gateway transport concerns, Kaiwen Agent intelligence, and the consuming application's legal/business authority.
