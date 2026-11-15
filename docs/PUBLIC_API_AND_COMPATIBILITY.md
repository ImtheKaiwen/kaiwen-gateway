# Kaiwen Gateway — Public API Surface and Compatibility Policy

> **Status:** Active Alpha Reference (`MAINTENANCE — INTEGRATION PENDING`)
> **Applies to:** `kaiwen-gateway` (`v0.1.0a1` published alpha + unreleased `main` post-alpha additions through PR #49)

This document defines the supported public API surface, versioning model, compatibility expectations, internal boundaries, and provider adapter ownership rules for Kaiwen Gateway.

---

## 1. Alpha-Stage Compatibility Expectations

Kaiwen Gateway is currently in **Alpha (`Development Status :: 3 - Alpha`)**:

- **No permanent API stability promise during alpha:** Public interfaces may evolve as real provider integrations and operational requirements are validated.
- **Controlled evolution:** Breaking changes to supported public symbols are made deliberately, documented in `CHANGELOG.md` (and ADRs where architectural), and never introduced silently.
- **Explicit surface over implicit exports:** Only symbols listed in the documented package `__all__` exports below are part of the supported public API.

---

## 2. Three Distinct Version Dimensions

Kaiwen Gateway separates three independent version identifiers so package releases, module contracts, and wire protocols do not conflate:

| Dimension | Identifier | Current Value | Scope |
|---|---|---|---|
| **Python Package Version** | `kaiwen_gateway.__version__` | `0.1.0a1` | Distribution releases on PyPI / GitHub Releases (intends SemVer post-1.0). |
| **Core Module Contract Version** | `kaiwen_gateway.core.CORE_CONTRACT_VERSION` | `"1.0"` | Compatibility of `GatewayModule` / `ModuleDescriptor` contracts enforced by `ModuleRegistry`. |
| **Synthetic WebSocket Protocol Versions** | `SYNTHETIC_PROTOCOL_VERSION` / `SYNTHETIC_MEDIA_PROTOCOL_VERSION` | `"v1"` | Wire framing for `/ws/synthetic/v1` (control) and `/ws/synthetic-media/v1` (binary media). |

---

## 3. Supported Public Surface

### 3.1 Top-Level Package (`kaiwen_gateway`)
- `kaiwen_gateway.__version__`

### 3.2 Framework-Independent Core (`kaiwen_gateway.core`)
Zero mandatory third-party dependencies (standard library only):
- **Composition & Registry:** `GatewayCore`, `compose_gateway_core`, `ModuleRegistry`
- **Module Contracts & Lifecycle Orchestration:** `GatewayModule`, `ModuleLifecycle`, `ModuleDescriptor`, `CORE_CONTRACT_VERSION`, `SUPPORTED_CONTRACT_VERSIONS`, `ModuleLifecycleManager`, `ModuleLifecyclePolicy`, `ModuleLifecyclePhase`, `ModuleFailure`, `ModuleLifecycleError`, `ModuleStartupError`, `ModuleShutdownError`, `InvalidModuleLifecycleStateError`
- **Configuration:** `CoreConfig`, `validate_core_config`, `INGRESS_MODE_DISABLED`, `INGRESS_MODE_SYNTHETIC`, `SUPPORTED_INGRESS_MODES`
- **Identity & Security Ports:** `ProviderAuthenticatorPort`, `CallAuthorizationPort`, `ProviderPrincipal`, `TenantCallBinding`, `VerifiedConnectionContext`, `UntrustedConnectionClaims`, `bind_verified_connection_context`, `resolve_verified_connection_context`
- **Admission, Connection Lifecycle & Draining:** `AdmissionController`, `AdmissionLease`, `AdmissionSnapshot`, `ConnectionSupervisor`, `ConnectionState`, `ConnectionOutcome`, `LifecyclePolicy`, `TerminalReason`, `InstanceDrainCoordinator`, `InstanceDrainState`, `DrainPolicy`, `DrainReport`
- **Typed Exceptions:** `CoreError`, `CoreConfigurationError`, `ModuleRegistrationError`, `InvalidModuleDescriptorError`, `InvalidModuleIdError`, `UnsupportedContractVersionError`, `DuplicateModuleError`, `ModuleNotRegisteredError`, `IdentityError`, `InvalidIdentityError`, `InvalidScopeError`, `IdentityAuthorizationError`, `AdmissionError`, `AdmissionCapacityExceededError`, `GlobalCapacityExceededError`, `TenantCapacityExceededError`, `DuplicateConnectionAdmissionError`, `InvalidLeaseError`, `ConnectionLifecycleError`, `InvalidConnectionTransitionError`, `LifecycleTimeoutError`, `DrainError`, `InstanceDrainingError`, `InvalidDrainTransitionError`

### 3.3 Voice Domain & Media Ports (`kaiwen_gateway.modules.voice`)
Provider-neutral domain and streaming contracts (zero external provider dependencies):
- **Ports:** `AIRealtimeMediaPort`, `TelephonyMediaPort`, `InterruptibleAIPort`, `InterruptibleTelephonyPort`, `InterruptionCapabilities`, `InterruptionOutcome`
- **Media Domain:** `AudioFrame`, `AudioFormat`, `create_synthetic_pcm16le_frame`, `DEFAULT_MAX_PAYLOAD_SIZE`
- **Voice Bridge & Telemetry:** `VoiceBridgeSession`, `VoiceBridgeState`, `VoiceTelemetrySink`, `InMemoryVoiceTelemetrySink`, `VoiceSessionMetricsSnapshot`, `VoiceAggregateMetricsSnapshot`, `VoiceErrorCategory`, `VoiceTerminalCleanupState`, `MetricAvailabilityReason`, `LatencyDistributionSummary`, `BoundedLatencyAggregator`, `classify_voice_error`
- **Voice Exceptions:** `VoiceDomainError`, `InvalidAudioFrameError`, `OversizedAudioFrameError`, `OutOfOrderAudioFrameError`, `MediaBackpressureOverflowError`, `MediaPortError`, `MediaPortClosedError`, `VoiceBridgeSessionClosedError`, `UnsupportedInterruptionCapabilityError`, `FutureTurnAudioFrameError`
- **Synthetic Test Doubles:** `FakeTelephonyMediaAdapter`, `FakeAIMediaAdapter`, `InterruptibleFakeTelephonyAdapter`, `BoundedFakeAIEchoPort`, `BoundedInterruptibleFakeAIEchoPort`, `InterruptibleFakeAIPort`

### 3.4 Optional ASGI Server & CLI (`kaiwen_gateway.server`, requires `kaiwen-gateway[server]`)
- **Application Factory & Lifespan:** `create_gateway_app`, `default_app`, `gateway_lifespan`
- **CLI Entry Points:** `kaiwen-gateway` console script (`kaiwen_gateway.server.__main__:main`), `main`, `build_parser`, `resolve_config`, `run_check_config`, `run_demo`, `run_serve`
- **Server Exceptions:** `ServerError`, `ServerDependencyError`, `ServerLifecycleError`

### 3.5 Adapter Conformance Testing (`kaiwen_gateway.testing`)
- `RealtimeAdapterConformanceSuite`, `SyntheticUpstreamFailingAIAdapter`, `create_test_frame`

---

## 4. Explicit Internal / Private APIs

The following are **internal** and not covered by compatibility guarantees:
- Any symbol, module, attribute, or method whose name begins with `_` (including private helpers such as `_resolve_factory_binding`, `_instantiate_ai_port`, `_BaseFakeMediaAdapter`, etc.).
- Internal Starlette route/middleware wiring inside `kaiwen_gateway.server.app` beyond `create_gateway_app` and `default_app`.
- Internal `transports.synthetic` helper functions and wire-parser internals outside documented endpoint paths and protocol constants.

---

## 5. Breaking-Change and Deprecation Policy

- **During Alpha (`0.x`):**
  - Breaking changes may occur in minor or alpha iterations when required for correctness, security, or real provider integration.
  - Any breaking change must be recorded in `CHANGELOG.md` with clear migration guidance.
- **Post-1.0 Target Policy:**
  - Follow Semantic Versioning (`MAJOR.MINOR.PATCH`).
  - Deprecate public symbols for at least one minor release with `DeprecationWarning` before removal in a major release.

---

## 6. Module Lifecycle & Provider Adapter Ownership Boundaries

1. **Embedded vs. Hosted Module Lifecycle:**
   - `GatewayCore` only validates and registers `GatewayModule` instances; construction is synchronous with zero side effects.
   - The **hosting application** owns runtime `startup()` / `shutdown()` orchestration:
     - When hosted via `create_gateway_app()`, `gateway_lifespan` orchestrates `ModuleLifecycleManager` automatically (starts in registration order before readiness, drains active connections first on shutdown, then stops in reverse order).
     - `InstanceDrainState.STOPPED` indicates that the bounded drain coordinator has concluded, **not** that every connection exited cleanly; hosts must inspect `DrainReport` (`unresolved_connections == 0` and `active_leases_remaining == 0`). When unresolved connections or active admission leases remain after draining, `gateway_lifespan` skips `modules.shutdown()` (avoiding tearing down module resources underneath still-active workers), sets `app.state.shutdown_failed = True` (`app.state.stopped = False`), and raises `ServerLifecycleError`.
     - When embedding `GatewayCore` without ASGI, the host application drives `ModuleLifecycleManager(core)` directly (plain `asyncio`, no web-framework dependency) and should apply the same `DrainReport` check before shutting down modules.
2. **Provider Adapter Ownership:**
   - External provider integrations (e.g. OpenAI Realtime, Kaiwen Agent, Kaiwen Speech, Verimor) belong in dedicated adapter packages or explicit composition modules, **never** inside `kaiwen_gateway.core` or `kaiwen_gateway.modules.voice`.
   - `ai_media_port_factory` executes **synchronously** per call session and is invoked **exactly once** with verified `call_id`, `tenant_id`, and `context`. Any external provider requiring asynchronous handshake/connection setup must manage that setup lazily and safely inside its own adapter-owned lifecycle (for example on first `send_frame`/`receive_frame` or an internal background task cleaned up in `close()`).
   - Provider credentials and client secrets are strictly server-owned and injected at composition/factory time, never supplied by WebSocket/telephony clients.
