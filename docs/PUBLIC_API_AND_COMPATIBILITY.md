# Kaiwen Gateway — Public API Surface and Compatibility Policy

> **Status:** Active Beta Reference (**Gateway Core + OpenAI Voice Adapters Beta**)
> **Applies to:** `kaiwen-gateway` (`v0.1.0b1` published on PyPI & GitHub Releases)

This document defines the supported public API surface, versioning model, compatibility expectations, internal boundaries, and provider adapter ownership rules for Kaiwen Gateway.

---

## 1. Pre-1.0 Beta Compatibility Expectations

Kaiwen Gateway is currently in **Beta (`Development Status :: 4 - Beta`)** for the **Gateway Core + OpenAI Voice Adapters** scope:

- **No permanent API stability promise pre-1.0:** Public interfaces may evolve as deferred external integrations (Kaiwen Agent, Kaiwen Speech, Verimor) and operational requirements are validated.
- **Controlled evolution:** Breaking changes to supported public symbols are made deliberately, documented in `CHANGELOG.md` (and ADRs where architectural), and never introduced silently.
- **Explicit surface over implicit exports:** Only symbols listed in the documented package `__all__` exports below are part of the supported public API.

---

## 2. Three Distinct Version Dimensions

Kaiwen Gateway separates three independent version identifiers so package releases, module contracts, and wire protocols do not conflate:

| Dimension | Identifier | Current Value | Scope |
|---|---|---|---|
| **Python Package Version** | `kaiwen_gateway.__version__` | `0.1.0b1` | Distribution releases on PyPI / GitHub Releases (intends SemVer post-1.0). |
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
- **Configuration:** `CoreConfig`, `validate_core_config`, `INGRESS_MODE_DISABLED`, `INGRESS_MODE_SYNTHETIC`, `SUPPORTED_INGRESS_MODES`, `EVENTS_INGRESS_MODE_DISABLED`, `EVENTS_INGRESS_MODE_AUTHENTICATED`, `SUPPORTED_EVENTS_INGRESS_MODES`
- **Identity & Security Ports:** `ProviderAuthenticatorPort`, `CallAuthorizationPort`, `PrincipalAuthorizationPort`, `ProviderPrincipal`, `TenantCallBinding`, `TenantPrincipalBinding`, `VerifiedConnectionContext`, `VerifiedPrincipalContext`, `VerifiedConnectionIdentity`, `PrincipalId`, `UntrustedConnectionClaims`, `bind_verified_connection_context`, `bind_verified_principal_context`, `resolve_verified_connection_context`, `resolve_verified_principal_context`, `validate_principal_id`
- **Admission, Connection Lifecycle & Draining:** `AdmissionController`, `AdmissionLease`, `PrincipalAdmissionLease`, `AdmissionSnapshot`, `ConnectionSupervisor`, `PrincipalConnectionSupervisor`, `ConnectionState`, `ConnectionOutcome`, `LifecyclePolicy`, `TerminalReason`, `InstanceDrainCoordinator`, `InstanceDrainState`, `DrainPolicy`, `DrainReport`
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

### 3.6 Optional OpenAI Voice Adapters (`kaiwen_gateway.adapters`, requires `kaiwen-gateway[openai]` or `kaiwen-gateway[server]` for live WebSocket transport)
- **OpenAI Realtime (`kaiwen_gateway.adapters.openai_realtime`, `wss://api.openai.com/v1/realtime`):** `OpenAIRealtimeConfig`, `OpenAIRealtimeMediaAdapter`, `OpenAIRealtimeTransport`, `OpenAIRealtimeTransportConnector`, `OpenAIRealtimeError`, `OpenAIRealtimeDependencyError`, `create_openai_realtime_adapter_factory`, `connect_openai_realtime_websocket`, `DEFAULT_OPENAI_REALTIME_URL`, `DEFAULT_OPENAI_REALTIME_MODEL`, `DEFAULT_OPENAI_REALTIME_VOICE`, `OPENAI_PCM_SAMPLE_RATE`, `PCM16Resampler16kTo24k`, `PCM16Resampler24kTo16k`, `OutboundFramePacker`, `validate_inbound_frame`, `validate_endpoint_url`, `sanitize_provider_message`
- **OpenAI GPT-Live (`kaiwen_gateway.adapters.openai_gpt_live`, `wss://api.openai.com/v1/live/sessions`):** `OpenAIGPTLiveConfig`, `OpenAIGPTLiveMediaAdapter`, `OpenAIGPTLiveTransport`, `OpenAIGPTLiveTransportConnector`, `OpenAIGPTLiveError`, `OpenAIGPTLiveDependencyError`, `create_openai_gpt_live_adapter_factory`, `connect_openai_gpt_live_websocket`, `DEFAULT_OPENAI_GPT_LIVE_URL`, `DEFAULT_OPENAI_GPT_LIVE_MODEL`, `DEFAULT_OPENAI_GPT_LIVE_VOICE`, `GPT_LIVE_PCM_SAMPLE_RATE`

### 3.7 Events Domain & Foundation (`kaiwen_gateway.modules.events`)
Framework-independent Events foundation (zero mandatory third-party dependencies):
- **Domain & Validation:** `EVENTS_PROTOCOL_VERSION`, `EventEnvelope`, `PublishResult`, `DurableEventRecord`, `SlowConsumerPolicy`, `EventDeliveryOutcome`, `validate_event_id`, `validate_event_type`, `validate_topic`, `validate_topic_pattern`, `topic_matches_pattern`, `validate_correlation_id`, `validate_timestamp`
- **Ports:** `EventSubscription`, `EventConnectionConsumer`, `EventPublisherPort`, `EventSubscriptionPort`, `EventBrokerPort`, `EventBrokerStats`, `DurableEventStorePort`, `RateLimiterPort`, `TicketReplayCachePort`
- **Broker, Auth, Rate Limiter, Module & Telemetry:** `EventBrokerConfig`, `InMemoryEventBroker`, `InMemoryEventConsumer`, `InMemoryEventSubscription`, `EventsAuthenticatorPort`, `EventsAuthorizerPort`, `EventsTicketVerifierPort`, `EventsTicketClaims`, `BoundedTicketReplayCache`, `verify_and_consume_ticket`, `EVENTS_TICKET_AUDIENCE`, `DEFAULT_MAX_TICKET_TTL_SECONDS`, `DEFAULT_REPLAY_CACHE_MAX_ENTRIES`, `DEFAULT_PUBLISH_RATE_PER_SECOND`, `DEFAULT_PUBLISH_BURST`, `DEFAULT_SUBSCRIBE_RATE_PER_SECOND`, `DEFAULT_SUBSCRIBE_BURST`, `RateLimitConfig`, `RateLimitDecision`, `TokenBucketRateLimiter`, `EventsModule`, `EventsTelemetrySink`, `InMemoryEventsTelemetrySink`, `EventsMetricsSnapshot`, `EventsErrorCategory`
- **Events Exceptions:** `EventsError`, `InvalidEventError`, `OversizedEventError`, `UnsupportedEventVersionError`, `EventAuthorizationError`, `EventRateLimitError`, `EventBackpressureError`, `EventsBackendUnavailableError`, `EventsDependencyError`, `EventsModuleClosedError`

### 3.8 Authenticated WebSocket Events Transport (`kaiwen_gateway.transports.events`, requires `kaiwen-gateway[server]`)
- **Endpoint & Constants:** `EVENTS_WS_ENDPOINT_PATH` (`/ws/events/v1`), `EVENTS_WS_PROTOCOL_VERSION` (`"v1"`), `EVENTS_MAX_FRAME_BYTES` (`65_536`), `EVENTS_MAX_JSON_DEPTH` (`16`), `DEFAULT_EVENTS_IDLE_TIMEOUT_SECONDS` (`60.0`), `DEFAULT_EVENTS_SEND_TIMEOUT_SECONDS` (`5.0`), `DEFAULT_CONTROL_RATE_PER_SECOND` (`20.0`), `DEFAULT_CONTROL_BURST` (`40.0`), `MAX_REPEATED_RATE_LIMIT_VIOLATIONS` (`3`), `SUPPORTED_CLIENT_COMMANDS`, `CMD_EVENTS_SUBSCRIBE`, `CMD_EVENTS_UNSUBSCRIBE`, `CMD_SESSION_PING`, `CMD_EVENTS_PUBLISH`, `EVENT_SESSION_READY`, `EVENT_EVENTS_SUBSCRIBED`, `EVENT_EVENTS_UNSUBSCRIBED`, `EVENT_SESSION_PONG`, `EVENT_EVENTS_DELIVERY`, `EVENT_EVENTS_ERROR`
- **Handler, Parser & Frame Builders:** `events_websocket_endpoint`, `parse_and_validate_events_client_frame`, `ParsedEventsClientFrame`, `build_session_ready_frame`, `build_subscribed_frame`, `build_unsubscribed_frame`, `build_session_pong_frame`, `build_delivery_frame`, `build_error_frame`, `EventsProtocolError`, `InvalidEventsFrameError`, `OversizedEventsFrameError`, `EventsPolicyViolationError`
- **Wire Contract Summary (`/ws/events/v1`):** `session.ready` includes `allowed_scopes`; `events.subscribe` accepts `topic` (or `topic_pattern`) + optional `request_id`; `events.unsubscribe` accepts `topic` (or `subscription_id`) + optional `request_id`; `session.ping`/`session.pong` echo optional `seq` and `request_id`; `events.delivery` enforces `<= 65_536` bytes on the complete serialized wire frame; unsupported `protocol_version` and `events.publish` close `1008`; malformed JSON/UTF-8/schema close `1007`; oversized frames close `1009`; idle/normal closes `1000`.

### 3.9 Optional Redis Events Backend (`kaiwen_gateway.modules.events.redis`, requires `kaiwen-gateway[redis]`)
- **Config & Client Factory:** `RedisClientConfig`, `create_async_redis_client`, `DEFAULT_REDIS_CONNECT_TIMEOUT_SECONDS`, `DEFAULT_REDIS_SOCKET_TIMEOUT_SECONDS`, `DEFAULT_INITIAL_RECONNECT_BACKOFF_SECONDS`, `DEFAULT_MAX_RECONNECT_BACKOFF_SECONDS`, `KEY_PREFIX_PATTERN`
- **Cross-Instance Broker, Rate Limiter & Replay Cache:** `RedisPubSubBrokerConfig`, `RedisPubSubEventBroker`, `RedisEventConsumer`, `RedisEventSubscription`, `RedisDistributedRateLimiter`, `RedisTicketReplayCache`

---

## 4. Explicit Internal / Private APIs

The following are **internal** and not covered by compatibility guarantees:
- Any symbol, module, attribute, or method whose name begins with `_` (including private helpers such as `_resolve_factory_binding`, `_instantiate_ai_port`, `_BaseFakeMediaAdapter`, etc.).
- Internal Starlette route/middleware wiring inside `kaiwen_gateway.server.app` beyond `create_gateway_app` and `default_app`.
- Internal `transports.synthetic` helper functions and wire-parser internals outside documented endpoint paths and protocol constants.

---

## 5. Breaking-Change and Deprecation Policy

- **During Pre-1.0 (`0.x` Alpha / Beta):**
  - Breaking changes may occur in minor or pre-release iterations when required for correctness, security, or real provider integration.
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
