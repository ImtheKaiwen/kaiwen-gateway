# ADR-0004: Synthetic Binary WebSocket Media Protocol & Vertical Slice

- Status: Proposed
- Date: 2026-11-07
- Decision owners: Maintainer review pending
- Related requirements: FR-VOICE-01, FR-VOICE-02, SEC-01, SEC-02, NFR-RES-01, NFR-LAT-01
- Supersedes: None

## Context and problem

Milestone M4.1 established the provider-neutral in-process `VoiceBridgeSession` and typed `TelephonyMediaPort` / `AIRealtimeMediaPort` contracts operating on immutable `AudioFrame` domain envelopes. However, M4.1 was purely in-process; raw audio was not transported over an actual network transport.

Milestones M3.1–M3.3 provided the authenticated synthetic control WebSocket endpoint at `/ws/synthetic/v1`. By design and contract, `/ws/synthetic/v1` is strictly text-only (JSON control events) and explicitly rejects all binary frames with WebSocket close code `1003` (`WS_1003_UNSUPPORTED_DATA`).

For Milestone M4.2A, the Gateway requires an end-to-end vertical slice connecting a real local WebSocket transport to `VoiceBridgeSession` carrying real binary audio frames over loopback. We must define:
1. An isolated, test-only synthetic media wire contract that transports binary audio without modifying or breaking the existing text-only `/ws/synthetic/v1` contract.
2. A single-read-loop demultiplexing architecture for WebSocket connections that handle both text control events (`call.start`, `call.end`, `session.ping`) and raw binary audio frames without concurrent `websocket.receive()` collisions.
3. Strict serialization of outbound WebSocket transmissions across asynchronous control and media writers.
4. Bounded per-call Fake AI audio processing with deterministic behavior and backpressure, avoiding unbounded memory accumulation.

## Decision

1. **Dedicated Experimental Endpoint (`/ws/synthetic-media/v1`):**
   - We introduce a dedicated, versioned, test-only endpoint at `/ws/synthetic-media/v1`.
   - Disabled by default. Only enabled when explicitly opted into via `enable_synthetic_media=True` in the application factory (`create_gateway_app`).
   - Strictly intended for local development and synthetic loopback testing; does not claim production WSS, Verimor compatibility, or public telephony protocol support.
   - Preserves `/ws/synthetic/v1` untouched as a text-only control protocol rejecting binary frames.

2. **Binary Media Wire Profile:**
   - Codec: Raw Linear PCM 16-bit little-endian (PCM16LE), mono (1 channel), 16,000 Hz sample rate.
   - Frame Duration: 20 milliseconds per audio frame.
   - Frame Size: Exactly 640 bytes per binary WebSocket message (16,000 samples/sec * 2 bytes/sample * 0.020 sec = 640 bytes).
   - Zero JSON or Base64 encoding: Audio payloads are transported directly as native WebSocket binary messages.
   - Binary frame size is strictly validated: frames not matching 640 bytes are rejected with close code 1008/1003.

3. **Control and Media Separation over a Single Ingress Read-Loop:**
   - A single ASGI WebSocket reader task executes `websocket.receive()`.
   - Text messages are parsed and validated as JSON control events (`call.start`, `call.end`, `session.ping`, `session.end`).
   - Binary messages are validated as PCM16LE audio frames and dispatched to the inbound queue of the `WebSocketTelephonyMediaAdapter`.
   - Binary audio received prior to `call.start` or after `call.end` is rejected immediately.
   - Outbound WebSocket writes (text control responses and binary media frames) are synchronized under an `asyncio.Lock` to guarantee frame integrity and avoid interleaved ASGI write corruption.

4. **Security, Admission, and Identity Invariants:**
   - Enforces full M3 authentication: Bearer token HMAC signature verification, nonce replay protection, timestamp expiration.
   - Requires verified permission scope `voice.stream`.
   - Enforces Core admission limits (global concurrency and per-tenant concurrency) via `InstanceDrainCoordinator` before allocating any media session or AI buffer.
   - Rejects unauthenticated or over-capacity connections prior to WebSocket upgrade acceptance.
   - Call exclusivity is registered and owned by the connection supervisor.

5. **Bounded Fake AI Echo Port:**
   - `BoundedFakeAIEchoPort` implements `AIRealtimeMediaPort` with bounded FIFO queues and backpressure.
   - Operates per-call with zero shared global state and zero unbounded list accumulation.
   - Produces deterministic echo responses matching inbound frame byte contents for verifiable round-trip assertions.

## Alternatives considered

- *Multiplexing binary media into existing `/ws/synthetic/v1`:* Rejected. Modifying `/ws/synthetic/v1` would break the M3 text-only contract and introduce regression risks to existing soak/load runners and clients.
- *Base64-encoding audio within JSON control frames:* Rejected. Violates ADR-0002 and Gateway voice invariants. High-frequency audio must not suffer JSON parsing and Base64 serialization overhead.
- *Two concurrent WebSocket reader tasks:* Rejected. ASGI specification explicitly forbids concurrent reader coroutines awaiting `websocket.receive()` on the same connection.
- *Unbounded media buffer for Fake AI:* Rejected. Unbounded queues violate Gateway memory bounds (NFR-RES-01).

## Consequences and trade-offs

- **Positive:**
  - Complete, verifiable end-to-end binary streaming over a real loopback WebSocket network connection.
  - Zero changes or regressions to existing text-only `/ws/synthetic/v1` transport.
  - Clean hexagonal boundary: `VoiceBridgeSession` remains completely decoupled from ASGI and WebSocket transport details.
  - Fail-closed security: unauthenticated or out-of-order attempts are rejected before allocating media resources.
- **Trade-offs / Limitations:**
  - Limited to fixed PCM16LE 16kHz mono (no runtime codec negotiation or transcoding in this milestone).
  - Test-only endpoint enabled solely via explicit opt-in; not exposed in production default configurations.
  - Loopback boundary: Loopback isolation is enforced at the network socket bind layer (e.g. binding Uvicorn exclusively to `127.0.0.1`). The application does not rely on untrusted client-supplied headers (such as `Host` or `X-Forwarded-For`) to infer loopback residency. When embedded in third-party ASGI runners, host operators are responsible for socket bind configuration.

## Validation / acceptance evidence

- Deterministic unit and integration tests in `tests/test_synthetic_media_endpoint.py`.
- Real loopback Uvicorn server and WebSocket client smoke test in `tools/synthetic_media_smoke.py`.
- Negative security test suite (tampered tokens, replay attacks, missing scopes, oversized/undersized frames, out-of-order controls).
- Verified capacity recovery: zero active calls, zero active leases, zero leaked background tasks after session completion.

## Reconsideration triggers

- Introduction of real telephony provider WSS adapters (e.g. Verimor) with provider-specific framing and codec requirements.
- Standardized multi-tenant audio streaming protocol specifications for production Gateway deployments.

