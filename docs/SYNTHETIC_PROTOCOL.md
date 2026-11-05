# Synthetic Telephony Protocol Specification (v1)

> **Status:** Reference specification for local simulation and synthetic ingress (M3.2A/M3.2B)<br>
> **Package:** `kaiwen-gateway` / `kaiwen_gateway.transports.synthetic`<br>
> **Endpoint:** `/ws/synthetic/v1`<br>
> **Protocol Version:** `v1`<br>
> **Important:** This is a **fictional, project-owned synthetic test protocol**. It is **not** real Verimor, has **no** relationship to any proprietary carrier or telephony API, and must **never** be represented as live telephony interoperability.

---

## 1. Scope and Architectural Boundary

The Synthetic Telephony Protocol establishes a controlled, deterministic WebSocket ingress path for testing Kaiwen Gateway's core lifecycle, connection supervision, admission control, and call state management without requiring real telephony infrastructure or live carrier credentials.

In accordance with [`docs/ARCHITECTURE.md`](ARCHITECTURE.md) and [`AGENTS.md`](../AGENTS.md):
- **Control plane only:** This protocol currently manages connection handshakes, session readiness, ping/pong liveness, and call lifecycle (`call.start`, `call.end`). Audio media streaming is deferred to Milestone 4.
- **Provider-neutral port binding:** Inbound synthetic events are normalized into domain-level `TelephonyControlEvent` instances handled by `SyntheticTelephonyAdapter`, implementing `TelephonyControlPort`.
- **Zero Core runtime dependencies:** All protocol parsing, schema validation, and token cryptography remain strictly inside `kaiwen_gateway.transports.synthetic` and the optional `server` extra.

---

## 2. Transport & Framing

- **Underlying transport:** WebSocket over TLS/TCP (`wss://` in production, `ws://` in local loopback tests).
- **Endpoint path:** `/ws/synthetic/v1`
- **Frame type:** **Text frames only** (`Opcode 1`), containing UTF-8 encoded JSON.
- **Binary frames:** Strictly forbidden; receiving a binary frame terminates the connection immediately with WebSocket close code `1003` (`Unsupported Data`).
- **Maximum frame size:** 64 KiB (`65,536 bytes`). Frames exceeding this limit are rejected immediately with WebSocket close code `1009` (`Message Too Big`).
- **Strict JSON conformance (RFC 8259):** Non-standard JSON constants (`NaN`, `Infinity`, `-Infinity`) are strictly forbidden and rejected with close code `1007` (`Invalid Frame Payload`).
- **Close reason size limit (RFC 6455 §5.5):** All WebSocket close reason strings emitted by the gateway are strictly bounded to at most 123 UTF-8 bytes to guarantee compliance with the 125-byte control frame payload limit. Client-controlled event names are never reflected into close reasons.

---

## 3. Authentication & Handshake

Ingress authentication occurs during the HTTP WebSocket upgrade handshake via standard HTTP authorization headers:

```text
GET /ws/synthetic/v1 HTTP/1.1
Host: localhost:8000
Upgrade: websocket
Connection: Upgrade
Authorization: Bearer <hmac_token>
```

> [!CAUTION]
> **Token Transport Security:** Bearer tokens must **never** be transmitted via query-string URL parameters, as URLs are routinely logged by intermediate proxies, access logs, and browser histories. The gateway strictly requires the `Authorization: Bearer <token>` header.

### 3.1 Token Verification
The token is verified by `SyntheticTokenVerifier` using HMAC-SHA256 (`<payload_b64>.<signature_b64>` format):
- **Cryptographic verification:** HMAC-SHA256 signature verified in constant time using `hmac.compare_digest`.
- **Required claims:**
  - `pvd` (str): Provider identifier (e.g. `"synthetic"`).
  - `tnt` (str): Tenant identifier.
  - `cll` (str): Call identifier.
  - `scp` (list[str]): Allowed permission scopes (must contain `"voice.stream"`).
  - `iat` (float): Unix epoch timestamp of token issuance (wall-clock time).
  - `exp` (float): Unix epoch timestamp of token expiration (wall-clock time).
- **Optional claims:**
  - `tid` (str): Unique token identifier used for optional single-use replay protection cache.
  - `ref` (str): Optional provider call reference.
- **Temporal validation rules:**
  - Evaluated against wall-clock Unix epoch time (`time.time()`).
  - Bounded clock skew tolerance: `MAX_CLOCK_SKEW_SECONDS = 30.0` (both `iat <= now + 30.0` and `now <= exp + 30.0`).
  - Lifetime check: `exp >= iat` and maximum lifetime `exp - iat <= 3600.0` (1 hour).
  - Replay cache: If enabled, unique `tid` is recorded upon admission to prevent replay.
  - Note: `nbf` (not-before) is not defined or required in the synthetic v1 token schema.

### 3.2 Pre-Admission Handshake Policy
1. If the `Authorization` header is missing, malformed, or carries an expired/forged token or lacks required scopes, the handshake is rejected with WebSocket close code `1008` (`Policy Violation`).
2. If the token is valid, the Gateway checks `AdmissionController`:
   - If capacity is exhausted or the instance is draining, the server rejects the connection with close code `1008` (`Admission rejected`) **before** the WebSocket handshake is accepted.
3. If admitted, the server registers `(tenant_id, call_id)` exclusivity:
   - A concurrent inbound connection with an active duplicate `(tenant_id, call_id)` is rejected with close code `1008` (`Call session already active`).

---

## 4. Message Schemas & Event Types

Every frame is a JSON object with a required `"event"` field.

### 4.1 Server-to-Client Events

#### `session.ready`
Emitted by the server immediately upon accepting the connection:
```json
{
  "event": "session.ready",
  "protocol_version": "v1",
  "connection_id": "conn_syn_a1b2c3d4e5f67890",
  "tenant_id": "tenant_prod",
  "call_id": "call_12345",
  "allowed_scopes": [
    "voice.stream"
  ]
}
```

#### `session.pong`
Emitted by the server in response to `session.ping`:
```json
{
  "event": "session.pong",
  "seq": 42
}
```
*(The `seq` field echoes the client's `seq` if provided).*

#### `call.started`
Emitted by the server in response to a valid `call.start` request:
```json
{
  "event": "call.started",
  "call_id": "call_12345",
  "state": "connected",
  "stream_id": "str_abc"
}
```

#### `call.ended`
Emitted by the server in response to a `call.end` request:
```json
{
  "event": "call.ended",
  "call_id": "call_12345",
  "state": "disconnected",
  "reason": "caller_hangup",
  "code": 16
}
```
*(If `code` was supplied in `call.end`, it is echoed in `call.ended`. For duplicate `call.end` frames, `"idempotent": true` is included).*

---

### 4.2 Client-to-Server Events

#### `session.ping`
Keep-alive probe sent by the client.
```json
{
  "event": "session.ping",
  "seq": 42
}
```
- `seq` (optional): Non-negative integer (`0` to `2,147,483,647`). Boolean values are rejected.

#### `session.end`
Graceful session teardown requested by client.
```json
{
  "event": "session.end"
}
```
If a call is active when `session.end` is received, the adapter deterministically transitions the call state to `DISCONNECTED` with reason `"session_ended"`. The server acknowledges by closing the WebSocket connection with code `1000` (`Normal Closure`).

#### `call.start`
Initiates the telephony call leg.
```json
{
  "event": "call.start",
  "stream_id": "str_abc",
  "metadata": {
    "carrier_zone": "us-east-1"
  }
}
```
- `stream_id` (optional): String (1 to 128 characters).
- `metadata` (optional): JSON object (maximum 32 keys, key names <= 64 characters, values primitive scalar string/int/float/bool, strings <= 256 chars, no NaN/Infinity).
- **Identity spoofing protection:** Any attempt by the client to supply `tenant_id`, `provider_id`, `call_id`, or `allowed_scopes` conflicting with the verified token context causes immediate termination with close code `1008`.

#### `call.end`
Terminates the telephony call leg.
```json
{
  "event": "call.end",
  "reason": "caller_hangup",
  "code": 16
}
```
- `reason` (optional): String (1 to 128 characters). Default: `"caller_hangup"`.
- `code` (optional): Integer (`0` to `65535`). Boolean values are rejected.
- **Idempotency:** A duplicate `call.end` when the call is already in `DISCONNECTED` state is handled idempotently and returns `call.ended` with `"idempotent": true` without error, preserving the initial terminal reason and emitting no duplicate domain event.

---

### 4.3 Unknown Event & Field Validation Policy
- **Event Vocabulary:** Inbound client messages must specify one of the four supported client events: `session.ping`, `session.end`, `call.start`, `call.end`. Any other event name immediately triggers a WebSocket close with code `1008` and fixed reason `"Unsupported event type"` (never reflecting client-controlled event names).
- **Identity Claim Integrity:** In `call.start` and `call.end`, wire payloads attempting to supply mismatched `tenant_id`, `provider_id`, `call_id`, or `allowed_scopes` are rejected fail-closed with code `1008`.
- **Unknown Fields:** Unrecognized extra payload keys in recognized events are ignored for forward compatibility with future v1 extensions, provided they do not conflict with verified identity claims.
- **Metadata Immutability:** `TelephonyCallStartedEvent.metadata` is exposed as an immutable `Mapping[str, Any]` (wrapped in `types.MappingProxyType`). The synthetic framing validator strictly enforces primitive scalar types (`str`, `int`, `float`, `bool`) and string lengths <= 256 characters with no NaN or Infinite values. Because only immutable primitive scalars are admitted, the entire mapping is deeply immutable.

---

## 5. Call State Machine & Transport Teardown

The synthetic telephony call leg follows a strict, unidirectional state machine:

```text
                  ┌──────────────┐
                  │     IDLE     │
                  └──────┬───────┘
                         │
             call.start  │
                         ▼
                  ┌──────────────┐
       ┌─────────►│  CONNECTED   │
       │          └──────┬───────┘
       │                 │
ping / │     call.end    │ transport_close /
pong   │                 │ session.end
       │                 ▼
       │          ┌──────────────┐
       └──────────┤ DISCONNECTED │ (Terminal; idempotent call.end)
                  └──────────────┘
```

### Transition & Teardown Invariants
1. `IDLE -> CONNECTED`: Valid only on `call.start`. Emits `TelephonyCallStartedEvent`.
2. `CONNECTED -> DISCONNECTED`: Valid on `call.end` (wire event), client `session.end`, socket disconnect (`WebSocketDisconnect`), timeout, or server cancellation. The first transition emits `TelephonyCallEndedEvent`.
3. `call.start` while `CONNECTED` or `DISCONNECTED`: Illegal state transition; closes connection with code `1008`.
4. `call.end` while `IDLE`: Illegal state transition; closes connection with code `1008`.
5. `call.end` while `DISCONNECTED`: Idempotent; responds with `call.ended` acknowledging existing disconnection; emits `None` as the domain event to guarantee downstream terminal idempotency.
6. **Separation of Semantics:** A WebSocket transport close does not imply telephony call completion, but an active telephony call leg bound to a closed transport is deterministically cleaned up and marked `DISCONNECTED` with reason `"transport_closed"` or `"session_ended"`.

---

## 6. Capability Negotiation & Unsupported Operations

The synthetic adapter declares its capabilities deterministically:

| Capability | Status | Behavior when requested |
|---|---|---|
| `call_start` | **Supported** | Transitions `IDLE -> CONNECTED`, acknowledges with `call.started` |
| `call_end` | **Supported** | Transitions `CONNECTED -> DISCONNECTED`, acknowledges with `call.ended` |
| `ping_pong` | **Supported** | Responds with `session.pong` |
| `call_transfer` | **Unsupported** | Fail-closed: raises `UnsupportedOperationError`, closes socket with `1008` |
| `dtmf` | **Unsupported** | Fail-closed: raises `UnsupportedOperationError`, closes socket with `1008` |
| `media_streaming` | **Unsupported** *(M3.2)* | Out of scope until Milestone 4 |

---

## 7. WebSocket Close Codes & Error Mapping

| Close Code | Name | Maximum Reason Length | Condition |
|---|---|---|---|
| `1000` | Normal Closure | 123 bytes | Graceful client exit via `session.end` or server-initiated graceful drain. |
| `1003` | Unsupported Data | 123 bytes | Client transmitted a binary WebSocket frame. |
| `1007` | Invalid Frame Payload | 123 bytes | Malformed JSON, non-UTF-8 payload, non-standard constants (`NaN`/`Infinity`), or schema validation failure. |
| `1008` | Policy Violation | 123 bytes | Auth failure, expired token, identity spoofing, duplicate active call, illegal state transition, or unsupported event/capability. |
| `1009` | Message Too Big | 123 bytes | Received frame exceeding 64 KiB (65,536 bytes). |
| `1011` | Internal Error | 123 bytes | Unexpected server exception or connection worker startup failure. |
