# ADR-0008: Provider-Neutral Semantic and Tool-Result Side Channel for Realtime AI Ports

- **Status:** Proposed (implemented on `feat/realtime-semantic-tool-bridge`; maintainer acceptance pending)
- **Date:** 2026-11-23
- **Decision owners:** Kaiwen Gateway maintainers
- **Scope:** `kaiwen_gateway.modules.voice.semantic`, `kaiwen_gateway.adapters.openai_realtime`, `kaiwen_gateway.adapters.openai_gpt_live`
- **Related decisions:** [ADR-0002 — Hexagonal Architecture](0002-hexagonal-architecture.md) · [ADR-0003 — Library and Server](0003-library-and-server.md)
- **Related documents:** [Voice Media Bridge](../VOICE_MEDIA_BRIDGE.md) · [OpenAI Realtime Adapter](../guides/OPENAI_REALTIME_ADAPTER.md) · [OpenAI GPT-Live Adapter](../guides/OPENAI_GPT_LIVE_ADAPTER.md) · [Public API](../PUBLIC_API_AND_COMPATIBILITY.md)

## 1. Context

`v0.1.0b2` adapters stream audio only. Consuming applications (first: the Relay
desktop assistant) need the model's function-call proposals, transcripts and
response boundaries, and must return tool results to the same upstream session so
the model can continue speaking. Without a public Gateway channel the only options
are forking adapter internals or opening a second provider connection — both
violate AGENTS.md §3.2 (no private-internal shortcuts, single upstream session).

## 2. Decision

1. Add a **provider-neutral semantic contract** in `kaiwen_gateway.modules.voice.semantic`:
   frozen event dataclasses (`ToolCallRequested`, `ToolCallCancelled`, `TranscriptEvent`,
   `ResponseLifecycleEvent`, `ProviderErrorEvent`, `SessionLifecycleEvent`),
   `ToolDeclaration`, `ToolResult`, `SemanticCapabilities` and the optional
   `SemanticRealtimePort` protocol (`semantic_capabilities`, `receive_semantic_event()`,
   `submit_tool_result()`). No provider event names appear in this module.
2. **Media and control planes stay separate.** Adapters publish semantic events into a
   `BoundedSemanticEventChannel` with a synchronous, non-blocking `publish`. Non-final
   transcript deltas are droppable (counted); if the channel fills with non-droppable
   events it terminates with `SemanticBackpressureError` instead of blocking the shared
   provider receive loop. Audio delivery is never awaited on semantic consumers.
3. **Gateway carries proposals; it never authorizes.** `ToolCallRequested` is untrusted
   model output. Argument parsing is offered as a helper (`parse_tool_arguments`) but
   validation, policy, approval and execution stay in the consuming application
   (for Relay: Kaiwen Agent `ToolExecutor`). Gateway contains no tool registry or loop.
4. **At-most-once, turn-bound results.** A per-adapter `ToolCallTracker` deduplicates the
   same `call_id` reported by several provider events, rejects replays of settled calls,
   bounds pending calls (32), and cancels pending calls when their turn is fenced by
   `cancel_turn()` (`ToolCallCancelled(reason="turn_interrupted")`) or when the session
   closes/fails (`reason="session_closed"`). `submit_tool_result()` for a cancelled call
   raises `StaleToolCallError` and sends nothing upstream; a second submission raises
   `DuplicateToolResultError`.
5. **Identifier separation.** `turn_id` is the Gateway turn generation (trusted, local);
   `call_id` is the provider's opaque identifier, validated for length/control
   characters and never used as application identity.
6. **Opt-in and capability-honest.** `enable_semantic_events=False` by default; legacy
   configurations produce byte-identical `session.update` / `session.start` events and
   `semantic_capabilities` reports all `False`. Each adapter declares only verified
   capabilities.

## 3. Provider mapping (verified against official documentation, 2026-11-23)

| Concern | OpenAI Realtime (`/v1/realtime`) | OpenAI GPT-Live (`/v1/live/sessions`) |
|---|---|---|
| Tool declaration | `session.tools[]` (`type:"function"`, `name`, `description`, `parameters`) + `tool_choice` | Not supported by this adapter (client delegation documents no function-call contract) |
| Tool call | `response.function_call_arguments.done`, `response.output_item.done` / `response.done` `function_call` items (deduped by `call_id`) | — |
| Tool result | `conversation.item.create` `{type:"function_call_output", call_id, output}` then `response.create` | `UnsupportedSemanticCapabilityError` |
| Transcripts | `conversation.item.input_audio_transcription.delta/.completed` (requires `session.audio.input.transcription.model`), `response.output_audio_transcript.delta/.done` | `session.input_transcript.delta`, `session.output_transcript.delta` (no final marker documented) |
| Lifecycle | `session.updated` → ready; `response.created`/`response.done` status → started/completed/cancelled/failed/incomplete | `session.started` → ready; `session.closed` (`reason`) → closed |

GPT-Live Responses delegation (`session.delegation.type="responses"`, wrapped
`response.event` envelopes, `response.item.create`) is documented but not implemented;
it is a separate decision because it changes where the reasoning model runs.

## 4. Consequences

- Positive: applications can run a complete voice → tool proposal → approval → result →
  spoken continuation loop over one Gateway-managed upstream session.
- Positive: no new runtime dependency; core remains provider-free.
- Negative: tool results share the provider WebSocket with audio sends (serialized by the
  transport); a stuck send is bounded by `cancel_timeout_seconds`.
- Risk: semantic event mapping is mock-verified only. Live provider verification of tool
  calling, transcripts and continuation is **NOT_VERIFIED** until an authorized,
  cost-bounded sandbox run.

## 5. Compatibility

Additive only. All existing public symbols, defaults and wire behavior are unchanged
(`tests/test_public_api_compatibility.py`, full legacy suite). The semantic contract is
versioned independently as `SEMANTIC_CONTRACT_VERSION = "1.0"`.
