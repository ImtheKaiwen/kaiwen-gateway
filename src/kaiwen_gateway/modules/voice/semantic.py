"""Provider-neutral semantic (control-plane) side channel for realtime AI ports.

The media plane (``AIRealtimeMediaPort``) carries audio frames. Some realtime AI
providers additionally emit low-frequency *semantic* events: function/tool call
proposals, transcripts and response lifecycle boundaries. This module defines a
small, provider-neutral contract for carrying those events to a trusted
application layer, and for returning tool results to the provider.

Invariants:

- **No authority in Gateway.** A ``ToolCallRequested`` event is a model proposal,
  never authorization. Validation, policy, approval and execution belong to the
  consuming application (for example Kaiwen Agent's ``ToolExecutor``).
- **Media never waits for semantics.** Adapters publish into a
  ``BoundedSemanticEventChannel`` with a non-blocking ``publish``; a slow consumer
  can only lose droppable events (non-final transcript deltas) or terminate the
  semantic channel. It never stalls audio.
- **Trusted identifiers stay separate.** ``turn_id`` is the Gateway-assigned turn
  generation; ``call_id`` is the provider's opaque tool-call identifier. Neither is
  derived from caller speech, and neither replaces the application's own session
  identity.
- **Stale results are rejected.** A tool call from a turn that was interrupted or a
  session that closed is reported as ``ToolCallCancelled``; submitting a result for
  it raises ``StaleToolCallError`` and sends nothing upstream.
- **Opt-in.** Adapters expose this channel only when explicitly enabled, so legacy
  audio-only behavior is unchanged.
"""

from __future__ import annotations

import asyncio
import json
import re
from collections import OrderedDict, deque
from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Any, Final, Literal, Protocol, runtime_checkable

from kaiwen_gateway.modules.voice.domain import VoiceDomainError

__all__ = [
    "SEMANTIC_CONTRACT_VERSION",
    "DEFAULT_SEMANTIC_QUEUE_CAPACITY",
    "MAX_TOOL_ARGUMENTS_BYTES",
    "MAX_TOOL_RESULT_BYTES",
    "MAX_TRANSCRIPT_TEXT_CHARS",
    "SemanticCapabilities",
    "ToolDeclaration",
    "ToolResult",
    "ToolCallRequested",
    "ToolCallCancelled",
    "TranscriptEvent",
    "ResponseLifecycleEvent",
    "ProviderErrorEvent",
    "SessionLifecycleEvent",
    "SemanticEvent",
    "SemanticRealtimePort",
    "BoundedSemanticEventChannel",
    "ToolCallTracker",
    "SemanticChannelError",
    "UnsupportedSemanticCapabilityError",
    "SemanticBackpressureError",
    "UnknownToolCallError",
    "StaleToolCallError",
    "DuplicateToolResultError",
    "MalformedToolArgumentsError",
    "parse_tool_arguments",
]

SEMANTIC_CONTRACT_VERSION: Final[str] = "1.0"
DEFAULT_SEMANTIC_QUEUE_CAPACITY: Final[int] = 64
MAX_TOOL_ARGUMENTS_BYTES: Final[int] = 32_768
MAX_TOOL_RESULT_BYTES: Final[int] = 32_768
MAX_TRANSCRIPT_TEXT_CHARS: Final[int] = 4_000
MAX_PENDING_TOOL_CALLS: Final[int] = 32
_SETTLED_HISTORY: Final[int] = 256

# Function names accepted by the providers currently supported (letters, digits,
# underscore, hyphen). Kept strict so names never carry path or markup characters.
_TOOL_NAME_RE: Final[re.Pattern[str]] = re.compile(r"^[A-Za-z0-9_-]{1,64}$")
_CALL_ID_MAX: Final[int] = 128


# --------------------------------------------------------------------------- errors
class SemanticChannelError(VoiceDomainError):
    """Base error for the semantic side channel."""


class UnsupportedSemanticCapabilityError(SemanticChannelError):
    """Raised when a semantic operation is not supported or not enabled."""


class SemanticBackpressureError(SemanticChannelError):
    """Raised to the consumer when non-droppable semantic events overflowed."""


class UnknownToolCallError(SemanticChannelError):
    """Raised when a tool result references a call the port never emitted."""


class StaleToolCallError(SemanticChannelError):
    """Raised when a tool result targets a cancelled (interrupted/closed) call."""


class DuplicateToolResultError(SemanticChannelError):
    """Raised when a result is submitted twice for the same call."""


class MalformedToolArgumentsError(SemanticChannelError):
    """Raised by ``parse_tool_arguments`` for invalid or non-object JSON."""


# ------------------------------------------------------------------- declarations
@dataclass(frozen=True, slots=True)
class SemanticCapabilities:
    """Verified semantic capabilities of a realtime AI port.

    Every flag defaults to ``False``; adapters set only what they verified.
    """

    tool_calls: bool = False
    tool_result_continuation: bool = False
    tool_call_cancellation: bool = False
    input_transcript_delta: bool = False
    input_transcript_final: bool = False
    output_transcript_delta: bool = False
    output_transcript_final: bool = False
    response_lifecycle: bool = False
    session_lifecycle: bool = False

    def __post_init__(self) -> None:
        if self.tool_result_continuation and not self.tool_calls:
            raise SemanticChannelError("tool_result_continuation requires tool_calls")


@dataclass(frozen=True, slots=True)
class ToolDeclaration:
    """Provider-neutral function tool declaration (name + JSON Schema parameters)."""

    name: str
    description: str
    parameters: Mapping[str, Any] = field(
        default_factory=lambda: {"type": "object", "properties": {}}
    )

    def __post_init__(self) -> None:
        if not _TOOL_NAME_RE.fullmatch(self.name):
            raise ValueError("tool name must match ^[A-Za-z0-9_-]{1,64}$")
        if len(self.description) > 1024:
            raise ValueError("tool description must be <= 1024 characters")
        if not isinstance(self.parameters, Mapping) or self.parameters.get("type") != "object":
            raise ValueError("tool parameters must be a JSON Schema object")
        try:
            encoded = json.dumps(dict(self.parameters))
        except (TypeError, ValueError) as exc:
            raise ValueError("tool parameters must be JSON serializable") from exc
        if len(encoded.encode("utf-8")) > 16_384:
            raise ValueError("tool parameters schema is too large")


@dataclass(frozen=True, slots=True)
class ToolResult:
    """Result returned by the trusted application for one provider tool call."""

    call_id: str
    output: str

    def __post_init__(self) -> None:
        _validate_call_id(self.call_id)
        if not isinstance(self.output, str):
            raise ValueError("tool output must be a string")
        if len(self.output.encode("utf-8")) > MAX_TOOL_RESULT_BYTES:
            raise ValueError(f"tool output exceeds {MAX_TOOL_RESULT_BYTES} bytes")


def _validate_call_id(call_id: str) -> None:
    if not isinstance(call_id, str) or not call_id or len(call_id) > _CALL_ID_MAX:
        raise ValueError("call_id must be a non-empty string of at most 128 characters")
    if any(ord(ch) < 0x20 for ch in call_id):
        raise ValueError("call_id must not contain control characters")


# ------------------------------------------------------------------------ events
@dataclass(frozen=True, slots=True)
class ToolCallRequested:
    """A model proposed a function call. Not authorization."""

    sequence: int
    turn_id: int
    call_id: str
    name: str
    arguments_json: str
    kind: Literal["tool_call.requested"] = "tool_call.requested"


@dataclass(frozen=True, slots=True)
class ToolCallCancelled:
    """A previously requested call can no longer accept a result."""

    sequence: int
    turn_id: int
    call_id: str
    reason: Literal["turn_interrupted", "session_closed", "backend_failed"]
    kind: Literal["tool_call.cancelled"] = "tool_call.cancelled"


@dataclass(frozen=True, slots=True)
class TranscriptEvent:
    """Transcript text for the user (input) or the assistant (output)."""

    sequence: int
    turn_id: int
    role: Literal["user", "assistant"]
    text: str
    is_final: bool
    segment_id: str | None = None
    kind: Literal["transcript"] = "transcript"


@dataclass(frozen=True, slots=True)
class ResponseLifecycleEvent:
    """Assistant response boundary observed from the provider."""

    sequence: int
    turn_id: int
    phase: Literal["started", "completed", "cancelled", "failed", "incomplete"]
    response_ref: str | None = None
    kind: Literal["response.lifecycle"] = "response.lifecycle"


@dataclass(frozen=True, slots=True)
class ProviderErrorEvent:
    """Sanitized provider error (no credentials, no raw payloads)."""

    sequence: int
    turn_id: int
    code: str
    message: str
    fatal: bool
    kind: Literal["provider.error"] = "provider.error"


@dataclass(frozen=True, slots=True)
class SessionLifecycleEvent:
    """Upstream provider session readiness or termination."""

    sequence: int
    turn_id: int
    phase: Literal["ready", "closed"]
    reason: str | None = None
    kind: Literal["session.lifecycle"] = "session.lifecycle"


SemanticEvent = (
    ToolCallRequested
    | ToolCallCancelled
    | TranscriptEvent
    | ResponseLifecycleEvent
    | ProviderErrorEvent
    | SessionLifecycleEvent
)


def parse_tool_arguments(event: ToolCallRequested) -> dict[str, Any]:
    """Parse ``arguments_json`` into a JSON object or raise ``MalformedToolArgumentsError``."""
    try:
        value = json.loads(event.arguments_json) if event.arguments_json else {}
    except (TypeError, ValueError) as exc:
        raise MalformedToolArgumentsError("tool arguments are not valid JSON") from exc
    if not isinstance(value, dict):
        raise MalformedToolArgumentsError("tool arguments must be a JSON object")
    return value


# ------------------------------------------------------------------------ port
@runtime_checkable
class SemanticRealtimePort(Protocol):
    """Optional protocol for AI ports that expose the semantic side channel."""

    @property
    def semantic_capabilities(self) -> SemanticCapabilities:
        """Return verified, currently enabled semantic capabilities."""
        ...

    async def receive_semantic_event(self) -> SemanticEvent | None:
        """Return the next semantic event, or None once the channel has ended.

        Raises:
            UnsupportedSemanticCapabilityError: If the channel is not enabled.
            SemanticBackpressureError: If non-droppable events overflowed.
        """
        ...

    async def submit_tool_result(self, result: ToolResult) -> None:
        """Return a tool result to the provider and continue the response.

        Raises:
            UnsupportedSemanticCapabilityError: If tool results are not supported.
            UnknownToolCallError / StaleToolCallError / DuplicateToolResultError.
            MediaPortClosedError: If the port is closed.
        """
        ...


# --------------------------------------------------------------------- channel
class BoundedSemanticEventChannel:
    """Single-consumer bounded event channel with a non-blocking producer side.

    Overflow policy: non-final transcript deltas are droppable (counted). When the
    channel is full of non-droppable events the channel terminates with
    ``SemanticBackpressureError`` instead of blocking the producer.
    """

    def __init__(self, capacity: int = DEFAULT_SEMANTIC_QUEUE_CAPACITY) -> None:
        if capacity <= 0:
            raise ValueError("capacity must be > 0")
        self._capacity = capacity
        self._items: deque[SemanticEvent] = deque()
        self._available = asyncio.Event()
        self._closed = False
        self._error: SemanticChannelError | None = None
        self._dropped = 0
        self._sequence = 0

    @property
    def dropped_count(self) -> int:
        return self._dropped

    @property
    def is_closed(self) -> bool:
        return self._closed

    @property
    def depth(self) -> int:
        return len(self._items)

    def next_sequence(self) -> int:
        seq = self._sequence
        self._sequence += 1
        return seq

    @staticmethod
    def _droppable(event: SemanticEvent) -> bool:
        return isinstance(event, TranscriptEvent) and not event.is_final

    def publish(self, event: SemanticEvent) -> bool:
        """Enqueue without blocking. Returns False if the event was not enqueued."""
        if self._closed:
            return False
        if len(self._items) >= self._capacity:
            if self._droppable(event):
                self._dropped += 1
                return False
            for index, queued in enumerate(self._items):
                if self._droppable(queued):
                    del self._items[index]
                    self._dropped += 1
                    break
            else:
                self._error = SemanticBackpressureError(
                    "semantic event channel overflowed with non-droppable events"
                )
                self._closed = True
                self._available.set()
                return False
        self._items.append(event)
        self._available.set()
        return True

    def close(self) -> None:
        """End the channel; queued events remain readable, then ``None``."""
        self._closed = True
        self._available.set()

    async def receive(self) -> SemanticEvent | None:
        while True:
            if self._items:
                item = self._items.popleft()
                if not self._items and not self._closed:
                    self._available.clear()
                return item
            if self._error is not None:
                raise self._error
            if self._closed:
                return None
            self._available.clear()
            await self._available.wait()


# --------------------------------------------------------------------- tracker
class ToolCallTracker:
    """Tracks provider tool calls so results are applied at most once, to the
    turn they belong to, and never after interruption or close."""

    def __init__(self, max_pending: int = MAX_PENDING_TOOL_CALLS) -> None:
        self._max_pending = max_pending
        self._pending: dict[str, int] = {}
        self._settled: OrderedDict[str, Literal["submitted", "cancelled"]] = OrderedDict()

    @property
    def pending_count(self) -> int:
        return len(self._pending)

    def turn_of(self, call_id: str) -> int | None:
        return self._pending.get(call_id)

    def register(self, call_id: str, turn_id: int) -> bool:
        """Record a new call. Returns False for duplicates/replays or when full."""
        if call_id in self._pending or call_id in self._settled:
            return False
        if len(self._pending) >= self._max_pending:
            return False
        self._pending[call_id] = turn_id
        return True

    def _settle(self, call_id: str, state: Literal["submitted", "cancelled"]) -> None:
        self._pending.pop(call_id, None)
        self._settled[call_id] = state
        while len(self._settled) > _SETTLED_HISTORY:
            self._settled.popitem(last=False)

    def claim_for_result(self, call_id: str) -> int:
        """Mark a pending call as answered and return its turn id."""
        if call_id in self._pending:
            turn = self._pending[call_id]
            self._settle(call_id, "submitted")
            return turn
        state = self._settled.get(call_id)
        if state == "cancelled":
            raise StaleToolCallError("tool call was cancelled; result not delivered")
        if state == "submitted":
            raise DuplicateToolResultError("a result was already submitted for this call")
        raise UnknownToolCallError("unknown tool call id")

    def cancel_through_turn(self, turn_id: int) -> list[tuple[str, int]]:
        """Cancel every pending call whose turn is <= ``turn_id``."""
        victims = [(cid, turn) for cid, turn in self._pending.items() if turn <= turn_id]
        for cid, _ in victims:
            self._settle(cid, "cancelled")
        return victims

    def cancel(self, call_id: str) -> int | None:
        """Cancel one pending call; returns its turn id, or None if it is not pending."""
        turn = self._pending.get(call_id)
        if turn is not None:
            self._settle(call_id, "cancelled")
        return turn

    def cancel_all(self) -> list[tuple[str, int]]:
        victims = list(self._pending.items())
        for cid, _ in victims:
            self._settle(cid, "cancelled")
        return victims
