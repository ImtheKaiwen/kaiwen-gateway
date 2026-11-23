"""OpenAI GPT-Live media adapter implementing AIRealtimeMediaPort and InterruptibleAIPort."""

from __future__ import annotations

import asyncio
import base64
import binascii
import time
from collections.abc import Callable
from typing import Any

from kaiwen_gateway.adapters.openai_gpt_live.config import (
    OpenAIGPTLiveConfig,
    OpenAIGPTLiveError,
    sanitize_provider_message,
)
from kaiwen_gateway.adapters.openai_gpt_live.transport import (
    OpenAIGPTLiveTransport,
    OpenAIGPTLiveTransportConnector,
    connect_openai_gpt_live_websocket,
)
from kaiwen_gateway.adapters.openai_realtime.audio import (
    GATEWAY_FRAME_DURATION_MS,
    OutboundFramePacker,
    PCM16Resampler16kTo24k,
    PCM16Resampler24kTo16k,
    validate_inbound_frame,
)
from kaiwen_gateway.modules.voice.domain import (
    AudioFrame,
    MediaBackpressureOverflowError,
    MediaPortClosedError,
    MediaPortError,
)
from kaiwen_gateway.modules.voice.ports import InterruptionCapabilities
from kaiwen_gateway.modules.voice.semantic import (
    MAX_TRANSCRIPT_TEXT_CHARS,
    BoundedSemanticEventChannel,
    ProviderErrorEvent,
    SemanticCapabilities,
    SemanticEvent,
    SessionLifecycleEvent,
    ToolResult,
    TranscriptEvent,
    UnsupportedSemanticCapabilityError,
)


class _EOFSentinel:
    pass


class _ErrorSentinel:
    def __init__(self, error: Exception) -> None:
        self.error = error


_EOF = _EOFSentinel()


def _drain_queue_nowait(queue: asyncio.Queue[Any]) -> None:
    while not queue.empty():
        try:
            queue.get_nowait()
        except asyncio.QueueEmpty:
            break


class OpenAIGPTLiveMediaAdapter:
    """Per-call OpenAI GPT-Live WebSocket adapter for Kaiwen Gateway.

    Implements ``AIRealtimeMediaPort`` and ``InterruptibleAIPort`` against
    ``wss://api.openai.com/v1/live/sessions`` (``gpt-live-1``).

    Protocol & Lifecycle Invariants:
    - ``__init__`` is strictly synchronous with zero network I/O.
    - On first ``send_frame`` or ``receive_frame``, opens the WebSocket connection
      and sends ``session.start``.
    - No ``session.input_audio.append`` event is transmitted until the server
      confirms readiness with ``session.started`` (bounded by
      ``session_start_timeout_seconds``).
    - GPT-Live handles overlapping speech natively in full-duplex and does not
      expose a ``response.cancel`` wire command; ``interruption_capabilities``
      truthfully declares ``can_cancel_future_generation=False`` and
      ``can_discard_local_output=True``. Calling ``cancel_turn(turn_id)``
      purges local buffered output, resets the resampler/packer, fences stale
      turn frames until new caller input arrives, and advances the active turn ID.
    """

    def __init__(
        self,
        *,
        call_id: str,
        tenant_id: str,
        config: OpenAIGPTLiveConfig,
        queue_capacity: int | None = None,
        backpressure_timeout: float | None = None,
        transport_connector: OpenAIGPTLiveTransportConnector | None = None,
    ) -> None:
        if not call_id or not call_id.strip():
            raise ValueError("call_id must be a non-empty string")
        if not tenant_id or not tenant_id.strip():
            raise ValueError("tenant_id must be a non-empty string")
        if queue_capacity is not None and queue_capacity <= 0:
            raise ValueError("queue_capacity must be > 0")
        if backpressure_timeout is not None and backpressure_timeout <= 0:
            raise ValueError("backpressure_timeout must be > 0")

        self._call_id = call_id
        self._tenant_id = tenant_id
        self._config = config
        self._connector: OpenAIGPTLiveTransportConnector = (
            transport_connector or connect_openai_gpt_live_websocket
        )

        inbound_cap = (
            queue_capacity if queue_capacity is not None else config.inbound_queue_capacity
        )
        outbound_cap = (
            queue_capacity if queue_capacity is not None else config.outbound_queue_capacity
        )
        self._backpressure_timeout = (
            backpressure_timeout
            if backpressure_timeout is not None
            else config.backpressure_timeout_seconds
        )

        self._inbound_queue: asyncio.Queue[AudioFrame] = asyncio.Queue(maxsize=inbound_cap)
        self._outbound_queue: asyncio.Queue[AudioFrame | _EOFSentinel | _ErrorSentinel] = (
            asyncio.Queue(maxsize=outbound_cap)
        )

        self._resampler_in = PCM16Resampler16kTo24k()
        self._resampler_out = PCM16Resampler24kTo16k()
        self._packer = OutboundFramePacker()

        self._init_lock = asyncio.Lock()
        self._cancel_lock = asyncio.Lock()
        self._transport: OpenAIGPTLiveTransport | None = None
        self._sender_task: asyncio.Task[None] | None = None
        self._receiver_task: asyncio.Task[None] | None = None

        self._session_started_event = asyncio.Event()
        self._provider_session_id: str | None = None

        self._closed = False
        self._closed_event = asyncio.Event()
        self._close_task: asyncio.Task[None] | None = None
        self._transport_close_task: asyncio.Task[None] | None = None
        self._eof_reached = False
        self._eof_delivered = False
        self._terminal_error: Exception | None = None

        self._fenced_turn_id = 0
        self._current_turn_id = 1
        self._committed_turn_id = 1
        self._turn_transition_event = asyncio.Event()
        self._turn_transition_event.set()
        self._suppress_until_next_input = False
        self._delivered_outbound_seq = 0

        # Semantic side channel (opt-in). GPT-Live documents transcript deltas without
        # a final marker and no client-delegation tool-call contract (see guide).
        self._semantic: BoundedSemanticEventChannel | None = (
            BoundedSemanticEventChannel(config.semantic_queue_capacity)
            if config.enable_semantic_events
            else None
        )
        self._semantic_closed = False
        self._last_provider_error_code: str | None = None

    @property
    def call_id(self) -> str:
        """Return the call ID bound to this adapter instance."""
        return self._call_id

    @property
    def tenant_id(self) -> str:
        """Return the tenant ID bound to this adapter instance."""
        return self._tenant_id

    @property
    def provider_session_id(self) -> str | None:
        """Return the provider session ID from ``session.started``, if established."""
        return self._provider_session_id

    @property
    def is_closed(self) -> bool:
        """Return True if this adapter has been closed or terminated by an error."""
        return self._closed

    @property
    def buffered_frame_count(self) -> int:
        """Return the number of currently buffered frames (0 when closed)."""
        if self._closed:
            return 0
        return max(self._outbound_queue.qsize(), self._inbound_queue.qsize())

    @property
    def interruption_capabilities(self) -> InterruptionCapabilities:
        """Declare verified interruption capabilities for OpenAI GPT-Live WebSocket.

        GPT-Live handles overlapping speech natively in full-duplex and does not
        provide a separate client cancellation command; local buffered output
        discarding and turn fencing are supported by the Gateway adapter.
        """
        return InterruptionCapabilities(
            can_cancel_future_generation=False,
            can_discard_local_output=True,
            can_cancel_transport_pending=False,
            can_clear_provider_playback=False,
            can_observe_playback_acks=False,
            cannot_confirm_playback=True,
        )

    @property
    def semantic_capabilities(self) -> SemanticCapabilities:
        """Declare semantic capabilities documented for GPT-Live client delegation.

        Tool calls are not declared: client delegation exposes no function-call
        contract, and Responses delegation is not implemented by this adapter.
        """
        if self._semantic is None:
            return SemanticCapabilities()
        return SemanticCapabilities(
            input_transcript_delta=True,
            output_transcript_delta=True,
            session_lifecycle=True,
        )

    @property
    def semantic_dropped_count(self) -> int:
        """Number of droppable semantic events discarded under consumer backpressure."""
        return self._semantic.dropped_count if self._semantic is not None else 0

    async def receive_semantic_event(self) -> SemanticEvent | None:
        """Return the next semantic event, or None once the channel has ended."""
        if self._semantic is None:
            raise UnsupportedSemanticCapabilityError(
                "semantic events are disabled (enable_semantic_events=False)"
            )
        return await self._semantic.receive()

    async def submit_tool_result(self, result: ToolResult) -> None:
        """Tool results are not supported by this adapter (no verified contract)."""
        raise UnsupportedSemanticCapabilityError(
            "OpenAI GPT-Live adapter does not support tool results"
        )

    def _emit_semantic(self, event: SemanticEvent) -> None:
        if self._semantic is not None and not self._semantic_closed:
            self._semantic.publish(event)

    def _next_seq(self) -> int:
        return self._semantic.next_sequence() if self._semantic is not None else 0

    def _close_semantic(self, reason: str, error: Exception | None = None) -> None:
        if self._semantic is None or self._semantic_closed:
            return
        if error is not None:
            self._emit_semantic(
                ProviderErrorEvent(
                    sequence=self._next_seq(),
                    turn_id=self._current_turn_id,
                    code=self._last_provider_error_code or "upstream_failure",
                    message=sanitize_provider_message(str(error), self._config.api_key)[:300],
                    fatal=True,
                )
            )
        self._emit_semantic(
            SessionLifecycleEvent(
                sequence=self._next_seq(),
                turn_id=self._current_turn_id,
                phase="closed",
                reason=reason[:64],
            )
        )
        self._semantic_closed = True
        self._semantic.close()

    def trigger_upstream_failure(self, error: Exception | None = None) -> None:
        """Inject a simulated upstream transport failure for deterministic testing."""
        err = error or OpenAIGPTLiveError("Simulated upstream GPT-Live transport failure")
        self._fail_sync(err)

    def _fail_sync(self, error: Exception) -> None:
        """Transition adapter synchronously to terminal closed state on upstream failure."""
        if self._terminal_error is None:
            self._terminal_error = error
        self._closed = True
        self._closed_event.set()
        self._session_started_event.set()
        self._turn_transition_event.set()
        self._close_semantic("upstream_failure", error)
        _drain_queue_nowait(self._inbound_queue)
        _drain_queue_nowait(self._outbound_queue)
        current = asyncio.current_task()
        for task in (self._sender_task, self._receiver_task):
            if task is not None and task is not current and not task.done():
                task.cancel()
        try:
            self._outbound_queue.put_nowait(_ErrorSentinel(error))
        except asyncio.QueueFull:
            pass

    def _raise_if_closed(self) -> None:
        if self._closed:
            if self._terminal_error is not None:
                raise self._terminal_error
            raise MediaPortClosedError("OpenAIGPTLiveMediaAdapter is closed")

    async def _ensure_connected(self) -> OpenAIGPTLiveTransport:
        self._raise_if_closed()
        if self._transport is not None:
            return self._transport

        async with self._init_lock:
            self._raise_if_closed()
            existing: OpenAIGPTLiveTransport | None = self._transport
            if existing is not None:
                return existing

            transport: OpenAIGPTLiveTransport | None = None
            try:
                transport = await asyncio.wait_for(
                    self._connector(self._config),
                    timeout=self._config.connect_timeout_seconds,
                )
                try:
                    self._raise_if_closed()
                    session_start = self._config.build_session_start_event()
                    await asyncio.wait_for(
                        transport.send_json(session_start),
                        timeout=self._config.connect_timeout_seconds,
                    )
                    self._raise_if_closed()
                except MediaPortClosedError:
                    await self._close_transport_bounded(transport)
                    raise
            except asyncio.CancelledError:
                if transport is not None:
                    await self._close_transport_bounded(transport)
                self._closed = True
                raise
            except MediaPortClosedError:
                self._closed = True
                raise
            except Exception as exc:
                if transport is not None:
                    await self._close_transport_bounded(transport)
                self._closed = True
                wrapped = (
                    exc
                    if isinstance(exc, MediaPortError)
                    else OpenAIGPTLiveError(
                        sanitize_provider_message(
                            f"OpenAI GPT-Live initialization failed: {exc}",
                            self._config.api_key,
                        )
                    )
                )
                self._terminal_error = wrapped
                if wrapped is exc:
                    raise
                raise wrapped from None

            self._transport = transport
            self._receiver_task = asyncio.create_task(
                self._receiver_loop(transport),
                name=f"openai-gpt-live-receiver-{self._call_id}",
            )
            self._sender_task = asyncio.create_task(
                self._sender_loop(transport),
                name=f"openai-gpt-live-sender-{self._call_id}",
            )
            return transport

    async def _close_transport_bounded(self, transport: OpenAIGPTLiveTransport) -> None:
        if self._transport_close_task is None:
            self._transport_close_task = asyncio.create_task(
                asyncio.wait_for(
                    transport.close(),
                    timeout=self._config.close_timeout_seconds,
                )
            )
        try:
            await asyncio.shield(self._transport_close_task)
        except Exception:
            pass

    async def _wait_for_session_started(self) -> None:
        """Gate outbound audio transmission on receiving ``session.started`` from GPT-Live."""
        if self._session_started_event.is_set():
            return
        started_task = asyncio.create_task(self._session_started_event.wait())
        closed_task = asyncio.create_task(self._closed_event.wait())
        try:
            done, pending = await asyncio.wait(
                {started_task, closed_task},
                timeout=self._config.session_start_timeout_seconds,
                return_when=asyncio.FIRST_COMPLETED,
            )
            for task in pending:
                task.cancel()
            if pending:
                await asyncio.gather(*pending, return_exceptions=True)

            if self._closed or closed_task in done:
                return
            if started_task not in done:
                raise OpenAIGPTLiveError("Timed out waiting for OpenAI GPT-Live session.started")
        finally:
            for task in (started_task, closed_task):
                if not task.done():
                    task.cancel()
            await asyncio.gather(started_task, closed_task, return_exceptions=True)

    async def _sender_loop(self, transport: OpenAIGPTLiveTransport) -> None:
        try:
            while not self._closed:
                frame = await self._inbound_queue.get()
                await self._wait_for_session_started()
                if self._closed_event.is_set():
                    return
                pcm_24k = self._resampler_in.resample(frame.payload)
                if not pcm_24k:
                    continue
                encoded = base64.b64encode(pcm_24k).decode("ascii")
                await transport.send_json(
                    {
                        "type": "session.input_audio.append",
                        "audio": encoded,
                    }
                )
        except asyncio.CancelledError:
            return
        except Exception as exc:
            err = (
                exc
                if isinstance(exc, MediaPortError)
                else OpenAIGPTLiveError(
                    sanitize_provider_message(
                        f"OpenAI GPT-Live send loop failed: {exc}", self._config.api_key
                    )
                )
            )
            self._fail_sync(err)
            await self._close_transport_bounded(transport)

    async def _put_outbound_bounded(self, item: AudioFrame | _EOFSentinel | _ErrorSentinel) -> None:
        try:
            await asyncio.wait_for(
                self._outbound_queue.put(item),
                timeout=self._backpressure_timeout,
            )
        except TimeoutError as exc:
            raise MediaBackpressureOverflowError(
                "OpenAI GPT-Live outbound audio queue saturated by slow consumer"
            ) from exc

    async def _receiver_loop(self, transport: OpenAIGPTLiveTransport) -> None:
        try:
            while not self._closed and not self._eof_reached:
                event = await transport.receive_json()
                if event is None or self._eof_reached:
                    if not self._eof_reached:
                        await self._handle_clean_eof()
                    return
                await self._handle_server_event(event)
        except asyncio.CancelledError:
            return
        except Exception as exc:
            err = (
                exc
                if isinstance(exc, (MediaPortError, MediaBackpressureOverflowError))
                else OpenAIGPTLiveError(
                    sanitize_provider_message(
                        f"OpenAI GPT-Live receive loop failed: {exc}", self._config.api_key
                    )
                )
            )
            self._fail_sync(err)
            await self._close_transport_bounded(transport)

    async def _handle_clean_eof(self) -> None:
        if not self._session_started_event.is_set():
            raise OpenAIGPTLiveError(
                "OpenAI GPT-Live connection closed before session.started was received"
            )
        if not self._suppress_until_next_input and self._current_turn_id > self._fenced_turn_id:
            tail = self._packer.flush_partial(turn_id=self._current_turn_id)
            if tail is not None:
                await self._put_outbound_bounded(tail)
        self._eof_reached = True
        self._close_semantic("provider_closed")
        try:
            self._outbound_queue.put_nowait(_EOF)
        except asyncio.QueueFull:
            pass

    async def _handle_server_event(self, event: dict[str, Any]) -> None:
        event_type = event.get("type")
        if not isinstance(event_type, str) or not event_type:
            raise OpenAIGPTLiveError("OpenAI GPT-Live event missing string 'type' field")

        if event_type == "error":
            err_obj = event.get("error")
            if isinstance(err_obj, dict):
                code = err_obj.get("code") or err_obj.get("type") or "provider_error"
                self._last_provider_error_code = str(code)[:64]
                msg = str(err_obj.get("message") or "OpenAI GPT-Live error")
                safe_msg = sanitize_provider_message(msg, self._config.api_key)
                raise OpenAIGPTLiveError(f"OpenAI GPT-Live error ({code}): {safe_msg}")
            raise OpenAIGPTLiveError("OpenAI GPT-Live returned an error event")

        if event_type == "session.started":
            sess = event.get("session")
            if isinstance(sess, dict):
                sid = sess.get("id")
                if isinstance(sid, str) and sid:
                    self._provider_session_id = sid
            self._session_started_event.set()
            self._emit_semantic(
                SessionLifecycleEvent(
                    sequence=self._next_seq(), turn_id=self._current_turn_id, phase="ready"
                )
            )
            return

        if event_type in {"session.input_transcript.delta", "session.output_transcript.delta"}:
            delta = event.get("delta")
            if isinstance(delta, str) and delta:
                is_input = event_type == "session.input_transcript.delta"
                if is_input or not self._suppress_until_next_input:
                    self._emit_semantic(
                        TranscriptEvent(
                            sequence=self._next_seq(),
                            turn_id=self._current_turn_id,
                            role="user" if is_input else "assistant",
                            text=delta[:MAX_TRANSCRIPT_TEXT_CHARS],
                            is_final=False,
                        )
                    )
            return

        if event_type == "session.closed":
            reason = event.get("reason")
            self._close_semantic(reason if isinstance(reason, str) else "provider_closed")
            return

        if event_type == "session.output_audio.delta":
            if not self._session_started_event.is_set():
                raise OpenAIGPTLiveError(
                    "Received session.output_audio.delta before session.started"
                )
            delta_b64 = event.get("delta")
            if not isinstance(delta_b64, str):
                raise OpenAIGPTLiveError("session.output_audio.delta missing string 'delta'")
            try:
                pcm_24k = base64.b64decode(delta_b64, validate=True)
            except (binascii.Error, ValueError) as exc:
                raise OpenAIGPTLiveError(
                    "Invalid base64 audio payload in session.output_audio.delta"
                ) from exc

            explicit_turn = event.get("turn_id")
            if isinstance(explicit_turn, int) and not isinstance(explicit_turn, bool):
                if explicit_turn <= self._fenced_turn_id:
                    return
                turn_id = explicit_turn
            else:
                if self._suppress_until_next_input:
                    return
                turn_id = self._current_turn_id

            if turn_id <= self._fenced_turn_id:
                return

            pcm_16k = self._resampler_out.resample(pcm_24k)
            frames = self._packer.push_bytes(pcm_16k, turn_id=turn_id)
            for frame in frames:
                if self._eof_reached or (
                    frame.turn_id is not None and frame.turn_id <= self._fenced_turn_id
                ):
                    return
                await self._put_outbound_bounded(frame)
            return

        if event_type in {"session.output_audio.done", "session.ended", "session.stopped"}:
            if not self._suppress_until_next_input and self._current_turn_id > self._fenced_turn_id:
                tail = self._packer.flush_partial(turn_id=self._current_turn_id)
                if tail is not None:
                    await self._put_outbound_bounded(tail)
            return

    async def send_frame(self, frame: AudioFrame) -> None:
        """Validate, enqueue, resample (16 kHz -> 24 kHz), and stream an inbound frame."""
        validate_inbound_frame(frame)
        self._raise_if_closed()

        await self._ensure_connected()
        self._raise_if_closed()

        if frame.turn_id is not None and frame.turn_id <= self._fenced_turn_id:
            return
        self._suppress_until_next_input = False

        try:
            self._inbound_queue.put_nowait(frame)
            return
        except asyncio.QueueFull:
            pass

        put_task = asyncio.create_task(self._inbound_queue.put(frame))
        close_task = asyncio.create_task(self._closed_event.wait())
        try:
            done, pending = await asyncio.wait(
                {put_task, close_task},
                timeout=self._backpressure_timeout,
                return_when=asyncio.FIRST_COMPLETED,
            )
            for task in pending:
                task.cancel()
            if pending:
                await asyncio.gather(*pending, return_exceptions=True)

            if self._closed or close_task in done:
                if put_task in done:
                    _drain_queue_nowait(self._inbound_queue)
                self._raise_if_closed()
            if put_task not in done:
                raise MediaBackpressureOverflowError(
                    "OpenAI GPT-Live inbound audio queue saturated"
                )
        finally:
            for task in (put_task, close_task):
                if not task.done():
                    task.cancel()
            await asyncio.gather(put_task, close_task, return_exceptions=True)
            if self._closed:
                _drain_queue_nowait(self._inbound_queue)

    async def receive_frame(self) -> AudioFrame | None:
        """Receive the next resampled 16 kHz PCM16LE AudioFrame, or None on clean EOF."""
        if self._terminal_error is not None:
            raise self._terminal_error
        if self._eof_reached and self._outbound_queue.empty():
            self._eof_delivered = True
            return None
        if self._closed and self._outbound_queue.empty():
            if self._eof_delivered:
                return None
            raise MediaPortClosedError("OpenAIGPTLiveMediaAdapter is closed")

        await self._ensure_connected()

        while True:
            if self._eof_reached and self._outbound_queue.empty():
                self._eof_delivered = True
                return None
            item = await self._outbound_queue.get()
            if isinstance(item, _ErrorSentinel):
                raise item.error
            if isinstance(item, _EOFSentinel):
                self._eof_delivered = True
                return None
            if item.turn_id is not None and item.turn_id > self._committed_turn_id:
                await self._turn_transition_event.wait()
                if self._terminal_error is not None:
                    raise self._terminal_error
                if self._closed and self._outbound_queue.empty():
                    if self._eof_delivered:
                        return None
                    raise MediaPortClosedError("OpenAIGPTLiveMediaAdapter is closed")
            if item.turn_id is not None and item.turn_id <= self._fenced_turn_id:
                continue
            seq = self._delivered_outbound_seq
            self._delivered_outbound_seq += 1
            if item.sequence != seq:
                item = AudioFrame(
                    payload=item.payload,
                    format=item.format,
                    sample_rate=item.sample_rate,
                    channels=item.channels,
                    sequence=seq,
                    timestamp_ms=seq * GATEWAY_FRAME_DURATION_MS,
                    turn_id=item.turn_id,
                )
            return item

    async def cancel_turn(self, turn_id: int) -> None:
        """Purge local output buffers, fence stale output for ``turn_id``, and advance turn ID."""
        if self._closed:
            raise MediaPortClosedError("OpenAIGPTLiveMediaAdapter is closed")

        async with self._cancel_lock:
            self._turn_transition_event.clear()
            try:
                if turn_id > self._fenced_turn_id:
                    self._suppress_until_next_input = True
                self._fenced_turn_id = max(self._fenced_turn_id, turn_id)
                self._current_turn_id = max(self._current_turn_id, turn_id + 1)
                self._packer.reset()
                self._resampler_out.reset()

                retained: list[AudioFrame | _EOFSentinel | _ErrorSentinel] = []
                while not self._outbound_queue.empty():
                    try:
                        item = self._outbound_queue.get_nowait()
                    except asyncio.QueueEmpty:
                        break
                    if isinstance(item, AudioFrame):
                        if item.turn_id is not None and item.turn_id > self._fenced_turn_id:
                            retained.append(item)
                    else:
                        retained.append(item)

                for item in retained:
                    try:
                        self._outbound_queue.put_nowait(item)
                    except asyncio.QueueFull:
                        break
            finally:
                self._committed_turn_id = max(self._committed_turn_id, self._current_turn_id)
                self._turn_transition_event.set()

    async def push_eof(self, timeout: float | None = None) -> None:
        """Signal clean end-of-stream once queued inbound frames have been transmitted."""
        if self._closed or self._eof_reached:
            return
        wait_s = min(timeout if timeout is not None and timeout > 0 else 0.5, 0.5)
        deadline = time.monotonic() + wait_s
        while not self._inbound_queue.empty() and not self._closed and time.monotonic() < deadline:
            await asyncio.sleep(0.005)
        self._eof_reached = True
        self._close_semantic("local_eof")
        if self._receiver_task is not None and not self._receiver_task.done():
            self._receiver_task.cancel()
        try:
            self._outbound_queue.put_nowait(_EOF)
        except asyncio.QueueFull:
            pass

    async def close(self) -> None:
        """Idempotently close the adapter, cancel background tasks, and close transport."""
        self._closed = True
        self._closed_event.set()
        self._session_started_event.set()
        self._turn_transition_event.set()
        self._close_semantic("local_close")
        if self._close_task is None:
            self._close_task = asyncio.create_task(self._perform_close())
        await asyncio.shield(self._close_task)

    async def _perform_close(self) -> None:
        """Execute full adapter shutdown once in an independently owned task."""
        tasks: list[asyncio.Task[None]] = []
        for task in (self._sender_task, self._receiver_task):
            if task is not None and not task.done():
                task.cancel()
                tasks.append(task)

        if tasks:
            try:
                await asyncio.wait_for(
                    asyncio.gather(*tasks, return_exceptions=True),
                    timeout=self._config.close_timeout_seconds,
                )
            except TimeoutError:
                pass

        if self._transport is not None:
            await self._close_transport_bounded(self._transport)

        _drain_queue_nowait(self._inbound_queue)
        _drain_queue_nowait(self._outbound_queue)
        try:
            self._outbound_queue.put_nowait(_EOF)
        except asyncio.QueueFull:
            pass


def create_openai_gpt_live_adapter_factory(
    config: OpenAIGPTLiveConfig,
    *,
    transport_connector: OpenAIGPTLiveTransportConnector | None = None,
) -> Callable[..., OpenAIGPTLiveMediaAdapter]:
    """Build a synchronous ``ai_media_port_factory`` bound to ``config``.

    Compatible with ``create_gateway_app(ai_media_port_factory=...)``; receives
    verified ``call_id`` and ``tenant_id`` per call session.
    """

    def _factory(*, call_id: str, tenant_id: str, **_kwargs: Any) -> OpenAIGPTLiveMediaAdapter:
        return OpenAIGPTLiveMediaAdapter(
            call_id=call_id,
            tenant_id=tenant_id,
            config=config,
            transport_connector=transport_connector,
        )

    return _factory
