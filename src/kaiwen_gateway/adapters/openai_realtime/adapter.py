"""OpenAI Realtime media adapter implementing AIRealtimeMediaPort and InterruptibleAIPort."""

from __future__ import annotations

import asyncio
import base64
import binascii
from collections.abc import Callable
from typing import Any

from kaiwen_gateway.adapters.openai_realtime.audio import (
    OutboundFramePacker,
    PCM16Resampler16kTo24k,
    PCM16Resampler24kTo16k,
    validate_inbound_frame,
)
from kaiwen_gateway.adapters.openai_realtime.config import (
    OpenAIRealtimeConfig,
    OpenAIRealtimeError,
    sanitize_provider_message,
)
from kaiwen_gateway.adapters.openai_realtime.transport import (
    OpenAIRealtimeTransport,
    OpenAIRealtimeTransportConnector,
    connect_openai_realtime_websocket,
)
from kaiwen_gateway.modules.voice.domain import (
    AudioFrame,
    MediaBackpressureOverflowError,
    MediaPortClosedError,
    MediaPortError,
)
from kaiwen_gateway.modules.voice.ports import InterruptionCapabilities


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


_MAX_RESPONSE_TURN_HISTORY = 64


class OpenAIRealtimeMediaAdapter:
    """Per-call OpenAI Realtime WebSocket adapter for Kaiwen Gateway.

    Implements both ``AIRealtimeMediaPort`` and ``InterruptibleAIPort``.

    Lifecycle:
    - ``__init__`` is strictly synchronous with zero network I/O.
    - Connection and GA ``session.update`` happen lazily on first ``send_frame``
      or ``receive_frame`` under ``_init_lock``, bounded by ``connect_timeout_seconds``.
    - ``close()`` is idempotent and cancels/joins all adapter-owned background tasks.
    """

    def __init__(
        self,
        *,
        call_id: str,
        tenant_id: str,
        config: OpenAIRealtimeConfig,
        queue_capacity: int | None = None,
        backpressure_timeout: float | None = None,
        transport_connector: OpenAIRealtimeTransportConnector | None = None,
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
        self._connector: OpenAIRealtimeTransportConnector = (
            transport_connector or connect_openai_realtime_websocket
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
        self._transport: OpenAIRealtimeTransport | None = None
        self._sender_task: asyncio.Task[None] | None = None
        self._receiver_task: asyncio.Task[None] | None = None

        self._closed = False
        self._closed_event = asyncio.Event()
        self._close_task: asyncio.Task[None] | None = None
        self._transport_close_task: asyncio.Task[None] | None = None
        self._eof_reached = False
        self._eof_delivered = False
        self._terminal_error: Exception | None = None

        self._fenced_turn_id = 0
        self._current_turn_id = 1
        self._active_response_id: str | None = None
        self._pending_cancel_acks = 0
        self._pending_cancel_boundaries = 0
        self._response_turn_map: dict[str, int] = {}

    @property
    def call_id(self) -> str:
        """Return the call ID bound to this adapter instance."""
        return self._call_id

    @property
    def tenant_id(self) -> str:
        """Return the tenant ID bound to this adapter instance."""
        return self._tenant_id

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
        """Declare verified interruption capabilities for OpenAI Realtime WebSocket."""
        return InterruptionCapabilities(
            can_cancel_future_generation=True,
            can_discard_local_output=True,
            can_cancel_transport_pending=False,
            can_clear_provider_playback=False,
            can_observe_playback_acks=False,
            cannot_confirm_playback=True,
        )

    def trigger_upstream_failure(self, error: Exception | None = None) -> None:
        """Inject a simulated upstream transport failure for deterministic testing."""
        err = error or OpenAIRealtimeError("Simulated upstream transport failure")
        self._fail_sync(err)

    def _fail_sync(self, error: Exception) -> None:
        """Transition adapter synchronously to terminal closed state on upstream failure."""
        if self._terminal_error is None:
            self._terminal_error = error
        self._closed = True
        self._closed_event.set()
        _drain_queue_nowait(self._inbound_queue)
        _drain_queue_nowait(self._outbound_queue)
        self._response_turn_map.clear()
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
            raise MediaPortClosedError("OpenAIRealtimeMediaAdapter is closed")

    async def _ensure_connected(self) -> OpenAIRealtimeTransport:
        self._raise_if_closed()
        if self._transport is not None:
            return self._transport

        async with self._init_lock:
            self._raise_if_closed()
            existing: OpenAIRealtimeTransport | None = self._transport
            if existing is not None:
                return existing

            transport: OpenAIRealtimeTransport | None = None
            try:
                transport = await asyncio.wait_for(
                    self._connector(self._config),
                    timeout=self._config.connect_timeout_seconds,
                )
                try:
                    self._raise_if_closed()
                    session_update = self._config.build_session_update_event()
                    await asyncio.wait_for(
                        transport.send_json(session_update),
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
                    else OpenAIRealtimeError(
                        sanitize_provider_message(
                            f"OpenAI Realtime initialization failed: {exc}",
                            self._config.api_key,
                        )
                    )
                )
                self._terminal_error = wrapped
                if wrapped is exc:
                    raise
                raise wrapped from None

            self._transport = transport
            self._sender_task = asyncio.create_task(
                self._sender_loop(transport),
                name=f"openai-rt-sender-{self._call_id}",
            )
            self._receiver_task = asyncio.create_task(
                self._receiver_loop(transport),
                name=f"openai-rt-receiver-{self._call_id}",
            )
            return transport

    async def _close_transport_bounded(self, transport: OpenAIRealtimeTransport) -> None:
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

    async def _sender_loop(self, transport: OpenAIRealtimeTransport) -> None:
        try:
            while not self._closed:
                frame = await self._inbound_queue.get()
                pcm_24k = self._resampler_in.resample(frame.payload)
                if not pcm_24k:
                    continue
                encoded = base64.b64encode(pcm_24k).decode("ascii")
                await transport.send_json(
                    {
                        "type": "input_audio_buffer.append",
                        "audio": encoded,
                    }
                )
        except asyncio.CancelledError:
            return
        except Exception as exc:
            err = (
                exc
                if isinstance(exc, MediaPortError)
                else OpenAIRealtimeError(
                    sanitize_provider_message(
                        f"OpenAI Realtime send loop failed: {exc}", self._config.api_key
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
                "OpenAI Realtime outbound audio queue saturated by slow consumer"
            ) from exc

    async def _receiver_loop(self, transport: OpenAIRealtimeTransport) -> None:
        try:
            while not self._closed:
                event = await transport.receive_json()
                if event is None:
                    await self._handle_clean_eof()
                    return
                await self._handle_server_event(event)
        except asyncio.CancelledError:
            return
        except Exception as exc:
            err = (
                exc
                if isinstance(exc, (MediaPortError, MediaBackpressureOverflowError))
                else OpenAIRealtimeError(
                    sanitize_provider_message(
                        f"OpenAI Realtime receive loop failed: {exc}", self._config.api_key
                    )
                )
            )
            self._fail_sync(err)
            await self._close_transport_bounded(transport)

    def _record_response_turn(self, resp_id: str, turn_id: int) -> int:
        """Record ``resp_id -> turn_id`` within a bounded FIFO history."""
        self._response_turn_map[resp_id] = turn_id
        while len(self._response_turn_map) > _MAX_RESPONSE_TURN_HISTORY:
            oldest_key = next(iter(self._response_turn_map))
            del self._response_turn_map[oldest_key]
        return turn_id

    async def _handle_clean_eof(self) -> None:
        if self._active_response_id is not None:
            turn_id = self._response_turn_map.get(self._active_response_id, self._current_turn_id)
            if turn_id > self._fenced_turn_id:
                tail = self._packer.flush_partial(turn_id=turn_id)
                if tail is not None:
                    await self._put_outbound_bounded(tail)
        self._eof_reached = True
        try:
            self._outbound_queue.put_nowait(_EOF)
        except asyncio.QueueFull:
            pass

    async def _handle_server_event(self, event: dict[str, Any]) -> None:
        event_type = event.get("type")
        if not isinstance(event_type, str) or not event_type:
            raise OpenAIRealtimeError("OpenAI Realtime event missing valid 'type' field")

        if event_type == "error":
            err_obj = event.get("error")
            if isinstance(err_obj, dict):
                code = err_obj.get("code") or err_obj.get("type") or "provider_error"
                if code == "response_cancel_not_active" and self._pending_cancel_acks > 0:
                    self._pending_cancel_acks -= 1
                    if self._pending_cancel_boundaries > 0:
                        self._pending_cancel_boundaries -= 1
                    return
                msg = str(err_obj.get("message") or "OpenAI Realtime error")
                safe_msg = sanitize_provider_message(msg, self._config.api_key)
                raise OpenAIRealtimeError(f"OpenAI Realtime error ({code}): {safe_msg}")
            raise OpenAIRealtimeError("OpenAI Realtime returned an error event")

        if event_type == "response.created":
            resp = event.get("response")
            if isinstance(resp, dict):
                resp_id = resp.get("id")
                if isinstance(resp_id, str) and resp_id:
                    self._active_response_id = resp_id
                    if self._pending_cancel_boundaries > 0:
                        self._record_response_turn(resp_id, self._fenced_turn_id)
                    else:
                        self._record_response_turn(resp_id, self._current_turn_id)
            return

        if event_type == "response.output_audio.delta":
            delta_b64 = event.get("delta")
            if not isinstance(delta_b64, str):
                raise OpenAIRealtimeError("response.output_audio.delta missing string 'delta'")
            try:
                pcm_24k = base64.b64decode(delta_b64, validate=True)
            except (binascii.Error, ValueError) as exc:
                raise OpenAIRealtimeError(
                    "Invalid base64 audio payload in response.output_audio.delta"
                ) from exc

            resp_id = event.get("response_id")
            default_turn = (
                self._fenced_turn_id
                if self._pending_cancel_boundaries > 0
                else self._current_turn_id
            )
            if isinstance(resp_id, str) and resp_id:
                turn_id = self._response_turn_map.get(resp_id)
                if turn_id is None:
                    turn_id = self._record_response_turn(resp_id, default_turn)
            else:
                turn_id = default_turn

            if turn_id <= self._fenced_turn_id:
                return

            pcm_16k = self._resampler_out.resample(pcm_24k)
            frames = self._packer.push_bytes(pcm_16k, turn_id=turn_id)
            for frame in frames:
                if frame.turn_id is not None and frame.turn_id <= self._fenced_turn_id:
                    return
                await self._put_outbound_bounded(frame)
            return

        if event_type in {"response.output_audio.done", "response.done"}:
            resp_obj = event.get("response") if isinstance(event.get("response"), dict) else None
            resp_id = event.get("response_id")
            if resp_id is None and resp_obj is not None:
                resp_id = resp_obj.get("id")
            if isinstance(resp_id, str) and resp_id in self._response_turn_map:
                turn_id = self._response_turn_map[resp_id]
            else:
                turn_id = (
                    self._fenced_turn_id
                    if self._pending_cancel_boundaries > 0
                    else self._current_turn_id
                )

            if turn_id > self._fenced_turn_id:
                tail = self._packer.flush_partial(turn_id=turn_id)
                if tail is not None:
                    await self._put_outbound_bounded(tail)
            if event_type == "response.done":
                if isinstance(resp_id, str) and resp_id == self._active_response_id:
                    self._active_response_id = None
                status = resp_obj.get("status") if resp_obj is not None else None
                if status == "cancelled":
                    if self._pending_cancel_acks > 0:
                        self._pending_cancel_acks -= 1
                    if self._pending_cancel_boundaries > 0:
                        self._pending_cancel_boundaries -= 1
                elif status == "failed":
                    status_details = (
                        resp_obj.get("status_details") if resp_obj is not None else None
                    )
                    err_obj = (
                        status_details.get("error") if isinstance(status_details, dict) else None
                    )
                    if isinstance(err_obj, dict):
                        code = err_obj.get("code") or err_obj.get("type") or "response_failed"
                        msg = str(err_obj.get("message") or "OpenAI Realtime response failed")
                        safe_msg = sanitize_provider_message(msg, self._config.api_key)
                        raise OpenAIRealtimeError(
                            f"OpenAI Realtime response failed ({code}): {safe_msg}"
                        )
                    raise OpenAIRealtimeError("OpenAI Realtime response failed")
            return

    async def send_frame(self, frame: AudioFrame) -> None:
        """Validate, enqueue, resample (16 kHz -> 24 kHz), and stream an inbound frame."""
        validate_inbound_frame(frame)
        self._raise_if_closed()

        await self._ensure_connected()
        self._raise_if_closed()

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
                    "OpenAI Realtime inbound audio queue saturated"
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
            raise MediaPortClosedError("OpenAIRealtimeMediaAdapter is closed")

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
            if item.turn_id is not None and item.turn_id <= self._fenced_turn_id:
                continue
            return item

    async def cancel_turn(self, turn_id: int) -> None:
        """Fence stale output for ``turn_id`` and send ``response.cancel`` to OpenAI."""
        if self._closed:
            raise MediaPortClosedError("OpenAIRealtimeMediaAdapter is closed")

        async with self._cancel_lock:
            self._fenced_turn_id = max(self._fenced_turn_id, turn_id)
            self._current_turn_id = max(self._current_turn_id, turn_id + 1)
            if self._transport is not None and self._active_response_id is None:
                # A response.created for the interrupted generation may still be in-flight
                # or buffered on the socket; fence it until its terminal boundary arrives.
                self._pending_cancel_boundaries += 1
            self._packer.reset()
            self._resampler_out.reset()

            # Purge locally queued frames belonging to superseded turns
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

            if self._transport is not None and not self._closed:
                payload: dict[str, Any] = {"type": "response.cancel"}
                self._pending_cancel_acks += 1
                try:
                    await asyncio.wait_for(
                        self._transport.send_json(payload),
                        timeout=self._config.cancel_timeout_seconds,
                    )
                except TimeoutError as exc:
                    self._pending_cancel_acks = max(0, self._pending_cancel_acks - 1)
                    raise OpenAIRealtimeError("Timed out sending response.cancel") from exc

    async def close(self) -> None:
        """Idempotently close the adapter, cancel background tasks, and close transport."""
        self._closed = True
        self._closed_event.set()
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
        self._response_turn_map.clear()
        try:
            self._outbound_queue.put_nowait(_EOF)
        except asyncio.QueueFull:
            pass


def create_openai_realtime_adapter_factory(
    config: OpenAIRealtimeConfig,
    *,
    transport_connector: OpenAIRealtimeTransportConnector | None = None,
) -> Callable[..., OpenAIRealtimeMediaAdapter]:
    """Build a synchronous ``ai_media_port_factory`` bound to ``config``.

    Compatible with ``create_gateway_app(ai_media_port_factory=...)``; receives
    verified ``call_id`` and ``tenant_id`` per call session.
    """

    def _factory(*, call_id: str, tenant_id: str, **_kwargs: Any) -> OpenAIRealtimeMediaAdapter:
        return OpenAIRealtimeMediaAdapter(
            call_id=call_id,
            tenant_id=tenant_id,
            config=config,
            transport_connector=transport_connector,
        )

    return _factory
