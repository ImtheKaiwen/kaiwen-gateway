"""Bidirectional in-process audio bridge for voice streaming sessions.

Invariants:
- One call, one isolated session: session holds private queues and tasks.
- Concurrently supervised pumps: inbound (telephony -> AI) and outbound (AI -> telephony).
- Strictly bounded media queues: backpressure applied on full queue; zero silent drops.
- Precise task ownership: supervisor manages pump lifecycles; cancels upon error or close.
- Idempotent termination: close() can be called multiple times safely.
- Zero conversion to JSON: raw AudioFrame instances are streamed directly.
"""

from __future__ import annotations

import asyncio
import math
from enum import StrEnum
from types import TracebackType
from typing import Final, Self

from kaiwen_gateway.modules.voice.domain import (
    AudioFrame,
    InvalidAudioFrameError,
    MediaBackpressureOverflowError,
    OutOfOrderAudioFrameError,
    VoiceBridgeSessionClosedError,
    VoiceDomainError,
)
from kaiwen_gateway.modules.voice.ports import (
    AIRealtimeMediaPort,
    TelephonyMediaPort,
)

__all__ = [
    "VoiceBridgeState",
    "VoiceBridgeSession",
]


class VoiceBridgeState(StrEnum):
    """Lifecycle state of a VoiceBridgeSession."""

    IDLE = "idle"
    RUNNING = "running"
    CLOSING = "closing"
    CLOSED = "closed"
    FAILED = "failed"


class VoiceBridgeSession:
    """Supervised bidirectional media bridge between a telephony port and an AI realtime port.

    Orchestrates two independent concurrent pumps:
    1. Inbound pump: TelephonyMediaPort -> bounded queue -> AIRealtimeMediaPort.
    2. Outbound pump: AIRealtimeMediaPort -> bounded queue -> TelephonyMediaPort.
    """

    def __init__(
        self,
        *,
        call_id: str,
        tenant_id: str,
        telephony_port: TelephonyMediaPort,
        ai_port: AIRealtimeMediaPort,
        queue_capacity: int = 8,
        backpressure_timeout: float | None = None,
        strict_sequence: bool = False,
    ) -> None:
        if not isinstance(call_id, str) or not call_id.strip():
            raise VoiceDomainError("call_id must be a non-empty string")
        if not isinstance(tenant_id, str) or not tenant_id.strip():
            raise VoiceDomainError("tenant_id must be a non-empty string")
        if (
            isinstance(queue_capacity, bool)
            or not isinstance(queue_capacity, int)
            or queue_capacity < 1
        ):
            raise VoiceDomainError("queue_capacity must be an integer >= 1")
        if backpressure_timeout is not None:
            if isinstance(backpressure_timeout, bool) or not isinstance(
                backpressure_timeout, (int, float)
            ):
                raise VoiceDomainError(
                    "backpressure_timeout must be a positive finite float or None"
                )
            timeout_float = float(backpressure_timeout)
            if not math.isfinite(timeout_float) or timeout_float <= 0.0:
                raise VoiceDomainError(
                    "backpressure_timeout must be a positive finite float or None"
                )

        self.call_id: Final[str] = call_id.strip()
        self.tenant_id: Final[str] = tenant_id.strip()
        self._telephony_port: Final[TelephonyMediaPort] = telephony_port
        self._ai_port: Final[AIRealtimeMediaPort] = ai_port
        self._queue_capacity: Final[int] = queue_capacity
        self._backpressure_timeout: Final[float | None] = (
            float(backpressure_timeout) if backpressure_timeout is not None else None
        )
        self._strict_sequence: Final[bool] = bool(strict_sequence)

        # State management
        self._state: VoiceBridgeState = VoiceBridgeState.IDLE
        self._error: BaseException | None = None
        self._closing_event: asyncio.Event = asyncio.Event()
        self._closed_event: asyncio.Event = asyncio.Event()
        self._cleanup_lock: asyncio.Lock = asyncio.Lock()
        self._is_cleaned_up: bool = False

        # Bounded per-direction queues
        self._inbound_queue: asyncio.Queue[AudioFrame | None] = asyncio.Queue(
            maxsize=self._queue_capacity
        )
        self._outbound_queue: asyncio.Queue[AudioFrame | None] = asyncio.Queue(
            maxsize=self._queue_capacity
        )

        # Task tracking
        self._inbound_task: asyncio.Task[None] | None = None
        self._outbound_task: asyncio.Task[None] | None = None
        self._supervisor_task: asyncio.Task[None] | None = None

        # Counters and progression tracking
        self._inbound_frames_transferred: int = 0
        self._outbound_frames_transferred: int = 0
        self._inbound_completed: bool = False
        self._outbound_completed: bool = False
        self._expected_inbound_seq: int = 0
        self._expected_outbound_seq: int = 0

    @property
    def state(self) -> VoiceBridgeState:
        """Current lifecycle state of the bridge session."""
        return self._state

    @property
    def error(self) -> BaseException | None:
        """Exception that caused the bridge to fail, if any."""
        return self._error

    @property
    def inbound_queue_depth(self) -> int:
        """Current number of buffered audio frames in the inbound queue."""
        return self._inbound_queue.qsize()

    @property
    def outbound_queue_depth(self) -> int:
        """Current number of buffered audio frames in the outbound queue."""
        return self._outbound_queue.qsize()

    @property
    def inbound_frames_transferred(self) -> int:
        """Total audio frames successfully transferred from telephony to AI."""
        return self._inbound_frames_transferred

    @property
    def outbound_frames_transferred(self) -> int:
        """Total audio frames successfully transferred from AI to telephony."""
        return self._outbound_frames_transferred

    @property
    def inbound_completed(self) -> bool:
        """Return True if the inbound media leg completed cleanly on EOF."""
        return self._inbound_completed

    @property
    def outbound_completed(self) -> bool:
        """Return True if the outbound media leg completed cleanly on EOF."""
        return self._outbound_completed

    async def start(self) -> None:
        """Start the bidirectional media pumps under task supervision."""
        if self._state != VoiceBridgeState.IDLE:
            raise VoiceBridgeSessionClosedError(
                f"Cannot start VoiceBridgeSession in state {self._state.value}"
            )

        self._state = VoiceBridgeState.RUNNING
        self._inbound_task = asyncio.create_task(
            self._run_inbound_pump(),
            name=f"voice-inbound-{self.call_id}",
        )
        self._outbound_task = asyncio.create_task(
            self._run_outbound_pump(),
            name=f"voice-outbound-{self.call_id}",
        )
        self._supervisor_task = asyncio.create_task(
            self._run_supervisor(),
            name=f"voice-supervisor-{self.call_id}",
        )

    async def wait_until_closed(self, *, propagate_error: bool = True) -> None:
        """Wait until the bridge session completes, fails, or is closed."""
        await self._closed_event.wait()
        if propagate_error and self._error is not None:
            raise self._error

    async def run(self) -> None:
        """Start the bridge and wait for completion."""
        await self.start()
        try:
            await self.wait_until_closed()
        except asyncio.CancelledError:
            await self.close()
            raise

    async def close(self) -> None:
        """Idempotently close the session, cancel pump tasks, and release port resources."""
        if self._closed_event.is_set():
            return
        if self._state != VoiceBridgeState.FAILED:
            self._state = VoiceBridgeState.CLOSING
        self._closing_event.set()

        await self._cleanup()

        current = asyncio.current_task()
        if (
            self._supervisor_task is not None
            and self._supervisor_task is not current
            and not self._supervisor_task.done()
        ):
            try:
                await self._supervisor_task
            except (asyncio.CancelledError, Exception):
                pass

        await self._closed_event.wait()

    async def __aenter__(self) -> Self:
        await self.start()
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        await self.close()

    async def _cleanup(self) -> None:
        """Perform terminal cleanup of tasks and ports exactly once under lock."""
        async with self._cleanup_lock:
            if self._is_cleaned_up:
                return
            self._closing_event.set()

            # Cancel pump tasks if still running
            pending_pumps = [
                t
                for t in (self._inbound_task, self._outbound_task)
                if t is not None and not t.done()
            ]
            for t in pending_pumps:
                t.cancel()
            if pending_pumps:
                await asyncio.gather(*pending_pumps, return_exceptions=True)

            # Idempotently close underlying media ports
            try:
                await self._telephony_port.close()
            except Exception:
                pass

            try:
                await self._ai_port.close()
            except Exception:
                pass

            if self._error is not None or self._state == VoiceBridgeState.FAILED:
                self._state = VoiceBridgeState.FAILED
            else:
                self._state = VoiceBridgeState.CLOSED

            self._is_cleaned_up = True
            self._closed_event.set()

    async def _run_supervisor(self) -> None:
        """Supervise the two pumps, handle early failure, and drive termination."""
        try:
            assert self._inbound_task is not None
            assert self._outbound_task is not None

            done, pending = await asyncio.wait(
                [self._inbound_task, self._outbound_task],
                return_when=asyncio.FIRST_EXCEPTION,
            )

            failed_exc: BaseException | None = None
            for t in done:
                if t.cancelled():
                    if not self._closing_event.is_set():
                        failed_exc = asyncio.CancelledError()
                    break
                exc = t.exception()
                if exc is not None:
                    failed_exc = exc
                    break

            if failed_exc is not None:
                self._error = failed_exc
                self._state = VoiceBridgeState.FAILED
                for t in pending:
                    t.cancel()
                if pending:
                    await asyncio.gather(*pending, return_exceptions=True)
            else:
                if self._state not in (VoiceBridgeState.CLOSING, VoiceBridgeState.CLOSED):
                    self._state = VoiceBridgeState.CLOSED

            await self._cleanup()
        except asyncio.CancelledError:
            for pump_task in (self._inbound_task, self._outbound_task):
                if pump_task is not None and not pump_task.done():
                    pump_task.cancel()
            await self._cleanup()
            raise
        except Exception as exc:
            self._error = exc
            self._state = VoiceBridgeState.FAILED
            await self._cleanup()

    async def _run_inbound_pump(self) -> None:
        """Run the inbound media pump (Telephony -> AI)."""

        async def receiver() -> None:
            while not self._closing_event.is_set():
                frame = await self._telephony_port.receive_frame()
                if frame is None:
                    # Clean EOF from telephony media stream
                    await self._inbound_queue.put(None)
                    break

                if not isinstance(frame, AudioFrame):
                    raise InvalidAudioFrameError(
                        f"Expected AudioFrame from telephony port, got {type(frame).__name__}"
                    )

                if self._strict_sequence:
                    if frame.sequence != self._expected_inbound_seq:
                        raise OutOfOrderAudioFrameError(
                            f"Inbound frame sequence mismatch: expected {self._expected_inbound_seq}, "
                            f"got {frame.sequence}"
                        )
                    self._expected_inbound_seq += 1

                if self._backpressure_timeout is not None:
                    try:
                        await asyncio.wait_for(
                            self._inbound_queue.put(frame),
                            timeout=self._backpressure_timeout,
                        )
                    except TimeoutError as err:
                        raise MediaBackpressureOverflowError(
                            f"Inbound queue capacity {self._queue_capacity} exceeded "
                            f"and backpressure timed out after {self._backpressure_timeout}s"
                        ) from err
                else:
                    await self._inbound_queue.put(frame)

        async def forwarder() -> None:
            while not self._closing_event.is_set():
                item = await self._inbound_queue.get()
                if item is None:
                    self._inbound_queue.task_done()
                    break

                await self._ai_port.send_frame(item)
                self._inbound_frames_transferred += 1
                self._inbound_queue.task_done()

        rec_task = asyncio.create_task(receiver())
        fwd_task = asyncio.create_task(forwarder())
        try:
            done, pending = await asyncio.wait(
                [rec_task, fwd_task],
                return_when=asyncio.FIRST_EXCEPTION,
            )
            for t in pending:
                t.cancel()
            if pending:
                await asyncio.gather(*pending, return_exceptions=True)

            for t in done:
                if t.cancelled():
                    raise asyncio.CancelledError()
                exc = t.exception()
                if exc is not None:
                    raise exc
            self._inbound_completed = True
        finally:
            for t in (rec_task, fwd_task):
                if not t.done():
                    t.cancel()
            await asyncio.gather(rec_task, fwd_task, return_exceptions=True)

    async def _run_outbound_pump(self) -> None:
        """Run the outbound media pump (AI -> Telephony)."""

        async def receiver() -> None:
            while not self._closing_event.is_set():
                frame = await self._ai_port.receive_frame()
                if frame is None:
                    # Clean EOF from AI realtime media stream
                    await self._outbound_queue.put(None)
                    break

                if not isinstance(frame, AudioFrame):
                    raise InvalidAudioFrameError(
                        f"Expected AudioFrame from AI port, got {type(frame).__name__}"
                    )

                if self._strict_sequence:
                    if frame.sequence != self._expected_outbound_seq:
                        raise OutOfOrderAudioFrameError(
                            f"Outbound frame sequence mismatch: expected {self._expected_outbound_seq}, "
                            f"got {frame.sequence}"
                        )
                    self._expected_outbound_seq += 1

                if self._backpressure_timeout is not None:
                    try:
                        await asyncio.wait_for(
                            self._outbound_queue.put(frame),
                            timeout=self._backpressure_timeout,
                        )
                    except TimeoutError as err:
                        raise MediaBackpressureOverflowError(
                            f"Outbound queue capacity {self._queue_capacity} exceeded "
                            f"and backpressure timed out after {self._backpressure_timeout}s"
                        ) from err
                else:
                    await self._outbound_queue.put(frame)

        async def forwarder() -> None:
            while not self._closing_event.is_set():
                item = await self._outbound_queue.get()
                if item is None:
                    self._outbound_queue.task_done()
                    break

                await self._telephony_port.send_frame(item)
                self._outbound_frames_transferred += 1
                self._outbound_queue.task_done()

        rec_task = asyncio.create_task(receiver())
        fwd_task = asyncio.create_task(forwarder())
        try:
            done, pending = await asyncio.wait(
                [rec_task, fwd_task],
                return_when=asyncio.FIRST_EXCEPTION,
            )
            for t in pending:
                t.cancel()
            if pending:
                await asyncio.gather(*pending, return_exceptions=True)

            for t in done:
                if t.cancelled():
                    raise asyncio.CancelledError()
                exc = t.exception()
                if exc is not None:
                    raise exc
            self._outbound_completed = True
        finally:
            for t in (rec_task, fwd_task):
                if not t.done():
                    t.cancel()
            await asyncio.gather(rec_task, fwd_task, return_exceptions=True)
