"""Framework-independent, per-connection state machine and supervised lifecycle contracts.

This module enforces FR-CONN-004, FR-CONN-006, FR-CONN-007, NFR-REL-001, and ENG-004:
- Explicit connection state machine with a strictly validated, guarded transition matrix.
- Per-connection task supervisor managing worker coroutine lifecycles and cancellation propagation.
- Deterministic, multi-step cleanup order ensuring capacity lease release under normal exits,
  unhandled exceptions, timeouts, and task cancellations.
- Single ownership by construction: Each supervisor acquires and owns its dedicated capacity lease
  via `admit()`; no external lease handover or module-level shared mutable registries.
- Diagnostic privacy: Fixed canonical reason summaries preventing secret, PII, or identifier leakage.
- Synthetic safe task names without raw connection identifiers.
- Pure in-memory, process-local lifecycle management without network sockets, WebSocket handshakes,
  or upstream AI session creation.

Trust Boundaries & Explicit Limitations:
    `ConnectionSupervisor` manages process-local task supervision and capacity lease lifecycle for
    a single logical gateway connection. Closing a gateway transport connection does not prove that
    an underlying PSTN telephony leg has terminated, nor does it guarantee automatic session
    resumption across transport reconnects. Complete multi-instance draining, ASGI transport
    integration, and upstream AI session teardown are orchestrated in subsequent modules (M2.4B+, M3+).

    Task Preemption and Capacity Ceilings:
    In Python asyncio, non-cooperative coroutines that catch and ignore `CancelledError` or block
    the thread cannot be forcibly aborted without terminating the Python OS process. When worker
    shutdown exceeds `cleanup_timeout`, the supervisor marks the lifecycle as FAILED with TIMEOUT,
    records the failure, and releases the capacity lease to prevent permanent system-wide capacity
    deadlocks. Until non-cooperative worker tasks fully yield, actual running background tasks
    in the process may temporarily exceed nominal capacity limits.
"""

from __future__ import annotations

import asyncio
import math
from collections.abc import Callable, Coroutine
from dataclasses import dataclass
from enum import StrEnum
from typing import Any, Final

from kaiwen_gateway.core.admission import AdmissionController, AdmissionLease
from kaiwen_gateway.core.exceptions import (
    AdmissionError,
    ConnectionLifecycleError,
    CoreConfigurationError,
    InvalidConnectionTransitionError,
)
from kaiwen_gateway.core.identity import VerifiedConnectionContext


class ConnectionState(StrEnum):
    """Guarded connection states representing a gateway transport connection lifecycle."""

    NEW = "NEW"
    AUTHENTICATING = "AUTHENTICATING"
    ADMITTED = "ADMITTED"
    ACTIVE = "ACTIVE"
    CLOSING = "CLOSING"
    CLOSED = "CLOSED"
    REJECTED = "REJECTED"
    FAILED = "FAILED"

    @property
    def is_terminal(self) -> bool:
        """Return True if this state is terminal (no further transitions permitted)."""
        return self in (
            ConnectionState.CLOSED,
            ConnectionState.FAILED,
            ConnectionState.REJECTED,
        )

    @property
    def is_active(self) -> bool:
        """Return True if connection is currently active and processing media/control."""
        return self == ConnectionState.ACTIVE


_ALLOWED_TRANSITIONS: Final[dict[ConnectionState, frozenset[ConnectionState]]] = {
    ConnectionState.NEW: frozenset(
        {
            ConnectionState.AUTHENTICATING,
            ConnectionState.REJECTED,
        }
    ),
    ConnectionState.AUTHENTICATING: frozenset(
        {
            ConnectionState.ADMITTED,
            ConnectionState.REJECTED,
        }
    ),
    ConnectionState.ADMITTED: frozenset(
        {
            ConnectionState.ACTIVE,
            ConnectionState.CLOSING,
        }
    ),
    ConnectionState.ACTIVE: frozenset(
        {
            ConnectionState.CLOSING,
        }
    ),
    ConnectionState.CLOSING: frozenset(
        {
            ConnectionState.CLOSED,
            ConnectionState.FAILED,
        }
    ),
    ConnectionState.CLOSED: frozenset(),
    ConnectionState.FAILED: frozenset(),
    ConnectionState.REJECTED: frozenset(),
}


class TerminalReason(StrEnum):
    """Canonical classification for how a connection lifecycle terminated."""

    NORMAL = "normal"
    CANCELLED = "cancelled"
    ADMISSION_REJECTED = "admission_rejected"
    STARTUP_FAILED = "startup_failed"
    WORKER_FAILED = "worker_failed"
    TIMEOUT = "timeout"
    CLEANUP_FAILED = "cleanup_failed"


_SAFE_REASON_SUMMARIES: Final[dict[TerminalReason, str]] = {
    TerminalReason.NORMAL: "Normal connection closure",
    TerminalReason.CANCELLED: "Connection was cancelled",
    TerminalReason.ADMISSION_REJECTED: "Admission capacity rejected",
    TerminalReason.STARTUP_FAILED: "Connection startup failed",
    TerminalReason.WORKER_FAILED: "Worker task execution failed",
    TerminalReason.TIMEOUT: "Lifecycle deadline expired",
    TerminalReason.CLEANUP_FAILED: "Teardown cleanup failed",
}


@dataclass(frozen=True, slots=True)
class ConnectionOutcome:
    """Immutable terminal outcome for a terminated connection lifecycle.

    Guarantees:
    - `state` is always a terminal state (CLOSED, FAILED, or REJECTED).
    - `reason` is a canonical `TerminalReason`.
    - `error_summary` is strictly bound to safe canonical reason descriptions,
      preventing accidental credential, identifier, or exception message leakage.
    - `repr` and `str` outputs expose only `state` and `reason`.
    """

    state: ConnectionState
    reason: TerminalReason
    error_summary: str | None = None

    def __post_init__(self) -> None:
        safe_summary = _SAFE_REASON_SUMMARIES.get(self.reason, "Connection terminated")
        object.__setattr__(self, "error_summary", safe_summary)

    def __repr__(self) -> str:
        return f"ConnectionOutcome(state={self.state.value!r}, reason={self.reason.value!r})"

    def __str__(self) -> str:
        return self.__repr__()


@dataclass(frozen=True, slots=True)
class LifecyclePolicy:
    """Configurable timeout and supervision policy for a connection lifecycle."""

    startup_timeout: float = 5.0
    cleanup_timeout: float = 5.0

    def __post_init__(self) -> None:
        self._validate_timeout("startup_timeout", self.startup_timeout)
        self._validate_timeout("cleanup_timeout", self.cleanup_timeout)

    @staticmethod
    def _validate_timeout(name: str, value: object) -> None:
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            raise CoreConfigurationError(
                f"{name} must be a float or int (not bool), got {type(value).__name__}",
                field_name=name,
            )
        if not math.isfinite(value) or value <= 0:
            raise CoreConfigurationError(
                f"{name} must be a strictly positive finite number, got {value}",
                field_name=name,
            )


class ConnectionSupervisor:
    """Per-connection async task supervisor and lifecycle state machine manager.

    Invariants (M2.4A):
    - Single lifecycle owner: Owns the connection's state transitions and worker tasks.
    - Single lease ownership by construction: Supervisor acquires and owns its dedicated lease
      via `admit()`; no external lease injection or module-level shared registries.
    - Guarded state machine: Transitions strictly adhere to `_ALLOWED_TRANSITIONS`.
    - Admission bound: Cannot enter `ACTIVE` without a valid `AdmissionLease` acquired via `admit()`.
    - Sibling cancellation: If any worker task raises an unhandled error, sibling tasks
      are deterministically cancelled and awaited before cleanup.
    - Idempotent cleanup: Multiple concurrent or sequential `close()` calls execute cleanup
      exactly once without resource leaks.
    - Cancellation resilience: Cleanup runs within an owned supervisory task, ensuring
      that outer task cancellation does not prevent capacity lease release.
    - Zero orphan tasks: All spawned worker tasks are cancelled, observed, and awaited on termination.
    - Diagnostic privacy: Task names and terminal outcomes use safe synthetic names and canonical summaries.
    - Stale callback isolation: Callbacks reference this supervisor instance; closing does
      not affect subsequent supervisors even if the same connection ID is re-admitted.
    """

    MAX_FALLBACK_ATTEMPTS: Final[int] = 1

    __slots__ = (
        "_context",
        "_controller",
        "_policy",
        "_state",
        "_outcome",
        "_lease",
        "_worker_factories",
        "_worker_tasks",
        "_resources",
        "_lock",
        "_cleanup_task",
        "_trigger_close_tasks",
        "_closed_event",
        "_fallback_attempts",
    )

    def __init__(
        self,
        *,
        context: VerifiedConnectionContext,
        controller: AdmissionController,
        policy: LifecyclePolicy | None = None,
    ) -> None:
        if not isinstance(context, VerifiedConnectionContext):
            raise TypeError(
                f"context must be a VerifiedConnectionContext instance, got {type(context).__name__}"
            )
        if not isinstance(controller, AdmissionController):
            raise TypeError(
                f"controller must be an AdmissionController instance, got {type(controller).__name__}"
            )

        self._context: VerifiedConnectionContext = context
        self._controller: AdmissionController = controller
        self._policy: LifecyclePolicy = policy if policy is not None else LifecyclePolicy()
        self._outcome: ConnectionOutcome | None = None
        self._lease: AdmissionLease | None = None
        self._state: ConnectionState = ConnectionState.NEW
        self._worker_factories: list[Callable[[], Coroutine[Any, Any, None]]] = []
        self._worker_tasks: list[asyncio.Task[None]] = []
        self._resources: list[Callable[[], Coroutine[Any, Any, None]]] = []
        self._lock: asyncio.Lock = asyncio.Lock()
        self._cleanup_task: asyncio.Task[None] | None = None
        self._trigger_close_tasks: set[asyncio.Task[Any]] = set()
        self._closed_event: asyncio.Event = asyncio.Event()
        self._fallback_attempts: int = 0

    @property
    def connection_id(self) -> str:
        """Return canonical connection identifier for this supervised connection."""
        return self._context.connection_id

    @property
    def tenant_id(self) -> str:
        """Return canonical tenant identifier for this supervised connection."""
        return self._context.tenant_id

    @property
    def context(self) -> VerifiedConnectionContext:
        """Return the verified connection context bound to this supervisor."""
        return self._context

    @property
    def controller(self) -> AdmissionController:
        """Return the AdmissionController associated with this supervisor."""
        return self._controller

    @property
    def policy(self) -> LifecyclePolicy:
        """Return the LifecyclePolicy configured for this supervisor."""
        return self._policy

    @property
    def state(self) -> ConnectionState:
        """Return current lifecycle state."""
        return self._state

    @property
    def lease(self) -> AdmissionLease | None:
        """Return active AdmissionLease if admitted, or None."""
        return self._lease

    @property
    def outcome(self) -> ConnectionOutcome | None:
        """Return terminal ConnectionOutcome if closed, or None."""
        return self._outcome

    @property
    def is_closed(self) -> bool:
        """Return True if connection has reached a terminal lifecycle state."""
        return self._state.is_terminal

    @property
    def fallback_attempts(self) -> int:
        """Return the number of fallback close attempts executed by this supervisor."""
        return self._fallback_attempts

    def _transition_to(self, target: ConnectionState) -> None:
        """Atomically transition state or raise InvalidConnectionTransitionError."""
        allowed = _ALLOWED_TRANSITIONS.get(self._state, frozenset())
        if target not in allowed:
            raise InvalidConnectionTransitionError(
                self._state.value,
                target.value,
                f"Cannot transition connection from {self._state.value} to {target.value}",
            )
        self._state = target

    async def admit(self) -> bool:
        """Transition from NEW through AUTHENTICATING and attempt atomic admission.

        Returns True if capacity was granted and state is ADMITTED.
        Returns False if admission capacity was exhausted, transitioning to REJECTED.
        """
        async with self._lock:
            if self._state != ConnectionState.NEW:
                raise InvalidConnectionTransitionError(
                    self._state.value,
                    ConnectionState.AUTHENTICATING.value,
                    f"admit() can only be called from NEW state, currently in {self._state.value}",
                )

            self._transition_to(ConnectionState.AUTHENTICATING)

            try:
                async with asyncio.timeout(self._policy.startup_timeout):
                    lease = await self._controller.acquire(self._context)
            except AdmissionError:
                self._transition_to(ConnectionState.REJECTED)
                self._outcome = ConnectionOutcome(
                    state=ConnectionState.REJECTED,
                    reason=TerminalReason.ADMISSION_REJECTED,
                )
                self._closed_event.set()
                return False
            except TimeoutError:
                self._transition_to(ConnectionState.REJECTED)
                self._outcome = ConnectionOutcome(
                    state=ConnectionState.REJECTED,
                    reason=TerminalReason.TIMEOUT,
                )
                self._closed_event.set()
                return False
            except (KeyboardInterrupt, SystemExit):
                raise
            except asyncio.CancelledError:
                self._transition_to(ConnectionState.REJECTED)
                self._outcome = ConnectionOutcome(
                    state=ConnectionState.REJECTED,
                    reason=TerminalReason.CANCELLED,
                )
                self._closed_event.set()
                raise
            except Exception:
                self._transition_to(ConnectionState.REJECTED)
                self._outcome = ConnectionOutcome(
                    state=ConnectionState.REJECTED,
                    reason=TerminalReason.STARTUP_FAILED,
                )
                self._closed_event.set()
                raise

            self._lease = lease
            self._transition_to(ConnectionState.ADMITTED)
            return True

    def register_resource(
        self,
        close_fn: Callable[[], Coroutine[Any, Any, None]],
    ) -> None:
        """Register an async cleanup callback owned by this connection lifecycle.

        Resources can only be registered on ADMITTED or ACTIVE connections.
        """
        if not callable(close_fn):
            raise TypeError("close_fn must be a callable returning a coroutine")
        if self._state not in (ConnectionState.ADMITTED, ConnectionState.ACTIVE):
            raise ConnectionLifecycleError(
                f"Cannot register resources on a connection in {self._state.value} state; "
                f"connection must be ADMITTED or ACTIVE"
            )
        self._resources.append(close_fn)

    async def start(
        self,
        *worker_factories: Callable[[], Coroutine[Any, Any, None]],
    ) -> None:
        """Start supervised worker tasks. State must be ADMITTED."""
        if not worker_factories:
            raise ValueError("At least one worker factory must be provided to start()")

        for idx, factory in enumerate(worker_factories):
            if not callable(factory):
                raise TypeError(f"Worker factory at index {idx} must be a callable")

        async with self._lock:
            if self._state != ConnectionState.ADMITTED:
                raise InvalidConnectionTransitionError(
                    self._state.value,
                    ConnectionState.ACTIVE.value,
                    f"start() requires ADMITTED state, currently in {self._state.value}",
                )

            coros: list[Coroutine[Any, Any, None]] = []
            for idx, factory in enumerate(worker_factories):
                try:
                    coro_obj: object = factory()
                except (KeyboardInterrupt, SystemExit):
                    for c in coros:
                        c.close()
                    raise
                except asyncio.CancelledError:
                    for c in coros:
                        c.close()
                    self._transition_to(ConnectionState.CLOSING)
                    self._cleanup_task = asyncio.create_task(
                        self._execute_cleanup(
                            target_terminal=ConnectionState.FAILED,
                            reason=TerminalReason.CANCELLED,
                            error_summary=None,
                        ),
                        name="gw-cleanup",
                    )
                    self._cleanup_task.add_done_callback(self._on_cleanup_task_done)
                    await asyncio.shield(self._cleanup_task)
                    raise
                except Exception as exc:
                    for c in coros:
                        c.close()
                    self._transition_to(ConnectionState.CLOSING)
                    self._cleanup_task = asyncio.create_task(
                        self._execute_cleanup(
                            target_terminal=ConnectionState.FAILED,
                            reason=TerminalReason.STARTUP_FAILED,
                            error_summary=None,
                        ),
                        name="gw-cleanup",
                    )
                    self._cleanup_task.add_done_callback(self._on_cleanup_task_done)
                    await asyncio.shield(self._cleanup_task)
                    raise ConnectionLifecycleError(
                        f"Worker factory at index {idx} failed during invocation"
                    ) from exc

                if not asyncio.iscoroutine(coro_obj):
                    for c in coros:
                        c.close()
                    self._transition_to(ConnectionState.CLOSING)
                    self._cleanup_task = asyncio.create_task(
                        self._execute_cleanup(
                            target_terminal=ConnectionState.FAILED,
                            reason=TerminalReason.STARTUP_FAILED,
                            error_summary=None,
                        ),
                        name="gw-cleanup",
                    )
                    self._cleanup_task.add_done_callback(self._on_cleanup_task_done)
                    await asyncio.shield(self._cleanup_task)
                    raise TypeError(
                        f"Worker factory at index {idx} must return a coroutine, got {type(coro_obj).__name__}"
                    )
                coros.append(coro_obj)

            self._transition_to(ConnectionState.ACTIVE)

            # Spawn supervised workers with synthetic task names and exception observation
            for idx, coro in enumerate(coros):
                task = asyncio.create_task(
                    self._run_worker(idx, coro),
                    name=f"gw-worker-{idx}",
                )
                task.add_done_callback(self._on_worker_task_done)
                self._worker_tasks.append(task)

    def _on_worker_task_done(self, task: asyncio.Task[None]) -> None:
        """Observe task completion to prevent unobserved task exception warnings."""
        if not task.cancelled():
            task.exception()

    async def _run_worker(
        self,
        worker_idx: int,
        coro: Coroutine[Any, Any, None],
    ) -> None:
        """Execute a worker coroutine, catching exceptions and coordinating sibling teardown."""
        exc_to_report: BaseException | None = None
        try:
            await coro
        except asyncio.CancelledError as cancel_exc:
            exc_to_report = cancel_exc
            raise
        except (KeyboardInterrupt, SystemExit):
            raise
        except Exception as exc:
            exc_to_report = exc
        finally:
            # If worker failed with unhandled Exception, trigger supervised closure
            if exc_to_report is not None and not isinstance(exc_to_report, asyncio.CancelledError):
                trigger_task = asyncio.create_task(
                    self.close(
                        reason=TerminalReason.WORKER_FAILED,
                    ),
                    name="gw-autoclose",
                )
                self._trigger_close_tasks.add(trigger_task)
                trigger_task.add_done_callback(self._on_auto_close_done)
            elif all(t.done() for t in self._worker_tasks if t is not asyncio.current_task()):
                # All workers completed without error
                if self._state == ConnectionState.ACTIVE:
                    trigger_task = asyncio.create_task(
                        self.close(reason=TerminalReason.NORMAL),
                        name="gw-autoclose",
                    )
                    self._trigger_close_tasks.add(trigger_task)
                    trigger_task.add_done_callback(self._on_auto_close_done)

    def _on_auto_close_done(self, task: asyncio.Task[Any]) -> None:
        """Handle completion of auto-close tasks, observing cancellations and failures."""
        self._trigger_close_tasks.discard(task)

        is_failed = False
        if task.cancelled():
            is_failed = True
        else:
            exc = task.exception()
            if exc is not None:
                is_failed = True

        if not is_failed:
            return

        # If supervisor is already in a terminal state, no further action is needed
        if self._state.is_terminal:
            return

        # If an internal cleanup task is already active, let it supervise teardown
        if self._cleanup_task is not None and not self._cleanup_task.done():
            return

        # Cleanup has not started and supervisor is not terminal: execute bounded fallback
        if self._fallback_attempts < self.MAX_FALLBACK_ATTEMPTS:
            self._fallback_attempts += 1
            fallback_task = asyncio.create_task(
                self.close(
                    reason=TerminalReason.CLEANUP_FAILED,
                ),
                name="gw-fallback",
            )
            self._trigger_close_tasks.add(fallback_task)
            fallback_task.add_done_callback(self._on_fallback_close_done)
        else:
            self._record_fallback_exhausted_failure()

    _on_trigger_close_done = _on_auto_close_done

    def _on_fallback_close_done(self, task: asyncio.Task[Any]) -> None:
        """Handle completion of bounded fallback close task, ensuring termination."""
        self._trigger_close_tasks.discard(task)

        if not task.cancelled():
            task.exception()

        if self._state.is_terminal:
            return

        if self._cleanup_task is not None and not self._cleanup_task.done():
            return

        self._record_fallback_exhausted_failure()

    def _record_fallback_exhausted_failure(self) -> None:
        """Record terminal FAILED state when all fallback close attempts are exhausted."""
        if not self._state.is_terminal:
            if self._state in (ConnectionState.NEW, ConnectionState.AUTHENTICATING):
                self._transition_to(ConnectionState.REJECTED)
                self._outcome = ConnectionOutcome(
                    state=ConnectionState.REJECTED,
                    reason=TerminalReason.CLEANUP_FAILED,
                )
            else:
                if self._state != ConnectionState.CLOSING:
                    self._transition_to(ConnectionState.CLOSING)
                self._transition_to(ConnectionState.FAILED)
                self._outcome = ConnectionOutcome(
                    state=ConnectionState.FAILED,
                    reason=TerminalReason.CLEANUP_FAILED,
                )
            self._closed_event.set()

    def _on_cleanup_task_done(self, task: asyncio.Task[None]) -> None:
        """Handle completion of cleanup task, ensuring exceptions are observed."""
        if not task.cancelled():
            task.exception()

    async def close(
        self,
        *,
        reason: TerminalReason = TerminalReason.NORMAL,
        error_summary: str | None = None,
    ) -> ConnectionOutcome:
        """Idempotently close the connection lifecycle, stop workers, and release capacity."""
        cleanup_task: asyncio.Task[None] | None = None
        async with self._lock:
            if self._state.is_terminal:
                assert self._outcome is not None
                # If a prior cleanup was cancelled leaving the lease active, retry release now
                if self._lease is not None and self._lease.is_active:
                    try:
                        await self._lease.release()
                    except Exception:
                        pass
                return self._outcome

            if self._state == ConnectionState.CLOSING:
                assert self._cleanup_task is not None
                cleanup_task = self._cleanup_task

            elif self._state in (ConnectionState.NEW, ConnectionState.AUTHENTICATING):
                self._transition_to(ConnectionState.REJECTED)
                self._outcome = ConnectionOutcome(
                    state=ConnectionState.REJECTED,
                    reason=reason if reason != TerminalReason.NORMAL else TerminalReason.CANCELLED,
                )
                self._closed_event.set()
                return self._outcome

            else:
                # Determine terminal state based on reason
                if reason in (
                    TerminalReason.WORKER_FAILED,
                    TerminalReason.STARTUP_FAILED,
                    TerminalReason.TIMEOUT,
                    TerminalReason.CLEANUP_FAILED,
                ):
                    target_terminal = ConnectionState.FAILED
                else:
                    target_terminal = ConnectionState.CLOSED

                self._transition_to(ConnectionState.CLOSING)

                # Spawn dedicated cleanup task with synthetic safe task name
                self._cleanup_task = asyncio.create_task(
                    self._execute_cleanup(
                        target_terminal=target_terminal,
                        reason=reason,
                        error_summary=error_summary,
                    ),
                    name="gw-cleanup",
                )
                self._cleanup_task.add_done_callback(self._on_cleanup_task_done)
                cleanup_task = self._cleanup_task

        assert cleanup_task is not None
        await asyncio.shield(cleanup_task)
        assert self._outcome is not None
        return self._outcome

    async def _execute_cleanup(
        self,
        *,
        target_terminal: ConnectionState,
        reason: TerminalReason,
        error_summary: str | None,
    ) -> None:
        """Internal cleanup execution running within a protected task."""
        loop = asyncio.get_running_loop()
        deadline = loop.time() + self._policy.cleanup_timeout
        actual_terminal = target_terminal
        actual_reason = reason

        try:
            # 1. Cancel all active worker tasks
            for task in self._worker_tasks:
                if not task.done():
                    task.cancel()

            # 2. Await workers completion within monotonic cleanup deadline
            if self._worker_tasks:
                try:
                    remaining = max(0.0, deadline - loop.time())
                    async with asyncio.timeout(remaining):
                        await asyncio.gather(*self._worker_tasks, return_exceptions=True)
                except TimeoutError:
                    actual_terminal = ConnectionState.FAILED
                    actual_reason = TerminalReason.TIMEOUT

            # 3. Close registered synthetic async resources (LIFO order)
            for resource_close in reversed(self._resources):
                remaining = deadline - loop.time()
                if remaining > 0:
                    try:
                        async with asyncio.timeout(remaining):
                            await resource_close()
                    except Exception:
                        actual_terminal = ConnectionState.FAILED
                        actual_reason = TerminalReason.CLEANUP_FAILED
                else:
                    actual_terminal = ConnectionState.FAILED
                    actual_reason = TerminalReason.TIMEOUT

            # 4. Release admission lease (idempotent, retry-safe with dedicated bounded window)
            if self._lease is not None:
                lease_window = min(1.0, max(0.1, self._policy.cleanup_timeout))
                for _ in range(3):
                    if not self._lease.is_active:
                        break
                    try:
                        async with asyncio.timeout(lease_window):
                            await self._lease.release()
                        break
                    except Exception:
                        pass
                if self._lease.is_active:
                    actual_terminal = ConnectionState.FAILED
                    actual_reason = TerminalReason.CLEANUP_FAILED

            # 5. Record terminal state and outcome
            self._transition_to(actual_terminal)
            self._outcome = ConnectionOutcome(
                state=self._state,
                reason=actual_reason,
            )
        except (KeyboardInterrupt, SystemExit):
            raise
        except asyncio.CancelledError:
            # Direct cancellation of internal cleanup task: teardown was aborted!
            # Never record a successful CLOSED outcome when cleanup was aborted mid-flight.
            self._state = ConnectionState.FAILED
            self._outcome = ConnectionOutcome(
                state=ConnectionState.FAILED,
                reason=TerminalReason.CLEANUP_FAILED,
            )
            raise
        except Exception:
            # Emergency fail-safe
            self._state = ConnectionState.FAILED
            self._outcome = ConnectionOutcome(
                state=ConnectionState.FAILED,
                reason=TerminalReason.CLEANUP_FAILED,
            )
        finally:
            self._closed_event.set()

    async def wait(self) -> ConnectionOutcome:
        """Wait until connection reaches a terminal lifecycle state."""
        await self._closed_event.wait()
        assert self._outcome is not None
        return self._outcome

    async def wait_closed(self) -> ConnectionOutcome:
        """Wait until connection reaches a terminal lifecycle state (alias for wait())."""
        return await self.wait()

    async def __aenter__(self) -> ConnectionSupervisor:
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: Any,
    ) -> None:
        if exc_val is not None:
            reason = (
                TerminalReason.CANCELLED
                if isinstance(exc_val, asyncio.CancelledError)
                else TerminalReason.WORKER_FAILED
            )
            await self.close(reason=reason)
        else:
            await self.close(reason=TerminalReason.NORMAL)

    def __repr__(self) -> str:
        return f"ConnectionSupervisor(state={self._state.value!r})"


__all__ = [
    "ConnectionOutcome",
    "ConnectionState",
    "ConnectionSupervisor",
    "LifecyclePolicy",
    "TerminalReason",
]
