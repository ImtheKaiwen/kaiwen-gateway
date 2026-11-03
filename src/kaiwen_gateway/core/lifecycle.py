"""Framework-independent, per-connection state machine and supervised lifecycle contracts.

This module enforces FR-CONN-004, FR-CONN-006, FR-CONN-007, NFR-REL-001, and ENG-004:
- Explicit connection state machine with a strictly validated, guarded transition matrix.
- Per-connection task supervisor managing worker coroutine lifecycles and cancellation propagation.
- Deterministic, multi-step cleanup order ensuring capacity lease release under normal exits,
  unhandled exceptions, timeouts, and task cancellations.
- Stale callback isolation via explicit object ownership.
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
import re
import weakref
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


def _sanitize_error_summary(
    summary: str | None,
    reason: TerminalReason,
    *,
    context: VerifiedConnectionContext | None = None,
) -> str:
    """Sanitize diagnostic summary to guarantee zero credential or identifier leakage."""
    default_summary = _SAFE_REASON_SUMMARIES.get(reason, "Connection terminated")
    if summary is None or not summary.strip():
        return default_summary

    sanitized = summary.strip()
    if context is not None:
        sanitized = sanitized.replace(context.connection_id, "[REDACTED_CONN]")
        sanitized = sanitized.replace(context.tenant_id, "[REDACTED_TENANT]")
        sanitized = sanitized.replace(context.call_id, "[REDACTED_CALL]")
        if context.provider_call_ref is not None:
            sanitized = sanitized.replace(context.provider_call_ref, "[REDACTED_REF]")

    # Redact common sensitive markers like SECRET_MARKER_123, passwords, tokens, API keys
    sanitized = re.sub(
        r"(?i)(secret\w*|bearer\s+\w+|api[_-]?key\w*|token\w*)",
        "[REDACTED]",
        sanitized,
    )
    return sanitized


@dataclass(frozen=True, slots=True)
class ConnectionOutcome:
    """Immutable terminal outcome for a terminated connection lifecycle.

    Guarantees:
    - `state` is always a terminal state (CLOSED, FAILED, or REJECTED).
    - `error_summary` is sanitized upon assignment to prevent credential leakage.
    - Raw tokens, credentials, or PII are never echoed into outcome representations.
    """

    state: ConnectionState
    reason: TerminalReason
    error_summary: str | None = None

    def __post_init__(self) -> None:
        if self.error_summary is not None:
            sanitized = _sanitize_error_summary(self.error_summary, self.reason)
            object.__setattr__(self, "error_summary", sanitized)

    def __repr__(self) -> str:
        summary_repr = f", error_summary={self.error_summary!r}" if self.error_summary else ""
        return f"ConnectionOutcome(state={self.state.value!r}, reason={self.reason.value!r}{summary_repr})"

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


# Process-local registry tracking active supervisor ownership of leases
_CLAIMED_LEASES: weakref.WeakKeyDictionary[AdmissionLease, ConnectionSupervisor] = (
    weakref.WeakKeyDictionary()
)


class ConnectionSupervisor:
    """Per-connection async task supervisor and lifecycle state machine manager.

    Invariants (M2.4A):
    - Single lifecycle owner: Owns the connection's state transitions and worker tasks.
    - Guarded state machine: Transitions strictly adhere to `_ALLOWED_TRANSITIONS`.
    - Admission bound: Cannot enter `ACTIVE` without a valid `AdmissionLease`.
    - Sibling cancellation: If any worker task raises an unhandled error, sibling tasks
      are deterministically cancelled and awaited before cleanup.
    - Idempotent cleanup: Multiple concurrent or sequential `close()` calls execute cleanup
      exactly once without resource leaks.
    - Cancellation resilience: Cleanup runs within an owned supervisory task, ensuring
      that outer task cancellation does not prevent capacity lease release.
    - Zero orphan tasks: All spawned worker tasks are cancelled and awaited on termination.
    - Stale callback isolation: Callbacks reference this supervisor instance; closing does
      not affect subsequent supervisors even if the same connection ID is re-admitted.
    """

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
        "__weakref__",
    )

    def __init__(
        self,
        *,
        context: VerifiedConnectionContext,
        controller: AdmissionController,
        policy: LifecyclePolicy | None = None,
        lease: AdmissionLease | None = None,
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
        self._worker_factories: list[Callable[[], Coroutine[Any, Any, None]]] = []
        self._worker_tasks: list[asyncio.Task[None]] = []
        self._resources: list[Callable[[], Coroutine[Any, Any, None]]] = []
        self._lock: asyncio.Lock = asyncio.Lock()
        self._cleanup_task: asyncio.Task[None] | None = None
        self._trigger_close_tasks: set[asyncio.Task[Any]] = set()
        self._closed_event: asyncio.Event = asyncio.Event()

        if lease is not None:
            if not isinstance(lease, AdmissionLease):
                raise TypeError(
                    f"lease must be an AdmissionLease instance or None, got {type(lease).__name__}"
                )
            if lease.controller is not controller:
                raise ConnectionLifecycleError(
                    "Cannot initialize supervisor with a lease issued by a different AdmissionController"
                )
            if lease.connection_id != context.connection_id:
                raise ConnectionLifecycleError(
                    "Lease connection_id does not match the provided VerifiedConnectionContext"
                )
            # Strict trust boundary: lease context must match provided context completely
            if lease.context != context:
                raise ConnectionLifecycleError(
                    "Lease VerifiedConnectionContext does not match the provided context"
                )
            if not lease.is_active:
                raise ConnectionLifecycleError(
                    "Cannot initialize supervisor with an inactive or already released lease"
                )
            # Exclusive lease ownership check
            existing = _CLAIMED_LEASES.get(lease)
            if existing is not None and existing is not self and not existing.is_closed:
                raise ConnectionLifecycleError(
                    "AdmissionLease is already owned by an active ConnectionSupervisor"
                )
            _CLAIMED_LEASES[lease] = self
            self._lease: AdmissionLease | None = lease
            self._state: ConnectionState = ConnectionState.ADMITTED
        else:
            self._lease = None
            self._state = ConnectionState.NEW

    @classmethod
    def from_lease(
        cls,
        lease: AdmissionLease,
        *,
        policy: LifecyclePolicy | None = None,
    ) -> ConnectionSupervisor:
        """Construct a ConnectionSupervisor taking direct, exclusive ownership of an acquired lease."""
        if not isinstance(lease, AdmissionLease):
            raise TypeError(f"lease must be an AdmissionLease instance, got {type(lease).__name__}")
        return cls(
            context=lease.context,
            controller=lease.controller,
            policy=policy,
            lease=lease,
        )

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
                    error_summary="Admission capacity rejected",
                )
                self._closed_event.set()
                return False
            except TimeoutError:
                self._transition_to(ConnectionState.REJECTED)
                self._outcome = ConnectionOutcome(
                    state=ConnectionState.REJECTED,
                    reason=TerminalReason.TIMEOUT,
                    error_summary="Admission acquisition timed out",
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
                    error_summary="Admission acquisition was cancelled",
                )
                self._closed_event.set()
                raise
            except Exception:
                self._transition_to(ConnectionState.REJECTED)
                self._outcome = ConnectionOutcome(
                    state=ConnectionState.REJECTED,
                    reason=TerminalReason.STARTUP_FAILED,
                    error_summary="Admission acquisition failed with unexpected error",
                )
                self._closed_event.set()
                raise

            _CLAIMED_LEASES[lease] = self
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
                            error_summary="Startup cancelled during worker factory invocation",
                        ),
                        name=f"cleanup-{self._context.connection_id}",
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
                            error_summary="Worker factory failed during invocation",
                        ),
                        name=f"cleanup-{self._context.connection_id}",
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
                            error_summary=f"Worker factory at index {idx} must return a coroutine, got {type(coro_obj).__name__}",
                        ),
                        name=f"cleanup-{self._context.connection_id}",
                    )
                    self._cleanup_task.add_done_callback(self._on_cleanup_task_done)
                    await asyncio.shield(self._cleanup_task)
                    raise TypeError(
                        f"Worker factory at index {idx} must return a coroutine, got {type(coro_obj).__name__}"
                    )
                coros.append(coro_obj)

            self._transition_to(ConnectionState.ACTIVE)

            # Spawn supervised workers
            for idx, coro in enumerate(coros):
                task = asyncio.create_task(
                    self._run_worker(idx, coro),
                    name=f"worker-{idx}-{self._context.connection_id}",
                )
                self._worker_tasks.append(task)

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
                        error_summary="Worker coroutine raised unhandled exception",
                    )
                )
                self._trigger_close_tasks.add(trigger_task)
                trigger_task.add_done_callback(self._on_trigger_close_done)
            elif all(t.done() for t in self._worker_tasks if t is not asyncio.current_task()):
                # All workers completed without error
                if self._state == ConnectionState.ACTIVE:
                    trigger_task = asyncio.create_task(self.close(reason=TerminalReason.NORMAL))
                    self._trigger_close_tasks.add(trigger_task)
                    trigger_task.add_done_callback(self._on_trigger_close_done)

    def _on_trigger_close_done(self, task: asyncio.Task[Any]) -> None:
        """Handle completion of auto-close tasks, observing unhandled exceptions."""
        self._trigger_close_tasks.discard(task)
        if not task.cancelled():
            exc = task.exception()
            if exc is not None:
                # The auto-close task itself failed with an unhandled exception
                if not self._state.is_terminal:
                    self._state = ConnectionState.FAILED
                    self._outcome = ConnectionOutcome(
                        state=ConnectionState.FAILED,
                        reason=TerminalReason.CLEANUP_FAILED,
                        error_summary="Auto-close task failed with unhandled exception",
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
                    error_summary=_sanitize_error_summary(
                        error_summary,
                        TerminalReason.CANCELLED,
                        context=self._context,
                    ),
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

                # Spawn dedicated cleanup task to shield against outer task cancellation
                self._cleanup_task = asyncio.create_task(
                    self._execute_cleanup(
                        target_terminal=target_terminal,
                        reason=reason,
                        error_summary=error_summary,
                    ),
                    name=f"cleanup-{self._context.connection_id}",
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
        actual_summary = error_summary

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
                    actual_summary = "Worker termination exceeded cleanup deadline"

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
                        actual_summary = "Async resource cleanup failed or timed out"
                else:
                    actual_terminal = ConnectionState.FAILED
                    actual_reason = TerminalReason.TIMEOUT
                    actual_summary = "Cleanup deadline exceeded before resource teardown"

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
                    actual_summary = "Failed to release admission lease"

            # 5. Record terminal state and outcome
            self._transition_to(actual_terminal)

            self._outcome = ConnectionOutcome(
                state=self._state,
                reason=actual_reason,
                error_summary=_sanitize_error_summary(
                    actual_summary,
                    actual_reason,
                    context=self._context,
                ),
            )
        except (KeyboardInterrupt, SystemExit):
            raise
        except asyncio.CancelledError:
            # Direct cancellation of internal cleanup task: teardown was aborted!
            # Never record a successful CLOSED outcome when cleanup was aborted mid-flight.
            self._state = ConnectionState.FAILED
            if self._lease is not None and self._lease.is_active:
                self._outcome = ConnectionOutcome(
                    state=ConnectionState.FAILED,
                    reason=TerminalReason.CLEANUP_FAILED,
                    error_summary="Cleanup task cancelled before capacity lease was released",
                )
            else:
                self._outcome = ConnectionOutcome(
                    state=ConnectionState.FAILED,
                    reason=TerminalReason.CLEANUP_FAILED,
                    error_summary="Cleanup task cancelled before completing teardown",
                )
            raise
        except Exception:
            # Emergency fail-safe
            self._state = ConnectionState.FAILED
            self._outcome = ConnectionOutcome(
                state=ConnectionState.FAILED,
                reason=TerminalReason.CLEANUP_FAILED,
                error_summary="Unexpected error in cleanup supervisor",
            )
        finally:
            if self._lease is not None:
                _CLAIMED_LEASES.pop(self._lease, None)
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
            await self.close(reason=reason, error_summary="Context manager exited with exception")
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
