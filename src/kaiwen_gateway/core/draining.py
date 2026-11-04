"""Instance-scoped connection draining and graceful shutdown coordinator for Kaiwen Gateway.

This module provides:
- InstanceDrainState: Finite state machine governing gateway instance admission and shutdown.
- DrainPolicy: Bounded timing configuration for graceful drain and force-close deadlines.
- DrainReport: Immutable, sanitized diagnostic summary of instance drain execution.
- InstanceDrainCoordinator: Instance-scoped coordinator managing connection admission gates,
  graceful drain timeouts, and force-close phases.

Architecture and Concurrency Invariants:
- Instance Scoping: Each coordinator is strictly bound to an individual Gateway instance and its
  AdmissionController. Zero module-level mutable registries, singletons, or global call state exist.
- Admission/Drain Linearization: All managed connection admission requests and drain transitions
  are linearized through an instance-local lock. Once draining commences, subsequent admission
  requests are rejected atomically before capacity allocation or supervisor creation.
- Immediate Supervisor Ownership: Connections are registered into the coordinator's managed inventory
  prior to capacity acquisition. In the event of admission cancellation or failure, the coordinator
  shields rollback teardown to guarantee zero unmanaged orphan capacity leases.
- Single-Flight Drain: Concurrent or repeated `drain()` invocations share the single underlying
  supervisory drain task, returning an identical immutable DrainReport.
- Cancellation Shielding: External cancellation of a caller awaiting `drain()` does not cancel
  or interrupt the internal instance shutdown supervisor.
- Bounded Execution: All phases operate under monotonic deadlines (`grace_period` and
  `force_close_timeout`). Non-cooperative worker tasks or unreleased leases are bounded and
  reported as unresolved rather than causing indefinite hangs.
- Exact Mathematical Accounting: DrainReport guarantees that
  ``initial_connections == graceful_completions + force_closed + unresolved_connections``.
- Clean Task Lifecycle: Lingering observer tracker tasks are cancelled and awaited upon shutdown,
  preventing orphan background tasks or unretrieved exception warnings.
- Diagnostic Privacy: String representations of policies, coordinators, and reports expose only
  safe aggregate metrics, never raw connection, tenant, or call identifiers.
"""

from __future__ import annotations

import asyncio
import math
from dataclasses import dataclass
from enum import StrEnum
from typing import Final

from kaiwen_gateway.core.admission import AdmissionController
from kaiwen_gateway.core.exceptions import (
    CoreConfigurationError,
    DuplicateConnectionAdmissionError,
    InstanceDrainingError,
    InvalidDrainTransitionError,
)
from kaiwen_gateway.core.identity import VerifiedConnectionContext
from kaiwen_gateway.core.lifecycle import (
    ConnectionState,
    ConnectionSupervisor,
    LifecyclePolicy,
    TerminalReason,
)


class InstanceDrainState(StrEnum):
    """Guarded states representing the lifecycle of an instance drain coordinator."""

    ACCEPTING = "ACCEPTING"
    DRAINING = "DRAINING"
    STOPPED = "STOPPED"

    @property
    def is_accepting(self) -> bool:
        """Return True if this instance is actively accepting new managed connections."""
        return self == InstanceDrainState.ACCEPTING

    @property
    def is_draining(self) -> bool:
        """Return True if this instance is actively draining existing connections."""
        return self == InstanceDrainState.DRAINING

    @property
    def is_stopped(self) -> bool:
        """Return True if this instance has stopped and concluded draining."""
        return self == InstanceDrainState.STOPPED

    @property
    def is_terminal(self) -> bool:
        """Return True if this state is terminal (no further transitions permitted)."""
        return self == InstanceDrainState.STOPPED


_ALLOWED_DRAIN_TRANSITIONS: Final[dict[InstanceDrainState, frozenset[InstanceDrainState]]] = {
    InstanceDrainState.ACCEPTING: frozenset(
        {
            InstanceDrainState.DRAINING,
            InstanceDrainState.STOPPED,
        }
    ),
    InstanceDrainState.DRAINING: frozenset(
        {
            InstanceDrainState.STOPPED,
        }
    ),
    InstanceDrainState.STOPPED: frozenset(),
}


@dataclass(frozen=True, slots=True)
class DrainPolicy:
    """Configurable, validated timeout policy governing instance draining phases."""

    grace_period: float = 10.0
    force_close_timeout: float = 5.0

    def __post_init__(self) -> None:
        """Validate timeout constraints."""
        if isinstance(self.grace_period, bool) or not isinstance(self.grace_period, (int, float)):
            raise CoreConfigurationError(
                "grace_period must be a float or int", field_name="grace_period"
            )
        if math.isnan(self.grace_period) or math.isinf(self.grace_period):
            raise CoreConfigurationError(
                "grace_period cannot be NaN or infinity", field_name="grace_period"
            )
        if self.grace_period < 0.0:
            raise CoreConfigurationError(
                "grace_period cannot be negative", field_name="grace_period"
            )

        if isinstance(self.force_close_timeout, bool) or not isinstance(
            self.force_close_timeout, (int, float)
        ):
            raise CoreConfigurationError(
                "force_close_timeout must be a float or int", field_name="force_close_timeout"
            )
        if math.isnan(self.force_close_timeout) or math.isinf(self.force_close_timeout):
            raise CoreConfigurationError(
                "force_close_timeout cannot be NaN or infinity", field_name="force_close_timeout"
            )
        if self.force_close_timeout <= 0.0:
            raise CoreConfigurationError(
                "force_close_timeout must be strictly positive", field_name="force_close_timeout"
            )

    def __repr__(self) -> str:
        return f"DrainPolicy(grace_period={self.grace_period}, force_close_timeout={self.force_close_timeout})"


@dataclass(frozen=True, slots=True)
class DrainReport:
    """Immutable, sanitized report summarizing the outcome of an instance drain operation.

    Semantic Invariants:
    - ``STOPPED`` indicates that the coordinator has closed new admissions and completed
      its orchestrated drain and force-close phases under the configured policy.
    - ``STOPPED`` does NOT imply that all background tasks have stopped or all capacity leases
      have been released; unresolved resources are explicitly quantified in
      ``unresolved_connections`` and ``active_leases_remaining``.
    - Mathematical consistency:
      ``initial_connections == graceful_completions + force_closed + unresolved_connections``
      where each initial connection belongs to exactly one mutually exclusive outcome cohort.
    - ``failed_outcomes`` is an orthogonal quality metric counting connections that reached
      the terminal FAILED state.
    - Diagnostic privacy: String representations expose only safe aggregated numeric counts,
      never raw connection, tenant, or call identifiers.
    """

    initial_connections: int
    graceful_completions: int
    force_closed: int
    failed_outcomes: int
    unresolved_connections: int
    active_leases_remaining: int
    deadline_exceeded: bool
    final_state: InstanceDrainState
    duration_seconds: float

    def __repr__(self) -> str:
        return (
            f"DrainReport(final_state='{self.final_state.value}', "
            f"initial={self.initial_connections}, "
            f"graceful={self.graceful_completions}, "
            f"force_closed={self.force_closed}, "
            f"failed={self.failed_outcomes}, "
            f"unresolved={self.unresolved_connections}, "
            f"active_leases={self.active_leases_remaining}, "
            f"deadline_exceeded={self.deadline_exceeded})"
        )

    def __str__(self) -> str:
        return self.__repr__()


class InstanceDrainCoordinator:
    """Instance-scoped coordinator managing connection admission, graceful draining, and shutdown.

    Invariants:
    - Thread-free, process-local coordination tied to a single event loop.
    - Zero global state or cross-instance shared registries.
    - Linearized admission gate: Once `drain()` begins, no new connections are admitted.
    - Idempotent and single-flight: Multiple concurrent `drain()` calls await the same task.
    - Monotonically bounded: Closes unclosed connections if grace period expires.
    """

    __slots__ = (
        "_controller",
        "_policy",
        "_supervisor_policy",
        "_state",
        "_lock",
        "_supervisors",
        "_drain_task",
        "_report",
        "_stopped_event",
        "_tracker_tasks",
    )

    def __init__(
        self,
        *,
        controller: AdmissionController,
        policy: DrainPolicy | None = None,
        supervisor_policy: LifecyclePolicy | None = None,
    ) -> None:
        if not isinstance(controller, AdmissionController):
            raise TypeError(
                f"controller must be an AdmissionController instance, got {type(controller).__name__}"
            )
        if policy is not None and not isinstance(policy, DrainPolicy):
            raise TypeError(f"policy must be a DrainPolicy instance, got {type(policy).__name__}")
        if supervisor_policy is not None and not isinstance(supervisor_policy, LifecyclePolicy):
            raise TypeError(
                f"supervisor_policy must be a LifecyclePolicy instance, got {type(supervisor_policy).__name__}"
            )

        self._controller: AdmissionController = controller
        self._policy: DrainPolicy = policy if policy is not None else DrainPolicy()
        self._supervisor_policy: LifecyclePolicy = (
            supervisor_policy if supervisor_policy is not None else LifecyclePolicy()
        )
        self._state: InstanceDrainState = InstanceDrainState.ACCEPTING
        self._lock: asyncio.Lock = asyncio.Lock()
        self._supervisors: dict[str, ConnectionSupervisor] = {}
        self._drain_task: asyncio.Task[DrainReport] | None = None
        self._report: DrainReport | None = None
        self._stopped_event: asyncio.Event = asyncio.Event()
        self._tracker_tasks: set[asyncio.Task[None]] = set()

    @property
    def state(self) -> InstanceDrainState:
        """Return current drain state."""
        return self._state

    @property
    def policy(self) -> DrainPolicy:
        """Return configured drain policy."""
        return self._policy

    @property
    def controller(self) -> AdmissionController:
        """Return bound AdmissionController."""
        return self._controller

    @property
    def is_accepting(self) -> bool:
        """Return True if this instance is actively accepting new managed connections."""
        return self._state == InstanceDrainState.ACCEPTING

    @property
    def is_draining(self) -> bool:
        """Return True if this instance is in the process of draining connections."""
        return self._state == InstanceDrainState.DRAINING

    @property
    def is_stopped(self) -> bool:
        """Return True if this instance has concluded draining."""
        return self._state == InstanceDrainState.STOPPED

    @property
    def active_connections_count(self) -> int:
        """Return count of currently managed unclosed connections."""
        return sum(1 for s in self._supervisors.values() if not s.is_closed)

    @property
    def total_managed_count(self) -> int:
        """Return total count of currently tracked connections."""
        return len(self._supervisors)

    @property
    def report(self) -> DrainReport | None:
        """Return final DrainReport if stopped, or None."""
        return self._report

    def _transition_to(self, target: InstanceDrainState) -> None:
        """Atomically transition state or raise InvalidDrainTransitionError."""
        allowed = _ALLOWED_DRAIN_TRANSITIONS.get(self._state, frozenset())
        if target not in allowed:
            raise InvalidDrainTransitionError(
                self._state.value,
                target.value,
                f"Cannot transition instance drain state from {self._state.value} to {target.value}",
            )
        self._state = target

    async def admit_connection(
        self,
        context: VerifiedConnectionContext,
        *,
        supervisor_policy: LifecyclePolicy | None = None,
    ) -> ConnectionSupervisor:
        """Admit a new managed connection into this gateway instance under supervisor ownership.

        If the instance is in DRAINING or STOPPED, raises InstanceDrainingError without allocating capacity.
        If the connection ID is already managed by this coordinator, raises DuplicateConnectionAdmissionError.
        """
        if not isinstance(context, VerifiedConnectionContext):
            raise TypeError(
                f"context must be a VerifiedConnectionContext instance, got {type(context).__name__}"
            )

        async with self._lock:
            if self._state != InstanceDrainState.ACCEPTING:
                raise InstanceDrainingError(
                    f"Cannot admit new connection: instance is in {self._state.value} state"
                )

            if context.connection_id in self._supervisors:
                raise DuplicateConnectionAdmissionError(context.connection_id)

            supervisor = ConnectionSupervisor(
                context=context,
                controller=self._controller,
                policy=supervisor_policy or self._supervisor_policy,
            )
            # Register immediately into coordinator inventory so connection is tracked from inception
            self._supervisors[context.connection_id] = supervisor

            try:
                admitted = await supervisor.admit()
                if not admitted:
                    # Clean capacity rejection: prune from inventory
                    self._supervisors.pop(context.connection_id, None)
                    return supervisor

                tracker = asyncio.create_task(
                    self._track_supervisor_completion(supervisor),
                    name="gw-drain-track",
                )
                self._tracker_tasks.add(tracker)
                tracker.add_done_callback(self._on_tracker_done)
                return supervisor
            except (KeyboardInterrupt, SystemExit):
                self._supervisors.pop(context.connection_id, None)
                raise
            except BaseException:
                # Admission cancelled or failed: shield rollback and wait for completion
                close_task = asyncio.create_task(supervisor.close(reason=TerminalReason.CANCELLED))
                try:
                    await asyncio.shield(close_task)
                except asyncio.CancelledError:
                    await asyncio.gather(close_task, return_exceptions=True)
                except Exception:
                    pass
                finally:
                    if supervisor.lease is None or not supervisor.lease.is_active:
                        self._supervisors.pop(context.connection_id, None)
                    # If rollback failed and lease is STILL active, KEEP in self._supervisors!
                raise

    def _on_tracker_done(self, task: asyncio.Task[None]) -> None:
        """Handle completion of an observer tracker task, ensuring exceptions are observed."""
        self._tracker_tasks.discard(task)
        if not task.cancelled():
            task.exception()

    async def _track_supervisor_completion(self, supervisor: ConnectionSupervisor) -> None:
        """Background tracker observing supervisor termination to prune cleanly closed connections."""
        try:
            await supervisor.wait_closed()
        except asyncio.CancelledError:
            pass
        finally:
            async with self._lock:
                # If still ACCEPTING, and connection closed cleanly without an active lease,
                # prune it to prevent unbounded dictionary growth.
                if self._state == InstanceDrainState.ACCEPTING:
                    if supervisor.state == ConnectionState.CLOSED and (
                        supervisor.lease is None or not supervisor.lease.is_active
                    ):
                        self._supervisors.pop(supervisor.connection_id, None)

    async def drain(self, policy: DrainPolicy | None = None) -> DrainReport:
        """Initiate graceful draining and bounded force-close of all managed connections.

        Idempotent and single-flight: concurrent or repeated calls share the same underlying
        drain supervisor task and return the identical DrainReport upon conclusion.
        """
        drain_task: asyncio.Task[DrainReport] | None = None
        async with self._lock:
            if self._state == InstanceDrainState.STOPPED:
                assert self._report is not None
                return self._report

            if self._state == InstanceDrainState.DRAINING:
                assert self._drain_task is not None
                drain_task = self._drain_task
            else:
                self._transition_to(InstanceDrainState.DRAINING)
                active_policy = policy if policy is not None else self._policy
                self._drain_task = asyncio.create_task(
                    self._execute_drain(active_policy),
                    name="gw-instance-drain",
                )
                drain_task = self._drain_task

        assert drain_task is not None
        return await asyncio.shield(drain_task)

    async def _execute_drain(self, policy: DrainPolicy) -> DrainReport:
        """Internal execution supervisor orchestrating the multi-phase bounded drain."""
        loop = asyncio.get_running_loop()
        start_time = loop.time()

        # 1. Snapshot initial managed supervisors
        async with self._lock:
            initial_supervisors = list(self._supervisors.values())

        initial_count = len(initial_supervisors)
        deadline_exceeded = False

        # Phase 1: Graceful completion wait for currently unclosed supervisors
        pending_supervisors = [s for s in initial_supervisors if not s.is_closed]
        graceful_supervisors: set[str] = {
            s.connection_id
            for s in initial_supervisors
            if s.is_closed and (s.lease is None or not s.lease.is_active)
        }

        if pending_supervisors and policy.grace_period > 0:
            grace_deadline = start_time + policy.grace_period
            wait_tasks = [
                asyncio.create_task(s.wait_closed(), name="gw-drain-wait")
                for s in pending_supervisors
            ]
            try:
                remaining_grace = max(0.0, grace_deadline - loop.time())
                async with asyncio.timeout(remaining_grace):
                    await asyncio.gather(*wait_tasks, return_exceptions=True)
            except TimeoutError:
                deadline_exceeded = True
            finally:
                for wt in wait_tasks:
                    wt.cancel()

        # Connections that completed closed during Phase 1 without active lease
        for s in pending_supervisors:
            if s.is_closed and (s.lease is None or not s.lease.is_active):
                graceful_supervisors.add(s.connection_id)

        graceful_completions = len(graceful_supervisors)
        unclosed_candidates = [
            s for s in initial_supervisors if s.connection_id not in graceful_supervisors
        ]

        # Phase 2: Force-close phase for remaining unfinalized supervisors
        if unclosed_candidates:
            force_close_deadline = loop.time() + policy.force_close_timeout
            close_tasks = [
                asyncio.create_task(
                    s.close(reason=TerminalReason.CLEANUP_FAILED), name="gw-drain-forceclose"
                )
                for s in unclosed_candidates
                if not s.is_closed
            ]
            if close_tasks:
                try:
                    remaining_force = max(0.0, force_close_deadline - loop.time())
                    async with asyncio.timeout(remaining_force):
                        await asyncio.gather(*close_tasks, return_exceptions=True)
                except TimeoutError:
                    deadline_exceeded = True
                finally:
                    for ct in close_tasks:
                        ct.cancel()

        # Phase 3: Final evaluation ensuring exact mathematical partition:
        # initial_connections == graceful_completions + force_closed + unresolved_connections
        force_closed = 0
        unresolved = 0
        failed_outcomes = 0
        active_leases_remaining = 0

        for s in initial_supervisors:
            if s.state == ConnectionState.FAILED:
                failed_outcomes += 1

            if s.lease is not None and s.lease.is_active:
                active_leases_remaining += 1

            if s.connection_id in graceful_supervisors:
                continue

            # Candidate must either be successfully force-closed (closed and no active lease)
            # or counted as unresolved (not closed or lease still active).
            if s.is_closed and (s.lease is None or not s.lease.is_active):
                force_closed += 1
            else:
                unresolved += 1

        end_time = loop.time()
        duration = max(0.0, end_time - start_time)

        report = DrainReport(
            initial_connections=initial_count,
            graceful_completions=graceful_completions,
            force_closed=force_closed,
            failed_outcomes=failed_outcomes,
            unresolved_connections=unresolved,
            active_leases_remaining=active_leases_remaining,
            deadline_exceeded=deadline_exceeded,
            final_state=InstanceDrainState.STOPPED,
            duration_seconds=duration,
        )

        async with self._lock:
            self._transition_to(InstanceDrainState.STOPPED)
            self._report = report
            self._stopped_event.set()
            trackers_to_cancel = list(self._tracker_tasks)
            for tracker in trackers_to_cancel:
                tracker.cancel()

        # Await completion of all cancelled observer tasks so zero pending tasks outlive drain
        if trackers_to_cancel:
            await asyncio.gather(*trackers_to_cancel, return_exceptions=True)

        return report

    async def wait_stopped(self) -> DrainReport:
        """Wait until this instance drain coordinator reaches the STOPPED state."""
        await self._stopped_event.wait()
        assert self._report is not None
        return self._report

    def __repr__(self) -> str:
        return f"InstanceDrainCoordinator(state='{self._state.value}', managed={len(self._supervisors)})"

    def __str__(self) -> str:
        return self.__repr__()


__all__ = [
    "DrainPolicy",
    "DrainReport",
    "InstanceDrainCoordinator",
    "InstanceDrainState",
]
