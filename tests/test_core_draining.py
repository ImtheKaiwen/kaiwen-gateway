"""Unit, concurrency, race condition, and invariant tests for M2.4B Instance Draining.

These tests verify:
- FR-CONN-004: Explicit connection and drain state tracking with strictly guarded transitions.
- FR-CONN-005: Atomic admission control gating before and during instance draining.
- FR-CONN-006: Proper resource cleanup on disconnect, failure, and bounded force-close.
- FR-CONN-009: Draining mode entry on shutdown, rejecting new calls while allowing existing to complete.
- FR-CORE-008: Explicit distinction between instance readiness/admission gating and external health.
- NFR-REL-003: Monotonically bounded graceful drain and force-close deadlines without indefinite awaits.
- AT-012: Graceful instance draining during active calls with accurate diagnostic reporting.
"""

from __future__ import annotations

import asyncio
from typing import Any

import pytest

from kaiwen_gateway.core import (
    AdmissionController,
    ConnectionState,
    ConnectionSupervisor,
    CoreConfig,
    CoreConfigurationError,
    DrainError,
    DrainPolicy,
    DrainReport,
    DuplicateConnectionAdmissionError,
    InstanceDrainCoordinator,
    InstanceDrainingError,
    InstanceDrainState,
    InvalidDrainTransitionError,
    LifecyclePolicy,
    TerminalReason,
    VerifiedConnectionContext,
)


def _make_context(
    connection_id: str = "conn_drain_1",
    call_id: str = "call_drain_1",
    tenant_id: str = "tenant_test",
) -> VerifiedConnectionContext:
    return VerifiedConnectionContext(
        provider_id="prov_test",
        tenant_id=tenant_id,
        connection_id=connection_id,
        call_id=call_id,
        allowed_scopes=("voice.stream",),
    )


def _make_controller(
    max_concurrent: int = 25, max_tenant: int | None = None
) -> AdmissionController:
    if max_tenant is None:
        max_tenant = min(10, max_concurrent)
    config = CoreConfig(
        max_concurrent_connections=max_concurrent,
        max_connections_per_tenant=max_tenant,
    )
    return AdmissionController(config)


# =============================================================================
# 1. DrainPolicy Validation Tests (Section 10 & 18.29)
# =============================================================================


def test_drain_policy_defaults_and_immutability() -> None:
    """Verify default timeout values, frozen slots, and repr."""
    policy = DrainPolicy()
    assert policy.grace_period == 10.0
    assert policy.force_close_timeout == 5.0
    assert repr(policy) == "DrainPolicy(grace_period=10.0, force_close_timeout=5.0)"

    with pytest.raises(AttributeError):
        policy.grace_period = 20.0  # type: ignore[misc]


def test_drain_policy_custom_valid_values() -> None:
    """Verify custom valid float and int timeouts."""
    p1 = DrainPolicy(grace_period=0.0, force_close_timeout=1.0)
    assert p1.grace_period == 0.0
    assert p1.force_close_timeout == 1.0

    p2 = DrainPolicy(grace_period=30, force_close_timeout=10)
    assert p2.grace_period == 30.0
    assert p2.force_close_timeout == 10.0


@pytest.mark.parametrize(
    ("grace", "force", "field"),
    [
        (True, 5.0, "grace_period"),
        ("invalid", 5.0, "grace_period"),
        (-1.0, 5.0, "grace_period"),
        (float("nan"), 5.0, "grace_period"),
        (float("inf"), 5.0, "grace_period"),
        (10.0, False, "force_close_timeout"),
        (10.0, "invalid", "force_close_timeout"),
        (10.0, 0.0, "force_close_timeout"),
        (10.0, -1.0, "force_close_timeout"),
        (10.0, float("nan"), "force_close_timeout"),
        (10.0, float("inf"), "force_close_timeout"),
    ],
)
def test_drain_policy_invalid_values_rejected(grace: Any, force: Any, field: str) -> None:
    """Verify boolean, non-numeric, NaN, infinite, negative, or zero force-close timeouts are rejected."""
    with pytest.raises(CoreConfigurationError) as exc_info:
        DrainPolicy(grace_period=grace, force_close_timeout=force)
    assert exc_info.value.field_name == field


# =============================================================================
# 2. InstanceDrainState Machine & Transitions (Section 5)
# =============================================================================


def test_instance_drain_state_properties() -> None:
    """Verify state enum values and boolean property queries."""
    s_acc = InstanceDrainState.ACCEPTING
    assert s_acc.is_accepting is True
    assert s_acc.is_draining is False
    assert s_acc.is_stopped is False
    assert s_acc.is_terminal is False

    s_drain = InstanceDrainState.DRAINING
    assert s_drain.is_accepting is False
    assert s_drain.is_draining is True
    assert s_drain.is_stopped is False
    assert s_drain.is_terminal is False

    s_stop = InstanceDrainState.STOPPED
    assert s_stop.is_accepting is False
    assert s_stop.is_draining is False
    assert s_stop.is_stopped is True
    assert s_stop.is_terminal is True


def _get_coord_state(coord: InstanceDrainCoordinator) -> InstanceDrainState:
    """Helper to query coordinator state across mutating transitions without mypy narrowing."""
    return coord.state


def test_invalid_drain_state_transitions() -> None:
    """Verify transition validation and rejection of illegal backwards or duplicate transitions."""
    coord = InstanceDrainCoordinator(controller=_make_controller())
    assert _get_coord_state(coord) == InstanceDrainState.ACCEPTING

    # Cannot transition from ACCEPTING back to ACCEPTING
    with pytest.raises(InvalidDrainTransitionError) as exc_info:
        coord._transition_to(InstanceDrainState.ACCEPTING)
    assert exc_info.value.current_state == "ACCEPTING"
    assert exc_info.value.target_state == "ACCEPTING"

    # Transition to STOPPED directly from ACCEPTING is allowed
    coord._transition_to(InstanceDrainState.STOPPED)
    assert _get_coord_state(coord) == InstanceDrainState.STOPPED

    # From STOPPED, no further transitions are permitted
    with pytest.raises(InvalidDrainTransitionError):
        coord._transition_to(InstanceDrainState.ACCEPTING)

    with pytest.raises(InvalidDrainTransitionError):
        coord._transition_to(InstanceDrainState.DRAINING)


# =============================================================================
# 3. Constructor Validation & Inspection APIs (Section 4, 14, 18.28-30)
# =============================================================================


def test_coordinator_constructor_validation() -> None:
    """Verify constructor accepts valid parameters and rejects invalid types."""
    ctrl = _make_controller()
    policy = DrainPolicy(grace_period=5.0)
    sup_policy = LifecyclePolicy(startup_timeout=2.0)

    coord = InstanceDrainCoordinator(controller=ctrl, policy=policy, supervisor_policy=sup_policy)
    assert coord.controller is ctrl
    assert coord.policy is policy
    assert coord.state == InstanceDrainState.ACCEPTING
    assert coord.is_accepting is True
    assert coord.is_draining is False
    assert coord.is_stopped is False
    assert coord.active_connections_count == 0
    assert coord.total_managed_count == 0
    assert coord.report is None

    # Invalid types
    with pytest.raises(TypeError, match="controller must be an AdmissionController"):
        InstanceDrainCoordinator(controller="invalid")  # type: ignore[arg-type]

    with pytest.raises(TypeError, match="policy must be a DrainPolicy"):
        InstanceDrainCoordinator(controller=ctrl, policy="invalid")  # type: ignore[arg-type]

    with pytest.raises(TypeError, match="supervisor_policy must be a LifecyclePolicy"):
        InstanceDrainCoordinator(controller=ctrl, supervisor_policy="invalid")  # type: ignore[arg-type]


# =============================================================================
# 4. Managed Admission Tests (Section 6, 7 & 18.1-7)
# =============================================================================


def test_managed_admission_in_accepting_state() -> None:
    """Item 1: Normal admission under ACCEPTING state creates and tracks supervisor."""

    async def _run() -> None:
        ctrl = _make_controller()
        coord = InstanceDrainCoordinator(controller=ctrl)
        ctx = _make_context()

        supervisor = await coord.admit_connection(ctx)
        assert supervisor.state == ConnectionState.ADMITTED
        assert supervisor.lease is not None
        assert supervisor.lease.is_active is True
        assert coord.active_connections_count == 1
        assert coord.total_managed_count == 1
        assert ctrl.snapshot().active_connections == 1

        await supervisor.close()
        # Cleanly closed supervisor is pruned when in ACCEPTING
        await asyncio.sleep(0.02)
        assert coord.active_connections_count == 0
        assert ctrl.snapshot().active_connections == 0

    asyncio.run(_run())


def test_managed_admission_rejected_when_draining_or_stopped() -> None:
    """Item 2 & 3: Admission attempts while in DRAINING or STOPPED raise InstanceDrainingError."""

    async def _run() -> None:
        ctrl = _make_controller()
        coord = InstanceDrainCoordinator(controller=ctrl)

        # 1. In DRAINING state
        coord._state = InstanceDrainState.DRAINING
        with pytest.raises(InstanceDrainingError, match="instance is in DRAINING state"):
            await coord.admit_connection(_make_context(connection_id="conn_draining"))
        assert ctrl.snapshot().active_connections == 0

        # 2. In STOPPED state
        coord._state = InstanceDrainState.STOPPED
        with pytest.raises(InstanceDrainingError, match="instance is in STOPPED state"):
            await coord.admit_connection(_make_context(connection_id="conn_stopped"))
        assert ctrl.snapshot().active_connections == 0

    asyncio.run(_run())


def test_managed_admission_invalid_context_type() -> None:
    """Verify TypeError when invalid context type is passed."""

    async def _run() -> None:
        coord = InstanceDrainCoordinator(controller=_make_controller())
        with pytest.raises(TypeError, match="context must be a VerifiedConnectionContext"):
            await coord.admit_connection("invalid")  # type: ignore[arg-type]

    asyncio.run(_run())


def test_managed_admission_duplicate_connection_id_rejected() -> None:
    """Item 5: Duplicate connection ID within the same coordinator is rejected immediately."""

    async def _run() -> None:
        coord = InstanceDrainCoordinator(controller=_make_controller())
        ctx = _make_context(connection_id="conn_dup")

        sup1 = await coord.admit_connection(ctx)
        assert sup1.state == ConnectionState.ADMITTED

        with pytest.raises(DuplicateConnectionAdmissionError):
            await coord.admit_connection(ctx)

        await sup1.close()

    asyncio.run(_run())


def test_managed_admission_capacity_exhausted_returns_rejected_supervisor() -> None:
    """Item 6 & 7: Controller quota exhaustion returns REJECTED supervisor without registering in coordinator."""

    async def _run() -> None:
        ctrl = _make_controller(max_concurrent=1)
        coord = InstanceDrainCoordinator(controller=ctrl)

        sup1 = await coord.admit_connection(_make_context(connection_id="conn_1"))
        assert sup1.state == ConnectionState.ADMITTED
        assert coord.active_connections_count == 1

        # Second connection exceeds global capacity (1/1)
        sup2 = await coord.admit_connection(_make_context(connection_id="conn_2"))
        assert sup2.state == ConnectionState.REJECTED
        assert sup2.outcome is not None
        assert sup2.outcome.reason == TerminalReason.ADMISSION_REJECTED
        assert coord.active_connections_count == 1
        assert "conn_2" not in coord._supervisors

        await sup1.close()

    asyncio.run(_run())


def test_concurrent_admission_and_drain_race() -> None:
    """Item 4: Critical Invariant — Linearized race between concurrent admission and drain."""

    async def _run() -> None:
        ctrl = _make_controller(max_concurrent=10)
        coord = InstanceDrainCoordinator(controller=ctrl, policy=DrainPolicy(grace_period=0.1))

        admission_results: list[str] = []

        async def worker_admit(idx: int) -> None:
            ctx = _make_context(connection_id=f"conn_race_{idx}")
            try:
                sup = await coord.admit_connection(ctx)
                if sup.state == ConnectionState.ADMITTED:
                    admission_results.append(f"admitted_{idx}")
                else:
                    admission_results.append(f"rejected_{idx}")
            except InstanceDrainingError:
                admission_results.append(f"draining_rejected_{idx}")

        # Spawn 10 concurrent admissions while simultaneously initiating drain
        tasks = [asyncio.create_task(worker_admit(i)) for i in range(10)]
        drain_task = asyncio.create_task(coord.drain())

        await asyncio.gather(*tasks)
        report = await drain_task

        assert report.final_state == InstanceDrainState.STOPPED
        # Every admission was either admitted and tracked, or rejected with InstanceDrainingError.
        # There is no orphaned in-between connection.
        for res in admission_results:
            assert res.startswith("admitted_") or res.startswith("draining_rejected_")

        # Capacity is completely 0 after drain completes
        assert ctrl.snapshot().active_connections == 0

    asyncio.run(_run())


# =============================================================================
# 5. Graceful Drain Scenarios (Section 9, 11 & 18.8-17)
# =============================================================================


def test_drain_with_zero_connections() -> None:
    """Item 8: Draining an instance with zero active connections completes immediately."""

    async def _run() -> None:
        ctrl = _make_controller()
        coord = InstanceDrainCoordinator(controller=ctrl)

        report = await coord.drain()
        assert report.initial_connections == 0
        assert report.graceful_completions == 0
        assert report.force_closed == 0
        assert report.failed_outcomes == 0
        assert report.unresolved_connections == 0
        assert report.active_leases_remaining == 0
        assert report.deadline_exceeded is False
        assert report.final_state == InstanceDrainState.STOPPED
        assert coord.is_stopped is True
        assert coord.report is report

    asyncio.run(_run())


def test_drain_with_single_connection_graceful_completion() -> None:
    """Item 9 & 11: Single active connection completes normally within grace period."""

    async def _run() -> None:
        ctrl = _make_controller()
        coord = InstanceDrainCoordinator(controller=ctrl, policy=DrainPolicy(grace_period=2.0))
        sup = await coord.admit_connection(_make_context())

        async def natural_worker() -> None:
            await asyncio.sleep(0.05)

        await sup.start(natural_worker)
        assert sup.state == ConnectionState.ACTIVE

        report = await coord.drain()
        assert report.initial_connections == 1
        assert report.graceful_completions == 1
        assert report.force_closed == 0
        assert report.failed_outcomes == 0
        assert report.unresolved_connections == 0
        assert report.active_leases_remaining == 0
        assert report.deadline_exceeded is False
        assert ctrl.snapshot().active_connections == 0

    asyncio.run(_run())


def test_drain_with_multiple_connections_graceful_completion() -> None:
    """Item 10: Multiple active connections all complete gracefully within grace period."""

    async def _run() -> None:
        ctrl = _make_controller()
        coord = InstanceDrainCoordinator(controller=ctrl, policy=DrainPolicy(grace_period=2.0))

        supervisors: list[ConnectionSupervisor] = []
        for i in range(5):
            sup = await coord.admit_connection(_make_context(connection_id=f"conn_multi_{i}"))

            async def worker(sleep_time: float = 0.02 * (i + 1)) -> None:
                await asyncio.sleep(sleep_time)

            await sup.start(worker)
            supervisors.append(sup)

        report = await coord.drain()
        assert report.initial_connections == 5
        assert report.graceful_completions == 5
        assert report.force_closed == 0
        assert report.failed_outcomes == 0
        assert report.unresolved_connections == 0
        assert report.active_leases_remaining == 0
        assert ctrl.snapshot().active_connections == 0

    asyncio.run(_run())


def test_drain_grace_deadline_exceeded_triggers_force_close() -> None:
    """Item 12 & 13: Connections that do not finish before grace deadline are force-closed."""

    async def _run() -> None:
        ctrl = _make_controller()
        # Short grace period (0.05s) and sufficient force close timeout (1.0s)
        coord = InstanceDrainCoordinator(
            controller=ctrl,
            policy=DrainPolicy(grace_period=0.05, force_close_timeout=1.0),
        )
        sup = await coord.admit_connection(_make_context())

        async def slow_worker() -> None:
            await asyncio.sleep(10.0)

        await sup.start(slow_worker)

        report = await coord.drain()
        assert report.initial_connections == 1
        assert report.graceful_completions == 0
        assert report.force_closed == 1
        assert report.failed_outcomes == 0  # cancelled cleanly without failure
        assert report.unresolved_connections == 0
        assert report.active_leases_remaining == 0
        assert report.deadline_exceeded is True
        assert ctrl.snapshot().active_connections == 0

    asyncio.run(_run())


def test_concurrent_drain_invocations_share_single_flight() -> None:
    """Item 14: Multiple concurrent drain calls share the exact same underlying task and report."""

    async def _run() -> None:
        ctrl = _make_controller()
        coord = InstanceDrainCoordinator(controller=ctrl, policy=DrainPolicy(grace_period=0.1))
        sup = await coord.admit_connection(_make_context())

        async def worker() -> None:
            await asyncio.sleep(0.02)

        await sup.start(worker)

        # Call drain concurrently from 3 tasks
        t1 = asyncio.create_task(coord.drain())
        t2 = asyncio.create_task(coord.drain())
        t3 = asyncio.create_task(coord.drain())

        r1, r2, r3 = await asyncio.gather(t1, t2, t3)
        assert r1 is r2
        assert r2 is r3
        assert r1.initial_connections == 1
        assert r1.final_state == InstanceDrainState.STOPPED

    asyncio.run(_run())


def test_repeated_drain_call_after_stopped_is_idempotent() -> None:
    """Item 15 & 17: Calling drain again once STOPPED immediately returns cached report."""

    async def _run() -> None:
        ctrl = _make_controller()
        coord = InstanceDrainCoordinator(controller=ctrl)

        r1 = await coord.drain()
        assert coord.state == InstanceDrainState.STOPPED

        r2 = await coord.drain()
        assert r1 is r2

        r3 = await coord.wait_stopped()
        assert r1 is r3

    asyncio.run(_run())


def test_drain_caller_cancellation_preserves_internal_drain_execution() -> None:
    """Item 16: External cancellation of a caller awaiting drain() does not abort shutdown."""

    async def _run() -> None:
        ctrl = _make_controller()
        coord = InstanceDrainCoordinator(controller=ctrl, policy=DrainPolicy(grace_period=0.2))
        sup = await coord.admit_connection(_make_context())

        async def worker() -> None:
            await asyncio.sleep(0.05)

        await sup.start(worker)

        # Caller 1 starts drain and will be cancelled
        caller1_task = asyncio.create_task(coord.drain())
        # Caller 2 also awaits drain
        caller2_task = asyncio.create_task(coord.drain())

        await asyncio.sleep(0.01)
        caller1_task.cancel()

        with pytest.raises(asyncio.CancelledError):
            await caller1_task

        # Caller 2 receives the completed report; internal drain was shielded
        r2 = await caller2_task
        assert r2.initial_connections == 1
        assert r2.graceful_completions == 1
        assert r2.final_state == InstanceDrainState.STOPPED
        assert ctrl.snapshot().active_connections == 0

    asyncio.run(_run())


# =============================================================================
# 6. Failure Modes, Unresolved Resources & Recovery (Section 12 & 18.18-27)
# =============================================================================


def test_drain_with_worker_failure_outcome() -> None:
    """Item 18: Worker failure during draining is tracked as a failed outcome."""

    async def _run() -> None:
        ctrl = _make_controller()
        coord = InstanceDrainCoordinator(controller=ctrl, policy=DrainPolicy(grace_period=0.5))
        sup = await coord.admit_connection(_make_context())

        async def failing_worker() -> None:
            await asyncio.sleep(0.02)
            raise ValueError("Simulated worker error")

        await sup.start(failing_worker)

        report = await coord.drain()
        assert report.initial_connections == 1
        assert report.graceful_completions == 0
        assert report.force_closed == 0
        assert report.failed_outcomes == 1
        assert report.unresolved_connections == 1
        assert report.active_leases_remaining == 0
        assert ctrl.snapshot().active_connections == 0

    asyncio.run(_run())


def test_drain_with_resource_cleanup_failure() -> None:
    """Item 19: Resource cleanup failure records failed outcome without orphaning lease."""

    async def _run() -> None:
        ctrl = _make_controller()
        coord = InstanceDrainCoordinator(controller=ctrl, policy=DrainPolicy(grace_period=0.1))
        sup = await coord.admit_connection(_make_context())

        async def failing_resource() -> None:
            raise RuntimeError("Resource cleanup explosion")

        sup.register_resource(failing_resource)

        async def worker() -> None:
            await asyncio.sleep(0.01)

        await sup.start(worker)

        report = await coord.drain()
        assert report.initial_connections == 1
        assert report.graceful_completions == 0
        assert report.force_closed == 0
        assert report.failed_outcomes == 1
        assert report.unresolved_connections == 1
        assert report.active_leases_remaining == 0
        assert ctrl.snapshot().active_connections == 0

    asyncio.run(_run())


def test_drain_with_stubborn_worker_force_close_timeout() -> None:
    """Item 22 & 23: Non-cooperative worker that ignores cancellation exceeds force-close deadline."""

    async def _run() -> None:
        ctrl = _make_controller()
        # Fast timeouts for deterministic testing
        policy = DrainPolicy(grace_period=0.02, force_close_timeout=0.05)
        # Supervisor cleanup timeout longer than coordinator force close timeout
        sup_policy = LifecyclePolicy(cleanup_timeout=2.0)
        coord = InstanceDrainCoordinator(
            controller=ctrl,
            policy=policy,
            supervisor_policy=sup_policy,
        )
        sup = await coord.admit_connection(_make_context())

        async def stubborn_worker() -> None:
            try:
                await asyncio.sleep(10.0)
            except asyncio.CancelledError:
                # Stubbornly sleep past force_close_timeout
                await asyncio.sleep(5.0)

        await sup.start(stubborn_worker)

        report = await coord.drain()
        assert report.initial_connections == 1
        assert report.force_closed == 0
        assert report.deadline_exceeded is True
        # Supervisor had not finished when force close deadline expired
        assert report.unresolved_connections == 1

        # Cleanup stubborn worker task so test suite exits cleanly
        await sup.close()

    asyncio.run(_run())


def test_drain_with_unreleased_lease_reported_as_unresolved_without_unsafe_recovery() -> None:
    """Item 20 & 25: Unreleased capacity lease is tracked in DrainReport as unresolved without unsafe recovery."""

    async def _run() -> None:
        class _FailingReleaseController(AdmissionController):
            def __init__(self, cfg: CoreConfig) -> None:
                super().__init__(cfg)
                self.fail_release = True

            async def _release_lease(self, lease: Any) -> bool:
                if self.fail_release:
                    raise RuntimeError("Controller simulated release network error")
                return await super()._release_lease(lease)

        ctrl = _FailingReleaseController(CoreConfig())
        coord = InstanceDrainCoordinator(
            controller=ctrl,
            policy=DrainPolicy(grace_period=0.02, force_close_timeout=0.1),
        )
        sup = await coord.admit_connection(_make_context())

        # Close supervisor before drain; release fails, so supervisor is closed with active lease
        await sup.close(reason=TerminalReason.CLEANUP_FAILED)
        assert sup.is_closed is True
        assert sup.lease is not None and sup.lease.is_active is True

        # Drain will encounter closed supervisor with active lease -> skips close_tasks, marks unresolved
        report = await coord.drain()
        assert report.initial_connections == 1
        assert report.graceful_completions == 0
        assert report.force_closed == 0
        assert report.active_leases_remaining == 1
        assert report.unresolved_connections == 1
        assert ctrl.snapshot().active_connections == 1

        # Repeated drain call returns identical report and never releases capacity unsafely
        second_report = await coord.drain()
        assert second_report == report
        assert ctrl.snapshot().active_connections == 1

    asyncio.run(_run())


# =============================================================================
# 7. Two-Instance Isolation & Zero Global State (Section 13 & 18.28, 30)
# =============================================================================


def test_two_instance_coordinator_isolation() -> None:
    """Item 28 & 30: Coordinator A draining has zero side-effects on independent Coordinator B."""

    async def _run() -> None:
        ctrl_a = _make_controller()
        ctrl_b = _make_controller()

        coord_a = InstanceDrainCoordinator(controller=ctrl_a)
        coord_b = InstanceDrainCoordinator(controller=ctrl_b)

        sup_a = await coord_a.admit_connection(_make_context(connection_id="conn_shared_id"))
        sup_b = await coord_b.admit_connection(_make_context(connection_id="conn_shared_id"))

        assert sup_a.state == ConnectionState.ADMITTED
        assert sup_b.state == ConnectionState.ADMITTED
        assert coord_a.active_connections_count == 1
        assert coord_b.active_connections_count == 1

        # Drain Coordinator A
        report_a = await coord_a.drain()
        assert coord_a.state == InstanceDrainState.STOPPED
        assert report_a.initial_connections == 1
        assert ctrl_a.snapshot().active_connections == 0

        # Coordinator B is completely unaffected!
        assert coord_b.state == InstanceDrainState.ACCEPTING
        assert coord_b.is_accepting is True
        assert coord_b.active_connections_count == 1
        assert ctrl_b.snapshot().active_connections == 1

        # Coordinator B can admit more connections
        sup_b2 = await coord_b.admit_connection(_make_context(connection_id="conn_b2"))
        assert sup_b2.state == ConnectionState.ADMITTED
        assert coord_b.active_connections_count == 2

        await sup_b.close()
        await sup_b2.close()
        assert ctrl_b.snapshot().active_connections == 0

    asyncio.run(_run())


# =============================================================================
# 8. Diagnostic Privacy & String Representations (Section 12 & 18.34)
# =============================================================================


def test_drain_report_and_coordinator_diagnostic_privacy() -> None:
    """Item 34: Verify repr/str do not leak connection_id, tenant_id, call_id, or secrets."""
    secret = "sk-super-secret-key-12345"
    report = DrainReport(
        initial_connections=5,
        graceful_completions=4,
        force_closed=1,
        failed_outcomes=0,
        unresolved_connections=0,
        active_leases_remaining=0,
        deadline_exceeded=False,
        final_state=InstanceDrainState.STOPPED,
        duration_seconds=1.23,
    )

    repr_str = repr(report)
    assert secret not in repr_str
    assert "conn_" not in repr_str
    assert "tenant_" not in repr_str
    assert "call_" not in repr_str
    assert repr_str == str(report)
    assert "final_state='STOPPED'" in repr_str
    assert "initial=5" in repr_str

    ctrl = _make_controller()
    coord = InstanceDrainCoordinator(controller=ctrl)
    coord_repr = repr(coord)
    assert secret not in coord_repr
    assert "state='ACCEPTING'" in coord_repr
    assert "managed=0" in coord_repr
    assert str(coord) == coord_repr


def test_tracker_cancellation_and_non_closed_pruning_branches() -> None:
    """Verify _track_supervisor_completion handles CancelledError and preserves non-clean supervisors."""

    async def _run() -> None:
        ctrl = _make_controller()
        coord = InstanceDrainCoordinator(controller=ctrl)

        # 1. Admit connection
        sup = await coord.admit_connection(_make_context(connection_id="conn_cancel_track"))
        # Allow tracker task to start executing and begin awaiting wait_closed()
        await asyncio.sleep(0.01)
        assert len(coord._tracker_tasks) == 1
        tracker = next(iter(coord._tracker_tasks))

        # Cancel tracker task directly while it is actively awaiting
        tracker.cancel()
        await asyncio.sleep(0.01)

        # 2. Admit second connection that ends in FAILED state
        sup_fail = await coord.admit_connection(_make_context(connection_id="conn_fail_track"))
        await sup_fail.close(reason=TerminalReason.WORKER_FAILED)

        # Allow tracker to execute its finally block
        await asyncio.sleep(0.01)

        # Verify supervisor is in FAILED state and NOT popped from coordinator's _supervisors
        assert sup_fail.state == ConnectionState.FAILED
        assert "conn_fail_track" in coord._supervisors

        await sup.close()

    asyncio.run(_run())


def test_exact_mathematical_partition_invariant() -> None:
    """Blocker D & E: initial_connections == graceful_completions + force_closed + unresolved_connections."""

    async def _run() -> None:
        ctrl = _make_controller(max_concurrent=10)
        coord = InstanceDrainCoordinator(
            controller=ctrl,
            policy=DrainPolicy(grace_period=0.05, force_close_timeout=0.05),
        )

        # 1. Graceful connection: closes before grace period ends
        sup_grace = await coord.admit_connection(_make_context(connection_id="conn_p_grace"))
        # 2. Force-closed connection: stays open during grace, but closes cooperative on force-close
        sup_force = await coord.admit_connection(_make_context(connection_id="conn_p_force"))
        # 3. Stubborn connection: ignores cancellation, times out force-close -> unresolved
        sup_unres = await coord.admit_connection(_make_context(connection_id="conn_p_unres"))

        stop_zombie = asyncio.Event()

        async def worker_grace() -> None:
            await asyncio.sleep(0.01)

        async def worker_force() -> None:
            await asyncio.sleep(10.0)

        async def worker_zombie() -> None:
            while not stop_zombie.is_set():
                try:
                    await asyncio.sleep(0.01)
                except asyncio.CancelledError:
                    pass

        await sup_grace.start(worker_grace)
        await sup_force.start(worker_force)
        await sup_unres.start(worker_zombie)

        report = await coord.drain()

        # Invariant check: exact partition of initial connections
        assert report.initial_connections == 3
        assert (
            report.initial_connections
            == report.graceful_completions + report.force_closed + report.unresolved_connections
        )
        assert report.graceful_completions == 1
        assert report.force_closed == 1
        assert report.unresolved_connections == 1
        assert report.deadline_exceeded is True
        assert report.final_state == InstanceDrainState.STOPPED

        # Cleanup zombie
        stop_zombie.set()
        await asyncio.sleep(0.05)

    asyncio.run(_run())


def test_non_cooperative_worker_nominal_vs_real_workload_distinction() -> None:
    """Blocker B & D: Non-cooperative worker keeps supervisor pending; capacity is NOT unsafely freed."""

    async def _run() -> None:
        ctrl = _make_controller()
        coord = InstanceDrainCoordinator(
            controller=ctrl,
            policy=DrainPolicy(grace_period=0.01, force_close_timeout=0.05),
        )

        sup = await coord.admit_connection(_make_context(connection_id="conn_zombie"))

        worker_started = asyncio.Event()
        stop_zombie = asyncio.Event()

        async def stubborn_worker() -> None:
            worker_started.set()
            while not stop_zombie.is_set():
                try:
                    await asyncio.sleep(0.01)
                except asyncio.CancelledError:
                    # Non-cooperative worker ignores cancellation!
                    pass

        await sup.start(stubborn_worker)
        await worker_started.wait()

        # Drain will exceed force_close_timeout because worker refuses to terminate
        report = await coord.drain()
        assert report.deadline_exceeded is True
        assert report.unresolved_connections == 1
        assert report.active_leases_remaining == 1
        assert report.final_state == InstanceDrainState.STOPPED

        # Because worker is still running, sup.is_closed is False and capacity stays reserved!
        assert sup.is_closed is False
        assert ctrl.snapshot().active_connections == 1

        # Stop zombie to cleanly allow task cleanup
        stop_zombie.set()
        await asyncio.sleep(0.05)

    asyncio.run(_run())


def test_observer_tasks_awaited_on_shutdown() -> None:
    """Blocker C: lingering observer tasks are cleanly cancelled and awaited during drain completion."""

    async def _run() -> None:
        ctrl = _make_controller()
        coord = InstanceDrainCoordinator(controller=ctrl)

        _ = await coord.admit_connection(_make_context(connection_id="conn_obs_wait"))
        assert len(coord._tracker_tasks) == 1

        # Drain will cancel and gather all tracker tasks
        report = await coord.drain(DrainPolicy(grace_period=0.01, force_close_timeout=0.05))
        assert report.final_state == InstanceDrainState.STOPPED
        # Verify no active observer tasks remain
        assert len(coord._tracker_tasks) == 0

    asyncio.run(_run())


def test_repeated_cancellation_during_admission_rollback(monkeypatch: pytest.MonkeyPatch) -> None:
    """Blocker B: Shielded rollback prevents orphan lease even if admission task is repeatedly cancelled."""
    rollback_started = asyncio.Event()

    orig_close = ConnectionSupervisor.close

    async def _close_with_event(self: ConnectionSupervisor, *args: Any, **kwargs: Any) -> Any:
        rollback_started.set()
        await asyncio.sleep(0.02)
        return await orig_close(self, *args, **kwargs)

    monkeypatch.setattr(ConnectionSupervisor, "close", _close_with_event)

    async def _run() -> None:
        ctrl = _make_controller()
        coord = InstanceDrainCoordinator(controller=ctrl)

        orig_admit = ConnectionSupervisor.admit

        async def _admit_and_cancel(self: ConnectionSupervisor) -> bool:
            res = await orig_admit(self)
            assert res is True
            raise asyncio.CancelledError()

        monkeypatch.setattr(ConnectionSupervisor, "admit", _admit_and_cancel)

        admit_task = asyncio.create_task(
            coord.admit_connection(_make_context(connection_id="conn_repeat_cancel"))
        )

        # Wait for rollback to start
        await rollback_started.wait()

        # Attempt second cancellation during rollback
        admit_task.cancel()

        with pytest.raises(asyncio.CancelledError):
            await admit_task

        # Verify capacity lease was cleanly released (shielded rollback succeeded)
        assert ctrl.snapshot().active_connections == 0
        assert "conn_repeat_cancel" not in coord._supervisors

    asyncio.run(_run())


def test_tracker_done_callback_observes_exceptions() -> None:
    """Blocker C: _on_tracker_done safely retrieves exceptions without warnings and handles cancelled tasks."""

    async def _run() -> None:
        ctrl = _make_controller()
        coord = InstanceDrainCoordinator(controller=ctrl)

        async def _raising_coro() -> None:
            raise RuntimeError("Simulated observer crash")

        task = asyncio.create_task(_raising_coro())
        coord._tracker_tasks.add(task)

        # Wait for task to finish with exception
        await asyncio.sleep(0.01)

        # Invoking _on_tracker_done retrieves the exception and removes from set
        coord._on_tracker_done(task)
        assert task not in coord._tracker_tasks

        # Cancelled task test
        async def _long_coro() -> None:
            await asyncio.sleep(10.0)

        task2 = asyncio.create_task(_long_coro())
        coord._tracker_tasks.add(task2)
        task2.cancel()
        await asyncio.sleep(0.01)

        coord._on_tracker_done(task2)
        assert task2 not in coord._tracker_tasks

    asyncio.run(_run())


def test_admission_rollback_exception_paths(monkeypatch: pytest.MonkeyPatch) -> None:
    """Blocker A: verify rollback skips when lease is None, and catches exception during close()."""

    async def _run() -> None:
        ctrl = _make_controller()
        coord = InstanceDrainCoordinator(controller=ctrl)

        # 1. Exception during admit() before lease is acquired (lease is None)
        async def _failing_admit_no_lease(self: ConnectionSupervisor) -> bool:
            raise RuntimeError("Early admit failure before lease")

        monkeypatch.setattr(ConnectionSupervisor, "admit", _failing_admit_no_lease)
        with pytest.raises(RuntimeError, match="Early admit failure before lease"):
            await coord.admit_connection(_make_context(connection_id="conn_early_fail"))

        # 2. Cancellation after lease acquired, but supervisor.close() raises an Exception during rollback
        orig_admit = ConnectionSupervisor.admit
        orig_close = ConnectionSupervisor.close

        async def _admit_then_cancel(self: ConnectionSupervisor) -> bool:
            # Manually acquire lease so self._lease is active
            lease = await self._controller.acquire(self._context)
            self._lease = lease
            # Simulate cancellation
            raise asyncio.CancelledError()

        async def _failing_close(self: ConnectionSupervisor, *args: Any, **kwargs: Any) -> Any:
            raise RuntimeError("Simulated close failure during rollback")

        monkeypatch.setattr(ConnectionSupervisor, "admit", _admit_then_cancel)
        monkeypatch.setattr(ConnectionSupervisor, "close", _failing_close)

        with pytest.raises(asyncio.CancelledError):
            await coord.admit_connection(_make_context(connection_id="conn_rollback_close_fail"))

        # Rollback close failed and lease is active -> connection is retained in _supervisors for drain accounting!
        assert "conn_rollback_close_fail" in coord._supervisors

        # 3. Fatal interpreter exit (SystemExit) is not swallowed
        async def _system_exit_admit(self: ConnectionSupervisor) -> bool:
            raise SystemExit(1)

        monkeypatch.setattr(ConnectionSupervisor, "admit", _system_exit_admit)
        with pytest.raises(SystemExit):
            await coord.admit_connection(_make_context(connection_id="conn_system_exit"))

        assert "conn_system_exit" not in coord._supervisors

        # Cleanup monkeypatches
        monkeypatch.setattr(ConnectionSupervisor, "admit", orig_admit)
        monkeypatch.setattr(ConnectionSupervisor, "close", orig_close)

    asyncio.run(_run())


# =============================================================================
# 9. Drain Policy Validation, Internal Failure & Fatal Error Tests
# =============================================================================


def test_drain_runtime_policy_type_validation_upfront() -> None:
    """Verify runtime type validation for policy passed to drain() occurs upfront without state mutation."""

    async def _run() -> None:
        ctrl = _make_controller()
        coord = InstanceDrainCoordinator(controller=ctrl)
        assert coord.state == InstanceDrainState.ACCEPTING

        # Pass invalid types to drain()
        with pytest.raises(TypeError, match="policy must be a DrainPolicy instance"):
            await coord.drain(policy="invalid")  # type: ignore[arg-type]

        # Coordinator must remain in ACCEPTING state!
        assert coord.state == InstanceDrainState.ACCEPTING
        assert coord.is_accepting is True

        with pytest.raises(TypeError, match="policy must be a DrainPolicy instance"):
            await coord.drain(policy=123)  # type: ignore[arg-type]

        assert coord.state == InstanceDrainState.ACCEPTING
        assert coord.is_accepting is True

    asyncio.run(_run())


def test_drain_internal_task_failure_transitions_to_stopped_and_unblocks_waiters(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify internal drain task failure fails closed to STOPPED, records error, and unblocks waiters."""

    async def _run() -> None:
        ctrl = _make_controller()
        coord = InstanceDrainCoordinator(controller=ctrl)

        def _failing_timeout(delay: float) -> Any:
            raise RuntimeError("Simulated internal drain supervisor failure")

        sup = await coord.admit_connection(_make_context(connection_id="conn_drain_fault"))
        assert len(coord._tracker_tasks) == 1
        monkeypatch.setattr(asyncio, "timeout", _failing_timeout)

        waiter_task = asyncio.create_task(coord.wait_stopped())

        # Calling drain() raises the underlying exception
        with pytest.raises(RuntimeError, match="Simulated internal drain supervisor failure"):
            await coord.drain()

        # Coordinator is in STOPPED state (fail-closed)
        assert coord.state == InstanceDrainState.STOPPED
        assert coord.drain_error is not None
        assert isinstance(coord.drain_error, RuntimeError)
        assert len(coord._tracker_tasks) == 0

        # Waiter is unblocked and raises DrainError chained from the underlying fault
        with pytest.raises(
            DrainError, match="Instance drain failed due to internal error"
        ) as exc_info:
            await waiter_task
        assert isinstance(exc_info.value.__cause__, RuntimeError)

        # Subsequent drain() calls raise DrainError without hanging
        with pytest.raises(DrainError, match="Instance drain previously failed"):
            await coord.drain()

        # Subsequent admissions are rejected immediately
        with pytest.raises(InstanceDrainingError, match="instance is in STOPPED state"):
            await coord.admit_connection(_make_context(connection_id="conn_after_stop"))

        await sup.close()

    asyncio.run(_run())


def test_drain_internal_task_failure_with_zero_connections_transitions_to_stopped(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify internal failure during zero-connection drain triggers exception path without trackers."""

    async def _run() -> None:
        ctrl = _make_controller()
        coord = InstanceDrainCoordinator(controller=ctrl)

        def _failing_drain_report(*args: Any, **kwargs: Any) -> Any:
            raise RuntimeError("Simulated failure with zero connections")

        monkeypatch.setattr("kaiwen_gateway.core.draining.DrainReport", _failing_drain_report)

        with pytest.raises(RuntimeError, match="Simulated failure with zero connections"):
            await coord.drain()

        assert coord.state == InstanceDrainState.STOPPED
        assert coord.drain_error is not None
        assert isinstance(coord.drain_error, RuntimeError)

    asyncio.run(_run())


def test_observer_cleanup_order_before_wait_stopped_returns() -> None:
    """Verify observer tracker tasks are cancelled and awaited before wait_stopped returns."""

    async def _run() -> None:
        ctrl = _make_controller()
        coord = InstanceDrainCoordinator(controller=ctrl)

        sup = await coord.admit_connection(_make_context(connection_id="conn_order"))
        assert len(coord._tracker_tasks) == 1

        waiter_done = asyncio.Event()
        observer_count_at_waiter_done = -1

        async def waiter() -> None:
            await coord.wait_stopped()
            nonlocal observer_count_at_waiter_done
            observer_count_at_waiter_done = len(coord._tracker_tasks)
            waiter_done.set()

        wait_task = asyncio.create_task(waiter())

        async def worker() -> None:
            await asyncio.sleep(0.01)

        await sup.start(worker)

        report = await coord.drain()
        await waiter_done.wait()
        await wait_task

        assert report.final_state == InstanceDrainState.STOPPED
        assert observer_count_at_waiter_done == 0
        assert len(coord._tracker_tasks) == 0

    asyncio.run(_run())


def test_admission_fatal_exception_with_active_lease_retains_supervisor(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify fatal KeyboardInterrupt during admission retains supervisor if capacity lease is active."""

    async def _run() -> None:
        ctrl = _make_controller()
        coord = InstanceDrainCoordinator(controller=ctrl)

        orig_admit = ConnectionSupervisor.admit

        async def _admit_then_sigint(self: ConnectionSupervisor) -> bool:
            # Call actual admit() so supervisor enters ADMITTED state and lease is active
            res = await orig_admit(self)
            assert res is True
            raise KeyboardInterrupt()

        monkeypatch.setattr(ConnectionSupervisor, "admit", _admit_then_sigint)

        with pytest.raises(KeyboardInterrupt):
            await coord.admit_connection(_make_context(connection_id="conn_fatal_sigint"))

        # Supervisor must be RETAINED in coordinator inventory so ownership is not lost!
        assert "conn_fatal_sigint" in coord._supervisors
        retained_sup = coord._supervisors["conn_fatal_sigint"]
        assert retained_sup.lease is not None and retained_sup.lease.is_active is True

        # When draining, this retained supervisor is accounted for (and force closed)
        report = await coord.drain()
        assert report.initial_connections == 1
        assert report.force_closed == 1
        assert report.active_leases_remaining == 0
        assert ctrl.snapshot().active_connections == 0

        monkeypatch.setattr(ConnectionSupervisor, "admit", orig_admit)

    asyncio.run(_run())


def test_failed_supervisor_with_released_lease_is_unresolved_not_graceful() -> None:
    """Verify a supervisor in FAILED state with released lease is accounted as unresolved and failed."""

    async def _run() -> None:
        ctrl = _make_controller()
        coord = InstanceDrainCoordinator(controller=ctrl, policy=DrainPolicy(grace_period=0.1))

        sup = await coord.admit_connection(_make_context(connection_id="conn_failed_released"))

        async def crash_worker() -> None:
            raise RuntimeError("Worker exploded immediately")

        await sup.start(crash_worker)
        # Wait for supervisor to finish terminating and reach FAILED state
        await sup.wait_closed()
        assert sup.state == ConnectionState.FAILED
        assert sup.lease is not None and sup.lease.is_active is False

        # Drain the instance
        report = await coord.drain()
        assert report.initial_connections == 1
        assert report.graceful_completions == 0
        assert report.force_closed == 0
        assert report.unresolved_connections == 1
        assert report.failed_outcomes == 1
        assert report.active_leases_remaining == 0
        assert (
            report.initial_connections
            == report.graceful_completions + report.force_closed + report.unresolved_connections
        )

    asyncio.run(_run())


def test_cancelled_drain_caller_with_internal_failure_no_unretrieved_warning(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify cancelled drain caller followed by internal drain failure is observed without runtime warnings."""

    async def _run() -> None:
        loop = asyncio.get_running_loop()
        unhandled_exceptions: list[dict[str, Any]] = []

        def _custom_handler(
            current_loop: asyncio.AbstractEventLoop, context: dict[str, Any]
        ) -> None:
            unhandled_exceptions.append(context)

        loop.set_exception_handler(_custom_handler)

        ctrl = _make_controller()
        coord = InstanceDrainCoordinator(controller=ctrl)

        drain_started = asyncio.Event()
        caller_cancelled = asyncio.Event()

        sup = await coord.admit_connection(_make_context(connection_id="conn_unobserved"))

        async def worker() -> None:
            drain_started.set()
            await caller_cancelled.wait()

        await sup.start(worker)

        def _failing_drain_report(*args: Any, **kwargs: Any) -> Any:
            raise RuntimeError("Injected internal drain failure after caller cancel")

        monkeypatch.setattr("kaiwen_gateway.core.draining.DrainReport", _failing_drain_report)

        # 3. Drain caller in a separate task
        caller_task = asyncio.create_task(coord.drain())

        # 4. Wait until internal drain starts
        await drain_started.wait()

        # 5. Cancel external caller task
        caller_task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await caller_task
        caller_cancelled.set()

        # Wait for internal drain task to finish executing without direct await on result
        assert coord._drain_task is not None
        drain_task = coord._drain_task
        while not drain_task.done():
            await asyncio.sleep(0.01)

        # 8. Coordinator reaches STOPPED state
        assert coord.state == InstanceDrainState.STOPPED
        assert coord.is_stopped is True

        # 10. Error is visible on coordinator
        assert coord.drain_error is not None
        assert isinstance(coord.drain_error, RuntimeError)
        assert str(coord.drain_error) == "Injected internal drain failure after caller cancel"

        # 9. wait_stopped() produces DrainError
        with pytest.raises(
            DrainError, match="Instance drain failed due to internal error"
        ) as exc_info:
            await coord.wait_stopped()
        assert isinstance(exc_info.value.__cause__, RuntimeError)

        # 11-13. Verify zero unhandled/unretrieved task exception warnings after garbage collection
        del caller_task
        del drain_task
        import gc

        gc.collect()

        assert len(unhandled_exceptions) == 0

        # Reset exception handler
        loop.set_exception_handler(None)
        await sup.close()

    asyncio.run(_run())


def test_drain_with_non_cooperative_observer_task_bounded_by_force_close_timeout() -> None:
    """Verify non-cooperative observer task is bounded by force_close_timeout and sets deadline_exceeded."""

    async def _run() -> None:
        ctrl = _make_controller()
        policy = DrainPolicy(grace_period=0.01, force_close_timeout=0.05)
        coord = InstanceDrainCoordinator(controller=ctrl, policy=policy)

        sup = await coord.admit_connection(_make_context(connection_id="conn_stubborn_observer"))

        stop_stubborn = asyncio.Event()

        # Create a stubborn observer task that ignores cancellation
        async def _stubborn_tracker() -> None:
            while not stop_stubborn.is_set():
                try:
                    await asyncio.sleep(0.01)
                except asyncio.CancelledError:
                    # Non-cooperative observer refuses to exit immediately
                    await asyncio.sleep(2.0)
                    break

        stubborn_task = asyncio.create_task(_stubborn_tracker(), name="gw-drain-track-stubborn")
        coord._tracker_tasks.add(stubborn_task)
        stubborn_task.add_done_callback(coord._on_tracker_done)

        loop = asyncio.get_running_loop()
        start = loop.time()
        report = await coord.drain()
        elapsed = loop.time() - start

        # Must be bounded by force_close_timeout (~0.05s) rather than sleeping 2.0s
        assert elapsed < 0.5
        assert report.deadline_exceeded is True
        assert report.final_state == InstanceDrainState.STOPPED
        assert coord.is_stopped is True

        # Cleanup stubborn task
        stop_stubborn.set()
        await sup.close()
        await stubborn_task

    asyncio.run(_run())


def test_drain_fatal_interpreter_exit_semantics(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify fatal SystemExit in _execute_drain is re-raised directly without normal drain_error."""
    ctrl = _make_controller()
    coord = InstanceDrainCoordinator(controller=ctrl)

    def _fatal_drain_report(*args: Any, **kwargs: Any) -> Any:
        raise SystemExit(2)

    monkeypatch.setattr("kaiwen_gateway.core.draining.DrainReport", _fatal_drain_report)

    async def _run() -> None:
        await coord.drain()

    with pytest.raises(SystemExit) as exc_info:
        asyncio.run(_run())

    assert exc_info.value.code == 2
    # Fatal exit is not converted to normal drain error
    assert coord.drain_error is None


def test_drain_task_cancellation_done_callback() -> None:
    """Verify _on_drain_task_done safely handles cancelled internal drain task."""

    async def _run() -> None:
        ctrl = _make_controller()
        coord = InstanceDrainCoordinator(controller=ctrl)

        sup = await coord.admit_connection(_make_context(connection_id="conn_cancel_drain_task"))

        async def worker() -> None:
            await asyncio.sleep(10.0)

        await sup.start(worker)

        # Start drain task and explicitly cancel the underlying internal task
        drain_coro_task = asyncio.create_task(coord.drain())
        await asyncio.sleep(0.01)
        assert coord._drain_task is not None
        coord._drain_task.cancel()

        with pytest.raises(asyncio.CancelledError):
            await drain_coro_task

        assert coord._drain_task.cancelled() is True
        # Explicitly verify done callback did not crash on cancelled task
        coord._on_drain_task_done(coord._drain_task)

        await sup.close()

    asyncio.run(_run())
