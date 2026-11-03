"""Unit, concurrency, lifecycle, and invariant tests for M2.4A Connection Lifecycle.

These tests verify:
- FR-CONN-004: Explicit connection state machine with strictly guarded transitions.
- FR-CONN-006: Proper resource release on normal disconnect, error, and cancellation.
- FR-CONN-007: Interruption and teardown without capacity reservation leaks.
- NFR-REL-001: Robust connection lifecycle recovery under worker failures and timeouts.
- NFR-REL-003: Graceful degradation and zero orphan tasks under cancellation storms.
- ENG-004: Concurrency safety, race-free lease release, and stale callback isolation.
"""

from __future__ import annotations

import asyncio
from collections.abc import Coroutine
from dataclasses import FrozenInstanceError
from typing import Any

import pytest

from kaiwen_gateway.core import (
    AdmissionController,
    AdmissionLease,
    ConnectionLifecycleError,
    ConnectionOutcome,
    ConnectionState,
    ConnectionSupervisor,
    CoreConfig,
    CoreConfigurationError,
    InvalidConnectionTransitionError,
    LifecyclePolicy,
    TerminalReason,
    VerifiedConnectionContext,
)


def _make_context(
    *,
    connection_id: str = "conn_01",
    tenant_id: str = "tenant_alpha",
    provider_id: str = "synthetic_provider",
    call_id: str = "call_01",
    scopes: tuple[str, ...] = ("voice.stream",),
    provider_call_ref: str | None = None,
) -> VerifiedConnectionContext:
    """Helper to construct a deterministic VerifiedConnectionContext for tests."""
    return VerifiedConnectionContext(
        provider_id=provider_id,
        tenant_id=tenant_id,
        connection_id=connection_id,
        call_id=call_id,
        allowed_scopes=scopes,
        provider_call_ref=provider_call_ref,
    )


def _get_state(supervisor: ConnectionSupervisor) -> ConnectionState:
    """Helper to query supervisor state across mutating calls without type narrowing."""
    return supervisor.state


def _is_lease_active(lease: AdmissionLease | None) -> bool:
    """Helper to query lease activity state across mutating calls without type narrowing."""
    return lease.is_active if lease is not None else False


def _make_controller(
    *,
    max_concurrent: int = 10,
    max_per_tenant: int = 5,
) -> AdmissionController:
    """Helper to construct an AdmissionController with defined capacities."""
    config = CoreConfig(
        max_concurrent_connections=max_concurrent,
        max_connections_per_tenant=max_per_tenant,
    )
    return AdmissionController(config)


# ==============================================================================
# Group 1: State Machine & Transition Matrix
# ==============================================================================


def test_connection_state_values_and_properties() -> None:
    """Item 1: Verify all ConnectionState enum members, strings, and property flags."""
    expected_members = {
        "NEW": "NEW",
        "AUTHENTICATING": "AUTHENTICATING",
        "ADMITTED": "ADMITTED",
        "ACTIVE": "ACTIVE",
        "CLOSING": "CLOSING",
        "CLOSED": "CLOSED",
        "REJECTED": "REJECTED",
        "FAILED": "FAILED",
    }
    assert len(ConnectionState) == 8
    for name, value in expected_members.items():
        state = ConnectionState(value)
        assert state.name == name
        assert state.value == value
        assert str(state) == value
        assert repr(state) == f"<ConnectionState.{name}: {value!r}>"

    # Verify is_terminal
    assert ConnectionState.CLOSED.is_terminal is True
    assert ConnectionState.FAILED.is_terminal is True
    assert ConnectionState.REJECTED.is_terminal is True
    assert ConnectionState.NEW.is_terminal is False
    assert ConnectionState.AUTHENTICATING.is_terminal is False
    assert ConnectionState.ADMITTED.is_terminal is False
    assert ConnectionState.ACTIVE.is_terminal is False
    assert ConnectionState.CLOSING.is_terminal is False

    # Verify is_active
    assert ConnectionState.ACTIVE.is_active is True
    assert ConnectionState.NEW.is_active is False
    assert ConnectionState.AUTHENTICATING.is_active is False
    assert ConnectionState.ADMITTED.is_active is False
    assert ConnectionState.CLOSING.is_active is False
    assert ConnectionState.CLOSED.is_active is False
    assert ConnectionState.FAILED.is_active is False
    assert ConnectionState.REJECTED.is_active is False


def test_terminal_reason_values() -> None:
    """Item 2: Verify all TerminalReason enum members and string values."""
    expected = {
        "NORMAL": "normal",
        "CANCELLED": "cancelled",
        "ADMISSION_REJECTED": "admission_rejected",
        "STARTUP_FAILED": "startup_failed",
        "WORKER_FAILED": "worker_failed",
        "TIMEOUT": "timeout",
        "CLEANUP_FAILED": "cleanup_failed",
    }
    assert len(TerminalReason) == 7
    for name, val in expected.items():
        reason = TerminalReason(val)
        assert reason.name == name
        assert reason.value == val


def test_connection_outcome_immutability_and_slots() -> None:
    """Item 3: Verify ConnectionOutcome is a frozen slots dataclass with sanitization."""
    outcome = ConnectionOutcome(
        state=ConnectionState.CLOSED,
        reason=TerminalReason.NORMAL,
        error_summary="All workers done",
    )
    assert outcome.state == ConnectionState.CLOSED
    assert outcome.reason == TerminalReason.NORMAL
    assert outcome.error_summary == "All workers done"

    with pytest.raises(FrozenInstanceError):
        outcome.state = ConnectionState.FAILED  # type: ignore[misc]

    # Slots prevent adding unexpected attributes
    assert hasattr(outcome, "__slots__")
    assert "extra_field" not in outcome.__slots__


def test_lifecycle_policy_validation() -> None:
    """Item 4: Verify LifecyclePolicy accepts valid timeouts and rejects invalid ones."""
    policy = LifecyclePolicy()
    assert policy.startup_timeout == 5.0
    assert policy.cleanup_timeout == 5.0

    custom = LifecyclePolicy(startup_timeout=2.5, cleanup_timeout=1.0)
    assert custom.startup_timeout == 2.5
    assert custom.cleanup_timeout == 1.0

    # Ints are permitted
    int_policy = LifecyclePolicy(startup_timeout=3, cleanup_timeout=4)
    assert int_policy.startup_timeout == 3
    assert int_policy.cleanup_timeout == 4

    # Rejection of bools
    with pytest.raises(CoreConfigurationError, match=r"must be a float or int \(not bool\)"):
        LifecyclePolicy(startup_timeout=True)

    with pytest.raises(CoreConfigurationError, match=r"must be a float or int \(not bool\)"):
        LifecyclePolicy(cleanup_timeout=False)

    # Rejection of negative / zero
    with pytest.raises(CoreConfigurationError, match="strictly positive finite number"):
        LifecyclePolicy(startup_timeout=0.0)

    with pytest.raises(CoreConfigurationError, match="strictly positive finite number"):
        LifecyclePolicy(startup_timeout=-1.5)

    with pytest.raises(CoreConfigurationError, match="strictly positive finite number"):
        LifecyclePolicy(cleanup_timeout=0)

    # Rejection of non-finite (inf, nan)
    with pytest.raises(CoreConfigurationError, match="strictly positive finite number"):
        LifecyclePolicy(startup_timeout=float("inf"))

    with pytest.raises(CoreConfigurationError, match="strictly positive finite number"):
        LifecyclePolicy(cleanup_timeout=float("nan"))

    # Rejection of string or None
    with pytest.raises(CoreConfigurationError, match="must be a float or int"):
        LifecyclePolicy(startup_timeout="5.0")  # type: ignore[arg-type]


def test_supervisor_initialization_defaults() -> None:
    """Item 5: Verify default initialization of ConnectionSupervisor without lease."""
    ctx = _make_context()
    controller = _make_controller()
    supervisor = ConnectionSupervisor(context=ctx, controller=controller)

    assert _get_state(supervisor) == ConnectionState.NEW
    assert supervisor.connection_id == "conn_01"
    assert supervisor.tenant_id == "tenant_alpha"
    assert supervisor.context is ctx
    assert supervisor.controller is controller
    assert supervisor.lease is None
    assert supervisor.outcome is None
    assert supervisor.is_closed is False
    assert repr(supervisor) == "ConnectionSupervisor(state='NEW')"


def test_supervisor_initialization_type_validation() -> None:
    """Item 6: Verify supervisor constructor rejects non-context or non-controller."""
    ctx = _make_context()
    controller = _make_controller()

    with pytest.raises(TypeError, match="context must be a VerifiedConnectionContext"):
        ConnectionSupervisor(context="invalid", controller=controller)  # type: ignore[arg-type]

    with pytest.raises(TypeError, match="controller must be an AdmissionController"):
        ConnectionSupervisor(context=ctx, controller="invalid")  # type: ignore[arg-type]

    with pytest.raises(TypeError, match="lease must be an AdmissionLease instance"):
        ConnectionSupervisor(context=ctx, controller=controller, lease="invalid")  # type: ignore[arg-type]


def test_supervisor_initialization_with_mismatched_lease() -> None:
    """Item 7: Verify supervisor constructor rejects invalid or mismatched leases."""

    async def _run() -> None:
        ctx1 = _make_context(connection_id="conn_01")
        ctx2 = _make_context(connection_id="conn_02")
        controller1 = _make_controller()
        controller2 = _make_controller()

        lease1 = await controller1.acquire(ctx1)

        # Lease from different controller
        with pytest.raises(ConnectionLifecycleError, match="different AdmissionController"):
            ConnectionSupervisor(context=ctx1, controller=controller2, lease=lease1)

        # Lease with mismatched connection_id
        with pytest.raises(ConnectionLifecycleError, match="connection_id does not match"):
            ConnectionSupervisor(context=ctx2, controller=controller1, lease=lease1)

        # Inactive / released lease
        await lease1.release()
        with pytest.raises(ConnectionLifecycleError, match="inactive or already released"):
            ConnectionSupervisor(context=ctx1, controller=controller1, lease=lease1)

    asyncio.run(_run())


def test_supervisor_from_lease_factory() -> None:
    """Item 8: Verify from_lease class method constructs supervisor in ADMITTED state."""

    async def _run() -> None:
        ctx = _make_context()
        controller = _make_controller()
        lease = await controller.acquire(ctx)

        supervisor = ConnectionSupervisor.from_lease(lease)
        assert _get_state(supervisor) == ConnectionState.ADMITTED
        assert supervisor.connection_id == "conn_01"
        assert supervisor.lease is lease
        assert supervisor.is_closed is False

        with pytest.raises(TypeError, match="lease must be an AdmissionLease instance"):
            ConnectionSupervisor.from_lease("not_a_lease")  # type: ignore[arg-type]

        await supervisor.close()

    asyncio.run(_run())


def test_state_machine_invalid_direct_jumps() -> None:
    """Item 9: Verify invalid transitions raise InvalidConnectionTransitionError."""
    ctx = _make_context()
    controller = _make_controller()
    supervisor = ConnectionSupervisor(context=ctx, controller=controller)

    # From NEW, cannot transition directly to ACTIVE, CLOSING, CLOSED, FAILED
    for target in (
        ConnectionState.ACTIVE,
        ConnectionState.CLOSING,
        ConnectionState.CLOSED,
        ConnectionState.FAILED,
    ):
        with pytest.raises(InvalidConnectionTransitionError) as exc_info:
            supervisor._transition_to(target)
        assert exc_info.value.current_state == "NEW"
        assert exc_info.value.target_state == target.value


def test_state_machine_terminal_freeze() -> None:
    """Item 10: Verify terminal states (CLOSED, FAILED, REJECTED) reject any further transition."""

    async def _run() -> None:
        ctx = _make_context()
        controller = _make_controller()

        # Test CLOSED terminal freeze
        s1 = ConnectionSupervisor(context=ctx, controller=controller)
        await s1.admit()
        await s1.close()
        assert s1.state == ConnectionState.CLOSED
        with pytest.raises(InvalidConnectionTransitionError):
            s1._transition_to(ConnectionState.ACTIVE)
        with pytest.raises(InvalidConnectionTransitionError):
            s1._transition_to(ConnectionState.NEW)

        # Test REJECTED terminal freeze
        s2 = ConnectionSupervisor(context=_make_context(connection_id="c2"), controller=controller)
        await s2.close()  # close from NEW transitions to REJECTED
        assert s2.state == ConnectionState.REJECTED
        with pytest.raises(InvalidConnectionTransitionError):
            s2._transition_to(ConnectionState.AUTHENTICATING)

    asyncio.run(_run())


# ==============================================================================
# Group 2: Admission Integration & Capacity Leases
# ==============================================================================


def test_admit_success_transitions_to_admitted() -> None:
    """Item 11: admit() transitions NEW -> AUTHENTICATING -> ADMITTED and stores lease."""

    async def _run() -> None:
        ctx = _make_context()
        controller = _make_controller()
        supervisor = ConnectionSupervisor(context=ctx, controller=controller)

        admitted = await supervisor.admit()
        assert admitted is True
        assert _get_state(supervisor) == ConnectionState.ADMITTED
        assert supervisor.lease is not None
        assert supervisor.lease.is_active is True
        assert controller.snapshot().active_connections == 1

        await supervisor.close()
        assert controller.snapshot().active_connections == 0

    asyncio.run(_run())


def test_admit_global_capacity_rejection() -> None:
    """Item 12: admit() transitions to REJECTED when global capacity is exceeded."""

    async def _run() -> None:
        controller = _make_controller(max_concurrent=1, max_per_tenant=1)
        ctx1 = _make_context(connection_id="conn_01", tenant_id="tenant_a")
        ctx2 = _make_context(connection_id="conn_02", tenant_id="tenant_b")

        s1 = ConnectionSupervisor(context=ctx1, controller=controller)
        assert await s1.admit() is True

        s2 = ConnectionSupervisor(context=ctx2, controller=controller)
        admitted = await s2.admit()
        assert admitted is False
        assert s2.state == ConnectionState.REJECTED
        assert s2.outcome is not None
        assert s2.outcome.state == ConnectionState.REJECTED
        assert s2.outcome.reason == TerminalReason.ADMISSION_REJECTED
        assert s2.lease is None
        assert s2.is_closed is True

        await s1.close()

    asyncio.run(_run())


def test_admit_tenant_capacity_rejection() -> None:
    """Item 13: admit() transitions to REJECTED when tenant capacity is exceeded."""

    async def _run() -> None:
        controller = _make_controller(max_concurrent=10, max_per_tenant=1)
        ctx1 = _make_context(connection_id="conn_01", tenant_id="tenant_x")
        ctx2 = _make_context(connection_id="conn_02", tenant_id="tenant_x")

        s1 = ConnectionSupervisor(context=ctx1, controller=controller)
        assert await s1.admit() is True

        s2 = ConnectionSupervisor(context=ctx2, controller=controller)
        assert await s2.admit() is False
        assert s2.state == ConnectionState.REJECTED
        assert s2.outcome is not None
        assert s2.outcome.reason == TerminalReason.ADMISSION_REJECTED

        await s1.close()

    asyncio.run(_run())


def test_cannot_call_admit_twice_or_from_non_new() -> None:
    """Item 14: Calling admit() when not in NEW raises InvalidConnectionTransitionError."""

    async def _run() -> None:
        ctx = _make_context()
        controller = _make_controller()
        supervisor = ConnectionSupervisor(context=ctx, controller=controller)

        assert await supervisor.admit() is True
        with pytest.raises(
            InvalidConnectionTransitionError, match=r"admit\(\) can only be called from NEW"
        ):
            await supervisor.admit()

        await supervisor.close()

    asyncio.run(_run())


def test_cannot_call_admit_when_constructed_from_lease() -> None:
    """Item 15: Supervisor constructed from_lease starts in ADMITTED, cannot call admit()."""

    async def _run() -> None:
        ctx = _make_context()
        controller = _make_controller()
        lease = await controller.acquire(ctx)
        supervisor = ConnectionSupervisor.from_lease(lease)

        with pytest.raises(
            InvalidConnectionTransitionError, match=r"admit\(\) can only be called from NEW"
        ):
            await supervisor.admit()

        await supervisor.close()

    asyncio.run(_run())


def test_admit_unexpected_exception_rejection() -> None:
    """Item 16: Unexpected exception during acquire transitions to REJECTED and re-raises."""

    async def _run() -> None:
        ctx = _make_context()
        controller = _make_controller()

        async def _failing_acquire(context: VerifiedConnectionContext) -> AdmissionLease:
            raise RuntimeError("Unexpected internal crash")

        controller.acquire = _failing_acquire  # type: ignore[method-assign]

        supervisor = ConnectionSupervisor(context=ctx, controller=controller)
        with pytest.raises(RuntimeError, match="Unexpected internal crash"):
            await supervisor.admit()

        assert _get_state(supervisor) == ConnectionState.REJECTED
        assert supervisor.outcome is not None
        assert supervisor.outcome.reason == TerminalReason.STARTUP_FAILED

    asyncio.run(_run())


def test_stale_callback_isolation() -> None:
    """Item 17: Re-admitting same connection_id with new supervisor is isolated from old supervisor."""

    async def _run() -> None:
        ctx = _make_context(connection_id="conn_01")
        controller = _make_controller()

        s1 = ConnectionSupervisor(context=ctx, controller=controller)
        await s1.admit()
        assert controller.snapshot().active_connections == 1

        outcome1 = await s1.close()
        assert outcome1.state == ConnectionState.CLOSED
        assert controller.snapshot().active_connections == 0

        # New supervisor with identical connection_id
        s2 = ConnectionSupervisor(context=ctx, controller=controller)
        await s2.admit()
        assert controller.snapshot().active_connections == 1

        # Calling close on old supervisor s1 again does not affect s2
        outcome1_again = await s1.close()
        assert outcome1_again is outcome1
        assert controller.snapshot().active_connections == 1
        assert s2.state == ConnectionState.ADMITTED

        await s2.close()
        assert controller.snapshot().active_connections == 0

    asyncio.run(_run())


# ==============================================================================
# Group 3: Task Ownership & Worker Supervision
# ==============================================================================


def test_register_resource_validation() -> None:
    """Item 18: register_resource validates callback type and prevents registration when closing/terminal."""
    ctx = _make_context()
    controller = _make_controller()
    supervisor = ConnectionSupervisor(context=ctx, controller=controller)

    with pytest.raises(TypeError, match="close_fn must be a callable"):
        supervisor.register_resource("not_callable")  # type: ignore[arg-type]


def test_start_validation_and_state_guard() -> None:
    """Item 19: start() requires at least one worker and requires ADMITTED state."""

    async def _run() -> None:
        ctx = _make_context()
        controller = _make_controller()
        supervisor = ConnectionSupervisor(context=ctx, controller=controller)

        # Calling start without workers
        with pytest.raises(ValueError, match="At least one worker factory must be provided"):
            await supervisor.start()

        # Calling start while in NEW state
        async def dummy() -> None:
            pass

        with pytest.raises(
            InvalidConnectionTransitionError, match=r"start\(\) requires ADMITTED state"
        ):
            await supervisor.start(dummy)

        await supervisor.close()

    asyncio.run(_run())


def test_workers_normal_completion_triggers_normal_close() -> None:
    """Item 20: When all worker tasks complete normally, supervisor transitions to CLOSED."""

    async def _run() -> None:
        ctx = _make_context()
        controller = _make_controller()
        supervisor = ConnectionSupervisor(context=ctx, controller=controller)
        await supervisor.admit()

        worker_run_count = 0

        async def worker1() -> None:
            nonlocal worker_run_count
            worker_run_count += 1
            await asyncio.sleep(0.01)

        async def worker2() -> None:
            nonlocal worker_run_count
            worker_run_count += 1
            await asyncio.sleep(0.02)

        await supervisor.start(worker1, worker2)
        assert _get_state(supervisor) == ConnectionState.ACTIVE

        outcome = await supervisor.wait()
        assert outcome.state == ConnectionState.CLOSED
        assert outcome.reason == TerminalReason.NORMAL
        assert worker_run_count == 2
        assert _get_state(supervisor) == ConnectionState.CLOSED
        assert controller.snapshot().active_connections == 0

    asyncio.run(_run())


def test_worker_unhandled_exception_cancels_siblings() -> None:
    """Item 21: An unhandled exception in one worker cancels sibling tasks and marks FAILED."""

    async def _run() -> None:
        ctx = _make_context()
        controller = _make_controller()
        supervisor = ConnectionSupervisor(context=ctx, controller=controller)
        await supervisor.admit()

        sibling_cancelled = False

        async def failing_worker() -> None:
            await asyncio.sleep(0.01)
            raise ValueError("Critical worker failure")

        async def sibling_worker() -> None:
            nonlocal sibling_cancelled
            try:
                await asyncio.sleep(10.0)
            except asyncio.CancelledError:
                sibling_cancelled = True
                raise

        await supervisor.start(failing_worker, sibling_worker)
        outcome = await supervisor.wait()

        assert outcome.state == ConnectionState.FAILED
        assert outcome.reason == TerminalReason.WORKER_FAILED
        assert outcome.error_summary == "Worker coroutine raised unhandled exception"
        assert sibling_cancelled is True
        assert _get_state(supervisor) == ConnectionState.FAILED
        assert controller.snapshot().active_connections == 0

    asyncio.run(_run())


def test_zero_orphan_tasks_on_explicit_close() -> None:
    """Item 22: Closing supervisor cancels all running worker tasks with zero orphan tasks."""

    async def _run() -> None:
        ctx = _make_context()
        controller = _make_controller()
        supervisor = ConnectionSupervisor(context=ctx, controller=controller)
        await supervisor.admit()

        worker_tasks_cancelled = 0

        async def long_worker() -> None:
            nonlocal worker_tasks_cancelled
            try:
                await asyncio.sleep(60.0)
            except asyncio.CancelledError:
                worker_tasks_cancelled += 1
                raise

        await supervisor.start(long_worker, long_worker, long_worker)
        assert _get_state(supervisor) == ConnectionState.ACTIVE

        outcome = await supervisor.close(reason=TerminalReason.NORMAL)
        assert outcome.state == ConnectionState.CLOSED
        assert outcome.reason == TerminalReason.NORMAL
        assert worker_tasks_cancelled == 3
        assert controller.snapshot().active_connections == 0

    asyncio.run(_run())


def test_cannot_register_resource_after_shutdown() -> None:
    """Item 23: Cannot register async resource once supervisor is CLOSING or terminal."""

    async def _run() -> None:
        ctx = _make_context()
        controller = _make_controller()
        supervisor = ConnectionSupervisor(context=ctx, controller=controller)
        await supervisor.admit()
        await supervisor.close()

        async def dummy_cleanup() -> None:
            pass

        with pytest.raises(
            ConnectionLifecycleError, match="Cannot register resources on a connection in CLOSED"
        ):
            supervisor.register_resource(dummy_cleanup)

    asyncio.run(_run())


# ==============================================================================
# Group 4: Cleanup, Resources & Cancellation Resilience
# ==============================================================================


def test_close_idempotent_and_stable_outcome() -> None:
    """Item 24: Calling close() multiple times returns the exact same outcome instance."""

    async def _run() -> None:
        ctx = _make_context()
        controller = _make_controller()
        supervisor = ConnectionSupervisor(context=ctx, controller=controller)
        await supervisor.admit()

        outcome1 = await supervisor.close()
        outcome2 = await supervisor.close()
        outcome3 = await supervisor.close()

        assert outcome1 is outcome2
        assert outcome2 is outcome3
        assert outcome1.state == ConnectionState.CLOSED
        assert controller.snapshot().active_connections == 0

    asyncio.run(_run())


def test_concurrent_close_invocations() -> None:
    """Item 25: Multiple concurrent tasks calling close() all receive the identical outcome."""

    async def _run() -> None:
        ctx = _make_context()
        controller = _make_controller()
        supervisor = ConnectionSupervisor(context=ctx, controller=controller)
        await supervisor.admit()

        async def call_close() -> ConnectionOutcome:
            return await supervisor.close()

        results = await asyncio.gather(*[call_close() for _ in range(10)])
        first = results[0]
        for res in results:
            assert res is first
        assert controller.snapshot().active_connections == 0

    asyncio.run(_run())


def test_close_from_new_and_authenticating_states() -> None:
    """Item 26: Closing in NEW or AUTHENTICATING transitions cleanly to REJECTED."""

    async def _run() -> None:
        controller = _make_controller()

        # Close from NEW
        s1 = ConnectionSupervisor(context=_make_context(connection_id="c1"), controller=controller)
        outcome1 = await s1.close()
        assert outcome1.state == ConnectionState.REJECTED
        assert outcome1.reason == TerminalReason.CANCELLED

        # Close from AUTHENTICATING
        s2 = ConnectionSupervisor(context=_make_context(connection_id="c2"), controller=controller)
        s2._transition_to(ConnectionState.AUTHENTICATING)
        outcome2 = await s2.close(reason=TerminalReason.NORMAL)
        assert outcome2.state == ConnectionState.REJECTED
        assert outcome2.reason == TerminalReason.CANCELLED

    asyncio.run(_run())


def test_cancellation_during_close_guarantees_lease_release() -> None:
    """Item 27: Cancelling the caller task awaiting close() does NOT stop lease release."""

    async def _run() -> None:
        ctx = _make_context()
        controller = _make_controller()
        supervisor = ConnectionSupervisor(context=ctx, controller=controller)
        await supervisor.admit()
        assert controller.snapshot().active_connections == 1

        # Register a resource that takes a short moment to close
        async def slow_resource() -> None:
            await asyncio.sleep(0.05)

        supervisor.register_resource(slow_resource)

        # Spawn an outer task that calls close()
        caller_task = asyncio.create_task(supervisor.close())
        await asyncio.sleep(0.01)

        # Cancel caller task while close() is in progress
        caller_task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await caller_task

        # Background shielded cleanup must finish and release capacity
        outcome = await supervisor.wait()
        assert outcome.state == ConnectionState.CLOSED
        assert controller.snapshot().active_connections == 0

    asyncio.run(_run())


def test_repeated_cancellation_resilience() -> None:
    """Item 28: Repeated cancellations in caller task cannot prevent lease release."""

    async def _run() -> None:
        ctx = _make_context()
        controller = _make_controller()
        supervisor = ConnectionSupervisor(context=ctx, controller=controller)
        await supervisor.admit()
        assert controller.snapshot().active_connections == 1

        async def slow_cleanup() -> None:
            await asyncio.sleep(0.05)

        supervisor.register_resource(slow_cleanup)

        async def canceling_caller() -> None:
            for _ in range(5):
                try:
                    await supervisor.close()
                except asyncio.CancelledError:
                    pass

        task = asyncio.create_task(canceling_caller())
        for _ in range(4):
            await asyncio.sleep(0.005)
            task.cancel()

        await supervisor.wait()
        assert controller.snapshot().active_connections == 0

    asyncio.run(_run())


def test_resource_teardown_lifo_order() -> None:
    """Item 29: Registered async resources are torn down in strict reverse (LIFO) order."""

    async def _run() -> None:
        ctx = _make_context()
        controller = _make_controller()
        supervisor = ConnectionSupervisor(context=ctx, controller=controller)
        await supervisor.admit()

        order: list[str] = []

        async def r1() -> None:
            order.append("r1")

        async def r2() -> None:
            order.append("r2")

        async def r3() -> None:
            order.append("r3")

        supervisor.register_resource(r1)
        supervisor.register_resource(r2)
        supervisor.register_resource(r3)

        await supervisor.close()
        assert order == ["r3", "r2", "r1"]
        assert controller.snapshot().active_connections == 0

    asyncio.run(_run())


def test_resource_teardown_exception_isolation() -> None:
    """Item 30: Exception in one resource does not block other resources or lease release."""

    async def _run() -> None:
        ctx = _make_context()
        controller = _make_controller()
        supervisor = ConnectionSupervisor(context=ctx, controller=controller)
        await supervisor.admit()

        r1_executed = False

        async def r1() -> None:
            nonlocal r1_executed
            r1_executed = True

        async def failing_resource() -> None:
            raise RuntimeError("Teardown error in resource")

        supervisor.register_resource(r1)
        supervisor.register_resource(failing_resource)

        outcome = await supervisor.close()
        assert r1_executed is True
        assert outcome.state == ConnectionState.FAILED
        assert outcome.reason == TerminalReason.CLEANUP_FAILED
        assert controller.snapshot().active_connections == 0

    asyncio.run(_run())


def test_resource_teardown_timeout() -> None:
    """Item 31: A hanging resource times out and marks FAILED while releasing capacity."""

    async def _run() -> None:
        ctx = _make_context()
        controller = _make_controller()
        policy = LifecyclePolicy(cleanup_timeout=0.05)
        supervisor = ConnectionSupervisor(context=ctx, controller=controller, policy=policy)
        await supervisor.admit()

        async def hanging_resource() -> None:
            await asyncio.sleep(10.0)

        supervisor.register_resource(hanging_resource)

        outcome = await supervisor.close()
        assert outcome.state == ConnectionState.FAILED
        assert outcome.reason == TerminalReason.CLEANUP_FAILED
        assert controller.snapshot().active_connections == 0

    asyncio.run(_run())


def test_async_context_manager_normal() -> None:
    """Item 32: Async context manager closes supervisor with NORMAL on clean exit."""

    async def _run() -> None:
        ctx = _make_context()
        controller = _make_controller()

        async with ConnectionSupervisor(context=ctx, controller=controller) as supervisor:
            await supervisor.admit()
            assert _get_state(supervisor) == ConnectionState.ADMITTED

        assert _get_state(supervisor) == ConnectionState.CLOSED
        assert supervisor.outcome is not None
        assert supervisor.outcome.reason == TerminalReason.NORMAL
        assert controller.snapshot().active_connections == 0

    asyncio.run(_run())


def test_async_context_manager_exception() -> None:
    """Item 33: Async context manager closes supervisor with WORKER_FAILED on exception."""

    async def _run() -> None:
        ctx = _make_context()
        controller = _make_controller()

        with pytest.raises(ValueError, match="Block failure"):
            async with ConnectionSupervisor(context=ctx, controller=controller) as supervisor:
                await supervisor.admit()
                raise ValueError("Block failure")

        assert _get_state(supervisor) == ConnectionState.FAILED
        assert supervisor.outcome is not None
        assert supervisor.outcome.reason == TerminalReason.WORKER_FAILED
        assert controller.snapshot().active_connections == 0

    asyncio.run(_run())


def test_async_context_manager_cancellation() -> None:
    """Item 34: Async context manager closes supervisor with CANCELLED on task cancellation."""

    async def _run() -> None:
        ctx = _make_context()
        controller = _make_controller()
        supervisor_ref: ConnectionSupervisor | None = None

        async def inside_block() -> None:
            nonlocal supervisor_ref
            async with ConnectionSupervisor(context=ctx, controller=controller) as supervisor:
                supervisor_ref = supervisor
                await supervisor.admit()
                await asyncio.sleep(10.0)

        task = asyncio.create_task(inside_block())
        await asyncio.sleep(0.01)
        task.cancel()

        with pytest.raises(asyncio.CancelledError):
            await task

        assert supervisor_ref is not None
        assert supervisor_ref.state == ConnectionState.CLOSED
        assert supervisor_ref.outcome is not None
        assert supervisor_ref.outcome.reason == TerminalReason.CANCELLED
        assert controller.snapshot().active_connections == 0

    asyncio.run(_run())


# ==============================================================================
# Group 5: Privacy, Repr & Core Exports
# ==============================================================================


def test_supervisor_and_outcome_privacy() -> None:
    """Item 35: Supervisor and Outcome string representations do not leak secrets or credentials."""
    ctx = _make_context(
        connection_id="conn_sensitive_01",
        tenant_id="tenant_secret_corp",
    )
    controller = _make_controller()
    supervisor = ConnectionSupervisor(context=ctx, controller=controller)

    rep = repr(supervisor)
    assert "conn_sensitive_01" not in rep
    assert "tenant_secret_corp" not in rep
    assert rep == "ConnectionSupervisor(state='NEW')"

    outcome = ConnectionOutcome(
        state=ConnectionState.FAILED,
        reason=TerminalReason.WORKER_FAILED,
        error_summary="Sanitized error description",
    )
    out_rep = repr(outcome)
    assert "conn_sensitive" not in out_rep
    assert "Sanitized error description" in out_rep


def test_core_package_exports_all_lifecycle_symbols() -> None:
    """Item 36: Verify all lifecycle symbols are cleanly exported by kaiwen_gateway.core."""
    import kaiwen_gateway.core as core

    expected_symbols = [
        "ConnectionLifecycleError",
        "InvalidConnectionTransitionError",
        "LifecycleTimeoutError",
        "ConnectionState",
        "TerminalReason",
        "ConnectionOutcome",
        "LifecyclePolicy",
        "ConnectionSupervisor",
    ]

    for sym in expected_symbols:
        assert hasattr(core, sym), f"kaiwen_gateway.core missing {sym}"
        assert sym in core.__all__, f"{sym} not listed in core.__all__"


def test_supervisor_policy_property() -> None:
    """Verify supervisor policy property returns configured policy."""
    ctx = _make_context()
    controller = _make_controller()
    custom_policy = LifecyclePolicy(startup_timeout=1.0, cleanup_timeout=2.0)
    supervisor = ConnectionSupervisor(context=ctx, controller=controller, policy=custom_policy)
    assert supervisor.policy is custom_policy
    assert supervisor.policy.startup_timeout == 1.0


def test_admit_startup_timeout_rejection() -> None:
    """Verify admit() transitions to REJECTED with TIMEOUT when acquire hangs."""

    async def _run() -> None:
        ctx = _make_context()
        controller = _make_controller()

        async def _hanging_acquire(context: VerifiedConnectionContext) -> AdmissionLease:
            await asyncio.sleep(10.0)
            raise RuntimeError("Should not be reached")

        controller.acquire = _hanging_acquire  # type: ignore[method-assign]
        policy = LifecyclePolicy(startup_timeout=0.02)
        supervisor = ConnectionSupervisor(context=ctx, controller=controller, policy=policy)

        admitted = await supervisor.admit()
        assert admitted is False
        assert _get_state(supervisor) == ConnectionState.REJECTED
        assert supervisor.outcome is not None
        assert supervisor.outcome.state == ConnectionState.REJECTED
        assert supervisor.outcome.reason == TerminalReason.TIMEOUT
        assert supervisor.outcome.error_summary == "Admission acquisition timed out"

    asyncio.run(_run())


def test_worker_termination_exceeds_cleanup_timeout() -> None:
    """Verify uncooperative worker that ignores cancellation triggers cleanup timeout."""

    async def _run() -> None:
        ctx = _make_context()
        controller = _make_controller()
        policy = LifecyclePolicy(cleanup_timeout=0.02)
        supervisor = ConnectionSupervisor(context=ctx, controller=controller, policy=policy)
        await supervisor.admit()

        async def stubborn_worker() -> None:
            try:
                await asyncio.sleep(10.0)
            except asyncio.CancelledError:
                # Stubbornly sleep past cleanup deadline
                await asyncio.sleep(1.0)

        await supervisor.start(stubborn_worker)
        outcome = await supervisor.close()

        assert outcome.state == ConnectionState.FAILED
        assert outcome.reason == TerminalReason.TIMEOUT
        assert outcome.error_summary == "Worker termination exceeded cleanup deadline"

    asyncio.run(_run())


def test_lease_release_retry_and_error_handling() -> None:
    """Verify retry loop when lease release encounters transient exception."""

    async def _run() -> None:
        ctx = _make_context()
        controller = _make_controller()
        supervisor = ConnectionSupervisor(context=ctx, controller=controller)
        await supervisor.admit()
        assert supervisor.lease is not None

        attempt = 0
        original_release = controller._release_lease

        async def _failing_release(lease: AdmissionLease) -> bool:
            nonlocal attempt
            attempt += 1
            if attempt == 1:
                raise RuntimeError("Transient release glitch")
            return await original_release(lease)

        controller._release_lease = _failing_release  # type: ignore[method-assign]

        outcome = await supervisor.close()
        assert attempt >= 2
        assert outcome.state == ConnectionState.CLOSED
        assert controller.snapshot().active_connections == 0

    asyncio.run(_run())


def test_close_called_while_already_closing() -> None:
    """Verify second caller entering close() while state is CLOSING awaits cleanup task."""

    async def _run() -> None:
        ctx = _make_context()
        controller = _make_controller()
        supervisor = ConnectionSupervisor(context=ctx, controller=controller)
        await supervisor.admit()

        closing_event = asyncio.Event()

        async def slow_resource() -> None:
            closing_event.set()
            await asyncio.sleep(0.05)

        supervisor.register_resource(slow_resource)

        task1 = asyncio.create_task(supervisor.close())
        await closing_event.wait()
        assert _get_state(supervisor) == ConnectionState.CLOSING

        # Task 2 calls close while state is CLOSING
        outcome2 = await supervisor.close()
        outcome1 = await task1

        assert outcome1 is outcome2
        assert outcome1.state == ConnectionState.CLOSED
        assert controller.snapshot().active_connections == 0

    asyncio.run(_run())


def test_close_when_lease_already_released_manually() -> None:
    """Verify close handles lease already released before close() was called."""

    async def _run() -> None:
        ctx = _make_context()
        controller = _make_controller()
        supervisor = ConnectionSupervisor(context=ctx, controller=controller)
        await supervisor.admit()
        assert supervisor.lease is not None
        assert _is_lease_active(supervisor.lease) is True

        # Manually release lease before calling close
        await supervisor.lease.release()
        assert _is_lease_active(supervisor.lease) is False

        outcome = await supervisor.close()
        assert outcome.state == ConnectionState.CLOSED
        assert controller.snapshot().active_connections == 0

    asyncio.run(_run())


def test_close_when_lease_release_fails_all_retries() -> None:
    """Verify close records CLEANUP_FAILED when lease release permanently fails."""

    async def _run() -> None:
        ctx = _make_context()
        controller = _make_controller()
        supervisor = ConnectionSupervisor(context=ctx, controller=controller)
        await supervisor.admit()

        async def _always_failing_release(lease: AdmissionLease) -> bool:
            raise RuntimeError("Permanent controller release failure")

        controller._release_lease = _always_failing_release  # type: ignore[method-assign]

        outcome = await supervisor.close()
        assert outcome.state == ConnectionState.FAILED
        assert outcome.reason == TerminalReason.CLEANUP_FAILED
        assert outcome.error_summary == "Failed to release admission lease"

    asyncio.run(_run())


def test_execute_cleanup_with_no_lease() -> None:
    """Verify _execute_cleanup handles a scenario where lease is None."""

    async def _run() -> None:
        ctx = _make_context()
        controller = _make_controller()
        supervisor = ConnectionSupervisor(context=ctx, controller=controller)
        supervisor._transition_to(ConnectionState.AUTHENTICATING)
        await supervisor._execute_cleanup(
            target_terminal=ConnectionState.REJECTED,
            reason=TerminalReason.CANCELLED,
            error_summary=None,
        )
        assert _get_state(supervisor) == ConnectionState.REJECTED

    asyncio.run(_run())


def test_wait_closed_alias() -> None:
    """Verify wait_closed() behaves identically to wait()."""

    async def _run() -> None:
        ctx = _make_context()
        controller = _make_controller()
        supervisor = ConnectionSupervisor(context=ctx, controller=controller)
        await supervisor.admit()

        close_task = asyncio.create_task(supervisor.close())
        outcome = await supervisor.wait_closed()
        assert outcome.state == ConnectionState.CLOSED
        await close_task

    asyncio.run(_run())


def test_start_rejects_non_callable_factory() -> None:
    """Verify start() rejects worker factory that is not callable."""

    async def _run() -> None:
        ctx = _make_context()
        controller = _make_controller()
        supervisor = ConnectionSupervisor(context=ctx, controller=controller)
        await supervisor.admit()

        with pytest.raises(TypeError, match="Worker factory at index 0 must be a callable"):
            await supervisor.start("not_callable")  # type: ignore[arg-type]

        await supervisor.close()

    asyncio.run(_run())


def test_start_rejects_factory_returning_non_coroutine() -> None:
    """Verify start() rejects factory that returns non-coroutine."""

    async def _run() -> None:
        ctx = _make_context()
        controller = _make_controller()
        supervisor = ConnectionSupervisor(context=ctx, controller=controller)
        await supervisor.admit()

        def sync_worker() -> None:
            return None

        with pytest.raises(TypeError, match="must return a coroutine"):
            await supervisor.start(sync_worker)  # type: ignore[arg-type]

        await supervisor.close()

    asyncio.run(_run())


def test_start_rejects_factory_raising_exception() -> None:
    """Verify start() wraps factory exception in ConnectionLifecycleError."""

    async def _run() -> None:
        ctx = _make_context()
        controller = _make_controller()
        supervisor = ConnectionSupervisor(context=ctx, controller=controller)
        await supervisor.admit()

        def crashing_factory() -> Coroutine[Any, Any, None]:
            raise RuntimeError("Factory setup failed")

        with pytest.raises(ConnectionLifecycleError, match="failed during invocation"):
            await supervisor.start(crashing_factory)

        assert _get_state(supervisor) == ConnectionState.FAILED
        assert supervisor.outcome is not None
        assert supervisor.outcome.reason == TerminalReason.STARTUP_FAILED
        assert controller.snapshot().active_connections == 0

    asyncio.run(_run())


def test_start_rejects_second_factory_raising_exception_and_closes_prior_coros() -> None:
    """Verify start() closes prior created coroutines if later factory raises."""

    async def _run() -> None:
        ctx = _make_context()
        controller = _make_controller()
        supervisor = ConnectionSupervisor(context=ctx, controller=controller)
        await supervisor.admit()

        async def valid_worker() -> None:
            pass

        def crashing_factory() -> Any:
            raise RuntimeError("Second factory failed")

        with pytest.raises(ConnectionLifecycleError, match="Worker factory at index 1 failed"):
            await supervisor.start(valid_worker, crashing_factory)

        assert _get_state(supervisor) == ConnectionState.FAILED
        assert supervisor.outcome is not None
        assert supervisor.outcome.reason == TerminalReason.STARTUP_FAILED
        assert controller.snapshot().active_connections == 0

    asyncio.run(_run())


def test_start_rejects_second_factory_returning_non_coroutine() -> None:
    """Verify start() closes prior created coroutines if later factory returns non-coro."""

    async def _run() -> None:
        ctx = _make_context()
        controller = _make_controller()
        supervisor = ConnectionSupervisor(context=ctx, controller=controller)
        await supervisor.admit()

        async def valid_worker() -> None:
            pass

        def non_coro_factory() -> Any:
            return 12345

        with pytest.raises(TypeError, match="Worker factory at index 1 must return a coroutine"):
            await supervisor.start(valid_worker, non_coro_factory)

        assert _get_state(supervisor) == ConnectionState.FAILED
        assert supervisor.outcome is not None
        assert supervisor.outcome.reason == TerminalReason.STARTUP_FAILED
        assert controller.snapshot().active_connections == 0

    asyncio.run(_run())


def test_execute_cleanup_catastrophic_exception_fail_safe(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify emergency fail-safe in _execute_cleanup when unexpected exception occurs."""

    def _exploding_transition(self: ConnectionSupervisor, target: ConnectionState) -> None:
        raise RuntimeError("Catastrophic state transition failure")

    monkeypatch.setattr(ConnectionSupervisor, "_transition_to", _exploding_transition)

    async def _run() -> None:
        ctx = _make_context()
        controller = _make_controller()
        supervisor = ConnectionSupervisor(context=ctx, controller=controller)

        await supervisor._execute_cleanup(
            target_terminal=ConnectionState.FAILED,
            reason=TerminalReason.WORKER_FAILED,
            error_summary="Explosion test",
        )

        assert supervisor.state == ConnectionState.FAILED
        assert supervisor.outcome is not None
        assert supervisor.outcome.state == ConnectionState.FAILED
        assert supervisor.outcome.reason == TerminalReason.CLEANUP_FAILED
        assert supervisor.outcome.error_summary == "Unexpected error in cleanup supervisor"

    asyncio.run(_run())


def test_execute_cleanup_keyboard_interrupt_re_raised() -> None:
    """Verify KeyboardInterrupt is re-raised and never swallowed by cleanup."""

    async def _run() -> None:
        ctx = _make_context()
        controller = _make_controller()
        supervisor = ConnectionSupervisor(context=ctx, controller=controller)
        await supervisor.admit()

        async def exploding_resource() -> None:
            raise KeyboardInterrupt()

        supervisor.register_resource(exploding_resource)
        await supervisor.close()

    with pytest.raises(KeyboardInterrupt):
        asyncio.run(_run())


def test_cleanup_task_cancellation_explicit() -> None:
    """Verify _execute_cleanup sets CANCELLED outcome when its own task is cancelled."""

    async def _run() -> None:
        ctx = _make_context()
        controller = _make_controller()
        supervisor = ConnectionSupervisor(context=ctx, controller=controller)
        await supervisor.admit()

        in_cleanup_event = asyncio.Event()

        async def slow_cleanup() -> None:
            in_cleanup_event.set()
            await asyncio.sleep(10.0)

        supervisor.register_resource(slow_cleanup)

        # Start close
        close_caller = asyncio.create_task(supervisor.close())
        await in_cleanup_event.wait()

        # Cancel the internal cleanup task directly
        assert supervisor._cleanup_task is not None
        supervisor._cleanup_task.cancel()

        with pytest.raises(asyncio.CancelledError):
            await close_caller

        assert _get_state(supervisor) == ConnectionState.CLOSED
        assert supervisor.outcome is not None
        assert supervisor.outcome.state == ConnectionState.CLOSED
        assert supervisor.outcome.reason == TerminalReason.CANCELLED

    asyncio.run(_run())


def test_cleanup_deadline_exceeded_skips_further_resources() -> None:
    """Verify resources are not called when cleanup deadline is already exceeded."""

    async def _run() -> None:
        ctx = _make_context()
        controller = _make_controller()
        # Set tiny cleanup timeout
        policy = LifecyclePolicy(cleanup_timeout=0.01)
        supervisor = ConnectionSupervisor(context=ctx, controller=controller, policy=policy)
        await supervisor.admit()

        second_resource_called = False

        async def slow_first_resource() -> None:
            await asyncio.sleep(0.05)

        async def second_resource() -> None:
            nonlocal second_resource_called
            second_resource_called = True

        supervisor.register_resource(second_resource)
        supervisor.register_resource(slow_first_resource)

        outcome = await supervisor.close()
        assert outcome.state == ConnectionState.FAILED
        assert outcome.reason in (TerminalReason.CLEANUP_FAILED, TerminalReason.TIMEOUT)
        assert second_resource_called is False
        assert controller.snapshot().active_connections == 0

    asyncio.run(_run())
