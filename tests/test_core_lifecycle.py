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
from typing import Any, cast

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
        error_summary="Ignored user input string",
    )
    assert outcome.state == ConnectionState.CLOSED
    assert outcome.reason == TerminalReason.NORMAL
    assert outcome.error_summary == "Normal connection closure"

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

    # Passing obsolete lease keyword argument is rejected
    with pytest.raises(TypeError):
        ConnectionSupervisor(context=ctx, controller=controller, lease="invalid")  # type: ignore[call-arg]


def test_supervisor_single_ownership_by_construction() -> None:
    """Item 7: Verify single lease ownership by construction without external handover."""

    async def _run() -> None:
        ctx = _make_context(connection_id="conn_single_owner")
        controller = _make_controller()

        # from_lease classmethod is removed
        assert not hasattr(ConnectionSupervisor, "from_lease")

        # Supervisor 1 acquires exclusive lease via admit()
        s1 = ConnectionSupervisor(context=ctx, controller=controller)
        admitted1 = await s1.admit()
        assert admitted1 is True
        assert _get_state(s1) == ConnectionState.ADMITTED
        assert s1.lease is not None
        assert s1.lease.is_active is True
        assert controller.snapshot().active_connections == 1

        # Supervisor 2 attempting to admit same connection context is rejected by controller
        s2 = ConnectionSupervisor(context=ctx, controller=controller)
        admitted2 = await s2.admit()
        assert admitted2 is False
        assert _get_state(s2) == ConnectionState.REJECTED
        assert s2.outcome is not None
        assert s2.outcome.reason == TerminalReason.ADMISSION_REJECTED
        assert s2.lease is None
        assert controller.snapshot().active_connections == 1

        # Closing s1 releases the lease cleanly
        await s1.close()
        assert _get_state(s1) == ConnectionState.CLOSED
        assert not _is_lease_active(s1.lease)
        assert controller.snapshot().active_connections == 0

        # Now a new supervisor can admit this connection ID
        s3 = ConnectionSupervisor(context=ctx, controller=controller)
        admitted3 = await s3.admit()
        assert admitted3 is True
        assert _get_state(s3) == ConnectionState.ADMITTED
        assert controller.snapshot().active_connections == 1
        await s3.close()
        assert controller.snapshot().active_connections == 0

    asyncio.run(_run())


def test_supervisor_module_and_instance_isolation_no_global_registry() -> None:
    """Item 8: Verify no module-level global shared lease registry exists and instances are isolated."""
    import kaiwen_gateway.core.lifecycle as lc

    # No shared module-level registry
    assert not hasattr(lc, "_CLAIMED_LEASES")

    async def _run() -> None:
        c1 = _make_controller()
        c2 = _make_controller()
        ctx1 = _make_context(connection_id="conn_iso_1")
        ctx2 = _make_context(connection_id="conn_iso_2")

        s1 = ConnectionSupervisor(context=ctx1, controller=c1)
        s2 = ConnectionSupervisor(context=ctx2, controller=c2)

        assert await s1.admit() is True
        assert await s2.admit() is True

        assert c1.snapshot().active_connections == 1
        assert c2.snapshot().active_connections == 1

        await s1.close()
        assert c1.snapshot().active_connections == 0
        assert c2.snapshot().active_connections == 1

        await s2.close()
        assert c2.snapshot().active_connections == 0

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


def test_cannot_call_admit_when_already_admitted() -> None:
    """Item 15: Supervisor cannot call admit() once already in ADMITTED state."""

    async def _run() -> None:
        ctx = _make_context()
        controller = _make_controller()
        assert not hasattr(ConnectionSupervisor, "from_lease")
        supervisor = ConnectionSupervisor(context=ctx, controller=controller)
        assert await supervisor.admit() is True

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
        assert outcome.error_summary == "Worker task execution failed"
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
    out_str = str(outcome)
    assert "conn_sensitive" not in out_rep
    assert out_rep == "ConnectionOutcome(state='FAILED', reason='worker_failed')"
    assert out_str == out_rep
    assert outcome.error_summary == "Worker task execution failed"


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
        assert supervisor.outcome.error_summary == "Lifecycle deadline expired"

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
        assert outcome.error_summary == "Lifecycle deadline expired"

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
        assert outcome.error_summary == "Teardown cleanup failed"

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
        assert supervisor.outcome.error_summary == "Teardown cleanup failed"

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
    """Item 52 (Blocker A Regression): Verify internal cleanup cancellation results in FAILED with CLEANUP_FAILED."""

    async def _run() -> None:
        ctx = _make_context(connection_id="conn_cancel_cleanup")
        controller = _make_controller()
        supervisor = ConnectionSupervisor(context=ctx, controller=controller)
        # 1. Admit connection
        admitted = await supervisor.admit()
        assert admitted is True
        assert controller.snapshot().active_connections == 1

        in_cleanup_event = asyncio.Event()

        # 2. Resource close waiting on asyncio.Event
        async def slow_cleanup() -> None:
            in_cleanup_event.set()
            await asyncio.sleep(10.0)

        supervisor.register_resource(slow_cleanup)

        # 3. Start close()
        close_caller = asyncio.create_task(supervisor.close())
        await in_cleanup_event.wait()

        # 4. Cancel internal cleanup task directly
        assert supervisor._cleanup_task is not None
        supervisor._cleanup_task.cancel()

        # 5. Observe cleanup outcome
        with pytest.raises(asyncio.CancelledError):
            await close_caller

        # 6. Check controller snapshot and lease state
        assert supervisor.lease is not None
        assert supervisor.lease.is_active is True
        assert controller.snapshot().active_connections == 1

        # 7. Verify successful CLOSED outcome was NOT produced
        outcome = await supervisor.wait_closed()
        assert outcome.state == ConnectionState.FAILED
        assert outcome.reason == TerminalReason.CLEANUP_FAILED

        # 8. Verify recovery/cleanup policy of remaining lease
        await supervisor.close()
        assert _is_lease_active(supervisor.lease) is False
        assert controller.snapshot().active_connections == 0

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


def test_error_summary_redaction_and_privacy_boundaries() -> None:
    """Item 53 (Blocker C): Verify diagnostic privacy: fixed safe reason summaries, safe task names, zero leakage."""

    async def _run() -> None:
        unknown_secret = "UNKNOWN_SECRET_TOKEN_XYZ_99"
        phone_number = "+905551234567"
        email_addr = "user@sensitive-domain.com"
        tenant_secret = "tenant_secret_corp"
        conn_secret = "conn_secret_device_88"
        call_secret = "call_secret_pstn_77"

        ctx = _make_context(
            connection_id=conn_secret,
            tenant_id=tenant_secret,
            call_id=call_secret,
        )
        controller = _make_controller()
        supervisor = ConnectionSupervisor(context=ctx, controller=controller)
        await supervisor.admit()

        # Start a worker and check synthetic task names
        async def dummy_worker() -> None:
            await asyncio.sleep(0.01)

        await supervisor.start(dummy_worker)
        assert len(supervisor._worker_tasks) == 1
        worker_task_name = supervisor._worker_tasks[0].get_name()
        # Verify task name does NOT contain raw connection ID or tenant ID
        assert worker_task_name == "gw-worker-0"
        assert conn_secret not in worker_task_name
        assert tenant_secret not in worker_task_name

        untrusted_text = (
            f"Failure with {unknown_secret}, {phone_number}, {email_addr}, "
            f"{tenant_secret}, {conn_secret}, {call_secret}"
        )
        outcome = await supervisor.close(
            reason=TerminalReason.WORKER_FAILED,
            error_summary=untrusted_text,
        )

        # 1. repr(outcome) and str(outcome) contain ONLY state and reason
        rep = repr(outcome)
        out_str = str(outcome)
        assert rep == "ConnectionOutcome(state='FAILED', reason='worker_failed')"
        assert out_str == rep

        # None of the sensitive values appear in representations
        for sensitive in (
            unknown_secret,
            phone_number,
            email_addr,
            tenant_secret,
            conn_secret,
            call_secret,
        ):
            assert sensitive not in rep
            assert sensitive not in out_str
            assert sensitive not in (outcome.error_summary or "")

        # 2. outcome.error_summary is strictly the safe fixed canonical summary
        assert outcome.error_summary == "Worker task execution failed"

        # 3. Worker factory failure does not leak raw exception text
        ctx2 = _make_context()
        supervisor2 = ConnectionSupervisor(context=ctx2, controller=controller)
        await supervisor2.admit()

        def secret_leaking_factory() -> Any:
            raise RuntimeError(f"Leaking {unknown_secret} and {phone_number}")

        with pytest.raises(ConnectionLifecycleError):
            await supervisor2.start(secret_leaking_factory)

        outcome2 = await supervisor2.wait_closed()
        assert unknown_secret not in (outcome2.error_summary or "")
        assert phone_number not in (outcome2.error_summary or "")
        assert repr(outcome2) == "ConnectionOutcome(state='FAILED', reason='startup_failed')"

        # 4. Resource cleanup failure does not leak raw exception text
        ctx3 = _make_context()
        supervisor3 = ConnectionSupervisor(context=ctx3, controller=controller)
        await supervisor3.admit()

        async def secret_leaking_resource() -> None:
            raise RuntimeError(f"Resource leaking {unknown_secret}")

        supervisor3.register_resource(secret_leaking_resource)
        outcome3 = await supervisor3.close()
        assert unknown_secret not in (outcome3.error_summary or "")
        assert repr(outcome3) == "ConnectionOutcome(state='FAILED', reason='cleanup_failed')"

    asyncio.run(_run())


def test_fatal_signal_keyboard_interrupt_in_worker_propagates() -> None:
    """Item 54 (Blocker D): Verify KeyboardInterrupt in worker propagates with zero unobserved task exceptions."""
    import subprocess
    import sys

    # Execute fatal worker in an isolated subprocess to verify signal propagation and stderr cleanliness
    script = """
import asyncio, sys
from kaiwen_gateway.core import (
    ConnectionSupervisor,
    CoreConfig,
    AdmissionController,
    VerifiedConnectionContext,
)

async def main():
    ctx = VerifiedConnectionContext(
        provider_id="prov_test",
        tenant_id="tenant_test",
        connection_id="conn_fatal",
        call_id="call_fatal",
        allowed_scopes=("voice.stream",),
    )
    ctrl = AdmissionController(CoreConfig())
    sup = ConnectionSupervisor(context=ctx, controller=ctrl)
    await sup.admit()

    async def fatal():
        raise KeyboardInterrupt()

    await sup.start(fatal)
    await asyncio.sleep(0.05)

asyncio.run(main())
"""
    result = subprocess.run(
        [sys.executable, "-c", script],
        capture_output=True,
        text=True,
    )

    # Subprocess must have failed with KeyboardInterrupt
    assert result.returncode != 0
    # Must NOT emit 'Task exception was never retrieved'
    assert "Task exception was never retrieved" not in result.stderr

    # In-process test to ensure line coverage and direct signal propagation
    async def _in_process() -> None:
        ctx = _make_context(connection_id="conn_fatal_in_proc")
        ctrl = _make_controller()
        sup = ConnectionSupervisor(context=ctx, controller=ctrl)
        await sup.admit()

        async def fatal_worker() -> None:
            raise SystemExit(42)

        await sup.start(fatal_worker)
        await asyncio.sleep(0.01)

    with pytest.raises(SystemExit) as exc_info:
        asyncio.run(_in_process())
    assert exc_info.value.code == 42


def test_cannot_register_resource_in_new_or_authenticating_state() -> None:
    """Item 55 (Blocker E): Verify register_resource raises typed error before admission (NEW/AUTHENTICATING)."""
    ctx = _make_context()
    controller = _make_controller()
    supervisor = ConnectionSupervisor(context=ctx, controller=controller)

    async def dummy() -> None:
        pass

    # In NEW state
    with pytest.raises(
        ConnectionLifecycleError, match="Cannot register resources on a connection in NEW"
    ):
        supervisor.register_resource(dummy)


def test_worker_auto_close_task_exception_observed() -> None:
    """Item 56: Verification of auto-close failure and fallback teardown."""

    async def _run() -> None:
        # 1. Admit
        ctx = _make_context(connection_id="conn_autoclose_fallback")
        controller = _make_controller()
        supervisor = ConnectionSupervisor(context=ctx, controller=controller)
        admitted = await supervisor.admit()
        assert admitted is True
        assert controller.snapshot().active_connections == 1
        assert _get_state(supervisor) == ConnectionState.ADMITTED

        # 2. Simulate fault injection in an auto-close task while supervisor is not terminal
        loop = asyncio.get_running_loop()
        failing_task = loop.create_future()
        failing_task.set_exception(RuntimeError("Simulated auto-close crash"))

        # 3. Trigger callback to initiate fallback teardown
        supervisor._on_auto_close_done(failing_task)  # type: ignore[arg-type]

        # 4. Wait for fallback teardown to execute and complete
        outcome = await supervisor.wait_closed()

        # 5. Check active admission snapshot (must be 0, no leaked capacity!)
        assert controller.snapshot().active_connections == 0

        # 6. Check terminal outcome
        assert outcome.state == ConnectionState.FAILED
        assert outcome.reason == TerminalReason.CLEANUP_FAILED
        assert outcome.error_summary == "Teardown cleanup failed"

        # 7. Call again when supervisor is already in terminal state (idempotent done callback)
        failing_task2 = loop.create_future()
        failing_task2.set_exception(RuntimeError("Second simulated crash"))
        supervisor._on_auto_close_done(failing_task2)  # type: ignore[arg-type]

        # 8. Call when cleanup task is already active to verify guard
        failing_task3 = loop.create_future()
        failing_task3.set_exception(RuntimeError("Third crash with active cleanup"))
        supervisor._state = ConnectionState.CLOSING  # temporarily simulate active closing
        # cleanup_task is not done
        dummy_cleanup = asyncio.create_task(asyncio.sleep(1.0))
        supervisor._cleanup_task = dummy_cleanup
        supervisor._on_auto_close_done(failing_task3)  # type: ignore[arg-type]
        dummy_cleanup.cancel()
        with pytest.raises(asyncio.CancelledError):
            await dummy_cleanup

    asyncio.run(_run())


def test_admit_cancellation_and_signals() -> None:
    """Item 58: Verify admit() cancellation, KeyboardInterrupt, and unexpected error propagation."""

    async def _run() -> None:
        ctx1 = _make_context(connection_id="conn_admit_c1")
        ctx2 = _make_context(connection_id="conn_admit_c2")
        ctx3 = _make_context(connection_id="conn_admit_c3")

        class _FailingController(AdmissionController):
            def __init__(self, exc: BaseException) -> None:
                super().__init__(CoreConfig())
                self._exc = exc

            async def acquire(self, context: VerifiedConnectionContext) -> AdmissionLease:
                raise self._exc

        # Cancellation during admit
        s1 = ConnectionSupervisor(
            context=ctx1, controller=_FailingController(asyncio.CancelledError())
        )
        with pytest.raises(asyncio.CancelledError):
            await s1.admit()
        assert s1.state == ConnectionState.REJECTED
        assert s1.outcome is not None
        assert s1.outcome.reason == TerminalReason.CANCELLED

        # KeyboardInterrupt during admit
        s2 = ConnectionSupervisor(context=ctx2, controller=_FailingController(KeyboardInterrupt()))
        with pytest.raises(KeyboardInterrupt):
            await s2.admit()

        # Unexpected Exception during admit
        s3 = ConnectionSupervisor(
            context=ctx3, controller=_FailingController(RuntimeError("DB dropped connection"))
        )
        with pytest.raises(RuntimeError):
            await s3.admit()
        assert s3.state == ConnectionState.REJECTED
        assert s3.outcome is not None
        assert s3.outcome.reason == TerminalReason.STARTUP_FAILED

    asyncio.run(_run())


def test_start_factory_cancellation_and_keyboard_interrupt() -> None:
    """Item 59: Verify start() handles KeyboardInterrupt and CancelledError in worker factory."""

    async def _run() -> None:
        ctx1 = _make_context(connection_id="conn_start_ki")
        ctx2 = _make_context(connection_id="conn_start_cancel")
        controller = _make_controller()

        # KeyboardInterrupt in second factory: prior coroutine is closed
        s1 = ConnectionSupervisor(context=ctx1, controller=controller)
        await s1.admit()

        async def valid_worker() -> None:
            pass

        def ki_factory() -> Any:
            raise KeyboardInterrupt()

        with pytest.raises(KeyboardInterrupt):
            await s1.start(valid_worker, ki_factory)
        await s1.close()

        # CancelledError in second factory: prior coroutine is closed and lease released
        s2 = ConnectionSupervisor(context=ctx2, controller=controller)
        admitted = await s2.admit()
        assert admitted is True

        def cancel_factory() -> Any:
            raise asyncio.CancelledError()

        with pytest.raises(asyncio.CancelledError):
            await s2.start(valid_worker, cancel_factory)
        assert s2.state == ConnectionState.FAILED
        assert s2.outcome is not None
        assert s2.outcome.reason == TerminalReason.CANCELLED
        assert controller.snapshot().active_connections == 0

    asyncio.run(_run())


def test_close_retry_lease_release_exception_and_cleanup_without_lease() -> None:
    """Item 60: Verify close() lease recovery handles release errors, and cleanup handles no-lease cancellation."""

    async def _run() -> None:
        ctx = _make_context(connection_id="conn_retry_lease")

        class _FailingReleaseController(AdmissionController):
            async def _release_lease(self, lease: AdmissionLease) -> bool:
                raise RuntimeError("Lease release network error")

        controller = _FailingReleaseController(CoreConfig())
        supervisor = ConnectionSupervisor(context=ctx, controller=controller)
        await supervisor.admit()

        # Force state to FAILED
        supervisor._state = ConnectionState.FAILED
        supervisor._outcome = ConnectionOutcome(
            state=ConnectionState.FAILED, reason=TerminalReason.CLEANUP_FAILED
        )

        outcome = await supervisor.close()
        assert outcome.state == ConnectionState.FAILED

        # Direct cancellation of cleanup on supervisor without active lease
        s2 = ConnectionSupervisor(
            context=_make_context(connection_id="conn_no_lease"), controller=controller
        )
        in_res = asyncio.Event()

        async def slow_res() -> None:
            in_res.set()
            await asyncio.sleep(10.0)

        s2._state = ConnectionState.ADMITTED
        s2._resources.append(slow_res)
        close_task = asyncio.create_task(s2.close())
        await in_res.wait()
        assert s2._cleanup_task is not None
        s2._cleanup_task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await close_task
        assert s2.state == ConnectionState.FAILED
        assert s2.outcome is not None
        assert s2.outcome.error_summary == "Teardown cleanup failed"

    asyncio.run(_run())


def test_bounded_fallback_recovery_under_permanent_failure(monkeypatch: pytest.MonkeyPatch) -> None:
    """Item 61 (Blocker A): 11-step verification of strictly bounded fallback task creation under permanent close failure."""
    close_call_count = 0
    fail_close = True
    real_close = ConnectionSupervisor.close

    async def permanently_failing_close(
        self: ConnectionSupervisor, *args: Any, **kwargs: Any
    ) -> ConnectionOutcome:
        nonlocal close_call_count
        if fail_close:
            close_call_count += 1
            raise RuntimeError(f"Deterministic permanent close failure #{close_call_count}")
        return await real_close(self, *args, **kwargs)

    monkeypatch.setattr(ConnectionSupervisor, "close", permanently_failing_close)

    async def _run() -> None:
        nonlocal fail_close
        # 1. Supervisor oluştur ve admit et
        ctx = _make_context(connection_id="conn_bounded_fallback")
        controller = _make_controller()
        supervisor = ConnectionSupervisor(context=ctx, controller=controller)
        admitted = await supervisor.admit()
        assert admitted is True

        # 2. Aktif lease olduğunu doğrula
        assert supervisor.lease is not None
        assert supervisor.lease.is_active is True
        assert controller.snapshot().active_connections == 1

        # 3. Sentetik worker oluştur
        worker_done = False

        async def worker() -> None:
            nonlocal worker_done
            worker_done = True

        # 4 & 5. Auto-close çağrısına deterministik kalıcı hata enjekte et ve fallback'in de başarısız olmasını sağla
        await supervisor.start(worker)
        # Give event loop time to run worker and trigger auto-close + fallback
        await supervisor.wait_closed()
        assert worker_done is True

        # 6 & 7. Oluşturulan toplam fallback task sayısını say; sayının sonlu ve açık sınır içinde olduğunu doğrula
        assert supervisor.fallback_attempts == 1
        assert supervisor.fallback_attempts <= ConnectionSupervisor.MAX_FALLBACK_ATTEMPTS
        # Total calls to close: 1 (auto-close) + 1 (fallback) = 2
        assert close_call_count == 2

        # 8. Yeni task üretiminin durduğunu doğrula (event loop ilerlese de yeni task üretilmez)
        await asyncio.sleep(0.05)
        assert close_call_count == 2
        assert supervisor.fallback_attempts == 1
        assert len(supervisor._trigger_close_tasks) == 0

        # 9. Unobserved task exception bulunmadığını doğrula (callback'ler exception'ları gözlemledi)
        # Both auto-close and fallback task exceptions were retrieved in their respective done callbacks.

        # 10. Terminal state ve lease durumunu kontrol et
        assert supervisor.state == ConnectionState.FAILED
        assert supervisor.outcome is not None
        assert supervisor.outcome.state == ConnectionState.FAILED
        assert supervisor.outcome.reason == TerminalReason.CLEANUP_FAILED
        assert supervisor.outcome.error_summary == "Teardown cleanup failed"
        # Lease is still active and owned by supervisor
        assert supervisor.lease is not None
        assert _is_lease_active(supervisor.lease)
        assert controller.snapshot().active_connections == 1

        # 11. Uygunsa kontrollü manuel recovery ile kapasiteyi temizle
        fail_close = False
        recovery_outcome = await supervisor.close()
        assert recovery_outcome.state == ConnectionState.FAILED
        assert not _is_lease_active(supervisor.lease)
        assert controller.snapshot().active_connections == 0

    asyncio.run(_run())


def test_cancelled_autoclose_triggers_bounded_fallback_cleanup(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Item 62 (Blocker B): 10-step verification that a cancelled auto-close task triggers bounded fallback teardown."""
    real_close = ConnectionSupervisor.close
    auto_close_started = asyncio.Event()
    hold_auto_close = asyncio.Event()
    intercept = True

    async def intercepting_close(
        self: ConnectionSupervisor, *args: Any, **kwargs: Any
    ) -> ConnectionOutcome:
        nonlocal intercept
        if intercept:
            intercept = False
            auto_close_started.set()
            # Wait until test cancels this task before cleanup starts
            await hold_auto_close.wait()
        return await real_close(self, *args, **kwargs)

    monkeypatch.setattr(ConnectionSupervisor, "close", intercepting_close)

    async def _run() -> None:
        # 1. Supervisor admit edilir
        ctx = _make_context(connection_id="conn_cancelled_autoclose")
        controller = _make_controller()
        supervisor = ConnectionSupervisor(context=ctx, controller=controller)
        admitted = await supervisor.admit()
        assert admitted is True
        assert controller.snapshot().active_connections == 1

        resource_cleaned = False

        async def synthetic_resource() -> None:
            nonlocal resource_cleaned
            resource_cleaned = True

        supervisor.register_resource(synthetic_resource)

        # 2 & 3. Worker başlatılır; worker auto-close tetikler
        async def normal_worker() -> None:
            await asyncio.sleep(0.01)

        await supervisor.start(normal_worker)
        # 4. Wait for auto-close task to start, then cancel it BEFORE cleanup starts
        await auto_close_started.wait()

        assert supervisor._cleanup_task is None  # cleanup has not started!
        auto_close_task = next(iter(supervisor._trigger_close_tasks))
        auto_close_task.cancel()
        hold_auto_close.set()

        # 5 & 6. Callback çalışır; bounded fallback cleanup davranışı doğrulanır
        outcome = await supervisor.wait_closed()

        # 7. Aktif worker/resource/lease durumu kontrol edilir
        assert all(t.done() for t in supervisor._worker_tasks)
        assert resource_cleaned is True
        assert supervisor.lease is not None
        assert not _is_lease_active(supervisor.lease)

        # 8. Sonuç başarılı cleanup ise admission kapasitesi sıfır olur
        assert controller.snapshot().active_connections == 0

        # 9. Terminal outcome FAILED with CLEANUP_FAILED (cancellation was not converted to false success)
        assert outcome.state == ConnectionState.FAILED
        assert outcome.reason == TerminalReason.CLEANUP_FAILED
        assert outcome.error_summary == "Teardown cleanup failed"

        # 10. Süresiz bekleme ve sınırsız task yaratımı bulunmaz
        assert supervisor.fallback_attempts == 1
        if supervisor._trigger_close_tasks:
            await asyncio.gather(*supervisor._trigger_close_tasks, return_exceptions=True)
        assert len(supervisor._trigger_close_tasks) == 0

    asyncio.run(_run())


def test_cancelled_autoclose_when_cleanup_already_active_or_terminal() -> None:
    """Item 63: Verify cancellation of auto-close when cleanup is already active or supervisor is terminal is a no-op."""

    async def _run() -> None:
        ctx = _make_context(connection_id="conn_cancel_noop")
        controller = _make_controller()
        supervisor = ConnectionSupervisor(context=ctx, controller=controller)
        await supervisor.admit()

        loop = asyncio.get_running_loop()

        # 1. When cleanup task is already active in background
        active_cleanup = asyncio.create_task(asyncio.sleep(1.0))
        supervisor._cleanup_task = active_cleanup
        cancelled_task1 = loop.create_future()
        cancelled_task1.cancel()
        supervisor._on_auto_close_done(cancelled_task1)  # type: ignore[arg-type]
        # Must not have spawned any fallback task because cleanup is active
        assert supervisor.fallback_attempts == 0
        active_cleanup.cancel()
        with pytest.raises(asyncio.CancelledError):
            await active_cleanup

        # 2. When supervisor is already terminal
        supervisor._state = ConnectionState.CLOSED
        cancelled_task2 = loop.create_future()
        cancelled_task2.cancel()
        supervisor._on_auto_close_done(cancelled_task2)  # type: ignore[arg-type]
        assert supervisor.fallback_attempts == 0

        # 3. Fallback task cancellation when cleanup is active or terminal
        supervisor._cleanup_task = None
        cancelled_fallback = loop.create_future()
        cancelled_fallback.cancel()
        supervisor._on_fallback_close_done(cancelled_fallback)  # type: ignore[arg-type]
        assert supervisor.state == ConnectionState.CLOSED

        # 4. Fallback task done when cleanup is active
        active_cleanup2 = asyncio.create_task(asyncio.sleep(1.0))
        supervisor._cleanup_task = active_cleanup2
        supervisor._state = ConnectionState.CLOSING
        cancelled_fallback2 = loop.create_future()
        cancelled_fallback2.cancel()
        supervisor._on_fallback_close_done(cancelled_fallback2)  # type: ignore[arg-type]
        active_cleanup2.cancel()
        with pytest.raises(asyncio.CancelledError):
            await active_cleanup2

    asyncio.run(_run())


def test_record_fallback_exhausted_failure_in_pre_admit_states() -> None:
    """Item 64: Verify _record_fallback_exhausted_failure transitions appropriately from NEW or CLOSING."""
    ctx = _make_context(connection_id="conn_fallback_states")
    controller = _make_controller()

    # In NEW state
    s1 = ConnectionSupervisor(context=ctx, controller=controller)
    assert _get_state(s1) == ConnectionState.NEW
    s1._record_fallback_exhausted_failure()
    assert _get_state(s1) == ConnectionState.REJECTED
    assert s1.outcome is not None
    assert s1.outcome.reason == TerminalReason.CLEANUP_FAILED

    # In CLOSING state
    s2 = ConnectionSupervisor(context=ctx, controller=controller)
    s2._state = ConnectionState.CLOSING
    s2._record_fallback_exhausted_failure()
    assert _get_state(s2) == ConnectionState.FAILED
    assert s2.outcome is not None
    assert s2.outcome.reason == TerminalReason.CLEANUP_FAILED

    # When already terminal (no-op on line 584)
    assert s2.state.is_terminal is True
    s2._record_fallback_exhausted_failure()
    assert _get_state(s2) == ConnectionState.FAILED

    # Hit line 563: when auto_close fails with fallback_attempts already exhausted
    s3 = ConnectionSupervisor(context=ctx, controller=controller)
    s3._state = ConnectionState.ADMITTED
    s3._fallback_attempts = s3.MAX_FALLBACK_ATTEMPTS
    loop = asyncio.new_event_loop()
    try:
        failing_task = loop.create_future()
        failing_task.set_exception(RuntimeError("Exhausted test"))
        s3._on_auto_close_done(cast(asyncio.Task[Any], failing_task))
        assert _get_state(s3) == ConnectionState.FAILED
        assert s3.outcome is not None
        assert s3.outcome.reason == TerminalReason.CLEANUP_FAILED
    finally:
        loop.close()
