"""Unit, concurrency, race condition, and invariant tests for M2.3 Core Admission Control.

These tests verify:
- FR-CONN-005: Enforce admission controls and configurable global/per-tenant concurrency ceilings.
- FR-CONN-006: Proper resource release on normal disconnect, error, and cancellation.
- SEC-003 / SEC-004: Server-side tenant quotas and aggregate resource budgets.
- NFR-REL-002: Rejection rather than unbounded allocation when capacity is exhausted.
- ENG-004: Concurrency races, duplicate connection attempts, and cross-tenant isolation.
"""

from __future__ import annotations

import asyncio
from dataclasses import FrozenInstanceError
from pathlib import Path
from typing import Final

import pytest
from mypy import api as mypy_api

from kaiwen_gateway.core import (
    AdmissionCapacityExceededError,
    AdmissionController,
    AdmissionError,
    AdmissionLease,
    AdmissionSnapshot,
    ConnectionId,
    CoreConfig,
    CoreError,
    DuplicateConnectionAdmissionError,
    GlobalCapacityExceededError,
    InvalidLeaseError,
    TenantCapacityExceededError,
    TenantId,
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


def _is_active(lease: AdmissionLease) -> bool:
    """Helper to query lease activity state across mutating calls without type narrowing."""
    return lease.is_active


def test_empty_controller_initial_state_and_snapshot() -> None:
    """Item 1: Empty AdmissionController has zero active connections and full capacity."""
    config = CoreConfig(max_concurrent_connections=10, max_connections_per_tenant=5)
    controller = AdmissionController(config)

    assert controller.max_concurrent_connections == 10
    assert controller.max_connections_per_tenant == 5

    snapshot = controller.snapshot()
    assert isinstance(snapshot, AdmissionSnapshot)
    assert snapshot.active_connections == 0
    assert snapshot.available_capacity == 10
    assert snapshot.max_concurrent_connections == 10
    assert snapshot.max_connections_per_tenant == 5
    assert snapshot.active_tenants == 0

    assert controller.active_connections_for_tenant("tenant_alpha") == 0
    assert controller.active_connections_for_tenant(TenantId("tenant_alpha")) == 0
    assert controller.has_active_connection("conn_01") is False
    assert controller.has_active_connection(ConnectionId("conn_01")) is False
    assert "AdmissionController(active=0, max_global=10, max_per_tenant=5)" in repr(controller)


def test_successful_first_admission_and_lease_attributes() -> None:
    """Item 2: Successful admission grants a valid AdmissionLease with matching properties."""

    async def _run() -> None:
        controller = AdmissionController(max_concurrent_connections=5, max_connections_per_tenant=2)
        ctx = _make_context(
            connection_id="conn_01",
            tenant_id="tenant_alpha",
            provider_id="provider_a",
            call_id="call_01",
        )

        lease = await controller.acquire(ctx)
        assert isinstance(lease, AdmissionLease)
        assert lease.connection_id == "conn_01"
        assert lease.tenant_id == "tenant_alpha"
        assert lease.provider_id == "provider_a"
        assert lease.call_id == "call_01"
        assert lease.controller is controller
        assert lease.context is ctx
        assert lease.is_active is True
        assert controller.has_active_connection("conn_01") is True
        assert controller.is_active_lease(lease) is True
        assert "status='active'" in repr(lease)

        snapshot = controller.snapshot()
        assert snapshot.active_connections == 1
        assert snapshot.available_capacity == 4
        assert snapshot.active_tenants == 1
        assert controller.active_connections_for_tenant("tenant_alpha") == 1

    asyncio.run(_run())


def test_global_capacity_limit_rejection() -> None:
    """Item 3: Global connection capacity exhaustion raises GlobalCapacityExceededError."""

    async def _run() -> None:
        controller = AdmissionController(max_concurrent_connections=2, max_connections_per_tenant=2)
        ctx1 = _make_context(connection_id="conn_01", tenant_id="tenant_a")
        ctx2 = _make_context(connection_id="conn_02", tenant_id="tenant_b")
        ctx3 = _make_context(connection_id="conn_03", tenant_id="tenant_c")

        l1 = await controller.acquire(ctx1)
        l2 = await controller.acquire(ctx2)
        assert l1.is_active and l2.is_active

        with pytest.raises(GlobalCapacityExceededError) as exc_info:
            await controller.acquire(ctx3)

        err = exc_info.value
        assert isinstance(err, AdmissionCapacityExceededError)
        assert isinstance(err, AdmissionError)
        assert isinstance(err, CoreError)
        assert err.limit == 2
        assert err.current == 2
        assert "Global connection capacity exhausted: 2/2 active" in str(err)

        # Invariant: Failed admission leaves controller state untouched
        snapshot = controller.snapshot()
        assert snapshot.active_connections == 2
        assert snapshot.available_capacity == 0
        assert controller.has_active_connection("conn_03") is False

    asyncio.run(_run())


def test_tenant_capacity_limit_rejection() -> None:
    """Item 4: Tenant connection capacity exhaustion raises TenantCapacityExceededError."""

    async def _run() -> None:
        controller = AdmissionController(
            max_concurrent_connections=10, max_connections_per_tenant=2
        )
        ctx1 = _make_context(connection_id="conn_01", tenant_id="tenant_alpha")
        ctx2 = _make_context(connection_id="conn_02", tenant_id="tenant_alpha")
        ctx3 = _make_context(connection_id="conn_03", tenant_id="tenant_alpha")

        await controller.acquire(ctx1)
        await controller.acquire(ctx2)

        with pytest.raises(TenantCapacityExceededError) as exc_info:
            await controller.acquire(ctx3)

        err = exc_info.value
        assert isinstance(err, AdmissionCapacityExceededError)
        assert err.tenant_id == "tenant_alpha"
        assert err.limit == 2
        assert err.current == 2
        assert "Tenant connection capacity exhausted for 'tenant_alpha': 2/2 active" in str(err)

        # Invariant: Global capacity is not fully consumed, but tenant quota blocks
        snapshot = controller.snapshot()
        assert snapshot.active_connections == 2
        assert snapshot.available_capacity == 8
        assert controller.active_connections_for_tenant("tenant_alpha") == 2
        assert controller.has_active_connection("conn_03") is False

    asyncio.run(_run())


def test_combined_global_and_tenant_limits_reference_scenario() -> None:
    """Item 5: Reference scenario from §5 (global=3, tenant_a=2, tenant_b=2)."""

    async def _run() -> None:
        controller = AdmissionController(max_concurrent_connections=3, max_connections_per_tenant=2)

        ctx_a1 = _make_context(connection_id="a_01", tenant_id="tenant_a")
        ctx_a2 = _make_context(connection_id="a_02", tenant_id="tenant_a")
        ctx_a3 = _make_context(connection_id="a_03", tenant_id="tenant_a")
        ctx_b1 = _make_context(connection_id="b_01", tenant_id="tenant_b")
        ctx_b2 = _make_context(connection_id="b_02", tenant_id="tenant_b")

        # Tenant A acquires 2 connections
        l_a1 = await controller.acquire(ctx_a1)
        l_a2 = await controller.acquire(ctx_a2)

        # Tenant A attempts a 3rd connection: rejected by tenant limit (global has 1 slot left)
        with pytest.raises(TenantCapacityExceededError):
            await controller.acquire(ctx_a3)
        assert controller.snapshot().active_connections == 2

        # Tenant B acquires 1 connection: global now reaches 3/3
        l_b1 = await controller.acquire(ctx_b1)
        assert controller.snapshot().active_connections == 3
        assert controller.snapshot().available_capacity == 0

        # Tenant B attempts 2nd connection: rejected by global capacity limit!
        with pytest.raises(GlobalCapacityExceededError):
            await controller.acquire(ctx_b2)

        # Releasing one connection from A frees 1 slot globally and 1 slot for tenant A
        assert await l_a1.release() is True
        assert controller.snapshot().active_connections == 2
        assert controller.snapshot().available_capacity == 1
        assert controller.active_connections_for_tenant("tenant_a") == 1

        # Now Tenant B can successfully acquire its 2nd connection
        l_b2 = await controller.acquire(ctx_b2)
        assert l_b2.is_active is True
        assert controller.snapshot().active_connections == 3
        assert controller.active_connections_for_tenant("tenant_b") == 2

        # Cleanup remaining
        await l_a2.release()
        await l_b1.release()
        await l_b2.release()
        assert controller.snapshot().active_connections == 0
        assert controller.snapshot().active_tenants == 0

    asyncio.run(_run())


def test_duplicate_connection_id_rejection() -> None:
    """Item 7: An active connection_id cannot hold a second concurrent lease."""

    async def _run() -> None:
        controller = AdmissionController(
            max_concurrent_connections=10, max_connections_per_tenant=5
        )
        ctx1 = _make_context(connection_id="conn_dup", tenant_id="tenant_a")
        ctx2 = _make_context(connection_id="conn_dup", tenant_id="tenant_a")

        lease1 = await controller.acquire(ctx1)
        assert lease1.is_active is True

        with pytest.raises(DuplicateConnectionAdmissionError) as exc_info:
            await controller.acquire(ctx2)

        assert exc_info.value.connection_id == "conn_dup"
        assert "already holds an active capacity lease" in str(exc_info.value)

        # Invariant: Counters are not incremented on rejection
        assert controller.snapshot().active_connections == 1
        assert controller.active_connections_for_tenant("tenant_a") == 1
        assert lease1.is_active is True

    asyncio.run(_run())


def test_duplicate_connection_id_across_different_tenants_rejection() -> None:
    """Item 8: Attempting to reuse an active connection_id with a different tenant is rejected."""

    async def _run() -> None:
        controller = AdmissionController(
            max_concurrent_connections=10, max_connections_per_tenant=5
        )
        ctx_a = _make_context(connection_id="conn_shared", tenant_id="tenant_a")
        ctx_b = _make_context(connection_id="conn_shared", tenant_id="tenant_b")

        lease_a = await controller.acquire(ctx_a)
        assert lease_a.is_active is True

        with pytest.raises(DuplicateConnectionAdmissionError):
            await controller.acquire(ctx_b)

        # Invariant: tenant_b counter remains 0, global counter remains 1
        assert controller.active_connections_for_tenant("tenant_b") == 0
        assert controller.active_connections_for_tenant("tenant_a") == 1
        assert controller.snapshot().active_connections == 1

    asyncio.run(_run())


def test_same_call_id_with_different_connection_ids_allowed() -> None:
    """Item 9: Same call_id in separate transport connection attempts is admitted."""

    async def _run() -> None:
        controller = AdmissionController(max_concurrent_connections=5, max_connections_per_tenant=5)
        ctx1 = _make_context(connection_id="attempt_01", call_id="call_x", tenant_id="tenant_a")
        ctx2 = _make_context(connection_id="attempt_02", call_id="call_x", tenant_id="tenant_a")

        l1 = await controller.acquire(ctx1)
        l2 = await controller.acquire(ctx2)

        assert l1.is_active and l2.is_active
        assert controller.snapshot().active_connections == 2
        assert controller.active_connections_for_tenant("tenant_a") == 2

    asyncio.run(_run())


def test_cross_tenant_quota_isolation() -> None:
    """Item 10: Usage by Tenant A does not reduce the per-tenant capacity available to Tenant B."""

    async def _run() -> None:
        controller = AdmissionController(
            max_concurrent_connections=10, max_connections_per_tenant=2
        )
        ctx_a1 = _make_context(connection_id="a1", tenant_id="tenant_a")
        ctx_a2 = _make_context(connection_id="a2", tenant_id="tenant_a")

        await controller.acquire(ctx_a1)
        await controller.acquire(ctx_a2)

        # Tenant B still has full 2 slots available
        ctx_b1 = _make_context(connection_id="b1", tenant_id="tenant_b")
        ctx_b2 = _make_context(connection_id="b2", tenant_id="tenant_b")
        lb1 = await controller.acquire(ctx_b1)
        lb2 = await controller.acquire(ctx_b2)

        assert lb1.is_active and lb2.is_active
        assert controller.active_connections_for_tenant("tenant_b") == 2

    asyncio.run(_run())


def test_two_independent_controllers_isolation() -> None:
    """Item 11: Two AdmissionController instances maintain completely isolated state."""

    async def _run() -> None:
        c1 = AdmissionController(max_concurrent_connections=1, max_connections_per_tenant=1)
        c2 = AdmissionController(max_concurrent_connections=1, max_connections_per_tenant=1)

        ctx = _make_context(connection_id="conn_common", tenant_id="tenant_a")
        l1 = await c1.acquire(ctx)
        l2 = await c2.acquire(ctx)

        assert l1.is_active and l2.is_active
        assert c1.snapshot().active_connections == 1
        assert c2.snapshot().active_connections == 1

        # c1 is full, but releasing l1 does not affect c2
        await l1.release()
        assert c1.snapshot().active_connections == 0
        assert c2.snapshot().active_connections == 1

    asyncio.run(_run())


def test_invalid_context_type_rejected() -> None:
    """Item 12: acquire() and admit() reject invalid context types with TypeError."""
    controller = AdmissionController()

    with pytest.raises(TypeError, match="must be a VerifiedConnectionContext instance"):
        asyncio.run(controller.acquire("not_a_context"))  # type: ignore[arg-type]

    async def _bad_admit() -> None:
        async with controller.admit(123):  # type: ignore[arg-type]
            pass

    with pytest.raises(TypeError, match="must be a VerifiedConnectionContext instance"):
        asyncio.run(_bad_admit())


def test_normal_release_behavior() -> None:
    """Item 13: Normal release decrements counters and marks lease inactive."""

    async def _run() -> None:
        controller = AdmissionController(max_concurrent_connections=5, max_connections_per_tenant=5)
        ctx = _make_context(connection_id="conn_rel", tenant_id="tenant_rel")

        lease = await controller.acquire(ctx)
        assert _is_active(lease) is True
        assert controller.snapshot().active_connections == 1

        result = await lease.release()
        assert result is True
        assert _is_active(lease) is False
        assert controller.snapshot().active_connections == 0
        assert controller.active_connections_for_tenant("tenant_rel") == 0
        assert controller.has_active_connection("conn_rel") is False
        assert controller.is_active_lease(lease) is False
        assert "status='released'" in repr(lease)

    asyncio.run(_run())


def test_double_and_multiple_release_idempotence() -> None:
    """Item 14: Sequential duplicate release calls return False and never decrement below zero."""

    async def _run() -> None:
        controller = AdmissionController(max_concurrent_connections=5, max_connections_per_tenant=5)
        ctx = _make_context(connection_id="conn_idem", tenant_id="tenant_idem")

        lease = await controller.acquire(ctx)
        assert await lease.release() is True
        assert await lease.release() is False
        assert await lease.release() is False

        snapshot = controller.snapshot()
        assert snapshot.active_connections == 0
        assert snapshot.available_capacity == 5
        assert controller.active_connections_for_tenant("tenant_idem") == 0

    asyncio.run(_run())


def test_concurrent_double_release_exactly_once_semantics() -> None:
    """Item 15: Concurrent release calls guarantee exactly one True and no counter underflow."""

    async def _run() -> None:
        controller = AdmissionController(max_concurrent_connections=5, max_connections_per_tenant=5)
        ctx = _make_context(connection_id="conn_concur_rel", tenant_id="tenant_x")
        lease = await controller.acquire(ctx)

        results = await asyncio.gather(
            lease.release(),
            lease.release(),
            lease.release(),
            lease.release(),
        )
        assert results.count(True) == 1
        assert results.count(False) == 3
        assert controller.snapshot().active_connections == 0
        assert controller.active_connections_for_tenant("tenant_x") == 0

    asyncio.run(_run())


def test_foreign_lease_rejection() -> None:
    """Item 16: An AdmissionLease from controller A cannot be released on controller B."""

    async def _run() -> None:
        c1 = AdmissionController(max_concurrent_connections=5, max_connections_per_tenant=5)
        c2 = AdmissionController(max_concurrent_connections=5, max_connections_per_tenant=5)

        ctx = _make_context(connection_id="conn_c1", tenant_id="tenant_c1")
        lease_c1 = await c1.acquire(ctx)

        # Calling c2._release_lease with a lease from c1 raises InvalidLeaseError
        with pytest.raises(InvalidLeaseError, match="different AdmissionController"):
            await c2._release_lease(lease_c1)

        # c1 is unaffected
        assert lease_c1.is_active is True
        assert c1.snapshot().active_connections == 1

        # Fake/counterfeit lease object with matching connection_id cannot release active lease
        class FakeLease:
            def __init__(self, controller: AdmissionController) -> None:
                self.controller = controller
                self.connection_id = "conn_c1"
                self.tenant_id = "tenant_c1"

        fake = FakeLease(c1)
        assert await c1._release_lease(fake) is False  # type: ignore[arg-type]
        assert c1.snapshot().active_connections == 1
        assert lease_c1.is_active is True

        # Non-lease object passed to is_active_lease returns False
        assert c1.is_active_lease(fake) is False

    asyncio.run(_run())


def test_readmission_after_release() -> None:
    """Item 17: After release, connection_id and capacity become available for new admissions."""

    async def _run() -> None:
        controller = AdmissionController(max_concurrent_connections=1, max_connections_per_tenant=1)
        ctx = _make_context(connection_id="conn_reuse", tenant_id="tenant_a")

        l1 = await controller.acquire(ctx)
        assert await l1.release() is True

        # Re-admitting the same connection_id succeeds now that the previous lease is released
        l2 = await controller.acquire(ctx)
        assert l2.is_active is True
        assert controller.snapshot().active_connections == 1
        await l2.release()

    asyncio.run(_run())


def test_scoped_cleanup_under_exception() -> None:
    """Item 18: async with controller.admit() releases lease even when an exception is raised."""

    async def _run() -> None:
        controller = AdmissionController(max_concurrent_connections=2, max_connections_per_tenant=2)
        ctx = _make_context(connection_id="conn_exc", tenant_id="tenant_exc")

        captured_lease: AdmissionLease | None = None
        try:
            async with controller.admit(ctx) as lease:
                captured_lease = lease
                assert captured_lease.is_active is True
                assert controller.snapshot().active_connections == 1
                raise RuntimeError("simulated failure")
        except RuntimeError:
            pass

        assert captured_lease is not None
        assert captured_lease.is_active is False
        assert controller.snapshot().active_connections == 0
        assert controller.active_connections_for_tenant("tenant_exc") == 0

    asyncio.run(_run())


def test_scoped_cleanup_under_task_cancellation() -> None:
    """Item 19: async with controller.admit() releases lease when task is cancelled."""

    async def _run() -> None:
        controller = AdmissionController(max_concurrent_connections=2, max_connections_per_tenant=2)
        ctx = _make_context(connection_id="conn_cancel", tenant_id="tenant_cancel")

        started = asyncio.Event()
        captured_lease: AdmissionLease | None = None

        async def _worker() -> None:
            nonlocal captured_lease
            async with controller.admit(ctx) as lease:
                captured_lease = lease
                started.set()
                await asyncio.sleep(10)  # Waiting for cancellation

        task = asyncio.create_task(_worker())
        await started.wait()
        assert captured_lease is not None
        assert _is_active(captured_lease) is True
        assert controller.snapshot().active_connections == 1

        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass

        assert _is_active(captured_lease) is False
        assert controller.snapshot().active_connections == 0
        assert controller.active_connections_for_tenant("tenant_cancel") == 0

    asyncio.run(_run())


def test_lease_context_manager_usage() -> None:
    """Item 18b: Using async with on the lease itself releases upon block exit."""

    async def _run() -> None:
        controller = AdmissionController(max_concurrent_connections=5, max_connections_per_tenant=5)
        ctx = _make_context(connection_id="conn_lease_cm", tenant_id="tenant_cm")

        lease = await controller.acquire(ctx)
        async with lease:
            assert _is_active(lease) is True
            assert controller.snapshot().active_connections == 1

        assert _is_active(lease) is False
        assert controller.snapshot().active_connections == 0

    asyncio.run(_run())


def test_acquisition_cancellation_leaves_no_reservation() -> None:
    """Item 20: If acquire() is cancelled while waiting for the lock, no capacity is leaked."""

    async def _run() -> None:
        controller = AdmissionController(max_concurrent_connections=5, max_connections_per_tenant=5)
        ctx = _make_context(connection_id="conn_pre_cancel", tenant_id="tenant_pre")

        # Manually hold controller lock to force acquire() to wait
        await controller._lock.acquire()

        acquire_task = asyncio.create_task(controller.acquire(ctx))
        # Give acquire_task a tick to hit the lock await
        await asyncio.sleep(0.01)

        # Cancel while it is waiting
        acquire_task.cancel()
        try:
            await acquire_task
        except asyncio.CancelledError:
            pass

        # Release the lock and verify no reservations were made
        controller._lock.release()
        assert controller.snapshot().active_connections == 0
        assert controller.has_active_connection("conn_pre_cancel") is False

    asyncio.run(_run())


def test_concurrent_acquisition_races_never_oversubscribe() -> None:
    """Items 21 & 22: 60 concurrent tasks race for 10 global and 4 per-tenant slots."""

    async def _run() -> None:
        max_global: Final[int] = 10
        max_per_tenant: Final[int] = 4
        controller = AdmissionController(
            max_concurrent_connections=max_global,
            max_connections_per_tenant=max_per_tenant,
        )

        barrier = asyncio.Barrier(60)
        admitted: list[AdmissionLease] = []
        rejected: list[Exception] = []

        async def _attempt(conn_idx: int, tenant: str) -> None:
            ctx = _make_context(
                connection_id=f"race_conn_{conn_idx:03d}",
                tenant_id=tenant,
            )
            await barrier.wait()
            try:
                lease = await controller.acquire(ctx)
                admitted.append(lease)
            except AdmissionError as exc:
                rejected.append(exc)

        # 60 total attempts distributed across 3 tenants (20 per tenant)
        tasks = [asyncio.create_task(_attempt(idx, f"tenant_{(idx % 3):d}")) for idx in range(60)]
        await asyncio.gather(*tasks)

        # Invariants:
        # 1. Total admitted must not exceed global capacity
        assert len(admitted) <= max_global
        assert len(admitted) + len(rejected) == 60

        # 2. Controller snapshot matches admitted count
        snapshot = controller.snapshot()
        assert snapshot.active_connections == len(admitted)
        assert snapshot.available_capacity == max_global - len(admitted)

        # 3. No tenant exceeded per-tenant limit
        for tenant_name in ("tenant_0", "tenant_1", "tenant_2"):
            assert controller.active_connections_for_tenant(tenant_name) <= max_per_tenant

        # Release all admitted leases and verify exact return to zero
        release_results = await asyncio.gather(*(lease_item.release() for lease_item in admitted))
        assert all(release_results)

        final_snapshot = controller.snapshot()
        assert final_snapshot.active_connections == 0
        assert final_snapshot.available_capacity == max_global
        assert final_snapshot.active_tenants == 0

    asyncio.run(_run())


def test_tenant_counter_pruning_and_bounded_memory() -> None:
    """Item 23: When all connections for a tenant close, the tenant counter record is pruned."""

    async def _run() -> None:
        controller = AdmissionController(
            max_concurrent_connections=10, max_connections_per_tenant=5
        )

        ctx1 = _make_context(connection_id="t1_c1", tenant_id="tenant_ephemeral")
        ctx2 = _make_context(connection_id="t1_c2", tenant_id="tenant_ephemeral")

        l1 = await controller.acquire(ctx1)
        l2 = await controller.acquire(ctx2)
        assert controller.snapshot().active_tenants == 1
        assert "tenant_ephemeral" in controller._tenant_counts

        await l1.release()
        assert controller.snapshot().active_tenants == 1
        assert "tenant_ephemeral" in controller._tenant_counts

        await l2.release()
        # Now that count reached 0, the key is pruned from _tenant_counts
        assert controller.snapshot().active_tenants == 0
        assert "tenant_ephemeral" not in controller._tenant_counts
        assert controller.active_connections_for_tenant("tenant_ephemeral") == 0

    asyncio.run(_run())


def test_immutable_admission_snapshot() -> None:
    """Item 24: AdmissionSnapshot is frozen and cannot be mutated."""
    snap = AdmissionSnapshot(
        active_connections=3,
        available_capacity=7,
        max_concurrent_connections=10,
        max_connections_per_tenant=4,
        active_tenants=2,
    )
    with pytest.raises(FrozenInstanceError):
        snap.active_connections = 4  # type: ignore[misc]


def test_failed_admission_does_not_create_upstream_or_session() -> None:
    """Item 25: Boundary test proving failed admission rejects before any session/upstream work."""

    async def _run() -> None:
        controller = AdmissionController(max_concurrent_connections=1, max_connections_per_tenant=1)
        ctx1 = _make_context(connection_id="c1", tenant_id="t1")
        ctx2 = _make_context(connection_id="c2", tenant_id="t1")

        await controller.acquire(ctx1)

        upstream_session_created = False
        try:
            await controller.acquire(ctx2)
            upstream_session_created = True
        except AdmissionError:
            pass

        assert upstream_session_created is False
        assert controller.snapshot().active_connections == 1

    asyncio.run(_run())


def test_controller_initialization_with_config_or_defaults() -> None:
    """Item 26: AdmissionController initializes cleanly with CoreConfig, kwargs, or defaults."""
    # From CoreConfig
    cfg = CoreConfig(max_concurrent_connections=50, max_connections_per_tenant=15)
    c1 = AdmissionController(cfg)
    assert c1.max_concurrent_connections == 50
    assert c1.max_connections_per_tenant == 15

    # From kwargs
    c2 = AdmissionController(max_concurrent_connections=80, max_connections_per_tenant=20)
    assert c2.max_concurrent_connections == 80
    assert c2.max_connections_per_tenant == 20

    # From defaults
    c3 = AdmissionController()
    assert c3.max_concurrent_connections == 100
    assert c3.max_connections_per_tenant == 25

    # Invalid config type
    with pytest.raises(TypeError, match="must be a CoreConfig instance or None"):
        AdmissionController(config="invalid")  # type: ignore[arg-type]


def test_lease_release_cancellation_and_retry_safety() -> None:
    """Item 28: Cancellation during lease.release() does not leave lease falsely released if uncompleted."""

    async def _run() -> None:
        controller = AdmissionController(max_concurrent_connections=5, max_connections_per_tenant=5)
        ctx = _make_context(connection_id="conn_cancel_rel", tenant_id="tenant_cancel_rel")

        lease = await controller.acquire(ctx)
        assert _is_active(lease) is True
        assert controller.snapshot().active_connections == 1

        # Hold controller._lock to force release() to wait inside _release_lease
        await controller._lock.acquire()

        release_task = asyncio.create_task(lease.release())
        # Yield to let release_task enter release(), acquire _release_lock, and wait on controller._lock
        await asyncio.sleep(0.01)

        # Cancel the release_task while waiting for controller._lock
        release_task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await release_task

        # Release the controller lock
        controller._lock.release()

        # Because _release_lease was cancelled before execution, lease is still active in controller
        assert controller.snapshot().active_connections == 1
        assert _is_active(lease) is True

        # Retrying release now succeeds cleanly!
        retry_result = await lease.release()
        assert retry_result is True
        assert _is_active(lease) is False
        assert controller.snapshot().active_connections == 0
        assert controller.active_connections_for_tenant("tenant_cancel_rel") == 0

        # Subsequent release returns False
        assert await lease.release() is False

    asyncio.run(_run())


def test_stale_lease_release_does_not_evict_readmitted_lease() -> None:
    """Item 29: Stale release on an old lease does not evict a newly re-admitted lease for same connection_id."""

    async def _run() -> None:
        controller = AdmissionController(max_concurrent_connections=5, max_connections_per_tenant=5)
        ctx = _make_context(connection_id="conn_reuse_stale", tenant_id="tenant_stale")

        lease_a = await controller.acquire(ctx)
        assert lease_a.is_active is True
        assert controller.is_active_lease(lease_a) is True

        # Normal release of lease_a
        assert await lease_a.release() is True
        assert controller.snapshot().active_connections == 0

        # Re-admission of same connection_id produces lease_b
        lease_b = await controller.acquire(ctx)
        assert lease_b.is_active is True
        assert controller.is_active_lease(lease_b) is True
        assert controller.is_active_lease(lease_a) is False
        assert controller.snapshot().active_connections == 1

        # Late / stale release on lease_a must return False and NOT touch lease_b
        stale_result = await lease_a.release()
        assert stale_result is False

        # Invariant: lease_b is still active and controller state is preserved
        assert controller.has_active_connection("conn_reuse_stale") is True
        assert controller.is_active_lease(lease_b) is True
        assert controller.snapshot().active_connections == 1
        assert controller.active_connections_for_tenant("tenant_stale") == 1

        # Finally release lease_b
        assert await lease_b.release() is True
        assert controller.snapshot().active_connections == 0
        assert controller.active_connections_for_tenant("tenant_stale") == 0

    asyncio.run(_run())


def test_cross_event_loop_boundary_raises_runtime_error() -> None:
    """Item 30: Using an AdmissionController across different event loops is rejected by asyncio.Lock."""
    controller = AdmissionController(max_concurrent_connections=5, max_connections_per_tenant=5)
    ctx = _make_context(connection_id="conn_loop1", tenant_id="tenant_loop")

    # Run acquire in loop 1
    lease = asyncio.run(controller.acquire(ctx))

    # In Python asyncio, using the controller from a different event loop fails
    async def _run_loop2() -> None:
        ctx2 = _make_context(connection_id="conn_loop2", tenant_id="tenant_loop")
        await controller.acquire(ctx2)

    with pytest.raises(RuntimeError, match="is bound to a single event loop"):
        asyncio.run(_run_loop2())

    # Releasing from a different event loop is likewise rejected
    async def _release_loop2() -> None:
        await lease.release()

    with pytest.raises(RuntimeError, match="is bound to a single event loop"):
        asyncio.run(_release_loop2())


def test_static_type_checker_enforces_admission_types(tmp_path: Path) -> None:
    """Item 27: mypy --strict verifies AdmissionController type signatures outside tests."""
    snippet = tmp_path / "check_admission_types.py"
    snippet.write_text(
        "from kaiwen_gateway.core import (\n"
        "    AdmissionController,\n"
        "    AdmissionLease,\n"
        "    AdmissionSnapshot,\n"
        "    VerifiedConnectionContext,\n"
        ")\n"
        "async def test_types(ctrl: AdmissionController, ctx: VerifiedConnectionContext) -> None:\n"
        "    lease: AdmissionLease = await ctrl.acquire(ctx)\n"
        "    active: bool = lease.is_active\n"
        "    snap: AdmissionSnapshot = ctrl.snapshot()\n"
        "    _ = (active, snap)\n"
        "    await lease.release()\n",
        encoding="utf-8",
    )
    stdout, stderr, exit_code = mypy_api.run(["--strict", str(snippet)])
    assert exit_code == 0, f"mypy strict failed:\n{stdout}\n{stderr}"
