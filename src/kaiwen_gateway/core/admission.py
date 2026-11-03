"""Framework-independent, process-local connection admission control and capacity leases.

This module enforces FR-CONN-005, SEC-003, SEC-004, and NFR-REL-002:
- Atomically checks and reserves process-local global and per-tenant connection capacity ceilings.
- Operates strictly on verified :class:`~kaiwen_gateway.core.identity.VerifiedConnectionContext`
  instances.
- Deduplicates active connections by ``connection_id``.
- Provides exactly-once, idempotent release semantics via :class:`AdmissionLease`.
- Maintains bounded memory by pruning zero-count tenant records.
- Does not create network sockets, upstream AI sessions, or background tasks.

Note on Trust Boundaries:
    :class:`~kaiwen_gateway.core.identity.VerifiedConnectionContext` is a Python dataclass that
    can be constructed in user code. Passing it to :class:`AdmissionController` proves only that
    the object meets the typing contract; real provider authentication and host authorization
    must precede admission in the ASGI/transport layer (M3+).

Note on Cancellation and Teardown Guarantees:
    :class:`AdmissionLease` release is designed to be retry-safe: if a coroutine
    is cancelled prior to completing `_release_lease`, the lease is not falsely marked
    as released, enabling cleanup supervisors to safely retry release.
    While :meth:`AdmissionController.admit` and :class:`AdmissionLease` context managers
    attempt cleanup in `finally` blocks under normal exits, unhandled errors, and initial
    task cancellation, absolute unconditional teardown under repeated or nested cancellation
    requires higher-level lifecycle orchestration, which is implemented in M2.4
    (Connection Lifecycle).
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass
from types import TracebackType

from kaiwen_gateway.core.config import CoreConfig
from kaiwen_gateway.core.exceptions import (
    CoreConfigurationError,
    DuplicateConnectionAdmissionError,
    GlobalCapacityExceededError,
    InvalidLeaseError,
    TenantCapacityExceededError,
)
from kaiwen_gateway.core.identity import (
    ConnectionId,
    TenantId,
    VerifiedConnectionContext,
)


@dataclass(frozen=True, slots=True)
class AdmissionSnapshot:
    """Immutable snapshot of an AdmissionController's active capacity state."""

    active_connections: int
    available_capacity: int
    max_concurrent_connections: int
    max_connections_per_tenant: int
    active_tenants: int


class AdmissionLease:
    """Represents an active, acquired connection capacity reservation."""

    __slots__ = (
        "_context",
        "_controller",
        "_release_lock",
        "_released",
        "__weakref__",
    )

    def __init__(
        self,
        *,
        controller: AdmissionController,
        context: VerifiedConnectionContext,
    ) -> None:
        self._controller: AdmissionController = controller
        self._context: VerifiedConnectionContext = context
        self._released: bool = False
        self._release_lock: asyncio.Lock = asyncio.Lock()

    @property
    def connection_id(self) -> str:
        """Return the canonical connection identifier for this lease."""
        return self._context.connection_id

    @property
    def tenant_id(self) -> str:
        """Return the canonical tenant identifier for this lease."""
        return self._context.tenant_id

    @property
    def provider_id(self) -> str:
        """Return the canonical provider identifier for this lease."""
        return self._context.provider_id

    @property
    def call_id(self) -> str:
        """Return the canonical call identifier for this lease."""
        return self._context.call_id

    @property
    def controller(self) -> AdmissionController:
        """Return the issuing AdmissionController instance."""
        return self._controller

    @property
    def context(self) -> VerifiedConnectionContext:
        """Return the verified connection context bound to this lease."""
        return self._context

    @property
    def is_active(self) -> bool:
        """Return True if this lease is currently holding an active capacity reservation."""
        return not self._released

    async def release(self) -> bool:
        """Release the capacity reservation held by this lease.

        Idempotent and retry-safe: returns True on the first successful release,
        and False on all subsequent calls. Counters in the issuing controller are
        never decremented more than once.

        Cancellation semantics:
        If cancelled while waiting for the controller lock, the lease remains
        unreleased so that cleanup supervisors can safely retry release.
        """
        async with self._release_lock:
            if self._released:
                return False
            released = await self._controller._release_lease(self)
            self._released = True
            return released

    async def __aenter__(self) -> AdmissionLease:
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        await self.release()

    def __repr__(self) -> str:
        status = "active" if self.is_active else "released"
        return (
            f"AdmissionLease(connection_id={self.connection_id!r}, "
            f"tenant_id={self.tenant_id!r}, status={status!r})"
        )


class AdmissionController:
    """Process-local, tenant-scoped, concurrency-safe connection admission controller.

    Invariants (M2.3):
    - Instance-scoped state; no global or process-wide shared registry.
    - Single-process, single-event-loop concurrency model guarded by an internal asyncio.Lock.
    - Event-loop ownership & thread boundaries: Mutating async methods (`acquire()` and
      `_release_lease()`) enforce single-event-loop affinity via `_check_loop()`. Synchronous
      inspection methods (`snapshot()`, `has_active_connection()`, `active_connections_for_tenant()`,
      `is_active_lease()`, `__repr__()`) are process-local, lock-free readers intended for the
      owning thread/event-loop; they do not perform cross-loop fail-fast checks or cross-thread
      synchronization and are not thread-safe for direct concurrent access across separate OS threads.
    - Distribution boundary: Process-local; does not coordinate across separate processes
      or cluster nodes (distributed coordination is out of V1 scope per ADR-0001/ADR-0003).
    - Enforces atomic capacity reservation against global `max_concurrent_connections`
      and per-tenant `max_connections_per_tenant` ceilings without race conditions.
    - Accepts only verified `VerifiedConnectionContext` instances.
    - Deduplicates active connections: rejects any connection_id that already holds an active lease.
    - Idempotent exactly-once lease release semantics.
    - Cleans up tenant counter records when a tenant has zero active connections (bounded memory).
    - Never creates network connections, upstream AI sessions, or persistent background tasks.
    """

    def __init__(
        self,
        config: CoreConfig | None = None,
        *,
        max_concurrent_connections: int | None = None,
        max_connections_per_tenant: int | None = None,
    ) -> None:
        if config is not None:
            if not isinstance(config, CoreConfig):
                raise TypeError(
                    f"config must be a CoreConfig instance or None, got {type(config).__name__}"
                )
            if max_concurrent_connections is not None or max_connections_per_tenant is not None:
                raise CoreConfigurationError(
                    "Cannot specify explicit capacity override arguments "
                    "(max_concurrent_connections, max_connections_per_tenant) "
                    "when a CoreConfig instance is provided",
                    field_name="config",
                )
            self._max_concurrent_connections: int = config.max_concurrent_connections
            self._max_connections_per_tenant: int = config.max_connections_per_tenant
        else:
            kwargs: dict[str, object] = {}
            if max_concurrent_connections is not None:
                kwargs["max_concurrent_connections"] = max_concurrent_connections
            if max_connections_per_tenant is not None:
                kwargs["max_connections_per_tenant"] = max_connections_per_tenant
            effective_config = CoreConfig(**kwargs)  # type: ignore[arg-type]
            self._max_concurrent_connections = effective_config.max_concurrent_connections
            self._max_connections_per_tenant = effective_config.max_connections_per_tenant

        self._active_leases: dict[str, AdmissionLease] = {}
        self._tenant_counts: dict[str, int] = {}
        self._lock: asyncio.Lock = asyncio.Lock()
        self._loop: asyncio.AbstractEventLoop | None = None

    def _check_loop(self) -> None:
        """Enforce single-event-loop affinity."""
        current_loop = asyncio.get_running_loop()
        if self._loop is None:
            self._loop = current_loop
        elif self._loop is not current_loop:
            raise RuntimeError(
                "AdmissionController is bound to a single event loop and cannot be "
                "used from a different event loop"
            )

    @property
    def max_concurrent_connections(self) -> int:
        """Return the maximum global concurrent connections allowed."""
        return self._max_concurrent_connections

    @property
    def max_connections_per_tenant(self) -> int:
        """Return the maximum concurrent connections allowed per tenant."""
        return self._max_connections_per_tenant

    async def acquire(self, context: VerifiedConnectionContext) -> AdmissionLease:
        """Atomically admit a connection and grant a capacity lease or raise an AdmissionError.

        Guarantees:
        - Validates that `context` is a `VerifiedConnectionContext`.
        - Atomic evaluation of duplicate connection_id, global ceiling, and per-tenant ceiling.
        - Rejection incurs zero allocation or counter increment.
        """
        if not isinstance(context, VerifiedConnectionContext):
            raise TypeError(
                f"context must be a VerifiedConnectionContext instance, got {type(context).__name__}"
            )

        self._check_loop()

        async with self._lock:
            # 1. Duplicate connection check
            if context.connection_id in self._active_leases:
                raise DuplicateConnectionAdmissionError(context.connection_id)

            # 2. Global capacity check
            current_global = len(self._active_leases)
            if current_global >= self._max_concurrent_connections:
                raise GlobalCapacityExceededError(
                    limit=self._max_concurrent_connections,
                    current=current_global,
                )

            # 3. Tenant capacity check
            current_tenant = self._tenant_counts.get(context.tenant_id, 0)
            if current_tenant >= self._max_connections_per_tenant:
                raise TenantCapacityExceededError(
                    tenant_id=context.tenant_id,
                    limit=self._max_connections_per_tenant,
                    current=current_tenant,
                )

            # 4. Atomic reservation
            lease = AdmissionLease(
                controller=self,
                context=context,
            )
            self._active_leases[context.connection_id] = lease
            self._tenant_counts[context.tenant_id] = current_tenant + 1
            return lease

    @asynccontextmanager
    async def admit(self, context: VerifiedConnectionContext) -> AsyncIterator[AdmissionLease]:
        """Async context manager that acquires a lease and ensures release upon exit.

        Guarantees cleanup upon normal block exit, unhandled errors, and first-order
        task cancellations. Repeated or nested cancellations during release require
        outer lifecycle supervision (M2.4).
        """
        lease = await self.acquire(context)
        try:
            yield lease
        finally:
            await lease.release()

    async def _release_lease(self, lease: AdmissionLease) -> bool:
        """Internal atomic release method called by AdmissionLease.release()."""
        if lease.controller is not self:
            raise InvalidLeaseError(
                "Cannot release a lease issued by a different AdmissionController"
            )

        self._check_loop()

        async with self._lock:
            active = self._active_leases.get(lease.connection_id)
            if active is not lease:
                return False

            del self._active_leases[lease.connection_id]

            tenant_id = lease.tenant_id
            current_tenant = self._tenant_counts.get(tenant_id, 0)
            if current_tenant <= 1:
                self._tenant_counts.pop(tenant_id, None)
            else:
                self._tenant_counts[tenant_id] = current_tenant - 1

            return True

    def snapshot(self) -> AdmissionSnapshot:
        """Return an immutable snapshot of current controller capacity.

        Note: Synchronous, lock-free inspection method intended for the owning thread
        and event-loop context. Does not perform cross-loop checks or cross-thread synchronization.
        """
        active = len(self._active_leases)
        return AdmissionSnapshot(
            active_connections=active,
            available_capacity=max(0, self._max_concurrent_connections - active),
            max_concurrent_connections=self._max_concurrent_connections,
            max_connections_per_tenant=self._max_connections_per_tenant,
            active_tenants=len(self._tenant_counts),
        )

    def active_connections_for_tenant(self, tenant_id: str | TenantId) -> int:
        """Return active connection count for a specific tenant.

        Note: Synchronous, lock-free inspection method intended for the owning thread
        and event-loop context.
        """
        canonical = tenant_id.value if isinstance(tenant_id, TenantId) else tenant_id
        return self._tenant_counts.get(canonical, 0)

    def has_active_connection(self, connection_id: str | ConnectionId) -> bool:
        """Return True if connection_id currently holds an active lease.

        Note: Synchronous, lock-free inspection method intended for the owning thread
        and event-loop context.
        """
        canonical = (
            connection_id.value if isinstance(connection_id, ConnectionId) else connection_id
        )
        return canonical in self._active_leases

    def is_active_lease(self, lease: object) -> bool:
        """Return True if the given lease is currently recorded as active in this controller.

        Note: Synchronous, lock-free inspection method intended for the owning thread
        and event-loop context.
        """
        if not isinstance(lease, AdmissionLease):
            return False
        return self._active_leases.get(lease.connection_id) is lease

    def __repr__(self) -> str:
        return (
            f"AdmissionController(active={len(self._active_leases)}, "
            f"max_global={self._max_concurrent_connections}, "
            f"max_per_tenant={self._max_connections_per_tenant})"
        )


__all__ = [
    "AdmissionController",
    "AdmissionLease",
    "AdmissionSnapshot",
]
