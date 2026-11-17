"""Sections F & G tests: Core Principal Identity compatibility, mixed draining, and isolation."""

from __future__ import annotations

import asyncio
import subprocess
import sys

import pytest

from kaiwen_gateway.core import (
    AdmissionController,
    AdmissionLease,
    CoreConfig,
    DrainPolicy,
    InstanceDrainCoordinator,
    ModuleLifecycleManager,
    PrincipalAdmissionLease,
    PrincipalConnectionSupervisor,
    ProviderPrincipal,
    TenantCallBinding,
    TenantCapacityExceededError,
    TenantPrincipalBinding,
    UntrustedConnectionClaims,
    VerifiedConnectionContext,
    VerifiedPrincipalContext,
    bind_verified_principal_context,
    compose_gateway_core,
    resolve_verified_principal_context,
)
from kaiwen_gateway.modules.events import EventsModule


class _StubProviderAuthenticator:
    def __init__(self, principal: ProviderPrincipal) -> None:
        self._principal = principal

    async def authenticate_provider(
        self,
        *,
        connection_id: str,
        claims: UntrustedConnectionClaims,
    ) -> ProviderPrincipal:
        return self._principal


class _StubPrincipalAuthPort:
    def __init__(self, binding: TenantPrincipalBinding) -> None:
        self._binding = binding

    async def authorize_principal(
        self,
        *,
        principal: ProviderPrincipal,
        connection_id: str,
        claims: UntrustedConnectionClaims,
    ) -> TenantPrincipalBinding:
        return self._binding


def test_voice_and_principal_typing_contracts() -> None:
    voice_ctx = VerifiedConnectionContext(
        provider_id="provider-1",
        tenant_id="tenant-1",
        call_id="call-123",
        connection_id="conn-voice-1",
        allowed_scopes=frozenset({"voice:connect"}),
    )
    assert isinstance(voice_ctx.call_id, str)
    assert voice_ctx.call_id == "call-123"

    binding = TenantCallBinding(
        authorized_provider_id="provider-1",
        tenant_id="tenant-1",
        call_id="call-123",
    )
    assert binding.call_id == "call-123"

    principal_ctx = VerifiedPrincipalContext(
        provider_id="provider-1",
        tenant_id="tenant-1",
        principal_id="user-42",
        connection_id="conn-evt-1",
        allowed_scopes=frozenset({"events:subscribe"}),
    )
    assert not hasattr(principal_ctx, "call_id")
    assert principal_ctx.principal_id == "user-42"


def test_principal_context_binding_and_mixed_admission_limits() -> None:
    async def scenario() -> None:
        principal = ProviderPrincipal(
            provider_id="provider-1",
        )
        binding = TenantPrincipalBinding(
            authorized_provider_id="provider-1",
            tenant_id="tenant-1",
            principal_id="user-42",
            allowed_scopes=frozenset({"events:subscribe"}),
        )
        ctx = await resolve_verified_principal_context(
            authenticator=_StubProviderAuthenticator(principal),
            authorizer=_StubPrincipalAuthPort(binding),
            connection_id="conn-evt-1",
            untrusted_claims=UntrustedConnectionClaims(
                claimed_scopes=frozenset({"events:subscribe"}),
            ),
        )
        assert isinstance(ctx, VerifiedPrincipalContext)

        bound = bind_verified_principal_context(
            principal=principal,
            binding=binding,
            connection_id="conn-evt-2",
        )
        assert bound.principal_id == "user-42"

        controller = AdmissionController(
            CoreConfig(max_concurrent_connections=3, max_connections_per_tenant=2)
        )
        voice_ctx = VerifiedConnectionContext(
            provider_id="provider-1",
            tenant_id="tenant-1",
            call_id="call-1",
            connection_id="conn-voice-1",
            allowed_scopes=frozenset({"voice:connect"}),
        )

        voice_lease = await controller.acquire(voice_ctx)
        assert isinstance(voice_lease, AdmissionLease)
        assert voice_lease.call_id == "call-1"

        events_lease = await controller.acquire_principal(ctx)
        assert isinstance(events_lease, PrincipalAdmissionLease)
        assert not hasattr(events_lease, "call_id")

        # Tenant cap of 2 is now saturated across Voice + Events
        with pytest.raises(TenantCapacityExceededError):
            await controller.acquire_principal(bound)

        assert await events_lease.release() is True
        assert await events_lease.release() is False
        assert await voice_lease.release() is True
        assert controller.snapshot().active_connections == 0

    asyncio.run(scenario())


def test_mixed_voice_and_events_drain_coordinator() -> None:
    async def scenario() -> None:
        controller = AdmissionController(
            CoreConfig(max_concurrent_connections=10, max_connections_per_tenant=5)
        )
        coordinator = InstanceDrainCoordinator(controller=controller)

        voice_sup = await coordinator.admit_connection(
            VerifiedConnectionContext(
                provider_id="provider-1",
                tenant_id="tenant-1",
                call_id="call-1",
                connection_id="conn-voice",
                allowed_scopes=frozenset({"voice:connect"}),
            )
        )
        evt_sup = await coordinator.admit_principal_connection(
            VerifiedPrincipalContext(
                provider_id="provider-1",
                tenant_id="tenant-1",
                principal_id="user-1",
                connection_id="conn-evt",
                allowed_scopes=frozenset({"events:subscribe"}),
            )
        )
        assert isinstance(evt_sup, PrincipalConnectionSupervisor)

        async def idle_worker() -> None:
            await asyncio.sleep(10)

        await voice_sup.start(idle_worker)
        await evt_sup.start(idle_worker)

        report = await coordinator.drain(DrainPolicy(grace_period=0.05, force_close_timeout=1.0))
        assert report.initial_connections == 2
        assert report.force_closed == 2
        assert report.unresolved_connections == 0
        assert report.active_leases_remaining == 0
        assert controller.snapshot().active_connections == 0

    asyncio.run(scenario())


def _assert_module_state(mod: EventsModule, *, started: bool, closed: bool) -> None:
    assert mod.is_started is started
    assert mod.broker.is_closed is closed


def test_events_module_lifecycle_manager_integration() -> None:
    async def scenario() -> None:
        events_module = EventsModule()
        core = compose_gateway_core(CoreConfig(), modules=[events_module])

        manager = ModuleLifecycleManager(core)
        await manager.startup()
        _assert_module_state(events_module, started=True, closed=False)

        await manager.shutdown()
        _assert_module_state(events_module, started=False, closed=True)

    asyncio.run(scenario())


def test_core_and_events_import_without_optional_runtime_dependencies() -> None:
    script = """
import importlib.abc
import sys

BLOCKED = {"redis", "starlette", "uvicorn", "websockets"}

class _Blocker(importlib.abc.MetaPathFinder):
    def find_spec(self, fullname, path=None, target=None):
        if fullname.split(".")[0] in BLOCKED:
            raise ModuleNotFoundError(f"Blocked optional package: {fullname}")
        return None

sys.meta_path.insert(0, _Blocker())
import kaiwen_gateway.core
import kaiwen_gateway.modules.events
print("OK")
"""
    proc = subprocess.run(
        [sys.executable, "-c", script],
        capture_output=True,
        text=True,
        check=True,
    )
    assert proc.stdout.strip() == "OK"
