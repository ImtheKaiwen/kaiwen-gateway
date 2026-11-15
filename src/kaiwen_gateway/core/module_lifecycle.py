"""Framework-independent, host-owned lifecycle orchestration for registered Gateway modules.

Ownership model (S1-A):

- :class:`~kaiwen_gateway.core.GatewayCore` only *registers* and validates modules. It never
  runs module ``startup()``/``shutdown()`` hooks, and construction stays synchronous and
  side-effect free.
- The **hosting application** owns runtime orchestration. A host (for example, an ASGI host
  or an embedding application) drives one :class:`ModuleLifecycleManager` per
  :class:`~kaiwen_gateway.core.GatewayCore` instance.
- The manager is plain ``asyncio`` code with no web-framework dependency, so Core can be
  embedded without ASGI.

Semantics:

- ``startup()`` runs hooks sequentially in registration order. Each hook is bounded by
  ``startup_timeout``. The first failure, timeout or cancellation rolls back every module
  that already started, in reverse order, and then raises. The module whose own ``startup()``
  failed is *not* asked to ``shutdown()``: a module must release partially acquired resources
  itself when its ``startup()`` raises.
- ``shutdown()`` runs hooks sequentially in reverse registration order, each bounded by
  ``shutdown_timeout``. A failing or timed-out hook never prevents the remaining modules from
  being stopped. Failures are aggregated and raised once, after all modules were attempted.
- ``shutdown()`` is idempotent and concurrency-safe: every started module is closed at most
  once. Cancelling a caller does not abandon cleanup; it continues in a bounded background
  task that later ``shutdown()`` calls await.
- Instances are single-use. After ``shutdown()`` or a failed ``startup()`` the manager
  refuses to start again (fail closed).
- Errors expose only module identifiers, phases and exception *type names*. Exception
  messages and tracebacks from module code are deliberately not chained or copied because
  they may contain secrets. Modules should log their own sanitized diagnostics.

Hosts must stop accepting and drain connections **before** calling ``shutdown()`` so that
dependent module resources outlive active calls (the reference ASGI lifespan does this).
"""

from __future__ import annotations

import asyncio
import math
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from enum import StrEnum
from typing import Final

from kaiwen_gateway.core.composition import GatewayCore
from kaiwen_gateway.core.contracts import GatewayModule
from kaiwen_gateway.core.exceptions import CoreConfigurationError, CoreError

DEFAULT_MODULE_STARTUP_TIMEOUT: Final[float] = 30.0
DEFAULT_MODULE_SHUTDOWN_TIMEOUT: Final[float] = 30.0
MAX_MODULE_HOOK_TIMEOUT: Final[float] = 3600.0


class ModuleLifecyclePhase(StrEnum):
    """Lifecycle phase of a :class:`ModuleLifecycleManager`."""

    CREATED = "created"
    STARTING = "starting"
    STARTED = "started"
    STOPPING = "stopping"
    STOPPED = "stopped"
    FAILED = "failed"


class ModuleLifecycleError(CoreError):
    """Base class for module lifecycle orchestration errors."""


class InvalidModuleLifecycleStateError(ModuleLifecycleError):
    """Raised when an operation is invalid for the manager's current phase."""


@dataclass(frozen=True, slots=True)
class ModuleFailure:
    """Safe, secret-free description of one failed module lifecycle hook."""

    module_id: str
    hook: str
    error_type: str
    timed_out: bool = False

    def __str__(self) -> str:
        reason = "timed out" if self.timed_out else self.error_type
        return f"{self.module_id}.{self.hook}: {reason}"


class ModuleStartupError(ModuleLifecycleError):
    """Raised when module startup fails; previously started modules were rolled back.

    Attributes:
        failure: The failed ``startup`` hook.
        rollback_failures: Failures that occurred while rolling back already-started modules.
    """

    def __init__(
        self,
        failure: ModuleFailure,
        rollback_failures: tuple[ModuleFailure, ...] = (),
    ) -> None:
        self.failure: ModuleFailure = failure
        self.rollback_failures: tuple[ModuleFailure, ...] = rollback_failures
        message = f"Module startup failed ({failure})"
        if rollback_failures:
            message += f"; {len(rollback_failures)} rollback error(s)"
        super().__init__(message)


class ModuleShutdownError(ModuleLifecycleError):
    """Raised after all modules were attempted if one or more shutdown hooks failed."""

    def __init__(self, failures: tuple[ModuleFailure, ...]) -> None:
        self.failures: tuple[ModuleFailure, ...] = failures
        listed = ", ".join(str(f) for f in failures)
        super().__init__(f"Module shutdown completed with {len(failures)} error(s): {listed}")


def _validate_timeout(value: object, *, field_name: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise CoreConfigurationError(
            f"{field_name} must be a positive finite number of seconds", field_name=field_name
        )
    seconds = float(value)
    if not math.isfinite(seconds) or seconds <= 0.0 or seconds > MAX_MODULE_HOOK_TIMEOUT:
        raise CoreConfigurationError(
            f"{field_name} must be greater than 0 and at most {MAX_MODULE_HOOK_TIMEOUT:g} seconds",
            field_name=field_name,
        )
    return seconds


@dataclass(frozen=True, slots=True)
class ModuleLifecyclePolicy:
    """Per-hook time bounds for module lifecycle orchestration (seconds)."""

    startup_timeout: float = DEFAULT_MODULE_STARTUP_TIMEOUT
    shutdown_timeout: float = DEFAULT_MODULE_SHUTDOWN_TIMEOUT

    def __post_init__(self) -> None:
        _validate_timeout(self.startup_timeout, field_name="startup_timeout")
        _validate_timeout(self.shutdown_timeout, field_name="shutdown_timeout")


async def _run_hook(
    module: GatewayModule,
    hook_name: str,
    timeout: float,
) -> ModuleFailure | None:
    """Run one lifecycle hook with a bound; return a safe failure or ``None`` on success.

    ``asyncio.CancelledError`` and other ``BaseException`` types propagate to the caller.
    """
    module_id = module.descriptor.module_id
    hook: Callable[[], Awaitable[None]] = getattr(module, hook_name)
    try:
        async with asyncio.timeout(timeout) as bound:
            await hook()
    except TimeoutError as exc:
        if bound.expired():
            return ModuleFailure(module_id, hook_name, "TimeoutError", timed_out=True)
        return ModuleFailure(module_id, hook_name, type(exc).__name__)
    except Exception as exc:
        return ModuleFailure(module_id, hook_name, type(exc).__name__)
    return None


class ModuleLifecycleManager:
    """Host-owned orchestrator for the registered modules of exactly one ``GatewayCore``.

    Use as an async context manager for embedded hosts::

        core = compose_gateway_core(modules=[voice_module])
        async with ModuleLifecycleManager(core):
            ...  # serve; drain connections before leaving the block

    Independent managers (one per ``GatewayCore``) share no state.
    """

    __slots__ = (
        "_failures",
        "_modules",
        "_phase",
        "_policy",
        "_reported",
        "_shutdown_task",
        "_started",
    )

    def __init__(self, core: GatewayCore, *, policy: ModuleLifecyclePolicy | None = None) -> None:
        if not isinstance(core, GatewayCore):
            raise TypeError(f"core must be a GatewayCore, got {type(core).__name__}")
        if policy is not None and not isinstance(policy, ModuleLifecyclePolicy):
            raise TypeError(f"policy must be a ModuleLifecyclePolicy, got {type(policy).__name__}")
        self._modules: tuple[GatewayModule, ...] = core.modules()
        self._policy: ModuleLifecyclePolicy = (
            policy if policy is not None else ModuleLifecyclePolicy()
        )
        self._phase: ModuleLifecyclePhase = ModuleLifecyclePhase.CREATED
        self._started: list[GatewayModule] = []
        self._shutdown_task: asyncio.Task[tuple[ModuleFailure, ...]] | None = None
        self._failures: tuple[ModuleFailure, ...] = ()
        self._reported: bool = False

    @property
    def phase(self) -> ModuleLifecyclePhase:
        """Current lifecycle phase."""
        return self._phase

    @property
    def policy(self) -> ModuleLifecyclePolicy:
        """Immutable per-hook time bounds."""
        return self._policy

    @property
    def module_count(self) -> int:
        """Number of registered modules this manager orchestrates."""
        return len(self._modules)

    @property
    def started_module_ids(self) -> tuple[str, ...]:
        """Identifiers of modules that started and have not been stopped, in start order."""
        return tuple(m.descriptor.module_id for m in self._started)

    @property
    def shutdown_failures(self) -> tuple[ModuleFailure, ...]:
        """Safe descriptions of shutdown/rollback hook failures observed so far."""
        return self._failures

    async def startup(self) -> None:
        """Start all registered modules in registration order, or roll back and raise."""
        if self._phase is not ModuleLifecyclePhase.CREATED:
            raise InvalidModuleLifecycleStateError(
                f"Cannot start modules from phase {self._phase.value!r}; "
                "a ModuleLifecycleManager is single-use"
            )
        self._phase = ModuleLifecyclePhase.STARTING
        failure: ModuleFailure | None = None
        try:
            for module in self._modules:
                failure = await _run_hook(module, "startup", self._policy.startup_timeout)
                if failure is not None:
                    break
                self._started.append(module)
        except BaseException:
            # Cancellation, KeyboardInterrupt, SystemExit: roll back, then re-raise unchanged.
            self._phase = ModuleLifecyclePhase.FAILED
            self._reported = True
            await self._stop_all_shielded()
            raise

        if failure is None:
            self._phase = ModuleLifecyclePhase.STARTED
            return

        self._phase = ModuleLifecyclePhase.FAILED
        self._reported = True
        rollback_failures = await self._stop_all_shielded()
        raise ModuleStartupError(failure, rollback_failures) from None

    async def shutdown(self) -> None:
        """Stop started modules in reverse order.

        Idempotent: each started module is closed at most once. Raises
        :class:`ModuleShutdownError` once, after every module was attempted, if any hook failed.
        """
        if self._phase is ModuleLifecyclePhase.STARTING:
            raise InvalidModuleLifecycleStateError("Cannot shut down while startup is in progress")
        if self._phase is ModuleLifecyclePhase.CREATED:
            self._phase = ModuleLifecyclePhase.STOPPED
            return
        if self._phase is ModuleLifecyclePhase.STARTED:
            self._phase = ModuleLifecyclePhase.STOPPING
        failures = await self._stop_all_shielded()
        if self._phase is ModuleLifecyclePhase.STOPPING:
            self._phase = ModuleLifecyclePhase.STOPPED
        if failures and not self._reported:
            self._reported = True
            raise ModuleShutdownError(failures) from None

    async def _stop_started(self) -> tuple[ModuleFailure, ...]:
        failures: list[ModuleFailure] = []
        cancelled_exc: asyncio.CancelledError | None = None
        while self._started:
            # Pop before awaiting so a module can never be closed twice.
            module = self._started.pop()
            try:
                failure = await _run_hook(module, "shutdown", self._policy.shutdown_timeout)
            except asyncio.CancelledError as exc:
                current = asyncio.current_task()
                if current is not None and current.cancelling() > 0:
                    raise
                if cancelled_exc is None:
                    cancelled_exc = exc
                failures.append(
                    ModuleFailure(module.descriptor.module_id, "shutdown", "CancelledError")
                )
                continue
            if failure is not None:
                failures.append(failure)
        if cancelled_exc is not None:
            self._failures = tuple(failures)
            raise cancelled_exc
        return tuple(failures)

    async def _stop_all_shielded(self) -> tuple[ModuleFailure, ...]:
        """Run (or join) the single bounded cleanup task, immune to caller cancellation."""
        if self._shutdown_task is None:
            self._shutdown_task = asyncio.ensure_future(self._stop_started())
        failures = await asyncio.shield(self._shutdown_task)
        self._failures = failures
        return failures

    async def __aenter__(self) -> ModuleLifecycleManager:
        await self.startup()
        return self

    async def __aexit__(self, exc_type: object, exc: object, tb: object) -> None:
        await self.shutdown()


__all__ = [
    "DEFAULT_MODULE_SHUTDOWN_TIMEOUT",
    "DEFAULT_MODULE_STARTUP_TIMEOUT",
    "InvalidModuleLifecycleStateError",
    "ModuleFailure",
    "ModuleLifecycleError",
    "ModuleLifecycleManager",
    "ModuleLifecyclePhase",
    "ModuleLifecyclePolicy",
    "ModuleShutdownError",
    "ModuleStartupError",
]
