"""ASGI lifespan coordination for Kaiwen Gateway.

The reference ASGI host owns runtime orchestration for registered Gateway modules
(see :mod:`kaiwen_gateway.core.module_lifecycle`):

* Startup: registered modules start in registration order **before** the application is
  marked started (readiness stays ``not_started`` until every module started). A module
  failure rolls back already-started modules, leaves the instance permanently stopped and
  refusing traffic, and fails the ASGI lifespan startup.
* Shutdown: connections are drained first (bounded by the drain policy); only afterwards are
  modules stopped in reverse order, so module resources outlive active calls. Module cleanup
  is attempted even if the drain failed or the lifespan was cancelled, and is itself bounded
  by per-hook timeouts.
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Any

from kaiwen_gateway.core.draining import InstanceDrainCoordinator, InstanceDrainState
from kaiwen_gateway.core.module_lifecycle import ModuleLifecycleManager
from kaiwen_gateway.server.exceptions import ServerLifecycleError


async def _fail_closed_after_startup_error(
    state: Any, coordinator: InstanceDrainCoordinator
) -> None:
    """Make a failed instance permanently non-serving (readiness 503, admission closed)."""
    state.started = False
    state.startup_failed = True
    state.shutting_down = True
    try:
        if coordinator.state == InstanceDrainState.ACCEPTING:
            await coordinator.drain()
    except Exception:
        # Nothing was admitted before startup completed; the instance is stopped regardless.
        pass
    state.stopped = True


@asynccontextmanager
async def gateway_lifespan(app: Any) -> AsyncIterator[None]:
    """Manage startup and bounded graceful shutdown for a Gateway ASGI application.

    Startup:
        Verifies instance state, starts registered modules in registration order and only
        then marks the application started. Fails closed if the instance was previously
        terminated, lacks required coordinators, or any module fails to start.

    Shutdown:
        Coordinates bounded draining via :class:`InstanceDrainCoordinator`, then stops
        modules in reverse registration order. Draining occurs before modules are closed and
        before the application state transitions to stopped. If shutdown is cancelled or
        encounters unhandled failures, the application does not falsely report clean
        completion.
    """
    state = getattr(app, "state", None)
    if state is None:
        raise ServerLifecycleError("Application instance lacks state attribute")

    coordinator: InstanceDrainCoordinator | None = getattr(state, "drain_coordinator", None)
    if coordinator is None or not isinstance(coordinator, InstanceDrainCoordinator):
        raise ServerLifecycleError("Application state missing valid InstanceDrainCoordinator")

    # Prevent restarting an already-terminated, draining, or terminating instance
    initial_state = coordinator.state
    if (
        initial_state != InstanceDrainState.ACCEPTING
        or coordinator.is_stopped
        or getattr(state, "stopped", False)
        or getattr(state, "shutting_down", False)
    ):
        raise ServerLifecycleError(
            "Cannot start lifespan on a stopped, draining, or terminating Gateway instance"
        )

    modules: ModuleLifecycleManager | None = getattr(state, "module_lifecycle", None)

    state.started = False
    state.shutting_down = False
    state.stopped = False
    state.shutdown_failed = False
    state.startup_failed = False

    if modules is not None:
        try:
            await modules.startup()
        except asyncio.CancelledError:
            await _fail_closed_after_startup_error(state, coordinator)
            raise
        except Exception as exc:
            await _fail_closed_after_startup_error(state, coordinator)
            # ``exc`` is already a safe, secret-free ModuleStartupError (or state error).
            raise ServerLifecycleError("Gateway module startup failed") from exc
        except BaseException:
            await _fail_closed_after_startup_error(state, coordinator)
            raise

    state.started = True

    try:
        yield
    finally:
        # Shutdown phase initiated
        state.shutting_down = True
        control_flow_exc: BaseException | None = None
        drain_failure: BaseException | None = None
        module_failure: BaseException | None = None

        # 1. Drain active connections first.
        try:
            if coordinator.state == InstanceDrainState.ACCEPTING:
                await coordinator.drain()
            elif not coordinator.is_stopped:
                await coordinator.wait_stopped()

            if coordinator.drain_error is not None:
                drain_failure = coordinator.drain_error
        except asyncio.CancelledError as exc:
            control_flow_exc = exc
            # If a real drain task is in progress (initiated by lifespan or an embedding
            # host), wait for it to reach STOPPED before stopping dependent modules.
            drain_task = getattr(coordinator, "_drain_task", None)
            if drain_task is not None and not drain_task.done() and not coordinator.is_stopped:
                try:
                    await coordinator.wait_stopped()
                except asyncio.CancelledError:
                    pass
                except Exception as wait_exc:
                    drain_failure = (
                        coordinator.drain_error if coordinator.drain_error is not None else wait_exc
                    )
        except (KeyboardInterrupt, SystemExit) as exc:
            control_flow_exc = exc
        except Exception as exc:
            drain_failure = coordinator.drain_error if coordinator.drain_error is not None else exc

        # 2. Only close dependent module resources when all active connections and
        # admission leases have been cleanly resolved; otherwise skip module
        # shutdown to avoid tearing down resources underneath still-active workers.
        report = coordinator.report
        drain_resolved = (
            drain_failure is None
            and coordinator.drain_error is None
            and coordinator.state == InstanceDrainState.STOPPED
            and (
                report is None
                or (report.unresolved_connections == 0 and report.active_leases_remaining == 0)
            )
        )

        if modules is not None and drain_resolved:
            notif_dispatcher = getattr(state, "notifications_dispatcher", None)
            close_fn = getattr(notif_dispatcher, "close", None)
            if callable(close_fn):
                try:
                    await close_fn()
                except Exception as exc:
                    module_failure = exc
            try:
                await modules.shutdown()
            except asyncio.CancelledError as exc:
                control_flow_exc = control_flow_exc or exc
                # ModuleLifecycleManager shields its bounded shutdown task; join it so
                # remaining modules finish closing before the event loop tears down.
                try:
                    await modules.shutdown()
                except asyncio.CancelledError:
                    pass
                except Exception as shutdown_exc:
                    module_failure = module_failure or shutdown_exc
            except (KeyboardInterrupt, SystemExit) as exc:
                control_flow_exc = control_flow_exc or exc
            except Exception as exc:
                module_failure = exc

        if control_flow_exc is not None:
            state.shutdown_failed = True
            raise control_flow_exc

        if drain_failure is not None or module_failure is not None:
            state.shutdown_failed = True
            if drain_failure is not None and module_failure is not None:
                raise ServerLifecycleError(
                    "Gateway shutdown failed due to drain error and module shutdown errors"
                ) from drain_failure
            if drain_failure is not None:
                raise ServerLifecycleError(
                    "Gateway shutdown failed due to drain error"
                ) from drain_failure
            raise ServerLifecycleError("Gateway module shutdown failed") from module_failure

        if not drain_resolved:
            state.shutdown_failed = True
            if report is not None:
                raise ServerLifecycleError(
                    "Gateway shutdown failed with unresolved drain resources "
                    f"(unresolved_connections={report.unresolved_connections}, "
                    f"active_leases_remaining={report.active_leases_remaining})"
                )
            raise ServerLifecycleError("Gateway shutdown failed: drain did not complete cleanly")

        state.stopped = True
