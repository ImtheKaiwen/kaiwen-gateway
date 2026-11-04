"""ASGI lifespan coordination for Kaiwen Gateway."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Any

from kaiwen_gateway.core.draining import InstanceDrainCoordinator, InstanceDrainState
from kaiwen_gateway.server.exceptions import ServerLifecycleError


@asynccontextmanager
async def gateway_lifespan(app: Any) -> AsyncIterator[None]:
    """Manage startup and bounded graceful shutdown for a Gateway ASGI application.

    Startup:
        Verifies instance state and marks the application started. Fails closed
        if the instance was previously terminated or lacks required coordinators.

    Shutdown:
        Coordinates bounded draining via :class:`InstanceDrainCoordinator`.
        Draining occurs before the application state transitions to stopped.
        If shutdown is cancelled or encounters unhandled failures, the application
        does not falsely report clean completion.
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

    state.started = True
    state.shutting_down = False
    state.stopped = False
    state.shutdown_failed = False

    try:
        yield
    finally:
        # Shutdown phase initiated
        state.shutting_down = True
        try:
            if coordinator.state == InstanceDrainState.ACCEPTING:
                await coordinator.drain()
            elif not coordinator.is_stopped:
                await coordinator.wait_stopped()

            if coordinator.drain_error is not None:
                raise ServerLifecycleError(
                    "Gateway shutdown failed due to drain error"
                ) from coordinator.drain_error

            state.stopped = True
        except (asyncio.CancelledError, KeyboardInterrupt, SystemExit):
            state.shutdown_failed = True
            raise
        except Exception as exc:
            state.shutdown_failed = True
            cause = coordinator.drain_error if coordinator.drain_error is not None else exc
            raise ServerLifecycleError("Gateway shutdown failed due to drain error") from cause
