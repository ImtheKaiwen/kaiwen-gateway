"""ASGI lifespan coordination for Kaiwen Gateway."""

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Any

from kaiwen_gateway.core.draining import InstanceDrainCoordinator, InstanceDrainState
from kaiwen_gateway.server.exceptions import ServerLifecycleError


@asynccontextmanager
async def gateway_lifespan(app: Any) -> AsyncIterator[None]:
    """Manage startup and bounded graceful shutdown for a Gateway ASGI application.

    Startup:
        Verifies instance state and marks the application started.

    Shutdown:
        Transitions the instance drain coordinator to initiate bounded draining,
        awaits active connection drain, and observes termination status.
    """
    # Startup phase
    state = getattr(app, "state", None)
    if state is None:
        raise ServerLifecycleError("Application instance lacks state attribute")

    coordinator: InstanceDrainCoordinator | None = getattr(state, "drain_coordinator", None)
    if coordinator is None or not isinstance(coordinator, InstanceDrainCoordinator):
        raise ServerLifecycleError("Application state missing valid InstanceDrainCoordinator")

    state.started = True
    state.stopped = False

    try:
        yield
    finally:
        # Shutdown phase
        try:
            if coordinator.state == InstanceDrainState.ACCEPTING:
                await coordinator.drain()
            elif not coordinator.is_stopped:
                await coordinator.wait_stopped()

            if coordinator.drain_error is not None:
                raise ServerLifecycleError(
                    "Gateway shutdown failed due to drain error"
                ) from coordinator.drain_error
        finally:
            state.stopped = True
