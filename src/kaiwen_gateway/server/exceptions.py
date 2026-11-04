"""Exception classes for the ASGI server and runtime foundation."""

from kaiwen_gateway.core.exceptions import CoreError


class ServerError(CoreError):
    """Base class for all ASGI server and runtime errors."""


class ServerDependencyError(ServerError, ImportError):
    """Raised when required ASGI server dependencies are not installed."""


class ServerLifecycleError(ServerError):
    """Raised when server startup or graceful shutdown encounters a terminal failure."""
