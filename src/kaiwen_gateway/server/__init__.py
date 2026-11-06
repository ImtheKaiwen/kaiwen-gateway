"""ASGI server and runtime foundation for Kaiwen Gateway.

This module provides the ASGI application factory, lifespan management, and
HTTP health/readiness endpoints for hosting Kaiwen Gateway.
"""

from kaiwen_gateway.server.app import create_gateway_app, default_app
from kaiwen_gateway.server.cli import (
    build_parser,
    main,
    resolve_config,
    run_check_config,
    run_demo,
    run_serve,
)
from kaiwen_gateway.server.exceptions import (
    ServerDependencyError,
    ServerError,
    ServerLifecycleError,
)
from kaiwen_gateway.server.lifespan import gateway_lifespan

__all__ = [
    "ServerError",
    "ServerDependencyError",
    "ServerLifecycleError",
    "build_parser",
    "create_gateway_app",
    "default_app",
    "gateway_lifespan",
    "main",
    "resolve_config",
    "run_check_config",
    "run_demo",
    "run_serve",
]
