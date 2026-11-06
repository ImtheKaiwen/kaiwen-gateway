"""Local development entrypoint and CLI runner for Kaiwen Gateway.

Binds exclusively to the loopback interface (127.0.0.1:8000) by default,
or dispatches subcommands via :mod:`kaiwen_gateway.server.cli`.
"""

from __future__ import annotations

import sys

from kaiwen_gateway.server.cli import main

__all__ = ["main"]

if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
