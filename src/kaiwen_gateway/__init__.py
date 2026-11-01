"""Kaiwen Gateway — modular communication gateway for real-time AI applications.

Importing :mod:`kaiwen_gateway` has no runtime side effects: it does not start
network listeners, contact external providers, require credentials, or spawn
background tasks.
"""

from typing import Final

__version__: Final[str] = "0.1.0a1"

__all__ = ["__version__"]
