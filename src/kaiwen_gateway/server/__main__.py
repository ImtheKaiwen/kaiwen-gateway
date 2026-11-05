"""Local development entrypoint for Kaiwen Gateway ASGI server.

Binds exclusively to the loopback interface (127.0.0.1:8000).
"""

from __future__ import annotations

import sys


def main() -> None:
    """Run local development ASGI server on 127.0.0.1:8000."""
    try:
        import uvicorn
    except ImportError:  # pragma: no cover
        sys.stderr.write(
            "Error: 'uvicorn' is required to run the local development server.\n"
            "Developer installation: uv sync --locked --group dev --extra server\n"
            "Consumer installation: pip install 'kaiwen-gateway[server]'\n"
        )
        sys.exit(1)

    uvicorn.run(
        "kaiwen_gateway.server:create_gateway_app",
        factory=True,
        host="127.0.0.1",
        port=8000,
        log_level="info",
        ws_max_size=65536,
    )


if __name__ == "__main__":  # pragma: no cover
    main()
