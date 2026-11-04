"""Package foundation, typing marker, and import side-effect tests for kaiwen_gateway."""

from __future__ import annotations

import importlib
import importlib.metadata
import os
import subprocess
import sys
import tomllib
from pathlib import Path

import kaiwen_gateway

REPO_ROOT = Path(__file__).resolve().parents[1]


def test_package_importable() -> None:
    """The top-level package can be imported without error."""
    module = importlib.import_module("kaiwen_gateway")
    assert module is kaiwen_gateway
    assert module.__name__ == "kaiwen_gateway"


def test_public_api_surface_is_minimal() -> None:
    """Only documented package-foundation symbols are exported in __all__."""
    assert hasattr(kaiwen_gateway, "__all__")
    assert list(kaiwen_gateway.__all__) == ["__version__"]
    for symbol in kaiwen_gateway.__all__:
        assert hasattr(kaiwen_gateway, symbol)
    unexported_public_objects = [
        name
        for name in dir(kaiwen_gateway)
        if not name.startswith("_") and name not in {"Final", "core", "server", "transports"}
    ]
    assert unexported_public_objects == []


def test_version_matches_distribution_and_pyproject_metadata() -> None:
    """__version__ stays synchronized with installed distribution metadata and pyproject.toml."""
    pyproject_data = tomllib.loads((REPO_ROOT / "pyproject.toml").read_text(encoding="utf-8"))
    pyproject_version = pyproject_data["project"]["version"]
    installed_version = importlib.metadata.version("kaiwen-gateway")

    assert isinstance(kaiwen_gateway.__version__, str)
    assert kaiwen_gateway.__version__ == pyproject_version
    assert kaiwen_gateway.__version__ == installed_version


def test_py_typed_marker_is_present() -> None:
    """PEP 561 py.typed marker exists inside the installed/importable package directory."""
    assert kaiwen_gateway.__file__ is not None
    package_dir = Path(kaiwen_gateway.__file__).resolve().parent
    py_typed = package_dir / "py.typed"
    assert py_typed.is_file()


def test_runtime_dependencies_remain_empty() -> None:
    """Development tools must not leak into mandatory package runtime dependencies."""
    pyproject_data = tomllib.loads((REPO_ROOT / "pyproject.toml").read_text(encoding="utf-8"))
    assert pyproject_data["project"]["dependencies"] == []
    assert "server" in pyproject_data["project"]["optional-dependencies"]
    reqs = importlib.metadata.requires("kaiwen-gateway")
    mandatory_reqs = [r for r in (reqs or []) if "extra ==" not in r]
    assert mandatory_reqs == []
    optional_reqs = [r for r in (reqs or []) if "extra ==" in r]
    assert len(optional_reqs) == 3
    for r in optional_reqs:
        assert "server" in r and "extra ==" in r


def test_import_has_no_runtime_side_effects_in_isolated_subprocess(tmp_path: Path) -> None:
    """Importing kaiwen_gateway in a fresh process has no network, thread, file, or SDK side effects."""
    expected_init = str((REPO_ROOT / "src" / "kaiwen_gateway" / "__init__.py").resolve())
    probe_script = """
from __future__ import annotations

import os
import pathlib
import socket
import sys
import threading
import typing

def _deny_network(*args: object, **kwargs: object) -> None:
    raise AssertionError(
        f"Unexpected network socket operation during import: {args!r} {kwargs!r}"
    )

socket.socket.connect = _deny_network  # type: ignore[method-assign]
socket.socket.connect_ex = _deny_network  # type: ignore[method-assign]
socket.socket.bind = _deny_network  # type: ignore[method-assign]
socket.create_connection = _deny_network  # type: ignore[assignment]
socket.getaddrinfo = _deny_network  # type: ignore[assignment]

_ = typing.Final
cwd_before = set(os.listdir("."))
threads_before = threading.active_count()
modules_before = set(sys.modules.keys())

import kaiwen_gateway

cwd_after = set(os.listdir("."))
threads_after = threading.active_count()
modules_after = set(sys.modules.keys())
new_modules = sorted(modules_after - modules_before)

assert kaiwen_gateway.__file__ is not None
assert str(pathlib.Path(kaiwen_gateway.__file__).resolve()) == sys.argv[1]
assert kaiwen_gateway.__version__ == "0.1.0a1"
assert cwd_before == cwd_after, f"Files created on import: {cwd_after - cwd_before}"
assert threads_before == threads_after == 1, f"Threads changed: {threads_before} -> {threads_after}"
assert new_modules == ["kaiwen_gateway"], f"Unexpected modules loaded: {new_modules}"

for forbidden in ("fastapi", "starlette", "uvicorn", "openai", "kaiwen_agent", "asyncio.base_events"):
    assert forbidden not in sys.modules, f"Forbidden module loaded during import: {forbidden}"
"""
    clean_env = {
        key: value
        for key, value in os.environ.items()
        if not key.startswith(("OPENAI_", "KAIWEN_", "VERIMOR_"))
    }
    result = subprocess.run(
        [sys.executable, "-c", probe_script, expected_init],
        cwd=tmp_path,
        env=clean_env,
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, (
        f"Subprocess failed:\nSTDOUT:\n{result.stdout}\nSTDERR:\n{result.stderr}"
    )
