"""Public API surface, package extras, entry points, and version distinction tests (S1-C)."""

from __future__ import annotations

import importlib
import subprocess
import sys
import tomllib
from pathlib import Path

import kaiwen_gateway
import kaiwen_gateway.core as core_pkg
import kaiwen_gateway.modules.voice as voice_pkg
import kaiwen_gateway.server as server_pkg
import kaiwen_gateway.testing as testing_pkg
from kaiwen_gateway.transports.synthetic.media_protocol import (
    SYNTHETIC_MEDIA_PROTOCOL_VERSION,
)
from kaiwen_gateway.transports.synthetic.protocol import (
    SYNTHETIC_PROTOCOL_VERSION,
)

REPO_ROOT = Path(__file__).resolve().parents[1]


def test_supported_public_exports_exist_and_have_no_private_leaks() -> None:
    """All __all__ entries in supported public modules exist and exclude private names."""
    for pkg in (kaiwen_gateway, core_pkg, voice_pkg, server_pkg, testing_pkg):
        exported = getattr(pkg, "__all__", None)
        assert isinstance(exported, list) and len(exported) > 0, f"{pkg.__name__} missing __all__"
        for symbol in exported:
            assert not symbol.startswith("_") or symbol == "__version__", (
                f"Private symbol {symbol!r} leaked into {pkg.__name__}.__all__"
            )
            assert hasattr(pkg, symbol), f"{pkg.__name__}.__all__ lists missing symbol {symbol!r}"


def test_key_public_symbols_remain_importable_from_documented_namespaces() -> None:
    """Key documented public symbols are directly importable from their canonical namespaces."""
    expected_core = {
        "CORE_CONTRACT_VERSION",
        "CoreConfig",
        "GatewayCore",
        "GatewayModule",
        "ModuleDescriptor",
        "ModuleLifecycle",
        "ModuleLifecycleManager",
        "ModuleLifecyclePolicy",
        "ModuleRegistry",
        "AdmissionController",
        "ConnectionSupervisor",
        "InstanceDrainCoordinator",
        "VerifiedConnectionContext",
        "compose_gateway_core",
    }
    assert expected_core.issubset(set(core_pkg.__all__))

    expected_voice = {
        "AIRealtimeMediaPort",
        "TelephonyMediaPort",
        "InterruptibleAIPort",
        "InterruptibleTelephonyPort",
        "AudioFrame",
        "AudioFormat",
        "VoiceBridgeSession",
        "VoiceBridgeState",
        "VoiceTelemetrySink",
        "InMemoryVoiceTelemetrySink",
    }
    assert expected_voice.issubset(set(voice_pkg.__all__))

    expected_server = {
        "create_gateway_app",
        "default_app",
        "gateway_lifespan",
        "main",
        "ServerError",
        "ServerDependencyError",
        "ServerLifecycleError",
    }
    assert expected_server.issubset(set(server_pkg.__all__))

    expected_testing = {
        "RealtimeAdapterConformanceSuite",
        "SyntheticUpstreamFailingAIAdapter",
        "create_test_frame",
    }
    assert expected_testing.issubset(set(testing_pkg.__all__))


def test_version_dimensions_are_distinct_and_consistent() -> None:
    """Package version, Core contract version, and synthetic wire protocol versions are distinct."""
    assert kaiwen_gateway.__version__ == "0.1.0a1"
    assert core_pkg.CORE_CONTRACT_VERSION == "1.0"
    assert SYNTHETIC_PROTOCOL_VERSION == "v1"
    assert SYNTHETIC_MEDIA_PROTOCOL_VERSION == "v1"
    assert kaiwen_gateway.__version__ != core_pkg.CORE_CONTRACT_VERSION


def test_pyproject_extras_and_entrypoints_match_implementation() -> None:
    """Documented package extras and console script entry points match pyproject.toml."""
    pyproject = tomllib.loads((REPO_ROOT / "pyproject.toml").read_text(encoding="utf-8"))
    assert pyproject["project"]["scripts"] == {
        "kaiwen-gateway": "kaiwen_gateway.server.__main__:main"
    }
    extras = pyproject["project"]["optional-dependencies"]
    assert set(extras.keys()) == {"server"}
    server_deps = " ".join(extras["server"])
    for dep in ("starlette", "uvicorn", "websockets"):
        assert dep in server_deps

    # Verify target callable exists
    mod_name, attr_name = "kaiwen_gateway.server.__main__", "main"
    mod = importlib.import_module(mod_name)
    assert callable(getattr(mod, attr_name))


def test_core_and_voice_import_without_server_extras_in_subprocess(tmp_path: Path) -> None:
    """Importing core and modules.voice does not load starlette, uvicorn, or websockets."""
    script = """
import sys
import kaiwen_gateway.core
import kaiwen_gateway.modules.voice

for forbidden in ("starlette", "uvicorn", "websockets", "fastapi", "openai", "kaiwen_agent"):
    assert forbidden not in sys.modules, f"{forbidden} unexpectedly imported"
"""
    res = subprocess.run(
        [sys.executable, "-c", script],
        cwd=tmp_path,
        capture_output=True,
        text=True,
        check=False,
    )
    assert res.returncode == 0, f"STDERR: {res.stderr}\nSTDOUT: {res.stdout}"
