"""Unit, contract, and architectural boundary tests for kaiwen_gateway.core (M2.1)."""

from __future__ import annotations

import ast
import asyncio
import dataclasses
import os
import subprocess
import sys
from pathlib import Path

import pytest

from kaiwen_gateway.core import (
    CORE_CONTRACT_VERSION,
    MAX_DESCRIPTION_LENGTH,
    SUPPORTED_CONTRACT_VERSIONS,
    CoreError,
    DuplicateModuleError,
    GatewayModule,
    InvalidModuleDescriptorError,
    InvalidModuleIdError,
    ModuleDescriptor,
    ModuleLifecycle,
    ModuleNotRegisteredError,
    ModuleRegistrationError,
    ModuleRegistry,
    UnsupportedContractVersionError,
    validate_contract_version,
    validate_gateway_module,
    validate_module_descriptor,
    validate_module_id,
)

REPO_ROOT = Path(__file__).resolve().parents[1]
CORE_DIR = REPO_ROOT / "src" / "kaiwen_gateway" / "core"

FORBIDDEN_IMPORT_PREFIXES = (
    "fastapi",
    "starlette",
    "uvicorn",
    "openai",
    "kaiwen_agent",
    "verimor",
    "redis",
    "sqlalchemy",
    "psycopg",
    "httpx",
    "aiohttp",
    "websockets",
    "kaiwen_gateway.modules",
    "kaiwen_gateway.voice",
    "kaiwen_gateway.transports",
    "kaiwen_gateway.server",
    "kaiwen_gateway.cli",
)


class FakeGatewayModule:
    """Synthetic in-memory module satisfying the GatewayModule protocol."""

    def __init__(self, descriptor: ModuleDescriptor) -> None:
        self._descriptor: ModuleDescriptor = descriptor
        self.events: list[str] = []

    @property
    def descriptor(self) -> ModuleDescriptor:
        return self._descriptor

    async def startup(self) -> None:
        self.events.append("startup")

    async def shutdown(self) -> None:
        self.events.append("shutdown")


def _make_module(
    module_id: str = "voice",
    *,
    contract_version: str = CORE_CONTRACT_VERSION,
    description: str = "Synthetic test module",
) -> FakeGatewayModule:
    return FakeGatewayModule(
        ModuleDescriptor(
            module_id=module_id,
            contract_version=contract_version,
            description=description,
        )
    )


def test_register_valid_fake_module_and_lifecycle_protocol() -> None:
    """Scenario 1: A valid fake module satisfies GatewayModule and registers cleanly."""
    registry = ModuleRegistry()
    module = _make_module("voice", description="Bidirectional synthetic voice module")

    assert isinstance(module, ModuleLifecycle)
    assert isinstance(module, GatewayModule)
    assert module.descriptor.validate() is module.descriptor

    registered = registry.register(module)
    assert registered is module
    assert len(registry) == 1
    assert "voice" in registry
    assert registry.contains("voice") is True

    # Verify async lifecycle hooks are awaitable coroutines without side effects at registration
    assert module.events == []
    asyncio.run(module.startup())
    assert module.events == ["startup"]
    asyncio.run(module.shutdown())
    assert module.events == ["startup", "shutdown"]


def test_lookup_registered_and_unregistered_modules() -> None:
    """Scenario 2: Registered modules can be retrieved by ID; missing IDs fail deterministically."""
    registry = ModuleRegistry()
    voice_module = _make_module("voice")
    registry.register(voice_module)

    assert registry.get("voice") is voice_module
    assert registry.find("voice") is voice_module
    assert registry.find("telemetry_sink") is None
    assert registry.contains("telemetry_sink") is False
    assert "telemetry_sink" not in registry
    assert 123 not in registry

    with pytest.raises(ModuleNotRegisteredError) as exc_info:
        registry.get("telemetry_sink")
    assert exc_info.value.module_id == "telemetry_sink"
    assert isinstance(exc_info.value, CoreError)
    assert isinstance(exc_info.value, KeyError)


def test_duplicate_module_id_rejection() -> None:
    """Scenario 3: Registering a duplicate module ID is rejected with DuplicateModuleError."""
    registry = ModuleRegistry()
    first = _make_module("voice", description="First instance")
    second = _make_module("voice", description="Second instance")

    registry.register(first)
    with pytest.raises(DuplicateModuleError) as exc_info:
        registry.register(second)

    assert exc_info.value.module_id == "voice"
    assert isinstance(exc_info.value, ModuleRegistrationError)
    assert registry.get("voice") is first


@pytest.mark.parametrize(
    "bad_id",
    [
        "",
        " ",
        "Voice",
        "VOICE",
        "1voice",
        "_voice",
        "voice-module",
        "voice.module",
        "voice module",
        "voice/module",
        "a" * 65,
        None,
        42,
    ],
)
def test_invalid_module_id_rejection(bad_id: object) -> None:
    """Scenario 4: Invalid module IDs are rejected by validation, registration, and lookup."""
    with pytest.raises(InvalidModuleIdError) as exc_info:
        validate_module_id(bad_id)
    assert exc_info.value.module_id == bad_id
    assert isinstance(exc_info.value, ModuleRegistrationError)

    registry = ModuleRegistry()
    bad_module = FakeGatewayModule(
        ModuleDescriptor(module_id=bad_id)  # type: ignore[arg-type]
    )
    with pytest.raises(InvalidModuleIdError):
        registry.register(bad_module)

    with pytest.raises(InvalidModuleIdError):
        registry.get(bad_id)  # type: ignore[arg-type]
    with pytest.raises(InvalidModuleIdError):
        registry.find(bad_id)  # type: ignore[arg-type]
    with pytest.raises(InvalidModuleIdError):
        registry.contains(bad_id)  # type: ignore[arg-type]


@pytest.mark.parametrize("unsupported_version", ["0.1", "0.9", "1.1", "2.0", "1.0.0", "v1.0"])
def test_unsupported_contract_version_rejection(unsupported_version: str) -> None:
    """Scenario 5: Unsupported contract versions are rejected with UnsupportedContractVersionError."""
    with pytest.raises(UnsupportedContractVersionError) as exc_info:
        validate_contract_version(unsupported_version, module_id="voice")
    assert exc_info.value.contract_version == unsupported_version
    assert exc_info.value.supported_versions == SUPPORTED_CONTRACT_VERSIONS
    assert exc_info.value.module_id == "voice"

    registry = ModuleRegistry()
    incompatible_module = _make_module("voice", contract_version=unsupported_version)
    with pytest.raises(UnsupportedContractVersionError) as reg_exc:
        registry.register(incompatible_module)
    assert reg_exc.value.contract_version == unsupported_version
    assert reg_exc.value.module_id == "voice"


def test_invalid_descriptor_and_lifecycle_rejection() -> None:
    """Scenario 6: Malformed descriptors, non-frozen mutations, and bad lifecycle hooks fail."""
    descriptor = ModuleDescriptor(module_id="voice")
    with pytest.raises(dataclasses.FrozenInstanceError):
        descriptor.module_id = "tampered"  # type: ignore[misc]

    # Non-ModuleDescriptor passed to validate_module_descriptor
    with pytest.raises(InvalidModuleDescriptorError):
        validate_module_descriptor("not-a-descriptor")

    # Empty, whitespace-padded, or non-str contract_version
    for bad_version in ("", "   ", " 1.0", "1.0 ", None, 10):
        with pytest.raises(InvalidModuleDescriptorError):
            validate_contract_version(bad_version)

    # Invalid description types, length, or control characters
    with pytest.raises(InvalidModuleDescriptorError):
        ModuleDescriptor(module_id="voice", description=123).validate()  # type: ignore[arg-type]
    with pytest.raises(InvalidModuleDescriptorError):
        ModuleDescriptor(
            module_id="voice",
            description="x" * (MAX_DESCRIPTION_LENGTH + 1),
        ).validate()
    with pytest.raises(InvalidModuleDescriptorError):
        ModuleDescriptor(module_id="voice", description="bad\x00char").validate()
    with pytest.raises(InvalidModuleDescriptorError):
        ModuleDescriptor(module_id="voice", description="bad\x7fchar").validate()

    # None or class object instead of module instance
    with pytest.raises(InvalidModuleDescriptorError):
        validate_gateway_module(None)
    with pytest.raises(InvalidModuleDescriptorError):
        validate_gateway_module(FakeGatewayModule)

    # Object without descriptor attribute or with raising descriptor property
    class NoDescriptorModule:
        async def startup(self) -> None:
            pass

        async def shutdown(self) -> None:
            pass

    class RaisingDescriptorModule:
        @property
        def descriptor(self) -> ModuleDescriptor:
            raise RuntimeError("boom")

        async def startup(self) -> None:
            pass

        async def shutdown(self) -> None:
            pass

    class SyncStartupModule:
        @property
        def descriptor(self) -> ModuleDescriptor:
            return ModuleDescriptor(module_id="voice")

        def startup(self) -> None:
            pass

        async def shutdown(self) -> None:
            pass

    class SyncShutdownModule:
        @property
        def descriptor(self) -> ModuleDescriptor:
            return ModuleDescriptor(module_id="voice")

        async def startup(self) -> None:
            pass

        def shutdown(self) -> None:
            pass

    registry = ModuleRegistry()
    for invalid_module in (
        NoDescriptorModule(),
        RaisingDescriptorModule(),
        SyncStartupModule(),
        SyncShutdownModule(),
    ):
        with pytest.raises(InvalidModuleDescriptorError):
            registry.register(invalid_module)


def test_failed_registration_does_not_mutate_registry_state() -> None:
    """Scenario 7: Failed registration attempts leave existing registry entries untouched."""
    registry = ModuleRegistry()
    voice = _make_module("voice", description="Original voice module")
    registry.register(voice)
    snapshot_ids = registry.module_ids()
    snapshot_descriptors = registry.descriptors()
    snapshot_modules = registry.modules()

    # Attempt duplicate registration
    with pytest.raises(DuplicateModuleError):
        registry.register(_make_module("voice", description="Duplicate voice module"))

    # Attempt invalid module_id
    with pytest.raises(InvalidModuleIdError):
        registry.register(_make_module("Invalid-ID"))

    # Attempt unsupported contract version
    with pytest.raises(UnsupportedContractVersionError):
        registry.register(_make_module("voice_v2", contract_version="2.0"))

    # Attempt invalid module object
    with pytest.raises(InvalidModuleDescriptorError):
        registry.register(object())

    assert len(registry) == 1
    assert registry.module_ids() == snapshot_ids == ("voice",)
    assert registry.descriptors() == snapshot_descriptors
    assert registry.modules() == snapshot_modules
    assert registry.get("voice") is voice


def test_registry_instance_isolation() -> None:
    """Scenario 8: Separate ModuleRegistry instances never share registered modules."""
    registry_a = ModuleRegistry()
    registry_b = ModuleRegistry()

    voice_a = _make_module("voice", description="Instance A voice")
    voice_b = _make_module("voice", description="Instance B voice")
    audit_b = _make_module("audit_probe", description="Instance B probe")

    registry_a.register(voice_a)
    registry_b.register(voice_b)
    registry_b.register(audit_b)

    assert registry_a.module_ids() == ("voice",)
    assert registry_b.module_ids() == ("voice", "audit_probe")
    assert registry_a.get("voice") is voice_a
    assert registry_b.get("voice") is voice_b
    assert "audit_probe" not in registry_a


def test_deterministic_module_listing_and_iteration() -> None:
    """Scenario 9: Module IDs, descriptors, and modules are returned in exact registration order."""
    registry = ModuleRegistry()
    mod_alpha = _make_module("alpha_mod", description="Alpha")
    mod_beta = _make_module("beta_mod", description="Beta")
    mod_gamma = _make_module("gamma_mod", description="Gamma")

    for mod in (mod_alpha, mod_beta, mod_gamma):
        registry.register(mod)

    assert registry.module_ids() == ("alpha_mod", "beta_mod", "gamma_mod")
    assert tuple(registry) == ("alpha_mod", "beta_mod", "gamma_mod")
    assert registry.modules() == (mod_alpha, mod_beta, mod_gamma)
    assert registry.descriptors() == (
        mod_alpha.descriptor,
        mod_beta.descriptor,
        mod_gamma.descriptor,
    )


def test_internal_registry_state_is_not_exposed_mutably() -> None:
    """Scenario 10: Read views return immutable snapshots and cannot mutate internal state."""
    registry = ModuleRegistry()
    voice = _make_module("voice")
    registry.register(voice)

    mapping_view = registry.as_mapping()
    assert mapping_view["voice"] is voice
    with pytest.raises(TypeError):
        mapping_view["injected"] = _make_module("injected")  # type: ignore[index]

    # Registering a new module after taking a snapshot does not mutate the prior snapshot
    extra = _make_module("extra_mod")
    registry.register(extra)
    assert "extra_mod" not in mapping_view
    assert "extra_mod" in registry.as_mapping()

    # ModuleRegistry uses __slots__ and has no __dict__
    assert not hasattr(registry, "__dict__")


def test_core_static_import_boundaries() -> None:
    """Scenario 11a: Static AST check confirms kaiwen_gateway.core imports only stdlib and core."""
    py_files = sorted(CORE_DIR.rglob("*.py"))
    assert len(py_files) >= 4

    for py_file in py_files:
        tree = ast.parse(py_file.read_text(encoding="utf-8"), filename=str(py_file))
        for node in ast.walk(tree):
            imported_names: list[str] = []
            if isinstance(node, ast.Import):
                imported_names.extend(alias.name for alias in node.names)
            elif isinstance(node, ast.ImportFrom) and node.module is not None:
                imported_names.append(node.module)

            for imported in imported_names:
                for forbidden in FORBIDDEN_IMPORT_PREFIXES:
                    assert not (imported == forbidden or imported.startswith(f"{forbidden}.")), (
                        f"Forbidden import {imported!r} in {py_file}"
                    )
                if imported.startswith("kaiwen_gateway"):
                    assert imported == "kaiwen_gateway.core" or imported.startswith(
                        "kaiwen_gateway.core."
                    ), f"Non-core internal import {imported!r} in {py_file}"


def test_core_subprocess_import_has_no_side_effects_or_forbidden_modules(tmp_path: Path) -> None:
    """Scenario 11b: Importing kaiwen_gateway.core in a clean process has no side effects."""
    probe_script = """
from __future__ import annotations

import os
import socket
import sys
import threading

def _deny_network(*args: object, **kwargs: object) -> None:
    raise AssertionError(f"Unexpected network call during core import: {args!r} {kwargs!r}")

socket.socket.connect = _deny_network  # type: ignore[method-assign]
socket.socket.connect_ex = _deny_network  # type: ignore[method-assign]
socket.socket.bind = _deny_network  # type: ignore[method-assign]
socket.create_connection = _deny_network  # type: ignore[assignment]
socket.getaddrinfo = _deny_network  # type: ignore[assignment]

cwd_before = set(os.listdir("."))
threads_before = threading.active_count()

import kaiwen_gateway.core as core

cwd_after = set(os.listdir("."))
threads_after = threading.active_count()

assert core.CORE_CONTRACT_VERSION == "1.0"
assert cwd_before == cwd_after
assert threads_before == threads_after == 1

forbidden_prefixes = (
    "fastapi",
    "starlette",
    "uvicorn",
    "openai",
    "kaiwen_agent",
    "verimor",
    "redis",
    "sqlalchemy",
    "psycopg",
    "kaiwen_gateway.modules",
    "kaiwen_gateway.voice",
    "kaiwen_gateway.transports",
    "kaiwen_gateway.server",
)
for loaded in tuple(sys.modules.keys()):
    for prefix in forbidden_prefixes:
        assert not (
            loaded == prefix or loaded.startswith(f"{prefix}.")
        ), f"Forbidden module {loaded!r} loaded when importing kaiwen_gateway.core"
"""
    clean_env = {
        key: value
        for key, value in os.environ.items()
        if not key.startswith(("OPENAI_", "KAIWEN_", "VERIMOR_"))
    }
    result = subprocess.run(
        [sys.executable, "-c", probe_script],
        cwd=tmp_path,
        env=clean_env,
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, (
        f"Subprocess failed:\nSTDOUT:\n{result.stdout}\nSTDERR:\n{result.stderr}"
    )
