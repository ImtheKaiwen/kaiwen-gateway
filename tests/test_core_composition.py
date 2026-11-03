"""Unit, contract, transactional composition, and isolation tests for M2.2A."""

from __future__ import annotations

import asyncio
import dataclasses
import threading
from collections.abc import Iterator
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import NoReturn, cast

import pytest
from mypy import api as mypy_api

from kaiwen_gateway.core import (
    CORE_CONTRACT_VERSION,
    DEFAULT_INSTANCE_NAME,
    DEFAULT_MAX_CONCURRENT_CONNECTIONS,
    DEFAULT_MAX_CONNECTIONS_PER_TENANT,
    INGRESS_MODE_DISABLED,
    MAX_CONFIGURED_CONNECTIONS,
    MAX_INSTANCE_NAME_LENGTH,
    SUPPORTED_INGRESS_MODES,
    CoreConfig,
    CoreConfigurationError,
    CoreError,
    DuplicateModuleError,
    GatewayCore,
    GatewayModule,
    InvalidModuleDescriptorError,
    InvalidModuleIdError,
    ModuleDescriptor,
    ModuleNotRegisteredError,
    ModuleRegistry,
    UnsupportedContractVersionError,
    compose_gateway_core,
    validate_core_config,
)
from kaiwen_gateway.core.composition import MAX_COMPOSED_MODULES


class FakeModule:
    """In-memory synthetic module for composition and lifecycle isolation tests."""

    def __init__(
        self,
        module_id: str = "voice",
        *,
        contract_version: str = CORE_CONTRACT_VERSION,
        description: str = "Synthetic module",
    ) -> None:
        self._descriptor = ModuleDescriptor(
            module_id=module_id,
            contract_version=contract_version,
            description=description,
        )
        self.events: list[str] = []

    @property
    def descriptor(self) -> ModuleDescriptor:
        return self._descriptor

    async def startup(self) -> None:
        self.events.append("startup")

    async def shutdown(self) -> None:
        self.events.append("shutdown")


def test_valid_core_config_defaults_and_custom_values() -> None:
    """CoreConfig provides safe validated defaults and accepts valid custom bounds."""
    default_cfg = CoreConfig()
    assert default_cfg.instance_name == DEFAULT_INSTANCE_NAME == "default"
    assert default_cfg.max_concurrent_connections == DEFAULT_MAX_CONCURRENT_CONNECTIONS == 25
    assert default_cfg.max_connections_per_tenant == DEFAULT_MAX_CONNECTIONS_PER_TENANT == 10
    assert default_cfg.ingress_mode == INGRESS_MODE_DISABLED == "disabled"
    assert default_cfg.validate() is default_cfg
    assert validate_core_config(default_cfg) is default_cfg

    custom_cfg = CoreConfig(
        instance_name="voice_gateway-01.prod_sim",
        max_concurrent_connections=100,
        max_connections_per_tenant=100,
        ingress_mode="disabled",
    )
    assert custom_cfg.instance_name == "voice_gateway-01.prod_sim"
    assert custom_cfg.max_concurrent_connections == 100
    assert custom_cfg.max_connections_per_tenant == 100
    assert custom_cfg.ingress_mode == "disabled"


def test_core_config_from_mapping_and_fail_closed_unknown_keys() -> None:
    """CoreConfig.from_mapping constructs valid configs and fails closed on unknown keys."""
    cfg_defaults = CoreConfig.from_mapping({})
    assert cfg_defaults == CoreConfig()

    cfg_custom = CoreConfig.from_mapping(
        {
            "instance_name": "embedded_a",
            "max_concurrent_connections": 50,
            "max_connections_per_tenant": 20,
            "ingress_mode": "disabled",
        }
    )
    assert cfg_custom.instance_name == "embedded_a"
    assert cfg_custom.max_concurrent_connections == 50
    assert cfg_custom.max_connections_per_tenant == 20

    # Non-mapping input rejected
    for bad_mapping in (None, "not-a-dict", ["instance_name"], 42):
        with pytest.raises(CoreConfigurationError) as exc_info:
            CoreConfig.from_mapping(bad_mapping)  # type: ignore[arg-type]
        assert isinstance(exc_info.value, CoreError)
        assert isinstance(exc_info.value, ValueError)

    # Non-string key rejected
    with pytest.raises(CoreConfigurationError):
        CoreConfig.from_mapping({123: "default"})  # type: ignore[dict-item]

    # Unknown or security-bypass keys fail closed
    for forbidden_payload in (
        {"allow_unauthenticated": True},
        {"disable_auth": True},
        {"public_ingress": True},
        {"enable_wss": True},
        {"host": "0.0.0.0", "port": 8080},
        {"unknown_setting": "value"},
    ):
        with pytest.raises(CoreConfigurationError) as exc_info:
            CoreConfig.from_mapping(forbidden_payload)
        assert "fail-closed" in str(exc_info.value)


def test_core_config_immutability() -> None:
    """CoreConfig instances are frozen, slotted value objects."""
    cfg = CoreConfig()
    assert not hasattr(cfg, "__dict__")
    with pytest.raises(dataclasses.FrozenInstanceError):
        cfg.instance_name = "mutated"  # type: ignore[misc]
    with pytest.raises(dataclasses.FrozenInstanceError):
        cfg.max_concurrent_connections = 999  # type: ignore[misc]


@pytest.mark.parametrize(
    "bad_instance_name",
    [
        "",
        "   ",
        " leading",
        "trailing ",
        "-starts-with-dash",
        "_starts-with-underscore",
        ".starts-with-dot",
        "has space",
        "has/slash",
        "bad\x00char",
        "bad\x7fchar",
        "a" * (MAX_INSTANCE_NAME_LENGTH + 1),
        None,
        123,
        True,
    ],
)
def test_core_config_rejects_invalid_instance_name(bad_instance_name: object) -> None:
    """Invalid instance_name values or types fail deterministically."""
    with pytest.raises(CoreConfigurationError) as exc_info:
        CoreConfig(instance_name=bad_instance_name)  # type: ignore[arg-type]
    assert exc_info.value.field_name == "instance_name"


@pytest.mark.parametrize(
    "bad_limit",
    [
        0,
        -1,
        -100,
        MAX_CONFIGURED_CONNECTIONS + 1,
        True,
        False,
        25.0,
        "25",
        None,
    ],
)
def test_core_config_rejects_invalid_numeric_limits(bad_limit: object) -> None:
    """Zero, negative, above-ceiling, boolean, or non-int connection limits are rejected."""
    with pytest.raises(CoreConfigurationError) as exc_info:
        CoreConfig(max_concurrent_connections=bad_limit)  # type: ignore[arg-type]
    assert exc_info.value.field_name == "max_concurrent_connections"

    with pytest.raises(CoreConfigurationError) as exc_info_tenant:
        CoreConfig(
            max_concurrent_connections=100,
            max_connections_per_tenant=bad_limit,  # type: ignore[arg-type]
        )
    assert exc_info_tenant.value.field_name == "max_connections_per_tenant"


def test_core_config_rejects_per_tenant_exceeding_total_limit() -> None:
    """max_connections_per_tenant cannot exceed max_concurrent_connections."""
    with pytest.raises(CoreConfigurationError) as exc_info:
        CoreConfig(max_concurrent_connections=10, max_connections_per_tenant=11)
    assert exc_info.value.field_name == "max_connections_per_tenant"


@pytest.mark.parametrize(
    "bad_ingress_mode",
    [
        "",
        "enabled",
        "public",
        "wss",
        "http",
        "tls",
        "insecure",
        "unauthenticated",
        "DISABLED",
        " disabled ",
        None,
        True,
        False,
        1,
    ],
)
def test_core_config_fails_closed_on_unverified_ingress_mode(bad_ingress_mode: object) -> None:
    """Any attempt to enable external/public ingress in CoreConfig fails closed."""
    assert SUPPORTED_INGRESS_MODES == frozenset({"disabled"})
    with pytest.raises(CoreConfigurationError) as exc_info:
        CoreConfig(ingress_mode=bad_ingress_mode)  # type: ignore[arg-type]
    assert exc_info.value.field_name == "ingress_mode"


def test_validate_core_config_rejects_non_config_objects() -> None:
    """validate_core_config and GatewayCore reject non-CoreConfig objects."""
    for bad_config in ("default", {"instance_name": "default"}, 123, CoreConfig):
        with pytest.raises(CoreConfigurationError):
            validate_core_config(bad_config)
        with pytest.raises(CoreConfigurationError):
            GatewayCore(config=bad_config)  # type: ignore[arg-type]


def test_valid_gateway_core_composition_and_lookup_methods() -> None:
    """GatewayCore and compose_gateway_core wire config and modules without mutating modules."""
    voice = FakeModule("voice", description="Voice module")
    telemetry = FakeModule("telemetry_probe", description="Telemetry probe")
    voice_dict_before = dict(voice.__dict__)
    telemetry_dict_before = dict(telemetry.__dict__)

    cfg = CoreConfig(
        instance_name="gateway_alpha",
        max_concurrent_connections=40,
        max_connections_per_tenant=15,
    )

    core = compose_gateway_core(config=cfg, modules=[voice, telemetry])

    assert isinstance(core, GatewayCore)
    assert core.config is cfg
    assert core.instance_id.startswith("gateway_alpha-")

    # Caller module objects are never mutated with private ownership attributes
    assert voice.__dict__ == voice_dict_before
    assert telemetry.__dict__ == telemetry_dict_before
    assert not hasattr(voice, "_kaiwen_gateway_owner_id")
    assert not hasattr(telemetry, "_kaiwen_gateway_owner_id")

    # Lookup, container, and inspection methods
    assert len(core) == 2
    assert "voice" in core
    assert "unregistered" not in core
    assert 123 not in core
    assert tuple(core) == ("voice", "telemetry_probe")
    assert core.module_ids() == ("voice", "telemetry_probe")
    assert core.modules() == (voice, telemetry)
    assert core.descriptors() == (voice.descriptor, telemetry.descriptor)
    assert core.has_module("voice") is True
    assert core.has_module("unregistered") is False
    assert core.get_module("voice") is voice
    assert core.find_module("telemetry_probe") is telemetry
    assert core.find_module("unregistered") is None

    with pytest.raises(ModuleNotRegisteredError):
        core.get_module("unregistered")

    # Construction must NEVER invoke startup() or shutdown()
    assert voice.events == []
    assert telemetry.events == []
    assert not hasattr(core, "__dict__")


def test_gateway_core_prevents_post_construction_registry_mutation() -> None:
    """GatewayCore separates read-only inspection from mutation and hides its internal registry."""
    standalone_registry = ModuleRegistry()
    voice = FakeModule("voice")
    standalone_registry.register(voice)

    core = compose_gateway_core(modules=standalone_registry.modules())
    assert len(core) == 1

    # Mutating the standalone registry after composition does not affect GatewayCore
    extra = FakeModule("extra_mod")
    standalone_registry.register(extra)
    assert core.module_ids() == ("voice",)
    assert "extra_mod" not in core

    # GatewayCore does not expose a mutable .registry property or .register_module method
    assert not hasattr(core, "registry")
    assert not hasattr(core, "register_module")
    with pytest.raises(AttributeError):
        _ = core.registry  # type: ignore[attr-defined]

    # Read-only mapping snapshot rejects mutation
    mapping_view = core.modules_mapping()
    assert mapping_view["voice"] is voice
    with pytest.raises(TypeError):
        mapping_view["injected"] = extra  # type: ignore[index]


@pytest.mark.parametrize(
    "bad_modules_arg",
    [
        "voice",
        b"voice",
        bytearray(b"voice"),
        {"voice": FakeModule("voice")},
        123,
        FakeModule("voice"),
    ],
)
def test_gateway_core_rejects_non_collection_modules_argument(bad_modules_arg: object) -> None:
    """Passing a string, bytes, mapping, or bare module instead of an iterable fails."""
    with pytest.raises(CoreConfigurationError) as exc_info:
        GatewayCore(modules=bad_modules_arg)  # type: ignore[arg-type]
    assert exc_info.value.field_name == "modules"


def test_slotted_frozen_and_attribute_restricted_modules_compose_cleanly() -> None:
    """Bulgu A & B: Slotted, frozen, and read-only GatewayModule implementations succeed."""

    class SlotOnlyModule:
        __slots__ = ("_descriptor",)

        def __init__(self, module_id: str = "slot_only") -> None:
            self._descriptor = ModuleDescriptor(module_id=module_id)

        @property
        def descriptor(self) -> ModuleDescriptor:
            return self._descriptor

        async def startup(self) -> None:
            pass

        async def shutdown(self) -> None:
            pass

    @dataclasses.dataclass(frozen=True, slots=True)
    class FrozenDataclassModule:
        descriptor: ModuleDescriptor = ModuleDescriptor(module_id="frozen_mod")

        async def startup(self) -> None:
            pass

        async def shutdown(self) -> None:
            pass

    class ReadOnlyOwnerPropertyModule:
        """Previously caused Phase 2 object.__setattr__ to fail mid-batch with AttributeError."""

        def __init__(self) -> None:
            self._descriptor = ModuleDescriptor(module_id="readonly_prop_mod")

        @property
        def descriptor(self) -> ModuleDescriptor:
            return self._descriptor

        @property
        def _kaiwen_gateway_owner_id(self) -> None:
            return None

        async def startup(self) -> None:
            pass

        async def shutdown(self) -> None:
            pass

    class RaisingOwnerPropertyModule:
        """Previously caused ownership inspection to fail on a valid GatewayModule."""

        @property
        def descriptor(self) -> ModuleDescriptor:
            return ModuleDescriptor(module_id="raising_prop_mod")

        @property
        def _kaiwen_gateway_owner_id(self) -> NoReturn:
            raise RuntimeError("custom attribute access boom")

        async def startup(self) -> None:
            pass

        async def shutdown(self) -> None:
            pass

    class PrepopulatedOwnerAttrModule:
        """Module defining its own _kaiwen_gateway_owner_id attribute for unrelated reasons."""

        def __init__(self) -> None:
            self._descriptor = ModuleDescriptor(module_id="prepopulated_mod")
            self._kaiwen_gateway_owner_id = "application-owned-tag"

        @property
        def descriptor(self) -> ModuleDescriptor:
            return self._descriptor

        async def startup(self) -> None:
            pass

        async def shutdown(self) -> None:
            pass

    class StrictNoSetattrModule:
        """Module forbidding any attribute assignment after construction."""

        _descriptor: ModuleDescriptor = ModuleDescriptor(module_id="strict_mod")

        @property
        def descriptor(self) -> ModuleDescriptor:
            return self._descriptor

        def __setattr__(self, name: str, value: object) -> NoReturn:
            raise PermissionError(f"Mutation of {name!r} is forbidden")

        async def startup(self) -> None:
            pass

        async def shutdown(self) -> None:
            pass

    strict_mod = StrictNoSetattrModule()
    with pytest.raises(PermissionError):
        strict_mod._kaiwen_gateway_owner_id = "forbidden"

    mods: list[GatewayModule] = [
        SlotOnlyModule(),
        FrozenDataclassModule(),
        ReadOnlyOwnerPropertyModule(),
        RaisingOwnerPropertyModule(),
        PrepopulatedOwnerAttrModule(),
        StrictNoSetattrModule(),
    ]
    core = compose_gateway_core(modules=mods)
    assert core.module_ids() == (
        "slot_only",
        "frozen_mod",
        "readonly_prop_mod",
        "raising_prop_mod",
        "prepopulated_mod",
        "strict_mod",
    )


def test_transactional_construction_and_repeated_composition_attempts() -> None:
    """Bulgu B: Failed multi-module composition leaves earlier modules completely untouched."""
    mod_first = FakeModule("voice", description="First valid module")
    mod_second = FakeModule("telemetry_probe", description="Second valid module")
    first_snapshot = dict(mod_first.__dict__)
    second_snapshot = dict(mod_second.__dict__)

    duplicate_voice = FakeModule("voice", description="Duplicate ID")
    bad_id_module = FakeModule("Invalid-ID")
    bad_version_module = FakeModule("voice_v2", contract_version="9.9")

    # Failure on 3rd module due to duplicate module_id
    with pytest.raises(DuplicateModuleError):
        compose_gateway_core(modules=[mod_first, mod_second, duplicate_voice])

    # Failure on 3rd module due to passing the exact same module object twice in one call
    with pytest.raises(DuplicateModuleError):
        compose_gateway_core(modules=[mod_first, mod_second, mod_first])

    # Failure on 3rd module due to invalid module_id
    with pytest.raises(InvalidModuleIdError):
        compose_gateway_core(modules=[mod_first, mod_second, bad_id_module])

    # Failure on 3rd module due to unsupported contract version
    with pytest.raises(UnsupportedContractVersionError):
        compose_gateway_core(modules=[mod_first, mod_second, bad_version_module])

    # Failure on 3rd module due to invalid module object
    with pytest.raises(InvalidModuleDescriptorError):
        compose_gateway_core(modules=[mod_first, mod_second, cast(GatewayModule, object())])

    # Earlier valid modules were never mutated or started
    assert mod_first.__dict__ == first_snapshot
    assert mod_second.__dict__ == second_snapshot
    assert mod_first.events == []
    assert mod_second.events == []

    # Subsequent valid composition with the exact same mod_first and mod_second succeeds cleanly
    core = compose_gateway_core(modules=[mod_first, mod_second])
    assert core.module_ids() == ("voice", "telemetry_probe")
    assert core.get_module("voice") is mod_first
    assert core.get_module("telemetry_probe") is mod_second


def test_two_gateway_core_instances_are_completely_isolated() -> None:
    """Two GatewayCore instances in the same process never share config, registry, or module state."""
    cfg_a = CoreConfig(
        instance_name="instance_a",
        max_concurrent_connections=20,
        max_connections_per_tenant=5,
    )
    cfg_b = CoreConfig(
        instance_name="instance_b",
        max_concurrent_connections=50,
        max_connections_per_tenant=25,
    )
    voice_a = FakeModule("voice", description="Voice A")
    voice_b = FakeModule("voice", description="Voice B")
    extra_b = FakeModule("audit_sink", description="Audit B")

    input_list_a = [voice_a]
    core_a = compose_gateway_core(config=cfg_a, modules=input_list_a)
    core_b = compose_gateway_core(config=cfg_b, modules=[voice_b, extra_b])

    # Mutating caller's input list after composition has no effect on core_a
    input_list_a.append(FakeModule("injected"))

    assert core_a.instance_id != core_b.instance_id
    assert core_a.config is cfg_a
    assert core_b.config is cfg_b
    assert core_a.module_ids() == ("voice",)
    assert core_b.module_ids() == ("voice", "audit_sink")
    assert core_a.get_module("voice") is voice_a
    assert core_b.get_module("voice") is voice_b
    assert core_a.has_module("audit_sink") is False
    assert core_b.has_module("audit_sink") is True

    # Invoking lifecycle on voice_a does not affect voice_b
    asyncio.run(voice_a.startup())
    assert voice_a.events == ["startup"]
    assert voice_b.events == []


def test_concurrent_gateway_core_composition_and_read_isolation() -> None:
    """Bulgu C: Independent GatewayCore compositions and reads across threads are isolated."""
    worker_count = 4
    barrier = threading.Barrier(worker_count)

    def _compose_and_inspect(index: int) -> tuple[str, str, FakeModule]:
        cfg = CoreConfig(
            instance_name=f"worker_{index}",
            max_concurrent_connections=10 + index,
            max_connections_per_tenant=5,
        )
        mod = FakeModule("voice", description=f"Worker {index} voice")
        barrier.wait(timeout=5.0)
        core = compose_gateway_core(config=cfg, modules=[mod])
        assert core.config is cfg
        assert core.get_module("voice") is mod
        assert core.module_ids() == ("voice",)
        return core.instance_id, core.config.instance_name, mod

    with ThreadPoolExecutor(max_workers=worker_count) as pool:
        results = list(pool.map(_compose_and_inspect, range(worker_count)))

    instance_ids = {instance_id for instance_id, _, _ in results}
    instance_names = [name for _, name, _ in results]
    modules = {id(mod) for _, _, mod in results}

    assert len(instance_ids) == worker_count
    assert instance_names == ["worker_0", "worker_1", "worker_2", "worker_3"]
    assert len(modules) == worker_count


def test_static_type_checker_rejects_invalid_composition_types(tmp_path: Path) -> None:
    """mypy --strict rejects non-CoreConfig or non-GatewayModule arguments to compose_gateway_core."""
    snippet = tmp_path / "bad_composition_call.py"
    snippet.write_text(
        "from kaiwen_gateway.core import compose_gateway_core\n"
        "compose_gateway_core(config='bad', modules=[object()])\n",
        encoding="utf-8",
    )
    stdout, stderr, exit_code = mypy_api.run(["--strict", str(snippet)])
    assert exit_code != 0, f"Expected mypy failure, got exit_code=0:\n{stdout}\n{stderr}"
    assert "[arg-type]" in stdout or "[list-item]" in stdout


def test_bounded_composition_accepts_empty_and_single_module_iterables() -> None:
    """Items 1 & 2: Empty iterable and single-module iterable compose cleanly."""
    empty_core = compose_gateway_core(modules=())
    assert len(empty_core) == 0
    assert empty_core.module_ids() == ()
    assert empty_core.modules() == ()

    empty_gen_core = compose_gateway_core(modules=(FakeModule(f"m_{i}") for i in range(0)))
    assert len(empty_gen_core) == 0

    voice = FakeModule("voice")
    single_core = compose_gateway_core(modules=[voice])
    assert len(single_core) == 1
    assert single_core.module_ids() == ("voice",)
    assert single_core.get_module("voice") is voice
    assert voice.events == []


def test_bounded_composition_accepts_exactly_64_unique_modules() -> None:
    """Item 3: Exactly 64 valid unique modules compose cleanly."""
    assert MAX_COMPOSED_MODULES == 64
    modules_64 = [FakeModule(f"mod_{idx:02d}") for idx in range(MAX_COMPOSED_MODULES)]
    snapshots = [dict(mod.__dict__) for mod in modules_64]

    core = compose_gateway_core(modules=modules_64)
    assert len(core) == 64
    assert core.module_ids() == tuple(f"mod_{idx:02d}" for idx in range(64))
    assert core.get_module("mod_00") is modules_64[0]
    assert core.get_module("mod_63") is modules_64[63]
    assert [dict(mod.__dict__) for mod in modules_64] == snapshots
    assert all(mod.events == [] for mod in modules_64)


def test_bounded_composition_rejects_65_modules_without_mutating_or_calling_hooks() -> None:
    """Items 4, 8, & 9: 65 modules fail with CoreConfigurationError(field_name='modules')."""
    modules_65 = [FakeModule(f"mod_{idx:02d}") for idx in range(MAX_COMPOSED_MODULES + 1)]
    snapshots = [dict(mod.__dict__) for mod in modules_65]

    with pytest.raises(CoreConfigurationError) as exc_info:
        compose_gateway_core(modules=modules_65)

    assert exc_info.value.field_name == "modules"
    assert "64" in str(exc_info.value)

    # Item 8 & 9: None of the supplied modules were mutated or had startup/shutdown called
    assert [dict(mod.__dict__) for mod in modules_65] == snapshots
    assert all(mod.events == [] for mod in modules_65)

    # First 64 modules remain reusable in a subsequent valid composition
    core = compose_gateway_core(modules=modules_65[:MAX_COMPOSED_MODULES])
    assert len(core) == 64
    assert core.get_module("mod_63") is modules_65[63]


def test_bounded_composition_supports_finite_generator_and_bounds_consumption() -> None:
    """Items 5, 6, & 10: Finite generators work; oversized generators stop at 65 items."""
    finite_gen = (FakeModule(f"gen_mod_{idx}") for idx in range(4))
    core_a = compose_gateway_core(
        config=CoreConfig(instance_name="gen_a"),
        modules=finite_gen,
    )
    assert core_a.module_ids() == ("gen_mod_0", "gen_mod_1", "gen_mod_2", "gen_mod_3")

    consumed_count = 0

    def _controlled_unbounded_generator() -> Iterator[GatewayModule]:
        nonlocal consumed_count
        for idx in range(1_000):
            if idx >= MAX_COMPOSED_MODULES + 1:
                raise AssertionError(
                    f"Generator consumed past MAX_COMPOSED_MODULES + 1 (idx={idx})"
                )
            consumed_count += 1
            yield FakeModule(f"stream_mod_{idx:03d}")

    with pytest.raises(CoreConfigurationError) as exc_info:
        compose_gateway_core(modules=_controlled_unbounded_generator())

    assert exc_info.value.field_name == "modules"
    assert consumed_count == MAX_COMPOSED_MODULES + 1 == 65

    # Item 10: Existing core_a and a newly composed core_b remain completely isolated
    core_b = compose_gateway_core(
        config=CoreConfig(instance_name="gen_b"),
        modules=(FakeModule(f"other_{idx}") for idx in range(2)),
    )
    assert core_a.module_ids() == ("gen_mod_0", "gen_mod_1", "gen_mod_2", "gen_mod_3")
    assert core_b.module_ids() == ("other_0", "other_1")
    assert core_a.instance_id != core_b.instance_id


def test_bounded_composition_wraps_generator_exception_without_catching_base_exception() -> None:
    """Item 7 & 9: Standard Exception during iteration wraps in CoreConfigurationError; BaseException propagates."""
    yielded_modules: list[FakeModule] = []

    def _failing_generator() -> Iterator[GatewayModule]:
        first = FakeModule("mod_a")
        second = FakeModule("mod_b")
        yielded_modules.extend([first, second])
        yield first
        yield second
        raise RuntimeError("synthetic iterator failure")

    with pytest.raises(CoreConfigurationError) as exc_info:
        compose_gateway_core(modules=_failing_generator())

    assert exc_info.value.field_name == "modules"
    assert isinstance(exc_info.value.__cause__, RuntimeError)
    assert "synthetic iterator failure" in str(exc_info.value)
    assert len(yielded_modules) == 2
    assert all(mod.events == [] for mod in yielded_modules)

    def _interrupting_generator() -> Iterator[GatewayModule]:
        yield FakeModule("mod_a")
        raise KeyboardInterrupt("simulated interrupt")

    with pytest.raises(KeyboardInterrupt, match="simulated interrupt"):
        compose_gateway_core(modules=_interrupting_generator())
