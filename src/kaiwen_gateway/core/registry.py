"""Explicit, instance-scoped module registry for Kaiwen Gateway Core."""

from __future__ import annotations

from collections.abc import Iterator, Mapping
from types import MappingProxyType

from kaiwen_gateway.core.contracts import (
    GatewayModule,
    ModuleDescriptor,
    validate_gateway_module,
    validate_module_id,
)
from kaiwen_gateway.core.exceptions import (
    DuplicateModuleError,
    ModuleNotRegisteredError,
)


class ModuleRegistry:
    """Explicit, instance-scoped registry of Gateway communication modules.

    Concurrency and ownership semantics:
    - Each :class:`ModuleRegistry` instance owns its own in-memory table; there is
      no process-wide singleton or cross-instance shared state.
    - Registration is intended to run explicitly during application composition
      before traffic admission begins, followed by deterministic read-only lookups.
    - Failed :meth:`register` calls leave existing registry entries untouched.
    - Inspection methods (:meth:`module_ids`, :meth:`descriptors`, :meth:`modules`,
      and :meth:`as_mapping`) return immutable snapshots so callers cannot mutate
      internal registry state.
    """

    __slots__ = ("_entries",)

    def __init__(self) -> None:
        self._entries: dict[str, tuple[ModuleDescriptor, GatewayModule]] = {}

    def register(self, module: object) -> GatewayModule:
        """Validate and register ``module`` under its ``descriptor.module_id``.

        Raises:
            InvalidModuleDescriptorError: If ``module`` or ``module.descriptor`` is missing,
                malformed, or lacks async ``startup``/``shutdown`` hooks.
            InvalidModuleIdError: If ``module.descriptor.module_id`` is invalid.
            UnsupportedContractVersionError: If ``module.descriptor.contract_version`` is not
                in ``SUPPORTED_CONTRACT_VERSIONS``.
            DuplicateModuleError: If ``module.descriptor.module_id`` is already registered
                in this registry instance.
        """
        validated_module, descriptor = validate_gateway_module(module)
        module_id = descriptor.module_id
        if module_id in self._entries:
            raise DuplicateModuleError(module_id)
        self._entries[module_id] = (descriptor, validated_module)
        return validated_module

    def get(self, module_id: str) -> GatewayModule:
        """Return the registered module for ``module_id`` or raise :class:`ModuleNotRegisteredError`."""
        canonical_id = validate_module_id(module_id)
        entry = self._entries.get(canonical_id)
        if entry is None:
            raise ModuleNotRegisteredError(canonical_id)
        return entry[1]

    def find(self, module_id: str) -> GatewayModule | None:
        """Return the registered module for ``module_id``, or ``None`` if not registered."""
        canonical_id = validate_module_id(module_id)
        entry = self._entries.get(canonical_id)
        return entry[1] if entry is not None else None

    def contains(self, module_id: str) -> bool:
        """Return ``True`` if a valid ``module_id`` is registered in this instance."""
        canonical_id = validate_module_id(module_id)
        return canonical_id in self._entries

    def module_ids(self) -> tuple[str, ...]:
        """Return registered module IDs in deterministic registration order."""
        return tuple(self._entries.keys())

    def descriptors(self) -> tuple[ModuleDescriptor, ...]:
        """Return validated module descriptors in deterministic registration order."""
        return tuple(descriptor for descriptor, _ in self._entries.values())

    def modules(self) -> tuple[GatewayModule, ...]:
        """Return registered modules in deterministic registration order."""
        return tuple(module for _, module in self._entries.values())

    def as_mapping(self) -> Mapping[str, GatewayModule]:
        """Return a read-only mapping snapshot of ``module_id -> GatewayModule``."""
        snapshot = {module_id: module for module_id, (_, module) in self._entries.items()}
        return MappingProxyType(snapshot)

    def __contains__(self, module_id: object) -> bool:
        return isinstance(module_id, str) and module_id in self._entries

    def __len__(self) -> int:
        return len(self._entries)

    def __iter__(self) -> Iterator[str]:
        return iter(tuple(self._entries.keys()))


__all__ = ["ModuleRegistry"]
