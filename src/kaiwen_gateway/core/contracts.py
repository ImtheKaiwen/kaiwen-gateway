"""Provider-neutral module metadata, version policy, and lifecycle contracts for Gateway Core."""

from __future__ import annotations

import inspect
import re
from dataclasses import dataclass
from typing import Final, Protocol, cast, runtime_checkable

from kaiwen_gateway.core.exceptions import (
    InvalidModuleDescriptorError,
    InvalidModuleIdError,
    UnsupportedContractVersionError,
)

CORE_CONTRACT_VERSION: Final[str] = "1.0"
SUPPORTED_CONTRACT_VERSIONS: Final[frozenset[str]] = frozenset({CORE_CONTRACT_VERSION})
MODULE_ID_PATTERN: Final[re.Pattern[str]] = re.compile(r"^[a-z][a-z0-9_]{0,63}$")
MAX_DESCRIPTION_LENGTH: Final[int] = 256
_DESCRIPTOR_ATTR: Final[str] = "descriptor"


def validate_module_id(module_id: object) -> str:
    """Validate that ``module_id`` is a canonical lowercase Core module identifier."""
    if not isinstance(module_id, str):
        raise InvalidModuleIdError(module_id, "module_id must be a str")
    if not module_id:
        raise InvalidModuleIdError(module_id, "module_id must not be empty")
    if not MODULE_ID_PATTERN.fullmatch(module_id):
        raise InvalidModuleIdError(
            module_id,
            "module_id must match ^[a-z][a-z0-9_]{0,63}$ (lowercase ASCII letters, digits, "
            "or underscores, starting with a letter, up to 64 characters)",
        )
    return module_id


def validate_contract_version(
    contract_version: object,
    *,
    module_id: str | None = None,
) -> str:
    """Validate that ``contract_version`` is a non-empty string in ``SUPPORTED_CONTRACT_VERSIONS``."""
    if not isinstance(contract_version, str) or not contract_version.strip():
        raise InvalidModuleDescriptorError(
            f"contract_version must be a non-empty str, got {contract_version!r}"
        )
    if contract_version != contract_version.strip():
        raise InvalidModuleDescriptorError(
            f"contract_version must not have leading or trailing whitespace: {contract_version!r}"
        )
    if contract_version not in SUPPORTED_CONTRACT_VERSIONS:
        raise UnsupportedContractVersionError(
            contract_version,
            SUPPORTED_CONTRACT_VERSIONS,
            module_id=module_id,
        )
    return contract_version


@dataclass(frozen=True, slots=True)
class ModuleDescriptor:
    """Immutable metadata describing a Gateway module and its Core contract version.

    ``ModuleDescriptor`` instances are frozen value objects. Structural and
    version-compatibility validation can be invoked explicitly via :meth:`validate`
    or :func:`validate_module_descriptor`, and is always enforced by
    :class:`~kaiwen_gateway.core.registry.ModuleRegistry` before a module is registered.
    """

    module_id: str
    contract_version: str = CORE_CONTRACT_VERSION
    description: str = ""

    def validate(self) -> ModuleDescriptor:
        """Validate this descriptor's module ID, contract version, and description."""
        return validate_module_descriptor(self)


def validate_module_descriptor(descriptor: object) -> ModuleDescriptor:
    """Validate that ``descriptor`` is a well-formed, compatible :class:`ModuleDescriptor`."""
    if not isinstance(descriptor, ModuleDescriptor):
        raise InvalidModuleDescriptorError(
            f"Expected ModuleDescriptor instance, got {type(descriptor).__name__}"
        )
    module_id = validate_module_id(descriptor.module_id)
    validate_contract_version(descriptor.contract_version, module_id=module_id)

    description = descriptor.description
    if not isinstance(description, str):
        raise InvalidModuleDescriptorError(
            f"ModuleDescriptor.description for {module_id!r} must be a str, "
            f"got {type(description).__name__}"
        )
    if len(description) > MAX_DESCRIPTION_LENGTH:
        raise InvalidModuleDescriptorError(
            f"ModuleDescriptor.description for {module_id!r} exceeds {MAX_DESCRIPTION_LENGTH} "
            f"characters ({len(description)})"
        )
    if any(ord(ch) < 32 or ord(ch) == 127 for ch in description):
        raise InvalidModuleDescriptorError(
            f"ModuleDescriptor.description for {module_id!r} must not contain control characters"
        )
    return descriptor


@runtime_checkable
class ModuleLifecycle(Protocol):
    """Typed startup and shutdown lifecycle contract for Gateway modules."""

    async def startup(self) -> None:
        """Initialize module resources before accepting connections."""

    async def shutdown(self) -> None:
        """Release module resources deterministically during shutdown."""


@runtime_checkable
class GatewayModule(ModuleLifecycle, Protocol):
    """Framework-neutral contract implemented by opt-in Gateway communication modules."""

    @property
    def descriptor(self) -> ModuleDescriptor:
        """Return the immutable descriptor identifying this module."""


def validate_gateway_module(module: object) -> tuple[GatewayModule, ModuleDescriptor]:
    """Validate that ``module`` satisfies :class:`GatewayModule` and has a valid descriptor."""
    if module is None or isinstance(module, type):
        raise InvalidModuleDescriptorError(
            "GatewayModule must be an instantiated module object, not None or a class"
        )
    try:
        raw_descriptor = getattr(module, _DESCRIPTOR_ATTR)
    except Exception as exc:
        raise InvalidModuleDescriptorError(
            f"Module {type(module).__name__} failed to provide a valid 'descriptor' attribute"
        ) from exc

    descriptor = validate_module_descriptor(raw_descriptor)

    for hook_name in ("startup", "shutdown"):
        hook = getattr(module, hook_name, None)
        if not inspect.iscoroutinefunction(hook):
            raise InvalidModuleDescriptorError(
                f"Module {descriptor.module_id!r} ({type(module).__name__}) must define "
                f"'async def {hook_name}(self) -> None'"
            )

    return cast(GatewayModule, module), descriptor


__all__ = [
    "CORE_CONTRACT_VERSION",
    "MAX_DESCRIPTION_LENGTH",
    "MODULE_ID_PATTERN",
    "SUPPORTED_CONTRACT_VERSIONS",
    "GatewayModule",
    "ModuleDescriptor",
    "ModuleLifecycle",
    "validate_contract_version",
    "validate_gateway_module",
    "validate_module_descriptor",
    "validate_module_id",
]
