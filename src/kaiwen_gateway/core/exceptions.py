"""Typed exceptions for Kaiwen Gateway Core configuration, contracts, and module registration."""

from __future__ import annotations


class CoreError(Exception):
    """Base exception for all framework-independent Gateway Core errors."""


class CoreConfigurationError(CoreError, ValueError):
    """Raised when Core configuration is malformed, out of range, or violates fail-closed policy."""

    def __init__(self, message: str, *, field_name: str | None = None) -> None:
        self.field_name: str | None = field_name
        prefix = f"Invalid CoreConfig.{field_name}: " if field_name else "Invalid CoreConfig: "
        super().__init__(f"{prefix}{message}")


class ModuleRegistrationError(CoreError):
    """Base exception for module contract validation and registration errors."""


class InvalidModuleDescriptorError(ModuleRegistrationError, ValueError):
    """Raised when a module or its descriptor is missing, malformed, or incomplete."""


class InvalidModuleIdError(ModuleRegistrationError, ValueError):
    """Raised when a module identifier does not satisfy canonical Core naming rules."""

    def __init__(self, module_id: object, reason: str | None = None) -> None:
        self.module_id: object = module_id
        detail = f": {reason}" if reason else ""
        super().__init__(f"Invalid module_id {module_id!r}{detail}")


class UnsupportedContractVersionError(ModuleRegistrationError, ValueError):
    """Raised when a module targets a Core contract version not supported by this runtime."""

    def __init__(
        self,
        contract_version: object,
        supported_versions: frozenset[str],
        *,
        module_id: str | None = None,
    ) -> None:
        self.contract_version: object = contract_version
        self.supported_versions: frozenset[str] = supported_versions
        self.module_id: str | None = module_id
        supported_display = ", ".join(sorted(supported_versions))
        target = f" for module {module_id!r}" if module_id is not None else ""
        super().__init__(
            f"Unsupported Core contract version {contract_version!r}{target}; "
            f"supported versions: [{supported_display}]"
        )


class DuplicateModuleError(ModuleRegistrationError, ValueError):
    """Raised when attempting to register a module ID that is already registered."""

    def __init__(self, module_id: str) -> None:
        self.module_id: str = module_id
        super().__init__(f"Module {module_id!r} is already registered in this ModuleRegistry")


class ModuleNotRegisteredError(CoreError, KeyError):
    """Raised when looking up a module ID that has not been registered."""

    def __init__(self, module_id: str) -> None:
        self.module_id: str = module_id
        super().__init__(f"Module {module_id!r} is not registered in this ModuleRegistry")


__all__ = [
    "CoreConfigurationError",
    "CoreError",
    "DuplicateModuleError",
    "InvalidModuleDescriptorError",
    "InvalidModuleIdError",
    "ModuleNotRegisteredError",
    "ModuleRegistrationError",
    "UnsupportedContractVersionError",
]
