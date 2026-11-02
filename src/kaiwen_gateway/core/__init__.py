"""Framework-independent Kaiwen Gateway Core contracts and explicit module registry.

Importing :mod:`kaiwen_gateway.core` has no runtime side effects and depends only
on the Python standard library.
"""

from __future__ import annotations

from kaiwen_gateway.core.contracts import (
    CORE_CONTRACT_VERSION,
    MAX_DESCRIPTION_LENGTH,
    MODULE_ID_PATTERN,
    SUPPORTED_CONTRACT_VERSIONS,
    GatewayModule,
    ModuleDescriptor,
    ModuleLifecycle,
    validate_contract_version,
    validate_gateway_module,
    validate_module_descriptor,
    validate_module_id,
)
from kaiwen_gateway.core.exceptions import (
    CoreError,
    DuplicateModuleError,
    InvalidModuleDescriptorError,
    InvalidModuleIdError,
    ModuleNotRegisteredError,
    ModuleRegistrationError,
    UnsupportedContractVersionError,
)
from kaiwen_gateway.core.registry import ModuleRegistry

__all__ = [
    "CORE_CONTRACT_VERSION",
    "MAX_DESCRIPTION_LENGTH",
    "MODULE_ID_PATTERN",
    "SUPPORTED_CONTRACT_VERSIONS",
    "CoreError",
    "DuplicateModuleError",
    "GatewayModule",
    "InvalidModuleDescriptorError",
    "InvalidModuleIdError",
    "ModuleDescriptor",
    "ModuleLifecycle",
    "ModuleNotRegisteredError",
    "ModuleRegistrationError",
    "ModuleRegistry",
    "UnsupportedContractVersionError",
    "validate_contract_version",
    "validate_gateway_module",
    "validate_module_descriptor",
    "validate_module_id",
]
