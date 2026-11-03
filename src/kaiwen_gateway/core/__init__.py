"""Framework-independent Kaiwen Gateway Core contracts, configuration, and composition.

Importing :mod:`kaiwen_gateway.core` has no runtime side effects and depends only
on the Python standard library.
"""

from __future__ import annotations

from kaiwen_gateway.core.composition import (
    GatewayCore,
    compose_gateway_core,
)
from kaiwen_gateway.core.config import (
    DEFAULT_INSTANCE_NAME,
    DEFAULT_MAX_CONCURRENT_CONNECTIONS,
    DEFAULT_MAX_CONNECTIONS_PER_TENANT,
    INGRESS_MODE_DISABLED,
    INSTANCE_NAME_PATTERN,
    MAX_CONFIGURED_CONNECTIONS,
    MAX_INSTANCE_NAME_LENGTH,
    SUPPORTED_INGRESS_MODES,
    CoreConfig,
    validate_core_config,
)
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
    CoreConfigurationError,
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
    "CoreConfig",
    "CoreConfigurationError",
    "CoreError",
    "DEFAULT_INSTANCE_NAME",
    "DEFAULT_MAX_CONCURRENT_CONNECTIONS",
    "DEFAULT_MAX_CONNECTIONS_PER_TENANT",
    "DuplicateModuleError",
    "GatewayCore",
    "GatewayModule",
    "INGRESS_MODE_DISABLED",
    "INSTANCE_NAME_PATTERN",
    "InvalidModuleDescriptorError",
    "InvalidModuleIdError",
    "MAX_CONFIGURED_CONNECTIONS",
    "MAX_DESCRIPTION_LENGTH",
    "MAX_INSTANCE_NAME_LENGTH",
    "MODULE_ID_PATTERN",
    "ModuleDescriptor",
    "ModuleLifecycle",
    "ModuleNotRegisteredError",
    "ModuleRegistrationError",
    "ModuleRegistry",
    "SUPPORTED_CONTRACT_VERSIONS",
    "SUPPORTED_INGRESS_MODES",
    "UnsupportedContractVersionError",
    "compose_gateway_core",
    "validate_contract_version",
    "validate_core_config",
    "validate_gateway_module",
    "validate_module_descriptor",
    "validate_module_id",
]
