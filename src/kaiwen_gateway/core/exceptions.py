"""Typed exceptions for Kaiwen Gateway Core configuration, contracts, identity, and registration."""

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


class IdentityError(CoreError):
    """Base exception for Core connection/call identity, scope, and authorization errors."""


class InvalidIdentityError(IdentityError, ValueError):
    """Raised when an identity or external reference field is malformed or out of bounds.

    Raw input values are intentionally omitted from the formatted error message so
    accidentally supplied secrets, tokens, or personal identifiers are never echoed
    into logs or tracebacks.
    """

    def __init__(self, field_name: str, reason: str) -> None:
        self.field_name: str = field_name
        self.reason: str = reason
        super().__init__(f"Invalid identity field {field_name!r}: {reason}")


class InvalidScopeError(IdentityError, ValueError):
    """Raised when a permission scope name or scope collection is malformed or out of bounds.

    Raw scope inputs are intentionally omitted from the formatted error message so
    accidentally passed secrets or tokens are never echoed into logs or tracebacks.
    """

    def __init__(self, reason: str, *, field_name: str = "allowed_scopes") -> None:
        self.field_name: str = field_name
        self.reason: str = reason
        super().__init__(f"Invalid scope in {field_name!r}: {reason}")


class IdentityAuthorizationError(IdentityError, PermissionError):
    """Raised when provider/tenant/call binding or scope verification fails."""

    def __init__(self, reason: str) -> None:
        self.reason: str = reason
        super().__init__(f"Identity authorization failed: {reason}")


class AdmissionError(CoreError):
    """Base exception for all admission control, lease, and capacity errors."""


class AdmissionCapacityExceededError(AdmissionError):
    """Raised when an inbound connection cannot be admitted because capacity is exhausted."""

    def __init__(self, message: str, *, limit: int, current: int) -> None:
        self.limit: int = limit
        self.current: int = current
        super().__init__(message)


class GlobalCapacityExceededError(AdmissionCapacityExceededError):
    """Raised when the process-local global concurrent connection limit is reached."""

    def __init__(self, *, limit: int, current: int) -> None:
        super().__init__(
            f"Global connection capacity exhausted: {current}/{limit} active",
            limit=limit,
            current=current,
        )


class TenantCapacityExceededError(AdmissionCapacityExceededError):
    """Raised when a specific tenant's concurrent connection limit is reached.

    Note: The error message does not include the raw tenant_id to avoid leaking
    identifiers into unredacted application logs. The identifier remains accessible
    programmatically via the ``tenant_id`` attribute.
    """

    def __init__(self, *, tenant_id: str, limit: int, current: int) -> None:
        self.tenant_id: str = tenant_id
        super().__init__(
            f"Tenant connection capacity exhausted: {current}/{limit} active",
            limit=limit,
            current=current,
        )


class DuplicateConnectionAdmissionError(AdmissionError):
    """Raised when an admission attempt is made for a connection_id that already holds an active lease.

    Note: The error message does not include the raw connection_id to avoid leaking
    identifiers into unredacted application logs. The identifier remains accessible
    programmatically via the ``connection_id`` attribute.
    """

    def __init__(self, connection_id: str) -> None:
        self.connection_id: str = connection_id
        super().__init__(
            "Inbound connection already holds an active capacity lease in this AdmissionController"
        )


class InvalidLeaseError(AdmissionError, ValueError):
    """Raised when a lease operation is invalid, malformed, or targets a foreign controller."""

    def __init__(self, message: str) -> None:
        super().__init__(message)


class ConnectionLifecycleError(CoreError):
    """Base exception for all connection lifecycle and supervisor errors."""


class InvalidConnectionTransitionError(ConnectionLifecycleError, ValueError):
    """Raised when an invalid connection state machine transition is attempted."""

    def __init__(
        self,
        current_state: str,
        target_state: str,
        message: str | None = None,
    ) -> None:
        self.current_state: str = current_state
        self.target_state: str = target_state
        msg = message or f"Cannot transition from {current_state} to {target_state}"
        super().__init__(msg)


class LifecycleTimeoutError(ConnectionLifecycleError, TimeoutError):
    """Raised when a lifecycle operation or cleanup deadline expires."""


class DrainError(CoreError):
    """Base exception for Gateway instance draining and shutdown coordination errors."""


class InstanceDrainingError(DrainError):
    """Raised when managed admission or operations are attempted on a draining or stopped instance."""

    def __init__(
        self, message: str = "Instance is draining or stopped; new connections rejected"
    ) -> None:
        super().__init__(message)


class InvalidDrainTransitionError(DrainError):
    """Raised when an invalid instance drain state machine transition is attempted."""

    def __init__(
        self,
        current_state: str,
        target_state: str,
        message: str | None = None,
    ) -> None:
        self.current_state: str = current_state
        self.target_state: str = target_state
        msg = (
            message
            or f"Cannot transition instance drain state from {current_state} to {target_state}"
        )
        super().__init__(msg)


__all__ = [
    "AdmissionCapacityExceededError",
    "AdmissionError",
    "ConnectionLifecycleError",
    "CoreConfigurationError",
    "CoreError",
    "DrainError",
    "DuplicateConnectionAdmissionError",
    "DuplicateModuleError",
    "GlobalCapacityExceededError",
    "IdentityAuthorizationError",
    "IdentityError",
    "InstanceDrainingError",
    "InvalidConnectionTransitionError",
    "InvalidDrainTransitionError",
    "InvalidIdentityError",
    "InvalidLeaseError",
    "InvalidModuleDescriptorError",
    "InvalidModuleIdError",
    "InvalidScopeError",
    "LifecycleTimeoutError",
    "ModuleNotRegisteredError",
    "ModuleRegistrationError",
    "TenantCapacityExceededError",
    "UnsupportedContractVersionError",
]
