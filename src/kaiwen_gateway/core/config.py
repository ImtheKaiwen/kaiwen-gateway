"""Framework-independent, immutable, and fail-closed Core configuration model."""

from __future__ import annotations

import re
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Final

from kaiwen_gateway.core.exceptions import CoreConfigurationError

DEFAULT_INSTANCE_NAME: Final[str] = "default"
DEFAULT_MAX_CONCURRENT_CONNECTIONS: Final[int] = 25
DEFAULT_MAX_CONNECTIONS_PER_TENANT: Final[int] = 10
MAX_CONFIGURED_CONNECTIONS: Final[int] = 10_000
MAX_INSTANCE_NAME_LENGTH: Final[int] = 64
INSTANCE_NAME_PATTERN: Final[re.Pattern[str]] = re.compile(r"^[a-zA-Z0-9][a-zA-Z0-9._-]{0,63}$")
INGRESS_MODE_DISABLED: Final[str] = "disabled"
SUPPORTED_INGRESS_MODES: Final[frozenset[str]] = frozenset({INGRESS_MODE_DISABLED})

_ALLOWED_CONFIG_KEYS: Final[frozenset[str]] = frozenset(
    {
        "instance_name",
        "max_concurrent_connections",
        "max_connections_per_tenant",
        "ingress_mode",
    }
)


def _validate_instance_name(instance_name: object) -> str:
    """Validate that ``instance_name`` is a canonical non-empty ASCII instance identifier."""
    if not isinstance(instance_name, str):
        raise CoreConfigurationError(
            f"instance_name must be a str, got {type(instance_name).__name__}",
            field_name="instance_name",
        )
    if not instance_name or instance_name != instance_name.strip():
        raise CoreConfigurationError(
            "instance_name must be a non-empty string without leading or trailing whitespace",
            field_name="instance_name",
        )
    if len(instance_name) > MAX_INSTANCE_NAME_LENGTH:
        raise CoreConfigurationError(
            f"instance_name exceeds {MAX_INSTANCE_NAME_LENGTH} characters ({len(instance_name)})",
            field_name="instance_name",
        )
    if not INSTANCE_NAME_PATTERN.fullmatch(instance_name):
        raise CoreConfigurationError(
            "instance_name must match ^[a-zA-Z0-9][a-zA-Z0-9._-]{0,63}$",
            field_name="instance_name",
        )
    return instance_name


def _validate_positive_connection_limit(value: object, *, field_name: str) -> int:
    """Validate that a connection limit is a strictly positive integer within bounds."""
    if isinstance(value, bool) or not isinstance(value, int):
        raise CoreConfigurationError(
            f"{field_name} must be an int (not bool), got {type(value).__name__}",
            field_name=field_name,
        )
    if value < 1 or value > MAX_CONFIGURED_CONNECTIONS:
        raise CoreConfigurationError(
            f"{field_name} must be between 1 and {MAX_CONFIGURED_CONNECTIONS}, got {value}",
            field_name=field_name,
        )
    return value


def _validate_ingress_mode(ingress_mode: object) -> str:
    """Enforce fail-closed ingress policy while no verified transport or authenticator exists."""
    if not isinstance(ingress_mode, str):
        raise CoreConfigurationError(
            f"ingress_mode must be a str, got {type(ingress_mode).__name__}",
            field_name="ingress_mode",
        )
    if ingress_mode not in SUPPORTED_INGRESS_MODES:
        raise CoreConfigurationError(
            f"Unsupported ingress_mode {ingress_mode!r}; external/public ingress fails closed in "
            f"Gateway Core until an authenticated transport and TLS boundary are configured "
            f"(allowed: {sorted(SUPPORTED_INGRESS_MODES)})",
            field_name="ingress_mode",
        )
    return ingress_mode


@dataclass(frozen=True, slots=True)
class CoreConfig:
    """Immutable, validated configuration for a single :class:`~kaiwen_gateway.core.GatewayCore` instance.

    Scope and operational caveats (M2.2A):
    - ``CoreConfig`` is pure, in-memory, and instance-scoped; importing or constructing it
      never reads environment variables, secret stores, or filesystem paths.
    - ``max_concurrent_connections`` and ``max_connections_per_tenant`` define validated
      process-local capacity ceilings for future admission enforcement; they are **not yet**
      enforced by an active connection admission controller or transport listener in M2.2A,
      and they never coordinate capacity across multiple OS processes or hosts.
    - ``ingress_mode`` defaults to ``"disabled"`` and fails closed on any other value because
      ASGI/WSS ingress, TLS termination, and provider authentication are not yet implemented.
      Possessing a valid ``CoreConfig`` does **not** mean a secure network server is running.
    """

    instance_name: str = DEFAULT_INSTANCE_NAME
    max_concurrent_connections: int = DEFAULT_MAX_CONCURRENT_CONNECTIONS
    max_connections_per_tenant: int = DEFAULT_MAX_CONNECTIONS_PER_TENANT
    ingress_mode: str = INGRESS_MODE_DISABLED

    def __post_init__(self) -> None:
        validate_core_config(self)

    def validate(self) -> CoreConfig:
        """Validate this configuration instance and return ``self``."""
        return validate_core_config(self)

    @classmethod
    def from_mapping(cls, data: Mapping[str, object]) -> CoreConfig:
        """Construct and validate a :class:`CoreConfig` from an explicit key-value mapping.

        Unknown keys or attempts to pass unverified ingress/authentication-bypass settings
        fail closed with :class:`~kaiwen_gateway.core.exceptions.CoreConfigurationError`.
        """
        if not isinstance(data, Mapping):
            raise CoreConfigurationError(
                f"Configuration mapping must be a Mapping, got {type(data).__name__}"
            )
        for raw_key in data:
            if not isinstance(raw_key, str):
                raise CoreConfigurationError(
                    f"Configuration keys must be str, got {type(raw_key).__name__}"
                )
        unknown_keys = sorted(set(data.keys()) - _ALLOWED_CONFIG_KEYS)
        if unknown_keys:
            raise CoreConfigurationError(
                f"Unknown or unsupported configuration keys rejected (fail-closed): {unknown_keys}"
            )

        instance_name = data.get("instance_name", DEFAULT_INSTANCE_NAME)
        max_concurrent = data.get(
            "max_concurrent_connections",
            DEFAULT_MAX_CONCURRENT_CONNECTIONS,
        )
        max_per_tenant = data.get(
            "max_connections_per_tenant",
            DEFAULT_MAX_CONNECTIONS_PER_TENANT,
        )
        ingress_mode = data.get("ingress_mode", INGRESS_MODE_DISABLED)

        return cls(
            instance_name=instance_name,  # type: ignore[arg-type]
            max_concurrent_connections=max_concurrent,  # type: ignore[arg-type]
            max_connections_per_tenant=max_per_tenant,  # type: ignore[arg-type]
            ingress_mode=ingress_mode,  # type: ignore[arg-type]
        )


def validate_core_config(config: object) -> CoreConfig:
    """Validate that ``config`` is a well-formed, fail-closed :class:`CoreConfig`."""
    if not isinstance(config, CoreConfig):
        raise CoreConfigurationError(f"Expected CoreConfig instance, got {type(config).__name__}")

    _validate_instance_name(config.instance_name)
    max_concurrent = _validate_positive_connection_limit(
        config.max_concurrent_connections,
        field_name="max_concurrent_connections",
    )
    max_per_tenant = _validate_positive_connection_limit(
        config.max_connections_per_tenant,
        field_name="max_connections_per_tenant",
    )
    if max_per_tenant > max_concurrent:
        raise CoreConfigurationError(
            f"max_connections_per_tenant ({max_per_tenant}) must not exceed "
            f"max_concurrent_connections ({max_concurrent})",
            field_name="max_connections_per_tenant",
        )
    _validate_ingress_mode(config.ingress_mode)
    return config


__all__ = [
    "CoreConfig",
    "DEFAULT_INSTANCE_NAME",
    "DEFAULT_MAX_CONCURRENT_CONNECTIONS",
    "DEFAULT_MAX_CONNECTIONS_PER_TENANT",
    "INGRESS_MODE_DISABLED",
    "INSTANCE_NAME_PATTERN",
    "MAX_CONFIGURED_CONNECTIONS",
    "MAX_INSTANCE_NAME_LENGTH",
    "SUPPORTED_INGRESS_MODES",
    "validate_core_config",
]
