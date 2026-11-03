"""Immutable connection/call identity, tenant scope contracts, and trust-boundary ports (M2.2B).

Security and architectural boundaries:
1. Three distinct trust levels:
   - :class:`UntrustedConnectionClaims`: unverified caller/provider payload metadata
     (such as presented tenant IDs, call IDs, provider call references, or requested
     scopes). Never treated as authority on its own.
   - :class:`ProviderPrincipal`: the authenticated telephony/transport provider identity
     produced by a server-side :class:`ProviderAuthenticatorPort`. Authenticating a
     provider does **not** grant access to arbitrary tenants or calls.
   - :class:`TenantCallBinding` & :class:`VerifiedConnectionContext`: the server-side
     application/policy authorization decision binding an authenticated provider and a
     specific transport ``connection_id`` to a trusted ``tenant_id``, ``call_id``, and
     immutable ``allowed_scopes`` set.
2. Three distinct identity layers (see ``docs/ARCHITECTURE.md §2.2``):
   - Transport connection: single connection attempt (:class:`ConnectionId` / ``connection_id``).
   - Call / session: tenant-scoped authorized call (:class:`CallId` / ``call_id``), kept
     strictly separate from non-authoritative external ``provider_call_ref`` metadata.
   - Conversation: application-owned ``conversation_id`` (intentionally outside Gateway Core).
3. Honest trust boundary caveat:
   - Frozen dataclasses and validation functions enforce in-process type and structural
     invariants; they are **not** a cryptographic authenticator or a capability sandbox
     against arbitrary in-process Python code. Real security enforcement depends on the
     concrete transport authenticator and host application authorization policy wired in
     later milestones.
"""

from __future__ import annotations

import itertools
import re
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from typing import Final, Protocol, runtime_checkable

from kaiwen_gateway.core.exceptions import (
    IdentityAuthorizationError,
    InvalidIdentityError,
    InvalidScopeError,
)

MAX_IDENTITY_LENGTH: Final[int] = 64
MAX_PROVIDER_CALL_REF_LENGTH: Final[int] = 128
MAX_SCOPE_LENGTH: Final[int] = 64
MAX_ALLOWED_SCOPES: Final[int] = 64

IDENTITY_ID_PATTERN: Final[re.Pattern[str]] = re.compile(r"^[a-zA-Z0-9][a-zA-Z0-9._:-]{0,63}$")
PROVIDER_CALL_REF_PATTERN: Final[re.Pattern[str]] = re.compile(
    r"^[a-zA-Z0-9][a-zA-Z0-9._:/-]{0,127}$"
)
SCOPE_NAME_PATTERN: Final[re.Pattern[str]] = re.compile(r"^[a-z][a-z0-9_.:-]{0,63}$")

_NON_COLLECTION_SCOPE_ITERABLES: Final[tuple[type[object], ...]] = (
    str,
    bytes,
    bytearray,
    Mapping,
)


def _has_control_or_surrogate_chars(value: str) -> bool:
    """Return True if ``value`` contains ASCII control chars, DEL, or non-ASCII characters."""
    return any(ord(ch) < 33 or ord(ch) >= 127 for ch in value)


def _validate_canonical_id(value: object, *, field_name: str) -> str:
    """Validate that ``value`` is a canonical, non-empty ASCII identifier without normalizing."""
    if not isinstance(value, str):
        raise InvalidIdentityError(
            field_name,
            f"must be a str, got {type(value).__name__}",
        )
    if not value:
        raise InvalidIdentityError(field_name, "must not be empty")
    if len(value) > MAX_IDENTITY_LENGTH:
        raise InvalidIdentityError(
            field_name,
            f"exceeds maximum length of {MAX_IDENTITY_LENGTH} characters ({len(value)})",
        )
    if _has_control_or_surrogate_chars(value):
        raise InvalidIdentityError(
            field_name,
            "must contain only printable ASCII characters without whitespace or control characters",
        )
    if not IDENTITY_ID_PATTERN.fullmatch(value):
        raise InvalidIdentityError(
            field_name,
            "must match ^[a-zA-Z0-9][a-zA-Z0-9._:-]{0,63}$",
        )
    return value


@dataclass(frozen=True, slots=True)
class ProviderId:
    """Immutable value object representing a canonical provider identifier."""

    value: str

    def __post_init__(self) -> None:
        _validate_canonical_id(self.value, field_name="provider_id")

    def __str__(self) -> str:
        return self.value


@dataclass(frozen=True, slots=True)
class TenantId:
    """Immutable value object representing a canonical tenant identifier."""

    value: str

    def __post_init__(self) -> None:
        _validate_canonical_id(self.value, field_name="tenant_id")

    def __str__(self) -> str:
        return self.value


@dataclass(frozen=True, slots=True)
class ConnectionId:
    """Immutable value object representing a single transport connection attempt identifier."""

    value: str

    def __post_init__(self) -> None:
        _validate_canonical_id(self.value, field_name="connection_id")

    def __str__(self) -> str:
        return self.value


@dataclass(frozen=True, slots=True)
class CallId:
    """Immutable value object representing a Gateway-trusted call/session identifier."""

    value: str

    def __post_init__(self) -> None:
        _validate_canonical_id(self.value, field_name="call_id")

    def __str__(self) -> str:
        return self.value


@dataclass(frozen=True, slots=True)
class PermissionScope:
    """Immutable value object representing a single canonical permission scope."""

    value: str

    def __post_init__(self) -> None:
        validate_scope(self.value)

    def __str__(self) -> str:
        return self.value


def validate_provider_id(provider_id: object) -> str:
    """Validate a provider identifier (``str`` or :class:`ProviderId`) and return canonical ``str``."""
    if isinstance(provider_id, ProviderId):
        return provider_id.value
    return _validate_canonical_id(provider_id, field_name="provider_id")


def validate_tenant_id(tenant_id: object) -> str:
    """Validate a tenant identifier (``str`` or :class:`TenantId`) and return canonical ``str``."""
    if isinstance(tenant_id, TenantId):
        return tenant_id.value
    return _validate_canonical_id(tenant_id, field_name="tenant_id")


def validate_connection_id(connection_id: object) -> str:
    """Validate a transport connection identifier (``str`` or :class:`ConnectionId`)."""
    if isinstance(connection_id, ConnectionId):
        return connection_id.value
    return _validate_canonical_id(connection_id, field_name="connection_id")


def validate_call_id(call_id: object) -> str:
    """Validate a Gateway call/session identifier (``str`` or :class:`CallId`)."""
    if isinstance(call_id, CallId):
        return call_id.value
    return _validate_canonical_id(call_id, field_name="call_id")


def validate_provider_call_ref(provider_call_ref: object) -> str | None:
    """Validate optional non-authoritative provider call reference metadata."""
    if provider_call_ref is None:
        return None
    if not isinstance(provider_call_ref, str):
        raise InvalidIdentityError(
            "provider_call_ref",
            f"must be a str or None, got {type(provider_call_ref).__name__}",
        )
    if not provider_call_ref:
        raise InvalidIdentityError("provider_call_ref", "must not be an empty string")
    if len(provider_call_ref) > MAX_PROVIDER_CALL_REF_LENGTH:
        raise InvalidIdentityError(
            "provider_call_ref",
            f"exceeds maximum length of {MAX_PROVIDER_CALL_REF_LENGTH} characters "
            f"({len(provider_call_ref)})",
        )
    if _has_control_or_surrogate_chars(provider_call_ref):
        raise InvalidIdentityError(
            "provider_call_ref",
            "must contain only printable ASCII characters without whitespace or control characters",
        )
    if not PROVIDER_CALL_REF_PATTERN.fullmatch(provider_call_ref):
        raise InvalidIdentityError(
            "provider_call_ref",
            "must match ^[a-zA-Z0-9][a-zA-Z0-9._:/-]{0,127}$",
        )
    return provider_call_ref


def validate_scope(scope: object, *, field_name: str = "allowed_scopes") -> str:
    """Validate a single canonical permission scope without normalizing untrusted input."""
    if isinstance(scope, PermissionScope):
        return scope.value
    if not isinstance(scope, str):
        raise InvalidScopeError(
            f"scope must be a str or PermissionScope, got {type(scope).__name__}",
            field_name=field_name,
        )
    if not scope:
        raise InvalidScopeError("scope must not be empty", field_name=field_name)
    if len(scope) > MAX_SCOPE_LENGTH:
        raise InvalidScopeError(
            f"scope exceeds maximum length of {MAX_SCOPE_LENGTH} characters ({len(scope)})",
            field_name=field_name,
        )
    if _has_control_or_surrogate_chars(scope):
        raise InvalidScopeError(
            "scope must contain only printable ASCII characters without whitespace or control characters",
            field_name=field_name,
        )
    if not SCOPE_NAME_PATTERN.fullmatch(scope):
        raise InvalidScopeError(
            "scope must match ^[a-z][a-z0-9_.:-]{0,63}$ (lowercase ASCII slug)",
            field_name=field_name,
        )
    return scope


def _validate_untrusted_claim_string(
    value: object,
    *,
    field_name: str,
    max_length: int = MAX_IDENTITY_LENGTH,
) -> str | None:
    """Validate an untrusted identity field without normalizing or copying raw input into errors."""
    if value is None:
        return None
    if not isinstance(value, str):
        raise InvalidIdentityError(
            field_name,
            f"must be a str or None, got {type(value).__name__}",
        )
    if not value:
        raise InvalidIdentityError(field_name, "must not be empty when supplied")
    if len(value) > max_length:
        raise InvalidIdentityError(
            field_name,
            f"exceeds maximum length of {max_length} characters ({len(value)})",
        )
    if _has_control_or_surrogate_chars(value):
        raise InvalidIdentityError(
            field_name,
            "must contain only printable ASCII characters without whitespace or control characters",
        )
    return value


def validate_scopes(
    scopes: object,
    *,
    field_name: str = "allowed_scopes",
) -> frozenset[str]:
    """Validate and materialize a bounded, immutable ``frozenset`` of canonical scopes.

    Inspects at most ``MAX_ALLOWED_SCOPES + 1`` items via :func:`itertools.islice`.
    If an external caller-supplied iterator raises an exception, the exception is caught
    and a sanitized :class:`InvalidScopeError` is raised outside the exception handler block.
    This guarantees that neither ``__cause__`` nor ``__context__`` retains the caller's
    exception instance, preventing arbitrary caller-provided exceptions from leaking secrets
    into tracebacks or exception inspection.
    """
    if isinstance(scopes, _NON_COLLECTION_SCOPE_ITERABLES) or not isinstance(scopes, Iterable):
        raise InvalidScopeError(
            f"must be an iterable collection of scope strings, got {type(scopes).__name__}",
            field_name=field_name,
        )
    iterator_exc_type: type[BaseException] | None = None
    try:
        bounded_items = tuple(itertools.islice(scopes, MAX_ALLOWED_SCOPES + 1))
    except Exception as exc:
        iterator_exc_type = type(exc)

    if iterator_exc_type is not None:
        raise InvalidScopeError(
            f"failed while iterating scope collection: {iterator_exc_type.__name__}",
            field_name=field_name,
        )

    if len(bounded_items) > MAX_ALLOWED_SCOPES:
        raise InvalidScopeError(
            f"scope count exceeds maximum supported limit of {MAX_ALLOWED_SCOPES}",
            field_name=field_name,
        )

    validated = [validate_scope(item, field_name=field_name) for item in bounded_items]
    return frozenset(validated)


@dataclass(frozen=True, slots=True, repr=False)
class UntrustedConnectionClaims:
    """Unverified external metadata presented by an inbound connection or payload.

    Invariants:
    - Carries zero authorization or identity authority on its own.
    - Never stores raw credentials or bearer tokens; ``__repr__`` redacts claim values so
      unverified external strings cannot leak into logs or exception tracebacks.
    - Fields are bounded: ``claimed_provider_id``, ``claimed_tenant_id``, and ``claimed_call_id``
      are at most ``MAX_IDENTITY_LENGTH`` (64) characters when supplied; ``provider_call_ref`` is
      at most ``MAX_PROVIDER_CALL_REF_LENGTH`` (128) characters; control and whitespace characters
      are rejected; raw input is never copied into error messages.
    """

    claimed_provider_id: str | None = None
    claimed_tenant_id: str | None = None
    claimed_call_id: str | None = None
    provider_call_ref: str | None = None
    claimed_scopes: frozenset[str] = frozenset()

    def __init__(
        self,
        *,
        claimed_provider_id: str | None = None,
        claimed_tenant_id: str | None = None,
        claimed_call_id: str | None = None,
        provider_call_ref: str | None = None,
        claimed_scopes: Iterable[str | PermissionScope] = (),
    ) -> None:
        valid_provider = _validate_untrusted_claim_string(
            claimed_provider_id,
            field_name="claimed_provider_id",
            max_length=MAX_IDENTITY_LENGTH,
        )
        valid_tenant = _validate_untrusted_claim_string(
            claimed_tenant_id,
            field_name="claimed_tenant_id",
            max_length=MAX_IDENTITY_LENGTH,
        )
        valid_call = _validate_untrusted_claim_string(
            claimed_call_id,
            field_name="claimed_call_id",
            max_length=MAX_IDENTITY_LENGTH,
        )
        valid_ref = _validate_untrusted_claim_string(
            provider_call_ref,
            field_name="provider_call_ref",
            max_length=MAX_PROVIDER_CALL_REF_LENGTH,
        )
        validated_claimed_scopes = validate_scopes(
            claimed_scopes,
            field_name="claimed_scopes",
        )
        object.__setattr__(self, "claimed_provider_id", valid_provider)
        object.__setattr__(self, "claimed_tenant_id", valid_tenant)
        object.__setattr__(self, "claimed_call_id", valid_call)
        object.__setattr__(self, "provider_call_ref", valid_ref)
        object.__setattr__(self, "claimed_scopes", validated_claimed_scopes)

    def __repr__(self) -> str:
        return "UntrustedConnectionClaims(<redacted-untrusted-claims>)"


@dataclass(frozen=True, slots=True)
class ProviderPrincipal:
    """Authenticated provider identity verified by a transport authenticator.

    Being an authenticated :class:`ProviderPrincipal` proves only which telephony/transport
    provider connected; it does **not** authorize access to any tenant or call until bound
    through :class:`TenantCallBinding`.
    """

    provider_id: str

    def __init__(self, provider_id: str | ProviderId) -> None:
        canonical = validate_provider_id(provider_id)
        object.__setattr__(self, "provider_id", canonical)

    @property
    def provider(self) -> ProviderId:
        """Return the typed :class:`ProviderId` value object."""
        return ProviderId(self.provider_id)


@dataclass(frozen=True, slots=True)
class TenantCallBinding:
    """Server-owned authorization decision binding a provider to a tenant, call, and scopes.

    Note: ``provider_call_ref`` is optional non-authoritative external correlation metadata;
    it never substitutes for Gateway's trusted ``call_id`` or ``tenant_id``.
    """

    authorized_provider_id: str
    tenant_id: str
    call_id: str
    allowed_scopes: frozenset[str] = frozenset()
    provider_call_ref: str | None = None

    def __init__(
        self,
        *,
        authorized_provider_id: str | ProviderId,
        tenant_id: str | TenantId,
        call_id: str | CallId,
        allowed_scopes: Iterable[str | PermissionScope] = (),
        provider_call_ref: str | None = None,
    ) -> None:
        canonical_provider = validate_provider_id(authorized_provider_id)
        canonical_tenant = validate_tenant_id(tenant_id)
        canonical_call = validate_call_id(call_id)
        canonical_scopes = validate_scopes(allowed_scopes, field_name="allowed_scopes")
        canonical_ref = validate_provider_call_ref(provider_call_ref)

        object.__setattr__(self, "authorized_provider_id", canonical_provider)
        object.__setattr__(self, "tenant_id", canonical_tenant)
        object.__setattr__(self, "call_id", canonical_call)
        object.__setattr__(self, "allowed_scopes", canonical_scopes)
        object.__setattr__(self, "provider_call_ref", canonical_ref)


@dataclass(frozen=True, slots=True)
class VerifiedConnectionContext:
    """Immutable, server-verified connection and call security context.

    Invariants (M2.2B):
    - Separates transport attempt identity (``connection_id``) from tenant-bound call
      identity (``call_id``) and optional non-authoritative ``provider_call_ref`` metadata.
    - ``allowed_scopes`` is always an immutable ``frozenset[str]`` of validated scope names.
    - Contains no raw credentials, API keys, bearer tokens, or caller phone numbers.
    - Public Python constructor: Note that while this class has a standard Python
      constructor, security guarantees (preventing tenant escalation, validating providers,
      and rejecting forged untrusted claims) depend entirely on host application and adapter
      integration using :func:`resolve_verified_connection_context` or
      :func:`bind_verified_connection_context`.
    """

    provider_id: str
    tenant_id: str
    connection_id: str
    call_id: str
    allowed_scopes: frozenset[str] = frozenset()
    provider_call_ref: str | None = None

    def __init__(
        self,
        *,
        provider_id: str | ProviderId,
        tenant_id: str | TenantId,
        connection_id: str | ConnectionId,
        call_id: str | CallId,
        allowed_scopes: Iterable[str | PermissionScope] = (),
        provider_call_ref: str | None = None,
    ) -> None:
        canonical_provider = validate_provider_id(provider_id)
        canonical_tenant = validate_tenant_id(tenant_id)
        canonical_conn = validate_connection_id(connection_id)
        canonical_call = validate_call_id(call_id)
        canonical_scopes = validate_scopes(allowed_scopes, field_name="allowed_scopes")
        canonical_ref = validate_provider_call_ref(provider_call_ref)

        object.__setattr__(self, "provider_id", canonical_provider)
        object.__setattr__(self, "tenant_id", canonical_tenant)
        object.__setattr__(self, "connection_id", canonical_conn)
        object.__setattr__(self, "call_id", canonical_call)
        object.__setattr__(self, "allowed_scopes", canonical_scopes)
        object.__setattr__(self, "provider_call_ref", canonical_ref)

    @property
    def provider(self) -> ProviderId:
        """Return the typed :class:`ProviderId` value object."""
        return ProviderId(self.provider_id)

    @property
    def tenant(self) -> TenantId:
        """Return the typed :class:`TenantId` value object."""
        return TenantId(self.tenant_id)

    @property
    def connection(self) -> ConnectionId:
        """Return the typed :class:`ConnectionId` value object."""
        return ConnectionId(self.connection_id)

    @property
    def call(self) -> CallId:
        """Return the typed :class:`CallId` value object."""
        return CallId(self.call_id)

    def has_scope(self, scope: str | PermissionScope) -> bool:
        """Return ``True`` if canonical ``scope`` is granted in ``allowed_scopes``."""
        canonical = validate_scope(scope, field_name="scope")
        return canonical in self.allowed_scopes

    def require_scope(self, scope: str | PermissionScope) -> None:
        """Verify that ``scope`` is granted in ``allowed_scopes`` or raise ``IdentityAuthorizationError``."""
        canonical = validate_scope(scope, field_name="scope")
        if canonical not in self.allowed_scopes:
            raise IdentityAuthorizationError(
                "Required permission scope is not granted in this VerifiedConnectionContext"
            )

    def require_tenant_and_call(
        self,
        *,
        tenant_id: str | TenantId,
        call_id: str | CallId | None = None,
    ) -> None:
        """Verify that a target operation matches this context's trusted tenant (and optional call)."""
        canonical_tenant = validate_tenant_id(tenant_id)
        if canonical_tenant != self.tenant_id:
            raise IdentityAuthorizationError(
                "Cross-tenant access denied: target tenant_id does not match verified context"
            )
        if call_id is not None:
            canonical_call = validate_call_id(call_id)
            if canonical_call != self.call_id:
                raise IdentityAuthorizationError(
                    "Cross-call access denied: target call_id does not match verified context"
                )


@runtime_checkable
class ProviderAuthenticatorPort(Protocol):
    """Inbound port that authenticates an external transport connection's provider principal.

    Note: This is a pure Core contract in M2.2B; concrete transport authenticators
    (e.g. mTLS or signed short-lived token verifiers) belong in transport/provider adapters.
    Note that Python's ``@runtime_checkable`` validates only the presence of the method name
    at runtime; it does not verify asynchronous signature compatibility or cryptographic
    credential validity.
    """

    async def authenticate_provider(
        self,
        *,
        connection_id: str,
        claims: UntrustedConnectionClaims,
    ) -> ProviderPrincipal:
        """Verify provider credentials for ``connection_id`` or raise :class:`IdentityAuthorizationError`."""


@runtime_checkable
class CallAuthorizationPort(Protocol):
    """Application/policy port that authorizes a tenant, call, and scopes for an authenticated provider.

    Note: Pure Core contract in M2.2B; concrete authorization policy engines belong in
    host application adapters. ``@runtime_checkable`` validates only method name presence.
    """

    async def authorize_call(
        self,
        *,
        principal: ProviderPrincipal,
        connection_id: str,
        claims: UntrustedConnectionClaims,
    ) -> TenantCallBinding:
        """Authorize a tenant/call binding for ``principal`` or raise :class:`IdentityAuthorizationError`."""


def bind_verified_connection_context(
    *,
    principal: ProviderPrincipal,
    binding: TenantCallBinding,
    connection_id: str | ConnectionId,
    untrusted_claims: UntrustedConnectionClaims | None = None,
) -> VerifiedConnectionContext:
    """Bind an authenticated :class:`ProviderPrincipal` and :class:`TenantCallBinding` into a :class:`VerifiedConnectionContext`.

    Enforces trust-boundary invariants:
    - ``principal`` must be a :class:`ProviderPrincipal` and ``binding`` must be a :class:`TenantCallBinding``.
    - ``principal.provider_id`` must match ``binding.authorized_provider_id``.
    - If ``untrusted_claims`` is supplied, any conflicting ``claimed_provider_id``,
      ``claimed_tenant_id``, ``claimed_call_id``, or ungranted ``claimed_scopes`` are
      rejected with :class:`IdentityAuthorizationError` rather than overriding or
      expanding the server-verified binding.
    """
    if not isinstance(principal, ProviderPrincipal):
        raise InvalidIdentityError(
            "principal",
            f"must be a ProviderPrincipal instance, got {type(principal).__name__}",
        )
    if not isinstance(binding, TenantCallBinding):
        raise InvalidIdentityError(
            "binding",
            f"must be a TenantCallBinding instance, got {type(binding).__name__}",
        )
    canonical_connection_id = validate_connection_id(connection_id)

    if untrusted_claims is not None and not isinstance(untrusted_claims, UntrustedConnectionClaims):
        raise InvalidIdentityError(
            "untrusted_claims",
            f"must be an UntrustedConnectionClaims instance or None, "
            f"got {type(untrusted_claims).__name__}",
        )

    if principal.provider_id != binding.authorized_provider_id:
        raise IdentityAuthorizationError(
            "Authenticated provider_id does not match binding.authorized_provider_id"
        )

    if untrusted_claims is not None:
        if (
            untrusted_claims.claimed_provider_id is not None
            and untrusted_claims.claimed_provider_id != principal.provider_id
        ):
            raise IdentityAuthorizationError(
                "Untrusted claimed_provider_id conflicts with authenticated provider principal"
            )
        if (
            untrusted_claims.claimed_tenant_id is not None
            and untrusted_claims.claimed_tenant_id != binding.tenant_id
        ):
            raise IdentityAuthorizationError(
                "Untrusted claimed_tenant_id conflicts with server-authorized tenant_id"
            )
        if (
            untrusted_claims.claimed_call_id is not None
            and untrusted_claims.claimed_call_id != binding.call_id
        ):
            raise IdentityAuthorizationError(
                "Untrusted claimed_call_id conflicts with server-authorized call_id"
            )
        if not untrusted_claims.claimed_scopes.issubset(binding.allowed_scopes):
            raise IdentityAuthorizationError(
                "Untrusted claimed_scopes attempt to escalate beyond server-authorized allowed_scopes"
            )
        if (
            untrusted_claims.provider_call_ref is not None
            and binding.provider_call_ref is not None
            and untrusted_claims.provider_call_ref != binding.provider_call_ref
        ):
            raise IdentityAuthorizationError(
                "Untrusted provider_call_ref conflicts with server-authorized provider_call_ref"
            )

    return VerifiedConnectionContext(
        provider_id=principal.provider_id,
        tenant_id=binding.tenant_id,
        connection_id=canonical_connection_id,
        call_id=binding.call_id,
        allowed_scopes=binding.allowed_scopes,
        provider_call_ref=binding.provider_call_ref,
    )


async def resolve_verified_connection_context(
    *,
    authenticator: ProviderAuthenticatorPort,
    authorizer: CallAuthorizationPort,
    connection_id: str | ConnectionId,
    untrusted_claims: UntrustedConnectionClaims | None = None,
) -> VerifiedConnectionContext:
    """Authenticate provider first, authorize tenant/call second, and bind immutable context.

    ``connection_id`` is a host-assigned per-connection transport identifier, never an
    authority token chosen by external payload fields. If ``authenticate_provider`` fails
    or returns a non-:class:`ProviderPrincipal` value, ``authorize_call`` is never invoked.
    """
    if not isinstance(authenticator, ProviderAuthenticatorPort):
        raise InvalidIdentityError(
            "authenticator",
            f"must satisfy ProviderAuthenticatorPort, got {type(authenticator).__name__}",
        )
    if not isinstance(authorizer, CallAuthorizationPort):
        raise InvalidIdentityError(
            "authorizer",
            f"must satisfy CallAuthorizationPort, got {type(authorizer).__name__}",
        )
    canonical_conn_id = validate_connection_id(connection_id)
    claims = UntrustedConnectionClaims() if untrusted_claims is None else untrusted_claims
    if not isinstance(claims, UntrustedConnectionClaims):
        raise InvalidIdentityError(
            "untrusted_claims",
            f"must be an UntrustedConnectionClaims instance or None, got {type(claims).__name__}",
        )

    principal = await authenticator.authenticate_provider(
        connection_id=canonical_conn_id,
        claims=claims,
    )
    if not isinstance(principal, ProviderPrincipal):
        raise InvalidIdentityError(
            "principal",
            f"authenticator must return a ProviderPrincipal instance, "
            f"got {type(principal).__name__}",
        )

    binding = await authorizer.authorize_call(
        principal=principal,
        connection_id=canonical_conn_id,
        claims=claims,
    )
    return bind_verified_connection_context(
        principal=principal,
        binding=binding,
        connection_id=canonical_conn_id,
        untrusted_claims=claims,
    )


__all__ = [
    "CallAuthorizationPort",
    "CallId",
    "ConnectionId",
    "IDENTITY_ID_PATTERN",
    "MAX_ALLOWED_SCOPES",
    "MAX_IDENTITY_LENGTH",
    "MAX_PROVIDER_CALL_REF_LENGTH",
    "MAX_SCOPE_LENGTH",
    "PROVIDER_CALL_REF_PATTERN",
    "PermissionScope",
    "ProviderAuthenticatorPort",
    "ProviderId",
    "ProviderPrincipal",
    "SCOPE_NAME_PATTERN",
    "TenantCallBinding",
    "TenantId",
    "UntrustedConnectionClaims",
    "VerifiedConnectionContext",
    "bind_verified_connection_context",
    "resolve_verified_connection_context",
    "validate_call_id",
    "validate_connection_id",
    "validate_provider_call_ref",
    "validate_provider_id",
    "validate_scope",
    "validate_scopes",
    "validate_tenant_id",
]
