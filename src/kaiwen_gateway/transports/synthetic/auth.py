"""Synthetic transport authentication and authorization adapters for Gateway Core."""

from __future__ import annotations

import asyncio

from kaiwen_gateway.core.exceptions import IdentityAuthorizationError
from kaiwen_gateway.core.identity import (
    CallAuthorizationPort,
    ConnectionId,
    ProviderAuthenticatorPort,
    ProviderPrincipal,
    TenantCallBinding,
    UntrustedConnectionClaims,
    VerifiedConnectionContext,
    resolve_verified_connection_context,
)
from kaiwen_gateway.transports.synthetic.tokens import (
    SyntheticTokenClaims,
    SyntheticTokenVerifier,
)


class SyntheticProviderAuthenticator(ProviderAuthenticatorPort):
    """Authenticator implementing :class:`ProviderAuthenticatorPort` for synthetic transports.

    Authenticates the transport connection using verified token claims registered
    by :class:`SyntheticAuthBridge`.
    """

    def __init__(self, bridge: SyntheticAuthBridge) -> None:
        self._bridge = bridge

    async def authenticate_provider(
        self,
        *,
        connection_id: str,
        claims: UntrustedConnectionClaims,
    ) -> ProviderPrincipal:
        token_claims = self._bridge.get_verified_claims(connection_id)
        if token_claims is None:
            raise IdentityAuthorizationError(
                f"No verified transport credentials registered for connection {connection_id}"
            )

        # Untrusted claims cannot override the verified token's provider identity
        if (
            claims.claimed_provider_id is not None
            and claims.claimed_provider_id != token_claims.provider_id
        ):
            raise IdentityAuthorizationError(
                "Untrusted claimed_provider_id conflicts with verified synthetic token provider"
            )

        return ProviderPrincipal(token_claims.provider_id)


class SyntheticCallAuthorizer(CallAuthorizationPort):
    """Authorizer implementing :class:`CallAuthorizationPort` for synthetic transports.

    Authorizes tenant, call, and allowed scopes strictly from verified token claims.
    """

    def __init__(self, bridge: SyntheticAuthBridge) -> None:
        self._bridge = bridge

    async def authorize_call(
        self,
        *,
        principal: ProviderPrincipal,
        connection_id: str,
        claims: UntrustedConnectionClaims,
    ) -> TenantCallBinding:
        token_claims = self._bridge.get_verified_claims(connection_id)
        if token_claims is None:
            raise IdentityAuthorizationError(
                f"No verified transport credentials registered for connection {connection_id}"
            )

        if principal.provider_id != token_claims.provider_id:
            raise IdentityAuthorizationError(
                "Authenticated principal does not match verified token provider"
            )

        # Binding derived strictly from verified token authority
        return TenantCallBinding(
            authorized_provider_id=token_claims.provider_id,
            tenant_id=token_claims.tenant_id,
            call_id=token_claims.call_id,
            allowed_scopes=token_claims.allowed_scopes,
            provider_call_ref=token_claims.provider_call_ref,
        )


class SyntheticAuthBridge:
    """Bridges transport-level token verification with Core identity and authorization ports.

    Responsibilities:
    - Verifies raw bearer tokens cryptographically via :class:`SyntheticTokenVerifier`.
    - Enforces single-use token consumption and active call exclusivity.
    - Temporarily binds verified token claims to the host-assigned `connection_id`.
    - Executes Core :func:`resolve_verified_connection_context` through typed ports.
    - Prevents privilege escalation and untrusted claim override via Core invariants.
    - Automatically cleans up transient connection claim registrations.
    """

    def __init__(
        self,
        verifier: SyntheticTokenVerifier,
        *,
        enforce_call_exclusivity: bool = True,
    ) -> None:
        self._verifier = verifier
        self._enforce_call_exclusivity = enforce_call_exclusivity
        self._claims_by_conn: dict[str, SyntheticTokenClaims] = {}
        self._active_calls: dict[tuple[str, str], str] = {}  # (tenant_id, call_id) -> connection_id
        self._lock = asyncio.Lock()

        self._authenticator = SyntheticProviderAuthenticator(self)
        self._authorizer = SyntheticCallAuthorizer(self)

    @property
    def verifier(self) -> SyntheticTokenVerifier:
        return self._verifier

    @property
    def authenticator(self) -> ProviderAuthenticatorPort:
        return self._authenticator

    @property
    def authorizer(self) -> CallAuthorizationPort:
        return self._authorizer

    def get_verified_claims(self, connection_id: str) -> SyntheticTokenClaims | None:
        return self._claims_by_conn.get(connection_id)

    async def register_active_call(self, connection_id: str, tenant_id: str, call_id: str) -> None:
        """Register an active connection for the given (tenant_id, call_id) pair."""
        if not self._enforce_call_exclusivity:
            return
        async with self._lock:
            existing = self._active_calls.get((tenant_id, call_id))
            if existing is not None and existing != connection_id:
                raise IdentityAuthorizationError(
                    f"Call session is already active for call_id '{call_id}' in tenant '{tenant_id}'"
                )
            self._active_calls[(tenant_id, call_id)] = connection_id

    async def unregister_active_call(
        self, connection_id: str, tenant_id: str, call_id: str
    ) -> None:
        """Unregister an active connection for the given (tenant_id, call_id) pair."""
        if not self._enforce_call_exclusivity:
            return
        async with self._lock:
            if self._active_calls.get((tenant_id, call_id)) == connection_id:
                self._active_calls.pop((tenant_id, call_id), None)

    async def resolve_context(
        self,
        *,
        connection_id: str | ConnectionId,
        raw_token: str,
        untrusted_claims: UntrustedConnectionClaims | None = None,
        clock: float | None = None,
    ) -> VerifiedConnectionContext:
        """Verify token and resolve into immutable :class:`VerifiedConnectionContext`."""
        canonical_conn_id = (
            connection_id.value if isinstance(connection_id, ConnectionId) else connection_id
        )

        # 1. Cryptographic token verification (fails closed if forged, expired, malformed, or replayed)
        token_claims = self._verifier.verify_token(raw_token, clock=clock, consume=True)

        # 2. Enforce active call exclusivity
        if self._enforce_call_exclusivity:
            async with self._lock:
                existing_conn = self._active_calls.get(
                    (token_claims.tenant_id, token_claims.call_id)
                )
                if existing_conn is not None and existing_conn != canonical_conn_id:
                    raise IdentityAuthorizationError(
                        f"Call session is already active for call_id '{token_claims.call_id}' in tenant '{token_claims.tenant_id}'"
                    )

        # 3. Register verified claims for the duration of identity resolution
        async with self._lock:
            self._claims_by_conn[canonical_conn_id] = token_claims

        try:
            # 4. Resolve through Core ports and invariants
            return await resolve_verified_connection_context(
                authenticator=self._authenticator,
                authorizer=self._authorizer,
                connection_id=canonical_conn_id,
                untrusted_claims=untrusted_claims,
            )
        finally:
            async with self._lock:
                self._claims_by_conn.pop(canonical_conn_id, None)
