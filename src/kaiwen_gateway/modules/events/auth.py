"""Framework-independent authentication ports and process-local ticket replay cache for Events."""

from __future__ import annotations

import asyncio
import math
import time
from collections import OrderedDict
from collections.abc import Callable, Collection
from dataclasses import dataclass
from typing import Final, Protocol, runtime_checkable

from kaiwen_gateway.core.identity import (
    ProviderPrincipal,
    TenantPrincipalBinding,
    VerifiedPrincipalContext,
    validate_connection_id,
    validate_principal_id,
    validate_provider_id,
    validate_scopes,
    validate_tenant_id,
)
from kaiwen_gateway.modules.events.domain import EventAuthorizationError, InvalidEventError

EVENTS_TICKET_AUDIENCE: Final[str] = "kaiwen-gateway:/ws/events/v1"
DEFAULT_MAX_TICKET_TTL_SECONDS: Final[float] = 30.0
DEFAULT_REPLAY_CACHE_MAX_ENTRIES: Final[int] = 100_000


@dataclass(frozen=True, slots=True)
class EventsTicketClaims:
    """Verified server-side claims extracted from an application-issued WebSocket ticket."""

    jti: str
    tenant_id: str
    principal_id: str
    allowed_scopes: frozenset[str]
    origin: str
    audience: str
    expires_at: float
    provider_id: str = "events-ticket"

    def __post_init__(self) -> None:
        if not isinstance(self.jti, str) or not self.jti or len(self.jti) > 256:
            raise InvalidEventError("ticket jti must be a non-empty str (<= 256 chars)")
        if not isinstance(self.origin, str) or not self.origin:
            raise InvalidEventError("ticket origin must be a non-empty str")
        if not isinstance(self.audience, str) or not self.audience:
            raise InvalidEventError("ticket audience must be a non-empty str")
        if not math.isfinite(self.expires_at) or self.expires_at <= 0.0:
            raise InvalidEventError("ticket expires_at must be a finite positive timestamp")

        object.__setattr__(self, "tenant_id", validate_tenant_id(self.tenant_id))
        object.__setattr__(self, "principal_id", validate_principal_id(self.principal_id))
        object.__setattr__(self, "provider_id", validate_provider_id(self.provider_id))
        object.__setattr__(
            self,
            "allowed_scopes",
            validate_scopes(self.allowed_scopes, field_name="allowed_scopes"),
        )


@runtime_checkable
class EventsAuthenticatorPort(Protocol):
    """Port for authenticating `Authorization: Bearer <token>` on `/ws/events/v1`."""

    async def authenticate_bearer(
        self,
        *,
        token: str,
        connection_id: str,
    ) -> ProviderPrincipal:
        """Verify bearer token and return an authenticated ProviderPrincipal."""
        ...


@runtime_checkable
class EventsAuthorizerPort(Protocol):
    """Port for resolving server-side tenant/principal/scope bindings."""

    async def authorize_principal(
        self,
        *,
        principal: ProviderPrincipal,
        connection_id: str,
    ) -> TenantPrincipalBinding:
        """Resolve immutable TenantPrincipalBinding for an authenticated principal."""
        ...


@runtime_checkable
class EventsTicketVerifierPort(Protocol):
    """Port for verifying short-lived single-use browser WebSocket tickets."""

    async def verify_ticket(
        self,
        *,
        ticket: str,
        origin: str,
        connection_id: str,
    ) -> EventsTicketClaims:
        """Verify ticket signature/integrity and return parsed EventsTicketClaims."""
        ...


class BoundedTicketReplayCache:
    """Bounded, process-local single-instance single-use ticket `jti` replay cache.

    Scope caveat (E2):
        This cache is process-local only and does not coordinate across multiple worker
        processes or Gateway instances. Cross-instance atomic replay protection belongs to E3.
    """

    __slots__ = ("_max_entries", "_clock", "_seen", "_lock")

    def __init__(
        self,
        *,
        max_entries: int = DEFAULT_REPLAY_CACHE_MAX_ENTRIES,
        clock: Callable[[], float] | None = None,
    ) -> None:
        if isinstance(max_entries, bool) or max_entries <= 0:
            raise InvalidEventError("max_entries must be a positive int")
        self._max_entries: int = max_entries
        self._clock: Callable[[], float] = clock if clock is not None else time.time
        self._seen: OrderedDict[str, float] = OrderedDict()
        self._lock: asyncio.Lock = asyncio.Lock()

    @property
    def active_entries(self) -> int:
        """Return current number of tracked non-expired ticket nonces."""
        return len(self._seen)

    def _evict_expired_unlocked(self, now: float) -> None:
        expired = [jti for jti, exp in self._seen.items() if exp <= now]
        for jti in expired:
            self._seen.pop(jti, None)

    async def consume(self, jti: str, *, expires_at: float) -> bool:
        """Atomically consume `jti` if unused and unexpired; return False on replay."""
        if not isinstance(jti, str) or not jti:
            return False
        now = float(self._clock())
        if not math.isfinite(expires_at) or expires_at <= now:
            return False

        async with self._lock:
            now = float(self._clock())
            if expires_at <= now:
                return False
            self._evict_expired_unlocked(now)
            if jti in self._seen:
                return False
            while len(self._seen) >= self._max_entries:
                self._seen.popitem(last=False)
            self._seen[jti] = expires_at
            return True


async def verify_and_consume_ticket(
    *,
    ticket: str,
    request_origin: str,
    allowed_origins: Collection[str],
    connection_id: str,
    verifier: EventsTicketVerifierPort,
    replay_cache: BoundedTicketReplayCache,
    max_ttl_seconds: float = DEFAULT_MAX_TICKET_TTL_SECONDS,
    clock: Callable[[], float] | None = None,
) -> VerifiedPrincipalContext:
    """Validate Origin, verify ticket claims, enforce TTL/audience, consume `jti`, and bind context."""
    canonical_conn_id = validate_connection_id(connection_id)
    if not request_origin or request_origin not in allowed_origins:
        raise EventAuthorizationError("Request Origin is missing or not in allowlist")

    claims = await verifier.verify_ticket(
        ticket=ticket,
        origin=request_origin,
        connection_id=canonical_conn_id,
    )
    if not isinstance(claims, EventsTicketClaims):
        raise EventAuthorizationError("Ticket verifier returned invalid claims type")
    if claims.audience != EVENTS_TICKET_AUDIENCE:
        raise EventAuthorizationError("Ticket audience mismatch")
    if claims.origin != request_origin:
        raise EventAuthorizationError("Ticket origin mismatch")

    now = float(clock()) if clock is not None else float(replay_cache._clock())
    if claims.expires_at <= now:
        raise EventAuthorizationError("Ticket expired")
    if (claims.expires_at - now) > max_ttl_seconds:
        raise EventAuthorizationError("Ticket TTL exceeds maximum allowed 30s")

    consumed = await replay_cache.consume(claims.jti, expires_at=claims.expires_at)
    if not consumed:
        raise EventAuthorizationError("Ticket jti already used or expired")

    return VerifiedPrincipalContext(
        provider_id=claims.provider_id,
        tenant_id=claims.tenant_id,
        connection_id=canonical_conn_id,
        principal_id=claims.principal_id,
        allowed_scopes=claims.allowed_scopes,
    )


__all__ = [
    "BoundedTicketReplayCache",
    "DEFAULT_MAX_TICKET_TTL_SECONDS",
    "DEFAULT_REPLAY_CACHE_MAX_ENTRIES",
    "EVENTS_TICKET_AUDIENCE",
    "EventsAuthenticatorPort",
    "EventsAuthorizerPort",
    "EventsTicketClaims",
    "EventsTicketVerifierPort",
    "verify_and_consume_ticket",
]
