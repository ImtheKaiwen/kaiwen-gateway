"""Unit, negative-auth, cross-tenant isolation, and redaction tests for M2.2B Core identity.

Note: Authenticator and authorizer fixtures in this module are deterministic synthetic
in-memory test doubles exercising Core identity and scope contracts; they do not test
live mTLS handshakes, cryptographic token verification, or Verimor wire protocols.
"""

from __future__ import annotations

import asyncio
import dataclasses
import traceback
from collections.abc import Iterator
from pathlib import Path

import pytest
from mypy import api as mypy_api

from kaiwen_gateway.core import (
    MAX_ALLOWED_SCOPES,
    MAX_IDENTITY_LENGTH,
    MAX_PROVIDER_CALL_REF_LENGTH,
    MAX_SCOPE_LENGTH,
    CallAuthorizationPort,
    CallId,
    ConnectionId,
    CoreError,
    IdentityAuthorizationError,
    IdentityError,
    InvalidIdentityError,
    InvalidScopeError,
    PermissionScope,
    ProviderAuthenticatorPort,
    ProviderId,
    ProviderPrincipal,
    TenantCallBinding,
    TenantId,
    UntrustedConnectionClaims,
    VerifiedConnectionContext,
    bind_verified_connection_context,
    resolve_verified_connection_context,
    validate_call_id,
    validate_connection_id,
    validate_provider_call_ref,
    validate_provider_id,
    validate_scope,
    validate_scopes,
    validate_tenant_id,
)


class SyntheticProviderAuthenticator:
    """Deterministic in-memory test double for ProviderAuthenticatorPort (not cryptographic auth)."""

    def __init__(self, verified_provider_id: str | None = "synthetic_telephony") -> None:
        self._verified_provider_id = verified_provider_id
        self.calls: list[str] = []

    async def authenticate_provider(
        self,
        *,
        connection_id: str,
        claims: UntrustedConnectionClaims,
    ) -> ProviderPrincipal:
        _ = claims
        self.calls.append(connection_id)
        if self._verified_provider_id is None:
            raise IdentityAuthorizationError("Synthetic provider authentication rejected")
        return ProviderPrincipal(self._verified_provider_id)


class SyntheticCallAuthorizer:
    """Deterministic in-memory test double for CallAuthorizationPort (server-owned policy)."""

    def __init__(self, binding: TenantCallBinding | None) -> None:
        self._binding = binding
        self.calls: list[tuple[str, str]] = []

    async def authorize_call(
        self,
        *,
        principal: ProviderPrincipal,
        connection_id: str,
        claims: UntrustedConnectionClaims,
    ) -> TenantCallBinding:
        _ = claims
        self.calls.append((principal.provider_id, connection_id))
        if self._binding is None:
            raise IdentityAuthorizationError("Synthetic call authorization rejected by policy")
        return self._binding


def test_valid_identity_value_objects_and_validators() -> None:
    """Item 1: Valid ProviderId, TenantId, ConnectionId, CallId, PermissionScope, and validators."""
    pid = ProviderId("synthetic_provider-01.tr:edge")
    tid = TenantId("tenant_alpha-01")
    cid = ConnectionId("conn_0001:attempt_1")
    call_id = CallId("call_session-2026_01")
    scope = PermissionScope("voice.media:bidirectional-stream")

    assert str(pid) == "synthetic_provider-01.tr:edge"
    assert str(tid) == "tenant_alpha-01"
    assert str(cid) == "conn_0001:attempt_1"
    assert str(call_id) == "call_session-2026_01"
    assert str(scope) == "voice.media:bidirectional-stream"

    # Validators accept both str and typed value objects
    assert validate_provider_id(pid) == pid.value
    assert validate_provider_id(pid.value) == pid.value
    assert validate_tenant_id(tid) == tid.value
    assert validate_tenant_id(tid.value) == tid.value
    assert validate_connection_id(cid) == cid.value
    assert validate_connection_id(cid.value) == cid.value
    assert validate_call_id(call_id) == call_id.value
    assert validate_call_id(call_id.value) == call_id.value
    assert validate_scope(scope) == scope.value
    assert validate_scope(scope.value) == scope.value
    assert validate_provider_call_ref(None) is None
    assert validate_provider_call_ref("ext_ref/2026:call-01") == "ext_ref/2026:call-01"

    principal = ProviderPrincipal(pid)
    assert principal.provider_id == pid.value
    assert principal.provider == pid


@pytest.mark.parametrize(
    "bad_id",
    [
        "",
        " ",
        " leading",
        "trailing ",
        "-starts-with-dash",
        "_starts-with-underscore",
        ".starts-with-dot",
        ":starts-with-colon",
        "has space",
        "has/slash",
        "bad\x00null",
        "bad\x1fctrl",
        "bad\x7fdel",
        "non_ascii_ğ",
        "a" * (MAX_IDENTITY_LENGTH + 1),
        None,
        True,
        False,
        123,
        ["id"],
        {"id": "x"},
    ],
)
def test_invalid_provider_tenant_connection_and_call_ids_rejected(bad_id: object) -> None:
    """Items 2, 3, 4, 5, 6, & 7: Invalid, empty, wrong-type, overlong, or control-char IDs fail."""
    with pytest.raises(InvalidIdentityError) as exc_provider:
        ProviderId(bad_id)  # type: ignore[arg-type]
    assert exc_provider.value.field_name == "provider_id"
    assert isinstance(exc_provider.value, IdentityError)
    assert isinstance(exc_provider.value, CoreError)
    assert isinstance(exc_provider.value, ValueError)

    with pytest.raises(InvalidIdentityError) as exc_tenant:
        TenantId(bad_id)  # type: ignore[arg-type]
    assert exc_tenant.value.field_name == "tenant_id"

    with pytest.raises(InvalidIdentityError) as exc_conn:
        ConnectionId(bad_id)  # type: ignore[arg-type]
    assert exc_conn.value.field_name == "connection_id"

    with pytest.raises(InvalidIdentityError) as exc_call:
        CallId(bad_id)  # type: ignore[arg-type]
    assert exc_call.value.field_name == "call_id"


@pytest.mark.parametrize(
    "bad_ref",
    [
        "",
        " ",
        " leading",
        "-leading-dash",
        "/leading-slash",
        "has space",
        "bad\x00char",
        "bad\x7fchar",
        "non_ascii_ş",
        "r" * (MAX_PROVIDER_CALL_REF_LENGTH + 1),
        True,
        False,
        42,
        ["ref"],
    ],
)
def test_invalid_provider_call_ref_rejected(bad_ref: object) -> None:
    """Item 5, 6, & 7: Invalid provider_call_ref values are rejected deterministically."""
    with pytest.raises(InvalidIdentityError) as exc_info:
        validate_provider_call_ref(bad_ref)
    assert exc_info.value.field_name == "provider_call_ref"


def test_immutability_of_identity_models_and_scopes() -> None:
    """Items 8 & 9: All identity models and scope sets are frozen, slotted, and immutable."""
    mutable_input_scopes: list[str | PermissionScope] = [
        "voice.stream",
        PermissionScope("voice.interrupt"),
    ]
    ctx = VerifiedConnectionContext(
        provider_id="synthetic_provider",
        tenant_id="tenant_a",
        connection_id="conn_1",
        call_id="call_1",
        allowed_scopes=mutable_input_scopes,
        provider_call_ref="ext_01",
    )

    # Mutating caller's input list after construction has no effect
    mutable_input_scopes.append("voice.admin")
    assert ctx.allowed_scopes == frozenset({"voice.stream", "voice.interrupt"})
    assert isinstance(ctx.allowed_scopes, frozenset)
    assert ctx.provider == ProviderId("synthetic_provider")
    assert ctx.tenant == TenantId("tenant_a")
    assert ctx.connection == ConnectionId("conn_1")
    assert ctx.call == CallId("call_1")

    for obj in (
        ProviderId("p1"),
        TenantId("t1"),
        ConnectionId("c1"),
        CallId("call1"),
        PermissionScope("voice.stream"),
        ProviderPrincipal("p1"),
        TenantCallBinding(
            authorized_provider_id="p1",
            tenant_id="t1",
            call_id="call1",
            allowed_scopes=["voice.stream"],
        ),
        UntrustedConnectionClaims(claimed_tenant_id="t1"),
        ctx,
    ):
        assert not hasattr(obj, "__dict__")

    with pytest.raises(dataclasses.FrozenInstanceError):
        ctx.tenant_id = "tenant_b"  # type: ignore[misc]
    with pytest.raises(dataclasses.FrozenInstanceError):
        ctx.allowed_scopes = frozenset({"escalated"})  # type: ignore[misc]


@pytest.mark.parametrize(
    "bad_scope",
    [
        "",
        " ",
        "Voice.stream",
        "VOICE",
        "1voice",
        "_voice",
        "-voice",
        "voice/stream",
        "voice stream",
        "bad\x00scope",
        "bad\x7fscope",
        "non_ascii_ü",
        "s" * (MAX_SCOPE_LENGTH + 1),
        None,
        True,
        False,
        123,
    ],
)
def test_invalid_scope_and_scope_collection_rejection(bad_scope: object) -> None:
    """Item 10: Malformed scopes, non-collection iterables, and oversized scope sets fail."""
    with pytest.raises(InvalidScopeError) as exc_info:
        PermissionScope(bad_scope)  # type: ignore[arg-type]
    assert isinstance(exc_info.value, IdentityError)
    assert isinstance(exc_info.value, ValueError)

    with pytest.raises(InvalidScopeError):
        validate_scopes([bad_scope])

    # Security Review B: single string "admin" MUST NOT be split into 5 single-char scopes
    with pytest.raises(InvalidScopeError):
        validate_scopes("admin")

    for bad_collection in ("voice.stream", b"voice.stream", bytearray(b"x"), {"a": 1}, 123):
        with pytest.raises(InvalidScopeError):
            validate_scopes(bad_collection)

    # Nested collections inside scope iterables are rejected
    for nested_bad in ([["voice.stream"]], [("voice.stream",)], [{"voice.stream"}]):
        with pytest.raises(InvalidScopeError):
            validate_scopes(nested_bad)

    # Exactly 64 unique scopes succeed
    scopes_64 = [f"voice.scope_{idx:02d}" for idx in range(MAX_ALLOWED_SCOPES)]
    assert len(validate_scopes(scopes_64)) == MAX_ALLOWED_SCOPES

    # 65 scopes fail with bounded consumption
    consumed = 0

    def _oversized_scope_gen() -> Iterator[str]:
        nonlocal consumed
        for idx in range(1_000):
            if idx >= MAX_ALLOWED_SCOPES + 1:
                raise AssertionError("Unbounded scope consumption")
            consumed += 1
            yield f"voice.scope_{idx:03d}"

    with pytest.raises(InvalidScopeError):
        validate_scopes(_oversized_scope_gen())
    assert consumed == MAX_ALLOWED_SCOPES + 1 == 65

    def _failing_scope_gen() -> Iterator[str]:
        yield "voice.stream"
        raise RuntimeError("iterator boom")

    with pytest.raises(InvalidScopeError) as gen_exc:
        validate_scopes(_failing_scope_gen())
    assert gen_exc.value.__cause__ is None
    assert gen_exc.value.__context__ is None
    assert "RuntimeError" in gen_exc.value.reason


def test_permission_scope_membership_and_enforcement() -> None:
    """Item 11: has_scope and require_scope enforce exact granted scope membership."""
    ctx = VerifiedConnectionContext(
        provider_id="synthetic_provider",
        tenant_id="tenant_alpha",
        connection_id="conn_01",
        call_id="call_01",
        allowed_scopes=["voice.stream.inbound", "voice.stream.outbound"],
    )

    assert ctx.has_scope("voice.stream.inbound") is True
    assert ctx.has_scope(PermissionScope("voice.stream.outbound")) is True
    assert ctx.has_scope("voice.transfer") is False

    ctx.require_scope("voice.stream.inbound")
    ctx.require_scope(PermissionScope("voice.stream.outbound"))

    with pytest.raises(IdentityAuthorizationError) as exc_info:
        ctx.require_scope("voice.transfer")
    assert isinstance(exc_info.value, PermissionError)
    assert "Required permission scope is not granted" in exc_info.value.reason
    assert "voice.transfer" not in exc_info.value.reason


def test_untrusted_payload_cannot_override_tenant_provider_call_or_escalate_scopes() -> None:
    """Items 12, 13, 14, & 18: Forged untrusted claims or mismatched provider bindings are rejected."""
    principal = ProviderPrincipal("synthetic_provider_a")
    binding = TenantCallBinding(
        authorized_provider_id="synthetic_provider_a",
        tenant_id="tenant_alpha",
        call_id="call_alpha_01",
        allowed_scopes=["voice.stream"],
        provider_call_ref="ext_call_100",
    )

    # Item 18: Principal provider_id mismatching binding.authorized_provider_id fails
    wrong_principal = ProviderPrincipal("synthetic_provider_b")
    with pytest.raises(IdentityAuthorizationError, match="authorized_provider_id"):
        bind_verified_connection_context(
            principal=wrong_principal,
            binding=binding,
            connection_id="conn_01",
        )

    # Item 12: Forged claimed_tenant_id rejected
    with pytest.raises(IdentityAuthorizationError, match="claimed_tenant_id"):
        bind_verified_connection_context(
            principal=principal,
            binding=binding,
            connection_id="conn_01",
            untrusted_claims=UntrustedConnectionClaims(claimed_tenant_id="tenant_victim"),
        )

    # Item 13: Forged claimed_provider_id rejected
    with pytest.raises(IdentityAuthorizationError, match="claimed_provider_id"):
        bind_verified_connection_context(
            principal=principal,
            binding=binding,
            connection_id="conn_01",
            untrusted_claims=UntrustedConnectionClaims(claimed_provider_id="spoofed_provider"),
        )

    # Forged claimed_call_id rejected
    with pytest.raises(IdentityAuthorizationError, match="claimed_call_id"):
        bind_verified_connection_context(
            principal=principal,
            binding=binding,
            connection_id="conn_01",
            untrusted_claims=UntrustedConnectionClaims(claimed_call_id="call_other"),
        )

    # Item 14: Scope escalation via claimed_scopes rejected
    with pytest.raises(IdentityAuthorizationError, match="claimed_scopes"):
        bind_verified_connection_context(
            principal=principal,
            binding=binding,
            connection_id="conn_01",
            untrusted_claims=UntrustedConnectionClaims(
                claimed_scopes=["voice.stream", "voice.admin_escalate"]
            ),
        )

    # Invalid argument types to bind_verified_connection_context rejected
    with pytest.raises(InvalidIdentityError) as bad_p:
        bind_verified_connection_context(
            principal="not_a_principal",  # type: ignore[arg-type]
            binding=binding,
            connection_id="conn_01",
        )
    assert bad_p.value.field_name == "principal"

    with pytest.raises(InvalidIdentityError) as bad_b:
        bind_verified_connection_context(
            principal=principal,
            binding="not_a_binding",  # type: ignore[arg-type]
            connection_id="conn_01",
        )
    assert bad_b.value.field_name == "binding"

    with pytest.raises(InvalidIdentityError) as bad_c:
        bind_verified_connection_context(
            principal=principal,
            binding=binding,
            connection_id="conn_01",
            untrusted_claims={"claimed_tenant_id": "tenant_alpha"},  # type: ignore[arg-type]
        )
    assert bad_c.value.field_name == "untrusted_claims"


def test_cross_tenant_and_same_external_call_ref_isolation() -> None:
    """Items 15 & 16: Two tenants sharing the same provider_call_ref remain strictly isolated."""
    principal = ProviderPrincipal("synthetic_provider")
    shared_external_ref = "provider_leg_ref_999"

    binding_tenant_a = TenantCallBinding(
        authorized_provider_id="synthetic_provider",
        tenant_id="tenant_alpha",
        call_id="gw_call_alpha_1",
        allowed_scopes=["voice.stream"],
        provider_call_ref=shared_external_ref,
    )
    binding_tenant_b = TenantCallBinding(
        authorized_provider_id="synthetic_provider",
        tenant_id="tenant_bravo",
        call_id="gw_call_bravo_1",
        allowed_scopes=["voice.stream", "voice.interrupt"],
        provider_call_ref=shared_external_ref,
    )

    ctx_a = bind_verified_connection_context(
        principal=principal,
        binding=binding_tenant_a,
        connection_id="conn_alpha_1",
    )
    ctx_b = bind_verified_connection_context(
        principal=principal,
        binding=binding_tenant_b,
        connection_id="conn_bravo_1",
    )

    assert ctx_a.provider_call_ref == ctx_b.provider_call_ref == shared_external_ref
    assert ctx_a.tenant_id == "tenant_alpha"
    assert ctx_b.tenant_id == "tenant_bravo"
    assert ctx_a.call_id != ctx_b.call_id

    # Matching tenant and call succeeds
    ctx_a.require_tenant_and_call(tenant_id="tenant_alpha", call_id="gw_call_alpha_1")
    ctx_a.require_tenant_and_call(tenant_id=TenantId("tenant_alpha"))

    # Cross-tenant access fails even when call_id is omitted or guessed
    with pytest.raises(IdentityAuthorizationError, match="Cross-tenant"):
        ctx_a.require_tenant_and_call(tenant_id="tenant_bravo", call_id="gw_call_bravo_1")

    # Cross-call access within the same tenant fails
    with pytest.raises(IdentityAuthorizationError, match="Cross-call"):
        ctx_a.require_tenant_and_call(tenant_id="tenant_alpha", call_id="gw_call_alpha_other")


def test_distinct_connection_ids_and_verified_resolution_pipeline() -> None:
    """Items 17 & 18: Each transport connection attempt has a distinct connection_id and requires auth before authz."""
    binding = TenantCallBinding(
        authorized_provider_id="synthetic_telephony",
        tenant_id="tenant_alpha",
        call_id="call_alpha_01",
        allowed_scopes=["voice.stream"],
        provider_call_ref="ext_01",
    )
    authenticator = SyntheticProviderAuthenticator("synthetic_telephony")
    authorizer = SyntheticCallAuthorizer(binding)

    assert isinstance(authenticator, ProviderAuthenticatorPort)
    assert isinstance(authorizer, CallAuthorizationPort)

    claims = UntrustedConnectionClaims(
        claimed_provider_id="synthetic_telephony",
        claimed_tenant_id="tenant_alpha",
        claimed_call_id="call_alpha_01",
        provider_call_ref="ext_01",
        claimed_scopes=["voice.stream"],
    )

    ctx_attempt_1 = asyncio.run(
        resolve_verified_connection_context(
            authenticator=authenticator,
            authorizer=authorizer,
            connection_id="conn_attempt_1",
            untrusted_claims=claims,
        )
    )
    ctx_attempt_2 = asyncio.run(
        resolve_verified_connection_context(
            authenticator=authenticator,
            authorizer=authorizer,
            connection_id=ConnectionId("conn_attempt_2"),
            untrusted_claims=None,
        )
    )

    assert ctx_attempt_1.connection_id == "conn_attempt_1"
    assert ctx_attempt_2.connection_id == "conn_attempt_2"
    assert ctx_attempt_1.connection_id != ctx_attempt_2.connection_id
    assert ctx_attempt_1.call_id == ctx_attempt_2.call_id == "call_alpha_01"
    assert ctx_attempt_1.provider_call_ref == "ext_01"
    assert authenticator.calls == ["conn_attempt_1", "conn_attempt_2"]
    assert authorizer.calls == [
        ("synthetic_telephony", "conn_attempt_1"),
        ("synthetic_telephony", "conn_attempt_2"),
    ]

    # Security Review D: Conflicting provider_call_ref between untrusted_claims and binding is rejected
    with pytest.raises(IdentityAuthorizationError, match="provider_call_ref"):
        bind_verified_connection_context(
            principal=ProviderPrincipal("synthetic_telephony"),
            binding=binding,
            connection_id="conn_conflict_ref",
            untrusted_claims=UntrustedConnectionClaims(provider_call_ref="conflicting_ext_99"),
        )

    # When binding.provider_call_ref is None, untrusted provider_call_ref is never copied into context
    binding_without_ref = TenantCallBinding(
        authorized_provider_id="synthetic_telephony",
        tenant_id="tenant_alpha",
        call_id="call_alpha_01",
        allowed_scopes=["voice.stream"],
        provider_call_ref=None,
    )
    ctx_no_ref = bind_verified_connection_context(
        principal=ProviderPrincipal("synthetic_telephony"),
        binding=binding_without_ref,
        connection_id="conn_no_ref",
        untrusted_claims=UntrustedConnectionClaims(provider_call_ref="untrusted_ext_ref"),
    )
    assert ctx_no_ref.provider_call_ref is None

    # When authentication fails, authorizer MUST NOT be called (FR-CONN-003), even if same connection_id was used earlier
    failing_auth = SyntheticProviderAuthenticator(None)
    spy_authorizer = SyntheticCallAuthorizer(binding)
    with pytest.raises(IdentityAuthorizationError, match="authentication rejected"):
        asyncio.run(
            resolve_verified_connection_context(
                authenticator=failing_auth,
                authorizer=spy_authorizer,
                connection_id="conn_attempt_1",
            )
        )
    assert spy_authorizer.calls == []

    # Security Review C: When authorizer rejects, no context is returned
    rejecting_authorizer = SyntheticCallAuthorizer(None)
    with pytest.raises(IdentityAuthorizationError, match="authorization rejected"):
        asyncio.run(
            resolve_verified_connection_context(
                authenticator=authenticator,
                authorizer=rejecting_authorizer,
                connection_id="conn_authz_reject",
            )
        )

    # Security Review C: Authenticator returning wrong type fails BEFORE authorizer is called
    class WrongTypeAuthenticator:
        async def authenticate_provider(
            self,
            *,
            connection_id: str,
            claims: UntrustedConnectionClaims,
        ) -> ProviderPrincipal:
            _ = (connection_id, claims)
            return "not_a_principal"  # type: ignore[return-value]

    spy_after_wrong_auth = SyntheticCallAuthorizer(binding)
    with pytest.raises(InvalidIdentityError) as bad_auth_ret:
        asyncio.run(
            resolve_verified_connection_context(
                authenticator=WrongTypeAuthenticator(),
                authorizer=spy_after_wrong_auth,
                connection_id="conn_wrong_auth_ret",
            )
        )
    assert bad_auth_ret.value.field_name == "principal"
    assert spy_after_wrong_auth.calls == []

    # Security Review C: Authorizer returning wrong type fails cleanly
    class WrongTypeAuthorizer:
        async def authorize_call(
            self,
            *,
            principal: ProviderPrincipal,
            connection_id: str,
            claims: UntrustedConnectionClaims,
        ) -> TenantCallBinding:
            _ = (principal, connection_id, claims)
            return "not_a_binding"  # type: ignore[return-value]

    with pytest.raises(InvalidIdentityError) as bad_authz_ret:
        asyncio.run(
            resolve_verified_connection_context(
                authenticator=authenticator,
                authorizer=WrongTypeAuthorizer(),
                connection_id="conn_wrong_authz_ret",
            )
        )
    assert bad_authz_ret.value.field_name == "binding"

    # Security Review C: Invalid authenticator / authorizer objects rejected upfront
    with pytest.raises(InvalidIdentityError) as bad_auth_port:
        asyncio.run(
            resolve_verified_connection_context(
                authenticator=object(),  # type: ignore[arg-type]
                authorizer=authorizer,
                connection_id="conn_bad_port",
            )
        )
    assert bad_auth_port.value.field_name == "authenticator"

    with pytest.raises(InvalidIdentityError) as bad_authz_port:
        asyncio.run(
            resolve_verified_connection_context(
                authenticator=authenticator,
                authorizer=object(),  # type: ignore[arg-type]
                connection_id="conn_bad_port",
            )
        )
    assert bad_authz_port.value.field_name == "authorizer"

    # Security Review C: CancelledError and BaseException propagate untouched
    class CancellingAuthenticator:
        async def authenticate_provider(
            self,
            *,
            connection_id: str,
            claims: UntrustedConnectionClaims,
        ) -> ProviderPrincipal:
            _ = (connection_id, claims)
            raise asyncio.CancelledError("cancelled during auth")

    with pytest.raises(asyncio.CancelledError):
        asyncio.run(
            resolve_verified_connection_context(
                authenticator=CancellingAuthenticator(),
                authorizer=spy_after_wrong_auth,
                connection_id="conn_cancel",
            )
        )
    assert spy_after_wrong_auth.calls == []

    # Invalid untrusted_claims type rejected before calling authenticator
    with pytest.raises(InvalidIdentityError) as bad_claims_exc:
        asyncio.run(
            resolve_verified_connection_context(
                authenticator=authenticator,
                authorizer=spy_authorizer,
                connection_id="conn_bad_claims",
                untrusted_claims="raw_header_string",  # type: ignore[arg-type]
            )
        )
    assert bad_claims_exc.value.field_name == "untrusted_claims"


def test_secrets_and_untrusted_values_never_leak_into_repr_or_exception_messages() -> None:
    """Item 19 & Review E: Sensitive tokens or malformed untrusted strings never appear in repr() or error messages."""
    synthetic_secret = "bearer_tok_SYNTHETIC_SECRET_DO_NOT_LEAK_987654"

    claims = UntrustedConnectionClaims(
        claimed_provider_id=synthetic_secret,
        claimed_tenant_id=synthetic_secret,
        claimed_call_id=synthetic_secret,
        provider_call_ref=synthetic_secret,
    )
    assert synthetic_secret not in repr(claims)
    assert synthetic_secret not in str(claims)
    assert repr(claims) == "UntrustedConnectionClaims(<redacted-untrusted-claims>)"

    principal = ProviderPrincipal("synthetic_provider")
    binding = TenantCallBinding(
        authorized_provider_id="synthetic_provider",
        tenant_id="tenant_alpha",
        call_id="call_alpha_01",
        allowed_scopes=["voice.stream"],
    )
    ctx = bind_verified_connection_context(
        principal=principal,
        binding=binding,
        connection_id="conn_01",
    )
    assert "synthetic_provider" in repr(principal)
    assert "tenant_alpha" in repr(binding)
    assert "tenant_alpha" in repr(ctx)
    assert synthetic_secret not in repr(principal)
    assert synthetic_secret not in repr(binding)
    assert synthetic_secret not in repr(ctx)

    # Authorization exception message does not echo untrusted claim values
    with pytest.raises(IdentityAuthorizationError) as authz_exc:
        bind_verified_connection_context(
            principal=principal,
            binding=binding,
            connection_id="conn_01",
            untrusted_claims=claims,
        )
    assert synthetic_secret not in str(authz_exc.value)
    assert synthetic_secret not in repr(authz_exc.value)

    with pytest.raises(InvalidIdentityError) as bad_claim_type:
        UntrustedConnectionClaims(claimed_tenant_id=12345)  # type: ignore[arg-type]
    assert bad_claim_type.value.field_name == "claimed_tenant_id"

    bad_secret_input = f"invalid space {synthetic_secret}"
    for validator in (
        validate_provider_id,
        validate_tenant_id,
        validate_connection_id,
        validate_call_id,
        validate_provider_call_ref,
        validate_scope,
    ):
        with pytest.raises(IdentityError) as exc_info:
            validator(bad_secret_input)
        assert synthetic_secret not in str(exc_info.value)
        assert synthetic_secret not in repr(exc_info.value)

    # Exception chaining in validate_scopes omits raw exception text and suppresses cause/context
    def _secret_raising_iter() -> Iterator[str]:
        yield "voice.stream"
        raise ValueError(f"iterator failure with {synthetic_secret}")

    with pytest.raises(InvalidScopeError) as chained_exc:
        validate_scopes(_secret_raising_iter())
    assert synthetic_secret not in str(chained_exc.value)
    assert synthetic_secret not in repr(chained_exc.value)
    assert chained_exc.value.__cause__ is None
    assert chained_exc.value.__context__ is None
    formatted_tb = "".join(traceback.format_exception(chained_exc.value))
    assert synthetic_secret not in formatted_tb


def test_untrusted_connection_claims_field_boundaries_and_redaction() -> None:
    """Review Finding B: UntrustedConnectionClaims bounds and validates identity fields safely."""
    # Valid claims with none and valid bounded strings
    valid_claims = UntrustedConnectionClaims(
        claimed_provider_id="provider_a",
        claimed_tenant_id="tenant_b",
        claimed_call_id="call_c",
        provider_call_ref="ref_d",
        claimed_scopes=["voice.stream"],
    )
    assert valid_claims.claimed_provider_id == "provider_a"
    assert valid_claims.claimed_tenant_id == "tenant_b"
    assert valid_claims.claimed_call_id == "call_c"
    assert valid_claims.provider_call_ref == "ref_d"

    # Empty string rejected for each field
    for field_name in (
        "claimed_provider_id",
        "claimed_tenant_id",
        "claimed_call_id",
        "provider_call_ref",
    ):
        with pytest.raises(InvalidIdentityError) as empty_exc:
            UntrustedConnectionClaims(**{field_name: ""})
        assert empty_exc.value.field_name == field_name
        assert "must not be empty when supplied" in str(empty_exc.value)

    # Oversized strings rejected (> 64 for identity fields, > 128 for provider_call_ref)
    secret_oversized = "SECRET_" + ("x" * 80)
    for field_name in ("claimed_provider_id", "claimed_tenant_id", "claimed_call_id"):
        with pytest.raises(InvalidIdentityError) as over_exc:
            UntrustedConnectionClaims(**{field_name: secret_oversized})
        assert over_exc.value.field_name == field_name
        assert f"exceeds maximum length of {MAX_IDENTITY_LENGTH} characters" in str(over_exc.value)
        # Raw input secret is not copied into error message
        assert secret_oversized not in str(over_exc.value)
        assert secret_oversized not in repr(over_exc.value)

    secret_oversized_ref = "SECRET_REF_" + ("y" * 130)
    with pytest.raises(InvalidIdentityError) as over_ref_exc:
        UntrustedConnectionClaims(provider_call_ref=secret_oversized_ref)
    assert over_ref_exc.value.field_name == "provider_call_ref"
    assert f"exceeds maximum length of {MAX_PROVIDER_CALL_REF_LENGTH} characters" in str(
        over_ref_exc.value
    )
    assert secret_oversized_ref not in str(over_ref_exc.value)
    assert secret_oversized_ref not in repr(over_ref_exc.value)

    # Control characters and whitespace rejected without leaking raw input
    for bad_char in ("\n", "\r", "\t", "\x00", " ", "\x7f"):
        bad_value = f"bad{bad_char}secret_token"
        for field_name in (
            "claimed_provider_id",
            "claimed_tenant_id",
            "claimed_call_id",
            "provider_call_ref",
        ):
            with pytest.raises(InvalidIdentityError) as ctrl_exc:
                UntrustedConnectionClaims(**{field_name: bad_value})
            assert ctrl_exc.value.field_name == field_name
            assert "must contain only printable ASCII" in str(ctrl_exc.value)
            assert bad_value not in str(ctrl_exc.value)
            assert bad_value not in repr(ctrl_exc.value)


def test_require_scope_denial_does_not_leak_requested_scope_name() -> None:
    """Review Finding C: require_scope denial message does not echo caller's requested scope."""
    ctx = VerifiedConnectionContext(
        provider_id="synthetic_provider",
        tenant_id="tenant_alpha",
        connection_id="conn_01",
        call_id="call_01",
        allowed_scopes=["voice.stream"],
    )
    unauthorized_scope = "voice.restricted.admin.secret"
    with pytest.raises(IdentityAuthorizationError) as exc_info:
        ctx.require_scope(unauthorized_scope)
    assert "Required permission scope is not granted" in exc_info.value.reason
    assert unauthorized_scope not in exc_info.value.reason
    assert unauthorized_scope not in str(exc_info.value)


def test_synthetic_test_doubles_explicit_boundary_guarantee() -> None:
    """Review Finding D: Synthetic authenticator and authorizer test doubles do not implement live crypto."""
    auth = SyntheticProviderAuthenticator(verified_provider_id="synthetic_provider")
    authorizer = SyntheticCallAuthorizer(
        binding=TenantCallBinding(
            authorized_provider_id="synthetic_provider",
            tenant_id="tenant_alpha",
            call_id="call_01",
        )
    )
    # They satisfy Core protocols at runtime
    assert isinstance(auth, ProviderAuthenticatorPort)
    assert isinstance(authorizer, CallAuthorizationPort)

    # Documented boundary: They are in-memory stubs, not real JWT/HMAC/mTLS verifiers
    assert hasattr(auth, "calls")
    assert hasattr(authorizer, "calls")


def test_static_type_checker_rejects_untrusted_claims_as_verified_context(
    tmp_path: Path,
) -> None:
    """Item 20: mypy --strict rejects passing UntrustedConnectionClaims where VerifiedConnectionContext is required."""
    snippet = tmp_path / "bad_identity_usage.py"
    snippet.write_text(
        "from kaiwen_gateway.core import UntrustedConnectionClaims, VerifiedConnectionContext\n"
        "def accept_verified(ctx: VerifiedConnectionContext) -> str:\n"
        "    return ctx.tenant_id\n"
        "accept_verified(UntrustedConnectionClaims(claimed_tenant_id='t1'))\n",
        encoding="utf-8",
    )
    stdout, stderr, exit_code = mypy_api.run(["--strict", str(snippet)])
    assert exit_code != 0, f"Expected mypy failure, got exit_code=0:\n{stdout}\n{stderr}"
    assert "[arg-type]" in stdout
