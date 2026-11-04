"""Cryptographic HMAC-SHA256 token verification for synthetic WebSocket transport.

Security & Trust Boundary Notes:
- Tokens are issued and verified exclusively by server-controlled policy using a server-side secret key.
- Tokens encode immutable provider, tenant, call, and allowed-scope authorizations.
- Tokens expire deterministically; expired, forged, truncated, or malformed tokens fail closed.
- Single-use replay protection is enforced: once consumed, a token cannot be reused.
- Tokens must only be transmitted in secure headers (e.g. `Authorization: Bearer <token>`);
  they must NEVER be placed in URL query strings, application logs, or trace attributes.
- Synthetic tokens are an internal test mechanism and do NOT represent production telephony provider authentication.
- In-memory replay protection is process-local and bounded; it does not provide distributed replay protection across multi-instance clusters.
"""

from __future__ import annotations

import base64
import hashlib
import hmac
import json
import math
import time
import uuid
from collections.abc import Iterable
from dataclasses import dataclass
from typing import Any, Final

from kaiwen_gateway.core.exceptions import IdentityAuthorizationError
from kaiwen_gateway.core.identity import (
    validate_call_id,
    validate_provider_call_ref,
    validate_provider_id,
    validate_scopes,
    validate_tenant_id,
)

MIN_SECRET_KEY_BYTES: Final[int] = 16
MAX_TOKEN_LENGTH_BYTES: Final[int] = 4096
MAX_TOKEN_LIFESPAN_SECONDS: Final[float] = 3600.0  # 1 hour maximum token validity
MAX_FUTURE_SKEW_SECONDS: Final[float] = 30.0  # 30 seconds clock skew tolerance
DEFAULT_TOKEN_TTL_SECONDS: Final[float] = 300.0  # 5 minutes default
MAX_REPLAY_CACHE_SIZE: Final[int] = 10000


@dataclass(frozen=True, slots=True)
class SyntheticTokenClaims:
    """Immutable verified claims extracted from a valid synthetic bearer token."""

    provider_id: str
    tenant_id: str
    call_id: str
    allowed_scopes: frozenset[str]
    provider_call_ref: str | None
    issued_at: float
    expires_at: float
    token_id: str


class SyntheticTokenVerifier:
    """Issues and cryptographically verifies HMAC-SHA256 tokens for synthetic transport testing."""

    def __init__(
        self,
        secret_key: bytes | str,
        *,
        enable_replay_protection: bool = True,
        max_replay_entries: int = MAX_REPLAY_CACHE_SIZE,
    ) -> None:
        key_bytes = secret_key.encode("utf-8") if isinstance(secret_key, str) else secret_key
        if len(key_bytes) < MIN_SECRET_KEY_BYTES:
            raise ValueError(
                f"Secret key must be at least {MIN_SECRET_KEY_BYTES} bytes, got {len(key_bytes)}"
            )
        if isinstance(max_replay_entries, bool) or not isinstance(max_replay_entries, int):
            raise ValueError("max_replay_entries must be an int")
        if max_replay_entries < 1 or max_replay_entries > 1_000_000:
            raise ValueError(
                f"max_replay_entries must be between 1 and 1,000,000, got {max_replay_entries}"
            )
        self._key = key_bytes
        self._enable_replay_protection = enable_replay_protection
        self._max_replay_entries = max_replay_entries
        self._consumed_tokens: dict[str, float] = {}

    def __repr__(self) -> str:
        return (
            f"<SyntheticTokenVerifier key_length={len(self._key)} "
            f"replay_protection={self._enable_replay_protection}>"
        )

    def is_token_consumed(self, token_id: str) -> bool:
        """Return True if the given token_id has already been consumed."""
        return token_id in self._consumed_tokens

    def revoke_token(self, token_id: str, expires_at: float | None = None) -> None:
        """Explicitly record a token_id as revoked / consumed."""
        self._consumed_tokens[token_id] = expires_at if expires_at is not None else float("inf")

    def _prune_expired_tokens(self, now: float) -> None:
        """Prune expired tokens from the in-memory replay cache."""
        expired_keys = [tid for tid, exp in self._consumed_tokens.items() if now > exp]
        for tid in expired_keys:
            del self._consumed_tokens[tid]

    def issue_token(
        self,
        *,
        provider_id: str,
        tenant_id: str,
        call_id: str,
        allowed_scopes: Iterable[str],
        ttl_seconds: float = DEFAULT_TOKEN_TTL_SECONDS,
        provider_call_ref: str | None = None,
        token_id: str | None = None,
        clock: float | None = None,
    ) -> str:
        """Issue an HMAC-SHA256 signed synthetic bearer token with verified identity claims."""
        pvd = validate_provider_id(provider_id)
        tnt = validate_tenant_id(tenant_id)
        cll = validate_call_id(call_id)
        scopes = validate_scopes(allowed_scopes)
        ref = (
            validate_provider_call_ref(provider_call_ref) if provider_call_ref is not None else None
        )

        if isinstance(ttl_seconds, bool) or not isinstance(ttl_seconds, (int, float)):
            raise ValueError("ttl_seconds must be a float or int")
        if (
            not math.isfinite(ttl_seconds)
            or ttl_seconds <= 0.0
            or ttl_seconds > MAX_TOKEN_LIFESPAN_SECONDS
        ):
            raise ValueError(
                f"ttl_seconds must be a positive finite number <= {MAX_TOKEN_LIFESPAN_SECONDS}, got {ttl_seconds}"
            )

        now = clock if clock is not None else time.time()
        exp = now + ttl_seconds
        jti = token_id if token_id is not None else uuid.uuid4().hex

        payload_dict: dict[str, Any] = {
            "pvd": pvd,
            "tnt": tnt,
            "cll": cll,
            "scp": sorted(scopes),
            "iat": round(now, 3),
            "exp": round(exp, 3),
            "jti": jti,
        }
        if ref is not None:
            payload_dict["ref"] = ref

        payload_bytes = json.dumps(payload_dict, separators=(",", ":"), sort_keys=True).encode(
            "utf-8"
        )
        payload_b64 = base64.urlsafe_b64encode(payload_bytes).rstrip(b"=").decode("ascii")

        sig = hmac.new(self._key, payload_b64.encode("ascii"), hashlib.sha256).digest()
        sig_b64 = base64.urlsafe_b64encode(sig).rstrip(b"=").decode("ascii")

        return f"{payload_b64}.{sig_b64}"

    def verify_token(
        self,
        token: str,
        *,
        clock: float | None = None,
        consume: bool = True,
    ) -> SyntheticTokenClaims:
        """Verify an HMAC-SHA256 signed synthetic bearer token and return verified claims.

        Raises:
            IdentityAuthorizationError: If the token is malformed, has an invalid signature,
                has expired, violates temporal bounds, was already consumed, or contains invalid fields.
        """
        if not isinstance(token, str) or not token:
            raise IdentityAuthorizationError("Synthetic token must be a non-empty string")

        if len(token) > MAX_TOKEN_LENGTH_BYTES:
            raise IdentityAuthorizationError(
                f"Synthetic token length ({len(token)}) exceeds maximum limit of {MAX_TOKEN_LENGTH_BYTES} bytes"
            )

        parts = token.split(".")
        if len(parts) != 2:
            raise IdentityAuthorizationError(
                "Malformed synthetic token structure (expected 2 parts)"
            )

        payload_b64, sig_b64 = parts[0], parts[1]

        # Verify signature in constant time
        expected_sig = hmac.new(self._key, payload_b64.encode("ascii"), hashlib.sha256).digest()
        try:
            padded_sig = sig_b64 + "=" * (-len(sig_b64) % 4)
            actual_sig = base64.urlsafe_b64decode(padded_sig.encode("ascii"))
        except Exception as exc:
            raise IdentityAuthorizationError("Malformed token signature encoding") from exc

        if not hmac.compare_digest(actual_sig, expected_sig):
            raise IdentityAuthorizationError(
                "Synthetic token signature verification failed (forged or altered)"
            )

        # Decode payload
        try:
            padded_payload = payload_b64 + "=" * (-len(payload_b64) % 4)
            payload_bytes = base64.urlsafe_b64decode(padded_payload.encode("ascii"))
            payload = json.loads(payload_bytes.decode("utf-8"))
        except Exception as exc:
            raise IdentityAuthorizationError(
                "Malformed synthetic token payload encoding or JSON"
            ) from exc

        if not isinstance(payload, dict):
            raise IdentityAuthorizationError("Synthetic token payload must be a JSON object")

        # Validate required fields
        for field in ("pvd", "tnt", "cll", "scp", "iat", "exp"):
            if field not in payload:
                raise IdentityAuthorizationError(
                    f"Synthetic token missing required field {field!r}"
                )

        now = clock if clock is not None else time.time()
        exp = payload["exp"]
        iat = payload["iat"]

        if isinstance(exp, bool) or not isinstance(exp, (int, float)) or not math.isfinite(exp):
            raise IdentityAuthorizationError(
                "Synthetic token 'exp' must be a finite numeric timestamp"
            )
        if isinstance(iat, bool) or not isinstance(iat, (int, float)) or not math.isfinite(iat):
            raise IdentityAuthorizationError(
                "Synthetic token 'iat' must be a finite numeric timestamp"
            )

        # Temporal ordering and bounds
        if iat > exp:
            raise IdentityAuthorizationError(
                f"Synthetic token 'iat' ({iat}) cannot be after 'exp' ({exp})"
            )
        if exp - iat > MAX_TOKEN_LIFESPAN_SECONDS:
            raise IdentityAuthorizationError(
                f"Synthetic token lifespan ({exp - iat:.1f}s) exceeds maximum allowed duration ({MAX_TOKEN_LIFESPAN_SECONDS:.1f}s)"
            )
        if now < iat - MAX_FUTURE_SKEW_SECONDS:
            raise IdentityAuthorizationError(
                f"Synthetic token issued in the future (iat={iat}, current={now})"
            )
        if now > exp:
            raise IdentityAuthorizationError(f"Synthetic token expired at {exp} (current: {now})")

        # Validate identity fields BEFORE consuming replay token
        try:
            pvd = validate_provider_id(payload["pvd"])
            tnt = validate_tenant_id(payload["tnt"])
            cll = validate_call_id(payload["cll"])
            scp = validate_scopes(payload["scp"])
            ref = (
                validate_provider_call_ref(payload["ref"])
                if "ref" in payload and payload["ref"] is not None
                else None
            )
        except Exception as exc:
            raise IdentityAuthorizationError(
                f"Invalid identity field in synthetic token: {exc}"
            ) from exc

        raw_jti = payload.get("jti")
        token_id = str(raw_jti) if raw_jti is not None and str(raw_jti) else sig_b64

        # Replay protection check
        if self._enable_replay_protection and consume:
            if token_id in self._consumed_tokens:
                raise IdentityAuthorizationError(
                    "Synthetic token has already been consumed (replay detected)"
                )
            if len(self._consumed_tokens) >= self._max_replay_entries:
                self._prune_expired_tokens(now)
                if len(self._consumed_tokens) >= self._max_replay_entries:
                    raise IdentityAuthorizationError("Token replay cache capacity exceeded")
            self._consumed_tokens[token_id] = float(exp)

        return SyntheticTokenClaims(
            provider_id=pvd,
            tenant_id=tnt,
            call_id=cll,
            allowed_scopes=scp,
            provider_call_ref=ref,
            issued_at=float(iat),
            expires_at=float(exp),
            token_id=token_id,
        )
