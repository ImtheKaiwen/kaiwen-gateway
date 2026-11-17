"""Section A tests: EventEnvelope validation, immutability, depth, NaN, and size limits."""

from __future__ import annotations

import json
from types import MappingProxyType

import pytest

from kaiwen_gateway.core import InvalidIdentityError
from kaiwen_gateway.modules.events import (
    DEFAULT_MAX_EVENT_BYTES,
    EVENTS_PROTOCOL_VERSION,
    EventEnvelope,
    InvalidEventError,
    OversizedEventError,
    UnsupportedEventVersionError,
    topic_matches_pattern,
    validate_topic,
    validate_topic_pattern,
)


def _make_envelope(**overrides: object) -> EventEnvelope:
    defaults: dict[str, object] = {
        "protocol_version": EVENTS_PROTOCOL_VERSION,
        "event_id": "evt_01J00000000000000000000001",
        "event_type": "call.status.updated",
        "topic": "call.status",
        "timestamp": "2026-11-18T00:00:00Z",
        "tenant_id": "tenant-alpha",
        "principal_id": "principal-1",
        "payload": {"status": "active", "count": 1},
    }
    defaults.update(overrides)
    return EventEnvelope(**defaults)  # type: ignore[arg-type]


def test_event_envelope_valid_construction_serialization_and_privacy_repr() -> None:
    env = _make_envelope(
        correlation_id="corr-123",
        payload={"secret_field": "do-not-leak-in-repr", "nested": {"ok": True}, "items": [1, 2]},
    )

    assert env.protocol_version == "v1"
    assert env.timestamp == "2026-11-18T00:00:00Z"
    assert env.correlation_id == "corr-123"
    assert isinstance(env.payload, MappingProxyType)
    assert isinstance(env.payload["nested"], MappingProxyType)
    assert isinstance(env.payload["items"], tuple)

    raw_json = env.to_json()
    assert env.serialized_bytes == len(raw_json.encode("utf-8"))
    assert env.to_json_bytes() == raw_json.encode("utf-8")
    parsed = json.loads(raw_json)
    assert parsed["correlation_id"] == "corr-123"
    assert parsed["payload"]["nested"]["ok"] is True

    rep = repr(env)
    assert "do-not-leak-in-repr" not in rep
    assert "secret_field" not in rep
    assert env.event_id in rep


def test_event_envelope_caller_mutation_isolation() -> None:
    nested_list = [1, 2, 3]
    nested_dict = {"inner": "initial"}
    caller_payload: dict[str, object] = {"list": nested_list, "dict": nested_dict}

    env = _make_envelope(payload=caller_payload)
    caller_payload["injected"] = "bad"
    nested_list.append(99)
    nested_dict["inner"] = "mutated"

    assert "injected" not in env.payload
    assert env.payload["list"] == (1, 2, 3)
    assert env.payload["dict"]["inner"] == "initial"
    with pytest.raises(TypeError):
        env.payload["x"] = 1  # type: ignore[index]


def test_event_envelope_rejects_unsupported_protocol_version() -> None:
    with pytest.raises(UnsupportedEventVersionError):
        _make_envelope(protocol_version="v2")


@pytest.mark.parametrize(
    ("field_name", "bad_value"),
    [
        ("event_id", ""),
        ("event_id", "bad id with spaces"),
        ("event_id", "-leading-dash"),
        ("event_type", "singleword"),
        ("event_type", "UPPER.CASE"),
        ("event_type", "a.b.c.d.e.f"),
        ("topic", ""),
        ("topic", "call.*"),
        ("topic", "Call.Status"),
        ("timestamp", "2026-11-18T00:00:00"),
        ("timestamp", "2026-11-18T00:00:00+02:00"),
        ("timestamp", "not-a-timestamp"),
        ("tenant_id", ""),
        ("principal_id", ""),
        ("correlation_id", "bad correlation"),
    ],
)
def test_event_envelope_rejects_invalid_metadata_fields(
    field_name: str,
    bad_value: str,
) -> None:
    with pytest.raises((InvalidEventError, InvalidIdentityError)):
        _make_envelope(**{field_name: bad_value})


def test_topic_and_pattern_validation_rules() -> None:
    assert validate_topic("call.status") == "call.status"
    assert validate_topic_pattern("call.status") == "call.status"
    assert validate_topic_pattern("call.*") == "call.*"
    assert topic_matches_pattern("call.status.updated", "call.*") is True
    assert topic_matches_pattern("agent.status", "call.*") is False
    assert topic_matches_pattern("call.status", "call.status") is True

    for bad_pattern in ("*", ".*", "call.*.updated", "call*", "call.**", ""):
        with pytest.raises(InvalidEventError):
            validate_topic_pattern(bad_pattern)


def test_event_envelope_json_depth_limits() -> None:
    def make_nested(depth: int) -> dict[str, object]:
        node: dict[str, object] = {"leaf": 1}
        for _ in range(depth - 1):
            node = {"child": node}
        return node

    # Depth 16 payload (root dict = depth 1 -> 14 wrappers + leaf scalar = depth 16)
    ok_env = _make_envelope(payload=make_nested(15))
    assert ok_env.serialized_bytes > 0

    with pytest.raises(InvalidEventError, match="depth"):
        _make_envelope(payload=make_nested(16))


@pytest.mark.parametrize("bad_float", [float("nan"), float("inf"), float("-inf")])
def test_event_envelope_rejects_non_finite_floats(bad_float: float) -> None:
    with pytest.raises(InvalidEventError, match="non-finite"):
        _make_envelope(payload={"metric": bad_float})


def test_event_envelope_rejects_non_json_values_and_non_str_keys() -> None:
    with pytest.raises(InvalidEventError):
        _make_envelope(payload={"raw": b"bytes"})
    with pytest.raises(InvalidEventError):
        _make_envelope(payload={1: "int-key"})


def test_event_envelope_size_boundary_and_oversize_rejection() -> None:
    base_env = _make_envelope(payload={"data": ""})
    remaining = DEFAULT_MAX_EVENT_BYTES - base_env.serialized_bytes
    exact_env = _make_envelope(payload={"data": "a" * remaining})
    assert exact_env.serialized_bytes == DEFAULT_MAX_EVENT_BYTES

    with pytest.raises(OversizedEventError):
        _make_envelope(payload={"data": "a" * (remaining + 1)})
