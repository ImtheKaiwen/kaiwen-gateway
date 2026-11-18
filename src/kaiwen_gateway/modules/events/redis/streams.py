"""Optional Redis Streams durable store, PEL crash recovery, and poison handling (E4).

Implements `DurableEventStorePort` using Redis Streams (`XADD`, `XGROUP`, `XREADGROUP`,
`XACK`, `XRANGE`, `XPENDING`, `XAUTOCLAIM`) with bounded retention (`MAXLEN ~`),
tenant-isolated stream keys, explicit acknowledgment ownership, and bounded DLQ
quarantine for poison messages.

Delivery Guarantee:
    Intended **at-least-once** within the configured `MAXLEN` retention window and
    Redis persistence configuration. Never claims exactly-once processing; consumers
    must be idempotent using `EventEnvelope.event_id`.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import math
import re
import time
from collections.abc import Awaitable, Callable, Sequence
from dataclasses import dataclass, field
from typing import Any, Final

from kaiwen_gateway.core.identity import validate_tenant_id
from kaiwen_gateway.modules.events.domain import (
    DEFAULT_MAX_EVENT_BYTES,
    DurableEventRecord,
    EventEnvelope,
    EventsBackendUnavailableError,
    EventsModuleClosedError,
    InvalidEventError,
    validate_topic,
)
from kaiwen_gateway.modules.events.redis.config import (
    RedisClientConfig,
    create_async_redis_client,
)
from kaiwen_gateway.modules.events.telemetry import (
    DurableStreamTelemetrySink,
    InMemoryDurableStreamTelemetrySink,
)

DEFAULT_STREAM_MAXLEN: Final[int] = 10_000
DEFAULT_DEAD_LETTER_MAXLEN: Final[int] = 1_000
DEFAULT_STREAM_MAX_READ_BATCH_SIZE: Final[int] = 100
DEFAULT_STREAM_MAX_REPLAY_LIMIT: Final[int] = 500
DEFAULT_CLAIM_MIN_IDLE_MS: Final[int] = 30_000
DEFAULT_MAX_DELIVERY_ATTEMPTS: Final[int] = 5
DEFAULT_RECOVERY_INTERVAL_SECONDS: Final[float] = 5.0
DEFAULT_RECOVERY_BATCH_SIZE: Final[int] = 50

_STREAM_IDENT_PATTERN: Final[re.Pattern[str]] = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$")
_ENTRY_ID_PATTERN: Final[re.Pattern[str]] = re.compile(r"^\d+-\d+$")


def _validate_stream_ident(value: str, field_name: str) -> str:
    if not isinstance(value, str) or not _STREAM_IDENT_PATTERN.fullmatch(value):
        raise InvalidEventError(f"{field_name} must match ^[A-Za-z0-9][A-Za-z0-9._-]{{0,127}}$")
    return value


def _validate_positive_int(value: int, field_name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise InvalidEventError(f"{field_name} must be a positive integer")
    return value


def _validate_non_negative_int(value: int, field_name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise InvalidEventError(f"{field_name} must be a non-negative integer")
    return value


def _as_str(value: Any) -> str:
    return value.decode("utf-8", errors="replace") if isinstance(value, bytes) else str(value)


def _field_str(fields: dict[Any, Any], key: str, default: str | None = None) -> str | None:
    val = fields.get(key)
    if val is None:
        val = fields.get(key.encode("utf-8"))
    if val is None:
        return default
    return _as_str(val)


def _parse_entry_id(entry_id: str) -> tuple[int, int] | None:
    if not _ENTRY_ID_PATTERN.fullmatch(entry_id):
        return None
    ms_str, seq_str = entry_id.split("-", 1)
    return int(ms_str), int(seq_str)


@dataclass(frozen=True, slots=True)
class RedisStreamsConfig:
    """Validated configuration for `RedisStreamsDurableStore`."""

    redis: RedisClientConfig = field(default_factory=RedisClientConfig)
    maxlen: int = DEFAULT_STREAM_MAXLEN
    approximate_trim: bool = True
    max_read_batch_size: int = DEFAULT_STREAM_MAX_READ_BATCH_SIZE
    max_replay_limit: int = DEFAULT_STREAM_MAX_REPLAY_LIMIT
    claim_min_idle_ms: int = DEFAULT_CLAIM_MIN_IDLE_MS
    max_delivery_attempts: int = DEFAULT_MAX_DELIVERY_ATTEMPTS
    dead_letter_maxlen: int = DEFAULT_DEAD_LETTER_MAXLEN
    recovery_interval_seconds: float = DEFAULT_RECOVERY_INTERVAL_SECONDS
    recovery_batch_size: int = DEFAULT_RECOVERY_BATCH_SIZE
    max_event_bytes: int = DEFAULT_MAX_EVENT_BYTES

    def __post_init__(self) -> None:
        if not isinstance(self.redis, RedisClientConfig):
            raise InvalidEventError("redis must be a RedisClientConfig instance")
        if not isinstance(self.approximate_trim, bool):
            raise InvalidEventError("approximate_trim must be a bool")
        _validate_positive_int(self.maxlen, "maxlen")
        _validate_positive_int(self.max_read_batch_size, "max_read_batch_size")
        _validate_positive_int(self.max_replay_limit, "max_replay_limit")
        _validate_non_negative_int(self.claim_min_idle_ms, "claim_min_idle_ms")
        _validate_positive_int(self.max_delivery_attempts, "max_delivery_attempts")
        _validate_positive_int(self.dead_letter_maxlen, "dead_letter_maxlen")
        _validate_positive_int(self.recovery_batch_size, "recovery_batch_size")
        _validate_positive_int(self.max_event_bytes, "max_event_bytes")
        if (
            isinstance(self.recovery_interval_seconds, bool)
            or not isinstance(self.recovery_interval_seconds, (int, float))
            or not math.isfinite(self.recovery_interval_seconds)
            or self.recovery_interval_seconds <= 0.0
        ):
            raise InvalidEventError("recovery_interval_seconds must be a finite positive number")


@dataclass(frozen=True, slots=True)
class StreamPendingSummary:
    """Bounded summary of pending entries in a Redis Stream consumer group."""

    pending_count: int
    min_entry_id: str | None
    max_entry_id: str | None


@dataclass(frozen=True, slots=True)
class StreamDeadLetterRecord:
    """Diagnostic metadata for a quarantined poison or malformed stream entry."""

    dlq_entry_id: str
    original_entry_id: str
    event_id: str | None
    group: str
    delivery_attempts: int
    reason: str
    correlation_id: str | None = None


@dataclass(frozen=True, slots=True)
class StreamRecoveryResult:
    """Outcome of a single bounded `reclaim_pending()` pass."""

    reclaimed: tuple[DurableEventRecord, ...]
    dead_lettered: tuple[StreamDeadLetterRecord, ...]
    next_start_id: str


@dataclass(frozen=True, slots=True)
class StreamStats:
    """Observable Redis Stream and consumer-group metrics for diagnostics & load testing."""

    length: int
    first_entry_id: str | None
    last_entry_id: str | None
    group_pending: int | None
    group_lag: int | None


class RedisStreamsDurableStore:
    """Optional E4 Redis Streams durable event store implementing `DurableEventStorePort`.

    Owns no automatic acknowledgment on read: `ack()` must be called explicitly by the
    downstream consumer worker only after side-effect processing succeeds.
    """

    __slots__ = (
        "_config",
        "_telemetry",
        "_client",
        "_owns_client",
        "_ensured_groups",
        "_started",
        "_closed",
        "_healthy",
    )

    def __init__(
        self,
        config: RedisStreamsConfig | None = None,
        *,
        redis_client: Any | None = None,
        telemetry: DurableStreamTelemetrySink | None = None,
    ) -> None:
        self._config: RedisStreamsConfig = config if config is not None else RedisStreamsConfig()
        self._telemetry: DurableStreamTelemetrySink = (
            telemetry if telemetry is not None else InMemoryDurableStreamTelemetrySink()
        )
        if redis_client is not None:
            self._client: Any = redis_client
            self._owns_client: bool = False
        else:
            self._client = create_async_redis_client(self._config.redis)
            self._owns_client = True

        self._ensured_groups: set[tuple[str, str]] = set()
        self._started: bool = False
        self._closed: bool = False
        self._healthy: bool = False

    @property
    def config(self) -> RedisStreamsConfig:
        """Return immutable stream store configuration."""
        return self._config

    @property
    def telemetry(self) -> DurableStreamTelemetrySink:
        """Return bounded-cardinality durable stream telemetry sink."""
        return self._telemetry

    @property
    def is_started(self) -> bool:
        """Return True after `startup()` completes and before `shutdown()`."""
        return self._started and not self._closed

    @property
    def is_closed(self) -> bool:
        """Return True after `shutdown()` has been called."""
        return self._closed

    @property
    def is_healthy(self) -> bool:
        """Return True when the backing Redis connection is healthy."""
        return self._healthy and not self._closed

    def stream_key(self, tenant_id: str, topic: str) -> str:
        """Return canonical tenant-isolated Redis Stream key."""
        t_id = validate_tenant_id(tenant_id)
        t_topic = validate_topic(topic)
        return f"{self._config.redis.key_prefix}:str:v1:{t_id}:{t_topic}"

    def dead_letter_key(self, tenant_id: str, topic: str) -> str:
        """Return canonical tenant-isolated Redis dead-letter stream key."""
        t_id = validate_tenant_id(tenant_id)
        t_topic = validate_topic(topic)
        return f"{self._config.redis.key_prefix}:dlq:v1:{t_id}:{t_topic}"

    async def startup(self) -> None:
        """Verify Redis connectivity for durable stream operations."""
        if self._closed:
            raise EventsModuleClosedError(
                "Cannot restart RedisStreamsDurableStore after terminal shutdown"
            )
        if self._started:
            return
        try:
            await self._client.ping()
        except Exception as exc:
            self._healthy = False
            self._telemetry.record_redis_error()
            raise EventsBackendUnavailableError(
                "Redis Streams backend unavailable during startup"
            ) from exc
        self._started = True
        self._healthy = True

    async def shutdown(self) -> None:
        """Idempotently close the store and release owned Redis client resources."""
        if self._closed:
            return
        self._closed = True
        self._healthy = False
        self._ensured_groups.clear()
        if self._owns_client:
            with contextlib.suppress(Exception):
                await self._client.aclose()

    def _require_open(self) -> None:
        if self._closed:
            raise EventsModuleClosedError("RedisStreamsDurableStore is closed")

    def _mark_redis_failure(self, exc: Exception, *, message: str) -> EventsBackendUnavailableError:
        self._healthy = False
        self._telemetry.record_redis_error()
        return EventsBackendUnavailableError(message)

    async def _ensure_group(self, stream: str, group: str) -> None:
        cache_key = (stream, group)
        if cache_key in self._ensured_groups:
            return
        try:
            await self._client.xgroup_create(stream, group, id="0", mkstream=True)
        except Exception as exc:
            if "BUSYGROUP" not in str(exc):
                raise
        self._ensured_groups.add(cache_key)

    def _deserialize_envelope(
        self,
        fields: dict[Any, Any],
        *,
        expected_tenant_id: str,
        expected_topic: str,
    ) -> EventEnvelope | None:
        raw_env = fields.get("envelope")
        if raw_env is None:
            raw_env = fields.get(b"envelope")
        if isinstance(raw_env, bytes):
            raw_env = raw_env.decode("utf-8", errors="ignore")
        if not isinstance(raw_env, str) or not raw_env:
            return None
        try:
            parsed = json.loads(raw_env)
            if not isinstance(parsed, dict):
                return None
            envelope = EventEnvelope(
                protocol_version=parsed["protocol_version"],
                event_id=parsed["event_id"],
                event_type=parsed["event_type"],
                topic=parsed["topic"],
                timestamp=parsed["timestamp"],
                tenant_id=parsed["tenant_id"],
                principal_id=parsed["principal_id"],
                payload=parsed["payload"],
                correlation_id=parsed.get("correlation_id"),
                max_event_bytes=self._config.max_event_bytes,
            )
        except Exception:
            return None
        if envelope.tenant_id != expected_tenant_id or envelope.topic != expected_topic:
            return None
        return envelope

    async def _quarantine_entry(
        self,
        *,
        stream: str,
        dlq_stream: str,
        group: str,
        entry_id: str,
        delivery_attempts: int | None,
        reason: str,
        envelope: EventEnvelope | None,
        raw_fields: dict[Any, Any] | None = None,
    ) -> StreamDeadLetterRecord:
        event_id: str | None = envelope.event_id if envelope is not None else None
        correlation_id: str | None = envelope.correlation_id if envelope is not None else None
        if event_id is None and raw_fields is not None:
            raw_eid = raw_fields.get("event_id") or raw_fields.get(b"event_id")
            if isinstance(raw_eid, bytes):
                raw_eid = raw_eid.decode("utf-8", errors="ignore")
            if isinstance(raw_eid, str) and raw_eid:
                event_id = raw_eid[:128]

        dlq_fields: dict[str, str] = {
            "original_entry_id": entry_id,
            "group": group,
            "reason": reason,
        }
        if delivery_attempts is not None:
            dlq_fields["delivery_attempts"] = str(delivery_attempts)
        if event_id is not None:
            dlq_fields["event_id"] = event_id
        if correlation_id is not None:
            dlq_fields["correlation_id"] = correlation_id
        if envelope is not None:
            dlq_fields["envelope"] = envelope.to_json()

        try:
            dlq_id_raw = await self._client.xadd(
                dlq_stream,
                dlq_fields,
                maxlen=self._config.dead_letter_maxlen,
                approximate=self._config.approximate_trim,
            )
            dlq_id = (
                dlq_id_raw.decode("utf-8") if isinstance(dlq_id_raw, bytes) else str(dlq_id_raw)
            )
            await self._client.xack(stream, group, entry_id)
            self._telemetry.record_dead_letter(succeeded=True)
            return StreamDeadLetterRecord(
                dlq_entry_id=dlq_id,
                original_entry_id=entry_id,
                event_id=event_id,
                group=group,
                delivery_attempts=delivery_attempts if delivery_attempts is not None else 0,
                reason=reason,
                correlation_id=correlation_id,
            )
        except Exception as exc:
            self._telemetry.record_dead_letter(succeeded=False)
            raise self._mark_redis_failure(
                exc, message="Redis Streams dead-letter quarantine failed"
            ) from exc

    async def append(self, event: EventEnvelope) -> str:
        """Persist `event` to its tenant/topic Redis Stream with `MAXLEN` retention."""
        self._require_open()
        if not isinstance(event, EventEnvelope):
            raise TypeError(f"event must be an EventEnvelope, got {type(event).__name__}")
        if event.serialized_bytes > self._config.max_event_bytes:
            raise InvalidEventError(
                f"Event size {event.serialized_bytes} exceeds max_event_bytes {self._config.max_event_bytes}"
            )

        stream = self.stream_key(event.tenant_id, event.topic)
        fields: dict[str, str] = {
            "event_id": event.event_id,
            "protocol_version": event.protocol_version,
            "envelope": event.to_json(),
        }
        try:
            entry_id_raw = await self._client.xadd(
                stream,
                fields,
                maxlen=self._config.maxlen,
                approximate=self._config.approximate_trim,
            )
            self._healthy = True
            self._telemetry.record_append(succeeded=True)
            return (
                entry_id_raw.decode("utf-8")
                if isinstance(entry_id_raw, bytes)
                else str(entry_id_raw)
            )
        except TimeoutError as exc:
            self._telemetry.record_append(succeeded=False, ambiguous=True)
            raise self._mark_redis_failure(
                exc,
                message=(
                    "Redis Streams append timed out; write outcome is ambiguous and was not retried"
                ),
            ) from exc
        except Exception as exc:
            if "timeout" in type(exc).__name__.lower():
                self._telemetry.record_append(succeeded=False, ambiguous=True)
                raise self._mark_redis_failure(
                    exc,
                    message=(
                        "Redis Streams append timed out; write outcome is ambiguous and was not retried"
                    ),
                ) from exc
            self._telemetry.record_append(succeeded=False, ambiguous=False)
            raise self._mark_redis_failure(exc, message="Redis Streams append failed") from exc

    async def read_group(
        self,
        *,
        tenant_id: str,
        topic: str,
        group: str,
        consumer: str,
        count: int,
        block_ms: int,
    ) -> list[DurableEventRecord]:
        """Read new entries via `XREADGROUP` without auto-acknowledging."""
        self._require_open()
        stream = self.stream_key(tenant_id, topic)
        dlq_stream = self.dead_letter_key(tenant_id, topic)
        canonical_group = _validate_stream_ident(group, "group")
        canonical_consumer = _validate_stream_ident(consumer, "consumer")
        _validate_positive_int(count, "count")
        _validate_non_negative_int(block_ms, "block_ms")
        if count > self._config.max_read_batch_size:
            raise InvalidEventError(
                f"count {count} exceeds max_read_batch_size {self._config.max_read_batch_size}"
            )

        try:
            await self._ensure_group(stream, canonical_group)
            raw_resp = await self._client.xreadgroup(
                canonical_group,
                canonical_consumer,
                {stream: ">"},
                count=count,
                block=block_ms if block_ms > 0 else None,
            )
            self._healthy = True
        except Exception as exc:
            self._telemetry.record_read(succeeded=False)
            raise self._mark_redis_failure(exc, message="Redis Streams read_group failed") from exc

        records: list[DurableEventRecord] = []
        if not raw_resp:
            self._telemetry.record_read(succeeded=True, record_count=0)
            return records

        for _stream_name, entries in raw_resp:
            for raw_id, fields in entries:
                entry_id = raw_id.decode("utf-8") if isinstance(raw_id, bytes) else str(raw_id)
                envelope = self._deserialize_envelope(
                    fields,
                    expected_tenant_id=tenant_id,
                    expected_topic=topic,
                )
                if envelope is None:
                    self._telemetry.record_malformed_record()
                    await self._quarantine_entry(
                        stream=stream,
                        dlq_stream=dlq_stream,
                        group=canonical_group,
                        entry_id=entry_id,
                        delivery_attempts=1,
                        reason="malformed_or_tenant_mismatch",
                        envelope=None,
                        raw_fields=fields,
                    )
                    continue
                records.append(DurableEventRecord(envelope=envelope, sequence_id=entry_id))

        self._telemetry.record_read(succeeded=True, record_count=len(records))
        return records

    async def ack(
        self,
        *,
        tenant_id: str,
        topic: str,
        group: str,
        record_ids: Sequence[str],
    ) -> int:
        """Explicitly acknowledge processed stream entries via `XACK`."""
        self._require_open()
        stream = self.stream_key(tenant_id, topic)
        canonical_group = _validate_stream_ident(group, "group")
        if not record_ids:
            return 0
        validated_ids: list[str] = []
        for rid in record_ids:
            if not isinstance(rid, str) or not rid or any(ch.isspace() for ch in rid):
                raise InvalidEventError(
                    "record_ids entries must be non-empty non-whitespace strings"
                )
            validated_ids.append(rid)

        try:
            acked = int(await self._client.xack(stream, canonical_group, *validated_ids))
            self._healthy = True
            self._telemetry.record_ack(succeeded=True, acked_count=acked)
            return acked
        except Exception as exc:
            self._telemetry.record_ack(succeeded=False)
            raise self._mark_redis_failure(exc, message="Redis Streams ack failed") from exc

    async def dead_letter(
        self,
        *,
        tenant_id: str,
        topic: str,
        group: str,
        record_id: str,
        reason: str,
        envelope: EventEnvelope | None = None,
        delivery_attempts: int | None = None,
    ) -> StreamDeadLetterRecord:
        """Quarantine a poison record to the DLQ stream and XACK only after DLQ XADD succeeds."""
        self._require_open()
        stream = self.stream_key(tenant_id, topic)
        dlq_stream = self.dead_letter_key(tenant_id, topic)
        canonical_group = _validate_stream_ident(group, "group")
        if not isinstance(record_id, str) or not record_id or any(ch.isspace() for ch in record_id):
            raise InvalidEventError("record_id must be a non-empty non-whitespace string")
        if not isinstance(reason, str) or not reason or len(reason) > 128:
            raise InvalidEventError("reason must be a non-empty string (<= 128 chars)")
        if delivery_attempts is not None:
            _validate_positive_int(delivery_attempts, "delivery_attempts")
        return await self._quarantine_entry(
            stream=stream,
            dlq_stream=dlq_stream,
            group=canonical_group,
            entry_id=record_id,
            delivery_attempts=delivery_attempts,
            reason=reason,
            envelope=envelope,
        )

    async def replay(
        self,
        *,
        tenant_id: str,
        topic: str,
        from_id: str = "0-0",
        limit: int = 100,
    ) -> list[DurableEventRecord]:
        """Replay retained stream entries from `from_id` within `max_replay_limit`."""
        self._require_open()
        stream = self.stream_key(tenant_id, topic)
        if not isinstance(from_id, str) or not from_id or any(ch.isspace() for ch in from_id):
            raise InvalidEventError("from_id must be a non-empty non-whitespace string")
        _validate_positive_int(limit, "limit")
        if limit > self._config.max_replay_limit:
            raise InvalidEventError(
                f"limit {limit} exceeds max_replay_limit {self._config.max_replay_limit}"
            )

        try:
            # Check for retention-trimmed loss if caller requested a concrete cursor > 0-0
            parsed_cursor = _parse_entry_id(from_id)
            if parsed_cursor is not None and parsed_cursor != (0, 0):
                with contextlib.suppress(Exception):
                    info = await self._client.xinfo_stream(stream)
                    first_entry = info.get("first-entry")
                    if first_entry is not None:
                        first_id_raw = first_entry[0]
                        first_id = (
                            first_id_raw.decode("utf-8")
                            if isinstance(first_id_raw, bytes)
                            else str(first_id_raw)
                        )
                        parsed_first = _parse_entry_id(first_id)
                        if parsed_first is not None and parsed_first > parsed_cursor:
                            self._telemetry.record_retention_loss(count=1)

            raw_entries = await self._client.xrange(stream, min=from_id, max="+", count=limit)
            self._healthy = True
        except Exception as exc:
            raise self._mark_redis_failure(exc, message="Redis Streams replay failed") from exc

        records: list[DurableEventRecord] = []
        for raw_id, fields in raw_entries:
            entry_id = raw_id.decode("utf-8") if isinstance(raw_id, bytes) else str(raw_id)
            envelope = self._deserialize_envelope(
                fields,
                expected_tenant_id=tenant_id,
                expected_topic=topic,
            )
            if envelope is None:
                self._telemetry.record_malformed_record()
                continue
            records.append(DurableEventRecord(envelope=envelope, sequence_id=entry_id))
        return records

    async def pending_summary(
        self,
        *,
        tenant_id: str,
        topic: str,
        group: str,
    ) -> StreamPendingSummary:
        """Return bounded `XPENDING` summary for `(tenant_id, topic, group)`."""
        self._require_open()
        stream = self.stream_key(tenant_id, topic)
        canonical_group = _validate_stream_ident(group, "group")
        try:
            await self._ensure_group(stream, canonical_group)
            summary = await self._client.xpending(stream, canonical_group)
            self._healthy = True
        except Exception as exc:
            raise self._mark_redis_failure(exc, message="Redis Streams xpending failed") from exc

        min_id = summary.get("min")
        max_id = summary.get("max")
        return StreamPendingSummary(
            pending_count=int(summary.get("pending", 0) or 0),
            min_entry_id=min_id.decode("utf-8") if isinstance(min_id, bytes) else min_id,
            max_entry_id=max_id.decode("utf-8") if isinstance(max_id, bytes) else max_id,
        )

    async def reclaim_pending(
        self,
        *,
        tenant_id: str,
        topic: str,
        group: str,
        consumer: str,
        min_idle_ms: int | None = None,
        count: int | None = None,
        start_id: str = "0-0",
    ) -> StreamRecoveryResult:
        """Inspect `XPENDING` and reclaim idle entries via `XAUTOCLAIM` with poison quarantine."""
        self._require_open()
        stream = self.stream_key(tenant_id, topic)
        dlq_stream = self.dead_letter_key(tenant_id, topic)
        canonical_group = _validate_stream_ident(group, "group")
        canonical_consumer = _validate_stream_ident(consumer, "consumer")
        idle_ms = (
            self._config.claim_min_idle_ms
            if min_idle_ms is None
            else _validate_non_negative_int(min_idle_ms, "min_idle_ms")
        )
        batch = (
            self._config.recovery_batch_size
            if count is None
            else _validate_positive_int(count, "count")
        )
        if not isinstance(start_id, str) or not start_id or any(ch.isspace() for ch in start_id):
            raise InvalidEventError("start_id must be a non-empty non-whitespace string")
        if batch > self._config.max_read_batch_size:
            raise InvalidEventError(
                f"count {batch} exceeds max_read_batch_size {self._config.max_read_batch_size}"
            )

        try:
            await self._ensure_group(stream, canonical_group)
            claim_resp = await self._client.xautoclaim(
                stream,
                canonical_group,
                canonical_consumer,
                min_idle_time=idle_ms,
                start_id=start_id,
                count=batch,
            )
            claimed_entries = claim_resp[1]
            if len(claim_resp) >= 3 and claim_resp[2]:
                self._telemetry.record_retention_loss(count=len(claim_resp[2]))

            delivery_counts: dict[str, int] = {}
            claimed_ids = [_as_str(raw_id) for raw_id, _fields in claimed_entries]
            if claimed_ids:
                pending_rows = await self._client.xpending_range(
                    stream,
                    canonical_group,
                    min=claimed_ids[0],
                    max=claimed_ids[-1],
                    count=len(claimed_ids),
                )
                for row in pending_rows:
                    mid = row.get("message_id")
                    if mid is not None:
                        delivery_counts[_as_str(mid)] = int(row.get("times_delivered", 1))
            self._healthy = True
        except Exception as exc:
            self._telemetry.record_recovery(succeeded=False)
            raise self._mark_redis_failure(
                exc, message="Redis Streams reclaim_pending failed"
            ) from exc

        next_start_id = _as_str(claim_resp[0])

        reclaimed: list[DurableEventRecord] = []
        dead_lettered: list[StreamDeadLetterRecord] = []

        for raw_id, fields in claimed_entries:
            entry_id = _as_str(raw_id)
            if fields is None:
                self._telemetry.record_retention_loss(count=1)
                continue
            # XAUTOCLAIM already incremented times_delivered before xpending_range above
            attempts_now = delivery_counts.get(entry_id, 1)

            envelope = self._deserialize_envelope(
                fields,
                expected_tenant_id=tenant_id,
                expected_topic=topic,
            )
            if envelope is None:
                self._telemetry.record_malformed_record()
                dlq_rec = await self._quarantine_entry(
                    stream=stream,
                    dlq_stream=dlq_stream,
                    group=canonical_group,
                    entry_id=entry_id,
                    delivery_attempts=attempts_now,
                    reason="malformed_or_tenant_mismatch",
                    envelope=None,
                    raw_fields=fields,
                )
                dead_lettered.append(dlq_rec)
                continue

            if attempts_now > self._config.max_delivery_attempts:
                self._telemetry.record_retry_exhausted()
                dlq_rec = await self._quarantine_entry(
                    stream=stream,
                    dlq_stream=dlq_stream,
                    group=canonical_group,
                    entry_id=entry_id,
                    delivery_attempts=attempts_now,
                    reason="max_delivery_attempts_exceeded",
                    envelope=envelope,
                )
                dead_lettered.append(dlq_rec)
                continue

            reclaimed.append(
                DurableEventRecord(
                    envelope=envelope,
                    sequence_id=entry_id,
                    delivery_attempts=delivery_counts.get(entry_id),
                )
            )

        self._telemetry.record_recovery(succeeded=True, reclaimed_count=len(reclaimed))
        return StreamRecoveryResult(
            reclaimed=tuple(reclaimed),
            dead_lettered=tuple(dead_lettered),
            next_start_id=next_start_id,
        )

    async def dead_letter_entries(
        self,
        *,
        tenant_id: str,
        topic: str,
        limit: int | None = None,
    ) -> list[StreamDeadLetterRecord]:
        """Read bounded quarantine metadata entries from the tenant/topic DLQ stream."""
        self._require_open()
        dlq_stream = self.dead_letter_key(tenant_id, topic)
        effective_limit = (
            min(100, self._config.max_replay_limit)
            if limit is None
            else _validate_positive_int(limit, "limit")
        )
        if effective_limit > self._config.max_replay_limit:
            raise InvalidEventError(
                f"limit {effective_limit} exceeds max_replay_limit {self._config.max_replay_limit}"
            )
        try:
            rows = await self._client.xrange(dlq_stream, min="-", max="+", count=effective_limit)
            self._healthy = True
        except Exception as exc:
            raise self._mark_redis_failure(
                exc, message="Redis Streams dead_letter_entries failed"
            ) from exc

        out: list[StreamDeadLetterRecord] = []
        for raw_id, fields in rows:
            dlq_id = _as_str(raw_id)
            orig_id = _field_str(fields, "original_entry_id", "") or ""
            event_id_val = _field_str(fields, "event_id")
            corr_val = _field_str(fields, "correlation_id")
            group_val = _field_str(fields, "group", "") or ""
            attempts_raw = _field_str(fields, "delivery_attempts", "0") or "0"
            attempts_val = int(attempts_raw)
            reason_val = _field_str(fields, "reason", "unknown") or "unknown"
            out.append(
                StreamDeadLetterRecord(
                    dlq_entry_id=dlq_id,
                    original_entry_id=orig_id,
                    event_id=event_id_val,
                    group=group_val,
                    delivery_attempts=attempts_val,
                    reason=reason_val,
                    correlation_id=corr_val,
                )
            )
        return out

    async def stream_stats(
        self,
        *,
        tenant_id: str,
        topic: str,
        group: str | None = None,
    ) -> StreamStats:
        """Inspect `XINFO STREAM` and optional `XINFO GROUPS` lag for diagnostics."""
        self._require_open()
        stream = self.stream_key(tenant_id, topic)
        canonical_group = _validate_stream_ident(group, "group") if group is not None else None
        try:
            info = await self._client.xinfo_stream(stream)
            first_e = info.get("first-entry") or info.get(b"first-entry")
            last_e = info.get("last-entry") or info.get(b"last-entry")
            first_id = _as_str(first_e[0]) if first_e else None
            last_id = _as_str(last_e[0]) if last_e else None
            raw_len = info.get("length")
            if raw_len is None:
                raw_len = info.get(b"length", 0)
            length = int(raw_len or 0)

            group_pending: int | None = None
            group_lag: int | None = None
            if canonical_group is not None:
                groups = await self._client.xinfo_groups(stream)
                for grp in groups:
                    grp_name = _field_str(grp, "name", "")
                    if grp_name == canonical_group:
                        raw_pend = grp.get("pending")
                        if raw_pend is None:
                            raw_pend = grp.get(b"pending", 0)
                        group_pending = int(raw_pend or 0)
                        raw_lag = grp.get("lag")
                        if raw_lag is None:
                            raw_lag = grp.get(b"lag")
                        group_lag = int(raw_lag) if raw_lag is not None else None
                        if group_lag is not None:
                            self._telemetry.update_consumer_lag(group_lag)
                        break

            self._healthy = True
            return StreamStats(
                length=length,
                first_entry_id=first_id,
                last_entry_id=last_id,
                group_pending=group_pending,
                group_lag=group_lag,
            )
        except Exception as exc:
            if "no such key" in str(exc).lower():
                return StreamStats(
                    length=0,
                    first_entry_id=None,
                    last_entry_id=None,
                    group_pending=0 if canonical_group is not None else None,
                    group_lag=0 if canonical_group is not None else None,
                )
            raise self._mark_redis_failure(
                exc, message="Redis Streams stream_stats failed"
            ) from exc


class StreamRecoveryWorker:
    """Cancellation-safe bounded background PEL recovery worker for a consumer group."""

    __slots__ = (
        "_store",
        "_tenant_id",
        "_topic",
        "_group",
        "_consumer",
        "_handler",
        "_task",
        "_closed",
    )

    def __init__(
        self,
        *,
        store: RedisStreamsDurableStore,
        tenant_id: str,
        topic: str,
        group: str,
        consumer: str,
        handler: Callable[[DurableEventRecord], Awaitable[None]],
    ) -> None:
        self._store = store
        self._tenant_id = validate_tenant_id(tenant_id)
        self._topic = validate_topic(topic)
        self._group = _validate_stream_ident(group, "group")
        self._consumer = _validate_stream_ident(consumer, "consumer")
        self._handler = handler
        self._task: asyncio.Task[None] | None = None
        self._closed = False

    @property
    def is_running(self) -> bool:
        return self._task is not None and not self._task.done()

    async def run_once(self, *, min_idle_ms: int | None = None) -> StreamRecoveryResult:
        """Execute one bounded recovery pass and `XACK` only records whose handler succeeds."""
        result = await self._store.reclaim_pending(
            tenant_id=self._tenant_id,
            topic=self._topic,
            group=self._group,
            consumer=self._consumer,
            min_idle_ms=min_idle_ms,
        )
        acked_ids: list[str] = []
        first_exc: BaseException | None = None
        try:
            for record in result.reclaimed:
                start_ns = time.perf_counter_ns()
                try:
                    await self._handler(record)
                except Exception as exc:
                    if first_exc is None:
                        first_exc = exc
                    continue
                self._store.telemetry.record_delivery_latency_ms(
                    (time.perf_counter_ns() - start_ns) / 1_000_000.0
                )
                if record.sequence_id is not None:
                    acked_ids.append(record.sequence_id)
        finally:
            if acked_ids:
                await self._store.ack(
                    tenant_id=self._tenant_id,
                    topic=self._topic,
                    group=self._group,
                    record_ids=acked_ids,
                )
        if first_exc is not None:
            raise first_exc
        return result

    async def _loop(self) -> None:
        interval = self._store.config.recovery_interval_seconds
        while not self._closed and not self._store.is_closed:
            try:
                await self.run_once()
            except asyncio.CancelledError:
                raise
            except Exception:
                # Telemetry already recorded in store; avoid busy retry loop
                pass
            await asyncio.sleep(interval)

    def start(self) -> None:
        """Start the background recovery loop if not already running."""
        if self._closed:
            raise EventsModuleClosedError("StreamRecoveryWorker is closed")
        if self._task is None or self._task.done():
            self._task = asyncio.create_task(self._loop(), name="gw-redis-stream-recovery")

    async def stop(self) -> None:
        """Cancel and await the background recovery task cleanly."""
        self._closed = True
        task = self._task
        self._task = None
        if task is not None:
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task


__all__ = [
    "DEFAULT_CLAIM_MIN_IDLE_MS",
    "DEFAULT_DEAD_LETTER_MAXLEN",
    "DEFAULT_MAX_DELIVERY_ATTEMPTS",
    "DEFAULT_RECOVERY_BATCH_SIZE",
    "DEFAULT_RECOVERY_INTERVAL_SECONDS",
    "DEFAULT_STREAM_MAXLEN",
    "DEFAULT_STREAM_MAX_READ_BATCH_SIZE",
    "DEFAULT_STREAM_MAX_REPLAY_LIMIT",
    "RedisStreamsConfig",
    "RedisStreamsDurableStore",
    "StreamDeadLetterRecord",
    "StreamPendingSummary",
    "StreamRecoveryResult",
    "StreamRecoveryWorker",
    "StreamStats",
]
