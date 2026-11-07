"""Verification suite for M5.1A provider-independent Voice Bridge observability.

Covers:
- Gate B: Voice-local in-memory metrics, separated latency dimensions, and error taxonomy.
- Gate C: Bounded sliding latency windows, unavailable/None semantics when sample count is 0,
  small-sample percentile threshold, and bounded retained session ring buffer.
- Gate D: Strict privacy and redaction guarantees (zero raw audio, transcripts, tokens, secrets,
  phone numbers, prompts, exception strings, or high-cardinality call_id/tenant_id dimensions).
- Gate E: Session lifecycle, concurrent call isolation, queue draining on close, and best-effort
  telemetry sink failure isolation.
- Gate F: Deterministic synthetic baseline measurement (16 kHz mono PCM16LE, 20 ms / 640-byte frames)
  for both in-process and synthetic WebSocket loopback normal and interrupted calls.
"""

from __future__ import annotations

import asyncio
import gc
import json
import socket
import weakref
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Any, Final

import pytest
import uvicorn
import websockets

from kaiwen_gateway.core.admission import AdmissionController
from kaiwen_gateway.core.config import INGRESS_MODE_SYNTHETIC, CoreConfig
from kaiwen_gateway.core.draining import InstanceDrainCoordinator
from kaiwen_gateway.modules.voice import (
    DEFAULT_LATENCY_WINDOW_SIZE,
    DEFAULT_MAX_RETAINED_SESSIONS,
    SMALL_SAMPLE_THRESHOLD,
    AudioFormat,
    AudioFrame,
    BoundedLatencyAggregator,
    FakeAIMediaAdapter,
    FakeTelephonyMediaAdapter,
    FutureTurnAudioFrameError,
    InMemoryVoiceTelemetrySink,
    InterruptibleFakeAIPort,
    InterruptibleFakeTelephonyAdapter,
    InterruptionCapabilities,
    InvalidAudioFrameError,
    LatencyDistributionSummary,
    MediaBackpressureOverflowError,
    MediaPortClosedError,
    MediaPortError,
    MetricAvailabilityReason,
    OutOfOrderAudioFrameError,
    OversizedAudioFrameError,
    UnsupportedInterruptionCapabilityError,
    VoiceAggregateMetricsSnapshot,
    VoiceBridgeSession,
    VoiceBridgeSessionClosedError,
    VoiceBridgeState,
    VoiceDomainError,
    VoiceErrorCategory,
    VoiceSessionMetricsSnapshot,
    VoiceTelemetrySink,
    VoiceTerminalCleanupState,
    classify_voice_error,
    create_synthetic_pcm16le_frame,
)
from kaiwen_gateway.server import create_gateway_app
from kaiwen_gateway.transports.synthetic import (
    EVENT_CALL_END,
    EVENT_CALL_ENDED,
    EVENT_CALL_INTERRUPT,
    EVENT_CALL_INTERRUPTED,
    EVENT_CALL_START,
    EVENT_CALL_STARTED,
    EVENT_SESSION_READY,
    PCM16LE_FRAME_BYTES,
    SYNTHETIC_MEDIA_WS_ENDPOINT_PATH,
    SyntheticAuthBridge,
    SyntheticTokenVerifier,
)

TEST_SECRET_KEY: Final[bytes] = b"m5_observability_secret_key_32b!"
SYNTHETIC_PCM_20MS: Final[bytes] = b"\x55" * PCM16LE_FRAME_BYTES


def _make_frame(sequence: int = 0, turn_id: int | None = None) -> AudioFrame:
    return create_synthetic_pcm16le_frame(
        SYNTHETIC_PCM_20MS,
        sequence=sequence,
        timestamp_ms=float(sequence * 20),
        turn_id=turn_id,
    )


def _make_telephony_port(
    frames: list[AudioFrame], *, push_eof: bool = True
) -> FakeTelephonyMediaAdapter:
    port = FakeTelephonyMediaAdapter()
    for frame in frames:
        port.push_frame(frame)
    if push_eof:
        port.push_eof()
    return port


def _make_ai_port(frames: list[AudioFrame], *, push_eof: bool = True) -> FakeAIMediaAdapter:
    port = FakeAIMediaAdapter()
    for frame in frames:
        port.push_frame(frame)
    if push_eof:
        port.push_eof()
    return port


def _get_free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


@asynccontextmanager
async def _run_test_server(app: Any, port: int) -> AsyncIterator[str]:
    config = uvicorn.Config(
        app=app,
        host="127.0.0.1",
        port=port,
        log_level="warning",
        ws="auto",
    )
    server = uvicorn.Server(config)
    server_task = asyncio.create_task(server.serve())
    for _ in range(100):
        if server.started:
            break
        await asyncio.sleep(0.02)
    assert server.started, "Uvicorn server failed to start on loopback"
    try:
        yield f"ws://127.0.0.1:{port}"
    finally:
        server.should_exit = True
        try:
            await asyncio.wait_for(server_task, timeout=5.0)
        except (TimeoutError, asyncio.CancelledError):
            pass


def _create_server_app(
    telemetry_sink: VoiceTelemetrySink | None = None,
) -> tuple[Any, SyntheticTokenVerifier, SyntheticAuthBridge, InstanceDrainCoordinator]:
    verifier = SyntheticTokenVerifier(secret_key=TEST_SECRET_KEY, enable_replay_protection=True)
    auth_bridge = SyntheticAuthBridge(verifier=verifier, enforce_call_exclusivity=True)
    admission = AdmissionController(
        CoreConfig(ingress_mode=INGRESS_MODE_SYNTHETIC, max_concurrent_connections=10)
    )
    coordinator = InstanceDrainCoordinator(controller=admission)

    app = create_gateway_app(
        config=CoreConfig(ingress_mode=INGRESS_MODE_SYNTHETIC),
        synthetic_auth_bridge=auth_bridge,
        enable_synthetic_media=True,
    )
    app.state.drain_coordinator = coordinator
    if telemetry_sink is not None:
        app.state.voice_telemetry_sink = telemetry_sink
    return app, verifier, auth_bridge, coordinator


# =============================================================================
# GATE B & C: Error Classification, Bounded Latency Aggregator & Unavailable Semantics
# =============================================================================


def test_classify_voice_error_taxonomy_complete() -> None:
    """Verify every voice domain/runtime exception maps deterministically to a bounded category."""
    assert (
        classify_voice_error(OversizedAudioFrameError("too big"))
        == VoiceErrorCategory.INVALID_FRAME
    )
    assert (
        classify_voice_error(OutOfOrderAudioFrameError("bad seq"))
        == VoiceErrorCategory.OUT_OF_ORDER_FRAME
    )
    assert (
        classify_voice_error(FutureTurnAudioFrameError("future"))
        == VoiceErrorCategory.FUTURE_TURN_FRAME
    )
    assert (
        classify_voice_error(InvalidAudioFrameError("invalid")) == VoiceErrorCategory.INVALID_FRAME
    )
    assert (
        classify_voice_error(MediaBackpressureOverflowError("overflow"))
        == VoiceErrorCategory.BACKPRESSURE_OVERFLOW
    )
    assert (
        classify_voice_error(UnsupportedInterruptionCapabilityError("unsupported"))
        == VoiceErrorCategory.UNSUPPORTED_CAPABILITY
    )
    assert classify_voice_error(MediaPortClosedError("closed")) == VoiceErrorCategory.PORT_CLOSED
    assert classify_voice_error(MediaPortError("port err")) == VoiceErrorCategory.PORT_IO_ERROR
    assert (
        classify_voice_error(VoiceBridgeSessionClosedError("session closed"))
        == VoiceErrorCategory.INVALID_FRAME
    )
    assert (
        classify_voice_error(TimeoutError("timed out")) == VoiceErrorCategory.INTERRUPTION_TIMEOUT
    )
    assert (
        classify_voice_error(TimeoutError("intr timeout"), during_interruption=True)
        == VoiceErrorCategory.INTERRUPTION_TIMEOUT
    )
    assert (
        classify_voice_error(VoiceDomainError("intr domain"), during_interruption=True)
        == VoiceErrorCategory.INVALID_FRAME
    )
    assert (
        classify_voice_error(RuntimeError("intr boom"), during_interruption=True)
        == VoiceErrorCategory.INTERRUPTION_FAILED
    )
    assert (
        classify_voice_error(RuntimeError("cleanup boom"), during_cleanup=True)
        == VoiceErrorCategory.CLEANUP_ERROR
    )
    assert classify_voice_error(asyncio.CancelledError()) == VoiceErrorCategory.SESSION_CANCELLED
    assert classify_voice_error(VoiceDomainError("domain")) == VoiceErrorCategory.INVALID_FRAME
    assert classify_voice_error(RuntimeError("unexpected")) == VoiceErrorCategory.INTERNAL_ERROR


def test_latency_distribution_unavailable_when_zero_samples_never_faked_as_zero() -> None:
    """Gate C: Zero-sample latency summary must report available=False and None fields, never 0.0."""
    agg = BoundedLatencyAggregator(window_capacity=16, min_percentile_samples=5)
    assert agg.sample_count == 0
    assert agg.retained_sample_count == 0
    assert agg.window_capacity == 16
    assert agg.min_percentile_samples == 5

    summary = agg.snapshot()
    assert summary.available is False
    assert summary.availability_reason == MetricAvailabilityReason.NO_SAMPLES
    assert summary.sample_count == 0
    assert summary.retained_sample_count == 0
    assert summary.window_capacity == 16
    assert summary.is_small_sample is False
    assert summary.min_ms is None
    assert summary.max_ms is None
    assert summary.mean_ms is None
    assert summary.p50_ms is None
    assert summary.p95_ms is None
    assert summary.p99_ms is None

    as_dict = summary.to_dict()
    assert as_dict["available"] is False
    assert as_dict["availability_reason"] == "no_samples"
    assert as_dict["p50_ms"] is None
    assert as_dict["p95_ms"] is None
    assert as_dict["p99_ms"] is None
    assert as_dict["min_ms"] is None
    assert as_dict["max_ms"] is None
    assert as_dict["mean_ms"] is None


def test_latency_distribution_explicit_unavailable_reasons() -> None:
    """Gate B: Unmeasured provider AI and remote playback latencies carry explicit unknown reasons."""
    ai_summary = LatencyDistributionSummary.unavailable(
        MetricAvailabilityReason.UNKNOWN_WITHOUT_PROVIDER_INSTRUMENTATION,
        window_capacity=64,
    )
    assert ai_summary.available is False
    assert (
        ai_summary.availability_reason
        == MetricAvailabilityReason.UNKNOWN_WITHOUT_PROVIDER_INSTRUMENTATION
    )
    assert ai_summary.p50_ms is None
    assert ai_summary.p95_ms is None
    assert ai_summary.p99_ms is None

    playback_summary = LatencyDistributionSummary.unavailable(
        MetricAvailabilityReason.UNKNOWN_WITHOUT_PLAYBACK_ACKNOWLEDGMENT,
        window_capacity=64,
    )
    assert playback_summary.available is False
    assert (
        playback_summary.availability_reason
        == MetricAvailabilityReason.UNKNOWN_WITHOUT_PLAYBACK_ACKNOWLEDGMENT
    )
    assert playback_summary.min_ms is None


def test_latency_aggregator_small_sample_threshold_and_percentiles() -> None:
    """Gate C: Small sample threshold suppresses noisy percentiles when configured > 1."""
    agg = BoundedLatencyAggregator(window_capacity=32, min_percentile_samples=5)

    # Record 3 samples (< min_percentile_samples=5)
    agg.record(1.0)
    agg.record(2.0)
    agg.record(3.0)

    small_snap = agg.snapshot()
    assert small_snap.available is True
    assert (
        small_snap.availability_reason
        == MetricAvailabilityReason.INSUFFICIENT_SAMPLES_FOR_PERCENTILES
    )
    assert small_snap.sample_count == 3
    assert small_snap.retained_sample_count == 3
    assert small_snap.is_small_sample is True
    assert small_snap.min_ms == pytest.approx(1.0)
    assert small_snap.max_ms == pytest.approx(3.0)
    assert small_snap.mean_ms == pytest.approx(2.0)
    assert small_snap.p50_ms is None
    assert small_snap.p95_ms is None
    assert small_snap.p99_ms is None

    # Record 2 more samples to reach min_percentile_samples=5
    agg.record(4.0)
    agg.record(5.0)
    full_snap = agg.snapshot()
    assert full_snap.available is True
    assert full_snap.availability_reason == MetricAvailabilityReason.MEASURED
    assert full_snap.sample_count == 5
    assert full_snap.is_small_sample is True  # 5 < SMALL_SAMPLE_THRESHOLD (20)
    assert full_snap.p50_ms == pytest.approx(3.0)
    assert full_snap.p95_ms is not None
    assert full_snap.p99_ms is not None
    assert full_snap.min_ms == pytest.approx(1.0)
    assert full_snap.max_ms == pytest.approx(5.0)

    # Record 15 more samples to reach SMALL_SAMPLE_THRESHOLD (20)
    for val in range(6, 21):
        agg.record(float(val))
    large_snap = agg.snapshot()
    assert large_snap.retained_sample_count == 20
    assert large_snap.is_small_sample is False


def test_latency_aggregator_single_sample_and_validation_guards() -> None:
    """Verify single-sample interpolation and invalid parameter rejection."""
    with pytest.raises(VoiceDomainError, match="window_capacity"):
        BoundedLatencyAggregator(window_capacity=0)
    with pytest.raises(VoiceDomainError, match="window_capacity"):
        BoundedLatencyAggregator(window_capacity=True)
    with pytest.raises(VoiceDomainError, match="min_percentile_samples"):
        BoundedLatencyAggregator(window_capacity=8, min_percentile_samples=0)
    with pytest.raises(VoiceDomainError, match="min_percentile_samples"):
        BoundedLatencyAggregator(window_capacity=8, min_percentile_samples=True)

    agg = BoundedLatencyAggregator(window_capacity=8, min_percentile_samples=1)
    with pytest.raises(VoiceDomainError, match="duration_ms"):
        agg.record(True)
    with pytest.raises(VoiceDomainError, match="duration_ms"):
        agg.record("bad")  # type: ignore[arg-type]
    with pytest.raises(VoiceDomainError, match="duration_ms"):
        agg.record(-5.0)
    with pytest.raises(VoiceDomainError, match="duration_ms"):
        agg.record(float("nan"))
    with pytest.raises(VoiceDomainError, match="duration_ms"):
        agg.record(float("inf"))
    assert agg.sample_count == 0

    agg.record(4.25)
    snap = agg.snapshot()
    assert snap.available is True
    assert snap.sample_count == 1
    assert snap.is_small_sample is True
    assert snap.p50_ms == pytest.approx(4.25)
    assert snap.p95_ms == pytest.approx(4.25)
    assert snap.p99_ms == pytest.approx(4.25)

    # Record decreasing sample to exercise _min_ms update branch
    agg.record(1.25)
    snap2 = agg.snapshot()
    assert snap2.min_ms == pytest.approx(1.25)
    assert snap2.max_ms == pytest.approx(4.25)


def test_bounded_latency_window_does_not_grow_unbounded() -> None:
    """Gate C: BoundedLatencyAggregator retains at most window_capacity samples in memory."""
    capacity = 32
    agg = BoundedLatencyAggregator(window_capacity=capacity, min_percentile_samples=1)
    for i in range(2_000):
        agg.record(float(i))

    snap = agg.snapshot()
    assert snap.sample_count == 2_000
    assert snap.retained_sample_count == capacity
    assert snap.window_capacity == capacity
    assert snap.min_ms == pytest.approx(0.0)
    assert snap.max_ms == pytest.approx(1999.0)
    # Percentiles are computed over the most recent 32 samples (1968..1999)
    assert snap.p50_ms == pytest.approx(1968.0 + 0.50 * 31.0)


def test_in_memory_telemetry_sink_validation_and_bounded_ring_buffer() -> None:
    """Gate C: InMemoryVoiceTelemetrySink enforces bounded retained session ring buffer."""
    with pytest.raises(VoiceDomainError, match="max_retained_sessions"):
        InMemoryVoiceTelemetrySink(max_retained_sessions=0)
    with pytest.raises(VoiceDomainError, match="max_retained_sessions"):
        InMemoryVoiceTelemetrySink(max_retained_sessions=True)
    with pytest.raises(VoiceDomainError, match="window_capacity"):
        InMemoryVoiceTelemetrySink(latency_window_capacity=0)
    with pytest.raises(VoiceDomainError, match="min_percentile_samples"):
        InMemoryVoiceTelemetrySink(min_percentile_samples=0)

    assert DEFAULT_LATENCY_WINDOW_SIZE == 256
    assert DEFAULT_MAX_RETAINED_SESSIONS == 64
    assert SMALL_SAMPLE_THRESHOLD == 20

    sink = InMemoryVoiceTelemetrySink(
        max_retained_sessions=8,
        latency_window_capacity=16,
        min_percentile_samples=1,
    )
    assert sink.max_retained_sessions == 8
    assert sink.active_sessions == 0

    async def _run_many_sessions() -> None:
        for idx in range(50):
            f = _make_frame(sequence=0)
            tel = _make_telephony_port([f])
            ai = _make_ai_port([f])
            session = VoiceBridgeSession(
                call_id=f"call_{idx}",
                tenant_id="tenant_ring",
                telephony_port=tel,
                ai_port=ai,
                telemetry_sink=sink,
                latency_window_capacity=16,
            )
            await session.start()
            await session.wait_until_closed()

    asyncio.run(_run_many_sessions())

    agg_snap = sink.snapshot()
    assert isinstance(agg_snap, VoiceAggregateMetricsSnapshot)
    assert agg_snap.active_sessions == 0
    assert agg_snap.peak_active_sessions == 1
    assert agg_snap.total_sessions_started == 50
    assert agg_snap.total_sessions_closed == 50
    assert agg_snap.total_sessions_completed_clean == 50
    assert agg_snap.total_sessions_failed == 0
    assert agg_snap.max_retained_sessions == 8
    assert agg_snap.retained_session_count == 8
    assert len(agg_snap.recent_sessions) == 8
    assert agg_snap.total_inbound_frames_received == 50
    assert agg_snap.total_outbound_frames_sent == 50
    assert agg_snap.session_duration_ms.sample_count == 50
    assert agg_snap.session_duration_ms.retained_sample_count == 16
    assert agg_snap.cleanup_duration_ms.sample_count == 50
    assert agg_snap.cleanup_duration_ms.retained_sample_count == 16


# =============================================================================
# GATE D: Security, Privacy & Redaction Invariants
# =============================================================================


def test_privacy_redaction_no_sensitive_material_in_snapshots() -> None:
    """Gate D: Sensitive call_id, tenant_id, audio bytes, tokens, phone numbers, and error strings never leak."""
    secret_token = "sk-live-SUPER-SECRET-TOKEN-99999"
    secret_phone = "+905551234567"
    secret_prompt = "SYSTEM_PROMPT_CONFIDENTIAL_INSTRUCTION"
    secret_transcript = "Hello my credit card number is 4111-2222-3333-4444"
    secret_call_id = f"call-{secret_phone}-{secret_token}"
    secret_tenant_id = f"tenant-{secret_prompt}"
    secret_audio_marker = b"RAW_AUDIO_SECRET_PCM_BYTES_DO_NOT_LOG" * 20
    secret_audio_payload = secret_audio_marker[:640]

    sink = InMemoryVoiceTelemetrySink(max_retained_sessions=8)

    class FailingSensitiveAIPort(FakeAIMediaAdapter):
        async def send_frame(self, frame: AudioFrame) -> None:
            raise MediaPortError(
                f"Provider failed for {secret_phone} with token={secret_token} "
                f"transcript={secret_transcript} prompt={secret_prompt}"
            )

    async def _run() -> VoiceSessionMetricsSnapshot:
        frame = AudioFrame(
            payload=secret_audio_payload,
            format=AudioFormat.PCM16LE,
            sample_rate=16000,
            channels=1,
            sequence=0,
            timestamp_ms=0.0,
        )
        tel = _make_telephony_port([frame])
        ai = FailingSensitiveAIPort()
        ai.push_eof()
        session = VoiceBridgeSession(
            call_id=secret_call_id,
            tenant_id=secret_tenant_id,
            telephony_port=tel,
            ai_port=ai,
            telemetry_sink=sink,
        )
        await session.start()
        await session.wait_until_closed(propagate_error=False)
        return session.metrics_snapshot()

    session_snap = asyncio.run(_run())
    agg_snap = sink.snapshot()

    # Check both structured dict representations and string/repr forms
    serialized_blobs = [
        json.dumps(session_snap.to_dict()),
        repr(session_snap),
        str(session_snap),
        json.dumps(agg_snap.to_dict()),
        repr(agg_snap),
        str(agg_snap),
    ]

    forbidden_substrings = [
        secret_token,
        secret_phone,
        secret_prompt,
        secret_transcript,
        secret_call_id,
        secret_tenant_id,
        "RAW_AUDIO_SECRET_PCM_BYTES_DO_NOT_LOG",
        "4111-2222-3333-4444",
    ]
    for blob in serialized_blobs:
        for forbidden in forbidden_substrings:
            assert forbidden not in blob, f"Sensitive material leaked into telemetry: {forbidden}"

    # Verify error category was recorded by bounded enum key only
    assert session_snap.terminal_error_category == VoiceErrorCategory.PORT_IO_ERROR
    assert session_snap.error_count(VoiceErrorCategory.PORT_IO_ERROR) == 1
    assert session_snap.total_errors == 1
    assert agg_snap.error_counts[VoiceErrorCategory.PORT_IO_ERROR] == 1


# =============================================================================
# GATE E: Lifecycle, Failure Scenarios, Concurrency & Sink Failure Isolation
# =============================================================================


def test_normal_single_call_completion_metrics() -> None:
    """Scenario 1: Normal single-call completion records exact counts and clean terminal state."""
    sink = InMemoryVoiceTelemetrySink()

    async def _run() -> VoiceSessionMetricsSnapshot:
        inbound = [_make_frame(sequence=i) for i in range(5)]
        outbound = [_make_frame(sequence=i) for i in range(4)]
        tel = _make_telephony_port(inbound)
        ai = _make_ai_port(outbound)
        session = VoiceBridgeSession(
            call_id="call_norm_1",
            tenant_id="tenant_1",
            telephony_port=tel,
            ai_port=ai,
            telemetry_sink=sink,
            min_percentile_samples=1,
        )
        # Snapshot before start
        pre_snap = session.metrics_snapshot()
        assert pre_snap.session_state == VoiceBridgeState.IDLE.value
        assert pre_snap.cleanup_state == VoiceTerminalCleanupState.NOT_STARTED
        assert pre_snap.session_duration_ms is None
        assert pre_snap.cleanup_duration_ms is None
        assert pre_snap.inbound_processing_ms.available is False

        await session.start()
        # Snapshot while running exercises live duration_ms branch
        running_snap = session.metrics_snapshot()
        assert running_snap.session_duration_ms is not None
        await session.wait_until_closed()
        assert session.cleanup_state == VoiceTerminalCleanupState.COMPLETED_CLEAN
        assert session.inbound_queue_high_water_mark >= 1
        assert session.outbound_queue_high_water_mark >= 1
        assert session.inbound_frames_received == 5
        assert session.inbound_bytes_received == 5 * 640
        assert session.inbound_bytes_transferred == 5 * 640
        assert session.outbound_frames_received == 4
        assert session.outbound_bytes_received == 4 * 640
        assert session.outbound_bytes_sent == 4 * 640
        assert session.interruptions_requested == 0
        assert session.interruptions_noop == 0
        assert session.interruptions_failed == 0
        assert session.telemetry_sink_errors == 0
        return session.metrics_snapshot()

    snap = asyncio.run(_run())
    assert snap.session_state == VoiceBridgeState.CLOSED.value
    assert snap.cleanup_state == VoiceTerminalCleanupState.COMPLETED_CLEAN
    assert snap.inbound_frames_received == 5
    assert snap.inbound_bytes_received == 5 * 640
    assert snap.inbound_frames_transferred == 5
    assert snap.inbound_bytes_transferred == 5 * 640
    assert snap.outbound_frames_received == 4
    assert snap.outbound_bytes_received == 4 * 640
    assert snap.outbound_frames_sent == 4
    assert snap.outbound_bytes_sent == 4 * 640
    assert snap.dropped_frames_total == 0
    assert snap.locally_discarded_frames == 0
    assert snap.unforwarded_inbound_frames == 0
    assert snap.unforwarded_outbound_frames == 0
    assert snap.stale_frames_suppressed == 0
    assert snap.inbound_queue_depth == 0
    assert snap.outbound_queue_depth == 0
    assert snap.inbound_queue_high_water_mark >= 1
    assert snap.outbound_queue_high_water_mark >= 1
    assert snap.session_duration_ms is not None and snap.session_duration_ms >= 0.0
    assert snap.cleanup_duration_ms is not None and snap.cleanup_duration_ms >= 0.0
    assert snap.inbound_processing_ms.available is True
    assert snap.inbound_processing_ms.sample_count == 5
    assert snap.outbound_processing_ms.available is True
    assert snap.outbound_processing_ms.sample_count == 4
    assert snap.inbound_queue_wait_ms.available is True
    assert snap.outbound_queue_wait_ms.available is True
    # In-process FakeTelephonyMediaAdapter has no network socket write duration
    assert snap.network_transport_ms.available is False
    assert snap.network_transport_ms.availability_reason == MetricAvailabilityReason.NO_SAMPLES
    assert snap.upstream_ai_generation_ms.available is False
    assert snap.remote_playback_latency_ms.available is False


def test_forced_disconnect_drains_buffered_frames_and_counts_unforwarded() -> None:
    """Scenario 2: Forced close while queues hold buffered frames counts unforwarded drops and releases frames."""
    sink = InMemoryVoiceTelemetrySink()

    class BlockingSendTelephonyPort(FakeTelephonyMediaAdapter):
        def __init__(self) -> None:
            super().__init__()
            self.send_started = asyncio.Event()
            self.block_forever = asyncio.Event()

        async def receive_frame(self) -> AudioFrame | None:
            await self.block_forever.wait()
            return None

        async def send_frame(self, frame: AudioFrame) -> None:
            self.send_started.set()
            await self.block_forever.wait()

    async def _run() -> tuple[VoiceSessionMetricsSnapshot, VoiceBridgeSession]:
        tel = BlockingSendTelephonyPort()
        outbound_frames = [_make_frame(sequence=i) for i in range(5)]
        ai = _make_ai_port(outbound_frames)
        session = VoiceBridgeSession(
            call_id="call_forced_close",
            tenant_id="tenant_1",
            telephony_port=tel,
            ai_port=ai,
            queue_capacity=8,
            telemetry_sink=sink,
        )
        await session.start()
        await asyncio.wait_for(tel.send_started.wait(), timeout=2.0)
        # Allow producer to fill remaining 4 frames into _outbound_queue
        await asyncio.sleep(0.03)
        assert session.outbound_queue_depth >= 1

        await session.close()
        snap = session.metrics_snapshot()
        outbound_frames.clear()
        return snap, session

    snap, closed_session = asyncio.run(_run())
    assert closed_session._inbound_queue.empty()
    assert closed_session._outbound_queue.empty()
    assert len(closed_session._inbound_timing_queue) == 0
    assert len(closed_session._outbound_timing_queue) == 0
    assert snap.outbound_queue_depth == 0
    assert snap.inbound_queue_depth == 0
    assert snap.unforwarded_outbound_frames == 5
    assert snap.dropped_frames_total == 5
    assert snap.cleanup_state == VoiceTerminalCleanupState.COMPLETED_CLEAN


def test_ai_port_failure_and_cleanup_failure_states() -> None:
    """Scenario 3: AI port failure and port close failure update error taxonomy and cleanup_state."""
    sink = InMemoryVoiceTelemetrySink()

    class FailingCloseAIPort(FakeAIMediaAdapter):
        async def send_frame(self, frame: AudioFrame) -> None:
            raise MediaPortError("AI send failed")

        async def close(self) -> None:
            await super().close()
            raise MediaPortError("AI close failed")

    async def _run() -> VoiceSessionMetricsSnapshot:
        tel = _make_telephony_port([_make_frame(sequence=0)])
        ai = FailingCloseAIPort()
        ai.push_eof()
        session = VoiceBridgeSession(
            call_id="call_fail_cleanup",
            tenant_id="tenant_1",
            telephony_port=tel,
            ai_port=ai,
            telemetry_sink=sink,
        )
        await session.start()
        with pytest.raises(MediaPortError):
            await session.wait_until_closed(propagate_error=True)
        return session.metrics_snapshot()

    snap = asyncio.run(_run())
    assert snap.session_state == VoiceBridgeState.FAILED.value
    assert snap.cleanup_state == VoiceTerminalCleanupState.COMPLETED_WITH_ERRORS
    assert snap.terminal_error_category == VoiceErrorCategory.PORT_IO_ERROR
    assert snap.error_count(VoiceErrorCategory.PORT_IO_ERROR) >= 1
    assert snap.error_count(VoiceErrorCategory.CLEANUP_ERROR) >= 1

    agg = sink.snapshot()
    assert agg.total_sessions_failed == 1
    assert agg.error_counts[VoiceErrorCategory.CLEANUP_ERROR] == 1


def test_backpressure_and_queue_saturation_metrics() -> None:
    """Scenario 4: Queue saturation records high-water mark and BACKPRESSURE_OVERFLOW category."""
    sink = InMemoryVoiceTelemetrySink()

    class SlowAIPort(FakeAIMediaAdapter):
        async def send_frame(self, frame: AudioFrame) -> None:
            await asyncio.sleep(0.5)

        async def receive_frame(self) -> AudioFrame | None:
            await asyncio.sleep(0.5)
            return None

    async def _run() -> VoiceSessionMetricsSnapshot:
        inbound = [_make_frame(sequence=i) for i in range(6)]
        tel = _make_telephony_port(inbound)
        ai = SlowAIPort()
        session = VoiceBridgeSession(
            call_id="call_bp",
            tenant_id="tenant_1",
            telephony_port=tel,
            ai_port=ai,
            queue_capacity=2,
            backpressure_timeout=0.03,
            telemetry_sink=sink,
        )
        await session.start()
        await session.wait_until_closed(propagate_error=False)
        return session.metrics_snapshot()

    snap = asyncio.run(_run())
    assert snap.session_state == VoiceBridgeState.FAILED.value
    assert snap.inbound_queue_high_water_mark == 2
    assert snap.terminal_error_category == VoiceErrorCategory.BACKPRESSURE_OVERFLOW
    assert snap.error_count(VoiceErrorCategory.BACKPRESSURE_OVERFLOW) == 1
    assert snap.inbound_queue_depth == 0


def test_interruption_during_active_audio_metrics() -> None:
    """Scenario 5: Interruption tracks requested, completed, noop, failed, discarded, and stale frames."""
    sink = InMemoryVoiceTelemetrySink()

    async def _run() -> VoiceSessionMetricsSnapshot:
        tel = InterruptibleFakeTelephonyAdapter(
            capabilities=InterruptionCapabilities(
                can_discard_local_output=True,
                can_cancel_transport_pending=True,
                can_clear_provider_playback=True,
            ),
        )
        ai = InterruptibleFakeAIPort(
            capabilities=InterruptionCapabilities(
                can_discard_local_output=True,
                can_cancel_future_generation=True,
            ),
        )
        session = VoiceBridgeSession(
            call_id="call_intr_metrics",
            tenant_id="tenant_1",
            telephony_port=tel,
            ai_port=ai,
            queue_capacity=8,
            telemetry_sink=sink,
        )
        await session.start()

        # Enqueue 2 turn-1 frames directly into outbound queue
        f1 = _make_frame(sequence=0, turn_id=1)
        f2 = _make_frame(sequence=1, turn_id=1)
        session._outbound_queue.put_nowait(f1)
        session._outbound_queue.put_nowait(f2)

        # 1. Active turn interruption
        outcome1 = await session.interrupt(turn_id=1)
        assert outcome1.interrupted_turn_id == 1
        assert outcome1.new_turn_id == 2

        # 2. Stale/duplicate turn interruption (noop)
        outcome_noop = await session.interrupt(turn_id=1)
        assert outcome_noop.new_turn_id == 2
        assert outcome_noop.locally_discarded_frames == 0

        # 3. Future turn interruption (rejected -> failed counter)
        with pytest.raises(VoiceDomainError, match="Cannot interrupt future turn 99"):
            await session.interrupt(turn_id=99)

        # 4. Feed a stale turn-1 frame from AI port and a valid turn-2 frame
        ai.push_frame(_make_frame(sequence=2, turn_id=1))
        ai.push_frame(_make_frame(sequence=3, turn_id=2))
        ai.push_eof()
        tel.push_eof()

        await session.wait_until_closed()
        return session.metrics_snapshot()

    snap = asyncio.run(_run())
    assert snap.interruptions_requested == 3
    assert snap.interruptions_completed == 1
    assert snap.interruptions_noop == 1
    assert snap.interruptions_failed == 1
    assert snap.stale_frames_suppressed >= 1
    assert (
        snap.dropped_frames_total
        == snap.locally_discarded_frames
        + snap.unforwarded_inbound_frames
        + snap.unforwarded_outbound_frames
    )
    assert snap.error_count(VoiceErrorCategory.FUTURE_TURN_FRAME) == 1

    agg = sink.snapshot()
    assert agg.total_interruptions_requested == 3
    assert agg.total_interruptions_completed == 1
    assert agg.total_interruptions_noop == 1
    assert agg.total_interruptions_failed == 1


def test_two_independent_simultaneous_calls_isolation_and_peak_concurrency() -> None:
    """Scenario 6: Two simultaneous calls maintain isolated per-call snapshots and peak_active_sessions == 2."""
    sink = InMemoryVoiceTelemetrySink()

    class CoordinatedTelephonyPort(FakeTelephonyMediaAdapter):
        def __init__(self, frames: list[AudioFrame], barrier: asyncio.Event) -> None:
            super().__init__()
            for frame in frames:
                self.push_frame(frame)
            self.push_eof()
            self._barrier = barrier

        async def receive_frame(self) -> AudioFrame | None:
            await self._barrier.wait()
            return await super().receive_frame()

    async def _run() -> tuple[VoiceSessionMetricsSnapshot, VoiceSessionMetricsSnapshot]:
        barrier = asyncio.Event()
        frames_a = [_make_frame(sequence=i) for i in range(3)]
        frames_b = [_make_frame(sequence=i) for i in range(7)]

        session_a = VoiceBridgeSession(
            call_id="call_simul_a",
            tenant_id="tenant_a",
            telephony_port=CoordinatedTelephonyPort(frames_a, barrier),
            ai_port=_make_ai_port(frames_a),
            telemetry_sink=sink,
        )
        session_b = VoiceBridgeSession(
            call_id="call_simul_b",
            tenant_id="tenant_b",
            telephony_port=CoordinatedTelephonyPort(frames_b, barrier),
            ai_port=_make_ai_port(frames_b),
            telemetry_sink=sink,
        )

        await session_a.start()
        await session_b.start()
        assert sink.snapshot().active_sessions == 2
        assert sink.snapshot().peak_active_sessions == 2

        barrier.set()
        await asyncio.gather(
            session_a.wait_until_closed(),
            session_b.wait_until_closed(),
        )
        return session_a.metrics_snapshot(), session_b.metrics_snapshot()

    snap_a, snap_b = asyncio.run(_run())
    assert snap_a.inbound_frames_transferred == 3
    assert snap_b.inbound_frames_transferred == 7

    agg = sink.snapshot()
    assert agg.active_sessions == 0
    assert agg.peak_active_sessions == 2
    assert agg.total_sessions_started == 2
    assert agg.total_sessions_closed == 2
    assert agg.total_inbound_frames_transferred == 10


def test_failing_telemetry_sink_does_not_corrupt_active_call_or_cleanup() -> None:
    """Gate E: A failing telemetry sink is isolated best-effort and never breaks call streaming or cleanup."""

    class ExplodingTelemetrySink:
        def on_session_started(self) -> None:
            raise RuntimeError("sink startup boom")

        def on_session_closed(self, snapshot: VoiceSessionMetricsSnapshot) -> None:
            raise RuntimeError("sink close boom")

    async def _run() -> VoiceSessionMetricsSnapshot:
        frames = [_make_frame(sequence=i) for i in range(3)]
        tel = _make_telephony_port(frames)
        ai = _make_ai_port(frames)
        session = VoiceBridgeSession(
            call_id="call_sink_boom",
            tenant_id="tenant_1",
            telephony_port=tel,
            ai_port=ai,
            telemetry_sink=ExplodingTelemetrySink(),
        )
        await session.start()
        await session.wait_until_closed()
        # Repeated close() must remain idempotent and not call sink again
        await session.close()
        return session.metrics_snapshot()

    snap = asyncio.run(_run())
    assert snap.session_state == VoiceBridgeState.CLOSED.value
    assert snap.cleanup_state == VoiceTerminalCleanupState.COMPLETED_CLEAN
    assert snap.inbound_frames_transferred == 3
    assert snap.outbound_frames_sent == 3
    assert snap.telemetry_sink_errors == 2


def test_voice_bridge_invalid_telemetry_parameters_rejected() -> None:
    """Verify VoiceBridgeSession validates latency_window_capacity and min_percentile_samples."""
    tel = FakeTelephonyMediaAdapter()
    ai = FakeAIMediaAdapter()
    with pytest.raises(VoiceDomainError, match="window_capacity"):
        VoiceBridgeSession(
            call_id="c1",
            tenant_id="t1",
            telephony_port=tel,
            ai_port=ai,
            latency_window_capacity=0,
        )
    with pytest.raises(VoiceDomainError, match="min_percentile_samples"):
        VoiceBridgeSession(
            call_id="c1",
            tenant_id="t1",
            telephony_port=tel,
            ai_port=ai,
            min_percentile_samples=0,
        )
    with pytest.raises(VoiceDomainError, match="telemetry_sink"):
        VoiceBridgeSession(
            call_id="c1",
            tenant_id="t1",
            telephony_port=tel,
            ai_port=ai,
            telemetry_sink="invalid_sink",  # type: ignore[arg-type]
        )

    # Also verify closing an unstarted session notifies sink cleanly with session_duration_ms=None
    sink = InMemoryVoiceTelemetrySink()

    async def _close_unstarted() -> None:
        unstarted = VoiceBridgeSession(
            call_id="c_unstarted",
            tenant_id="t1",
            telephony_port=FakeTelephonyMediaAdapter(),
            ai_port=FakeAIMediaAdapter(),
            telemetry_sink=sink,
        )
        await unstarted.close()
        assert unstarted.metrics_snapshot().session_duration_ms is None

    asyncio.run(_close_unstarted())
    assert sink.snapshot().total_sessions_closed == 1
    assert sink.snapshot().active_sessions == 0


def test_sink_does_not_retain_closed_session_references() -> None:
    """Gate E: InMemoryVoiceTelemetrySink retains only immutable value snapshots, never VoiceBridgeSession."""
    sink = InMemoryVoiceTelemetrySink(max_retained_sessions=16)

    async def _run() -> weakref.ReferenceType[VoiceBridgeSession]:
        f = _make_frame(sequence=0)
        session = VoiceBridgeSession(
            call_id="call_gc",
            tenant_id="tenant_gc",
            telephony_port=_make_telephony_port([f]),
            ai_port=_make_ai_port([f]),
            telemetry_sink=sink,
        )
        ref = weakref.ref(session)
        await session.start()
        await session.wait_until_closed()
        return ref

    session_ref = asyncio.run(_run())
    gc.collect()
    assert session_ref() is None
    assert sink.snapshot().retained_session_count == 1


# =============================================================================
# GATE F: Reproducible Synthetic Baseline Measurement (In-Process & WebSocket)
# =============================================================================


def test_reproducible_baseline_in_process_and_websocket_normal_and_interrupted() -> None:
    """Gate F: Deterministic synthetic baseline measurement with 16 kHz mono PCM16LE 20 ms (640B) frames."""
    ws_sink = InMemoryVoiceTelemetrySink(
        max_retained_sessions=16,
        latency_window_capacity=256,
        min_percentile_samples=1,
    )

    async def _run_baseline() -> tuple[
        VoiceSessionMetricsSnapshot,
        VoiceSessionMetricsSnapshot,
        VoiceSessionMetricsSnapshot,
        VoiceAggregateMetricsSnapshot,
    ]:
        # 1. In-process normal 25-frame call (500 ms synthetic audio)
        inproc_frames = [_make_frame(sequence=i, turn_id=1) for i in range(25)]
        inproc_session = VoiceBridgeSession(
            call_id="baseline_inproc_normal",
            tenant_id="tenant_baseline",
            telephony_port=_make_telephony_port(inproc_frames),
            ai_port=_make_ai_port(inproc_frames),
            min_percentile_samples=1,
        )
        await inproc_session.start()
        await inproc_session.wait_until_closed()
        inproc_normal_snap = inproc_session.metrics_snapshot()

        # 2. Synthetic WebSocket loopback: Run 1 normal call + 1 interrupted call
        app, verifier, _, _ = _create_server_app(telemetry_sink=ws_sink)
        port = _get_free_port()

        async with _run_test_server(app, port) as server_url:
            uri = f"{server_url}{SYNTHETIC_MEDIA_WS_ENDPOINT_PATH}"

            # 2a. WebSocket Normal Call (25 frames = 16,000 bytes each direction)
            token_norm = verifier.issue_token(
                provider_id="smoke_provider",
                tenant_id="tenant_baseline",
                call_id="ws_baseline_normal",
                allowed_scopes=["voice.stream"],
            )
            async with websockets.connect(
                uri, additional_headers={"Authorization": f"Bearer {token_norm}"}
            ) as ws:
                ready = json.loads(await ws.recv())
                assert ready["event"] == EVENT_SESSION_READY
                await ws.send(
                    json.dumps({"event": EVENT_CALL_START, "call_id": "ws_baseline_normal"})
                )
                started = json.loads(await ws.recv())
                assert started["event"] == EVENT_CALL_STARTED

                payload_turn1 = b"\x11" * PCM16LE_FRAME_BYTES
                for _ in range(25):
                    await ws.send(payload_turn1)

                received_echoes = 0
                while received_echoes < 25:
                    msg = await asyncio.wait_for(ws.recv(), timeout=5.0)
                    if isinstance(msg, bytes):
                        assert len(msg) == PCM16LE_FRAME_BYTES
                        received_echoes += 1

                await ws.send(
                    json.dumps({"event": EVENT_CALL_END, "call_id": "ws_baseline_normal"})
                )
                ended = json.loads(await asyncio.wait_for(ws.recv(), timeout=5.0))
                assert ended["event"] == EVENT_CALL_ENDED
                await ws.wait_closed()
                assert ws.close_code == 1000

            # 2b. WebSocket Interrupted Call (10 frames Turn 1 -> interrupt -> 15 frames Turn 2)
            token_intr = verifier.issue_token(
                provider_id="smoke_provider",
                tenant_id="tenant_baseline",
                call_id="ws_baseline_interrupted",
                allowed_scopes=["voice.stream"],
            )
            async with websockets.connect(
                uri, additional_headers={"Authorization": f"Bearer {token_intr}"}
            ) as ws:
                await ws.recv()  # session.ready
                await ws.send(
                    json.dumps({"event": EVENT_CALL_START, "call_id": "ws_baseline_interrupted"})
                )
                await ws.recv()  # call.started

                for _ in range(10):
                    await ws.send(b"\x11" * PCM16LE_FRAME_BYTES)

                # Trigger turn 1 interruption
                await ws.send(
                    json.dumps(
                        {
                            "event": EVENT_CALL_INTERRUPT,
                            "call_id": "ws_baseline_interrupted",
                            "turn_id": 1,
                            "request_id": "req_base_1",
                        }
                    )
                )

                while True:
                    msg = await asyncio.wait_for(ws.recv(), timeout=5.0)
                    if isinstance(msg, str):
                        ctrl = json.loads(msg)
                        if ctrl.get("event") == EVENT_CALL_INTERRUPTED:
                            assert ctrl["interrupted_turn_id"] == 1
                            assert ctrl["new_turn_id"] == 2
                            assert ctrl["playback_confirmed"] is False
                            break

                # Stream 15 frames in Turn 2
                payload_turn2 = b"\x22" * PCM16LE_FRAME_BYTES
                for _ in range(15):
                    await ws.send(payload_turn2)

                turn2_received = 0
                while turn2_received < 15:
                    msg = await asyncio.wait_for(ws.recv(), timeout=5.0)
                    if isinstance(msg, bytes):
                        assert msg == payload_turn2
                        turn2_received += 1

                await ws.send(
                    json.dumps({"event": EVENT_CALL_END, "call_id": "ws_baseline_interrupted"})
                )
                ended = json.loads(await asyncio.wait_for(ws.recv(), timeout=5.0))
                assert ended["event"] == EVENT_CALL_ENDED
                await ws.wait_closed()
                assert ws.close_code == 1000

        agg_snap = ws_sink.snapshot()
        assert len(agg_snap.recent_sessions) == 2
        return (
            inproc_normal_snap,
            agg_snap.recent_sessions[0],
            agg_snap.recent_sessions[1],
            agg_snap,
        )

    inproc_snap, ws_normal_snap, ws_intr_snap, ws_agg_snap = asyncio.run(_run_baseline())

    # Verify in-process baseline
    assert inproc_snap.inbound_frames_transferred == 25
    assert inproc_snap.outbound_frames_sent == 25
    assert inproc_snap.inbound_bytes_transferred == 16_000
    assert inproc_snap.outbound_bytes_sent == 16_000
    assert inproc_snap.inbound_processing_ms.available is True
    assert inproc_snap.inbound_processing_ms.p50_ms is not None
    assert inproc_snap.inbound_processing_ms.p95_ms is not None
    assert inproc_snap.inbound_processing_ms.p99_ms is not None
    assert inproc_snap.network_transport_ms.available is False

    # Verify WebSocket normal call baseline (includes network_transport_ms)
    assert ws_normal_snap.inbound_frames_transferred == 25
    assert ws_normal_snap.outbound_frames_sent == 25
    assert ws_normal_snap.inbound_bytes_transferred == 16_000
    assert ws_normal_snap.outbound_bytes_sent == 16_000
    assert ws_normal_snap.inbound_processing_ms.available is True
    assert ws_normal_snap.outbound_processing_ms.available is True
    assert ws_normal_snap.network_transport_ms.available is True
    assert ws_normal_snap.network_transport_ms.sample_count == 25
    assert ws_normal_snap.network_transport_ms.p50_ms is not None
    assert ws_normal_snap.upstream_ai_generation_ms.available is False
    assert ws_normal_snap.remote_playback_latency_ms.available is False

    # Verify WebSocket interrupted call baseline
    assert ws_intr_snap.interruptions_requested == 1
    assert ws_intr_snap.interruptions_completed == 1
    assert ws_intr_snap.inbound_frames_received == 25
    assert ws_intr_snap.network_transport_ms.available is True

    # Verify WebSocket aggregate across normal + interrupted calls
    assert ws_agg_snap.active_sessions == 0
    assert ws_agg_snap.total_sessions_started == 2
    assert ws_agg_snap.total_sessions_closed == 2
    assert ws_agg_snap.total_sessions_completed_clean == 2
    assert ws_agg_snap.total_interruptions_requested == 1
    assert ws_agg_snap.total_interruptions_completed == 1
    assert ws_agg_snap.total_inbound_frames_received == 50
    assert ws_agg_snap.total_inbound_bytes_received == 32_000


# =============================================================================
# AUDIT GATES B, C, D: Differential Behavior, Conservation Identities & Correlation
# =============================================================================


def _assert_conservation_identities(
    snap: VoiceSessionMetricsSnapshot,
    *,
    direct_queue_injected_outbound: int = 0,
) -> None:
    """Assert exact mathematical conservation identities on a closed VoiceSessionMetricsSnapshot."""
    assert (
        snap.inbound_frames_received
        == snap.inbound_frames_transferred + snap.unforwarded_inbound_frames
    )
    assert (
        snap.outbound_frames_received + direct_queue_injected_outbound
        == snap.outbound_frames_sent
        + snap.locally_discarded_frames
        + snap.stale_frames_suppressed
        + snap.unforwarded_outbound_frames
    )
    assert (
        snap.dropped_frames_total
        == snap.locally_discarded_frames
        + snap.unforwarded_inbound_frames
        + snap.unforwarded_outbound_frames
    )
    assert (
        snap.interruptions_requested
        == snap.interruptions_completed + snap.interruptions_noop + snap.interruptions_failed
    )


def test_gate_b_differential_equivalence_sink_disabled_vs_enabled() -> None:
    """Gate B: Telemetry disabled vs enabled produces identical delivered frames, outcomes, and lease recovery."""

    async def _run_suite(enable_sink: bool) -> dict[str, Any]:
        sink = InMemoryVoiceTelemetrySink() if enable_sink else None

        # 1. Normal completion + EOF
        in_frames = [
            create_synthetic_pcm16le_frame(
                bytes([0x10 + i]) * PCM16LE_FRAME_BYTES,
                sequence=i,
                timestamp_ms=float(i * 20),
                turn_id=1,
            )
            for i in range(6)
        ]
        out_frames = [
            create_synthetic_pcm16le_frame(
                bytes([0x80 + i]) * PCM16LE_FRAME_BYTES,
                sequence=i,
                timestamp_ms=float(i * 20),
                turn_id=1,
            )
            for i in range(6)
        ]
        tel_norm = _make_telephony_port(in_frames)
        ai_norm = _make_ai_port(out_frames)
        s_norm = VoiceBridgeSession(
            call_id="diff_norm",
            tenant_id="tenant_diff",
            telephony_port=tel_norm,
            ai_port=ai_norm,
            telemetry_sink=sink,
        )
        await s_norm.start()
        await s_norm.wait_until_closed()
        await asyncio.wait_for(s_norm._inbound_queue.join(), timeout=0.2)
        await asyncio.wait_for(s_norm._outbound_queue.join(), timeout=0.2)

        # 2. Interruption + stale suppression + Turn 2 delivery
        tel_intr = InterruptibleFakeTelephonyAdapter(
            capabilities=InterruptionCapabilities(
                can_discard_local_output=True,
                can_cancel_transport_pending=True,
                can_clear_provider_playback=True,
            )
        )
        ai_intr = InterruptibleFakeAIPort(
            capabilities=InterruptionCapabilities(
                can_discard_local_output=True,
                can_cancel_future_generation=True,
            )
        )
        s_intr = VoiceBridgeSession(
            call_id="diff_intr",
            tenant_id="tenant_diff",
            telephony_port=tel_intr,
            ai_port=ai_intr,
            telemetry_sink=sink,
        )
        await s_intr.start()
        s_intr._outbound_queue.put_nowait(_make_frame(sequence=0, turn_id=1))
        outcome = await s_intr.interrupt(turn_id=1)
        ai_intr.push_frame(_make_frame(sequence=1, turn_id=1))  # stale
        ai_intr.push_frame(_make_frame(sequence=2, turn_id=2))  # valid turn 2
        ai_intr.push_eof()
        tel_intr.push_eof()
        await s_intr.wait_until_closed()
        await asyncio.wait_for(s_intr._inbound_queue.join(), timeout=0.2)
        await asyncio.wait_for(s_intr._outbound_queue.join(), timeout=0.2)

        # 3. Forced failure + concurrent close
        tel_fail = _make_telephony_port([_make_frame(sequence=0)])
        ai_fail = FakeAIMediaAdapter()
        ai_fail.inject_send_error(MediaPortError("simulated upstream failure"))
        ai_fail.push_eof()
        s_fail = VoiceBridgeSession(
            call_id="diff_fail",
            tenant_id="tenant_diff",
            telephony_port=tel_fail,
            ai_port=ai_fail,
            telemetry_sink=sink,
        )
        await s_fail.start()
        await asyncio.gather(
            s_fail.wait_until_closed(propagate_error=False),
            s_fail.close(),
            s_fail.close(),
        )
        await asyncio.wait_for(s_fail._inbound_queue.join(), timeout=0.2)
        await asyncio.wait_for(s_fail._outbound_queue.join(), timeout=0.2)

        # 4. WebSocket loopback call with normal call.end drain & lease recovery
        app, verifier, auth_bridge, coordinator = _create_server_app(telemetry_sink=sink)
        port = _get_free_port()
        ws_echoed: list[bytes] = []
        async with _run_test_server(app, port) as url:
            uri = f"{url}{SYNTHETIC_MEDIA_WS_ENDPOINT_PATH}"
            tok = verifier.issue_token(
                provider_id="smoke_provider",
                tenant_id="tenant_diff",
                call_id="ws_diff_call",
                allowed_scopes=["voice.stream"],
            )
            async with websockets.connect(
                uri, additional_headers={"Authorization": f"Bearer {tok}"}
            ) as ws:
                await ws.recv()
                await ws.send(json.dumps({"event": EVENT_CALL_START, "call_id": "ws_diff_call"}))
                await ws.recv()
                for idx in range(5):
                    await ws.send(bytes([0x30 + idx]) * PCM16LE_FRAME_BYTES)
                while len(ws_echoed) < 5:
                    msg = await asyncio.wait_for(ws.recv(), timeout=5.0)
                    if isinstance(msg, bytes):
                        ws_echoed.append(msg)
                await ws.send(json.dumps({"event": EVENT_CALL_END, "call_id": "ws_diff_call"}))
                await ws.recv()
                await ws.wait_closed()

        return {
            "norm_in_payloads": [f.payload for f in ai_norm.sent_frames],
            "norm_out_payloads": [f.payload for f in tel_norm.sent_frames],
            "norm_state": s_norm.state,
            "intr_outcome": outcome,
            "intr_out_seqs": [(f.sequence, f.turn_id) for f in tel_intr.sent_frames],
            "intr_state": s_intr.state,
            "fail_state": s_fail.state,
            "fail_err_type": type(s_fail.error).__name__,
            "ws_echoed": ws_echoed,
            "active_leases": coordinator.controller.snapshot().active_connections,
            "active_calls": len(auth_bridge._active_calls),
        }

    res_disabled = asyncio.run(_run_suite(enable_sink=False))
    res_enabled = asyncio.run(_run_suite(enable_sink=True))
    assert res_disabled == res_enabled
    assert res_enabled["active_leases"] == 0
    assert res_enabled["active_calls"] == 0


def test_gate_c_frame_and_interruption_accounting_identities_and_queue_join() -> None:
    """Gate C: Verify exact frame & interruption conservation identities and queue.join() across 10 scenarios."""

    async def _run_all() -> None:
        # 1. Failed upstream send (inbound forwarder send_frame raises)
        tel1 = _make_telephony_port([_make_frame(sequence=0), _make_frame(sequence=1)])
        ai1 = FakeAIMediaAdapter()
        ai1.inject_send_error(MediaPortError("upstream send error"), after_n_frames=1)
        ai1.push_eof()
        s1 = VoiceBridgeSession(call_id="c_s1", tenant_id="t1", telephony_port=tel1, ai_port=ai1)
        await s1.start()
        await s1.wait_until_closed(propagate_error=False)
        await asyncio.wait_for(s1._inbound_queue.join(), timeout=0.2)
        snap1 = s1.metrics_snapshot()
        assert snap1.inbound_frames_transferred == 1
        assert snap1.unforwarded_inbound_frames == snap1.inbound_frames_received - 1
        _assert_conservation_identities(snap1)

        # 2. Failed downstream send (outbound forwarder send_frame raises)
        tel2 = FakeTelephonyMediaAdapter()
        tel2.inject_send_error(MediaPortError("downstream send error"), after_n_frames=1)
        tel2.push_eof()
        ai2 = _make_ai_port([_make_frame(sequence=0), _make_frame(sequence=1)])
        s2 = VoiceBridgeSession(call_id="c_s2", tenant_id="t1", telephony_port=tel2, ai_port=ai2)
        await s2.start()
        await s2.wait_until_closed(propagate_error=False)
        await asyncio.wait_for(s2._outbound_queue.join(), timeout=0.2)
        snap2 = s2.metrics_snapshot()
        assert snap2.outbound_frames_sent == 1
        assert snap2.unforwarded_outbound_frames == snap2.outbound_frames_received - 1
        _assert_conservation_identities(snap2)

        # 3 & 4 & 5 & 9. Buffered interruption purge + in-flight old-turn + late stale + duplicate interrupt
        tel3 = InterruptibleFakeTelephonyAdapter()
        ai3 = InterruptibleFakeAIPort()
        s3 = VoiceBridgeSession(call_id="c_s3", tenant_id="t1", telephony_port=tel3, ai_port=ai3)
        await s3.start()
        # Pause outbound forwarder on first frame while two more turn-1 frames sit in _outbound_queue
        tel3.set_send_delay(0.04)
        ai3.push_frame(_make_frame(sequence=0, turn_id=1))
        ai3.push_frame(_make_frame(sequence=1, turn_id=1))
        ai3.push_frame(_make_frame(sequence=2, turn_id=1))
        await asyncio.sleep(0.01)
        out_intr = await s3.interrupt(turn_id=1)
        assert out_intr.new_turn_id == 2
        # Duplicate interruption -> noop
        out_dup = await s3.interrupt(turn_id=1)
        assert out_dup.locally_discarded_frames == 0
        tel3.set_send_delay(0.0)
        # Late stale frame from turn 1 + valid frame from turn 2
        ai3.push_frame(_make_frame(sequence=3, turn_id=1))
        ai3.push_frame(_make_frame(sequence=4, turn_id=2))
        ai3.push_eof()
        tel3.push_eof()
        await s3.wait_until_closed()
        await asyncio.wait_for(s3._outbound_queue.join(), timeout=0.2)
        snap3 = s3.metrics_snapshot()
        assert snap3.outbound_frames_received == 5
        assert snap3.interruptions_requested == 2
        assert snap3.interruptions_completed == 1
        assert snap3.interruptions_noop == 1
        _assert_conservation_identities(snap3)

        # 6. Backpressure timeout on inbound queue
        class BlockingAIPort(FakeAIMediaAdapter):
            async def send_frame(self, frame: AudioFrame) -> None:
                await asyncio.sleep(1.0)

        tel6 = _make_telephony_port([_make_frame(sequence=i) for i in range(5)])
        ai6 = BlockingAIPort()
        ai6.push_eof()
        s6 = VoiceBridgeSession(
            call_id="c_s6",
            tenant_id="t1",
            telephony_port=tel6,
            ai_port=ai6,
            queue_capacity=2,
            backpressure_timeout=0.02,
        )
        await s6.start()
        await s6.wait_until_closed(propagate_error=False)
        await asyncio.wait_for(s6._inbound_queue.join(), timeout=0.2)
        snap6 = s6.metrics_snapshot()
        assert snap6.inbound_frames_transferred == 0
        assert snap6.unforwarded_inbound_frames == snap6.inbound_frames_received
        _assert_conservation_identities(snap6)

        # 7 & 8. Forced close with full inbound and outbound queues
        class HoldBothPorts(FakeTelephonyMediaAdapter):
            def __init__(self) -> None:
                super().__init__()
                self.block = asyncio.Event()

            async def send_frame(self, frame: AudioFrame) -> None:
                await self.block.wait()

        class HoldAIPort(FakeAIMediaAdapter):
            def __init__(self) -> None:
                super().__init__()
                self.block = asyncio.Event()

            async def send_frame(self, frame: AudioFrame) -> None:
                await self.block.wait()

        tel7 = HoldBothPorts()
        ai7 = HoldAIPort()
        for i in range(4):
            tel7.push_frame(_make_frame(sequence=i))
            ai7.push_frame(_make_frame(sequence=i))
        s7 = VoiceBridgeSession(
            call_id="c_s7",
            tenant_id="t1",
            telephony_port=tel7,
            ai_port=ai7,
            queue_capacity=2,
        )
        await s7.start()
        await asyncio.sleep(0.03)
        await s7.close()
        await asyncio.wait_for(s7._inbound_queue.join(), timeout=0.2)
        await asyncio.wait_for(s7._outbound_queue.join(), timeout=0.2)
        snap7 = s7.metrics_snapshot()
        assert snap7.inbound_frames_transferred == 0
        assert snap7.outbound_frames_sent == 0
        assert snap7.unforwarded_inbound_frames == snap7.inbound_frames_received
        assert snap7.unforwarded_outbound_frames == snap7.outbound_frames_received
        _assert_conservation_identities(snap7)

        # 10. Partial provider failure during interrupt followed by successful retry + closed-session interrupt
        tel10 = InterruptibleFakeTelephonyAdapter(
            capabilities=InterruptionCapabilities(
                can_discard_local_output=True,
                can_cancel_transport_pending=True,
                can_clear_provider_playback=True,
            ),
            simulate_clear_error=MediaPortError("transient telephony clear error"),
        )
        ai10 = InterruptibleFakeAIPort(
            capabilities=InterruptionCapabilities(
                can_discard_local_output=True,
                can_cancel_future_generation=True,
            )
        )
        s10 = VoiceBridgeSession(
            call_id="c_s10", tenant_id="t1", telephony_port=tel10, ai_port=ai10
        )
        await s10.start()
        with pytest.raises(MediaPortError, match="transient telephony clear error"):
            await s10.interrupt(
                turn_id=1,
                require_future_generation_cancellation=True,
                require_playback_clear=True,
            )
        # Retry succeeds and advances to turn 2
        retry_outcome = await s10.interrupt(
            turn_id=1,
            require_future_generation_cancellation=True,
            require_playback_clear=True,
        )
        assert retry_outcome.new_turn_id == 2
        tel10.push_eof()
        ai10.push_eof()
        await s10.wait_until_closed()
        # Calling interrupt after session close increments requested & failed
        with pytest.raises(VoiceBridgeSessionClosedError):
            await s10.interrupt(turn_id=2)
        snap10 = s10.metrics_snapshot()
        assert snap10.interruptions_requested == 3
        assert snap10.interruptions_completed == 1
        assert snap10.interruptions_noop == 0
        assert snap10.interruptions_failed == 2
        _assert_conservation_identities(snap10)

    asyncio.run(_run_all())


def test_gate_d_same_frame_transport_correlation_and_duplicate_frame_references() -> None:
    """Gate D: consume_last_send_duration_ms correlates strictly per-frame and duplicate frame refs preserve timing."""
    from kaiwen_gateway.core.identity import VerifiedConnectionContext
    from kaiwen_gateway.transports.synthetic import WebSocketTelephonyMediaAdapter

    class SelectiveSendWS:
        async def send_bytes(self, data: bytes) -> None:
            await asyncio.sleep(0.001)

    async def _run() -> None:
        ctx = VerifiedConnectionContext(
            provider_id="p1",
            tenant_id="t1",
            connection_id="conn1",
            call_id="call1",
            allowed_scopes=frozenset({"voice.stream"}),
        )
        adapter = WebSocketTelephonyMediaAdapter(websocket=SelectiveSendWS(), context=ctx)
        f1 = _make_frame(sequence=0, turn_id=1)
        f2 = _make_frame(sequence=1, turn_id=1)

        await adapter.send_frame(f1)
        # Cannot consume f1's duration for a different frame f2
        assert adapter.consume_last_send_duration_ms(f2) is None
        # Consuming for f1 succeeds once and clears the token
        dur1 = adapter.consume_last_send_duration_ms(f1)
        assert dur1 is not None and dur1 >= 0.0
        # Second consume for f1 returns None (never reuses stale measurement)
        assert adapter.consume_last_send_duration_ms(f1) is None

        # Also test duplicate AudioFrame object references through VoiceBridgeSession + interrupt()
        shared_turn1 = _make_frame(sequence=0, turn_id=1)
        shared_turn2 = _make_frame(sequence=1, turn_id=2)
        tel = InterruptibleFakeTelephonyAdapter()
        ai = InterruptibleFakeAIPort()
        session = VoiceBridgeSession(
            call_id="c_dup_ref", tenant_id="t1", telephony_port=tel, ai_port=ai
        )
        await session.start()
        # Enqueue duplicate object references into _outbound_queue and _outbound_timing_queue
        session._outbound_queue.put_nowait(shared_turn1)
        session._outbound_timing_queue.append((id(shared_turn1), 1.0, 1.0, 1.0))
        session._outbound_queue.put_nowait(shared_turn1)
        session._outbound_timing_queue.append((id(shared_turn1), 2.0, 2.0, 2.0))
        session._outbound_queue.put_nowait(shared_turn2)
        session._outbound_timing_queue.append((id(shared_turn2), 3.0, 3.0, 3.0))

        outcome = await session.interrupt(turn_id=1)
        assert outcome.locally_discarded_frames == 2
        assert outcome.new_turn_id == 2

        tel.push_eof()
        ai.push_eof()
        await session.wait_until_closed()
        assert len(tel.sent_frames) == 1
        assert tel.sent_frames[0] is shared_turn2

    asyncio.run(_run())
