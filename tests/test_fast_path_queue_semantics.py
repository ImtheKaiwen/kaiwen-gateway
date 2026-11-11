"""Deterministic regression tests for fast-path queue dispatch semantics.

Verifies fast-path put_nowait with backpressure fallback across:
- WebSocketTelephonyMediaAdapter
- BoundedFakeAIEchoPort
- VoiceBridgeSession (inbound and outbound pumps)

Validates:
1. Empty queues (immediate fast-path dispatch)
2. Non-full queues (immediate fast-path dispatch without future allocation)
3. Full queues with finite timeout (blocks and raises MediaBackpressureOverflowError)
4. Full queues without timeout (blocks until space is available)
5. Concurrent producers and consumers (race-free handling under load)
6. Strict FIFO ordering preservation
7. Task cancellation during backpressure wait
8. Clean EOF propagation following fast-path frames
9. Shutdown / close() during backpressure wait
10. Strict capacity enforcement (fail-closed overflow)
"""

from __future__ import annotations

import asyncio

import pytest

from kaiwen_gateway.core.identity import VerifiedConnectionContext
from kaiwen_gateway.modules.voice.adapters.fake import (
    FakeAIMediaAdapter,
    FakeTelephonyMediaAdapter,
)
from kaiwen_gateway.modules.voice.adapters.fake_ai import BoundedFakeAIEchoPort
from kaiwen_gateway.modules.voice.bridge import (
    VoiceBridgeSession,
    VoiceBridgeState,
)
from kaiwen_gateway.modules.voice.domain import (
    AudioFrame,
    MediaBackpressureOverflowError,
    create_synthetic_pcm16le_frame,
)
from kaiwen_gateway.transports.synthetic.media_adapter import (
    WebSocketTelephonyMediaAdapter,
)


class DummyWebSocket:
    """Minimal async WebSocket mock for testing WebSocketTelephonyMediaAdapter."""

    def __init__(self) -> None:
        self.sent_bytes: list[bytes] = []
        self.sent_texts: list[str] = []

    async def send_bytes(self, data: bytes) -> None:
        self.sent_bytes.append(data)

    async def send_text(self, text: str) -> None:
        self.sent_texts.append(text)


def _make_context(conn_id: str = "conn_fastpath") -> VerifiedConnectionContext:
    return VerifiedConnectionContext(
        provider_id="test_provider",
        tenant_id="tenant_fastpath",
        connection_id=conn_id,
        call_id=f"call_{conn_id}",
        allowed_scopes=frozenset({"voice.stream"}),
    )


# =============================================================================
# 1. WebSocketTelephonyMediaAdapter Fast-Path Queue Semantics
# =============================================================================


class TestWebSocketTelephonyMediaAdapterFastPathQueue:
    @pytest.mark.anyio
    async def test_empty_and_non_full_queue_fast_path(self) -> None:
        ws = DummyWebSocket()
        adapter = WebSocketTelephonyMediaAdapter(ws, _make_context(), queue_capacity=5)

        # 1. Empty queue enqueue
        f0 = create_synthetic_pcm16le_frame(b"\x01" * 640, sequence=0)
        await adapter.push_inbound_frame(f0)
        assert adapter.inbound_queue_depth == 1
        assert adapter.inbound_queue_high_water_mark == 1

        # 2. Non-full queue enqueues
        for i in range(1, 4):
            fi = create_synthetic_pcm16le_frame(b"\x02" * 640, sequence=i)
            await adapter.push_inbound_frame(fi)
        assert adapter.inbound_queue_depth == 4
        assert adapter.inbound_queue_high_water_mark == 4

        # Read frames and verify FIFO order
        for i in range(4):
            frame = await adapter.receive_frame()
            assert frame is not None
            assert frame.sequence == i
        assert adapter.inbound_queue_depth == 0

    @pytest.mark.anyio
    async def test_full_queue_with_finite_timeout_overflow(self) -> None:
        ws = DummyWebSocket()
        adapter = WebSocketTelephonyMediaAdapter(
            ws, _make_context(), queue_capacity=2, backpressure_timeout=0.04
        )

        # Fill capacity
        await adapter.push_inbound_frame(create_synthetic_pcm16le_frame(b"\x01" * 640, sequence=0))
        await adapter.push_inbound_frame(create_synthetic_pcm16le_frame(b"\x02" * 640, sequence=1))
        assert adapter.inbound_queue_depth == 2

        # Exceed capacity: must wait and raise MediaBackpressureOverflowError
        t0 = asyncio.get_running_loop().time()
        with pytest.raises(MediaBackpressureOverflowError, match="overflowed"):
            await adapter.push_inbound_frame(
                create_synthetic_pcm16le_frame(b"\x03" * 640, sequence=2)
            )
        elapsed = asyncio.get_running_loop().time() - t0
        assert elapsed >= 0.03

        # Existing frames in queue must remain untouched and uncorrupted
        r0 = await adapter.receive_frame()
        r1 = await adapter.receive_frame()
        assert r0 is not None and r0.sequence == 0
        assert r1 is not None and r1.sequence == 1

    @pytest.mark.anyio
    async def test_full_queue_drains_before_timeout_succeeds(self) -> None:
        ws = DummyWebSocket()
        adapter = WebSocketTelephonyMediaAdapter(
            ws, _make_context(), queue_capacity=2, backpressure_timeout=0.2
        )

        # Fill capacity
        await adapter.push_inbound_frame(create_synthetic_pcm16le_frame(b"\x01" * 640, sequence=0))
        await adapter.push_inbound_frame(create_synthetic_pcm16le_frame(b"\x02" * 640, sequence=1))

        # Consumer drains one item after 25ms
        async def drain_later() -> None:
            await asyncio.sleep(0.025)
            f = await adapter.receive_frame()
            assert f is not None and f.sequence == 0

        drain_task = asyncio.create_task(drain_later())

        # Push 3rd item; blocks briefly then succeeds via fallback
        f2 = create_synthetic_pcm16le_frame(b"\x03" * 640, sequence=2)
        await adapter.push_inbound_frame(f2)
        await drain_task

        # Remaining queue items in FIFO order
        r1 = await adapter.receive_frame()
        r2 = await adapter.receive_frame()
        assert r1 is not None and r1.sequence == 1
        assert r2 is not None and r2.sequence == 2

    @pytest.mark.anyio
    async def test_full_queue_without_timeout_blocks_until_space(self) -> None:
        ws = DummyWebSocket()
        # backpressure_timeout=None means wait indefinitely
        adapter = WebSocketTelephonyMediaAdapter(
            ws, _make_context(), queue_capacity=1, backpressure_timeout=None
        )

        await adapter.push_inbound_frame(create_synthetic_pcm16le_frame(b"\x01" * 640, sequence=0))

        async def drain_later() -> None:
            await asyncio.sleep(0.03)
            f = await adapter.receive_frame()
            assert f is not None and f.sequence == 0

        drain_task = asyncio.create_task(drain_later())

        f1 = create_synthetic_pcm16le_frame(b"\x02" * 640, sequence=1)
        await adapter.push_inbound_frame(f1)
        await drain_task

        r1 = await adapter.receive_frame()
        assert r1 is not None and r1.sequence == 1

    @pytest.mark.anyio
    async def test_concurrent_producers_and_consumers(self) -> None:
        ws = DummyWebSocket()
        adapter = WebSocketTelephonyMediaAdapter(
            ws, _make_context(), queue_capacity=3, backpressure_timeout=1.0
        )

        total_frames = 30
        received: list[AudioFrame] = []

        async def producer(start_seq: int, step: int) -> None:
            for seq in range(start_seq, total_frames, step):
                f = create_synthetic_pcm16le_frame(b"\xaa" * 640, sequence=seq)
                await adapter.push_inbound_frame(f)

        async def consumer() -> None:
            for _ in range(total_frames):
                frame = await adapter.receive_frame()
                assert frame is not None
                received.append(frame)

        # 2 concurrent producers pushing interleaved sequences
        prod_tasks = asyncio.gather(
            producer(0, 2),
            producer(1, 2),
        )
        cons_task = asyncio.create_task(consumer())

        await asyncio.gather(prod_tasks, cons_task)
        assert len(received) == total_frames
        received_seqs = [f.sequence for f in received]
        assert sorted(received_seqs) == list(range(total_frames))

    @pytest.mark.anyio
    async def test_cancellation_during_backpressure_wait(self) -> None:
        ws = DummyWebSocket()
        adapter = WebSocketTelephonyMediaAdapter(
            ws, _make_context(), queue_capacity=1, backpressure_timeout=5.0
        )

        # Fill queue
        await adapter.push_inbound_frame(create_synthetic_pcm16le_frame(b"\x01" * 640, sequence=0))

        # Push task that will block
        async def push_blocked() -> None:
            f = create_synthetic_pcm16le_frame(b"\x02" * 640, sequence=1)
            await adapter.push_inbound_frame(f)

        task = asyncio.create_task(push_blocked())
        await asyncio.sleep(0.02)
        assert not task.done()

        # Cancel blocked task
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

        # Queue remains valid and original item intact
        r0 = await adapter.receive_frame()
        assert r0 is not None and r0.sequence == 0
        assert adapter.inbound_queue_depth == 0

    @pytest.mark.anyio
    async def test_eof_handling_after_fast_path(self) -> None:
        ws = DummyWebSocket()
        adapter = WebSocketTelephonyMediaAdapter(ws, _make_context(), queue_capacity=3)

        await adapter.push_inbound_frame(create_synthetic_pcm16le_frame(b"\x01" * 640, sequence=0))
        await adapter.push_inbound_frame(create_synthetic_pcm16le_frame(b"\x02" * 640, sequence=1))
        await adapter.push_inbound_eof()

        r0 = await adapter.receive_frame()
        r1 = await adapter.receive_frame()
        r_eof = await adapter.receive_frame()

        assert r0 is not None and r0.sequence == 0
        assert r1 is not None and r1.sequence == 1
        assert r_eof is None

    @pytest.mark.anyio
    async def test_shutdown_during_backpressure(self) -> None:
        ws = DummyWebSocket()
        adapter = WebSocketTelephonyMediaAdapter(
            ws, _make_context(), queue_capacity=1, backpressure_timeout=5.0
        )

        await adapter.push_inbound_frame(create_synthetic_pcm16le_frame(b"\x01" * 640, sequence=0))

        async def push_blocked() -> None:
            f = create_synthetic_pcm16le_frame(b"\x02" * 640, sequence=1)
            await adapter.push_inbound_frame(f)

        task = asyncio.create_task(push_blocked())
        await asyncio.sleep(0.02)

        # Close adapter while push is pending
        await adapter.close()
        assert adapter.is_closed

        # Wait task or cancel
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task


# =============================================================================
# 2. BoundedFakeAIEchoPort Fast-Path Queue Semantics
# =============================================================================


class TestBoundedFakeAIEchoPortFastPathQueue:
    @pytest.mark.anyio
    async def test_empty_and_non_full_queue_fast_path(self) -> None:
        port = BoundedFakeAIEchoPort(queue_capacity=5)

        # Empty queue
        f0 = create_synthetic_pcm16le_frame(b"\x01" * 640, sequence=0)
        await port.send_frame(f0)
        assert port.queue_depth == 1
        assert port.frames_received == 1

        # Non-full queue
        for i in range(1, 4):
            fi = create_synthetic_pcm16le_frame(b"\x02" * 640, sequence=i)
            await port.send_frame(fi)
        assert port.queue_depth == 4
        assert port.frames_received == 4

        # Read in FIFO order
        for i in range(4):
            echoed = await port.receive_frame()
            assert echoed is not None
            assert echoed.sequence == i
        assert port.queue_depth == 0

    @pytest.mark.anyio
    async def test_full_queue_overflow_and_fifo_integrity(self) -> None:
        port = BoundedFakeAIEchoPort(queue_capacity=2, backpressure_timeout=0.04)

        await port.send_frame(create_synthetic_pcm16le_frame(b"\x01" * 640, sequence=0))
        await port.send_frame(create_synthetic_pcm16le_frame(b"\x02" * 640, sequence=1))
        assert port.queue_depth == 2

        with pytest.raises(MediaBackpressureOverflowError, match="overflowed"):
            await port.send_frame(create_synthetic_pcm16le_frame(b"\x03" * 640, sequence=2))

        # Initial frames remain intact
        e0 = await port.receive_frame()
        e1 = await port.receive_frame()
        assert e0 is not None and e0.sequence == 0
        assert e1 is not None and e1.sequence == 1

    @pytest.mark.anyio
    async def test_full_queue_drains_and_succeeds(self) -> None:
        port = BoundedFakeAIEchoPort(queue_capacity=2, backpressure_timeout=0.2)

        await port.send_frame(create_synthetic_pcm16le_frame(b"\x01" * 640, sequence=0))
        await port.send_frame(create_synthetic_pcm16le_frame(b"\x02" * 640, sequence=1))

        async def drain_later() -> None:
            await asyncio.sleep(0.025)
            f = await port.receive_frame()
            assert f is not None and f.sequence == 0

        drain_task = asyncio.create_task(drain_later())

        await port.send_frame(create_synthetic_pcm16le_frame(b"\x03" * 640, sequence=2))
        await drain_task

        e1 = await port.receive_frame()
        e2 = await port.receive_frame()
        assert e1 is not None and e1.sequence == 1
        assert e2 is not None and e2.sequence == 2

    @pytest.mark.anyio
    async def test_full_queue_without_timeout(self) -> None:
        port = BoundedFakeAIEchoPort(queue_capacity=1, backpressure_timeout=None)

        await port.send_frame(create_synthetic_pcm16le_frame(b"\x01" * 640, sequence=0))

        async def drain_later() -> None:
            await asyncio.sleep(0.03)
            f = await port.receive_frame()
            assert f is not None and f.sequence == 0

        drain_task = asyncio.create_task(drain_later())

        await port.send_frame(create_synthetic_pcm16le_frame(b"\x02" * 640, sequence=1))
        await drain_task

        e1 = await port.receive_frame()
        assert e1 is not None and e1.sequence == 1


# =============================================================================
# 3. VoiceBridgeSession Fast-Path Pump Semantics
# =============================================================================


class TestVoiceBridgeFastPathQueueSemantics:
    @pytest.mark.anyio
    async def test_bridge_inbound_and_outbound_pumps_fast_path(self) -> None:
        tel = FakeTelephonyMediaAdapter()
        ai = FakeAIMediaAdapter()

        session = VoiceBridgeSession(
            call_id="call-fastpath-1",
            tenant_id="tenant-1",
            telephony_port=tel,
            ai_port=ai,
            queue_capacity=10,
            backpressure_timeout=1.0,
        )

        # Enqueue frames before session start (fast path through fake ports)
        total = 5
        for i in range(total):
            f_in = create_synthetic_pcm16le_frame(b"\x11" * 640, sequence=i)
            tel.push_frame(f_in)
        tel.push_eof()

        for i in range(total):
            f_out = create_synthetic_pcm16le_frame(b"\x22" * 640, sequence=i)
            ai.push_frame(f_out)
        ai.push_eof()

        # Run session to completion
        await session.run()

        assert session.state == VoiceBridgeState.CLOSED
        assert session.inbound_completed
        assert session.outbound_completed
        assert session.inbound_frames_transferred == total
        assert session.outbound_frames_transferred == total
        assert ai.send_count == total
        assert [f.sequence for f in ai.sent_frames] == list(range(total))

    @pytest.mark.anyio
    async def test_bridge_backpressure_timeout_fail_close(self) -> None:
        tel = FakeTelephonyMediaAdapter()

        # AI port that never consumes from receive_frame to induce backpressure
        class StalledAIPort(FakeAIMediaAdapter):
            async def send_frame(self, frame: AudioFrame) -> None:
                await asyncio.sleep(10.0)

        ai = StalledAIPort()
        session = VoiceBridgeSession(
            call_id="call-fastpath-stall",
            tenant_id="tenant-1",
            telephony_port=tel,
            ai_port=ai,
            queue_capacity=2,
            backpressure_timeout=0.04,
        )

        for i in range(5):
            tel.push_frame(create_synthetic_pcm16le_frame(b"\x22" * 640, sequence=i))

        with pytest.raises(MediaBackpressureOverflowError):
            await session.run()

        assert session.state == VoiceBridgeState.FAILED
        assert isinstance(session.error, MediaBackpressureOverflowError)
