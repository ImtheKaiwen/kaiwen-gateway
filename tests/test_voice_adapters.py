"""Unit and conformance tests for fake voice media adapters."""

from __future__ import annotations

import asyncio

from kaiwen_gateway.modules.voice.adapters.fake import (
    FakeAIMediaAdapter,
    FakeTelephonyMediaAdapter,
)
from kaiwen_gateway.modules.voice.domain import (
    MediaPortClosedError,
    create_synthetic_pcm16le_frame,
)
from kaiwen_gateway.modules.voice.ports import (
    AIRealtimeMediaPort,
    TelephonyMediaPort,
)


class TestFakeMediaAdapters:
    def test_protocol_conformance(self) -> None:
        tel = FakeTelephonyMediaAdapter()
        ai = FakeAIMediaAdapter()
        assert isinstance(tel, TelephonyMediaPort)
        assert isinstance(ai, AIRealtimeMediaPort)
        assert not tel.is_closed
        assert not ai.is_closed

    def test_push_and_receive_frames(self) -> None:
        async def _test() -> None:
            adapter = FakeTelephonyMediaAdapter(name="test-tel")
            f1 = create_synthetic_pcm16le_frame(b"\x01\x02", sequence=0)
            f2 = create_synthetic_pcm16le_frame(b"\x03\x04", sequence=1)

            adapter.push_frame(f1)
            adapter.push_frame(f2)
            adapter.push_eof()

            rec1 = await adapter.receive_frame()
            rec2 = await adapter.receive_frame()
            rec_eof = await adapter.receive_frame()

            assert rec1 == f1
            assert rec2 == f2
            assert rec_eof is None
            assert adapter.receive_count == 2

        asyncio.run(_test())

    def test_send_frame_recording(self) -> None:
        async def _test() -> None:
            adapter = FakeAIMediaAdapter(name="test-ai")
            f1 = create_synthetic_pcm16le_frame(b"\x01\x02", sequence=0)
            f2 = create_synthetic_pcm16le_frame(b"\x03\x04", sequence=1)

            await adapter.send_frame(f1)
            await adapter.send_frame(f2)

            assert adapter.send_count == 2
            assert adapter.sent_frames == [f1, f2]

        asyncio.run(_test())

    def test_operations_after_close_fail(self) -> None:
        async def _test() -> None:
            adapter = FakeTelephonyMediaAdapter(name="test-tel")
            await adapter.close()
            assert adapter.is_closed
            assert adapter.close_count == 1

            f = create_synthetic_pcm16le_frame(b"\x01\x02", sequence=0)
            try:
                adapter.push_frame(f)
                raise AssertionError("Expected MediaPortClosedError")
            except MediaPortClosedError:
                pass

            try:
                adapter.push_eof()
                raise AssertionError("Expected MediaPortClosedError")
            except MediaPortClosedError:
                pass

            try:
                await adapter.receive_frame()
                raise AssertionError("Expected MediaPortClosedError")
            except MediaPortClosedError:
                pass

            try:
                await adapter.send_frame(f)
                raise AssertionError("Expected MediaPortClosedError")
            except MediaPortClosedError:
                pass

        asyncio.run(_test())

    def test_idempotent_close(self) -> None:
        async def _test() -> None:
            adapter = FakeAIMediaAdapter()
            await adapter.close()
            await adapter.close()
            await adapter.close()
            assert adapter.is_closed
            assert adapter.close_count == 3

        asyncio.run(_test())

    def test_close_unblocks_pending_receive(self) -> None:
        async def _test() -> None:
            adapter = FakeTelephonyMediaAdapter()

            async def waiter() -> None:
                try:
                    await adapter.receive_frame()
                    raise AssertionError("Expected MediaPortClosedError")
                except MediaPortClosedError:
                    pass

            task = asyncio.create_task(waiter())
            await asyncio.sleep(0.01)
            await adapter.close()
            await task

        asyncio.run(_test())

    def test_injected_send_error(self) -> None:
        async def _test() -> None:
            adapter = FakeAIMediaAdapter()
            f1 = create_synthetic_pcm16le_frame(b"\x01\x02", sequence=0)
            f2 = create_synthetic_pcm16le_frame(b"\x03\x04", sequence=1)
            adapter.inject_send_error(ValueError("simulated-send-failure"), after_n_frames=1)

            await adapter.send_frame(f1)
            try:
                await adapter.send_frame(f2)
                raise AssertionError("Expected ValueError")
            except ValueError as err:
                assert str(err) == "simulated-send-failure"

        asyncio.run(_test())

    def test_injected_receive_error(self) -> None:
        async def _test() -> None:
            adapter = FakeTelephonyMediaAdapter()
            f1 = create_synthetic_pcm16le_frame(b"\x01\x02", sequence=0)
            adapter.push_frame(f1)
            adapter.inject_receive_error(RuntimeError("simulated-recv-failure"), after_n_frames=1)

            rec1 = await adapter.receive_frame()
            assert rec1 == f1

            try:
                await adapter.receive_frame()
                raise AssertionError("Expected RuntimeError")
            except RuntimeError as err:
                assert str(err) == "simulated-recv-failure"

        asyncio.run(_test())

    def test_delay_simulation(self) -> None:
        async def _test() -> None:
            adapter = FakeTelephonyMediaAdapter()
            adapter.set_send_delay(0.02)
            adapter.set_receive_delay(0.02)
            f = create_synthetic_pcm16le_frame(b"\x01\x02", sequence=0)
            adapter.push_frame(f)

            start = asyncio.get_running_loop().time()
            rec = await adapter.receive_frame()
            mid = asyncio.get_running_loop().time()
            assert rec == f
            assert mid - start >= 0.015

            await adapter.send_frame(f)
            end = asyncio.get_running_loop().time()
            assert end - mid >= 0.015

        asyncio.run(_test())
