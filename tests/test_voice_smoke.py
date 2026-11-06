"""Tests for the runnable in-process voice media smoke runner."""

from __future__ import annotations

import asyncio

import pytest
from tools.voice_smoke import main, run_voice_media_smoke

from kaiwen_gateway.modules.voice.bridge import VoiceBridgeState


def test_run_voice_media_smoke_full() -> None:
    async def _test() -> None:
        res = await run_voice_media_smoke(
            call_id="test-smoke-01",
            tenant_id="tenant-test",
            frame_count=20,
            verbose=False,
        )
        assert res.call_id == "test-smoke-01"
        assert res.tenant_id == "tenant-test"
        assert res.telephony_frames_sent == 20
        assert res.telephony_frames_received == 20
        assert res.ai_frames_sent == 20
        assert res.ai_frames_received == 20
        assert res.byte_integrity_verified
        assert res.concurrency_verified
        assert res.tasks_terminated
        assert res.final_state == VoiceBridgeState.CLOSED
        assert res.duration_seconds >= 0.0

    asyncio.run(_test())


def test_main_cli_execution(monkeypatch: pytest.MonkeyPatch) -> None:
    # Test that main() executes and returns 0 exit code
    exit_code = main()
    assert exit_code == 0
