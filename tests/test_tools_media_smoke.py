"""Unit tests verifying tools/synthetic_media_smoke.py execution and result structures."""

from __future__ import annotations

import asyncio

from tools.synthetic_media_smoke import (
    SyntheticMediaSmokeResult,
    run_synthetic_media_smoke,
)


def test_synthetic_media_smoke_execution() -> None:
    """Verify run_synthetic_media_smoke runs to completion and satisfies all assertions."""

    async def _run() -> None:
        result = await run_synthetic_media_smoke(
            frame_count=10,
            tenant_id="tenant_test_smoke",
            call_id="call_test_smoke",
            verbose=False,
        )
        assert isinstance(result, SyntheticMediaSmokeResult)
        assert result.success is True
        assert result.frames_sent == 10
        assert result.frames_received == 10
        assert result.bytes_sent == 6400
        assert result.bytes_received == 6400
        assert result.byte_integrity_verified is True
        assert result.sequence_ordering_verified is True
        assert result.active_calls_recovered is True
        assert result.active_leases_recovered is True
        assert result.duration_seconds > 0.0

    asyncio.run(_run())
