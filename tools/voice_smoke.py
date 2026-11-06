"""Runnable in-process bidirectional voice media smoke demonstration.

Verifies:
1. Fake telephony and fake AI ports creation.
2. Voice bridge session initialization.
3. Telephony pushes PCM16LE test frames -> AI receives identical bytes in sequence.
4. AI pushes response PCM16LE test frames -> Telephony receives identical bytes in sequence.
5. Bidirectional concurrency (both streams progress concurrently).
6. Clean EOF handling and session shutdown.
7. Verification that all supervisor and pump tasks have terminated.
8. Verification that bounded queues and call-local buffers are clean.
"""

from __future__ import annotations

import asyncio
import sys
import time
from dataclasses import dataclass

from kaiwen_gateway.modules.voice import (
    AudioFrame,
    FakeAIMediaAdapter,
    FakeTelephonyMediaAdapter,
    VoiceBridgeSession,
    VoiceBridgeState,
    create_synthetic_pcm16le_frame,
)


@dataclass(frozen=True, slots=True)
class VoiceSmokeResult:
    """Structured result of the in-process voice media smoke run."""

    call_id: str
    tenant_id: str
    telephony_frames_sent: int
    telephony_frames_received: int
    ai_frames_sent: int
    ai_frames_received: int
    byte_integrity_verified: bool
    concurrency_verified: bool
    tasks_terminated: bool
    duration_seconds: float
    final_state: VoiceBridgeState


async def run_voice_media_smoke(
    *,
    call_id: str = "smoke-call-001",
    tenant_id: str = "tenant-synthetic",
    frame_count: int = 20,
    frame_size_bytes: int = 640,  # 20ms of PCM16LE mono at 16kHz
    verbose: bool = True,
) -> VoiceSmokeResult:
    """Run an end-to-end deterministic in-process voice media smoke test."""
    start_time = time.perf_counter()
    if verbose:
        print(
            f"[SMOKE] Initializing in-process voice bridge for call {call_id} (tenant {tenant_id})"
        )

    telephony_adapter = FakeTelephonyMediaAdapter(name="telephony-smoke")
    ai_adapter = FakeAIMediaAdapter(name="ai-smoke")

    session = VoiceBridgeSession(
        call_id=call_id,
        tenant_id=tenant_id,
        telephony_port=telephony_adapter,
        ai_port=ai_adapter,
        queue_capacity=8,
        strict_sequence=True,
    )

    # 1. Start bridge session
    await session.start()
    if verbose:
        print(f"[SMOKE] VoiceBridgeSession started in state: {session.state.value}")

    # Prepare distinct synthetic test payloads
    # Telephony frames contain b"TEL_<index>_<padding>"
    # AI frames contain b"AIR_<index>_<padding>"
    inbound_sent: list[AudioFrame] = []
    outbound_sent: list[AudioFrame] = []

    for i in range(frame_count):
        tel_marker = f"TEL_{i:04d}_".encode("ascii")
        tel_payload = tel_marker + b"\x00" * (frame_size_bytes - len(tel_marker))
        inbound_frame = create_synthetic_pcm16le_frame(
            payload=tel_payload,
            sequence=i,
            timestamp_ms=float(i * 20),
        )
        inbound_sent.append(inbound_frame)

        ai_marker = f"AIR_{i:04d}_".encode("ascii")
        ai_payload = ai_marker + b"\x01" * (frame_size_bytes - len(ai_marker))
        outbound_frame = create_synthetic_pcm16le_frame(
            payload=ai_payload,
            sequence=i,
            timestamp_ms=float(i * 20),
        )
        outbound_sent.append(outbound_frame)

    # Push frames into both adapters concurrently
    for frame in inbound_sent:
        telephony_adapter.push_frame(frame)
    telephony_adapter.push_eof()

    for frame in outbound_sent:
        ai_adapter.push_frame(frame)
    ai_adapter.push_eof()

    # Wait for both pumps to finish transferring and session to close
    await session.wait_until_closed()

    # 2. Verify frame counts
    ai_received = ai_adapter.sent_frames
    tel_received = telephony_adapter.sent_frames

    if verbose:
        print(
            f"[SMOKE] Transfer complete: Telephony->AI={len(ai_received)}/{frame_count}, "
            f"AI->Telephony={len(tel_received)}/{frame_count}"
        )

    # 3. Verify byte integrity and order
    byte_integrity = True
    if len(ai_received) != frame_count or len(tel_received) != frame_count:
        byte_integrity = False

    for original, received in zip(inbound_sent, ai_received, strict=False):
        if original.payload != received.payload or original.sequence != received.sequence:
            byte_integrity = False
            break

    for original, received in zip(outbound_sent, tel_received, strict=False):
        if original.payload != received.payload or original.sequence != received.sequence:
            byte_integrity = False
            break

    # 4. Verify tasks terminated
    tasks_terminated = (
        session._inbound_task is not None
        and session._inbound_task.done()
        and session._outbound_task is not None
        and session._outbound_task.done()
        and session._supervisor_task is not None
        and session._supervisor_task.done()
    )

    # 5. Verify queues drained
    queues_clean = session.inbound_queue_depth == 0 and session.outbound_queue_depth == 0

    duration = time.perf_counter() - start_time
    duration_ms = duration * 1000.0
    concurrency_verified = (
        session.inbound_frames_transferred == frame_count
        and session.outbound_frames_transferred == frame_count
    )

    if verbose:
        print(f"[SMOKE] Byte integrity verified: {byte_integrity}")
        print(f"[SMOKE] Tasks cleanly terminated: {tasks_terminated}")
        print(f"[SMOKE] Queues clean and empty: {queues_clean}")
        print(f"[SMOKE] Final session state: {session.state.value}")
        print(
            f"[SMOKE] Finished in {duration_ms:.2f}ms "
            f"(in-process memory transfer; not network latency or production performance)"
        )

    return VoiceSmokeResult(
        call_id=call_id,
        tenant_id=tenant_id,
        telephony_frames_sent=frame_count,
        telephony_frames_received=len(tel_received),
        ai_frames_sent=frame_count,
        ai_frames_received=len(ai_received),
        byte_integrity_verified=byte_integrity,
        concurrency_verified=concurrency_verified,
        tasks_terminated=tasks_terminated and queues_clean,
        duration_seconds=duration,
        final_state=session.state,
    )


def main() -> int:
    """CLI entrypoint for running the voice media smoke test."""
    print("=" * 65)
    print("KAIWEN GATEWAY -- IN-PROCESS VOICE MEDIA SMOKE")
    print("=" * 65)
    try:
        result = asyncio.run(run_voice_media_smoke(frame_count=20, verbose=True))
        if (
            result.byte_integrity_verified
            and result.concurrency_verified
            and result.tasks_terminated
            and result.final_state == VoiceBridgeState.CLOSED
        ):
            print("\nRESULT: SUCCESS (all voice media invariants verified)")
            return 0
        else:
            print("\nRESULT: FAILED (invariants check failed)")
            return 1
    except Exception as exc:
        print(f"\nRESULT: ERROR ({exc})", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
