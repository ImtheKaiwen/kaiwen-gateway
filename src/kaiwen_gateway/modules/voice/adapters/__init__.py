"""Fake and test adapters for voice media ports."""

from kaiwen_gateway.modules.voice.adapters.fake import (
    FakeAIMediaAdapter,
    FakeTelephonyMediaAdapter,
)

__all__ = [
    "FakeTelephonyMediaAdapter",
    "FakeAIMediaAdapter",
]
