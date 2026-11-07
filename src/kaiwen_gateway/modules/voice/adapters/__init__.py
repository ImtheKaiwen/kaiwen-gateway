"""Fake and test adapters for voice media ports."""

from kaiwen_gateway.modules.voice.adapters.fake import (
    FakeAIMediaAdapter,
    FakeTelephonyMediaAdapter,
)
from kaiwen_gateway.modules.voice.adapters.fake_ai import (
    BoundedFakeAIEchoPort,
    BoundedInterruptibleFakeAIEchoPort,
)

__all__ = [
    "FakeTelephonyMediaAdapter",
    "FakeAIMediaAdapter",
    "BoundedFakeAIEchoPort",
    "BoundedInterruptibleFakeAIEchoPort",
]
