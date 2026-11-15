"""Isolated OpenAI Realtime adapter package for Kaiwen Gateway."""

from __future__ import annotations

from kaiwen_gateway.adapters.openai_realtime.adapter import (
    OpenAIRealtimeMediaAdapter,
    create_openai_realtime_adapter_factory,
)
from kaiwen_gateway.adapters.openai_realtime.config import (
    DEFAULT_OPENAI_REALTIME_MODEL,
    DEFAULT_OPENAI_REALTIME_URL,
    DEFAULT_OPENAI_REALTIME_VOICE,
    OPENAI_PCM_SAMPLE_RATE,
    OpenAIRealtimeConfig,
    OpenAIRealtimeDependencyError,
    OpenAIRealtimeError,
)
from kaiwen_gateway.adapters.openai_realtime.transport import (
    OpenAIRealtimeTransport,
    OpenAIRealtimeTransportConnector,
    connect_openai_realtime_websocket,
)

__all__ = [
    "DEFAULT_OPENAI_REALTIME_MODEL",
    "DEFAULT_OPENAI_REALTIME_URL",
    "DEFAULT_OPENAI_REALTIME_VOICE",
    "OPENAI_PCM_SAMPLE_RATE",
    "OpenAIRealtimeConfig",
    "OpenAIRealtimeDependencyError",
    "OpenAIRealtimeError",
    "OpenAIRealtimeMediaAdapter",
    "OpenAIRealtimeTransport",
    "OpenAIRealtimeTransportConnector",
    "connect_openai_realtime_websocket",
    "create_openai_realtime_adapter_factory",
]
