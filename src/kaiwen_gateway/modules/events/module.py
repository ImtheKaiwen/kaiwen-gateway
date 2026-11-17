"""GatewayModule lifecycle adapter for the Kaiwen Gateway Events module."""

from __future__ import annotations

from kaiwen_gateway.core.contracts import CORE_CONTRACT_VERSION, ModuleDescriptor
from kaiwen_gateway.modules.events.broker import EventBrokerConfig, InMemoryEventBroker
from kaiwen_gateway.modules.events.ratelimit import TokenBucketRateLimiter
from kaiwen_gateway.modules.events.telemetry import EventsTelemetrySink


class EventsModule:
    """Lifecycle-managed Events communication module implementing Core's GatewayModule."""

    __slots__ = (
        "_descriptor",
        "_broker",
        "_started",
        "_stopped",
    )

    def __init__(
        self,
        *,
        broker: InMemoryEventBroker | None = None,
        broker_config: EventBrokerConfig | None = None,
        telemetry: EventsTelemetrySink | None = None,
        publish_rate_limiter: TokenBucketRateLimiter | None = None,
        subscribe_rate_limiter: TokenBucketRateLimiter | None = None,
    ) -> None:
        self._descriptor: ModuleDescriptor = ModuleDescriptor(
            module_id="events",
            contract_version=CORE_CONTRACT_VERSION,
            description="E1 bounded in-memory tenant-scoped events foundation",
        )
        self._broker: InMemoryEventBroker = (
            broker
            if broker is not None
            else InMemoryEventBroker(
                config=broker_config,
                telemetry=telemetry,
                publish_rate_limiter=publish_rate_limiter,
                subscribe_rate_limiter=subscribe_rate_limiter,
            )
        )
        self._started: bool = False
        self._stopped: bool = False

    @property
    def descriptor(self) -> ModuleDescriptor:
        """Return canonical Gateway Core module descriptor."""
        return self._descriptor

    @property
    def broker(self) -> InMemoryEventBroker:
        """Return the underlying bounded event broker."""
        return self._broker

    @property
    def is_started(self) -> bool:
        """Return True if startup() has completed and shutdown() has not run."""
        return self._started and not self._stopped

    async def startup(self) -> None:
        """Deterministically initialize the Events module."""
        self._started = True
        self._stopped = False

    async def shutdown(self) -> None:
        """Idempotently shut down the Events module and release all broker resources."""
        self._stopped = True
        await self._broker.close()


__all__ = ["EventsModule"]
