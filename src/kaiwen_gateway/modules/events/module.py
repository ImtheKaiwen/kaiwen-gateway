"""GatewayModule lifecycle adapter for the Kaiwen Gateway Events module."""

from __future__ import annotations

from kaiwen_gateway.core.contracts import CORE_CONTRACT_VERSION, ModuleDescriptor
from kaiwen_gateway.modules.events.broker import EventBrokerConfig, InMemoryEventBroker
from kaiwen_gateway.modules.events.domain import EventsModuleClosedError
from kaiwen_gateway.modules.events.ports import EventBrokerPort
from kaiwen_gateway.modules.events.ratelimit import RateLimiterPort
from kaiwen_gateway.modules.events.telemetry import EventsTelemetrySink


class EventsModule:
    """Lifecycle-managed Events communication module implementing Core's GatewayModule.

    Follows single-use terminal shutdown semantics: once `shutdown()` has run,
    subsequent `startup()` calls fail closed with `EventsModuleClosedError`.
    """

    __slots__ = (
        "_descriptor",
        "_broker",
        "_started",
        "_stopped",
    )

    def __init__(
        self,
        *,
        broker: EventBrokerPort | None = None,
        broker_config: EventBrokerConfig | None = None,
        telemetry: EventsTelemetrySink | None = None,
        publish_rate_limiter: RateLimiterPort | None = None,
        subscribe_rate_limiter: RateLimiterPort | None = None,
    ) -> None:
        self._descriptor: ModuleDescriptor = ModuleDescriptor(
            module_id="events",
            contract_version=CORE_CONTRACT_VERSION,
            description="E1/E3 bounded tenant-scoped events module",
        )
        self._broker: EventBrokerPort = (
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
    def broker(self) -> EventBrokerPort:
        """Return the underlying bounded event broker."""
        return self._broker

    @property
    def is_started(self) -> bool:
        """Return True if startup() has completed and shutdown() has not run."""
        return self._started and not self._stopped and not self._broker.is_closed

    async def startup(self) -> None:
        """Deterministically initialize the Events module; rejects restart after shutdown."""
        if self._stopped or self._broker.is_closed:
            raise EventsModuleClosedError("Cannot restart EventsModule after terminal shutdown")
        await self._broker.startup()
        self._started = True

    async def shutdown(self) -> None:
        """Idempotently shut down the Events module and release all broker resources."""
        self._stopped = True
        self._started = False
        await self._broker.shutdown()


__all__ = ["EventsModule"]
