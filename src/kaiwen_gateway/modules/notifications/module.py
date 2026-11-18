"""GatewayModule lifecycle adapter for the Kaiwen Gateway Notifications module (N1)."""

from __future__ import annotations

from kaiwen_gateway.core.contracts import CORE_CONTRACT_VERSION, ModuleDescriptor
from kaiwen_gateway.modules.notifications.domain import NotificationsModuleClosedError
from kaiwen_gateway.modules.notifications.ports import (
    DenyAllProducerAuthorizer,
    NotificationProducerAuthorizerPort,
)
from kaiwen_gateway.modules.notifications.preferences import (
    InMemoryNotificationPreferenceStore,
    NotificationPreferencePort,
)
from kaiwen_gateway.modules.notifications.telemetry import (
    InMemoryNotificationsTelemetrySink,
    NotificationsTelemetrySink,
)


class NotificationsModule:
    """Lifecycle-managed Notifications core module implementing Core's `GatewayModule`.

    In N1, owns only core configuration/ports/telemetry with zero background delivery
    workers, zero Redis connections, and zero WebSocket endpoints. Follows single-use
    terminal shutdown semantics matching `EventsModule`.
    """

    __slots__ = (
        "_descriptor",
        "_preference_port",
        "_producer_authorizer",
        "_telemetry",
        "_started",
        "_stopped",
    )

    def __init__(
        self,
        *,
        preference_port: NotificationPreferencePort | None = None,
        producer_authorizer: NotificationProducerAuthorizerPort | None = None,
        telemetry: NotificationsTelemetrySink | None = None,
    ) -> None:
        self._descriptor: ModuleDescriptor = ModuleDescriptor(
            module_id="notifications",
            contract_version=CORE_CONTRACT_VERSION,
            description="N1 bounded notifications core module",
        )
        self._preference_port: NotificationPreferencePort = (
            preference_port
            if preference_port is not None
            else InMemoryNotificationPreferenceStore()
        )
        self._producer_authorizer: NotificationProducerAuthorizerPort = (
            producer_authorizer if producer_authorizer is not None else DenyAllProducerAuthorizer()
        )
        self._telemetry: NotificationsTelemetrySink = (
            telemetry if telemetry is not None else InMemoryNotificationsTelemetrySink()
        )
        self._started: bool = False
        self._stopped: bool = False

    @property
    def descriptor(self) -> ModuleDescriptor:
        """Return canonical Gateway Core module descriptor."""
        return self._descriptor

    @property
    def preference_port(self) -> NotificationPreferencePort:
        """Return configured recipient preference port."""
        return self._preference_port

    @property
    def producer_authorizer(self) -> NotificationProducerAuthorizerPort:
        """Return configured fail-closed producer authorizer port."""
        return self._producer_authorizer

    @property
    def telemetry(self) -> NotificationsTelemetrySink:
        """Return bounded-cardinality Notifications telemetry sink."""
        return self._telemetry

    @property
    def is_started(self) -> bool:
        """Return True if `startup()` has completed and `shutdown()` has not run."""
        return self._started and not self._stopped

    @property
    def is_closed(self) -> bool:
        """Return True after `shutdown()` has run."""
        return self._stopped

    async def startup(self) -> None:
        """Idempotently initialize Notifications core; rejects restart after shutdown."""
        if self._stopped:
            raise NotificationsModuleClosedError(
                "Cannot restart NotificationsModule after terminal shutdown"
            )
        self._started = True

    async def shutdown(self) -> None:
        """Idempotently shut down NotificationsModule."""
        self._stopped = True
        self._started = False


__all__ = ["NotificationsModule"]
