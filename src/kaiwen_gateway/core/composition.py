"""Explicit, instance-scoped Gateway Core composition without global state or import side effects."""

from __future__ import annotations

import itertools
import uuid
from collections.abc import Iterable, Iterator, Mapping
from typing import Final

from kaiwen_gateway.core.config import CoreConfig, validate_core_config
from kaiwen_gateway.core.contracts import GatewayModule, ModuleDescriptor
from kaiwen_gateway.core.exceptions import CoreConfigurationError
from kaiwen_gateway.core.registry import ModuleRegistry

MAX_COMPOSED_MODULES: Final[int] = 64

_NON_COLLECTION_ITERABLE_TYPES: Final[tuple[type[object], ...]] = (
    str,
    bytes,
    bytearray,
    Mapping,
)


def _validate_modules_iterable(
    modules: object,
    *,
    max_modules: int = MAX_COMPOSED_MODULES,
) -> tuple[GatewayModule, ...]:
    """Validate and materialize at most ``max_modules`` items from ``modules``.

    Inspects at most ``max_modules + 1`` items via :func:`itertools.islice` so an
    unbounded generator cannot exhaust memory. Note that bounding item count does
    not bound wall-clock time if a caller-supplied iterator blocks internally, and
    Core cannot roll back external side effects performed by the iterator itself.
    """
    if isinstance(modules, _NON_COLLECTION_ITERABLE_TYPES) or not isinstance(modules, Iterable):
        raise CoreConfigurationError(
            f"modules must be an iterable collection of GatewayModule instances, "
            f"got {type(modules).__name__}",
            field_name="modules",
        )
    try:
        bounded_batch = tuple(itertools.islice(modules, max_modules + 1))
    except Exception as exc:
        raise CoreConfigurationError(
            f"Failed while iterating modules collection: {type(exc).__name__}: {exc}",
            field_name="modules",
        ) from exc

    if len(bounded_batch) > max_modules:
        raise CoreConfigurationError(
            f"modules count exceeds maximum supported composition limit of {max_modules}",
            field_name="modules",
        )
    return bounded_batch


class GatewayCore:
    """Framework-independent, instance-scoped composition root for Kaiwen Gateway Core.

    Invariants, ownership, and concurrency boundaries (M2.2A):
    - Every ``GatewayCore`` instance owns an isolated :class:`~kaiwen_gateway.core.CoreConfig`
      and its own internal :class:`~kaiwen_gateway.core.ModuleRegistry` (stored on the
      private-convention slot ``_registry``). Post-construction module registration is
      omitted from the public API; callers inspect registered modules through read-only
      lookup methods, though registered ``GatewayModule`` objects themselves may be stateful
      and Python private attributes are a convention rather than a hard capability boundary.
    - Bounded collection: ``GatewayCore`` accepts at most :data:`MAX_COMPOSED_MODULES` (``64``)
      modules per instance and inspects at most ``65`` items from ``modules`` before rejecting
      oversized or unbounded iterables with :class:`~kaiwen_gateway.core.CoreConfigurationError`
      (``field_name="modules"``). Bounding item count does not bound wall-clock execution time
      if a caller-supplied iterator blocks internally without yielding, and Core cannot undo
      external side effects executed by the caller's iterator or ``descriptor`` getters.
    - Construction is transactional with respect to ``GatewayCore`` state: all configuration
      fields and module descriptors/hooks are validated and registered in a fresh local
      ``ModuleRegistry`` before returning the ``GatewayCore`` instance, and ``GatewayCore``
      never writes ownership attributes onto caller-supplied module objects.
      If ``modules`` exceeds :data:`MAX_COMPOSED_MODULES`, raises during iteration, fails
      contract validation, or duplicates a ``module_id`` within the same instance, construction
      raises deterministically and discards the local registry.
    - Module ownership contract: ``GatewayModule`` is a structural ``Protocol`` (including
      slotted or frozen implementations), and ``kaiwen_gateway.core`` intentionally avoids
      process-wide global state or locks. Callers MUST instantiate a separate stateful
      ``GatewayModule`` object for each ``GatewayCore`` instance; sharing a stateful module
      object across multiple ``GatewayCore`` instances is unsupported caller usage and is
      not tracked via global runtime state or private attribute injection.
    - Concurrency contract: Independent ``GatewayCore`` instances may be constructed
      concurrently on separate threads when given distinct module instances, and public
      read-only lookups on a constructed ``GatewayCore`` do not mutate its registration table.
    - Construction is purely synchronous: it never executes module ``startup()`` or
      ``shutdown()`` coroutines, spawns background tasks/threads, or opens network sockets.
    - ``GatewayCore`` is **not** an ASGI application and does not provide multi-process or
      distributed connection admission coordination.
    """

    __slots__ = ("_config", "_instance_id", "_registry")

    def __init__(
        self,
        config: CoreConfig | None = None,
        *,
        modules: Iterable[GatewayModule] = (),
    ) -> None:
        validated_config = CoreConfig() if config is None else validate_core_config(config)
        module_batch = _validate_modules_iterable(modules)

        registry = ModuleRegistry()
        for module in module_batch:
            registry.register(module)

        self._instance_id: str = f"{validated_config.instance_name}-{uuid.uuid4().hex[:12]}"
        self._config: CoreConfig = validated_config
        self._registry: ModuleRegistry = registry

    @property
    def instance_id(self) -> str:
        """Return the unique in-process identifier for this ``GatewayCore`` instance."""
        return self._instance_id

    @property
    def config(self) -> CoreConfig:
        """Return the immutable :class:`~kaiwen_gateway.core.CoreConfig` for this instance."""
        return self._config

    def get_module(self, module_id: str) -> GatewayModule:
        """Return the registered module for ``module_id`` or raise ``ModuleNotRegisteredError``."""
        return self._registry.get(module_id)

    def find_module(self, module_id: str) -> GatewayModule | None:
        """Return the registered module for ``module_id``, or ``None`` if not registered."""
        return self._registry.find(module_id)

    def has_module(self, module_id: str) -> bool:
        """Return ``True`` if ``module_id`` is registered in this ``GatewayCore`` instance."""
        return self._registry.contains(module_id)

    def module_ids(self) -> tuple[str, ...]:
        """Return registered module IDs in deterministic registration order."""
        return self._registry.module_ids()

    def descriptors(self) -> tuple[ModuleDescriptor, ...]:
        """Return validated module descriptors in deterministic registration order."""
        return self._registry.descriptors()

    def modules(self) -> tuple[GatewayModule, ...]:
        """Return registered module instances in deterministic registration order."""
        return self._registry.modules()

    def modules_mapping(self) -> Mapping[str, GatewayModule]:
        """Return a read-only mapping snapshot of ``module_id -> GatewayModule``."""
        return self._registry.as_mapping()

    def __contains__(self, module_id: object) -> bool:
        return module_id in self._registry

    def __len__(self) -> int:
        return len(self._registry)

    def __iter__(self) -> Iterator[str]:
        return iter(self._registry)


def compose_gateway_core(
    config: CoreConfig | None = None,
    *,
    modules: Iterable[GatewayModule] = (),
) -> GatewayCore:
    """Explicit factory that constructs an isolated, validated :class:`GatewayCore` instance."""
    return GatewayCore(config=config, modules=modules)


__all__ = [
    "GatewayCore",
    "MAX_COMPOSED_MODULES",
    "compose_gateway_core",
]
