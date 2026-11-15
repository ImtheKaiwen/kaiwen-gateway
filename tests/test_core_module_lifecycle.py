"""Host-owned module lifecycle orchestration tests (S1-A), independent of any ASGI framework."""

from __future__ import annotations

import asyncio
import time

import pytest

from kaiwen_gateway.core import (
    CoreConfigurationError,
    GatewayCore,
    ModuleDescriptor,
    ModuleFailure,
    ModuleLifecycleManager,
    ModuleLifecyclePhase,
    ModuleLifecyclePolicy,
    ModuleShutdownError,
    ModuleStartupError,
    compose_gateway_core,
)
from kaiwen_gateway.core.module_lifecycle import InvalidModuleLifecycleStateError

SECRET = "sk-live-SUPER-SECRET-TOKEN"


class RecordingModule:
    """Fake module recording hook calls into a shared event log."""

    def __init__(
        self,
        module_id: str,
        events: list[str],
        *,
        fail_startup: Exception | None = None,
        fail_shutdown: Exception | None = None,
        hang_startup: bool = False,
        hang_shutdown: bool = False,
        shutdown_delay: float = 0.0,
    ) -> None:
        self.descriptor = ModuleDescriptor(module_id=module_id)
        self._events = events
        self._fail_startup = fail_startup
        self._fail_shutdown = fail_shutdown
        self._hang_startup = hang_startup
        self._hang_shutdown = hang_shutdown
        self._shutdown_delay = shutdown_delay
        self.startup_calls = 0
        self.shutdown_calls = 0

    async def startup(self) -> None:
        self.startup_calls += 1
        if self._hang_startup:
            await asyncio.sleep(3600)
        if self._fail_startup is not None:
            raise self._fail_startup
        self._events.append(f"start:{self.descriptor.module_id}")

    async def shutdown(self) -> None:
        self.shutdown_calls += 1
        if self._hang_shutdown:
            await asyncio.sleep(3600)
        if self._shutdown_delay:
            await asyncio.sleep(self._shutdown_delay)
        if self._fail_shutdown is not None:
            raise self._fail_shutdown
        self._events.append(f"stop:{self.descriptor.module_id}")


def _core(*modules: RecordingModule) -> GatewayCore:
    return compose_gateway_core(modules=list(modules))


FAST = ModuleLifecyclePolicy(startup_timeout=0.2, shutdown_timeout=0.2)


def _phase(mgr: ModuleLifecycleManager) -> ModuleLifecyclePhase:
    return mgr.phase


def _started(mgr: ModuleLifecycleManager) -> tuple[str, ...]:
    return mgr.started_module_ids


def test_startup_in_registration_order_and_shutdown_in_reverse() -> None:
    events: list[str] = []
    mods = [RecordingModule(n, events) for n in ("alpha", "beta", "gamma")]
    mgr = ModuleLifecycleManager(_core(*mods))

    async def run() -> None:
        assert _phase(mgr) is ModuleLifecyclePhase.CREATED
        await mgr.startup()
        assert _phase(mgr) is ModuleLifecyclePhase.STARTED
        assert _started(mgr) == ("alpha", "beta", "gamma")
        await mgr.shutdown()
        assert _phase(mgr) is ModuleLifecyclePhase.STOPPED
        assert _started(mgr) == ()

    asyncio.run(run())
    assert events == [
        "start:alpha",
        "start:beta",
        "start:gamma",
        "stop:gamma",
        "stop:beta",
        "stop:alpha",
    ]


def test_no_registered_modules_is_a_noop() -> None:
    mgr = ModuleLifecycleManager(compose_gateway_core())

    async def run() -> None:
        await mgr.startup()
        assert _phase(mgr) is ModuleLifecyclePhase.STARTED
        await mgr.shutdown()
        await mgr.shutdown()
        assert _phase(mgr) is ModuleLifecyclePhase.STOPPED

    asyncio.run(run())
    assert mgr.module_count == 0


def test_partial_startup_failure_rolls_back_started_modules_only() -> None:
    events: list[str] = []
    a = RecordingModule("alpha", events)
    b = RecordingModule("beta", events)
    bad = RecordingModule("bad_mod", events, fail_startup=RuntimeError(f"boom {SECRET}"))
    never = RecordingModule("never", events)
    mgr = ModuleLifecycleManager(_core(a, b, bad, never))

    async def run() -> None:
        with pytest.raises(ModuleStartupError) as info:
            await mgr.startup()
        err = info.value
        assert err.failure == ModuleFailure("bad_mod", "startup", "RuntimeError")
        assert err.rollback_failures == ()
        assert mgr.phase is ModuleLifecyclePhase.FAILED
        assert mgr.started_module_ids == ()
        # Failed manager is single-use (fail closed)
        with pytest.raises(InvalidModuleLifecycleStateError):
            await mgr.startup()
        # Subsequent shutdown is a harmless no-op and does not double-close.
        await mgr.shutdown()

    asyncio.run(run())
    assert events == ["start:alpha", "start:beta", "stop:beta", "stop:alpha"]
    assert (a.shutdown_calls, b.shutdown_calls) == (1, 1)
    # The module whose own startup failed is not asked to shut down; later ones never started.
    assert bad.shutdown_calls == 0
    assert never.startup_calls == 0


def test_rollback_continues_when_a_rollback_hook_fails() -> None:
    events: list[str] = []
    a = RecordingModule("alpha", events)
    b = RecordingModule("beta", events, fail_shutdown=ValueError(SECRET))
    c = RecordingModule("gamma", events)
    bad = RecordingModule("bad_mod", events, fail_startup=RuntimeError("x"))
    mgr = ModuleLifecycleManager(_core(a, b, c, bad))

    async def run() -> None:
        with pytest.raises(ModuleStartupError) as info:
            await mgr.startup()
        assert info.value.rollback_failures == (ModuleFailure("beta", "shutdown", "ValueError"),)
        assert SECRET not in str(info.value)

    asyncio.run(run())
    assert events == ["start:alpha", "start:beta", "start:gamma", "stop:gamma", "stop:alpha"]


def test_shutdown_continues_after_module_failure_and_reports_all_failures_once() -> None:
    events: list[str] = []
    a = RecordingModule("alpha", events)
    b = RecordingModule("beta", events, fail_shutdown=RuntimeError(SECRET))
    c = RecordingModule("gamma", events, fail_shutdown=KeyError("k"))
    mgr = ModuleLifecycleManager(_core(a, b, c))

    async def run() -> None:
        await mgr.startup()
        with pytest.raises(ModuleShutdownError) as info:
            await mgr.shutdown()
        assert info.value.failures == (
            ModuleFailure("gamma", "shutdown", "KeyError"),
            ModuleFailure("beta", "shutdown", "RuntimeError"),
        )
        assert mgr.phase is ModuleLifecyclePhase.STOPPED
        # Repeated shutdown: no re-raise, no double close
        await mgr.shutdown()
        await mgr.shutdown()

    asyncio.run(run())
    assert events == ["start:alpha", "start:beta", "start:gamma", "stop:alpha"]
    assert (a.shutdown_calls, b.shutdown_calls, c.shutdown_calls) == (1, 1, 1)
    assert len(mgr.shutdown_failures) == 2


def test_errors_do_not_disclose_module_exception_messages_or_chain_causes() -> None:
    events: list[str] = []
    bad = RecordingModule("vault", events, fail_startup=RuntimeError(f"key={SECRET}"))
    mgr = ModuleLifecycleManager(_core(bad))

    async def run() -> None:
        with pytest.raises(ModuleStartupError) as info:
            await mgr.startup()
        err = info.value
        for rendered in (str(err), repr(err), str(err.failure), repr(err.failure)):
            assert SECRET not in rendered
        assert "RuntimeError" in str(err)
        assert err.__cause__ is None
        assert err.__suppress_context__ is True

    asyncio.run(run())


def test_startup_timeout_is_bounded_and_rolls_back() -> None:
    events: list[str] = []
    a = RecordingModule("alpha", events)
    slow = RecordingModule("slow", events, hang_startup=True)
    mgr = ModuleLifecycleManager(
        _core(a, slow), policy=ModuleLifecyclePolicy(startup_timeout=0.05, shutdown_timeout=0.2)
    )

    async def run() -> None:
        started = time.monotonic()
        with pytest.raises(ModuleStartupError) as info:
            await mgr.startup()
        assert time.monotonic() - started < 2.0
        assert info.value.failure.timed_out is True
        assert info.value.failure.module_id == "slow"

    asyncio.run(run())
    assert events == ["start:alpha", "stop:alpha"]


def test_hook_raising_timeout_error_itself_is_not_reported_as_bounded_timeout() -> None:
    events: list[str] = []
    bad = RecordingModule("flaky", events, fail_startup=TimeoutError("upstream"))
    mgr = ModuleLifecycleManager(_core(bad))

    async def run() -> None:
        with pytest.raises(ModuleStartupError) as info:
            await mgr.startup()
        assert info.value.failure.timed_out is False
        assert info.value.failure.error_type == "TimeoutError"

    asyncio.run(run())


def test_shutdown_timeout_is_bounded_and_remaining_modules_still_close() -> None:
    events: list[str] = []
    a = RecordingModule("alpha", events)
    hang = RecordingModule("hang", events, hang_shutdown=True)
    c = RecordingModule("gamma", events)
    mgr = ModuleLifecycleManager(
        _core(a, hang, c), policy=ModuleLifecyclePolicy(startup_timeout=1.0, shutdown_timeout=0.05)
    )

    async def run() -> None:
        await mgr.startup()
        started = time.monotonic()
        with pytest.raises(ModuleShutdownError) as info:
            await mgr.shutdown()
        assert time.monotonic() - started < 2.0
        assert [f.timed_out for f in info.value.failures] == [True]
        assert info.value.failures[0].module_id == "hang"

    asyncio.run(run())
    assert events == ["start:alpha", "start:hang", "start:gamma", "stop:gamma", "stop:alpha"]


def test_cancellation_during_startup_rolls_back_and_propagates() -> None:
    events: list[str] = []
    a = RecordingModule("alpha", events)
    hang = RecordingModule("hang", events, hang_startup=True)
    mgr = ModuleLifecycleManager(_core(a, hang), policy=ModuleLifecyclePolicy(30.0, 30.0))

    async def run() -> None:
        task = asyncio.create_task(mgr.startup())
        await asyncio.sleep(0.05)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert mgr.phase is ModuleLifecyclePhase.FAILED
        assert mgr.started_module_ids == ()

    asyncio.run(run())
    assert events == ["start:alpha", "stop:alpha"]
    assert a.shutdown_calls == 1


def test_cancelled_shutdown_caller_does_not_abandon_cleanup() -> None:
    events: list[str] = []
    a = RecordingModule("alpha", events)
    b = RecordingModule("beta", events, shutdown_delay=0.1)
    mgr = ModuleLifecycleManager(_core(a, b))

    async def run() -> None:
        await mgr.startup()
        task = asyncio.create_task(mgr.shutdown())
        await asyncio.sleep(0.02)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        # A later shutdown joins the same bounded cleanup and completes it exactly once.
        await mgr.shutdown()
        assert mgr.started_module_ids == ()

    asyncio.run(run())
    assert events == ["start:alpha", "start:beta", "stop:beta", "stop:alpha"]
    assert (a.shutdown_calls, b.shutdown_calls) == (1, 1)


def test_concurrent_shutdown_calls_close_each_module_once() -> None:
    events: list[str] = []
    mods = [RecordingModule(n, events, shutdown_delay=0.02) for n in ("alpha", "beta")]
    mgr = ModuleLifecycleManager(_core(*mods))

    async def run() -> None:
        await mgr.startup()
        await asyncio.gather(mgr.shutdown(), mgr.shutdown(), mgr.shutdown())

    asyncio.run(run())
    assert [m.shutdown_calls for m in mods] == [1, 1]
    assert events[-2:] == ["stop:beta", "stop:alpha"]


def test_shutdown_before_startup_is_noop_and_blocks_later_start() -> None:
    events: list[str] = []
    m = RecordingModule("alpha", events)
    mgr = ModuleLifecycleManager(_core(m))

    async def run() -> None:
        await mgr.shutdown()
        assert mgr.phase is ModuleLifecyclePhase.STOPPED
        with pytest.raises(InvalidModuleLifecycleStateError):
            await mgr.startup()

    asyncio.run(run())
    assert m.startup_calls == 0 and m.shutdown_calls == 0


def test_shutdown_during_startup_is_rejected() -> None:
    events: list[str] = []
    hang = RecordingModule("hang", events, hang_startup=True)
    mgr = ModuleLifecycleManager(_core(hang), policy=ModuleLifecyclePolicy(30.0, 30.0))

    async def run() -> None:
        task = asyncio.create_task(mgr.startup())
        await asyncio.sleep(0.02)
        with pytest.raises(InvalidModuleLifecycleStateError):
            await mgr.shutdown()
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

    asyncio.run(run())


def test_async_context_manager_for_embedded_hosts() -> None:
    events: list[str] = []
    mods = [RecordingModule(n, events) for n in ("alpha", "beta")]
    core = _core(*mods)

    async def run() -> None:
        async with ModuleLifecycleManager(core) as mgr:
            assert _phase(mgr) is ModuleLifecyclePhase.STARTED
            events.append("serving")
        assert _phase(mgr) is ModuleLifecyclePhase.STOPPED

    asyncio.run(run())
    assert events == ["start:alpha", "start:beta", "serving", "stop:beta", "stop:alpha"]


def test_independent_managers_share_no_state() -> None:
    ev_a: list[str] = []
    ev_b: list[str] = []
    mod_a = RecordingModule("voice", ev_a)
    mod_b = RecordingModule("voice", ev_b, fail_startup=RuntimeError("b fails"))
    mgr_a = ModuleLifecycleManager(_core(mod_a))
    mgr_b = ModuleLifecycleManager(_core(mod_b))

    async def run() -> None:
        await mgr_a.startup()
        with pytest.raises(ModuleStartupError):
            await mgr_b.startup()
        assert mgr_a.phase is ModuleLifecyclePhase.STARTED
        assert mgr_b.phase is ModuleLifecyclePhase.FAILED
        await mgr_a.shutdown()

    asyncio.run(run())
    assert ev_a == ["start:voice", "stop:voice"]
    assert ev_b == []


def test_construction_has_no_side_effects_and_validates_inputs() -> None:
    events: list[str] = []
    m = RecordingModule("alpha", events)
    core = _core(m)
    ModuleLifecycleManager(core)
    assert m.startup_calls == 0 and events == []
    with pytest.raises(TypeError):
        ModuleLifecycleManager(object())  # type: ignore[arg-type]
    with pytest.raises(TypeError):
        ModuleLifecycleManager(core, policy=object())  # type: ignore[arg-type]


@pytest.mark.parametrize("bad", [0, -1.0, float("inf"), float("nan"), True, "5", 10**9])
def test_policy_rejects_unbounded_or_invalid_timeouts(bad: object) -> None:
    with pytest.raises(CoreConfigurationError):
        ModuleLifecyclePolicy(startup_timeout=bad)  # type: ignore[arg-type]
    with pytest.raises(CoreConfigurationError):
        ModuleLifecyclePolicy(shutdown_timeout=bad)  # type: ignore[arg-type]
