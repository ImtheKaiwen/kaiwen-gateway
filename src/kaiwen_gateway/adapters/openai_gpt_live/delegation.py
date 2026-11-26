"""Responses-delegation bookkeeping for the GPT-Live adapter (no I/O, no authority).

Wire contract (developers.openai.com/api/docs/guides/live-delegation, 2026-11-26):

* backend progress arrives as ``response.event`` envelopes carrying a nested Responses
  streaming event and the outer ``delegation_id``;
* completed function calls are read from nested ``response.output_item.done`` items
  (the forwarded ``response.completed`` snapshot has an empty ``output``);
* each call is answered with ``response.item.create`` (``function_call_output``) and the
  backend resumes only after a ``response.create`` once *every* call has an output.

A batch is therefore continued exactly once, after the nested response completed (so all
of its calls are known) and every call in it has an output.
"""

from __future__ import annotations

from collections import OrderedDict
from dataclasses import dataclass, field
from typing import Final

_MAX_BATCHES: Final[int] = 64


@dataclass(slots=True)
class DelegationBatch:
    """Function calls of one backend response."""

    delegation_id: str
    response_id: str | None = None
    call_ids: set[str] = field(default_factory=set)
    pending: set[str] = field(default_factory=set)
    completed: bool = False
    continued: bool = False

    def ready_to_continue(self) -> bool:
        return self.completed and bool(self.call_ids) and not self.pending and not self.continued


class DelegationLedger:
    """Tracks the current backend response per delegation (bounded)."""

    def __init__(self) -> None:
        self._current: OrderedDict[str, DelegationBatch] = OrderedDict()
        self._by_call: dict[str, DelegationBatch] = {}

    def start(self, delegation_id: str, response_id: str | None) -> DelegationBatch:
        batch = DelegationBatch(delegation_id=delegation_id, response_id=response_id)
        self._current[delegation_id] = batch
        self._current.move_to_end(delegation_id)
        while len(self._current) > _MAX_BATCHES:
            _, evicted = self._current.popitem(last=False)
            for call_id in evicted.call_ids:
                self._by_call.pop(call_id, None)
        return batch

    def current(self, delegation_id: str) -> DelegationBatch:
        batch = self._current.get(delegation_id)
        if batch is None or batch.continued:
            batch = self.start(delegation_id, None)
        return batch

    def add_call(self, delegation_id: str, call_id: str) -> DelegationBatch:
        batch = self.current(delegation_id)
        batch.call_ids.add(call_id)
        batch.pending.add(call_id)
        self._by_call[call_id] = batch
        return batch

    def answer(self, call_id: str) -> DelegationBatch | None:
        batch = self._by_call.get(call_id)
        if batch is not None:
            batch.pending.discard(call_id)
        return batch


__all__ = ["DelegationBatch", "DelegationLedger"]
