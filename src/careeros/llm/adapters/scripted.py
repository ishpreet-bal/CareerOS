"""Scripted simulator: forces specific outcomes to test retries and escalation.

A script maps a key to a queue of directives, consumed one per call. Keys are matched in
this order: ``"<task_type>@<model_id>"``, ``"<task_type>"``, ``"*"``. When no directive
applies, behaviour falls back to the seeded :class:`MockAdapter`.

Directives: ``success``, ``success:<quality>``, ``low_confidence``, ``fail_quality``,
``malformed``, ``invalid``, ``timeout``, ``rate_limit``, ``review:<accept|revise|escalate>``.
"""

from __future__ import annotations

from collections import deque
from typing import Any, ClassVar

from careeros.core.config import Effort, ModelProfile
from careeros.llm.adapters.mock import MockAdapter, Outcome
from careeros.llm.contracts import ModelRequest, ModelResponse

_KINDS = {
    "success",
    "low_confidence",
    "fail_quality",
    "malformed",
    "invalid",
    "timeout",
    "rate_limit",
}


def parse_directive(text: str) -> Outcome:
    head, _, arg = text.partition(":")
    if head == "review":
        if arg not in ("accept", "revise", "escalate"):
            raise ValueError(f"Bad review directive {text!r}")
        return Outcome("success", review_verdict=arg)
    if head not in _KINDS:
        raise ValueError(f"Unknown directive {text!r}")
    return Outcome(head, quality=float(arg) if arg else None)  # type: ignore[arg-type]


class ScriptedAdapter:
    """Serves mock-provider profiles with scripted outcomes."""

    provider: ClassVar[str] = "mock"
    offline: ClassVar[bool] = True
    mode_label: ClassVar[str] = "scripted"

    def __init__(self, script: dict[str, list[str]] | None = None) -> None:
        self._mock = MockAdapter()
        self._queues: dict[str, deque[Outcome]] = {
            k: deque(parse_directive(d) for d in v) for k, v in (script or {}).items()
        }
        self.calls: list[tuple[str, str, str]] = []  # (task_type, model_id, effort)

    def map_effort(self, effort: Effort) -> dict[str, Any]:
        return self._mock.map_effort(effort)

    def call(self, profile: ModelProfile, request: ModelRequest) -> ModelResponse:
        self.calls.append((request.task_type, profile.id, request.effort))
        forced = None
        for key in (f"{request.task_type}@{profile.id}", request.task_type, "*"):
            q = self._queues.get(key)
            if q:
                forced = q.popleft()
                break
        return self._mock.simulate(profile, request, forced=forced)
