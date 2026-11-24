"""Shared helpers for Phase B (ownership, fencing, late replies) tests."""

from __future__ import annotations

import threading
from typing import Any, ClassVar

from careeros.context import AppContext
from careeros.core.config import AppConfig, ModelProfile
from careeros.domain.models import ObjectiveStatus
from careeros.llm.adapters.mock import MockAdapter
from careeros.llm.contracts import ModelRequest, ModelResponse, ModelTimeoutError
from careeros.manager.manager import CareerManager, ObjectiveRequest
from careeros.manager.plans import Playbook, PlaybookLibrary


class BlockingAdapter(MockAdapter):
    """Mock adapter whose calls for ``block_steps`` wait (inside the 'provider') until
    ``release`` is set; ``entered`` is set when such a call starts. ``fail_late`` makes a
    released call fail with a timeout instead of replying."""

    mode_label: ClassVar[str] = "mock"

    def __init__(
        self, block_steps: set[str], *, fail_late: bool = False, timeout: float = 15.0
    ) -> None:
        self.block_steps = set(block_steps)
        self.release = threading.Event()
        self.entered = threading.Event()
        self.fail_late = fail_late
        self.timeout = timeout

    def call(self, profile: ModelProfile, request: ModelRequest) -> ModelResponse:
        step = request.simulation.get("step")
        if step in self.block_steps and request.task_type != "quality.review":
            self.block_steps.discard(step)
            self.entered.set()
            self.release.wait(self.timeout)
            if self.fail_late:
                raise ModelTimeoutError("late failure", input_tokens=100, latency_ms=5)
        return super().call(profile, request)


def one_step_book(
    name: str = "one", task_type: str = "research.company", **step: Any
) -> PlaybookLibrary:
    return PlaybookLibrary(
        [
            Playbook.model_validate(
                {
                    "name": name,
                    "plan": {
                        "steps": [
                            {
                                "id": "m",
                                "worker": "model_step",
                                "task_type": task_type,
                                "output": {"required": ["summary"]},
                                **step,
                            }
                        ]
                    },
                }
            )
        ]
    )


def submit(m: CareerManager, kind: str = "one") -> int:
    return m.submit(
        ObjectiveRequest(title=kind, kind=kind, inputs={"text": "robotics firmware intern sensors"})
    )


def with_limits(cfg: AppConfig, **limits: Any) -> AppConfig:
    return cfg.model_copy(update={"limits": cfg.limits.model_copy(update=limits)})


def ledger_consistent(ctx: AppContext, objective_id: int) -> tuple[float, float]:
    """(objective budget spent, sum of call costs) for an objective."""
    line = ctx.db.query_one(
        "SELECT spent_usd FROM budgets WHERE scope='objective' AND scope_ref=?",
        (str(objective_id),),
    )
    total = ctx.db.query_one(
        "SELECT COALESCE(SUM(cost_usd),0) AS s FROM model_calls WHERE objective_id=?",
        (objective_id,),
    )["s"]
    return float(line["spent_usd"] if line else 0.0), float(total)


def done(ctx: AppContext, objective_id: int) -> bool:
    return ctx.orchestration.get_objective(objective_id).status is ObjectiveStatus.DONE
