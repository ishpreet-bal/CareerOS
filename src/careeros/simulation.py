"""Offline demonstration scenarios for M2 (synthetic inputs, simulated models only)."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any

from careeros.context import AppContext
from careeros.core.errors import CareerOSError
from careeros.domain.models import Task
from careeros.llm.adapters.mock import MockAdapter
from careeros.llm.adapters.scripted import ScriptedAdapter
from careeros.llm.base import ModelAdapter
from careeros.manager.manager import CareerManager, ObjectiveRequest

SYNTHETIC_JOB = {
    "company": "Examplar AI (synthetic)",
    "job_title": "Machine Learning Intern",
    "text": (
        "Machine learning intern. Build Python data pipelines, train PyTorch models, "
        "evaluate model accuracy, and deploy embedded inference on microcontrollers. "
        "Python testing and clear writing required."
    ),
}

#: Forces a realistic sequence of failures so retry/escalation paths are visible.
ESCALATION_SCRIPT: dict[str, list[str]] = {
    "research.company": ["timeout"],  # transient -> retry same model
    "job.classify_role@mock-small": ["malformed", "invalid"],  # repair retry, then tier up
    "resume.tailor": ["low_confidence"],  # -> raise reasoning effort
    "quality.review": ["review:accept", "review:revise", "review:accept"],
}

SCENARIOS = ("standard", "escalation", "budget", "interrupt", "novel")


class SimulatedCrashError(CareerOSError):
    """Stands in for a session being terminated mid-task."""


@dataclass
class ScenarioResult:
    objective_id: int
    events: list[str]
    approvals: list[dict[str, Any]] = field(default_factory=list)


def _manager(
    ctx: AppContext, adapter: ModelAdapter, fault: Callable[[str, Task], None] | None = None
) -> CareerManager:
    return CareerManager(ctx, adapter=adapter, fault_hook=fault)


def run_scenario(ctx: AppContext, name: str) -> ScenarioResult:
    if ctx.db.dataset_kind != "synthetic":
        raise CareerOSError("Demo scenarios only run against a synthetic database.")
    request = ObjectiveRequest(
        title="Prepare application package for a synthetic posting",
        kind="demo_application_package",
        inputs=SYNTHETIC_JOB,
    )
    if name == "standard":
        m = _manager(ctx, MockAdapter())
        oid = m.submit(request)
        m.run(oid)
        return ScenarioResult(oid, m.events)

    if name == "escalation":
        m = _manager(ctx, ScriptedAdapter(ESCALATION_SCRIPT))
        oid = m.submit(request)
        m.run(oid)
        return ScenarioResult(oid, m.events)

    if name == "budget":
        # Enough budget for everything except the final *critical* review, which must not
        # be downgraded: the manager stops and asks; the simulated user approves.
        m = _manager(ctx, MockAdapter())
        oid = m.submit(ObjectiveRequest(**{**request.__dict__, "budget_usd": 0.12}))
        m.run(oid)
        asked = m.pending_requests(oid)
        events = list(m.events)
        for _ in range(5):
            pending = m.pending_requests(oid)
            if not pending:
                break
            req = pending[0]
            extra = round(float(req["details"].get("needed_usd", 0.05)) + 0.02, 4)
            events.append(
                f"*** USER APPROVES request {req['id']} ({req['kind']}): "
                f"+${extra:.4f} simulated budget ***"
            )
            m.approve(req["id"], extra_usd=extra)
            before = len(m.events)
            m.run(oid)
            events += m.events[before:]
        return ScenarioResult(oid, events, asked)

    if name == "interrupt":
        crashed: list[int] = []

        def crash_once(point: str, task: Task) -> None:
            if point == "after_model_response" and task.step == "tailor_resume" and not crashed:
                crashed.append(task.id or 0)
                raise SimulatedCrashError("session terminated after the model replied")

        m1 = _manager(ctx, MockAdapter(), crash_once)
        oid = m1.submit(request)
        crash_events: list[str] = []
        try:
            m1.run(oid)
        except SimulatedCrashError as exc:
            crash_events = [*m1.events, f"*** SESSION TERMINATED: {exc} ***"]
        m2 = _manager(ctx, MockAdapter())  # a new session / process
        m2.run(oid)
        return ScenarioResult(oid, [*crash_events, "*** NEW SESSION: resuming ***", *m2.events])

    if name == "novel":
        m = _manager(ctx, MockAdapter())
        oid = m.submit(
            ObjectiveRequest(
                title="Summarize what my robotics club notes say",
                inputs={"text": SYNTHETIC_JOB["text"]},
            )
        )
        m.run(oid)
        return ScenarioResult(oid, m.events, m.pending_requests(oid))

    raise CareerOSError(f"Unknown scenario {name!r}; choose from {', '.join(SCENARIOS)}")
