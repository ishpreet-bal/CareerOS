"""C5 reproductions that run against any version: public behaviour only (``submit``,
``run``, ``approve``), nothing introduced by C5 is imported.

Each failed on the baseline (``f1d954d``):

* L9  - a crash during planning left an objective that ``resume`` then marked failed;
* L9  - planning that took longer than a lease was taken for dead by another session,
        which abandoned (and charged) its call;
* N3  - answering a planning clarification did not lead to a plan;
* N5  - a plan whose dependency rows were not all written let a step run before its
        prerequisite had finished.
"""

from __future__ import annotations

import contextlib
import sqlite3
import threading
from typing import Any, ClassVar

from careeros.context import AppContext, open_context_at
from careeros.core.config import ModelProfile
from careeros.core.errors import SessionTerminatedError
from careeros.domain.models import ObjectiveStatus
from careeros.llm.adapters.mock import MockAdapter
from careeros.llm.adapters.scripted import ScriptedAdapter
from careeros.llm.contracts import ModelRequest, ModelResponse
from careeros.manager.manager import CareerManager, ObjectiveRequest
from careeros.manager.plans import PlaybookLibrary
from careeros.simulation import later
from tests.m3_helpers import book, model_step

TITLE = "Summarize my club notes"
INPUTS = {"text": "Robotics club notes about sensors and motors"}


class CrashInPlanning(MockAdapter):
    mode_label: ClassVar[str] = "mock"

    def call(self, profile: ModelProfile, request: ModelRequest) -> ModelResponse:
        if request.task_type == "manager.plan_novel":
            raise SessionTerminatedError("simulated crash during planning")
        return super().call(profile, request)


def test_crash_during_planning_is_recovered_not_failed(ctx: AppContext) -> None:
    with contextlib.suppress(SessionTerminatedError):
        CareerManager(ctx, adapter=CrashInPlanning()).submit(
            ObjectiveRequest(title=TITLE, inputs=INPUTS)
        )
    [row] = ctx.db.query("SELECT id FROM objectives")
    s = CareerManager(ctx, clock=later(10)).run(int(row["id"]))
    assert s.status is ObjectiveStatus.DONE, s.stop_reason  # baseline: FAILED


def test_slow_planning_is_not_mistaken_for_a_dead_session(ctx: AppContext) -> None:
    entered, release = threading.Event(), threading.Event()

    class Slow(MockAdapter):
        mode_label: ClassVar[str] = "mock"

        def call(self, profile: ModelProfile, request: ModelRequest) -> ModelResponse:
            if request.task_type == "manager.plan_novel" and not entered.is_set():
                entered.set()
                release.wait(30)
            return super().call(profile, request)

    other = CareerManager(ctx, playbooks=PlaybookLibrary([book("o", [model_step("m")])]))
    other_oid = other.submit(ObjectiveRequest(title="o", kind="o", inputs={"text": "x"}))
    done: dict[str, Any] = {}

    def plan() -> None:
        with open_context_at(ctx.config, ctx.data_dir) as c:
            done["oid"] = CareerManager(c, adapter=Slow()).submit(
                ObjectiveRequest(title=TITLE, inputs=INPUTS)
            )

    t = threading.Thread(target=plan, daemon=True)
    t.start()
    assert entered.wait(15)
    # Another session runs 3 minutes later (> the 120 s lease) and recovers what it can.
    CareerManager(ctx, playbooks=other.playbooks, clock=later(3)).run(other_oid)
    release.set()
    t.join(30)
    statuses = [
        r["status"] for r in ctx.db.query("SELECT status FROM model_calls WHERE purpose='planning'")
    ]
    assert "abandoned" not in statuses, statuses  # baseline: abandoned and charged
    plan = ctx.orchestration.current_plan(done["oid"])
    assert plan is not None and plan.source == "model"


def test_answering_a_clarification_leads_to_a_plan(ctx: AppContext) -> None:
    m = CareerManager(ctx, adapter=ScriptedAdapter({"manager.plan_novel": ["fail_quality"]}))
    oid = m.submit(ObjectiveRequest(title=TITLE, inputs=INPUTS))
    [req] = m.pending_requests(oid)
    assert req["kind"] == "clarification"
    try:
        m.approve(int(req["id"]), answer="three bullet points")  # type: ignore[call-arg]
    except TypeError:  # versions without clarification answers
        m.approve(int(req["id"]))
    s = CareerManager(ctx).run(oid)
    assert s.status is ObjectiveStatus.DONE, s.stop_reason  # baseline: FAILED, no plan


def test_a_step_never_runs_before_its_prerequisite_when_dependency_rows_are_missing(
    ctx: AppContext,
) -> None:
    observed: list[tuple[str, str]] = []

    class Watch(MockAdapter):
        mode_label: ClassVar[str] = "mock"

        def call(self, profile: ModelProfile, request: ModelRequest) -> ModelResponse:
            step = request.simulation.get("step")
            if step == "b" and request.task_type != "quality.review":
                conn = sqlite3.connect(str(ctx.data_dir / "careeros.db"))
                try:
                    a = conn.execute("SELECT status FROM tasks WHERE step='a'").fetchone()[0]
                finally:
                    conn.close()
                observed.append(("b started while a was", a))
            return super().call(profile, request)

    lib = PlaybookLibrary([book("two", [model_step("a"), model_step("b", deps=["a"])])])
    m = CareerManager(ctx, adapter=Watch(), playbooks=lib)
    oid = m.submit(ObjectiveRequest(title="two", kind="two", inputs={"text": "x"}))
    with ctx.db.transaction() as c:  # a partial installation: no dependency rows written
        c.execute("DELETE FROM task_dependencies")
    assert m.run(oid).status is ObjectiveStatus.DONE
    assert observed == [("b started while a was", "succeeded")], observed
