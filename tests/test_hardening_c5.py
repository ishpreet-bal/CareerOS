"""Hardening C5: planning safety (audit L9, findings N3 and N5).

Planning runs as an owned task of kind ``planning`` under a planning-phase plan version;
installing the real plan supersedes that version atomically with the plan's tasks and
dependency rows. In-process tests use explicit synchronisation; process-level tests kill
or race separate planner processes.
"""

from __future__ import annotations

import json
import multiprocessing as mp
import sqlite3
import threading
import time
from collections.abc import Callable
from pathlib import Path
from typing import Any, ClassVar

import pytest

import careeros.core.db as dbmod
from careeros.context import AppContext, open_context_at
from careeros.core.config import AppConfig, ModelProfile
from careeros.core.db import Database
from careeros.core.errors import CareerOSError, SessionTerminatedError
from careeros.domain.models import Objective, ObjectiveStatus
from careeros.domain.repositories import OrchestrationRepository
from careeros.llm.adapters.mock import MockAdapter
from careeros.llm.adapters.scripted import ScriptedAdapter
from careeros.llm.contracts import ModelRequest, ModelResponse
from careeros.manager.manager import CareerManager, ObjectiveRequest, RunSummary
from careeros.manager.plans import PlaybookLibrary
from careeros.orchestration.fencing import claim
from careeros.simulation import later
from tests import mp_workers
from tests.m2_helpers import calls, task_by_step
from tests.phase_b_helpers import ledger_consistent, with_limits

SPAWN = mp.get_context("spawn")
TITLE = "Summarize my club notes"
INPUTS = {"text": "Robotics club notes about sensors and motors"}
FAR, NOW = "2099-01-01T00:00:00+00:00", "2026-01-01T00:00:00+00:00"


# ------------------------------------------------------------------------- helpers
def _novel(ctx: AppContext) -> int:
    """A novel objective in its planning phase, not yet run."""
    m = CareerManager(ctx)
    draft = Objective(
        kind="novel",
        title=TITLE,
        inputs=INPUTS,
        definition_of_done=["accepted:all_required"],
        budget_usd=2.0,
    )
    with ctx.db.transaction() as c:
        oid = m.repo.create_objective_in(c, draft)
        m._start_planning_in(c, draft.model_copy(update={"id": oid}), "test: novel objective")
    return oid


def _planning_task(ctx: AppContext, oid: int) -> dict[str, Any]:
    r = ctx.db.query_one("SELECT * FROM tasks WHERE objective_id=? AND kind='planning'", (oid,))
    assert r is not None
    return dict(r)


def _plans(ctx: AppContext, oid: int) -> list[tuple[int, str, bool]]:
    return [
        (int(r["version"]), str(r["source"]), r["superseded_at"] is None)
        for r in ctx.db.query(
            "SELECT version, source, superseded_at FROM plans WHERE objective_id=? ORDER BY id",
            (oid,),
        )
    ]


def _step_tasks(ctx: AppContext, oid: int) -> int:
    return int(
        ctx.db.query_one(
            "SELECT COUNT(*) n FROM tasks WHERE objective_id=? AND kind='step'", (oid,)
        )["n"]
    )


def _ledger_ok(ctx: AppContext, oid: int) -> None:
    spent, total = ledger_consistent(ctx, oid)
    assert spent == pytest.approx(total)
    assert ctx.db.query("SELECT * FROM budget_reservations WHERE status='held'") == []


def _poll(pred: Callable[[], bool], timeout: float = 15.0) -> bool:
    end = time.monotonic() + timeout
    while time.monotonic() < end:
        if pred():
            return True
        time.sleep(0.02)
    return pred()


class PlanHold(MockAdapter):
    """The first planning call waits inside the 'provider' until ``release``."""

    mode_label: ClassVar[str] = "mock"

    def __init__(self, timeout: float = 30.0) -> None:
        self.entered, self.release = threading.Event(), threading.Event()
        self.timeout, self.held = timeout, False
        self.prompts: list[str] = []

    def call(self, profile: ModelProfile, request: ModelRequest) -> ModelResponse:
        if request.task_type == "manager.plan_novel":
            self.prompts.append(request.prompt)
            if not self.held:
                self.held = True
                self.entered.set()
                self.release.wait(self.timeout)
        return super().call(profile, request)


class Runner:
    def __init__(
        self, ctx: AppContext, factory: Callable[[AppContext], CareerManager], **run: Any
    ) -> None:
        self.ready = threading.Event()
        self.manager: CareerManager | None = None
        self.summary: RunSummary | None = None
        self.error: BaseException | None = None
        self.run_kw = run

        def go() -> None:
            try:
                with open_context_at(ctx.config, ctx.data_dir) as c:
                    self.manager = factory(c)
                    self.ready.set()
                    self.summary = self.manager.run(**self.run_kw)
            except BaseException as exc:
                self.error = exc
                self.ready.set()

        self.thread = threading.Thread(target=go, daemon=True)
        self.thread.start()
        assert self.ready.wait(15)

    def join(self, timeout: float = 30.0) -> RunSummary:
        self.thread.join(timeout)
        assert not self.thread.is_alive(), "run did not return"
        assert self.error is None, repr(self.error)
        assert self.summary is not None
        return self.summary


# ============================================================ L9: owned planning phase
def test_submit_runs_planning_as_an_owned_task(ctx: AppContext) -> None:
    m = CareerManager(ctx)
    oid = m.submit(ObjectiveRequest(title=TITLE, inputs=INPUTS))
    pt = _planning_task(ctx, oid)
    assert (pt["status"], pt["attempt"], pt["owner_generation"]) == ("succeeded", 1, 1)
    assert _plans(ctx, oid) == [(1, "planning", False), (2, "model", True)]
    [call] = calls(ctx, purpose="planning")
    assert call["task_id"] == pt["id"] and call["owner_generation"] == 1
    assert call["idempotency_key"] == f"task:{pt['id']}:a1:planning:0"
    sess = ctx.db.query_one("SELECT * FROM execution_sessions ORDER BY rowid DESC LIMIT 1")
    assert sess["ended_at"] is not None and sess["end_reason"] == "plan installed"
    assert m.run(oid).status is ObjectiveStatus.DONE
    _ledger_ok(ctx, oid)


def test_planning_longer_than_a_lease_is_kept_alive_by_the_heartbeat(ctx: AppContext) -> None:
    ctx.config = with_limits(ctx.config, lease_seconds=2, heartbeat_seconds=0.05)
    oid = _novel(ctx)
    hold = PlanHold()
    r = Runner(ctx, lambda c: CareerManager(c, adapter=hold), objective_id=oid, planning_only=True)
    assert hold.entered.wait(15)
    other = CareerManager(ctx)  # honest clock: would take over a dead planner
    sid = other.repo.start_session()
    end = time.monotonic() + 3.0  # longer than the 2 s lease
    while time.monotonic() < end:
        other._recover(oid, sid)
        time.sleep(0.1)
    hold.release.set()
    s = r.join()
    assert s.stop_reason == "plan installed"
    pt = _planning_task(ctx, oid)
    assert (pt["status"], pt["owner_generation"]) == ("succeeded", 1)  # never taken over
    assert ctx.db.query("SELECT * FROM manager_decisions WHERE decision='recover'") == []
    assert [c["status"] for c in calls(ctx, purpose="planning")] == ["ok"]


def test_failed_heartbeat_halts_planning_and_recovery_replays_the_call(
    ctx: AppContext, monkeypatch: pytest.MonkeyPatch
) -> None:
    ctx.config = with_limits(
        ctx.config, lease_seconds=4, heartbeat_seconds=0.05, lease_safety_margin_seconds=1.5
    )
    oid = _novel(ctx)
    monkeypatch.setattr(CareerManager, "_renew_leases", lambda self, db, s, st: False)
    hold = PlanHold()
    r = Runner(ctx, lambda c: CareerManager(c, adapter=hold), objective_id=oid, planning_only=True)
    assert hold.entered.wait(15)
    assert r.manager is not None
    assert _poll(r.manager._lease_lost.is_set)  # halts at lease - margin = 2.5 s ...
    hold.release.set()  # ... and the reply still lands while the 4 s lease is valid
    s = r.join()
    assert s.stop_reason == "halted: lease renewal failed"
    pt = _planning_task(ctx, oid)
    assert (pt["status"], pt["attempt"], pt["attempt_open"]) == ("running", 1, 1)
    assert [c["status"] for c in calls(ctx, purpose="planning")] == ["ok"]
    monkeypatch.undo()
    s2 = CareerManager(ctx, clock=later(10)).run(oid)
    assert s2.status is ObjectiveStatus.DONE
    assert [c["status"] for c in calls(ctx, purpose="planning")] == ["ok"]  # replayed
    pt = _planning_task(ctx, oid)
    assert (pt["status"], pt["attempt"], pt["owner_generation"]) == ("succeeded", 1, 2)
    _ledger_ok(ctx, oid)


def test_stale_planner_cannot_install_a_plan(ctx: AppContext) -> None:
    """Planner A is stuck in its provider call; B takes planning over, installs a plan
    and finishes the objective. A's reply is late: A installs nothing."""
    oid = _novel(ctx)
    hold = PlanHold()
    r = Runner(ctx, lambda c: CareerManager(c, adapter=hold), objective_id=oid, planning_only=True)
    assert hold.entered.wait(15)
    s_b = CareerManager(ctx).run(oid, force_recover=True)
    assert s_b.status is ObjectiveStatus.DONE
    plans_before = _plans(ctx, oid)
    hold.release.set()
    r.join()
    assert _plans(ctx, oid) == plans_before == [(1, "planning", False), (2, "model", True)]
    assert [c["status"] for c in calls(ctx, purpose="planning")] == ["abandoned", "ok"]
    assert len(ctx.db.query("SELECT * FROM late_replies")) == 1
    assert _planning_task(ctx, oid)["owner_generation"] == 2
    assert len(ctx.db.query("SELECT * FROM manager_decisions WHERE decision='plan'")) == 1
    _ledger_ok(ctx, oid)


def test_graceful_shutdown_during_planning(ctx: AppContext) -> None:
    oid = _novel(ctx)
    hold = PlanHold()
    r = Runner(ctx, lambda c: CareerManager(c, adapter=hold), objective_id=oid, planning_only=True)
    assert hold.entered.wait(15)
    assert r.manager is not None and r.manager.request_cancel("test") is True
    hold.release.set()
    s = r.join()
    assert s.stop_reason == "cancelled: test"
    pt = _planning_task(ctx, oid)
    assert (pt["status"], pt["attempt"], pt["attempt_open"], pt["lease_owner"]) == (
        "ready",
        1,
        1,
        None,
    )
    assert _plans(ctx, oid) == [(1, "planning", True)]  # nothing installed
    s2 = CareerManager(ctx).run(oid)
    assert s2.status is ObjectiveStatus.DONE
    assert [c["status"] for c in calls(ctx, purpose="planning")] == ["ok"]  # replayed


# ======================================================== N5: atomic installation
def test_crash_inside_installation_leaves_no_partial_plan(
    ctx: AppContext, monkeypatch: pytest.MonkeyPatch
) -> None:
    oid = _novel(ctx)
    orig = OrchestrationRepository.add_task_in.__func__  # type: ignore[attr-defined]
    count = [0]

    def crashing(cls: Any, c: sqlite3.Connection, task: Any) -> int:
        count[0] += 1
        if count[0] == 2:  # simulated process death inside the installing transaction
            raise SessionTerminatedError("crash half-way through installation")
        return int(orig(cls, c, task))

    monkeypatch.setattr(OrchestrationRepository, "add_task_in", classmethod(crashing))
    with pytest.raises(SessionTerminatedError):
        CareerManager(ctx).run(oid, planning_only=True)
    assert count[0] == 2
    assert _plans(ctx, oid) == [(1, "planning", True)]  # the previous valid (planning) plan
    assert _step_tasks(ctx, oid) == 0
    assert ctx.db.query("SELECT * FROM task_dependencies") == []
    assert [c["status"] for c in calls(ctx, purpose="planning")] == ["ok"]
    monkeypatch.undo()
    assert CareerManager(ctx, clock=later(10)).run(oid).status is ObjectiveStatus.DONE
    assert _plans(ctx, oid) == [(1, "planning", False), (2, "model", True)]
    assert [c["status"] for c in calls(ctx, purpose="planning")] == ["ok"]  # replayed
    _ledger_ok(ctx, oid)


def test_a_failed_playbook_submission_leaves_nothing(
    ctx: AppContext, monkeypatch: pytest.MonkeyPatch
) -> None:
    from careeros.simulation import SYNTHETIC_JOB

    orig = OrchestrationRepository.add_task_in.__func__  # type: ignore[attr-defined]
    count = [0]

    def failing(cls: Any, c: sqlite3.Connection, task: Any) -> int:
        count[0] += 1
        if count[0] == 3:
            raise RuntimeError("simulated failure during installation")
        return int(orig(cls, c, task))

    monkeypatch.setattr(OrchestrationRepository, "add_task_in", classmethod(failing))
    with pytest.raises(RuntimeError):
        CareerManager(ctx).submit(
            ObjectiveRequest(title="x", kind="demo_application_package", inputs=SYNTHETIC_JOB)
        )
    for table in ("objectives", "plans", "tasks", "task_dependencies"):
        assert ctx.db.query(f"SELECT * FROM {table}") == [], table


def _two_step_playbook(ctx: AppContext) -> tuple[CareerManager, int]:
    from tests.m3_helpers import book, model_step

    m = CareerManager(
        ctx,
        playbooks=PlaybookLibrary([book("two", [model_step("a"), model_step("b", deps=["a"])])]),
    )
    oid = m.submit(ObjectiveRequest(title="two", kind="two", inputs={"text": "x"}))
    return m, oid


def _claim(ctx: AppContext, task_id: int) -> Any:
    sid = CareerManager(ctx).repo.start_session()
    with ctx.db.transaction() as c:
        return claim(c, task_id, sid, FAR, NOW)


def test_tasks_with_missing_dependency_records_cannot_run(ctx: AppContext) -> None:
    """As older code could leave after a crash mid-installation: b's dependency row on a
    is missing. b is not claimable even once a has succeeded; the next run completes the
    installation first and then runs a before b."""
    m, oid = _two_step_playbook(ctx)
    a, b = (int(str(task_by_step(ctx, oid, s)["id"])) for s in "ab")
    with ctx.db.transaction() as c:
        c.execute("DELETE FROM task_dependencies")
        c.execute("UPDATE tasks SET status='succeeded' WHERE id=?", (a,))
    assert _claim(ctx, b) is None  # incomplete dependency rows
    assert [t.id for t in m._ready_tasks(oid)] == []
    with ctx.db.transaction() as c:
        c.execute("UPDATE tasks SET status='pending' WHERE id=?", (a,))
    s = m.run(oid)
    assert s.status is ObjectiveStatus.DONE
    rows = ctx.db.query(
        "SELECT inputs FROM manager_decisions WHERE decision='plan' AND rationale LIKE"
        " 'completed an incomplete%'"
    )
    assert len(rows) == 1 and json.loads(rows[0]["inputs"])["rows_created"] == 1
    starts = {s_: task_by_step(ctx, oid, s_)["started_at"] for s_ in "ab"}  # a ran before b
    assert str(starts["a"]) <= str(starts["b"])


def test_a_missing_task_row_is_recreated_before_done(ctx: AppContext) -> None:
    m, oid = _two_step_playbook(ctx)
    b = int(str(task_by_step(ctx, oid, "b")["id"]))
    with ctx.db.transaction() as c:  # the installation never wrote task b
        c.execute("DELETE FROM task_dependencies WHERE task_id=?", (b,))
        c.execute("DELETE FROM tasks WHERE id=?", (b,))
    obj = m.repo.get_objective(oid)
    with ctx.db.transaction() as c:  # even with a succeeded, the plan is not done
        c.execute("UPDATE tasks SET status='succeeded' WHERE objective_id=?", (oid,))
    assert m._done(obj) is False
    with ctx.db.transaction() as c:
        c.execute("UPDATE tasks SET status='pending' WHERE objective_id=?", (oid,))
    assert m.run(oid).status is ObjectiveStatus.DONE
    assert {str(task_by_step(ctx, oid, s_)["status"]) for s_ in "ab"} == {"succeeded"}


def test_tasks_of_an_outdated_plan_version_cannot_run(ctx: AppContext) -> None:
    m, oid = _two_step_playbook(ctx)
    a = int(str(task_by_step(ctx, oid, "a")["id"]))
    with ctx.db.transaction() as c:
        cur = c.execute(
            "INSERT INTO plans (objective_id, version, source, graph, superseded_at,"
            " created_at) VALUES (?, 99, 'old', '{}', ?, ?)",
            (oid, NOW, NOW),
        )
        c.execute("UPDATE tasks SET plan_id=? WHERE id=?", (cur.lastrowid, a))
    assert _claim(ctx, a) is None
    assert all(t.id != a for t in m._ready_tasks(oid))
    current = m.repo.current_plan(oid)
    assert current is not None
    with ctx.db.transaction() as c:
        c.execute("UPDATE tasks SET plan_id=? WHERE id=?", (current.id, a))
    assert _claim(ctx, a) is not None


def test_the_planning_task_cannot_run_once_a_plan_is_installed(ctx: AppContext) -> None:
    m = CareerManager(ctx)
    oid = m.submit(ObjectiveRequest(title=TITLE, inputs=INPUTS))
    pt = _planning_task(ctx, oid)
    with ctx.db.transaction() as c:  # even if something reset it
        c.execute("UPDATE tasks SET status='ready' WHERE id=?", (pt["id"],))
    assert _claim(ctx, int(pt["id"])) is None  # its plan version is no longer current


def test_one_current_plan_per_objective_is_enforced_by_the_database(ctx: AppContext) -> None:
    _m, oid = _two_step_playbook(ctx)
    with pytest.raises(sqlite3.IntegrityError), ctx.db.transaction() as c:
        c.execute(
            "INSERT INTO plans (objective_id, version, source, graph, created_at)"
            " VALUES (?, 50, 'competing', '{}', ?)",
            (oid, NOW),
        )
    assert [p[2] for p in _plans(ctx, oid)].count(True) == 1


# ============================================================= N3: clarification
class Recording(ScriptedAdapter):
    def __init__(self, script: dict[str, list[str]], crash_on: int | None = None) -> None:
        super().__init__(script)
        self.prompts: list[str] = []
        self.crash_on = crash_on

    def call(self, profile: ModelProfile, request: ModelRequest) -> ModelResponse:
        if request.task_type == "manager.plan_novel":
            self.prompts.append(request.prompt)
            if self.crash_on is not None and len(self.prompts) == self.crash_on:
                raise SessionTerminatedError("simulated crash in a planning call")
        return super().call(profile, request)


def _clarification(m: CareerManager, oid: int) -> int:
    [req] = m.pending_requests(oid)
    assert req["kind"] == "clarification" and req["task_id"] is not None
    return int(req["id"])


def test_clarification_answer_resumes_planning(ctx: AppContext) -> None:
    # One invalid plan exhausts the attempt (no stronger tier to escalate to).
    adapter = Recording({"manager.plan_novel": ["fail_quality"]})
    m = CareerManager(ctx, adapter=adapter)
    oid = m.submit(ObjectiveRequest(title=TITLE, inputs=INPUTS))
    assert m.repo.get_objective(oid).status is ObjectiveStatus.AWAITING_HUMAN
    rid = _clarification(m, oid)
    with pytest.raises(CareerOSError, match="answer"):
        m.approve(rid)
    m.approve(rid, answer="Summarise the robotics notes into three bullet points")
    r = ctx.db.query_one("SELECT status, answer FROM approval_requests WHERE id=?", (rid,))
    assert (r["status"], r["answer"]) == (
        "approved",
        "Summarise the robotics notes into three bullet points",
    )
    s = m.run(oid)
    assert s.status is ObjectiveStatus.DONE, s.stop_reason
    assert "Summarise the robotics notes" in adapter.prompts[-1]  # planning used the answer
    pt = _planning_task(ctx, oid)
    assert (pt["status"], pt["attempt"]) == ("succeeded", 2)
    assert _plans(ctx, oid)[-1][1:] == ("model", True)
    _ledger_ok(ctx, oid)


def test_repeated_clarification_and_interrupted_replanning(ctx: AppContext) -> None:
    adapter = Recording({"manager.plan_novel": ["fail_quality"] * 2}, crash_on=3)
    m = CareerManager(ctx, adapter=adapter)
    oid = m.submit(ObjectiveRequest(title=TITLE, inputs=INPUTS))
    m.approve(_clarification(m, oid), answer="first answer")
    s = m.run(oid)  # second attempt fails too: asks again
    assert s.status is ObjectiveStatus.AWAITING_HUMAN
    m.approve(_clarification(m, oid), answer="second answer")
    with pytest.raises(SessionTerminatedError):  # replanning interrupted mid-call
        m.run(oid)
    pt = _planning_task(ctx, oid)
    assert (pt["status"], pt["attempt"]) == ("running", 3)
    recovering = Recording({"manager.plan_novel": ["success"]})  # deterministic, not seeded
    s = CareerManager(ctx, adapter=recovering, clock=later(10)).run(oid)
    assert s.status is ObjectiveStatus.DONE, s.stop_reason
    pt = _planning_task(ctx, oid)
    assert (pt["status"], pt["attempt"], pt["owner_generation"]) == ("succeeded", 3, 4)
    final = [c for c in calls(ctx, purpose="planning") if c["status"] == "ok"][-1]
    assert final["attempt"] == 3
    assert "first answer" in recovering.prompts[-1] and "second answer" in recovering.prompts[-1]
    assert [c["status"] for c in calls(ctx, purpose="planning")].count("abandoned") == 1
    _ledger_ok(ctx, oid)


# ===================================================== separate planner processes
@pytest.mark.parametrize("point", mp_workers.PLAN_CRASH_POINTS)
def test_planner_process_killed_then_recovered(ctx: AppContext, point: str) -> None:
    oid = _novel(ctx)
    q = SPAWN.Queue()
    p = SPAWN.Process(target=mp_workers.plan_and_die, args=(str(ctx.data_dir), oid, point, q))
    p.start()
    res = q.get(timeout=60)
    p.join(60)
    assert res.get("died_at") == point and p.exitcode == 17, res
    assert _plans(ctx, oid) == [(1, "planning", True)]  # still planning: nothing partial
    assert _step_tasks(ctx, oid) == 0 and ctx.db.query("SELECT * FROM task_dependencies") == []
    statuses = [c["status"] for c in calls(ctx, purpose="planning")]
    assert statuses == (["in_flight"] if point == "in_call" else ["ok"])
    s = CareerManager(ctx, clock=later(10)).run(oid)
    assert s.status is ObjectiveStatus.DONE, s.stop_reason
    expect = ["abandoned", "ok"] if point == "in_call" else ["ok"]  # no duplicate billing
    assert [c["status"] for c in calls(ctx, purpose="planning")] == expect
    pt = _planning_task(ctx, oid)
    assert (pt["status"], pt["attempt"], pt["owner_generation"]) == ("succeeded", 1, 2)
    assert _plans(ctx, oid) == [(1, "planning", False), (2, "model", True)]
    _ledger_ok(ctx, oid)


def test_heartbeat_failure_in_a_separate_planner_process(ctx: AppContext) -> None:
    oid = _novel(ctx)
    entered, release, q = SPAWN.Event(), SPAWN.Event(), SPAWN.Queue()
    limits = {"lease_seconds": 4, "heartbeat_seconds": 0.05, "lease_safety_margin_seconds": 1.5}
    p = SPAWN.Process(
        target=mp_workers.plan_with_hold,
        args=(str(ctx.data_dir), oid, entered, release, q, limits, True, 3.0),
    )
    p.start()
    res = q.get(timeout=60)
    p.join(60)
    assert "error" not in res, res.get("error")
    assert res["stop_reason"] == "halted: lease renewal failed" and res["lease_lost"] is True
    assert _planning_task(ctx, oid)["status"] == "running"
    assert CareerManager(ctx, clock=later(10)).run(oid).status is ObjectiveStatus.DONE
    assert [c["status"] for c in calls(ctx, purpose="planning")] == ["ok"]


def test_concurrent_planner_processes_install_one_plan(ctx: AppContext) -> None:
    oid = _novel(ctx)
    n = 4
    barrier, q = SPAWN.Barrier(n), SPAWN.Queue()
    procs = [
        SPAWN.Process(
            target=mp_workers.plan_concurrently, args=(str(ctx.data_dir), oid, barrier, q)
        )
        for _ in range(n)
    ]
    for p in procs:
        p.start()
    results = [q.get(timeout=60) for _ in procs]
    for p in procs:
        p.join(60)
    assert not [r for r in results if "error" in r], results
    assert sorted(r["dispatches"] for r in results) == [0] * (n - 1) + [1]
    assert _plans(ctx, oid) == [(1, "planning", False), (2, "model", True)]
    assert [c["status"] for c in calls(ctx, purpose="planning")] == ["ok"]
    assert len(ctx.db.query("SELECT * FROM manager_decisions WHERE decision='plan'")) == 1


# ======================================================================= migration
def _v5(path: Path, monkeypatch: pytest.MonkeyPatch) -> Database:
    orig = dbmod.load_migrations
    monkeypatch.setattr(dbmod, "load_migrations", lambda: [m for m in orig() if m.version <= 5])
    db = Database.open(path, dataset_kind="synthetic")
    monkeypatch.setattr(dbmod, "load_migrations", orig)
    assert db.schema_version() == 5
    return db


def _graph(*steps: tuple[str, list[str]]) -> str:
    return json.dumps(
        {
            "steps": [
                {
                    "id": s,
                    "worker": "model_step",
                    "task_type": "research.company",
                    "depends_on": d,
                    "output": {"required": ["summary"]},
                }
                for s, d in steps
            ],
            "waves": [[s] for s, _ in steps],
            "skipped": [],
        }
    )


def test_migration_backfills_plan_association_dependencies_and_one_current_plan(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    path = tmp_path / "v5.db"
    db = _v5(path, monkeypatch)
    c = db.conn
    c.execute(
        "INSERT INTO objectives (id, kind, title, definition_of_done, created_at, updated_at)"
        " VALUES (1,'k','t','[\"accepted:all_required\"]',?,?)",
        (NOW, NOW),
    )
    graph = _graph(("a", []), ("b", ["a"]))
    # v1 (superseded), v2 and v3 both current (an old race): v3 must stay current.
    for v, sup in ((1, NOW), (2, None), (3, None)):
        c.execute(
            "INSERT INTO plans (id, objective_id, version, source, graph, superseded_at,"
            " created_at) VALUES (?,1,?,'playbook:x',?,?,?)",
            (v, v, graph, sup, NOW),
        )
    for tid, step in ((1, "a"), (2, "b")):  # both still on v1 (replans never moved them)
        c.execute(
            "INSERT INTO tasks (id, objective_id, plan_id, worker, step, task_type, status,"
            " created_at, updated_at) VALUES (?,1,1,'model_step',?,'research.company',"
            " 'pending',?,?)",
            (tid, step, NOW, NOW),
        )
    # No dependency rows at all: a partial installation (N5).
    c.execute(
        "INSERT INTO approval_requests (objective_id, kind, status, created_at)"
        " VALUES (1,'clarification','pending',?)",
        (NOW,),
    )
    db.close()
    with Database.open(path, dataset_kind="synthetic") as db2:
        assert db2.schema_version() == 6
        current = db2.query("SELECT id FROM plans WHERE superseded_at IS NULL")
        assert [r["id"] for r in current] == [3]
        assert {r["plan_id"] for r in db2.query("SELECT plan_id FROM tasks")} == {3}
        deps = db2.query("SELECT task_id, depends_on_task_id FROM task_dependencies")
        assert [(r[0], r[1]) for r in deps] == [(2, 1)]
        assert {r["kind"] for r in db2.query("SELECT kind FROM tasks")} == {"step"}
        assert db2.query_one("SELECT answer FROM approval_requests")["answer"] is None
        assert db2.query("PRAGMA foreign_key_check") == []
        assert db2.migrate() == []  # idempotent


def test_legacy_objective_without_a_plan_is_planned_on_resume(
    cfg: AppConfig, data_dir: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Left by the old unowned planning (crash during submit, or a clarification with no
    task): resume starts an owned planning phase and uses the user's answer (N3)."""
    data_dir.mkdir(parents=True, exist_ok=True)
    db = _v5(data_dir / dbmod.DB_FILENAME, monkeypatch)
    for oid, status in ((1, "active"), (2, "awaiting_human")):
        db.conn.execute(
            "INSERT INTO objectives (id, kind, title, inputs, definition_of_done, budget_usd,"
            " status, created_at, updated_at) VALUES (?,'novel',?,?,"
            "'[\"accepted:all_required\"]',2.0,?,?,?)",
            (oid, TITLE, json.dumps(INPUTS), status, NOW, NOW),
        )
    db.conn.execute(
        "INSERT INTO approval_requests (id, objective_id, kind, details, status, created_at)"
        " VALUES (7,2,'clarification','{}','pending',?)",
        (NOW,),
    )
    db.close()
    with open_context_at(cfg, data_dir) as ctx:
        adapter = PlanHold(timeout=0)
        m = CareerManager(ctx, adapter=adapter)
        assert m.run(1).status is ObjectiveStatus.DONE  # crashed planning: re-planned
        assert _plans(ctx, 1)[-1][1:] == ("model", True)
        m.approve(7, answer="three bullet points please")
        assert m.run(2).status is ObjectiveStatus.DONE
        assert "three bullet points please" in adapter.prompts[-1]
