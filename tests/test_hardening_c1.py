"""Hardening C1: conditional state transitions (audit L3, L10; findings N1, N2).

Every objective, approval-request and task transition outside an owned (fenced) task
transaction must be conditional on the state it expects, decided inside one write
transaction, so a stale or concurrent caller cannot overwrite a newer decision.
"""

from __future__ import annotations

import threading
from collections.abc import Callable, Iterator
from contextlib import contextmanager, suppress
from typing import Any

import pytest

from careeros.context import AppContext, open_context_at
from careeros.core.errors import CareerOSError
from careeros.domain.models import ObjectiveStatus, TaskStatus
from careeros.manager.manager import CareerManager, ObjectiveRequest
from careeros.manager.plans import Playbook, PlaybookLibrary
from careeros.orchestration.fencing import claim
from tests.m2_helpers import task_by_step
from tests.phase_b_helpers import one_step_book

try:  # the claim's readiness condition, probed without claiming
    from careeros.orchestration.fencing import CLAIMABLE_SQL as _CLAIMABLE_PROBE
except ImportError:  # pragma: no cover - baseline before C1: status was the only rule
    _CLAIMABLE_PROBE = "status IN ('pending','ready','retry_scheduled')"

FAR = "2099-01-01T00:00:00+00:00"


def _blocked_on_budget(ctx: AppContext) -> tuple[CareerManager, int, int]:
    """An objective whose only task is blocked on a (simulated) budget request."""
    m = CareerManager(ctx, playbooks=one_step_book())
    oid = m.submit(
        ObjectiveRequest(title="one", kind="one", inputs={"text": "x"}, budget_usd=0.0001)
    )
    m.run(oid)
    [req] = m.pending_requests(oid)
    assert req["kind"] == "budget_increase"
    return m, oid, int(req["id"])


@contextmanager
def _interleave(on_validated: Callable[[], None] | None = None) -> Iterator[threading.Barrier]:
    """Pause every request decision right after its validation, until a second decision
    has also validated (or 1 s has passed). With validation and the write in separate
    transactions both decisions pass validation; with one transaction the second waits
    for the first to commit and then sees the request already decided."""
    barrier = threading.Barrier(2)
    orig = CareerManager._validate_request

    def paused(self: CareerManager, *args: Any, **kwargs: Any) -> Any:
        r = orig(self, *args, **kwargs)
        if on_validated is not None:
            on_validated()
        with suppress(threading.BrokenBarrierError):
            barrier.wait(1.0)
        return r

    CareerManager._validate_request = paused  # type: ignore[method-assign]
    try:
        yield barrier
    finally:
        CareerManager._validate_request = orig  # type: ignore[method-assign]


def _in_threads(ctx: AppContext, *ops: Callable[[CareerManager], None]) -> list[str]:
    """Run each operation in its own thread with its own connection; return outcomes."""
    results: list[str] = [""] * len(ops)

    def run(i: int, op: Callable[[CareerManager], None]) -> None:
        try:
            with open_context_at(ctx.config, ctx.data_dir) as c:
                op(CareerManager(c, playbooks=one_step_book()))
            results[i] = "ok"
        except CareerOSError as exc:
            results[i] = f"refused: {exc}"

    threads = [threading.Thread(target=run, args=(i, op)) for i, op in enumerate(ops)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(30)
    return results


def _state(ctx: AppContext, oid: int, rid: int) -> dict[str, Any]:
    o = ctx.db.query_one("SELECT status, budget_usd FROM objectives WHERE id=?", (oid,))
    r = ctx.db.query_one("SELECT status FROM approval_requests WHERE id=?", (rid,))
    assert o is not None and r is not None
    return {
        "objective": o["status"],
        "budget": float(o["budget_usd"] or 0),
        "request": r["status"],
        "task": task_by_step(ctx, oid, "m")["status"],
        "grants": len(ctx.db.query("SELECT * FROM approvals")),
        "stops": len(
            ctx.db.query(
                "SELECT * FROM manager_decisions WHERE objective_id=? AND decision='stop'",
                (oid,),
            )
        ),
    }


# ============================================================ N1: request decisions race
def test_concurrent_approvals_apply_exactly_once(ctx: AppContext) -> None:
    _, oid, rid = _blocked_on_budget(ctx)
    before = _state(ctx, oid, rid)["budget"]
    with _interleave():
        results = _in_threads(
            ctx, lambda m: m.approve(rid, extra_usd=1.0), lambda m: m.approve(rid, extra_usd=1.0)
        )
    assert sorted(r.split(":")[0] for r in results) == ["ok", "refused"], results
    s = _state(ctx, oid, rid)
    assert s["budget"] == pytest.approx(before + 1.0)  # not + 2.0
    assert s["grants"] == 1 and s["request"] == "approved" and s["task"] == "ready"
    line = ctx.db.query_one(
        "SELECT limit_usd FROM budgets WHERE scope='objective' AND scope_ref=?", (str(oid),)
    )
    assert line is not None and float(line["limit_usd"]) == pytest.approx(before + 1.0)


def test_concurrent_approve_and_deny_apply_exactly_one(ctx: AppContext) -> None:
    _, oid, rid = _blocked_on_budget(ctx)
    before = _state(ctx, oid, rid)["budget"]
    with _interleave():
        results = _in_threads(ctx, lambda m: m.approve(rid, extra_usd=1.0), lambda m: m.deny(rid))
    assert sorted(r.split(":")[0] for r in results) == ["ok", "refused"], results
    s = _state(ctx, oid, rid)
    if results[0] == "ok":  # approval won
        assert (s["request"], s["task"], s["objective"]) == ("approved", "ready", "active")
        assert s["budget"] == pytest.approx(before + 1.0) and s["stops"] == 0
    else:  # denial won
        assert (s["request"], s["task"], s["objective"]) == ("denied", "failed", "stopped")
        assert s["budget"] == pytest.approx(before) and s["grants"] == 0 and s["stops"] == 1


def test_approval_cannot_reopen_an_objective_finished_concurrently(ctx: AppContext) -> None:
    _, oid, rid = _blocked_on_budget(ctx)
    finisher: list[threading.Thread] = []

    def finish_elsewhere() -> None:  # another process stops the objective meanwhile
        def stop() -> None:
            with open_context_at(ctx.config, ctx.data_dir) as c:
                c.orchestration.finish_objective(oid, ObjectiveStatus.STOPPED, "stopped elsewhere")

        t = threading.Thread(target=stop)
        t.start()
        t.join(1.0)  # completes now if nothing holds the write lock
        finisher.append(t)

    m = CareerManager(ctx, playbooks=one_step_book())
    with _interleave(finish_elsewhere), suppress(CareerOSError):  # refused: also fine
        m.approve(rid, extra_usd=1.0)
    finisher[0].join(30)
    s = _state(ctx, oid, rid)
    # Whichever happened first, the objective ends finished and nothing is left open.
    assert s["objective"] == "stopped", s
    assert s["task"] == "cancelled"
    assert m.pending_requests(oid) == []
    assert s["stops"] == 1


# ================================================================== L3: stale finishes
def test_stale_done_cannot_overwrite_a_user_stop(ctx: AppContext) -> None:
    m, oid, rid = _blocked_on_budget(ctx)
    stale = m.repo.get_objective(oid)  # snapshot taken while the objective was open
    m.deny(rid)  # the user stops it
    m._finish(stale, ObjectiveStatus.DONE, "definition of done met")
    o = m.repo.get_objective(oid)
    assert (o.status, o.stop_reason) == (ObjectiveStatus.STOPPED, "user declined the request")
    assert _state(ctx, oid, rid)["stops"] == 1


def test_stale_idle_settlement_cannot_reopen_or_overwrite(ctx: AppContext) -> None:
    m, oid, rid = _blocked_on_budget(ctx)
    stale = m.repo.get_objective(oid)
    m.deny(rid)
    sid = m.repo.start_session()
    m._settle_idle(stale, sid)
    o = m.repo.get_objective(oid)
    assert o.status is ObjectiveStatus.STOPPED and _state(ctx, oid, rid)["stops"] == 1


def test_waiting_for_the_user_is_only_set_on_an_active_objective(ctx: AppContext) -> None:
    m, oid, _ = _blocked_on_budget(ctx)
    assert m.repo.get_objective(oid).status is ObjectiveStatus.AWAITING_HUMAN
    m.repo.finish_objective(oid, ObjectiveStatus.STOPPED, "stopped")
    assert not m.repo.set_objective_status(
        oid, ObjectiveStatus.AWAITING_HUMAN, "late", only_from=(ObjectiveStatus.ACTIVE,)
    )
    assert m.repo.get_objective(oid).status is ObjectiveStatus.STOPPED


def test_objective_level_request_on_a_finished_objective_is_withdrawn(ctx: AppContext) -> None:
    m, oid, _ = _blocked_on_budget(ctx)
    m.repo.finish_objective(oid, ObjectiveStatus.STOPPED, "stopped")
    m._request_user(oid, None, "clarification", {"message": "late planning question"})
    assert m.repo.get_objective(oid).status is ObjectiveStatus.STOPPED
    assert m.pending_requests(oid) == []
    late = ctx.db.query_one(
        "SELECT status FROM approval_requests WHERE objective_id=? AND kind='clarification'",
        (oid,),
    )
    assert late is not None and late["status"] == "withdrawn"


def test_finishing_twice_records_one_stop(ctx: AppContext) -> None:
    m, oid, rid = _blocked_on_budget(ctx)
    first = m.repo.finish_objective(oid, ObjectiveStatus.STOPPED, "first")
    second = m.repo.finish_objective(oid, ObjectiveStatus.DONE, "second")
    assert first is not None and second is None
    o = m.repo.get_objective(oid)
    assert (o.status, o.stop_reason) == (ObjectiveStatus.STOPPED, "first")
    assert _state(ctx, oid, rid)["stops"] == 1


# ================================================================ N2: session end reason
def test_ending_a_session_keeps_the_first_end_reason(ctx: AppContext) -> None:
    repo = CareerManager(ctx).repo
    sid = repo.start_session()
    with ctx.db.transaction() as c:  # recovery presumed it dead
        c.execute(
            "UPDATE execution_sessions SET ended_at='2026-01-01T00:00:00+00:00',"
            " end_reason='presumed dead (recovered)' WHERE id=?",
            (sid,),
        )
    repo.end_session(sid, "planning finished")
    r = ctx.db.query_one("SELECT ended_at, end_reason FROM execution_sessions WHERE id=?", (sid,))
    assert r is not None
    assert (r["ended_at"], r["end_reason"]) == (
        "2026-01-01T00:00:00+00:00",
        "presumed dead (recovered)",
    )


# ====================================================== L10: claim-time prerequisites
def _two_step(ctx: AppContext) -> tuple[CareerManager, int]:
    book = PlaybookLibrary(
        [
            Playbook.model_validate(
                {
                    "name": "two",
                    "plan": {
                        "steps": [
                            {
                                "id": "a",
                                "worker": "model_step",
                                "task_type": "research.company",
                                "output": {"required": ["summary"]},
                            },
                            {
                                "id": "b",
                                "worker": "model_step",
                                "task_type": "research.company",
                                "depends_on": ["a"],
                                "output": {"required": ["summary"]},
                            },
                        ]
                    },
                }
            )
        ]
    )
    m = CareerManager(ctx, playbooks=book)
    oid = m.submit(ObjectiveRequest(title="two", kind="two", inputs={"text": "x"}))
    return m, oid


def _claim(ctx: AppContext, task_id: int) -> Any:
    sid = CareerManager(ctx).repo.start_session()
    with ctx.db.transaction() as c:
        return claim(c, task_id, sid, FAR, "2026-01-01T00:00:00+00:00")


def test_claim_refuses_a_task_whose_prerequisite_has_not_succeeded(ctx: AppContext) -> None:
    _, oid = _two_step(ctx)
    a, b = (int(str(task_by_step(ctx, oid, s)["id"])) for s in ("a", "b"))
    assert _claim(ctx, b) is None  # 'a' has not run
    for status in ("running", "failed", "skipped", "cancelled", "awaiting_human"):
        with ctx.db.transaction() as c:
            c.execute("UPDATE tasks SET status=? WHERE id=?", (status, a))
        assert _claim(ctx, b) is None, status
    assert task_by_step(ctx, oid, "b")["status"] == "pending"
    assert task_by_step(ctx, oid, "b")["owner_generation"] == 0
    with ctx.db.transaction() as c:
        c.execute("UPDATE tasks SET status='succeeded' WHERE id=?", (a,))
    fence = _claim(ctx, b)
    assert fence is not None and fence.generation == 1


@pytest.mark.parametrize("status", ["awaiting_human", "stopped", "done", "failed"])
def test_claim_refuses_tasks_of_an_objective_that_is_not_active(
    ctx: AppContext, status: str
) -> None:
    _, oid = _two_step(ctx)
    a = int(str(task_by_step(ctx, oid, "a")["id"]))
    with ctx.db.transaction() as c:
        c.execute("UPDATE objectives SET status=? WHERE id=?", (status, oid))
    assert _claim(ctx, a) is None
    with ctx.db.transaction() as c:
        c.execute("UPDATE objectives SET status='active' WHERE id=?", (oid,))
    assert _claim(ctx, a) is not None


def test_claim_rules_match_the_dispatcher_on_every_playbook(ctx: AppContext) -> None:
    """Differential check: a task is claimable exactly when the dispatcher lists it as
    ready, at every point of a full run of each shipped playbook."""
    from careeros.simulation import SYNTHETIC_JOB

    m = CareerManager(ctx)
    for kind in ("demo_application_package", "job_triage"):
        oid = m.submit(ObjectiveRequest(title=kind, kind=kind, inputs=SYNTHETIC_JOB))
        for _ in range(20):
            ready = {t.id for t in m._ready_tasks(oid)}
            for t in ctx.orchestration.list_tasks(oid):
                if t.status in (TaskStatus.PENDING, TaskStatus.READY, TaskStatus.RETRY_SCHEDULED):
                    with ctx.db.transaction() as c:
                        cur = c.execute(
                            "SELECT 1 FROM tasks WHERE id=? AND " + _CLAIMABLE_PROBE, (t.id,)
                        )
                        assert (cur.fetchone() is not None) == (t.id in ready), (kind, t.step)
            if m.run(oid, max_attempts=1).status is not ObjectiveStatus.ACTIVE:
                break
        assert m.repo.get_objective(oid).status is ObjectiveStatus.DONE


# ================================================== N1 across separate OS processes
@pytest.mark.parametrize("actions", [("approve",) * 6, ("approve", "deny") * 3])
def test_request_decided_once_across_processes(ctx: AppContext, actions: tuple[str, ...]) -> None:
    import multiprocessing as mp

    from tests import mp_workers

    _, oid, rid = _blocked_on_budget(ctx)
    before = _state(ctx, oid, rid)["budget"]
    spawn = mp.get_context("spawn")
    barrier, q = spawn.Barrier(len(actions)), spawn.Queue()
    procs = [
        spawn.Process(
            target=mp_workers.decide_request, args=(str(ctx.data_dir), rid, a, barrier, q)
        )
        for a in actions
    ]
    for p in procs:
        p.start()
    results = [q.get(timeout=60) for _ in procs]
    for p in procs:
        p.join(60)
    assert not [r for r in results if "error" in r], results
    winners = [r for r in results if r["result"] == "ok"]
    assert len(winners) == 1, results
    s = _state(ctx, oid, rid)
    if winners[0]["action"] == "approve":
        assert (s["request"], s["task"], s["objective"]) == ("approved", "ready", "active")
        assert s["budget"] == pytest.approx(before + 1.0) and s["grants"] == 1
    else:
        assert (s["request"], s["task"], s["objective"]) == ("denied", "failed", "stopped")
        assert s["budget"] == pytest.approx(before) and s["stops"] == 1
