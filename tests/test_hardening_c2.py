"""Hardening C2: attempt counting (audit L5).

Two counters exist and must stay distinct:

* the per-task attempt number (``tasks.attempt`` / ``attempt_open``, checked against the
  task's ``max_attempts`` retry limit) advances only when a new execution attempt starts;
  a claim that resumes an open attempt (after a budget-contention requeue, a halt, lost
  ownership or recovery) keeps the number;
* the per-invocation quota (``run(max_attempts=...)``, reported as
  ``RunSummary.attempts_executed``) counts genuine execution attempts in this invocation,
  not dispatcher activity: claiming the same attempt again in the same run is a dispatch,
  not a new attempt.
"""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any

import pytest

from careeros.context import AppContext
from careeros.core.errors import SessionTerminatedError
from careeros.cost.budget import BudgetManager
from careeros.domain.models import ObjectiveStatus
from careeros.manager.manager import CareerManager, ObjectiveRequest
from careeros.manager.plans import PlaybookLibrary
from careeros.simulation import later
from tests.m2_helpers import decisions, task_by_step
from tests.m3_helpers import GatedAdapter, book, model_step


def _manager(ctx: AppContext, steps: list[dict[str, Any]], **kw: Any) -> CareerManager:
    return CareerManager(ctx, playbooks=PlaybookLibrary([book("c2", steps)]), **kw)


def _submit(m: CareerManager) -> int:
    return m.submit(ObjectiveRequest(title="c2", kind="c2", inputs={"text": "x"}, budget_usd=1.0))


@contextmanager
def _contention(ctx: AppContext, oid: int, requeues: int) -> Iterator[list[int]]:
    """Another session holds almost the whole objective budget, so every reservation
    attempt is *contention* (it would fit once that reservation settles): the task is
    requeued, never blocked. The held reservation is released after ``requeues``
    requeues. Yields the list of requeue counts seen."""
    bm = BudgetManager(ctx.db, ctx.config)
    bm.lines_for(oid, None, None)
    line = ctx.db.query_one(
        "SELECT id, limit_usd FROM budgets WHERE scope='objective' AND scope_ref=?", (str(oid),)
    )
    assert line is not None
    held = bm.reserve([int(line["id"])], float(line["limit_usd"]) - 0.001, session_id=None)
    seen: list[int] = []
    orig = CareerManager._requeue

    def requeue(self: CareerManager, fence: Any, task: Any, reason: str) -> None:
        orig(self, fence, task, reason)
        seen.append(fence.attempt)
        if len(seen) == requeues:
            with self.db.transaction() as c:
                BudgetManager.release_in(c, held.id)

    CareerManager._requeue = requeue  # type: ignore[method-assign]
    try:
        yield seen
    finally:
        CareerManager._requeue = orig  # type: ignore[method-assign]


def _requeues(ctx: AppContext, oid: int) -> int:
    return len(
        ctx.db.query(
            "SELECT * FROM manager_decisions WHERE objective_id=? AND decision='requeue'", (oid,)
        )
    )


# ============================================================== the L5 reproduction
def test_contention_requeues_do_not_use_up_the_run_quota(ctx: AppContext) -> None:
    """Baseline: every claim counted, so with ``max_attempts=1`` a single contention
    requeue ended the run ("attempt limit for this invocation") before the task had run
    at all."""
    m = _manager(ctx, [model_step("m")])
    oid = _submit(m)
    with _contention(ctx, oid, requeues=3) as seen:
        s = m.run(oid, max_attempts=1)
    assert s.stop_reason == "definition of done met", s.stop_reason
    assert seen == [1, 1, 1]  # three requeues, all of attempt 1
    assert s.status is ObjectiveStatus.DONE
    assert s.attempts_executed == 1  # one genuine execution attempt ...
    assert s.dispatches == 4  # ... dispatched four times
    t = task_by_step(ctx, oid, "m")
    assert (t["status"], t["attempt"]) == ("succeeded", 1)
    assert _requeues(ctx, oid) == 3


def test_reported_attempts_exclude_requeued_dispatches(ctx: AppContext) -> None:
    m = _manager(ctx, [model_step("m")])
    oid = _submit(m)
    with _contention(ctx, oid, requeues=2):
        s = m.run(oid)
    assert s.status is ObjectiveStatus.DONE
    assert s.attempts_executed == 1, s.attempts_executed  # baseline reported 3
    assert s.dispatches == 3


# ===================================================== preserved behaviour (guards)
def test_the_quota_still_limits_genuine_attempts(ctx: AppContext) -> None:
    from careeros.llm.adapters.scripted import ScriptedAdapter

    always = ScriptedAdapter({"*": ["success"] * 10})
    m = _manager(ctx, [model_step("a"), model_step("b"), model_step("c")], adapter=always)
    oid = _submit(m)
    s = m.run(oid, max_attempts=2)
    assert s.status is ObjectiveStatus.ACTIVE
    assert s.stop_reason == "attempt limit for this invocation"
    assert s.attempts_executed == 2 and s.dispatches == 2
    statuses = sorted(str(task_by_step(ctx, oid, x)["status"]) for x in "abc")
    assert statuses == ["pending", "succeeded", "succeeded"]
    s2 = m.run(oid)
    assert s2.status is ObjectiveStatus.DONE and s2.attempts_executed == 1


def test_genuine_retries_count_against_the_quota(ctx: AppContext) -> None:
    """A new attempt of the same task (after a failed one) is a genuine attempt."""
    from careeros.llm.adapters.scripted import ScriptedAdapter

    m = _manager(
        ctx,
        [model_step("m")],
        adapter=ScriptedAdapter({"m:research.company": ["timeout", "success"]}),
    )
    oid = _submit(m)
    s = m.run(oid, max_attempts=1)
    assert s.attempts_executed == 1 and s.status is ObjectiveStatus.ACTIVE
    assert task_by_step(ctx, oid, "m")["attempt"] == 1
    s2 = m.run(oid, max_attempts=1)
    assert s2.attempts_executed == 1 and s2.status is ObjectiveStatus.DONE
    assert task_by_step(ctx, oid, "m")["attempt"] == 2


def test_contention_does_not_consume_the_task_retry_limit(ctx: AppContext) -> None:
    """A task allowed exactly one attempt still gets it after many contention requeues."""
    m = _manager(ctx, [model_step("m", max_attempts=1)])
    oid = _submit(m)
    with _contention(ctx, oid, requeues=5):
        s = m.run(oid)
    assert s.status is ObjectiveStatus.DONE
    t = task_by_step(ctx, oid, "m")
    assert (t["status"], t["attempt"], t["max_attempts"]) == ("succeeded", 1, 1)
    assert decisions(ctx, t["id"]).count("requeue") == 5


def test_recovery_resumes_the_attempt_and_counts_it_once_per_run(ctx: AppContext) -> None:
    """An attempt interrupted by a crash is resumed (same number) by a later session,
    which counts it as one genuine attempt of its own invocation."""
    steps = [model_step("m", max_attempts=1)]
    m = _manager(ctx, steps, adapter=GatedAdapter(n=1, crash_in_call={"m"}))
    oid = _submit(m)
    with pytest.raises(SessionTerminatedError):
        m.run(oid)
    assert task_by_step(ctx, oid, "m")["attempt"] == 1
    s = _manager(ctx, steps, clock=later(10)).run(oid, max_attempts=1)
    assert s.status is ObjectiveStatus.DONE
    assert s.attempts_executed == 1
    t = task_by_step(ctx, oid, "m")
    assert (t["status"], t["attempt"]) == ("succeeded", 1)  # max_attempts=1 not exceeded


def test_lost_ownership_does_not_advance_the_attempt(ctx: AppContext) -> None:
    """A worker whose ownership is taken over stops; the new owner resumes the same
    attempt number (generation advances, attempt does not)."""
    from tests.phase_b_helpers import BlockingAdapter

    steps = [model_step("m", max_attempts=1)]
    slow = BlockingAdapter({"m"})
    m = _manager(ctx, steps, adapter=slow)
    oid = _submit(m)
    import threading

    from careeros.context import open_context_at

    def first() -> None:
        with open_context_at(ctx.config, ctx.data_dir) as c:
            CareerManager(c, adapter=slow, playbooks=m.playbooks).run(oid)

    t = threading.Thread(target=first)
    t.start()
    assert slow.entered.wait(10)
    taker = _manager(ctx, steps).run(oid, force_recover=True)  # takes ownership
    slow.release.set()
    t.join(15)
    assert taker.status is ObjectiveStatus.DONE and taker.attempts_executed == 1
    row = task_by_step(ctx, oid, "m")
    assert (row["status"], row["attempt"], row["owner_generation"]) == ("succeeded", 1, 2)


# ================================ N6: dispatch loop under permanent budget contention
def _hold_forever(ctx: AppContext, oid: int) -> None:
    bm = BudgetManager(ctx.db, ctx.config)
    bm.lines_for(oid, None, None)
    line = ctx.db.query_one(
        "SELECT id, limit_usd FROM budgets WHERE scope='objective' AND scope_ref=?", (str(oid),)
    )
    assert line is not None
    bm.reserve([int(line["id"])], float(line["limit_usd"]) - 0.001, session_id=None)


def _run_bounded(ctx: AppContext, m: CareerManager, oid: int, **kw: Any) -> Any:
    """Run in a thread with its own connection; fail (not hang) if it does not return."""
    import threading

    from careeros.context import open_context_at

    out: dict[str, Any] = {}

    def go() -> None:
        with open_context_at(ctx.config, ctx.data_dir) as c:
            out["s"] = CareerManager(c, playbooks=m.playbooks).run(oid, **kw)

    t = threading.Thread(target=go, daemon=True)
    t.start()
    t.join(20)
    assert not t.is_alive(), (
        f"run did not return: {_requeues(ctx, oid)} requeues, dispatcher spinning"
    )
    return out["s"]


@pytest.mark.parametrize("quota", [None, 1])
def test_permanent_contention_backs_off_and_stops(ctx: AppContext, quota: int | None) -> None:
    """Baseline: a lone task contending with another session's reservation was claimed
    and requeued in a hot loop (~770 claim/requeue cycles per second, each a decision
    and a generation) and the run never returned. It must back off and stop, resumable,
    with its attempt untouched."""
    m = _manager(ctx, [model_step("m", max_attempts=1)])
    oid = _submit(m)
    _hold_forever(ctx, oid)
    s = _run_bounded(ctx, m, oid, max_attempts=quota)
    assert s.stop_reason == "budget is reserved by another active session"
    assert s.status is ObjectiveStatus.ACTIVE
    assert s.attempts_executed == 1
    assert 1 <= _requeues(ctx, oid) <= 60 and s.dispatches == _requeues(ctx, oid)
    t = task_by_step(ctx, oid, "m")
    assert (t["status"], t["attempt"], t["attempt_open"]) == ("ready", 1, 1)
    assert ctx.db.query("SELECT * FROM approval_requests") == []  # waits, never blocks


# ============================ quota race across separate processes (C2 follow-up)
def test_quota_cannot_be_exceeded_when_another_process_closes_the_attempt(
    ctx: AppContext,
) -> None:
    """Invocation A (quota 1) has counted attempt 1, which was requeued. A decides the
    re-claim is the same attempt; before it claims, process B resumes attempt 1 and
    closes it (timeout -> retry scheduled). A's claim must not then start attempt 2: the
    quota has to hold for the attempt actually claimed, decided atomically with it."""
    import multiprocessing as mp

    from tests import mp_workers

    pb = {"name": "c2", "plan": {"steps": [model_step("m")]}}
    m = CareerManager(ctx, playbooks=PlaybookLibrary([book("c2", [model_step("m")])]))
    oid = _submit(m)
    bm = BudgetManager(ctx.db, ctx.config)
    bm.lines_for(oid, None, None)
    line = ctx.db.query_one(
        "SELECT id, limit_usd FROM budgets WHERE scope='objective' AND scope_ref=?", (str(oid),)
    )
    assert line is not None
    held = bm.reserve([int(line["id"])], float(line["limit_usd"]) - 0.001, session_id=None)

    spawn = mp.get_context("spawn")
    paused, proceed, q = spawn.Event(), spawn.Event(), spawn.Queue()
    a = spawn.Process(
        target=mp_workers.quota_race_owner,
        args=(str(ctx.data_dir), oid, pb, held.id, paused, proceed, q),
    )
    a.start()
    assert paused.wait(60), "invocation A never reached its second claim"
    t = task_by_step(ctx, oid, "m")
    assert (t["status"], t["attempt"], t["attempt_open"]) == ("ready", 1, 1)
    b = spawn.Process(
        target=mp_workers.run_scripted,
        args=(str(ctx.data_dir), oid, pb, {"m:research.company": ["timeout"]}, 1, q),
    )
    b.start()
    rb = q.get(timeout=60)
    b.join(60)
    assert "error" not in rb, rb
    t = task_by_step(ctx, oid, "m")
    assert (t["status"], t["attempt"], t["attempt_open"]) == ("retry_scheduled", 1, 0)
    proceed.set()
    ra = q.get(timeout=60)
    a.join(60)
    assert "error" not in ra, ra.get("error")
    # A executed attempt 1 only; attempt 2 is left for a later invocation.
    assert ra["attempts_executed"] == 1, ra
    assert ra["stop_reason"] == "attempt limit for this invocation", ra
    assert ra["task_attempt"] == 1, ra
    t = task_by_step(ctx, oid, "m")
    assert (t["status"], t["attempt"]) == ("retry_scheduled", 1)


def test_claim_restricted_to_an_attempt_resumes_it_but_never_starts_another(
    ctx: AppContext,
) -> None:
    from careeros.orchestration.fencing import claim

    m = _manager(ctx, [model_step("m")])
    oid = _submit(m)
    tid = int(str(task_by_step(ctx, oid, "m")["id"]))
    sid = m.repo.start_session()
    far, now = "2099-01-01T00:00:00+00:00", "2026-01-01T00:00:00+00:00"

    def take(only: int | None) -> Any:
        with ctx.db.transaction() as c:
            return claim(c, tid, sid, far, now, only_attempt=only)

    def set_row(status: str, attempt: int, open_: int) -> None:
        with ctx.db.transaction() as c:
            c.execute(
                "UPDATE tasks SET status=?, attempt=?, attempt_open=?, lease_owner=NULL WHERE id=?",
                (status, attempt, open_, tid),
            )

    set_row("ready", 1, 1)  # attempt 1 open (requeued): resuming it is allowed
    f = take(1)
    assert f is not None and f.attempt == 1
    set_row("retry_scheduled", 1, 0)  # attempt 1 closed: the claim would start attempt 2
    assert take(1) is None
    assert task_by_step(ctx, oid, "m")["status"] == "retry_scheduled"  # untouched
    set_row("ready", 2, 1)  # a different open attempt
    assert take(1) is None
    f = take(None)  # unrestricted (quota left): starts or resumes as before
    assert f is not None and f.attempt == 2
