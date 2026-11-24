"""Hardening C6: bounded, expiring, scoped, single-use approvals (audit L12).

Expiry is tested with injected clocks (``CareerManager(clock=...)``), never wall time.
"""

from __future__ import annotations

import multiprocessing as mp
import sqlite3
from collections.abc import Callable
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

import pytest

import careeros.core.db as dbmod
from careeros.context import AppContext
from careeros.core.clock import utc_now
from careeros.core.config import AppConfig
from careeros.core.db import Database, from_json
from careeros.core.errors import CareerOSError
from careeros.domain.models import ObjectiveStatus
from careeros.llm.adapters.scripted import ScriptedAdapter
from careeros.manager.manager import CareerManager, ObjectiveRequest
from careeros.manager.plans import PlaybookLibrary
from tests import mp_workers
from tests.m2_helpers import task_by_step
from tests.m3_helpers import book, model_step

SPAWN = mp.get_context("spawn")
T0 = utc_now().replace(microsecond=0)
TTL = timedelta(hours=72)


def at(delta: timedelta) -> Callable[[], datetime]:
    return lambda: T0 + delta


AT_T0 = at(timedelta(0))


def _approvals(ctx: AppContext, **limits: Any) -> None:
    ctx.config = ctx.config.model_copy(
        update={"approvals": ctx.config.approvals.model_copy(update=limits)}
    )


def _blocked(
    ctx: AppContext, n: int = 1, clock: Callable[[], datetime] = AT_T0
) -> tuple[int, list[int]]:
    """An objective whose ``n`` independent tasks are blocked on budget requests, created
    at the given clock time. Returns (objective id, request ids)."""
    lib = PlaybookLibrary([book("c6", [model_step(f"m{i}") for i in range(n)])])
    m = CareerManager(ctx, playbooks=lib, clock=clock)
    oid = m.submit(ObjectiveRequest(title="c6", kind="c6", inputs={"text": "x"}, budget_usd=0.0001))
    m.run(oid)
    reqs = m.pending_requests(oid)
    assert [r["kind"] for r in reqs] == ["budget_increase"] * n, reqs
    return oid, [int(r["id"]) for r in reqs]


def _mgr(ctx: AppContext, clock: Callable[[], datetime] = AT_T0) -> CareerManager:
    return CareerManager(ctx, clock=clock)


def _state(ctx: AppContext, oid: int) -> dict[str, Any]:
    o = ctx.db.query_one("SELECT status, budget_usd FROM objectives WHERE id=?", (oid,))
    line = ctx.db.query_one(
        "SELECT limit_usd FROM budgets WHERE scope='objective' AND scope_ref=?", (str(oid),)
    )
    assert o is not None and line is not None
    return {
        "objective": o["status"],
        "budget": round(float(o["budget_usd"] or 0), 6),
        "line": round(float(line["limit_usd"]), 6),
        "grants": len(ctx.db.query("SELECT * FROM approvals WHERE objective_id=?", (oid,))),
        "global": [
            (r["scope"], round(float(r["limit_usd"]), 6))
            for r in ctx.db.query(
                "SELECT scope, limit_usd FROM budgets WHERE scope IN ('month','session')"
                " ORDER BY id"
            )
        ],
    }


def _request(ctx: AppContext, rid: int) -> dict[str, Any]:
    r = ctx.db.query_one("SELECT * FROM approval_requests WHERE id=?", (rid,))
    assert r is not None
    return dict(r)


# =========================================================================== expiry
def test_every_request_has_an_explicit_expiry(ctx: AppContext) -> None:
    _, [rid] = _blocked(ctx)
    r = _request(ctx, rid)
    assert r["expires_at"] == (T0 + TTL).isoformat(timespec="seconds")
    [p] = _mgr(ctx).pending_requests()
    assert p["expires_at"] == r["expires_at"] and p["expired"] is False


@pytest.mark.parametrize(
    ("delta", "ok"),
    [(TTL - timedelta(seconds=1), True), (TTL, False), (TTL + timedelta(days=30), False)],
    ids=["one_second_before", "at_expiry", "long_after"],
)
def test_approval_just_before_and_after_expiry(ctx: AppContext, delta: timedelta, ok: bool) -> None:
    oid, [rid] = _blocked(ctx)
    before = _state(ctx, oid)
    m = _mgr(ctx, at(delta))
    if ok:
        m.approve(rid, extra_usd=0.5)
        assert _request(ctx, rid)["status"] == "approved"
        assert _state(ctx, oid)["budget"] == pytest.approx(before["budget"] + 0.5)
        return
    with pytest.raises(CareerOSError, match="expired") as exc:
        m.approve(rid, extra_usd=0.5)
    assert _request(ctx, rid)["status"] == "withdrawn"
    assert _state(ctx, oid) == before  # nothing applied: no budget, no grant
    assert task_by_step(ctx, oid, "m0")["status"] == "blocked_budget"
    # The waiting work is not stranded: a fresh request replaces the expired one.
    [new] = m.pending_requests(oid)
    assert f"request {new['id']}" in str(exc.value)
    assert new["details"]["replaces_request"] == rid
    assert new["expires_at"] == (T0 + delta + TTL).isoformat(timespec="seconds")
    m.approve(int(new["id"]), extra_usd=0.5)
    assert _state(ctx, oid)["grants"] == 1


def test_an_expired_request_cannot_be_denied_either(ctx: AppContext) -> None:
    oid, [rid] = _blocked(ctx)
    with pytest.raises(CareerOSError, match="expired"):
        _mgr(ctx, at(TTL)).deny(rid)
    assert ctx.orchestration.get_objective(oid).status is not ObjectiveStatus.STOPPED


def test_expired_requests_are_replaced_when_a_run_starts(ctx: AppContext) -> None:
    oid, [rid] = _blocked(ctx)
    m = _mgr(ctx, at(TTL + timedelta(hours=1)))
    m.run(oid)
    assert _request(ctx, rid)["status"] == "withdrawn"
    [new] = m.pending_requests(oid)
    assert new["details"]["replaces_request"] == rid and new["expired"] is False


# =============================================================== single consumption
def test_repeated_delivery_is_applied_once(ctx: AppContext) -> None:
    oid, [rid] = _blocked(ctx)
    m = _mgr(ctx)
    m.approve(rid, extra_usd=0.5)
    after = _state(ctx, oid)
    for _ in range(3):
        with pytest.raises(CareerOSError, match="already approved"):
            m.approve(rid, extra_usd=0.5)
    assert _state(ctx, oid) == after and after["grants"] == 1


def test_a_grant_can_never_be_recorded_twice_for_one_request(ctx: AppContext) -> None:
    """Defence in depth below the request's state check: even if a request were made
    pending again, the database refuses a second grant for it."""
    oid, [rid] = _blocked(ctx)
    m = _mgr(ctx)
    m.approve(rid, extra_usd=0.5)
    after = _state(ctx, oid)
    with ctx.db.transaction() as c:  # simulate a replayed decision
        c.execute("UPDATE approval_requests SET status='pending' WHERE id=?", (rid,))
        c.execute("UPDATE tasks SET status='blocked_budget' WHERE objective_id=?", (oid,))
    with pytest.raises(CareerOSError, match="already granted"):
        m.approve(rid, extra_usd=0.5)
    assert _state(ctx, oid) == after
    with pytest.raises(sqlite3.IntegrityError), ctx.db.transaction() as c:
        c.execute(
            "INSERT INTO approvals (kind, granted_at, request_id) VALUES ('other', 'x', ?)",
            (rid,),
        )


@pytest.mark.parametrize(
    "actions", [("approve",) * 6, ("approve", "deny") * 3], ids=["approvals", "with_denials"]
)
def test_concurrent_processes_consume_a_request_once(
    ctx: AppContext, actions: tuple[str, ...]
) -> None:
    oid, [rid] = _blocked(ctx, clock=utc_now)
    before = _state(ctx, oid)
    barrier, q = SPAWN.Barrier(len(actions)), SPAWN.Queue()
    procs = [
        SPAWN.Process(
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
    [winner] = [r for r in results if r["result"] == "ok"]
    s = _state(ctx, oid)
    if winner["action"] == "approve":
        assert s["grants"] == 1 and s["budget"] == pytest.approx(before["budget"] + 1.0)
    else:
        assert s["grants"] == 0 and s["objective"] == "stopped"


# ============================================================================ limits
def test_per_request_and_cumulative_budget_limits(ctx: AppContext) -> None:
    _approvals(ctx, max_budget_increase_usd=0.5, max_total_budget_increase_usd=0.8)
    oid, [a, b] = _blocked(ctx, n=2)
    m = _mgr(ctx)
    before = _state(ctx, oid)
    with pytest.raises(CareerOSError, match="per-approval limit"):
        m.approve(a, extra_usd=0.6)
    assert _request(ctx, a)["status"] == "pending" and _state(ctx, oid) == before
    m.approve(a, extra_usd=0.5)
    with pytest.raises(CareerOSError, match=r"over the limit of \$0.80"):
        m.approve(b, extra_usd=0.5)  # 0.5 + 0.5 > 0.8 for this objective
    m.approve(b, extra_usd=0.3)
    s = _state(ctx, oid)
    assert s["budget"] == pytest.approx(before["budget"] + 0.8) and s["grants"] == 2
    assert s["global"] == before["global"]  # session and monthly limits never raised


def _retry_limited(ctx: AppContext) -> tuple[CareerManager, int]:
    """A task allowed one attempt whose every attempt fails: it asks for more attempts."""
    lib = PlaybookLibrary([book("c6r", [model_step("m", max_attempts=1)])])
    adapter = ScriptedAdapter({"m:research.company": ["fail_quality"] * 30})
    m = CareerManager(ctx, adapter=adapter, playbooks=lib, clock=at(timedelta(0)))
    oid = m.submit(ObjectiveRequest(title="r", kind="c6r", inputs={"text": "x"}))
    m.run(oid)
    return m, oid


def _retry_request(m: CareerManager, oid: int) -> int:
    [r] = m.pending_requests(oid)
    assert r["kind"] == "retry_limit", r
    return int(r["id"])


def test_per_request_and_cumulative_attempt_limits(ctx: AppContext) -> None:
    _approvals(ctx, max_extra_attempts=2, max_total_extra_attempts=3)
    m, oid = _retry_limited(ctx)
    rid = _retry_request(m, oid)
    with pytest.raises(CareerOSError, match=r"outside 1\.\.2"):
        m.approve(rid, extra_attempts=3)
    with pytest.raises(CareerOSError, match=r"outside 1\.\.2"):
        m.approve(rid, extra_attempts=0)
    m.approve(rid, extra_attempts=2)
    assert task_by_step(ctx, oid, "m")["max_attempts"] == 3
    m.run(oid)  # both extra attempts fail too
    rid2 = _retry_request(m, oid)
    with pytest.raises(CareerOSError, match="already has 2 approved extra attempt"):
        m.approve(rid2, extra_attempts=2)  # 2 + 2 > 3 for this task
    m.approve(rid2, extra_attempts=1)
    t = task_by_step(ctx, oid, "m")
    assert t["max_attempts"] == 4
    grants = ctx.db.query("SELECT kind, extra_attempts, task_id FROM approvals ORDER BY id")
    assert [(g["kind"], g["extra_attempts"], g["task_id"]) for g in grants] == [
        ("extra_attempts", 2, t["id"]),
        ("extra_attempts", 1, t["id"]),
    ]


# ======================================================================== downgrade
def _downgrade_request(ctx: AppContext) -> tuple[CareerManager, int, int]:
    step = {
        "id": "analyze",
        "worker": "model_step",
        "task_type": "job.analyze",
        "params": {"complexity_hint": 0.30},
        "budget_usd": 0.002,
        "priority": "high",
        "output": {"required": ["summary"], "types": {"summary": "string"}},
    }
    m = CareerManager(
        ctx,
        adapter=ScriptedAdapter({"analyze:job.analyze": ["success:0.9"] * 3}),
        playbooks=PlaybookLibrary([book("dg", [step])]),
        clock=at(timedelta(0)),
    )
    oid = m.submit(ObjectiveRequest(title="x", kind="dg", inputs={"text": "x"}))
    m.run(oid)
    [req] = m.pending_requests(oid)
    assert req["details"].get("downgrade_option"), req
    return m, oid, int(req["id"])


def test_downgrade_grant_is_scoped_to_task_purpose_attempt_and_expires(ctx: AppContext) -> None:
    m, oid, rid = _downgrade_request(ctx)
    m.approve(rid, accept_downgrade=True)
    t = task_by_step(ctx, oid, "analyze")
    [g] = ctx.db.query("SELECT * FROM approvals")
    assert (g["kind"], g["objective_id"], g["task_id"], g["request_id"], g["purpose"]) == (
        "quality_downgrade",
        oid,
        t["id"],
        rid,
        "task",
    )
    assert g["attempt"] == t["attempt"] and g["expires_at"] == (T0 + TTL).isoformat(
        timespec="seconds"
    )
    tid, attempt = int(str(t["id"])), int(str(t["attempt"]))
    assert m._approved_floor(tid, attempt, "task") is not None  # this attempt, task routing
    assert m._approved_floor(tid, attempt, "review") is None  # not the review
    assert m._approved_floor(tid, attempt + 1, "task") is None  # not a later attempt
    assert m._approved_floor(tid + 999, attempt, "task") is None  # not another task
    late = _mgr(ctx, at(TTL))
    assert late._approved_floor(tid, attempt, "task") is None  # not after expiry
    assert m.run(oid).status is ObjectiveStatus.DONE  # used within its scope


def test_an_expired_downgrade_grant_is_not_used(ctx: AppContext) -> None:
    m, oid, rid = _downgrade_request(ctx)
    m.approve(rid, accept_downgrade=True)
    s = CareerManager(ctx, playbooks=m.playbooks, adapter=m.adapter, clock=at(TTL)).run(oid)
    assert s.status is not ObjectiveStatus.DONE  # no silent downgrade: it asks again
    t = task_by_step(ctx, oid, "analyze")
    assert t["status"] == "blocked_budget"
    [new] = m.pending_requests(oid)
    assert new["kind"] == "budget_increase" and int(new["id"]) != rid


# ============================================================== stale approvals
def test_stale_approval_after_objective_completion(ctx: AppContext) -> None:
    oid, [rid] = _blocked(ctx)
    ctx.orchestration.finish_objective(oid, ObjectiveStatus.STOPPED, "stopped elsewhere")
    before = _state(ctx, oid)
    with pytest.raises(CareerOSError):
        _mgr(ctx).approve(rid, extra_usd=0.5)
    assert _state(ctx, oid) == before and before["objective"] == "stopped"


def test_stale_approval_after_replanning(ctx: AppContext) -> None:
    """The plan changed after the request was raised: the task no longer belongs to the
    current plan version, so the approval cannot apply to it (or reopen anything)."""
    oid, [rid] = _blocked(ctx)
    m = _mgr(ctx)
    obj = m.repo.get_objective(oid)
    from careeros.manager.plans import PlanSpec, StepSpec

    with ctx.db.transaction() as c:  # a replan that keeps the step: task carried over ...
        m._install_plan_in(
            c,
            obj,
            PlanSpec(steps=[StepSpec.model_validate(model_step("m0"))]),
            source="replan",
            rationale="test",
        )
        c.execute(  # ... but here it is left on the old version (outdated)
            "UPDATE tasks SET plan_id = (SELECT MIN(id) FROM plans) WHERE objective_id=?",
            (oid,),
        )
    before = _state(ctx, oid)
    with pytest.raises(CareerOSError, match="no longer part of the current plan"):
        m.approve(rid, extra_usd=0.5)
    assert _state(ctx, oid) == before
    oid2, [rid2] = _blocked(ctx)
    with ctx.db.transaction() as c:  # a replan that drops the step: the task is cancelled
        m._install_plan_in(
            c,
            m.repo.get_objective(oid2),
            PlanSpec(steps=[StepSpec.model_validate(model_step("other"))]),
            source="replan",
            rationale="test",
        )
    with pytest.raises(CareerOSError, match="cancelled"):
        m.approve(rid2, extra_usd=0.5)


# =========================================================== crash and recovery
def test_crash_during_grant_consumption_applies_nothing(ctx: AppContext) -> None:
    oid, [rid] = _blocked(ctx, clock=utc_now)
    before = _state(ctx, oid)
    q = SPAWN.Queue()
    p = SPAWN.Process(target=mp_workers.approve_and_die, args=(str(ctx.data_dir), rid, 0.5, q))
    p.start()
    res = q.get(timeout=60)
    p.join(60)
    assert res.get("died_at") == "grant consumption" and p.exitcode == 17, res
    assert _request(ctx, rid)["status"] == "pending"  # the decision rolled back entirely
    assert _state(ctx, oid) == before and before["grants"] == 0
    assert task_by_step(ctx, oid, "m0")["status"] == "blocked_budget"
    # Recovery after the partially attempted approval: decide again, exactly once.
    m = CareerManager(ctx)
    m.approve(rid, extra_usd=0.5)
    assert _state(ctx, oid)["grants"] == 1
    assert m.run(oid).status is ObjectiveStatus.DONE


# ===================================================== clarification vs permission
def test_clarification_answers_are_not_permission_grants(ctx: AppContext) -> None:
    _approvals(ctx, max_clarifications=1)
    adapter = ScriptedAdapter({"manager.plan_novel": ["fail_quality"] * 2})
    m = CareerManager(ctx, adapter=adapter)
    oid = m.submit(ObjectiveRequest(title="Summarize my club notes", inputs={"text": "x"}))
    [req] = m.pending_requests(oid)
    m.approve(int(req["id"]), answer="three bullets")
    assert ctx.db.query("SELECT * FROM approvals") == []  # an answer, not a grant
    assert _request(ctx, int(req["id"]))["answer"] == "three bullets"
    m.run(oid)  # fails again and asks again
    [req2] = m.pending_requests(oid)
    with pytest.raises(CareerOSError, match="clarification"):
        m.approve(int(req2["id"]), answer="more detail")  # limit of 1 reached
    assert _request(ctx, int(req2["id"]))["status"] == "pending"
    assert ctx.db.query("SELECT * FROM approvals") == []


# ======================================================================= migration
def test_migration_scopes_grants_and_archives_duplicates(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    orig = dbmod.load_migrations
    monkeypatch.setattr(dbmod, "load_migrations", lambda: [x for x in orig() if x.version <= 6])
    path = tmp_path / "v6.db"
    db = Database.open(path, dataset_kind="synthetic")
    monkeypatch.setattr(dbmod, "load_migrations", orig)
    c = db.conn
    now = "2026-01-01T00:00:00+00:00"
    c.execute(
        "INSERT INTO objectives (id, kind, title, definition_of_done, created_at, updated_at)"
        " VALUES (1,'k','t','[]',?,?)",
        (now, now),
    )
    c.execute(
        "INSERT INTO plans (id, objective_id, version, source, created_at) VALUES (1,1,1,'x',?)",
        (now,),
    )
    c.execute(
        "INSERT INTO tasks (id, objective_id, plan_id, worker, step, task_type, attempt,"
        " created_at, updated_at) VALUES (5,1,1,'w','m','job.analyze',2,?,?)",
        (now, now),
    )
    for rid, kind in ((10, "budget_increase"), (11, "retry_limit")):
        c.execute(
            "INSERT INTO approval_requests (id, objective_id, task_id, kind, status,"
            " created_at) VALUES (?,1,5,?,'approved',?)",
            (rid, kind, now),
        )
    rows = [  # the N1-era double approval of request 10, and a retry-limit grant
        ("budget_increase", 10, 1.0),
        ("budget_increase", 10, 1.0),
        ("other", 11, None),
    ]
    for kind, rid, usd in rows:
        c.execute(
            "INSERT INTO approvals (kind, scope, details, max_usd, granted_at)"
            " VALUES (?, 'objective:1/task:5', ?, ?, ?)",
            (kind, f'{{"request_id": {rid}}}', usd, now),
        )
    db.close()
    with Database.open(path, dataset_kind="synthetic") as db2:
        assert db2.schema_version() == max(x.version for x in dbmod.load_migrations())
        got = [
            (r["kind"], r["request_id"], r["objective_id"], r["task_id"], r["extra_usd"])
            for r in db2.query("SELECT * FROM approvals ORDER BY id")
        ]
        assert got == [
            ("budget_increase", 10, 1, 5, 1.0),
            ("extra_attempts", 11, 1, 5, None),
        ]
        [dup] = db2.query("SELECT * FROM approvals_duplicates")
        assert dup["request_id"] == 10 and dup["kept_approval_id"] == 1
        assert db2.query_one("SELECT expires_at FROM approval_requests")["expires_at"] is None
        assert db2.query("PRAGMA foreign_key_check") == []
        assert db2.migrate() == []
        assert (
            from_json(db2.query_one("SELECT details FROM approvals")["details"])["request_id"] == 10
        )


def test_limits_default_to_existing_settings(cfg: AppConfig) -> None:
    lim = cfg.approval_limits()
    assert lim.max_budget_increase_usd == cfg.budgets.default_objective_usd
    assert lim.max_total_budget_increase_usd == cfg.budgets.simulated_monthly_usd
    assert lim.max_extra_attempts == lim.max_total_extra_attempts == cfg.limits.max_attempts
    assert lim.max_clarifications == cfg.limits.max_attempts
    assert lim.request_ttl_hours == 72 and lim.downgrade_ttl_hours == 72
