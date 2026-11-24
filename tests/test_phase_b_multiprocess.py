"""Phase B with real separate operating-system processes (multiprocessing 'spawn').

Every process has its own interpreter, connections, session and heartbeat; they share only
the database file. These tests exercise the database-level guarantees: exclusive claims,
fenced writes, conditional takeover, late-reply accounting and safe halting.
"""

from __future__ import annotations

import multiprocessing as mp
from typing import Any

import pytest

from careeros.context import AppContext
from careeros.core.errors import SessionTerminatedError
from careeros.domain.models import TaskStatus
from careeros.manager.manager import CareerManager
from careeros.manager.plans import Playbook, PlaybookLibrary
from careeros.simulation import SYNTHETIC_JOB, later
from tests import mp_workers
from tests.m2_helpers import calls, decisions, task_by_step
from tests.m3_helpers import GatedAdapter
from tests.phase_b_helpers import done, ledger_consistent, submit

SPAWN = mp.get_context("spawn")
ONE: dict[str, Any] = {
    "name": "one",
    "plan": {
        "steps": [
            {
                "id": "m",
                "worker": "model_step",
                "task_type": "research.company",
                "output": {"required": ["summary"]},
            }
        ]
    },
}
THREE: dict[str, Any] = {
    "name": "three",
    "plan": {
        "steps": [
            {
                "id": f"m{i}",
                "worker": "model_step",
                "task_type": "research.company",
                "output": {"required": ["summary"]},
            }
            for i in range(3)
        ]
    },
}


def _lib(book: dict[str, Any]) -> PlaybookLibrary:
    return PlaybookLibrary([Playbook.model_validate(book)])


def _start(target: Any, *args: Any) -> Any:
    p = SPAWN.Process(target=target, args=args)
    p.start()
    return p


def _collect(q: Any, n: int, timeout: float = 60) -> list[dict[str, Any]]:
    out = [q.get(timeout=timeout) for _ in range(n)]
    errors = [r["error"] for r in out if "error" in r]
    assert not errors, "\n".join(errors)
    return out


def _join(*procs: Any) -> None:
    for p in procs:
        p.join(60)
        assert not p.is_alive()


def test_concurrent_claims_from_separate_processes(ctx: AppContext) -> None:
    m = CareerManager(ctx, playbooks=_lib(ONE))
    oid = submit(m)
    task = ctx.orchestration.list_tasks(oid)[0]
    n = 6
    barrier, q = SPAWN.Barrier(n), SPAWN.Queue()
    procs = [
        _start(mp_workers.claim_task, str(ctx.data_dir), task.id, barrier, q) for _ in range(n)
    ]
    results = _collect(q, n)
    _join(*procs)
    winners = [r for r in results if r["fence"] is not None]
    assert len(winners) == 1 and tuple(winners[0]["fence"]) == (1, 1)  # generation 1, attempt 1
    assert len({r["pid"] for r in results}) == n  # really separate processes
    t = ctx.orchestration.get_task(task.id or 0)
    assert t.status is TaskStatus.RUNNING
    assert (
        ctx.db.query_one("SELECT owner_generation FROM tasks WHERE id=?", (task.id,))[
            "owner_generation"
        ]
        == 1
    )


def test_concurrent_recovery_from_separate_processes(ctx: AppContext) -> None:
    m = CareerManager(ctx, adapter=GatedAdapter(n=3, crash_in_call={"m0"}), playbooks=_lib(THREE))
    oid = submit(m, "three")
    with pytest.raises(SessionTerminatedError):
        m.run(oid)
    running = [t for t in ctx.orchestration.list_tasks(oid) if t.status is TaskStatus.RUNNING]
    assert running
    n = 5
    barrier, q = SPAWN.Barrier(n), SPAWN.Queue()
    procs = [
        _start(mp_workers.recover_once, str(ctx.data_dir), oid, THREE, 10.0, barrier, q)
        for _ in range(n)
    ]
    _collect(q, n)
    _join(*procs)
    for t in running:
        assert decisions(ctx, t.id).count("recover") == 1
    assert ctx.db.query("SELECT * FROM budget_reservations WHERE status='held'") == []
    spent, total = ledger_consistent(ctx, oid)
    assert spent == pytest.approx(total)


def test_stale_owner_process_cannot_overwrite_the_current_owner(ctx: AppContext) -> None:
    oid = submit(CareerManager(ctx, playbooks=_lib(ONE)))
    claimed, proceed, q = SPAWN.Event(), SPAWN.Event(), SPAWN.Queue()
    stale = _start(mp_workers.stale_owner, str(ctx.data_dir), oid, ONE, claimed, proceed, q)
    assert claimed.wait(60)
    # Another process, whose clock says the stale owner's lease expired, takes over.
    taker = _start(mp_workers.run_objective, str(ctx.data_dir), oid, ONE, q, 10.0)
    [taker_result] = _collect(q, 1)
    _join(taker)
    assert taker_result["status"] == "done"
    before = dict(task_by_step(ctx, oid, "m"))
    proceed.set()
    [stale_result] = _collect(q, 1)
    _join(stale)
    assert stale_result["outcomes"] == {
        "accept": "rejected",
        "requeue": "rejected",
        "block": "rejected",
        "model_call": "rejected",
    }
    after = dict(task_by_step(ctx, oid, "m"))
    assert after == before and after["owner_generation"] == 2
    assert all(r["owner_generation"] == 2 for r in calls(ctx, purpose="task"))
    assert ctx.db.query("SELECT * FROM approval_requests") == []


def test_live_heartbeating_process_keeps_its_task(ctx: AppContext) -> None:
    limits = {"lease_seconds": 2, "heartbeat_seconds": 0.05}
    oid = submit(CareerManager(ctx, playbooks=_lib(ONE)))
    entered, release, q = SPAWN.Event(), SPAWN.Event(), SPAWN.Queue()
    owner = _start(
        mp_workers.run_blocking,
        str(ctx.data_dir),
        oid,
        ONE,
        ["m"],
        entered,
        release,
        q,
        30.0,
        limits,
    )
    assert entered.wait(60)
    thief = _start(mp_workers.try_steal, str(ctx.data_dir), oid, ONE, 3.0, q)  # > 2 s lease
    [steal] = _collect(q, 1)
    _join(thief)
    assert steal["attempts"] >= 10
    release.set()
    [own] = _collect(q, 1)
    _join(owner)
    assert own["status"] == "done"
    t = task_by_step(ctx, oid, "m")
    assert decisions(ctx, t["id"]).count("recover") == 0 and t["owner_generation"] == 1
    assert [r["status"] for r in calls(ctx, purpose="task")] == ["ok"]


def test_late_reply_racing_with_recovery_across_processes(ctx: AppContext) -> None:
    oid = submit(CareerManager(ctx, playbooks=_lib(ONE)))
    entered, release, q = SPAWN.Event(), SPAWN.Event(), SPAWN.Queue()
    slow = _start(mp_workers.run_blocking, str(ctx.data_dir), oid, ONE, ["m"], entered, release, q)
    assert entered.wait(60)
    # Recovery in another process (forced: it believes the first one is dead).
    taker = _start(mp_workers.run_objective, str(ctx.data_dir), oid, ONE, q, 0.0, True)
    [took] = _collect(q, 1)
    _join(taker)
    assert took["status"] == "done"
    release.set()  # the first process's provider finally replies
    [late_owner] = _collect(q, 1)
    _join(slow)
    assert "error" not in late_owner
    rows = calls(ctx, purpose="task")
    assert [r["status"] for r in rows] == ["abandoned", "ok"]
    [late] = ctx.db.query("SELECT * FROM late_replies")
    assert late["call_id"] == rows[0]["id"] and late["adjustment_applied"] == 1
    assert rows[0]["cost_usd"] == pytest.approx(late["actual_cost_usd"])
    t = task_by_step(ctx, oid, "m")
    assert t["status"] == "succeeded" and t["owner_generation"] == 2
    assert [
        r["owner_generation"]
        for r in ctx.db.query(
            "SELECT owner_generation FROM manager_decisions WHERE decision='accept'"
        )
    ] == [2]
    spent, total = ledger_consistent(ctx, oid)
    assert spent == pytest.approx(total)
    assert t["spent_usd"] == pytest.approx(sum(float(r["cost_usd"]) for r in rows))


def test_heartbeat_failure_in_a_separate_process_halts_then_recovers(ctx: AppContext) -> None:
    limits = {"lease_seconds": 4, "heartbeat_seconds": 0.05, "lease_safety_margin_seconds": 1.5}
    oid = submit(CareerManager(ctx, playbooks=_lib(ONE)))
    entered, release, q = SPAWN.Event(), SPAWN.Event(), SPAWN.Queue()
    p = _start(
        mp_workers.run_blocking,
        str(ctx.data_dir),
        oid,
        ONE,
        ["m"],
        entered,
        release,
        q,
        3.0,
        limits,
        True,
    )
    [r] = _collect(q, 1)
    _join(p)
    assert r["stop_reason"] == "halted: lease renewal failed" and r["lease_lost"] is True
    assert task_by_step(ctx, oid, "m")["status"] == "running"  # left for recovery
    assert CareerManager(ctx, playbooks=_lib(ONE)).run(oid).status.value == "done"
    assert len(calls(ctx, purpose="task")) == 1  # the completed call is replayed by key


def test_full_objective_with_two_processes_and_a_killed_process(ctx: AppContext) -> None:
    from careeros.manager.manager import ObjectiveRequest

    m = CareerManager(ctx)
    oid = m.submit(
        ObjectiveRequest(title="x", kind="demo_application_package", inputs=SYNTHETIC_JOB)
    )
    q, entered, release = SPAWN.Queue(), SPAWN.Event(), SPAWN.Event()
    # Process 1 claims the first wave and blocks inside research_company's model call.
    killed = _start(
        mp_workers.block_then_die, str(ctx.data_dir), oid, "research_company", entered, release, q
    )
    assert entered.wait(60)
    # Process 2 works concurrently on everything it can (research_company is still owned).
    helper = _start(mp_workers.run_objective, str(ctx.data_dir), oid, None, q)
    [helped] = _collect(q, 1, timeout=120)
    _join(helper)
    assert helped["stop_reason"] == "tasks are running in another session"
    assert task_by_step(ctx, oid, "extract_keywords")["status"] == "succeeded"
    # Process 1 now gets its reply and is killed abruptly right after it.
    release.set()
    [died] = _collect(q, 1, timeout=120)
    _join(killed)
    assert killed.exitcode == 17 and died["died_at"] == "research_company"
    assert task_by_step(ctx, oid, "research_company")["status"] == "running"  # orphaned
    # Whatever is left is resumed once the dead process's leases have expired.
    assert CareerManager(ctx, clock=later(10)).run(oid).status.value == "done"
    assert done(ctx, oid)
    rows = ctx.db.query(
        "SELECT task_id, attempt, purpose, COUNT(*) n FROM model_calls"
        " WHERE status IN ('ok','malformed','timeout','rate_limit','error')"
        " GROUP BY task_id, attempt, purpose HAVING n > 1"
    )
    assert rows == []  # no logical call completed twice
    assert (
        ctx.db.query(
            "SELECT task_id, attempt FROM task_results GROUP BY task_id, attempt"
            " HAVING COUNT(*) > 1"
        )
        == []
    )
    for t in ctx.orchestration.list_tasks(oid):
        assert decisions(ctx, t.id).count("accept") == 1
    assert ctx.db.query("SELECT * FROM budget_reservations WHERE status='held'") == []
    spent, total = ledger_consistent(ctx, oid)
    assert spent == pytest.approx(total)


def test_concurrent_first_time_initialisation_from_processes(tmp_path: Any) -> None:
    """Audit L4: several processes opening the same fresh database used to fail
    ('table meta already exists' / 'database is locked')."""
    for trial in range(4):
        path = tmp_path / f"fresh{trial}.db"
        n = 6
        barrier, q = SPAWN.Barrier(n), SPAWN.Queue()
        procs = [_start(mp_workers.open_fresh, str(path), barrier, q) for _ in range(n)]
        results = _collect(q, n)
        _join(*procs)
        assert {(r["version"], r["kind"]) for r in results} == {(5, "synthetic")}
