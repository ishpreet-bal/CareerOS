"""C4 (audit L8) tests that run against any version: they read the settings with raw
``PRAGMA`` queries and import nothing introduced by C4.

* Connection durability: every way CareerOS opens the database must yield WAL with
  ``synchronous=FULL`` (2). On the baseline every connection used NORMAL (1).
* Crash injection (separate processes killed with ``os._exit``) around a model call's
  reservation (T1) and settlement (T2). These show **atomicity and recovery after a
  process crash** - which already held before C4 and must keep holding - and say nothing
  about power-loss durability, which needs real hardware/filesystem fault injection.
"""

from __future__ import annotations

import multiprocessing as mp
import sqlite3
import threading
from pathlib import Path
from typing import Any, ClassVar

import pytest

from careeros.context import AppContext
from careeros.core.config import ModelProfile
from careeros.core.db import DB_FILENAME, Database
from careeros.domain.models import ObjectiveStatus
from careeros.llm.adapters.mock import MockAdapter
from careeros.llm.contracts import ModelRequest, ModelResponse
from careeros.manager.manager import CareerManager, ObjectiveRequest
from careeros.manager.plans import PlaybookLibrary
from careeros.simulation import later
from tests import mp_workers
from tests.m2_helpers import calls, task_by_step
from tests.m3_helpers import book, model_step
from tests.phase_b_helpers import ledger_consistent, with_limits

SPAWN = mp.get_context("spawn")


def _settings(conn: sqlite3.Connection) -> tuple[str, int]:
    return (
        str(conn.execute("PRAGMA journal_mode").fetchone()[0]).lower(),
        int(conn.execute("PRAGMA synchronous").fetchone()[0]),
    )


def _lib(n: int = 1) -> PlaybookLibrary:
    return PlaybookLibrary([book("c4", [model_step(f"m{i}") for i in range(n)])])


def _submit(ctx: AppContext, n: int = 1) -> int:
    return CareerManager(ctx, playbooks=_lib(n)).submit(
        ObjectiveRequest(title="c4", kind="c4", inputs={"text": "x"}, budget_usd=2.0)
    )


def test_all_connection_paths_use_wal_and_synchronous_full(
    ctx: AppContext, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Main, worker (clone) and heartbeat connections, and a direct open (the path the
    heartbeat and other processes use). Baseline: synchronous=1 (NORMAL) everywhere."""
    seen: dict[str, list[tuple[str, int]]] = {"worker": [], "heartbeat": []}
    orig_clone, orig_renew = CareerManager._clone, CareerManager._renew_leases

    def clone(self: CareerManager) -> CareerManager:
        m = orig_clone(self)
        seen["worker"].append(_settings(m.db.conn))
        return m

    def renew(self: CareerManager, db: Database, session: str, state: Any) -> bool:
        seen["heartbeat"].append(_settings(db.conn))
        return orig_renew(self, db, session, state)

    monkeypatch.setattr(CareerManager, "_clone", clone)
    monkeypatch.setattr(CareerManager, "_renew_leases", renew)
    ctx.config = with_limits(ctx.config, heartbeat_seconds=0.05)

    class Slow(MockAdapter):
        mode_label: ClassVar[str] = "mock"

        def call(self, profile: ModelProfile, request: ModelRequest) -> ModelResponse:
            threading.Event().wait(0.2)
            return super().call(profile, request)

    oid = _submit(ctx, 3)
    assert CareerManager(ctx, adapter=Slow(), playbooks=_lib(3)).run(oid).status is (
        ObjectiveStatus.DONE
    )
    assert _settings(ctx.db.conn) == ("wal", 2), "main connection"
    assert seen["worker"] and set(seen["worker"]) == {("wal", 2)}, seen["worker"]
    assert seen["heartbeat"] and set(seen["heartbeat"]) == {("wal", 2)}, seen["heartbeat"]
    direct = Database.open(ctx.data_dir / DB_FILENAME, dataset_kind="synthetic", migrate=False)
    try:
        assert _settings(direct.conn) == ("wal", 2), "direct open"
    finally:
        direct.close()


def _invariants(db_path: Path, oid: int) -> None:
    conn = sqlite3.connect(str(db_path))
    try:
        assert conn.execute("PRAGMA integrity_check").fetchone()[0] == "ok"
        held = conn.execute(
            "SELECT COALESCE(SUM(amount_usd),0) FROM budget_reservations WHERE status='held'"
        ).fetchone()[0]
        reserved = conn.execute(
            "SELECT reserved_usd FROM budgets WHERE scope='objective' AND scope_ref=?",
            (str(oid),),
        ).fetchone()[0]
        assert reserved == pytest.approx(held)
        for r in conn.execute("SELECT reservation_id FROM model_calls WHERE status='in_flight'"):
            st = conn.execute(
                "SELECT status FROM budget_reservations WHERE id=?", (r[0],)
            ).fetchone()
            assert st is not None and st[0] == "held"  # T1 is all-or-nothing
    finally:
        conn.close()


@pytest.mark.parametrize("point", mp_workers.CRASH_POINTS)
def test_process_crash_around_reservation_and_settlement(ctx: AppContext, point: str) -> None:
    """After an abrupt kill at each boundary the file holds a consistent committed state;
    recovery completes the work with one authoritative call and a balanced ledger."""
    oid = _submit(ctx)
    q = SPAWN.Queue()
    bk = {"name": "c4", "plan": {"steps": [model_step("m0")]}}
    p = SPAWN.Process(target=mp_workers.crash_at, args=(str(ctx.data_dir), oid, bk, point, q))
    p.start()
    res = q.get(timeout=60)
    p.join(60)
    assert res.get("died_at") == point and p.exitcode == 17, res
    live = ctx.data_dir / DB_FILENAME
    _invariants(live, oid)
    [row] = calls(ctx, purpose="task")
    held = ctx.db.query("SELECT * FROM budget_reservations WHERE status='held'")
    if point == "after_settlement":
        assert row["status"] == "ok" and held == []  # T2 committed in full
    else:  # T2 never ran, or was rolled back by the crash
        assert row["status"] == "in_flight" and len(held) == 1
        assert not row["cost_usd"]
    s = CareerManager(ctx, playbooks=_lib(1), clock=later(10)).run(oid)
    assert s.status is ObjectiveStatus.DONE
    expect = ["ok"] if point == "after_settlement" else ["abandoned", "ok"]
    assert [r["status"] for r in calls(ctx, purpose="task")] == expect
    t = task_by_step(ctx, oid, "m0")
    assert (t["status"], t["attempt"]) == ("succeeded", 1)
    assert ctx.db.query("SELECT * FROM budget_reservations WHERE status='held'") == []
    _invariants(live, oid)
    spent, total = ledger_consistent(ctx, oid)
    assert spent == pytest.approx(total) and t["spent_usd"] == pytest.approx(total)
