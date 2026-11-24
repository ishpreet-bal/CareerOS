"""Hardening C4: SQLite durability, online backup and restore (audit L8).

What these tests can and cannot show:

* Durability *settings* are verified on real connections (``PRAGMA synchronous`` /
  ``journal_mode`` read back from SQLite) for every way CareerOS opens the database.
* Crash-injection tests kill a separate process (``os._exit``) at the boundaries of a
  model call's reservation (T1) and settlement (T2). They prove **atomicity and
  recovery after a process crash**. They do **not** prove durability across power loss
  or an operating-system crash: that depends on ``synchronous=FULL`` plus storage that
  honours fsync, which cannot be exercised without real hardware/filesystem fault
  injection (see ARCHITECTURE section 19.4).
"""

from __future__ import annotations

import multiprocessing as mp
import sqlite3
import threading
from pathlib import Path
from typing import Any, ClassVar

import pytest
from typer.testing import CliRunner

import careeros.core.db as dbmod
from careeros.cli.main import app
from careeros.context import AppContext, open_context_at
from careeros.core.backup import (
    backup_database,
    live_sessions,
    restore_database,
    verify_backup,
)
from careeros.core.config import AppConfig, ModelProfile
from careeros.core.db import DB_FILENAME, Database
from careeros.core.errors import DurabilityError
from careeros.domain.models import ObjectiveStatus
from careeros.llm.adapters.mock import MockAdapter
from careeros.llm.contracts import ModelRequest, ModelResponse
from careeros.llm.gateway import Reply
from careeros.manager.manager import CareerManager, ObjectiveRequest
from careeros.manager.plans import PlaybookLibrary
from careeros.simulation import later
from tests import mp_workers
from tests.conftest import CONFIG_DIR
from tests.m2_helpers import calls, task_by_step
from tests.m3_helpers import book, model_step
from tests.phase_b_helpers import ledger_consistent

SPAWN = mp.get_context("spawn")
FULL = {"journal_mode": "wal", "synchronous": "FULL", "synchronous_level": 2, "fullfsync": True}


def _durable(d: dict[str, Any]) -> bool:
    return d == FULL


def _lib(n: int = 1) -> PlaybookLibrary:
    return PlaybookLibrary([book("c4", [model_step(f"m{i}") for i in range(n)])])


def _submit(ctx: AppContext, n: int = 1) -> int:
    return CareerManager(ctx, playbooks=_lib(n)).submit(
        ObjectiveRequest(title="c4", kind="c4", inputs={"text": "x"}, budget_usd=2.0)
    )


def _invariants(db_path: Path, oid: int) -> None:
    """Accounting invariants that must hold in any committed state (live or backup)."""
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    try:
        for line in conn.execute("SELECT * FROM budgets"):
            held = conn.execute(
                "SELECT COALESCE(SUM(amount_usd),0) FROM budget_reservations"
                " WHERE status='held' AND budget_ids LIKE ?",
                (f"%{line['id']}%",),
            ).fetchone()[0]
            if line["scope"] == "objective":
                assert line["reserved_usd"] == pytest.approx(held), dict(line)
        spent = conn.execute(
            "SELECT spent_usd FROM budgets WHERE scope='objective' AND scope_ref=?", (str(oid),)
        ).fetchone()[0]
        total = conn.execute(
            "SELECT COALESCE(SUM(cost_usd),0) FROM model_calls WHERE objective_id=?", (oid,)
        ).fetchone()[0]
        assert spent == pytest.approx(total)
        for r in conn.execute("SELECT * FROM model_calls WHERE status='in_flight'"):
            res = conn.execute(
                "SELECT status FROM budget_reservations WHERE id=?", (r["reservation_id"],)
            ).fetchone()
            assert res is not None and res[0] == "held"  # T1 is all-or-nothing
    finally:
        conn.close()


# ============================================================ connection durability
def test_main_connection_is_wal_and_full(ctx: AppContext) -> None:
    assert ctx.db.durability() == FULL


def test_every_connection_path_is_durable(ctx: AppContext, monkeypatch: pytest.MonkeyPatch) -> None:
    """Main, worker (clone) and heartbeat connections all report WAL + FULL."""
    seen: dict[str, list[dict[str, Any]]] = {"worker": [], "heartbeat": []}
    orig_clone, orig_renew = CareerManager._clone, CareerManager._renew_leases

    def clone(self: CareerManager) -> CareerManager:
        m = orig_clone(self)
        seen["worker"].append(m.db.durability())
        return m

    def renew(self: CareerManager, db: Database, session: str, state: Any) -> bool:
        seen["heartbeat"].append(db.durability())
        return orig_renew(self, db, session, state)

    monkeypatch.setattr(CareerManager, "_clone", clone)
    monkeypatch.setattr(CareerManager, "_renew_leases", renew)
    from tests.phase_b_helpers import with_limits

    ctx.config = with_limits(ctx.config, heartbeat_seconds=0.05)

    class Slow(MockAdapter):  # long enough for a few heartbeats
        mode_label: ClassVar[str] = "mock"

        def call(self, profile: ModelProfile, request: ModelRequest) -> ModelResponse:
            threading.Event().wait(0.2)
            return super().call(profile, request)

    oid = _submit(ctx, 3)
    assert CareerManager(ctx, adapter=Slow(), playbooks=_lib(3)).run(oid).status is (
        ObjectiveStatus.DONE
    )
    assert seen["worker"] and all(_durable(d) for d in seen["worker"])
    assert seen["heartbeat"] and all(_durable(d) for d in seen["heartbeat"])
    direct = Database.open(ctx.data_dir / DB_FILENAME, dataset_kind="synthetic", migrate=False)
    try:
        assert direct.durability() == FULL  # the path the heartbeat uses
    finally:
        direct.close()


def test_recovery_connection_in_a_separate_process_is_durable(ctx: AppContext) -> None:
    oid = _submit(ctx)
    q = SPAWN.Queue()
    p = SPAWN.Process(target=mp_workers.durability_after_recover, args=(str(ctx.data_dir), oid, q))
    p.start()
    res = q.get(timeout=60)
    p.join(60)
    assert "error" not in res, res.get("error")
    assert res["durability"] == FULL


def test_weaker_settings_are_refused_on_open(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Settings are verified on the actual connection: if WAL cannot be enabled (e.g. an
    unsupported filesystem), opening fails instead of silently running less durably."""
    monkeypatch.setattr(dbmod, "_set_wal", lambda conn, attempts=100: None)
    with pytest.raises(DurabilityError, match="journal_mode=delete"):
        Database.open(tmp_path / "x.db", dataset_kind="synthetic")


def test_in_memory_databases_are_exempt() -> None:
    db = Database.open(":memory:", dataset_kind="synthetic")
    try:
        db.verify_durability()  # nothing to make durable; no error
    finally:
        db.close()


# ============================================================================ backup
class Hold(MockAdapter):
    mode_label: ClassVar[str] = "mock"

    def __init__(self) -> None:
        self.entered, self.release = threading.Event(), threading.Event()

    def call(self, profile: ModelProfile, request: ModelRequest) -> ModelResponse:
        if request.task_type != "quality.review" and not self.entered.is_set():
            self.entered.set()
            self.release.wait(30)
        return super().call(profile, request)


def _run_in_thread(
    ctx: AppContext, oid: int, adapter: MockAdapter, n: int
) -> tuple[Any, dict[str, Any]]:
    out: dict[str, Any] = {}

    def go() -> None:
        try:
            with open_context_at(ctx.config, ctx.data_dir) as c:
                out["s"] = CareerManager(c, adapter=adapter, playbooks=_lib(n)).run(oid)
        except BaseException as exc:
            out["e"] = exc

    t = threading.Thread(target=go, daemon=True)
    t.start()
    return t, out


def test_backup_while_in_use_is_consistent_and_does_not_disturb_workers(
    ctx: AppContext, tmp_path: Path
) -> None:
    """A backup taken while a worker holds a lease, a reservation and an in-flight call,
    and while another connection keeps writing, is a consistent committed snapshot; the
    run carries on unaffected."""
    oid = _submit(ctx, 2)
    hold = Hold()
    t, out = _run_in_thread(ctx, oid, hold, 2)
    assert hold.entered.wait(15)
    stop = threading.Event()
    writes = [0]

    def writer() -> None:  # concurrent committed writes from another connection
        with open_context_at(ctx.config, ctx.data_dir) as c:
            while not stop.is_set():
                sid = c.orchestration.start_session()
                c.orchestration.end_session(sid, "writer")
                writes[0] += 1

    w = threading.Thread(target=writer, daemon=True)
    w.start()
    try:
        backups = [
            backup_database(ctx.data_dir / DB_FILENAME, tmp_path / f"b{i}.db") for i in range(3)
        ]
    finally:
        stop.set()
        w.join(10)
    assert writes[0] > 0
    for b in backups:
        assert b.ok and b.schema_version == 5 and b.dataset_kind == "synthetic"
        assert verify_backup(b.path).ok
        _invariants(b.path, oid)
        conn = sqlite3.connect(str(b.path))
        try:  # the snapshot holds the in-flight state exactly as committed
            assert (
                conn.execute(
                    "SELECT COUNT(*) FROM model_calls WHERE status='in_flight'"
                ).fetchone()[0]
                == 1
            )
            assert (
                conn.execute(
                    "SELECT COUNT(*) FROM budget_reservations WHERE status='held'"
                ).fetchone()[0]
                >= 1
            )
            assert conn.execute("PRAGMA journal_mode").fetchone()[0] == "delete"  # one file
        finally:
            conn.close()
    hold.release.set()
    t.join(30)
    assert "e" not in out, out.get("e")
    assert out["s"].status is ObjectiveStatus.DONE
    assert not any("ownership lost" in e for e in out["s"].events)
    _invariants(ctx.data_dir / DB_FILENAME, oid)


def test_backup_refuses_to_overwrite(ctx: AppContext, tmp_path: Path) -> None:
    src = ctx.data_dir / DB_FILENAME
    with pytest.raises(DurabilityError):
        backup_database(src, src)
    backup_database(src, tmp_path / "b.db")
    with pytest.raises(FileExistsError):
        backup_database(src, tmp_path / "b.db")
    assert backup_database(src, tmp_path / "b.db", overwrite=True).ok


def test_backup_that_fails_verification_leaves_nothing(
    ctx: AppContext, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    import careeros.core.backup as bk
    from careeros.core.backup import VerifyResult

    monkeypatch.setattr(bk, "verify_backup", lambda p: VerifyResult(Path(p), False, ["simulated"]))
    out = tmp_path / "backups"
    with pytest.raises(DurabilityError, match="simulated"):
        backup_database(ctx.data_dir / DB_FILENAME, out / "b.db")
    assert list(out.iterdir()) == []  # no backup, no partial file


# ==================================================================== verification
def _corrupt(path: Path, offset: int, data: bytes) -> None:
    with path.open("r+b") as fh:
        fh.seek(offset)
        fh.write(data)


def test_verification_reports_corruption_clearly(ctx: AppContext, tmp_path: Path) -> None:
    oid = _submit(ctx, 3)
    CareerManager(ctx, playbooks=_lib(3)).run(oid)
    good = backup_database(ctx.data_dir / DB_FILENAME, tmp_path / "good.db")
    assert good.ok and "OK" in good.summary()
    size = good.path.stat().st_size
    page = 4096
    cases: dict[str, Path] = {}
    for name in ("pages", "truncated", "garbage", "missing", "foreign"):
        cases[name] = tmp_path / f"{name}.db"
    cases["pages"].write_bytes(good.path.read_bytes())
    for k in range(2, min(size // page, 12) + 1):  # page k starts at (k - 1) * page
        _corrupt(cases["pages"], (k - 1) * page, b"\x07")  # invalid b-tree page type
    cases["truncated"].write_bytes(good.path.read_bytes()[: size // 3 + 100])
    cases["garbage"].write_bytes(b"not a database" * 500)
    other = sqlite3.connect(str(cases["foreign"]))
    other.execute("CREATE TABLE t (x)")
    other.commit()
    other.close()
    for name, path in cases.items():
        r = verify_backup(path)
        assert not r.ok, name
        assert r.problems and "FAILED" in r.summary(), (name, r)
    # Logical damage SQLite can open but integrity_check / foreign_key_check reject.
    index = tmp_path / "index.db"
    index.write_bytes(good.path.read_bytes())
    conn = sqlite3.connect(str(index))
    conn.executescript(
        "CREATE TABLE zz(x); CREATE INDEX zz_i ON zz(x); INSERT INTO zz VALUES (1),(2),(3);"
        "CREATE TABLE zz2(x); CREATE INDEX zz2_i ON zz2(x);"
    )
    root = conn.execute("SELECT rootpage FROM sqlite_master WHERE name='zz2_i'").fetchone()[0]
    conn.execute("PRAGMA writable_schema=ON")
    conn.execute("UPDATE sqlite_master SET rootpage=? WHERE name='zz_i'", (root,))
    conn.commit()
    conn.close()
    r = verify_backup(index)
    assert not r.ok and any("missing from index zz_i" in x for x in r.problems), r
    orphan = tmp_path / "orphan.db"
    orphan.write_bytes(good.path.read_bytes())
    conn = sqlite3.connect(str(orphan))  # foreign keys off: insert a dangling reference
    conn.execute(
        "INSERT INTO plans (objective_id, version, source, created_at) VALUES (999,1,'x','t')"
    )
    conn.commit()
    conn.close()
    r = verify_backup(orphan)
    assert not r.ok and any("foreign_key_check" in x for x in r.problems), r
    assert "does not exist" in verify_backup(cases["missing"]).problems[0]
    assert "not a CareerOS database" in verify_backup(cases["foreign"]).problems[0]
    assert "not a valid SQLite database" in verify_backup(cases["garbage"]).problems[0]


# =========================================================================== restore
def _objective_count(path: Path) -> int:
    conn = sqlite3.connect(str(path))
    try:
        return int(conn.execute("SELECT COUNT(*) FROM objectives").fetchone()[0])
    finally:
        conn.close()


def test_restore_from_a_valid_backup(ctx: AppContext, tmp_path: Path) -> None:
    live = ctx.data_dir / DB_FILENAME
    _submit(ctx)
    a = backup_database(live, tmp_path / "a.db")
    _submit(ctx)
    _submit(ctx)
    assert _objective_count(live) == 3
    restored, saved = restore_database(
        a.path, live, expected_dataset_kind="synthetic", safety_copy=tmp_path / "pre.db"
    )
    assert restored.ok and _objective_count(live) == 1
    assert saved is not None and verify_backup(saved).ok and _objective_count(saved) == 3
    assert ctx.db.query_one("SELECT COUNT(*) n FROM objectives")["n"] == 1  # open connection
    with open_context_at(ctx.config, ctx.data_dir) as c:  # reopens WAL + FULL
        assert c.db.durability() == FULL
        assert c.db.query("PRAGMA integrity_check")[0][0] == "ok"


def test_restore_refuses_bad_backups_and_leaves_the_database_untouched(
    ctx: AppContext, tmp_path: Path
) -> None:
    live = ctx.data_dir / DB_FILENAME
    _submit(ctx)
    bad = tmp_path / "bad.db"
    bad.write_bytes(b"garbage" * 1000)
    with pytest.raises(DurabilityError, match="Refusing to restore"):
        restore_database(bad, live, expected_dataset_kind="synthetic")
    personal = tmp_path / "personal.db"
    Database.open(personal, dataset_kind="personal").close()
    with pytest.raises(DurabilityError, match="'personal' data"):
        restore_database(personal, live, expected_dataset_kind="synthetic")
    assert _objective_count(live) == 1 and verify_backup(live).ok
    assert not list(ctx.data_dir.glob("*.pre-restore-*"))  # nothing was replaced


def test_restore_refuses_while_a_session_is_running(ctx: AppContext, tmp_path: Path) -> None:
    live = ctx.data_dir / DB_FILENAME
    b = backup_database(live, tmp_path / "b.db")
    sid = ctx.orchestration.start_session()  # a running CareerOS session
    assert live_sessions(live) == [sid]
    with pytest.raises(DurabilityError, match="session"):
        restore_database(b.path, live, expected_dataset_kind="synthetic")
    restored, _ = restore_database(b.path, live, expected_dataset_kind="synthetic", force=True)
    assert restored.ok


def test_restore_mid_run_snapshot_then_recovery_keeps_all_guarantees(
    ctx: AppContext, tmp_path: Path
) -> None:
    """Restore a backup taken while a call was in flight; recovery then treats it like a
    crash: the attempt is resumed, the in-flight call abandoned (charged its reservation),
    the work completed once, and a late reply is reconciled exactly once."""
    oid = _submit(ctx)
    hold = Hold()
    t, out = _run_in_thread(ctx, oid, hold, 1)
    assert hold.entered.wait(15)
    snap = backup_database(ctx.data_dir / DB_FILENAME, tmp_path / "snap.db")
    hold.release.set()
    t.join(30)
    assert out["s"].status is ObjectiveStatus.DONE
    live = ctx.data_dir / DB_FILENAME
    restore_database(snap.path, live, expected_dataset_kind="synthetic", force=True)
    t0 = task_by_step(ctx, oid, "m0")
    assert (t0["status"], t0["attempt"], t0["owner_generation"]) == ("running", 1, 1)
    m = CareerManager(ctx, playbooks=_lib(1), clock=later(10))
    assert m.run(oid).status is ObjectiveStatus.DONE
    t1 = task_by_step(ctx, oid, "m0")
    assert (t1["status"], t1["attempt"], t1["owner_generation"]) == ("succeeded", 1, 2)
    rows = calls(ctx, purpose="task")
    assert [r["status"] for r in rows] == ["abandoned", "ok"]
    assert ctx.db.query("SELECT * FROM budget_reservations WHERE status='held'") == []
    spent, total = ledger_consistent(ctx, oid)
    assert spent == pytest.approx(total)
    reply = Reply("ok", None, 100, 100, 5, float(rows[0]["reserved_usd"]) * 0.5, "{}", "prov-x")
    assert m.gateway.record_late_reply(int(str(rows[0]["id"])), reply) is True
    assert m.gateway.record_late_reply(int(str(rows[0]["id"])), reply) is False
    spent, total = ledger_consistent(ctx, oid)
    assert spent == pytest.approx(total)
    assert len(ctx.db.query("SELECT * FROM manager_decisions WHERE decision='accept'")) == 1


# =============================================================================== CLI
def _cli(*args: str) -> Any:
    return CliRunner().invoke(app, [*args, "--config-dir", str(CONFIG_DIR)])


def test_cli_backup_verify_and_restore(cfg: AppConfig, data_dir: Path, tmp_path: Path) -> None:
    with open_context_at(cfg, data_dir) as c:
        _submit(c)
    dest = tmp_path / "out" / "b.db"
    res = _cli("backup", "--to", str(dest))
    assert res.exit_code == 0 and "verified" in res.output and dest.exists(), res.output
    assert CliRunner().invoke(app, ["verify-backup", str(dest)]).exit_code == 0
    bad = tmp_path / "bad.db"
    bad.write_bytes(b"x" * 5000)
    res = CliRunner().invoke(app, ["verify-backup", str(bad)])
    assert res.exit_code == 1 and "FAILED" in res.output
    res = _cli("restore", str(dest))
    assert res.exit_code == 0 and "Restored" in res.output, res.output
    res = _cli("backup")  # default location under the data directory
    assert res.exit_code == 0 and list((data_dir / "backups").glob("careeros-*.db"))


def test_cli_refuses_a_backup_inside_a_git_repository(cfg: AppConfig, data_dir: Path) -> None:
    with open_context_at(cfg, data_dir) as c:
        _submit(c)
    inside = Path(__file__).resolve().parents[1] / "build" / "never.db"
    res = _cli("backup", "--to", str(inside))
    assert res.exit_code == 2 and "inside the git repository" in res.output
    assert not inside.exists()
