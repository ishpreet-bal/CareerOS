"""Migration 0005: open-attempt backfill and duplicate archiving on pre-Phase-B databases."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

import careeros.core.db as dbmod
from careeros.core.db import Database
from careeros.orchestration.fencing import claim

NOW = "2026-11-20T10:00:00+00:00"
LATER = "2026-11-20T11:00:00+00:00"

# step: (status, stored attempt, decisions [(kind, inputs, created_at)], approval requests
#        [(kind, status, resolved_at)], expected (attempt, attempt_open))
CASES: dict[
    str,
    tuple[
        str,
        int,
        list[tuple[str, dict[str, Any], str]],
        list[tuple[str, str, str | None]],
        tuple[int, int],
    ],
] = {
    "running": ("running", 2, [("route", {}, NOW)], [], (2, 1)),
    "blocked": (
        "blocked_budget",
        1,
        [("escalate_tier", {"attempt": 1}, NOW), ("await_human", {"needed_usd": 1}, NOW)],
        [("budget_increase", "pending", None)],
        (2, 1),
    ),
    "requeued": ("ready", 0, [("route", {}, NOW), ("requeue", {}, NOW)], [], (1, 1)),
    "recovered": ("ready", 1, [("recover", {"recovered": True}, NOW)], [], (2, 1)),
    "legacy_recovered": ("ready", 1, [("retry_same", {"recovered": True}, NOW)], [], (2, 1)),
    "retrying": ("retry_scheduled", 1, [("escalate_tier", {"attempt": 1}, NOW)], [], (1, 0)),
    "never_started": ("pending", 0, [], [], (0, 0)),
    "completed": ("succeeded", 1, [("accept", {}, NOW)], [], (1, 0)),
    "approved_budget": (
        "ready",
        1,
        [("escalate_tier", {"attempt": 1}, NOW), ("await_human", {"needed_usd": 1}, NOW)],
        [("budget_increase", "approved", NOW)],
        (2, 1),
    ),
    "approved_retry_limit": (
        "ready",
        3,
        [("await_human", {"outcome": "malformed"}, NOW)],
        [("retry_limit", "approved", NOW)],
        (3, 0),
    ),
    "retry_after_approval": (
        "ready",
        2,
        [("await_human", {"needed_usd": 1}, NOW), ("retry_same", {"attempt": 2}, LATER)],
        [("budget_increase", "approved", NOW)],
        (2, 0),
    ),
}


def _v4_database(path: Path, monkeypatch: pytest.MonkeyPatch) -> dict[str, int]:
    """Build a schema-v4 (pre-Phase-B) database with one task per case."""
    orig = dbmod.load_migrations
    monkeypatch.setattr(dbmod, "load_migrations", lambda: [m for m in orig() if m.version <= 4])
    db = Database.open(path, dataset_kind="synthetic")
    assert db.schema_version() == 4
    c = db.conn
    c.execute(
        "INSERT INTO objectives (id, kind, title, definition_of_done, created_at,"
        " updated_at) VALUES (1,'k','t','[\"x\"]',?,?)",
        (NOW, NOW),
    )
    c.execute(
        "INSERT INTO plans (id, objective_id, version, source, created_at) VALUES (1,1,1,'test',?)",
        (NOW,),
    )
    for sid in ("old", "new"):
        c.execute("INSERT INTO execution_sessions (id, started_at) VALUES (?, ?)", (sid, NOW))
    ids: dict[str, int] = {}
    for step, (status, attempt, decs, reqs, _) in CASES.items():
        cur = c.execute(
            "INSERT INTO tasks (objective_id, plan_id, worker, step, task_type, status, attempt,"
            " lease_owner, lease_expires_at, created_at, updated_at)"
            " VALUES (1,1,'w',?,'job.analyze',?,?,?,?,?,?)",
            (
                step,
                status,
                attempt,
                "old" if status == "running" else None,
                NOW if status == "running" else None,
                NOW,
                NOW,
            ),
        )
        tid = int(cur.lastrowid or 0)
        ids[step] = tid
        for kind, inputs, at in decs:
            c.execute(
                "INSERT INTO manager_decisions (objective_id, task_id, decision, inputs,"
                " created_at) VALUES (1,?,?,?,?)",
                (tid, kind, json.dumps(inputs), at),
            )
        for kind, rstatus, resolved in reqs:
            c.execute(
                "INSERT INTO approval_requests (objective_id, task_id, kind, status,"
                " created_at, resolved_at) VALUES (1,?,?,?,?,?)",
                (tid, kind, rstatus, NOW, resolved),
            )
    # Duplicate results from the old late-writer bug, with the accepted one in the middle.
    tid = ids["completed"]
    rid = []
    for _ in range(3):
        cur = c.execute(
            "INSERT INTO task_results (task_id, attempt, schema_version, status,"
            " confidence, created_at) VALUES (?,1,'1','succeeded',0.9,?)",
            (tid, NOW),
        )
        rid.append(int(cur.lastrowid or 0))
    c.execute("UPDATE tasks SET accepted_result_id=? WHERE id=?", (rid[1], tid))
    for r in (rid[0], rid[0], rid[2]):  # two duplicate reviews of an archived result
        c.execute(
            "INSERT INTO reviews (task_id, result_id, reviewer, verdict, created_at)"
            " VALUES (?,?,'model:x','accept',?)",
            (tid, r, NOW),
        )
    db.close()
    monkeypatch.setattr(dbmod, "load_migrations", orig)
    ids["_kept"], ids["_dup_a"], ids["_dup_b"] = rid[1], rid[0], rid[2]
    return ids


def test_backfill_preserves_open_attempts(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    path = tmp_path / "v4.db"
    ids = _v4_database(path, monkeypatch)
    with Database.open(path, dataset_kind="synthetic") as db:
        assert db.schema_version() == 5
        for step, (_, _old, _, _, (exp_attempt, exp_open)) in CASES.items():
            r = db.query_one(
                "SELECT attempt, attempt_open, owner_generation FROM tasks WHERE id=?", (ids[step],)
            )
            assert (r["attempt"], r["attempt_open"], r["owner_generation"]) == (
                exp_attempt,
                exp_open,
                0,
            ), step


@pytest.mark.parametrize(
    "step", [s for s, c in CASES.items() if c[0] in ("ready", "pending", "retry_scheduled")]
)
def test_next_claim_uses_the_same_attempt_number_as_before(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, step: str
) -> None:
    """The pre-Phase-B code would have claimed attempt = stored + 1. After migration the
    new claim must arrive at the same number (resuming or starting as appropriate)."""
    path = tmp_path / "v4.db"
    ids = _v4_database(path, monkeypatch)
    old_next = CASES[step][1] + 1
    with Database.open(path, dataset_kind="synthetic") as db, db.transaction() as c:
        fence = claim(c, ids[step], "new", "2099-01-01T00:00:00+00:00", NOW)
    assert fence is not None and fence.attempt == old_next and fence.generation == 1


def test_running_task_resumes_its_claimed_attempt(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    path = tmp_path / "v4.db"
    ids = _v4_database(path, monkeypatch)
    with Database.open(path, dataset_kind="synthetic") as db:
        with db.transaction() as c:  # recovery would release it; then it is claimed again
            c.execute(
                "UPDATE tasks SET status='ready', lease_owner=NULL WHERE id=?", (ids["running"],)
            )
        with db.transaction() as c:
            fence = claim(c, ids["running"], "new", "2099-01-01T00:00:00+00:00", NOW)
    assert fence is not None and fence.attempt == 2  # same attempt, not a new one


def test_duplicate_results_and_reviews_are_archived_not_lost(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    path = tmp_path / "v4.db"
    ids = _v4_database(path, monkeypatch)
    with Database.open(path, dataset_kind="synthetic") as db:
        live = [r["id"] for r in db.query("SELECT id FROM task_results")]
        assert live == [ids["_kept"]]
        archived = db.query("SELECT id, kept_result_id FROM task_results_duplicates ORDER BY id")
        assert [(r["id"], r["kept_result_id"]) for r in archived] == [
            (ids["_dup_a"], ids["_kept"]),
            (ids["_dup_b"], ids["_kept"]),
        ]
        assert {r["result_id"] for r in db.query("SELECT result_id FROM reviews")} == {ids["_kept"]}
        assert (
            len(db.query("SELECT * FROM reviews"))
            + len(db.query("SELECT * FROM reviews_duplicates"))
            == 3
        )
        assert db.query("PRAGMA foreign_key_check") == []
        assert db.query_one("PRAGMA integrity_check")[0] == "ok"
        for _ in range(2):  # re-opening is a no-op
            assert db.migrate() == []
