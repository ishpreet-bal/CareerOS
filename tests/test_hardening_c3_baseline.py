"""C3 (audit L7) reproduction that runs against any version: SIGINT to a separate process
running an objective the way the ``resume`` command does. Kept apart from
``test_hardening_c3.py`` because it imports nothing introduced by C3."""

from __future__ import annotations

import multiprocessing as mp
import os
import signal
from typing import Any

from careeros.context import AppContext
from careeros.manager.manager import CareerManager, ObjectiveRequest
from careeros.manager.plans import PlaybookLibrary
from tests import mp_workers
from tests.m2_helpers import task_by_step
from tests.m3_helpers import book, model_step


def test_sigint_stops_at_a_safe_point_and_ends_the_session(ctx: AppContext) -> None:
    """Two dependent steps; SIGINT while the first is inside its provider call.

    Baseline: KeyboardInterrupt in the dispatcher; the worker nevertheless ran its task to
    completion (no safe stopping point), the run raised, and the session was never ended,
    so the work stayed leased until expiry. Now: the worker stops at the next safe point,
    hands its task back with the attempt open, nothing new is dispatched, and the session
    ends cleanly."""
    steps: list[dict[str, Any]] = [model_step("a"), model_step("b", deps=["a"])]
    oid = CareerManager(ctx, playbooks=PlaybookLibrary([book("l7", steps)])).submit(
        ObjectiveRequest(title="l7", kind="l7", inputs={"text": "x"}, budget_usd=2.0)
    )
    spawn = mp.get_context("spawn")
    entered, release, out = spawn.Queue(), spawn.Event(), spawn.Queue()
    p = spawn.Process(
        target=mp_workers.run_like_cli,
        args=(
            str(ctx.data_dir),
            oid,
            {"name": "l7", "plan": {"steps": steps}},
            ["a"],
            entered,
            release,
            out,
        ),
    )
    p.start()
    assert entered.get(timeout=30) == "a"
    os.kill(p.pid, signal.SIGINT)
    release.set()
    res = out.get(timeout=30)
    p.join(30)
    assert res.get("stop_reason") == "cancelled: SIGINT", res
    a = task_by_step(ctx, oid, "a")
    assert (a["status"], a["attempt"], a["attempt_open"], a["lease_owner"]) == (
        "ready",
        1,
        1,
        None,
    ), a
    assert task_by_step(ctx, oid, "b")["status"] == "pending"
    sess = ctx.db.query_one(
        "SELECT * FROM execution_sessions ORDER BY started_at DESC, rowid DESC LIMIT 1"
    )
    assert sess["ended_at"] is not None and sess["end_reason"] == "cancelled: SIGINT"
