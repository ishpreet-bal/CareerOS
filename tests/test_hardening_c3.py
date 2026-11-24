"""Hardening C3: graceful shutdown and cancellation (audit L7).

In-process tests drive ``CareerManager.request_cancel`` from the test thread with explicit
synchronisation (events set inside the simulated provider, decisions observed in the
database). Process-level tests send real SIGINT/SIGTERM to separate processes running the
CLI's shutdown handling (``graceful_shutdown``).
"""

from __future__ import annotations

import multiprocessing as mp
import os
import signal
import sqlite3
import threading
import time
from collections.abc import Callable
from typing import Any, ClassVar

import pytest

from careeros.context import AppContext, open_context_at
from careeros.core.config import ModelProfile
from careeros.cost.budget import BudgetManager
from careeros.domain.models import ObjectiveStatus
from careeros.llm.adapters.mock import MockAdapter
from careeros.llm.contracts import ModelRequest, ModelResponse
from careeros.manager.manager import CareerManager, ObjectiveRequest, RunSummary
from careeros.manager.plans import PlaybookLibrary
from careeros.orchestration.shutdown import forced_exit_code, graceful_shutdown
from careeros.simulation import later
from tests import mp_workers
from tests.m2_helpers import calls, task_by_step
from tests.m3_helpers import book, model_step
from tests.phase_b_helpers import ledger_consistent, with_limits

SPAWN = mp.get_context("spawn")


class Hold(MockAdapter):
    """Calls for the given steps wait inside the 'provider' until ``release``; each sets
    its own ``entered[step]`` event when it starts."""

    mode_label: ClassVar[str] = "mock"

    def __init__(self, steps: set[str], timeout: float = 30.0) -> None:
        self.pending = set(steps)
        self.entered = {s: threading.Event() for s in steps}
        self.release = threading.Event()
        self.returned = threading.Event()
        self.timeout = timeout
        self._lock = threading.Lock()

    def call(self, profile: ModelProfile, request: ModelRequest) -> ModelResponse:
        step = request.simulation.get("step")
        block = False
        with self._lock:
            if step in self.pending and request.task_type != "quality.review":
                self.pending.discard(step)
                block = True
        if block:
            self.entered[step].set()
            self.release.wait(self.timeout)
            try:
                return super().call(profile, request)
            finally:
                self.returned.set()
        return super().call(profile, request)

    def wait_all_entered(self, timeout: float = 15.0) -> bool:
        return all(e.wait(timeout) for e in self.entered.values())


def _steps(n: int) -> list[dict[str, Any]]:
    return [model_step(f"m{i}") for i in range(n)]


def _lib(steps: list[dict[str, Any]]) -> PlaybookLibrary:
    return PlaybookLibrary([book("c3", steps)])


def _submit(ctx: AppContext, steps: list[dict[str, Any]]) -> int:
    return CareerManager(ctx, playbooks=_lib(steps)).submit(
        ObjectiveRequest(title="c3", kind="c3", inputs={"text": "x"}, budget_usd=2.0)
    )


class Runner:
    """``run`` in a thread with its own connection; the manager is exposed so the test
    can request cancellation."""

    def __init__(
        self, ctx: AppContext, oid: int, factory: Callable[[AppContext], CareerManager]
    ) -> None:
        self.ready = threading.Event()
        self.manager: CareerManager | None = None
        self.summary: RunSummary | None = None
        self.error: BaseException | None = None

        def go() -> None:
            try:
                with open_context_at(ctx.config, ctx.data_dir) as c:
                    self.manager = factory(c)
                    self.ready.set()
                    self.summary = self.manager.run(oid)
            except BaseException as exc:  # surfaced by the test
                self.error = exc
                self.ready.set()

        self.thread = threading.Thread(target=go, daemon=True)
        self.thread.start()
        assert self.ready.wait(15)

    def cancel(self, reason: str = "test") -> bool:
        assert self.manager is not None
        return self.manager.request_cancel(reason)

    def join(self, timeout: float = 30.0) -> RunSummary:
        self.thread.join(timeout)
        assert not self.thread.is_alive(), "run did not return"
        assert self.error is None, repr(self.error)
        assert self.summary is not None
        return self.summary


def _session(ctx: AppContext, sid: str | None = None) -> Any:
    if sid is None:
        return ctx.db.query_one(
            "SELECT * FROM execution_sessions ORDER BY started_at DESC, rowid DESC LIMIT 1"
        )
    return ctx.db.query_one("SELECT * FROM execution_sessions WHERE id=?", (sid,))


def _shutdown_decisions(ctx: AppContext) -> list[dict[str, Any]]:
    """Inputs of the hand-back decisions written on shutdown."""
    import json

    return [
        json.loads(r["inputs"])
        for r in ctx.db.query(
            "SELECT inputs FROM manager_decisions WHERE decision='requeue'"
            " AND json_extract(inputs, '$.cancelled') = 1 ORDER BY id"
        )
    ]


def _assert_released(ctx: AppContext, oid: int, step: str, attempt: int = 1) -> None:
    t = task_by_step(ctx, oid, step)
    assert (t["status"], t["attempt"], t["attempt_open"], t["lease_owner"]) == (
        "ready",
        attempt,
        1,
        None,
    ), t


def _no_held_reservations(ctx: AppContext) -> None:
    assert ctx.db.query("SELECT * FROM budget_reservations WHERE status='held'") == []


def _ledger_ok(ctx: AppContext, oid: int) -> None:
    spent, total = ledger_consistent(ctx, oid)
    assert spent == pytest.approx(total)


# ======================================================================== in-process
def test_cancel_before_dispatch_claims_nothing(ctx: AppContext) -> None:
    steps = _steps(2)
    oid = _submit(ctx, steps)
    m = CareerManager(ctx, playbooks=_lib(steps))
    assert m.request_cancel("before start") is True
    s = m.run(oid)
    assert s.stop_reason == "cancelled: before start"
    assert (s.attempts_executed, s.dispatches) == (0, 0)
    assert [str(task_by_step(ctx, oid, f"m{i}")["status"]) for i in range(2)] == [
        "pending",
        "pending",
    ]
    assert _session(ctx)["end_reason"] == "cancelled: before start"


def test_cancel_while_idle_in_contention_backoff(ctx: AppContext) -> None:
    """The dispatcher is idle (no worker running) waiting out budget contention."""
    steps = _steps(1)
    oid = _submit(ctx, steps)
    bm = BudgetManager(ctx.db, ctx.config)
    bm.lines_for(oid, None, None)
    line = ctx.db.query_one(
        "SELECT id, limit_usd FROM budgets WHERE scope='objective' AND scope_ref=?", (str(oid),)
    )
    assert line is not None
    bm.reserve([int(line["id"])], float(line["limit_usd"]) - 0.001, session_id=None)
    requeued = threading.Event()
    orig = CareerManager._requeue

    def requeue(self: CareerManager, fence: Any, task: Any, reason: str) -> None:
        orig(self, fence, task, reason)
        requeued.set()

    CareerManager._requeue = requeue  # type: ignore[method-assign]
    try:
        r = Runner(ctx, oid, lambda c: CareerManager(c, playbooks=_lib(steps)))
        assert requeued.wait(15)
        t0 = time.monotonic()
        r.cancel("idle")
        s = r.join()
    finally:
        CareerManager._requeue = orig  # type: ignore[method-assign]
    assert s.stop_reason == "cancelled: idle"
    assert time.monotonic() - t0 < 2.0  # well before the 2.5 s contention back-off ends
    _assert_released(ctx, oid, "m0")
    assert _session(ctx)["ended_at"] is not None


@pytest.mark.parametrize("n", [1, 3], ids=["one_worker", "three_workers"])
def test_cancel_with_workers_inside_provider_calls_stops_at_safe_points(
    ctx: AppContext, n: int
) -> None:
    """Shutdown during in-flight model calls (several workers concurrently): each call
    completes authoritatively while its worker still owns the task, then the worker stops
    at the next safe point and hands the task back with its attempt still open."""
    steps = _steps(n)
    oid = _submit(ctx, steps)
    hold = Hold({f"m{i}" for i in range(n)})
    r = Runner(ctx, oid, lambda c: CareerManager(c, adapter=hold, playbooks=_lib(steps)))
    assert hold.wait_all_entered()
    assert r.cancel() is True
    hold.release.set()
    s = r.join()
    assert s.stop_reason == "cancelled: test"
    for i in range(n):
        _assert_released(ctx, oid, f"m{i}")
    assert len(_shutdown_decisions(ctx)) == n
    assert all(d["grace_expired"] is False for d in _shutdown_decisions(ctx))
    assert [r_["status"] for r_ in calls(ctx, purpose="task")] == ["ok"] * n
    _no_held_reservations(ctx)
    _ledger_ok(ctx, oid)
    sess = _session(ctx)
    assert sess["ended_at"] is not None and sess["end_reason"] == "cancelled: test"
    # Resuming continues the same attempts; completed calls are replayed, not repeated.
    s2 = CareerManager(ctx, playbooks=_lib(steps)).run(oid)
    assert s2.status is ObjectiveStatus.DONE
    assert len(calls(ctx, purpose="task")) == n
    for i in range(n):
        assert task_by_step(ctx, oid, f"m{i}")["attempt"] == 1
    _ledger_ok(ctx, oid)


def test_repeated_cancel_requests_are_idempotent(ctx: AppContext) -> None:
    steps = _steps(2)
    oid = _submit(ctx, steps)
    hold = Hold({"m0", "m1"})
    r = Runner(ctx, oid, lambda c: CareerManager(c, adapter=hold, playbooks=_lib(steps)))
    assert hold.wait_all_entered()
    assert [r.cancel(f"#{k}") for k in range(5)] == [True, False, False, False, False]
    hold.release.set()
    s = r.join()
    assert s.stop_reason == "cancelled: #0"
    assert len(_shutdown_decisions(ctx)) == 2  # one hand-back per task
    sessions = ctx.db.query("SELECT * FROM execution_sessions WHERE end_reason LIKE 'cancelled%'")
    assert len(sessions) == 1


def test_heartbeat_keeps_leases_alive_during_the_grace_period(ctx: AppContext) -> None:
    """While a worker legitimately owns its task inside a provider call, its lease keeps
    being renewed after the shutdown request, so another session cannot take it over."""
    ctx.config = with_limits(
        ctx.config, lease_seconds=2, heartbeat_seconds=0.05, shutdown_grace_seconds=30
    )
    steps = _steps(1)
    oid = _submit(ctx, steps)
    hold = Hold({"m0"})
    r = Runner(ctx, oid, lambda c: CareerManager(c, adapter=hold, playbooks=_lib(steps)))
    assert hold.entered["m0"].wait(15)
    r.cancel()
    other = CareerManager(ctx, playbooks=_lib(steps))  # honest clock
    sid = other.repo.start_session()
    deadline = time.monotonic() + 3.0  # longer than the 2 s lease
    while time.monotonic() < deadline:
        other._recover(oid, sid)
        time.sleep(0.1)
    assert task_by_step(ctx, oid, "m0")["owner_generation"] == 1  # never taken over
    hold.release.set()
    s = r.join()
    assert s.stop_reason == "cancelled: test"
    assert [c["status"] for c in calls(ctx, purpose="task")] == ["ok"]
    _assert_released(ctx, oid, "m0")


def _poll(predicate: Callable[[], bool], timeout: float = 15.0) -> bool:
    end = time.monotonic() + timeout
    while time.monotonic() < end:
        if predicate():
            return True
        time.sleep(0.02)
    return predicate()


def test_grace_expiry_releases_tasks_and_late_replies_never_overwrite(ctx: AppContext) -> None:
    """Grace expiry: the worker is still inside its provider call. Its task is released
    (conditional on its ownership), the call abandoned and charged its reservation, the
    session ended - without waiting for the worker. A new session completes the task;
    when the old provider finally replies, the reply is recorded as late and changes
    neither the new owner's work nor the ledger's consistency."""
    ctx.config = with_limits(ctx.config, shutdown_grace_seconds=0.2)
    steps = _steps(1)
    oid = _submit(ctx, steps)
    hold = Hold({"m0"})
    r = Runner(ctx, oid, lambda c: CareerManager(c, adapter=hold, playbooks=_lib(steps)))
    assert hold.entered["m0"].wait(15)
    r.cancel()
    s = r.join()  # returns although the worker is still blocked
    assert s.stop_reason is not None and "grace period expired with 1 worker" in s.stop_reason
    assert not hold.returned.is_set()
    _assert_released(ctx, oid, "m0")
    [old] = calls(ctx, purpose="task")
    assert old["status"] == "abandoned" and old["cost_usd"] == pytest.approx(old["reserved_usd"])
    _no_held_reservations(ctx)
    _ledger_ok(ctx, oid)
    assert _session(ctx, str(old["session_id"]))["ended_at"] is not None
    [d] = _shutdown_decisions(ctx)
    assert d["grace_expired"] is True and d["abandoned_calls"] == 1
    # Another session takes the work over and completes it (same attempt, generation 2).
    s2 = CareerManager(ctx, playbooks=_lib(steps)).run(oid)
    assert s2.status is ObjectiveStatus.DONE
    t = task_by_step(ctx, oid, "m0")
    assert (t["status"], t["attempt"], t["owner_generation"]) == ("succeeded", 1, 2)
    accepted_before = t["accepted_result_id"]
    # Now the old provider replies.
    hold.release.set()
    assert _poll(lambda: bool(ctx.db.query("SELECT 1 FROM late_replies")))
    [late] = ctx.db.query("SELECT * FROM late_replies")
    assert late["call_id"] == old["id"] and late["adjustment_applied"] == 1
    t = task_by_step(ctx, oid, "m0")
    assert (t["status"], t["owner_generation"], t["accepted_result_id"]) == (
        "succeeded",
        2,
        accepted_before,
    )
    assert [c["status"] for c in calls(ctx, purpose="task")] == ["abandoned", "ok"]
    accepts = ctx.db.query("SELECT * FROM manager_decisions WHERE decision='accept'")
    assert len(accepts) == 1
    _ledger_ok(ctx, oid)


def test_cancel_after_takeover_does_not_touch_the_new_owner(ctx: AppContext) -> None:
    """A worker whose task was taken over by recovery holds no valid ownership: on
    shutdown it releases nothing, and its late reply is fenced."""
    steps = _steps(1)
    oid = _submit(ctx, steps)
    hold = Hold({"m0"})
    r = Runner(ctx, oid, lambda c: CareerManager(c, adapter=hold, playbooks=_lib(steps)))
    assert hold.entered["m0"].wait(15)
    s2 = CareerManager(ctx, playbooks=_lib(steps)).run(oid, force_recover=True)
    assert s2.status is ObjectiveStatus.DONE
    r.cancel()
    hold.release.set()
    r.join()
    assert _shutdown_decisions(ctx) == []  # nothing of ours left to hand back
    t = task_by_step(ctx, oid, "m0")
    assert (t["status"], t["owner_generation"]) == ("succeeded", 2)
    assert [c["status"] for c in calls(ctx, purpose="task")] == ["abandoned", "ok"]
    assert len(ctx.db.query("SELECT * FROM late_replies")) == 1
    _ledger_ok(ctx, oid)


def test_failed_hand_back_leaves_the_session_open_for_recovery(
    ctx: AppContext, monkeypatch: pytest.MonkeyPatch
) -> None:
    """If a stopped worker's task cannot be handed back (database unavailable), the
    session is not ended: its lease expires and recovery resumes the same attempt."""
    steps = _steps(1)
    oid = _submit(ctx, steps)
    hold = Hold({"m0"})

    def unavailable(self: CareerManager, fence: Any, *, grace_expired: bool) -> bool:
        raise sqlite3.OperationalError("disk I/O error (simulated)")

    monkeypatch.setattr(CareerManager, "_release_for_shutdown", unavailable)
    monkeypatch.setattr("careeros.manager.manager.time.sleep", lambda _s: None)
    r = Runner(ctx, oid, lambda c: CareerManager(c, adapter=hold, playbooks=_lib(steps)))
    assert hold.entered["m0"].wait(15)
    r.cancel()
    hold.release.set()
    s = r.join()
    assert s.stop_reason is not None and "left for recovery" in s.stop_reason
    t = task_by_step(ctx, oid, "m0")
    assert t["status"] == "running" and t["lease_owner"] is not None
    assert _session(ctx, str(t["lease_owner"]))["ended_at"] is None  # not ended
    monkeypatch.undo()
    s2 = CareerManager(ctx, playbooks=_lib(steps), clock=later(10)).run(oid)
    assert s2.status is ObjectiveStatus.DONE
    t = task_by_step(ctx, oid, "m0")
    assert (t["attempt"], t["owner_generation"]) == (1, 2)
    assert [c["status"] for c in calls(ctx, purpose="task")] == ["ok"]  # replayed


def test_signal_handler_cancels_once_then_forces_exit() -> None:
    """``graceful_shutdown`` unit test with real signals to this (main) thread."""

    class Target:
        def __init__(self) -> None:
            self.reasons: list[str] = []

        def request_cancel(self, reason: str = "x") -> bool:
            self.reasons.append(reason)
            return len(self.reasons) == 1

    exits: list[int] = []
    target = Target()
    before = signal.getsignal(signal.SIGINT)
    with graceful_shutdown(target, notify=lambda _m: None, force_exit=exits.append):
        os.kill(os.getpid(), signal.SIGINT)
        os.kill(os.getpid(), signal.SIGTERM)
        os.kill(os.getpid(), signal.SIGINT)
    assert target.reasons == ["SIGINT"]
    assert exits == [forced_exit_code(signal.SIGTERM), forced_exit_code(signal.SIGINT)]
    assert exits == [143, 130]
    signal.signal(signal.SIGINT, before)
    signal.signal(signal.SIGTERM, signal.SIG_DFL)
    with graceful_shutdown(target, notify=lambda _m: None, force_exit=exits.append):
        pass
    assert signal.getsignal(signal.SIGINT) is before  # restored when nothing arrived


# ================================================================== separate processes
def _proc(target: Any, *args: Any) -> Any:
    p = SPAWN.Process(target=target, args=args)
    p.start()
    return p


def _book_dict(n: int) -> dict[str, Any]:
    return {"name": "c3", "plan": {"steps": _steps(n)}}


def test_sigint_during_idle_dispatch_in_a_separate_process(ctx: AppContext) -> None:
    steps = _steps(1)
    oid = _submit(ctx, steps)
    bm = BudgetManager(ctx.db, ctx.config)
    bm.lines_for(oid, None, None)
    line = ctx.db.query_one(
        "SELECT id, limit_usd FROM budgets WHERE scope='objective' AND scope_ref=?", (str(oid),)
    )
    assert line is not None
    bm.reserve([int(line["id"])], float(line["limit_usd"]) - 0.001, session_id=None)
    entered, release, out, requeued = SPAWN.Queue(), SPAWN.Event(), SPAWN.Queue(), SPAWN.Event()
    p = _proc(
        mp_workers.run_with_signals,
        str(ctx.data_dir),
        oid,
        _book_dict(1),
        [],
        entered,
        release,
        out,
        None,
        requeued,
    )
    assert requeued.wait(30)
    os.kill(p.pid, signal.SIGINT)
    res = out.get(timeout=30)
    p.join(30)
    assert p.exitcode == 0 and "error" not in res, res
    assert res["stop_reason"] == "cancelled: SIGINT"
    _assert_released(ctx, oid, "m0")


@pytest.mark.parametrize(
    ("sig", "n"), [(signal.SIGINT, 1), (signal.SIGTERM, 2)], ids=["sigint_1", "sigterm_2"]
)
def test_signal_with_active_workers_in_a_separate_process(
    ctx: AppContext, sig: signal.Signals, n: int
) -> None:
    steps = _steps(n)
    oid = _submit(ctx, steps)
    entered, release, out = SPAWN.Queue(), SPAWN.Event(), SPAWN.Queue()
    p = _proc(
        mp_workers.run_with_signals,
        str(ctx.data_dir),
        oid,
        _book_dict(n),
        [f"m{i}" for i in range(n)],
        entered,
        release,
        out,
    )
    assert sorted(entered.get(timeout=30) for _ in range(n)) == [f"m{i}" for i in range(n)]
    os.kill(p.pid, sig)
    release.set()
    res = out.get(timeout=30)
    p.join(30)
    assert p.exitcode == 0 and "error" not in res, res
    assert res["stop_reason"] == f"cancelled: {sig.name}"
    for i in range(n):
        _assert_released(ctx, oid, f"m{i}")
    assert [c["status"] for c in calls(ctx, purpose="task")] == ["ok"] * n
    _no_held_reservations(ctx)
    _ledger_ok(ctx, oid)
    assert CareerManager(ctx, playbooks=_lib(steps)).run(oid).status is ObjectiveStatus.DONE
    assert len(calls(ctx, purpose="task")) == n  # replayed, not repeated


def test_second_signal_forces_exit_and_recovery_is_deterministic(ctx: AppContext) -> None:
    steps = _steps(2)
    oid = _submit(ctx, steps)
    entered, release, out, signalled = SPAWN.Queue(), SPAWN.Event(), SPAWN.Queue(), SPAWN.Event()
    p = _proc(
        mp_workers.run_with_signals,
        str(ctx.data_dir),
        oid,
        _book_dict(2),
        ["m0", "m1"],
        entered,
        release,
        out,
        None,
        None,
        30.0,
        signalled,
    )
    assert sorted(entered.get(timeout=30) for _ in range(2)) == ["m0", "m1"]
    os.kill(p.pid, signal.SIGINT)
    # Wait until the child has handled the first signal: two pending SIGINTs delivered
    # before the handler runs coalesce into one (standard POSIX signals do not queue).
    assert signalled.wait(30)
    assert p.is_alive()  # graceful shutdown in progress: workers still inside their calls
    # A second signal forces the exit at once.
    os.kill(p.pid, signal.SIGINT)
    p.join(30)
    assert p.exitcode == forced_exit_code(signal.SIGINT) == 130
    assert out.empty()  # forced: no summary, no clean-up
    # Exactly as after a crash: tasks still owned by the dead session, which is not ended.
    for i in range(2):
        t = task_by_step(ctx, oid, f"m{i}")
        assert (t["status"], t["attempt"], t["owner_generation"]) == ("running", 1, 1)
    assert _session(ctx, str(task_by_step(ctx, oid, "m0")["lease_owner"]))["ended_at"] is None
    # Recovery once the leases have expired: same attempts, in-flight calls abandoned.
    s = CareerManager(ctx, playbooks=_lib(steps), clock=later(10)).run(oid)
    assert s.status is ObjectiveStatus.DONE
    for i in range(2):
        t = task_by_step(ctx, oid, f"m{i}")
        assert (t["status"], t["attempt"], t["owner_generation"]) == ("succeeded", 1, 2)
    assert sorted(c["status"] for c in calls(ctx, purpose="task")) == [
        "abandoned",
        "abandoned",
        "ok",
        "ok",
    ]
    _no_held_reservations(ctx)
    _ledger_ok(ctx, oid)


def test_late_reply_after_grace_expiry_and_recovery_across_processes(ctx: AppContext) -> None:
    """Process A is shut down with a worker stuck in a provider call; its grace period
    expires, its task is released and its session ended while the call is still open.
    This process then recovers and completes the task. When A's provider finally replies,
    A records a late reply that cannot overwrite the new work; A then exits."""
    steps = _steps(1)
    oid = _submit(ctx, steps)
    entered, release, out = SPAWN.Queue(), SPAWN.Event(), SPAWN.Queue()
    p = _proc(
        mp_workers.run_with_signals,
        str(ctx.data_dir),
        oid,
        _book_dict(1),
        ["m0"],
        entered,
        release,
        out,
        {"shutdown_grace_seconds": 0.3},
    )
    assert entered.get(timeout=30) == "m0"
    os.kill(p.pid, signal.SIGINT)
    res = out.get(timeout=30)  # A's run returned; A itself is still alive
    assert "error" not in res, res
    assert "grace period expired with 1 worker" in res["stop_reason"]
    assert p.is_alive()
    _assert_released(ctx, oid, "m0")
    s = CareerManager(ctx, playbooks=_lib(steps)).run(oid)
    assert s.status is ObjectiveStatus.DONE
    release.set()
    p.join(30)
    assert p.exitcode == 0
    [late] = ctx.db.query("SELECT * FROM late_replies")
    assert late["adjustment_applied"] == 1
    t = task_by_step(ctx, oid, "m0")
    assert (t["status"], t["owner_generation"]) == ("succeeded", 2)
    assert [c["status"] for c in calls(ctx, purpose="task")] == ["abandoned", "ok"]
    assert len(ctx.db.query("SELECT * FROM manager_decisions WHERE decision='accept'")) == 1
    _no_held_reservations(ctx)
    _ledger_ok(ctx, oid)
