"""Phase B: fencing tokens, late replies, heartbeat safety and atomic transitions.

Deterministic in-process tests (threads with separate connections). Real separate-process
tests are in ``test_phase_b_multiprocess.py``.
"""

from __future__ import annotations

import sqlite3
import threading
import time
from collections.abc import Callable
from contextlib import contextmanager
from datetime import timedelta
from typing import Any

import pytest

from careeros.context import AppContext, open_context_at
from careeros.core.clock import utc_now
from careeros.core.config import ModelProfile
from careeros.core.db import from_json
from careeros.core.errors import OwnershipLostError, SessionTerminatedError
from careeros.cost.budget import BudgetManager
from careeros.domain.models import TaskStatus
from careeros.domain.repositories import OrchestrationRepository
from careeros.llm.adapters.scripted import ScriptedAdapter
from careeros.llm.contracts import (
    IdempotencyMismatchError,
    ModelRequest,
    ModelResponse,
    OutputSchema,
)
from careeros.llm.gateway import CallContext, Reply
from careeros.manager.manager import CareerManager, LeaseState
from careeros.manager.plans import Playbook, PlaybookLibrary
from careeros.simulation import later
from tests.m2_helpers import calls, decisions, task_by_step
from tests.m3_helpers import GatedAdapter
from tests.phase_b_helpers import (
    BlockingAdapter,
    done,
    ledger_consistent,
    one_step_book,
    submit,
    with_limits,
)


def _task(ctx: AppContext, oid: int) -> dict[str, Any]:
    return task_by_step(ctx, oid, "m")


def _run_in_thread(
    ctx: AppContext, factory: Callable[[AppContext], CareerManager], oid: int
) -> tuple[threading.Thread, list[BaseException]]:
    errors: list[BaseException] = []

    def target() -> None:
        try:
            with open_context_at(ctx.config, ctx.data_dir) as c:
                factory(c).run(oid)
        except BaseException as exc:  # surfaced by the test
            errors.append(exc)

    t = threading.Thread(target=target)
    t.start()
    return t, errors


# ====================================================================== stale tokens
def test_stale_fencing_token_writes_are_all_rejected(ctx: AppContext) -> None:
    lib = one_step_book()
    m = CareerManager(ctx, playbooks=lib)
    oid = submit(m)
    obj = ctx.orchestration.get_objective(oid)
    task = ctx.orchestration.list_tasks(oid)[0]
    s1 = ctx.orchestration.start_session()
    f1 = m._claim(task, s1)
    assert f1 is not None and f1.generation == 1 and f1.attempt == 1
    # Owner 1 "pauses"; its lease expires; a new owner takes over and claims (gen 2).
    m2 = CareerManager(ctx, playbooks=lib, clock=later(10))
    s2 = ctx.orchestration.start_session()
    m2._recover(oid, s2)
    f2 = m2._claim(ctx.orchestration.get_task(task.id or 0), s2)
    assert f2 is not None and f2.generation == 2 and f2.attempt == 1  # same, still-open attempt
    before = dict(_task(ctx, oid))
    n_decisions = ctx.db.query_one("SELECT COUNT(*) AS n FROM manager_decisions")["n"]
    stale_ops: list[Callable[[], Any]] = [
        lambda: m._requeue(f1, task, "stale"),
        lambda: m._block(f1, obj, task, "budget_increase", {"reason": "stale"}),
        lambda: m._accept(f1, obj, task, 1, "stale"),
        lambda: m.gateway.call(
            "mock-small",
            ModelRequest(
                task_type="job.classify_role",
                prompt="x",
                output_schema=OutputSchema(("role_family",)),
            ),
            CallContext(oid, task.id, 1, "task", s1, "stale:key", f1),
        ),
    ]
    for op in stale_ops:
        with pytest.raises(OwnershipLostError):
            op()
    with pytest.raises(OwnershipLostError), m._owned(f1):
        pass
    after = dict(_task(ctx, oid))
    after.pop("updated_at"), before.pop("updated_at")
    assert after == before  # the current owner's task row is untouched
    assert after["owner_generation"] == 2 and after["lease_owner"] == s2
    # Only the recover decision was added; nothing written by the stale owner.
    assert ctx.db.query_one("SELECT COUNT(*) AS n FROM manager_decisions")["n"] == n_decisions
    assert calls(ctx) == [] and m.pending_requests(oid) == []


# ====================================================================== late replies
@pytest.mark.parametrize("fail_late", [False, True], ids=["late_success", "late_failure"])
def test_late_reply_after_takeover_never_overwrites_the_new_owner(
    ctx: AppContext, fail_late: bool
) -> None:
    """Audit H2 reproduction: the old owner's delayed reply used to overwrite the abandoned
    call, double-charge task spend and record an 'accept' after the objective stopped."""
    lib = one_step_book()
    slow = BlockingAdapter({"m"}, fail_late=fail_late)
    oid = submit(CareerManager(ctx, playbooks=lib))
    t, errors = _run_in_thread(ctx, lambda c: CareerManager(c, adapter=slow, playbooks=lib), oid)
    assert slow.entered.wait(5)
    # A second session believes the first is dead (its lease is past expiry by its clock).
    new_owner = CareerManager(ctx, playbooks=lib, clock=later(10))
    assert new_owner.run(oid).status.value == "done"
    slow.release.set()
    t.join(10)
    assert errors == []
    rows = calls(ctx, purpose="task")
    assert [r["status"] for r in rows] == ["abandoned", "ok"]
    first, second = rows
    assert (first["owner_generation"], second["owner_generation"]) == (1, 2)
    [late] = ctx.db.query("SELECT * FROM late_replies")
    assert late["call_id"] == first["id"] and late["adjustment_applied"] == 1
    assert late["status"] == ("timeout" if fail_late else "ok")
    assert late["charged_before_usd"] == pytest.approx(first["reserved_usd"])
    assert late["adjustment_usd"] == pytest.approx(late["actual_cost_usd"] - first["reserved_usd"])
    assert first["cost_usd"] == pytest.approx(late["actual_cost_usd"])  # reconciled once
    assert late["provider_request_id"] is None or late["provider_request_id"].startswith("mock-")
    # Authoritative state belongs to generation 2 only.
    task = _task(ctx, oid)
    assert task["status"] == "succeeded" and task["owner_generation"] == 2
    [result] = ctx.orchestration.list_results(task["id"])
    assert result.result.metrics["call_id"] == second["id"]
    accepts = ctx.db.query("SELECT owner_generation FROM manager_decisions WHERE decision='accept'")
    assert [r["owner_generation"] for r in accepts] == [2]
    # Money: task spend and every budget level match the ledger; nothing charged twice.
    spent, total = ledger_consistent(ctx, oid)
    assert spent == pytest.approx(total)
    # Task spend covers all of the task's calls (its task calls and, since C7, the factual
    # review of its research result).
    assert task["spent_usd"] == pytest.approx(
        sum(float(r["cost_usd"]) for r in calls(ctx, task_id=task["id"]))
    )
    assert ctx.db.query("SELECT * FROM budget_reservations WHERE status='held'") == []


def test_stale_completion_without_recovery_goes_to_the_late_path(ctx: AppContext) -> None:
    """The lease expired (e.g. heartbeat stalled) but nobody recovered yet: T2 must not
    complete the call as authoritative. It settles at actual cost and is marked late."""
    lib = one_step_book()
    slow = BlockingAdapter({"m"})
    oid = submit(CareerManager(ctx, playbooks=lib))
    t, errors = _run_in_thread(ctx, lambda c: CareerManager(c, adapter=slow, playbooks=lib), oid)
    assert slow.entered.wait(5)
    with ctx.db.transaction() as c:  # the lease runs out while the call is in flight
        c.execute("UPDATE tasks SET lease_expires_at='2000-01-01T00:00:00+00:00'")
    slow.release.set()
    t.join(10)
    assert errors == []
    [row] = calls(ctx, purpose="task")
    assert row["status"] == "late"
    [late] = ctx.db.query("SELECT * FROM late_replies")
    assert late["charged_before_usd"] == 0 and late["adjustment_usd"] == pytest.approx(
        row["cost_usd"]
    )
    assert ctx.orchestration.list_results(_task(ctx, oid)["id"]) == []  # never used
    assert ctx.db.query("SELECT * FROM budget_reservations WHERE status='held'") == []
    # The next session takes the task over and completes it with a fresh call.
    assert CareerManager(ctx, playbooks=lib).run(oid).status.value == "done"
    assert [r["status"] for r in calls(ctx, purpose="task")] == ["late", "ok"]
    spent, total = ledger_consistent(ctx, oid)
    assert spent == pytest.approx(total)


def _abandoned_call(ctx: AppContext) -> tuple[CareerManager, int, dict[str, Any]]:
    """A call left in flight by a crash, then abandoned by recovery (charged R)."""
    lib = one_step_book()
    m = CareerManager(ctx, adapter=GatedAdapter(n=1, crash_in_call={"m"}), playbooks=lib)
    oid = submit(m)
    with pytest.raises(SessionTerminatedError):
        m.run(oid)
    CareerManager(ctx, playbooks=lib, clock=later(10))._recover(oid, "recoverer")
    [row] = [r for r in calls(ctx) if r["status"] == "abandoned"]
    return m, oid, row


@pytest.mark.parametrize("ratio", [0.3, 1.5], ids=["under_reservation", "over_reservation"])
def test_late_cost_reconciliation_and_duplicate_delivery(ctx: AppContext, ratio: float) -> None:
    m, oid, row = _abandoned_call(ctx)
    reserved = float(row["reserved_usd"])
    assert row["cost_usd"] == pytest.approx(reserved)  # conservatively charged at abandonment
    spent0, _ = ledger_consistent(ctx, oid)
    reply = Reply("ok", None, 100, 200, 10, reserved * ratio, "{}", "prov-123")
    assert m.gateway.record_late_reply(row["id"], reply) is True
    spent1, total1 = ledger_consistent(ctx, oid)
    assert spent1 == pytest.approx(spent0 + reserved * (ratio - 1))  # refund or extra charge
    assert spent1 == pytest.approx(total1)
    [late] = ctx.db.query("SELECT * FROM late_replies")
    assert late["overshoot_usd"] == pytest.approx(max(0.0, reserved * (ratio - 1)))
    assert late["provider_request_id"] == "prov-123"
    call = ctx.db.query_one("SELECT * FROM model_calls WHERE id=?", (row["id"],))
    assert call["overshoot_usd"] == pytest.approx(max(0.0, reserved * (ratio - 1)))
    task_spent = _task(ctx, oid)["spent_usd"]
    # Duplicate delivery: no second charge or refund, anywhere.
    assert m.gateway.record_late_reply(row["id"], reply) is False
    assert ledger_consistent(ctx, oid) == (pytest.approx(spent1), pytest.approx(total1))
    assert _task(ctx, oid)["spent_usd"] == pytest.approx(task_spent)
    assert len(ctx.db.query("SELECT * FROM late_replies")) == 1


# --------------------------------------------- replies for calls that were already settled
def _accounting_snapshot(ctx: AppContext, oid: int) -> dict[str, Any]:
    """Every value a stray reply must not change: spend at every level, reservations,
    call rows (status, cost, idempotency key) and the authoritative results."""
    return {
        "budgets": [
            tuple(r) for r in ctx.db.query("SELECT id, spent_usd, reserved_usd FROM budgets")
        ],
        "reservations": [
            tuple(r)
            for r in ctx.db.query("SELECT id, status, settled_usd FROM budget_reservations")
        ],
        "calls": [
            tuple(r)
            for r in ctx.db.query(
                "SELECT id, status, cost_usd, input_tokens, output_tokens, overshoot_usd,"
                " idempotency_key, response_json, provider_request_id FROM model_calls"
            )
        ],
        "tasks": [
            tuple(r)
            for r in ctx.db.query("SELECT id, status, spent_usd, accepted_result_id FROM tasks")
        ],
        "results": [tuple(r) for r in ctx.db.query("SELECT * FROM task_results")],
        "late_replies": [tuple(r) for r in ctx.db.query("SELECT * FROM late_replies")],
        "ledger": ledger_consistent(ctx, oid),
    }


SETTLED_STATUSES = ["ok", "malformed", "timeout", "rate_limit", "error", "cache_hit", "late"]


@pytest.mark.parametrize("status", SETTLED_STATUSES)
def test_late_reply_for_an_already_settled_call_changes_nothing(
    ctx: AppContext, status: str
) -> None:
    """A reply delivered (again) for a call that already completed - its reservation
    settled, its result possibly accepted - is a duplicate: no accounting at all."""
    lib = one_step_book()
    m = CareerManager(ctx, playbooks=lib)
    oid = submit(m)
    assert m.run(oid).status.value == "done"
    [row] = calls(ctx, purpose="task")
    assert row["status"] == "ok"
    call_id = int(str(row["id"]))
    res = ctx.db.query_one(
        "SELECT status FROM budget_reservations WHERE id=?", (row["reservation_id"],)
    )
    assert res is not None and res["status"] == "settled"
    if status != "ok":  # the same settled call, finished with another terminal status
        with ctx.db.transaction() as c:
            c.execute("UPDATE model_calls SET status=? WHERE id=?", (status, row["id"]))
    before = _accounting_snapshot(ctx, oid)
    assert before["ledger"][0] == pytest.approx(before["ledger"][1])
    for cost in (0.0001, float(str(row["cost_usd"])), 5.0):  # cheaper, equal, far dearer
        reply = Reply("ok", None, 100, 200, 10, cost, '{"payload": {"x": 1}}', "dup-prov")
        assert m.gateway.record_late_reply(call_id, reply) is False
        assert _accounting_snapshot(ctx, oid) == before
    # The authoritative call still replays by its key, unchanged.
    if status == "ok":
        t = _task(ctx, oid)
        assert t["accepted_result_id"] is not None and t["status"] == "succeeded"
        same = ctx.db.query_one(
            "SELECT id FROM model_calls WHERE idempotency_key=?", (row["idempotency_key"],)
        )
        assert same is not None and same["id"] == call_id


class _DuplicateCompletionAdapter(GatedAdapter):
    """While the provider call is in progress, another delivery path completes the same
    call row first (as T2 would: status, cost, reservation settled, task spend), through
    its own connection. The original T2 then arrives second for a settled call."""

    def __init__(self, ctx: AppContext, first_cost: float) -> None:
        super().__init__(n=1)
        self.ctx, self.first_cost = ctx, first_cost

    def call(self, profile: ModelProfile, request: ModelRequest) -> ModelResponse:
        response = super().call(profile, request)
        with (
            open_context_at(self.ctx.config, self.ctx.data_dir) as other,
            other.db.transaction() as c,
        ):
            r = c.execute("SELECT * FROM model_calls WHERE status='in_flight'").fetchone()
            c.execute(
                "UPDATE model_calls SET status='ok', cost_usd=?, response_json=? WHERE id=?",
                (self.first_cost, '{"payload": {"role_family": "first"}}', r["id"]),
            )
            BudgetManager.settle_in(c, r["reservation_id"], self.first_cost)
            if r["task_id"] is not None:
                c.execute(
                    "UPDATE tasks SET spent_usd = spent_usd + ? WHERE id=?",
                    (self.first_cost, r["task_id"]),
                )
        return response


def test_duplicate_t2_completion_of_a_settled_call_changes_nothing(ctx: AppContext) -> None:
    first_cost = 0.00123
    adapter = _DuplicateCompletionAdapter(ctx, first_cost)
    m = CareerManager(ctx, adapter=adapter, playbooks=one_step_book())
    oid = submit(m)
    tid = _task(ctx, oid)["id"]
    req = ModelRequest(
        task_type="job.classify_role",
        prompt="classify",
        output_schema=OutputSchema(("role_family",)),
    )
    key = f"task:{tid}:a1:task:0"
    out = m.gateway.call("mock-small", req, CallContext(oid, tid, 1, "task", None, key))
    # The second completion is reported as non-authoritative and applied nowhere.
    assert out.status == "late" and out.payload is None, out.error
    [row] = calls(ctx, purpose="task")
    assert row["status"] == "ok" and row["cost_usd"] == pytest.approx(first_cost)
    assert row["idempotency_key"] == key
    assert from_json(str(row["response_json"]))["payload"] == {"role_family": "first"}
    assert ctx.db.query("SELECT * FROM late_replies") == []
    res = ctx.db.query_one("SELECT * FROM budget_reservations WHERE id=?", (row["reservation_id"],))
    assert res is not None and res["status"] == "settled"
    assert res["settled_usd"] == pytest.approx(first_cost)
    assert _task(ctx, oid)["spent_usd"] == pytest.approx(first_cost)
    spent, total = ledger_consistent(ctx, oid)
    assert spent == pytest.approx(total) == pytest.approx(first_cost)
    # A replay by key returns the first, authoritative completion; nothing is re-charged.
    before = _accounting_snapshot(ctx, oid)
    again = m.gateway.call("mock-small", req, CallContext(oid, tid, 1, "task", None, key))
    assert again.call_id == row["id"] and again.payload == {"role_family": "first"}
    assert m.gateway.record_late_reply(row["id"], Reply("ok", None, 1, 1, 1, 9.9, "{}")) is False
    assert _accounting_snapshot(ctx, oid) == before


def test_late_reply_for_in_flight_call_whose_reservation_is_gone_is_refused(
    ctx: AppContext,
) -> None:
    """Defensive: an in-flight row whose reservation is no longer held cannot be settled
    at the budget level, so the late path must not charge the task level alone."""
    m, oid, row = _abandoned_call(ctx)
    with ctx.db.transaction() as c:  # an inconsistent row: in flight, reservation settled
        c.execute("UPDATE model_calls SET status='in_flight' WHERE id=?", (row["id"],))
    before = _accounting_snapshot(ctx, oid)
    reply = Reply("ok", None, 100, 200, 10, 0.5, "{}", "prov")
    assert m.gateway.record_late_reply(row["id"], reply) is False
    assert _accounting_snapshot(ctx, oid) == before


# ====================================================================== idempotency (L1)
def test_replay_validates_model_effort_and_request(ctx: AppContext) -> None:
    adapter = GatedAdapter(n=1)
    m = CareerManager(ctx, adapter=adapter)
    oid = submit(m, "job_triage")
    req = ModelRequest(
        task_type="job.classify_role",
        prompt="classify",
        output_schema=OutputSchema(("role_family",)),
    )
    key = "objective:1:a1:task:0"
    first = m.gateway.call("mock-small", req, CallContext(oid, None, 1, "task", None, key))
    again = m.gateway.call("mock-small", req, CallContext(oid, None, 1, "task", None, key))
    assert again.call_id == first.call_id and len(adapter.calls) == 1
    for model, r in [
        ("mock-medium", req),
        ("mock-small", ModelRequest(**{**req.__dict__, "effort": "low"})),
        ("mock-small", ModelRequest(**{**req.__dict__, "prompt": "different"})),
    ]:
        with pytest.raises(IdempotencyMismatchError):
            m.gateway.call(model, r, CallContext(oid, None, 1, "task", None, key))
    assert len(adapter.calls) == 1


def test_resumed_attempt_reuses_its_planned_route(
    ctx: AppContext, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Crash after the call completed but before its checkpoint: routing history then
    changes. The resumed attempt must replay the *same* logical call, not re-route."""
    lib = one_step_book()
    real = OrchestrationRepository.save_checkpoint_in
    crashed: list[bool] = []

    def crash_on_response(c: Any, cp: Any, generation: int | None = None) -> None:
        if cp.step == "model_response" and not crashed:
            crashed.append(True)
            raise SessionTerminatedError("killed before the checkpoint")
        real(c, cp, generation)

    monkeypatch.setattr(
        OrchestrationRepository, "save_checkpoint_in", staticmethod(crash_on_response)
    )
    m = CareerManager(ctx, playbooks=lib)
    oid = submit(m)
    with pytest.raises(SessionTerminatedError):
        m.run(oid)
    original = _task(ctx, oid)["current_model_id"]
    # History now says the original model is terrible: a fresh route would differ.
    for _ in range(30):
        m.stats.record(
            "research.company",
            "mid",
            original,
            "low",
            accepted=False,
            prior_alpha=1,
            prior_beta=1,
            quality=0.1,
            cost_usd=0.01,
            latency_ms=1,
        )
    resumed = CareerManager(ctx, playbooks=lib, clock=later(10))
    assert resumed.run(oid).status.value == "done"
    assert [r["status"] for r in calls(ctx, purpose="task")] == ["ok"]  # replayed, no new call
    assert _task(ctx, oid)["current_model_id"] == original
    assert any("resuming planned route" in e for e in resumed.events)


# ====================================================================== heartbeat (M4)
def test_heartbeat_halts_exactly_at_the_safety_boundary(ctx: AppContext) -> None:
    limits = ctx.config.limits
    assert (
        limits.lease_seconds,
        limits.heartbeat_interval(),
        limits.safety_margin(),
        limits.halt_after(),
    ) == (120, 20, 40, 80)
    m = CareerManager(ctx)
    t0 = utc_now()
    state = LeaseState(last_success=t0)
    m.clock = lambda: t0 + timedelta(seconds=79.9)
    assert m._lease_unsafe(state) is False
    m.clock = lambda: t0 + timedelta(seconds=80)
    assert m._lease_unsafe(state) is True


class _FailingDB:
    def __init__(self, failures: int, real: Any) -> None:
        self.failures, self.real, self.calls = failures, real, 0

    @contextmanager
    def transaction(self) -> Any:
        self.calls += 1
        if self.calls <= self.failures:
            raise sqlite3.OperationalError("database is locked")
        with self.real.transaction() as c:
            yield c


def test_renewal_retries_are_bounded_and_transient_errors_recover(ctx: AppContext) -> None:
    m = CareerManager(ctx)
    retries = ctx.config.limits.heartbeat_retries
    state = LeaseState(last_success=utc_now())
    assert m._renew_leases(_FailingDB(retries + 1, ctx.db), "s", state) is False  # type: ignore[arg-type]
    assert state.failures == retries + 1 and "locked" in (state.last_error or "")
    assert m._renew_leases(_FailingDB(retries, ctx.db), "s", state) is True  # type: ignore[arg-type]
    assert state.failures == 0


def test_heartbeat_failure_halts_workers_before_their_lease_expires(
    ctx: AppContext, monkeypatch: pytest.MonkeyPatch
) -> None:
    # lease 4 s, heartbeat 0.05 s, margin 1.5 s -> halt after 2.5 s without renewal.
    ctx.config = with_limits(
        ctx.config, lease_seconds=4, heartbeat_seconds=0.05, lease_safety_margin_seconds=1.5
    )
    monkeypatch.setattr(CareerManager, "_renew_leases", lambda self, db, s, st: False)
    lib = PlaybookLibrary(
        [
            Playbook.model_validate(
                {
                    "name": "hb",
                    "plan": {
                        "steps": [
                            {
                                "id": "m",
                                "worker": "model_step",
                                "task_type": "research.company",
                                "output": {"required": ["summary"]},
                            },
                            {
                                "id": "after",
                                "worker": "extract_keywords",
                                "mode": "deterministic",
                                "task_type": "job.normalize",
                                "depends_on": ["m"],
                            },
                        ]
                    },
                }
            )
        ]
    )
    slow = BlockingAdapter({"m"}, timeout=3.0)  # replies at ~3 s: after halt, before expiry
    m = CareerManager(ctx, adapter=slow, playbooks=lib)
    oid = submit(m, "hb")
    t0 = time.monotonic()
    summary = m.run(oid)
    assert summary.stop_reason == "halted: lease renewal failed"
    assert 2.4 <= time.monotonic() - t0 < 4.0  # halted at the boundary, not at lease expiry
    assert task_by_step(ctx, oid, "after")["status"] == "pending"  # nothing new dispatched
    sess = ctx.db.query_one("SELECT * FROM execution_sessions ORDER BY rowid DESC LIMIT 1")
    assert sess["end_reason"] == "halted: lease renewal failed"
    [row] = calls(ctx, purpose="task")
    assert row["status"] == "ok"  # it completed while the lease was still valid
    monkeypatch.undo()
    resumed = CareerManager(ctx, playbooks=lib)  # owner session ended: immediate takeover
    assert resumed.run(oid).status.value == "done"
    assert len(calls(ctx, purpose="task")) == 1  # replayed by key; no second call
    assert decisions(ctx, _task(ctx, oid)["id"]).count("recover") == 1


def test_transient_heartbeat_errors_do_not_halt(
    ctx: AppContext, monkeypatch: pytest.MonkeyPatch
) -> None:
    ctx.config = with_limits(
        ctx.config, lease_seconds=4, heartbeat_seconds=0.05, lease_safety_margin_seconds=1.5
    )
    real = CareerManager._renew_leases
    count = {"n": 0}

    def flaky(self: CareerManager, db: Any, s: str, st: LeaseState) -> bool:
        count["n"] += 1
        return False if count["n"] <= 5 else real(self, db, s, st)  # 0.25 s of outage

    monkeypatch.setattr(CareerManager, "_renew_leases", flaky)
    lib = one_step_book()
    slow = BlockingAdapter({"m"}, timeout=1.0)
    m = CareerManager(ctx, adapter=slow, playbooks=lib)
    oid = submit(m)
    assert m.run(oid).status.value == "done"
    assert not m._lease_lost.is_set() and count["n"] > 5


def test_live_heartbeating_session_keeps_its_task(ctx: AppContext) -> None:
    ctx.config = with_limits(ctx.config, lease_seconds=2, heartbeat_seconds=0.05)
    lib = one_step_book()
    slow = BlockingAdapter({"m"}, timeout=10)
    oid = submit(CareerManager(ctx, playbooks=lib))
    t, errors = _run_in_thread(ctx, lambda c: CareerManager(c, adapter=slow, playbooks=lib), oid)
    assert slow.entered.wait(5)
    deadline = time.monotonic() + 3.0  # longer than the 2 s lease
    other = CareerManager(ctx, playbooks=lib)
    while time.monotonic() < deadline:
        other._recover(oid, "thief")
        time.sleep(0.2)
    slow.release.set()
    t.join(10)
    assert errors == [] and done(ctx, oid)
    assert decisions(ctx, _task(ctx, oid)["id"]).count("recover") == 0
    assert [r["status"] for r in calls(ctx, purpose="task")] == ["ok"]


# ====================================================================== atomic transitions
def _crash_on(monkeypatch: pytest.MonkeyPatch, decision: str) -> list[bool]:
    real = OrchestrationRepository.record_decision_in
    fired: list[bool] = []

    def crash(c: Any, kind: str, **kw: Any) -> int:
        if kind == decision and not fired:
            fired.append(True)
            raise SessionTerminatedError(f"killed while writing {decision}")
        return real(c, kind, **kw)

    monkeypatch.setattr(OrchestrationRepository, "record_decision_in", staticmethod(crash))
    return fired


CASES: dict[str, dict[str, Any]] = {
    "accept": {"script": {"m:research.company": ["success:0.9"]}},
    "route": {"script": {"m:research.company": ["success:0.9"]}},
    "escalate_effort": {
        "script": {"m:research.company": ["low_confidence", "success:0.9"]},
        # a new adapter in the resumed process: only what the crashed run had not consumed
        "resume_script": {"m:research.company": ["success:0.9"]},
    },
    "await_human": {
        "script": {"m:research.company": ["malformed"] * 4},
        "step": {"max_attempts": 1},
    },
    "budget_await_human": {"script": {}, "step": {"priority": "high", "budget_usd": 0.0001}},
}


@pytest.mark.parametrize("case", list(CASES))
def test_crash_inside_each_atomic_transition_rolls_back_completely(
    ctx: AppContext, monkeypatch: pytest.MonkeyPatch, case: str
) -> None:
    spec = CASES[case]
    decision = "await_human" if case == "budget_await_human" else case
    lib = one_step_book(**spec.get("step", {}))
    fired = _crash_on(monkeypatch, decision)
    m = CareerManager(ctx, adapter=ScriptedAdapter(spec["script"]), playbooks=lib)
    oid = submit(m)
    with pytest.raises(SessionTerminatedError):
        m.run(oid)
    assert fired
    t = _task(ctx, oid)
    # Nothing of the interrupted transition is visible: still running, attempt open, no
    # decision of that kind, no extra escalation, no approval request.
    assert (t["status"], t["attempt"], t["attempt_open"], t["escalations"]) == ("running", 1, 1, 0)
    assert decision not in decisions(ctx, t["id"])
    assert ctx.db.query("SELECT * FROM approval_requests") == []
    if case == "route":
        assert ctx.db.query("SELECT * FROM task_checkpoints") == [] and calls(ctx) == []
    if case == "accept":
        assert ctx.db.query("SELECT * FROM routing_stats") == []
        assert ctx.db.query("SELECT * FROM cache_entries") == []
    monkeypatch.undo()
    # Recovery resumes the same attempt; the transition then happens exactly once.
    resumed = CareerManager(
        ctx,
        adapter=ScriptedAdapter(spec.get("resume_script", spec["script"])),
        playbooks=lib,
        clock=later(10),
    )
    resumed.run(oid)
    t = _task(ctx, oid)
    ds = decisions(ctx, t["id"])
    task_routes = ctx.db.query(  # the review's own route decision (C7) is not the task's
        "SELECT 1 FROM manager_decisions WHERE task_id=? AND decision='route'"
        " AND COALESCE(json_extract(inputs, '$.purpose'), 'task') <> 'review'",
        (t["id"],),
    )
    count = len(task_routes) if decision == "route" else ds.count(decision)
    assert ds.count("recover") == 1 and count == 1
    if case in ("accept", "route"):
        assert t["status"] == "succeeded" and len(calls(ctx, purpose="task")) == 1
        assert ctx.db.query_one("SELECT SUM(attempts) AS n FROM routing_stats")["n"] == 1
    if case == "escalate_effort":
        assert t["status"] == "succeeded" and t["escalations"] == 1
    if case in ("await_human", "budget_await_human"):
        assert len(ctx.db.query("SELECT * FROM approval_requests")) == 1
        assert t["attempt"] == 1


def test_crash_during_recovery_takeover_rolls_back(
    ctx: AppContext, monkeypatch: pytest.MonkeyPatch
) -> None:
    lib = one_step_book()
    m = CareerManager(ctx, adapter=GatedAdapter(n=1, crash_in_call={"m"}), playbooks=lib)
    oid = submit(m)
    with pytest.raises(SessionTerminatedError):
        m.run(oid)
    [stuck] = [r for r in calls(ctx) if r["status"] == "in_flight"]
    _crash_on(monkeypatch, "recover")
    with pytest.raises(SessionTerminatedError):
        CareerManager(ctx, playbooks=lib, clock=later(10))._recover(oid, "r1")
    # The takeover was all-or-nothing: still owned, call still in flight, money held.
    assert _task(ctx, oid)["status"] == "running"
    assert (
        ctx.db.query_one("SELECT status FROM model_calls WHERE id=?", (stuck["id"],))["status"]
        == "in_flight"
    )
    assert len(ctx.db.query("SELECT * FROM budget_reservations WHERE status='held'")) == 1
    monkeypatch.undo()
    assert CareerManager(ctx, playbooks=lib, clock=later(10)).run(oid).status.value == "done"
    assert decisions(ctx, _task(ctx, oid)["id"]).count("recover") == 1
    abandoned = ctx.db.query_one("SELECT * FROM model_calls WHERE id=?", (stuck["id"],))
    assert abandoned["status"] == "abandoned"
    spent, total = ledger_consistent(ctx, oid)
    assert spent == pytest.approx(total)


def test_crash_inside_requeue_and_worker_error_rolls_back(
    ctx: AppContext, monkeypatch: pytest.MonkeyPatch
) -> None:
    m = CareerManager(ctx, playbooks=one_step_book())
    oid = submit(m)
    task = ctx.orchestration.list_tasks(oid)[0]
    fence = m._claim(task, ctx.orchestration.start_session())
    assert fence is not None
    for decision, op in [
        ("requeue", lambda: m._requeue(fence, task, "contention")),
        ("worker_error", lambda: m._worker_failed(oid, fence, RuntimeError("bug"))),
    ]:
        _crash_on(monkeypatch, decision)
        with pytest.raises(SessionTerminatedError):
            op()
        monkeypatch.undo()
        t = _task(ctx, oid)
        assert (t["status"], t["attempt_open"], t["lease_owner"]) == ("running", 1, fence.session)
        assert decision not in decisions(ctx, t["id"])


# ====================================================================== concurrent recovery
def test_concurrent_recoveries_take_over_each_task_exactly_once(ctx: AppContext) -> None:
    lib = PlaybookLibrary(
        [
            Playbook.model_validate(
                {
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
            )
        ]
    )
    m = CareerManager(ctx, adapter=GatedAdapter(n=3, crash_in_call={"m0"}), playbooks=lib)
    oid = submit(m, "three")
    with pytest.raises(SessionTerminatedError):
        m.run(oid)
    running = [t for t in ctx.orchestration.list_tasks(oid) if t.status is TaskStatus.RUNNING]
    assert running
    barrier = threading.Barrier(6)

    def recover(i: int) -> None:
        with open_context_at(ctx.config, ctx.data_dir) as c:
            r = CareerManager(c, playbooks=lib, clock=later(10))
            sid = c.orchestration.start_session()
            barrier.wait(5)
            r._recover(oid, sid)

    threads = [threading.Thread(target=recover, args=(i,)) for i in range(6)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    for t in running:
        assert decisions(ctx, t.id).count("recover") == 1
    abandoned = [r for r in calls(ctx) if r["status"] == "abandoned"]
    assert all(r["cost_usd"] == pytest.approx(r["reserved_usd"]) for r in abandoned)
    assert ctx.db.query("SELECT * FROM budget_reservations WHERE status='held'") == []
    spent, total = ledger_consistent(ctx, oid)
    assert spent == pytest.approx(total)
    assert CareerManager(ctx, playbooks=lib).run(oid).status.value == "done"
    keys = [(r["task_id"], r["owner_generation"]) for r in calls(ctx, purpose="task")]
    assert len(keys) == len(set(keys))


# ====================================================================== no duplicates
def test_no_duplicate_results_reviews_or_decisions_after_interruptions(ctx: AppContext) -> None:
    from careeros.simulation import run_scenario

    oid = run_scenario(ctx, "interrupt").objective_id
    assert done(ctx, oid)
    dup = ctx.db.query(
        "SELECT task_id, attempt, COUNT(*) n FROM task_results"
        " GROUP BY task_id, attempt HAVING n > 1"
    )
    assert dup == []
    for t in ctx.orchestration.list_tasks(oid):
        ds = decisions(ctx, t.id)
        assert ds.count("accept") <= 1
    rows = ctx.db.query(
        "SELECT task_id, attempt, purpose, COUNT(*) n FROM model_calls"
        " WHERE status IN ('ok','malformed','timeout','rate_limit','error')"
        " GROUP BY task_id, attempt, purpose HAVING n > 1"
    )
    assert rows == []
    with pytest.raises(sqlite3.IntegrityError):
        ctx.db.conn.execute(
            "INSERT INTO task_results (task_id, attempt, schema_version, status, confidence,"
            " created_at) SELECT task_id, attempt, '1', 'succeeded', 1, 'x' FROM task_results"
            " LIMIT 1"
        )
    stop = from_json(
        ctx.db.query_one("SELECT inputs FROM manager_decisions WHERE decision='stop'")["inputs"]
    )
    assert stop["status"] == "done"


def test_lock_contention_cannot_bypass_the_ownership_check(ctx: AppContext) -> None:
    """A stale owner's write that has to wait for the write lock (held by a concurrent
    takeover) is checked *after* acquiring it, so it is rejected, not applied."""
    lib = one_step_book()
    m = CareerManager(ctx, playbooks=lib)
    oid = submit(m)
    task = ctx.orchestration.list_tasks(oid)[0]
    fence = m._claim(task, ctx.orchestration.start_session())
    assert fence is not None
    locked, release = threading.Event(), threading.Event()

    def takeover() -> None:
        with open_context_at(ctx.config, ctx.data_dir) as c, c.db.transaction() as conn:
            conn.execute("UPDATE tasks SET status='ready', lease_owner=NULL WHERE id=?", (task.id,))
            locked.set()
            release.wait(5)  # hold the write lock while the stale owner tries to write

    t = threading.Thread(target=takeover)
    t.start()
    assert locked.wait(5)
    threading.Timer(0.5, release.set).start()
    started = time.monotonic()
    with pytest.raises(OwnershipLostError):
        m._requeue(fence, task, "stale write under contention")
    assert time.monotonic() - started >= 0.4  # it waited for the lock, then was fenced out
    t.join(5)
    assert _task(ctx, oid)["status"] == "ready"
    assert "requeue" not in decisions(ctx, task.id)
