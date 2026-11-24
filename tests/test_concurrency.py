"""M3: bounded, dependency-aware concurrent execution."""

from __future__ import annotations

import threading
from typing import Any

import pytest

from careeros.context import AppContext, open_context
from careeros.core.config import AppConfig
from careeros.core.db import DB_FILENAME, Database
from careeros.cost.budget import BudgetManager
from careeros.domain.models import ObjectiveStatus, TaskStatus
from careeros.llm.contracts import BudgetExceededError, ModelRequest, OutputSchema
from careeros.llm.gateway import CallContext
from careeros.manager.manager import CareerManager, ObjectiveRequest
from careeros.manager.plans import PlaybookLibrary
from careeros.simulation import SimulatedCrashError, later
from tests.m2_helpers import calls, decisions, task_by_step
from tests.m3_helpers import (
    GatedAdapter,
    Probe,
    book,
    model_step,
    probe_manager,
    probe_step,
    with_limits,
)


def _submit(m: CareerManager, kind: str, **kw: Any) -> int:
    return m.submit(ObjectiveRequest(title=kind, kind=kind, inputs={"text": "synthetic"}, **kw))


def _step_reservation(ctx: AppContext, step: dict[str, Any]) -> float:
    """What the gateway will reserve for this step on its routed model (worst case)."""
    from careeros.llm.tokens import estimate_tokens
    from careeros.manager.plans import StepSpec
    from careeros.routing.router import Router
    from careeros.routing.stats import RoutingStatsRepository
    from careeros.workers.base import StepContext
    from careeros.workers.generic import ModelStepWorker

    spec = StepSpec.model_validate(step)
    req = ModelStepWorker().build_request(
        StepContext(
            step_id=spec.id,
            task_type=spec.task_type,
            title=spec.title or spec.task_type,
            instructions=spec.instructions,
            params={"text": "synthetic", **spec.params},
            deps={},
            output_schema=spec.output.to_schema(),
        )
    )
    profile = next(m for m in ctx.config.models if m.id == "mock-medium")
    return Router(ctx.config, RoutingStatsRepository(ctx.db)).reserve_cost(
        profile, spec.task_type, "low", estimate_tokens((req.system or "") + req.prompt)
    )


def _statuses(ctx: AppContext, oid: int) -> dict[str, str]:
    return {t.step or "": t.status.value for t in ctx.orchestration.list_tasks(oid)}


# ------------------------------------------------------------------ concurrency and limits
def test_independent_tasks_run_concurrently_up_to_the_default_limit(ctx: AppContext) -> None:
    assert ctx.config.execution.max_concurrency == 3
    probe = Probe()
    pb = book("wide", [probe_step(f"s{i}", target=3, hold=2.0) for i in range(6)])
    m = probe_manager(ctx, pb, probe)
    summary = m.run(_submit(m, "wide"))
    assert summary.status is ObjectiveStatus.DONE
    assert probe.max_active == 3  # reached, and never exceeded
    assert len(probe.threads) <= 3
    assert all(n == 1 for n in probe.runs.values()) and len(probe.runs) == 6


@pytest.mark.parametrize("limit", [1, 2, 5])
def test_concurrency_limit_is_configurable(ctx: AppContext, limit: int) -> None:
    ctx.config = with_limits(ctx.config, max_concurrency=limit)
    probe = Probe()
    pb = book("wide", [probe_step(f"s{i}", target=limit, hold=1.0) for i in range(6)])
    m = probe_manager(ctx, pb, probe)
    assert m.run(_submit(m, "wide")).status is ObjectiveStatus.DONE
    assert probe.max_active == limit


def test_dependents_wait_for_all_prerequisites(ctx: AppContext) -> None:
    probe = Probe()
    pb = book(
        "diamond",
        [
            probe_step("a", target=2, hold=2.0, work=0.05),
            probe_step("b", target=2, hold=2.0, work=0.05),
            probe_step("c", ["a", "b"]),
            probe_step("d", ["c"]),
        ],
    )
    m = probe_manager(ctx, pb, probe)
    assert m.run(_submit(m, "diamond")).status is ObjectiveStatus.DONE
    assert probe.max_active >= 2  # a and b overlapped
    assert probe.starts["c"] >= max(probe.ends["a"], probe.ends["b"])
    assert probe.starts["d"] >= probe.ends["c"]


def test_failed_prerequisite_skips_only_its_dependents(ctx: AppContext) -> None:
    probe = Probe()
    pb = book(
        "branch",
        [
            probe_step("bad", fail=True),
            probe_step("good"),
            probe_step("after_bad", ["bad"]),
            probe_step("after_after_bad", ["after_bad"]),
            probe_step("after_good", ["good"]),
        ],
    )
    m = probe_manager(ctx, pb, probe)
    oid = _submit(m, "branch")
    summary = m.run(oid)
    st = _statuses(ctx, oid)
    assert st["bad"] == "failed" and st["good"] == "succeeded"
    assert st["after_bad"] == "skipped" and st["after_after_bad"] == "skipped"
    assert st["after_good"] == "succeeded"
    assert "after_bad" not in probe.runs and "after_after_bad" not in probe.runs
    assert summary.status is ObjectiveStatus.FAILED  # all_required cannot be met
    assert "skip" in decisions(ctx, task_by_step(ctx, oid, "after_bad")["id"])


def test_worker_exception_is_retried_without_disturbing_others(ctx: AppContext) -> None:
    probe = Probe()
    pb = book(
        "flaky",
        [
            probe_step("flaky", raise_times=1, target=3, hold=1.0),
            probe_step("x", target=3, hold=1.0),
            probe_step("y", target=3, hold=1.0),
            probe_step("z", ["flaky"]),
        ],
    )
    m = probe_manager(ctx, pb, probe)
    oid = _submit(m, "flaky")
    assert m.run(oid).status is ObjectiveStatus.DONE
    flaky = task_by_step(ctx, oid, "flaky")
    assert flaky["attempt"] == 2 and probe.runs["flaky"] == 2
    assert decisions(ctx, flaky["id"]).count("worker_error") == 1
    assert probe.runs["x"] == probe.runs["y"] == probe.runs["z"] == 1


def test_worker_exception_retries_are_bounded(ctx: AppContext) -> None:
    probe = Probe()
    pb = book("broken", [probe_step("broken", raise_times=99), probe_step("child", ["broken"])])
    m = probe_manager(ctx, pb, probe)
    oid = _submit(m, "broken")
    m.run(oid)
    assert probe.runs["broken"] == ctx.config.limits.max_attempts
    assert _statuses(ctx, oid) == {"broken": "failed", "child": "skipped"}


def test_definition_of_done_cancels_unstarted_optional_work(ctx: AppContext) -> None:
    probe = Probe()
    steps = [
        probe_step("core"),
        {**probe_step("extra", ["core"]), "optional": True},
        {**probe_step("extra2", ["extra"]), "optional": True},
    ]
    pb = book("done_early", steps, definition_of_done=["accepted:core"])
    m = probe_manager(ctx, pb, probe)
    oid = _submit(m, "done_early")
    assert m.run(oid).status is ObjectiveStatus.DONE
    st = _statuses(ctx, oid)
    assert st == {"core": "succeeded", "extra": "cancelled", "extra2": "cancelled"}
    assert set(probe.runs) == {"core"}


def test_concurrent_model_tasks_complete_and_are_routed_independently(ctx: AppContext) -> None:
    adapter = GatedAdapter(n=3)
    pb = book(
        "models",
        [model_step(f"m{i}") for i in range(3)]
        + [model_step("classify", ["m0"], task_type="job.classify_role")],
    )
    m = CareerManager(ctx, adapter=adapter, playbooks=PlaybookLibrary([pb]))
    oid = _submit(m, "models")
    assert m.run(oid).status is ObjectiveStatus.DONE
    assert adapter.max_active == 3
    tiers = {task_by_step(ctx, oid, s)["current_tier"] for s in ("m0", "classify")}
    assert tiers == {2, 3}


# ------------------------------------------------------------------ budgets under contention
def test_reservations_are_atomic_across_connections(ctx: AppContext) -> None:
    from careeros.domain.models import Objective

    obj = ctx.orchestration.create_objective(
        Objective(kind="t", title="t", definition_of_done=["x"], budget_usd=1.0)
    )
    assert obj.id is not None
    lines = BudgetManager(ctx.db, ctx.config).lines_for(obj.id, None)
    path = ctx.data_dir / DB_FILENAME
    barrier = threading.Barrier(20)
    wins: list[bool] = []
    lock = threading.Lock()

    def contend() -> None:
        db = Database.open(path, dataset_kind="synthetic")
        try:
            barrier.wait(timeout=5)
            try:
                BudgetManager(db, ctx.config).reserve(lines, 0.1)
                ok = True
            except BudgetExceededError:
                ok = False
            with lock:
                wins.append(ok)
        finally:
            db.close()

    threads = [threading.Thread(target=contend) for _ in range(20)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert wins.count(True) == 10  # exactly the budget's worth, never more
    obj_line = BudgetManager(ctx.db, ctx.config).line(lines[1])
    assert obj_line.reserved_usd == pytest.approx(1.0)
    assert obj_line.reserved_usd <= obj_line.limit_usd + 1e-9


def test_concurrent_tasks_never_exceed_the_objective_budget(ctx: AppContext) -> None:
    path = ctx.data_dir / DB_FILENAME
    violations: list[str] = []
    seen_reserved: list[float] = []

    def check_invariant(_: str | None) -> None:
        db = Database.open(path, dataset_kind="synthetic")
        try:
            for r in db.query("SELECT * FROM budgets"):
                if r["scope"] == "objective":
                    seen_reserved.append(r["reserved_usd"])
                if r["spent_usd"] + r["reserved_usd"] > r["limit_usd"] + 1e-9:
                    violations.append(f"{r['scope']}:{r['scope_ref']}")
        finally:
            db.close()

    def contention_seen() -> bool:
        db = Database.open(path, dataset_kind="synthetic")
        try:
            return bool(db.query("SELECT 1 FROM manager_decisions WHERE decision='requeue'"))
        finally:
            db.close()

    # The first two calls stay in flight until a third task has hit the reservation limit.
    adapter = GatedAdapter(n=2, timeout=3.0, on_call=check_invariant, hold_until=contention_seen)
    # Budget for two (not three) concurrent worst-case reservations.
    reserve = _step_reservation(ctx, model_step("m0"))
    pb = book(
        "tight",
        [model_step(f"m{i}") for i in range(4)],
        budget_usd=2.5 * reserve,
        priority="normal",
    )
    m = CareerManager(ctx, adapter=adapter, playbooks=PlaybookLibrary([pb]))
    oid = _submit(m, "tight")
    m.run(oid)
    assert violations == []
    assert adapter.max_active == 2  # a third concurrent reservation did not fit
    requeues = ctx.db.query(
        "SELECT * FROM manager_decisions WHERE objective_id=? AND decision='requeue'", (oid,)
    )
    assert requeues, "budget contention should requeue, not ask the user"
    line = ctx.db.query_one(
        "SELECT * FROM budgets WHERE scope='objective' AND scope_ref=?", (str(oid),)
    )
    total = sum(float(c["cost_usd"]) for c in calls(ctx, objective_id=oid))
    assert line["spent_usd"] == pytest.approx(total)
    assert line["spent_usd"] <= line["limit_usd"] + 1e-9
    assert line["reserved_usd"] == pytest.approx(0.0)
    assert ctx.db.query("SELECT * FROM budget_reservations WHERE status='held'") == []
    # Every task either completed or is waiting for the user's budget decision.
    assert set(_statuses(ctx, oid).values()) <= {"succeeded", "blocked_budget", "cancelled"}


def test_session_budget_is_enforced(ctx: AppContext) -> None:
    ctx.config = ctx.config.model_copy(
        update={"budgets": ctx.config.budgets.model_copy(update={"session_simulated_usd": 0.001})}
    )
    m = CareerManager(ctx)
    oid = _submit(m, "job_triage")
    session = ctx.orchestration.start_session()
    req = ModelRequest(
        task_type="research.company",
        prompt="x" * 400,
        output_schema=OutputSchema(("summary",)),
        effort="low",
    )
    with pytest.raises(BudgetExceededError) as exc:
        m.gateway.call("mock-medium", req, CallContext(oid, None, 1, "task", session))
    assert exc.value.scope == "session"


# ------------------------------------------------------------------ duplicate prevention
def test_claim_is_exclusive_across_sessions(ctx: AppContext, cfg: AppConfig) -> None:
    m = CareerManager(ctx)
    oid = _submit(m, "job_triage")
    task = ctx.orchestration.list_tasks(oid)[0]
    barrier = threading.Barrier(8)
    wins: list[bool] = []
    lock = threading.Lock()

    def claim() -> None:
        with open_context(cfg) as c2:
            other = CareerManager(c2)
            sid = c2.orchestration.start_session()
            barrier.wait(timeout=5)
            ok = other._claim(task, sid) is not None  # a fencing token, or None
            with lock:
                wins.append(ok)

    threads = [threading.Thread(target=claim) for _ in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert wins.count(True) == 1
    assert ctx.orchestration.get_task(task.id or 0).attempt == 1


def test_two_concurrent_sessions_never_duplicate_work(ctx: AppContext, cfg: AppConfig) -> None:
    pb = book(
        "shared",
        [model_step(f"m{i}") for i in range(4)]
        + [model_step("final", [f"m{i}" for i in range(4)], task_type="job.classify_role")],
    )
    adapter = GatedAdapter(n=4, timeout=1.0)
    first = CareerManager(ctx, adapter=adapter, playbooks=PlaybookLibrary([pb]))
    oid = _submit(first, "shared")
    errors: list[BaseException] = []

    def run_other() -> None:
        try:
            with open_context(cfg) as c2:
                CareerManager(c2, adapter=adapter, playbooks=PlaybookLibrary([pb])).run(oid)
        except BaseException as exc:  # surfaced below
            errors.append(exc)

    t = threading.Thread(target=run_other)
    t.start()
    first.run(oid)
    t.join()
    assert errors == []
    if ctx.orchestration.get_objective(oid).status is ObjectiveStatus.ACTIVE:
        first.run(oid)  # the session that finished first may have seen the other's work
    assert ctx.orchestration.get_objective(oid).status is ObjectiveStatus.DONE
    rows = calls(ctx, objective_id=oid, purpose="task")
    keys = [(r["task_id"], r["attempt"]) for r in rows]
    assert len(keys) == len(set(keys)), "a task attempt made more than one model call"
    for task in ctx.orchestration.list_tasks(oid):
        attempts = [r.attempt for r in ctx.orchestration.list_results(task.id or 0)]
        assert len(attempts) == len(set(attempts))
    # Every task model call the adapter served is one recorded row (reviews, required for
    # research since C7, are separate calls of their own).
    assert len([c for c in adapter.calls if c[1] != "quality.review"]) == len(rows)


def test_gateway_idempotency_key_prevents_duplicate_calls(ctx: AppContext) -> None:
    adapter = GatedAdapter(n=1)
    m = CareerManager(ctx, adapter=adapter)
    oid = _submit(m, "job_triage")
    req = ModelRequest(
        task_type="job.classify_role",
        prompt="classify",
        output_schema=OutputSchema(("role_family",)),
    )
    ctx_call = CallContext(oid, None, 1, "task", None, "unit-test-key")
    a = m.gateway.call("mock-small", req, ctx_call)
    b = m.gateway.call("mock-small", req, ctx_call)
    assert a.call_id == b.call_id and b.cost_usd == 0 and a.payload == b.payload
    assert len(adapter.calls) == 1 and len(calls(ctx)) == 1


# ------------------------------------------------------------------ interruption / recovery
def _crash_two_workers(ctx: AppContext) -> tuple[CareerManager, int, GatedAdapter]:
    crashed: list[bool] = []

    def crash(point: str, task: Any) -> None:
        if point == "after_model_response" and task.step == "m0" and not crashed:
            crashed.append(True)
            raise SimulatedCrashError("process killed")

    pb = book(
        "pair",
        [
            model_step("m0"),
            model_step("m1"),
            model_step("m2"),
            model_step("join", ["m0", "m1", "m2"], task_type="job.classify_role"),
        ],
    )
    adapter = GatedAdapter(n=3)
    m = CareerManager(ctx, adapter=adapter, playbooks=PlaybookLibrary([pb]), fault_hook=crash)
    oid = _submit(m, "pair")
    with pytest.raises(SimulatedCrashError):
        m.run(oid)
    return m, oid, adapter


def test_crash_with_several_active_workers_halts_all_and_keeps_state(ctx: AppContext) -> None:
    _, oid, adapter = _crash_two_workers(ctx)
    assert adapter.max_active == 3  # three workers were in flight together
    running = [t for t in ctx.orchestration.list_tasks(oid) if t.status is TaskStatus.RUNNING]
    assert running, "interrupted tasks keep their leases"
    assert all(t.lease_owner and t.lease_expires_at for t in running)
    assert task_by_step(ctx, oid, "join")["status"] == "pending"  # never dispatched
    assert ctx.db.query("SELECT * FROM budget_reservations WHERE status='held'") == []


def test_live_leases_are_not_stolen(ctx: AppContext) -> None:
    _, oid, _ = _crash_two_workers(ctx)
    before = len(calls(ctx))
    summary = CareerManager(ctx, playbooks=PlaybookLibrary([])).run(oid)
    assert summary.stop_reason == "tasks are running in another session"
    assert len(calls(ctx)) == before  # nothing was re-executed


@pytest.mark.parametrize("how", ["lease_expired", "force"])
def test_recovery_after_multi_worker_crash_has_no_duplicate_calls(
    ctx: AppContext, how: str
) -> None:
    m, oid, _ = _crash_two_workers(ctx)
    completed_before = {
        (r["task_id"], r["attempt"]) for r in calls(ctx, purpose="task") if r["status"] == "ok"
    }
    kw: dict[str, Any] = {"clock": later(10)} if how == "lease_expired" else {}
    resumed = CareerManager(ctx, adapter=GatedAdapter(n=1), playbooks=m.playbooks, **kw)
    summary = resumed.run(oid, force_recover=(how == "force"))
    assert summary.status is ObjectiveStatus.DONE
    rows = calls(ctx, purpose="task")
    keys = [(r["task_id"], r["attempt"]) for r in rows]
    assert len(keys) == len(set(keys))
    for key in completed_before:  # replies received before the crash were reused
        assert keys.count(key) == 1
    recovered = ctx.db.query("SELECT * FROM manager_decisions WHERE decision='recover'")
    assert recovered and all(r["objective_id"] == oid for r in recovered)


def test_call_in_flight_at_crash_is_abandoned_and_charged_conservatively(ctx: AppContext) -> None:
    pb = book("inflight", [model_step("m0"), model_step("m1")])
    adapter = GatedAdapter(n=2, crash_in_call={"m1"})
    m = CareerManager(ctx, adapter=adapter, playbooks=PlaybookLibrary([pb]))
    oid = _submit(m, "inflight")
    with pytest.raises(SimulatedCrashError):
        m.run(oid)
    [stuck] = [r for r in calls(ctx) if r["status"] == "in_flight"]
    held = ctx.db.query("SELECT * FROM budget_reservations WHERE status='held'")
    assert len(held) == 1 and held[0]["id"] == stuck["reservation_id"]

    resumed = CareerManager(ctx, playbooks=m.playbooks, clock=later(10))
    assert resumed.run(oid).status is ObjectiveStatus.DONE
    row = ctx.db.query_one("SELECT * FROM model_calls WHERE id=?", (stuck["id"],))
    # Charged the full worst-case reservation (a capped reply), not the optimistic estimate.
    assert row["status"] == "abandoned" and row["cost_usd"] == pytest.approx(row["reserved_usd"])
    assert row["reserved_usd"] > row["est_cost_usd"]
    m1 = task_by_step(ctx, oid, "m1")
    statuses = [r["status"] for r in calls(ctx, task_id=m1["id"], purpose="task")]
    assert statuses == ["abandoned", "ok"]
    assert ctx.db.query("SELECT * FROM budget_reservations WHERE status='held'") == []
    line = ctx.db.query_one(
        "SELECT * FROM budgets WHERE scope='objective' AND scope_ref=?", (str(oid),)
    )
    total = sum(float(r["cost_usd"]) for r in calls(ctx, objective_id=oid))
    assert line["spent_usd"] == pytest.approx(total)


def test_heartbeat_renews_leases(ctx: AppContext) -> None:
    ctx.config = ctx.config.model_copy(
        update={"limits": ctx.config.limits.model_copy(update={"heartbeat_seconds": 0.05})}
    )
    m = CareerManager(ctx)
    oid = _submit(m, "job_triage")
    task = ctx.orchestration.list_tasks(oid)[0]
    session = ctx.orchestration.start_session()
    assert m._claim(task, session) is not None
    first = ctx.orchestration.get_task(task.id or 0).lease_expires_at
    from careeros.manager.manager import LeaseState

    stop = threading.Event()
    # 30 s later: still inside the 120 s lease, so the heartbeat must extend it. (A lease
    # that has already expired is never resurrected - see the Phase B tests.)
    m.clock = later(0.5)
    hb = threading.Thread(
        target=m._heartbeat, args=(session, stop, "synthetic", LeaseState(m.clock()))
    )
    hb.start()
    threading.Event().wait(0.3)
    stop.set()
    hb.join()
    assert (ctx.orchestration.get_task(task.id or 0).lease_expires_at or "") > (first or "")


# ------------------------------------------------------------------ determinism
def test_concurrent_escalation_scenario_is_deterministic(tmp_path: Any, monkeypatch: Any) -> None:
    from careeros.core.config import load_config
    from careeros.simulation import run_scenario
    from tests.conftest import CONFIG_DIR

    fingerprints = []
    for i in range(4):
        monkeypatch.setenv("CAREEROS_DATA_DIR", str(tmp_path / f"run{i}"))
        with open_context(load_config(CONFIG_DIR)) as c:
            oid = run_scenario(c, "escalation").objective_id
            per_task = {
                t.step: (
                    t.status.value,
                    t.attempt,
                    t.current_model_id,
                    t.current_effort,
                    tuple(d for d in decisions(c, t.id) if d != "route"),
                )
                for t in c.orchestration.list_tasks(oid)
            }
            cost = round(sum(float(r["cost_usd"]) for r in calls(c, objective_id=oid)), 8)
            fingerprints.append((per_task, cost))
    assert all(f == fingerprints[0] for f in fingerprints)
