"""Phase A regression tests: each reproduces a finding of the post-M3 audit."""

from __future__ import annotations

import threading
from typing import Any, ClassVar

import pytest

from careeros.context import AppContext, open_context_at
from careeros.core.config import AppConfig, ModelProfile
from careeros.core.db import from_json
from careeros.core.errors import CareerOSError
from careeros.cost.budget import BudgetManager
from careeros.domain.models import ObjectiveStatus, TaskStatus
from careeros.llm.adapters.mock import MockAdapter
from careeros.llm.adapters.scripted import ScriptedAdapter
from careeros.llm.contracts import (
    BudgetExceededError,
    ModelRequest,
    ModelResponse,
    OutputSchema,
)
from careeros.llm.gateway import CallContext
from careeros.manager.manager import CareerManager, ObjectiveRequest
from careeros.manager.plans import Playbook, PlaybookLibrary
from careeros.routing.complexity import estimate_complexity
from careeros.routing.router import Router
from careeros.routing.stats import RoutingStatsRepository
from tests.m2_helpers import calls, task_by_step

FACTUAL = ("resume.tailor", "cover_letter.draft", "application.answers", "eligibility.assess")
PRIORITIES = ("critical", "high", "normal", "low")


def _router(ctx: AppContext) -> tuple[Router, RoutingStatsRepository]:
    stats = RoutingStatsRepository(ctx.db)
    return Router(ctx.config, stats), stats


def _history(
    stats: RoutingStatsRepository,
    router: Router,
    task_type: str,
    accept_rate: Any,
    params: dict[str, Any] | None = None,
    n: int = 40,
) -> None:
    """Record ``n`` reviewed outcomes per option with the given acceptance rate."""
    cx = estimate_complexity(router.config.routing.policy(task_type), params or {})
    for c in router.candidates(task_type, cx):
        rate = accept_rate(c) if callable(accept_rate) else accept_rate
        for i in range(n):
            stats.record(
                task_type,
                cx.bucket,
                c.model_id,
                c.effort,
                accepted=(i / n) < rate,
                prior_alpha=1,
                prior_beta=1,
                quality=0.5,
                cost_usd=0.01,
                latency_ms=1,
            )


def _book(name: str, steps: list[dict[str, Any]], **kw: Any) -> PlaybookLibrary:
    return PlaybookLibrary(
        [Playbook.model_validate({"name": name, "plan": {"steps": steps}, **kw})]
    )


def _submit(m: CareerManager, kind: str, **inputs: Any) -> int:
    return m.submit(
        ObjectiveRequest(
            title=kind, kind=kind, inputs={"text": "robotics firmware intern", **inputs}
        )
    )


# ====================================================================== H1: A9 policy
def test_h1_critical_task_not_downgraded_by_budget_after_history(ctx: AppContext) -> None:
    """Audit reproduction: max effort is good (p~0.86) but costly; high effort is cheap but
    poor (p~0.36). The old router silently picked the cheap poor option."""
    router, stats = _router(ctx)
    tt = "manager.strategic_review"
    _history(stats, router, tt, lambda c: 0.85 if c.effort == "max" else 0.3)
    cx = estimate_complexity(ctx.config.routing.policy(tt), {})
    top = max(router.candidates(tt, cx), key=lambda c: c.p_accept)
    d = router.route(tt, "critical", cx, top.reserve_usd * 0.9)
    assert d.status == "needs_approval" and d.choice is None
    assert "never downgraded automatically" in d.reason
    # The cheap option is below the hard minimum (0.5), so it is not even offered.
    assert d.extra["downgrade_option"] is None


def test_h1_high_factual_task_never_routed_below_its_minimum(ctx: AppContext) -> None:
    """Audit reproduction: all options at p~0.58 < resume.tailor minimum 0.80."""
    router, stats = _router(ctx)
    _history(stats, router, "resume.tailor", 0.5)
    cx = estimate_complexity(ctx.config.routing.policy("resume.tailor"), {})
    d = router.route("resume.tailor", "high", cx, 10.0)
    assert d.choice is None and d.status == "no_candidate"
    assert "hard minimum quality 0.80" in d.reason


@pytest.mark.parametrize("task_type", (*FACTUAL, "job.analyze", "research.company"))
@pytest.mark.parametrize("priority", PRIORITIES)
@pytest.mark.parametrize("rate", [0.2, 0.55, 0.75, 0.9])
@pytest.mark.parametrize("budget_frac", [0.3, 0.7, 2.0])
def test_h1_history_can_never_bypass_the_policy(
    ctx: AppContext, task_type: str, priority: str, rate: float, budget_frac: float
) -> None:
    router, stats = _router(ctx)
    # Make stronger options look better than weaker ones, scaled by `rate`.
    _history(stats, router, task_type, lambda c: min(1.0, rate + 0.1 * (3 - c.tier)))
    cx = estimate_complexity(ctx.config.routing.policy(task_type), {})
    cands = router.candidates(task_type, cx)
    budget = max(c.reserve_usd for c in cands) * budget_frac
    d = router.route(task_type, priority, cx, budget)
    floor, degrade_floor = router.floors(task_type, priority)
    hard_min = max(
        ctx.config.routing.absolute_min_quality, ctx.config.routing.policy(task_type).min_quality
    )
    if d.choice is None:
        assert d.status in ("needs_approval", "no_candidate")
        return
    assert d.choice.p_accept >= hard_min - 1e-9, "never below the hard minimum"
    assert d.choice.reserve_usd <= budget + 1e-12, "never chooses what it cannot reserve"
    if d.choice.p_accept < floor - 1e-9:
        assert priority in ("normal", "low"), "critical/high are never downgraded"
        assert d.status == "degraded"
        assert degrade_floor is not None and d.choice.p_accept >= degrade_floor - 1e-9
    else:
        assert d.status == "routed"


def test_h1_manager_blocks_critical_task_instead_of_downgrading(ctx: AppContext) -> None:
    pb = _book(
        "crit",
        [
            {
                "id": "review_pkg",
                "worker": "model_step",
                "task_type": "manager.strategic_review",
                "priority": "critical",
                "review": False,
                "output": {"required": ["verdict"]},
            }
        ],
    )
    m = CareerManager(ctx, playbooks=pb)
    _history(
        m.stats, m.router, "manager.strategic_review", lambda c: 0.85 if c.effort == "max" else 0.3
    )
    oid = _submit(m, "crit")
    t = task_by_step(ctx, oid, "review_pkg")
    with ctx.db.transaction() as c:  # task budget between the cheap and the good option
        c.execute("UPDATE tasks SET budget_usd=0.13 WHERE id=?", (t["id"],))
    m.run(oid)
    t = task_by_step(ctx, oid, "review_pkg")
    assert t["status"] == "blocked_budget"
    assert calls(ctx, task_id=t["id"]) == []  # nothing ran below the floor
    [req] = m.pending_requests(oid)
    assert "never downgraded automatically" in req["details"]["reason"]


def test_h1_effort_escalation_falls_back_to_tier_rather_than_asking(ctx: AppContext) -> None:
    """A critical task that cannot meet its floor by raising effort on the same model
    escalates to a stronger tier instead of stopping to ask."""
    router, _ = _router(ctx)
    cx = estimate_complexity(ctx.config.routing.policy("job.analyze"), {"complexity_hint": 0.5})
    effort_only = router.route(
        "job.analyze", "critical", cx, 10.0, current=("mock-medium", "high"), escalation="effort"
    )
    assert effort_only.status == "no_candidate"  # mock-medium has no higher effort
    pb = _book(
        "esc",
        [
            {
                "id": "x",
                "worker": "model_step",
                "task_type": "job.analyze",
                "priority": "critical",
                "params": {"complexity_hint": 0.5},
                "output": {"required": ["summary"]},
            }
        ],
    )
    m = CareerManager(
        ctx, adapter=ScriptedAdapter({"x:job.analyze": ["low_confidence"]}), playbooks=pb
    )
    oid = _submit(m, "esc")
    assert m.run(oid).status is ObjectiveStatus.DONE
    tiers = [r["tier"] for r in calls(ctx, task_id=task_by_step(ctx, oid, "x")["id"])]
    assert tiers[0] >= tiers[-1]


# ====================================================================== M6: downgrades recorded
def test_m6_review_downgrade_is_recorded(ctx: AppContext) -> None:
    pb = _book(
        "rev",
        [
            {
                "id": "draft",
                "worker": "model_step",
                "task_type": "research.company",
                "priority": "normal",
                "important": True,
                "review": True,
                "output": {"required": ["summary"]},
            }
        ],
    )
    m = CareerManager(
        ctx,
        adapter=ScriptedAdapter(
            {"draft:research.company": ["success:0.95"], "draft:quality.review": ["review:accept"]}
        ),
        playbooks=pb,
    )
    _history(m.stats, m.router, "quality.review", 0.7)  # reviewers in [0.65, 0.75)
    oid = _submit(m, "rev")
    assert m.run(oid).status is ObjectiveStatus.DONE
    rows = ctx.db.query("SELECT inputs FROM manager_decisions WHERE decision='degrade'")
    purposes = [from_json(r["inputs"])["purpose"] for r in rows]
    assert purposes == ["review:draft"]


def test_m6_planning_downgrade_is_recorded(ctx: AppContext) -> None:
    m = CareerManager(ctx)
    _history(m.stats, m.router, "manager.plan_novel", 0.7, params={"title": "x", "ambiguous": True})
    oid = m.submit(
        ObjectiveRequest(title="Summarize my club notes", inputs={"text": "robotics notes sensors"})
    )
    rows = ctx.db.query(
        "SELECT inputs FROM manager_decisions WHERE decision='degrade' AND objective_id=?", (oid,)
    )
    assert [from_json(r["inputs"])["purpose"] for r in rows] == ["planning"]


def test_m6_review_approval_applies_only_to_the_review(ctx: AppContext) -> None:
    pb = _book(
        "revhigh",
        [
            {
                "id": "draft",
                "worker": "model_step",
                "task_type": "research.company",
                "priority": "high",
                "important": True,
                "review": True,
                "output": {"required": ["summary"]},
            }
        ],
    )
    m = CareerManager(
        ctx,
        adapter=ScriptedAdapter(
            {"draft:research.company": ["success:0.95"], "draft:quality.review": ["review:accept"]}
        ),
        playbooks=pb,
    )
    _history(m.stats, m.router, "quality.review", 0.7)  # below the high floor (0.85)
    oid = _submit(m, "revhigh")
    m.run(oid)
    [req] = m.pending_requests(oid)
    assert req["details"]["route_purpose"] == "review"
    m.approve(req["id"], accept_downgrade=True)
    t = task_by_step(ctx, oid, "draft")
    assert t["approved_floor"] is None and t["approved_review_floor"] is not None
    assert m.run(oid).status is ObjectiveStatus.DONE
    rows = ctx.db.query("SELECT inputs FROM manager_decisions WHERE decision='degrade'")
    [inputs] = [from_json(r["inputs"]) for r in rows]
    assert inputs["purpose"] == "review:draft" and inputs["user_approved"] is True
    task_calls = calls(ctx, task_id=t["id"], purpose="task")
    assert len(task_calls) == 1  # the task route itself was never downgraded or re-run


# ====================================================================== M1: budget overshoot
class _SpyAdapter(MockAdapter):
    seen_caps: ClassVar[list[int]] = []

    def call(self, profile: ModelProfile, request: ModelRequest) -> ModelResponse:
        _SpyAdapter.seen_caps.append(request.max_output_tokens)
        return super().call(profile, request)


class _CapIgnoringAdapter(MockAdapter):
    """Misbehaving provider: replies with 5x the requested output cap."""

    def call(self, profile: ModelProfile, request: ModelRequest) -> ModelResponse:
        r = super().call(profile, request)
        return ModelResponse(
            r.text,
            r.input_tokens,
            request.max_output_tokens * 5,
            r.latency_ms,
            "stop",
            r.simulation,
        )


def _tight_tasks(ctx: AppContext, m: CareerManager, n: int, req: ModelRequest) -> list[Any]:
    from careeros.domain.models import Objective, Plan, Task

    est = m.gateway.estimate(m.gateway.profile("mock-medium"), req)
    obj = ctx.orchestration.create_objective(
        Objective(kind="t", title="t", definition_of_done=["x"], budget_usd=est.reserve_usd * n)
    )
    plan = ctx.orchestration.create_plan(Plan(objective_id=obj.id or 0, version=1, source="t"))
    return [obj.id] + [
        ctx.orchestration.add_task(
            Task(
                objective_id=obj.id or 0,
                plan_id=plan.id or 0,
                worker="w",
                task_type=req.task_type,
                budget_usd=est.reserve_usd,
            )
        ).id
        for _ in range(n)
    ]


def test_m1_budget_invariant_holds_after_settlement(ctx: AppContext) -> None:
    """Audit reproduction: task limit == one call; the old estimate-sized reservation let
    actual cost settle 1.5% over the limit."""
    _SpyAdapter.seen_caps.clear()
    m = CareerManager(ctx, adapter=_SpyAdapter())
    req = ModelRequest(
        task_type="cover_letter.draft",
        prompt="Draft a letter. " * 20,
        output_schema=OutputSchema(("letter",)),
        effort="high",
        simulation={"target_output_tokens": 600},
    )
    oid, *tids = _tight_tasks(ctx, m, 30, req)
    for i, tid in enumerate(tids):
        r = ModelRequest(**{**req.__dict__, "seed_salt": str(i)})
        out = m.gateway.call("mock-medium", r, CallContext(oid, tid, 1))
        assert out.overshoot_usd == 0
    for line in ctx.db.query("SELECT * FROM budgets"):
        assert line["spent_usd"] <= line["limit_usd"] + 1e-12, line["scope"]
        assert line["reserved_usd"] == pytest.approx(0)
    rows = calls(ctx)
    cap = rows[0]["output_cap_tokens"]
    assert set(_SpyAdapter.seen_caps) == {cap}  # the provider was given the hard cap
    assert all(r["output_tokens"] <= r["output_cap_tokens"] for r in rows)
    assert all(r["cost_usd"] <= r["reserved_usd"] + 1e-12 for r in rows)
    assert any(r["cost_usd"] > r["est_cost_usd"] for r in rows)  # estimates were exceeded...
    # ...but never the reservation.


def test_m1_overshoot_is_recorded_and_blocks_further_spend(ctx: AppContext) -> None:
    m = CareerManager(ctx, adapter=_CapIgnoringAdapter())
    req = ModelRequest(
        task_type="research.company",
        prompt="Summarize. " * 10,
        output_schema=OutputSchema(("summary",)),
        effort="low",
    )
    oid, tid = _tight_tasks(ctx, m, 1, req)
    out = m.gateway.call("mock-medium", req, CallContext(oid, tid, 1))
    assert out.overshoot_usd > 0
    [row] = calls(ctx)
    assert row["overshoot_usd"] == pytest.approx(out.overshoot_usd)
    assert "exceeds reservation" in (row["error"] or "")
    with pytest.raises(BudgetExceededError):  # the overrun line refuses any further call
        m.gateway.call(
            "mock-medium",
            ModelRequest(**{**req.__dict__, "seed_salt": "2"}),
            CallContext(oid, tid, 2),
        )


def test_m1_router_affordability_uses_the_worst_case_reservation(ctx: AppContext) -> None:
    router, _ = _router(ctx)
    cx = estimate_complexity(ctx.config.routing.policy("research.company"), {})
    c = next(
        c
        for c in router.candidates("research.company", cx)
        if (c.model_id, c.effort) == ("mock-medium", "low")
    )
    assert c.reserve_usd > c.est_cost_usd
    between = (c.est_cost_usd + c.reserve_usd) / 2
    d = router.route("research.company", "normal", cx, between)
    assert d.choice is None or d.choice.reserve_usd <= between


def test_m1_concurrent_run_keeps_every_line_within_limit(ctx: AppContext) -> None:
    from tests.m3_helpers import book, model_step

    pb = book("tight3", [model_step(f"m{i}") for i in range(6)], budget_usd=0.05, priority="normal")
    m = CareerManager(ctx, playbooks=PlaybookLibrary([pb]))
    oid = _submit(m, "tight3")
    m.run(oid)
    for line in ctx.db.query("SELECT * FROM budgets"):
        assert line["spent_usd"] <= line["limit_usd"] + 1e-12, line["scope"]
    assert ctx.db.query("SELECT * FROM budget_reservations WHERE status='held'") == []
    total = sum(float(r["cost_usd"]) for r in calls(ctx, objective_id=oid))
    obj = ctx.db.query_one("SELECT spent_usd FROM budgets WHERE scope='objective'")
    assert obj["spent_usd"] == pytest.approx(total)


# ====================================================================== M2: replans
def _gates(n: int) -> list[dict[str, Any]]:
    steps: list[dict[str, Any]] = [
        {
            "id": "keywords",
            "worker": "extract_keywords",
            "mode": "deterministic",
            "task_type": "job.normalize",
        }
    ]
    for i in range(1, n + 1):
        steps += [
            {
                "id": f"gate{i}",
                "worker": "check_gate",
                "mode": "deterministic",
                "task_type": "eligibility.assess",
                "params": {"pass": False, "reason": f"rule {i}"},
            },
            {
                "id": f"child{i}",
                "worker": "extract_keywords",
                "mode": "deterministic",
                "task_type": "job.normalize",
                "depends_on": [f"gate{i}"],
            },
            {
                "id": f"grandchild{i}",
                "worker": "extract_keywords",
                "mode": "deterministic",
                "task_type": "job.normalize",
                "depends_on": [f"child{i}"],
            },
        ]
    return steps


@pytest.mark.parametrize("concurrency", [1, 3])
def test_m2_multiple_failing_gates_keep_all_skipped_steps(
    ctx: AppContext, concurrency: int
) -> None:
    """Audit reproduction: the second replan used to drop child1 from the skipped list,
    making it 'required' again and the objective end as failed."""
    ctx.config = ctx.config.model_copy(
        update={
            "execution": ctx.config.execution.model_copy(update={"max_concurrency": concurrency})
        }
    )
    m = CareerManager(ctx, playbooks=_book("gates", _gates(3)))
    oid = _submit(m, "gates")
    assert m.run(oid).status is ObjectiveStatus.DONE
    plan = ctx.orchestration.current_plan(oid)
    assert plan is not None
    assert plan.graph["skipped"] == sorted(
        f"{k}{i}" for i in range(1, 4) for k in ("child", "grandchild")
    )
    assert plan.version == 4  # initial + one replan per gate
    st = {t.step: t.status.value for t in ctx.orchestration.list_tasks(oid)}
    assert all(st[f"gate{i}"] == "succeeded" and st[f"child{i}"] == "skipped" for i in (1, 2, 3))


def test_m2_concurrent_skip_steps_do_not_lose_updates(ctx: AppContext, cfg: AppConfig) -> None:
    m = CareerManager(ctx, playbooks=_book("gates", _gates(6)))
    oid = _submit(m, "gates")
    tasks = {t.step: t.id for t in ctx.orchestration.list_tasks(oid)}
    barrier = threading.Barrier(6)

    def prune(i: int) -> None:
        with open_context_at(cfg, ctx.data_dir) as c:
            barrier.wait(timeout=5)
            c.orchestration.skip_steps(
                oid, {tasks[f"child{i}"]: f"child{i}"}, rationale=f"gate {i}", reason=f"rule {i}"
            )

    threads = [threading.Thread(target=prune, args=(i,)) for i in range(1, 7)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    plan = ctx.orchestration.current_plan(oid)
    assert plan is not None and plan.graph["skipped"] == sorted(f"child{i}" for i in range(1, 7))
    assert plan.version == 7


# ====================================================================== M3: completion
def _early_book() -> PlaybookLibrary:
    return _book(
        "early",
        [
            {
                "id": "core",
                "worker": "extract_keywords",
                "mode": "deterministic",
                "task_type": "job.normalize",
            },
            {
                "id": "blocked",
                "worker": "model_step",
                "task_type": "research.company",
                "optional": True,
                "priority": "high",
                "budget_usd": 0.0001,
                "output": {"required": ["summary"]},
            },
            {
                "id": "asks",
                "worker": "model_step",
                "task_type": "job.classify_role",
                "optional": True,
                "max_attempts": 1,
                "output": {"required": ["role_family"]},
            },
        ],
        definition_of_done=["accepted:core"],
    )


def test_m3_early_completion_leaves_a_consistent_final_state(ctx: AppContext) -> None:
    """Audit reproduction: objective 'done' while a task awaited the user and two requests
    were pending; approving one reopened the objective."""
    m = CareerManager(
        ctx,
        adapter=ScriptedAdapter({"asks:job.classify_role": ["malformed"]}),
        playbooks=_early_book(),
    )
    oid = _submit(m, "early")
    assert m.run(oid).status is ObjectiveStatus.DONE
    st = {t.step: t.status.value for t in ctx.orchestration.list_tasks(oid)}
    assert st == {"core": "succeeded", "blocked": "cancelled", "asks": "cancelled"}
    assert m.pending_requests(oid) == []
    reqs = ctx.db.query("SELECT * FROM approval_requests WHERE objective_id=?", (oid,))
    assert len(reqs) == 2 and {r["status"] for r in reqs} == {"withdrawn"}
    for r in reqs:
        with pytest.raises(CareerOSError, match="already withdrawn"):
            m.approve(r["id"], extra_usd=1.0)
    assert ctx.orchestration.get_objective(oid).status is ObjectiveStatus.DONE
    stop = ctx.db.query_one("SELECT inputs FROM manager_decisions WHERE decision='stop'")
    assert from_json(stop["inputs"]) == {
        "status": "done",
        "tasks_cancelled": 2,
        "requests_withdrawn": 2,
    }


def test_m3_approval_rejected_when_task_no_longer_blocked(ctx: AppContext) -> None:
    pb = _book(
        "blk",
        [
            {
                "id": "x",
                "worker": "model_step",
                "task_type": "research.company",
                "priority": "high",
                "budget_usd": 0.0001,
                "output": {"required": ["summary"]},
            }
        ],
    )
    m = CareerManager(ctx, playbooks=pb)
    oid = _submit(m, "blk")
    m.run(oid)
    [req] = m.pending_requests(oid)
    ctx.orchestration.set_task_status(req["task_id"], TaskStatus.READY)  # unblocked elsewhere
    with pytest.raises(CareerOSError, match="obsolete"):
        m.approve(req["id"], extra_usd=1.0)
    row = ctx.db.query_one(
        "SELECT status, resolution FROM approval_requests WHERE id=?", (req["id"],)
    )
    assert row["status"] == "withdrawn" and "no longer blocked_budget" in row["resolution"]
    assert ctx.db.query_one("SELECT COUNT(*) AS n FROM approvals")["n"] == 0


def test_m3_deny_finishes_consistently(ctx: AppContext) -> None:
    m = CareerManager(
        ctx,
        adapter=ScriptedAdapter({"asks:job.classify_role": ["malformed"]}),
        playbooks=_book(
            "deny",
            [
                {
                    "id": "asks",
                    "worker": "model_step",
                    "task_type": "job.classify_role",
                    "max_attempts": 1,
                    "output": {"required": ["role_family"]},
                },
                {
                    "id": "other",
                    "worker": "model_step",
                    "task_type": "research.company",
                    "priority": "high",
                    "budget_usd": 0.0001,
                    "output": {"required": ["summary"]},
                },
            ],
        ),
    )
    oid = _submit(m, "deny")
    m.run(oid)
    first, second = m.pending_requests(oid)
    m.deny(first["id"])
    assert ctx.orchestration.get_objective(oid).status is ObjectiveStatus.STOPPED
    assert m.pending_requests(oid) == []
    with pytest.raises(CareerOSError):
        m.approve(second["id"], extra_usd=1.0)


# ====================================================================== M7: monthly limit
def test_m7_lowered_monthly_limit_applies_immediately(ctx: AppContext) -> None:
    from careeros.domain.models import Objective

    obj = ctx.orchestration.create_objective(
        Objective(kind="t", title="t", definition_of_done=["x"])
    )
    assert obj.id is not None

    def month_limit(cfg: AppConfig) -> float:
        b = BudgetManager(ctx.db, cfg)
        return b.line(b.lines_for(obj.id or 0, None)[0]).limit_usd

    assert month_limit(ctx.config) == pytest.approx(5.0)
    lower = ctx.config.model_copy(
        update={"budgets": ctx.config.budgets.model_copy(update={"simulated_monthly_usd": 1.25})}
    )
    assert month_limit(lower) == pytest.approx(1.25)
    higher = ctx.config.model_copy(
        update={"budgets": ctx.config.budgets.model_copy(update={"simulated_monthly_usd": 50.0})}
    )
    assert month_limit(higher) == pytest.approx(1.25)  # never raised mid-month by config


# ====================================================================== repeated execution
def test_repeated_runs_after_completion_and_recovery_are_no_ops(ctx: AppContext) -> None:
    from careeros.simulation import run_scenario

    oid = run_scenario(ctx, "interrupt").objective_id
    before_calls = len(calls(ctx))
    before_decisions = ctx.db.query_one("SELECT COUNT(*) AS n FROM manager_decisions")["n"]
    for _ in range(3):
        s = CareerManager(ctx).run(oid)
        assert s.status is ObjectiveStatus.DONE and s.attempts_executed == 0
    assert len(calls(ctx)) == before_calls
    assert ctx.db.query_one("SELECT COUNT(*) AS n FROM manager_decisions")["n"] == before_decisions
    for line in ctx.db.query("SELECT * FROM budgets"):
        assert line["spent_usd"] <= line["limit_usd"] + 1e-12
