import pytest

from careeros.context import AppContext
from careeros.core.db import from_json
from careeros.core.errors import CareerOSError
from careeros.cost.report import build_report
from careeros.domain.models import ObjectiveStatus, TaskStatus
from careeros.llm.adapters.scripted import ScriptedAdapter
from careeros.manager.manager import ObjectiveRequest
from careeros.manager.plans import Playbook, PlaybookLibrary, waves
from careeros.simulation import ESCALATION_SCRIPT, SimulatedCrashError, later, run_scenario
from tests.m2_helpers import DEMO, calls, decisions, manager, task_by_step


# ------------------------------------------------------------------ understanding / planning
def test_familiar_objective_uses_playbook_without_model_calls(ctx: AppContext) -> None:
    m = manager(ctx)
    oid = m.submit(
        ObjectiveRequest(
            title="please triage and classify this job posting",
            inputs={"text": "Embedded C firmware intern"},
        )
    )
    assert ctx.orchestration.get_objective(oid).kind == "job_triage"
    assert calls(ctx) == []  # understanding and planning were deterministic
    assert "keyword match" in m.events[0]


def test_decomposition_records_dependencies_and_parallel_waves(ctx: AppContext) -> None:
    m = manager(ctx)
    oid = m.submit(DEMO)
    plan = ctx.orchestration.current_plan(oid)
    assert plan is not None and plan.source == "playbook:demo_application_package"
    assert plan.graph["waves"][0] == ["extract_keywords", "research_company"]
    assert ["draft_cover_letter", "tailor_resume"] in plan.graph["waves"]
    tailor = task_by_step(ctx, oid, "tailor_resume")
    deps = {
        ctx.orchestration.get_task(d).step
        for d, _ in ctx.orchestration.dependencies(int(tailor["id"]))
    }  # type: ignore[arg-type]
    assert deps == {"classify_role", "research_company"}
    assert tailor["important"] == 1


def test_unfamiliar_objective_gets_model_assisted_plan(ctx: AppContext) -> None:
    m = manager(ctx)
    oid = m.submit(
        ObjectiveRequest(
            title="Summarize my club notes",
            inputs={"text": "Robotics club notes about sensors and motors"},
        )
    )
    plan = ctx.orchestration.current_plan(oid)
    assert plan is not None and plan.source == "model"
    [planning] = calls(ctx, purpose="planning")
    assert planning["tier"] == 1 and planning["task_id"] is None
    assert m.run(oid).status is ObjectiveStatus.DONE


def test_invalid_model_plan_is_escalated_then_user_asked(ctx: AppContext) -> None:
    m = manager(ctx, ScriptedAdapter({"manager.plan_novel": ["fail_quality"] * 5}))
    oid = m.submit(ObjectiveRequest(title="Do something unusual", inputs={}))
    obj = ctx.orchestration.get_objective(oid)
    assert obj.status is ObjectiveStatus.AWAITING_HUMAN
    assert [r["kind"] for r in m.pending_requests(oid)] == ["clarification"]
    assert len(calls(ctx, purpose="planning")) <= ctx.config.limits.max_attempts


def test_ambiguous_keywords_do_not_guess(ctx: AppContext) -> None:
    lib = PlaybookLibrary(
        [
            Playbook.model_validate(
                {
                    "name": n,
                    "keywords": ["job", "role"],
                    "plan": {
                        "steps": [
                            {
                                "id": "a",
                                "worker": "extract_keywords",
                                "mode": "deterministic",
                                "task_type": "job.normalize",
                            }
                        ]
                    },
                }
            )
            for n in ("one", "two")
        ]
    )
    book, why = lib.match(None, "a job role thing")
    assert book is None and why.startswith("ambiguous")


def test_plan_validation_rejects_cycles_and_unknown_steps() -> None:
    from careeros.manager.plans import PlanSpec

    with pytest.raises(ValueError, match="cycle"):
        PlanSpec.model_validate(
            {
                "steps": [
                    {"id": "a", "worker": "w", "task_type": "t", "depends_on": ["b"]},
                    {"id": "b", "worker": "w", "task_type": "t", "depends_on": ["a"]},
                ]
            }
        )
    with pytest.raises(ValueError, match="unknown step"):
        PlanSpec.model_validate(
            {"steps": [{"id": "a", "worker": "w", "task_type": "t", "depends_on": ["zzz"]}]}
        )
    spec = PlanSpec.model_validate(
        {
            "steps": [
                {"id": "a", "worker": "w", "task_type": "t"},
                {"id": "b", "worker": "w", "task_type": "t"},
                {"id": "c", "worker": "w", "task_type": "t", "depends_on": ["a", "b"]},
            ]
        }
    )
    assert waves(spec) == [["a", "b"], ["c"]]


# ------------------------------------------------------------------ execution
def test_standard_run_routes_subtasks_to_different_tiers(ctx: AppContext) -> None:
    result = run_scenario(ctx, "standard")
    assert ctx.orchestration.get_objective(result.objective_id).status is ObjectiveStatus.DONE
    rows = calls(ctx, purpose="task")
    tier_of = {
        task_by_step(ctx, result.objective_id, s)["id"]: s
        for s in ("research_company", "classify_role", "tailor_resume", "strategic_review")
    }
    by_step = {tier_of[r["task_id"]]: r["tier"] for r in rows if r["task_id"] in tier_of}
    assert by_step == {
        "research_company": 2,
        "classify_role": 3,
        "tailor_resume": 2,
        "strategic_review": 1,
    }
    # Deterministic steps made no model calls.
    for step in ("extract_keywords", "assemble"):
        t = task_by_step(ctx, result.objective_id, step)
        assert t["status"] == "succeeded" and calls(ctx, task_id=t["id"]) == []


def test_important_deliverables_get_independent_review(ctx: AppContext) -> None:
    result = run_scenario(ctx, "standard")
    for step in ("tailor_resume", "draft_cover_letter"):
        t = task_by_step(ctx, result.objective_id, step)
        [review] = calls(ctx, task_id=t["id"], purpose="review")
        [author] = calls(ctx, task_id=t["id"], purpose="task")
        assert review["model_id"] != author["model_id"]
        assert (
            ctx.db.query_one("SELECT verdict FROM reviews WHERE task_id=?", (t["id"],))["verdict"]
            == "accept"
        )
    t = task_by_step(ctx, result.objective_id, "classify_role")
    assert calls(ctx, task_id=t["id"], purpose="review") == []  # routine: no model review


def test_failures_retry_and_escalate_then_complete(ctx: AppContext) -> None:
    result = run_scenario(ctx, "escalation")
    oid = result.objective_id
    assert ctx.orchestration.get_objective(oid).status is ObjectiveStatus.DONE
    research = task_by_step(ctx, oid, "research_company")
    assert decisions(ctx, research["id"]).count("retry_same") == 1
    classify = task_by_step(ctx, oid, "classify_role")
    assert [d for d in decisions(ctx, classify["id"]) if d != "route"] == [
        "retry_with_feedback",
        "escalate_tier",
        "accept",
    ]
    tiers = [r["tier"] for r in calls(ctx, task_id=classify["id"])]
    assert tiers == [3, 3, 2]
    tailor = task_by_step(ctx, oid, "tailor_resume")
    efforts = [r["effort"] for r in calls(ctx, task_id=tailor["id"], purpose="task")]
    assert efforts == ["low", "medium"] and tailor["escalations"] == 1
    letter = task_by_step(ctx, oid, "draft_cover_letter")
    assert "escalate_effort" in decisions(ctx, letter["id"])


def test_retries_are_bounded_and_then_the_user_is_asked(ctx: AppContext) -> None:
    script = {"job.classify_role": ["malformed"] * 10}
    m = manager(ctx, ScriptedAdapter(script))
    oid = m.submit(
        ObjectiveRequest(
            title="triage",
            kind="job_triage",
            inputs={"text": "Embedded firmware intern writing C drivers"},
        )
    )
    summary = m.run(oid)
    t = task_by_step(ctx, oid, "classify_role")
    assert summary.status is ObjectiveStatus.AWAITING_HUMAN
    assert t["status"] == "awaiting_human"
    assert len(calls(ctx, task_id=t["id"])) == ctx.config.limits.max_attempts
    [req] = m.pending_requests(oid)
    assert req["kind"] == "retry_limit"
    # The user may grant one more attempt; it then runs once more.
    m.approve(req["id"])
    m.run(oid)
    assert len(calls(ctx, task_id=t["id"])) == ctx.config.limits.max_attempts + 1


def test_critical_task_is_not_downgraded_to_fit_budget(ctx: AppContext) -> None:
    result = run_scenario(ctx, "budget")
    oid = result.objective_id
    [asked] = result.approvals
    assert asked["kind"] == "budget_increase"
    t = task_by_step(ctx, oid, "strategic_review")
    assert asked["task_id"] == t["id"]
    assert "critical" in asked["details"]["reason"]
    # It ran only after approval, and on the tier its quality floor requires.
    [call] = calls(ctx, task_id=t["id"])
    assert call["tier"] == 1
    approved_at = ctx.db.query_one(
        "SELECT resolved_at FROM approval_requests WHERE id=?", (asked["id"],)
    )["resolved_at"]
    assert call["created_at"] >= approved_at
    assert ctx.orchestration.get_objective(oid).status is ObjectiveStatus.DONE
    assert ctx.db.query_one("SELECT COUNT(*) AS n FROM approvals")["n"] == 1


def test_approve_requires_positive_amount_and_pending_state(ctx: AppContext) -> None:
    m = manager(ctx)
    oid = m.submit(ObjectiveRequest(**{**DEMO.__dict__, "budget_usd": 0.15}))
    m.run(oid)
    [req] = m.pending_requests(oid)
    with pytest.raises(CareerOSError):
        m.approve(req["id"], extra_usd=0)
    m.deny(req["id"])
    assert ctx.orchestration.get_objective(oid).status is ObjectiveStatus.STOPPED
    with pytest.raises(CareerOSError):
        m.approve(req["id"], extra_usd=1)


def test_replan_prunes_dependents_and_keeps_completed_work(ctx: AppContext) -> None:
    book = Playbook.model_validate(
        {
            "name": "gated",
            "definition_of_done": ["accepted:all_required"],
            "plan": {
                "steps": [
                    {
                        "id": "keywords",
                        "worker": "extract_keywords",
                        "mode": "deterministic",
                        "task_type": "job.normalize",
                    },
                    {
                        "id": "gate",
                        "worker": "check_gate",
                        "mode": "deterministic",
                        "task_type": "eligibility.assess",
                        "depends_on": ["keywords"],
                        "params": {"pass": False, "reason": "synthetic eligibility rule not met"},
                    },
                    {
                        "id": "draft",
                        "worker": "model_step",
                        "task_type": "cover_letter.draft",
                        "depends_on": ["gate"],
                        "output": {"required": ["letter"]},
                    },
                ]
            },
        }
    )
    m = manager(ctx, playbooks=PlaybookLibrary([book]))
    oid = m.submit(
        ObjectiveRequest(
            title="gated", kind="gated", inputs={"text": "Robotics intern building sensor firmware"}
        )
    )
    m.run(oid)
    assert task_by_step(ctx, oid, "draft")["status"] == "skipped"
    assert task_by_step(ctx, oid, "keywords")["status"] == "succeeded"
    plan = ctx.orchestration.current_plan(oid)
    assert plan is not None and plan.version == 2 and plan.graph["skipped"] == ["draft"]
    assert calls(ctx) == []  # no money spent on pruned work
    assert ctx.orchestration.get_objective(oid).status is ObjectiveStatus.DONE


# ------------------------------------------------------------------ persistence / recovery
def test_interrupted_objective_resumes_without_duplicate_work(ctx: AppContext) -> None:
    result = run_scenario(ctx, "interrupt")
    oid = result.objective_id
    assert ctx.orchestration.get_objective(oid).status is ObjectiveStatus.DONE
    assert any("SESSION TERMINATED" in e for e in result.events)
    for step in ("research_company", "classify_role", "tailor_resume"):
        t = task_by_step(ctx, oid, step)
        assert len(calls(ctx, task_id=t["id"], purpose="task")) == 1, step
    sessions = ctx.db.query("SELECT * FROM execution_sessions ORDER BY rowid")
    assert len(sessions) == 2
    assert sessions[0]["end_reason"] == "presumed dead (recovered)"  # crashed, then taken over


def test_crash_before_checkpoint_reroutes_without_losing_attempt_budget(ctx: AppContext) -> None:
    crashed: list[bool] = []

    def crash(point: str, task: object) -> None:
        if not crashed:
            crashed.append(True)
            raise SimulatedCrashError("boom")

    m = manager(ctx, fault_hook=crash)
    oid = m.submit(DEMO)
    with pytest.raises(SimulatedCrashError):
        m.run(oid)
    m2 = manager(ctx, clock=later(10))  # after the crashed session's leases expired
    assert m2.run(oid).status is ObjectiveStatus.DONE
    t = task_by_step(ctx, oid, "research_company")
    assert t["attempt"] == 1  # the interrupted attempt was resumed, not counted twice


def test_session_limit_then_resume(ctx: AppContext) -> None:
    m = manager(ctx)
    oid = m.submit(DEMO)
    first = m.run(oid, max_attempts=2)
    assert first.status is ObjectiveStatus.ACTIVE and first.attempts_executed == 2
    outstanding = ctx.orchestration.outstanding_tasks(oid)
    assert outstanding
    assert manager(ctx).run(oid).status is ObjectiveStatus.DONE


def test_everything_is_persisted(ctx: AppContext) -> None:
    result = run_scenario(ctx, "escalation")
    oid = result.objective_id
    routes = ctx.db.query(
        "SELECT inputs FROM manager_decisions WHERE objective_id=? AND decision='route'", (oid,)
    )
    assert routes
    first = from_json(routes[0]["inputs"])
    assert {"candidates", "choice", "quality_floor", "complexity", "bucket"} <= set(first)
    assert all(
        {"p_accept", "est_cost_usd", "expected_cost_usd"} <= set(c) for c in first["candidates"]
    )
    model_tasks = ctx.db.query("SELECT id FROM tasks WHERE objective_id=? AND mode='model'", (oid,))
    for t in model_tasks:
        assert ctx.db.query("SELECT 1 FROM task_checkpoints WHERE task_id=?", (t["id"],))
        assert ctx.db.query("SELECT 1 FROM task_results WHERE task_id=?", (t["id"],))
    kinds = {
        r["decision"]
        for r in ctx.db.query(
            "SELECT DISTINCT decision FROM manager_decisions WHERE objective_id=?", (oid,)
        )
    }
    assert {
        "plan",
        "route",
        "accept",
        "retry_same",
        "retry_with_feedback",
        "escalate_tier",
        "escalate_effort",
        "stop",
    } <= kinds
    assert ctx.db.query("SELECT COUNT(*) AS n FROM routing_stats")[0]["n"] > 0


def test_repeat_objective_reuses_cached_results_at_zero_cost(ctx: AppContext) -> None:
    run_scenario(ctx, "standard")
    second = run_scenario(ctx, "standard")
    rep = build_report(ctx.db, second.objective_id)
    assert rep.totals["total_cost_usd"] == 0
    assert rep.totals["cache_hits"] == 5
    assert rep.status == "done"


# ------------------------------------------------------------------ cost accounting
def test_quality_adjusted_cost_includes_failed_attempts(ctx: AppContext) -> None:
    result = run_scenario(ctx, "escalation")
    rep = build_report(ctx.db, result.objective_id)
    t = rep.totals
    assert t["total_cost_usd"] == pytest.approx(sum(c.cost_usd for c in rep.calls))
    assert t["total_cost_usd"] == pytest.approx(
        t["task_cost_usd"] + t["review_cost_usd"] + t["planning_cost_usd"]
    )
    assert t["wasted_cost_usd"] > 0
    assert t["cost_per_success_usd"] == pytest.approx(t["total_cost_usd"] / t["tasks_succeeded"])
    assert t["quality_adjusted_cost_per_success_usd"] == pytest.approx(
        t["cost_per_success_usd"] / t["mean_accepted_quality"]
    )
    assert t["escalations"] == 3 and t["retries"] == 2
    classify = next(k for k in rep.tasks if k.step == "classify_role")
    assert classify.wasted_usd > 0 and classify.accepted_attempt == 3
    assert classify.cost_usd > classify.wasted_usd
    assert all(c.mode == "scripted" for c in rep.calls)


def test_task_and_objective_budgets_tracked(ctx: AppContext) -> None:
    result = run_scenario(ctx, "standard")
    oid = result.objective_id
    obj_line = ctx.db.query_one(
        "SELECT * FROM budgets WHERE scope='objective' AND scope_ref=?", (str(oid),)
    )
    total = sum(float(r["cost_usd"]) for r in calls(ctx, objective_id=oid))
    assert obj_line["spent_usd"] == pytest.approx(total) and obj_line["reserved_usd"] == 0
    for t in ctx.orchestration.list_tasks(oid):
        line = ctx.db.query_one(
            "SELECT * FROM budgets WHERE scope='task' AND scope_ref=?", (str(t.id),)
        )
        if t.status is TaskStatus.SUCCEEDED and calls(ctx, task_id=t.id):
            assert line["spent_usd"] == pytest.approx(t.spent_usd)
            assert line["spent_usd"] <= line["limit_usd"]


def test_escalation_script_is_the_documented_one() -> None:
    assert ESCALATION_SCRIPT["classify_role:job.classify_role@mock-small"] == [
        "malformed",
        "invalid",
    ]
