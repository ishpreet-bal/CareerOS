import pytest

from careeros.context import AppContext
from careeros.cost.budget import BudgetManager
from careeros.domain.models import Objective, Plan, Task
from careeros.llm.adapters.scripted import ScriptedAdapter
from careeros.llm.contracts import BudgetExceededError, ModelRequest, OutputSchema
from careeros.llm.gateway import CallContext
from tests.m2_helpers import calls, manager

REQ = ModelRequest(
    task_type="research.company",
    prompt="Summarize the synthetic employer.",
    output_schema=OutputSchema(("summary",)),
    effort="low",
    simulation={"target_output_tokens": 500},
)


def _objective(ctx: AppContext, budget: float = 1.0, task_budget: float = 0.25) -> tuple[int, int]:
    repo = ctx.orchestration
    obj = repo.create_objective(
        Objective(kind="t", title="t", definition_of_done=["x"], budget_usd=budget)
    )
    assert obj.id is not None
    plan = repo.create_plan(Plan(objective_id=obj.id, version=1, source="test"))
    assert plan.id is not None
    t = repo.add_task(
        Task(
            objective_id=obj.id,
            plan_id=plan.id,
            worker="w",
            task_type="research.company",
            budget_usd=task_budget,
        )
    )
    assert t.id is not None
    return obj.id, t.id


def test_call_is_ledgered_with_estimate_and_simulated_cost(ctx: AppContext) -> None:
    m = manager(ctx, ScriptedAdapter({"*": ["success"]}))
    oid, tid = _objective(ctx)
    out = m.gateway.call("mock-medium", REQ, CallContext(oid, tid, 1))
    assert out.ok and out.cost_usd > 0 and out.est_cost_usd > 0
    [row] = calls(ctx)
    assert row["mode"] == "scripted" and row["cost_is_simulated"] == 1
    assert row["input_tokens"] > 0 and row["output_tokens"] > 0 and row["latency_ms"] > 0
    assert row["est_cost_usd"] == pytest.approx(out.est_cost_usd)
    assert ctx.orchestration.get_task(tid).spent_usd == pytest.approx(out.cost_usd)


def test_budget_reserved_then_settled(ctx: AppContext) -> None:
    m = manager(ctx, ScriptedAdapter({"*": ["success"]}))
    oid, tid = _objective(ctx)
    out = m.gateway.call("mock-medium", REQ, CallContext(oid, tid, 1))
    lines = [m.budgets.line(i) for i in m.budgets.lines_for(oid, tid)]
    assert {ln.scope for ln in lines} == {"global_month", "objective", "task"}
    for ln in lines:
        assert ln.reserved_usd == 0
        assert ln.spent_usd == pytest.approx(out.cost_usd)


@pytest.mark.parametrize("scope_budget", [{"budget": 0.0001}, {"task_budget": 0.0001}])
def test_call_refused_when_any_budget_level_is_exhausted(
    ctx: AppContext, scope_budget: dict[str, float]
) -> None:
    adapter = ScriptedAdapter({"*": ["success"]})
    m = manager(ctx, adapter)
    oid, tid = _objective(ctx, **scope_budget)
    with pytest.raises(BudgetExceededError):
        m.gateway.call("mock-medium", REQ, CallContext(oid, tid, 1))
    assert adapter.calls == [] and calls(ctx) == []  # nothing was called or charged


def test_failed_calls_are_still_accounted(ctx: AppContext) -> None:
    m = manager(ctx, ScriptedAdapter({"*": ["timeout", "malformed"]}))
    oid, tid = _objective(ctx)
    t = m.gateway.call("mock-medium", REQ, CallContext(oid, tid, 1))
    mal = m.gateway.call("mock-medium", REQ, CallContext(oid, tid, 2))
    assert (t.status, mal.status) == ("timeout", "malformed")
    assert t.cost_usd > 0 and mal.cost_usd > 0  # input tokens / partial output are billed
    assert [r["status"] for r in calls(ctx)] == ["timeout", "malformed"]


def test_only_accepted_results_are_cached(ctx: AppContext) -> None:
    adapter = ScriptedAdapter({"*": ["success", "success"]})
    m = manager(ctx, adapter)
    oid, tid = _objective(ctx)
    first = m.gateway.call("mock-medium", REQ, CallContext(oid, tid, 1))
    second = m.gateway.call("mock-medium", REQ, CallContext(oid, tid, 2))
    assert not second.cache_hit  # not accepted yet -> no cache
    m.gateway.mark_accepted(first)
    third = m.gateway.call("mock-medium", REQ, CallContext(oid, tid, 3))
    assert third.cache_hit and third.cost_usd == 0 and third.payload == first.payload
    assert len(adapter.calls) == 2
    assert calls(ctx)[-1]["cache_hit"] == 1


def test_approval_raises_limits(ctx: AppContext) -> None:
    oid, tid = _objective(ctx, budget=0.01)
    b = BudgetManager(ctx.db, ctx.config)
    before = b.available(oid, tid)
    b.raise_limit("objective", str(oid), 0.5)
    assert b.available(oid, tid) == pytest.approx(min(before + 0.5, 0.25))
