import pytest

from careeros.context import AppContext
from careeros.core.config import AppConfig
from careeros.routing.complexity import bucket_for, estimate_complexity
from careeros.routing.router import Candidate, Router, _with_expected_costs
from careeros.routing.stats import RoutingStatsRepository


def _router(ctx: AppContext) -> Router:
    return Router(ctx.config, RoutingStatsRepository(ctx.db))


def _cx(cfg: AppConfig, task_type: str, **inp: object):  # type: ignore[no-untyped-def]
    return estimate_complexity(cfg.routing.policy(task_type), dict(inp))


def test_easy_task_routes_to_cheapest_tier(ctx: AppContext) -> None:
    r = _router(ctx).route("job.classify_role", "normal", _cx(ctx.config, "job.classify_role"), 1.0)
    assert r.status == "routed" and r.choice is not None
    assert (r.choice.model_id, r.choice.tier) == ("mock-small", 3)


def test_min_tier_and_capabilities_respected(ctx: AppContext) -> None:
    router = _router(ctx)
    research = router.route("research.company", "normal", _cx(ctx.config, "research.company"), 1.0)
    assert research.choice is not None and research.choice.tier <= 2
    assert all(c.tier <= 2 for c in research.candidates)
    review = router.route("quality.review", "high", _cx(ctx.config, "quality.review"), 1.0)
    assert {c.model_id for c in review.candidates} == {"mock-medium", "mock-large"}


def test_critical_strategic_work_uses_tier_one(ctx: AppContext) -> None:
    r = _router(ctx).route(
        "manager.strategic_review", "critical", _cx(ctx.config, "manager.strategic_review"), 1.0
    )
    assert r.choice is not None and r.choice.tier == 1 and r.choice.effort == "high"


def test_reasoning_effort_selected_per_subtask(ctx: AppContext) -> None:
    router = _router(ctx)
    letter = router.route("cover_letter.draft", "high", _cx(ctx.config, "cover_letter.draft"), 1.0)
    classify = router.route("job.classify_role", "high", _cx(ctx.config, "job.classify_role"), 1.0)
    assert letter.choice is not None and letter.choice.effort == "medium"
    assert classify.choice is not None and classify.choice.effort == "none"


def test_high_complexity_raises_capability(ctx: AppContext) -> None:
    router = _router(ctx)
    easy = router.route("job.analyze", "high", _cx(ctx.config, "job.analyze"), 1.0)
    hard = router.route(
        "job.analyze",
        "high",
        _cx(ctx.config, "job.analyze", ambiguous=True, complexity_hint=0.3),
        1.0,
    )
    assert easy.choice is not None and hard.choice is not None
    assert hard.choice.tier < easy.choice.tier


def test_expected_cost_counts_retries_and_escalation() -> None:
    cheap = Candidate("cheap", 3, "none", 0.5, 0.01)
    strong = Candidate("strong", 1, "low", 0.95, 0.10)
    out = {c.model_id: c for c in _with_expected_costs([cheap, strong])}
    e_strong = 0.10 / 0.95
    assert out["strong"].expected_cost_usd == pytest.approx(e_strong)
    assert out["cheap"].expected_cost_usd == pytest.approx(0.01 + 0.5 * e_strong)


def test_cheaper_call_loses_when_it_usually_fails(ctx: AppContext) -> None:
    """Least expected cost to success, not least per-call cost."""
    stats = RoutingStatsRepository(ctx.db)
    cx = _cx(ctx.config, "job.classify_role")
    for _ in range(30):
        stats.record(
            "job.classify_role",
            cx.bucket,
            "mock-small",
            "none",
            accepted=False,
            prior_alpha=3.9,
            prior_beta=0.1,
            quality=0.3,
            cost_usd=0.0002,
            latency_ms=200,
        )
    r = Router(ctx.config, stats).route("job.classify_role", "normal", cx, 1.0)
    small = next(c for c in r.candidates if c.model_id == "mock-small")
    assert small.p_accept < 0.2 and small.history_attempts == 30
    assert r.choice is not None and r.choice.model_id != "mock-small"
    assert small.est_cost_usd < r.choice.est_cost_usd  # it was cheaper per call


def test_history_raises_confidence_in_a_model(ctx: AppContext) -> None:
    stats = RoutingStatsRepository(ctx.db)
    router = Router(ctx.config, stats)
    cx = _cx(ctx.config, "resume.tailor", complexity_hint=0.25)
    before = next(
        c
        for c in router.candidates("resume.tailor", cx)
        if (c.model_id, c.effort) == ("mock-medium", "low")
    )
    for _ in range(20):
        stats.record(
            "resume.tailor",
            cx.bucket,
            "mock-medium",
            "low",
            accepted=True,
            prior_alpha=2,
            prior_beta=2,
            quality=0.9,
            cost_usd=0.01,
            latency_ms=1,
        )
    after = next(
        c
        for c in router.candidates("resume.tailor", cx)
        if (c.model_id, c.effort) == ("mock-medium", "low")
    )
    assert after.p_accept > before.p_accept


def test_critical_task_over_budget_needs_approval_not_downgrade(ctx: AppContext) -> None:
    r = _router(ctx).route(
        "manager.strategic_review", "critical", _cx(ctx.config, "manager.strategic_review"), 0.001
    )
    assert r.status == "needs_approval" and r.choice is None
    assert r.extra["needed_usd"] > 0.001


def test_noncritical_task_over_budget_is_degraded_explicitly(ctx: AppContext) -> None:
    cx = _cx(ctx.config, "job.analyze", complexity_hint=0.35)  # needs tier 1-2 for floor
    full = _router(ctx).route("job.analyze", "normal", cx, 1.0)
    assert full.choice is not None
    tight = _router(ctx).route("job.analyze", "normal", cx, full.choice.est_cost_usd * 0.5)
    assert tight.status == "degraded"
    assert tight.choice is not None and tight.choice.p_accept < tight.quality_floor
    assert "degraded" in tight.reason.lower()


def test_effort_escalation_keeps_model_and_raises_effort(ctx: AppContext) -> None:
    r = _router(ctx).route(
        "resume.tailor",
        "high",
        _cx(ctx.config, "resume.tailor"),
        1.0,
        current=("mock-medium", "low"),
        escalation="effort",
    )
    assert r.choice is not None and r.choice.model_id == "mock-medium"
    assert r.choice.effort in ("medium", "high")


def test_tier_escalation_moves_to_a_stronger_tier(ctx: AppContext) -> None:
    router = _router(ctx)
    r = router.route(
        "job.classify_role",
        "normal",
        _cx(ctx.config, "job.classify_role"),
        1.0,
        current=("mock-small", "none"),
        escalation="tier",
    )
    assert r.choice is not None and r.choice.tier < 3
    top = router.route(
        "manager.strategic_review",
        "critical",
        _cx(ctx.config, "manager.strategic_review"),
        1.0,
        current=("mock-large", "max"),
        escalation="tier",
    )
    assert top.status == "no_candidate"


def test_review_excludes_author_model(ctx: AppContext) -> None:
    r = _router(ctx).route(
        "quality.review",
        "high",
        _cx(ctx.config, "quality.review"),
        1.0,
        exclude_models=frozenset({"mock-medium"}),
    )
    assert r.choice is not None and r.choice.model_id == "mock-large"


def test_routing_is_deterministic(ctx: AppContext) -> None:
    a = _router(ctx).route("research.company", "high", _cx(ctx.config, "research.company"), 1.0)
    b = _router(ctx).route("research.company", "high", _cx(ctx.config, "research.company"), 1.0)
    assert a.to_dict() == b.to_dict()


def test_complexity_estimate() -> None:
    from careeros.core.config import TaskTypePolicy

    pol = TaskTypePolicy(base_complexity=0.3)
    plain = estimate_complexity(pol, {})
    amb = estimate_complexity(pol, {"ambiguous": True, "text": "x" * 5000}, prior_failures=2)
    assert plain.value == pytest.approx(0.3) and plain.bucket == "low"
    assert amb.value == pytest.approx(0.65) and amb.bucket == "mid"
    assert "prior_failures" in amb.factors
    assert bucket_for(0.9) == "high"
