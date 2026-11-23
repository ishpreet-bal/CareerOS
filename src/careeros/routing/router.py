"""Model and reasoning-effort selection for one subtask.

For each enabled (model, effort) pair that is capable of the task, the router estimates

* ``p``   — probability the result is accepted, from a Beta posterior: a prior based on
            the tier's configured skill vs. task complexity, updated by historical outcomes;
* ``c``   — estimated cost of one call;
* ``E``   — expected cost *to reach an accepted result*, including retries and escalation:
            ``E_i = c_i + (1 - p_i) * E_next(i)``, where ``next(i)`` is the cheapest-to-success
            option with a higher ``p`` (the escalation target). The strongest option repeats
            until success: ``E_top = c_top / p_top``.

It chooses the minimum ``E`` among candidates with ``p >= quality_floor(priority)`` and
``c <= remaining budget``. If no candidate meeting the floor fits the budget, a critical
task (configurable) returns ``needs_approval`` instead of being downgraded; other tasks may
be explicitly *degraded* and the degradation is recorded.

The router is pure code: it never calls a model to choose a model, and it knows nothing
about specific providers or model names.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any, Literal, cast

from careeros.core.config import EFFORT_ORDER, AppConfig, Effort, ModelProfile, Priority
from careeros.cost.pricing import call_cost_usd
from careeros.routing.complexity import ComplexityEstimate
from careeros.routing.stats import RoutingStatsRepository

RouteStatus = Literal["routed", "degraded", "needs_approval", "no_candidate"]
Escalation = Literal["effort", "tier"]


@dataclass(frozen=True)
class Candidate:
    model_id: str
    tier: int
    effort: Effort
    p_accept: float
    est_cost_usd: float
    expected_cost_usd: float = 0.0
    history_attempts: int = 0
    meets_floor: bool = False
    within_budget: bool = False


@dataclass(frozen=True)
class RouteDecision:
    status: RouteStatus
    choice: Candidate | None
    candidates: list[Candidate]
    task_type: str
    priority: str
    complexity: float
    bucket: str
    quality_floor: float
    budget_available_usd: float
    reason: str
    escalation: Escalation | None = None
    extra: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        d = asdict(self)
        d["candidates"] = [_round(asdict(c)) for c in self.candidates]
        d["choice"] = _round(asdict(self.choice)) if self.choice else None
        return d


def _round(d: dict[str, Any]) -> dict[str, Any]:
    return {k: round(v, 6) if isinstance(v, float) else v for k, v in d.items()}


class Router:
    def __init__(self, config: AppConfig, stats: RoutingStatsRepository) -> None:
        self.config = config
        self.stats = stats

    # ---------------------------------------------------------------- estimates
    def prior_p(self, profile: ModelProfile, effort: Effort, complexity: float) -> float:
        rc = self.config.routing
        skill = rc.tier_skill.get(profile.tier, 0.5) + rc.effort_bonus.get(effort, 0.0)
        return min(0.97, max(0.05, 0.97 - 1.5 * max(0.0, complexity - skill)))

    def posterior_p(
        self, task_type: str, bucket: str, profile: ModelProfile, effort: Effort, complexity: float
    ) -> tuple[float, int, float, float]:
        prior = self.prior_p(profile, effort, complexity)
        k = self.config.routing.prior_strength
        alpha, beta = k * prior, k * (1 - prior)
        row = self.stats.get(task_type, bucket, profile.id, effort)
        attempts = row.attempts if row else 0
        accepts = row.accepts if row else 0
        p = (alpha + accepts) / (alpha + beta + attempts)
        return p, attempts, alpha, beta

    def est_cost(
        self, profile: ModelProfile, task_type: str, effort: Effort, input_tokens: int | None
    ) -> float:
        policy = self.config.routing.policy(task_type)
        mult = self.config.routing.effort_output_multiplier.get(effort, 1.0)
        in_tok = input_tokens if input_tokens is not None else policy.expected_input_tokens
        return call_cost_usd(profile, in_tok, round(policy.expected_output_tokens * mult))

    # ---------------------------------------------------------------- candidates
    def _efforts(self, profile: ModelProfile, default: Effort) -> list[Effort]:
        floor = EFFORT_ORDER.index(default)
        effs = [e for e in profile.effort_levels if EFFORT_ORDER.index(e) >= floor]
        return effs or [profile.max_effort()]

    def candidates(
        self,
        task_type: str,
        complexity: ComplexityEstimate,
        *,
        input_tokens: int | None = None,
        exclude_models: frozenset[str] = frozenset(),
        current: tuple[str, Effort] | None = None,
        escalation: Escalation | None = None,
    ) -> list[Candidate]:
        policy = self.config.routing.policy(task_type)
        profiles = [
            m
            for m in self.config.enabled_models()
            if m.tier <= policy.min_tier
            and all(cap in m.capabilities for cap in policy.required_capabilities)
        ]
        if exclude_models and any(m.id not in exclude_models for m in profiles):
            profiles = [m for m in profiles if m.id not in exclude_models]
        cur_profile = next((m for m in profiles if current and m.id == current[0]), None)
        if current and cur_profile is None:
            cur_profile = next((m for m in self.config.models if m.id == current[0]), None)

        out: list[Candidate] = []
        for m in profiles:
            if escalation == "tier" and cur_profile is not None and m.tier >= cur_profile.tier:
                continue
            if escalation == "effort" and current is not None and m.id != current[0]:
                continue
            for e in self._efforts(m, policy.default_effort):
                if (
                    escalation == "effort"
                    and current is not None
                    and EFFORT_ORDER.index(e) <= EFFORT_ORDER.index(current[1])
                ):
                    continue
                p, n, _, _ = self.posterior_p(task_type, complexity.bucket, m, e, complexity.value)
                out.append(
                    Candidate(
                        m.id,
                        m.tier,
                        e,
                        p,
                        self.est_cost(m, task_type, e, input_tokens),
                        history_attempts=n,
                    )
                )
        return _with_expected_costs(out)

    # ---------------------------------------------------------------- selection
    def route(
        self,
        task_type: str,
        priority: str,
        complexity: ComplexityEstimate,
        budget_available_usd: float,
        *,
        input_tokens: int | None = None,
        exclude_models: frozenset[str] = frozenset(),
        current: tuple[str, Effort] | None = None,
        escalation: Escalation | None = None,
    ) -> RouteDecision:
        rc = self.config.routing
        floor = rc.quality_floors.get(cast(Priority, priority), 0.75)
        cands = self.candidates(
            task_type,
            complexity,
            input_tokens=input_tokens,
            exclude_models=exclude_models,
            current=current,
            escalation=escalation,
        )
        cands = [
            Candidate(
                **{
                    **asdict(c),
                    "meets_floor": c.p_accept >= floor,
                    "within_budget": c.est_cost_usd <= budget_available_usd + 1e-12,
                }
            )
            for c in cands
        ]

        def decision(
            status: RouteStatus, choice: Candidate | None, reason: str, **extra: Any
        ) -> RouteDecision:
            return RouteDecision(
                status,
                choice,
                cands,
                task_type,
                priority,
                complexity.value,
                complexity.bucket,
                floor,
                budget_available_usd,
                reason,
                escalation,
                extra,
            )

        if not cands:
            what = f"{escalation} escalation" if escalation else "this task"
            return decision("no_candidate", None, f"No enabled model can serve {what}.")

        def best(pool: list[Candidate]) -> Candidate:
            return min(
                pool, key=lambda c: (c.expected_cost_usd, c.est_cost_usd, -c.tier, c.model_id)
            )

        feasible = [c for c in cands if c.meets_floor and c.within_budget]
        if feasible:
            return decision(
                "routed",
                best(feasible),
                "Lowest expected cost to an accepted result that meets the floor.",
            )

        meeting = [c for c in cands if c.meets_floor]
        affordable = [c for c in cands if c.within_budget]
        if meeting:  # quality is reachable, but not within budget
            needed = min(c.est_cost_usd for c in meeting)
            if priority in rc.no_degrade_priorities or not affordable:
                return decision(
                    "needs_approval",
                    None,
                    f"Meeting the {priority} quality floor ({floor:.2f}) needs ~${needed:.4f}; "
                    f"only ${budget_available_usd:.4f} is available. Approval required.",
                    needed_usd=needed,
                )
            choice = max(affordable, key=lambda c: (c.p_accept, -c.est_cost_usd))
            return decision(
                "degraded",
                choice,
                f"No in-budget option meets the floor {floor:.2f}; using the best affordable "
                f"option (p={choice.p_accept:.2f}). Recorded as degraded.",
                needed_usd=needed,
            )

        # No model is expected to meet the floor at all: use the strongest option and rely
        # on review and escalation; this is a capability limit, not a budget trade-off.
        if affordable:
            choice = max(affordable, key=lambda c: (c.p_accept, -c.est_cost_usd))
            return decision(
                "routed",
                choice,
                "No option is expected to meet the floor; using the strongest "
                "affordable option. Review decides acceptance.",
            )
        return decision(
            "needs_approval",
            None,
            "No affordable option for this task. Approval required.",
            needed_usd=min(c.est_cost_usd for c in cands),
        )


def _with_expected_costs(cands: list[Candidate]) -> list[Candidate]:
    """Attach E (expected cost to an accepted result, counting retries and escalation)."""
    order = sorted(cands, key=lambda c: (-c.p_accept, c.est_cost_usd))
    expected: dict[int, float] = {}
    best_higher: float | None = None
    i = 0
    while i < len(order):
        # Candidates with equal p are peers: none escalates to another.
        j = i
        while j < len(order) and abs(order[j].p_accept - order[i].p_accept) < 1e-9:
            j += 1
        group_best: float | None = None
        for c in order[i:j]:
            if best_higher is None:
                e = c.est_cost_usd / max(c.p_accept, 1e-6)
            else:
                e = c.est_cost_usd + (1 - c.p_accept) * best_higher
            expected[id(c)] = e
            group_best = e if group_best is None else min(group_best, e)
        assert group_best is not None
        best_higher = group_best if best_higher is None else min(best_higher, group_best)
        i = j
    return [
        Candidate(**{**asdict(c), "expected_cost_usd": expected[id(c)]})
        for c in sorted(cands, key=lambda c: (c.tier, EFFORT_ORDER.index(c.effort)))
    ]
