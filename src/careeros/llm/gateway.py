"""The single entry point for model calls.

    safety gate -> cache lookup -> cost estimate -> budget reservation -> adapter ->
    parse -> ledger (tokens, latency, estimated and simulated cost) -> budget settlement

Every call, including failures and cache hits, is recorded in ``model_calls``.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any, Literal

from careeros.cache.store import CacheStore, cache_key
from careeros.core.clock import utc_now_iso
from careeros.core.config import AppConfig, ModelProfile
from careeros.core.db import Database, to_json
from careeros.core.errors import NotFoundError
from careeros.cost.budget import BudgetManager
from careeros.cost.pricing import call_cost_usd
from careeros.llm.contracts import ModelError, ModelRequest, ModelResponse
from careeros.llm.registry import AdapterRegistry
from careeros.llm.safety import check_call_allowed
from careeros.llm.tokens import estimate_tokens

Purpose = Literal["task", "review", "planning"]
CallStatus = Literal["ok", "malformed", "timeout", "rate_limit", "error", "cache_hit"]


@dataclass(frozen=True)
class CallContext:
    objective_id: int
    task_id: int | None = None
    attempt: int | None = None
    purpose: Purpose = "task"


@dataclass(frozen=True)
class CostEstimate:
    input_tokens: int
    output_tokens: int
    cost_usd: float


@dataclass(frozen=True)
class CallOutcome:
    call_id: int
    model_id: str
    status: CallStatus
    payload: dict[str, Any] | None
    response: ModelResponse | None
    est_cost_usd: float
    cost_usd: float
    cache_key: str
    error: str | None = None

    @property
    def ok(self) -> bool:
        return self.status in ("ok", "cache_hit") and self.payload is not None

    @property
    def cache_hit(self) -> bool:
        return self.status == "cache_hit"


class ModelGateway:
    def __init__(
        self,
        config: AppConfig,
        db: Database,
        registry: AdapterRegistry,
        budgets: BudgetManager,
        cache: CacheStore,
    ) -> None:
        self.config = config
        self.db = db
        self.registry = registry
        self.budgets = budgets
        self.cache = cache
        self._profiles = {m.id: m for m in config.models}
        self._sync_profiles()

    # ---------------------------------------------------------------- profiles
    def _sync_profiles(self) -> None:
        now = utc_now_iso()
        with self.db.transaction() as c:
            for m in self.config.models:
                c.execute(
                    "INSERT INTO model_profiles (id, provider, tier, capabilities, context_window,"
                    " effort_levels, input_per_mtok, output_per_mtok, pricing_simulated,"
                    " pricing_verified_at, enabled, updated_at) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)"
                    " ON CONFLICT(id) DO UPDATE SET provider=excluded.provider,"
                    " tier=excluded.tier, capabilities=excluded.capabilities,"
                    " context_window=excluded.context_window,"
                    " effort_levels=excluded.effort_levels,"
                    " input_per_mtok=excluded.input_per_mtok,"
                    " output_per_mtok=excluded.output_per_mtok,"
                    " pricing_simulated=excluded.pricing_simulated,"
                    " pricing_verified_at=excluded.pricing_verified_at,"
                    " enabled=excluded.enabled, updated_at=excluded.updated_at",
                    (
                        m.id,
                        m.provider,
                        m.tier,
                        to_json(list(m.capabilities)),
                        m.context_window,
                        to_json(list(m.effort_levels)),
                        m.pricing.input_per_mtok,
                        m.pricing.output_per_mtok,
                        int(m.pricing.simulated),
                        m.pricing.verified_at,
                        int(m.enabled),
                        now,
                    ),
                )

    def profile(self, model_id: str) -> ModelProfile:
        try:
            return self._profiles[model_id]
        except KeyError:
            raise NotFoundError(f"Unknown model profile {model_id!r}") from None

    # ---------------------------------------------------------------- estimates
    def estimate(self, profile: ModelProfile, request: ModelRequest) -> CostEstimate:
        policy = self.config.routing.policy(request.task_type)
        in_tok = estimate_tokens((request.system or "") + request.prompt)
        mult = self.config.routing.effort_output_multiplier.get(request.effort, 1.0)
        out_tok = min(request.max_output_tokens, round(policy.expected_output_tokens * mult))
        return CostEstimate(in_tok, out_tok, call_cost_usd(profile, in_tok, out_tok))

    # ---------------------------------------------------------------- calls
    def call(self, model_id: str, request: ModelRequest, ctx: CallContext) -> CallOutcome:
        profile = self.profile(model_id)
        adapter = self.registry.for_profile(profile)
        check_call_allowed(self.config, profile, adapter)
        mode = getattr(adapter, "mode_label", "mock")
        key = cache_key(profile.id, request)

        cached = self.cache.get(key)
        if cached is not None:
            payload = json.loads(cached)
            call_id = self._record(
                profile,
                request,
                ctx,
                mode,
                key,
                status="cache_hit",
                est=None,
                in_tok=0,
                out_tok=0,
                cost=0.0,
                latency=0,
                cache_hit=True,
            )
            return CallOutcome(call_id, profile.id, "cache_hit", payload, None, 0.0, 0.0, key)

        est = self.estimate(profile, request)
        lines = self.budgets.lines_for(ctx.objective_id, ctx.task_id)
        reservation = self.budgets.reserve(lines, est.cost_usd)  # raises BudgetExceededError

        response: ModelResponse | None = None
        result_payload: dict[str, Any] | None = None
        error: str | None = None
        try:
            response = adapter.call(profile, request)
            in_tok, out_tok, latency = (
                response.input_tokens,
                response.output_tokens,
                response.latency_ms,
            )
            try:
                parsed = json.loads(response.text)
                if not isinstance(parsed, dict):
                    raise ValueError("top-level JSON value is not an object")
                result_payload = parsed
                status: CallStatus = "ok"
            except ValueError as exc:
                status, error = "malformed", f"unparseable response: {exc}"
        except ModelError as exc:
            in_tok, out_tok, latency = exc.input_tokens, exc.output_tokens, exc.latency_ms
            status = exc.kind if exc.kind in ("timeout", "rate_limit") else "error"  # type: ignore[assignment]
            error = str(exc)

        cost = call_cost_usd(profile, in_tok, out_tok)
        self.budgets.settle(reservation, cost)
        call_id = self._record(
            profile,
            request,
            ctx,
            mode,
            key,
            status=status,
            est=est,
            in_tok=in_tok,
            out_tok=out_tok,
            cost=cost,
            latency=latency,
            cache_hit=False,
            error=error,
        )
        return CallOutcome(
            call_id, profile.id, status, result_payload, response, est.cost_usd, cost, key, error
        )

    def mark_accepted(self, outcome: CallOutcome) -> None:
        """Cache a reviewed-and-accepted result so identical future calls cost nothing."""
        if outcome.payload is not None and not outcome.cache_hit:
            self.cache.put(outcome.cache_key, to_json(outcome.payload))

    # ---------------------------------------------------------------- ledger
    def _record(
        self,
        profile: ModelProfile,
        request: ModelRequest,
        ctx: CallContext,
        mode: str,
        key: str,
        *,
        status: str,
        est: CostEstimate | None,
        in_tok: int,
        out_tok: int,
        cost: float,
        latency: int,
        cache_hit: bool,
        error: str | None = None,
    ) -> int:
        with self.db.transaction() as c:
            cur = c.execute(
                "INSERT INTO model_calls (task_id, attempt, objective_id, purpose, model_id,"
                " provider, tier, effort, mode, template_id, template_version, est_input_tokens,"
                " est_output_tokens, input_tokens, output_tokens, cached_tokens, est_cost_usd,"
                " cost_usd, cost_is_simulated, latency_ms, status, error, cache_key, cache_hit,"
                " created_at) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (
                    ctx.task_id,
                    ctx.attempt,
                    ctx.objective_id,
                    ctx.purpose,
                    profile.id,
                    profile.provider,
                    profile.tier,
                    request.effort,
                    mode,
                    request.task_type,
                    "1",
                    est.input_tokens if est else None,
                    est.output_tokens if est else None,
                    in_tok,
                    out_tok,
                    0,
                    est.cost_usd if est else 0.0,
                    cost,
                    1,  # every M2 cost is simulated
                    latency,
                    status,
                    error,
                    key,
                    int(cache_hit),
                    utc_now_iso(),
                ),
            )
            if ctx.task_id is not None and cost:
                c.execute(
                    "UPDATE tasks SET spent_usd = spent_usd + ? WHERE id=?", (cost, ctx.task_id)
                )
        assert cur.lastrowid is not None
        return cur.lastrowid
