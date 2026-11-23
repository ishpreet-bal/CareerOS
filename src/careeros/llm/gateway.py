"""The single entry point for model calls.

    safety gate -> idempotency check -> cache lookup -> cost estimate ->
    [T1: budget reservation + 'in_flight' ledger row] -> adapter -> parse ->
    [T2: complete ledger row with tokens/latency/cost/response + settle reservation]

T1 and T2 are each one atomic transaction, so a reservation and its call row always agree,
even with concurrent workers or a crash between them. A request carrying an idempotency key
that already completed returns the stored reply without calling the adapter again.
Every call, including failures, cache hits and abandoned calls, is recorded.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any, Literal

from careeros.cache.store import CacheStore, cache_key
from careeros.core.clock import utc_now_iso
from careeros.core.config import AppConfig, ModelProfile
from careeros.core.db import Database, to_json
from careeros.core.errors import CareerOSError, NotFoundError, SessionTerminatedError
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
    session_id: str | None = None
    #: Identifies one logical request (e.g. task 7, attempt 2, review). Repeats are served
    #: from the ledger instead of calling the model again.
    idempotency_key: str | None = None


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

        if ctx.idempotency_key is not None:
            previous = self._replay(ctx.idempotency_key)
            if previous is not None:
                return previous

        cached = self.cache.get(key)
        if cached is not None:
            payload = json.loads(cached)
            call_id = self._insert_complete(
                profile, request, ctx, mode, key, status="cache_hit", payload=payload
            )
            return CallOutcome(call_id, profile.id, "cache_hit", payload, None, 0.0, 0.0, key)

        est = self.estimate(profile, request)
        lines = self.budgets.lines_for(ctx.objective_id, ctx.task_id, ctx.session_id)
        with self.db.transaction() as c:  # T1
            reservation = self.budgets.reserve_in(  # raises BudgetExceededError
                c, lines, est.cost_usd, session_id=ctx.session_id, task_id=ctx.task_id
            )
            call_id = self._insert_in_flight(
                c, profile, request, ctx, mode, key, est, reservation.id
            )

        response: ModelResponse | None = None
        result_payload: dict[str, Any] | None = None
        error: str | None = None
        in_tok = out_tok = latency = 0
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
        except SessionTerminatedError:
            raise  # leave the call 'in_flight'; recovery accounts for it
        except ModelError as exc:
            in_tok, out_tok, latency = exc.input_tokens, exc.output_tokens, exc.latency_ms
            status = exc.kind if exc.kind in ("timeout", "rate_limit") else "error"  # type: ignore[assignment]
            error = str(exc)
        except Exception as exc:  # an adapter bug must not leak a reservation
            status, error = "error", f"adapter failure: {type(exc).__name__}: {exc}"

        cost = call_cost_usd(profile, in_tok, out_tok)
        stored = {
            "payload": result_payload,
            "simulation": response.simulation if response else {},
        }
        with self.db.transaction() as c:  # T2
            c.execute(
                "UPDATE model_calls SET status=?, error=?, input_tokens=?, output_tokens=?,"
                " cost_usd=?, latency_ms=?, response_json=? WHERE id=?",
                (status, error, in_tok, out_tok, cost, latency, to_json(stored), call_id),
            )
            self.budgets.settle_in(c, reservation.id, cost)
            if ctx.task_id is not None and cost:
                c.execute(
                    "UPDATE tasks SET spent_usd = spent_usd + ? WHERE id=?", (cost, ctx.task_id)
                )
        return CallOutcome(
            call_id, profile.id, status, result_payload, response, est.cost_usd, cost, key, error
        )

    def _replay(self, idempotency_key: str) -> CallOutcome | None:
        r = self.db.query_one(
            "SELECT * FROM model_calls WHERE idempotency_key=?", (idempotency_key,)
        )
        if r is None:
            return None
        if r["status"] == "in_flight":
            raise CareerOSError(
                f"Request {idempotency_key} is already in flight in session {r['session_id']}."
            )
        stored = json.loads(r["response_json"] or "{}")
        response = ModelResponse(
            text="",
            input_tokens=r["input_tokens"] or 0,
            output_tokens=r["output_tokens"] or 0,
            latency_ms=r["latency_ms"] or 0,
            simulation=stored.get("simulation") or {},
        )
        return CallOutcome(
            r["id"],
            r["model_id"],
            r["status"],
            stored.get("payload"),
            response,
            r["est_cost_usd"] or 0.0,
            0.0,
            r["cache_key"],
            r["error"],
        )  # cost 0: already accounted when the call was made

    def abandon_in_flight(self, stale_sessions: list[str]) -> int:
        """Settle calls left 'in_flight' by dead sessions.

        The provider may or may not have done (and billed) the work, so the estimated cost
        is charged conservatively, and the call is marked 'abandoned' so the task can make
        a fresh call.
        """
        if not stale_sessions:
            return 0
        marks = ",".join("?" * len(stale_sessions))
        rows = self.db.query(
            f"SELECT * FROM model_calls WHERE status='in_flight' AND session_id IN ({marks})",  # noqa: S608
            tuple(stale_sessions),
        )
        for r in rows:
            est = float(r["est_cost_usd"] or 0.0)
            with self.db.transaction() as c:
                c.execute(
                    "UPDATE model_calls SET status='abandoned', cost_usd=?, error=?,"
                    " idempotency_key = CASE WHEN idempotency_key IS NULL THEN NULL"
                    "   ELSE idempotency_key || ':abandoned:' || id END WHERE id=?",
                    (est, "session ended while the call was in flight", r["id"]),
                )
                if r["reservation_id"] is not None:
                    self.budgets.settle_in(c, r["reservation_id"], est)
                if r["task_id"] is not None and est:
                    c.execute(
                        "UPDATE tasks SET spent_usd = spent_usd + ? WHERE id=?", (est, r["task_id"])
                    )
        return len(rows)

    def mark_accepted(self, outcome: CallOutcome) -> None:
        """Cache a reviewed-and-accepted result so identical future calls cost nothing."""
        if outcome.payload is not None and not outcome.cache_hit:
            self.cache.put(outcome.cache_key, to_json(outcome.payload))

    # ---------------------------------------------------------------- ledger
    _COLUMNS = (
        "task_id, attempt, objective_id, purpose, session_id, idempotency_key, model_id,"
        " provider, tier, effort, mode, template_id, template_version, est_input_tokens,"
        " est_output_tokens, input_tokens, output_tokens, cached_tokens, est_cost_usd,"
        " cost_usd, cost_is_simulated, latency_ms, status, error, cache_key, cache_hit,"
        " reservation_id, response_json, created_at"
    )

    def _values(
        self,
        profile: ModelProfile,
        request: ModelRequest,
        ctx: CallContext,
        mode: str,
        key: str,
        *,
        status: str,
        est: CostEstimate | None,
        reservation_id: int | None,
        cache_hit: bool,
        response_json: str | None,
    ) -> tuple[Any, ...]:
        return (
            ctx.task_id,
            ctx.attempt,
            ctx.objective_id,
            ctx.purpose,
            ctx.session_id,
            ctx.idempotency_key,
            profile.id,
            profile.provider,
            profile.tier,
            request.effort,
            mode,
            request.task_type,
            "1",
            est.input_tokens if est else None,
            est.output_tokens if est else None,
            0,
            0,
            0,
            est.cost_usd if est else 0.0,
            0.0,
            1,  # every M2/M3 cost is simulated
            0,
            status,
            None,
            key,
            int(cache_hit),
            reservation_id,
            response_json,
            utc_now_iso(),
        )

    def _insert_in_flight(
        self,
        c: Any,
        profile: ModelProfile,
        request: ModelRequest,
        ctx: CallContext,
        mode: str,
        key: str,
        est: CostEstimate,
        reservation_id: int,
    ) -> int:
        vals = self._values(
            profile,
            request,
            ctx,
            mode,
            key,
            status="in_flight",
            est=est,
            reservation_id=reservation_id,
            cache_hit=False,
            response_json=None,
        )
        cur = c.execute(
            f"INSERT INTO model_calls ({self._COLUMNS}) VALUES ({','.join('?' * len(vals))})",  # noqa: S608
            vals,
        )
        assert cur.lastrowid is not None
        return int(cur.lastrowid)

    def _insert_complete(
        self,
        profile: ModelProfile,
        request: ModelRequest,
        ctx: CallContext,
        mode: str,
        key: str,
        *,
        status: str,
        payload: dict[str, Any],
    ) -> int:
        vals = self._values(
            profile,
            request,
            ctx,
            mode,
            key,
            status=status,
            est=None,
            reservation_id=None,
            cache_hit=True,
            response_json=to_json({"payload": payload, "simulation": {}}),
        )
        with self.db.transaction() as c:
            cur = c.execute(
                f"INSERT INTO model_calls ({self._COLUMNS}) VALUES ({','.join('?' * len(vals))})",  # noqa: S608
                vals,
            )
        assert cur.lastrowid is not None
        return int(cur.lastrowid)
