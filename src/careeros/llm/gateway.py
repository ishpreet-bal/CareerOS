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

import hashlib
import json
from collections.abc import Callable
from dataclasses import dataclass, replace
from datetime import datetime
from typing import Any, Literal

from careeros.cache.store import CacheStore, cache_key
from careeros.core.clock import utc_now, utc_now_iso
from careeros.core.config import AppConfig, ModelProfile
from careeros.core.db import Database, to_json
from careeros.core.errors import CareerOSError, NotFoundError, SessionTerminatedError
from careeros.cost.budget import BudgetManager
from careeros.cost.pricing import (
    call_cost_usd,
    expected_output_tokens,
    output_cap_tokens,
    reservation_usd,
)
from careeros.llm.contracts import (
    IdempotencyMismatchError,
    ModelError,
    ModelRequest,
    ModelResponse,
)
from careeros.llm.registry import AdapterRegistry
from careeros.llm.safety import check_call_allowed
from careeros.llm.tokens import estimate_tokens
from careeros.orchestration.fencing import Fence, check_fence, fence_valid

Purpose = Literal["task", "review", "planning"]


def request_hash(request: ModelRequest) -> str:
    """Fingerprint of a logical request; replay must match it (audit L1)."""
    return hashlib.sha256(
        to_json(
            {
                "task_type": request.task_type,
                "system": request.system,
                "prompt": request.prompt,
                "schema": request.output_schema.to_dict(),
            }
        ).encode()
    ).hexdigest()[:32]


@dataclass(frozen=True)
class Reply:
    """A model reply reduced to what the ledger records."""

    status: str
    error: str | None
    input_tokens: int
    output_tokens: int
    latency_ms: int
    cost_usd: float
    response_json: str
    provider_request_id: str | None = None


CallStatus = Literal["ok", "malformed", "timeout", "rate_limit", "error", "cache_hit", "late"]


@dataclass(frozen=True)
class CallContext:
    objective_id: int
    task_id: int | None = None
    attempt: int | None = None
    purpose: Purpose = "task"
    session_id: str | None = None
    #: Identifies one *logical* model call: task, attempt, purpose and a stable sequence
    #: number (e.g. ``task:7:a2:review:0``). Repeats are served from the ledger instead of
    #: calling the model again - after checking that model, effort and request match.
    idempotency_key: str | None = None
    #: The caller's fencing token. Calls for an owned task are recorded and settled only
    #: while the token is still the task's live owner (Phase B).
    fence: Fence | None = None


@dataclass(frozen=True)
class CostEstimate:
    input_tokens: int
    output_tokens: int  # expected
    cost_usd: float  # expected
    output_cap: int = 0  # hard limit sent to the provider
    reserve_usd: float = 0.0  # worst case at the cap: what is reserved


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
    #: Settled cost above the reservation (0 when the provider honoured the cap).
    overshoot_usd: float = 0.0

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
        clock: Callable[[], datetime] = utc_now,
    ) -> None:
        self.clock = clock
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
        in_tok = estimate_tokens((request.system or "") + request.prompt)
        out_tok = min(
            request.max_output_tokens,
            expected_output_tokens(self.config, request.task_type, request.effort),
        )
        cap = output_cap_tokens(
            self.config, request.task_type, request.effort, request.max_output_tokens
        )
        return CostEstimate(
            in_tok,
            out_tok,
            call_cost_usd(profile, in_tok, out_tok),
            cap,
            reservation_usd(self.config, profile, in_tok, cap),
        )

    # ---------------------------------------------------------------- calls
    def _now(self) -> str:
        return self.clock().isoformat(timespec="seconds")

    def call(self, model_id: str, request: ModelRequest, ctx: CallContext) -> CallOutcome:
        profile = self.profile(model_id)
        adapter = self.registry.for_profile(profile)
        check_call_allowed(self.config, profile, adapter)
        mode = getattr(adapter, "mode_label", "mock")
        key = cache_key(profile.id, request)
        rhash = request_hash(request)

        if ctx.idempotency_key is not None:
            previous = self._replay(ctx.idempotency_key, profile.id, request.effort, rhash)
            if previous is not None:
                return previous

        cached = self.cache.get(key)
        if cached is not None:
            payload = json.loads(cached)
            call_id = self._insert_complete(
                profile, request, ctx, mode, key, rhash, status="cache_hit", payload=payload
            )
            return CallOutcome(call_id, profile.id, "cache_hit", payload, None, 0.0, 0.0, key)

        est = self.estimate(profile, request)
        # The provider is always given a hard output cap; the worst case at that cap is
        # what is reserved (audit M1), so settlement cannot exceed the reservation unless
        # the provider ignores the cap or the input estimate is off by more than its margin.
        request = replace(
            request, max_output_tokens=est.output_cap, client_request_id=ctx.idempotency_key
        )
        lines = self.budgets.lines_for(ctx.objective_id, ctx.task_id, ctx.session_id)
        with self.db.transaction() as c:  # T1: ownership + reservation + in-flight row
            if ctx.fence is not None:
                check_fence(c, ctx.fence, self._now())
            reservation = self.budgets.reserve_in(  # raises BudgetExceededError
                c, lines, est.reserve_usd, session_id=ctx.session_id, task_id=ctx.task_id
            )
            call_id = self._insert_in_flight(
                c, profile, request, ctx, mode, key, rhash, est, reservation.id
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
        overshoot = max(0.0, cost - est.reserve_usd)
        if overshoot > 1e-12:
            note = (
                f"settled ${cost:.6f} exceeds reservation ${est.reserve_usd:.6f} "
                f"(output {out_tok} tokens, cap {est.output_cap})"
            )
            error = f"{error}; {note}" if error else note
        stored = {
            "payload": result_payload,
            "simulation": response.simulation if response else {},
        }
        reply = Reply(
            status=status,
            error=error,
            input_tokens=in_tok,
            output_tokens=out_tok,
            latency_ms=latency,
            cost_usd=cost,
            response_json=to_json(stored),
            provider_request_id=response.provider_request_id if response else None,
        )
        # T2: the call is authoritative only if it is still in flight AND (for an owned
        # task) the caller still owns the task with a live lease. Both are checked, and the
        # result recorded and the reservation settled, in one transaction. Otherwise the
        # reply is late and goes through the late-reply path (never used as a result).
        with self.db.transaction() as c:
            row = c.execute("SELECT * FROM model_calls WHERE id=?", (call_id,)).fetchone()
            owned = ctx.fence is None or fence_valid(c, ctx.fence, self._now())
            if row["status"] == "in_flight" and owned:
                c.execute(
                    "UPDATE model_calls SET status=?, error=?, input_tokens=?, output_tokens=?,"
                    " cost_usd=?, latency_ms=?, response_json=?, overshoot_usd=?,"
                    " provider_request_id=? WHERE id=?",
                    (
                        status,
                        error,
                        in_tok,
                        out_tok,
                        cost,
                        latency,
                        reply.response_json,
                        overshoot if overshoot > 1e-12 else 0.0,
                        reply.provider_request_id,
                        call_id,
                    ),
                )
                self.budgets.settle_in(c, reservation.id, cost)
                if ctx.task_id is not None and cost:
                    c.execute(
                        "UPDATE tasks SET spent_usd = spent_usd + ? WHERE id=?",
                        (cost, ctx.task_id),
                    )
                late = False
            else:
                self._record_late_in(c, row, reply)
                late = True
        if late:
            return CallOutcome(
                call_id,
                profile.id,
                "late",
                None,
                response,
                est.cost_usd,
                cost,
                key,
                "reply arrived after ownership of the call was lost; recorded as late",
            )
        return CallOutcome(
            call_id,
            profile.id,
            status,
            result_payload,
            response,
            est.cost_usd,
            cost,
            key,
            error,
            overshoot if overshoot > 1e-12 else 0.0,
        )

    # ---------------------------------------------------------------- late replies
    def record_late_reply(self, call_id: int, reply: Reply) -> bool:
        """Record a reply that arrived for a non-authoritative call (e.g. delivered again by
        a provider). Returns False if this call already has a late reply: duplicate
        delivery never applies its accounting twice."""
        with self.db.transaction() as c:
            row = c.execute("SELECT * FROM model_calls WHERE id=?", (call_id,)).fetchone()
            if row is None:
                raise NotFoundError(f"Model call {call_id} not found")
            return self._record_late_in(c, row, reply)

    def _record_late_in(self, c: Any, row: Any, reply: Reply) -> bool:
        """Atomically: record the late reply, reconcile the call's charge to the actual
        cost (once), update task spend and record any overshoot of the reservation.

        * call already ``abandoned`` (recovery charged its full reservation R): apply
          ``actual - R`` - a refund if the reply was cheaper, a charge if dearer;
        * call still ``in_flight`` but ownership lost: settle its held reservation at the
          actual cost, mark the call ``late``.

        Any other status (``ok``, ``malformed``, ``timeout``, ``rate_limit``, ``error``,
        ``cache_hit``, or ``late`` already) means the call was already accounted for:
        its reservation is settled and its result, if any, is authoritative. Such a reply
        is a duplicate delivery and changes nothing - no late-reply row, no spend at any
        level, no reservation, status or idempotency-key change. Returns False.
        """
        if row["status"] not in ("abandoned", "in_flight"):
            return False
        if row["status"] == "in_flight" and not self._reservation_unresolved(c, row):
            return False  # nothing left to settle: refuse rather than charge one level only
        reserved = float(row["reserved_usd"] if row["reserved_usd"] is not None else 0.0)
        charged_before = float(row["cost_usd"] or 0.0) if row["status"] == "abandoned" else 0.0
        actual = reply.cost_usd
        adjustment = actual - charged_before
        overshoot = max(0.0, actual - reserved) if row["reserved_usd"] is not None else 0.0
        ins = c.execute(
            "INSERT INTO late_replies (call_id, task_id, owner_generation, session_id,"
            " received_at, status, input_tokens, output_tokens, actual_cost_usd, reserved_usd,"
            " charged_before_usd, adjustment_usd, overshoot_usd, adjustment_applied,"
            " provider_request_id, response_json) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,1,?,?)"
            " ON CONFLICT(call_id) DO NOTHING",
            (
                row["id"],
                row["task_id"],
                row["owner_generation"],
                row["session_id"],
                utc_now_iso(),
                reply.status,
                reply.input_tokens,
                reply.output_tokens,
                actual,
                row["reserved_usd"],
                charged_before,
                adjustment,
                overshoot,
                reply.provider_request_id,
                reply.response_json,
            ),
        )
        if ins.rowcount != 1:
            return False  # already recorded: no second charge or refund
        if row["status"] == "abandoned":
            budget_ids = self._reservation_budget_ids(c, row["reservation_id"])
            for bid in budget_ids:
                c.execute(
                    "UPDATE budgets SET spent_usd = MAX(0, spent_usd + ?) WHERE id=?",
                    (adjustment, bid),
                )
            new_status = "abandoned"
        else:  # still in flight: settle the held reservation at the actual cost
            if row["reservation_id"] is not None:
                self.budgets.settle_in(c, row["reservation_id"], actual)
            new_status = "late"
        if row["task_id"] is not None and adjustment:
            c.execute(
                "UPDATE tasks SET spent_usd = MAX(0, spent_usd + ?) WHERE id=?",
                (adjustment, row["task_id"]),
            )
        # A late call is never authoritative: free its idempotency key (as abandonment
        # does) so the current owner's call for the same logical request is made afresh.
        c.execute(
            "UPDATE model_calls SET status=?, cost_usd=?, input_tokens=?, output_tokens=?,"
            " overshoot_usd=MAX(overshoot_usd, ?), provider_request_id=COALESCE(?,"
            " provider_request_id), error=?,"
            " idempotency_key = CASE WHEN idempotency_key IS NULL"
            "   OR idempotency_key LIKE '%:abandoned:%' THEN idempotency_key"
            "   ELSE idempotency_key || ':late:' || id END WHERE id=?",
            (
                new_status,
                actual,
                reply.input_tokens,
                reply.output_tokens,
                overshoot,
                reply.provider_request_id,
                f"late reply recorded (reconciled to actual ${actual:.6f}; "
                f"adjustment {adjustment:+.6f})",
                row["id"],
            ),
        )
        return True

    @staticmethod
    def _reservation_unresolved(c: Any, row: Any) -> bool:
        """An in-flight call's reservation must still be held for late settlement to apply
        the same amount to the budgets and the task (or the call had no reservation)."""
        if row["reservation_id"] is None:
            return True
        r = c.execute(
            "SELECT status FROM budget_reservations WHERE id=?", (row["reservation_id"],)
        ).fetchone()
        return r is not None and r["status"] == "held"

    @staticmethod
    def _reservation_budget_ids(c: Any, reservation_id: int | None) -> list[int]:
        if reservation_id is None:
            return []
        r = c.execute(
            "SELECT budget_ids FROM budget_reservations WHERE id=?", (reservation_id,)
        ).fetchone()
        return [int(b) for b in json.loads(r["budget_ids"])] if r else []

    # ---------------------------------------------------------------- replay
    def _replay(
        self, idempotency_key: str, model_id: str, effort: str, rhash: str
    ) -> CallOutcome | None:
        r = self.db.query_one(
            "SELECT * FROM model_calls WHERE idempotency_key=?", (idempotency_key,)
        )
        if r is None:
            return None
        if r["status"] == "in_flight":
            raise CareerOSError(
                f"Request {idempotency_key} is already in flight in session {r['session_id']}."
            )
        mismatch = [
            what
            for what, ok in (
                ("model", r["model_id"] == model_id),
                ("effort", r["effort"] == effort),
                ("request", r["request_hash"] is None or r["request_hash"] == rhash),
            )
            if not ok
        ]
        if mismatch:
            raise IdempotencyMismatchError(
                f"Call {idempotency_key} was made with model={r['model_id']} "
                f"effort={r['effort']}; replay requested model={model_id} effort={effort} "
                f"(mismatch: {', '.join(mismatch)})."
            )
        stored = json.loads(r["response_json"] or "{}")
        response = ModelResponse(
            text="",
            input_tokens=r["input_tokens"] or 0,
            output_tokens=r["output_tokens"] or 0,
            latency_ms=r["latency_ms"] or 0,
            simulation=stored.get("simulation") or {},
            provider_request_id=r["provider_request_id"],
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

    # ---------------------------------------------------------------- abandonment
    def abandon_calls_in(self, c: Any, rows: list[Any], reason: str) -> int:
        """Inside the caller's transaction: settle in-flight calls whose owner is gone.

        The provider may or may not have done (and billed) the work, so the full worst-case
        reservation (the cost of a capped reply) is charged conservatively, and the call is
        marked 'abandoned' so the task can make a fresh call. A reply that arrives later is
        reconciled to its actual cost by the late-reply path.
        """
        for r in rows:
            charge = float(
                r["reserved_usd"] if r["reserved_usd"] is not None else r["est_cost_usd"] or 0.0
            )
            cur = c.execute(
                "UPDATE model_calls SET status='abandoned', cost_usd=?, error=?,"
                " idempotency_key = CASE WHEN idempotency_key IS NULL THEN NULL"
                "   ELSE idempotency_key || ':abandoned:' || id END"
                " WHERE id=? AND status='in_flight'",
                (charge, reason, r["id"]),
            )
            if cur.rowcount != 1:
                continue
            if r["reservation_id"] is not None:
                self.budgets.settle_in(c, r["reservation_id"], charge)
            if r["task_id"] is not None and charge:
                c.execute(
                    "UPDATE tasks SET spent_usd = spent_usd + ? WHERE id=?", (charge, r["task_id"])
                )
        return len(rows)

    def abandon_in_flight(self, stale_sessions: list[str]) -> int:
        """Session-level abandonment for calls not tied to an owned task generation
        (planning calls, and calls recorded before Phase B)."""
        if not stale_sessions:
            return 0
        rows = self.db.query(
            "SELECT * FROM model_calls WHERE status='in_flight' AND owner_generation IS NULL"
            " AND session_id IN (SELECT value FROM json_each(?))",
            (json.dumps(stale_sessions),),
        )
        with self.db.transaction() as c:
            return self.abandon_calls_in(c, rows, "session ended while the call was in flight")

    def mark_accepted_in(self, c: Any, outcome: CallOutcome) -> None:
        """Cache a reviewed-and-accepted result (inside the accepting, fenced transaction)."""
        if outcome.payload is not None and not outcome.cache_hit:
            self.cache.put_in(c, outcome.cache_key, to_json(outcome.payload))

    def mark_accepted(self, outcome: CallOutcome) -> None:
        if outcome.payload is not None and not outcome.cache_hit:
            self.cache.put(outcome.cache_key, to_json(outcome.payload))

    # ---------------------------------------------------------------- ledger
    _COLUMNS = (
        "task_id, attempt, objective_id, purpose, session_id, idempotency_key, model_id,"
        " provider, tier, effort, mode, template_id, template_version, est_input_tokens,"
        " est_output_tokens, input_tokens, output_tokens, cached_tokens, est_cost_usd,"
        " cost_usd, cost_is_simulated, latency_ms, status, error, cache_key, cache_hit,"
        " reservation_id, response_json, reserved_usd, output_cap_tokens, owner_generation,"
        " request_hash, created_at"
    )

    def _values(
        self,
        profile: ModelProfile,
        request: ModelRequest,
        ctx: CallContext,
        mode: str,
        key: str,
        rhash: str,
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
            est.reserve_usd if est else None,
            est.output_cap if est else None,
            ctx.fence.generation if ctx.fence else None,
            rhash,
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
        rhash: str,
        est: CostEstimate,
        reservation_id: int,
    ) -> int:
        vals = self._values(
            profile,
            request,
            ctx,
            mode,
            key,
            rhash,
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
        rhash: str,
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
            rhash,
            status=status,
            est=None,
            reservation_id=None,
            cache_hit=True,
            response_json=to_json({"payload": payload, "simulation": {}}),
        )
        with self.db.transaction() as c:
            if ctx.fence is not None:
                check_fence(c, ctx.fence, self._now())
            cur = c.execute(
                f"INSERT INTO model_calls ({self._COLUMNS}) VALUES ({','.join('?' * len(vals))})",  # noqa: S608
                vals,
            )
        assert cur.lastrowid is not None
        return int(cur.lastrowid)
