"""Hierarchical budgets (global month ⊇ objective ⊇ task) with reservations.

Before every call the estimated cost is reserved at every level; afterwards the
reservation is settled against the actual (simulated) cost. A reservation that would
exceed any level fails, and the call is not made. All amounts are simulated in M2.
"""

from __future__ import annotations

from dataclasses import dataclass

from careeros.core.clock import utc_now
from careeros.core.config import AppConfig
from careeros.core.db import Database
from careeros.core.errors import NotFoundError
from careeros.llm.contracts import BudgetExceededError

MODE = "simulated"


@dataclass(frozen=True)
class BudgetLine:
    id: int
    scope: str
    scope_ref: str
    limit_usd: float
    spent_usd: float
    reserved_usd: float

    @property
    def available_usd(self) -> float:
        return self.limit_usd - self.spent_usd - self.reserved_usd


@dataclass(frozen=True)
class Reservation:
    budget_ids: tuple[int, ...]
    amount_usd: float


class BudgetManager:
    def __init__(self, db: Database, config: AppConfig) -> None:
        self.db = db
        self.config = config

    # ---------------------------------------------------------------- setup
    def _ensure(self, scope: str, scope_ref: str, limit: float, period: str = "") -> int:
        with self.db.transaction() as c:
            c.execute(
                "INSERT OR IGNORE INTO budgets (scope, scope_ref, period, limit_usd, mode)"
                " VALUES (?,?,?,?,?)",
                (scope, scope_ref, period, limit, MODE),
            )
        row = self.db.query_one(
            "SELECT id FROM budgets WHERE scope=? AND scope_ref=? AND period=? AND mode=?",
            (scope, scope_ref, period, MODE),
        )
        assert row is not None
        return int(row["id"])

    def lines_for(self, objective_id: int, task_id: int | None) -> list[int]:
        """Budget ids that a call for this objective/task must fit within."""
        obj = self.db.query_one("SELECT budget_usd FROM objectives WHERE id=?", (objective_id,))
        if obj is None:
            raise NotFoundError(f"Objective {objective_id} not found")
        b = self.config.budgets
        ids = [
            self._ensure(
                "global_month", "all", b.simulated_monthly_usd, utc_now().strftime("%Y-%m")
            ),
            self._ensure(
                "objective",
                str(objective_id),
                obj["budget_usd"] if obj["budget_usd"] is not None else b.default_objective_usd,
            ),
        ]
        if task_id is not None:
            t = self.db.query_one("SELECT budget_usd FROM tasks WHERE id=?", (task_id,))
            if t is None:
                raise NotFoundError(f"Task {task_id} not found")
            limit = t["budget_usd"] if t["budget_usd"] is not None else b.default_task_usd
            ids.append(self._ensure("task", str(task_id), limit))
        return ids

    def line(self, budget_id: int) -> BudgetLine:
        r = self.db.query_one("SELECT * FROM budgets WHERE id=?", (budget_id,))
        if r is None:
            raise NotFoundError(f"Budget {budget_id} not found")
        return BudgetLine(
            r["id"], r["scope"], r["scope_ref"], r["limit_usd"], r["spent_usd"], r["reserved_usd"]
        )

    def available(self, objective_id: int, task_id: int | None) -> float:
        return min(self.line(i).available_usd for i in self.lines_for(objective_id, task_id))

    def available_by_scope(self, objective_id: int, task_id: int | None) -> dict[str, float]:
        return {
            (ln := self.line(i)).scope: ln.available_usd
            for i in self.lines_for(objective_id, task_id)
        }

    # ---------------------------------------------------------------- reservations
    def reserve(self, budget_ids: list[int], amount: float) -> Reservation:
        with self.db.transaction() as c:
            for bid in budget_ids:
                r = c.execute("SELECT * FROM budgets WHERE id=?", (bid,)).fetchone()
                available = r["limit_usd"] - r["spent_usd"] - r["reserved_usd"]
                if amount > available + 1e-12:
                    raise BudgetExceededError(
                        f"{r['scope']} budget {r['scope_ref']} has ${available:.4f} left; "
                        f"call needs ~${amount:.4f}.",
                        scope=r["scope"],
                        needed_usd=amount,
                        available_usd=available,
                    )
            for bid in budget_ids:
                c.execute(
                    "UPDATE budgets SET reserved_usd = reserved_usd + ? WHERE id=?", (amount, bid)
                )
        return Reservation(tuple(budget_ids), amount)

    def settle(self, res: Reservation, actual_usd: float) -> None:
        with self.db.transaction() as c:
            for bid in res.budget_ids:
                c.execute(
                    "UPDATE budgets SET reserved_usd = MAX(0, reserved_usd - ?),"
                    " spent_usd = spent_usd + ? WHERE id=?",
                    (res.amount_usd, actual_usd, bid),
                )

    def release_all(self) -> None:
        """Drop stale reservations left by an interrupted session."""
        with self.db.transaction() as c:
            c.execute("UPDATE budgets SET reserved_usd = 0 WHERE reserved_usd > 0")

    def raise_limit(self, scope: str, scope_ref: str, extra_usd: float) -> None:
        with self.db.transaction() as c:
            c.execute(
                "UPDATE budgets SET limit_usd = limit_usd + ? WHERE scope=? AND scope_ref=?"
                " AND mode=?",
                (extra_usd, scope, scope_ref, MODE),
            )
