"""Hierarchical budgets (month ⊇ session, objective ⊇ task) with atomic reservations.

Concurrency guarantees:

* ``reserve_in`` runs inside a ``BEGIN IMMEDIATE`` transaction (SQLite's write lock), so
  the check "limit - spent - reserved >= amount" and the increment of ``reserved_usd`` at
  every level are one atomic step across all threads and processes. Concurrent tasks can
  therefore never jointly exceed any budget level.
* Each reservation is a ``budget_reservations`` row owned by an execution session. The
  gateway writes it in the same transaction as the in-flight call row, and settles it in
  the same transaction that completes the call, so the two can never disagree.
* Recovery releases or settles only reservations of sessions that are no longer alive.

All amounts are simulated in M2/M3.
"""

from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass

from careeros.core.clock import utc_now, utc_now_iso
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

    @property
    def uncommitted_usd(self) -> float:
        """Available if all in-flight reservations were released."""
        return self.limit_usd - self.spent_usd


@dataclass(frozen=True)
class Reservation:
    id: int
    budget_ids: tuple[int, ...]
    amount_usd: float


class BudgetManager:
    def __init__(self, db: Database, config: AppConfig) -> None:
        self.db = db
        self.config = config

    # ---------------------------------------------------------------- setup
    def _ensure(self, scope: str, scope_ref: str, limit: float, period: str = "") -> int:
        row = self.db.query_one(
            "SELECT id FROM budgets WHERE scope=? AND scope_ref=? AND period=? AND mode=?",
            (scope, scope_ref, period, MODE),
        )
        if row is not None:
            return int(row["id"])
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

    def lines_for(
        self, objective_id: int, task_id: int | None, session_id: str | None = None
    ) -> list[int]:
        """Budget ids that a call for this objective/task/session must fit within."""
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
        if session_id is not None:
            ids.append(self._ensure("session", session_id, b.session_simulated_usd))
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

    def available(
        self,
        objective_id: int,
        task_id: int | None,
        session_id: str | None = None,
        *,
        include_reservations: bool = True,
    ) -> float:
        """Smallest headroom across all levels.

        Routing uses ``include_reservations=False`` (committed spend only) so a transient
        reservation by a concurrent task does not trigger a downgrade or an approval
        request; the reservation step then enforces the budget exactly.
        """
        lines = [self.line(i) for i in self.lines_for(objective_id, task_id, session_id)]
        if include_reservations:
            return min(ln.available_usd for ln in lines)
        return min(ln.uncommitted_usd for ln in lines)

    # ---------------------------------------------------------------- reservations
    def reserve_in(
        self,
        c: sqlite3.Connection,
        budget_ids: list[int],
        amount: float,
        *,
        session_id: str | None,
        task_id: int | None,
    ) -> Reservation:
        """Reserve inside the caller's (IMMEDIATE) transaction. Raises on any shortfall."""
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
                    reserved_usd=r["reserved_usd"],
                    uncommitted_usd=r["limit_usd"] - r["spent_usd"],
                )
        for bid in budget_ids:
            c.execute(
                "UPDATE budgets SET reserved_usd = reserved_usd + ? WHERE id=?", (amount, bid)
            )
        cur = c.execute(
            "INSERT INTO budget_reservations (session_id, task_id, budget_ids, amount_usd,"
            " created_at) VALUES (?,?,?,?,?)",
            (session_id, task_id, json.dumps(budget_ids), amount, utc_now_iso()),
        )
        assert cur.lastrowid is not None
        return Reservation(cur.lastrowid, tuple(budget_ids), amount)

    def reserve(
        self,
        budget_ids: list[int],
        amount: float,
        *,
        session_id: str | None = None,
        task_id: int | None = None,
    ) -> Reservation:
        with self.db.transaction() as c:
            return self.reserve_in(c, budget_ids, amount, session_id=session_id, task_id=task_id)

    @staticmethod
    def settle_in(c: sqlite3.Connection, reservation_id: int, actual_usd: float) -> None:
        """Convert a held reservation into spend, inside the caller's transaction."""
        r = c.execute(
            "SELECT * FROM budget_reservations WHERE id=? AND status='held'", (reservation_id,)
        ).fetchone()
        if r is None:
            return  # already settled or released: idempotent
        for bid in json.loads(r["budget_ids"]):
            c.execute(
                "UPDATE budgets SET reserved_usd = MAX(0, reserved_usd - ?),"
                " spent_usd = spent_usd + ? WHERE id=?",
                (r["amount_usd"], actual_usd, bid),
            )
        c.execute(
            "UPDATE budget_reservations SET status='settled', settled_usd=?, resolved_at=?"
            " WHERE id=?",
            (actual_usd, utc_now_iso(), reservation_id),
        )

    @staticmethod
    def release_in(c: sqlite3.Connection, reservation_id: int) -> None:
        r = c.execute(
            "SELECT * FROM budget_reservations WHERE id=? AND status='held'", (reservation_id,)
        ).fetchone()
        if r is None:
            return
        for bid in json.loads(r["budget_ids"]):
            c.execute(
                "UPDATE budgets SET reserved_usd = MAX(0, reserved_usd - ?) WHERE id=?",
                (r["amount_usd"], bid),
            )
        c.execute(
            "UPDATE budget_reservations SET status='released', resolved_at=? WHERE id=?",
            (utc_now_iso(), reservation_id),
        )

    def settle(self, res: Reservation, actual_usd: float) -> None:
        with self.db.transaction() as c:
            self.settle_in(c, res.id, actual_usd)

    def held_reservations(self, session_id: str | None = None) -> list[sqlite3.Row]:
        if session_id is None:
            return self.db.query("SELECT * FROM budget_reservations WHERE status='held'")
        return self.db.query(
            "SELECT * FROM budget_reservations WHERE status='held' AND session_id=?",
            (session_id,),
        )

    def raise_limit(self, scope: str, scope_ref: str, extra_usd: float) -> None:
        with self.db.transaction() as c:
            c.execute(
                "UPDATE budgets SET limit_usd = limit_usd + ? WHERE scope=? AND scope_ref=?"
                " AND mode=?",
                (extra_usd, scope, scope_ref, MODE),
            )
