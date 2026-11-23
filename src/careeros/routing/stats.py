"""Historical acceptance per (task type, complexity bucket, model, effort)."""

from __future__ import annotations

from dataclasses import dataclass

from careeros.core.clock import utc_now_iso
from careeros.core.db import Database


@dataclass(frozen=True)
class StatsRow:
    attempts: int
    accepts: int
    avg_quality: float | None
    avg_cost_usd: float | None


class RoutingStatsRepository:
    def __init__(self, db: Database) -> None:
        self.db = db

    def get(self, task_type: str, bucket: str, model_id: str, effort: str) -> StatsRow | None:
        r = self.db.query_one(
            "SELECT * FROM routing_stats WHERE task_type=? AND complexity_bucket=?"
            " AND model_id=? AND effort=?",
            (task_type, bucket, model_id, effort),
        )
        if r is None:
            return None
        return StatsRow(r["attempts"], r["accepts"], r["avg_quality"], r["avg_cost_usd"])

    def record(
        self,
        task_type: str,
        bucket: str,
        model_id: str,
        effort: str,
        *,
        accepted: bool,
        prior_alpha: float,
        prior_beta: float,
        quality: float | None,
        cost_usd: float,
        latency_ms: int,
    ) -> None:
        """Fold one reviewed attempt into running averages."""
        q = quality if quality is not None else (1.0 if accepted else 0.0)
        with self.db.transaction() as c:
            c.execute(
                "INSERT INTO routing_stats (task_type, complexity_bucket, model_id, effort,"
                " attempts, accepts, prior_alpha, prior_beta, avg_quality, avg_cost_usd,"
                " avg_latency_ms, updated_at) VALUES (?,?,?,?,1,?,?,?,?,?,?,?)"
                " ON CONFLICT(task_type, complexity_bucket, model_id, effort) DO UPDATE SET"
                " avg_quality = (avg_quality * attempts + excluded.avg_quality) / (attempts + 1),"
                " avg_cost_usd = (avg_cost_usd * attempts + excluded.avg_cost_usd)"
                "   / (attempts + 1),"
                " avg_latency_ms = (avg_latency_ms * attempts + excluded.avg_latency_ms)"
                "   / (attempts + 1),"
                " attempts = attempts + 1, accepts = accepts + excluded.accepts,"
                " updated_at = excluded.updated_at",
                (
                    task_type,
                    bucket,
                    model_id,
                    effort,
                    int(accepted),
                    prior_alpha,
                    prior_beta,
                    q,
                    cost_usd,
                    latency_ms,
                    utc_now_iso(),
                ),
            )
