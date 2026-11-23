"""Simulated cost and quality reporting for an objective.

* **wasted cost**: cost of attempts that were not the accepted attempt (failures,
  rejected drafts) — it is still charged to the task that eventually succeeded.
* **cost per success**: all objective cost / tasks that succeeded.
* **quality-adjusted cost per success**: cost per success / mean accepted quality, so a
  cheap but mediocre result does not look better than it is.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from careeros.core.db import Database
from careeros.core.errors import NotFoundError


@dataclass(frozen=True)
class CallLine:
    call_id: int
    task_id: int | None
    step: str
    purpose: str
    attempt: int | None
    model_id: str
    tier: int | None
    effort: str | None
    mode: str
    status: str
    input_tokens: int
    output_tokens: int
    est_cost_usd: float
    cost_usd: float
    latency_ms: int


@dataclass(frozen=True)
class TaskLine:
    step: str
    task_type: str
    mode: str
    status: str
    attempts: int
    escalations: int
    cost_usd: float
    wasted_usd: float
    accepted_attempt: int | None
    quality: float | None
    final_model: str | None


@dataclass
class CostReport:
    objective_id: int
    title: str
    status: str
    calls: list[CallLine]
    tasks: list[TaskLine]
    totals: dict[str, Any] = field(default_factory=dict)


def build_report(db: Database, objective_id: int) -> CostReport:
    obj = db.query_one("SELECT * FROM objectives WHERE id=?", (objective_id,))
    if obj is None:
        raise NotFoundError(f"Objective {objective_id} not found")
    trows = db.query("SELECT * FROM tasks WHERE objective_id=? ORDER BY id", (objective_id,))
    step_of = {t["id"]: t["step"] for t in trows}
    crows = db.query("SELECT * FROM model_calls WHERE objective_id=? ORDER BY id", (objective_id,))
    calls = [
        CallLine(
            c["id"],
            c["task_id"],
            step_of.get(c["task_id"], "(planning)"),
            c["purpose"],
            c["attempt"],
            c["model_id"],
            c["tier"],
            c["effort"],
            c["mode"],
            c["status"],
            c["input_tokens"] or 0,
            c["output_tokens"] or 0,
            c["est_cost_usd"] or 0.0,
            c["cost_usd"] or 0.0,
            c["latency_ms"] or 0,
        )
        for c in crows
    ]
    tasks: list[TaskLine] = []
    for t in trows:
        acc_attempt, quality = None, None
        if t["accepted_result_id"] is not None:
            r = db.query_one(
                "SELECT attempt, effective_confidence FROM task_results WHERE id=?",
                (t["accepted_result_id"],),
            )
            if r is not None:
                acc_attempt, quality = r["attempt"], r["effective_confidence"]
        mine = [c for c in calls if c.task_id == t["id"]]
        cost = sum(c.cost_usd for c in mine)
        wasted = sum(c.cost_usd for c in mine if c.attempt != acc_attempt)
        tasks.append(
            TaskLine(
                t["step"],
                t["task_type"],
                t["mode"],
                t["status"],
                t["attempt"],
                t["escalations"],
                cost,
                wasted,
                acc_attempt,
                quality,
                t["current_model_id"],
            )
        )

    total = sum(c.cost_usd for c in calls)
    succeeded = [t for t in tasks if t.status == "succeeded"]
    qualities = [t.quality for t in succeeded if t.quality is not None]
    avg_q = sum(qualities) / len(qualities) if qualities else None
    per_success = total / len(succeeded) if succeeded else None
    by_tier: dict[str, float] = {}
    for c in calls:
        key = f"tier {c.tier} ({c.model_id})"
        by_tier[key] = by_tier.get(key, 0.0) + c.cost_usd
    est_total = sum(c.est_cost_usd for c in calls if c.status != "cache_hit")
    decisions = {
        r["decision"]: r["n"]
        for r in db.query(
            "SELECT decision, COUNT(*) AS n FROM manager_decisions WHERE objective_id=?"
            " GROUP BY decision",
            (objective_id,),
        )
    }
    return CostReport(
        objective_id=objective_id,
        title=obj["title"],
        status=obj["status"],
        calls=calls,
        tasks=tasks,
        totals={
            "simulated": True,
            "total_cost_usd": total,
            "task_cost_usd": sum(c.cost_usd for c in calls if c.purpose == "task"),
            "review_cost_usd": sum(c.cost_usd for c in calls if c.purpose == "review"),
            "planning_cost_usd": sum(c.cost_usd for c in calls if c.purpose == "planning"),
            "wasted_cost_usd": sum(t.wasted_usd for t in tasks),
            "estimated_cost_usd": est_total,
            "model_calls": len(calls),
            "cache_hits": sum(1 for c in calls if c.status == "cache_hit"),
            "tasks_succeeded": len(succeeded),
            "deterministic_tasks": sum(1 for t in tasks if t.mode == "deterministic"),
            "retries": sum(decisions.get(k, 0) for k in ("retry_same", "retry_with_feedback")),
            "escalations": sum(decisions.get(k, 0) for k in ("escalate_effort", "escalate_tier")),
            "cost_per_success_usd": per_success,
            "mean_accepted_quality": avg_q,
            "quality_adjusted_cost_per_success_usd": (
                per_success / avg_q if per_success is not None and avg_q else None
            ),
            "cost_by_tier": dict(sorted(by_tier.items())),
            "budget_usd": obj["budget_usd"],
        },
    )


def _usd(v: float | None) -> str:
    return "-" if v is None else f"${v:.5f}"


def format_report(rep: CostReport) -> str:
    t = rep.totals
    lines = [
        f"Objective {rep.objective_id}: {rep.title}  [{rep.status}]   (ALL COSTS SIMULATED)",
        "",
        "Model calls:",
        f"  {'#':>3} {'step':<18} {'purpose':<8} {'att':>3} {'model':<12} {'T':>1} "
        f"{'effort':<6} {'status':<10} {'in':>5} {'out':>5} {'est':>9} {'cost':>9} {'ms':>6}",
    ]
    for c in rep.calls:
        lines.append(
            f"  {c.call_id:>3} {c.step[:18]:<18} {c.purpose:<8} {c.attempt or '-':>3} "
            f"{c.model_id:<12} {c.tier or '-':>1} {c.effort or '-':<6} {c.status:<10} "
            f"{c.input_tokens:>5} {c.output_tokens:>5} {_usd(c.est_cost_usd):>9} "
            f"{_usd(c.cost_usd):>9} {c.latency_ms:>6}"
        )
    lines += [
        "",
        "Tasks:",
        f"  {'step':<18} {'mode':<13} {'status':<14} {'att':>3} {'esc':>3} "
        f"{'cost':>9} {'wasted':>9} {'quality':>7}  final model",
    ]
    for k in rep.tasks:
        lines.append(
            f"  {k.step[:18]:<18} {k.mode:<13} {k.status:<14} {k.attempts:>3} "
            f"{k.escalations:>3} {_usd(k.cost_usd):>9} {_usd(k.wasted_usd):>9} "
            f"{'-' if k.quality is None else f'{k.quality:.2f}':>7}  "
            f"{k.final_model or ('(no model)' if k.mode == 'deterministic' else '-')}"
        )
    lines += [
        "",
        "Totals (simulated USD):",
        f"  total {_usd(t['total_cost_usd'])} = tasks {_usd(t['task_cost_usd'])} + reviews "
        f"{_usd(t['review_cost_usd'])} + planning {_usd(t['planning_cost_usd'])}",
        f"  estimated before calls {_usd(t['estimated_cost_usd'])}; budget {_usd(t['budget_usd'])}",
        f"  spent on failed/rejected attempts {_usd(t['wasted_cost_usd'])}",
        f"  calls {t['model_calls']}, cache hits {t['cache_hits']}, retries {t['retries']}, "
        f"escalations {t['escalations']}, deterministic steps {t['deterministic_tasks']}",
        f"  tasks succeeded {t['tasks_succeeded']}; cost per success "
        f"{_usd(t['cost_per_success_usd'])}; mean accepted quality "
        f"{'-' if t['mean_accepted_quality'] is None else f'{t["mean_accepted_quality"]:.2f}'}",
        f"  quality-adjusted cost per success {_usd(t['quality_adjusted_cost_per_success_usd'])}",
        "  by tier: " + ", ".join(f"{k} {_usd(v)}" for k, v in t["cost_by_tier"].items()),
    ]
    return "\n".join(lines)
