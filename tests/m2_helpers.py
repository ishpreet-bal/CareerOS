"""Shared helpers for M2 tests."""

from __future__ import annotations

from careeros.context import AppContext
from careeros.core.config import AppConfig, ModelProfile
from careeros.llm.adapters.mock import MockAdapter
from careeros.llm.base import ModelAdapter
from careeros.manager.manager import CareerManager, ObjectiveRequest
from careeros.simulation import SYNTHETIC_JOB
from careeros.workers.base import WorkerRegistry

DEMO = ObjectiveRequest(
    title="Prepare application package", kind="demo_application_package", inputs=SYNTHETIC_JOB
)


def manager(ctx: AppContext, adapter: ModelAdapter | None = None, **kw: object) -> CareerManager:
    return CareerManager(ctx, adapter=adapter or MockAdapter(), **kw)  # type: ignore[arg-type]


def profile(cfg: AppConfig, model_id: str) -> ModelProfile:
    return next(m for m in cfg.models if m.id == model_id)


def calls(ctx: AppContext, **where: object) -> list[dict[str, object]]:
    sql = "SELECT * FROM model_calls"
    if where:
        sql += " WHERE " + " AND ".join(f"{k}=?" for k in where)
    return [dict(r) for r in ctx.db.query(sql + " ORDER BY id", tuple(where.values()))]


def task_by_step(ctx: AppContext, objective_id: int, step: str) -> dict[str, object]:
    r = ctx.db.query_one(
        "SELECT * FROM tasks WHERE objective_id=? AND step=?", (objective_id, step)
    )
    assert r is not None
    return dict(r)


def decisions(ctx: AppContext, task_id: object) -> list[str]:
    return [
        r["decision"]
        for r in ctx.db.query(
            "SELECT decision FROM manager_decisions WHERE task_id=? ORDER BY id", (task_id,)
        )
    ]


def empty_registry() -> WorkerRegistry:
    return WorkerRegistry()
