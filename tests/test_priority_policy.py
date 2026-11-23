"""Priority/downgrade policy through the manager (architecture §5.4, M3 amendment)."""

from __future__ import annotations

from typing import Any

from typer.testing import CliRunner

from careeros.cli.main import app
from careeros.context import AppContext, open_context
from careeros.core.config import AppConfig
from careeros.core.db import from_json
from careeros.domain.models import ObjectiveStatus
from careeros.llm.adapters.scripted import ScriptedAdapter
from careeros.manager.manager import CareerManager, ObjectiveRequest
from careeros.manager.plans import PlaybookLibrary
from tests.conftest import CONFIG_DIR
from tests.m2_helpers import calls, decisions, task_by_step
from tests.m3_helpers import book

# job.analyze at complexity 0.65: tier 2 meets the floor but costs ~$0.005; tier 3 costs
# ~$0.0005 with p~0.67. A $0.002 task budget forces the downgrade question.
STEP: dict[str, Any] = {
    "id": "analyze",
    "worker": "model_step",
    "task_type": "job.analyze",
    "params": {"complexity_hint": 0.30},
    "budget_usd": 0.002,
    "instructions": "Analyze the synthetic posting.",
    "output": {"required": ["summary"], "types": {"summary": "string"}},
}


def _manager(ctx: AppContext, priority: str) -> tuple[CareerManager, int]:
    pb = book("analyze_only", [{**STEP, "priority": priority}])
    m = CareerManager(
        ctx,
        adapter=ScriptedAdapter({"analyze:job.analyze": ["success:0.9"] * 3}),
        playbooks=PlaybookLibrary([pb]),
    )
    oid = m.submit(ObjectiveRequest(title="x", kind="analyze_only", inputs={"text": "x"}))
    return m, oid


def test_normal_priority_downgrades_within_limit_and_records_it(ctx: AppContext) -> None:
    m, oid = _manager(ctx, "normal")
    assert m.run(oid).status is ObjectiveStatus.DONE
    t = task_by_step(ctx, oid, "analyze")
    assert t["current_tier"] == 3
    assert "degrade" in decisions(ctx, t["id"])
    row = ctx.db.query_one(
        "SELECT inputs FROM manager_decisions WHERE task_id=? AND decision='degrade'", (t["id"],)
    )
    inputs = from_json(row["inputs"])
    assert inputs["status"] == "degraded" and inputs["user_approved"] is False
    assert inputs["chosen_p"] >= inputs["degrade_floor"]
    assert m.pending_requests(oid) == []


def test_high_priority_asks_and_user_approved_downgrade_is_recorded(ctx: AppContext) -> None:
    m, oid = _manager(ctx, "high")
    m.run(oid)
    t = task_by_step(ctx, oid, "analyze")
    assert t["status"] == "blocked_budget" and calls(ctx) == []  # nothing ran, no downgrade
    [req] = m.pending_requests(oid)
    option = req["details"]["downgrade_option"]
    assert option["model_id"] == "mock-small"
    assert "never downgraded automatically" in req["details"]["reason"]

    m.approve(req["id"], accept_downgrade=True)
    assert m.run(oid).status is ObjectiveStatus.DONE
    t = task_by_step(ctx, oid, "analyze")
    assert t["current_model_id"] == "mock-small"
    assert decisions(ctx, t["id"]).count("approve_downgrade") == 1
    degrade = from_json(
        ctx.db.query_one(
            "SELECT inputs FROM manager_decisions WHERE task_id=? AND decision='degrade'",
            (t["id"],),
        )["inputs"]
    )
    assert degrade["user_approved"] is True and degrade["status"] == "approved_downgrade"
    approval = ctx.db.query_one("SELECT * FROM approvals WHERE kind='quality_downgrade'")
    assert approval is not None and from_json(approval["details"])["request_id"] == req["id"]
    resolution = ctx.db.query_one(
        "SELECT resolution FROM approval_requests WHERE id=?", (req["id"],)
    )["resolution"]
    assert resolution == "accept_downgrade:task"  # tied to the route that asked


def test_critical_priority_also_asks(ctx: AppContext) -> None:
    m, oid = _manager(ctx, "critical")
    m.run(oid)
    assert task_by_step(ctx, oid, "analyze")["status"] == "blocked_budget"
    assert calls(ctx) == []


def test_cli_accept_downgrade_and_force_recover_flags(cfg: AppConfig) -> None:
    with open_context(cfg) as ctx:
        m, oid = _manager(ctx, "high")
        m.run(oid)
        [req] = m.pending_requests(oid)
    runner = CliRunner()
    res = runner.invoke(
        app, ["approve", str(req["id"]), "--accept-downgrade", "--config-dir", str(CONFIG_DIR)]
    )
    assert res.exit_code == 0, res.output
    res = runner.invoke(
        app, ["resume", str(oid), "--force-recover", "--config-dir", str(CONFIG_DIR)]
    )
    assert res.exit_code == 0, res.output
