from typer.testing import CliRunner

from careeros.cli.main import app
from careeros.core.config import AppConfig
from tests.conftest import CONFIG_DIR

runner = CliRunner()


def _run(*args: str) -> str:
    res = runner.invoke(app, [*args, "--config-dir", str(CONFIG_DIR)])
    assert res.exit_code == 0, res.output
    return res.output


def test_demo_scenarios_print_events_and_report(cfg: AppConfig) -> None:
    out = _run("demo", "--scenario", "escalation")
    assert "ESCALATE_TIER" in out and "ALL COSTS SIMULATED" in out
    assert "quality-adjusted cost per success" in out


def test_approve_and_resume_flow(cfg: AppConfig) -> None:
    from careeros.context import open_context
    from careeros.manager.manager import CareerManager, ObjectiveRequest
    from careeros.simulation import SYNTHETIC_JOB

    with open_context(cfg) as ctx:
        m = CareerManager(ctx)
        oid = m.submit(
            ObjectiveRequest(
                # Sized so that only the critical step asks (research now has a factual
                # review, C7).
                title="x",
                kind="demo_application_package",
                inputs=SYNTHETIC_JOB,
                budget_usd=0.18,
            )
        )
        m.run(oid)
    out = _run("approvals")
    assert "budget_increase" in out and "critical" in out
    req_id = out.split()[0].lstrip("#")
    _run("approve", req_id, "--usd", "0.2")
    out = _run("resume", str(oid))
    assert "done" in out
    assert "ALL COSTS SIMULATED" in _run("report", str(oid))


def test_demo_refuses_personal_database(cfg: AppConfig, monkeypatch) -> None:  # type: ignore[no-untyped-def]
    monkeypatch.setenv("CAREEROS_DATASET_KIND", "personal")
    res = runner.invoke(app, ["demo", "--config-dir", str(CONFIG_DIR)])
    assert res.exit_code == 1
