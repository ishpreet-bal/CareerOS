from pathlib import Path

import pytest
from typer.testing import CliRunner

from careeros.cli.main import app
from careeros.context import AppContext, open_context
from careeros.core.config import AppConfig
from careeros.core.errors import CareerOSError
from careeros.domain.models import WorkAuthStatus
from careeros.synthetic import seed_synthetic
from tests.conftest import CONFIG_DIR, REPO_ROOT

runner = CliRunner()


def test_seed_synthetic_leaves_authorization_unknown(ctx: AppContext) -> None:
    summary = seed_synthetic(ctx)
    assert summary.facts == 3 and summary.jobs == 2
    auths = ctx.personal.list_work_authorizations(summary.profile_id)
    assert {a.country for a in auths} == set(ctx.config.eligibility.target_countries)
    assert all(a.status is WorkAuthStatus.UNKNOWN for a in auths)
    with pytest.raises(CareerOSError):
        seed_synthetic(ctx)  # second seed refused


def test_seed_refused_on_personal_database(cfg: AppConfig) -> None:
    personal = cfg.model_copy(
        update={"storage": cfg.storage.model_copy(update={"dataset_kind": "personal"})}
    )
    with open_context(personal) as ctx, pytest.raises(CareerOSError):
        seed_synthetic(ctx)


def test_cli_init_and_status(cfg: AppConfig, data_dir: Path) -> None:
    res = runner.invoke(app, ["init", "--synthetic", "--config-dir", str(CONFIG_DIR)])
    assert res.exit_code == 0, res.output
    assert "Seeded synthetic data" in res.output
    assert (data_dir / "careeros.db").exists()

    res = runner.invoke(app, ["status", "--config-dir", str(CONFIG_DIR)])
    assert res.exit_code == 0, res.output
    assert "mock (real model calls disabled)" in res.output
    assert "needs your confirmation for CA, US" in res.output


def test_cli_config_validate(cfg: AppConfig) -> None:
    res = runner.invoke(app, ["config", "validate", "--config-dir", str(CONFIG_DIR)])
    assert res.exit_code == 0, res.output
    assert "real_budget=$0.00" in res.output


def test_cli_refuses_data_dir_in_repo(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("CAREEROS_DATA_DIR", str(REPO_ROOT / "data"))
    res = runner.invoke(app, ["init", "--config-dir", str(CONFIG_DIR)])
    assert res.exit_code != 0
    assert not (REPO_ROOT / "data").exists()


def test_cli_rejects_real_mode(monkeypatch: pytest.MonkeyPatch, cfg: AppConfig) -> None:
    monkeypatch.setenv("CAREEROS_EXECUTION_MODE", "real")
    res = runner.invoke(app, ["status", "--config-dir", str(CONFIG_DIR)])
    assert res.exit_code == 2
