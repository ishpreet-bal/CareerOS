from pathlib import Path

import pytest
import yaml

from careeros.core.config import AppConfig, ModelProfile, load_config
from careeros.core.errors import ConfigError
from tests.conftest import CONFIG_DIR


def _write_config(tmp: Path, main: dict[str, object], models: list[object] | None = None) -> Path:
    d = tmp / "cfg"
    d.mkdir()
    (d / "careeros.yaml").write_text(yaml.safe_dump(main))
    if models is not None:
        (d / "models.yaml").write_text(yaml.safe_dump({"models": models}))
    return d


def test_example_config_loads_in_safe_mock_mode() -> None:
    cfg = load_config(CONFIG_DIR)
    assert cfg.execution.mode == "mock"
    assert cfg.execution.allow_real_models is False
    assert cfg.budgets.real_monthly_usd == 0.0
    assert cfg.storage.dataset_kind == "synthetic"
    assert {m.provider for m in cfg.enabled_models()} == {"mock"}
    assert {m.tier for m in cfg.models} == {1, 2, 3}
    assert all(m.pricing.simulated for m in cfg.models)


def test_routing_and_eligibility_templates_validate() -> None:
    cfg = load_config(CONFIG_DIR)
    assert cfg.routing.quality_floors["critical"] > cfg.routing.quality_floors["low"]
    assert cfg.routing.task_types["resume.tailor"].important is True
    assert cfg.eligibility.rules, "eligibility rules should be configurable"
    assert all(r.on_unknown == "needs_user_confirmation" for r in cfg.eligibility.rules)


@pytest.mark.parametrize(
    "execution",
    [{"mode": "real"}, {"mode": "mock", "allow_real_models": True}, {"mode": "manual"}],
)
def test_real_and_manual_execution_rejected(tmp_path: Path, execution: dict[str, object]) -> None:
    d = _write_config(tmp_path, {"execution": execution})
    with pytest.raises(ConfigError):
        load_config(d)


def test_env_cannot_enable_real_mode(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("CAREEROS_EXECUTION_MODE", "real")
    with pytest.raises(ConfigError):
        load_config(CONFIG_DIR)


def test_nonzero_real_budget_rejected(tmp_path: Path) -> None:
    d = _write_config(tmp_path, {"budgets": {"real_monthly_usd": 1.0}})
    with pytest.raises(ConfigError, match="real_monthly_usd"):
        load_config(d)


def test_enabled_real_provider_rejected(tmp_path: Path) -> None:
    d = _write_config(
        tmp_path, {}, models=[{"id": "x", "provider": "some-vendor", "tier": 2, "enabled": True}]
    )
    with pytest.raises(ConfigError, match="cannot be enabled"):
        load_config(d)


def test_disabled_real_provider_template_allowed() -> None:
    m = ModelProfile(id="x", provider="some-vendor", tier=2, enabled=False)
    assert not m.enabled


def test_duplicate_model_ids_rejected() -> None:
    m = {"id": "dup", "provider": "mock", "tier": 3}
    with pytest.raises(ValueError, match="Duplicate"):
        AppConfig.model_validate({"models": [m, m]})


def test_eligibility_on_unknown_cannot_be_relaxed(tmp_path: Path) -> None:
    rule = {
        "id": "r",
        "description": "d",
        "requirement": "work_authorization",
        "on_unknown": "eligible",
    }
    d = _write_config(tmp_path, {"eligibility": {"rules": [rule]}})
    with pytest.raises(ConfigError):
        load_config(d)


def test_unknown_keys_rejected(tmp_path: Path) -> None:
    d = _write_config(tmp_path, {"execution": {"mode": "mock", "turbo": True}})
    with pytest.raises(ConfigError):
        load_config(d)


def test_invalid_country_code_rejected(tmp_path: Path) -> None:
    d = _write_config(tmp_path, {"eligibility": {"target_countries": ["Canada"]}})
    with pytest.raises(ConfigError):
        load_config(d)


def test_env_overrides_data_dir(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setenv("CAREEROS_DATA_DIR", str(tmp_path / "x"))
    monkeypatch.setenv("CAREEROS_DATASET_KIND", "personal")
    cfg = load_config(CONFIG_DIR)
    assert cfg.storage.data_dir == tmp_path / "x"
    assert cfg.storage.dataset_kind == "personal"


def test_missing_explicit_config_dir_errors(tmp_path: Path) -> None:
    with pytest.raises(ConfigError):
        load_config(tmp_path / "nope")
