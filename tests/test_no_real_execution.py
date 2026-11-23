"""Proof that no real model provider can be called in M2 (requirement 4)."""

import ast
import socket
from pathlib import Path
from typing import Any, ClassVar

import pytest
import yaml

from careeros.context import AppContext
from careeros.core.config import (
    AppConfig,
    ExecutionConfig,
    ModelProfile,
    Pricing,
    load_config,
)
from careeros.core.errors import ConfigError
from careeros.llm.adapters.mock import MockAdapter
from careeros.llm.contracts import ModelRequest, ModelResponse, RealExecutionBlockedError
from careeros.llm.gateway import CallContext
from careeros.llm.registry import AdapterRegistry
from careeros.llm.safety import check_call_allowed
from careeros.simulation import run_scenario
from tests.conftest import CONFIG_DIR, REPO_ROOT
from tests.m2_helpers import DEMO, calls, manager

FORBIDDEN_IMPORTS = {
    "anthropic",
    "openai",
    "httpx",
    "requests",
    "urllib.request",
    "urllib3",
    "socket",
    "http.client",
    "aiohttp",
    "google.generativeai",
}


class FakeRealAdapter:
    provider: ClassVar[str] = "anthropic"
    offline: ClassVar[bool] = False
    called = False

    def call(self, profile: ModelProfile, request: ModelRequest) -> ModelResponse:
        FakeRealAdapter.called = True
        raise AssertionError("a real adapter must never be invoked")

    def map_effort(self, effort: Any) -> dict[str, Any]:
        return {}


class LyingAdapter(FakeRealAdapter):
    """Claims to be offline but serves a real provider name."""

    offline: ClassVar[bool] = True


def _real_profile() -> ModelProfile:
    # model_construct bypasses validation, as a bug or tampering might.
    return ModelProfile.model_construct(
        id="real-tier1",
        provider="anthropic",
        provider_model="x",
        tier=1,
        capabilities=("reasoning",),
        context_window=1000,
        effort_levels=("low",),
        pricing=Pricing(input_per_mtok=1.0, output_per_mtok=1.0, simulated=False),
        simulation=None,
        enabled=True,
    )


def test_config_cannot_enable_a_real_provider(tmp_path: Path) -> None:
    d = tmp_path / "cfg"
    d.mkdir()
    (d / "models.yaml").write_text(
        yaml.safe_dump(
            {
                "models": [
                    {
                        "id": "real",
                        "provider": "anthropic",
                        "provider_model": "any",
                        "tier": 1,
                        "pricing": {"input_per_mtok": 1, "output_per_mtok": 1, "simulated": False},
                        "enabled": True,
                    }
                ]
            }
        )
    )
    with pytest.raises(ConfigError, match="cannot be enabled"):
        load_config(d)


@pytest.mark.parametrize("mode", ["real", "manual"])
def test_environment_cannot_enable_real_execution(
    monkeypatch: pytest.MonkeyPatch, mode: str
) -> None:
    monkeypatch.setenv("CAREEROS_EXECUTION_MODE", mode)
    monkeypatch.setenv("CAREEROS_ALLOW_REAL_MODELS", "1")
    monkeypatch.setenv("ANTHROPIC_API_KEY", "sk-test-not-real")
    with pytest.raises(ConfigError):
        load_config(CONFIG_DIR)


def test_api_key_in_environment_changes_nothing(
    monkeypatch: pytest.MonkeyPatch, ctx: AppContext
) -> None:
    monkeypatch.setenv("ANTHROPIC_API_KEY", "sk-test-not-real")
    monkeypatch.setenv("OPENAI_API_KEY", "sk-test-not-real")
    m = manager(ctx)
    oid = m.submit(DEMO)
    m.run(oid)
    rows = calls(ctx)
    assert rows and {r["provider"] for r in rows} == {"mock"}


def test_registry_refuses_real_adapters() -> None:
    reg = AdapterRegistry()
    with pytest.raises(RealExecutionBlockedError):
        reg.register(FakeRealAdapter())  # type: ignore[arg-type]
    with pytest.raises(RealExecutionBlockedError):
        reg.register(LyingAdapter())  # type: ignore[arg-type]
    reg.register(MockAdapter())
    with pytest.raises(RealExecutionBlockedError):
        reg.for_profile(_real_profile())


def test_gateway_refuses_real_profile_even_if_validation_is_bypassed(ctx: AppContext) -> None:
    bypassed = AppConfig.model_construct(
        **{**dict(ctx.config), "models": (*ctx.config.models, _real_profile())}
    )
    ctx.config = bypassed
    m = manager(ctx)
    with pytest.raises(RealExecutionBlockedError):
        m.gateway.call(
            "real-tier1",
            ModelRequest(task_type="job.analyze", prompt="x"),
            CallContext(objective_id=m.submit(DEMO)),
        )
    assert not FakeRealAdapter.called
    assert calls(ctx, model_id="real-tier1") == []


def test_safety_gate_checks_mode_provider_and_adapter(cfg: AppConfig) -> None:
    mock_profile = next(m for m in cfg.models if m.provider == "mock")
    check_call_allowed(cfg, mock_profile, MockAdapter())  # allowed
    real_mode = AppConfig.model_construct(
        **{
            **dict(cfg),
            "execution": ExecutionConfig.model_construct(
                mode="real", allow_real_models=True, max_concurrency=1
            ),
        }
    )
    with pytest.raises(RealExecutionBlockedError):
        check_call_allowed(real_mode, mock_profile, MockAdapter())
    with pytest.raises(RealExecutionBlockedError):
        check_call_allowed(cfg, _real_profile(), MockAdapter())
    with pytest.raises(RealExecutionBlockedError):
        check_call_allowed(cfg, mock_profile, FakeRealAdapter())  # type: ignore[arg-type]


def test_full_runs_make_no_network_connections(
    monkeypatch: pytest.MonkeyPatch, ctx: AppContext
) -> None:
    def blocked(*a: object, **k: object) -> None:
        raise AssertionError("network access attempted")

    monkeypatch.setattr(socket.socket, "connect", blocked)
    monkeypatch.setattr(socket.socket, "connect_ex", blocked)
    monkeypatch.setattr(socket, "create_connection", blocked)
    monkeypatch.setattr(socket, "getaddrinfo", blocked)
    for scenario in ("standard", "escalation", "novel"):
        run_scenario(ctx, scenario)
    assert calls(ctx)


def test_every_recorded_call_is_simulated(ctx: AppContext) -> None:
    run_scenario(ctx, "escalation")
    rows = calls(ctx)
    assert rows
    assert all(r["cost_is_simulated"] == 1 for r in rows)
    assert {r["mode"] for r in rows} <= {"mock", "scripted"}
    assert {r["provider"] for r in rows} == {"mock"}


def test_source_has_no_network_or_vendor_sdk_imports() -> None:
    offenders = []
    for path in (REPO_ROOT / "src" / "careeros").rglob("*.py"):
        tree = ast.parse(path.read_text())
        for node in ast.walk(tree):
            names = []
            if isinstance(node, ast.Import):
                names = [a.name for a in node.names]
            elif isinstance(node, ast.ImportFrom) and node.module:
                names = [node.module]
            for n in names:
                if any(n == f or n.startswith(f + ".") for f in FORBIDDEN_IMPORTS):
                    offenders.append(f"{path.name}: {n}")
    assert offenders == []


def test_only_offline_adapters_exist() -> None:
    adapters_dir = REPO_ROOT / "src" / "careeros" / "llm" / "adapters"
    assert sorted(p.name for p in adapters_dir.glob("*.py")) == [
        "__init__.py",
        "mock.py",
        "scripted.py",
    ]
