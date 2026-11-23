from __future__ import annotations

from collections.abc import Iterator
from pathlib import Path

import pytest

from careeros.context import AppContext, open_context
from careeros.core.config import AppConfig, load_config

REPO_ROOT = Path(__file__).resolve().parents[1]
CONFIG_DIR = REPO_ROOT / "config"


@pytest.fixture(autouse=True)
def _clean_env(monkeypatch: pytest.MonkeyPatch) -> None:
    for var in (
        "CAREEROS_CONFIG_DIR",
        "CAREEROS_DATA_DIR",
        "CAREEROS_DATASET_KIND",
        "CAREEROS_EXECUTION_MODE",
    ):
        monkeypatch.delenv(var, raising=False)


@pytest.fixture
def data_dir(tmp_path: Path) -> Path:
    return tmp_path / "careeros-data"


@pytest.fixture
def cfg(data_dir: Path, monkeypatch: pytest.MonkeyPatch) -> AppConfig:
    monkeypatch.setenv("CAREEROS_DATA_DIR", str(data_dir))
    return load_config(CONFIG_DIR)


@pytest.fixture
def ctx(cfg: AppConfig) -> Iterator[AppContext]:
    with open_context(cfg) as c:
        yield c
