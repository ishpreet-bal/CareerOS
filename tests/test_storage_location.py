from pathlib import Path

import pytest

from careeros.core.config import StorageConfig
from careeros.core.errors import StorageLocationError
from careeros.storage.classification import OPERATIONAL_TABLES, PERSONAL_TABLES, classify
from careeros.storage.location import default_data_root, resolve_data_dir
from tests.conftest import REPO_ROOT


def test_data_dir_inside_repo_refused() -> None:
    with pytest.raises(StorageLocationError):
        resolve_data_dir(StorageConfig(data_dir=REPO_ROOT / "data"))


def test_data_dir_inside_any_git_tree_refused(tmp_path: Path) -> None:
    (tmp_path / ".git").mkdir()
    with pytest.raises(StorageLocationError):
        resolve_data_dir(StorageConfig(data_dir=tmp_path / "nested" / "dir"))


def test_explicit_dir_outside_git_accepted(tmp_path: Path) -> None:
    assert resolve_data_dir(StorageConfig(data_dir=tmp_path / "d")) == (tmp_path / "d").resolve()


def test_default_dir_separates_dataset_kinds(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.setenv("HOME", str(tmp_path))
    syn = resolve_data_dir(StorageConfig(dataset_kind="synthetic"))
    per = resolve_data_dir(StorageConfig(dataset_kind="personal"))
    assert syn != per
    assert syn.parent == per.parent == default_data_root().resolve()


def test_classification_is_disjoint() -> None:
    assert not PERSONAL_TABLES & OPERATIONAL_TABLES
    assert classify("candidate_profile") == "personal"
    assert classify("tasks") == "operational"
    with pytest.raises(KeyError):
        classify("unclassified_table")
