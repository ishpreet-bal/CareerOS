import sqlite3
from pathlib import Path

import pytest

from careeros.core.db import Database, load_migrations
from careeros.core.errors import DatasetKindMismatchError
from careeros.storage.classification import OPERATIONAL_TABLES, PERSONAL_TABLES


def test_migrations_create_all_classified_tables(tmp_path: Path) -> None:
    with Database.open(tmp_path / "t.db", dataset_kind="synthetic") as db:
        tables = set(db.table_names())
        assert tables == PERSONAL_TABLES | OPERATIONAL_TABLES, "every table must be classified"
        assert db.schema_version() == max(m.version for m in load_migrations())


def test_migrate_is_idempotent(tmp_path: Path) -> None:
    path = tmp_path / "t.db"
    with Database.open(path, dataset_kind="synthetic") as db:
        assert db.migrate() == []
    with Database.open(path, dataset_kind="synthetic") as db:
        assert db.migrate() == []


def test_pragmas(tmp_path: Path) -> None:
    with Database.open(tmp_path / "t.db", dataset_kind="synthetic") as db:
        assert db.query_one("PRAGMA foreign_keys")[0] == 1
        assert db.query_one("PRAGMA journal_mode")[0] == "wal"


def test_foreign_keys_enforced(tmp_path: Path) -> None:
    with (
        Database.open(tmp_path / "t.db", dataset_kind="synthetic") as db,
        pytest.raises(sqlite3.IntegrityError),
    ):
        db.conn.execute(
            "INSERT INTO jobs (company_id, title, dedupe_key, first_seen_at, last_seen_at)"
            " VALUES (999, 't', 'k', 'now', 'now')"
        )


def test_dataset_kind_cannot_be_mixed(tmp_path: Path) -> None:
    path = tmp_path / "t.db"
    Database.open(path, dataset_kind="synthetic").close()
    with pytest.raises(DatasetKindMismatchError):
        Database.open(path, dataset_kind="personal")


def test_transaction_rolls_back(tmp_path: Path) -> None:
    with Database.open(tmp_path / "t.db", dataset_kind="synthetic") as db:
        with pytest.raises(RuntimeError), db.transaction():
            db.set_meta("probe", "1")
            raise RuntimeError("boom")
        assert db.get_meta("probe") is None


def test_json_columns_validated(tmp_path: Path) -> None:
    with (
        Database.open(tmp_path / "t.db", dataset_kind="synthetic") as db,
        pytest.raises(sqlite3.IntegrityError),
    ):
        db.conn.execute("INSERT INTO skills (name, evidence_fact_ids) VALUES ('x', 'not json')")


def test_simulated_calls_cannot_be_recorded_as_real_cost(tmp_path: Path) -> None:
    with Database.open(tmp_path / "t.db", dataset_kind="synthetic") as db:
        db.conn.execute(
            "INSERT INTO model_profiles (id, provider, tier, pricing_simulated, enabled,"
            " updated_at) VALUES ('mock-small', 'mock', 3, 1, 1, 'now')"
        )
        with pytest.raises(sqlite3.IntegrityError):
            db.conn.execute(
                "INSERT INTO model_calls (model_id, provider, mode, cost_is_simulated, status,"
                " created_at) VALUES ('mock-small', 'mock', 'mock', 0, 'ok', 'now')"
            )


def test_cost_per_success_view(tmp_path: Path) -> None:
    with Database.open(tmp_path / "t.db", dataset_kind="synthetic") as db:
        assert db.query("SELECT * FROM v_cost_per_success") == []
