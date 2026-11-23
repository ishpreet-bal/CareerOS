"""SQLite connection management and the migration runner.

Each database records its ``dataset_kind`` (``synthetic`` or ``personal``) in ``meta`` so
synthetic and personal data can never be mixed in one file.
"""

from __future__ import annotations

import json
import re
import sqlite3
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from importlib import resources
from pathlib import Path
from typing import Any

from careeros.core.clock import utc_now_iso
from careeros.core.config import DatasetKind
from careeros.core.errors import DatasetKindMismatchError, MigrationError

DB_FILENAME = "careeros.db"
_MIGRATION_RE = re.compile(r"^(\d{4})_([a-z0-9_]+)\.sql$")


@dataclass(frozen=True)
class Migration:
    version: int
    name: str
    sql: str


def load_migrations() -> list[Migration]:
    """Return bundled migrations sorted by version."""
    root = resources.files("careeros.core").joinpath("migrations")
    found: list[Migration] = []
    for entry in root.iterdir():
        m = _MIGRATION_RE.match(entry.name)
        if m:
            found.append(Migration(int(m.group(1)), m.group(2), entry.read_text(encoding="utf-8")))
    found.sort(key=lambda mig: mig.version)
    versions = [mig.version for mig in found]
    if len(set(versions)) != len(versions):
        raise MigrationError(f"Duplicate migration versions: {versions}")
    return found


class Database:
    """A thin wrapper over one SQLite connection with explicit transactions."""

    def __init__(self, conn: sqlite3.Connection, path: Path | None) -> None:
        self.conn = conn
        self.path = path

    # ------------------------------------------------------------------ lifecycle
    @classmethod
    def open(
        cls,
        path: Path | str,
        *,
        dataset_kind: DatasetKind,
        migrate: bool = True,
        thread_bound: bool = True,
    ) -> Database:
        """Open (creating if needed) a database file and verify its dataset kind."""
        p = Path(path)
        if str(p) != ":memory:":
            p.parent.mkdir(parents=True, exist_ok=True)
        # thread_bound=False is only for worker connections: each is still used by exactly one
        # worker thread, but the dispatcher may close it after that worker has exited.
        conn = sqlite3.connect(
            str(p), isolation_level=None, timeout=30.0, check_same_thread=thread_bound
        )
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys = ON")
        conn.execute("PRAGMA busy_timeout = 30000")
        if str(p) != ":memory:":
            conn.execute("PRAGMA journal_mode = WAL")
            conn.execute("PRAGMA synchronous = NORMAL")
        db = cls(conn, None if str(p) == ":memory:" else p)
        try:
            if migrate:
                db.migrate()
            db._bind_dataset_kind(dataset_kind)
        except Exception:
            conn.close()
            raise
        return db

    def close(self) -> None:
        self.conn.close()

    def __enter__(self) -> Database:
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    # ------------------------------------------------------------------ transactions
    @contextmanager
    def transaction(self) -> Iterator[sqlite3.Connection]:
        """Run statements atomically. Nested use is not supported."""
        self.conn.execute("BEGIN IMMEDIATE")
        try:
            yield self.conn
        except BaseException:
            self.conn.execute("ROLLBACK")
            raise
        else:
            self.conn.execute("COMMIT")

    def query(self, sql: str, params: tuple[Any, ...] | dict[str, Any] = ()) -> list[sqlite3.Row]:
        return list(self.conn.execute(sql, params).fetchall())

    def query_one(
        self, sql: str, params: tuple[Any, ...] | dict[str, Any] = ()
    ) -> sqlite3.Row | None:
        row: sqlite3.Row | None = self.conn.execute(sql, params).fetchone()
        return row

    # ------------------------------------------------------------------ migrations
    def _ensure_migrations_table(self) -> None:
        self.conn.execute(
            "CREATE TABLE IF NOT EXISTS schema_migrations ("
            " version INTEGER PRIMARY KEY, name TEXT NOT NULL, applied_at TEXT NOT NULL)"
        )

    def applied_versions(self) -> set[int]:
        self._ensure_migrations_table()
        return {r[0] for r in self.conn.execute("SELECT version FROM schema_migrations")}

    def migrate(self) -> list[int]:
        """Apply pending migrations, each atomically. Returns the versions applied."""
        applied = self.applied_versions()
        newly: list[int] = []
        for mig in load_migrations():
            if mig.version in applied:
                continue
            script = (
                "BEGIN;\n"
                + mig.sql
                + "\nINSERT INTO schema_migrations(version, name, applied_at) VALUES ("
                + f"{mig.version}, '{mig.name}', '{utc_now_iso()}');\nCOMMIT;"
            )
            try:
                self.conn.executescript(script)
            except sqlite3.Error as exc:
                if self.conn.in_transaction:
                    self.conn.execute("ROLLBACK")
                raise MigrationError(f"Migration {mig.version}_{mig.name} failed: {exc}") from exc
            newly.append(mig.version)
        return newly

    def schema_version(self) -> int:
        versions = self.applied_versions()
        return max(versions) if versions else 0

    # ------------------------------------------------------------------ metadata
    def get_meta(self, key: str) -> str | None:
        row = self.query_one("SELECT value FROM meta WHERE key = ?", (key,))
        return None if row is None else str(row["value"])

    def set_meta(self, key: str, value: str) -> None:
        self.conn.execute(
            "INSERT INTO meta(key, value) VALUES (?, ?) "
            "ON CONFLICT(key) DO UPDATE SET value = excluded.value",
            (key, value),
        )

    def _bind_dataset_kind(self, kind: DatasetKind) -> None:
        existing = self.get_meta("dataset_kind")
        if existing is None:
            with self.transaction():
                self.set_meta("dataset_kind", kind)
                self.set_meta("created_at", utc_now_iso())
        elif existing != kind:
            raise DatasetKindMismatchError(
                f"Database holds {existing!r} data but {kind!r} was requested. "
                "Synthetic and personal data must use separate databases."
            )

    @property
    def dataset_kind(self) -> str:
        return self.get_meta("dataset_kind") or "unknown"

    def table_names(self) -> list[str]:
        rows = self.query(
            "SELECT name FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite_%' "
            "ORDER BY name"
        )
        return [str(r["name"]) for r in rows]


# ---------------------------------------------------------------------- JSON helpers
def to_json(value: Any) -> str:
    """Serialize to compact, deterministic JSON for storage."""
    return json.dumps(value, separators=(",", ":"), sort_keys=True, default=str)


def from_json(text: str | None, default: Any = None) -> Any:
    if text is None:
        return default
    return json.loads(text)
