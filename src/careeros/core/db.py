"""SQLite connection management and the migration runner.

Each database records its ``dataset_kind`` (``synthetic`` or ``personal``) in ``meta`` so
synthetic and personal data can never be mixed in one file.
"""

from __future__ import annotations

import json
import re
import sqlite3
import time
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from importlib import resources
from pathlib import Path
from typing import Any

from careeros.core.clock import utc_now_iso
from careeros.core.config import DatasetKind
from careeros.core.errors import DatasetKindMismatchError, DurabilityError, MigrationError

DB_FILENAME = "careeros.db"

#: Durability settings applied to, and verified on, every file-backed connection (audit
#: L8). In WAL mode ``synchronous=FULL`` syncs the WAL on every commit, so a committed
#: transaction (a reservation, a call record, a settlement) survives an operating-system
#: crash or power loss - provided the storage honours fsync. ``NORMAL`` (used before) only
#: survived application crashes: the last commits could roll back after power loss.
#: ``fullfsync`` makes those syncs use F_FULLFSYNC on macOS (whose plain fsync does not
#: flush the drive's cache); it is ignored on other platforms.
REQUIRED_JOURNAL_MODE = "wal"
REQUIRED_SYNCHRONOUS = 2  # FULL
_SYNCHRONOUS_NAMES = {0: "OFF", 1: "NORMAL", 2: "FULL", 3: "EXTRA"}
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
        db = cls(conn, None if str(p) == ":memory:" else p)
        try:
            if str(p) != ":memory:":
                _set_wal(conn)
                conn.execute("PRAGMA synchronous = FULL")
                conn.execute("PRAGMA fullfsync = ON")
                conn.execute("PRAGMA checkpoint_fullfsync = ON")
                db.verify_durability()
            if migrate:
                db.migrate()
            db._bind_dataset_kind(dataset_kind)
        except Exception:
            conn.close()
            raise
        return db

    def close(self) -> None:
        self.conn.close()

    # ------------------------------------------------------------------ durability
    def durability(self) -> dict[str, Any]:
        """The settings actually in effect on this connection (read back from SQLite)."""
        sync = int(self.conn.execute("PRAGMA synchronous").fetchone()[0])
        return {
            "journal_mode": str(self.conn.execute("PRAGMA journal_mode").fetchone()[0]).lower(),
            "synchronous": _SYNCHRONOUS_NAMES.get(sync, str(sync)),
            "synchronous_level": sync,
            "fullfsync": bool(self.conn.execute("PRAGMA fullfsync").fetchone()[0]),
        }

    def verify_durability(self) -> None:
        """Raise ``DurabilityError`` unless this file-backed connection uses WAL with
        ``synchronous`` at least FULL. Checked on every open, so a connection can never
        silently run with weaker settings (e.g. WAL unavailable on the filesystem)."""
        if self.path is None:
            return  # in-memory databases have nothing to make durable
        d = self.durability()
        if (
            d["journal_mode"] != REQUIRED_JOURNAL_MODE
            or d["synchronous_level"] < REQUIRED_SYNCHRONOUS
        ):
            raise DurabilityError(
                f"{self.path}: journal_mode={d['journal_mode']}, synchronous={d['synchronous']};"
                f" required journal_mode={REQUIRED_JOURNAL_MODE}, synchronous=FULL"
            )

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
        self.conn.execute(  # IF NOT EXISTS: idempotent across concurrent openers
            "CREATE TABLE IF NOT EXISTS schema_migrations ("
            " version INTEGER PRIMARY KEY, name TEXT NOT NULL, applied_at TEXT NOT NULL)"
        )

    def applied_versions(self) -> set[int]:
        self._ensure_migrations_table()
        return {r[0] for r in self.conn.execute("SELECT version FROM schema_migrations")}

    def migrate(self) -> list[int]:
        """Apply pending migrations. Returns the versions this call applied.

        Safe when several processes open the same fresh database at once (audit L4): each
        migration runs statement by statement inside one ``BEGIN IMMEDIATE`` transaction
        that first re-checks whether another connection already applied it.
        """
        self._ensure_migrations_table()
        newly: list[int] = []
        for mig in load_migrations():
            if mig.version in self.applied_versions():
                continue
            self.conn.execute("BEGIN IMMEDIATE")
            try:
                done = self.conn.execute(
                    "SELECT 1 FROM schema_migrations WHERE version=?", (mig.version,)
                ).fetchone()
                if done is None:
                    for stmt in split_sql(mig.sql):
                        self.conn.execute(stmt)
                    self.conn.execute(
                        "INSERT INTO schema_migrations(version, name, applied_at) VALUES (?,?,?)",
                        (mig.version, mig.name, utc_now_iso()),
                    )
                    newly.append(mig.version)
                self.conn.execute("COMMIT")
            except sqlite3.Error as exc:
                self.conn.execute("ROLLBACK")
                raise MigrationError(f"Migration {mig.version}_{mig.name} failed: {exc}") from exc
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
        with self.transaction():  # read-then-bind under the write lock (audit L4)
            existing = self.get_meta("dataset_kind")
            if existing is None:
                self.set_meta("dataset_kind", kind)
                self.set_meta("created_at", utc_now_iso())
        if existing is not None and existing != kind:
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
def _set_wal(conn: sqlite3.Connection, attempts: int = 100) -> None:
    """Switch to WAL. On a brand-new file several openers can race for the exclusive lock
    this needs, and SQLite reports SQLITE_BUSY without consulting the busy timeout, so the
    switch is retried with bounded backoff (audit L4)."""
    for i in range(attempts):
        try:
            conn.execute("PRAGMA journal_mode = WAL")
            return
        except sqlite3.OperationalError as exc:
            if "locked" not in str(exc) and "busy" not in str(exc):
                raise
            if i == attempts - 1:
                raise
            time.sleep(min(0.1, 0.005 * (i + 1)))


def split_sql(script: str) -> list[str]:
    """Split a migration script into complete statements (comments are dropped)."""
    out: list[str] = []
    buf = ""
    for line in script.splitlines(keepends=True):
        if not buf and line.strip().startswith("--"):
            continue
        buf += line
        if sqlite3.complete_statement(buf):
            if buf.strip().strip(";").strip():
                out.append(buf.strip())
            buf = ""
    if buf.strip() and not all(
        ln.strip().startswith("--") or not ln.strip() for ln in buf.splitlines()
    ):
        raise MigrationError(f"Incomplete SQL statement at end of migration: {buf[:80]!r}")
    return out


def to_json(value: Any) -> str:
    """Serialize to compact, deterministic JSON for storage."""
    return json.dumps(value, separators=(",", ":"), sort_keys=True, default=str)


def from_json(text: str | None, default: Any = None) -> Any:
    if text is None:
        return default
    return json.loads(text)
