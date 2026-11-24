"""Online backup, verification and restore of the CareerOS database (audit L8).

Backups use SQLite's online backup API, never a file copy: copying ``careeros.db`` while
it is in use can miss committed transactions still in the ``-wal`` file, or capture a
torn mid-write state. The backup API copies a transactionally consistent snapshot (one
read transaction; in WAL mode readers do not block writers, so active workers, leases,
reservations and heartbeats carry on unaffected).

Every backup is written to a temporary file next to its destination, checked with
``PRAGMA integrity_check`` and ``PRAGMA foreign_key_check``, synced, and only then
atomically renamed into place - so a destination path either holds a verified backup or
nothing. A backup file contains everything in the database, including any personal data:
it must be stored like the database itself (never inside a git working tree).

Restore copies a verified backup *into* the live database through the same API (so other
connections never see a half-restored file), after taking a safety backup of the current
contents, and refuses while a CareerOS session is still running unless forced.
"""

from __future__ import annotations

import contextlib
import os
import sqlite3
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from pathlib import Path

from careeros.core.clock import utc_now
from careeros.core.errors import DurabilityError

#: A session whose heartbeat is newer than this is considered alive (restore refuses).
LIVE_SESSION_SECONDS = 300


@dataclass(frozen=True)
class VerifyResult:
    path: Path
    ok: bool
    problems: list[str] = field(default_factory=list)
    schema_version: int | None = None
    dataset_kind: str | None = None
    pages: int | None = None

    def summary(self) -> str:
        if self.ok:
            return (
                f"{self.path}: OK (integrity_check ok, schema v{self.schema_version}, "
                f"dataset {self.dataset_kind}, {self.pages} pages)"
            )
        return f"{self.path}: FAILED - " + "; ".join(self.problems)


def verify_backup(path: Path | str) -> VerifyResult:
    """Check a backup (or any CareerOS database file) without modifying it: SQLite
    ``integrity_check`` and ``foreign_key_check``, plus the CareerOS schema markers.
    Never raises for a bad file; the result says what is wrong."""
    p = Path(path)
    if not p.is_file():
        return VerifyResult(p, False, ["file does not exist"])
    try:
        conn = sqlite3.connect(f"file:{p}?mode=ro", uri=True)
    except sqlite3.Error as exc:
        return VerifyResult(p, False, [f"cannot open: {exc}"])
    try:
        problems: list[str] = []
        try:
            rows = [str(r[0]) for r in conn.execute("PRAGMA integrity_check").fetchall()]
        except sqlite3.DatabaseError as exc:
            return VerifyResult(p, False, [f"not a valid SQLite database: {exc}"])
        if rows != ["ok"]:
            problems.extend(f"integrity_check: {r}" for r in rows[:20])
        fk = conn.execute("PRAGMA foreign_key_check").fetchall()
        if fk:
            problems.append(f"foreign_key_check: {len(fk)} violation(s)")
        version = kind = None
        try:
            version = conn.execute("SELECT MAX(version) FROM schema_migrations").fetchone()[0]
            r = conn.execute("SELECT value FROM meta WHERE key='dataset_kind'").fetchone()
            kind = r[0] if r else None
        except sqlite3.DatabaseError as exc:
            problems.append(f"not a CareerOS database: {exc}")
        if not problems and (version is None or kind is None):
            problems.append("not a CareerOS database: missing schema version or dataset kind")
        pages = int(conn.execute("PRAGMA page_count").fetchone()[0])
        return VerifyResult(p, not problems, problems, version, kind, pages)
    except sqlite3.DatabaseError as exc:
        return VerifyResult(p, False, [f"not a valid SQLite database: {exc}"])
    finally:
        conn.close()


def _fsync_dir(directory: Path) -> None:
    with contextlib.suppress(OSError):  # not supported on every platform/filesystem
        fd = os.open(directory, os.O_RDONLY)
        try:
            os.fsync(fd)
        finally:
            os.close(fd)


def backup_database(
    source: Path | str, dest: Path | str, *, overwrite: bool = False
) -> VerifyResult:
    """Write a verified, consistent backup of the live database ``source`` to ``dest``.

    Raises ``DurabilityError`` if the backup fails verification (nothing is left at
    ``dest``) or ``FileExistsError`` if ``dest`` exists and ``overwrite`` is false."""
    src, dst = Path(source), Path(dest)
    if src.resolve() == dst.resolve():
        raise DurabilityError("A backup cannot overwrite the database it copies.")
    if dst.exists() and not overwrite:
        raise FileExistsError(f"{dst} already exists")
    dst.parent.mkdir(parents=True, exist_ok=True)
    tmp = dst.with_name(f".{dst.name}.partial-{os.getpid()}")
    tmp.unlink(missing_ok=True)
    src_conn = sqlite3.connect(str(src), timeout=30.0)
    try:
        dst_conn = sqlite3.connect(str(tmp))
        try:
            src_conn.backup(dst_conn)  # one consistent snapshot of committed state
            dst_conn.execute("PRAGMA journal_mode = DELETE")  # a self-contained single file
        finally:
            dst_conn.close()
    finally:
        src_conn.close()
    result = verify_backup(tmp)
    if not result.ok:
        tmp.unlink(missing_ok=True)
        raise DurabilityError(f"Backup failed verification: {result.summary()}")
    with tmp.open("rb") as fh:
        os.fsync(fh.fileno())
    tmp.replace(dst)
    _fsync_dir(dst.parent)
    return VerifyResult(dst, True, [], result.schema_version, result.dataset_kind, result.pages)


def live_sessions(database: Path | str, *, now: datetime | None = None) -> list[str]:
    """Sessions of ``database`` that are not ended and have a recent heartbeat."""
    p = Path(database)
    if not p.exists():
        return []
    cutoff = ((now or utc_now()) - timedelta(seconds=LIVE_SESSION_SECONDS)).isoformat(
        timespec="seconds"
    )
    conn = sqlite3.connect(str(p), timeout=30.0)
    try:
        rows = conn.execute(
            "SELECT id FROM execution_sessions WHERE ended_at IS NULL"
            " AND COALESCE(last_heartbeat_at, started_at) >= ?",
            (cutoff,),
        ).fetchall()
    except sqlite3.DatabaseError:
        return []
    finally:
        conn.close()
    return [str(r[0]) for r in rows]


def restore_database(
    backup: Path | str,
    target: Path | str,
    *,
    expected_dataset_kind: str,
    safety_copy: Path | str | None = None,
    force: bool = False,
) -> tuple[VerifyResult, Path | None]:
    """Replace the contents of the live database ``target`` with a verified backup.

    Refuses (``DurabilityError``) if the backup fails verification, holds a different
    dataset kind, or - unless ``force`` - a session is still running against ``target``.
    The current contents are first saved to ``safety_copy`` (a verified backup). The copy
    goes through the backup API into the live file, under SQLite's locks, so other
    connections see either the old or the new database, never a mixture. Returns the
    verification of the restored database and the safety copy's path."""
    b, t = Path(backup), Path(target)
    checked = verify_backup(b)
    if not checked.ok:
        raise DurabilityError(f"Refusing to restore: {checked.summary()}")
    if checked.dataset_kind != expected_dataset_kind:
        raise DurabilityError(
            f"Refusing to restore: backup holds {checked.dataset_kind!r} data, this "
            f"installation is configured for {expected_dataset_kind!r}"
        )
    running = live_sessions(t)
    if running and not force:
        raise DurabilityError(
            f"Refusing to restore while {len(running)} CareerOS session(s) are running "
            f"({', '.join(running[:3])}); stop them first (or force)"
        )
    saved: Path | None = None
    if t.exists():
        stamp = utc_now().strftime("%Y%m%dT%H%M%SZ")
        saved = (
            Path(safety_copy) if safety_copy else t.with_name(f"{t.stem}.pre-restore-{stamp}.db")
        )
        backup_database(t, saved)
    src_conn = sqlite3.connect(f"file:{b}?mode=ro", uri=True)
    try:
        dst_conn = sqlite3.connect(str(t), timeout=30.0)
        try:
            src_conn.backup(dst_conn)
        finally:
            dst_conn.close()
    finally:
        src_conn.close()
    restored = verify_backup(t)
    if not restored.ok:
        raise DurabilityError(f"Restored database failed verification: {restored.summary()}")
    return restored, saved
