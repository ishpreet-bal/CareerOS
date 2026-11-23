"""Wires configuration, storage location, database and repositories together."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from careeros.core.config import AppConfig
from careeros.core.db import DB_FILENAME, Database
from careeros.domain.repositories import CompanyRepository, JobRepository, OrchestrationRepository
from careeros.storage.base import PersonalDataStore
from careeros.storage.blob import LocalBlobStore
from careeros.storage.location import resolve_data_dir
from careeros.storage.sqlite_personal import SQLitePersonalStore


@dataclass
class AppContext:
    config: AppConfig
    data_dir: Path
    db: Database
    personal: PersonalDataStore
    companies: CompanyRepository
    jobs: JobRepository
    orchestration: OrchestrationRepository

    def close(self) -> None:
        self.db.close()

    def __enter__(self) -> AppContext:
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()


def open_context(config: AppConfig) -> AppContext:
    """Open (creating if needed) the data directory and database for ``config``."""
    return open_context_at(config, resolve_data_dir(config.storage))


def open_context_at(config: AppConfig, data_dir: Path, *, thread_bound: bool = True) -> AppContext:
    """Open a context on an already-resolved data directory.

    Each call creates a *new* SQLite connection; concurrent workers each use their own.
    """
    data_dir.mkdir(parents=True, exist_ok=True)
    db = Database.open(
        data_dir / DB_FILENAME, dataset_kind=config.storage.dataset_kind, thread_bound=thread_bound
    )
    blobs = LocalBlobStore(data_dir / "blobs")
    return AppContext(
        config=config,
        data_dir=data_dir,
        db=db,
        personal=SQLitePersonalStore(db, blobs),
        companies=CompanyRepository(db),
        jobs=JobRepository(db),
        orchestration=OrchestrationRepository(db),
    )
