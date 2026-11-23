"""Storage abstraction. Personal data is reachable only through these interfaces (A2)."""

from careeros.storage.base import BlobRef, BlobStore, PersonalDataStore
from careeros.storage.blob import LocalBlobStore
from careeros.storage.classification import OPERATIONAL_TABLES, PERSONAL_TABLES, classify
from careeros.storage.location import resolve_data_dir
from careeros.storage.sqlite_personal import SQLitePersonalStore

__all__ = [
    "OPERATIONAL_TABLES",
    "PERSONAL_TABLES",
    "BlobRef",
    "BlobStore",
    "LocalBlobStore",
    "PersonalDataStore",
    "SQLitePersonalStore",
    "classify",
    "resolve_data_dir",
]
