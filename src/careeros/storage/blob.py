"""Content-addressed local file storage for documents (resumes, letters, samples)."""

from __future__ import annotations

import hashlib
import re
from pathlib import Path

from careeros.core.errors import NotFoundError
from careeros.storage.base import BlobRef

_KEY_RE = re.compile(r"^[0-9a-f]{64}(\.[a-z0-9]{1,10})?$")


class LocalBlobStore:
    """Stores blobs under ``root/ab/cd/<sha256><suffix>``. Writes are atomic."""

    def __init__(self, root: Path) -> None:
        self.root = root
        root.mkdir(parents=True, exist_ok=True)

    def _path(self, key: str) -> Path:
        if not _KEY_RE.match(key):
            raise ValueError(f"Invalid blob key {key!r}")
        return self.root / key[:2] / key[2:4] / key

    def put(self, data: bytes, *, suffix: str = "") -> BlobRef:
        if suffix and not re.fullmatch(r"\.[a-z0-9]{1,10}", suffix):
            raise ValueError(f"Invalid suffix {suffix!r}")
        digest = hashlib.sha256(data).hexdigest()
        key = digest + suffix
        path = self._path(key)
        if not path.exists():
            path.parent.mkdir(parents=True, exist_ok=True)
            tmp = path.with_name(path.name + ".tmp")
            tmp.write_bytes(data)
            tmp.replace(path)
        return BlobRef(key=key, sha256=digest, size=len(data))

    def get(self, key: str) -> bytes:
        path = self._path(key)
        if not path.exists():
            raise NotFoundError(f"Blob {key} not found")
        return path.read_bytes()

    def exists(self, key: str) -> bool:
        return self._path(key).exists()

    def delete(self, key: str) -> None:
        self._path(key).unlink(missing_ok=True)
