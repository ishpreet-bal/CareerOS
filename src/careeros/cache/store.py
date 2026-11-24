"""Cache of model responses that were *accepted* after review.

Only accepted results are cached, so a retry after a rejected answer is never served the
same rejected answer. The key covers everything that changes the answer.
"""

from __future__ import annotations

import hashlib
from datetime import timedelta
from typing import Any

from careeros.core.clock import utc_now, utc_now_iso
from careeros.core.db import Database, to_json
from careeros.llm.contracts import ModelRequest


def cache_key(model_id: str, request: ModelRequest, template_version: str = "1") -> str:
    material = to_json(
        {
            "model": model_id,
            "effort": request.effort,
            "task_type": request.task_type,
            "system": request.system,
            "prompt": request.prompt,
            "schema": request.output_schema.to_dict(),
            "template_version": template_version,
        }
    )
    return hashlib.sha256(material.encode()).hexdigest()


class CacheStore:
    def __init__(self, db: Database, ttl_days: int = 30) -> None:
        self.db = db
        self.ttl = timedelta(days=ttl_days)

    def get(self, key: str) -> str | None:
        r = self.db.query_one(
            "SELECT value FROM cache_entries WHERE key=?"
            " AND (expires_at IS NULL OR expires_at > ?)",
            (key, utc_now_iso()),
        )
        if r is None:
            return None
        with self.db.transaction() as c:
            c.execute("UPDATE cache_entries SET hits = hits + 1 WHERE key=?", (key,))
        return str(r["value"])

    def put(self, key: str, value: str, kind: str = "model_response") -> None:
        with self.db.transaction() as c:
            self.put_in(c, key, value, kind)

    def put_in(self, c: Any, key: str, value: str, kind: str = "model_response") -> None:
        """Write inside the caller's transaction (e.g. a fenced accept)."""
        c.execute(
            "INSERT INTO cache_entries (key, kind, value, created_at, expires_at)"
            " VALUES (?,?,?,?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value,"
            " expires_at=excluded.expires_at",
            (key, kind, value, utc_now_iso(), (utc_now() + self.ttl).isoformat()),
        )
