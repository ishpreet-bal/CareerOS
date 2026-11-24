"""Fencing tokens: every write that depends on owning a task proves it, atomically.

A claim increments ``tasks.owner_generation`` and records the owning session and lease.
The resulting :class:`Fence` must accompany every task-owned write. ``check_fence`` runs
*inside* the writer's ``BEGIN IMMEDIATE`` transaction, so the ownership check and the
writes that follow are one atomic step: a takeover can never interleave, and a stale
owner (older generation, other session, expired lease, task no longer running) has its
whole transaction rolled back.
"""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass

from careeros.core.errors import OwnershipLostError

RUNNABLE = ("pending", "ready", "retry_scheduled")


@dataclass(frozen=True)
class Fence:
    task_id: int
    generation: int
    session: str
    attempt: int

    def token(self) -> str:
        return f"task:{self.task_id}:gen:{self.generation}"


def fence_valid(c: sqlite3.Connection, fence: Fence, now_iso: str) -> bool:
    """True iff ``fence`` is still the current, live owner. Touches the row so the check
    holds the write lock for the rest of the transaction."""
    cur = c.execute(
        "UPDATE tasks SET updated_at=? WHERE id=? AND owner_generation=? AND lease_owner=?"
        " AND status='running' AND lease_expires_at > ?",
        (now_iso, fence.task_id, fence.generation, fence.session, now_iso),
    )
    return cur.rowcount == 1


def check_fence(c: sqlite3.Connection, fence: Fence, now_iso: str) -> None:
    if not fence_valid(c, fence, now_iso):
        raise OwnershipLostError(
            f"{fence.token()} (session {fence.session}, attempt {fence.attempt}) no longer "
            "owns the task; write rejected"
        )


#: The claim-time readiness rule (audit L10), as a condition on a ``tasks`` row. It
#: mirrors the dispatcher (``CareerManager._ready_tasks``) so a claim can never run work
#: that a stale readiness snapshot let through: the task is runnable, its objective is
#: active, and every prerequisite (hard or soft, as the dispatcher treats them) has
#: succeeded. Static SQL; the id is the only parameter.
CLAIMABLE_SQL = (
    "status IN ('pending','ready','retry_scheduled')"
    " AND EXISTS (SELECT 1 FROM objectives o WHERE o.id = tasks.objective_id"
    "   AND o.status = 'active')"
    " AND NOT EXISTS (SELECT 1 FROM task_dependencies d JOIN tasks p"
    "   ON p.id = d.depends_on_task_id WHERE d.task_id = tasks.id"
    "   AND p.status <> 'succeeded')"
)

_CLAIM_SQL = (
    "UPDATE tasks SET status='running', owner_generation=owner_generation+1,"  # noqa: S608
    " attempt = attempt + (1 - attempt_open), attempt_open = 1,"
    " lease_owner=?, lease_expires_at=?, started_at=COALESCE(started_at, ?), updated_at=?"
    " WHERE id=? AND " + CLAIMABLE_SQL  # module constants only: no external input
)


def claim(
    c: sqlite3.Connection, task_id: int, session: str, lease_iso: str, now_iso: str
) -> Fence | None:
    """Take a runnable task. Resumes the same attempt if the last one is still open
    (interrupted, requeued or blocked before an answer); otherwise starts the next one.

    Returns None unless the task satisfies :data:`CLAIMABLE_SQL` at the moment of the
    claim, checked in the same statement that takes it."""
    cur = c.execute(_CLAIM_SQL, (session, lease_iso, now_iso, now_iso, task_id))
    if cur.rowcount != 1:
        return None
    r = c.execute("SELECT owner_generation, attempt FROM tasks WHERE id=?", (task_id,)).fetchone()
    return Fence(task_id, int(r["owner_generation"]), session, int(r["attempt"]))
