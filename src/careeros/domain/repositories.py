"""Repositories for operational data: companies, jobs, eligibility and orchestration state.

Scheduling, leasing and recovery *logic* belong to M3. This module only persists and reads
state, so it is already resumable: everything written here survives process exit.
"""

from __future__ import annotations

import hashlib
import os
import platform
import re
import sqlite3
import unicodedata
import uuid
from typing import Any

from careeros.core.clock import utc_now_iso
from careeros.core.db import Database, from_json, to_json
from careeros.core.errors import NotFoundError
from careeros.domain.models import (
    Checkpoint,
    Company,
    DependencyKind,
    EligibilityAssessment,
    EligibilityVerdict,
    Job,
    JobStatus,
    Objective,
    ObjectiveStatus,
    Plan,
    Priority,
    RemoteType,
    StoredResult,
    Task,
    TaskStatus,
    WorkerResult,
)


# =============================================================================== helpers
def normalize_name(text: str) -> str:
    """Lowercase, strip accents/punctuation and common company suffixes."""
    t = unicodedata.normalize("NFKD", text).encode("ascii", "ignore").decode()
    t = re.sub(r"[^a-z0-9 ]+", " ", t.lower())
    t = re.sub(r"\b(inc|incorporated|llc|ltd|limited|corp|corporation|co|gmbh|plc)\b", " ", t)
    return re.sub(r"\s+", " ", t).strip()


def make_dedupe_key(company_name: str, title: str, location: str | None, term: str | None) -> str:
    parts = [normalize_name(company_name), normalize_name(title)]
    parts += [normalize_name(location or ""), normalize_name(term or "")]
    return hashlib.sha256("|".join(parts).encode()).hexdigest()[:32]


def _hash_text(text: str | None) -> str | None:
    return None if text is None else hashlib.sha256(text.encode()).hexdigest()


# =============================================================================== companies
class CompanyRepository:
    def __init__(self, db: Database) -> None:
        self.db = db

    @staticmethod
    def _row(r: sqlite3.Row) -> Company:
        return Company(
            id=r["id"],
            name=r["name"],
            domain=r["domain"],
            headquarters=r["headquarters"],
            size_band=r["size_band"],
            industry=r["industry"],
            careers_url=r["careers_url"],
            ats_type=r["ats_type"],
            ats_slug=r["ats_slug"],
            notes=r["notes"],
        )

    def get_or_create(self, company: Company) -> Company:
        norm = normalize_name(company.name)
        existing = self.db.query_one("SELECT * FROM companies WHERE normalized_name=?", (norm,))
        if existing is not None:
            return self._row(existing)
        with self.db.transaction() as c:
            cur = c.execute(
                "INSERT INTO companies (name, normalized_name, domain, headquarters, size_band,"
                " industry, careers_url, ats_type, ats_slug, notes, created_at)"
                " VALUES (?,?,?,?,?,?,?,?,?,?,?)",
                (
                    company.name,
                    norm,
                    company.domain,
                    company.headquarters,
                    company.size_band,
                    company.industry,
                    company.careers_url,
                    company.ats_type,
                    company.ats_slug,
                    company.notes,
                    utc_now_iso(),
                ),
            )
        assert cur.lastrowid is not None
        return self.get(cur.lastrowid)

    def get(self, company_id: int) -> Company:
        r = self.db.query_one("SELECT * FROM companies WHERE id=?", (company_id,))
        if r is None:
            raise NotFoundError(f"Company {company_id} not found")
        return self._row(r)

    def count(self) -> int:
        row = self.db.query_one("SELECT COUNT(*) AS n FROM companies")
        return int(row["n"]) if row else 0


# =============================================================================== jobs
class JobRepository:
    def __init__(self, db: Database) -> None:
        self.db = db

    @staticmethod
    def _row(r: sqlite3.Row) -> Job:
        return Job(
            id=r["id"],
            company_id=r["company_id"],
            source_id=r["source_id"],
            external_id=r["external_id"],
            url=r["url"],
            title=r["title"],
            role_family=r["role_family"],
            location_raw=r["location_raw"],
            countries=from_json(r["countries"], []),
            remote_type=RemoteType(r["remote_type"]),
            employment_type=r["employment_type"],
            term=r["term"],
            posted_at=r["posted_at"],
            closes_at=r["closes_at"],
            description_raw=r["description_raw"],
            status=JobStatus(r["status"]),
        )

    def get(self, job_id: int) -> Job:
        r = self.db.query_one("SELECT * FROM jobs WHERE id=?", (job_id,))
        if r is None:
            raise NotFoundError(f"Job {job_id} not found")
        return self._row(r)

    def upsert(self, job: Job) -> tuple[Job, bool]:
        """Insert a job unless it already exists. Returns ``(job, created)``.

        A job is the same if it has the same (source, external id), or the same dedupe key
        (normalized company, title, location and term).
        """
        company = self.db.query_one("SELECT name FROM companies WHERE id=?", (job.company_id,))
        if company is None:
            raise NotFoundError(f"Company {job.company_id} not found")
        key = make_dedupe_key(company["name"], job.title, job.location_raw, job.term)
        now = utc_now_iso()

        existing = None
        if job.source_id is not None and job.external_id is not None:
            existing = self.db.query_one(
                "SELECT id FROM jobs WHERE source_id=? AND external_id=?",
                (job.source_id, job.external_id),
            )
        if existing is None:
            existing = self.db.query_one("SELECT id FROM jobs WHERE dedupe_key=?", (key,))
        if existing is not None:
            with self.db.transaction() as c:
                c.execute("UPDATE jobs SET last_seen_at=? WHERE id=?", (now, existing["id"]))
            return self.get(existing["id"]), False

        with self.db.transaction() as c:
            cur = c.execute(
                "INSERT INTO jobs (company_id, source_id, external_id, url, title, role_family,"
                " location_raw, countries, remote_type, employment_type, term, posted_at,"
                " closes_at, description_raw, description_hash, dedupe_key, status,"
                " first_seen_at, last_seen_at) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (
                    job.company_id,
                    job.source_id,
                    job.external_id,
                    job.url,
                    job.title,
                    job.role_family.value if job.role_family else None,
                    job.location_raw,
                    to_json(job.countries),
                    job.remote_type.value,
                    job.employment_type,
                    job.term,
                    job.posted_at,
                    job.closes_at,
                    job.description_raw,
                    _hash_text(job.description_raw),
                    key,
                    job.status.value,
                    now,
                    now,
                ),
            )
        assert cur.lastrowid is not None
        return self.get(cur.lastrowid), True

    def count(self) -> int:
        row = self.db.query_one("SELECT COUNT(*) AS n FROM jobs")
        return int(row["n"]) if row else 0

    # ---------------------------------------------------------------- eligibility
    def save_eligibility(self, a: EligibilityAssessment) -> EligibilityAssessment:
        with self.db.transaction() as c:
            c.execute(
                "INSERT INTO eligibility_assessments (job_id, profile_version, verdict, reasons,"
                " blocking_criteria, unknown_inputs, confidence, task_id, assessed_at)"
                " VALUES (?,?,?,?,?,?,?,?,?)"
                " ON CONFLICT(job_id, profile_version) DO UPDATE SET verdict=excluded.verdict,"
                " reasons=excluded.reasons, blocking_criteria=excluded.blocking_criteria,"
                " unknown_inputs=excluded.unknown_inputs, confidence=excluded.confidence,"
                " task_id=excluded.task_id, assessed_at=excluded.assessed_at",
                (
                    a.job_id,
                    a.profile_version,
                    a.verdict.value,
                    to_json(a.reasons),
                    to_json(a.blocking_criteria),
                    to_json(a.unknown_inputs),
                    a.confidence,
                    a.task_id,
                    utc_now_iso(),
                ),
            )
        got = self.get_eligibility(a.job_id, a.profile_version)
        assert got is not None
        return got

    def get_eligibility(self, job_id: int, profile_version: int) -> EligibilityAssessment | None:
        r = self.db.query_one(
            "SELECT * FROM eligibility_assessments WHERE job_id=? AND profile_version=?",
            (job_id, profile_version),
        )
        if r is None:
            return None
        return EligibilityAssessment(
            id=r["id"],
            job_id=r["job_id"],
            profile_version=r["profile_version"],
            verdict=EligibilityVerdict(r["verdict"]),
            reasons=from_json(r["reasons"], []),
            blocking_criteria=from_json(r["blocking_criteria"], []),
            unknown_inputs=from_json(r["unknown_inputs"], []),
            confidence=r["confidence"],
            task_id=r["task_id"],
        )


# =============================================================================== orchestration
OUTSTANDING_STATUSES = (
    TaskStatus.PENDING,
    TaskStatus.READY,
    TaskStatus.RUNNING,
    TaskStatus.REVIEWING,
    TaskStatus.RETRY_SCHEDULED,
    TaskStatus.BLOCKED_BUDGET,
    TaskStatus.AWAITING_HUMAN,
)


class OrchestrationRepository:
    """Durable objective/plan/task/result/checkpoint state (architecture §7.3, A7)."""

    def __init__(self, db: Database) -> None:
        self.db = db

    # ---------------------------------------------------------------- sessions
    def start_session(self) -> str:
        sid = f"{platform.node()}-{os.getpid()}-{uuid.uuid4().hex[:8]}"
        now = utc_now_iso()
        with self.db.transaction() as c:
            c.execute(
                "INSERT INTO execution_sessions (id, started_at, last_heartbeat_at, host)"
                " VALUES (?,?,?,?)",
                (sid, now, now, platform.node()),
            )
        return sid

    def end_session(self, session_id: str, reason: str = "completed") -> None:
        """Mark a session ended. A session already ended (e.g. presumed dead by another
        session's recovery) keeps its first end time and reason (finding N2)."""
        with self.db.transaction() as c:
            c.execute(
                "UPDATE execution_sessions SET ended_at=COALESCE(ended_at, ?),"
                " end_reason=CASE WHEN ended_at IS NULL THEN ? ELSE end_reason END WHERE id=?",
                (utc_now_iso(), reason, session_id),
            )

    # ---------------------------------------------------------------- objectives
    @staticmethod
    def _row_objective(r: sqlite3.Row) -> Objective:
        return Objective(
            id=r["id"],
            kind=r["kind"],
            title=r["title"],
            inputs=from_json(r["inputs"], {}),
            definition_of_done=from_json(r["definition_of_done"], []),
            priority=Priority(r["priority"]),
            budget_usd=r["budget_usd"],
            budget_tokens=r["budget_tokens"],
            status=ObjectiveStatus(r["status"]),
            stop_reason=r["stop_reason"],
        )

    def create_objective(self, obj: Objective) -> Objective:
        with self.db.transaction() as c:
            oid = self.create_objective_in(c, obj)
        return self.get_objective(oid)

    @staticmethod
    def create_objective_in(c: sqlite3.Connection, obj: Objective) -> int:
        now = utc_now_iso()
        cur = c.execute(
            "INSERT INTO objectives (kind, title, inputs, definition_of_done, priority,"
            " budget_usd, budget_tokens, status, created_at, updated_at)"
            " VALUES (?,?,?,?,?,?,?,?,?,?)",
            (
                obj.kind,
                obj.title,
                to_json(obj.inputs),
                to_json(obj.definition_of_done),
                obj.priority.value,
                obj.budget_usd,
                obj.budget_tokens,
                obj.status.value,
                now,
                now,
            ),
        )
        assert cur.lastrowid is not None
        return int(cur.lastrowid)

    def get_objective(self, objective_id: int) -> Objective:
        r = self.db.query_one("SELECT * FROM objectives WHERE id=?", (objective_id,))
        if r is None:
            raise NotFoundError(f"Objective {objective_id} not found")
        return self._row_objective(r)

    def list_objectives(self, status: ObjectiveStatus | None = None) -> list[Objective]:
        if status is None:
            rows = self.db.query("SELECT * FROM objectives ORDER BY id")
        else:
            rows = self.db.query(
                "SELECT * FROM objectives WHERE status=? ORDER BY id", (status.value,)
            )
        return [self._row_objective(r) for r in rows]

    def set_objective_status(
        self,
        objective_id: int,
        status: ObjectiveStatus,
        stop_reason: str | None = None,
        *,
        only_from: tuple[ObjectiveStatus, ...] | None = None,
    ) -> bool:
        """Set an objective's status. With ``only_from`` the change is conditional on the
        current status (audit L3); returns whether the row changed."""
        now = utc_now_iso()
        done = now if status in (ObjectiveStatus.DONE, ObjectiveStatus.STOPPED) else None
        allowed = [s.value for s in only_from] if only_from is not None else None
        with self.db.transaction() as c:
            cur = c.execute(
                "UPDATE objectives SET status=?, stop_reason=?, updated_at=?,"
                " completed_at=COALESCE(?, completed_at) WHERE id=?"
                " AND (? IS NULL OR status IN (SELECT value FROM json_each(?)))",
                (
                    status.value,
                    stop_reason,
                    now,
                    done,
                    objective_id,
                    None if allowed is None else 1,
                    to_json(allowed or []),
                ),
            )
        return cur.rowcount == 1

    # ---------------------------------------------------------------- plans
    def create_plan(self, plan: Plan) -> Plan:
        """Create the next plan version; any previous active version is superseded."""
        with self.db.transaction() as c:
            return self.create_plan_in(c, plan)

    @staticmethod
    def create_plan_in(c: sqlite3.Connection, plan: Plan) -> Plan:
        """Inside the caller's transaction: supersede the current version (if any) and
        insert the next one. A unique index allows one current plan per objective."""
        now = utc_now_iso()
        row = c.execute(
            "SELECT COALESCE(MAX(version), 0) AS v FROM plans WHERE objective_id=?",
            (plan.objective_id,),
        ).fetchone()
        version = int(row["v"]) + 1
        c.execute(
            "UPDATE plans SET superseded_at=? WHERE objective_id=? AND superseded_at IS NULL",
            (now, plan.objective_id),
        )
        cur = c.execute(
            "INSERT INTO plans (objective_id, version, source, rationale, graph, created_at)"
            " VALUES (?,?,?,?,?,?)",
            (plan.objective_id, version, plan.source, plan.rationale, to_json(plan.graph), now),
        )
        return plan.model_copy(update={"id": cur.lastrowid, "version": version})

    def skip_steps(
        self, objective_id: int, task_ids: dict[int, str], *, rationale: str, reason: str
    ) -> tuple[Plan, list[str]]:
        """Replan by skipping steps, atomically (audit M2).

        In one write transaction: mark the tasks skipped (only if they have not started or
        finished), and create the next plan version whose ``skipped`` list is the *union* of
        the previous list and the new steps. Concurrent prunes serialize on the write lock,
        so no update is lost and a skipped step can never become required again.
        """
        with self.db.transaction() as c:
            return self.skip_steps_in(c, objective_id, task_ids, rationale=rationale, reason=reason)

    @staticmethod
    def skip_steps_in(
        c: sqlite3.Connection,
        objective_id: int,
        task_ids: dict[int, str],
        *,
        rationale: str,
        reason: str,
    ) -> tuple[Plan, list[str]]:
        now = utc_now_iso()
        cur_plan = c.execute(
            "SELECT * FROM plans WHERE objective_id=? AND superseded_at IS NULL",
            (objective_id,),
        ).fetchone()
        if cur_plan is None:
            raise NotFoundError(f"Objective {objective_id} has no current plan")
        graph = from_json(cur_plan["graph"], {})
        before = set(graph.get("skipped", []))
        newly: list[str] = []
        for tid, step in sorted(task_ids.items()):
            changed = c.execute(
                "UPDATE tasks SET status='skipped', last_error=?, updated_at=?,"
                " finished_at=? WHERE id=? AND status IN"
                " ('pending','ready','retry_scheduled','blocked_budget','awaiting_human')",
                (f"pruned: {reason}"[:300], now, now, tid),
            ).rowcount
            if changed or step not in before:
                newly.append(step)
        graph["skipped"] = sorted(before | set(task_ids.values()))
        row = c.execute(
            "SELECT COALESCE(MAX(version), 0) AS v FROM plans WHERE objective_id=?",
            (objective_id,),
        ).fetchone()
        version = int(row["v"]) + 1
        c.execute(
            "UPDATE plans SET superseded_at=? WHERE objective_id=? AND superseded_at IS NULL",
            (now, objective_id),
        )
        ins = c.execute(
            "INSERT INTO plans (objective_id, version, source, rationale, graph, created_at)"
            " VALUES (?,?,?,?,?,?)",
            (objective_id, version, cur_plan["source"], rationale, to_json(graph), now),
        )
        # The new version carries every task of the previous one (finding N5: tasks are
        # explicitly associated with the current plan version; the claim checks it).
        c.execute(
            "UPDATE tasks SET plan_id=? WHERE objective_id=? AND plan_id=?",
            (ins.lastrowid, objective_id, cur_plan["id"]),
        )
        plan = Plan(
            id=ins.lastrowid,
            objective_id=objective_id,
            version=version,
            source=cur_plan["source"],
            rationale=rationale,
            graph=graph,
        )
        return plan, sorted(set(newly))

    def finish_objective(
        self, objective_id: int, status: ObjectiveStatus, reason: str
    ) -> tuple[int, int] | None:
        """Finish an objective consistently, in one transaction (audit M3).

        Returns (tasks cancelled, requests withdrawn), or None if the objective was no
        longer open - another decision finished it first and is left untouched (L3)."""
        with self.db.transaction() as c:
            return self.finish_objective_in(c, objective_id, status, reason)

    @staticmethod
    def finish_objective_in(
        c: sqlite3.Connection, objective_id: int, status: ObjectiveStatus, reason: str
    ) -> tuple[int, int] | None:
        """Inside the caller's transaction: only an open objective is finished. Its
        final status is set first and conditionally, so a stale caller (e.g. one whose
        snapshot said "done" after the user stopped it) changes nothing. Otherwise every
        unfinished task is cancelled (including tasks blocked on budget or awaiting the
        user), every pending request is withdrawn and the stop decision recorded."""
        now = utc_now_iso()
        won = c.execute(
            "UPDATE objectives SET status=?, stop_reason=?, updated_at=?,"
            " completed_at=COALESCE(completed_at, ?) WHERE id=?"
            " AND status IN ('active','awaiting_human')",
            (status.value, reason, now, now, objective_id),
        ).rowcount
        if won != 1:
            return None
        cancelled = c.execute(
            "UPDATE tasks SET status='cancelled', last_error=?, lease_owner=NULL,"
            " lease_expires_at=NULL, updated_at=?, finished_at=? WHERE objective_id=?"
            " AND status NOT IN ('succeeded','failed','cancelled','skipped')",
            ("not needed: objective finished", now, now, objective_id),
        ).rowcount
        withdrawn = c.execute(
            "UPDATE approval_requests SET status='withdrawn', resolution=?, resolved_at=?"
            " WHERE objective_id=? AND status='pending'",
            (f"objective {status.value}: {reason}"[:300], now, objective_id),
        ).rowcount
        c.execute(
            "INSERT INTO manager_decisions (objective_id, task_id, decision, rationale,"
            " inputs, created_at) VALUES (?,?,?,?,?,?)",
            (
                objective_id,
                None,
                "stop",
                reason,
                to_json(
                    {
                        "status": status.value,
                        "tasks_cancelled": cancelled,
                        "requests_withdrawn": withdrawn,
                    }
                ),
                now,
            ),
        )
        return cancelled, withdrawn

    def current_plan(self, objective_id: int) -> Plan | None:
        r = self.db.query_one(
            "SELECT * FROM plans WHERE objective_id=? AND superseded_at IS NULL", (objective_id,)
        )
        if r is None:
            return None
        return Plan(
            id=r["id"],
            objective_id=r["objective_id"],
            version=r["version"],
            source=r["source"],
            rationale=r["rationale"],
            graph=from_json(r["graph"], {}),
        )

    # ---------------------------------------------------------------- tasks
    _TASK_COLUMNS = (
        "objective_id, plan_id, parent_task_id, worker, step, task_type, title, input,"
        " acceptance_criteria, priority, important, complexity, status, attempt, max_attempts,"
        " escalations, current_tier, current_effort, idempotency_key, budget_usd, spent_usd,"
        " created_at, updated_at, kind"
    )

    @staticmethod
    def _row_task(r: sqlite3.Row) -> Task:
        return Task(
            id=r["id"],
            objective_id=r["objective_id"],
            plan_id=r["plan_id"],
            parent_task_id=r["parent_task_id"],
            worker=r["worker"],
            step=r["step"],
            task_type=r["task_type"],
            title=r["title"],
            input=from_json(r["input"], {}),
            acceptance_criteria=from_json(r["acceptance_criteria"], []),
            priority=Priority(r["priority"]),
            important=bool(r["important"]),
            complexity=r["complexity"],
            status=TaskStatus(r["status"]),
            attempt=r["attempt"],
            max_attempts=r["max_attempts"],
            escalations=r["escalations"],
            current_tier=r["current_tier"],
            current_effort=r["current_effort"],
            current_model_id=r["current_model_id"],
            idempotency_key=r["idempotency_key"],
            lease_owner=r["lease_owner"],
            lease_expires_at=r["lease_expires_at"],
            budget_usd=r["budget_usd"],
            spent_usd=r["spent_usd"],
            accepted_result_id=r["accepted_result_id"],
            last_error=r["last_error"],
            kind=r["kind"] if "kind" in r.keys() else "step",  # noqa: SIM118 - sqlite3.Row
        )

    def add_task(self, task: Task) -> Task:
        """Insert a task. If its idempotency key already exists, return the existing task."""
        with self.db.transaction() as c:
            tid = self.add_task_in(c, task)
        return self.get_task(tid)

    @classmethod
    def add_task_in(cls, c: sqlite3.Connection, task: Task) -> int:
        """Inside the caller's transaction; returns the (existing or new) task id."""
        if task.idempotency_key is not None:
            r = c.execute(
                "SELECT id FROM tasks WHERE idempotency_key=?", (task.idempotency_key,)
            ).fetchone()
            if r is not None:
                return int(r["id"])
        now = utc_now_iso()
        values: tuple[Any, ...] = (
            task.objective_id,
            task.plan_id,
            task.parent_task_id,
            task.worker,
            task.step,
            task.task_type,
            task.title,
            to_json(task.input),
            to_json(task.acceptance_criteria),
            task.priority.value,
            int(task.important),
            task.complexity,
            task.status.value,
            task.attempt,
            task.max_attempts,
            task.escalations,
            task.current_tier,
            task.current_effort,
            task.idempotency_key,
            task.budget_usd,
            task.spent_usd,
            now,
            now,
            task.kind,
        )
        placeholders = ",".join("?" * len(values))
        cur = c.execute(
            f"INSERT INTO tasks ({cls._TASK_COLUMNS}) VALUES ({placeholders})",  # noqa: S608
            values,
        )
        assert cur.lastrowid is not None
        return int(cur.lastrowid)

    def get_task(self, task_id: int) -> Task:
        r = self.db.query_one("SELECT * FROM tasks WHERE id=?", (task_id,))
        if r is None:
            raise NotFoundError(f"Task {task_id} not found")
        return self._row_task(r)

    def list_tasks(self, objective_id: int) -> list[Task]:
        rows = self.db.query(
            "SELECT * FROM tasks WHERE objective_id=? ORDER BY id", (objective_id,)
        )
        return [self._row_task(r) for r in rows]

    def set_task_status(
        self, task_id: int, status: TaskStatus, *, error: str | None = None
    ) -> Task:
        now = utc_now_iso()
        with self.db.transaction() as c:
            cur = c.execute(
                "UPDATE tasks SET status=?, last_error=COALESCE(?, last_error), updated_at=?,"
                " started_at = CASE WHEN ?='running' AND started_at IS NULL THEN ?"
                "   ELSE started_at END,"
                " finished_at = CASE WHEN ? IN ('succeeded','failed','cancelled','skipped')"
                "   THEN ? ELSE finished_at END"
                " WHERE id=?",
                (status.value, error, now, status.value, now, status.value, now, task_id),
            )
            if cur.rowcount == 0:
                raise NotFoundError(f"Task {task_id} not found")
        return self.get_task(task_id)

    def add_dependency(
        self, task_id: int, depends_on: int, kind: DependencyKind = DependencyKind.HARD
    ) -> None:
        with self.db.transaction() as c:
            self.add_dependency_in(c, task_id, depends_on, kind)

    @staticmethod
    def add_dependency_in(
        c: sqlite3.Connection,
        task_id: int,
        depends_on: int,
        kind: DependencyKind = DependencyKind.HARD,
    ) -> None:
        c.execute(
            "INSERT OR IGNORE INTO task_dependencies (task_id, depends_on_task_id, kind)"
            " VALUES (?,?,?)",
            (task_id, depends_on, kind.value),
        )

    def dependencies(self, task_id: int) -> list[tuple[int, DependencyKind]]:
        rows = self.db.query(
            "SELECT depends_on_task_id, kind FROM task_dependencies WHERE task_id=?"
            " ORDER BY depends_on_task_id",
            (task_id,),
        )
        return [(int(r[0]), DependencyKind(r[1])) for r in rows]

    def outstanding_tasks(self, objective_id: int | None = None) -> list[Task]:
        """Unfinished work of active objectives: what a resumed session must continue."""
        sql = "SELECT * FROM v_outstanding_tasks"
        params: tuple[Any, ...] = ()
        if objective_id is not None:
            sql += " WHERE objective_id=?"
            params = (objective_id,)
        rows = self.db.query(sql + " ORDER BY id", params)
        return [self._row_task(r) for r in rows if TaskStatus(r["status"]) in OUTSTANDING_STATUSES]

    # ---------------------------------------------------------------- results
    def record_result(
        self,
        task_id: int,
        attempt: int,
        result: WorkerResult,
        *,
        effective_confidence: float | None = None,
    ) -> StoredResult:
        with self.db.transaction() as c:
            return self.record_result_in(
                c, task_id, attempt, result, effective_confidence=effective_confidence
            )

    @staticmethod
    def record_result_in(
        c: sqlite3.Connection,
        task_id: int,
        attempt: int,
        result: WorkerResult,
        *,
        effective_confidence: float | None = None,
    ) -> StoredResult:
        """One result per (task, attempt): a repeated insert returns the existing row."""
        existing = c.execute(
            "SELECT id, effective_confidence FROM task_results WHERE task_id=? AND attempt=?",
            (task_id, attempt),
        ).fetchone()
        if existing is not None:
            return StoredResult(
                id=existing["id"],
                task_id=task_id,
                attempt=attempt,
                result=result,
                effective_confidence=existing["effective_confidence"],
            )
        cur = c.execute(
            "INSERT INTO task_results (task_id, attempt, schema_version, status, output,"
            " evidence, assumptions, confidence, effective_confidence, unresolved_questions,"
            " recommended_next_steps, metrics, created_at)"
            " VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (
                task_id,
                attempt,
                result.schema_version,
                result.status.value,
                to_json(result.output),
                to_json([e.model_dump(mode="json") for e in result.evidence]),
                to_json(result.assumptions),
                result.confidence,
                effective_confidence,
                to_json(result.unresolved_questions),
                to_json(result.recommended_next_steps),
                to_json(result.metrics),
                utc_now_iso(),
            ),
        )
        assert cur.lastrowid is not None
        return StoredResult(
            id=cur.lastrowid,
            task_id=task_id,
            attempt=attempt,
            result=result,
            effective_confidence=effective_confidence,
        )

    def list_results(self, task_id: int) -> list[StoredResult]:
        rows = self.db.query("SELECT * FROM task_results WHERE task_id=? ORDER BY id", (task_id,))
        out: list[StoredResult] = []
        for r in rows:
            result = WorkerResult.model_validate(
                {
                    "schema_version": r["schema_version"],
                    "status": r["status"],
                    "output": from_json(r["output"], {}),
                    "evidence": from_json(r["evidence"], []),
                    "assumptions": from_json(r["assumptions"], []),
                    "confidence": r["confidence"],
                    "unresolved_questions": from_json(r["unresolved_questions"], []),
                    "recommended_next_steps": from_json(r["recommended_next_steps"], []),
                    "metrics": from_json(r["metrics"], {}),
                }
            )
            out.append(
                StoredResult(
                    id=r["id"],
                    task_id=r["task_id"],
                    attempt=r["attempt"],
                    result=result,
                    effective_confidence=r["effective_confidence"],
                )
            )
        return out

    # ---------------------------------------------------------------- checkpoints
    def save_checkpoint(self, cp: Checkpoint) -> Checkpoint:
        with self.db.transaction() as c:
            self.save_checkpoint_in(c, cp)
        r = self.db.query_one(
            "SELECT * FROM task_checkpoints WHERE task_id=? AND attempt=? AND step=?",
            (cp.task_id, cp.attempt, cp.step),
        )
        assert r is not None
        return self._row_checkpoint(r)

    @staticmethod
    def save_checkpoint_in(
        c: sqlite3.Connection, cp: Checkpoint, generation: int | None = None
    ) -> None:
        c.execute(
            "INSERT INTO task_checkpoints (task_id, attempt, step, state, created_at,"
            " owner_generation) VALUES (?,?,?,?,?,?) ON CONFLICT(task_id, attempt, step)"
            " DO UPDATE SET state=excluded.state, created_at=excluded.created_at,"
            " owner_generation=excluded.owner_generation",
            (cp.task_id, cp.attempt, cp.step, to_json(cp.state), utc_now_iso(), generation),
        )

    @staticmethod
    def _row_checkpoint(r: sqlite3.Row) -> Checkpoint:
        return Checkpoint(
            id=r["id"],
            task_id=r["task_id"],
            attempt=r["attempt"],
            step=r["step"],
            state=from_json(r["state"], {}),
        )

    def latest_checkpoint(self, task_id: int) -> Checkpoint | None:
        r = self.db.query_one(
            "SELECT * FROM task_checkpoints WHERE task_id=? ORDER BY attempt DESC, id DESC LIMIT 1",
            (task_id,),
        )
        return None if r is None else self._row_checkpoint(r)

    # ---------------------------------------------------------------- decisions
    def record_decision(
        self,
        decision: str,
        *,
        objective_id: int | None = None,
        task_id: int | None = None,
        rationale: str | None = None,
        inputs: dict[str, Any] | None = None,
    ) -> int:
        with self.db.transaction() as c:
            return self.record_decision_in(
                c,
                decision,
                objective_id=objective_id,
                task_id=task_id,
                rationale=rationale,
                inputs=inputs,
            )

    @staticmethod
    def record_decision_in(
        c: sqlite3.Connection,
        decision: str,
        *,
        objective_id: int | None = None,
        task_id: int | None = None,
        rationale: str | None = None,
        inputs: dict[str, Any] | None = None,
        generation: int | None = None,
    ) -> int:
        cur = c.execute(
            "INSERT INTO manager_decisions (objective_id, task_id, decision, rationale,"
            " inputs, created_at, owner_generation) VALUES (?,?,?,?,?,?,?)",
            (
                objective_id,
                task_id,
                decision,
                rationale,
                to_json(inputs or {}),
                utc_now_iso(),
                generation,
            ),
        )
        assert cur.lastrowid is not None
        return cur.lastrowid

    @staticmethod
    def request_user_in(
        c: sqlite3.Connection,
        objective_id: int,
        task_id: int | None,
        kind: str,
        details: dict[str, Any],
    ) -> int:
        cur = c.execute(
            "INSERT INTO approval_requests (objective_id, task_id, kind, details, created_at)"
            " VALUES (?,?,?,?,?)",
            (objective_id, task_id, kind, to_json(details), utc_now_iso()),
        )
        assert cur.lastrowid is not None
        return cur.lastrowid
