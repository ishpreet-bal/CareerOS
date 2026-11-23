"""SQLite implementation of :class:`PersonalDataStore` (default local backend)."""

from __future__ import annotations

import sqlite3
from typing import Any

from careeros.core.clock import utc_now_iso
from careeros.core.db import Database, from_json, to_json
from careeros.core.errors import NotFoundError
from careeros.domain.models import (
    Application,
    ApplicationEvent,
    ApplicationStatus,
    CandidateProfile,
    Document,
    FactCategory,
    ProfileFact,
    Skill,
    WorkAuthorization,
    WorkAuthStatus,
)
from careeros.storage.base import BlobStore


class SQLitePersonalStore:
    """Personal data in the local SQLite database plus a blob store for file content."""

    def __init__(self, db: Database, blobs: BlobStore) -> None:
        self.db = db
        self.blobs = blobs

    # ================================================================ profile
    @staticmethod
    def _row_to_profile(r: sqlite3.Row) -> CandidateProfile:
        return CandidateProfile(
            id=r["id"],
            version=r["version"],
            full_name=r["full_name"],
            email=r["email"],
            phone=r["phone"],
            location=r["location"],
            links=from_json(r["links"], []),
            education_level=r["education_level"],
            graduation_date=r["graduation_date"],
            preferences=from_json(r["preferences"], {}),
        )

    def save_profile(self, profile: CandidateProfile) -> CandidateProfile:
        """Insert the profile, or update it and increment its version."""
        now = utc_now_iso()
        fields = (
            profile.full_name,
            profile.email,
            profile.phone,
            profile.location,
            to_json(profile.links),
            profile.education_level,
            profile.graduation_date,
            to_json(profile.preferences),
        )
        with self.db.transaction() as c:
            if profile.id is None:
                cur = c.execute(
                    "INSERT INTO candidate_profile (full_name, email, phone, location, links,"
                    " education_level, graduation_date, preferences, created_at, updated_at)"
                    " VALUES (?,?,?,?,?,?,?,?,?,?)",
                    (*fields, now, now),
                )
                pid = cur.lastrowid
            else:
                cur = c.execute(
                    "UPDATE candidate_profile SET full_name=?, email=?, phone=?, location=?,"
                    " links=?, education_level=?, graduation_date=?, preferences=?,"
                    " version = version + 1, updated_at=? WHERE id=?",
                    (*fields, now, profile.id),
                )
                if cur.rowcount == 0:
                    raise NotFoundError(f"Profile {profile.id} not found")
                pid = profile.id
        row = self.db.query_one("SELECT * FROM candidate_profile WHERE id=?", (pid,))
        assert row is not None
        return self._row_to_profile(row)

    def get_profile(self) -> CandidateProfile | None:
        row = self.db.query_one("SELECT * FROM candidate_profile ORDER BY id LIMIT 1")
        return None if row is None else self._row_to_profile(row)

    # ================================================================ work authorization
    @staticmethod
    def _row_to_auth(r: sqlite3.Row) -> WorkAuthorization:
        return WorkAuthorization(
            id=r["id"],
            profile_id=r["profile_id"],
            country=r["country"],
            status=WorkAuthStatus(r["status"]),
            details=r["details"],
            confirmed_by_user=bool(r["confirmed_by_user"]),
            confirmed_at=r["confirmed_at"],
        )

    def set_work_authorization(self, auth: WorkAuthorization) -> WorkAuthorization:
        now = utc_now_iso()
        confirmed_at = auth.confirmed_at or (now if auth.confirmed_by_user else None)
        with self.db.transaction() as c:
            c.execute(
                "INSERT INTO work_authorization (profile_id, country, status, details,"
                " confirmed_by_user, confirmed_at, updated_at) VALUES (?,?,?,?,?,?,?)"
                " ON CONFLICT(profile_id, country) DO UPDATE SET status=excluded.status,"
                " details=excluded.details, confirmed_by_user=excluded.confirmed_by_user,"
                " confirmed_at=excluded.confirmed_at, updated_at=excluded.updated_at",
                (
                    auth.profile_id,
                    auth.country,
                    auth.status.value,
                    auth.details,
                    int(auth.confirmed_by_user),
                    confirmed_at,
                    now,
                ),
            )
        return self.get_work_authorization(auth.profile_id, auth.country)

    def get_work_authorization(self, profile_id: int, country: str) -> WorkAuthorization:
        """Return the stored status, or an UNKNOWN placeholder if none was entered (A3)."""
        row = self.db.query_one(
            "SELECT * FROM work_authorization WHERE profile_id=? AND country=?",
            (profile_id, country.upper()),
        )
        if row is None:
            return WorkAuthorization(profile_id=profile_id, country=country)
        return self._row_to_auth(row)

    def list_work_authorizations(self, profile_id: int) -> list[WorkAuthorization]:
        rows = self.db.query(
            "SELECT * FROM work_authorization WHERE profile_id=? ORDER BY country", (profile_id,)
        )
        return [self._row_to_auth(r) for r in rows]

    # ================================================================ facts and skills
    @staticmethod
    def _row_to_fact(r: sqlite3.Row) -> ProfileFact:
        return ProfileFact(
            id=r["id"],
            category=FactCategory(r["category"]),
            title=r["title"],
            organization=r["organization"],
            start_date=r["start_date"],
            end_date=r["end_date"],
            description=r["description"],
            metrics=from_json(r["metrics"], {}),
            tags=from_json(r["tags"], []),
            technologies=from_json(r["technologies"], []),
            source_document_id=r["source_document_id"],
            verified=bool(r["verified"]),
            active=bool(r["active"]),
        )

    def add_fact(self, fact: ProfileFact) -> ProfileFact:
        now = utc_now_iso()
        with self.db.transaction() as c:
            cur = c.execute(
                "INSERT INTO profile_facts (category, title, organization, start_date, end_date,"
                " description, metrics, tags, technologies, source_document_id, verified, active,"
                " created_at, updated_at) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (
                    fact.category.value,
                    fact.title,
                    fact.organization,
                    fact.start_date,
                    fact.end_date,
                    fact.description,
                    to_json(fact.metrics),
                    to_json(fact.tags),
                    to_json(fact.technologies),
                    fact.source_document_id,
                    int(fact.verified),
                    int(fact.active),
                    now,
                    now,
                ),
            )
        assert cur.lastrowid is not None
        return self.get_fact(cur.lastrowid)

    def get_fact(self, fact_id: int) -> ProfileFact:
        row = self.db.query_one("SELECT * FROM profile_facts WHERE id=?", (fact_id,))
        if row is None:
            raise NotFoundError(f"Profile fact {fact_id} not found")
        return self._row_to_fact(row)

    def list_facts(
        self, *, category: FactCategory | None = None, active_only: bool = True
    ) -> list[ProfileFact]:
        sql = "SELECT * FROM profile_facts WHERE 1=1"
        params: list[Any] = []
        if category is not None:
            sql += " AND category=?"
            params.append(category.value)
        if active_only:
            sql += " AND active=1"
        rows = self.db.query(sql + " ORDER BY id", tuple(params))
        return [self._row_to_fact(r) for r in rows]

    def add_skill(self, skill: Skill) -> Skill:
        with self.db.transaction() as c:
            cur = c.execute(
                "INSERT INTO skills (name, category, proficiency, evidence_fact_ids)"
                " VALUES (?,?,?,?)",
                (skill.name, skill.category, skill.proficiency, to_json(skill.evidence_fact_ids)),
            )
        return skill.model_copy(update={"id": cur.lastrowid})

    def list_skills(self) -> list[Skill]:
        return [
            Skill(
                id=r["id"],
                name=r["name"],
                category=r["category"],
                proficiency=r["proficiency"],
                evidence_fact_ids=from_json(r["evidence_fact_ids"], []),
            )
            for r in self.db.query("SELECT * FROM skills ORDER BY name")
        ]

    # ================================================================ documents
    def add_document(self, doc: Document, content: bytes | None = None) -> Document:
        blob_key, content_hash = doc.blob_key, doc.content_hash
        if content is not None:
            suffix = f".{doc.format.lower()}" if doc.format else ""
            ref = self.blobs.put(content, suffix=suffix)
            blob_key, content_hash = ref.key, ref.sha256
        with self.db.transaction() as c:
            cur = c.execute(
                "INSERT INTO documents (kind, title, blob_key, format, content_hash,"
                " parent_document_id, version, job_id, task_id, created_at)"
                " VALUES (?,?,?,?,?,?,?,?,?,?)",
                (
                    doc.kind.value,
                    doc.title,
                    blob_key,
                    doc.format,
                    content_hash,
                    doc.parent_document_id,
                    doc.version,
                    doc.job_id,
                    doc.task_id,
                    utc_now_iso(),
                ),
            )
        assert cur.lastrowid is not None
        return self.get_document(cur.lastrowid)

    def get_document(self, doc_id: int) -> Document:
        r = self.db.query_one("SELECT * FROM documents WHERE id=?", (doc_id,))
        if r is None:
            raise NotFoundError(f"Document {doc_id} not found")
        return Document(
            id=r["id"],
            kind=r["kind"],
            title=r["title"],
            blob_key=r["blob_key"],
            format=r["format"],
            content_hash=r["content_hash"],
            parent_document_id=r["parent_document_id"],
            version=r["version"],
            job_id=r["job_id"],
            task_id=r["task_id"],
        )

    def read_document_content(self, doc_id: int) -> bytes:
        doc = self.get_document(doc_id)
        if doc.blob_key is None:
            raise NotFoundError(f"Document {doc_id} has no stored content")
        return self.blobs.get(doc.blob_key)

    # ================================================================ applications
    @staticmethod
    def _row_to_app(r: sqlite3.Row) -> Application:
        return Application(
            id=r["id"],
            job_id=r["job_id"],
            status=ApplicationStatus(r["status"]),
            priority=r["priority"],
            deadline=r["deadline"],
            resume_document_id=r["resume_document_id"],
            cover_letter_document_id=r["cover_letter_document_id"],
            answers_document_id=r["answers_document_id"],
            objective_id=r["objective_id"],
            submitted_at=r["submitted_at"],
            outcome_at=r["outcome_at"],
            notes=r["notes"],
        )

    def create_application(self, app: Application) -> Application:
        now = utc_now_iso()
        with self.db.transaction() as c:
            cur = c.execute(
                "INSERT INTO applications (job_id, status, priority, deadline,"
                " resume_document_id, cover_letter_document_id, answers_document_id,"
                " objective_id, notes, created_at, updated_at) VALUES (?,?,?,?,?,?,?,?,?,?,?)",
                (
                    app.job_id,
                    app.status.value,
                    app.priority.value,
                    app.deadline,
                    app.resume_document_id,
                    app.cover_letter_document_id,
                    app.answers_document_id,
                    app.objective_id,
                    app.notes,
                    now,
                    now,
                ),
            )
            app_id = cur.lastrowid
            c.execute(
                "INSERT INTO application_events (application_id, event_type, to_status,"
                " occurred_at) VALUES (?,?,?,?)",
                (app_id, "created", app.status.value, now),
            )
        assert app_id is not None
        return self.get_application(app_id)

    def get_application(self, app_id: int) -> Application:
        r = self.db.query_one("SELECT * FROM applications WHERE id=?", (app_id,))
        if r is None:
            raise NotFoundError(f"Application {app_id} not found")
        return self._row_to_app(r)

    def list_applications(self, status: ApplicationStatus | None = None) -> list[Application]:
        if status is None:
            rows = self.db.query("SELECT * FROM applications ORDER BY id")
        else:
            rows = self.db.query(
                "SELECT * FROM applications WHERE status=? ORDER BY id", (status.value,)
            )
        return [self._row_to_app(r) for r in rows]

    def update_application_status(
        self, app_id: int, status: ApplicationStatus, *, details: dict[str, object] | None = None
    ) -> Application:
        """Change status and append an event, atomically."""
        now = utc_now_iso()
        current = self.get_application(app_id)
        with self.db.transaction() as c:
            c.execute(
                "UPDATE applications SET status=?, updated_at=?,"
                " submitted_at = CASE WHEN ?='submitted' AND submitted_at IS NULL"
                "   THEN ? ELSE submitted_at END"
                " WHERE id=?",
                (status.value, now, status.value, now, app_id),
            )
            c.execute(
                "INSERT INTO application_events (application_id, event_type, from_status,"
                " to_status, details, occurred_at) VALUES (?,?,?,?,?,?)",
                (
                    app_id,
                    "status_changed",
                    current.status.value,
                    status.value,
                    to_json(details or {}),
                    now,
                ),
            )
        return self.get_application(app_id)

    def list_application_events(self, app_id: int) -> list[ApplicationEvent]:
        rows = self.db.query(
            "SELECT * FROM application_events WHERE application_id=? ORDER BY id", (app_id,)
        )
        return [
            ApplicationEvent(
                id=r["id"],
                application_id=r["application_id"],
                event_type=r["event_type"],
                from_status=r["from_status"],
                to_status=r["to_status"],
                details=from_json(r["details"], {}),
                occurred_at=r["occurred_at"],
            )
            for r in rows
        ]
