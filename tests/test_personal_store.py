import sqlite3

import pytest

from careeros.context import AppContext
from careeros.core.errors import NotFoundError
from careeros.domain.models import (
    Application,
    ApplicationStatus,
    CandidateProfile,
    Company,
    Document,
    DocumentKind,
    FactCategory,
    Job,
    ProfileFact,
    Skill,
    WorkAuthorization,
    WorkAuthStatus,
)
from careeros.storage.base import BlobStore, PersonalDataStore


def _profile(ctx: AppContext) -> CandidateProfile:
    return ctx.personal.save_profile(
        CandidateProfile(full_name="Test Person", email="t@example.com")
    )


def test_store_implements_protocols(ctx: AppContext) -> None:
    assert isinstance(ctx.personal, PersonalDataStore)
    assert isinstance(ctx.personal.blobs, BlobStore)  # type: ignore[attr-defined]


def test_profile_round_trip_and_versioning(ctx: AppContext) -> None:
    p = _profile(ctx)
    assert p.id is not None and p.version == 1
    assert p.graduation_date is None  # unknown by default
    p2 = ctx.personal.save_profile(p.model_copy(update={"location": "Exampleville"}))
    assert p2.version == 2 and p2.location == "Exampleville"
    assert ctx.personal.get_profile() == p2


def test_work_authorization_defaults_to_unknown(ctx: AppContext) -> None:
    p = _profile(ctx)
    assert p.id is not None
    auth = ctx.personal.get_work_authorization(p.id, "US")
    assert auth.status is WorkAuthStatus.UNKNOWN
    assert auth.needs_user_confirmation
    assert auth.id is None  # nothing was stored or assumed


def test_known_status_requires_user_confirmation(ctx: AppContext) -> None:
    p = _profile(ctx)
    assert p.id is not None
    with pytest.raises(ValueError, match="user-confirmed"):
        WorkAuthorization(profile_id=p.id, country="CA", status=WorkAuthStatus.AUTHORIZED)
    # The database enforces the same rule independently of the model.
    with pytest.raises(sqlite3.IntegrityError):
        ctx.db.conn.execute(
            "INSERT INTO work_authorization (profile_id, country, status, updated_at)"
            " VALUES (?, 'CA', 'citizen', 'now')",
            (p.id,),
        )
    confirmed = ctx.personal.set_work_authorization(
        WorkAuthorization(
            profile_id=p.id,
            country="ca",
            status=WorkAuthStatus.REQUIRES_SPONSORSHIP,
            confirmed_by_user=True,
        )
    )
    assert confirmed.country == "CA" and confirmed.confirmed_at is not None
    assert not confirmed.needs_user_confirmation


def test_facts_and_skills(ctx: AppContext) -> None:
    f = ctx.personal.add_fact(
        ProfileFact(
            category=FactCategory.PROJECT,
            title="Demo",
            description="A synthetic project.",
            technologies=["Python"],
            metrics={"accuracy": 0.9},
        )
    )
    assert f.id is not None and ctx.personal.get_fact(f.id) == f
    ctx.personal.add_fact(
        ProfileFact(category=FactCategory.AWARD, title="Old", description="x", active=False)
    )
    assert [x.title for x in ctx.personal.list_facts()] == ["Demo"]
    assert len(ctx.personal.list_facts(active_only=False)) == 2
    assert ctx.personal.list_facts(category=FactCategory.AWARD) == []
    ctx.personal.add_skill(Skill(name="Python", evidence_fact_ids=[f.id]))
    assert ctx.personal.list_skills()[0].evidence_fact_ids == [f.id]
    with pytest.raises(NotFoundError):
        ctx.personal.get_fact(9999)


def test_documents_use_blob_store(ctx: AppContext) -> None:
    doc = ctx.personal.add_document(
        Document(kind=DocumentKind.WRITING_SAMPLE, title="sample", format="md"), content=b"hello"
    )
    assert doc.id is not None and doc.blob_key and doc.blob_key.endswith(".md")
    assert ctx.personal.read_document_content(doc.id) == b"hello"
    blob_files = [p for p in (ctx.data_dir / "blobs").rglob("*") if p.is_file()]
    assert len(blob_files) == 1


def test_blob_store_rejects_path_traversal(ctx: AppContext) -> None:
    blobs = ctx.personal.blobs  # type: ignore[attr-defined]
    with pytest.raises(ValueError):
        blobs.get("../../etc/passwd")


def test_application_lifecycle_records_events(ctx: AppContext) -> None:
    c = ctx.companies.get_or_create(Company(name="Synthetic Co"))
    assert c.id is not None
    job, _ = ctx.jobs.upsert(Job(company_id=c.id, title="Intern"))
    assert job.id is not None
    app = ctx.personal.create_application(Application(job_id=job.id))
    assert app.id is not None
    app = ctx.personal.update_application_status(app.id, ApplicationStatus.SUBMITTED)
    assert app.submitted_at is not None
    ctx.personal.update_application_status(
        app.id, ApplicationStatus.INTERVIEW, details={"round": 1}
    )
    events = ctx.personal.list_application_events(app.id)
    assert [(e.from_status, e.to_status) for e in events] == [
        (None, ApplicationStatus.SHORTLISTED),
        (ApplicationStatus.SHORTLISTED, ApplicationStatus.SUBMITTED),
        (ApplicationStatus.SUBMITTED, ApplicationStatus.INTERVIEW),
    ]
    assert events[-1].details == {"round": 1}
    assert len(ctx.personal.list_applications(ApplicationStatus.INTERVIEW)) == 1
