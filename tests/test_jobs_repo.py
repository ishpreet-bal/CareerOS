import sqlite3

import pytest

from careeros.context import AppContext
from careeros.domain.models import (
    Company,
    EligibilityAssessment,
    EligibilityVerdict,
    Job,
    RemoteType,
    RoleFamily,
)
from careeros.domain.repositories import make_dedupe_key, normalize_name


def test_normalize_name() -> None:
    assert normalize_name("Examplar AI, Inc.") == "examplar ai"
    assert normalize_name("Café Robotics GmbH") == "cafe robotics"


def test_company_get_or_create_dedupes(ctx: AppContext) -> None:
    a = ctx.companies.get_or_create(Company(name="Examplar AI Inc."))
    b = ctx.companies.get_or_create(Company(name="examplar ai"))
    assert a.id == b.id and ctx.companies.count() == 1


def test_job_upsert_dedupes_by_key_and_external_id(ctx: AppContext) -> None:
    c = ctx.companies.get_or_create(Company(name="Widget Devices Ltd"))
    assert c.id is not None
    base = Job(
        company_id=c.id,
        title="Embedded Software Intern",
        location_raw="Exampleton",
        term="Summer 2027",
        role_family=RoleFamily.EMBEDDED,
        remote_type=RemoteType.ONSITE,
        countries=["CA"],
    )
    j1, created1 = ctx.jobs.upsert(base)
    j2, created2 = ctx.jobs.upsert(base.model_copy(update={"title": "embedded software intern!"}))
    assert created1 and not created2 and j1.id == j2.id
    j3, created3 = ctx.jobs.upsert(base.model_copy(update={"term": "Fall 2027"}))
    assert created3 and j3.id != j1.id
    assert j1.countries == ["CA"] and j1.role_family is RoleFamily.EMBEDDED
    assert make_dedupe_key("A", "B", None, None) == make_dedupe_key("a", "b", "", "")


def test_eligibility_unknown_inputs_require_confirmation(ctx: AppContext) -> None:
    c = ctx.companies.get_or_create(Company(name="Synthetic Co"))
    assert c.id is not None
    job, _ = ctx.jobs.upsert(Job(company_id=c.id, title="ML Intern"))
    assert job.id is not None
    with pytest.raises(ValueError, match="needs_user_confirmation"):
        EligibilityAssessment(
            job_id=job.id,
            profile_version=1,
            verdict=EligibilityVerdict.ELIGIBLE,
            unknown_inputs=["work_authorization:US"],
        )
    saved = ctx.jobs.save_eligibility(
        EligibilityAssessment(
            job_id=job.id,
            profile_version=1,
            verdict=EligibilityVerdict.NEEDS_USER_CONFIRMATION,
            unknown_inputs=["work_authorization:US"],
        )
    )
    assert saved.verdict is EligibilityVerdict.NEEDS_USER_CONFIRMATION
    # The database enforces the rule as well.
    with pytest.raises(sqlite3.IntegrityError):
        ctx.db.conn.execute(
            "UPDATE eligibility_assessments SET verdict='eligible' WHERE id=?", (saved.id,)
        )
