"""A small, entirely fictional dataset for development and tests (amendment A2).

Every person, organization and address here is invented. Email domains use the reserved
``example.com`` / ``example.org`` names. Work authorization is deliberately left UNKNOWN.
"""

from __future__ import annotations

from dataclasses import dataclass

from careeros.context import AppContext
from careeros.core.errors import CareerOSError
from careeros.domain.models import (
    Application,
    CandidateProfile,
    Company,
    Document,
    DocumentKind,
    FactCategory,
    Job,
    ProfileFact,
    RemoteType,
    RoleFamily,
    Skill,
    WorkAuthorization,
)

SYNTHETIC_PROFILE = CandidateProfile(
    full_name="Alex Example",
    email="alex.example@example.com",
    location="Exampleville",
    links=["https://example.org/alex-portfolio"],
    education_level="bachelor_in_progress",
    graduation_date="2028-04",
    preferences={
        "role_families": ["ai_ml", "swe", "embedded"],
        "remote_ok": True,
        "terms": ["Summer 2027"],
    },
)

SYNTHETIC_FACTS = [
    ProfileFact(
        category=FactCategory.EDUCATION,
        title="B.Eng. Computer Engineering (in progress)",
        organization="Example Institute of Technology",
        start_date="2024-09",
        description="Coursework in machine learning, operating systems and embedded systems.",
        tags=["education"],
    ),
    ProfileFact(
        category=FactCategory.PROJECT,
        title="Sensor anomaly detector",
        organization="Personal project",
        start_date="2025-05",
        end_date="2025-08",
        description="Built a small anomaly-detection model for simulated sensor data.",
        metrics={"f1": 0.87},
        technologies=["Python", "PyTorch"],
        tags=["ai_ml"],
    ),
    ProfileFact(
        category=FactCategory.PROJECT,
        title="Microcontroller motor driver",
        organization="Example Robotics Club",
        start_date="2025-01",
        end_date="2025-04",
        description="Wrote C firmware for a PWM motor controller on a hobby microcontroller.",
        technologies=["C", "FreeRTOS"],
        tags=["embedded"],
    ),
]

SYNTHETIC_SKILLS = [
    Skill(name="Python", category="language", proficiency="intermediate"),
    Skill(name="C", category="language", proficiency="intermediate"),
    Skill(name="PyTorch", category="framework", proficiency="beginner"),
]

SYNTHETIC_COMPANIES = [
    Company(name="Examplar AI Inc.", domain="examplar.example.com", industry="AI software"),
    Company(name="Widget Devices Ltd", domain="widgets.example.com", industry="Embedded hardware"),
]

SYNTHETIC_JOBS = [
    ("Examplar AI Inc.", "Machine Learning Intern", RoleFamily.AI_ML, "Remote", RemoteType.REMOTE),
    (
        "Widget Devices Ltd",
        "Embedded Software Intern",
        RoleFamily.EMBEDDED,
        "Exampleton",
        RemoteType.ONSITE,
    ),
]


@dataclass(frozen=True)
class SeedSummary:
    profile_id: int
    facts: int
    skills: int
    companies: int
    jobs: int
    applications: int


def seed_synthetic(ctx: AppContext) -> SeedSummary:
    """Load the synthetic dataset. Refuses to run against a personal database."""
    if ctx.db.dataset_kind != "synthetic":
        raise CareerOSError("Refusing to seed synthetic data into a non-synthetic database.")
    if ctx.personal.get_profile() is not None:
        raise CareerOSError("Database already contains a profile; synthetic seed skipped.")

    profile = ctx.personal.save_profile(SYNTHETIC_PROFILE)
    assert profile.id is not None
    for country in ctx.config.eligibility.target_countries:
        # Explicitly record UNKNOWN: no status is assumed (A3).
        ctx.personal.set_work_authorization(
            WorkAuthorization(profile_id=profile.id, country=country)
        )
    resume = ctx.personal.add_document(
        Document(kind=DocumentKind.RESUME_MASTER, title="Synthetic master resume", format="md"),
        content=b"# Alex Example\n\nSynthetic resume for testing only.\n",
    )
    facts = [
        ctx.personal.add_fact(f.model_copy(update={"source_document_id": resume.id}))
        for f in SYNTHETIC_FACTS
    ]
    for s in SYNTHETIC_SKILLS:
        ctx.personal.add_skill(s)

    companies = {c.name: ctx.companies.get_or_create(c) for c in SYNTHETIC_COMPANIES}
    job_ids: list[int] = []
    for company_name, title, family, location, remote in SYNTHETIC_JOBS:
        cid = companies[company_name].id
        assert cid is not None
        job, _ = ctx.jobs.upsert(
            Job(
                company_id=cid,
                title=title,
                role_family=family,
                location_raw=location,
                remote_type=remote,
                term="Summer 2027",
                description_raw=f"Synthetic posting: {title} at {company_name}.",
            )
        )
        assert job.id is not None
        job_ids.append(job.id)

    ctx.personal.create_application(Application(job_id=job_ids[0]))
    return SeedSummary(
        profile_id=profile.id,
        facts=len(facts),
        skills=len(SYNTHETIC_SKILLS),
        companies=len(companies),
        jobs=len(job_ids),
        applications=1,
    )
