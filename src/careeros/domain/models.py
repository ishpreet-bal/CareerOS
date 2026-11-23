"""Typed domain models. Persistence lives in repositories and storage backends."""

from __future__ import annotations

from enum import StrEnum
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator


class _Model(BaseModel):
    model_config = ConfigDict(extra="forbid", validate_assignment=True)


# =============================================================================== enums
class WorkAuthStatus(StrEnum):
    UNKNOWN = "unknown"
    CITIZEN = "citizen"
    PERMANENT_RESIDENT = "permanent_resident"
    AUTHORIZED = "authorized"
    REQUIRES_SPONSORSHIP = "requires_sponsorship"
    NOT_AUTHORIZED = "not_authorized"


class FactCategory(StrEnum):
    EDUCATION = "education"
    EXPERIENCE = "experience"
    PROJECT = "project"
    SKILL = "skill"
    AWARD = "award"
    PUBLICATION = "publication"
    ACTIVITY = "activity"
    OTHER = "other"


class DocumentKind(StrEnum):
    RESUME_MASTER = "resume_master"
    RESUME_TAILORED = "resume_tailored"
    COVER_LETTER = "cover_letter"
    WRITING_SAMPLE = "writing_sample"
    PORTFOLIO = "portfolio"
    APPLICATION_ANSWERS = "application_answers"
    OTHER = "other"


class RoleFamily(StrEnum):
    AI_ML = "ai_ml"
    SWE = "swe"
    EMBEDDED = "embedded"
    DATA = "data"
    OTHER = "other"


class RemoteType(StrEnum):
    ONSITE = "onsite"
    HYBRID = "hybrid"
    REMOTE = "remote"
    UNKNOWN = "unknown"


class JobStatus(StrEnum):
    OPEN = "open"
    CLOSED = "closed"
    UNKNOWN = "unknown"


class SourceKind(StrEnum):
    GREENHOUSE = "greenhouse"
    LEVER = "lever"
    ASHBY = "ashby"
    RSS = "rss"
    MANUAL = "manual"
    URL = "url"


class EligibilityVerdict(StrEnum):
    ELIGIBLE = "eligible"
    INELIGIBLE = "ineligible"
    NEEDS_USER_CONFIRMATION = "needs_user_confirmation"
    UNCERTAIN = "uncertain"


class Priority(StrEnum):
    LOW = "low"
    NORMAL = "normal"
    HIGH = "high"
    CRITICAL = "critical"


class ApplicationStatus(StrEnum):
    SHORTLISTED = "shortlisted"
    PREPARING = "preparing"
    READY_FOR_REVIEW = "ready_for_review"
    SUBMITTED = "submitted"
    ASSESSMENT = "assessment"
    INTERVIEW = "interview"
    OFFER = "offer"
    ACCEPTED = "accepted"
    REJECTED = "rejected"
    WITHDRAWN = "withdrawn"
    NO_RESPONSE = "no_response"


class ObjectiveStatus(StrEnum):
    ACTIVE = "active"
    DONE = "done"
    STOPPED = "stopped"
    AWAITING_HUMAN = "awaiting_human"
    FAILED = "failed"


class TaskStatus(StrEnum):
    PENDING = "pending"
    READY = "ready"
    RUNNING = "running"
    REVIEWING = "reviewing"
    RETRY_SCHEDULED = "retry_scheduled"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    BLOCKED_BUDGET = "blocked_budget"
    AWAITING_HUMAN = "awaiting_human"
    CANCELLED = "cancelled"
    SKIPPED = "skipped"

    @property
    def is_terminal(self) -> bool:
        return self in TERMINAL_TASK_STATUSES


TERMINAL_TASK_STATUSES = frozenset(
    {TaskStatus.SUCCEEDED, TaskStatus.FAILED, TaskStatus.CANCELLED, TaskStatus.SKIPPED}
)


class DependencyKind(StrEnum):
    HARD = "hard"
    SOFT = "soft"


class ResultStatus(StrEnum):
    SUCCEEDED = "succeeded"
    PARTIAL = "partial"
    FAILED = "failed"
    NEEDS_INPUT = "needs_input"


class EvidenceSource(StrEnum):
    PROFILE_FACT = "profile_fact"
    JOB_POSTING = "job_posting"
    WEB = "web"
    RESEARCH_CACHE = "research_cache"
    USER = "user"


# =============================================================================== profile
class CandidateProfile(_Model):
    id: int | None = None
    version: int = 1
    full_name: str | None = None
    email: str | None = None
    phone: str | None = None
    location: str | None = None
    links: list[str] = Field(default_factory=list)
    education_level: str | None = None  # None = unknown
    graduation_date: str | None = None  # None = unknown
    preferences: dict[str, Any] = Field(default_factory=dict)


class WorkAuthorization(_Model):
    """Candidate's authorization in one country. Defaults to UNKNOWN (amendment A3)."""

    id: int | None = None
    profile_id: int
    country: str
    status: WorkAuthStatus = WorkAuthStatus.UNKNOWN
    details: str | None = None
    confirmed_by_user: bool = False
    confirmed_at: str | None = None

    @field_validator("country")
    @classmethod
    def _iso(cls, v: str) -> str:
        if len(v) != 2 or not v.isalpha():
            raise ValueError("country must be an ISO 3166-1 alpha-2 code")
        return v.upper()

    @model_validator(mode="after")
    def _known_requires_confirmation(self) -> WorkAuthorization:
        if self.status is not WorkAuthStatus.UNKNOWN and not self.confirmed_by_user:
            raise ValueError(
                "A work-authorization status other than 'unknown' must be user-confirmed."
            )
        return self

    @property
    def needs_user_confirmation(self) -> bool:
        return self.status is WorkAuthStatus.UNKNOWN


class ProfileFact(_Model):
    id: int | None = None
    category: FactCategory
    title: str = Field(min_length=1)
    organization: str | None = None
    start_date: str | None = None
    end_date: str | None = None
    description: str = Field(min_length=1)
    metrics: dict[str, Any] = Field(default_factory=dict)
    tags: list[str] = Field(default_factory=list)
    technologies: list[str] = Field(default_factory=list)
    source_document_id: int | None = None
    verified: bool = False
    active: bool = True


class Skill(_Model):
    id: int | None = None
    name: str = Field(min_length=1)
    category: str | None = None
    proficiency: str | None = None
    evidence_fact_ids: list[int] = Field(default_factory=list)


class Document(_Model):
    id: int | None = None
    kind: DocumentKind
    title: str | None = None
    blob_key: str | None = None
    format: str | None = None
    content_hash: str | None = None
    parent_document_id: int | None = None
    version: int = 1
    job_id: int | None = None
    task_id: int | None = None


# =============================================================================== jobs
class Company(_Model):
    id: int | None = None
    name: str = Field(min_length=1)
    domain: str | None = None
    headquarters: str | None = None
    size_band: str | None = None
    industry: str | None = None
    careers_url: str | None = None
    ats_type: str | None = None
    ats_slug: str | None = None
    notes: str | None = None


class JobSource(_Model):
    id: int | None = None
    kind: SourceKind
    config: dict[str, Any] = Field(default_factory=dict)
    enabled: bool = True
    rate_limit_per_min: int = Field(default=10, ge=1)


class Job(_Model):
    id: int | None = None
    company_id: int
    source_id: int | None = None
    external_id: str | None = None
    url: str | None = None
    title: str = Field(min_length=1)
    role_family: RoleFamily | None = None
    location_raw: str | None = None
    countries: list[str] = Field(default_factory=list)
    remote_type: RemoteType = RemoteType.UNKNOWN
    employment_type: str | None = None
    term: str | None = None
    posted_at: str | None = None
    closes_at: str | None = None
    description_raw: str | None = None
    status: JobStatus = JobStatus.OPEN


class EligibilityAssessment(_Model):
    id: int | None = None
    job_id: int
    profile_version: int
    verdict: EligibilityVerdict
    reasons: list[str] = Field(default_factory=list)
    blocking_criteria: list[str] = Field(default_factory=list)
    unknown_inputs: list[str] = Field(default_factory=list)
    confidence: float | None = Field(default=None, ge=0, le=1)
    task_id: int | None = None

    @model_validator(mode="after")
    def _unknowns_not_eligible(self) -> EligibilityAssessment:
        if self.unknown_inputs and self.verdict is EligibilityVerdict.ELIGIBLE:
            raise ValueError(
                "An assessment with unknown inputs cannot be 'eligible'; "
                "use 'needs_user_confirmation'."
            )
        return self


# =============================================================================== applications
class Application(_Model):
    id: int | None = None
    job_id: int
    status: ApplicationStatus = ApplicationStatus.SHORTLISTED
    priority: Priority = Priority.NORMAL
    deadline: str | None = None
    resume_document_id: int | None = None
    cover_letter_document_id: int | None = None
    answers_document_id: int | None = None
    objective_id: int | None = None
    submitted_at: str | None = None
    outcome_at: str | None = None
    notes: str | None = None


class ApplicationEvent(_Model):
    id: int | None = None
    application_id: int
    event_type: str
    from_status: ApplicationStatus | None = None
    to_status: ApplicationStatus | None = None
    details: dict[str, Any] = Field(default_factory=dict)
    occurred_at: str | None = None


# =============================================================================== orchestration
class Objective(_Model):
    id: int | None = None
    kind: str = Field(min_length=1)
    title: str = Field(min_length=1)
    inputs: dict[str, Any] = Field(default_factory=dict)
    definition_of_done: list[str] = Field(min_length=1)
    priority: Priority = Priority.NORMAL
    budget_usd: float | None = Field(default=None, ge=0)
    budget_tokens: int | None = Field(default=None, ge=0)
    status: ObjectiveStatus = ObjectiveStatus.ACTIVE
    stop_reason: str | None = None


class Plan(_Model):
    id: int | None = None
    objective_id: int
    version: int = Field(ge=1)
    source: str
    rationale: str | None = None
    graph: dict[str, Any] = Field(default_factory=dict)


class Task(_Model):
    id: int | None = None
    objective_id: int
    plan_id: int
    parent_task_id: int | None = None
    worker: str
    step: str | None = None
    task_type: str
    title: str | None = None
    input: dict[str, Any] = Field(default_factory=dict)
    acceptance_criteria: list[str] = Field(default_factory=list)
    priority: Priority = Priority.NORMAL
    important: bool = False
    complexity: float | None = Field(default=None, ge=0, le=1)
    status: TaskStatus = TaskStatus.PENDING
    attempt: int = Field(default=0, ge=0)
    max_attempts: int = Field(default=3, ge=1)
    escalations: int = Field(default=0, ge=0)
    current_tier: int | None = Field(default=None, ge=1, le=3)
    current_effort: str | None = None
    current_model_id: str | None = None
    idempotency_key: str | None = None
    lease_owner: str | None = None
    lease_expires_at: str | None = None
    budget_usd: float | None = Field(default=None, ge=0)
    spent_usd: float = Field(default=0.0, ge=0)
    accepted_result_id: int | None = None
    last_error: str | None = None


class Evidence(_Model):
    claim: str
    source_type: EvidenceSource
    source_ref: str
    quote: str | None = None


class WorkerResult(_Model):
    """Structured output required from every worker attempt (architecture §4.1)."""

    schema_version: str = "1"
    status: ResultStatus
    output: dict[str, Any] = Field(default_factory=dict)
    evidence: list[Evidence] = Field(default_factory=list)
    assumptions: list[str] = Field(default_factory=list)
    confidence: float = Field(ge=0, le=1)
    unresolved_questions: list[str] = Field(default_factory=list)
    recommended_next_steps: list[str] = Field(default_factory=list)
    metrics: dict[str, Any] = Field(default_factory=dict)


class StoredResult(_Model):
    id: int
    task_id: int
    attempt: int
    result: WorkerResult
    effective_confidence: float | None = None


class Checkpoint(_Model):
    id: int | None = None
    task_id: int
    attempt: int
    step: str
    state: dict[str, Any] = Field(default_factory=dict)
