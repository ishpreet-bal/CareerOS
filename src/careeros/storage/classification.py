"""Classification of every table as personal or operational (architecture §10.0)."""

from typing import Literal

PERSONAL_TABLES = frozenset(
    {
        "candidate_profile",
        "work_authorization",
        "profile_facts",
        "skills",
        "documents",
        "style_profiles",
        "applications",
        "application_events",
    }
)

OPERATIONAL_TABLES = frozenset(
    {
        "meta",
        "schema_migrations",
        "companies",
        "company_research",
        "job_sources",
        "jobs",
        "job_analysis",
        "eligibility_assessments",
        "match_scores",
        "objectives",
        "plans",
        "tasks",
        "task_dependencies",
        "task_results",
        "task_checkpoints",
        "execution_sessions",
        "reviews",
        "manager_decisions",
        "model_profiles",
        "model_calls",
        "budgets",
        "cache_entries",
        "routing_stats",
        "approvals",
        "approval_requests",
    }
)


def classify(table: str) -> Literal["personal", "operational"]:
    if table in PERSONAL_TABLES:
        return "personal"
    if table in OPERATIONAL_TABLES:
        return "operational"
    raise KeyError(f"Table {table!r} has no data classification; add it to storage.classification.")
