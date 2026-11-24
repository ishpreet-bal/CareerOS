"""The manager's decision table (architecture §3.3) as a pure, testable function."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

Outcome = Literal[
    "accepted",
    "timeout",
    "rate_limit",
    "malformed",
    "invalid",
    "error",
    "low_confidence",
    "review_revise",
    "review_escalate",
    "review_reject",
    "needs_input",
    "factual_unsupported",
]
Action = Literal[
    "accept",
    "retry_same",
    "retry_with_feedback",
    "escalate_effort",
    "escalate_tier",
    "await_human",
]

TRANSIENT: frozenset[Outcome] = frozenset({"timeout", "rate_limit"})
FORMAT: frozenset[Outcome] = frozenset({"malformed", "invalid"})


@dataclass(frozen=True)
class AttemptState:
    attempt: int
    max_attempts: int
    escalations: int
    max_escalations: int
    transient_retries: int
    max_transient_retries: int
    repair_retries: int
    can_raise_effort: bool


@dataclass(frozen=True)
class Decision:
    action: Action
    reason: str


def decide(outcome: Outcome, s: AttemptState) -> Decision:
    if outcome == "accepted":
        return Decision("accept", "validators and review passed")
    if outcome == "needs_input":
        return Decision("await_human", "the worker needs information only the user has")
    if s.attempt >= s.max_attempts:
        return Decision("await_human", f"attempt limit reached ({s.attempt}/{s.max_attempts})")

    def escalate(reason: str, prefer_effort: bool) -> Decision:
        if s.escalations >= s.max_escalations:
            return Decision("await_human", f"escalation limit reached ({s.escalations}); {reason}")
        if prefer_effort and s.can_raise_effort:
            return Decision("escalate_effort", reason)
        return Decision("escalate_tier", reason)

    if outcome in TRANSIENT:
        if s.transient_retries < s.max_transient_retries:
            return Decision("retry_same", f"transient {outcome}; retry with backoff")
        return escalate(f"repeated {outcome}", prefer_effort=False)
    if outcome in FORMAT:
        if s.repair_retries < 1:
            return Decision("retry_with_feedback", f"{outcome} output; one repair retry")
        return escalate(f"{outcome} output after a repair retry", prefer_effort=False)
    if outcome == "factual_unsupported":
        # The evidence policy was not met (audit L11): one retry with the violations as
        # feedback, then a stronger model; never accepted as it stands.
        if s.repair_retries < 1:
            return Decision("retry_with_feedback", "evidence policy not met; one repair retry")
        return escalate("evidence policy not met after a repair retry", prefer_effort=False)
    if outcome in ("low_confidence", "review_revise"):
        return escalate(f"{outcome}: more reasoning should help", prefer_effort=True)
    return escalate(f"{outcome}: needs a stronger model", prefer_effort=False)
