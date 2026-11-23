import pytest

from careeros.manager.decisions import AttemptState, decide


def _s(**kw: object) -> AttemptState:
    base = dict(
        attempt=1,
        max_attempts=3,
        escalations=0,
        max_escalations=2,
        transient_retries=0,
        max_transient_retries=2,
        repair_retries=0,
        can_raise_effort=True,
    )
    return AttemptState(**{**base, **kw})  # type: ignore[arg-type]


@pytest.mark.parametrize(
    ("outcome", "state", "action"),
    [
        ("accepted", {}, "accept"),
        ("timeout", {}, "retry_same"),
        ("rate_limit", {"transient_retries": 2}, "escalate_tier"),
        ("malformed", {}, "retry_with_feedback"),
        ("invalid", {"repair_retries": 1}, "escalate_tier"),
        ("low_confidence", {}, "escalate_effort"),
        ("low_confidence", {"can_raise_effort": False}, "escalate_tier"),
        ("review_revise", {}, "escalate_effort"),
        ("review_escalate", {}, "escalate_tier"),
        ("review_reject", {}, "escalate_tier"),
        ("needs_input", {}, "await_human"),
        ("low_confidence", {"escalations": 2}, "await_human"),
        ("timeout", {"attempt": 3}, "await_human"),
        ("accepted", {"attempt": 3}, "accept"),
    ],
)
def test_decision_table(outcome: str, state: dict[str, object], action: str) -> None:
    assert decide(outcome, _s(**state)).action == action  # type: ignore[arg-type]
