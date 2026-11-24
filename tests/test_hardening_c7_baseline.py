"""C7 reproductions that run against any version: public behaviour only.

Each failed on the baseline (``c35112c``):

* L11 - a ``research.company`` result asserting facts with no checkable evidence (an
        invented web citation, high confidence) was accepted, without any review;
* N4  - a step that requires review was served a cached result from an earlier,
        unreviewed acceptance and was accepted with no review at all.
"""

from __future__ import annotations

import json
import sqlite3
from typing import Any, ClassVar

from careeros.context import AppContext
from careeros.core.config import ModelProfile
from careeros.llm.adapters.mock import MockAdapter
from careeros.llm.contracts import ModelRequest, ModelResponse
from careeros.manager.manager import CareerManager, ObjectiveRequest
from careeros.manager.plans import Playbook, PlaybookLibrary
from tests.m2_helpers import calls, task_by_step


class ConfidentButUnsupported(MockAdapter):
    """A research answer that states a 'fact' citing an invented web page, with very high
    confidence, and no claims a checker could verify."""

    mode_label: ClassVar[str] = "mock"

    def call(self, profile: ModelProfile, request: ModelRequest) -> ModelResponse:
        if request.task_type != "research.company":
            return super().call(profile, request)
        response = super().call(profile, request)
        payload = {
            "output": {"summary": "Examplar AI has 10000 employees and a lab in Paris."},
            "confidence": 0.99,
            "evidence": [
                {
                    "claim": "Examplar AI has 10000 employees",
                    "source_type": "web",
                    "source_ref": "https://example.invalid/about",
                    "quote": "We are 10,000 strong",
                }
            ],
            "assumptions": [],
            "unresolved_questions": [],
            "recommended_next_steps": [],
        }
        return response.__class__(**{**response.__dict__, "text": json.dumps(payload)})


def _lib(name: str, step: dict[str, Any]) -> PlaybookLibrary:
    return PlaybookLibrary([Playbook.model_validate({"name": name, "plan": {"steps": [step]}})])


RESEARCH = {
    "id": "research",
    "worker": "model_step",
    "task_type": "research.company",
    "output": {"required": ["summary"]},
}


def test_unsupported_research_is_never_accepted(ctx: AppContext) -> None:
    m = CareerManager(ctx, adapter=ConfidentButUnsupported(), playbooks=_lib("l11", RESEARCH))
    oid = m.submit(ObjectiveRequest(title="l11", kind="l11", inputs={"text": "Robotics intern."}))
    m.run(oid)
    t = task_by_step(ctx, oid, "research")
    # Baseline: accepted on confidence alone, with no review call.
    assert t["status"] != "succeeded" and t["accepted_result_id"] is None, (
        t["status"],
        [c["purpose"] for c in calls(ctx, task_id=t["id"])],
    )


def _has_review_record(ctx: AppContext, task_id: int, result_id: int) -> bool:
    if ctx.db.query("SELECT 1 FROM reviews WHERE task_id=? AND result_id=?", (task_id, result_id)):
        return True
    try:
        return bool(
            ctx.db.query(
                "SELECT 1 FROM result_checks WHERE result_id=? AND kind='review' AND passed=1",
                (result_id,),
            )
        )
    except sqlite3.OperationalError:  # versions without result_checks
        return False


def test_a_cache_hit_never_skips_a_required_review(ctx: AppContext) -> None:
    """Objective A runs a step without review: its result is accepted and cached. Objective
    B runs the identical step but requires review: it hits the cache. B must not be
    accepted without a review of its own (or a recorded, reusable one)."""
    plain = {
        "id": "analyze",
        "worker": "model_step",
        "task_type": "job.analyze",
        "output": {"required": ["summary"]},
        "review": False,
    }
    reviewed = {**plain, "review": True}
    inputs = {"text": "Embedded firmware intern."}
    a = CareerManager(ctx, playbooks=_lib("a", plain))
    oid_a = a.submit(ObjectiveRequest(title="t", kind="a", inputs=inputs))
    a.run(oid_a)
    assert task_by_step(ctx, oid_a, "analyze")["status"] == "succeeded"
    b = CareerManager(ctx, playbooks=_lib("b", reviewed))
    oid_b = b.submit(ObjectiveRequest(title="t", kind="b", inputs=inputs))
    b.run(oid_b)
    t = task_by_step(ctx, oid_b, "analyze")
    assert [c["status"] for c in calls(ctx, task_id=t["id"], purpose="task")] == ["cache_hit"]
    assert t["status"] == "succeeded"
    # Baseline: accepted from the cache with no review whatsoever.
    assert _has_review_record(ctx, int(str(t["id"])), int(str(t["accepted_result_id"])))
