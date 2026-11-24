"""C6 (audit L12) reproductions that run against any version: public behaviour only.

Each failed on the baseline (``6bf195f``), where approvals had no expiry and no limits,
and a clarification answer was recorded as a permission grant.
"""

from __future__ import annotations

from datetime import timedelta

import pytest

from careeros.context import AppContext
from careeros.core.clock import utc_now
from careeros.core.errors import CareerOSError
from careeros.llm.adapters.scripted import ScriptedAdapter
from careeros.manager.manager import CareerManager, ObjectiveRequest
from careeros.manager.plans import PlaybookLibrary
from tests.m3_helpers import book, model_step


def _blocked(ctx: AppContext) -> tuple[int, int]:
    lib = PlaybookLibrary([book("l12", [model_step("m")])])
    m = CareerManager(ctx, playbooks=lib)
    oid = m.submit(
        ObjectiveRequest(title="l12", kind="l12", inputs={"text": "x"}, budget_usd=0.0001)
    )
    m.run(oid)
    [req] = m.pending_requests(oid)
    return oid, int(req["id"])


def _budget(ctx: AppContext, oid: int) -> float:
    r = ctx.db.query_one("SELECT budget_usd FROM objectives WHERE id=?", (oid,))
    assert r is not None
    return float(r["budget_usd"])


def test_a_request_left_open_for_months_cannot_be_approved(ctx: AppContext) -> None:
    oid, rid = _blocked(ctx)
    before = _budget(ctx, oid)
    months_later = CareerManager(ctx, clock=lambda: utc_now() + timedelta(days=120))
    with pytest.raises(CareerOSError):  # baseline: approved, budget raised
        months_later.approve(rid, extra_usd=0.5)
    assert _budget(ctx, oid) == pytest.approx(before)


def test_one_approval_cannot_authorise_an_unbounded_budget(ctx: AppContext) -> None:
    oid, rid = _blocked(ctx)
    before = _budget(ctx, oid)
    with pytest.raises(CareerOSError):  # baseline: +$1000 (simulated) accepted
        CareerManager(ctx).approve(rid, extra_usd=1000.0)
    assert _budget(ctx, oid) == pytest.approx(before)


def test_one_approval_cannot_authorise_unbounded_attempts(ctx: AppContext) -> None:
    lib = PlaybookLibrary([book("l12r", [model_step("m", max_attempts=1)])])
    m = CareerManager(
        ctx,
        adapter=ScriptedAdapter({"m:research.company": ["fail_quality"] * 5}),
        playbooks=lib,
    )
    oid = m.submit(ObjectiveRequest(title="r", kind="l12r", inputs={"text": "x"}))
    m.run(oid)
    [req] = m.pending_requests(oid)
    assert req["kind"] == "retry_limit"
    with pytest.raises(CareerOSError):  # baseline: 100 more attempts accepted
        m.approve(int(req["id"]), extra_attempts=100)


def test_a_clarification_answer_is_not_a_permission_grant(ctx: AppContext) -> None:
    m = CareerManager(ctx, adapter=ScriptedAdapter({"manager.plan_novel": ["fail_quality"]}))
    oid = m.submit(ObjectiveRequest(title="Summarize my club notes", inputs={"text": "x"}))
    [req] = m.pending_requests(oid)
    try:
        m.approve(int(req["id"]), answer="three bullets")  # type: ignore[call-arg]
    except TypeError:  # versions without clarification answers
        m.approve(int(req["id"]))
    assert ctx.db.query("SELECT * FROM approvals") == []  # baseline: an 'other' grant row
