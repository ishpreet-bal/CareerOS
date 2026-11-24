"""Hardening C7: factual evidence, independent factual review and cache safety (audit L11,
N4). Synthetic sources only; scripted authors and reviewers; no network."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

import careeros.core.db as dbmod
from careeros.context import AppContext
from careeros.core.db import Database, from_json
from careeros.core.errors import FactualGateError, SessionTerminatedError
from careeros.domain.models import Claim, ClaimStatus, Evidence, ResultStatus, WorkerResult
from careeros.llm.adapters.scripted import ScriptedAdapter
from careeros.manager.manager import CareerManager, ObjectiveRequest
from careeros.manager.plans import Playbook, PlaybookLibrary
from careeros.quality.factual import (
    FACTUAL_POLICY_VERSION,
    check_claims,
    check_review,
    fingerprint,
    sources_for,
)
from careeros.simulation import later
from tests.m2_helpers import calls, decisions, task_by_step

POSTING = "Machine learning intern. Build Python data pipelines and train PyTorch models."
SOURCES = {"job_posting": POSTING}
STEP = {
    "id": "research",
    "worker": "model_step",
    "task_type": "research.company",
    "output": {"required": ["summary"]},
}


def _lib(**step: Any) -> PlaybookLibrary:
    return PlaybookLibrary(
        [Playbook.model_validate({"name": "c7", "plan": {"steps": [{**STEP, **step}]}})]
    )


def _run(ctx: AppContext, script: dict[str, list[str]], **kw: Any) -> tuple[CareerManager, int]:
    m = CareerManager(ctx, adapter=ScriptedAdapter(script), playbooks=_lib(), **kw)
    oid = m.submit(ObjectiveRequest(title="c7", kind="c7", inputs={"text": POSTING}))
    m.run(oid)
    return m, oid


def _checks(ctx: AppContext, task_id: Any) -> list[dict[str, Any]]:
    return [
        {**dict(r), "details": from_json(r["details"], {})}
        for r in ctx.db.query("SELECT * FROM result_checks WHERE task_id=? ORDER BY id", (task_id,))
    ]


def _result(claims: list[Claim], evidence: list[Evidence], conf: float = 0.99) -> WorkerResult:
    return WorkerResult(
        status=ResultStatus.SUCCEEDED,
        output={"summary": "s"},
        claims=claims,
        evidence=evidence,
        confidence=conf,
    )


def _ev(eid: str, quote: str, **kw: Any) -> Evidence:
    return Evidence.model_validate(
        {
            "id": eid,
            "claim": "c",
            "source_type": "job_posting",
            "source_ref": "job_posting",
            "quote": quote,
            **kw,
        }
    )


def _claim(cid: str, status: str = "verified", ev: list[str] | None = None) -> Claim:
    return Claim(id=cid, text=f"claim {cid}", status=ClaimStatus(status), evidence=ev or [])


# ================================================= deterministic evidence rules
def test_sufficient_evidence_passes_and_labels_are_kept() -> None:
    r = _result(
        [_claim("c1", ev=["e1"]), _claim("c2", "unverified"), _claim("c3", "assumption")],
        [_ev("e1", "build python data pipelines")],  # case/whitespace-insensitive
    )
    fc = check_claims(r, SOURCES)
    assert fc.passed and fc.verified == ["c1"] and fc.unverified == ["c2", "c3"]


@pytest.mark.parametrize(
    ("claims", "evidence", "rule"),
    [
        ([], [], "E1"),
        ([_claim("c1", ev=["e1"]), _claim("c1", ev=["e1"])], [_ev("e1", "PyTorch")], "E2"),
        ([_claim("c1")], [], "E3"),  # missing evidence
        ([_claim("c1", ev=["e9"])], [_ev("e1", "PyTorch")], "E3"),  # unknown evidence id
        ([_claim("c1", ev=["e1"])], [_ev("e1", "invented sentence")], "E4"),  # not in source
        ([_claim("c1", ev=["e1"])], [_ev("e1", "")], "E4"),  # no quote
        (
            [_claim("c1", ev=["e1"])],
            [_ev("e1", "PyTorch", source_type="web", source_ref="https://example.invalid")],
            "E4",
        ),
        ([_claim("c1", ev=["e1"])], [_ev("e1", "PyTorch", source_ref="other_doc")], "E4"),
        (
            [_claim("c1", ev=["e1", "e2"])],
            [_ev("e1", "PyTorch"), _ev("e2", "PyTorch", stance="contradicts")],
            "E5",
        ),
    ],
    ids=[
        "no_claims",
        "duplicate_ids",
        "missing",
        "unknown_id",
        "ungrounded",
        "no_quote",
        "web_source",
        "unheld_source",
        "contradicted",
    ],
)
def test_each_evidence_rule_is_enforced(
    claims: list[Claim], evidence: list[Evidence], rule: str
) -> None:
    fc = check_claims(_result(claims, evidence), SOURCES)
    assert not fc.passed and any(v.startswith(rule) for v in fc.violations), fc.violations


def test_confidence_never_substitutes_for_evidence() -> None:
    for conf in (0.0, 0.5, 1.0):
        assert not check_claims(_result([_claim("c1")], [], conf), SOURCES).passed
        good = _result([_claim("c1", ev=["e1"])], [_ev("e1", "PyTorch")], conf)
        assert check_claims(good, SOURCES).passed


def test_only_unverified_claims_pass_but_assert_no_facts() -> None:
    fc = check_claims(_result([_claim("c1", "uncertain")], []), SOURCES)
    assert fc.passed and fc.verified == [] and fc.unverified == ["c1"]


def test_sources_are_only_what_the_task_holds() -> None:
    s = sources_for({"text": POSTING, "company": "Examplar", "n": 3}, {"prev": {"x": 1}})
    assert s == {"job_posting": POSTING, "input:company": "Examplar", "step:prev": '{"x": 1}'}


# ======================================================== review contract checks
def test_review_contract() -> None:
    r = _result([_claim("c1", ev=["e1"]), _claim("c2", "unverified")], [_ev("e1", "PyTorch")])
    assert (
        check_review(r, "accept", {"claim_verdicts": [{"id": "c1", "verdict": "supported"}]})[0]
        == "accept"
    )
    assert check_review(r, "accept", {})[0] == "escalate"  # no per-claim verdicts
    assert check_review(r, "accept", {"claim_verdicts": []})[0] == "escalate"  # unassessed
    for bad in ("unsupported", "contradicted", "not_checkable"):
        v, issues, per = check_review(
            r, "accept", {"claim_verdicts": [{"id": "c1", "verdict": bad}]}
        )
        assert v == "revise" and per == {"c1": bad} and "c1" in issues[0]


# ============================================================ manager flows
def test_supported_research_is_accepted_with_both_checks_recorded(ctx: AppContext) -> None:
    _, oid = _run(ctx, {"research:research.company": ["facts:ok"]})
    t = task_by_step(ctx, oid, "research")
    assert t["status"] == "succeeded"
    ev, rv = _checks(ctx, t["id"])
    assert (ev["kind"], ev["passed"], ev["policy_version"]) == (
        "evidence",
        1,
        FACTUAL_POLICY_VERSION,
    )
    assert ev["details"]["verified"] == ["c1"] and ev["details"]["unverified"] == ["c2"]
    assert (rv["kind"], rv["passed"]) == ("review", 1)
    assert rv["details"]["claim_verdicts"] == {"c1": "supported"}
    # Independent: the reviewer is a different model from the author.
    [task_call] = calls(ctx, task_id=t["id"], purpose="task")
    [review_call] = calls(ctx, task_id=t["id"], purpose="review")
    assert rv["checker"] == f"model:{review_call['model_id']}"
    assert review_call["model_id"] != task_call["model_id"]
    assert rv["model_call_id"] == review_call["id"]
    stored = ctx.orchestration.list_results(int(str(t["id"])))[-1].result
    assert [(c.id, c.status.value) for c in stored.claims] == [
        ("c1", "verified"),
        ("c2", "unverified"),
    ]


@pytest.mark.parametrize(
    "variant",
    ["missing_evidence", "unknown_evidence", "ungrounded", "web", "contradicted", "no_claims"],
)
def test_evidence_failures_are_repaired_with_feedback_never_accepted(
    ctx: AppContext, variant: str
) -> None:
    _, oid = _run(ctx, {"research:research.company": [f"facts:{variant}", "facts:ok"]})
    t = task_by_step(ctx, oid, "research")
    ds = decisions(ctx, t["id"])
    assert "retry_with_feedback" in ds and t["status"] == "succeeded"
    checks = _checks(ctx, t["id"])
    first = next(c for c in checks if c["kind"] == "evidence")
    assert first["passed"] == 0 and first["details"]["violations"]
    # The failed attempt was never reviewed: the deterministic layer is not delegated.
    assert [c["attempt"] for c in checks if c["kind"] == "review"] == [2]
    accepted = ctx.db.query_one(
        "SELECT attempt FROM task_results WHERE id=?", (t["accepted_result_id"],)
    )
    assert accepted["attempt"] == 2


def test_malformed_claims_are_a_defined_failure(ctx: AppContext) -> None:
    _, oid = _run(ctx, {"research:research.company": ["facts:malformed", "facts:ok"]})
    t = task_by_step(ctx, oid, "research")
    assert t["status"] == "succeeded" and "retry_with_feedback" in decisions(ctx, t["id"])
    [failed] = [r for r in ctx.orchestration.list_results(int(str(t["id"]))) if r.attempt == 1]
    assert failed.result.status is ResultStatus.FAILED
    assert failed.result.metrics["error_kind"] == "invalid"


def test_unsupported_claim_caught_only_by_the_reviewer(ctx: AppContext) -> None:
    """The quote is real but does not support the claim's number: the deterministic check
    passes; the independent reviewer finds the claim unsupported; revision follows."""
    _, oid = _run(ctx, {"research:research.company": ["facts:overclaim", "facts:ok"]})
    t = task_by_step(ctx, oid, "research")
    checks = _checks(ctx, t["id"])
    first_ev = next(c for c in checks if c["kind"] == "evidence" and c["attempt"] == 1)
    first_rv = next(c for c in checks if c["kind"] == "review" and c["attempt"] == 1)
    assert first_ev["passed"] == 1
    assert first_rv["passed"] == 0 and first_rv["details"]["claim_verdicts"]["c1"] == "unsupported"
    assert first_rv["details"]["verdict"] == "revise"
    assert t["status"] == "succeeded" and t["attempt"] == 2


def test_a_review_without_per_claim_verdicts_is_not_accepted(ctx: AppContext) -> None:
    _, oid = _run(
        ctx,
        {
            "research:research.company": ["facts:ok", "facts:ok"],
            "research:quality.review": ["review:accept:omit", "review:accept"],
        },
    )
    t = task_by_step(ctx, oid, "research")
    first_rv = next(c for c in _checks(ctx, t["id"]) if c["kind"] == "review")
    assert first_rv["passed"] == 0 and first_rv["details"]["verdict"] == "escalate"
    assert t["attempt"] == 2 and t["status"] == "succeeded"


def test_a_lax_reviewer_cannot_rescue_ungrounded_evidence(ctx: AppContext) -> None:
    _, oid = _run(
        ctx,
        {
            "research:research.company": ["facts:ungrounded", "facts:ok"],
            "research:quality.review": ["review:accept:lax", "review:accept:lax"],
        },
    )
    t = task_by_step(ctx, oid, "research")
    assert [c["attempt"] for c in _checks(ctx, t["id"]) if c["kind"] == "review"] == [2]
    assert t["attempt"] == 2  # attempt 1 never reached the (lax) reviewer


# ====================================================================== the gate
def test_acceptance_requires_the_recorded_checks(ctx: AppContext) -> None:
    from careeros.orchestration.fencing import claim

    m = CareerManager(ctx, playbooks=_lib())
    oid = m.submit(ObjectiveRequest(title="c7", kind="c7", inputs={"text": POSTING}))
    t = m.repo.list_tasks(oid)[-1]
    sid = m.repo.start_session()
    with ctx.db.transaction() as c:
        fence = claim(c, int(t.id or 0), sid, "2099-01-01T00:00:00+00:00", m._now())
        stored = m.repo.record_result_in(
            c, int(t.id or 0), 1, _result([_claim("c1", ev=["e1"])], [_ev("e1", "PyTorch")])
        )
    assert fence is not None
    obj = m.repo.get_objective(oid)
    t = m.repo.get_task(int(t.id or 0))
    with pytest.raises(FactualGateError, match="evidence"):
        m._accept(fence, obj, t, stored.id, "no checks at all")
    phash, shash = fingerprint({}), fingerprint(SOURCES)
    with ctx.db.transaction() as c:
        m._record_check_in(
            c,
            fence,
            int(t.id or 0),
            stored.id,
            "evidence",
            "deterministic",
            FACTUAL_POLICY_VERSION,
            phash,
            shash,
            True,
            {},
        )
    with pytest.raises(FactualGateError, match="review"):
        m._accept(fence, obj, t, stored.id, "evidence but no review")
    with ctx.db.transaction() as c:
        m._record_check_in(
            c,
            fence,
            int(t.id or 0),
            stored.id,
            "review",
            "model:x",
            "factual-v0",
            phash,
            shash,
            True,
            {},
        )
    with pytest.raises(FactualGateError, match="review"):
        m._accept(fence, obj, t, stored.id, "review under another policy version")
    assert m.repo.get_task(int(t.id or 0)).status.value == "running"  # nothing accepted
    with ctx.db.transaction() as c:
        m._record_check_in(
            c,
            fence,
            int(t.id or 0),
            stored.id,
            "review",
            "model:x",
            FACTUAL_POLICY_VERSION,
            phash,
            shash,
            True,
            {},
        )
    m._accept(fence, obj, t, stored.id, "both checks passed")
    assert m.repo.get_task(int(t.id or 0)).status.value == "succeeded"


def test_recording_a_check_twice_keeps_one_row(ctx: AppContext) -> None:
    _, oid = _run(ctx, {"research:research.company": ["facts:ok"]})
    t = task_by_step(ctx, oid, "research")
    before = _checks(ctx, t["id"])
    m = CareerManager(ctx, playbooks=_lib())
    from careeros.orchestration.fencing import Fence

    fence = Fence(int(str(t["id"])), 1, "s", 1)
    with ctx.db.transaction() as c:
        for _ in range(3):  # repeated delivery of the same check
            m._record_check_in(
                c,
                fence,
                int(str(t["id"])),
                int(str(t["accepted_result_id"])),
                "review",
                "model:y",
                FACTUAL_POLICY_VERSION,
                "p",
                "s",
                True,
                {},
            )
    after = _checks(ctx, t["id"])
    assert len(after) == len(before)


# ================================================================== cache (N4)
def _second_objective(ctx: AppContext, **kw: Any) -> tuple[CareerManager, int]:
    m = CareerManager(ctx, playbooks=_lib(), **kw)
    oid = m.submit(ObjectiveRequest(title="c7", kind="c7", inputs={"text": POSTING}))
    m.run(oid)
    return m, oid


def test_cache_hit_reuses_only_a_matching_passing_review(ctx: AppContext) -> None:
    _, first = _second_objective(ctx)
    t1 = task_by_step(ctx, first, "research")
    _, second = _second_objective(ctx)
    t2 = task_by_step(ctx, second, "research")
    assert [c["status"] for c in calls(ctx, task_id=t2["id"])] == ["cache_hit"]  # no review call
    ev, rv = _checks(ctx, t2["id"])
    assert ev["kind"] == "evidence" and ev["passed"] == 1  # validation re-ran
    original = next(c for c in _checks(ctx, t1["id"]) if c["kind"] == "review")
    assert rv["reused_from"] == original["id"] and rv["passed"] == 1
    assert rv["payload_hash"] == original["payload_hash"]
    assert t2["status"] == "succeeded"


@pytest.mark.parametrize("change", ["failed", "other_policy", "other_sources", "same_author"])
def test_cache_hit_without_a_reusable_review_is_reviewed_again(
    ctx: AppContext, change: str
) -> None:
    _, first = _second_objective(ctx)
    t1 = task_by_step(ctx, first, "research")
    rv = next(c for c in _checks(ctx, t1["id"]) if c["kind"] == "review")
    author = calls(ctx, task_id=t1["id"], purpose="task")[0]["model_id"]
    column, value = {
        "failed": ("passed", 0),
        "other_policy": ("policy_version", "factual-v0"),
        "other_sources": ("sources_hash", "different"),
        "same_author": ("checker", f"model:{author}"),
    }[change]
    with ctx.db.transaction() as c:
        c.execute(f"UPDATE result_checks SET {column}=? WHERE id=?", (value, rv["id"]))
    _, second = _second_objective(ctx)
    t2 = task_by_step(ctx, second, "research")
    purposes = [c["purpose"] for c in calls(ctx, task_id=t2["id"])]
    assert (
        purposes == ["task", "review"] and calls(ctx, task_id=t2["id"])[0]["status"] == "cache_hit"
    )
    new = next(c for c in _checks(ctx, t2["id"]) if c["kind"] == "review")
    assert new["reused_from"] is None and new["model_call_id"] is not None


def test_a_legacy_cache_entry_is_validated_and_reviewed(ctx: AppContext) -> None:
    """An entry accepted before C7 (or by a path with no review) has no recorded review:
    a hit on it is validated and reviewed from scratch."""
    _, first = _second_objective(ctx)
    t1 = task_by_step(ctx, first, "research")
    with ctx.db.transaction() as c:  # forget every review ever recorded
        c.execute("DELETE FROM result_checks WHERE task_id=?", (t1["id"],))
    _, second = _second_objective(ctx)
    t2 = task_by_step(ctx, second, "research")
    assert [c["purpose"] for c in calls(ctx, task_id=t2["id"])] == ["task", "review"]
    assert {c["kind"] for c in _checks(ctx, t2["id"])} == {"evidence", "review"}


def test_a_policy_change_misses_the_cache(ctx: AppContext, monkeypatch: pytest.MonkeyPatch) -> None:
    _, _first = _second_objective(ctx)
    import careeros.quality.factual as factual

    monkeypatch.setattr(factual, "FACTUAL_POLICY_VERSION", "factual-v2")
    _, second = _second_objective(ctx)
    t2 = task_by_step(ctx, second, "research")
    assert calls(ctx, task_id=t2["id"], purpose="task")[0]["status"] == "ok"  # no cache hit


def test_cached_payload_with_bad_evidence_is_not_accepted(ctx: AppContext) -> None:
    """A cached payload is re-validated: if it no longer meets the evidence rules against
    the current sources, the hit is not accepted."""
    _, first = _second_objective(ctx)
    t1 = task_by_step(ctx, first, "research")
    key = calls(ctx, task_id=t1["id"], purpose="task")[0]["cache_key"]
    row = ctx.db.query_one("SELECT value FROM cache_entries WHERE key=?", (key,))
    payload = from_json(row["value"])
    for e in payload["evidence"]:
        if e.get("id"):
            e["quote"] = "a quote from nowhere"
    with ctx.db.transaction() as c:
        c.execute("UPDATE cache_entries SET value=? WHERE key=?", (dbmod.to_json(payload), key))
    _, second = _second_objective(ctx)
    t2 = task_by_step(ctx, second, "research")
    ev = next(c for c in _checks(ctx, t2["id"]) if c["kind"] == "evidence" and c["attempt"] == 1)
    assert ev["passed"] == 0
    accepted = ctx.db.query_one(
        "SELECT attempt FROM task_results WHERE id=?", (t2["accepted_result_id"],)
    )
    assert accepted is None or accepted["attempt"] > 1


# ================================================================ recovery
def test_crash_between_review_and_acceptance_recovers_without_duplicates(
    ctx: AppContext, monkeypatch: pytest.MonkeyPatch
) -> None:
    orig = CareerManager._accept
    crashed: list[bool] = []

    def crash_once(self: CareerManager, *a: Any, **k: Any) -> None:
        if not crashed:
            crashed.append(True)
            raise SessionTerminatedError("simulated crash before accepting")
        orig(self, *a, **k)

    monkeypatch.setattr(CareerManager, "_accept", crash_once)
    with pytest.raises(SessionTerminatedError):
        _run(ctx, {"research:research.company": ["facts:ok"]})
    oid = int(ctx.db.query_one("SELECT MAX(id) m FROM objectives")["m"])
    t = task_by_step(ctx, oid, "research")
    assert t["status"] == "running"
    assert {c["kind"] for c in _checks(ctx, t["id"])} == {"evidence", "review"}
    m = CareerManager(ctx, adapter=ScriptedAdapter({}), playbooks=_lib(), clock=later(10))
    m.run(oid)
    t = task_by_step(ctx, oid, "research")
    assert t["status"] == "succeeded" and t["attempt"] == 1
    assert [c["purpose"] for c in calls(ctx, task_id=t["id"])] == ["task", "review"]  # replayed
    checks = _checks(ctx, t["id"])
    assert sorted(c["kind"] for c in checks) == ["evidence", "review"]  # one row each
    assert len(ctx.db.query("SELECT * FROM manager_decisions WHERE decision='accept'")) == 1


# ================================================================ migration
def test_migration_adds_claims_and_result_checks(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    orig = dbmod.load_migrations
    monkeypatch.setattr(dbmod, "load_migrations", lambda: [x for x in orig() if x.version <= 7])
    db = Database.open(tmp_path / "v7.db", dataset_kind="synthetic")
    monkeypatch.setattr(dbmod, "load_migrations", orig)
    db.close()
    with Database.open(tmp_path / "v7.db", dataset_kind="synthetic") as db2:
        assert db2.schema_version() == max(x.version for x in dbmod.load_migrations())
        cols = {r["name"] for r in db2.query("PRAGMA table_info(task_results)")}
        assert "claims" in cols and "result_checks" in db2.table_names()
        assert db2.migrate() == []
