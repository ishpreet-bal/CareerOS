"""Factual evidence policy (audit L11).

A result of a *factual* task type (``research.company``) must state its material claims
explicitly, each labelled ``verified``, ``unverified``, ``assumption`` or ``uncertain``.
Only verified claims may be relied on as facts, and a claim is verified only if it is
backed by a quotation from a source CareerOS actually holds for the task. Confidence is
never consulted.

The rules are checked deterministically here (``check_claims``); an independent model
review then judges whether each quotation really supports its claim, and its per-claim
verdicts are validated here too (``check_review``). Nothing in this module fetches, invents
or "verifies" an external source: a source CareerOS does not hold (e.g. the web - this
build has no retrieval) can never make a claim verified.

Minimum evidence requirements (``EVIDENCE_RULES``), each justified by the task's purpose
of stating facts about an employer that later steps (letters, tailored resumes) reuse:

E1  At least one material claim is stated - a factual task that states nothing cannot be
    checked, and its prose would be the only, unchecked, carrier of facts.
E2  Claim ids are unique and every claim has a status - so each can be traced.
E3  Every verified material claim cites at least one supporting evidence entry, and every
    cited id exists - unsupported claims must not become facts.
E4  Each supporting entry of a verified claim names a source CareerOS holds for this task
    (``source_ref``), of a kind it can hold, and quotes it - the quote must occur in that
    source's text (whitespace- and case-insensitive) - so the support is inspectable and
    is not invented.
E5  A verified claim has no contradicting evidence - contradiction is surfaced, never
    averaged away.
E6  Unverified, assumption and uncertain claims need no evidence but are never treated as
    facts; they are counted and recorded as such.
"""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass, field
from typing import Any

from careeros.domain.models import ClaimStatus, WorkerResult

#: Bump when the rules or the review contract change: cached results and recorded reviews
#: from another version are never reused as validated (see ``cache`` notes in the
#: architecture, section 19.7).
FACTUAL_POLICY_VERSION = "factual-v1"
#: Version of the generic (non-factual) review contract, for the same reuse rule.
REVIEW_POLICY_VERSION = "review-v1"

EVIDENCE_RULES = {
    "E1": "at least one material claim",
    "E2": "unique claim ids",
    "E3": "every verified material claim cites existing supporting evidence",
    "E4": "supporting evidence quotes a source held for this task",
    "E5": "no contradicting evidence for a verified claim",
}

#: Source kinds whose content CareerOS can hold for a task. ``web`` is absent: there is no
#: retrieval in this build, so web content can never be quoted from a held source.
HELD_SOURCE_TYPES = frozenset({"job_posting", "profile_fact", "user", "research_cache"})

#: Verdicts a reviewer may give per claim.
CLAIM_VERDICTS = frozenset({"supported", "unsupported", "contradicted", "not_checkable"})


def _norm(text: str) -> str:
    return re.sub(r"\s+", " ", text).strip().lower()


def sources_for(params: dict[str, Any], deps: dict[str, dict[str, Any]]) -> dict[str, str]:
    """The sources a factual task may quote: the objective's text inputs and the accepted
    outputs of the steps it depends on - all held locally. Keys are the ``source_ref``s."""
    out: dict[str, str] = {}
    for key, value in params.items():
        if isinstance(value, str) and value.strip():
            out["job_posting" if key == "text" else f"input:{key}"] = value
    for step, output in deps.items():
        out[f"step:{step}"] = json.dumps(output, sort_keys=True)
    return out


def source_type_for(ref: str) -> str:
    return "job_posting" if ref == "job_posting" else "user"


def fingerprint(value: Any) -> str:
    return hashlib.sha256(json.dumps(value, sort_keys=True, default=str).encode()).hexdigest()[:32]


def evidence_instructions(sources: dict[str, str]) -> str:
    """The evidence contract stated to the authoring model (part of the prompt, so the
    cache key and replay hash change with the policy)."""
    listing = "\n".join(f"[{ref}] {text}" for ref, text in sources.items()) or "(none)"
    return (
        f"Evidence policy {FACTUAL_POLICY_VERSION}: list every factual claim under 'claims'"
        " as {id, text, status, evidence}, status one of verified/unverified/assumption/"
        "uncertain. A claim is 'verified' only if each id in its 'evidence' names an entry"
        " of 'evidence' as {id, claim, source_type, source_ref, quote, stance} whose quote is"
        " copied verbatim from one of the sources below (source_ref = the name in brackets)."
        " Anything else must be labelled unverified, assumption or uncertain. Do not use"
        " outside knowledge as evidence.\nSources:\n" + listing
    )


@dataclass
class FactCheck:
    passed: bool
    violations: list[str] = field(default_factory=list)
    verified: list[str] = field(default_factory=list)
    unverified: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "passed": self.passed,
            "violations": self.violations,
            "verified": self.verified,
            "unverified": self.unverified,
            "rules": EVIDENCE_RULES,
        }


def check_claims(result: WorkerResult, sources: dict[str, str]) -> FactCheck:
    """Deterministic evidence check of a factual result (rules E1-E6)."""
    v: list[str] = []
    material = [c for c in result.claims if c.material]
    if not material:
        v.append("E1: no material claims stated")
    ids = [c.id for c in result.claims]
    dupes = sorted({i for i in ids if ids.count(i) > 1})
    if dupes:
        v.append(f"E2: duplicate claim ids {dupes}")
    evidence = {e.id: e for e in result.evidence if e.id}
    normalised = {ref: _norm(text) for ref, text in sources.items()}
    verified, labelled = [], []
    for c in result.claims:
        if c.status is not ClaimStatus.VERIFIED:
            labelled.append(c.id)
            continue
        verified.append(c.id)
        if not c.material:
            continue
        cited = [evidence.get(eid) for eid in c.evidence]
        missing = [eid for eid, e in zip(c.evidence, cited, strict=True) if e is None]
        if missing:
            v.append(f"E3: claim {c.id} cites unknown evidence {missing}")
        supporting = [e for e in cited if e is not None and e.stance == "supports"]
        if not supporting:
            v.append(f"E3: verified claim {c.id} has no supporting evidence")
        for e in supporting:
            if e.source_type.value not in HELD_SOURCE_TYPES:
                v.append(
                    f"E4: claim {c.id} evidence {e.id} cites a {e.source_type.value} source,"
                    " which CareerOS does not hold"
                )
            elif e.source_ref not in normalised:
                v.append(f"E4: claim {c.id} evidence {e.id} cites unknown source {e.source_ref!r}")
            elif not (e.quote and e.quote.strip()):
                v.append(f"E4: claim {c.id} evidence {e.id} has no quote")
            elif _norm(e.quote) not in normalised[e.source_ref]:
                v.append(
                    f"E4: claim {c.id} evidence {e.id} quote does not occur in {e.source_ref!r}"
                )
        if any(e is not None and e.stance == "contradicts" for e in cited):
            v.append(f"E5: verified claim {c.id} has contradicting evidence")
    return FactCheck(not v, v, verified, labelled)


def check_review(
    result: WorkerResult, verdict: str, output: dict[str, Any]
) -> tuple[str, list[str], dict[str, str]]:
    """Validate a factual review: every verified material claim must have a per-claim
    verdict, and any verdict other than ``supported`` (``unsupported``, ``contradicted``,
    ``not_checkable``) turns the review into a revision request whatever the reviewer's
    overall verdict. Returns (verdict, issues,
    per-claim verdicts)."""
    must = {c.id for c in result.claims if c.status is ClaimStatus.VERIFIED and c.material}
    raw = output.get("claim_verdicts")
    per: dict[str, str] = {}
    issues: list[str] = []
    if not isinstance(raw, list):
        return "escalate", ["review did not return claim_verdicts"], per
    for item in raw:
        if isinstance(item, dict) and str(item.get("verdict")) in CLAIM_VERDICTS:
            per[str(item.get("id"))] = str(item["verdict"])
    unassessed = sorted(must - per.keys())
    if unassessed:
        return "escalate", [f"review did not assess verified claims {unassessed}"], per
    # A verified claim must be confirmed as supported; "not_checkable" is not enough.
    bad = sorted(i for i in must if per[i] != "supported")
    if bad:
        issues = [f"claim {i} is {per[i]} by its evidence" for i in bad]
        return ("revise" if verdict in ("accept", "revise") else verdict), issues, per
    return verdict, issues, per
