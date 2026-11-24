# ADR 0008: Factual evidence policy, independent factual review and review reuse on cache hits

- Status: accepted
- Date: 2026-11-24

## Context

Audit L11: `research.company` results - employer facts that cover letters and tailored
resumes reuse - were accepted on schema shape and model confidence. They were not reviewed
by default, their `evidence` list was never checked, and the reviewer (when there was one)
saw only the prose output. Audit N4: the cache-hit path skipped review on the assumption
that "only accepted (already reviewed) results are cached", but every accepted result was
cached, reviewed or not, and entries recorded nothing about review or policy. A step that
required review could therefore be accepted from the cache with no review at all.

## Decision

1. Task types may be marked `factual` (routing policy). `research.company` is factual.
2. A factual result states its material claims explicitly (`claims`: id, text, status
   `verified | unverified | assumption | uncertain`, evidence ids). Only verified claims
   may be relied on as facts. Evidence entries carry an id, the `source_ref` of a source
   CareerOS holds for the task, a verbatim quote and a stance (supports / contradicts).
3. Deterministic rules E1-E5 (`careeros.quality.factual`) are checked for every factual
   result before any review: at least one material claim; unique ids; every verified
   material claim cites existing supporting evidence; each supporting quote occurs in a
   held source of a holdable kind (the web is not one: there is no retrieval); no
   contradicting evidence for a verified claim. Confidence is never consulted. A failure
   is the outcome `factual_unsupported`: one repair retry with the violations as feedback,
   then escalation, then the user - never acceptance.
4. Every factual result that passes is reviewed by a different model (the existing
   author/reviewer exclusion) that is shown the claims, evidence and sources and must
   return a verdict per verified claim. Any verified claim not judged `supported` - or a
   review without per-claim verdicts - turns the review into a revision (or escalation)
   whatever the reviewer's overall verdict.
5. Every check is recorded in `result_checks` (one row per result and kind, idempotent),
   with the policy version, the fingerprints of the payload and of the sources, the
   reasons and the review's model call. The accepting transaction refuses to accept a
   result without the checks its task requires (`FactualGateError`).
6. A cache hit saves generation only. Validation always re-runs against the current
   sources. A prior review is reused only if it passed, under the current review policy
   version, for an identical payload judged against identical sources, by a model other
   than the payload's author; the reuse is recorded for the new result with
   `reused_from`. Otherwise the hit is reviewed afresh. The evidence contract is part of
   the system prompt, so a policy change changes the cache key and the replay hash.

## Consequences

- Research costs one more (simulated) review call per accepted result; demo budgets were
  re-sized so that the budget scenario still exercises the critical-review approval.
- The deterministic layer proves only that quotes exist in held sources, not that they
  support the claim; that judgement is the reviewer's, and in this build the reviewer is a
  simulated model. With real providers the reviewer's reliability must be calibrated
  (synthetic evaluation set with labelled fabrications) before relying on it.
- Claim extraction relies on the author listing its claims; prose that states facts not
  listed as claims is a residual risk the reviewer must catch.
