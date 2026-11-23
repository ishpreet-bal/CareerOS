"""Generic workers: one configurable model-step worker and a few deterministic utilities."""

from __future__ import annotations

import json
import re
from collections import Counter
from typing import Any

from pydantic import ValidationError

from careeros.domain.models import ResultStatus, WorkerResult
from careeros.llm.contracts import ModelRequest
from careeros.workers.base import OutputValidationError, StepContext, WorkerRegistry

SYSTEM_PROMPT = (
    "You are a CareerOS worker. Reply with one JSON object with keys: output, confidence "
    "(0-1), evidence, assumptions, unresolved_questions, recommended_next_steps. Use only "
    "facts present in the input; list anything uncertain under unresolved_questions."
)

_STOPWORDS = frozenset(
    [
        "a",
        "an",
        "and",
        "are",
        "as",
        "at",
        "be",
        "by",
        "for",
        "from",
        "has",
        "have",
        "in",
        "is",
        "it",
        "of",
        "on",
        "or",
        "our",
        "the",
        "their",
        "this",
        "to",
        "we",
        "will",
        "with",
        "you",
        "your",
        "who",
        "what",
        "experience",
        "work",
        "team",
    ]
)


class ModelStepWorker:
    """Builds a prompt from step instructions, parameters and dependency outputs."""

    name = "model_step"

    def build_request(self, step: StepContext) -> ModelRequest:
        parts = [
            f"Task: {step.title}",
            f"Instructions: {step.instructions}",
            "Inputs: " + json.dumps(step.params, sort_keys=True),
        ]
        if step.deps:
            parts.append("Results of earlier steps: " + json.dumps(step.deps, sort_keys=True))
        parts.append("Required output fields: " + json.dumps(step.output_schema.to_dict()))
        if step.feedback:
            parts.append(f"Your previous attempt was rejected: {step.feedback} Fix this.")
        return ModelRequest(
            task_type=step.task_type,
            prompt="\n".join(parts),
            system=SYSTEM_PROMPT,
            output_schema=step.output_schema,
        )

    def parse(self, step: StepContext, payload: dict[str, Any]) -> WorkerResult:
        return validate_payload(step, payload)


def validate_payload(step: StepContext, payload: dict[str, Any]) -> WorkerResult:
    output = payload.get("output")
    if not isinstance(output, dict):
        raise OutputValidationError("reply has no 'output' object")
    problems = []
    for name in step.output_schema.required:
        if name not in output:
            problems.append(f"missing field '{name}'")
            continue
        kind = step.output_schema.types.get(name, "string")
        value = output[name]
        if kind == "list" and not (isinstance(value, list) and value):
            problems.append(f"field '{name}' must be a non-empty list")
        elif kind == "number" and not isinstance(value, int | float):
            problems.append(f"field '{name}' must be a number")
        elif kind == "string" and not (isinstance(value, str) and value.strip()):
            problems.append(f"field '{name}' must be a non-empty string")
    if problems:
        raise OutputValidationError("; ".join(problems))
    try:
        return WorkerResult.model_validate({**payload, "status": "succeeded"})
    except ValidationError as exc:
        raise OutputValidationError(f"reply does not match the result contract: {exc}") from exc


# ------------------------------------------------------------------ deterministic workers
class ExtractKeywordsWorker:
    """Frequency-based keyword extraction from ``params.text`` (no model needed)."""

    name = "extract_keywords"

    def run(self, step: StepContext) -> WorkerResult:
        text = str(step.params.get("text", ""))
        words = [w for w in re.findall(r"[a-z][a-z+#.\-]{1,}", text.lower()) if w not in _STOPWORDS]
        top = [w for w, _ in Counter(words).most_common(int(step.params.get("limit", 8)))]
        return WorkerResult(
            status=ResultStatus.SUCCEEDED if top else ResultStatus.FAILED,
            output={"keywords": top, "word_count": len(words)},
            confidence=1.0 if top else 0.0,
            assumptions=["Keywords ranked by frequency after removing common words."],
        )


class GateWorker:
    """Checks a configured condition; when it fails, downstream steps are pruned (replan)."""

    name = "check_gate"

    def run(self, step: StepContext) -> WorkerResult:
        passed = bool(step.params.get("pass", True))
        reason = str(step.params.get("reason", "condition not met"))
        return WorkerResult(
            status=ResultStatus.SUCCEEDED,
            output={
                "passed": passed,
                "prune_dependents": not passed,
                "reason": "ok" if passed else reason,
            },
            confidence=1.0,
        )


class AssemblePackageWorker:
    """Collects accepted dependency outputs into one package (no model needed)."""

    name = "assemble_package"

    def run(self, step: StepContext) -> WorkerResult:
        return WorkerResult(
            status=ResultStatus.SUCCEEDED,
            output={"components": sorted(step.deps), "package": step.deps},
            confidence=1.0 if step.deps else 0.5,
        )


def default_registry() -> WorkerRegistry:
    reg = WorkerRegistry()
    reg.add_model(ModelStepWorker())
    reg.add_deterministic(ExtractKeywordsWorker())
    reg.add_deterministic(GateWorker())
    reg.add_deterministic(AssemblePackageWorker())
    return reg
