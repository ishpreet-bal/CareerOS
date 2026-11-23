"""Deterministic complexity estimation from task features (no model call)."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from careeros.core.config import TaskTypePolicy


@dataclass(frozen=True)
class ComplexityEstimate:
    value: float  # used for routing (includes prior-failure adjustment)
    bucket: str  # used to key historical stats (excludes prior failures)
    factors: dict[str, float]


def bucket_for(value: float) -> str:
    return "low" if value < 0.34 else "mid" if value < 0.67 else "high"


def estimate_complexity(
    policy: TaskTypePolicy, task_input: dict[str, Any], prior_failures: int = 0
) -> ComplexityEstimate:
    factors: dict[str, float] = {"base": policy.base_complexity}
    if task_input.get("ambiguous"):
        factors["ambiguity"] = 0.15
    hint = task_input.get("complexity_hint")
    if isinstance(hint, int | float):
        factors["hint"] = float(hint)
    size = sum(len(str(v)) for v in task_input.values())
    if size > 4000:
        factors["input_size"] = 0.10
    intrinsic = min(1.0, max(0.0, sum(factors.values())))
    failure_adj = min(0.15, 0.05 * prior_failures)
    if failure_adj:
        factors["prior_failures"] = failure_adj
    return ComplexityEstimate(
        value=min(1.0, intrinsic + failure_adj), bucket=bucket_for(intrinsic), factors=factors
    )
