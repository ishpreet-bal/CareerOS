"""Token -> USD conversion and the worst-case reservation rule (Phase A, audit M1).

Expected cost drives routing decisions; *reservations* use the worst case: every call is
sent with a hard output cap, and the amount reserved is the cost of the (margin-adjusted)
input plus a full cap of output. If the provider honours the cap and the input estimate is
within its margin, the settled cost can never exceed the reservation, so concurrent calls
can never jointly exceed a budget.
"""

from __future__ import annotations

import math

from careeros.core.config import AppConfig, Effort, ModelProfile


def call_cost_usd(profile: ModelProfile, input_tokens: int, output_tokens: int) -> float:
    p = profile.pricing
    if p.input_per_mtok is None or p.output_per_mtok is None:
        raise ValueError(f"Model {profile.id!r} has no pricing.")
    return (input_tokens * p.input_per_mtok + output_tokens * p.output_per_mtok) / 1_000_000


def expected_output_tokens(config: AppConfig, task_type: str, effort: Effort) -> int:
    policy = config.routing.policy(task_type)
    return round(
        policy.expected_output_tokens * config.routing.effort_output_multiplier.get(effort, 1.0)
    )


def output_cap_tokens(
    config: AppConfig, task_type: str, effort: Effort, max_output_tokens: int | None = None
) -> int:
    """Hard per-call output limit sent to the provider."""
    cap = math.ceil(
        expected_output_tokens(config, task_type, effort) * config.routing.output_cap_factor
    )
    if max_output_tokens is not None:
        cap = min(cap, max_output_tokens)
    return max(1, cap)


def reservation_usd(
    config: AppConfig, profile: ModelProfile, input_tokens: int, output_cap: int
) -> float:
    """Worst-case cost of one call: margin-adjusted input plus a full output cap."""
    margin_in = math.ceil(input_tokens * config.budgets.input_token_margin)
    return call_cost_usd(profile, margin_in, output_cap)
