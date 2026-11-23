"""Token -> USD conversion from a profile's configured (simulated) pricing."""

from careeros.core.config import ModelProfile


def call_cost_usd(profile: ModelProfile, input_tokens: int, output_tokens: int) -> float:
    p = profile.pricing
    if p.input_per_mtok is None or p.output_per_mtok is None:
        raise ValueError(f"Model {profile.id!r} has no pricing.")
    return (input_tokens * p.input_per_mtok + output_tokens * p.output_per_mtok) / 1_000_000
