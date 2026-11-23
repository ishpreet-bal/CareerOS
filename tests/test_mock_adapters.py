import json
import time

import pytest

from careeros.core.config import AppConfig
from careeros.llm.adapters.mock import MockAdapter, success_probability
from careeros.llm.adapters.scripted import ScriptedAdapter, parse_directive
from careeros.llm.contracts import (
    ModelRateLimitError,
    ModelRequest,
    ModelTimeoutError,
    OutputSchema,
)
from tests.m2_helpers import profile

SCHEMA = OutputSchema(("summary", "points"), {"summary": "string", "points": "list"})


def _req(salt: str = "", effort: str = "none", complexity: float = 0.5) -> ModelRequest:
    return ModelRequest(
        task_type="research.company",
        prompt="Summarize Examplar.",
        output_schema=SCHEMA,
        effort=effort,
        seed_salt=salt,  # type: ignore[arg-type]
        simulation={"complexity": complexity, "target_output_tokens": 400},
    )


def _success_rate(adapter: MockAdapter, prof: object, effort: str, complexity: float) -> float:
    ok = 0
    for i in range(300):
        try:
            r = adapter.call(prof, _req(str(i), effort, complexity))  # type: ignore[arg-type]
        except (ModelTimeoutError, ModelRateLimitError):
            continue
        ok += r.simulation["outcome"] == "success"
    return ok / 300


def test_deterministic_for_same_request(cfg: AppConfig) -> None:
    a, p = MockAdapter(), profile(cfg, "mock-medium")
    assert a.call(p, _req("x")) == a.call(p, _req("x"))


def test_successful_output_matches_schema(cfg: AppConfig) -> None:
    a = ScriptedAdapter({"*": ["success:0.9"]})
    r = a.call(profile(cfg, "mock-medium"), _req())
    payload = json.loads(r.text)
    assert set(SCHEMA.required) <= set(payload["output"])
    assert 0 <= payload["confidence"] <= 1
    assert {"evidence", "assumptions", "unresolved_questions", "recommended_next_steps"} <= set(
        payload
    )
    assert r.input_tokens > 0 and r.output_tokens > 0 and r.latency_ms > 0


def test_stronger_tiers_succeed_more_on_hard_tasks(cfg: AppConfig) -> None:
    a = MockAdapter()
    small = _success_rate(a, profile(cfg, "mock-small"), "none", 0.8)
    medium = _success_rate(a, profile(cfg, "mock-medium"), "none", 0.8)
    large = _success_rate(a, profile(cfg, "mock-large"), "low", 0.8)
    assert small < medium < large


def test_higher_effort_improves_quality_and_costs_tokens(cfg: AppConfig) -> None:
    a, p = MockAdapter(), profile(cfg, "mock-medium")
    assert _success_rate(a, p, "high", 0.8) > _success_rate(a, p, "none", 0.8)
    r_none = a.call(p, _req("s", "none"))
    r_high = a.call(p, _req("s", "high"))
    assert r_high.output_tokens > r_none.output_tokens
    assert success_probability(0.68, "high", 0.8) > success_probability(0.68, "none", 0.8)


def test_cost_and_latency_differ_by_tier(cfg: AppConfig) -> None:
    a = ScriptedAdapter({"*": ["success", "success"]})
    small = a.call(profile(cfg, "mock-small"), _req())
    large = a.call(profile(cfg, "mock-large"), _req())
    assert large.latency_ms > small.latency_ms


@pytest.mark.parametrize(
    "directive", ["malformed", "invalid", "low_confidence", "timeout", "rate_limit", "fail_quality"]
)
def test_failure_modes(cfg: AppConfig, directive: str) -> None:
    a, p = ScriptedAdapter({"*": [directive]}), profile(cfg, "mock-medium")
    if directive == "timeout":
        with pytest.raises(ModelTimeoutError) as exc:
            a.call(p, _req())
        assert exc.value.latency_ms == p.simulation.timeout_ms  # type: ignore[union-attr]
        assert exc.value.input_tokens > 0
        return
    if directive == "rate_limit":
        with pytest.raises(ModelRateLimitError):
            a.call(p, _req())
        return
    r = a.call(p, _req())
    if directive == "malformed":
        with pytest.raises(json.JSONDecodeError):
            json.loads(r.text)
        return
    payload = json.loads(r.text)
    if directive == "invalid":
        assert "summary" not in payload["output"]
    elif directive == "low_confidence":
        assert payload["confidence"] < 0.65
    else:
        assert r.simulation["sim_quality"] < 0.7


def test_scripted_key_precedence_and_fallback(cfg: AppConfig) -> None:
    a = ScriptedAdapter(
        {"research.company@mock-small": ["malformed"], "research.company": ["low_confidence"]}
    )
    small, medium = profile(cfg, "mock-small"), profile(cfg, "mock-medium")
    assert a.call(small, _req()).simulation["outcome"] == "malformed"
    assert a.call(medium, _req()).simulation["outcome"] == "low_confidence"
    # Queues exhausted: falls back to the seeded mock.
    assert a.call(medium, _req("z")).simulation["outcome"] in {
        "success",
        "fail_quality",
        "invalid",
        "malformed",
    }
    assert [c[1] for c in a.calls] == ["mock-small", "mock-medium", "mock-medium"]


def test_bad_directive_rejected() -> None:
    with pytest.raises(ValueError):
        parse_directive("explode")
    with pytest.raises(ValueError):
        parse_directive("review:maybe")


def test_review_verdicts_track_subject_quality(cfg: AppConfig) -> None:
    a, p = MockAdapter(), profile(cfg, "mock-large")
    schema = OutputSchema(("verdict", "score"), {"score": "number"}, kind="review")

    def verdict(q: float) -> str:
        r = a.call(
            p,
            ModelRequest(
                task_type="quality.review",
                prompt="x",
                output_schema=schema,
                simulation={"subject_sim_quality": q},
            ),
        )
        return str(json.loads(r.text)["output"]["verdict"])

    assert verdict(0.95) == "accept"
    assert verdict(0.3) == "escalate"


def test_simulated_latency_does_not_sleep(cfg: AppConfig) -> None:
    a, p = MockAdapter(), profile(cfg, "mock-large")
    t0 = time.perf_counter()
    total = 0
    for i in range(100):
        try:
            total += a.call(p, _req(str(i), "max")).latency_ms
        except ModelTimeoutError as e:
            total += e.latency_ms
    assert total > 100_000  # >100 simulated seconds...
    assert time.perf_counter() - t0 < 2.0  # ...in well under two real seconds
