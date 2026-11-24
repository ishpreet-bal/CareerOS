"""A deterministic, offline model simulator.

Each mock profile has a ``simulation.skill``: the task complexity it handles reliably.
Success probability falls as a task's true complexity exceeds the model's skill plus an
effort bonus, so stronger tiers and higher effort succeed more often but cost more. All
randomness is seeded from the request, so runs are reproducible.

Nothing here performs I/O, opens sockets or sleeps; latency is simulated and reported.
"""

from __future__ import annotations

import hashlib
import json
import random
from dataclasses import dataclass
from typing import Any, ClassVar, Literal

from careeros.core.config import Effort, ModelProfile, SimulationProfile
from careeros.llm.contracts import (
    ModelRateLimitError,
    ModelRequest,
    ModelResponse,
    ModelTimeoutError,
    OutputSchema,
)
from careeros.llm.tokens import estimate_tokens

OutcomeKind = Literal[
    "success", "low_confidence", "fail_quality", "malformed", "invalid", "timeout", "rate_limit"
]

#: Simulation ground truth (independent of the router's priors in routing.yaml).
_EFFORT_SKILL_BONUS: dict[Effort, float] = {
    "none": 0.0,
    "low": 0.04,
    "medium": 0.07,
    "high": 0.10,
    "max": 0.12,
}
_EFFORT_THINKING: dict[Effort, float] = {
    "none": 1.0,
    "low": 1.5,
    "medium": 2.0,
    "high": 3.0,
    "max": 4.0,
}
_DEFAULT_SIM = SimulationProfile(skill=0.5)


@dataclass(frozen=True)
class Outcome:
    kind: OutcomeKind
    quality: float | None = None  # forced hidden quality for success-like outcomes
    review_verdict: str | None = None  # forced verdict for review requests


def success_probability(skill: float, effort: Effort, complexity: float) -> float:
    effective = skill + _EFFORT_SKILL_BONUS[effort]
    return min(0.97, max(0.03, 0.97 - 1.6 * max(0.0, complexity - effective)))


class MockAdapter:
    provider: ClassVar[str] = "mock"
    offline: ClassVar[bool] = True
    mode_label: ClassVar[str] = "mock"
    supports_provider_idempotency: ClassVar[bool] = False

    def map_effort(self, effort: Effort) -> dict[str, Any]:
        return {"simulated_effort": effort, "thinking_multiplier": _EFFORT_THINKING[effort]}

    # ------------------------------------------------------------------ public API
    def call(self, profile: ModelProfile, request: ModelRequest) -> ModelResponse:
        return self.simulate(profile, request, forced=None)

    def simulate(
        self, profile: ModelProfile, request: ModelRequest, forced: Outcome | None
    ) -> ModelResponse:
        sim = profile.simulation or _DEFAULT_SIM
        rng = self._rng(profile, request)
        complexity = float(
            request.simulation.get("true_complexity", request.simulation.get("complexity", 0.5))
        )
        input_tokens = estimate_tokens((request.system or "") + request.prompt)
        outcome = forced or self._draw_outcome(sim, request.effort, complexity, rng)

        if outcome.kind in ("timeout", "rate_limit"):
            latency = sim.timeout_ms if outcome.kind == "timeout" else sim.base_latency_ms
            err_cls = ModelTimeoutError if outcome.kind == "timeout" else ModelRateLimitError
            raise err_cls(
                f"simulated {outcome.kind} from {profile.id}",
                input_tokens=input_tokens if outcome.kind == "timeout" else 0,
                latency_ms=latency,
            )

        quality = outcome.quality
        if quality is None:
            if outcome.kind in ("success", "malformed", "invalid"):
                quality = rng.uniform(0.80, 0.98)
            elif outcome.kind == "low_confidence":
                quality = rng.uniform(0.55, 0.68)
            else:
                quality = rng.uniform(0.35, 0.68)
        confidence = min(1.0, max(0.0, quality + rng.uniform(-0.08, 0.05)))
        if outcome.kind == "low_confidence":
            confidence = min(confidence, rng.uniform(0.40, 0.62))

        payload = self._payload(profile, request, quality, confidence, outcome, sim, rng)
        if outcome.kind == "invalid" and request.output_schema.required:
            drop = request.output_schema.required[0]
            payload["output"].pop(drop, None)
        text = json.dumps(payload, separators=(",", ":"))
        if outcome.kind == "malformed":
            text = text[: max(10, len(text) // 2)] + " ...[truncated]"

        visible = estimate_tokens(text)
        target = request.simulation.get("target_output_tokens")
        if isinstance(target, int | float) and outcome.kind != "malformed":
            # Real replies are longer than these compact synthetic payloads.
            visible = max(visible, round(float(target) * rng.uniform(0.7, 1.1)))
        output_tokens = min(
            request.max_output_tokens, round(visible * _EFFORT_THINKING[request.effort])
        )
        latency = round(sim.base_latency_ms + sim.ms_per_output_token * output_tokens)
        capped = output_tokens >= request.max_output_tokens
        rid_src = f"{profile.id}|{request.client_request_id or request.seed_salt}|{request.prompt}"
        rid = hashlib.sha256(rid_src.encode()).hexdigest()[:16]
        return ModelResponse(
            text=text,
            input_tokens=input_tokens,
            output_tokens=output_tokens,
            latency_ms=latency,
            finish_reason="length" if capped else "stop",
            simulation={"sim_quality": round(quality, 4), "outcome": outcome.kind},
            provider_request_id=f"mock-{rid}",
        )

    # ------------------------------------------------------------------ internals
    @staticmethod
    def _rng(profile: ModelProfile, request: ModelRequest) -> random.Random:
        material = "|".join(
            [profile.id, request.effort, request.task_type, request.prompt, request.seed_salt]
        )
        seed = int(hashlib.sha256(material.encode()).hexdigest()[:16], 16)
        return random.Random(seed)  # noqa: S311 - simulation, not security

    @staticmethod
    def _draw_outcome(
        sim: SimulationProfile, effort: Effort, complexity: float, rng: random.Random
    ) -> Outcome:
        u = rng.random()
        for kind, rate in (
            ("malformed", sim.malformed_rate),
            ("invalid", sim.invalid_rate),
            ("timeout", sim.timeout_rate),
            ("rate_limit", sim.rate_limit_rate),
        ):
            if u < rate:
                return Outcome(kind)  # type: ignore[arg-type]
            u -= rate
        if rng.random() < success_probability(sim.skill, effort, complexity):
            return Outcome("success")
        return Outcome("fail_quality")

    def _payload(
        self,
        profile: ModelProfile,
        request: ModelRequest,
        quality: float,
        confidence: float,
        outcome: Outcome,
        sim: SimulationProfile,
        rng: random.Random,
    ) -> dict[str, Any]:
        schema = request.output_schema
        if schema.kind == "review":
            output = self._review_output(request, sim, rng, outcome)
            confidence = min(1.0, max(0.0, sim.skill + rng.uniform(0.0, 0.1)))
        elif schema.kind == "plan":
            output = self._plan_output(request, quality)
        else:
            output = _fill_generic(schema, request.task_type, quality)
        return {
            "output": output,
            "confidence": round(confidence, 3),
            "evidence": [
                {
                    "claim": f"Simulated finding for {request.task_type}",
                    "source_type": "user",
                    "source_ref": "simulation",
                }
            ],
            "assumptions": ["Simulated output from an offline mock model."],
            "unresolved_questions": [] if quality >= 0.7 else ["Simulated gap in the answer."],
            "recommended_next_steps": [],
        }

    @staticmethod
    def _review_output(
        request: ModelRequest, sim: SimulationProfile, rng: random.Random, outcome: Outcome
    ) -> dict[str, Any]:
        subject = float(request.simulation.get("subject_sim_quality", 0.5))
        accept_at = float(request.simulation.get("accept_score", 0.75))
        noise = (1.0 - sim.skill) * 0.15
        score = min(1.0, max(0.0, subject + rng.uniform(-noise, noise)))
        verdict = outcome.review_verdict
        if verdict is None:
            verdict = "accept" if score >= accept_at else "revise" if score >= 0.55 else "escalate"
        elif verdict == "accept":
            score = max(score, accept_at)
        elif verdict == "revise":
            score = min(score, accept_at - 0.05)
        issues = [] if verdict == "accept" else ["Simulated: claims need more specific evidence."]
        return {"verdict": verdict, "score": round(score, 3), "issues": issues}

    @staticmethod
    def _plan_output(request: ModelRequest, quality: float) -> dict[str, Any]:
        workers: list[dict[str, Any]] = list(request.simulation.get("available_workers", []))
        if not workers:
            return {"steps": []}
        if quality < 0.7:
            # A weak plan: references a worker that does not exist.
            return {"steps": [{"id": "s1", "worker": "nonexistent_worker", "task_type": "x"}]}
        steps = []
        prev: str | None = None
        for i, w in enumerate(workers[:3], start=1):
            sid = f"step{i}"
            steps.append(
                {
                    "id": sid,
                    "worker": w["worker"],
                    "task_type": w["task_type"],
                    "mode": w.get("mode", "model"),
                    "depends_on": [prev] if prev else [],
                    "title": f"Planned step {i}: {w['task_type']}",
                }
            )
            prev = sid
        return {"steps": steps, "rationale": "Simulated plan: linear chain of known workers."}


def _fill_generic(schema: OutputSchema, task_type: str, quality: float) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for name in schema.required:
        kind = schema.types.get(name, "string")
        if kind == "list":
            out[name] = [f"synthetic {name} {i}" for i in range(1, 4)]
        elif kind == "number":
            out[name] = round(quality, 3)
        else:
            out[name] = f"Synthetic {name} for {task_type} (simulated quality {quality:.2f})."
    return out
