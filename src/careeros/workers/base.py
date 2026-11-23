"""Worker contracts.

* A **deterministic worker** is plain code: ``run(params, deps) -> WorkerResult``.
* A **model worker** builds a provider-neutral request and validates the structured reply.
  It never chooses a model; the manager and router do that.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Protocol

from careeros.domain.models import WorkerResult
from careeros.llm.contracts import ModelRequest, OutputSchema


class OutputValidationError(ValueError):
    """A model reply was valid JSON but did not satisfy the step's output contract."""


@dataclass(frozen=True)
class StepContext:
    step_id: str
    task_type: str
    title: str
    instructions: str
    params: dict[str, Any]
    #: Accepted outputs of dependencies, keyed by step id.
    deps: dict[str, dict[str, Any]]
    output_schema: OutputSchema
    #: Feedback from a failed attempt (validation errors, reviewer issues).
    feedback: str | None = None


class DeterministicWorker(Protocol):
    name: str

    def run(self, step: StepContext) -> WorkerResult: ...


class ModelWorker(Protocol):
    name: str

    def build_request(self, step: StepContext) -> ModelRequest: ...

    def parse(self, step: StepContext, payload: dict[str, Any]) -> WorkerResult: ...


class WorkerRegistry:
    def __init__(self) -> None:
        self.deterministic: dict[str, DeterministicWorker] = {}
        self.model: dict[str, ModelWorker] = {}

    def add_deterministic(self, worker: DeterministicWorker) -> None:
        self.deterministic[worker.name] = worker

    def add_model(self, worker: ModelWorker) -> None:
        self.model[worker.name] = worker

    def has(self, name: str, mode: str) -> bool:
        return name in (self.deterministic if mode == "deterministic" else self.model)

    def catalog(self) -> list[dict[str, str]]:
        return [{"worker": n, "mode": "deterministic"} for n in sorted(self.deterministic)] + [
            {"worker": n, "mode": "model"} for n in sorted(self.model)
        ]
