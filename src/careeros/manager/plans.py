"""Plan and playbook specifications, validation and parallel-wave analysis."""

from __future__ import annotations

import re
from pathlib import Path
from typing import Any, Literal

import yaml
from pydantic import BaseModel, ConfigDict, Field, ValidationError, model_validator

from careeros.core.config import Priority
from careeros.core.errors import ConfigError
from careeros.llm.contracts import OutputSchema
from careeros.workers.base import WorkerRegistry


class PlanValidationError(ValueError):
    pass


class OutputSpec(BaseModel):
    model_config = ConfigDict(extra="forbid")
    required: list[str] = Field(default_factory=list)
    types: dict[str, Literal["string", "list", "number"]] = Field(default_factory=dict)
    kind: str = "generic"

    def to_schema(self) -> OutputSchema:
        return OutputSchema(tuple(self.required), dict(self.types), self.kind)


class StepSpec(BaseModel):
    model_config = ConfigDict(extra="forbid")
    id: str = Field(pattern=r"^[a-z][a-z0-9_]{0,40}$")
    worker: str
    task_type: str
    mode: Literal["deterministic", "model"] = "model"
    title: str = ""
    depends_on: list[str] = Field(default_factory=list)
    instructions: str = ""
    params: dict[str, Any] = Field(default_factory=dict)
    output: OutputSpec = Field(default_factory=OutputSpec)
    priority: Priority | None = None  # defaults to the objective's priority
    important: bool | None = None  # defaults to the task-type policy
    review: bool | None = None  # independent model review; defaults to `important`
    optional: bool = False
    budget_usd: float | None = Field(default=None, ge=0)
    max_attempts: int | None = Field(default=None, ge=1, le=10)


class PlanSpec(BaseModel):
    model_config = ConfigDict(extra="forbid")
    steps: list[StepSpec] = Field(min_length=1)
    rationale: str | None = None

    @model_validator(mode="after")
    def _structure(self) -> PlanSpec:
        ids = [s.id for s in self.steps]
        if len(set(ids)) != len(ids):
            raise ValueError(f"duplicate step ids in {ids}")
        for s in self.steps:
            for d in s.depends_on:
                if d not in ids:
                    raise ValueError(f"step {s.id!r} depends on unknown step {d!r}")
        waves(self)  # raises on cycles
        return self


class Playbook(BaseModel):
    model_config = ConfigDict(extra="forbid")
    name: str
    description: str = ""
    keywords: list[str] = Field(default_factory=list)
    priority: Priority = "normal"
    budget_usd: float | None = Field(default=None, ge=0)
    definition_of_done: list[str] = Field(default_factory=lambda: ["accepted:all_required"])
    plan: PlanSpec


def waves(plan: PlanSpec) -> list[list[str]]:
    """Topological levels: steps in the same wave have no dependency on each other and
    could run in parallel. Raises ``ValueError`` on a cycle."""
    remaining = {s.id: set(s.depends_on) for s in plan.steps}
    done: set[str] = set()
    out: list[list[str]] = []
    while remaining:
        level = sorted(sid for sid, deps in remaining.items() if deps <= done)
        if not level:
            raise ValueError(f"dependency cycle among steps {sorted(remaining)}")
        out.append(level)
        done.update(level)
        for sid in level:
            del remaining[sid]
    return out


def check_workers(plan: PlanSpec, registry: WorkerRegistry) -> None:
    for s in plan.steps:
        if not registry.has(s.worker, s.mode):
            raise PlanValidationError(f"step {s.id!r}: no {s.mode} worker named {s.worker!r}")


def parse_plan(data: dict[str, Any], registry: WorkerRegistry) -> PlanSpec:
    try:
        plan = PlanSpec.model_validate(data)
    except ValidationError as exc:
        raise PlanValidationError(str(exc)) from exc
    check_workers(plan, registry)
    return plan


class PlaybookLibrary:
    def __init__(self, playbooks: list[Playbook]) -> None:
        self.playbooks = {p.name: p for p in playbooks}

    @classmethod
    def load(cls, directory: Path, registry: WorkerRegistry) -> PlaybookLibrary:
        books = []
        for path in sorted(directory.glob("*.yaml")) if directory.is_dir() else []:
            try:
                book = Playbook.model_validate(yaml.safe_load(path.read_text(encoding="utf-8")))
                check_workers(book.plan, registry)
            except (ValidationError, PlanValidationError, yaml.YAMLError) as exc:
                raise ConfigError(f"Invalid playbook {path.name}: {exc}") from exc
            books.append(book)
        return cls(books)

    def match(self, kind: str | None, title: str) -> tuple[Playbook | None, str]:
        """Deterministic understanding: exact kind, else unambiguous keyword match."""
        if kind and kind in self.playbooks:
            return self.playbooks[kind], f"exact playbook match for kind {kind!r}"
        tokens = set(re.findall(r"[a-z]+", title.lower()))
        scored = sorted(
            (
                (len(tokens & {k.lower() for k in p.keywords}), p.name)
                for p in self.playbooks.values()
            ),
            reverse=True,
        )
        if scored and scored[0][0] >= 2 and (len(scored) == 1 or scored[1][0] < scored[0][0]):
            return self.playbooks[scored[0][1]], f"keyword match (score {scored[0][0]})"
        if scored and scored[0][0] >= 2:
            return None, "ambiguous: several playbooks match equally"
        return None, "no familiar playbook matches"
