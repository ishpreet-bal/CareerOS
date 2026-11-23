"""Instrumented workers and adapters that make concurrency observable and deterministic."""

from __future__ import annotations

import threading
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any, ClassVar

from careeros.context import AppContext
from careeros.core.config import AppConfig, ModelProfile
from careeros.domain.models import ResultStatus, WorkerResult
from careeros.llm.adapters.mock import MockAdapter
from careeros.llm.contracts import ModelRequest, ModelResponse
from careeros.manager.manager import CareerManager
from careeros.manager.plans import Playbook, PlaybookLibrary
from careeros.simulation import SimulatedCrashError
from careeros.workers.base import StepContext
from careeros.workers.generic import default_registry


@dataclass
class Probe:
    """Shared record of what ran when, on which thread, and how many at once."""

    active: int = 0
    max_active: int = 0
    starts: dict[str, float] = field(default_factory=dict)
    ends: dict[str, float] = field(default_factory=dict)
    runs: dict[str, int] = field(default_factory=dict)
    threads: set[int] = field(default_factory=set)
    lock: threading.Lock = field(default_factory=threading.Lock)


class ProbeWorker:
    """Deterministic worker. Params: ``target`` (wait until this many probes are active,
    up to ``hold`` seconds), ``fail`` (return FAILED), ``raise_times`` (raise on the first
    N runs of this step)."""

    name = "probe"

    def __init__(self, probe: Probe) -> None:
        self.probe = probe

    def run(self, step: StepContext) -> WorkerResult:
        p, sid = self.probe, step.step_id
        with p.lock:
            p.active += 1
            p.max_active = max(p.max_active, p.active)
            p.starts.setdefault(sid, time.monotonic())
            p.runs[sid] = p.runs.get(sid, 0) + 1
            p.threads.add(threading.get_ident())
            run_no = p.runs[sid]
        try:
            target = int(step.params.get("target", 1))
            deadline = time.monotonic() + float(step.params.get("hold", 2.0))
            while time.monotonic() < deadline:
                with p.lock:
                    if p.active >= target:
                        break
                time.sleep(0.002)
            time.sleep(float(step.params.get("work", 0.01)))
            if run_no <= int(step.params.get("raise_times", 0)):
                raise RuntimeError(f"probe {sid} simulated worker bug (run {run_no})")
            failed = bool(step.params.get("fail"))
            return WorkerResult(
                status=ResultStatus.FAILED if failed else ResultStatus.SUCCEEDED,
                output={"step": sid},
                confidence=0.0 if failed else 1.0,
            )
        finally:
            with p.lock:
                p.active -= 1
                p.ends[sid] = time.monotonic()


class GatedAdapter(MockAdapter):
    """Mock adapter whose first ``n`` calls wait for each other, so they are provably in
    flight at the same time. Can crash (simulated process death) inside a call."""

    mode_label: ClassVar[str] = "mock"

    def __init__(
        self,
        n: int,
        *,
        timeout: float = 3.0,
        crash_in_call: set[str] | None = None,
        on_call: Callable[[str | None], None] | None = None,
        hold_until: Callable[[], bool] | None = None,
    ) -> None:
        self.n, self.timeout = n, timeout
        #: If set, the first ``n`` calls stay in flight until it returns True (or timeout).
        self.hold_until = hold_until
        self.crash_in_call = set(crash_in_call or ())
        self.on_call = on_call
        self.cond = threading.Condition()
        self.arrived = self.active = self.max_active = 0
        self.calls: list[tuple[str | None, str]] = []

    def call(self, profile: ModelProfile, request: ModelRequest) -> ModelResponse:
        step = request.simulation.get("step")
        with self.cond:
            self.arrived += 1
            self.active += 1
            self.max_active = max(self.max_active, self.active)
            self.calls.append((step, request.task_type))
            self.cond.notify_all()
            deadline = time.monotonic() + self.timeout
            while self.arrived < self.n and time.monotonic() < deadline:
                self.cond.wait(timeout=max(0.0, deadline - time.monotonic()))
            crash = step in self.crash_in_call and request.task_type != "quality.review"
            if crash:
                self.crash_in_call.discard(step)
            held = self.hold_until is not None and self.arrived <= self.n
        if held:
            assert self.hold_until is not None
            deadline = time.monotonic() + self.timeout
            while not self.hold_until() and time.monotonic() < deadline:
                time.sleep(0.005)
        try:
            if self.on_call:
                self.on_call(step)
            if crash:
                raise SimulatedCrashError(f"process died while {step} was waiting on a model")
            return super().call(profile, request)
        finally:
            with self.cond:
                self.active -= 1


def book(name: str, steps: list[dict[str, Any]], **extra: Any) -> Playbook:
    return Playbook.model_validate({"name": name, "plan": {"steps": steps}, **extra})


def probe_step(sid: str, deps: list[str] | None = None, **params: Any) -> dict[str, Any]:
    return {
        "id": sid,
        "worker": "probe",
        "mode": "deterministic",
        "task_type": "job.normalize",
        "depends_on": deps or [],
        "params": params,
    }


def model_step(
    sid: str, deps: list[str] | None = None, task_type: str = "research.company", **extra: Any
) -> dict[str, Any]:
    return {
        "id": sid,
        "worker": "model_step",
        "task_type": task_type,
        "depends_on": deps or [],
        "instructions": f"Synthetic instructions for {sid}.",
        "output": {"required": ["summary"], "types": {"summary": "string"}},
        **extra,
    }


def probe_manager(ctx: AppContext, playbook: Playbook, probe: Probe, **kw: Any) -> CareerManager:
    reg = default_registry()
    reg.add_deterministic(ProbeWorker(probe))
    kw.setdefault("adapter", MockAdapter())
    return CareerManager(ctx, workers=reg, playbooks=PlaybookLibrary([playbook]), **kw)


def with_limits(cfg: AppConfig, **execution: Any) -> AppConfig:
    return cfg.model_copy(update={"execution": cfg.execution.model_copy(update=execution)})
