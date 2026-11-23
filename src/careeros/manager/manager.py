"""The Career Manager.

A coordinator invoked on demand (``submit``, ``run``, ``approve``). It keeps no state in
memory between calls: plans, tasks, routing decisions, model calls, results, reviews,
retries, escalations and checkpoints are all persisted, so any later invocation can resume.

Routine orchestration is deterministic: understanding uses playbooks, decisions use a
fixed table, and models are called only by model steps, independent reviews and
model-assisted planning for unfamiliar objectives.

M2 executes ready tasks one at a time in dependency order and records which tasks could
run in parallel (plan "waves"); concurrent execution is deferred to M3.
"""

from __future__ import annotations

import json
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import timedelta
from typing import Any, cast

from careeros.cache.store import CacheStore
from careeros.context import AppContext
from careeros.core.clock import utc_now, utc_now_iso
from careeros.core.config import EFFORT_ORDER, Effort
from careeros.core.db import from_json, to_json
from careeros.core.errors import CareerOSError, NotFoundError
from careeros.cost.budget import BudgetManager
from careeros.domain.models import (
    Checkpoint,
    Objective,
    ObjectiveStatus,
    Plan,
    Priority,
    ResultStatus,
    Task,
    TaskStatus,
    WorkerResult,
)
from careeros.llm.adapters.mock import MockAdapter
from careeros.llm.base import ModelAdapter
from careeros.llm.contracts import BudgetExceededError, ModelRequest, OutputSchema
from careeros.llm.gateway import CallContext, CallOutcome, ModelGateway
from careeros.llm.registry import AdapterRegistry
from careeros.llm.tokens import estimate_tokens
from careeros.manager.decisions import AttemptState, Decision, Outcome, decide
from careeros.manager.plans import (
    PlanSpec,
    PlanValidationError,
    PlaybookLibrary,
    StepSpec,
    parse_plan,
    waves,
)
from careeros.routing.complexity import estimate_complexity
from careeros.routing.router import Escalation, RouteDecision, Router
from careeros.routing.stats import RoutingStatsRepository
from careeros.workers.base import OutputValidationError, StepContext, WorkerRegistry
from careeros.workers.generic import default_registry

FaultHook = Callable[[str, Task], None]
RETRY_ACTIONS = ("retry_same", "retry_with_feedback", "escalate_effort", "escalate_tier")
_RUNNABLE = (TaskStatus.PENDING, TaskStatus.READY, TaskStatus.RETRY_SCHEDULED)


class ObjectiveNeedsUserError(CareerOSError):
    pass


@dataclass(frozen=True)
class ObjectiveRequest:
    title: str
    kind: str | None = None
    inputs: dict[str, Any] = field(default_factory=dict)
    priority: Priority | None = None
    budget_usd: float | None = None


@dataclass
class RunSummary:
    objective_id: int
    status: ObjectiveStatus
    attempts_executed: int
    events: list[str]
    stop_reason: str | None = None


class CareerManager:
    def __init__(
        self,
        ctx: AppContext,
        *,
        adapter: ModelAdapter | None = None,
        workers: WorkerRegistry | None = None,
        playbooks: PlaybookLibrary | None = None,
        fault_hook: FaultHook | None = None,
    ) -> None:
        self.ctx = ctx
        self.db = ctx.db
        self.config = ctx.config
        self.repo = ctx.orchestration
        self.workers = workers or default_registry()
        if playbooks is None:
            pdir = (ctx.config.config_dir / "playbooks") if ctx.config.config_dir else None
            playbooks = PlaybookLibrary.load(pdir, self.workers) if pdir else PlaybookLibrary([])
        self.playbooks = playbooks
        registry = AdapterRegistry()
        registry.register(adapter or MockAdapter())
        self.budgets = BudgetManager(ctx.db, ctx.config)
        self.stats = RoutingStatsRepository(ctx.db)
        self.router = Router(ctx.config, self.stats)
        self.gateway = ModelGateway(ctx.config, ctx.db, registry, self.budgets, CacheStore(ctx.db))
        self.fault_hook = fault_hook
        self.events: list[str] = []

    # ================================================================ logging
    def _log(self, msg: str) -> None:
        self.events.append(msg)

    def _decide_record(
        self,
        decision: str,
        *,
        objective_id: int,
        task_id: int | None = None,
        rationale: str,
        inputs: dict[str, Any] | None = None,
    ) -> None:
        self.repo.record_decision(
            decision,
            objective_id=objective_id,
            task_id=task_id,
            rationale=rationale,
            inputs=inputs,
        )

    # ================================================================ submit / plan
    def submit(self, req: ObjectiveRequest) -> int:
        """Understand the objective, plan it and persist the plan. Returns the objective id."""
        playbook, why = self.playbooks.match(req.kind, req.title)
        priority = req.priority or (playbook.priority if playbook else "normal")
        budget = (
            req.budget_usd
            if req.budget_usd is not None
            else (
                playbook.budget_usd
                if playbook and playbook.budget_usd is not None
                else self.config.budgets.default_objective_usd
            )
        )
        obj = self.repo.create_objective(
            Objective(
                kind=playbook.name if playbook else (req.kind or "novel"),
                title=req.title,
                inputs=req.inputs,
                definition_of_done=(
                    playbook.definition_of_done if playbook else ["accepted:all_required"]
                ),
                priority=Priority(priority),
                budget_usd=budget,
            )
        )
        assert obj.id is not None
        self._log(f"Objective {obj.id}: {req.title!r} ({why})")
        if playbook is not None:
            self._install_plan(
                obj, playbook.plan, source=f"playbook:{playbook.name}", rationale=why
            )
        else:
            plan = self._model_assisted_plan(obj, why)
            if plan is None:
                return obj.id
            self._install_plan(obj, plan, source="model", rationale=plan.rationale or why)
        return obj.id

    def _install_plan(
        self,
        obj: Objective,
        spec: PlanSpec,
        *,
        source: str,
        rationale: str,
        skipped: frozenset[str] = frozenset(),
    ) -> Plan:
        assert obj.id is not None
        wave_list = waves(spec)
        plan = self.repo.create_plan(
            Plan(
                objective_id=obj.id,
                version=1,
                source=source,
                rationale=rationale,
                graph={
                    "steps": [s.model_dump(mode="json") for s in spec.steps],
                    "waves": wave_list,
                    "skipped": sorted(skipped),
                },
            )
        )
        assert plan.id is not None
        self._decide_record(
            "plan",
            objective_id=obj.id,
            rationale=rationale,
            inputs={"source": source, "plan_version": plan.version, "waves": wave_list},
        )
        existing = {t.step: t for t in self.repo.list_tasks(obj.id)}
        ids: dict[str, int] = {}
        for s in spec.steps:
            if s.id in existing:  # replans keep completed work
                tid = existing[s.id].id
                assert tid is not None
                ids[s.id] = tid
                continue
            policy = self.config.routing.policy(s.task_type)
            important = s.important if s.important is not None else policy.important
            task = self.repo.add_task(
                Task(
                    objective_id=obj.id,
                    plan_id=plan.id,
                    worker=s.worker,
                    step=s.id,
                    task_type=s.task_type,
                    title=s.title or s.task_type,
                    input={"step": s.model_dump(mode="json"), "objective_inputs": obj.inputs},
                    priority=s.priority or obj.priority,
                    important=important,
                    max_attempts=s.max_attempts or self.config.limits.max_attempts,
                    budget_usd=s.budget_usd
                    if s.budget_usd is not None
                    else self.config.budgets.default_task_usd,
                    idempotency_key=f"objective:{obj.id}:step:{s.id}",
                )
            )
            assert task.id is not None
            ids[s.id] = task.id
            with self.db.transaction() as c:
                c.execute(
                    "UPDATE tasks SET mode=?, optional=? WHERE id=?",
                    (s.mode, int(s.optional), task.id),
                )
        for s in spec.steps:
            for d in s.depends_on:
                self.repo.add_dependency(ids[s.id], ids[d])
        par = [w for w in wave_list if len(w) > 1]
        self._log(
            f"  plan v{plan.version} ({source}): {len(spec.steps)} steps in "
            f"{len(wave_list)} waves; parallelizable groups: {par or 'none'}"
        )
        return plan

    def _model_assisted_plan(self, obj: Objective, why: str) -> PlanSpec | None:
        """Ask a planning-capable model for a plan; validate, retry and escalate if needed."""
        assert obj.id is not None
        self._log(f"  {why}: requesting model-assisted planning")
        catalog = [
            {"worker": "extract_keywords", "mode": "deterministic", "task_type": "job.normalize"},
            {"worker": "model_step", "mode": "model", "task_type": "job.analyze"},
            {"worker": "assemble_package", "mode": "deterministic", "task_type": "job.normalize"},
        ]
        catalog = [w for w in catalog if self.workers.has(w["worker"], w["mode"])]
        task_type = "manager.plan_novel"
        complexity = estimate_complexity(
            self.config.routing.policy(task_type), {"title": obj.title, "ambiguous": True}
        )
        current: tuple[str, Effort] | None = None
        escalation: Escalation | None = None
        feedback = ""
        for attempt in range(1, self.config.limits.max_attempts + 1):
            route = self.router.route(
                task_type,
                obj.priority.value,
                complexity,
                self.budgets.available(obj.id, None),
                current=current,
                escalation=escalation,
            )
            self._decide_record(
                "route",
                objective_id=obj.id,
                rationale=route.reason,
                inputs={"purpose": "planning", **route.to_dict()},
            )
            if route.choice is None:
                break
            choice = route.choice
            request = ModelRequest(
                task_type=task_type,
                prompt=(
                    f"Objective: {obj.title}\nInputs: {to_json(obj.inputs)}\n"
                    f"Available workers: {to_json(catalog)}\n{feedback}"
                ),
                output_schema=OutputSchema(("steps",), {"steps": "list"}, kind="plan"),
                effort=choice.effort,
                seed_salt=f"plan:{obj.id}:{attempt}",
                simulation={
                    "complexity": complexity.value,
                    "available_workers": catalog,
                    "target_output_tokens": self.config.routing.policy(
                        task_type
                    ).expected_output_tokens,
                },
            )
            outcome = self.gateway.call(
                choice.model_id, request, CallContext(obj.id, None, attempt, "planning")
            )
            self._log(
                f"  planning attempt {attempt}: {choice.model_id}/{choice.effort} -> "
                f"{outcome.status} (${outcome.cost_usd:.5f})"
            )
            try:
                if not outcome.ok or outcome.payload is None:
                    raise PlanValidationError(outcome.error or outcome.status)
                out = outcome.payload.get("output", {})
                plan = parse_plan(
                    {"steps": out.get("steps", []), "rationale": out.get("rationale")}, self.workers
                )
                self.gateway.mark_accepted(outcome)
                return plan
            except PlanValidationError as exc:
                feedback = f"Previous plan was invalid: {exc}"
                current, escalation = (choice.model_id, choice.effort), "tier"
                self._decide_record(
                    "escalate_tier", objective_id=obj.id, rationale=f"invalid plan: {exc}"[:500]
                )
        self._request_user(
            obj.id,
            None,
            "clarification",
            {
                "message": "Could not produce a valid plan; please clarify the "
                "objective or choose a playbook."
            },
        )
        return None

    # ================================================================ run / resume
    def run(self, objective_id: int, *, max_attempts: int | None = None) -> RunSummary:
        """Advance an objective until done, blocked on the user, or ``max_attempts`` task
        attempts have run in this invocation. Safe to call repeatedly (resume)."""
        start = len(self.events)
        session = self.repo.start_session()
        self._recover(objective_id, session)
        executed = 0
        stop_reason: str | None = None
        # An exception here (e.g. the process is killed) deliberately leaves the session
        # open; the next invocation recovers its tasks.
        while True:
            obj = self.repo.get_objective(objective_id)
            if obj.status is not ObjectiveStatus.ACTIVE:
                stop_reason = obj.stop_reason
                break
            if self._done(obj):
                self._finish(obj, ObjectiveStatus.DONE, "definition of done met")
                stop_reason = "definition of done met"
                break
            task = self._next_ready(objective_id)
            if task is None:
                stop_reason = self._settle_idle(obj)
                break
            if max_attempts is not None and executed >= max_attempts:
                stop_reason = "attempt limit for this invocation"
                break
            self._execute(obj, task, session)
            executed += 1
        self.repo.end_session(session, stop_reason or "stopped")
        final = self.repo.get_objective(objective_id)
        return RunSummary(objective_id, final.status, executed, self.events[start:], stop_reason)

    def _recover(self, objective_id: int, session: str) -> None:
        """Tasks left running by a terminated session restart at the same attempt number,
        reusing any checkpointed model response so completed work is not repeated."""
        self.budgets.release_all()
        for t in self.repo.list_tasks(objective_id):
            if t.status in (TaskStatus.RUNNING, TaskStatus.REVIEWING) and t.lease_owner != session:
                assert t.id is not None
                with self.db.transaction() as c:
                    c.execute(
                        "UPDATE tasks SET status='ready', attempt=MAX(0, attempt-1),"
                        " lease_owner=NULL, lease_expires_at=NULL, updated_at=? WHERE id=?",
                        (utc_now_iso(), t.id),
                    )
                self._decide_record(
                    "retry_same",
                    objective_id=objective_id,
                    task_id=t.id,
                    rationale="recovered after interrupted session",
                    inputs={"recovered": True, "from_session": t.lease_owner},
                )
                self._log(f"  recovered task {t.id} ({t.step}) interrupted at attempt {t.attempt}")

    def _next_ready(self, objective_id: int) -> Task | None:
        tasks = {t.id: t for t in self.repo.list_tasks(objective_id)}
        plan = self.repo.current_plan(objective_id)
        order = {
            sid: i for i, w in enumerate((plan.graph if plan else {}).get("waves", [])) for sid in w
        }
        ready: list[Task] = []
        for t in tasks.values():
            if t.status not in _RUNNABLE:
                continue
            assert t.id is not None
            deps = [tasks[d] for d, _ in self.repo.dependencies(t.id)]
            if any(
                d.status in (TaskStatus.FAILED, TaskStatus.SKIPPED, TaskStatus.CANCELLED)
                for d in deps
            ):
                self.repo.set_task_status(
                    t.id, TaskStatus.SKIPPED, error="a required dependency did not succeed"
                )
                continue
            if all(d.status is TaskStatus.SUCCEEDED for d in deps):
                ready.append(t)
        ready.sort(key=lambda t: (order.get(t.step or "", 99), t.id or 0))
        return ready[0] if ready else None

    def _settle_idle(self, obj: Objective) -> str:
        assert obj.id is not None
        tasks = self.repo.list_tasks(obj.id)
        if any(t.status in (TaskStatus.AWAITING_HUMAN, TaskStatus.BLOCKED_BUDGET) for t in tasks):
            self.repo.set_objective_status(
                obj.id, ObjectiveStatus.AWAITING_HUMAN, "waiting for the user's decision"
            )
            self._log("Objective is waiting for your decision (see approvals).")
            return "waiting for the user's decision"
        if self._done(obj):
            self._finish(obj, ObjectiveStatus.DONE, "definition of done met")
            return "definition of done met"
        self._finish(obj, ObjectiveStatus.FAILED, "no runnable work and definition of done unmet")
        return "no runnable work and definition of done unmet"

    # ---------------------------------------------------------------- definition of done
    def _done(self, obj: Objective) -> bool:
        assert obj.id is not None
        tasks = self.repo.list_tasks(obj.id)
        by_step = {t.step: t for t in tasks}
        for check in obj.definition_of_done:
            kind, _, arg = check.partition(":")
            if kind != "accepted":
                raise CareerOSError(f"Unknown definition-of-done check {check!r}")
            if arg == "all_required":
                plan = self.repo.current_plan(obj.id)
                pruned = set((plan.graph if plan else {}).get("skipped", []))
                required = [t for t in tasks if not self._optional(t) and t.step not in pruned]
                if not required or any(t.status is not TaskStatus.SUCCEEDED for t in required):
                    return False
            else:
                t = by_step.get(arg)
                if t is None or t.status is not TaskStatus.SUCCEEDED:
                    return False
        return True

    def _optional(self, t: Task) -> bool:
        r = self.db.query_one("SELECT optional FROM tasks WHERE id=?", (t.id,))
        return bool(r and r["optional"])

    def _finish(self, obj: Objective, status: ObjectiveStatus, reason: str) -> None:
        assert obj.id is not None
        for t in self.repo.list_tasks(obj.id):
            if not t.status.is_terminal and t.status is not TaskStatus.AWAITING_HUMAN:
                assert t.id is not None
                self.repo.set_task_status(
                    t.id, TaskStatus.CANCELLED, error="not needed: objective finished"
                )
        self.repo.set_objective_status(obj.id, status, reason)
        self._decide_record("stop", objective_id=obj.id, rationale=reason)
        self._log(f"Objective {obj.id} {status.value}: {reason}")

    # ================================================================ task execution
    def _step(self, task: Task) -> StepSpec:
        return StepSpec.model_validate(task.input["step"])

    def _deps_outputs(self, task: Task) -> dict[str, dict[str, Any]]:
        assert task.id is not None
        out: dict[str, dict[str, Any]] = {}
        for dep_id, _ in self.repo.dependencies(task.id):
            dep = self.repo.get_task(dep_id)
            if dep.accepted_result_id is None:
                continue
            r = self.db.query_one(
                "SELECT output FROM task_results WHERE id=?", (dep.accepted_result_id,)
            )
            out[dep.step or str(dep_id)] = from_json(r["output"], {}) if r else {}
        return out

    def _execute(self, obj: Objective, task: Task, session: str) -> None:
        assert task.id is not None and obj.id is not None
        attempt = task.attempt + 1
        lease = (utc_now() + timedelta(seconds=self.config.limits.lease_seconds)).isoformat()
        with self.db.transaction() as c:
            c.execute(
                "UPDATE tasks SET status='running', attempt=?, lease_owner=?,"
                " lease_expires_at=?, started_at=COALESCE(started_at, ?), updated_at=?"
                " WHERE id=?",
                (attempt, session, lease, utc_now_iso(), utc_now_iso(), task.id),
            )
        task = self.repo.get_task(task.id)
        assert task.id is not None
        spec = self._step(task)
        step = StepContext(
            step_id=spec.id,
            task_type=spec.task_type,
            title=spec.title or spec.task_type,
            instructions=spec.instructions,
            params={**task.input.get("objective_inputs", {}), **spec.params},
            deps=self._deps_outputs(task),
            output_schema=spec.output.to_schema(),
            feedback=self._pending_feedback(task.id, attempt),
        )
        row = self.db.query_one("SELECT mode FROM tasks WHERE id=?", (task.id,))
        if row is not None and row["mode"] == "deterministic":
            self._execute_deterministic(obj, task, spec, step, attempt)
        else:
            self._execute_model(obj, task, spec, step, attempt)

    # ---------------------------------------------------------------- deterministic
    def _execute_deterministic(
        self, obj: Objective, task: Task, spec: StepSpec, step: StepContext, attempt: int
    ) -> None:
        assert task.id is not None
        worker = self.workers.deterministic[spec.worker]
        result = worker.run(step)
        stored = self.repo.record_result(
            task.id, attempt, result, effective_confidence=result.confidence
        )
        if result.status is ResultStatus.SUCCEEDED:
            self._accept(task, stored.id, f"deterministic {spec.worker}: $0")
            if result.output.get("prune_dependents"):
                self._prune(obj, task, str(result.output.get("reason", "")))
        else:
            self.repo.set_task_status(task.id, TaskStatus.FAILED, error="deterministic step failed")
            self._log(f"  [{spec.id}] deterministic step failed")

    # ---------------------------------------------------------------- model
    def _pending_action(self, task_id: int, attempt: int) -> tuple[str, dict[str, Any]] | None:
        """The retry/escalation decided after the previous attempt, if any."""
        rows = self.db.query(
            "SELECT decision, inputs FROM manager_decisions WHERE task_id=? AND decision IN"
            " ('retry_same','retry_with_feedback','escalate_effort','escalate_tier')"
            " ORDER BY id DESC",
            (task_id,),
        )
        for r in rows:
            inputs = from_json(r["inputs"], {})
            if inputs.get("recovered"):
                continue  # audit marker only
            if inputs.get("attempt") == attempt - 1:
                return str(r["decision"]), inputs
            return None
        return None

    def _pending_feedback(self, task_id: int, attempt: int) -> str | None:
        pending = self._pending_action(task_id, attempt)
        return pending[1].get("feedback") if pending else None

    def _count_decisions(self, task_id: int, decision: str) -> int:
        rows = self.db.query(
            "SELECT inputs FROM manager_decisions WHERE task_id=? AND decision=?",
            (task_id, decision),
        )
        return sum(1 for r in rows if not from_json(r["inputs"], {}).get("recovered"))

    def _route_for(
        self, obj: Objective, task: Task, spec: StepSpec, input_tokens: int
    ) -> RouteDecision | None:
        """Fresh routing, or the route implied by the pending retry/escalation decision."""
        assert task.id is not None and obj.id is not None
        pending = self._pending_action(task.id, task.attempt)
        prior_failures = max(0, task.attempt - 1)
        complexity = estimate_complexity(
            self.config.routing.policy(spec.task_type), {**spec.params}, prior_failures
        )
        budget = self.budgets.available(obj.id, task.id)
        current = (
            (task.current_model_id, cast(Effort, task.current_effort or "none"))
            if task.current_model_id
            else None
        )
        if pending and current and pending[0] in ("retry_same", "retry_with_feedback"):
            route = self.router.route(
                spec.task_type,
                task.priority.value,
                complexity,
                budget,
                current=current,
                input_tokens=input_tokens,
            )
            # keep the same model and effort
            same = next((c for c in route.candidates if (c.model_id, c.effort) == current), None)
            return (
                RouteDecision(
                    "routed",
                    same,
                    route.candidates,
                    route.task_type,
                    route.priority,
                    route.complexity,
                    route.bucket,
                    route.quality_floor,
                    budget,
                    f"{pending[0]}: same model and effort",
                    None,
                )
                if same
                else route
            )
        escalation: Escalation | None = None
        if pending and current and pending[0] == "escalate_effort":
            escalation = "effort"
        elif pending and current and pending[0] == "escalate_tier":
            escalation = "tier"
        route = self.router.route(
            spec.task_type,
            task.priority.value,
            complexity,
            budget,
            current=current,
            escalation=escalation,
            input_tokens=input_tokens,
        )
        if escalation == "effort" and route.status == "no_candidate":
            route = self.router.route(
                spec.task_type,
                task.priority.value,
                complexity,
                budget,
                current=current,
                escalation="tier",
                input_tokens=input_tokens,
            )
        return route

    def _execute_model(
        self, obj: Objective, task: Task, spec: StepSpec, step: StepContext, attempt: int
    ) -> None:
        assert task.id is not None and obj.id is not None
        worker = self.workers.model[spec.worker]
        cp = self._checkpoint(task.id, attempt)
        if cp is not None:
            model_id, effort = cp["model_id"], cp["effort"]
            self._log(
                f"  [{spec.id}] attempt {attempt}: resuming from checkpoint "
                f"(call {cp['call_id']}, no new model call)"
            )
            outcome = CallOutcome(
                cp["call_id"],
                model_id,
                cp["status"],
                cp["payload"],
                None,
                0.0,
                0.0,
                cp["cache_key"],
                cp.get("error"),
            )
            sim_quality = cp.get("sim_quality")
            route_tier = cp.get("tier")
        else:
            base = worker.build_request(step)
            in_tokens = estimate_tokens((base.system or "") + base.prompt)
            route = self._route_for(obj, task, spec, in_tokens)
            assert route is not None
            self._decide_record(
                "route",
                objective_id=obj.id,
                task_id=task.id,
                rationale=route.reason,
                inputs=route.to_dict(),
            )
            if route.choice is None:
                kind = "budget_increase" if route.status == "needs_approval" else "retry_limit"
                self._block(obj, task, kind, {"reason": route.reason, **route.extra})
                return
            if route.status == "degraded":
                self._log(f"  [{spec.id}] DEGRADED routing recorded: {route.reason}")
            choice = route.choice
            model_id, effort = choice.model_id, choice.effort
            route_tier = choice.tier
            with self.db.transaction() as c:
                c.execute(
                    "UPDATE tasks SET current_model_id=?, current_tier=?, current_effort=?"
                    " WHERE id=?",
                    (model_id, choice.tier, effort, task.id),
                )
            policy = self.config.routing.policy(spec.task_type)
            request = ModelRequest(
                task_type=base.task_type,
                prompt=base.prompt,
                system=base.system,
                output_schema=base.output_schema,
                effort=effort,
                seed_salt=f"task:{task.id}:attempt:{attempt}",
                simulation={
                    "complexity": route.complexity,
                    "target_output_tokens": policy.expected_output_tokens,
                    **(
                        {"true_complexity": spec.params["true_complexity"]}
                        if "true_complexity" in spec.params
                        else {}
                    ),
                },
            )
            try:
                outcome = self.gateway.call(
                    model_id, request, CallContext(obj.id, task.id, attempt, "task")
                )
            except BudgetExceededError as exc:
                self._block(
                    obj,
                    task,
                    "budget_increase",
                    {"reason": str(exc), "needed_usd": exc.needed_usd, "scope": exc.scope},
                )
                return
            sim_quality = (
                outcome.response.simulation.get("sim_quality") if outcome.response else None
            )
            self.repo.save_checkpoint(
                Checkpoint(
                    task_id=task.id,
                    attempt=attempt,
                    step="model_response",
                    state={
                        "call_id": outcome.call_id,
                        "model_id": model_id,
                        "effort": effort,
                        "tier": choice.tier,
                        "status": outcome.status,
                        "payload": outcome.payload,
                        "cache_key": outcome.cache_key,
                        "error": outcome.error,
                        "sim_quality": sim_quality,
                    },
                )
            )
            self._log(
                f"  [{spec.id}] attempt {attempt}: {model_id} (tier {choice.tier}, effort "
                f"{effort}) p={choice.p_accept:.2f} est=${choice.est_cost_usd:.5f} -> "
                f"{outcome.status} ${outcome.cost_usd:.5f}"
            )
        if self.fault_hook:
            self.fault_hook("after_model_response", task)

        # ---- validate ------------------------------------------------------------
        result: WorkerResult | None = None
        problem: Outcome | None = None
        detail = outcome.error or ""
        if outcome.status in ("timeout", "rate_limit", "malformed", "error"):
            problem = cast(Outcome, outcome.status)
        else:
            try:
                assert outcome.payload is not None
                result = worker.parse(step, outcome.payload)
            except OutputValidationError as exc:
                problem, detail = "invalid", str(exc)
        if result is None:
            result = WorkerResult(
                status=ResultStatus.FAILED,
                confidence=0.0,
                metrics={"error_kind": problem, "error": detail[:300]},
            )
        result = result.model_copy(
            update={
                "metrics": {
                    **result.metrics,
                    "call_id": outcome.call_id,
                    "model_id": model_id,
                    "effort": effort,
                    "sim_quality": sim_quality,
                }
            }
        )

        # ---- review -------------------------------------------------------------
        score: float | None = None
        if problem is None:
            if result.status is ResultStatus.NEEDS_INPUT:
                problem = "needs_input"
            elif result.confidence < self.config.routing.confidence_threshold:
                problem, detail = "low_confidence", f"confidence {result.confidence:.2f}"
        stored = self.repo.record_result(
            task.id, attempt, result, effective_confidence=round(result.confidence, 4)
        )
        review_enabled = spec.review if spec.review is not None else task.important
        if outcome.cache_hit and review_enabled:
            # Only accepted (already reviewed) results are cached; do not pay to re-review.
            review_enabled = False
            self._log(f"  [{spec.id}] cache hit on a previously reviewed result: $0, no review")
        if problem is None and review_enabled:
            verdict, score, issues = self._review(
                obj, task, spec, result, stored.id, model_id, attempt, sim_quality
            )
            if verdict == "unavailable":
                return  # blocked on approval; task already updated
            if verdict != "accept":
                problem = {"revise": "review_revise", "escalate": "review_escalate"}.get(
                    verdict, "review_reject"
                )  # type: ignore[assignment]
                detail = "; ".join(issues) or f"review score {score}"
        eff_conf = min(result.confidence, score) if score is not None else result.confidence
        with self.db.transaction() as c:
            c.execute(
                "UPDATE task_results SET effective_confidence=? WHERE id=?",
                (round(eff_conf, 4), stored.id),
            )

        # ---- learn + decide -------------------------------------------------------
        if problem not in ("timeout", "rate_limit"):
            self._update_stats(
                task,
                spec,
                model_id,
                effort,
                problem is None,
                score if score is not None else eff_conf,
                outcome,
            )
        if problem is None:
            self.gateway.mark_accepted(outcome)
            self._accept(
                task,
                stored.id,
                f"{model_id}/{effort} accepted"
                + (f" (review {score:.2f})" if score is not None else ""),
            )
            if result.output.get("prune_dependents"):
                self._prune(obj, task, str(result.output.get("reason", "")))
            return
        profile = self.gateway.profile(model_id)
        state = AttemptState(
            attempt=attempt,
            max_attempts=task.max_attempts,
            escalations=task.escalations,
            max_escalations=self.config.limits.max_escalations,
            transient_retries=self._count_decisions(task.id, "retry_same"),
            max_transient_retries=self.config.limits.max_retries_transient,
            repair_retries=self._count_decisions(task.id, "retry_with_feedback"),
            can_raise_effort=EFFORT_ORDER.index(effort) < EFFORT_ORDER.index(profile.max_effort()),
        )
        self._apply(obj, task, decide(problem, state), problem, detail, route_tier)

    def _checkpoint(self, task_id: int, attempt: int) -> dict[str, Any] | None:
        r = self.db.query_one(
            "SELECT state FROM task_checkpoints WHERE task_id=? AND attempt=? AND step=?",
            (task_id, attempt, "model_response"),
        )
        return from_json(r["state"], None) if r else None

    # ---------------------------------------------------------------- review
    def _review(
        self,
        obj: Objective,
        task: Task,
        spec: StepSpec,
        result: WorkerResult,
        result_id: int,
        author_model: str,
        attempt: int,
        sim_quality: float | None,
    ) -> tuple[str, float | None, list[str]]:
        """Independent review by a different model where possible (architecture §7.4)."""
        assert task.id is not None and obj.id is not None
        task_type = "quality.review"
        complexity = estimate_complexity(self.config.routing.policy(task_type), {})
        route = self.router.route(
            task_type,
            task.priority.value,
            complexity,
            self.budgets.available(obj.id, task.id),
            exclude_models=frozenset({author_model}),
        )
        self._decide_record(
            "route",
            objective_id=obj.id,
            task_id=task.id,
            rationale=f"review: {route.reason}",
            inputs={"purpose": "review", **route.to_dict()},
        )
        if route.choice is None:
            self._block(
                obj, task, "budget_increase", {"reason": f"review: {route.reason}", **route.extra}
            )
            return "unavailable", None, []
        choice = route.choice
        request = ModelRequest(
            task_type=task_type,
            prompt=(
                f"Review this deliverable for '{spec.title}'. Criteria: specific, "
                f"supported by evidence, matches instructions.\nInstructions: "
                f"{spec.instructions}\nDeliverable: {json.dumps(result.output)}"
            ),
            output_schema=OutputSchema(("verdict", "score"), {"score": "number"}, kind="review"),
            effort=choice.effort,
            seed_salt=f"review:{task.id}:{attempt}",
            simulation={
                "subject_sim_quality": sim_quality if sim_quality is not None else 0.5,
                "target_output_tokens": self.config.routing.policy(
                    task_type
                ).expected_output_tokens,
                "accept_score": self.config.routing.review_accept_score,
            },
        )
        try:
            outcome = self.gateway.call(
                choice.model_id, request, CallContext(obj.id, task.id, attempt, "review")
            )
        except BudgetExceededError as exc:
            self._block(
                obj,
                task,
                "budget_increase",
                {"reason": f"review: {exc}", "needed_usd": exc.needed_usd},
            )
            return "unavailable", None, []
        if not outcome.ok or outcome.payload is None:
            verdict, score, issues = "escalate", None, [f"review call {outcome.status}"]
        else:
            out = outcome.payload.get("output", {})
            verdict = str(out.get("verdict", "escalate"))
            score = float(out.get("score", 0.0))
            issues = [str(i) for i in out.get("issues", [])]
            if verdict == "accept" and score < self.config.routing.review_accept_score:
                verdict = "revise"
        with self.db.transaction() as c:
            c.execute(
                "INSERT INTO reviews (task_id, result_id, reviewer, verdict, score, issues,"
                " model_call_id, created_at) VALUES (?,?,?,?,?,?,?,?)",
                (
                    task.id,
                    result_id,
                    f"model:{choice.model_id}",
                    verdict
                    if verdict in ("accept", "revise", "escalate", "reject")
                    else "escalate",
                    score,
                    to_json(issues),
                    outcome.call_id,
                    utc_now_iso(),
                ),
            )
        self._log(
            f"  [{spec.id}] review by {choice.model_id}/{choice.effort}: {verdict}"
            + (f" score={score:.2f}" if score is not None else "")
            + f" (${outcome.cost_usd:.5f})"
        )
        return verdict, score, issues

    # ---------------------------------------------------------------- outcomes
    def _update_stats(
        self,
        task: Task,
        spec: StepSpec,
        model_id: str,
        effort: str,
        accepted: bool,
        quality: float | None,
        outcome: CallOutcome,
    ) -> None:
        profile = self.gateway.profile(model_id)
        cx = estimate_complexity(self.config.routing.policy(spec.task_type), {**spec.params})
        _, _, alpha, beta = self.router.posterior_p(
            spec.task_type, cx.bucket, profile, cast(Effort, effort), cx.value
        )
        latency = outcome.response.latency_ms if outcome.response else 0
        self.stats.record(
            spec.task_type,
            cx.bucket,
            model_id,
            effort,
            accepted=accepted,
            prior_alpha=alpha,
            prior_beta=beta,
            quality=quality,
            cost_usd=outcome.cost_usd,
            latency_ms=latency,
        )

    def _accept(self, task: Task, result_id: int, why: str) -> None:
        assert task.id is not None
        with self.db.transaction() as c:
            c.execute(
                "UPDATE tasks SET accepted_result_id=?, lease_owner=NULL,"
                " lease_expires_at=NULL WHERE id=?",
                (result_id, task.id),
            )
        self.repo.set_task_status(task.id, TaskStatus.SUCCEEDED)
        self._decide_record(
            "accept", objective_id=task.objective_id, task_id=task.id, rationale=why
        )
        self._log(f"  [{task.step}] ACCEPTED: {why}")

    def _apply(
        self, obj: Objective, task: Task, d: Decision, problem: str, detail: str, tier: int | None
    ) -> None:
        assert task.id is not None and obj.id is not None
        inputs = {"outcome": problem, "detail": detail[:300], "attempt": task.attempt, "tier": tier}
        if d.action == "retry_with_feedback" or d.action.startswith("escalate"):
            inputs["feedback"] = f"{problem}: {detail}"[:400]
        if d.action == "await_human":
            self._decide_record(
                "await_human",
                objective_id=obj.id,
                task_id=task.id,
                rationale=d.reason,
                inputs=inputs,
            )
            self._block(
                obj,
                task,
                "retry_limit",
                {"reason": d.reason, "last_outcome": problem},
                record=False,
            )
            return
        self._decide_record(
            d.action, objective_id=obj.id, task_id=task.id, rationale=d.reason, inputs=inputs
        )
        with self.db.transaction() as c:
            c.execute(
                "UPDATE tasks SET status='retry_scheduled', lease_owner=NULL,"
                " lease_expires_at=NULL, last_error=?,"
                " escalations = escalations + ? , updated_at=? WHERE id=?",
                (
                    f"{problem}: {detail}"[:300],
                    int(d.action.startswith("escalate")),
                    utc_now_iso(),
                    task.id,
                ),
            )
        self._log(f"  [{task.step}] {problem} -> {d.action.upper()} ({d.reason})")

    def _block(
        self, obj: Objective, task: Task, kind: str, details: dict[str, Any], record: bool = True
    ) -> None:
        assert task.id is not None and obj.id is not None
        if record:
            self._decide_record(
                "await_human",
                objective_id=obj.id,
                task_id=task.id,
                rationale=str(details.get("reason", kind)),
                inputs=details,
            )
        status = (
            TaskStatus.BLOCKED_BUDGET if kind == "budget_increase" else TaskStatus.AWAITING_HUMAN
        )
        with self.db.transaction() as c:
            # This attempt never produced an answer; give it back.
            c.execute(
                "UPDATE tasks SET attempt = MAX(0, attempt - ?), lease_owner=NULL,"
                " lease_expires_at=NULL WHERE id=?",
                (1 if kind == "budget_increase" else 0, task.id),
            )
        self.repo.set_task_status(task.id, status, error=str(details.get("reason", kind))[:300])
        rid = self._request_user(obj.id, task.id, kind, details)
        self._log(
            f"  [{task.step}] WAITING FOR YOU ({kind}, request {rid}): {details.get('reason', '')}"
        )

    def _request_user(
        self, objective_id: int, task_id: int | None, kind: str, details: dict[str, Any]
    ) -> int:
        with self.db.transaction() as c:
            cur = c.execute(
                "INSERT INTO approval_requests (objective_id, task_id, kind, details, created_at)"
                " VALUES (?,?,?,?,?)",
                (objective_id, task_id, kind, to_json(details), utc_now_iso()),
            )
        if task_id is None:
            self.repo.set_objective_status(
                objective_id, ObjectiveStatus.AWAITING_HUMAN, str(details.get("message", kind))
            )
        assert cur.lastrowid is not None
        return cur.lastrowid

    def _prune(self, obj: Objective, task: Task, reason: str) -> None:
        """Replan: skip every step downstream of ``task``; keep all other work."""
        assert obj.id is not None and task.id is not None
        tasks = self.repo.list_tasks(obj.id)
        children: dict[int, list[int]] = {}
        for t in tasks:
            assert t.id is not None
            for dep, _ in self.repo.dependencies(t.id):
                children.setdefault(dep, []).append(t.id)
        stack, pruned = list(children.get(task.id, [])), set()
        while stack:
            tid = stack.pop()
            if tid not in pruned:
                pruned.add(tid)
                stack.extend(children.get(tid, []))
        for tid in pruned:
            self.repo.set_task_status(tid, TaskStatus.SKIPPED, error=f"pruned: {reason}")
        plan = self.repo.current_plan(obj.id)
        steps = {t.id: t.step for t in tasks}
        if plan is not None:
            spec = PlanSpec.model_validate({"steps": plan.graph["steps"]})
            skipped = frozenset(str(steps[t]) for t in pruned)
            new = self.repo.create_plan(
                Plan(
                    objective_id=obj.id,
                    version=plan.version + 1,
                    source=plan.source,
                    rationale=f"replan after {task.step}: {reason}",
                    graph={**plan.graph, "waves": waves(spec), "skipped": sorted(skipped)},
                )
            )
            self._decide_record(
                "replan",
                objective_id=obj.id,
                task_id=task.id,
                rationale=f"{task.step} reported: {reason}",
                inputs={"pruned_steps": sorted(skipped), "plan_version": new.version},
            )
            self._log(f"  REPLAN v{new.version}: skipped {sorted(skipped)} ({reason})")

    # ================================================================ user decisions
    def pending_requests(self, objective_id: int | None = None) -> list[dict[str, Any]]:
        sql = "SELECT * FROM approval_requests WHERE status='pending'"
        params: tuple[Any, ...] = ()
        if objective_id is not None:
            sql += " AND objective_id=?"
            params = (objective_id,)
        return [
            {
                "id": r["id"],
                "objective_id": r["objective_id"],
                "task_id": r["task_id"],
                "kind": r["kind"],
                "details": from_json(r["details"], {}),
            }
            for r in self.db.query(sql + " ORDER BY id", params)
        ]

    def approve(self, request_id: int, *, extra_usd: float = 0.0, extra_attempts: int = 1) -> None:
        """Record the user's approval and make the blocked task runnable again."""
        r = self.db.query_one("SELECT * FROM approval_requests WHERE id=?", (request_id,))
        if r is None:
            raise NotFoundError(f"Approval request {request_id} not found")
        if r["status"] != "pending":
            raise CareerOSError(f"Approval request {request_id} is already {r['status']}")
        kind, obj_id, task_id = r["kind"], r["objective_id"], r["task_id"]
        if kind == "budget_increase" and extra_usd <= 0:
            raise CareerOSError("A budget approval needs a positive simulated amount.")
        now = utc_now_iso()
        with self.db.transaction() as c:
            c.execute(
                "UPDATE approval_requests SET status='approved', resolved_at=? WHERE id=?",
                (now, request_id),
            )
            c.execute(
                "INSERT INTO approvals (kind, scope, details, max_usd, granted_at)"
                " VALUES (?,?,?,?,?)",
                (
                    "budget_increase" if kind == "budget_increase" else "other",
                    f"objective:{obj_id}/task:{task_id}",
                    to_json({"request_id": request_id, "simulated": True, "kind": kind}),
                    extra_usd or None,
                    now,
                ),
            )
            if kind == "budget_increase":
                c.execute(
                    "UPDATE objectives SET budget_usd = COALESCE(budget_usd, 0) + ? WHERE id=?",
                    (extra_usd, obj_id),
                )
                if task_id is not None:
                    c.execute(
                        "UPDATE tasks SET budget_usd = COALESCE(budget_usd, 0) + ? WHERE id=?",
                        (extra_usd, task_id),
                    )
            elif kind == "retry_limit" and task_id is not None:
                c.execute(
                    "UPDATE tasks SET max_attempts = max_attempts + ? WHERE id=?",
                    (extra_attempts, task_id),
                )
        if kind == "budget_increase":
            self.budgets.raise_limit("objective", str(obj_id), extra_usd)
            if task_id is not None:
                self.budgets.raise_limit("task", str(task_id), extra_usd)
        if task_id is not None:
            self.repo.set_task_status(task_id, TaskStatus.READY)
        self.repo.set_objective_status(obj_id, ObjectiveStatus.ACTIVE, None)

    def deny(self, request_id: int) -> None:
        r = self.db.query_one("SELECT * FROM approval_requests WHERE id=?", (request_id,))
        if r is None:
            raise NotFoundError(f"Approval request {request_id} not found")
        with self.db.transaction() as c:
            c.execute(
                "UPDATE approval_requests SET status='denied', resolved_at=? WHERE id=?",
                (utc_now_iso(), request_id),
            )
        if r["task_id"] is not None:
            self.repo.set_task_status(r["task_id"], TaskStatus.FAILED, error="denied by user")
        self.repo.set_objective_status(
            r["objective_id"], ObjectiveStatus.STOPPED, "user declined the request"
        )
