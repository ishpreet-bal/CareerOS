"""The Career Manager.

A coordinator invoked on demand (``submit``, ``run``, ``approve``). It keeps no state in
memory between calls: plans, tasks, routing decisions, model calls, results, reviews,
retries, escalations and checkpoints are all persisted, so any later invocation can resume.

Routine orchestration is deterministic: understanding uses playbooks, decisions use a
fixed table, and models are called only by model steps, independent reviews and
model-assisted planning for unfamiliar objectives.

Execution (M3): ``run`` is a dispatcher. It claims ready tasks atomically and runs up to
``execution.max_concurrency`` of them at once on a bounded thread pool; each worker thread
uses its own database connection. Dependent tasks wait until their prerequisites succeed.
Workers exist only for the duration of one ``run`` call; nothing runs permanently.
"""

from __future__ import annotations

import json
import sqlite3
import threading
import time
from collections.abc import Callable, Iterator
from concurrent.futures import FIRST_COMPLETED, Future, ThreadPoolExecutor, wait
from contextlib import contextmanager
from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta
from typing import Any, ClassVar, cast

from careeros.cache.store import CacheStore
from careeros.context import AppContext, open_context_at
from careeros.core.clock import utc_now, utc_now_iso
from careeros.core.config import EFFORT_ORDER, DatasetKind, Effort
from careeros.core.db import Database, from_json, to_json
from careeros.core.errors import (
    CareerOSError,
    NotFoundError,
    OwnershipLostError,
    SessionCancelledError,
    SessionHaltedError,
    SessionTerminatedError,
)
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
from careeros.orchestration.fencing import CLAIMABLE_SQL, Fence, check_fence, claim
from careeros.routing.complexity import estimate_complexity
from careeros.routing.router import Candidate, Escalation, RouteDecision, Router
from careeros.routing.stats import RoutingStatsRepository
from careeros.workers.base import OutputValidationError, StepContext, WorkerRegistry
from careeros.workers.generic import default_registry

FaultHook = Callable[[str, Task], None]
Clock = Callable[[], datetime]
RETRY_ACTIONS = ("retry_same", "retry_with_feedback", "escalate_effort", "escalate_tier")
_RUNNABLE = (TaskStatus.PENDING, TaskStatus.READY, TaskStatus.RETRY_SCHEDULED)


def _iso(dt: datetime) -> str:
    return dt.isoformat(timespec="seconds")


def call_key(task_id: int, attempt: int, purpose: str, seq: int = 0) -> str:
    """Idempotency key of one *logical* model call: task, attempt, purpose, sequence."""
    return f"task:{task_id}:a{attempt}:{purpose}:{seq}"


@dataclass
class LeaseState:
    """Heartbeat bookkeeping shared between the heartbeat thread and its session."""

    last_success: datetime
    failures: int = 0
    last_error: str | None = None


# (step spec, model id, effort, accepted, quality, call outcome) for routing history
StatsArgs = tuple[StepSpec, str, str, bool, float | None, CallOutcome]


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
    #: Genuine execution attempts started in this invocation: distinct (task, attempt)
    #: pairs claimed. Re-claiming an attempt already counted (after a contention requeue,
    #: say) is a dispatch, not a new attempt (audit L5).
    attempts_executed: int
    events: list[str]
    stop_reason: str | None = None
    #: Every successful claim, including re-claims of an attempt already counted.
    dispatches: int = 0


class CareerManager:
    def __init__(
        self,
        ctx: AppContext,
        *,
        adapter: ModelAdapter | None = None,
        workers: WorkerRegistry | None = None,
        playbooks: PlaybookLibrary | None = None,
        fault_hook: FaultHook | None = None,
        clock: Clock = utc_now,
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
        self.adapter: ModelAdapter = adapter or MockAdapter()
        registry = AdapterRegistry()
        registry.register(self.adapter)
        self.budgets = BudgetManager(ctx.db, ctx.config)
        self.stats = RoutingStatsRepository(ctx.db)
        self.router = Router(ctx.config, self.stats)
        self.gateway = ModelGateway(
            ctx.config, ctx.db, registry, self.budgets, CacheStore(ctx.db), clock=clock
        )
        self.fault_hook = fault_hook
        self.clock = clock
        self.events: list[str] = []
        self._events_lock = threading.Lock()
        #: Set when the session is terminating abruptly; workers stop at the next check.
        self._halt = threading.Event()
        #: Set by the heartbeat when leases cannot be renewed safely; workers halt.
        self._lease_lost = threading.Event()
        #: Set by ``request_cancel`` (controlled shutdown, e.g. Ctrl-C); never cleared by
        #: ``run``, so a request that arrives between runs is honoured by the next one.
        self._cancel = threading.Event()
        self._cancel_reason: list[str] = []
        self._session: str | None = None

    def request_cancel(self, reason: str = "shutdown requested") -> bool:
        """Ask the running (or next) ``run`` to stop safely. Idempotent; returns True only
        for the first request. Safe to call from a signal handler or another thread: it
        only sets an event (no locks besides the event's own, no logging, no I/O), so a
        signal can never interrupt a transaction half-way."""
        first = not self._cancel.is_set()
        if first:
            self._cancel_reason.append(reason)
        self._cancel.set()
        return first

    def _cancel_text(self) -> str:
        return self._cancel_reason[0] if self._cancel_reason else "shutdown requested"

    # ================================================================ logging / halting
    def _log(self, msg: str) -> None:
        with self._events_lock:
            self.events.append(msg)

    def _guard(self) -> None:
        """Workers call this between steps; once the session is halting they stop
        immediately, leaving state exactly as an abrupt process exit would."""
        if self._halt.is_set():
            raise SessionTerminatedError("execution session halted")
        if self._lease_lost.is_set():
            raise SessionHaltedError("leases could not be renewed; worker stopped safely")
        if self._cancel.is_set():
            raise SessionCancelledError(self._cancel_text())

    def _clone(self) -> CareerManager:
        """A manager bound to a new database connection, for one worker thread."""
        ctx = open_context_at(self.config, self.ctx.data_dir, thread_bound=False)
        m = CareerManager(
            ctx,
            adapter=self.adapter,
            workers=self.workers,
            playbooks=self.playbooks,
            fault_hook=self.fault_hook,
            clock=self.clock,
        )
        m.events, m._events_lock, m._halt = self.events, self._events_lock, self._halt
        m._lease_lost = self._lease_lost
        m._cancel, m._cancel_reason = self._cancel, self._cancel_reason
        return m

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
        draft = Objective(
            kind=playbook.name if playbook else (req.kind or "novel"),
            title=req.title,
            inputs=req.inputs,
            definition_of_done=(
                playbook.definition_of_done if playbook else ["accepted:all_required"]
            ),
            priority=Priority(priority),
            budget_usd=budget,
        )
        # The objective and its plan (playbook) or planning phase are created in one
        # transaction: an objective never exists without either (finding N5).
        with self.db.transaction() as c:
            obj = draft.model_copy(update={"id": self.repo.create_objective_in(c, draft)})
            if playbook is not None:
                plan = self._install_plan_in(
                    c, obj, playbook.plan, source=f"playbook:{playbook.name}", rationale=why
                )
            else:
                # Planning is an owned, recoverable phase (audit L9): a planning task under a
                # planning-phase plan version, executed like any task (claim, lease,
                # heartbeat, fenced writes, idempotent calls, recovery, graceful shutdown).
                self._start_planning_in(c, obj, why)
        assert obj.id is not None
        self._log(f"Objective {obj.id}: {req.title!r} ({why})")
        if playbook is not None:
            self._log_plan(plan, playbook.plan, f"playbook:{playbook.name}")
        else:
            self._log(f"  {why}: model-assisted planning")
            self.run(obj.id, planning_only=True)
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
        """Install a plan atomically (finding N5): see ``_install_plan_in``."""
        with self.db.transaction() as c:
            plan = self._install_plan_in(
                c, obj, spec, source=source, rationale=rationale, skipped=skipped
            )
        self._log_plan(plan, spec, source)
        return plan

    def _log_plan(self, plan: Plan, spec: PlanSpec, source: str) -> None:
        par = [w for w in plan.graph.get("waves", []) if len(w) > 1]
        self._log(
            f"  plan v{plan.version} ({source}): {len(spec.steps)} steps in "
            f"{len(plan.graph.get('waves', []))} waves; parallelizable groups: {par or 'none'}"
        )

    def _install_plan_in(
        self,
        c: sqlite3.Connection,
        obj: Objective,
        spec: PlanSpec,
        *,
        source: str,
        rationale: str,
        skipped: frozenset[str] = frozenset(),
        generation: int | None = None,
    ) -> Plan:
        """Inside the caller's transaction: the new plan version (superseding the current
        one), its tasks, their modes and every dependency row, and the ``plan`` decision -
        all or nothing, so a partially installed plan can never exist (finding N5).

        Tasks of the objective whose step the new plan keeps are carried over to the new
        version (completed work is preserved); unfinished tasks whose step it drops are
        cancelled, so no task of an outdated version stays runnable."""
        assert obj.id is not None
        wave_list = waves(spec)
        plan = self.repo.create_plan_in(
            c,
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
            ),
        )
        assert plan.id is not None
        self.repo.record_decision_in(
            c,
            "plan",
            objective_id=obj.id,
            rationale=rationale,
            inputs={"source": source, "plan_version": plan.version, "waves": wave_list},
            generation=generation,
        )
        self._materialize_in(c, obj, plan.id, spec.steps)
        keep = [s.id for s in spec.steps]
        now = self._now()
        c.execute(
            "UPDATE tasks SET status='cancelled', last_error=?, lease_owner=NULL,"
            " lease_expires_at=NULL, updated_at=?, finished_at=? WHERE objective_id=?"
            " AND kind='step' AND step NOT IN (SELECT value FROM json_each(?))"
            " AND status NOT IN ('succeeded','failed','cancelled','skipped')",
            ("not in the current plan", now, now, obj.id, to_json(keep)),
        )
        return plan

    def _materialize_in(
        self, c: sqlite3.Connection, obj: Objective, plan_id: int, steps: list[StepSpec]
    ) -> int:
        """Inside the caller's transaction: make ``plan_id`` own a task for every step and
        every dependency row its graph lists. Existing tasks (same step) are carried over;
        missing ones are created. Idempotent. Returns how many rows it had to create."""
        assert obj.id is not None
        created = 0
        ids: dict[str, int] = {}
        for s_ in steps:
            row = c.execute(
                "SELECT id FROM tasks WHERE objective_id=? AND step=? AND kind='step'",
                (obj.id, s_.id),
            ).fetchone()
            if row is not None:
                ids[s_.id] = int(row["id"])
                c.execute("UPDATE tasks SET plan_id=? WHERE id=?", (plan_id, row["id"]))
                continue
            policy = self.config.routing.policy(s_.task_type)
            important = s_.important if s_.important is not None else policy.important
            tid = self.repo.add_task_in(
                c,
                Task(
                    objective_id=obj.id,
                    plan_id=plan_id,
                    worker=s_.worker,
                    step=s_.id,
                    task_type=s_.task_type,
                    title=s_.title or s_.task_type,
                    input={"step": s_.model_dump(mode="json"), "objective_inputs": obj.inputs},
                    priority=s_.priority or obj.priority,
                    important=important,
                    max_attempts=s_.max_attempts or self.config.limits.max_attempts,
                    budget_usd=s_.budget_usd
                    if s_.budget_usd is not None
                    else self.config.budgets.default_task_usd,
                    idempotency_key=f"objective:{obj.id}:step:{s_.id}",
                ),
            )
            c.execute(
                "UPDATE tasks SET mode=?, optional=? WHERE id=?",
                (s_.mode, int(s_.optional), tid),
            )
            ids[s_.id] = tid
            created += 1
        for s_ in steps:
            for d in s_.depends_on:
                cur = c.execute(
                    "INSERT OR IGNORE INTO task_dependencies (task_id, depends_on_task_id,"
                    " kind) VALUES (?,?,'hard')",
                    (ids[s_.id], ids[d]),
                )
                created += cur.rowcount
        return created

    # ---------------------------------------------------------------- planning phase
    PLANNING_STEP: ClassVar[str] = "__plan__"

    def _start_planning_in(self, c: sqlite3.Connection, obj: Objective, why: str) -> int:
        """Inside the caller's transaction: a planning-phase plan version (current while
        planning runs) and its single planning task. Installing the real plan supersedes
        the planning-phase version, after which the planning task can never be claimed
        again. Returns the planning task id."""
        assert obj.id is not None
        plan = self.repo.create_plan_in(
            c,
            Plan(
                objective_id=obj.id,
                version=1,
                source="planning",
                rationale=why,
                graph={
                    "phase": "planning",
                    "steps": [],
                    "waves": [[self.PLANNING_STEP]],
                    "skipped": [],
                },
            ),
        )
        assert plan.id is not None
        budget = obj.budget_usd if obj.budget_usd is not None else None
        return self.repo.add_task_in(
            c,
            Task(
                objective_id=obj.id,
                plan_id=plan.id,
                worker="planner",
                step=self.PLANNING_STEP,
                task_type="manager.plan_novel",
                title="Plan the objective",
                input={"why": why},
                priority=obj.priority,
                max_attempts=self.config.limits.max_attempts,
                budget_usd=budget,
                idempotency_key=f"objective:{obj.id}:planning:v{plan.version}",
                kind="planning",
            ),
        )

    def _ensure_plan(self, objective_id: int) -> None:
        """Before dispatching: an open objective without any plan (e.g. left by a crash
        inside the old, unowned planning, or after a legacy clarification) gets a planning
        phase; a current plan whose installation was left incomplete by older code is
        completed - missing tasks and dependency rows - in one transaction (finding N5)."""
        obj = self.repo.get_objective(objective_id)
        if obj.status not in (ObjectiveStatus.ACTIVE, ObjectiveStatus.AWAITING_HUMAN):
            return
        plan = self.repo.current_plan(objective_id)
        if plan is None:
            if obj.status is not ObjectiveStatus.ACTIVE:
                return  # waiting for the user; planning starts once they answer
            try:
                with self.db.transaction() as c:
                    if (
                        c.execute(
                            "SELECT 1 FROM plans WHERE objective_id=? AND superseded_at IS NULL",
                            (objective_id,),
                        ).fetchone()
                        is None
                    ):
                        self._start_planning_in(c, obj, "objective has no plan")
                        self._log("  no plan found: starting model-assisted planning")
            except sqlite3.IntegrityError:
                pass  # another session started it first (one current plan per objective)
            return
        if plan.graph.get("phase") == "planning":
            return
        steps = [StepSpec.model_validate(s_) for s_ in plan.graph.get("steps", [])]
        if not steps:
            return
        with self.db.transaction() as c:
            current = c.execute(
                "SELECT id FROM plans WHERE objective_id=? AND superseded_at IS NULL",
                (objective_id,),
            ).fetchone()
            if current is None or int(current["id"]) != plan.id:
                return  # replaced meanwhile
            assert plan.id is not None
            created = self._materialize_in(c, obj, plan.id, steps)
            if created:
                self.repo.record_decision_in(
                    c,
                    "plan",
                    objective_id=objective_id,
                    rationale="completed an incomplete plan installation",
                    inputs={"plan_version": plan.version, "rows_created": created},
                )
        if created:
            self._log(f"  plan v{plan.version}: completed {created} missing row(s)")

    @staticmethod
    def _degrade_inputs(route: RouteDecision, purpose: str) -> dict[str, Any]:
        assert route.choice is not None
        return {
            "purpose": purpose,
            "status": route.status,
            "priority": route.priority,
            "quality_floor": route.quality_floor,
            "chosen_p": route.choice.p_accept,
            "model_id": route.choice.model_id,
            "effort": route.choice.effort,
            "user_approved": route.status == "approved_downgrade",
            **route.extra,
        }

    def _degrade_in(
        self,
        c: sqlite3.Connection,
        fence: Fence | None,
        objective_id: int,
        task_id: int | None,
        route: RouteDecision,
        purpose: str,
    ) -> None:
        """Every downgraded routing choice, whatever initiated it, is a ``degrade`` decision
        (audit M6), written in the same transaction as the route decision."""
        if route.choice is None or route.status not in ("degraded", "approved_downgrade"):
            return
        self.repo.record_decision_in(
            c,
            "degrade",
            objective_id=objective_id,
            task_id=task_id,
            rationale=route.reason,
            inputs=self._degrade_inputs(route, purpose),
            generation=fence.generation if fence else None,
        )

    def _log_degrade(self, route: RouteDecision, purpose: str) -> None:
        if route.choice is None or route.status not in ("degraded", "approved_downgrade"):
            return
        label = (
            "USER-APPROVED DOWNGRADE"
            if route.status == "approved_downgrade"
            else "DOWNGRADE (within limits)"
        )
        self._log(f"  [{purpose}] {label} recorded: {route.reason}")

    def _clarifications(self, objective_id: int) -> list[str]:
        """The user's answers to this objective's clarification requests, in order (N3)."""
        rows = self.db.query(
            "SELECT answer FROM approval_requests WHERE objective_id=? AND kind='clarification'"
            " AND status='approved' AND answer IS NOT NULL ORDER BY id",
            (objective_id,),
        )
        return [str(r["answer"]) for r in rows]

    def _execute_planning(self, obj: Objective, task: Task, fence: Fence) -> str:
        """One attempt of the planning task (audit L9), under its fence like any task.

        Each try within the attempt has a stable logical call key
        (``task:<id>:a<attempt>:planning:<n>``) and a planned-route checkpoint, so a
        resumed attempt replays completed planning calls instead of repeating (and paying
        for) them, and validates the replay against the same model and request. Every
        write is fenced; the plan is installed atomically in the same transaction that
        completes the planning task, so a stale planner installs nothing. If no valid plan
        results, the user is asked for clarification; their answer is used when planning
        resumes (N3)."""
        assert obj.id is not None and task.id is not None
        why = str(task.input.get("why", "model-assisted planning"))
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
        answers = self._clarifications(obj.id)
        current: tuple[str, Effort] | None = None
        escalation: Escalation | None = None
        feedback = ""
        reason = "Could not produce a valid plan"
        for seq in range(self.config.limits.max_attempts):
            self._guard()
            step = f"plan_route:{seq}"
            planned = self._checkpoint(task.id, fence.attempt, step)
            if planned is not None:
                choice = Candidate(**planned["choice"])
            else:
                route = self.router.route(
                    task_type,
                    obj.priority.value,
                    complexity,
                    self.budgets.available(obj.id, task.id, fence.session),
                    current=current,
                    escalation=escalation,
                )
                with self._owned(fence) as c:
                    self.repo.record_decision_in(
                        c,
                        "route",
                        objective_id=obj.id,
                        task_id=task.id,
                        rationale=route.reason,
                        inputs={"purpose": "planning", **route.to_dict()},
                        generation=fence.generation,
                    )
                    if route.choice is not None:
                        self._degrade_in(c, fence, obj.id, task.id, route, "planning")
                        self.repo.save_checkpoint_in(
                            c,
                            Checkpoint(
                                task_id=task.id,
                                attempt=fence.attempt,
                                step=step,
                                state={"choice": asdict(route.choice)},
                            ),
                            fence.generation,
                        )
                self._log_degrade(route, "planning")
                if route.choice is None:
                    reason = f"No model can plan this objective: {route.reason}"
                    break
                choice = route.choice
            answered = (
                "Clarifications from the user:\n" + "\n".join(f"- {a}" for a in answers) + "\n"
                if answers
                else ""
            )
            salt = (
                f"plan:{obj.id}:{seq + 1}"
                if fence.attempt == 1
                else f"plan:{obj.id}:a{fence.attempt}:{seq + 1}"
            )
            request = ModelRequest(
                task_type=task_type,
                prompt=(
                    f"Objective: {obj.title}\nInputs: {to_json(obj.inputs)}\n"
                    f"Available workers: {to_json(catalog)}\n{answered}{feedback}"
                ),
                output_schema=OutputSchema(("steps",), {"steps": "list"}, kind="plan"),
                effort=choice.effort,
                seed_salt=salt,
                simulation={
                    "complexity": complexity.value,
                    "available_workers": catalog,
                    "target_output_tokens": self.config.routing.policy(
                        task_type
                    ).expected_output_tokens,
                },
            )
            self._guard()
            try:
                outcome = self.gateway.call(
                    choice.model_id,
                    request,
                    CallContext(
                        obj.id,
                        task.id,
                        fence.attempt,
                        "planning",
                        fence.session,
                        call_key(task.id, fence.attempt, "planning", seq),
                        fence,
                    ),
                )
            except BudgetExceededError as exc:
                if exc.is_contention:
                    self._requeue(fence, task, f"planning budget contention ({exc.scope})")
                    return "contention"
                self._block(
                    fence,
                    obj,
                    task,
                    "budget_increase",
                    {"reason": f"planning: {exc}", "needed_usd": exc.needed_usd},
                )
                return "done"
            if outcome.status == "late":
                raise OwnershipLostError(f"task {task.id} planning: {outcome.error}")
            self._log(
                f"  planning try {seq + 1}: {choice.model_id}/{choice.effort} -> "
                f"{outcome.status} (${outcome.cost_usd:.5f})"
            )
            try:
                if not outcome.ok or outcome.payload is None:
                    raise PlanValidationError(outcome.error or outcome.status)
                out = outcome.payload.get("output", {})
                plan_spec = parse_plan(
                    {"steps": out.get("steps", []), "rationale": out.get("rationale")},
                    self.workers,
                )
            except PlanValidationError as exc:
                feedback = f"Previous plan was invalid: {exc}"
                current, escalation = (choice.model_id, choice.effort), "tier"
                with self._owned(fence) as c:
                    self.repo.record_decision_in(
                        c,
                        "escalate_tier",
                        objective_id=obj.id,
                        task_id=task.id,
                        rationale=f"invalid plan: {exc}"[:500],
                        inputs={"try": seq, "attempt": fence.attempt},
                        generation=fence.generation,
                    )
                continue
            self._guard()
            with self._owned(fence) as c:  # ownership proven, then install atomically
                plan = self._install_plan_in(
                    c,
                    obj,
                    plan_spec,
                    source="model",
                    rationale=plan_spec.rationale or why,
                    generation=fence.generation,
                )
                c.execute(
                    "UPDATE tasks SET status='succeeded', attempt_open=0, lease_owner=NULL,"
                    " lease_expires_at=NULL, updated_at=?, finished_at=? WHERE id=?",
                    (self._now(), self._now(), task.id),
                )
                self.repo.record_decision_in(
                    c,
                    "accept",
                    objective_id=obj.id,
                    task_id=task.id,
                    rationale=f"plan v{plan.version} installed",
                    inputs={"attempt": fence.attempt, "plan_version": plan.version},
                    generation=fence.generation,
                )
                self.gateway.mark_accepted_in(c, outcome)
            self._log_plan(plan, plan_spec, "model")
            return "done"
        # No valid plan in this attempt: ask the user, atomically with closing the attempt.
        with self._owned(fence) as c:
            c.execute(
                "UPDATE tasks SET status='awaiting_human', attempt_open=0, lease_owner=NULL,"
                " lease_expires_at=NULL, last_error=?, updated_at=? WHERE id=?",
                (reason[:300], self._now(), task.id),
            )
            self.repo.record_decision_in(
                c,
                "await_human",
                objective_id=obj.id,
                task_id=task.id,
                rationale=reason,
                inputs={"attempt": fence.attempt, "kind": "clarification"},
                generation=fence.generation,
            )
            rid = self.repo.request_user_in(
                c,
                obj.id,
                task.id,
                "clarification",
                {
                    "message": f"{reason}; please clarify the objective (answer with "
                    "`approve <id> --answer ...`) or choose a playbook."
                },
                **self._request_times(),
            )
            c.execute(
                "UPDATE objectives SET status='awaiting_human', stop_reason=?, updated_at=?"
                " WHERE id=? AND status IN ('active','awaiting_human')",
                ("planning needs clarification", self._now(), obj.id),
            )
        self._log(f"  [planning] WAITING FOR YOU (clarification, request {rid}): {reason}")
        return "done"

    # ================================================================ run / resume
    def run(
        self,
        objective_id: int,
        *,
        max_attempts: int | None = None,
        force_recover: bool = False,
        planning_only: bool = False,
    ) -> RunSummary:
        """Advance an objective until done, blocked on the user, or ``max_attempts`` task
        attempts have started in this invocation. Safe to call repeatedly (resume).

        ``max_attempts`` limits genuine execution attempts - distinct (task, attempt)
        pairs - not dispatcher activity: a task requeued for budget contention and claimed
        again resumes the same attempt and is not counted twice (audit L5). A resumed
        attempt (after recovery) counts once in the invocation that resumes it.

        Independent ready tasks run concurrently, at most ``execution.max_concurrency`` at a
        time. ``force_recover`` treats every other session as dead (use only when you know
        no other CareerOS process is running, e.g. after a container restart).
        ``planning_only`` runs only the objective's planning task (used by ``submit``).
        """
        start = len(self.events)
        session = self.repo.start_session()
        self._session = session
        self._halt.clear()
        self._lease_lost.clear()
        self._recover(objective_id, session, force=force_recover)
        self._expire_requests(objective_id)
        self._ensure_plan(objective_id)
        limit = self.config.execution.max_concurrency
        hb_stop = threading.Event()
        lease = LeaseState(last_success=self.clock())
        heartbeat = threading.Thread(
            target=self._heartbeat,
            args=(session, hb_stop, cast(DatasetKind, self.db.dataset_kind), lease),
            daemon=True,
            name=f"careeros-heartbeat-{session}",
        )
        heartbeat.start()
        clones: list[CareerManager] = []
        clones_lock = threading.Lock()
        local = threading.local()

        def work(fence: Fence) -> str:
            m = getattr(local, "manager", None)
            if m is None:
                m = self._clone()
                local.manager = m
                with clones_lock:
                    clones.append(m)
            try:
                return m._execute_claimed(objective_id, fence)
            except SessionCancelledError as exc:
                # A safe point: hand the task back with an ownership-checked transition.
                try:
                    exc.released = m._release_for_shutdown(fence, grace_expired=False)
                except sqlite3.Error:
                    exc.released = None  # the dispatcher retries from its own connection
                raise

        running: dict[Future[str], Fence] = {}
        deferred: set[int] = set()  # waiting for a concurrent reservation to settle
        counted: set[tuple[int, int]] = set()  # (task, attempt) started in this run
        dispatches = 0
        idle_contention = 0
        stop_reason: str | None = None
        crash: SessionTerminatedError | None = None
        halted = False
        grace = self.config.limits.shutdown_grace_seconds
        cancel_deadline: float | None = None  # monotonic; set when a shutdown is requested
        unreleased: list[Fence] = []  # stopped workers whose task could not be handed back
        abandoned_workers = 0  # still inside provider calls when the grace period expired
        pool = ThreadPoolExecutor(max_workers=limit, thread_name_prefix="careeros-worker")
        try:
            while True:
                if self._lease_lost.is_set() and not halted:
                    # Ownership cannot be renewed safely: stop dispatching, halt workers.
                    halted = True
                    self._log("HEARTBEAT FAILURE: leases cannot be renewed; halting workers.")
                if self._cancel.is_set() and cancel_deadline is None:
                    # Controlled shutdown: dispatch nothing more; workers stop at safe points.
                    cancel_deadline = time.monotonic() + grace
                    self._log(
                        f"SHUTDOWN REQUESTED ({self._cancel_text()}): no new work; "
                        f"{len(running)} worker(s) stopping at a safe point "
                        f"(grace {grace:g} s)."
                    )
                cancelling = cancel_deadline is not None
                obj = self.repo.get_objective(objective_id)
                active = (
                    obj.status is ObjectiveStatus.ACTIVE
                    and crash is None
                    and not halted
                    and not cancelling
                )
                done_now = active and self._done(obj)
                quota_left = max_attempts is None or len(counted) < max_attempts
                if active and not done_now:
                    for task in self._ready_tasks(objective_id):
                        if len(running) >= limit:
                            break
                        if planning_only and task.kind != "planning":
                            continue
                        assert task.id is not None
                        if task.id in deferred:
                            # Requeued for budget contention: wait until another task
                            # settles or the bounded back-off below elapses, instead of
                            # re-claiming it at once in a hot loop (finding N6).
                            continue
                        allowed, only_attempt = self._quota_for(task, counted, max_attempts)
                        if not allowed:
                            continue
                        fence = self._claim(task, session, only_attempt=only_attempt)
                        if fence is not None:
                            dispatches += 1
                            counted.add((fence.task_id, fence.attempt))
                            deferred.discard(task.id)
                            running[pool.submit(work, fence)] = fence
                quota_left = max_attempts is None or len(counted) < max_attempts
                if not running:
                    if crash is not None:
                        break
                    if halted:
                        stop_reason = "halted: lease renewal failed"
                        break
                    if cancelling:
                        stop_reason = f"cancelled: {self._cancel_text()}"
                        break
                    if planning_only and not deferred:
                        plan = self.repo.current_plan(objective_id)
                        if plan is not None and plan.graph.get("phase") != "planning":
                            stop_reason = "plan installed"
                        else:
                            stop_reason = self._settle_planning(objective_id)
                        break
                    if not active:
                        stop_reason = obj.stop_reason
                        break
                    if done_now:
                        stop_reason = self._finish(
                            obj, ObjectiveStatus.DONE, "definition of done met"
                        )
                        break
                    # Deferred tasks resume an attempt already counted, so they are
                    # waited for (bounded) before the quota can end the run (audit L5).
                    if deferred:  # budget held by another session's reservation
                        idle_contention += 1
                        if idle_contention > 50:
                            stop_reason = "budget is reserved by another active session"
                            break
                        deferred.clear()
                        time.sleep(0.05)
                        continue
                    if not quota_left:
                        stop_reason = "attempt limit for this invocation"
                        break
                    stop_reason = self._settle_idle(obj, session)
                    break
                timeout = 0.5
                if cancel_deadline is not None:
                    timeout = max(0.0, min(timeout, cancel_deadline - time.monotonic()))
                finished, _ = wait(running, return_when=FIRST_COMPLETED, timeout=timeout)
                for fut in finished:
                    fence = running.pop(fut)
                    try:
                        outcome = fut.result()
                    except SessionTerminatedError as exc:
                        crash = crash or exc
                        self._halt.set()  # every other worker stops at its next step
                        continue
                    except SessionHaltedError:
                        continue  # stopped safely; recovery resumes the attempt
                    except SessionCancelledError as exc:
                        if exc.released is None and not self._retry_release(fence):
                            unreleased.append(fence)
                        continue
                    except OwnershipLostError as exc:
                        self._log(f"  task {fence.task_id}: ownership lost, worker stopped ({exc})")
                        continue
                    except Exception as exc:  # a worker failure must not stop the others
                        self._worker_failed(objective_id, fence, exc)
                        outcome = "error"
                    if outcome == "contention":
                        deferred.add(fence.task_id)
                    else:
                        deferred.clear()  # a settled task may have freed budget
                        idle_contention = 0
                if (
                    cancel_deadline is not None
                    and running
                    and crash is None
                    and time.monotonic() >= cancel_deadline
                ):
                    # Grace period over: workers still busy (inside provider calls) lose
                    # their tasks now, through the same conditional transition. Their
                    # calls are abandoned (charged the reservation, as recovery does)
                    # and any reply that still arrives is recorded as late.
                    for fence in list(running.values()):
                        try:
                            self._release_for_shutdown(fence, grace_expired=True)
                        except sqlite3.Error:
                            if not self._retry_release(fence, grace_expired=True):
                                unreleased.append(fence)
                    abandoned_workers = len(running)
                    stop_reason = (
                        f"cancelled: {self._cancel_text()}; grace period expired with "
                        f"{abandoned_workers} worker(s) inside provider calls"
                    )
                    self._log(f"SHUTDOWN: {stop_reason}; their tasks were released.")
                    break
        finally:
            # Workers abandoned at grace expiry may still be blocked in a provider call:
            # do not wait for them, and keep their connections open so their late replies
            # are still recorded (fenced, never authoritative).
            pool.shutdown(wait=abandoned_workers == 0, cancel_futures=True)
            hb_stop.set()
            heartbeat.join()
            if abandoned_workers == 0:
                for m in clones:
                    m.ctx.close()
        if crash is not None:
            # Abrupt end: the session stays open and its leases expire; the next session
            # recovers the work. Nothing is cleaned up, exactly as after a process kill.
            raise crash
        if unreleased:
            # A task could not be handed back (database unavailable): the session is not
            # ended, so its leases simply expire and recovery takes the work over safely.
            stop_reason = (
                f"{stop_reason}; {len(unreleased)} task(s) left for recovery after their "
                "lease expires"
            )
            self._log(f"SHUTDOWN: {stop_reason}")
            final = self.repo.get_objective(objective_id)
            return RunSummary(
                objective_id,
                final.status,
                len(counted),
                self.events[start:],
                stop_reason,
                dispatches,
            )
        # Ending the session (only after no worker holds valid ownership: all stopped, or
        # released at grace expiry) lets recovery take over its remaining tasks at once.
        self.repo.end_session(session, stop_reason or "stopped")
        final = self.repo.get_objective(objective_id)
        return RunSummary(
            objective_id,
            final.status,
            len(counted),
            self.events[start:],
            stop_reason,
            dispatches,
        )

    # ---------------------------------------------------------------- shutdown
    def _release_for_shutdown(self, fence: Fence, *, grace_expired: bool) -> bool:
        """Hand a task back on controlled shutdown, in one transaction, only if ``fence``
        is still its owner (same generation and session, still running). The lease is
        cleared, never extended, so an expired lease is not revived; if another session
        has taken the task over, nothing changes. The attempt stays open, so the task
        resumes under the same attempt number (and idempotency keys).

        Calls of this generation still in flight (only at grace expiry) are abandoned and
        charged their reservation, as recovery does; this generation's other held
        reservations are released. Returns whether this call released the task."""
        reason = (
            "shutdown: grace period expired while the worker was inside a provider call"
            if grace_expired
            else f"shutdown requested ({self._cancel_text()}): stopped at a safe point"
        )
        with self.db.transaction() as c:
            cur = c.execute(
                "UPDATE tasks SET status='ready', lease_owner=NULL, lease_expires_at=NULL,"
                " updated_at=? WHERE id=? AND owner_generation=? AND lease_owner=?"
                " AND status='running'",
                (self._now(), fence.task_id, fence.generation, fence.session),
            )
            if cur.rowcount != 1:
                return False  # finished, or already taken over: not ours to release
            calls = c.execute(
                "SELECT * FROM model_calls WHERE task_id=? AND status='in_flight'"
                " AND owner_generation=?",
                (fence.task_id, fence.generation),
            ).fetchall()
            abandoned = self.gateway.abandon_calls_in(c, calls, reason)
            for r in c.execute(
                "SELECT id FROM budget_reservations WHERE status='held' AND task_id=?"
                " AND session_id=?",
                (fence.task_id, fence.session),
            ).fetchall():
                self.budgets.release_in(c, r["id"])
            row = c.execute(
                "SELECT objective_id FROM tasks WHERE id=?", (fence.task_id,)
            ).fetchone()
            self.repo.record_decision_in(
                c,
                "requeue",
                objective_id=row["objective_id"],
                task_id=fence.task_id,
                rationale=reason,
                inputs={
                    "cancelled": True,
                    "attempt": fence.attempt,
                    "grace_expired": grace_expired,
                    "abandoned_calls": abandoned,
                },
                generation=fence.generation,
            )
        self._log(f"  task {fence.task_id}: released on shutdown (attempt {fence.attempt} kept)")
        return True

    def _retry_release(self, fence: Fence, *, grace_expired: bool = False) -> bool:
        """Bounded retries of a failed shutdown release from the dispatcher's own
        connection. True once released or no longer ours; False if the database stayed
        unavailable (the session is then left open so its leases expire)."""
        for i in range(3):
            try:
                self._release_for_shutdown(fence, grace_expired=grace_expired)
                return True
            except sqlite3.Error:
                time.sleep(0.05 * 2**i)
        return False

    # ---------------------------------------------------------------- ownership
    def _now(self) -> str:
        return _iso(self.clock())

    @contextmanager
    def _owned(self, fence: Fence) -> Iterator[sqlite3.Connection]:
        """A write transaction that first proves ``fence`` still owns its task. Every
        task-owned write goes through here; a stale owner's writes are rolled back."""
        with self.db.transaction() as c:
            check_fence(c, fence, self._now())
            yield c

    def _claim(self, task: Task, session: str, *, only_attempt: int | None = None) -> Fence | None:
        """Atomically take a runnable task and mint its fencing token. Exactly one
        claimant can succeed, across threads and processes. ``only_attempt`` restricts
        the claim to resuming that attempt (see ``fencing.claim``)."""
        assert task.id is not None
        lease = _iso(self.clock() + timedelta(seconds=self.config.limits.lease_seconds))
        with self.db.transaction() as c:
            return claim(c, task.id, session, lease, self._now(), only_attempt=only_attempt)

    @staticmethod
    def _quota_for(
        task: Task, counted: set[tuple[int, int]], max_attempts: int | None
    ) -> tuple[bool, int | None]:
        """(may dispatch, attempt the claim is restricted to) under the per-invocation
        genuine-attempt quota (audit L5).

        With quota left, any claim may proceed: it adds at most one attempt, and this
        invocation's dispatcher claims one task at a time. With no quota left, only an
        attempt this invocation already counted may be resumed - and that restriction is
        enforced inside the claiming statement (``only_attempt``), not by an earlier
        read, so another process closing or advancing the attempt in between cannot make
        the claim start a new one."""
        if max_attempts is None or len(counted) < max_attempts:
            return True, None
        mine = [a for t, a in counted if t == task.id]
        if not mine:
            return False, None
        return True, max(mine)

    def _heartbeat(
        self, session: str, stop: threading.Event, kind: DatasetKind, state: LeaseState
    ) -> None:
        """Renew this session's leases while it is alive (own connection).

        Failures are retried (bounded). If no renewal has succeeded for
        ``halt_after = lease - safety_margin`` seconds, the session's lease is no longer
        safe: ``_lease_lost`` is set, the dispatcher stops claiming and workers halt at their
        next checkpoint - before any lease can expire and be taken over. A database error is
        never treated as proof that someone else owns the work.
        """
        if self.db.path is None:
            return
        interval = self.config.limits.heartbeat_interval()
        db: Database | None = None
        try:
            while not stop.wait(interval):
                if db is None:
                    try:
                        db = Database.open(self.db.path, dataset_kind=kind, migrate=False)
                    except sqlite3.Error:
                        db = None
                renewed = db is not None and self._renew_leases(db, session, state)
                if not renewed and self._lease_unsafe(state):
                    self._lease_lost.set()
                    return
        except BaseException:  # the heartbeat itself died: never fail silently
            self._lease_lost.set()
            raise
        finally:
            if db is not None:
                db.close()

    def _renew_leases(self, db: Database, session: str, state: LeaseState) -> bool:
        """One renewal with bounded retries. Returns True on success."""
        for i in range(self.config.limits.heartbeat_retries + 1):
            try:
                lease = _iso(self.clock() + timedelta(seconds=self.config.limits.lease_seconds))
                now = self._now()
                with db.transaction() as c:
                    # Never resurrect a lease that has already expired.
                    c.execute(
                        "UPDATE tasks SET lease_expires_at=? WHERE lease_owner=?"
                        " AND status='running' AND lease_expires_at > ?",
                        (lease, session, now),
                    )
                    c.execute(
                        "UPDATE execution_sessions SET last_heartbeat_at=? WHERE id=?",
                        (now, session),
                    )
                state.last_success = self.clock()
                state.failures = 0
                return True
            except sqlite3.Error as exc:
                state.failures += 1
                state.last_error = f"{type(exc).__name__}: {exc}"
                if i < self.config.limits.heartbeat_retries:
                    time.sleep(min(1.0, 0.05 * 2**i))
        return False

    def _lease_unsafe(self, state: LeaseState) -> bool:
        """True once ``halt_after`` seconds have passed without a successful renewal."""
        elapsed = (self.clock() - state.last_success).total_seconds()
        return elapsed >= self.config.limits.halt_after()

    def _stale_sessions(self, current: str, *, force: bool) -> list[str]:
        """Sessions that are no longer alive: ended, or whose heartbeat is older than a
        lease (all other sessions when ``force``)."""
        cutoff = _iso(self.clock() - timedelta(seconds=self.config.limits.lease_seconds))
        stale = []
        for r in self.db.query("SELECT * FROM execution_sessions WHERE id <> ?", (current,)):
            beat = r["last_heartbeat_at"] or r["started_at"]
            if force or r["ended_at"] is not None or beat < cutoff:
                stale.append(str(r["id"]))
        return stale

    def _recover(self, objective_id: int, session: str, *, force: bool = False) -> None:
        """Take over work whose owner is gone, never from a live owner.

        A task is taken over only if its lease has expired, its owning session has ended,
        or ``force``. Each takeover is one transaction, conditional on the exact owner and
        generation (so concurrent recoveries cannot both win): the task becomes ready with
        its attempt still open, the generation's in-flight calls are abandoned and charged
        their reservation, its other held reservations are released, and a ``recover``
        decision is written. Late replies to the abandoned calls are reconciled later.
        """
        stale = self._stale_sessions(session, force=force)
        ended = {
            str(r["id"])
            for r in self.db.query("SELECT id FROM execution_sessions WHERE ended_at IS NOT NULL")
        }
        now = self._now()
        abandoned = self.gateway.abandon_in_flight(stale)  # legacy / planning calls
        released = 0
        for sid in stale:
            for r in self.budgets.held_reservations(sid):
                if r["task_id"] is not None:
                    continue  # task reservations are handled with their task below
                with self.db.transaction() as c:
                    self.budgets.release_in(c, r["id"])
                released += 1
        for t in self.repo.list_tasks(objective_id):
            if t.status is not TaskStatus.RUNNING or t.lease_owner == session:
                continue
            expired = t.lease_expires_at is None or t.lease_expires_at <= now
            if not (expired or force or t.lease_owner in ended):
                continue  # owned by a live session: leave it alone
            assert t.id is not None
            row = self.db.query_one("SELECT owner_generation FROM tasks WHERE id=?", (t.id,))
            gen = int(row["owner_generation"]) if row else 0
            with self.db.transaction() as c:
                cur = c.execute(
                    "UPDATE tasks SET status='ready', lease_owner=NULL, lease_expires_at=NULL,"
                    " updated_at=? WHERE id=? AND status='running' AND owner_generation=?"
                    " AND lease_owner IS ?",
                    (now, t.id, gen, t.lease_owner),
                )
                if cur.rowcount != 1:
                    continue  # another recovery won, or the owner finished meanwhile
                calls = c.execute(
                    "SELECT * FROM model_calls WHERE task_id=? AND status='in_flight'"
                    " AND (owner_generation=? OR owner_generation IS NULL)",
                    (t.id, gen),
                ).fetchall()
                abandoned += self.gateway.abandon_calls_in(
                    c, calls, f"ownership of task {t.id} generation {gen} was taken over"
                )
                for r in c.execute(
                    "SELECT id FROM budget_reservations WHERE status='held' AND task_id=?"
                    " AND session_id IS ?",
                    (t.id, t.lease_owner),
                ).fetchall():
                    self.budgets.release_in(c, r["id"])
                    released += 1
                self.repo.record_decision_in(
                    c,
                    "recover",
                    objective_id=objective_id,
                    task_id=t.id,
                    rationale="owner gone: lease expired or session ended; attempt kept open",
                    inputs={
                        "recovered": True,
                        "from_session": t.lease_owner,
                        "attempt": t.attempt,
                        "generation": gen,
                        "forced": force,
                    },
                    generation=gen,
                )
            self._log(f"  recovered task {t.id} ({t.step}) interrupted at attempt {t.attempt}")
        if abandoned or released:
            self._log(
                f"  recovery: {abandoned} in-flight call(s) abandoned, "
                f"{released} reservation(s) released"
            )
        if stale:
            with self.db.transaction() as c:
                for sid in stale:
                    c.execute(
                        "UPDATE execution_sessions SET ended_at=COALESCE(ended_at, ?),"
                        " end_reason=COALESCE(end_reason, 'presumed dead (recovered)')"
                        " WHERE id=?",
                        (utc_now_iso(), sid),
                    )

    def _worker_failed(self, objective_id: int, fence: Fence, exc: Exception) -> None:
        """An unexpected exception in one worker: retry the task (bounded), never crash the
        run or the other workers. Done with the worker's own token, atomically."""
        err = f"{type(exc).__name__}: {exc}"[:300]
        try:
            with self._owned(fence) as c:
                task = self.repo._row_task(
                    c.execute("SELECT * FROM tasks WHERE id=?", (fence.task_id,)).fetchone()
                )
                calls = c.execute(
                    "SELECT * FROM model_calls WHERE task_id=? AND status='in_flight'"
                    " AND owner_generation=?",
                    (fence.task_id, fence.generation),
                ).fetchall()
                self.gateway.abandon_calls_in(c, calls, f"worker failed: {err}")
                for r in c.execute(
                    "SELECT id FROM budget_reservations WHERE status='held' AND task_id=?"
                    " AND session_id=?",
                    (fence.task_id, fence.session),
                ).fetchall():
                    self.budgets.release_in(c, r["id"])
                retry = fence.attempt < task.max_attempts
                status = "retry_scheduled" if retry else "failed"
                c.execute(
                    "UPDATE tasks SET status=?, attempt_open=0, lease_owner=NULL,"
                    " lease_expires_at=NULL, last_error=?, updated_at=?,"
                    " finished_at=CASE WHEN ?='failed' THEN ? ELSE finished_at END WHERE id=?",
                    (status, err, self._now(), status, self._now(), fence.task_id),
                )
                self.repo.record_decision_in(
                    c,
                    "worker_error",
                    objective_id=objective_id,
                    task_id=fence.task_id,
                    rationale=f"worker raised {err}",
                    inputs={"attempt": fence.attempt, "will_retry": retry},
                    generation=fence.generation,
                )
        except OwnershipLostError:
            self._log(f"  task {fence.task_id}: worker error after ownership was lost ({err})")
            return
        self._log(
            f"  [{task.step}] WORKER ERROR ({err}) -> "
            + ("retry" if retry else "failed (attempt limit)")
        )

    def _requeue(self, fence: Fence, task: Task, reason: str) -> None:
        """Release the task without consuming its attempt (it stays open)."""
        assert task.id is not None
        with self._owned(fence) as c:
            c.execute(
                "UPDATE tasks SET status='ready', lease_owner=NULL, lease_expires_at=NULL,"
                " updated_at=? WHERE id=?",
                (self._now(), task.id),
            )
            self.repo.record_decision_in(
                c,
                "requeue",
                objective_id=task.objective_id,
                task_id=task.id,
                rationale=reason,
                inputs={"attempt": fence.attempt},
                generation=fence.generation,
            )
        self._log(f"  [{task.step}] REQUEUED: {reason}")

    # ---------------------------------------------------------------- readiness
    def _ready_tasks(self, objective_id: int) -> list[Task]:
        """Runnable tasks whose prerequisites all succeeded, in wave order. Tasks whose
        prerequisites failed, were skipped or were cancelled are skipped (atomically, with
        their decision)."""
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
                with self.db.transaction() as c:
                    cur = c.execute(
                        "UPDATE tasks SET status='skipped', last_error=?, updated_at=?,"
                        " finished_at=? WHERE id=? AND status IN"
                        " ('pending','ready','retry_scheduled')",
                        ("a required dependency did not succeed", self._now(), self._now(), t.id),
                    )
                    if cur.rowcount == 1:
                        self.repo.record_decision_in(
                            c,
                            "skip",
                            objective_id=objective_id,
                            task_id=t.id,
                            rationale="a prerequisite failed, was skipped or cancelled",
                        )
                continue
            if all(d.status is TaskStatus.SUCCEEDED for d in deps):
                ready.append(t)
        # The claim's own readiness rule decides (plan membership, complete dependency
        # rows, prerequisites, active objective), so dispatcher and claim cannot disagree.
        claimable = {
            int(r["id"])
            for r in self.db.query(
                "SELECT id FROM tasks WHERE objective_id=? AND " + CLAIMABLE_SQL,  # noqa: S608
                (objective_id,),
            )
        }
        ready = [t for t in ready if t.id in claimable]
        ready.sort(key=lambda t: (order.get(t.step or "", 99), t.id or 0))
        return ready

    def _settle_planning(self, objective_id: int) -> str:
        """Why a planning-only run stopped without installing a plan."""
        rows = self.db.query(
            "SELECT status FROM tasks WHERE objective_id=? AND kind='planning'"
            " ORDER BY id DESC LIMIT 1",
            (objective_id,),
        )
        status = rows[0]["status"] if rows else None
        if status in ("awaiting_human", "blocked_budget"):
            self.repo.set_objective_status(
                objective_id,
                ObjectiveStatus.AWAITING_HUMAN,
                "waiting for the user's decision",
                only_from=(ObjectiveStatus.ACTIVE,),
            )
            return "waiting for the user's decision"
        if status in ("running", "reviewing"):
            return "planning is running in another session"
        obj = self.repo.get_objective(objective_id)
        if obj.status is not ObjectiveStatus.ACTIVE:
            return obj.stop_reason or obj.status.value
        return "planning not finished"

    def _settle_idle(self, obj: Objective, session: str) -> str:
        assert obj.id is not None
        tasks = self.repo.list_tasks(obj.id)
        if any(t.status in (TaskStatus.RUNNING, TaskStatus.REVIEWING) for t in tasks):
            self._log("Some tasks are running in another live session; nothing to do here.")
            return "tasks are running in another session"
        if any(t.status in (TaskStatus.AWAITING_HUMAN, TaskStatus.BLOCKED_BUDGET) for t in tasks):
            if not self.repo.set_objective_status(
                obj.id,
                ObjectiveStatus.AWAITING_HUMAN,
                "waiting for the user's decision",
                only_from=(ObjectiveStatus.ACTIVE,),
            ):  # decided meanwhile (e.g. stopped by the user): leave that decision alone
                return self.repo.get_objective(obj.id).stop_reason or "objective changed"
            self._log("Objective is waiting for your decision (see approvals).")
            return "waiting for the user's decision"
        if self._done(obj):
            return self._finish(obj, ObjectiveStatus.DONE, "definition of done met")
        return self._finish(
            obj, ObjectiveStatus.FAILED, "no runnable work and definition of done unmet"
        )

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
                if plan is None or plan.graph.get("phase") == "planning":
                    return False  # nothing to be done yet: planning has not finished
                pruned = set(plan.graph.get("skipped", []))
                have = {t.step for t in tasks if t.status is TaskStatus.SUCCEEDED}
                for step_ in plan.graph.get("steps", []):
                    if (
                        step_["id"] not in pruned
                        and not step_.get("optional", False)
                        and step_["id"] not in have
                    ):
                        return False  # every required step of the plan, not just the rows
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

    def _finish(self, obj: Objective, status: ObjectiveStatus, reason: str) -> str:
        """Objective, tasks and approval requests reach a consistent final state together
        (audit M3): unfinished tasks are cancelled - including ones blocked on budget or
        awaiting the user - and pending requests are withdrawn, in one transaction.

        Conditional on the objective still being open (audit L3): if another decision
        finished it first (``obj`` is a stale snapshot), nothing changes. Returns the
        stop reason actually in effect."""
        assert obj.id is not None
        result = self.repo.finish_objective(obj.id, status, reason)
        if result is None:
            current = self.repo.get_objective(obj.id)
            self._log(
                f"Objective {obj.id} was already {current.status.value} "
                f"({current.stop_reason}); not marked {status.value}"
            )
            return current.stop_reason or current.status.value
        cancelled, withdrawn = result
        extra = []
        if cancelled:
            extra.append(f"{cancelled} unneeded task(s) cancelled")
        if withdrawn:
            extra.append(f"{withdrawn} pending request(s) withdrawn")
        self._log(
            f"Objective {obj.id} {status.value}: {reason}"
            + (f" ({'; '.join(extra)})" if extra else "")
        )
        return reason

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

    def _execute_claimed(self, objective_id: int, fence: Fence) -> str:
        """Run one claimed task attempt (inside a worker thread, own connection).

        Returns ``"done"`` or ``"contention"`` (requeued while a concurrent reservation
        settles). Raises ``OwnershipLostError`` if the token stops being the owner,
        ``SessionHaltedError``/``SessionTerminatedError`` if the session is stopping.
        """
        self._session = fence.session
        self._guard()
        obj = self.repo.get_objective(objective_id)
        task = self.repo.get_task(fence.task_id)
        with self._owned(fence):
            pass  # verify the token before doing any work
        assert task.id is not None and obj.id is not None
        if task.kind == "planning":
            return self._execute_planning(obj, task, fence)
        spec = self._step(task)
        step = StepContext(
            step_id=spec.id,
            task_type=spec.task_type,
            title=spec.title or spec.task_type,
            instructions=spec.instructions,
            params={**task.input.get("objective_inputs", {}), **spec.params},
            deps=self._deps_outputs(task),
            output_schema=spec.output.to_schema(),
            feedback=self._pending_feedback(task.id, fence.attempt),
        )
        row = self.db.query_one("SELECT mode FROM tasks WHERE id=?", (task.id,))
        if row is not None and row["mode"] == "deterministic":
            self._execute_deterministic(obj, task, spec, step, fence)
            return "done"
        return self._execute_model(obj, task, spec, step, fence)

    # ---------------------------------------------------------------- deterministic
    def _execute_deterministic(
        self, obj: Objective, task: Task, spec: StepSpec, step: StepContext, fence: Fence
    ) -> None:
        assert task.id is not None
        worker = self.workers.deterministic[spec.worker]
        result = worker.run(step)
        self._guard()
        with self._owned(fence) as c:
            stored = self.repo.record_result_in(
                c, task.id, fence.attempt, result, effective_confidence=result.confidence
            )
        if result.status is ResultStatus.SUCCEEDED:
            prune = (
                str(result.output.get("reason", ""))
                if result.output.get("prune_dependents")
                else None
            )
            self._accept(
                fence, obj, task, stored.id, f"deterministic {spec.worker}: $0", prune_reason=prune
            )
            return
        with self._owned(fence) as c:
            c.execute(
                "UPDATE tasks SET status='failed', attempt_open=0, lease_owner=NULL,"
                " lease_expires_at=NULL, last_error=?, updated_at=?, finished_at=? WHERE id=?",
                ("deterministic step failed", self._now(), self._now(), task.id),
            )
            self.repo.record_decision_in(
                c,
                "worker_error",
                objective_id=obj.id,
                task_id=task.id,
                rationale="deterministic step failed",
                inputs={
                    "attempt": fence.attempt,
                    "deterministic_failure": True,
                    "will_retry": False,
                },
                generation=fence.generation,
            )
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
        # Committed spend only: concurrent reservations are enforced at reservation time.
        budget = self.budgets.available(obj.id, task.id, self._session, include_reservations=False)
        approved = self._approved_floor(task.id, task.attempt, "task")
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
            approved_floor=approved,
        )
        if escalation == "effort" and route.status not in (
            "routed",
            "degraded",
            "approved_downgrade",
        ):
            route = self.router.route(
                spec.task_type,
                task.priority.value,
                complexity,
                budget,
                current=current,
                escalation="tier",
                input_tokens=input_tokens,
                approved_floor=approved,
            )
        return route

    def _execute_model(
        self, obj: Objective, task: Task, spec: StepSpec, step: StepContext, fence: Fence
    ) -> str:
        assert task.id is not None and obj.id is not None
        attempt = fence.attempt
        worker = self.workers.model[spec.worker]
        cp = self._checkpoint(task.id, attempt, "model_response")
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
            planned = self._checkpoint(task.id, attempt, "route")
            if planned is not None:
                # Resuming an attempt: reuse the route chosen before the interruption so the
                # logical call (and any reply already recorded for it) stays the same.
                choice = Candidate(**planned["choice"])
                complexity_value = float(planned["complexity"])
                self._log(
                    f"  [{spec.id}] attempt {attempt}: resuming planned route "
                    f"{choice.model_id}/{choice.effort}"
                )
            else:
                route = self._route_for(obj, task, spec, in_tokens)
                assert route is not None
                if route.choice is None:
                    kind = "budget_increase" if route.status == "needs_approval" else "retry_limit"
                    self._block(
                        fence,
                        obj,
                        task,
                        kind,
                        {
                            "reason": route.reason,
                            "quality_floor": route.quality_floor,
                            "route_purpose": "task",
                            **route.extra,
                        },
                        route=route,
                    )
                    return "done"
                choice = route.choice
                complexity_value = route.complexity
                with self._owned(fence) as c:
                    self.repo.record_decision_in(
                        c,
                        "route",
                        objective_id=obj.id,
                        task_id=task.id,
                        rationale=route.reason,
                        inputs=route.to_dict(),
                        generation=fence.generation,
                    )
                    self._degrade_in(c, fence, obj.id, task.id, route, f"task:{spec.id}")
                    c.execute(
                        "UPDATE tasks SET current_model_id=?, current_tier=?, current_effort=?"
                        " WHERE id=?",
                        (choice.model_id, choice.tier, choice.effort, task.id),
                    )
                    self.repo.save_checkpoint_in(
                        c,
                        Checkpoint(
                            task_id=task.id,
                            attempt=attempt,
                            step="route",
                            state={"choice": asdict(choice), "complexity": complexity_value},
                        ),
                        fence.generation,
                    )
                self._log_degrade(route, f"task:{spec.id}")
            model_id, effort = choice.model_id, choice.effort
            route_tier = choice.tier
            policy = self.config.routing.policy(spec.task_type)
            request = ModelRequest(
                task_type=base.task_type,
                prompt=base.prompt,
                system=base.system,
                output_schema=base.output_schema,
                effort=effort,
                seed_salt=f"task:{task.id}:attempt:{attempt}",
                simulation={
                    "step": spec.id,
                    "complexity": complexity_value,
                    "target_output_tokens": policy.expected_output_tokens,
                    **(
                        {"true_complexity": spec.params["true_complexity"]}
                        if "true_complexity" in spec.params
                        else {}
                    ),
                },
            )
            self._guard()
            try:
                outcome = self.gateway.call(
                    model_id,
                    request,
                    CallContext(
                        obj.id,
                        task.id,
                        attempt,
                        "task",
                        fence.session,
                        call_key(task.id, attempt, "task"),
                        fence,
                    ),
                )
            except BudgetExceededError as exc:
                if exc.is_contention:
                    self._requeue(
                        fence,
                        task,
                        f"budget contention ({exc.scope}): waiting for "
                        "concurrent reservations to settle",
                    )
                    return "contention"
                self._block(
                    fence,
                    obj,
                    task,
                    "budget_increase",
                    {
                        "reason": str(exc),
                        "needed_usd": exc.needed_usd,
                        "scope": exc.scope,
                        "route_purpose": "task",
                    },
                )
                return "done"
            if outcome.status == "late":
                raise OwnershipLostError(f"task {task.id}: {outcome.error}")
            self._guard()
            if outcome.overshoot_usd > 0:
                self._log(f"  [{spec.id}] BUDGET OVERSHOOT recorded: {outcome.error}")
            sim_quality = (
                outcome.response.simulation.get("sim_quality") if outcome.response else None
            )
            with self._owned(fence) as c:
                self.repo.save_checkpoint_in(
                    c,
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
                    ),
                    fence.generation,
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
        self._guard()
        with self._owned(fence) as c:
            stored = self.repo.record_result_in(
                c, task.id, attempt, result, effective_confidence=round(result.confidence, 4)
            )
        review_enabled = spec.review if spec.review is not None else task.important
        if outcome.cache_hit and review_enabled:
            # Only accepted (already reviewed) results are cached; do not pay to re-review.
            review_enabled = False
            self._log(f"  [{spec.id}] cache hit on a previously reviewed result: $0, no review")
        if problem is None and review_enabled:
            verdict, score, issues = self._review(
                fence, obj, task, spec, result, stored.id, model_id, sim_quality
            )
            if verdict in ("unavailable", "contention"):
                return "contention" if verdict == "contention" else "done"
            if verdict != "accept":
                problem = {"revise": "review_revise", "escalate": "review_escalate"}.get(
                    verdict, "review_reject"
                )  # type: ignore[assignment]
                detail = "; ".join(issues) or f"review score {score}"
        eff_conf = min(result.confidence, score) if score is not None else result.confidence
        stats = None
        if problem not in ("timeout", "rate_limit"):
            stats = (
                spec,
                model_id,
                effort,
                problem is None,
                score if score is not None else eff_conf,
                outcome,
            )

        # ---- decide (atomically with its state change, stats and records) -----------
        self._guard()
        if problem is None:
            prune = (
                str(result.output.get("reason", ""))
                if result.output.get("prune_dependents")
                else None
            )
            self._accept(
                fence,
                obj,
                task,
                stored.id,
                f"{model_id}/{effort} accepted"
                + (f" (review {score:.2f})" if score is not None else ""),
                prune_reason=prune,
                outcome=outcome,
                eff_conf=eff_conf,
                stats=stats,
            )
            return "done"
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
        self._apply(
            fence,
            obj,
            task,
            decide(problem, state),
            problem,
            detail,
            route_tier,
            eff_conf=eff_conf,
            stored_id=stored.id,
            stats=stats,
        )
        return "done"

    def _checkpoint(self, task_id: int, attempt: int, step: str) -> dict[str, Any] | None:
        r = self.db.query_one(
            "SELECT state FROM task_checkpoints WHERE task_id=? AND attempt=? AND step=?",
            (task_id, attempt, step),
        )
        return from_json(r["state"], None) if r else None

    # ---------------------------------------------------------------- review
    def _review(
        self,
        fence: Fence,
        obj: Objective,
        task: Task,
        spec: StepSpec,
        result: WorkerResult,
        result_id: int,
        author_model: str,
        sim_quality: float | None,
    ) -> tuple[str, float | None, list[str]]:
        """Independent review by a different model where possible (architecture §7.4)."""
        assert task.id is not None and obj.id is not None
        attempt = fence.attempt
        task_type = "quality.review"
        planned = self._checkpoint(task.id, attempt, "review_route")
        if planned is not None:
            choice = Candidate(**planned["choice"])
        else:
            complexity = estimate_complexity(self.config.routing.policy(task_type), {})
            review_floor = self._approved_floor(task.id, attempt, "review")
            route = self.router.route(
                task_type,
                task.priority.value,
                complexity,
                self.budgets.available(obj.id, task.id, fence.session, include_reservations=False),
                exclude_models=frozenset({author_model}),
                # Only an approval given for *this review* lowers its floor (audit M6).
                approved_floor=review_floor,
            )
            if route.choice is None:
                self._block(
                    fence,
                    obj,
                    task,
                    "budget_increase",
                    {
                        "reason": f"review: {route.reason}",
                        "quality_floor": route.quality_floor,
                        "route_purpose": "review",
                        **route.extra,
                    },
                    route=route,
                    purpose="review",
                )
                return "unavailable", None, []
            choice = route.choice
            with self._owned(fence) as c:
                self.repo.record_decision_in(
                    c,
                    "route",
                    objective_id=obj.id,
                    task_id=task.id,
                    rationale=f"review: {route.reason}",
                    inputs={"purpose": "review", **route.to_dict()},
                    generation=fence.generation,
                )
                self._degrade_in(c, fence, obj.id, task.id, route, f"review:{spec.id}")
                self.repo.save_checkpoint_in(
                    c,
                    Checkpoint(
                        task_id=task.id,
                        attempt=attempt,
                        step="review_route",
                        state={"choice": asdict(choice)},
                    ),
                    fence.generation,
                )
            self._log_degrade(route, f"review:{spec.id}")
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
                "step": spec.id,
                "subject_sim_quality": sim_quality if sim_quality is not None else 0.5,
                "target_output_tokens": self.config.routing.policy(
                    task_type
                ).expected_output_tokens,
                "accept_score": self.config.routing.review_accept_score,
            },
        )
        self._guard()
        try:
            outcome = self.gateway.call(
                choice.model_id,
                request,
                CallContext(
                    obj.id,
                    task.id,
                    attempt,
                    "review",
                    fence.session,
                    call_key(task.id, attempt, "review"),
                    fence,
                ),
            )
        except BudgetExceededError as exc:
            if exc.is_contention:
                self._requeue(fence, task, f"review budget contention ({exc.scope})")
                return "contention", None, []
            self._block(
                fence,
                obj,
                task,
                "budget_increase",
                {
                    "reason": f"review: {exc}",
                    "needed_usd": exc.needed_usd,
                    "route_purpose": "review",
                },
                purpose="review",
            )
            return "unavailable", None, []
        if outcome.status == "late":
            raise OwnershipLostError(f"task {task.id} review: {outcome.error}")
        if not outcome.ok or outcome.payload is None:
            verdict, score, issues = "escalate", None, [f"review call {outcome.status}"]
        else:
            out = outcome.payload.get("output", {})
            verdict = str(out.get("verdict", "escalate"))
            score = float(out.get("score", 0.0))
            issues = [str(i) for i in out.get("issues", [])]
            if verdict == "accept" and score < self.config.routing.review_accept_score:
                verdict = "revise"
        self._guard()
        with self._owned(fence) as c:
            c.execute(
                "INSERT INTO reviews (task_id, result_id, reviewer, verdict, score, issues,"
                " model_call_id, created_at) VALUES (?,?,?,?,?,?,?,?)"
                " ON CONFLICT(task_id, result_id) DO UPDATE SET reviewer=excluded.reviewer,"
                " verdict=excluded.verdict, score=excluded.score, issues=excluded.issues,"
                " model_call_id=excluded.model_call_id, created_at=excluded.created_at",
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

    # ---------------------------------------------------------------- outcomes (atomic)
    def _stats_in(self, c: sqlite3.Connection, stats: StatsArgs | None) -> None:
        if stats is None:
            return
        spec, model_id, effort, accepted, quality, outcome = stats
        profile = self.gateway.profile(model_id)
        cx = estimate_complexity(self.config.routing.policy(spec.task_type), {**spec.params})
        _, _, alpha, beta = self.router.posterior_p(
            spec.task_type, cx.bucket, profile, cast(Effort, effort), cx.value
        )
        latency = outcome.response.latency_ms if outcome.response else 0
        self.stats.record_in(
            c,
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

    def _accept(
        self,
        fence: Fence,
        obj: Objective,
        task: Task,
        result_id: int,
        why: str,
        *,
        prune_reason: str | None = None,
        outcome: CallOutcome | None = None,
        eff_conf: float | None = None,
        stats: StatsArgs | None = None,
    ) -> None:
        """One transaction: prune dependents (if the result asks), record the effective
        confidence, mark succeeded and close the attempt, write the decision, update routing
        history and cache the result - or none of it."""
        assert task.id is not None and obj.id is not None
        pruned: list[str] = []
        with self._owned(fence) as c:
            if prune_reason is not None:
                pruned = self._prune_in(c, fence, obj, task, prune_reason)
            if eff_conf is not None:
                c.execute(
                    "UPDATE task_results SET effective_confidence=? WHERE id=?",
                    (round(eff_conf, 4), result_id),
                )
            c.execute(
                "UPDATE tasks SET status='succeeded', accepted_result_id=?, attempt_open=0,"
                " lease_owner=NULL, lease_expires_at=NULL, updated_at=?, finished_at=?"
                " WHERE id=?",
                (result_id, self._now(), self._now(), task.id),
            )
            self.repo.record_decision_in(
                c,
                "accept",
                objective_id=obj.id,
                task_id=task.id,
                rationale=why,
                inputs={"attempt": fence.attempt},
                generation=fence.generation,
            )
            self._stats_in(c, stats)
            if outcome is not None:
                self.gateway.mark_accepted_in(c, outcome)
        if pruned:
            self._log(f"  REPLAN: skipped {pruned} ({prune_reason})")
        self._log(f"  [{task.step}] ACCEPTED: {why}")

    def _apply(
        self,
        fence: Fence,
        obj: Objective,
        task: Task,
        d: Decision,
        problem: str,
        detail: str,
        tier: int | None,
        *,
        eff_conf: float | None = None,
        stored_id: int | None = None,
        stats: StatsArgs | None = None,
    ) -> None:
        """One transaction: close the attempt with its outcome - retry/escalate (with the
        escalation count) or ask the user - plus its decision and routing history."""
        assert task.id is not None and obj.id is not None
        inputs: dict[str, Any] = {
            "outcome": problem,
            "detail": detail[:300],
            "attempt": fence.attempt,
            "tier": tier,
        }
        if d.action == "retry_with_feedback" or d.action.startswith("escalate"):
            inputs["feedback"] = f"{problem}: {detail}"[:400]
        with self._owned(fence) as c:
            if eff_conf is not None and stored_id is not None:
                c.execute(
                    "UPDATE task_results SET effective_confidence=? WHERE id=?",
                    (round(eff_conf, 4), stored_id),
                )
            self._stats_in(c, stats)
            if d.action == "await_human":
                c.execute(
                    "UPDATE tasks SET status='awaiting_human', attempt_open=0, lease_owner=NULL,"
                    " lease_expires_at=NULL, last_error=?, updated_at=? WHERE id=?",
                    (d.reason[:300], self._now(), task.id),
                )
                self.repo.record_decision_in(
                    c,
                    "await_human",
                    objective_id=obj.id,
                    task_id=task.id,
                    rationale=d.reason,
                    inputs=inputs,
                    generation=fence.generation,
                )
                rid = self.repo.request_user_in(
                    c,
                    obj.id,
                    task.id,
                    "retry_limit",
                    {"reason": d.reason, "last_outcome": problem},
                    **self._request_times(),
                )
            else:
                c.execute(
                    "UPDATE tasks SET status='retry_scheduled', attempt_open=0, lease_owner=NULL,"
                    " lease_expires_at=NULL, last_error=?, escalations = escalations + ?,"
                    " updated_at=? WHERE id=?",
                    (
                        f"{problem}: {detail}"[:300],
                        int(d.action.startswith("escalate")),
                        self._now(),
                        task.id,
                    ),
                )
                self.repo.record_decision_in(
                    c,
                    d.action,
                    objective_id=obj.id,
                    task_id=task.id,
                    rationale=d.reason,
                    inputs=inputs,
                    generation=fence.generation,
                )
        if d.action == "await_human":
            self._log(f"  [{task.step}] WAITING FOR YOU (retry_limit, request {rid}): {d.reason}")
        else:
            self._log(f"  [{task.step}] {problem} -> {d.action.upper()} ({d.reason})")

    def _block(
        self,
        fence: Fence,
        obj: Objective,
        task: Task,
        kind: str,
        details: dict[str, Any],
        *,
        route: RouteDecision | None = None,
        purpose: str = "task",
    ) -> None:
        """One transaction: stop the task to ask the user. No answer was produced, so the
        attempt stays open (the next claim resumes it). The planned route for this step is
        dropped so that, after the user's decision, routing is redone with the new budget or
        approved floor."""
        assert task.id is not None and obj.id is not None
        status = "blocked_budget" if kind == "budget_increase" else "awaiting_human"
        with self._owned(fence) as c:
            if route is not None:
                self.repo.record_decision_in(
                    c,
                    "route",
                    objective_id=obj.id,
                    task_id=task.id,
                    rationale=(f"{purpose}: " if purpose != "task" else "") + route.reason,
                    inputs={"purpose": purpose, **route.to_dict()},
                    generation=fence.generation,
                )
            c.execute(
                "DELETE FROM task_checkpoints WHERE task_id=? AND attempt=? AND step=?",
                (task.id, fence.attempt, "route" if purpose == "task" else "review_route"),
            )
            c.execute(
                "UPDATE tasks SET status=?, lease_owner=NULL, lease_expires_at=NULL,"
                " last_error=?, updated_at=? WHERE id=?",
                (status, str(details.get("reason", kind))[:300], self._now(), task.id),
            )
            self.repo.record_decision_in(
                c,
                "await_human",
                objective_id=obj.id,
                task_id=task.id,
                rationale=str(details.get("reason", kind)),
                inputs={**details, "attempt": fence.attempt, "attempt_open": True},
                generation=fence.generation,
            )
            rid = self.repo.request_user_in(
                c, obj.id, task.id, kind, details, **self._request_times()
            )
        self._log(
            f"  [{task.step}] WAITING FOR YOU ({kind}, request {rid}): {details.get('reason', '')}"
        )

    def _request_user(
        self, objective_id: int, task_id: int | None, kind: str, details: dict[str, Any]
    ) -> int:
        """Objective-level requests (e.g. planning clarification), not tied to an owner.

        The objective waits for the user only if it is still open (audit L3); a question
        raised after another decision finished it is recorded as withdrawn."""
        with self.db.transaction() as c:
            rid = self.repo.request_user_in(
                c, objective_id, task_id, kind, details, **self._request_times()
            )
            if task_id is None:
                cur = c.execute(
                    "UPDATE objectives SET status='awaiting_human', stop_reason=?, updated_at=?"
                    " WHERE id=? AND status IN ('active','awaiting_human')",
                    (str(details.get("message", kind)), self._now(), objective_id),
                )
                if cur.rowcount != 1:
                    c.execute(
                        "UPDATE approval_requests SET status='withdrawn', resolution=?,"
                        " resolved_at=? WHERE id=?",
                        ("withdrawn: objective already finished", self._now(), rid),
                    )
        return rid

    def _prune_in(
        self, c: sqlite3.Connection, fence: Fence, obj: Objective, task: Task, reason: str
    ) -> list[str]:
        """Replan inside the accepting transaction: skip every step downstream of ``task``
        (union with earlier replans) and record the replan decision."""
        assert obj.id is not None and task.id is not None
        rows = c.execute(
            "SELECT d.task_id, d.depends_on_task_id FROM task_dependencies d"
            " JOIN tasks t ON t.id = d.task_id WHERE t.objective_id=?",
            (obj.id,),
        ).fetchall()
        children: dict[int, list[int]] = {}
        for r in rows:
            children.setdefault(int(r["depends_on_task_id"]), []).append(int(r["task_id"]))
        stack, pruned = list(children.get(task.id, [])), set()
        while stack:
            tid = stack.pop()
            if tid not in pruned:
                pruned.add(tid)
                stack.extend(children.get(tid, []))
        if not pruned:
            return []
        steps = {
            int(r["id"]): str(r["step"])
            for r in c.execute("SELECT id, step FROM tasks WHERE objective_id=?", (obj.id,))
        }
        new, _ = self.repo.skip_steps_in(
            c,
            obj.id,
            {tid: steps[tid] for tid in pruned},
            rationale=f"replan after {task.step}: {reason}",
            reason=reason,
        )
        names = sorted(steps[t] for t in pruned)
        self.repo.record_decision_in(
            c,
            "replan",
            objective_id=obj.id,
            task_id=task.id,
            rationale=f"{task.step} reported: {reason}",
            inputs={
                "pruned_steps": names,
                "all_skipped": new.graph.get("skipped", []),
                "plan_version": new.version,
            },
            generation=fence.generation,
        )
        return names

    # ================================================================ user decisions
    def pending_requests(self, objective_id: int | None = None) -> list[dict[str, Any]]:
        """Pending requests with their expiry. An expired request is listed as expired
        (it can no longer be decided; the next run replaces it if the work still waits)."""
        sql = "SELECT * FROM approval_requests WHERE status='pending'"
        params: tuple[Any, ...] = ()
        if objective_id is not None:
            sql += " AND objective_id=?"
            params = (objective_id,)
        now = self._now()
        out = []
        for r in self.db.query(sql + " ORDER BY id", params):
            expires = self._expiry_of(r)
            out.append(
                {
                    "id": r["id"],
                    "objective_id": r["objective_id"],
                    "task_id": r["task_id"],
                    "kind": r["kind"],
                    "details": from_json(r["details"], {}),
                    "expires_at": expires,
                    "expired": expires <= now,
                }
            )
        return out

    def _approved_floor(self, task_id: int, attempt: int, purpose: str) -> float | None:
        """The quality floor a user approved for this task's routing step, if a grant
        exists for exactly this task, purpose and attempt and has not expired or been
        revoked (audit L12). Otherwise no downgrade applies."""
        r = self.db.query_one(
            "SELECT details FROM approvals WHERE kind='quality_downgrade' AND task_id=?"
            " AND purpose=? AND attempt=? AND revoked_at IS NULL"
            " AND (expires_at IS NULL OR expires_at > ?) ORDER BY id DESC LIMIT 1",
            (task_id, purpose, attempt, self._now()),
        )
        if r is None:
            return None
        return max(0.0, float(from_json(r["details"], {})["floor_to"]) - 1e-9)

    # ---------------------------------------------------------------- expiry (L12)
    def _hours_from_now(self, hours: float) -> str:
        return _iso(self.clock() + timedelta(hours=hours))

    def _request_times(self) -> dict[str, str]:
        """Creation time and explicit expiry for a new request (manager clock)."""
        return {
            "created_at": self._now(),
            "expires_at": self._hours_from_now(self.config.approval_limits().request_ttl_hours),
        }

    def _expiry_of(self, r: Any) -> str:
        if r["expires_at"]:
            return str(r["expires_at"])
        created = datetime.fromisoformat(str(r["created_at"]))
        return _iso(created + timedelta(hours=self.config.approval_limits().request_ttl_hours))

    def _expire_in(self, c: sqlite3.Connection, r: Any, now: str) -> int | None:
        """Inside the caller's transaction: withdraw an expired request (conditionally) and,
        if its objective is still open and its task still waits for exactly this kind of
        decision, raise a fresh request with the same details and a new expiry - so a stale
        request can never be applied, while the waiting work is not stranded. Returns the
        replacement request id, if any."""
        cur = c.execute(
            "UPDATE approval_requests SET status='withdrawn', resolution=?, resolved_at=?"
            " WHERE id=? AND status='pending'",
            (f"expired at {self._expiry_of(r)}", now, r["id"]),
        )
        if cur.rowcount != 1:
            return None
        obj = c.execute("SELECT status FROM objectives WHERE id=?", (r["objective_id"],)).fetchone()
        if obj is None or obj["status"] not in ("active", "awaiting_human"):
            return None
        if r["task_id"] is not None:
            t = c.execute("SELECT status FROM tasks WHERE id=?", (r["task_id"],)).fetchone()
            expected = self._BLOCKED_STATE.get(r["kind"])
            allowed = (expected.value,) if expected is not None else self._WAITING
            if t is None or t["status"] not in allowed:
                return None
        details = {**from_json(r["details"], {}), "replaces_request": r["id"]}
        return self.repo.request_user_in(
            c, r["objective_id"], r["task_id"], r["kind"], details, **self._request_times()
        )

    def _expire_requests(self, objective_id: int) -> None:
        """Before dispatching: withdraw (and, where the work still waits, replace) every
        expired pending request of the objective."""
        now = self._now()
        for r in self.db.query(
            "SELECT * FROM approval_requests WHERE objective_id=? AND status='pending'",
            (objective_id,),
        ):
            if self._expiry_of(r) > now:
                continue
            with self.db.transaction() as c:
                current = c.execute(
                    "SELECT * FROM approval_requests WHERE id=?", (r["id"],)
                ).fetchone()
                new = self._expire_in(c, current, now) if current else None
            self._log(
                f"  request {r['id']} ({r['kind']}) expired"
                + (f"; replaced by request {new}" if new else "")
            )

    _BLOCKED_STATE: ClassVar[dict[str, TaskStatus]] = {
        "budget_increase": TaskStatus.BLOCKED_BUDGET,
        "retry_limit": TaskStatus.AWAITING_HUMAN,
    }
    #: States a task may be in while any other kind of request about it is pending.
    _WAITING: ClassVar[tuple[str, ...]] = ("blocked_budget", "awaiting_human")

    def _validate_request(self, request_id: int, c: sqlite3.Connection) -> tuple[Any, str | None]:
        """Inside the deciding transaction: (request, problem). A request can only be
        decided while it is pending, its objective is unfinished and its task is still in
        the blocked state the request was made for. Because the check and the decision
        share one write transaction, two decisions on one request cannot both pass it
        (finding N1)."""
        r = c.execute("SELECT * FROM approval_requests WHERE id=?", (request_id,)).fetchone()
        if r is None:
            raise NotFoundError(f"Approval request {request_id} not found")
        if r["status"] != "pending":
            raise CareerOSError(f"Approval request {request_id} is already {r['status']}")
        obj = c.execute("SELECT status FROM objectives WHERE id=?", (r["objective_id"],)).fetchone()
        problem = None
        if self._expiry_of(r) <= self._now():  # enforced when deciding, not only when listed
            problem = f"it expired at {self._expiry_of(r)}"
        elif obj["status"] not in ("active", "awaiting_human"):
            problem = f"objective {r['objective_id']} is already {obj['status']}"
        elif r["task_id"] is not None:
            t = c.execute(
                "SELECT status, plan_id, objective_id FROM tasks WHERE id=?", (r["task_id"],)
            ).fetchone()
            expected = self._BLOCKED_STATE.get(r["kind"])
            allowed = (expected.value,) if expected is not None else self._WAITING
            current = c.execute(
                "SELECT id FROM plans WHERE objective_id=? AND superseded_at IS NULL",
                (t["objective_id"],),
            ).fetchone()
            if t["status"] not in allowed:
                problem = f"task {r['task_id']} is {t['status']}, no longer {'/'.join(allowed)}"
            elif current is None or int(current["id"]) != int(t["plan_id"]):
                problem = f"task {r['task_id']} is no longer part of the current plan"
        return r, problem

    def _decide_request(
        self, request_id: int, apply: Callable[[sqlite3.Connection, Any], None]
    ) -> Any:
        """Validate and decide a request in one write transaction. An obsolete request is
        withdrawn (and that withdrawal committed) instead of being applied."""
        replacement: int | None = None
        with self.db.transaction() as c:
            r, problem = self._validate_request(request_id, c)
            if problem is None:
                apply(c, r)
            elif problem.startswith("it expired"):
                replacement = self._expire_in(c, r, self._now())
            else:
                c.execute(
                    "UPDATE approval_requests SET status='withdrawn', resolution=?,"
                    " resolved_at=? WHERE id=? AND status='pending'",
                    (f"withdrawn: {problem}", self._now(), request_id),
                )
        if problem is not None:
            hint = f"; decide the new request {replacement} instead" if replacement else ""
            raise CareerOSError(f"Approval request {request_id} is obsolete: {problem}{hint}.")
        return r

    @staticmethod
    def _resolve_in(
        c: sqlite3.Connection, request_id: int, status: str, resolution: str, now: str
    ) -> None:
        """Conditionally resolve a pending request; anything else aborts the decision."""
        cur = c.execute(
            "UPDATE approval_requests SET status=?, resolution=?, resolved_at=?"
            " WHERE id=? AND status='pending'",
            (status, resolution, now, request_id),
        )
        if cur.rowcount != 1:
            raise CareerOSError(f"Approval request {request_id} was decided concurrently")

    def _release_task_in(
        self,
        c: sqlite3.Connection,
        task_id: int,
        kind: str,
        now: str,
        column: str | None = None,
        value: float | None = None,
    ) -> None:
        """Make a blocked task runnable again, only from the state the request was for."""
        expected = self._BLOCKED_STATE.get(kind)
        allowed = [expected.value] if expected is not None else list(self._WAITING)
        if column is None:
            cur = c.execute(
                "UPDATE tasks SET status='ready', updated_at=? WHERE id=?"
                " AND status IN (SELECT value FROM json_each(?))",
                (now, task_id, to_json(allowed)),
            )
        else:
            cur = c.execute(
                f"UPDATE tasks SET {column}=?, status='ready', updated_at=? WHERE id=?"  # noqa: S608
                " AND status IN (SELECT value FROM json_each(?))",
                (value, now, task_id, to_json(allowed)),
            )
        if cur.rowcount != 1:
            raise CareerOSError(f"Task {task_id} changed state while the request was decided")

    @staticmethod
    def _reopen_in(c: sqlite3.Connection, objective_id: int, now: str) -> None:
        cur = c.execute(
            "UPDATE objectives SET status='active', stop_reason=NULL, updated_at=? WHERE id=?"
            " AND status IN ('active','awaiting_human')",
            (now, objective_id),
        )
        if cur.rowcount != 1:
            raise CareerOSError(f"Objective {objective_id} finished while the request was decided")

    def approve(
        self,
        request_id: int,
        *,
        extra_usd: float = 0.0,
        extra_attempts: int = 1,
        accept_downgrade: bool = False,
        answer: str | None = None,
    ) -> None:
        """Record the user's decision and make the blocked task runnable again.

        For a budget request the user either adds (simulated) budget, or with
        ``accept_downgrade`` explicitly accepts the best affordable lower-quality option the
        request offered. The downgrade applies only to the routing step that asked for it
        (the task itself, or its review). Validation and every write are one transaction,
        each write conditional on the state it expects (audit L3, finding N1).

        A planning ``clarification`` needs the user's ``answer``: it is stored with the
        request and planning resumes with it in a new attempt (finding N3).
        """

        limits = self.config.approval_limits()

        def scope_attempt(c: sqlite3.Connection, task_id: int | None) -> int | None:
            if task_id is None:
                return None
            row = c.execute("SELECT attempt FROM tasks WHERE id=?", (task_id,)).fetchone()
            return int(row["attempt"]) if row else None

        def record_grant(
            c: sqlite3.Connection,
            r: Any,
            kind: str,
            now: str,
            *,
            details: dict[str, Any],
            purpose: str | None = None,
            extra: float | None = None,
            attempts: int | None = None,
            expires: str | None = None,
        ) -> None:
            """The grant, scoped to its objective, task, request and attempt. At most one
            grant per request exists (unique index): a request is never consumed twice."""
            try:
                c.execute(
                    "INSERT INTO approvals (kind, scope, details, max_usd, granted_at,"
                    " expires_at, objective_id, task_id, request_id, attempt, purpose,"
                    " extra_usd, extra_attempts) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
                    (
                        kind,
                        f"objective:{r['objective_id']}/task:{r['task_id']}"
                        + (f"/{purpose}" if purpose else ""),
                        to_json({"request_id": request_id, "simulated": True, **details}),
                        extra,
                        now,
                        expires,
                        r["objective_id"],
                        r["task_id"],
                        request_id,
                        scope_attempt(c, r["task_id"]),
                        purpose,
                        extra,
                        attempts,
                    ),
                )
            except sqlite3.IntegrityError as exc:
                raise CareerOSError(f"Approval request {request_id} was already granted") from exc

        def downgrade(c: sqlite3.Connection, r: Any) -> None:
            kind, obj_id, task_id = r["kind"], r["objective_id"], r["task_id"]
            details = from_json(r["details"], {})
            option = details.get("downgrade_option")
            purpose = details.get("route_purpose", "task")
            if kind != "budget_increase" or not option or task_id is None:
                raise CareerOSError("This request does not offer a quality downgrade.")
            if purpose not in ("task", "review"):
                raise CareerOSError(f"Downgrades cannot be approved for {purpose} routing.")
            column = "approved_floor" if purpose == "task" else "approved_review_floor"
            floor_to = float(option["p_accept"])
            now = self._now()
            self._resolve_in(c, request_id, "approved", f"accept_downgrade:{purpose}", now)
            # Scoped to this task, this routing purpose and the attempt it was asked for
            # (the blocked attempt stays open), and expiring: routing uses it only then.
            record_grant(
                c,
                r,
                "quality_downgrade",
                now,
                details={
                    "route_purpose": purpose,
                    "floor_from": details.get("quality_floor"),
                    "floor_to": floor_to,
                    "option": option,
                },
                purpose=purpose,
                expires=self._hours_from_now(limits.downgrade_ttl_hours),
            )
            self._release_task_in(c, task_id, kind, now, column, max(0.0, floor_to - 1e-9))
            self._reopen_in(c, obj_id, now)
            c.execute(
                "INSERT INTO manager_decisions (objective_id, task_id, decision, rationale,"
                " inputs, created_at) VALUES (?,?,?,?,?,?)",
                (
                    obj_id,
                    task_id,
                    "approve_downgrade",
                    f"user accepted a {purpose} quality downgrade: floor "
                    f"{details.get('quality_floor')} -> {floor_to:.3f}",
                    to_json({"request_id": request_id, "route_purpose": purpose, "option": option}),
                    now,
                ),
            )

        def grant(c: sqlite3.Connection, r: Any) -> None:
            kind, obj_id, task_id = r["kind"], r["objective_id"], r["task_id"]
            now = self._now()
            if kind == "budget_increase":
                if extra_usd <= 0:
                    raise CareerOSError("A budget approval needs a positive simulated amount.")
                if extra_usd > limits.max_budget_increase_usd + 1e-12:
                    raise CareerOSError(
                        f"${extra_usd:.2f} exceeds the per-approval limit of "
                        f"${limits.max_budget_increase_usd:.2f} (approvals.max_budget_increase_usd)"
                    )
                granted = float(
                    c.execute(
                        "SELECT COALESCE(SUM(extra_usd), 0) FROM approvals"
                        " WHERE kind='budget_increase' AND objective_id=? AND revoked_at IS NULL",
                        (obj_id,),
                    ).fetchone()[0]
                )
                if granted + extra_usd > limits.max_total_budget_increase_usd + 1e-12:
                    raise CareerOSError(
                        f"${extra_usd:.2f} would bring this objective's approved increases to "
                        f"${granted + extra_usd:.2f}, over the limit of "
                        f"${limits.max_total_budget_increase_usd:.2f}"
                        " (approvals.max_total_budget_increase_usd)"
                    )
            elif kind == "retry_limit":
                if not 1 <= extra_attempts <= limits.max_extra_attempts:
                    raise CareerOSError(
                        f"{extra_attempts} extra attempt(s) is outside "
                        f"1..{limits.max_extra_attempts} (approvals.max_extra_attempts)"
                    )
                granted_n = int(
                    c.execute(
                        "SELECT COALESCE(SUM(extra_attempts), 0) FROM approvals"
                        " WHERE kind='extra_attempts' AND task_id=? AND revoked_at IS NULL",
                        (task_id,),
                    ).fetchone()[0]
                )
                if granted_n + extra_attempts > limits.max_total_extra_attempts:
                    raise CareerOSError(
                        f"task {task_id} already has {granted_n} approved extra attempt(s); the "
                        f"limit is {limits.max_total_extra_attempts}"
                        " (approvals.max_total_extra_attempts)"
                    )
            elif kind == "clarification":
                if not (answer and answer.strip()):
                    raise CareerOSError("A clarification needs an answer (--answer).")
                answered = int(
                    c.execute(
                        "SELECT COUNT(*) FROM approval_requests WHERE objective_id=?"
                        " AND kind='clarification' AND status='approved' AND answer IS NOT NULL",
                        (obj_id,),
                    ).fetchone()[0]
                )
                if answered >= limits.max_clarifications:
                    raise CareerOSError(
                        f"objective {obj_id} has had {answered} clarification(s), the limit"
                        f" ({limits.max_clarifications}, approvals.max_clarifications);"
                        " deny the request or choose a playbook"
                    )
            self._resolve_in(
                c,
                request_id,
                "approved",
                "budget_increase" if kind == "budget_increase" else "approved",
                now,
            )
            if kind == "clarification":
                # An answer, not a permission: stored with the request; no grant row.
                c.execute(
                    "UPDATE approval_requests SET answer=? WHERE id=?",
                    (answer.strip() if answer else None, request_id),
                )
                if task_id is not None:  # planning resumes in a new attempt with the answer
                    c.execute(
                        "UPDATE tasks SET max_attempts = max_attempts + 1 WHERE id=?",
                        (task_id,),
                    )
            elif kind == "budget_increase":
                record_grant(c, r, "budget_increase", now, details={}, extra=extra_usd)
                # Only this objective's and this task's own lines rise; session and monthly
                # lines (the global limits) are untouched and still bound every reservation.
                c.execute(
                    "UPDATE objectives SET budget_usd = COALESCE(budget_usd, 0) + ? WHERE id=?",
                    (extra_usd, obj_id),
                )
                self.budgets.raise_limit_in(c, "objective", str(obj_id), extra_usd)
                if task_id is not None:
                    c.execute(
                        "UPDATE tasks SET budget_usd = COALESCE(budget_usd, 0) + ? WHERE id=?",
                        (extra_usd, task_id),
                    )
                    self.budgets.raise_limit_in(c, "task", str(task_id), extra_usd)
            elif kind == "retry_limit" and task_id is not None:
                record_grant(c, r, "extra_attempts", now, details={}, attempts=extra_attempts)
                c.execute(
                    "UPDATE tasks SET max_attempts = max_attempts + ? WHERE id=?",
                    (extra_attempts, task_id),
                )
            else:
                record_grant(c, r, "other", now, details={"kind": kind})
            if task_id is not None:
                self._release_task_in(c, task_id, kind, now)
            self._reopen_in(c, obj_id, now)

        self._decide_request(request_id, downgrade if accept_downgrade else grant)

    def deny(self, request_id: int) -> None:
        """Decline a request: its task fails and the objective stops - validated and
        applied in one transaction, so it cannot race an approval (finding N1)."""
        outcome: list[tuple[int, int]] = []

        def refuse(c: sqlite3.Connection, r: Any) -> None:
            now = utc_now_iso()
            self._resolve_in(c, request_id, "denied", "denied", now)
            if r["task_id"] is not None:
                expected = self._BLOCKED_STATE.get(r["kind"])
                allowed = [expected.value] if expected is not None else list(self._WAITING)
                cur = c.execute(
                    "UPDATE tasks SET status='failed', last_error='denied by user', updated_at=?,"
                    " finished_at=? WHERE id=? AND status IN (SELECT value FROM json_each(?))",
                    (now, now, r["task_id"], to_json(allowed)),
                )
                if cur.rowcount != 1:
                    raise CareerOSError(
                        f"Task {r['task_id']} changed state while the request was decided"
                    )
            result = self.repo.finish_objective_in(
                c, r["objective_id"], ObjectiveStatus.STOPPED, "user declined the request"
            )
            if result is None:
                raise CareerOSError(
                    f"Objective {r['objective_id']} finished while the request was decided"
                )
            outcome.append(result)

        r = self._decide_request(request_id, refuse)
        cancelled, withdrawn = outcome[0]
        extra = [
            text
            for n, text in (
                (cancelled, f"{cancelled} unneeded task(s) cancelled"),
                (withdrawn, f"{withdrawn} pending request(s) withdrawn"),
            )
            if n
        ]
        self._log(
            f"Objective {r['objective_id']} stopped: user declined the request"
            + (f" ({'; '.join(extra)})" if extra else "")
        )
