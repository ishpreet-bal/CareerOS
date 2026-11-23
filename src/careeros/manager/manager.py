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
import threading
import time
from collections.abc import Callable
from concurrent.futures import FIRST_COMPLETED, Future, ThreadPoolExecutor, wait
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any, ClassVar, cast

from careeros.cache.store import CacheStore
from careeros.context import AppContext, open_context_at
from careeros.core.clock import utc_now, utc_now_iso
from careeros.core.config import EFFORT_ORDER, DatasetKind, Effort
from careeros.core.db import Database, from_json, to_json
from careeros.core.errors import CareerOSError, NotFoundError, SessionTerminatedError
from careeros.cost.budget import BudgetManager
from careeros.domain.models import (
    Checkpoint,
    Objective,
    ObjectiveStatus,
    Plan,
    Priority,
    ResultStatus,
    StoredResult,
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
Clock = Callable[[], datetime]
RETRY_ACTIONS = ("retry_same", "retry_with_feedback", "escalate_effort", "escalate_tier")
_RUNNABLE = (TaskStatus.PENDING, TaskStatus.READY, TaskStatus.RETRY_SCHEDULED)


def _iso(dt: datetime) -> str:
    return dt.isoformat(timespec="seconds")


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
        self.gateway = ModelGateway(ctx.config, ctx.db, registry, self.budgets, CacheStore(ctx.db))
        self.fault_hook = fault_hook
        self.clock = clock
        self.events: list[str] = []
        self._events_lock = threading.Lock()
        #: Set when the session is terminating abruptly; workers stop at the next check.
        self._halt = threading.Event()
        self._session: str | None = None

    # ================================================================ logging / halting
    def _log(self, msg: str) -> None:
        with self._events_lock:
            self.events.append(msg)

    def _guard(self) -> None:
        """Workers call this between steps; once the session is halting they stop
        immediately, leaving state exactly as an abrupt process exit would."""
        if self._halt.is_set():
            raise SessionTerminatedError("execution session halted")

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
            session = self.repo.start_session()
            try:
                plan = self._model_assisted_plan(obj, why, session)
            finally:
                self.repo.end_session(session, "planning finished")
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

    def _record_degrade(
        self, objective_id: int, task_id: int | None, route: RouteDecision, purpose: str
    ) -> None:
        """Every downgraded routing choice, whatever initiated it, is a ``degrade`` decision
        (audit M6)."""
        if route.choice is None or route.status not in ("degraded", "approved_downgrade"):
            return
        self._decide_record(
            "degrade",
            objective_id=objective_id,
            task_id=task_id,
            rationale=route.reason,
            inputs={
                "purpose": purpose,
                "status": route.status,
                "priority": route.priority,
                "quality_floor": route.quality_floor,
                "chosen_p": route.choice.p_accept,
                "model_id": route.choice.model_id,
                "effort": route.choice.effort,
                "user_approved": route.status == "approved_downgrade",
                **route.extra,
            },
        )
        label = (
            "USER-APPROVED DOWNGRADE"
            if route.status == "approved_downgrade"
            else "DOWNGRADE (within limits)"
        )
        self._log(f"  [{purpose}] {label} recorded: {route.reason}")

    def _model_assisted_plan(self, obj: Objective, why: str, session: str) -> PlanSpec | None:
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
                self.budgets.available(obj.id, None, session),
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
            self._record_degrade(obj.id, None, route, "planning")
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
            try:
                outcome = self.gateway.call(
                    choice.model_id,
                    request,
                    CallContext(
                        obj.id, None, attempt, "planning", session, f"plan:{obj.id}:{attempt}"
                    ),
                )
            except BudgetExceededError as exc:
                self._log(f"  planning blocked by budget: {exc}")
                break
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
    def run(
        self,
        objective_id: int,
        *,
        max_attempts: int | None = None,
        force_recover: bool = False,
    ) -> RunSummary:
        """Advance an objective until done, blocked on the user, or ``max_attempts`` task
        attempts have started in this invocation. Safe to call repeatedly (resume).

        Independent ready tasks run concurrently, at most ``execution.max_concurrency`` at a
        time. ``force_recover`` treats every other session as dead (use only when you know
        no other CareerOS process is running, e.g. after a container restart).
        """
        start = len(self.events)
        session = self.repo.start_session()
        self._halt.clear()
        self._recover(objective_id, session, force=force_recover)
        limit = self.config.execution.max_concurrency
        hb_stop = threading.Event()
        heartbeat = threading.Thread(
            target=self._heartbeat,
            args=(session, hb_stop, cast(DatasetKind, self.db.dataset_kind)),
            daemon=True,
            name=f"careeros-heartbeat-{session}",
        )
        heartbeat.start()
        clones: list[CareerManager] = []
        clones_lock = threading.Lock()
        local = threading.local()

        def work(task_id: int) -> str:
            m = getattr(local, "manager", None)
            if m is None:
                m = self._clone()
                local.manager = m
                with clones_lock:
                    clones.append(m)
            return m._execute_claimed(objective_id, task_id, session)

        running: dict[Future[str], int] = {}
        deferred: set[int] = set()  # waiting for a concurrent reservation to settle
        started = 0
        idle_contention = 0
        stop_reason: str | None = None
        crash: SessionTerminatedError | None = None
        pool = ThreadPoolExecutor(max_workers=limit, thread_name_prefix="careeros-worker")
        try:
            while True:
                obj = self.repo.get_objective(objective_id)
                active = obj.status is ObjectiveStatus.ACTIVE and crash is None
                done_now = active and self._done(obj)
                quota_left = max_attempts is None or started < max_attempts
                if active and not done_now and quota_left:
                    for task in self._ready_tasks(objective_id):
                        if len(running) >= limit or not (
                            max_attempts is None or started < max_attempts
                        ):
                            break
                        assert task.id is not None
                        if task.id in deferred and running:
                            continue
                        if self._claim(task, session):
                            started += 1
                            deferred.discard(task.id)
                            running[pool.submit(work, task.id)] = task.id
                if not running:
                    if crash is not None:
                        break
                    if not active:
                        stop_reason = obj.stop_reason
                        break
                    if done_now:
                        self._finish(obj, ObjectiveStatus.DONE, "definition of done met")
                        stop_reason = "definition of done met"
                        break
                    if not quota_left:
                        stop_reason = "attempt limit for this invocation"
                        break
                    if deferred:  # budget held by another session's reservation
                        idle_contention += 1
                        if idle_contention > 50:
                            stop_reason = "budget is reserved by another active session"
                            break
                        deferred.clear()
                        time.sleep(0.05)
                        continue
                    stop_reason = self._settle_idle(obj, session)
                    break
                finished, _ = wait(running, return_when=FIRST_COMPLETED)
                for fut in finished:
                    task_id = running.pop(fut)
                    try:
                        outcome = fut.result()
                    except SessionTerminatedError as exc:
                        crash = crash or exc
                        self._halt.set()  # every other worker stops at its next step
                        continue
                    except Exception as exc:  # a worker failure must not stop the others
                        self._worker_failed(objective_id, task_id, exc)
                        outcome = "error"
                    if outcome == "contention":
                        deferred.add(task_id)
                    else:
                        deferred.clear()  # a settled task may have freed budget
                        idle_contention = 0
        finally:
            pool.shutdown(wait=True)
            hb_stop.set()
            heartbeat.join()
            for m in clones:
                m.ctx.close()
        if crash is not None:
            # Abrupt end: the session stays open and its leases expire; the next session
            # recovers the work. Nothing is cleaned up, exactly as after a process kill.
            raise crash
        self.repo.end_session(session, stop_reason or "stopped")
        final = self.repo.get_objective(objective_id)
        return RunSummary(objective_id, final.status, started, self.events[start:], stop_reason)

    # ---------------------------------------------------------------- claiming / leases
    def _claim(self, task: Task, session: str) -> bool:
        """Atomically take a runnable task. Exactly one claimant can succeed, across
        threads and processes, so a task attempt is never executed twice."""
        assert task.id is not None
        lease = _iso(self.clock() + timedelta(seconds=self.config.limits.lease_seconds))
        now = utc_now_iso()
        with self.db.transaction() as c:
            cur = c.execute(
                "UPDATE tasks SET status='running', attempt=attempt+1, lease_owner=?,"
                " lease_expires_at=?, started_at=COALESCE(started_at, ?), updated_at=?"
                " WHERE id=? AND status IN ('pending','ready','retry_scheduled')",
                (session, lease, now, now, task.id),
            )
        return cur.rowcount == 1

    def _heartbeat(self, session: str, stop: threading.Event, kind: DatasetKind) -> None:
        """Renew this session's leases while it is alive (runs on its own connection)."""
        if self.db.path is None:
            return
        interval = self.config.limits.heartbeat_seconds or max(
            0.5, self.config.limits.lease_seconds / 3
        )
        db = Database.open(self.db.path, dataset_kind=kind)
        try:
            while not stop.wait(interval):
                lease = _iso(self.clock() + timedelta(seconds=self.config.limits.lease_seconds))
                with db.transaction() as c:
                    c.execute(
                        "UPDATE tasks SET lease_expires_at=? WHERE lease_owner=?"
                        " AND status IN ('running','reviewing')",
                        (lease, session),
                    )
                    c.execute(
                        "UPDATE execution_sessions SET last_heartbeat_at=? WHERE id=?",
                        (_iso(self.clock()), session),
                    )
        finally:
            db.close()

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
        """Take over work left by dead sessions, without touching live ones.

        * Running tasks whose lease has expired (or whose owner is dead) become ready at the
          same attempt number; their checkpoint or idempotent call record supplies any model
          reply already received, so completed work is neither repeated nor re-billed.
        * Calls left in flight by dead sessions are marked abandoned and charged at their
          estimate (conservative); other reservations of dead sessions are released.
        """
        stale = self._stale_sessions(session, force=force)
        now = _iso(self.clock())
        abandoned = self.gateway.abandon_in_flight(stale)
        released = 0
        for sid in stale:
            for r in self.budgets.held_reservations(sid):
                with self.db.transaction() as c:
                    self.budgets.release_in(c, r["id"])
                released += 1
        if abandoned or released:
            self._log(
                f"  recovery: {abandoned} in-flight call(s) abandoned, "
                f"{released} reservation(s) released from dead sessions"
            )
        for t in self.repo.list_tasks(objective_id):
            if (
                t.status not in (TaskStatus.RUNNING, TaskStatus.REVIEWING)
                or t.lease_owner == session
            ):
                continue
            expired = t.lease_expires_at is None or t.lease_expires_at < now
            if not (expired or t.lease_owner in stale):
                continue  # owned by a live session: leave it alone
            assert t.id is not None
            with self.db.transaction() as c:
                cur = c.execute(
                    "UPDATE tasks SET status='ready', attempt=MAX(0, attempt-1),"
                    " lease_owner=NULL, lease_expires_at=NULL, updated_at=?"
                    " WHERE id=? AND lease_owner IS ? AND status IN ('running','reviewing')",
                    (utc_now_iso(), t.id, t.lease_owner),
                )
            if cur.rowcount != 1:
                continue
            self._decide_record(
                "recover",
                objective_id=objective_id,
                task_id=t.id,
                rationale="lease expired: session ended while the task was running",
                inputs={"recovered": True, "from_session": t.lease_owner, "attempt": t.attempt},
            )
            self._log(f"  recovered task {t.id} ({t.step}) interrupted at attempt {t.attempt}")
        if stale:
            with self.db.transaction() as c:
                for sid in stale:
                    c.execute(
                        "UPDATE execution_sessions SET ended_at=COALESCE(ended_at, ?),"
                        " end_reason=COALESCE(end_reason, 'presumed dead (recovered)')"
                        " WHERE id=?",
                        (utc_now_iso(), sid),
                    )

    def _worker_failed(self, objective_id: int, task_id: int, exc: Exception) -> None:
        """An unexpected exception in one worker: retry the task (bounded), never crash
        the run or the other workers."""
        task = self.repo.get_task(task_id)
        held = [r for r in self.budgets.held_reservations() if r["task_id"] == task_id]
        for r in held:
            with self.db.transaction() as c:
                self.budgets.release_in(c, r["id"])
        if task.status not in (TaskStatus.RUNNING, TaskStatus.REVIEWING):
            return
        retry = task.attempt < task.max_attempts
        err = f"{type(exc).__name__}: {exc}"[:300]
        self._decide_record(
            "worker_error",
            objective_id=objective_id,
            task_id=task_id,
            rationale=f"worker raised {err}",
            inputs={"attempt": task.attempt, "will_retry": retry},
        )
        with self.db.transaction() as c:
            c.execute(
                "UPDATE tasks SET status=?, lease_owner=NULL, lease_expires_at=NULL,"
                " last_error=?, updated_at=?,"
                " finished_at=CASE WHEN ?='failed' THEN ? ELSE finished_at END WHERE id=?",
                (
                    "retry_scheduled" if retry else "failed",
                    err,
                    utc_now_iso(),
                    "retry_scheduled" if retry else "failed",
                    utc_now_iso(),
                    task_id,
                ),
            )
        self._log(
            f"  [{task.step}] WORKER ERROR ({err}) -> "
            + ("retry" if retry else "failed (attempt limit)")
        )

    def _requeue(self, task: Task, reason: str) -> None:
        """Give an attempt back and make the task runnable again (no user involvement)."""
        assert task.id is not None
        with self.db.transaction() as c:
            c.execute(
                "UPDATE tasks SET status='ready', attempt=MAX(0, attempt-1), lease_owner=NULL,"
                " lease_expires_at=NULL, updated_at=? WHERE id=?",
                (utc_now_iso(), task.id),
            )
        self._decide_record(
            "requeue", objective_id=task.objective_id, task_id=task.id, rationale=reason
        )
        self._log(f"  [{task.step}] REQUEUED: {reason}")

    # ---------------------------------------------------------------- readiness
    def _ready_tasks(self, objective_id: int) -> list[Task]:
        """Runnable tasks whose prerequisites all succeeded, in wave order. Tasks whose
        prerequisites failed, were skipped or were cancelled are skipped."""
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
                self._decide_record(
                    "skip",
                    objective_id=objective_id,
                    task_id=t.id,
                    rationale="a prerequisite failed, was skipped or cancelled",
                )
                continue
            if all(d.status is TaskStatus.SUCCEEDED for d in deps):
                ready.append(t)
        ready.sort(key=lambda t: (order.get(t.step or "", 99), t.id or 0))
        return ready

    def _settle_idle(self, obj: Objective, session: str) -> str:
        assert obj.id is not None
        tasks = self.repo.list_tasks(obj.id)
        if any(t.status in (TaskStatus.RUNNING, TaskStatus.REVIEWING) for t in tasks):
            self._log("Some tasks are running in another live session; nothing to do here.")
            return "tasks are running in another session"
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
        """Objective, tasks and approval requests reach a consistent final state together
        (audit M3): unfinished tasks are cancelled - including ones blocked on budget or
        awaiting the user - and pending requests are withdrawn, in one transaction."""
        assert obj.id is not None
        cancelled, withdrawn = self.repo.finish_objective(obj.id, status, reason)
        extra = []
        if cancelled:
            extra.append(f"{cancelled} unneeded task(s) cancelled")
        if withdrawn:
            extra.append(f"{withdrawn} pending request(s) withdrawn")
        self._log(
            f"Objective {obj.id} {status.value}: {reason}"
            + (f" ({'; '.join(extra)})" if extra else "")
        )

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

    def _execute_claimed(self, objective_id: int, task_id: int, session: str) -> str:
        """Run one claimed task attempt (inside a worker thread, own connection).

        Returns ``"done"`` or ``"contention"`` (requeued while a concurrent reservation
        settles). Raises ``SessionTerminatedError`` if the session is halting.
        """
        self._session = session
        self._guard()
        obj = self.repo.get_objective(objective_id)
        task = self.repo.get_task(task_id)
        if task.status is not TaskStatus.RUNNING or task.lease_owner != session:
            return "done"  # not ours (should not happen: claims are exclusive)
        assert task.id is not None and obj.id is not None
        attempt = task.attempt
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
            return "done"
        return self._execute_model(obj, task, spec, step, attempt)

    def _record_result_once(
        self, task_id: int, attempt: int, result: WorkerResult, eff: float
    ) -> StoredResult:
        """One result row per (task, attempt), even if the attempt is resumed."""
        for r in self.repo.list_results(task_id):
            if r.attempt == attempt:
                return r
        return self.repo.record_result(task_id, attempt, result, effective_confidence=eff)

    # ---------------------------------------------------------------- deterministic
    def _execute_deterministic(
        self, obj: Objective, task: Task, spec: StepSpec, step: StepContext, attempt: int
    ) -> None:
        assert task.id is not None
        worker = self.workers.deterministic[spec.worker]
        result = worker.run(step)
        self._guard()
        stored = self._record_result_once(task.id, attempt, result, result.confidence)
        if result.status is ResultStatus.SUCCEEDED:
            # Prune before accepting, so no dependent can be dispatched in between.
            if result.output.get("prune_dependents"):
                self._prune(obj, task, str(result.output.get("reason", "")))
            self._accept(task, stored.id, f"deterministic {spec.worker}: $0")
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
        # Committed spend only: concurrent reservations are enforced at reservation time.
        budget = self.budgets.available(obj.id, task.id, self._session, include_reservations=False)
        row = self.db.query_one("SELECT approved_floor FROM tasks WHERE id=?", (task.id,))
        approved = row["approved_floor"] if row is not None else None
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
        self, obj: Objective, task: Task, spec: StepSpec, step: StepContext, attempt: int
    ) -> str:
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
                self._block(
                    obj,
                    task,
                    kind,
                    {
                        "reason": route.reason,
                        "quality_floor": route.quality_floor,
                        "route_purpose": "task",
                        **route.extra,
                    },
                )
                return "done"
            self._record_degrade(obj.id, task.id, route, f"task:{spec.id}")
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
                    "step": spec.id,
                    "complexity": route.complexity,
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
                        self._session,
                        f"task:{task.id}:attempt:{attempt}",
                    ),
                )
            except BudgetExceededError as exc:
                if exc.is_contention:
                    self._requeue(
                        task,
                        f"budget contention ({exc.scope}): waiting for "
                        "concurrent reservations to settle",
                    )
                    return "contention"
                self._block(
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
            self._guard()
            if outcome.overshoot_usd > 0:
                self._log(f"  [{spec.id}] BUDGET OVERSHOOT recorded: {outcome.error}")
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
        self._guard()
        stored = self._record_result_once(task.id, attempt, result, round(result.confidence, 4))
        review_enabled = spec.review if spec.review is not None else task.important
        if outcome.cache_hit and review_enabled:
            # Only accepted (already reviewed) results are cached; do not pay to re-review.
            review_enabled = False
            self._log(f"  [{spec.id}] cache hit on a previously reviewed result: $0, no review")
        if problem is None and review_enabled:
            verdict, score, issues = self._review(
                obj, task, spec, result, stored.id, model_id, attempt, sim_quality
            )
            if verdict in ("unavailable", "contention"):
                return "contention" if verdict == "contention" else "done"
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
        self._guard()
        if problem is None:
            self.gateway.mark_accepted(outcome)
            if result.output.get("prune_dependents"):
                self._prune(obj, task, str(result.output.get("reason", "")))
            self._accept(
                task,
                stored.id,
                f"{model_id}/{effort} accepted"
                + (f" (review {score:.2f})" if score is not None else ""),
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
        self._apply(obj, task, decide(problem, state), problem, detail, route_tier)
        return "done"

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
        session = self._session
        row = self.db.query_one("SELECT approved_review_floor FROM tasks WHERE id=?", (task.id,))
        route = self.router.route(
            task_type,
            task.priority.value,
            complexity,
            self.budgets.available(obj.id, task.id, session, include_reservations=False),
            exclude_models=frozenset({author_model}),
            # Only an approval given for *this review* lowers its floor (audit M6).
            approved_floor=row["approved_review_floor"] if row is not None else None,
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
                obj,
                task,
                "budget_increase",
                {
                    "reason": f"review: {route.reason}",
                    "quality_floor": route.quality_floor,
                    "route_purpose": "review",
                    **route.extra,
                },
            )
            return "unavailable", None, []
        self._record_degrade(obj.id, task.id, route, f"review:{spec.id}")
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
                    session,
                    f"review:{task.id}:attempt:{attempt}",
                ),
            )
        except BudgetExceededError as exc:
            if exc.is_contention:
                self._requeue(task, f"review budget contention ({exc.scope})")
                return "contention", None, []
            self._block(
                obj,
                task,
                "budget_increase",
                {
                    "reason": f"review: {exc}",
                    "needed_usd": exc.needed_usd,
                    "route_purpose": "review",
                },
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
        self._guard()
        already = self.db.query_one(
            "SELECT id FROM reviews WHERE task_id=? AND result_id=?", (task.id, result_id)
        )
        with self.db.transaction() as c:
            if already is not None:
                c.execute("DELETE FROM reviews WHERE id=?", (already["id"],))  # resumed review
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
        steps = {t.id: str(t.step) for t in tasks if t.id is not None}
        if not pruned:
            return
        # One transaction: skip the tasks and append them to the plan's skipped list
        # (union with earlier replans; concurrent prunes cannot lose each other's steps).
        new, _ = self.repo.skip_steps(
            obj.id,
            {tid: steps[tid] for tid in pruned},
            rationale=f"replan after {task.step}: {reason}",
            reason=reason,
        )
        self._decide_record(
            "replan",
            objective_id=obj.id,
            task_id=task.id,
            rationale=f"{task.step} reported: {reason}",
            inputs={
                "pruned_steps": sorted(steps[t] for t in pruned),
                "all_skipped": new.graph.get("skipped", []),
                "plan_version": new.version,
            },
        )
        self._log(f"  REPLAN v{new.version}: skipped {sorted(steps[t] for t in pruned)} ({reason})")

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

    _BLOCKED_STATE: ClassVar[dict[str, TaskStatus]] = {
        "budget_increase": TaskStatus.BLOCKED_BUDGET,
        "retry_limit": TaskStatus.AWAITING_HUMAN,
    }

    def _validate_request(self, request_id: int) -> Any:
        """A request can only be decided while it is pending, its objective is unfinished
        and its task is still in the blocked state the request was made for. Obsolete
        requests are withdrawn rather than applied (audit M3)."""
        r = self.db.query_one("SELECT * FROM approval_requests WHERE id=?", (request_id,))
        if r is None:
            raise NotFoundError(f"Approval request {request_id} not found")
        if r["status"] != "pending":
            raise CareerOSError(f"Approval request {request_id} is already {r['status']}")
        obj = self.repo.get_objective(r["objective_id"])
        problem = None
        if obj.status not in (ObjectiveStatus.ACTIVE, ObjectiveStatus.AWAITING_HUMAN):
            problem = f"objective {obj.id} is already {obj.status.value}"
        elif r["task_id"] is not None:
            task = self.repo.get_task(r["task_id"])
            expected = self._BLOCKED_STATE.get(r["kind"])
            if expected is not None and task.status is not expected:
                problem = f"task {task.id} is {task.status.value}, no longer {expected.value}"
        if problem is not None:
            with self.db.transaction() as c:
                c.execute(
                    "UPDATE approval_requests SET status='withdrawn', resolution=?,"
                    " resolved_at=? WHERE id=? AND status='pending'",
                    (f"withdrawn: {problem}", utc_now_iso(), request_id),
                )
            raise CareerOSError(f"Approval request {request_id} is obsolete: {problem}.")
        return r

    def approve(
        self,
        request_id: int,
        *,
        extra_usd: float = 0.0,
        extra_attempts: int = 1,
        accept_downgrade: bool = False,
    ) -> None:
        """Record the user's decision and make the blocked task runnable again.

        For a budget request the user either adds (simulated) budget, or with
        ``accept_downgrade`` explicitly accepts the best affordable lower-quality option the
        request offered. The downgrade applies only to the routing step that asked for it
        (the task itself, or its review). Everything is written in one transaction.
        """
        r = self._validate_request(request_id)
        kind, obj_id, task_id = r["kind"], r["objective_id"], r["task_id"]
        details = from_json(r["details"], {})
        now = utc_now_iso()
        if accept_downgrade:
            option = details.get("downgrade_option")
            purpose = details.get("route_purpose", "task")
            if kind != "budget_increase" or not option or task_id is None:
                raise CareerOSError("This request does not offer a quality downgrade.")
            if purpose not in ("task", "review"):
                raise CareerOSError(f"Downgrades cannot be approved for {purpose} routing.")
            column = "approved_floor" if purpose == "task" else "approved_review_floor"
            floor_to = float(option["p_accept"])
            with self.db.transaction() as c:
                c.execute(
                    "UPDATE approval_requests SET status='approved', resolution=?,"
                    " resolved_at=? WHERE id=?",
                    (f"accept_downgrade:{purpose}", now, request_id),
                )
                c.execute(
                    "INSERT INTO approvals (kind, scope, details, granted_at) VALUES (?,?,?,?)",
                    (
                        "quality_downgrade",
                        f"objective:{obj_id}/task:{task_id}/{purpose}",
                        to_json(
                            {
                                "request_id": request_id,
                                "route_purpose": purpose,
                                "floor_from": details.get("quality_floor"),
                                "floor_to": floor_to,
                                "option": option,
                            }
                        ),
                        now,
                    ),
                )
                c.execute(
                    f"UPDATE tasks SET {column}=?, status='ready', updated_at=? WHERE id=?",  # noqa: S608
                    (max(0.0, floor_to - 1e-9), now, task_id),
                )
                c.execute(
                    "UPDATE objectives SET status='active', stop_reason=NULL, updated_at=?"
                    " WHERE id=?",
                    (now, obj_id),
                )
                c.execute(
                    "INSERT INTO manager_decisions (objective_id, task_id, decision, rationale,"
                    " inputs, created_at) VALUES (?,?,?,?,?,?)",
                    (
                        obj_id,
                        task_id,
                        "approve_downgrade",
                        f"user accepted a {purpose} quality downgrade: floor "
                        f"{details.get('quality_floor')} -> {floor_to:.3f}",
                        to_json(
                            {"request_id": request_id, "route_purpose": purpose, "option": option}
                        ),
                        now,
                    ),
                )
            return
        if kind == "budget_increase" and extra_usd <= 0:
            raise CareerOSError("A budget approval needs a positive simulated amount.")
        with self.db.transaction() as c:
            c.execute(
                "UPDATE approval_requests SET status='approved', resolution=?, resolved_at=?"
                " WHERE id=?",
                ("budget_increase" if kind == "budget_increase" else "approved", now, request_id),
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
                self.budgets.raise_limit_in(c, "objective", str(obj_id), extra_usd)
                if task_id is not None:
                    c.execute(
                        "UPDATE tasks SET budget_usd = COALESCE(budget_usd, 0) + ? WHERE id=?",
                        (extra_usd, task_id),
                    )
                    self.budgets.raise_limit_in(c, "task", str(task_id), extra_usd)
            elif kind == "retry_limit" and task_id is not None:
                c.execute(
                    "UPDATE tasks SET max_attempts = max_attempts + ? WHERE id=?",
                    (extra_attempts, task_id),
                )
            if task_id is not None:
                c.execute(
                    "UPDATE tasks SET status='ready', updated_at=? WHERE id=?", (now, task_id)
                )
            c.execute(
                "UPDATE objectives SET status='active', stop_reason=NULL, updated_at=? WHERE id=?",
                (now, obj_id),
            )

    def deny(self, request_id: int) -> None:
        """Decline a request: its task fails and the objective stops, consistently."""
        r = self._validate_request(request_id)
        now = utc_now_iso()
        with self.db.transaction() as c:
            c.execute(
                "UPDATE approval_requests SET status='denied', resolution='denied',"
                " resolved_at=? WHERE id=?",
                (now, request_id),
            )
            if r["task_id"] is not None:
                c.execute(
                    "UPDATE tasks SET status='failed', last_error='denied by user', updated_at=?,"
                    " finished_at=? WHERE id=?",
                    (now, now, r["task_id"]),
                )
        self._finish(
            self.repo.get_objective(r["objective_id"]),
            ObjectiveStatus.STOPPED,
            "user declined the request",
        )
