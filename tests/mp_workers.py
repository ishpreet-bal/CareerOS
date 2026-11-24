"""Entry points for separate-process tests (spawn start method).

Each function runs in its own operating-system process with its own interpreter and its
own SQLite connections, against a shared data directory. Results go back through a queue.
"""

from __future__ import annotations

import os
import time
import traceback
from datetime import timedelta
from pathlib import Path
from typing import Any, ClassVar

CONFIG_DIR = Path(__file__).resolve().parents[1] / "config"


def _ctx(data_dir: str, limits: dict[str, Any] | None = None) -> Any:
    os.environ["CAREEROS_DATA_DIR"] = data_dir
    from careeros.context import open_context
    from careeros.core.config import load_config

    cfg = load_config(CONFIG_DIR)
    if limits:
        cfg = cfg.model_copy(update={"limits": cfg.limits.model_copy(update=limits)})
    return open_context(cfg)


def _library(book: dict[str, Any] | None) -> Any:
    from careeros.manager.plans import Playbook, PlaybookLibrary

    return PlaybookLibrary([Playbook.model_validate(book)]) if book else None


def _clock(offset_minutes: float) -> Any:
    from careeros.core.clock import utc_now

    return lambda: utc_now() + timedelta(minutes=offset_minutes)


def _manager(
    c: Any,
    book: dict[str, Any] | None,
    *,
    offset: float = 0.0,
    adapter: Any = None,
    fault_hook: Any = None,
) -> Any:
    from careeros.manager.manager import CareerManager

    kw: dict[str, Any] = {"clock": _clock(offset)}
    lib = _library(book)
    if lib is not None:
        kw["playbooks"] = lib
    if adapter is not None:
        kw["adapter"] = adapter
    if fault_hook is not None:
        kw["fault_hook"] = fault_hook
    return CareerManager(c, **kw)


def _blocking_adapter(steps: list[str], entered: Any, release: Any, timeout: float) -> Any:
    from careeros.llm.adapters.mock import MockAdapter

    class MPBlockingAdapter(MockAdapter):
        mode_label: ClassVar[str] = "mock"

        def call(self, profile: Any, request: Any) -> Any:
            step = request.simulation.get("step")
            if step in steps and request.task_type != "quality.review":
                steps.remove(step)
                entered.set()
                release.wait(timeout)
            return super().call(profile, request)

    return MPBlockingAdapter()


def _report(out: Any, **data: Any) -> None:
    out.put({"pid": os.getpid(), **data})


# ---------------------------------------------------------------------- entry points
def claim_task(data_dir: str, task_id: int, barrier: Any, out: Any) -> None:
    try:
        with _ctx(data_dir) as c:
            m = _manager(c, None)
            sid = c.orchestration.start_session()
            task = c.orchestration.get_task(task_id)
            barrier.wait(30)
            fence = m._claim(task, sid)
            _report(out, fence=None if fence is None else (fence.generation, fence.attempt))
    except BaseException:
        _report(out, error=traceback.format_exc())


def recover_once(
    data_dir: str, objective_id: int, book: dict[str, Any], offset: float, barrier: Any, out: Any
) -> None:
    try:
        with _ctx(data_dir) as c:
            m = _manager(c, book, offset=offset)
            sid = c.orchestration.start_session()
            barrier.wait(30)
            m._recover(objective_id, sid)
            _report(out, ok=True)
    except BaseException:
        _report(out, error=traceback.format_exc())


def run_objective(
    data_dir: str,
    objective_id: int,
    book: dict[str, Any] | None,
    out: Any,
    offset: float = 0.0,
    force: bool = False,
    limits: dict[str, Any] | None = None,
) -> None:
    try:
        with _ctx(data_dir, limits) as c:
            s = _manager(c, book, offset=offset).run(objective_id, force_recover=force)
            _report(out, status=s.status.value, stop_reason=s.stop_reason)
    except BaseException:
        _report(out, error=traceback.format_exc())


def run_blocking(
    data_dir: str,
    objective_id: int,
    book: dict[str, Any],
    steps: list[str],
    entered: Any,
    release: Any,
    out: Any,
    timeout: float = 20.0,
    limits: dict[str, Any] | None = None,
    fail_renewals: bool = False,
) -> None:
    """Run an objective whose model call for ``steps`` blocks until ``release``."""
    try:
        with _ctx(data_dir, limits) as c:
            if fail_renewals:  # simulated database outage for the heartbeat only
                from careeros.manager.manager import CareerManager

                CareerManager._renew_leases = lambda self, db, s, st: False  # type: ignore[method-assign]
            m = _manager(c, book, adapter=_blocking_adapter(steps, entered, release, timeout))
            s = m.run(objective_id)
            _report(
                out,
                status=s.status.value,
                stop_reason=s.stop_reason,
                lease_lost=m._lease_lost.is_set(),
            )
    except BaseException:
        _report(out, error=traceback.format_exc())


def try_steal(
    data_dir: str, objective_id: int, book: dict[str, Any], seconds: float, out: Any
) -> None:
    """Repeatedly attempt recovery with an honest clock for ``seconds``."""
    try:
        with _ctx(data_dir) as c:
            m = _manager(c, book)
            sid = c.orchestration.start_session()
            deadline = time.monotonic() + seconds
            n = 0
            while time.monotonic() < deadline:
                m._recover(objective_id, sid)
                n += 1
                time.sleep(0.1)
            _report(out, attempts=n)
    except BaseException:
        _report(out, error=traceback.format_exc())


def stale_owner(
    data_dir: str, objective_id: int, book: dict[str, Any], claimed: Any, proceed: Any, out: Any
) -> None:
    """Claim the task, pause (as if the process were frozen), then try to write."""
    try:
        from careeros.core.errors import OwnershipLostError
        from careeros.llm.contracts import ModelRequest, OutputSchema
        from careeros.llm.gateway import CallContext

        with _ctx(data_dir) as c:
            m = _manager(c, book)
            sid = c.orchestration.start_session()
            task = c.orchestration.list_tasks(objective_id)[0]
            obj = c.orchestration.get_objective(objective_id)
            fence = m._claim(task, sid)
            claimed.set()
            proceed.wait(60)
            outcomes = {}
            attempts = {
                "accept": lambda: m._accept(fence, obj, task, 1, "stale accept"),
                "requeue": lambda: m._requeue(fence, task, "stale requeue"),
                "block": lambda: m._block(fence, obj, task, "budget_increase", {"reason": "stale"}),
                "model_call": lambda: m.gateway.call(
                    "mock-small",
                    ModelRequest(
                        task_type="job.classify_role",
                        prompt="x",
                        output_schema=OutputSchema(("role_family",)),
                    ),
                    CallContext(objective_id, task.id, 1, "task", sid, "stale:key", fence),
                ),
            }
            for name, op in attempts.items():
                try:
                    op()
                    outcomes[name] = "WRITTEN"
                except OwnershipLostError:
                    outcomes[name] = "rejected"
            _report(out, fence=(fence.generation, fence.attempt), outcomes=outcomes)
    except BaseException:
        _report(out, error=traceback.format_exc())


def block_then_die(
    data_dir: str, objective_id: int, step: str, entered: Any, release: Any, out: Any
) -> None:
    """Block inside ``step``'s model call until ``release`` (so other processes can work
    concurrently), then die abruptly (os._exit) right after the reply - no cleanup, no
    session end, other worker threads killed mid-flight."""
    try:
        with _ctx(data_dir) as c:

            def die(point: str, task: Any) -> None:
                if point == "after_model_response" and task.step == step:
                    _report(out, died_at=step)
                    time.sleep(0.2)  # let the queue flush
                    os._exit(17)

            adapter = _blocking_adapter([step], entered, release, 60.0)
            _manager(c, None, adapter=adapter, fault_hook=die).run(objective_id)
            _report(out, status="finished-without-dying")
    except BaseException:
        _report(out, error=traceback.format_exc())


def open_fresh(path: str, barrier: Any, out: Any) -> None:
    """Open (and so initialise and migrate) a brand-new database concurrently."""
    try:
        from careeros.core.db import Database

        barrier.wait(30)
        with Database.open(Path(path), dataset_kind="synthetic") as db:
            _report(out, version=db.schema_version(), kind=db.dataset_kind)
    except BaseException:
        _report(out, error=traceback.format_exc())


def decide_request(data_dir: str, request_id: int, action: str, barrier: Any, out: Any) -> None:
    """Approve (+$1, simulated) or deny one approval request, in step with other
    processes doing the same."""
    try:
        from careeros.core.errors import CareerOSError

        with _ctx(data_dir) as c:
            m = _manager(c, None)
            barrier.wait(30)
            try:
                if action == "approve":
                    m.approve(request_id, extra_usd=1.0)
                else:
                    m.deny(request_id)
                _report(out, action=action, result="ok")
            except CareerOSError as exc:
                _report(out, action=action, result="refused", reason=str(exc))
    except BaseException:
        _report(out, error=traceback.format_exc())


def quota_race_owner(
    data_dir: str,
    objective_id: int,
    book: dict[str, Any],
    held_reservation: int,
    paused: Any,
    proceed: Any,
    out: Any,
) -> None:
    """Invocation A, quota 1. Its first claim of attempt 1 is requeued for budget
    contention (another session's reservation, released right after). Its next claim -
    of the same, already counted attempt - is paused between the quota decision and the
    claim itself, while another process changes the task."""
    try:
        from careeros.cost.budget import BudgetManager
        from careeros.manager.manager import CareerManager

        orig_requeue, orig_claim = CareerManager._requeue, CareerManager._claim
        claims: list[int] = []

        def requeue(self: Any, fence: Any, task: Any, reason: str) -> None:
            orig_requeue(self, fence, task, reason)
            with self.db.transaction() as c:
                BudgetManager.release_in(c, held_reservation)

        def claim(self: Any, *args: Any, **kwargs: Any) -> Any:
            claims.append(1)
            if len(claims) == 2:  # quota already decided for this claim
                paused.set()
                proceed.wait(60)
            return orig_claim(self, *args, **kwargs)

        CareerManager._requeue = requeue  # type: ignore[method-assign]
        CareerManager._claim = claim  # type: ignore[method-assign]
        with _ctx(data_dir) as c:
            s = _manager(c, book).run(objective_id, max_attempts=1)
            t = c.db.query_one("SELECT attempt FROM tasks WHERE objective_id=?", (objective_id,))
            _report(
                out,
                attempts_executed=s.attempts_executed,
                dispatches=getattr(s, "dispatches", None),
                stop_reason=s.stop_reason,
                task_attempt=t["attempt"],
            )
    except BaseException:
        _report(out, error=traceback.format_exc())


def run_scripted(
    data_dir: str,
    objective_id: int,
    book: dict[str, Any],
    script: dict[str, list[str]],
    max_attempts: int | None,
    out: Any,
) -> None:
    """Run an objective with a scripted adapter (e.g. to make an attempt time out)."""
    try:
        from careeros.llm.adapters.scripted import ScriptedAdapter

        with _ctx(data_dir) as c:
            s = _manager(c, book, adapter=ScriptedAdapter(script)).run(
                objective_id, max_attempts=max_attempts
            )
            _report(out, status=s.status.value, stop_reason=s.stop_reason)
    except BaseException:
        _report(out, error=traceback.format_exc())


def _hold_adapter(steps: list[str], entered: Any, release: Any, timeout: float) -> Any:
    """Calls for ``steps`` block (inside the 'provider') until ``release``; each puts its
    step on the ``entered`` queue when it starts."""
    from careeros.llm.adapters.mock import MockAdapter

    class Hold(MockAdapter):
        mode_label: ClassVar[str] = "mock"

        def call(self, profile: Any, request: Any) -> Any:
            step = request.simulation.get("step")
            if step in steps and request.task_type != "quality.review":
                steps.remove(step)
                entered.put(step)
                release.wait(timeout)
            return super().call(profile, request)

    return Hold()


def run_with_signals(
    data_dir: str,
    objective_id: int,
    book: dict[str, Any],
    steps: list[str],
    entered: Any,
    release: Any,
    out: Any,
    limits: dict[str, Any] | None = None,
    requeued: Any = None,
    timeout: float = 30.0,
    signalled: Any = None,
) -> None:
    """Run an objective the way the CLI does: SIGINT/SIGTERM handled by
    ``graceful_shutdown``. Reports the run summary (a forced exit reports nothing).
    ``signalled`` is set once the handler has processed a signal (so a test can send a
    second one only after the first was handled: pending signals of one kind coalesce)."""
    try:
        from careeros.manager.manager import CareerManager
        from careeros.orchestration.shutdown import graceful_shutdown

        if requeued is not None:
            orig = CareerManager._requeue

            def requeue(self: Any, fence: Any, task: Any, reason: str) -> None:
                orig(self, fence, task, reason)
                requeued.set()

            CareerManager._requeue = requeue  # type: ignore[method-assign]
        with _ctx(data_dir, limits) as c:
            m = _manager(c, book, adapter=_hold_adapter(steps, entered, release, timeout))

            def notify(_msg: str) -> None:
                if signalled is not None:
                    signalled.set()

            with graceful_shutdown(m, notify=notify):
                s = m.run(objective_id)
            _report(out, status=s.status.value, stop_reason=s.stop_reason, dispatches=s.dispatches)
    except BaseException:
        _report(out, error=traceback.format_exc())


def run_like_cli(
    data_dir: str,
    objective_id: int,
    book: dict[str, Any],
    steps: list[str],
    entered: Any,
    release: Any,
    out: Any,
) -> None:
    """Run an objective as the ``resume`` command does - with its shutdown handling if
    this version has one (``graceful_shutdown``), otherwise Python's default SIGINT
    behaviour (KeyboardInterrupt). Reports the summary, or the exception's name."""
    try:
        from contextlib import AbstractContextManager, nullcontext

        try:
            from careeros.orchestration.shutdown import graceful_shutdown
        except ImportError:  # versions before C3
            graceful_shutdown = None  # type: ignore[assignment]

        with _ctx(data_dir) as c:
            m = _manager(c, book, adapter=_hold_adapter(steps, entered, release, 30.0))
            guard: AbstractContextManager[Any] = (
                graceful_shutdown(m, notify=lambda _msg: None)
                if graceful_shutdown is not None
                else nullcontext()
            )
            with guard:
                s = m.run(objective_id)
            _report(out, stop_reason=s.stop_reason, status=s.status.value)
    except BaseException as exc:
        _report(out, raised=type(exc).__name__)


def durability_after_recover(data_dir: str, objective_id: int, out: Any) -> None:
    """A recovering process: report the settings in effect on its own connection."""
    try:
        with _ctx(data_dir) as c:
            m = _manager(c, None, offset=10.0)
            m._recover(objective_id, c.orchestration.start_session())
            _report(out, durability=c.db.durability())
    except BaseException:
        _report(out, error=traceback.format_exc())


#: Crash points around a model call's reservation (T1) and settlement (T2).
CRASH_POINTS = ("after_reserve", "after_reply", "inside_settlement", "after_settlement")


def crash_at(data_dir: str, objective_id: int, book: dict[str, Any], point: str, out: Any) -> None:
    """Run an objective and kill the process abruptly (``os._exit``) at ``point``:

    * ``after_reserve`` - T1 committed (reservation held, call in flight), before the
      provider is called;
    * ``after_reply`` - the provider replied, before T2 starts;
    * ``inside_settlement`` - inside T2, after the call row was updated and while the
      reservation is being settled, before COMMIT;
    * ``after_settlement`` - T2 committed, before the next step.
    """
    try:
        from careeros.cost.budget import BudgetManager
        from careeros.llm.adapters.mock import MockAdapter

        def die() -> None:
            _report(out, died_at=point)
            time.sleep(0.2)
            os._exit(17)

        class Crashing(MockAdapter):
            mode_label: ClassVar[str] = "mock"

            def call(self, profile: Any, request: Any) -> Any:
                if request.task_type == "quality.review":
                    return super().call(profile, request)
                if point == "after_reserve":
                    die()
                reply = super().call(profile, request)
                if point == "after_reply":
                    die()
                return reply

        if point == "inside_settlement":
            orig = BudgetManager.settle_in

            def settle_in(c: Any, reservation_id: int, actual: float) -> None:
                orig(c, reservation_id, actual)
                die()  # the transaction is still open: it must roll back entirely

            BudgetManager.settle_in = staticmethod(settle_in)  # type: ignore[method-assign]

        def hook(p: str, task: Any) -> None:
            if point == "after_settlement" and p == "after_model_response":
                die()

        with _ctx(data_dir) as c:
            _manager(c, book, adapter=Crashing(), fault_hook=hook).run(objective_id)
            _report(out, status="finished-without-crashing")
    except BaseException:
        _report(out, error=traceback.format_exc())


def _plan_hold_adapter(entered: Any, release: Any, timeout: float) -> Any:
    """The first planning call blocks inside the 'provider' until ``release``."""
    from careeros.llm.adapters.mock import MockAdapter

    class PlanHold(MockAdapter):
        mode_label: ClassVar[str] = "mock"
        held = False

        def call(self, profile: Any, request: Any) -> Any:
            if request.task_type == "manager.plan_novel" and not self.held:
                self.held = True
                entered.set()
                release.wait(timeout)
            return super().call(profile, request)

    return PlanHold()


def plan_with_hold(
    data_dir: str,
    objective_id: int,
    entered: Any,
    release: Any,
    out: Any,
    limits: dict[str, Any] | None = None,
    fail_renewals: bool = False,
    timeout: float = 30.0,
) -> None:
    """Run the objective's planning phase; its first planning call blocks."""
    try:
        if fail_renewals:
            from careeros.manager.manager import CareerManager

            CareerManager._renew_leases = lambda self, db, s, st: False  # type: ignore[method-assign]
        with _ctx(data_dir, limits) as c:
            m = _manager(c, None, adapter=_plan_hold_adapter(entered, release, timeout))
            s = m.run(objective_id, planning_only=True)
            _report(out, stop_reason=s.stop_reason, lease_lost=m._lease_lost.is_set())
    except BaseException:
        _report(out, error=traceback.format_exc())


#: Crash points of a planner process.
PLAN_CRASH_POINTS = ("in_call", "before_install", "mid_install")


def plan_and_die(data_dir: str, objective_id: int, point: str, out: Any) -> None:
    """Run planning and kill the process (``os._exit``) at ``point``:

    * ``in_call`` - the provider replied, before T2 (the call stays in flight);
    * ``before_install`` - T2 committed (call settled), before the plan is installed;
    * ``mid_install`` - inside the installing transaction, after some tasks were written.
    """
    try:
        from careeros.domain.repositories import OrchestrationRepository
        from careeros.llm.adapters.mock import MockAdapter
        from careeros.manager.manager import CareerManager

        def die() -> None:
            _report(out, died_at=point)
            time.sleep(0.2)
            os._exit(17)

        class Dying(MockAdapter):
            mode_label: ClassVar[str] = "mock"

            def call(self, profile: Any, request: Any) -> Any:
                reply = super().call(profile, request)
                if point == "in_call" and request.task_type == "manager.plan_novel":
                    die()
                return reply

        if point == "before_install":
            CareerManager._install_plan_in = lambda self, *a, **k: die()  # type: ignore[method-assign]
        if point == "mid_install":
            orig = OrchestrationRepository.add_task_in.__func__  # type: ignore[attr-defined]
            count = [0]

            def add_task_in(cls: Any, c: Any, task: Any) -> int:
                count[0] += 1
                if count[0] == 2:
                    die()  # the transaction is open: everything so far must roll back
                return int(orig(cls, c, task))

            OrchestrationRepository.add_task_in = classmethod(add_task_in)  # type: ignore[method-assign,assignment]
        with _ctx(data_dir) as c:
            _manager(c, None, adapter=Dying()).run(objective_id, planning_only=True)
            _report(out, status="finished-without-dying")
    except BaseException:
        _report(out, error=traceback.format_exc())


def plan_concurrently(data_dir: str, objective_id: int, barrier: Any, out: Any) -> None:
    """Several planner processes start the same objective's planning at once."""
    try:
        with _ctx(data_dir) as c:
            m = _manager(c, None)
            barrier.wait(30)
            s = m.run(objective_id, planning_only=True)
            _report(out, stop_reason=s.stop_reason, dispatches=s.dispatches)
    except BaseException:
        _report(out, error=traceback.format_exc())


def approve_and_die(data_dir: str, request_id: int, extra_usd: float, out: Any) -> None:
    """Approve a budget request and kill the process (``os._exit``) half-way through the
    deciding transaction - after the objective's budget line was raised, before COMMIT."""
    try:
        from careeros.cost.budget import BudgetManager

        orig = BudgetManager.raise_limit_in

        def raise_limit_in(c: Any, scope: str, ref: str, amount: float) -> None:
            orig(c, scope, ref, amount)
            _report(out, died_at="grant consumption")
            time.sleep(0.2)
            os._exit(17)

        BudgetManager.raise_limit_in = staticmethod(raise_limit_in)  # type: ignore[method-assign,assignment]
        with _ctx(data_dir) as c:
            _manager(c, None).approve(request_id, extra_usd=extra_usd)
            _report(out, status="finished-without-dying")
    except BaseException:
        _report(out, error=traceback.format_exc())
