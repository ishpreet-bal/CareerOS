"""Amendment A7: objectives, tasks, results and checkpoints survive the end of a session."""

from careeros.context import open_context
from careeros.core.config import AppConfig
from careeros.domain.models import (
    Checkpoint,
    DependencyKind,
    Evidence,
    EvidenceSource,
    Objective,
    ObjectiveStatus,
    Plan,
    Priority,
    ResultStatus,
    Task,
    TaskStatus,
    WorkerResult,
)


def _build_objective(cfg: AppConfig) -> tuple[int, list[int]]:
    with open_context(cfg) as ctx:
        repo = ctx.orchestration
        session = repo.start_session()
        obj = repo.create_objective(
            Objective(
                kind="prepare_application",
                title="Synthetic objective",
                definition_of_done=["resume.review.verdict == accept"],
                priority=Priority.HIGH,
                budget_usd=0.4,
            )
        )
        assert obj.id is not None
        plan = repo.create_plan(Plan(objective_id=obj.id, version=1, source="playbook:test"))
        assert plan.id is not None
        ids = []
        for i, (ttype, status) in enumerate(
            [
                ("job.analyze", TaskStatus.SUCCEEDED),
                ("resume.tailor", TaskStatus.RUNNING),
                ("quality.review", TaskStatus.PENDING),
            ]
        ):
            t = repo.add_task(
                Task(
                    objective_id=obj.id,
                    plan_id=plan.id,
                    worker="w",
                    task_type=ttype,
                    title=f"step {i}",
                    idempotency_key=f"obj{obj.id}-{ttype}",
                    important=ttype != "job.analyze",
                )
            )
            assert t.id is not None
            repo.set_task_status(t.id, status)
            ids.append(t.id)
        repo.add_dependency(ids[1], ids[0])
        repo.add_dependency(ids[2], ids[1], DependencyKind.HARD)
        repo.record_result(
            ids[0],
            1,
            WorkerResult(
                status=ResultStatus.SUCCEEDED,
                output={"keywords": ["python"]},
                evidence=[
                    Evidence(
                        claim="needs python",
                        source_type=EvidenceSource.JOB_POSTING,
                        source_ref="job:1",
                    )
                ],
                confidence=0.9,
            ),
            effective_confidence=0.85,
        )
        repo.save_checkpoint(
            Checkpoint(task_id=ids[1], attempt=1, step="select_facts", state={"fact_ids": [1, 2]})
        )
        repo.record_decision("plan", objective_id=obj.id, rationale="playbook match")
        # Simulate abrupt termination: no end_session call.
        assert session
        return obj.id, ids


def test_state_survives_reopen(cfg: AppConfig) -> None:
    obj_id, ids = _build_objective(cfg)

    with open_context(cfg) as ctx:  # a brand-new "session"
        repo = ctx.orchestration
        obj = repo.get_objective(obj_id)
        assert obj.status is ObjectiveStatus.ACTIVE and obj.priority is Priority.HIGH
        assert repo.current_plan(obj_id) is not None

        outstanding = repo.outstanding_tasks(obj_id)
        assert [t.id for t in outstanding] == ids[1:]
        assert {t.status for t in outstanding} == {TaskStatus.RUNNING, TaskStatus.PENDING}

        results = repo.list_results(ids[0])
        assert len(results) == 1
        assert results[0].result.output == {"keywords": ["python"]}
        assert results[0].result.evidence[0].source_type is EvidenceSource.JOB_POSTING
        assert results[0].effective_confidence == 0.85

        cp = repo.latest_checkpoint(ids[1])
        assert cp is not None and cp.step == "select_facts" and cp.state == {"fact_ids": [1, 2]}
        assert repo.dependencies(ids[2]) == [(ids[1], DependencyKind.HARD)]

        done = repo.get_task(ids[0])
        assert done.status is TaskStatus.SUCCEEDED


def test_idempotent_task_creation(cfg: AppConfig) -> None:
    obj_id, ids = _build_objective(cfg)
    with open_context(cfg) as ctx:
        repo = ctx.orchestration
        plan = repo.current_plan(obj_id)
        assert plan is not None and plan.id is not None
        again = repo.add_task(
            Task(
                objective_id=obj_id,
                plan_id=plan.id,
                worker="w",
                task_type="job.analyze",
                idempotency_key=f"obj{obj_id}-job.analyze",
            )
        )
        assert again.id == ids[0]
        assert len(repo.list_tasks(obj_id)) == 3


def test_replan_supersedes_previous_version(cfg: AppConfig) -> None:
    obj_id, _ = _build_objective(cfg)
    with open_context(cfg) as ctx:
        repo = ctx.orchestration
        v2 = repo.create_plan(Plan(objective_id=obj_id, version=1, source="playbook:revised"))
        assert v2.version == 2
        current = repo.current_plan(obj_id)
        assert current is not None and current.source == "playbook:revised"


def test_finished_objective_has_no_outstanding_work(cfg: AppConfig) -> None:
    obj_id, _ = _build_objective(cfg)
    with open_context(cfg) as ctx:
        ctx.orchestration.set_objective_status(obj_id, ObjectiveStatus.DONE, "definition_of_done")
        assert ctx.orchestration.outstanding_tasks() == []
        assert ctx.orchestration.get_objective(obj_id).stop_reason == "definition_of_done"


def test_checkpoint_update_overwrites_same_step(cfg: AppConfig) -> None:
    _, ids = _build_objective(cfg)
    with open_context(cfg) as ctx:
        cp = ctx.orchestration.save_checkpoint(
            Checkpoint(task_id=ids[1], attempt=1, step="select_facts", state={"fact_ids": [3]})
        )
        assert cp.state == {"fact_ids": [3]}
