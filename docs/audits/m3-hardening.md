# M3 hardening and audit closure — finding status

Baseline `e67b011`. Updated at the end of each phase. "Fixed" means a regression test that
fails on the baseline passes after the change, and CI is green on the fixing commit.

| ID | Finding | Phase | Status |
|---|---|---|---|
| L3 | Unconditional objective/task status writes in `_finish`, `deny` (and `_settle_idle`, `_request_user`) | C1 | **Fixed** in `523c227` (evidence below) |
| N1 | Approve/deny validated outside their write transaction: concurrent decisions on one request both applied | C1 | **Fixed** in `523c227` |
| N2 | `end_session` overwrote a "presumed dead" end reason | C1 | **Fixed** in `523c227` |
| L10 | Claim did not re-check prerequisites or objective status | C1 | **Fixed** in `523c227`; plan membership is part of C5 (N5) |
| L5 | Requeued claims count toward per-run `max_attempts` | C2 | **Fixed** in C2 (evidence below) |
| N6 | Hot, unbounded claim/requeue loop for a lone task under budget contention (found during C2) | C2 | **Fixed** in C2 |
| L7 | Ctrl-C does not stop workers; no SIGTERM handling | C3 | Open |
| L8 | `synchronous=NORMAL` | C4 | Open |
| L9 | Planning has no heartbeat, ownership or resume (reproduced) | C5 | Open |
| N3 | Approving a planning clarification does not re-plan | C5 | Open |
| N5 | Partial plan installation (found during C1) | C5 | Open — requirements below |
| L12 | Approvals have no expiry or caps | C6 | Open |
| L11 | No factual-accuracy policy | C7 | Open |
| N4 | Cached accepted results skip review (factual policy version) | C7 | Open |
| L6 | Worker-failure/T2 failure paths | C8 | Verified consistent by inspection; failure-injection tests pending |
| T1 | Timing-sensitive heartbeat test | C8 | Open — brief below |

## C1 evidence (`523c227`, CI run #8 green: lint, type-check and full test job)

Regression tests in `tests/test_hardening_c1.py`; each failed on `e67b011` and passes on
`523c227`.

| Finding | Tests | Baseline behaviour observed |
|---|---|---|
| N1 | `test_concurrent_approvals_apply_exactly_once`, `test_concurrent_approve_and_deny_apply_exactly_one`, `test_approval_cannot_reopen_an_objective_finished_concurrently`, `test_request_decided_once_across_processes[approve x6]`, `[approve/deny x3]` | Both approvals succeeded (budget +2.0 for one +1.0 request, two grants); approve and deny both succeeded; an approval reopened a concurrently stopped objective; across 6 separate processes 3 decisions "won" (5/5 runs) |
| L3 | `test_stale_done_cannot_overwrite_a_user_stop`, `test_stale_idle_settlement_cannot_reopen_or_overwrite`, `test_waiting_for_the_user_is_only_set_on_an_active_objective`, `test_objective_level_request_on_a_finished_objective_is_withdrawn`, `test_finishing_twice_records_one_stop` | A stale "done"/"failed" overwrote the user's stop and recorded a second stop decision; a late planning question reopened a stopped objective |
| N2 | `test_ending_a_session_keeps_the_first_end_reason` | "presumed dead (recovered)" was overwritten |
| L10 | `test_claim_refuses_a_task_whose_prerequisite_has_not_succeeded`, `test_claim_refuses_tasks_of_an_objective_that_is_not_active[4 statuses]`, `test_claim_rules_match_the_dispatcher_on_every_playbook` | Tasks with unmet prerequisites, and tasks of stopped/done/failed/waiting objectives, were claimable |

Verification on `523c227`: full suite 549 passed (three local runs and CI run #8);
`ruff check`, `ruff format --check`, `mypy` (strict, `src`) clean; coverage 96% (fencing
100%, repositories 96%, manager 94%); concurrency, ownership, separate-process, migration
and C1 suites (91 tests) 5/5 runs.

## C2 evidence

Regression tests in `tests/test_hardening_c2.py`. Against the baseline source (`488a283`,
same code as `523c227`) 5 fail and 4 pass; all 9 pass after C2.

| Finding | Test | Baseline behaviour observed |
|---|---|---|
| L5 | `test_contention_requeues_do_not_use_up_the_run_quota` | With `max_attempts=1`, one contention requeue ended the run with "attempt limit for this invocation"; the task never executed |
| L5 | `test_reported_attempts_exclude_requeued_dispatches` | One execution after two requeues reported as `attempts_executed == 3` |
| L5 | `test_the_quota_still_limits_genuine_attempts` | Behaviour preserved; fails on the baseline only because `RunSummary.dispatches` does not exist there |
| N6 | `test_permanent_contention_backs_off_and_stops[None]`, `[1]` | Run never returned: 10,350 and 7,748 requeues (and as many generations) in 20 s; reproduced standalone at ~770 cycles/s |
| guards | `test_genuine_retries_count_against_the_quota`, `test_contention_does_not_consume_the_task_retry_limit`, `test_recovery_resumes_the_attempt_and_counts_it_once_per_run`, `test_lost_ownership_does_not_advance_the_attempt` | Pass on both: the per-task attempt counter was already correct; these pin it |

Verification: full suite 558 passed; `ruff check`, `ruff format --check`, `mypy` clean;
coverage 96% (manager 95%); concurrency, ownership, separate-process, manager, C1 and C2
suites (110 tests) 5/5 runs.

## N5 — requirements carried into C5

Finding: `_install_plan` writes the plan, each task, task modes and dependency rows in
separate transactions. A crash in between can leave a plan whose tasks have no dependency
rows (claimable too early) or a partial task set. Task rows are not versioned by plan.

C5 must address the underlying consistency problem, not only add claim checks:

1. Install a plan, its tasks (including mode/optional) and all dependency rows in **one**
   transaction, together with superseding the previous plan.
2. A partially installed plan can never become executable (atomicity, plus a plan state
   that only becomes current when installation commits).
3. An explicit task-to-plan-version association (tasks carried over by a replan are
   associated with the new version).
4. Claim-time validation that the task belongs to the objective's current plan, mirrored
   in the dispatcher.
5. Safe recovery after a crash during planning or plan installation: resume without
   duplicate logical planning calls, without two current plans, and without orphaned
   tasks; migration backfill for existing databases.

## T1 — brief carried into C8

`test_heartbeat_failure_halts_workers_before_their_lease_expires` (Phase B) failed once
under full-suite load during C1 (`'late' == 'ok'`): the blocked provider call replies at
~3 s and must reach T2 before the 4 s lease expires, a ~1 s margin. It passed 6/6 alone and
10/10 under CPU load on both the baseline and C1, and in every later full run (including
CI run #8). C8 will investigate making it deterministic by synchronising on events (e.g.
release the provider reply when the halt is observed, and drive lease expiry from the test
clock rather than wall time) **without removing the test or weakening its assertions**.
