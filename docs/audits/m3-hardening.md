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
| L7 | Ctrl-C does not stop workers; no SIGTERM handling | C3 | **Fixed** in C3 (evidence below) |
| L8 | `synchronous=NORMAL` | C4 | **Fixed** in C4 (evidence below) |
| L9 | Planning has no heartbeat, ownership or resume (reproduced) | C5 | **Fixed** in C5 (evidence below) |
| N3 | Approving a planning clarification does not re-plan | C5 | **Fixed** in C5 |
| N5 | Partial plan installation (found during C1) | C5 | **Fixed** in C5 (all five requirements below) |
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
| L5 (race, follow-up) | `test_quota_cannot_be_exceeded_when_another_process_closes_the_attempt` (separate processes) | On `760351b`: invocation A (quota 1) decided its re-claim was the counted attempt 1; process B then resumed and closed attempt 1; A's claim started attempt 2 and executed it (`attempts_executed == 2`) |
| L5 (race, follow-up) | `test_claim_restricted_to_an_attempt_resumes_it_but_never_starts_another` | Fails on `760351b` only because `claim(..., only_attempt=)` does not exist there |
| guards | `test_genuine_retries_count_against_the_quota`, `test_contention_does_not_consume_the_task_retry_limit`, `test_recovery_resumes_the_attempt_and_counts_it_once_per_run`, `test_lost_ownership_does_not_advance_the_attempt` | Pass on both: the per-task attempt counter was already correct; these pin it |

Verification (`760351b`): full suite 558 passed; `ruff check`, `ruff format --check`,
`mypy` clean; coverage 96% (manager 95%); concurrency-related suites (110 tests) 5/5 runs;
CI run #10 green.

Follow-up (quota race): the quota restriction moved into the claiming statement
(`only_attempt`). Full suite 560 passed; lint, format, types clean; coverage 96% (fencing
100%, manager 95%); the separate-process race test 10/10; concurrency-related suites
(123 tests) 5/5 runs.

## C3 evidence

Baseline (`5863660`) behaviour, reproduced by `tests/test_hardening_c3_baseline.py` (SIGINT
to a separate process running an objective as `resume` does, two dependent steps, the first
inside its provider call): the process raised `KeyboardInterrupt`; the interrupted worker
nevertheless carried on to an outcome (here a retry decision that closed attempt 1); the
session was never ended (`ended_at` NULL), so remaining work stayed leased until expiry.
That test fails on the baseline and passes after C3 (task handed back as `ready` with
attempt 1 still open, the dependent step not dispatched, session ended "cancelled: SIGINT").

`tests/test_hardening_c3.py` (cannot be collected on the baseline: it imports the new
shutdown module):

| Requirement | Tests |
|---|---|
| SIGINT during idle dispatch | `test_cancel_while_idle_in_contention_backoff`, `test_sigint_during_idle_dispatch_in_a_separate_process` |
| SIGINT / SIGTERM with active workers | `test_signal_with_active_workers_in_a_separate_process[sigint_1]`, `[sigterm_2]` |
| Shutdown during an in-flight model call; several workers concurrently | `test_cancel_with_workers_inside_provider_calls_stops_at_safe_points[one_worker]`, `[three_workers]` |
| Heartbeat active during grace; no takeover | `test_heartbeat_keeps_leases_alive_during_the_grace_period` |
| Grace expiry; late reply never overwrites | `test_grace_expiry_releases_tasks_and_late_replies_never_overwrite`, `test_late_reply_after_grace_expiry_and_recovery_across_processes` |
| Second signal / forced exit; recovery | `test_second_signal_forces_exit_and_recovery_is_deterministic` |
| Repeated requests idempotent | `test_repeated_cancel_requests_are_idempotent`, `test_signal_handler_cancels_once_then_forces_exit` |
| No ownership after takeover | `test_cancel_after_takeover_does_not_touch_the_new_owner` |
| Session not ended while ownership could not be released | `test_failed_hand_back_leaves_the_session_open_for_recovery` |
| Cancel before dispatch | `test_cancel_before_dispatch_claims_nothing` |

Verification: full suite 576 passed; `ruff check`, `ruff format --check`, `mypy` clean;
coverage 96% (`orchestration/shutdown.py` 100%, manager 94%); C3 tests 8/8 runs normally
and 8/8 under CPU load (after fixing the forced-exit test to wait for the first signal to be
handled: pending signals coalesce); concurrency-related suites (128 tests) 3/3 runs.

## C4 evidence

`tests/test_hardening_c4_baseline.py` (raw `PRAGMA` reads; runs on any version):
`test_all_connection_paths_use_wal_and_synchronous_full` fails on the baseline
(`1223d03`: `('wal', 1)`, i.e. synchronous=NORMAL, on the main connection - and every other
path) and passes after C4 for the main, worker, heartbeat and direct-open connections.
`test_process_crash_around_reservation_and_settlement[after_reserve | after_reply |
inside_settlement | after_settlement]` pass on **both**: process-crash atomicity already held
and still holds. These are process-kill tests; they are not evidence of power-loss
durability (see ARCHITECTURE section 19.4).

`tests/test_hardening_c4.py` (imports the new backup module, so it cannot be collected on
the baseline): durability of every connection path including a recovering separate process;
refusal to open with weaker settings; backup while in use (lease, reservation, in-flight
call and a concurrent writer) is a consistent snapshot and does not disturb the run;
overwrite protection; a backup failing verification leaves nothing; verification of page
corruption, truncation, non-SQLite files, `integrity_check` row findings, foreign-key
violations, non-CareerOS and missing files; restore from a valid backup (with a verified
safety copy); refusal of corrupt and wrong-dataset backups with the database untouched;
refusal while a session is running; restore of a mid-run snapshot followed by recovery
keeps ownership, budget and late-reply guarantees; CLI backup/verify/restore and refusal of
a destination inside the git repository.

Verification: full suite 596 passed; `ruff check`, `ruff format --check`, `mypy` clean;
coverage 96% (`core/backup.py` 93%: uncovered lines are defensive error branches).

## C5 evidence

`tests/test_hardening_c5_baseline.py` (public behaviour only; runs on any version) - all
four fail on the baseline (`f1d954d`) and pass after C5:

| Finding | Test | Baseline behaviour observed |
|---|---|---|
| L9 | `test_crash_during_planning_is_recovered_not_failed` | resume marked the objective failed ("no runnable work") |
| L9 | `test_slow_planning_is_not_mistaken_for_a_dead_session` | another session abandoned (and charged) the planning call |
| N3 | `test_answering_a_clarification_leads_to_a_plan` | after the clarification was approved the objective failed with no plan |
| N5 | `test_a_step_never_runs_before_its_prerequisite_when_dependency_rows_are_missing` | step b started while a was still unfinished (`retry_scheduled`) |

`tests/test_hardening_c5.py` (uses the new APIs): planning as an owned task; planning
longer than a lease kept alive by the heartbeat; failed heartbeat halts planning and
recovery replays the call; stale planner cannot install; graceful shutdown during planning;
crash inside installation leaves the previous valid plan; a failed playbook submission
leaves nothing; missing dependency rows and missing task rows; tasks of an outdated
version; the planning task cannot run after installation; one current plan enforced by the
database; clarification answer resumes planning; repeated clarification and interrupted
replanning; separate processes: planner killed in the call, after settlement and inside
installation, then recovered without duplicate billing; heartbeat failure in a planner
process; four concurrent planner processes install one plan; migration backfill (one
current plan, re-association, dependency rows, new columns, idempotent) and legacy
objectives without a plan, including an answered legacy clarification.

N5 requirements: (1) atomic installation - `_install_plan_in`, one transaction, with the
objective for playbooks; (2) never executable when partial - atomicity, plus claim-time
dependency completeness for older data; (3) explicit task-to-plan-version association -
`tasks.plan_id` kept current by installs and prunes, migrated; (4) claim-time membership of
the current version - `CLAIMABLE_SQL`, shared with the dispatcher; (5) crash recovery -
installs roll back entirely; planning resumes by key; `_ensure_plan` completes older
partial installs; migration 0006 backfills.

Verification: full suite 621 passed; `ruff check`, `ruff format --check`, `mypy` clean;
coverage 96% (fencing 100%, repositories 96%, manager 94%).

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
