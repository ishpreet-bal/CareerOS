# M3 hardening and audit closure — finding status

Baseline `e67b011`. Updated at the end of each phase. "Fixed" means a regression test that
fails on the baseline passes after the change.

| ID | Finding | Phase | Status |
|---|---|---|---|
| L3 | Unconditional objective/task status writes in `_finish`, `deny` (and `_settle_idle`, `_request_user`) | C1 | **Fixed** (C1) |
| N1 | Approve/deny validated outside their write transaction: concurrent decisions on one request both applied (reproduced: budget raised twice; 3 of 6 processes "won") | C1 | **Fixed** (C1) |
| N2 | `end_session` overwrote a "presumed dead" end reason | C1 | **Fixed** (C1) |
| L10 | Claim did not re-check prerequisites or objective status | C1 | **Fixed** (C1); plan membership deferred to C5 (N5) |
| L5 | Requeued claims count toward per-run `max_attempts` | C2 | Open |
| L7 | Ctrl-C does not stop workers; no SIGTERM handling | C3 | Open |
| L8 | `synchronous=NORMAL` | C4 | Open |
| L9 | Planning has no heartbeat, ownership or resume (reproduced) | C5 | Open |
| N3 | Approving a planning clarification does not re-plan | C5 | Open |
| N5 | `_install_plan` writes plan, tasks and dependencies in separate transactions: a crash in between can leave tasks without their dependency rows (claimable too early) | C5 | Open (found during C1) |
| L12 | Approvals have no expiry or caps | C6 | Open |
| L11 | No factual-accuracy policy | C7 | Open |
| N4 | Cached accepted results skip review (factual policy version) | C7 | Open |
| L6 | Worker-failure/T2 failure paths | C8 | Verified consistent by inspection; failure-injection tests pending |
| T1 | `test_heartbeat_failure_halts_workers_before_their_lease_expires` has a ~1 s timing margin; failed once under full-suite load during C1 (passed 6/6 alone and 10/10 under CPU load on both baseline and C1, and in all later full runs) | C8 | Open (test robustness) |
