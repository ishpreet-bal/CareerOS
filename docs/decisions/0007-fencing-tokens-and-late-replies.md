# ADR 0007: Fencing tokens, fenced call completion and late-reply reconciliation

- Status: accepted
- Date: 2026-11-24

## Context

The post-M3 audit (H2, M4, M5) showed that ownership was checked only at claim time, that
a delayed reply could overwrite state written by a newer owner and double-charge spend,
that a dead heartbeat could leave live workers without valid leases, and that several state
transitions spanned multiple transactions.

## Decision

- Each claim mints a monotonically increasing `owner_generation`; the fencing token
  `(task, generation, session, attempt)` is checked inside every task-owned write
  transaction, together with a live lease, and stale writes roll back.
- State transitions commit atomically with their decisions; attempts are never
  decremented — an interrupted, requeued or blocked attempt stays open and is resumed.
- Call completion (T2) is fenced; replies that lose that check are recorded once in
  `late_replies` and reconciled to actual cost in the same transaction.
- The heartbeat retries, never resurrects an expired lease, and halts the session's workers
  after `lease − safety_margin` without a successful renewal.
- Recovery takes over only expired leases or ended sessions, conditionally on owner and
  generation.
- Idempotency keys name the logical call; replay validates model, effort and request.

## Consequences

- Several processes can safely share one database on one host.
- Correctness does not depend on provider-side idempotency, but abandoned calls can still
  cost twice with a real provider (bounded by the reservation).
- More transactions take the write lock briefly (fence checks), which is negligible at the
  supported concurrency.
