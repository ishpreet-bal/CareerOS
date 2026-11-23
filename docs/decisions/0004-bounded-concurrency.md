# ADR 0004: Bounded thread-pool concurrency on SQLite with database-level guarantees

- Status: accepted
- Date: 2026-11-23

## Context

M3 must run independent tasks concurrently (default 3), never exceed budgets, never run a
task attempt or a model call twice, and recover correctly when several workers were active
at a crash — without permanently running workers, new infrastructure or paid services.

## Decision

- One dispatcher per `run` call and a bounded `ThreadPoolExecutor`; workers live only for that
  call. Each worker thread has its own SQLite connection (WAL, short `BEGIN IMMEDIATE`
  transactions).
- Correctness lives in the database, not in process memory: exclusive task claims via a
  conditional update; budget reservations and the in-flight call row in one transaction, call
  completion and settlement in another; unique idempotency keys for model requests; leases
  renewed by a heartbeat; recovery takes over only expired leases and dead sessions.
- Budget contention between concurrent tasks requeues the task; it never downgrades quality
  or asks the user.

## Consequences

- Multiple processes can safely work on the same objective.
- A call interrupted mid-flight may be repeated once; it is charged at its estimate.
- Replacing SQLite later requires a backend that offers the same guarantees (ADR 0005).
