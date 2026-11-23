# ADR 0002: SQLite with numbered SQL migrations, no ORM

- Status: accepted
- Date: 2026-11-23

## Context

CareerOS needs a free, local, single-user database with durable task state.

## Decision

Use the standard-library `sqlite3` module in WAL mode with foreign keys on. Schema changes
are numbered files `src/careeros/core/migrations/NNNN_name.sql`, each applied atomically and
recorded in `schema_migrations`. Integrity rules that matter for safety are database
constraints, not only application code, for example:

- a non-`unknown` work-authorization status must be user-confirmed;
- an eligibility assessment with unknown inputs cannot be `eligible`;
- a non-real model call must be recorded as simulated cost.

## Consequences

No ORM dependency and a transparent schema. Repositories map rows to pydantic models by hand.
