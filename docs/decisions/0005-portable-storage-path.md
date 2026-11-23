# ADR 0005: Stay offline-first; keep storage portable

- Status: accepted
- Date: 2026-11-23

## Context

The owner decided (A2, A11) that no external database, cloud storage, deployment or paid
service may be introduced, that only synthetic data is used for now, and that durable storage
must be possible later without rewriting orchestration.

## Decision

- Keep SQLite in a `data_dir` outside the repository as the only store.
- All orchestration state goes through `core.db.Database` and its repositories; personal data
  through `PersonalDataStore` / `BlobStore`. These are the only seams a future backend touches.
- Document the contract a backend must satisfy (serializable short transactions, atomic
  conditional updates, unique constraints, read-your-writes) and the options in order of cost
  (architecture §9.4). Select none.

## Consequences

- Data in a cloud session is temporary; acceptable while data is synthetic.
- A durable option can be added later without changing the manager, router or gateway.
