# ADR 0001: Personal data lives outside git, behind a storage abstraction

- Status: accepted
- Date: 2026-11-23

## Context

CareerOS handles resumes, contact details, application histories and eligibility
information. The project owner decided (architecture amendment A2) that none of this may be
committed to GitHub, even in a private repository. Development happens in ephemeral cloud
containers, and no paid storage provider may be selected.

## Decision

- Personal tables are listed in `careeros.storage.classification` and accessed only through
  the `PersonalDataStore` and `BlobStore` protocols.
- The default backends are `SQLitePersonalStore` and `LocalBlobStore` in a data directory
  outside the repository. `resolve_data_dir` refuses any directory inside a git working tree.
- Each database records its `dataset_kind` (`synthetic` or `personal`) and refuses to be
  opened as the other kind.
- `.gitignore` excludes database files, `data/`, `personal/`, `snapshots/` and `.env*`.
  A test fails if tracked files match those patterns or contain non-example email addresses.

## Consequences

- Task state and personal data survive the end of a process or session, but not the
  reclamation of a cloud container. Durable storage is an open decision.
- A future encrypted or remote backend only has to implement the two protocols.
