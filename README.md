# CareerOS

Local-first AI career intelligence and internship application agent.
Design: [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md).

**Current status: M0 + M1** (project setup, configuration, database, data models).
The model router, orchestration engine and workers start at M2 and later.

## Safety defaults

- **Mock execution only.** The config loader rejects `mode: real`, any enabled non-mock
  model provider, and any non-zero real-money budget.
- **No personal data in git.** Data lives outside the repository
  (`~/.local/share/careeros/<dataset_kind>` or `$CAREEROS_DATA_DIR`). CareerOS refuses to
  create a data directory inside any git working tree. All committed fixtures are synthetic.
- **No eligibility assumptions.** Work authorization defaults to `unknown` and only you can
  set it.

## Quick start (cloud session or any machine with Python 3.12+)

```bash
uv venv .venv --python 3.12
```

```bash
uv pip install --python .venv/bin/python -e ".[dev]"
```

```bash
.venv/bin/careeros config validate
```

```bash
.venv/bin/careeros init --synthetic
```

```bash
.venv/bin/careeros status
```

Use a throwaway location for experiments:

```bash
CAREEROS_DATA_DIR=/tmp/careeros-demo .venv/bin/careeros init --synthetic
```

## Checks

```bash
make check
```

That runs `ruff check`, `ruff format --check`, `mypy --strict` and `pytest`. GitHub Actions
(`.github/workflows/ci.yml`) runs the same steps on pushes and pull requests.

## Layout

```
config/            YAML templates (execution, storage, budgets, limits, eligibility, models, routing)
src/careeros/
  core/            config loader, SQLite wrapper, migration runner, migrations/
  storage/         data-dir guard, PersonalDataStore / BlobStore interfaces + local backends
  domain/          pydantic models and operational repositories
  cli/             `careeros` command
  synthetic.py     fictional development dataset
tests/             unit and integration tests (synthetic data only)
docs/              architecture and decision records
```
