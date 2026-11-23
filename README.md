# CareerOS

Local-first AI career intelligence and internship application agent.
Design: [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md).

**Current status: M0–M3.** Configuration, storage and data models; the provider-neutral
model layer with mock adapters; adaptive model/effort routing with a priority-based downgrade
policy; simulated cost and budget controls; the Career Manager (playbooks, planning, review,
retries, escalation, approvals); and bounded concurrent execution (default 3 workers) with
atomic budget reservations and crash recovery. All model calls are simulated.

## Safety defaults

- **Mock execution only.** The config loader rejects `mode: real`, any enabled non-mock
  model provider, and any non-zero real-money budget. Only offline adapters exist, and a
  safety gate re-checks every call.
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

Run an offline demonstration (synthetic data, mock models, simulated costs):

```bash
.venv/bin/careeros demo --scenario escalation
```

Scenarios: `standard`, `escalation`, `budget`, `interrupt`, `novel`. Related commands:
`careeros approvals`, `careeros approve <id> --usd <simulated amount>` (or
`--accept-downgrade`), `careeros resume <objective>` (add `--force-recover` only when no other
CareerOS process is running), `careeros report <objective>`.

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
config/            YAML templates (execution, storage, budgets, limits, eligibility, models,
                   routing) and demonstration playbooks
src/careeros/
  core/            config loader, SQLite wrapper, migration runner, migrations/
  storage/         data-dir guard, PersonalDataStore / BlobStore interfaces + local backends
  domain/          pydantic models and operational repositories
  llm/             provider-neutral model interface, offline adapters, safety gate, gateway
  routing/         complexity estimation, model + effort router, outcome history
  cost/            pricing, budgets, cost/quality report
  cache/           cache of accepted results
  workers/         worker contracts and generic workers
  manager/         Career Manager, plans/playbooks, decision table
  simulation.py    offline demo scenarios
  cli/             `careeros` command
  synthetic.py     fictional development dataset
tests/             unit and integration tests (synthetic data only)
docs/              architecture and decision records
```
