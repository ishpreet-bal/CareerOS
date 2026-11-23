# ADR 0003: Provider-neutral routing by expected cost to an accepted result

- Status: accepted
- Date: 2026-11-23

## Context

CareerOS must pick a model and reasoning effort per subtask without hardcoding vendors or
model names, minimize cost without sacrificing required quality, and never spend money
during development.

## Decision

- Models are capability profiles in `models.yaml` (tier, capabilities, effort levels,
  pricing). Only the provider adapter reads provider-specific identifiers.
- The router minimizes **expected cost to an accepted result** (counting retries and the
  escalation path), subject to a priority-based quality floor and the remaining budget.
  Acceptance probabilities start from configured tier priors and are updated from reviewed
  outcomes per task type and complexity bucket.
- Critical work is never downgraded to fit a budget; the manager asks for approval.
- M2 ships only offline adapters (`mock`, `scripted`); every call is simulated and checked
  by a safety gate independent of configuration validation.

## Consequences

- Adding a provider later is one adapter class plus configuration.
- Priors are guesses until enough history exists; M4 adds calibration reports.
- Routing is deterministic (no exploration yet), which keeps runs reproducible.
