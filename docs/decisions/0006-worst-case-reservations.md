# ADR 0006: Reserve the worst case at a hard output cap

- Status: accepted
- Date: 2026-11-23

## Context

The post-M3 audit (M1) showed that reserving the *expected* cost lets settlement exceed a
budget whenever a reply is longer than expected; with real providers the overshoot would be
bounded only by the maximum output length.

## Decision

- Every call carries a hard output cap: expected output tokens × effort multiplier ×
  `routing.output_cap_factor` (default 1.5). Real adapters must pass it to the provider as
  the maximum output length.
- The gateway reserves the worst case: input estimate × `budgets.input_token_margin`
  (default 1.1) plus a full cap of output. The router uses the same figure to decide what is
  affordable; the expected cost still drives the choice between options.
- Settlement charges the actual cost. If it exceeds the reservation, the excess is recorded
  (`model_calls.overshoot_usd`, error note, report totals) and the overrun budget line
  refuses further calls; nothing is silently absorbed.
- Calls abandoned by a dead session are charged their full reservation.

## Consequences

- Budgets cannot be exceeded while providers honour the cap and input estimates stay within
  their margin.
- Reservations are larger than expected costs, so tight budgets ask for approval earlier and
  fewer calls fit concurrently under one budget.
- Real providers additionally need tokenizer-accurate input counts (or a larger margin) and
  verification that the cap is enforced provider-side.
