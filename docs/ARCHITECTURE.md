# CareerOS — Architecture & Implementation Plan (v3)

Status: **APPROVED (with amendments in §0).** M0–M3 implemented, plus Phase A audit fixes (§17); M4+ await approval.
Nothing is deployed, purchased or configured against a paid service.

---

## 0. Approved amendments (2026-11-23)

These decisions override anything below that conflicts with them.

| # | Decision | Where it is applied |
|---|---|---|
| A1 | Architecture approved. Keep the provider-neutral router, dynamic decomposition, quality-adjusted cost, bounded escalation, persistent task state, mock adapters and strict spending controls. | Whole document |
| A2 | **No personal data in GitHub** — no real resumes, profiles, contact details, application histories, credentials or personal database snapshots, even in a private repo. M0/M1 use **synthetic data only**. Personal data sits behind a storage abstraction so secure persistent storage can be added later. No paid storage provider is selected. | §9, §10.0, §11 |
| A3 | **No eligibility assumptions.** Citizenship, permanent residency and work authorization default to `unknown`. Rules are configurable. Unknown inputs produce `needs_user_confirmation`, not a guess. | §4, §10.1, §10.2 |
| A4 | Free public job-board APIs (Greenhouse, Lever, Ashby) are approved for M6. **Before implementation, verify the current endpoints, rate limits and terms** and record the findings in an ADR. No LinkedIn or Indeed scraping. | §13 (M6) |
| A5 | Manual AI bridge is an **optional future capability**. It is never required for normal operation. | §6.1 |
| A6 | GitHub Actions CI runs within the free allowance. No paid CI. Session-based testing stays available. | §11, §13 (M0) |
| A7 | **Resumable objectives.** The manager does not run continuously, but task execution, results, checkpoints and outstanding work persist and survive the end of any single execution session. | §7.3, §9.3 |
| A8 | **M2 scope (2026-11-23):** M2 delivers the provider-neutral model layer, mock adapters, adaptive routing, cost and quality controls, *and* the Career Manager coordinator (playbooks, model-assisted planning, decomposition, review, decision table, approvals, recovery). Tasks run one at a time in M2; concurrent execution moves to M3. | §13, §15 |
| A9 | **Priority and downgrade policy (2026-11-23):** critical and high-priority tasks are never downgraded automatically; if the floor cannot be met within budget the user is asked. Normal and low may be downgraded automatically within configured limits (`degrade_limits`), never below the task type's minimum quality (`min_quality`, e.g. factual deliverables) or `absolute_min_quality`. Every downgrade, automatic or user-approved, is recorded. Low priority optimizes for cost within its floor. | §5.4, §16.1 |
| A10 | **M3 bounded concurrency (2026-11-23):** dependency-aware concurrent execution with a configurable worker limit (default 3), atomic budget reservations, exclusive task claims, idempotent model calls, lease-based recovery with multiple active workers. All model calls remain simulated. | §7, §16 |
| A11 | **Storage stays offline-first (2026-11-23):** no external database, cloud storage, deployment or paid service. Keep a portable storage interface and document the path to durable storage. Synthetic data only. | §9.4, ADR 0005 |
| A12 | **Phase A audit fixes (2026-11-23):** the A9 policy is enforced in every routing branch, including when learned history puts every option below the floor; no choice is ever below the task type's hard minimum. Reservations use the worst case at a hard output cap sent to the provider; any overshoot is recorded. Replans append to the skipped list atomically. Finishing an objective cancels unneeded tasks and withdraws pending requests in one transaction; obsolete approvals are rejected. Every downgrade (task, review, planning) is a `degrade` decision; approvals apply only to the routing step that asked. Lowered config budget limits apply immediately. | §5.4, §6.3, §17, ADR 0006 |

CareerOS is a standalone, local-first career intelligence and internship application agent.
It shares no code, database, agents, infrastructure or modules with any other project. Ideas
about model routing, decomposition, orchestration, cost control and evaluation are applied
here from first principles; nothing is imported.

---

## 1. Goals and hard constraints

| Goal | How it is served |
|---|---|
| Find AI / ML / SWE / embedded internships in Canada, US, remote-international | Job Discovery worker over free public sources |
| Research companies and job descriptions | Employer Research and job-analysis workers, with cached research |
| Deep understanding of resume, portfolio, projects | Structured **profile fact base** (every claim is an atomic, citable fact) |
| Tailored resumes and cover letters | Tailoring and Cover Letter workers, limited to profile facts |
| Writing quality and personal style consistency | Deterministic stylometrics plus an optional model judge |
| Track applications and outcomes | Application tracker with an event history |

**Hard constraints (enforced in code, not only in policy):**

1. No paid API keys, subscriptions, hosting or purchases. Nothing that costs money is turned on without explicit written approval.
2. Real model execution is **off by default** and can only be turned on by passing several separate locks (§6.6).
3. Mock adapters and simulated costs let the whole system be built and tested for $0.
4. The Claude Pro / Claude Code subscription is used only for *developing* CareerOS. It is not treated as API access for CareerOS.
5. Provider-neutral: no vendor or model name is hardcoded outside configuration.
6. CareerOS never submits an application by itself. It prepares a package, and a person submits it.
7. No deployment. Everything runs in a cloud dev session (and could later run on any machine with Python).

---

## 2. System overview

```
                         ┌───────────────────────────────────────────┐
   CLI / reports  ─────▶ │              CAREER MANAGER               │
   (objectives)          │  understand → decompose → plan → route →  │
                         │  delegate → review → decide (next/retry/  │
                         │  escalate/replan/stop/ask-human)          │
                         └──────┬───────────────┬───────────────┬────┘
                                │ plans (DAG)   │ routing       │ reviews
                                ▼               ▼               ▼
                   ┌──────────────────┐ ┌──────────────┐ ┌──────────────┐
                   │  ORCHESTRATION   │ │ MODEL ROUTER │ │ QUALITY      │
                   │  task graph,     │ │ complexity,  │ │ REVIEW       │
                   │  scheduler,      │ │ model+effort │ │ deterministic│
                   │  leases/recovery │ │ selection,   │ │ checks +     │
                   │  parallel exec   │ │ escalation   │ │ model judge  │
                   └────────┬─────────┘ └──────┬───────┘ └──────────────┘
                            │ runs              │ calls via
                            ▼                   ▼
   ┌────────────────────────────────┐  ┌─────────────────────────────────┐
   │      SPECIALIST WORKERS        │  │       MODEL GATEWAY             │
   │ (stateless workflows)          │  │ safety gate → budget check →    │
   │ Discovery · Eligibility ·      │─▶│ cache → adapter → ledger        │
   │ Research · Matching · Resume · │  │ adapters: mock | scripted |     │
   │ Cover Letter · App Prep ·      │  │ manual-bridge | (disabled real) │
   │ Quality Review                 │  └─────────────────────────────────┘
   └───────────────┬────────────────┘
                   ▼
   ┌────────────────────────────────────────────────────────────────────┐
   │  SQLite (WAL): profile facts · jobs · companies · research ·       │
   │  applications · objectives/plans/tasks/results · model_calls ·     │
   │  budgets · cache · routing_stats · decisions · approvals           │
   └────────────────────────────────────────────────────────────────────┘
```

Main principle: **deterministic first, cache second, cheapest model that is good enough third,
escalate only on evidence.** A model is used only when a step really needs judgment or
language generation.

---

## 3. Career Manager

The manager is a component, not a long-running agent process. Each time it is invoked
(`careeros run <objective>` or when the scheduler resumes), it moves one or more objectives
forward and saves all of its state.

### 3.1 Responsibilities

| Function | Default implementation | When a model is used |
|---|---|---|
| Understand objective | Match the objective against a **playbook** catalog (YAML) | Free-form or new objectives: parsed by a Tier 3 model, and by Tier 1 only if still ambiguous |
| Decompose / plan | Fill in a playbook template into a task DAG | No playbook fits, or evidence makes the template invalid |
| Decide deterministic vs AI per subtask | Every step declares `mode: deterministic | model | hybrid`; hybrid steps try code first and fall back to a model only if code confidence is below a threshold | — |
| Select model + effort | Router (§5) | Router is code; it never calls a model to choose a model |
| Delegate | Enqueue tasks; the scheduler runs READY tasks in parallel | — |
| Review results | Deterministic validators always run; model review for deliverables marked `important` | Quality Review worker |
| Decide next action | Rule-based decision table (§3.3) | Tier 1 only for real strategy calls (e.g., "with 40 matches and 6 hours, which 10 should we prioritize?") |
| Revise plan | Replanning triggers (§3.4) produce a new plan version; completed work is kept | Model only if the change goes beyond a template-level edit |

The manager's own reasoning goes through the same router and budget. "Manager" does not
mean "always the most expensive model." Most manager turns cost $0 because they are rule
evaluations.

### 3.2 Objectives and definition of done

Every objective and every task has machine-checkable acceptance criteria:

```yaml
objective: prepare_application
inputs: {job_id: 1234}
priority: high            # low | normal | high | critical
budget: {usd: 0.40, tokens: 120000}   # simulated during development
definition_of_done:
  - eligibility.verdict in [eligible, uncertain_accepted_by_user]
  - resume.review.verdict == accept and resume.review.score >= 0.80
  - resume.truthfulness.unsupported_claims == 0
  - cover_letter.review.verdict == accept and style.distance <= 0.25
  - package.files_present == [resume.pdf|md, cover_letter.md, answers.md]
stop_when: definition_of_done_met | budget_exhausted | awaiting_human
```

The manager checks `definition_of_done` after each task finishes and **stops immediately**
when it is met. Optional "nice to have" tasks still pending at that point are cancelled.

### 3.3 Decision table (after each task result)

| Condition | Decision |
|---|---|
| Validators pass, confidence ≥ threshold, review accepts | `accept` → unblock dependents |
| Schema or format failure | `retry_same` with a repair prompt (max 1), then escalate |
| Transient error (timeout, rate limit) | `retry_same` with backoff (bounded) |
| Low confidence, or review says `revise` | `retry_with_feedback` at the same tier, or `escalate_effort` |
| Second failure, or review says `escalate` | `escalate_tier` |
| Max attempts or task budget reached | `await_human` (never loop without limit) |
| Result invalidates the plan (e.g., ineligible) | `replan` |
| Definition of done met | `stop` |

Every decision is written to `manager_decisions` with its inputs and rationale, which gives
an audit trail and training data for routing.

### 3.4 Replanning triggers

- Eligibility comes back `ineligible` → prune all downstream tasks for that job.
- Research finds that the posting is closed or a duplicate → cancel the branch.
- Matching score is below the user's threshold → skip tailoring and record why.
- A worker raises `unresolved_questions` that block the next step → add a `clarify` task (deterministic lookup first, then ask the user).
- Budget pressure → lower optional depth (fewer research sources) before lowering quality on critical deliverables.
- Repeated failures of one worker type → mark the step `await_human` instead of spending more.

Plans are versioned (`plans.version`), and a replan never discards completed, accepted results.

---

## 4. Specialist workers

Workers are **stateless workflow classes**, not running agents. Each is a sequence of typed
steps:

```python
class Step:  # conceptual
    name: str
    mode: Literal["deterministic", "model", "hybrid"]
    task_type: str  # key into routing policy, e.g. "job.classify_role"
    output_schema: type[BaseModel]
    min_tier: int | None
    cacheable: bool
```

| Worker | Mostly deterministic | Model steps (default tier) |
|---|---|---|
| **Job Discovery** | Fetch public ATS JSON/RSS, normalize, dedupe (hash + fuzzy title/company/location), keyword and location filters | Classifying ambiguous roles ("is this ML?") → T3 |
| **Eligibility Assessment** | Configurable rules: grad date window, degree level, work-authorization rules, location/remote, term/season. Any rule whose candidate input is `unknown` yields `needs_user_confirmation` (A3) | Parsing unusual requirement wording → T3; truly ambiguous cases → T2. A model never infers the candidate's status |
| **Employer Research** | Cache lookup, freshness check, source fetch | Summarizing and synthesizing facts with citations → T2; bounded depth |
| **Candidate Matching** | Skill/keyword overlap, embedding-free TF-IDF similarity, rule scores | Rationale for top candidates only → T2/T3 |
| **Resume Tailoring** | Select facts by relevance, enforce length and format, render | Bullet rewording limited to selected facts → T2 |
| **Cover Letter Writing** | Assemble outline from facts and research | Drafting → T2; final polish of high-priority letters → T1 |
| **Application Preparation** | Package files, checklist, deadline, application-question bank lookup | Drafting answers to free-text questions → T2 |
| **Quality Review** | Truthfulness check (every claim maps to a fact ID), stylometrics, length, keyword coverage, spelling | Rubric judge → T2; final strategic review for `critical` → T1 |

### 4.1 Required structured output (every worker, every attempt)

```python
class Evidence(BaseModel):
    claim: str
    source_type: Literal["profile_fact", "job_posting", "web", "research_cache", "user"]
    source_ref: str  # fact id, URL, job id ...
    quote: str | None


class WorkerResult(BaseModel):
    schema_version: str
    status: Literal["succeeded", "partial", "failed", "needs_input"]
    output: dict  # validated against the step's output_schema
    evidence: list[Evidence]
    assumptions: list[str]
    confidence: float  # 0..1, self-reported
    unresolved_questions: list[str]
    recommended_next_steps: list[str]
    metrics: dict  # tokens, cost, latency, cache hits (filled by the gateway)
```

A model's self-reported confidence is **not trusted alone**. Effective confidence is
`min(self_reported, validator_score, reviewer_score)`, and calibration is tracked per
(task_type, model) in `routing_stats`.

### 4.2 Truthfulness invariant

Tailored resumes and cover letters may only make claims backed by `profile_facts`. The
Quality Review worker deterministically rejects any output with unsupported claims. This is
the most important quality gate in the system.

---

## 5. Adaptive model routing (provider-neutral)

### 5.1 Model registry (config, not code)

`config/models.yaml` describes **capability profiles**. Vendors and models appear only here.

```yaml
models:
  - id: mock-large            # used in development
    provider: mock
    tier: 1
    capabilities: [reasoning, long_context, json]
    context_window: 200000
    effort_levels: [low, medium, high, max]
    pricing: {input_per_mtok: 15.0, output_per_mtok: 75.0, simulated: true}
    enabled: true
  - id: mock-medium
    provider: mock
    tier: 2
    effort_levels: [none, low, medium, high]
    pricing: {input_per_mtok: 3.0, output_per_mtok: 15.0, simulated: true}
    enabled: true
  - id: mock-small
    provider: mock
    tier: 3
    effort_levels: [none]
    pricing: {input_per_mtok: 0.25, output_per_mtok: 1.25, simulated: true}
    enabled: true
  # Real providers are listed only as disabled templates. Prices must be
  # verified by the user against the provider's current price page before enabling.
  # - id: <provider-model-id>
  #   provider: anthropic | openai_compatible | ollama | ...
  #   tier: 2
  #   pricing: {input_per_mtok: null, output_per_mtok: null, verified_at: null}
  #   enabled: false
```

Simulated prices are clearly labeled placeholders for testing cost logic. They are not
claims about any vendor's actual prices.

### 5.2 Abstract reasoning effort

`effort ∈ {none, low, medium, high, max}`. Each adapter maps this to its provider's controls
(thinking budget, reasoning-effort parameter, sampling settings, or nothing). Routing chooses a
**(model, effort)** pair, not just a model.

### 5.3 Routing inputs

| Signal | Source |
|---|---|
| Task type and base complexity | `config/routing.yaml` |
| Instance complexity | Deterministic features: input length, number of sources, ambiguity flags, conflicting evidence, prior failures (optionally a T3 classifier) |
| Priority → quality floor | `low: 0.6, normal: 0.75, high: 0.85, critical: 0.92` (configurable) |
| Expected quality P(accept \| task_type, complexity bucket, model, effort) | `routing_stats` (Beta prior from config, updated from review outcomes) |
| Estimated cost | Token estimate × pricing for the chosen effort |
| Remaining budget | Task, objective and global budgets |
| Confidence of prior attempt | Last `WorkerResult` and review |
| Latency (tie-break) | `routing_stats` |

### 5.4 Selection rule — quality-adjusted cost

For each enabled candidate `(m, e)` that meets capability and context requirements:

```
p      = P(accept | task_type, complexity, m, e)
c      = estimated_cost(m, e, est_tokens)
E_cost = c + (1 - p) * C_fallback        # C_fallback = cost of the next escalation step
feasible if  p >= quality_floor(priority, complexity)  and  c <= remaining_task_budget
choose argmin E_cost among feasible; tie-break by latency, then lower tier
```

This picks the option with the lowest **expected cost to reach an acceptable result**, not the
cheapest per call. A cheap model that fails 60% of the time often loses to a medium one.

If no candidate is feasible (amendment A9):

| Priority | Floor cannot be met within budget |
|---|---|
| `critical`, `high` | Never downgraded automatically → `await_human` with the cost needed and, if one exists, the best affordable lower-quality option. The user may add budget **or** explicitly accept that downgrade. |
| `normal` | Automatic downgrade allowed to at most `degrade_limits.normal` (default 0.10) below the floor; otherwise ask. |
| `low` | Optimizes for cost within its floor; automatic downgrade allowed to at most `degrade_limits.low` (default 0.20) below; otherwise ask. |
| every task | Never below `max(absolute_min_quality, task_type.min_quality)`; factual deliverables carry their own minimum (e.g. `resume.tailor: 0.80`). |

Every downgrade — automatic or user-approved, for a task, its review or model-assisted
planning — is a `degrade` decision in the history with the floor, the chosen option, the
routing step (`purpose`) and whether the user approved it; approvals are also rows in
`approvals` (`quality_downgrade`) and apply only to the routing step that requested them
(a review approval never lowers the task's own floor). Config validation rejects removing
`critical` or `high` from `no_degrade_priorities`.

The same rules hold when **no option can meet the floor at all** (e.g. after learned history
lowered every estimate): the best achievable option is treated as a downgrade — critical/high
ask, normal/low only within their limit — and when not even the best option reaches the hard
minimum, nothing runs and the user is asked (`no_candidate`). Learned history only changes
the estimates; it can never bypass the policy (Phase A, audit H1).

### 5.5 Escalation ladder (bounded)

```
deterministic → cache → T3/none → (repair retry ×1) → T2/low → T2/high → T1/medium → T1/high → await_human
```

Per task: `max_attempts` (default 3), `max_escalations` (default 2), and a hard budget cap.
Routing jumps straight to a higher rung when priority or complexity calls for it (for example,
the final strategic review of a critical application starts at T1).

### 5.6 Learning loop

Each reviewed result updates `routing_stats` (successes, attempts, average quality, cost,
latency) for `(task_type, complexity_bucket, model, effort)`. Over time, routing moves work to
cheaper models where they prove good enough. A small exploration rate (default 5%, only in mock
or approved modes) keeps the estimates honest.

---

## 6. Model gateway, cost control and safety

All model calls go through a single `ModelGateway.call(request)`:

```
safety gate → budget reservation → cache lookup → adapter.call → validate schema →
ledger record (tokens, latency, est/actual cost) → budget reconcile → cache store
```

### 6.1 Adapter interface

```python
class ModelAdapter(Protocol):
    provider: str

    def estimate_tokens(self, req: ModelRequest) -> TokenEstimate: ...
    async def call(self, req: ModelRequest) -> ModelResponse: ...
    def map_effort(self, effort: Effort) -> dict: ...
```

Adapters planned:

| Adapter | Status | Purpose |
|---|---|---|
| `mock` | **Built in M2, default** | Seeded, deterministic, schema-valid outputs; simulated tokens and cost |
| `scripted` | Built in M2 | Fixture-driven responses and **fault injection** (malformed JSON, low confidence, timeouts, rate limits, wrong answers) to test retry and escalation |
| `manual_bridge` | **Optional future capability** (not before M4; A5) | Exports a task as a self-contained prompt plus the expected JSON schema. You run it in an external AI tool and import the structured result, which is validated like any other `WorkerResult`. The task waits in `awaiting_human` meanwhile. Off by default; no component depends on it |
| `anthropic`, `openai_compatible`, `ollama` | Interface stubs only; **disabled** | Implemented only after explicit approval (M10). `ollama` would be free but needs a model download and CPU time. It is listed, not assumed |

### 6.2 Cost tracking

Every call records: provider, model, effort, tier, mode (`mock | scripted | manual | real`),
prompt template and version, input/output/cached tokens, **estimated cost** (before the call),
**actual or simulated cost** (after the call), latency, cache hit, status and error.

Reports (CLI and static HTML):
- Cost per **successfully completed** task, per task type and per objective (failed attempts and retries are charged to the eventual success).
- Quality-adjusted cost: cost ÷ average review score.
- Escalation rate, cache hit rate, the percentage of steps resolved deterministically.
- Estimate accuracy (estimated vs actual tokens).

### 6.3 Budgets (hierarchical)

`global (monthly) ⊇ session, objective ⊇ task`. Every call is sent with a **hard output cap**
(`expected output × effort multiplier × output_cap_factor`), and the gateway **reserves the
worst case** at every level: margin-adjusted input (`input_token_margin`) plus a full cap of
output. After the call it settles the reservation against the actual cost; if the provider
honours the cap, actual ≤ reserved, so no budget level can be exceeded, even with concurrent
calls. If actual ever exceeds the reservation (a provider ignoring the cap, or an input
estimate off by more than its margin), the excess is recorded as `model_calls.overshoot_usd`,
shown in reports, and the overrun line refuses further calls. Routing decides affordability
against the same worst-case figure. If a reservation fails, the call is not made, and the
manager decides (requeue, ask or skip). Configuration that lowers the monthly or session
limit applies to existing lines immediately; a raised limit applies to new lines (e.g. next
month). In real mode the global budget defaults to **$0.00**, so real calls are impossible
until you set one.

### 6.4 Limits

- `max_attempts`, `max_escalations`, `max_retries_transient` per task.
- Research: `max_sources`, `max_depth`, `max_queries`, `max_tokens_per_company`.
- Orchestration: `max_concurrency`, per-provider rate limits, `max_tasks_per_objective`.
- Wall-clock timeouts per task.

### 6.5 Caching and deduplication

- **Model-call cache:** key = hash(provider, model, effort, template_id, template_version, normalized inputs, output_schema_version). Identical calls are never repeated.
- **Research cache:** `company_research` rows with `researched_at` / `expires_at` (for example, 30 days for company overview, 7 days for recent news). Research is reused across every job at that company.
- **In-flight dedup:** concurrent identical requests share one call (single-flight).
- **Job dedup:** `dedupe_key` (normalized company + title + location + term) plus description hash.

### 6.6 Safety gate (real execution needs all of these)

1. `execution.mode: real` in config (default `mock`).
2. Environment flag `CAREEROS_ALLOW_REAL_MODELS=1`.
3. The provider is `enabled: true` with **verified** pricing (`verified_at` set).
4. An API key is present in the environment (never in files or git).
5. An `approvals` row of kind `enable_real_models` with an expiry and a budget limit.
6. Global budget > 0.

If any lock is missing, the gateway raises `RealExecutionBlocked`. A test asserts that a fresh
install cannot make a real call.

---

## 7. Task orchestration

### 7.1 Task state machine

```
PENDING → READY → RUNNING → REVIEWING → SUCCEEDED
                     │          │
                     │          └→ RETRY_SCHEDULED → READY
                     ├→ FAILED (terminal after max attempts) → AWAITING_HUMAN
                     └→ BLOCKED_BUDGET → AWAITING_HUMAN
any non-terminal → CANCELLED | SKIPPED (by replan / done-met)
```

### 7.2 Execution

- **DAG** in `tasks` + `task_dependencies` (`hard` = must succeed; `soft` = must finish, any outcome).
- **Dispatcher (M3):** `run` finds ready tasks (all prerequisites succeeded), claims each with one atomic conditional update, and executes up to `execution.max_concurrency` (default 3) at once on a bounded thread pool that exists only for that `run` call. Details in §16.
- **SQLite in WAL mode.** Each worker thread has its own connection; every write is a short `BEGIN IMMEDIATE` transaction, so writers serialize safely and readers never block.
- **Idempotency:** each task has an `idempotency_key`; each model request has an idempotency key (task, attempt, purpose); results are one row per (task, attempt).

### 7.3 Persistence and recovery (A7)

The manager is invoked, does work, and exits. Everything needed to continue is in the database:

- **Objectives, plans (versioned), tasks, dependencies, results, reviews, decisions, checkpoints** are written as they happen, inside transactions, before any dependent step proceeds.
- A `RUNNING` task holds a **lease** (`lease_owner` = execution-session id, `lease_expires_at`) renewed by a heartbeat. Every invocation registers itself in `execution_sessions`.
- **Checkpoints:** a multi-step worker writes a `task_checkpoints` row after each step (step name, state JSON). A retried task restarts from its last checkpoint, not from scratch.
- On `careeros resume` (or any `run`), expired leases → `READY` with `attempt += 1` (bounded by `max_attempts`). Objectives that are `active` with no runnable tasks are re-evaluated against their definition of done.
- Model-call results are cached, so a re-run step that already got an answer does not pay again.
- **Outstanding work** (`ready`, `retry_scheduled`, `awaiting_human`) is listed by `careeros status`.

This survives the end of a process or execution session. Surviving the end of a *cloud container* depends on where the store lives (§9.3).

### 7.4 Independent quality review

Deliverables marked `important` (resumes, cover letters, application answers, final shortlists)
always pass through Quality Review as a **separate task**. That review uses a different prompt
and, where possible, a different model or effort than the author step, so a model does not
grade its own work. Review returns `accept | revise | escalate | reject` with rubric scores and
specific issues. Those issues are fed back to the author step on retry.

---

## 8. Evaluation

| Layer | What it checks | Cost |
|---|---|---|
| Schema validation | Output matches the Pydantic schema | $0 |
| Deterministic checks | Length, section presence, keyword coverage vs job analysis, dates, contact info, banned phrases, spelling | $0 |
| Truthfulness | Every resume bullet or letter claim links to ≥1 `profile_fact` | $0 (fuzzy matching); optional T3 verification |
| Style consistency | Stylometric vector (sentence-length distribution, function-word frequencies, readability, passive-voice ratio, vocabulary richness, punctuation habits) vs your writing samples → distance score | $0 |
| Rubric judge | Relevance, specificity, tone, persuasiveness | T2 (T1 for critical) |
| Regression / golden sets | Fixed jobs + profile → expected properties; run in CI-like test suites with mock and scripted adapters | $0 |

Routing quality priors are updated from these outcomes (§5.6).

---

## 9. Data storage and privacy (A2, A7)

### 9.1 Rules

- **Never committed to git:** real resumes, profiles, contact details, writing samples, application histories, credentials, API keys, `.env`, personal databases or snapshots of them.
- The repo holds only code, configuration templates, documentation and **synthetic** fixtures (fictional people at fictional `example.com` addresses).
- `.gitignore` excludes `data/`, `personal/`, `*.db`, `*.sqlite*`, `snapshots/`, `.env*`. A CI test fails if a tracked file matches those patterns.

### 9.2 Storage abstraction

```
           ┌──────────────────────────── careeros.storage ───────────────────────────┐
 domain ──▶│ StorageConfig: data_dir, dataset_kind (synthetic | personal), backend   │
 repos     │ resolve_data_dir(): refuses any location inside the git working tree    │
           │                                                                         │
           │ PersonalDataStore (Protocol)        BlobStore (Protocol)                │
           │   profile, facts, skills,             put/get/delete documents          │
           │   documents meta, applications,       (resumes, letters, samples)       │
           │   events, style profiles                                                 │
           │      └─ SQLitePersonalStore (M1)      └─ LocalBlobStore (M1)            │
           │      └─ future: encrypted / remote backends (not selected, A2)          │
           └─────────────────────────────────────────────────────────────────────────┘
```

- Every table is classified as `personal` or `operational` (§10.0). Personal tables are accessed only through `PersonalDataStore`, so a different backend (e.g., an encrypted file, or a storage service you approve later) can replace it without touching the workers.
- Default `data_dir` is `$CAREEROS_DATA_DIR` or `~/.local/share/careeros/<dataset_kind>`. It is always outside the repository.
- The database records its `dataset_kind` in `meta`. Synthetic and personal data never share one database, and `careeros init --synthetic` refuses to seed into a personal database.

### 9.3 Persistence across sessions — current limit

Task state persists in SQLite under `data_dir`. That survives the end of a process or execution
session, so interrupted objectives resume (A7). **A cloud container's disk is reclaimed after
inactivity**, so personal data and task state in this cloud environment are not durable across
containers. Committing snapshots to GitHub is ruled out (A2). Until you choose a secure
persistent store, the options are:

1. Accept that cloud-session data is temporary (fine for synthetic development, M0–M5);
2. Export an **encrypted** snapshot (`careeros snapshot --encrypt`, future) and keep the file yourself, outside GitHub;
3. Later, pick a storage backend for `PersonalDataStore` (decision deferred; nothing paid is selected).

### 9.4 Path to durable storage without rewriting orchestration (A11)

Everything above the storage layer talks to two seams, so durability can be added later by
replacing what is below them:

| Seam | Used by | Current implementation |
|---|---|---|
| `core.db.Database` (+ repositories, `BudgetManager`, gateway ledger) | manager, router, gateway, workers | SQLite file in `data_dir`, WAL |
| `storage.PersonalDataStore`, `storage.BlobStore` | profile, documents, applications | `SQLitePersonalStore`, `LocalBlobStore` |

What any replacement backend must provide (the concurrency contract the orchestration relies
on): serializable short write transactions; an atomic conditional update (task claims); unique
constraints (idempotency keys, one result per attempt); and read-your-writes consistency.

Options, cheapest first — none selected, none requires changing the manager:

1. **Keep SQLite, make the file durable:** periodic `sqlite3` online backup (`VACUUM INTO`) to an
   encrypted file the user stores themselves. No new dependency.
2. **Keep SQLite, encrypt at rest:** an encrypted SQLite build or file-level encryption of the
   backup; `Database.open` is the only place that changes.
3. **A server database** (e.g. a self-hosted PostgreSQL) behind the same repositories. Needs a
   small SQL-dialect layer (`BEGIN IMMEDIATE` → `SELECT … FOR UPDATE`/serializable
   transactions, `json_valid` → JSON type, partial unique index syntax). Only if the user
   approves a host; nothing paid is assumed.

Until then the data is synthetic and temporary by design (ADR 0005).

---

## 10. Database schema (SQLite)

Conventions: `INTEGER PRIMARY KEY`, ISO-8601 UTC text timestamps, JSON columns stored as TEXT
with `CHECK(json_valid(...))`, foreign keys ON, WAL mode. Migrations are numbered `.sql` files
applied by a small migration runner. The authoritative schema is
`src/careeros/core/migrations/0001_init.sql`. The listing below is a summary and may omit
CHECK constraints.

### 10.0 Data classification and metadata

| Classification | Tables |
|---|---|
| `personal` | candidate_profile, work_authorization, profile_facts, skills, documents, style_profiles, applications, application_events |
| `operational` (may still reference personal data by id) | companies, company_research, job_sources, jobs, job_analysis, eligibility_assessments, match_scores, objectives, plans, tasks, task_dependencies, task_results, task_checkpoints, reviews, manager_decisions, execution_sessions, model_profiles, model_calls, budgets, cache_entries, routing_stats, approvals |

```sql
CREATE TABLE meta (key TEXT PRIMARY KEY, value TEXT NOT NULL);  -- dataset_kind, created_at
CREATE TABLE schema_migrations (version INTEGER PRIMARY KEY, name TEXT, applied_at TEXT);
```

Task inputs and outputs can contain personal content, so the whole database is treated as
personal for storage purposes (§9).

### 10.1 Profile

```sql
CREATE TABLE candidate_profile (
  id INTEGER PRIMARY KEY, version INTEGER NOT NULL,
  full_name TEXT, email TEXT, phone TEXT, location TEXT, links TEXT,         -- links JSON
  education_level TEXT, graduation_date TEXT,          -- NULL = unknown
  preferences TEXT,                -- JSON: roles, locations, remote, terms, min match score
  updated_at TEXT NOT NULL);

-- A3: no defaults that assume status. Absent row or 'unknown' => needs confirmation.
CREATE TABLE work_authorization (
  id INTEGER PRIMARY KEY, profile_id INTEGER NOT NULL REFERENCES candidate_profile(id),
  country TEXT NOT NULL,                 -- ISO 3166-1 alpha-2
  status TEXT NOT NULL DEFAULT 'unknown' CHECK(status IN
    ('unknown','citizen','permanent_resident','authorized','requires_sponsorship','not_authorized')),
  details TEXT,                          -- free-text note, e.g. permit type (user-entered only)
  confirmed_by_user INTEGER NOT NULL DEFAULT 0, confirmed_at TEXT,
  UNIQUE(profile_id, country));

CREATE TABLE profile_facts (       -- atomic, citable facts
  id INTEGER PRIMARY KEY,
  category TEXT NOT NULL CHECK(category IN
    ('education','experience','project','skill','award','publication','activity','other')),
  title TEXT NOT NULL, organization TEXT, start_date TEXT, end_date TEXT,
  description TEXT NOT NULL, metrics TEXT, tags TEXT, technologies TEXT,  -- JSON
  source_document_id INTEGER REFERENCES documents(id),
  verified INTEGER NOT NULL DEFAULT 0, active INTEGER NOT NULL DEFAULT 1,
  created_at TEXT NOT NULL, updated_at TEXT NOT NULL);

CREATE TABLE skills (
  id INTEGER PRIMARY KEY, name TEXT UNIQUE NOT NULL, category TEXT,
  proficiency TEXT, evidence_fact_ids TEXT);                                -- JSON

CREATE TABLE documents (
  id INTEGER PRIMARY KEY,
  kind TEXT NOT NULL CHECK(kind IN ('resume_master','resume_tailored','cover_letter',
        'writing_sample','portfolio','application_answers','other')),
  title TEXT, path TEXT, format TEXT, content_hash TEXT,
  parent_document_id INTEGER REFERENCES documents(id), version INTEGER DEFAULT 1,
  job_id INTEGER REFERENCES jobs(id), task_id INTEGER REFERENCES tasks(id),
  created_at TEXT NOT NULL);

CREATE TABLE style_profiles (
  id INTEGER PRIMARY KEY, features TEXT NOT NULL,                           -- JSON vector
  sample_document_ids TEXT NOT NULL, computed_at TEXT NOT NULL);
```

### 10.2 Jobs and companies

```sql
CREATE TABLE companies (
  id INTEGER PRIMARY KEY, name TEXT NOT NULL, normalized_name TEXT UNIQUE NOT NULL,
  domain TEXT, headquarters TEXT, size_band TEXT, industry TEXT,
  careers_url TEXT, ats_type TEXT, ats_slug TEXT, notes TEXT, created_at TEXT NOT NULL);

CREATE TABLE company_research (
  id INTEGER PRIMARY KEY, company_id INTEGER NOT NULL REFERENCES companies(id),
  topic TEXT NOT NULL,             -- overview, products, tech_stack, culture, news, intern_program
  summary TEXT, facts TEXT, sources TEXT,                                   -- JSON
  confidence REAL, depth INTEGER, task_id INTEGER REFERENCES tasks(id),
  researched_at TEXT NOT NULL, expires_at TEXT NOT NULL,
  UNIQUE(company_id, topic));

CREATE TABLE job_sources (
  id INTEGER PRIMARY KEY,
  kind TEXT NOT NULL CHECK(kind IN ('greenhouse','lever','ashby','rss','manual','url')),
  config TEXT NOT NULL, enabled INTEGER DEFAULT 1,
  rate_limit_per_min INTEGER DEFAULT 10, last_polled_at TEXT, last_status TEXT);

CREATE TABLE jobs (
  id INTEGER PRIMARY KEY,
  company_id INTEGER NOT NULL REFERENCES companies(id),
  source_id INTEGER REFERENCES job_sources(id), external_id TEXT, url TEXT,
  title TEXT NOT NULL, role_family TEXT,     -- ai_ml | swe | embedded | data | other
  location_raw TEXT, countries TEXT,         -- JSON
  remote_type TEXT CHECK(remote_type IN ('onsite','hybrid','remote','unknown')),
  employment_type TEXT, term TEXT,           -- e.g. "Summer 2027", "8-month co-op"
  posted_at TEXT, closes_at TEXT,
  description_raw TEXT, description_hash TEXT, dedupe_key TEXT,
  status TEXT NOT NULL DEFAULT 'open' CHECK(status IN ('open','closed','unknown')),
  first_seen_at TEXT NOT NULL, last_seen_at TEXT NOT NULL,
  UNIQUE(source_id, external_id));
CREATE INDEX idx_jobs_dedupe ON jobs(dedupe_key);

CREATE TABLE job_analysis (
  job_id INTEGER PRIMARY KEY REFERENCES jobs(id),
  required_skills TEXT, preferred_skills TEXT, keywords TEXT,               -- JSON
  degree_requirements TEXT, graduation_window TEXT, work_auth_requirements TEXT,
  responsibilities TEXT, seniority TEXT,
  method TEXT,                     -- deterministic | model:<id>
  analysis_version INTEGER, task_id INTEGER REFERENCES tasks(id), analyzed_at TEXT);

CREATE TABLE eligibility_assessments (
  id INTEGER PRIMARY KEY, job_id INTEGER NOT NULL REFERENCES jobs(id),
  profile_version INTEGER NOT NULL,
  verdict TEXT NOT NULL CHECK(verdict IN
    ('eligible','ineligible','needs_user_confirmation','uncertain')),
  reasons TEXT, blocking_criteria TEXT, unknown_inputs TEXT, confidence REAL,
  task_id INTEGER REFERENCES tasks(id), assessed_at TEXT NOT NULL,
  UNIQUE(job_id, profile_version));

CREATE TABLE match_scores (
  id INTEGER PRIMARY KEY, job_id INTEGER NOT NULL REFERENCES jobs(id),
  profile_version INTEGER NOT NULL, score REAL NOT NULL,
  components TEXT, rationale TEXT, top_fact_ids TEXT,
  task_id INTEGER REFERENCES tasks(id), scored_at TEXT NOT NULL,
  UNIQUE(job_id, profile_version));
```

### 10.3 Applications

```sql
CREATE TABLE applications (
  id INTEGER PRIMARY KEY, job_id INTEGER UNIQUE NOT NULL REFERENCES jobs(id),
  status TEXT NOT NULL CHECK(status IN ('shortlisted','preparing','ready_for_review',
        'submitted','assessment','interview','offer','accepted','rejected',
        'withdrawn','no_response')),
  priority TEXT, deadline TEXT,
  resume_document_id INTEGER REFERENCES documents(id),
  cover_letter_document_id INTEGER REFERENCES documents(id),
  answers_document_id INTEGER REFERENCES documents(id),
  objective_id INTEGER REFERENCES objectives(id),
  submitted_at TEXT, outcome_at TEXT, notes TEXT,
  created_at TEXT NOT NULL, updated_at TEXT NOT NULL);

CREATE TABLE application_events (
  id INTEGER PRIMARY KEY, application_id INTEGER NOT NULL REFERENCES applications(id),
  event_type TEXT NOT NULL, from_status TEXT, to_status TEXT,
  details TEXT, occurred_at TEXT NOT NULL);
```

### 10.4 Orchestration

```sql
CREATE TABLE objectives (
  id INTEGER PRIMARY KEY, kind TEXT NOT NULL, title TEXT NOT NULL, inputs TEXT,
  definition_of_done TEXT NOT NULL, priority TEXT NOT NULL DEFAULT 'normal',
  budget_usd REAL, budget_tokens INTEGER,
  status TEXT NOT NULL CHECK(status IN ('active','done','stopped','awaiting_human','failed')),
  stop_reason TEXT, created_at TEXT NOT NULL, completed_at TEXT);

CREATE TABLE plans (
  id INTEGER PRIMARY KEY, objective_id INTEGER NOT NULL REFERENCES objectives(id),
  version INTEGER NOT NULL, source TEXT NOT NULL,     -- playbook:<name> | model:<call_id>
  rationale TEXT, graph TEXT NOT NULL, superseded_at TEXT, created_at TEXT NOT NULL,
  UNIQUE(objective_id, version));

CREATE TABLE tasks (
  id INTEGER PRIMARY KEY,
  objective_id INTEGER NOT NULL REFERENCES objectives(id),
  plan_id INTEGER NOT NULL REFERENCES plans(id),
  parent_task_id INTEGER REFERENCES tasks(id),
  worker TEXT NOT NULL, step TEXT, task_type TEXT NOT NULL,
  title TEXT, input TEXT, acceptance_criteria TEXT,
  priority TEXT NOT NULL DEFAULT 'normal', importance TEXT DEFAULT 'normal',  -- important => review
  complexity REAL,
  status TEXT NOT NULL CHECK(status IN ('pending','ready','running','reviewing',
        'retry_scheduled','succeeded','failed','blocked_budget','awaiting_human',
        'cancelled','skipped')),
  attempt INTEGER NOT NULL DEFAULT 0, max_attempts INTEGER NOT NULL DEFAULT 3,
  escalations INTEGER NOT NULL DEFAULT 0, current_tier INTEGER, current_effort TEXT,
  idempotency_key TEXT UNIQUE,
  lease_owner TEXT, lease_expires_at TEXT,
  budget_usd REAL, spent_usd REAL NOT NULL DEFAULT 0,
  accepted_result_id INTEGER, last_error TEXT,
  created_at TEXT NOT NULL, started_at TEXT, finished_at TEXT);
CREATE INDEX idx_tasks_status ON tasks(status, priority);

CREATE TABLE task_dependencies (
  task_id INTEGER NOT NULL REFERENCES tasks(id),
  depends_on_task_id INTEGER NOT NULL REFERENCES tasks(id),
  kind TEXT NOT NULL DEFAULT 'hard' CHECK(kind IN ('hard','soft')),
  PRIMARY KEY(task_id, depends_on_task_id));

CREATE TABLE task_results (
  id INTEGER PRIMARY KEY, task_id INTEGER NOT NULL REFERENCES tasks(id),
  attempt INTEGER NOT NULL, schema_version TEXT NOT NULL,
  status TEXT NOT NULL, output TEXT, evidence TEXT, assumptions TEXT,
  confidence REAL, effective_confidence REAL,
  unresolved_questions TEXT, recommended_next_steps TEXT,
  created_at TEXT NOT NULL);

CREATE TABLE task_checkpoints (   -- A7: resume multi-step workers mid-way
  id INTEGER PRIMARY KEY, task_id INTEGER NOT NULL REFERENCES tasks(id),
  attempt INTEGER NOT NULL, step TEXT NOT NULL, state TEXT NOT NULL,
  created_at TEXT NOT NULL, UNIQUE(task_id, attempt, step));

CREATE TABLE execution_sessions (  -- one row per manager/scheduler invocation
  id TEXT PRIMARY KEY, started_at TEXT NOT NULL, last_heartbeat_at TEXT,
  ended_at TEXT, end_reason TEXT, host TEXT);

CREATE TABLE reviews (
  id INTEGER PRIMARY KEY, task_id INTEGER NOT NULL REFERENCES tasks(id),
  result_id INTEGER NOT NULL REFERENCES task_results(id),
  reviewer TEXT NOT NULL,          -- deterministic | model:<id>
  verdict TEXT NOT NULL CHECK(verdict IN ('accept','revise','escalate','reject')),
  score REAL, rubric_scores TEXT, issues TEXT,
  model_call_id INTEGER REFERENCES model_calls(id), created_at TEXT NOT NULL);

CREATE TABLE manager_decisions (
  id INTEGER PRIMARY KEY, objective_id INTEGER REFERENCES objectives(id),
  task_id INTEGER REFERENCES tasks(id),
  decision TEXT NOT NULL CHECK(decision IN ('plan','replan','route','accept','retry_same',
        'retry_with_feedback','escalate_effort','escalate_tier','skip','cancel',
        'await_human','stop')),
  rationale TEXT, inputs TEXT, created_at TEXT NOT NULL);
```

### 10.5 Models, cost, cache, approvals

```sql
CREATE TABLE model_profiles (      -- snapshot of config/models.yaml for history and joins
  id TEXT PRIMARY KEY, provider TEXT NOT NULL, tier INTEGER NOT NULL,
  capabilities TEXT, context_window INTEGER, effort_levels TEXT,
  input_per_mtok REAL, output_per_mtok REAL, pricing_simulated INTEGER NOT NULL,
  pricing_verified_at TEXT, enabled INTEGER NOT NULL, config_hash TEXT, updated_at TEXT);

CREATE TABLE model_calls (
  id INTEGER PRIMARY KEY, task_id INTEGER REFERENCES tasks(id), attempt INTEGER,
  model_id TEXT NOT NULL REFERENCES model_profiles(id), provider TEXT NOT NULL,
  tier INTEGER, effort TEXT,
  mode TEXT NOT NULL CHECK(mode IN ('mock','scripted','manual','real')),
  template_id TEXT, template_version TEXT,
  est_input_tokens INTEGER, est_output_tokens INTEGER,
  input_tokens INTEGER, output_tokens INTEGER, cached_tokens INTEGER,
  est_cost_usd REAL, cost_usd REAL, cost_is_simulated INTEGER NOT NULL,
  latency_ms INTEGER, status TEXT NOT NULL, error TEXT,
  cache_key TEXT, cache_hit INTEGER NOT NULL DEFAULT 0, created_at TEXT NOT NULL);
CREATE INDEX idx_calls_task ON model_calls(task_id);

CREATE TABLE budgets (
  id INTEGER PRIMARY KEY,
  scope TEXT NOT NULL CHECK(scope IN ('global_month','objective','task')),
  scope_ref TEXT NOT NULL, period TEXT,
  limit_usd REAL NOT NULL, limit_tokens INTEGER,
  spent_usd REAL NOT NULL DEFAULT 0, reserved_usd REAL NOT NULL DEFAULT 0,
  mode TEXT NOT NULL CHECK(mode IN ('simulated','real')),
  UNIQUE(scope, scope_ref, period, mode));

CREATE TABLE cache_entries (
  key TEXT PRIMARY KEY, kind TEXT NOT NULL, value TEXT, blob_path TEXT,
  created_at TEXT NOT NULL, expires_at TEXT, hits INTEGER NOT NULL DEFAULT 0);

CREATE TABLE routing_stats (
  task_type TEXT NOT NULL, complexity_bucket TEXT NOT NULL,
  model_id TEXT NOT NULL, effort TEXT NOT NULL,
  attempts INTEGER NOT NULL DEFAULT 0, accepts INTEGER NOT NULL DEFAULT 0,
  prior_alpha REAL NOT NULL, prior_beta REAL NOT NULL,
  avg_quality REAL, avg_cost_usd REAL, avg_latency_ms REAL, updated_at TEXT,
  PRIMARY KEY(task_type, complexity_bucket, model_id, effort));

CREATE TABLE approvals (
  id INTEGER PRIMARY KEY,
  kind TEXT NOT NULL CHECK(kind IN ('enable_real_models','budget_increase',
        'network_source','other')),
  scope TEXT, details TEXT, max_usd REAL,
  granted_at TEXT NOT NULL, expires_at TEXT, revoked_at TEXT);

-- Derived reporting view
CREATE VIEW v_cost_per_success AS
SELECT t.task_type,
       COUNT(DISTINCT CASE WHEN t.status='succeeded' THEN t.id END) AS successes,
       SUM(c.cost_usd) AS total_cost,
       SUM(c.cost_usd) * 1.0 /
         NULLIF(COUNT(DISTINCT CASE WHEN t.status='succeeded' THEN t.id END),0) AS cost_per_success
FROM tasks t LEFT JOIN model_calls c ON c.task_id = t.id
GROUP BY t.task_type;
```

---

## 11. Folder structure

```
CareerOS/
├── pyproject.toml                 # Python 3.12; deps: pydantic, typer, httpx, jinja2, pyyaml, rapidfuzz
├── README.md
├── .gitignore                     # data/, personal/, *.db, .env, var/, caches
├── .github/workflows/ci.yml       # lint + type-check + tests (free GitHub Actions allowance)
├── docs/
│   ├── ARCHITECTURE.md            # this document
│   └── decisions/                 # short ADRs (one per significant choice)
├── config/
│   ├── careeros.example.yaml      # execution mode (mock), storage, concurrency, limits, budgets, eligibility rules
│   ├── models.yaml                # capability profiles; mock enabled, real disabled templates
│   ├── routing.yaml               # task types, base complexity, quality floors, priors, ladders
│   └── playbooks/                 # objective → task-DAG templates
│       ├── discover_jobs.yaml
│       ├── prepare_application.yaml
│       └── weekly_review.yaml
├── src/careeros/
│   ├── core/                      # config loading, db + migration runner, clock, errors
│   │   └── migrations/0001_init.sql ...
│   ├── storage/                   # data-dir resolution, PersonalDataStore, BlobStore, SQLite/local impls
│   ├── domain/                    # pydantic models + repositories: profile, jobs, companies, applications, orchestration
│   ├── llm/
│   │   ├── contracts.py           # ModelRequest/Response, OutputSchema, errors
│   │   ├── base.py                # ModelAdapter protocol
│   │   ├── gateway.py             # safety → budget → cache → adapter → ledger
│   │   ├── safety.py              # real-execution locks
│   │   ├── registry.py            # offline-only adapter registry
│   │   ├── tokens.py              # provider-neutral token estimation
│   │   └── adapters/              # mock.py, scripted.py (offline only; real adapters at M10)
│   ├── routing/                   # complexity.py, policy.py, router.py, escalation.py, stats.py
│   ├── cost/                      # pricing.py, ledger.py, budget.py, reports.py
│   ├── cache/                     # keys.py, store.py, singleflight.py
│   ├── orchestration/             # states.py, graph.py, scheduler.py, executor.py, leases.py, results.py
│   ├── manager/                   # manager.py, planner.py, playbooks.py, decisions.py, done.py, review.py
│   ├── workers/
│   │   ├── base.py                # Worker, Step, WorkerResult
│   │   ├── discovery/  eligibility/  research/  matching/
│   │   ├── resume/  cover_letter/  application/  quality/
│   ├── sources/                   # greenhouse.py, lever.py, ashby.py, rss.py, manual.py, http.py
│   ├── evaluation/                # checks.py, truthfulness.py, style.py, rubrics/, golden.py
│   ├── documents/                 # render.py, templates/ (Markdown/HTML; PDF later)
│   ├── prompts/                   # versioned Jinja templates, one per task_type
│   └── cli/                       # `careeros` Typer app
├── tests/
│   ├── unit/  integration/  simulation/
│   └── fixtures/                  # SYNTHETIC profile and jobs only; scripted model responses (M2+)
└── (no personal data in the repo — it lives in data_dir outside the working tree, §9)
```

Tooling (all free): `pytest`, `ruff`, `mypy`. GitHub Actions runs the same checks on push and
pull request (A6). One Linux job, one Python version, a short timeout and concurrency
cancellation keep usage well inside the free private-repo allowance. The identical command
(`make check` or the commands in the README) runs in a session.

---

## 12. Example flow — `careeros run prepare_application --job 1234`

1. **Manager** matches the `prepare_application` playbook (deterministic, $0) and creates plan v1:
   ```
   analyze_job ─┬─▶ eligibility ─┬─▶ match ─▶ tailor_resume ─▶ review_resume ─┐
                └─▶ research_co ─┘        └▶ draft_letter ─▶ review_letter ──┼─▶ package ─▶ final_review(critical only)
   ```
2. `analyze_job` runs deterministic extraction. One unusual requirement phrase → T3/none (mock, ~$0.0003 simulated).
3. `research_co` and `eligibility` run **in parallel**. Research hits a 12-day-old cache entry → $0.
4. `eligibility`: rules return `eligible`, confidence 0.95 → accept.
5. `match`: TF-IDF + rules score 0.78 ≥ threshold. Rationale step skipped (not top-priority).
6. `tailor_resume`: the router picks T2/low (p=0.88 ≥ floor 0.85 for `high`) → draft.
7. `review_resume`: truthfulness check finds 1 unsupported claim → `revise`. Retry with feedback → accepted.
8. `draft_letter` → `review_letter`: style distance 0.31 > 0.25 → escalate effort (T2/high) → 0.19, accepted.
9. `package` (deterministic). The definition of done is met → **stop**. `final_review` is skipped because priority is `high`, not `critical`.
10. Report: 6 tasks, 4 model calls, 1 retry, 1 effort escalation, 1 cache hit, simulated cost $0.031, cost per success $0.0052.

---

## 13. Development milestones

Every milestone ends with passing tests, a short demo command, and your review before the next
one starts. All milestones through M9 run entirely on mock, scripted or manual adapters at $0.

| # | Milestone | Deliverables | Done when |
|---|---|---|---|
| **M0** | Scaffold | pyproject, package layout, ruff/mypy/pytest, `.gitignore` with personal-data patterns, README, ADR template, GitHub Actions CI | `pytest`, `ruff` and `mypy` pass locally; CI workflow is valid |
| **M1** | Core and data | Typed config loader (models/routing/eligibility config templates, mock-only), storage abstraction (§9.2), SQLite + migration runner, full schema (§10), domain models and repositories, synthetic fixtures, `careeros init/status/config` | Schema created; repository round-trip tests pass; data dir can't be inside the repo; unknown work authorization is the default; task state survives closing and reopening the database |
| **M2** | Model layer, routing, cost, Career Manager (A8) | Provider-neutral adapter protocol; `mock` and `scripted` adapters (capability, cost, latency, malformed/invalid/timeout/rate-limit/low-confidence outcomes); offline-only registry and per-call safety gate; gateway with token/cost ledger; hierarchical budgets with reservations; accepted-result cache; complexity estimator; quality-adjusted router with history; decision table; playbooks + model-assisted planning; independent review; replanning; approval requests; checkpoint-based recovery; cost/quality report; `demo/resume/report/approvals/approve` CLI | **Done.** 141 tests; real execution proven impossible (§15.6) |
| **M3** | Bounded concurrent execution (A10) + priority policy (A9) | Dispatcher with bounded worker pool (default 3), atomic claims, per-thread connections, lease heartbeats, lease-based multi-worker recovery, atomic two-phase budget reservations (task/objective/session/month), idempotent model calls with in-flight/abandoned accounting, budget-contention requeue, worker-error retries, prerequisite failure/cancellation handling; downgrade policy with user-approved downgrades | **Done.** 174 tests incl. deterministic concurrency, contention, crash and recovery tests (§16) |
| **M4** | Manager hardening and evaluation | Golden-set simulation harness with many seeded runs, routing-prior calibration report, exploration policy, richer definition-of-done checks, optional manual AI bridge (A5) | Calibration and cost/quality reports over ≥100 simulated objectives |
| **M5** | Profile | Import profile from YAML/Markdown into `profile_facts`; writing samples; stylometric style profile; `careeros profile show/validate` | Your real profile loads; facts are citable; style vector is computed |
| **M6** | Discovery and eligibility | **First: ADR verifying each source's current endpoints, rate limits and terms (A4).** Then Greenhouse/Lever/Ashby public board clients + manual URL/paste import; normalize, dedupe, filter; job analysis; configurable eligibility rules with `needs_user_confirmation` | Fixture-based tests pass; live fetch only after the ADR and the environment network allowlist are approved. No LinkedIn or Indeed scraping |
| **M7** | Research and matching | Bounded employer research with caching and freshness; TF-IDF/rule matching; shortlist report | Repeated research on the same company costs $0; ranked shortlist produced |
| **M8** | Documents and quality | Resume tailoring, cover letters, application answers, truthfulness checker, rubric reviewer, style scoring, document rendering | Golden tests: 0 unsupported claims; style distance reported; drafts produced via mock or manual bridge |
| **M9** | Tracking and reporting | Application lifecycle + events, deadlines, outcome analytics, cost/quality dashboard (static HTML/CLI), encrypted local `snapshot/restore` (never committed, A2) | End-to-end mock run from discovery → package → tracked application |
| **M10** | Real providers (**only with explicit approval**) | Implement chosen adapter(s), verified pricing, approval record, small budget, kill switch, contract tests vs mock | Not started unless you authorize a provider, a budget cap and an expiry in writing |

---

## 14. Outstanding decisions

Resolved: plan approval (A1), personal data in git (A2: no), eligibility (A3), job sources (A4),
manual bridge (A5: optional), CI (A6: GitHub Actions within the free allowance).

Still open (none blocks M0/M1):

1. **Durable personal storage** (§9.3): how real profile data and task state should persist across cloud containers. Deferred; nothing selected.
2. **Your eligibility inputs**: enter them yourself later through the CLI (M5). Until then every authorization is `unknown`.
3. **Network allowlist for job-board hosts**: to be requested at M6 after the terms review.
4. **Routing priors and quality floors** (`config/routing.yaml`) are starting guesses; resolved
   policy (A9) is implemented. Calibration is planned for M4.
5. **Durable storage** (§9.4): which of the documented options, if any, and when.

---

## 15. M2 implementation notes

### 15.1 Module map

| Module | Responsibility |
|---|---|
| `llm/contracts.py` | `ModelRequest`, `ModelResponse`, `OutputSchema`, error types (provider-neutral) |
| `llm/base.py` | `ModelAdapter` protocol: `call(profile, request)`, `map_effort(effort)` |
| `llm/adapters/mock.py` | Seeded simulator: capability (`simulation.skill`), effort, latency, failure rates |
| `llm/adapters/scripted.py` | Forces exact outcomes per task type / model for tests and demos |
| `llm/registry.py`, `llm/safety.py` | Offline-only adapter registry; per-call gate (mode, provider, adapter, enabled) |
| `llm/gateway.py` | gate → cache → estimate → reserve → adapter → parse → ledger → settle |
| `cost/budget.py`, `cost/pricing.py`, `cost/report.py` | Budgets with reservations; token pricing; cost/quality report |
| `cache/store.py` | Cache of *accepted* results only |
| `routing/complexity.py`, `routing/router.py`, `routing/stats.py` | Complexity features; model+effort selection; outcome history |
| `workers/base.py`, `workers/generic.py` | Worker contracts; generic model step; deterministic utilities |
| `manager/plans.py`, `manager/decisions.py`, `manager/manager.py` | Plans/playbooks/waves; decision table; the coordinator |
| `simulation.py` | Demo scenarios: standard, escalation, budget, interrupt, novel |
| `config/playbooks/*.yaml` | Demonstration playbooks (generic workers; real specialists arrive in M6–M8) |
| `migrations/0002_*.sql` | `tasks.mode/optional/current_model_id`, `model_calls.objective_id/purpose`, `approval_requests` |

### 15.2 Connecting a real provider later (M10, only with approval)

1. Implement one class satisfying `ModelAdapter`: translate `ModelRequest` to the provider's
   API, map `effort` (`none…max`) to its reasoning controls, return token counts.
2. Mark it `offline = False`, and extend the registry and safety gate *only* under the M10
   approval process (approval record, verified prices, budget cap, key via environment).
3. Add profiles to `models.yaml` (`provider`, `provider_model`, `tier`, `capabilities`,
   `effort_levels`, verified `pricing`). For Anthropic, the high-, medium- and low-cost model
   families would be tier 1, 2 and 3 profiles. Nothing in the router, manager or workers
   names a model or provider, so none of them change.

### 15.3 Routing rule (as implemented)

For every enabled `(model, effort)` allowed by the task type (`min_tier`,
`required_capabilities`, efforts ≥ the policy default):

* `p` = Beta posterior: prior `0.97 − 1.5·max(0, complexity − (tier_skill + effort_bonus))`
  with strength `prior_strength`, updated by `routing_stats` for the same
  (task type, complexity bucket, model, effort);
* `c` = estimated cost of one call (real prompt size, expected output × effort multiplier);
* `E` = expected cost to an accepted result: `E_i = c_i + (1−p_i)·min E_j over p_j > p_i`,
  and `E_top = c_top / p_top`.

Choose min `E` among `p ≥ floor(priority)` and `c ≤ remaining budget` (task, objective,
session and monthly, whichever is smallest; committed spend only — concurrent reservations are
enforced at reservation time, §16.3). Otherwise the A9 policy applies (§5.4): critical/high →
`needs_approval` (with a downgrade option the user may accept); normal/low → recorded
`degraded` choice within `degrade_limits`, else `needs_approval`. Retries reuse the model; effort
escalation keeps the model and raises effort; tier escalation requires a stronger tier.

### 15.4 Manager loop

`submit` → playbook match (exact kind, else unambiguous keywords) or model-assisted planning
(validated, escalated on invalid plans, else a clarification request) → tasks + dependencies
+ waves persisted. `run` → recover interrupted tasks → repeatedly pick the first ready task
in wave order → deterministic step or routed model step → validate → deterministic checks →
independent model review for important deliverables (a different model than the author) →
decision table → accept / retry / escalate / replan (prune dependents) / ask the user →
stop when the definition of done is met (remaining optional work is cancelled).

### 15.5 Persistence and recovery

Persisted per event: plan versions with waves; every routing decision with all candidates
(`p`, `c`, `E`, floor, budget); every model call (tokens, latency, estimated and simulated
cost, status, purpose); every result and review; every retry, escalation, replan, approval
request and stop; a `model_response` checkpoint per model attempt. A new session marks tasks
left running by a dead session as ready at the same attempt number; the checkpoint supplies
the model response, so completed work is neither repeated nor re-billed.

### 15.6 Why real execution is impossible in M2

1. Config rejects `mode: real|manual`, `allow_real_models: true`, a non-zero real budget and
   any enabled non-mock provider — from files *and* environment variables.
2. The only adapters in the codebase are `mock` and `scripted` (a test pins the file list).
3. The registry refuses any adapter that is not offline or serves a non-offline provider.
4. The gateway re-checks mode, provider, adapter and enabled state on every call, so even a
   configuration that bypasses validation is refused.
5. No module imports a network library or vendor SDK (AST test); full scenarios pass with
   sockets disabled; API keys in the environment are ignored.
6. The database rejects any non-real call recorded as non-simulated cost.

---

## 16. M3 implementation notes — bounded concurrent execution

### 16.1 Files

| File | Change |
|---|---|
| `manager/manager.py` | `run` is a dispatcher with a bounded `ThreadPoolExecutor`; `_claim`, `_heartbeat`, `_recover` (lease-based), `_worker_failed`, `_requeue`, `_ready_tasks`; workers run `_execute_claimed` on their own connection; halting on session termination; downgrade approval |
| `llm/gateway.py` | Two-phase call (T1 reserve + in-flight row, T2 complete + settle); idempotency-key replay; `abandon_in_flight` |
| `cost/budget.py` | Reservation rows owned by sessions; `reserve_in`/`settle_in`/`release_in` inside caller transactions; session budget level; committed-only availability for routing |
| `routing/router.py` | A9 priority policy: `floors`, `degrade_limits`, `min_quality`, `approved_floor`, `approved_downgrade` status, downgrade option offered to the user |
| `llm/adapters/scripted.py` | Thread-safe; step-qualified script keys |
| `core/config.py`, `config/*.yaml` | `max_concurrency` default 3; `session_simulated_usd`; `heartbeat_seconds`; policy fields and validation |
| `core/db.py`, `context.py` | 30 s busy timeout; per-thread worker connections |
| `migrations/0003_m3_concurrency.sql` | `budget_reservations`; `model_calls.session_id/idempotency_key/reservation_id/response_json`; `tasks.approved_floor`; extended decision/approval/budget kinds |

### 16.2 Concurrency, dependencies and the worker limit

* A `run` call is one **execution session**. Its dispatcher loop: read state → find ready
  tasks (pending/ready/retry-scheduled whose prerequisites all *succeeded*, in wave order) →
  **claim** each with `UPDATE tasks SET status='running', attempt=attempt+1, lease_owner=? …
  WHERE id=? AND status IN (runnable)`; only a claim that changed one row runs → submit to a
  `ThreadPoolExecutor(max_workers=execution.max_concurrency)` → wait for the first completion →
  repeat. At most `max_concurrency` tasks are in flight; the pool is shut down when `run`
  returns, so no worker outlives the call.
* Each worker thread gets its own `CareerManager` clone on its **own SQLite connection**; the
  adapter, worker registry, playbooks and event log are shared (thread-safe).
* A task whose prerequisite **failed, was skipped or was cancelled** is marked `skipped` (with a
  `skip` decision) and its own dependents follow. A **replan** prunes dependents *before* the
  parent is marked succeeded, so a dependent can never be dispatched in between.
* When the **definition of done** is met, the dispatcher stops claiming, waits for in-flight
  tasks, then cancels unstarted optional work.
* An unexpected **worker exception** affects only that task: its held reservations are
  released and it is retried (`worker_error` decision) until `max_attempts`, then `failed`.

### 16.3 Budget reservations and database consistency

* **Atomic reservation.** In one `BEGIN IMMEDIATE` transaction (T1) the gateway checks every
  budget level (month, session, objective, task: `limit − spent − reserved ≥ estimate`),
  increments `reserved_usd` on all of them, inserts the `budget_reservations` row and inserts
  the `in_flight` call row. SQLite's write lock makes this one step across all threads and
  processes, so concurrent tasks can never jointly exceed any level.
* **Atomic settlement.** In T2 the call row is completed (tokens, latency, cost, stored
  response) and the reservation is converted to spend in the same transaction.
* **Routing vs. reservation.** The router sizes its choice against *committed* spend; if a
  concurrent task's reservation makes the reservation fail although committed spend would
  allow it, the task is **requeued** (`requeue` decision) and retried once a running task
  settles — contention never triggers a downgrade or an approval request.
* **Duplicate prevention.** Exclusive claims (one executor per attempt); a unique idempotency
  key per model request (`task:<id>:attempt:<n>`, `review:…`, `plan:…`) — a repeated request
  returns the stored reply instead of calling the adapter; one result row per (task, attempt);
  a resumed review replaces rather than duplicates its review row.
* **Writes.** Short transactions only; WAL; 30 s busy timeout; checkpoints are written right
  after each model reply.

### 16.4 Interruption and recovery with several active workers

* A live session renews its leases every `heartbeat_seconds` (default `lease_seconds / 3`).
* If the process dies (simulated by `SessionTerminatedError` from any worker), the dispatcher
  sets a halt flag; every other worker stops at its next step; nothing is cleaned up — state is
  exactly what a kill leaves: running tasks with leases, possibly an `in_flight` call with a
  held reservation.
* The next `run` (any process) calls `_recover` first:
  * a session is **dead** if it ended or its heartbeat is older than one lease
    (`--force-recover` declares all other sessions dead);
  * its `in_flight` calls become `abandoned`, charged at their **estimate** (the provider may
    have billed them — conservative), and their reservations are settled;
  * its other held reservations are released;
  * tasks it was running whose lease expired return to `ready` **at the same attempt number**
    (`recover` decision); the checkpoint or the idempotent call record supplies any reply that
    already arrived, so it is neither repeated nor re-billed;
  * tasks held by a **live** session are left alone.

### 16.5 Remaining concerns and limitations

1. **Abandoned calls can be repeated.** If the process dies while a call is in flight, the
   replacement call is a second call; it is charged conservatively. With real providers this
   could mean paying twice for that one request; unavoidable without provider-side idempotency.
2. **Threads, not processes.** Model calls are I/O-bound, so threads suffice; CPU-heavy
   deterministic workers would serialize on the GIL. Several *processes* may run the same
   objective safely (claims and reservations are database-level), but there is no supervisor.
3. **Clock-based liveness.** A session that hangs (alive but stuck) keeps its leases fresh only
   while its heartbeat thread runs; a stopped heartbeat makes it look dead after one lease.
   `--force-recover` must only be used when no other process is running.
4. **SQLite write throughput** bounds concurrency in practice (fine at 3–16 workers).
5. **Determinism.** Per-task outcomes are deterministic (seeded per request, step-keyed
   scripts); the *interleaving* of the event log is not, and it does not need to be.
6. **Session budgets** reset with each `run` invocation; the monthly simulated cap still
   bounds total spend.

---

## 17. Phase A — post-M3 audit fixes

### 17.1 Fixed findings

| Audit finding | Fix | Where |
|---|---|---|
| **H1** silent downgrade of critical/high work; choices below the factual minimum after learned history | Hard minimum applied to every candidate; "floor unreachable" handled by the same priority policy; `no_candidate` when nothing reaches the minimum; effort escalation falls back to tier escalation before asking | `routing/router.py` `Router.route`; `manager._route_for` |
| **M1** settlement could exceed a budget | Hard output cap sent with every call; worst-case reservation; router affordability uses the same figure; overshoot recorded (`overshoot_usd`, error note, report) and the overrun line blocks further calls; abandoned calls charged the full reservation | `cost/pricing.py`, `llm/gateway.py`, `routing/router.py`, `cost/report.py` |
| **M2** a second replan dropped earlier skipped steps | `skip_steps` marks tasks skipped and writes the next plan version with the union of skipped steps in one write transaction | `domain/repositories.py`, `manager._prune` |
| **M3** inconsistent early completion; stale approvals reopened finished objectives | `finish_objective` cancels every unfinished task (incl. blocked/awaiting) and withdraws pending requests with the stop decision in one transaction; `approve`/`deny` validate the request, objective and task state, withdraw obsolete requests, and write atomically | `domain/repositories.py`, `manager._finish`, `manager.approve/deny` |
| **M6** review/planning downgrades unrecorded; approvals applied to the wrong route | `_record_degrade` used by task, review and planning routing; requests carry `route_purpose`; review approvals set `tasks.approved_review_floor` | `manager.py` |
| **M7** lowered monthly limit ignored until next month | Config-derived lines (month, session) are clamped to the lower configured limit | `cost/budget.py` `_ensure` |

Migration `0004_phase_a_fixes.sql` adds `model_calls.reserved_usd/output_cap_tokens/
overshoot_usd`, `tasks.approved_review_floor`, and the `withdrawn` request status.

### 17.2 Still open (deliberately deferred)

| Finding | Status |
|---|---|
| **H2** writes not fenced by lease ownership; late replies after takeover | Deferred to the ownership/recovery change (with M4, M5) |
| **M4** heartbeat thread can die silently | Deferred (same change) |
| **M5** multi-statement state transitions (`_block`, `_accept`, `_apply`, recovery) not atomic | Deferred (same change); `approve`, `deny`, `_finish` and `_prune` are now atomic |
| L1–L12 (audit) | Unchanged except where noted above; see the audit report |

### 17.3 New behaviour to be aware of

* Reservations are larger than expected costs (worst case at the cap), so tight budgets ask
  for approval earlier than before; demo budgets were re-sized accordingly.
* A task whose best option falls below its hard minimum now stops and asks instead of
  running; with poor learned history this can happen for factual task types.
* Raising a config budget limit does not raise an existing month's line.

