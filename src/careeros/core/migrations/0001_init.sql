-- CareerOS schema v1. Authoritative source for docs/ARCHITECTURE.md §10.
-- Conventions: ISO-8601 UTC TEXT timestamps; JSON stored as TEXT and checked with json_valid.
-- Classification (see storage.classification): PERSONAL tables hold data about the candidate.

-- ======================================================================== metadata
CREATE TABLE meta (
  key   TEXT PRIMARY KEY,
  value TEXT NOT NULL
);

-- ======================================================================== PERSONAL: profile
CREATE TABLE candidate_profile (
  id              INTEGER PRIMARY KEY,
  version         INTEGER NOT NULL DEFAULT 1,
  full_name       TEXT,
  email           TEXT,
  phone           TEXT,
  location        TEXT,
  links           TEXT NOT NULL DEFAULT '[]' CHECK (json_valid(links)),
  education_level TEXT,                      -- NULL = unknown
  graduation_date TEXT,                      -- NULL = unknown
  preferences     TEXT NOT NULL DEFAULT '{}' CHECK (json_valid(preferences)),
  created_at      TEXT NOT NULL,
  updated_at      TEXT NOT NULL
);

-- Amendment A3: no default assumes any status. Missing row or 'unknown' => needs confirmation.
CREATE TABLE work_authorization (
  id                INTEGER PRIMARY KEY,
  profile_id        INTEGER NOT NULL REFERENCES candidate_profile(id) ON DELETE CASCADE,
  country           TEXT NOT NULL CHECK (length(country) = 2),
  status            TEXT NOT NULL DEFAULT 'unknown' CHECK (status IN (
                      'unknown','citizen','permanent_resident','authorized',
                      'requires_sponsorship','not_authorized')),
  details           TEXT,
  confirmed_by_user INTEGER NOT NULL DEFAULT 0 CHECK (confirmed_by_user IN (0,1)),
  confirmed_at      TEXT,
  updated_at        TEXT NOT NULL,
  UNIQUE (profile_id, country),
  CHECK (status = 'unknown' OR confirmed_by_user = 1)
);

CREATE TABLE documents (
  id                 INTEGER PRIMARY KEY,
  kind               TEXT NOT NULL CHECK (kind IN (
                       'resume_master','resume_tailored','cover_letter','writing_sample',
                       'portfolio','application_answers','other')),
  title              TEXT,
  blob_key           TEXT,                   -- key in the BlobStore
  format             TEXT,
  content_hash       TEXT,
  parent_document_id INTEGER REFERENCES documents(id),
  version            INTEGER NOT NULL DEFAULT 1,
  job_id             INTEGER REFERENCES jobs(id),
  task_id            INTEGER REFERENCES tasks(id),
  created_at         TEXT NOT NULL
);

CREATE TABLE profile_facts (
  id                 INTEGER PRIMARY KEY,
  category           TEXT NOT NULL CHECK (category IN (
                       'education','experience','project','skill','award',
                       'publication','activity','other')),
  title              TEXT NOT NULL,
  organization       TEXT,
  start_date         TEXT,
  end_date           TEXT,
  description        TEXT NOT NULL,
  metrics            TEXT NOT NULL DEFAULT '{}' CHECK (json_valid(metrics)),
  tags               TEXT NOT NULL DEFAULT '[]' CHECK (json_valid(tags)),
  technologies       TEXT NOT NULL DEFAULT '[]' CHECK (json_valid(technologies)),
  source_document_id INTEGER REFERENCES documents(id),
  verified           INTEGER NOT NULL DEFAULT 0 CHECK (verified IN (0,1)),
  active             INTEGER NOT NULL DEFAULT 1 CHECK (active IN (0,1)),
  created_at         TEXT NOT NULL,
  updated_at         TEXT NOT NULL
);

CREATE TABLE skills (
  id                INTEGER PRIMARY KEY,
  name              TEXT NOT NULL UNIQUE,
  category          TEXT,
  proficiency       TEXT,
  evidence_fact_ids TEXT NOT NULL DEFAULT '[]' CHECK (json_valid(evidence_fact_ids))
);

CREATE TABLE style_profiles (
  id                  INTEGER PRIMARY KEY,
  features            TEXT NOT NULL CHECK (json_valid(features)),
  sample_document_ids TEXT NOT NULL CHECK (json_valid(sample_document_ids)),
  computed_at         TEXT NOT NULL
);

-- ======================================================================== OPERATIONAL: jobs
CREATE TABLE companies (
  id              INTEGER PRIMARY KEY,
  name            TEXT NOT NULL,
  normalized_name TEXT NOT NULL UNIQUE,
  domain          TEXT,
  headquarters    TEXT,
  size_band       TEXT,
  industry        TEXT,
  careers_url     TEXT,
  ats_type        TEXT,
  ats_slug        TEXT,
  notes           TEXT,
  created_at      TEXT NOT NULL
);

CREATE TABLE company_research (
  id            INTEGER PRIMARY KEY,
  company_id    INTEGER NOT NULL REFERENCES companies(id) ON DELETE CASCADE,
  topic         TEXT NOT NULL,
  summary       TEXT,
  facts         TEXT NOT NULL DEFAULT '[]' CHECK (json_valid(facts)),
  sources       TEXT NOT NULL DEFAULT '[]' CHECK (json_valid(sources)),
  confidence    REAL CHECK (confidence IS NULL OR confidence BETWEEN 0 AND 1),
  depth         INTEGER,
  task_id       INTEGER REFERENCES tasks(id),
  researched_at TEXT NOT NULL,
  expires_at    TEXT NOT NULL,
  UNIQUE (company_id, topic)
);

CREATE TABLE job_sources (
  id                 INTEGER PRIMARY KEY,
  kind               TEXT NOT NULL CHECK (kind IN ('greenhouse','lever','ashby','rss','manual','url')),
  config             TEXT NOT NULL DEFAULT '{}' CHECK (json_valid(config)),
  enabled            INTEGER NOT NULL DEFAULT 1 CHECK (enabled IN (0,1)),
  rate_limit_per_min INTEGER NOT NULL DEFAULT 10,
  last_polled_at     TEXT,
  last_status        TEXT
);

CREATE TABLE jobs (
  id               INTEGER PRIMARY KEY,
  company_id       INTEGER NOT NULL REFERENCES companies(id),
  source_id        INTEGER REFERENCES job_sources(id),
  external_id      TEXT,
  url              TEXT,
  title            TEXT NOT NULL,
  role_family      TEXT CHECK (role_family IS NULL OR role_family IN
                     ('ai_ml','swe','embedded','data','other')),
  location_raw     TEXT,
  countries        TEXT NOT NULL DEFAULT '[]' CHECK (json_valid(countries)),
  remote_type      TEXT NOT NULL DEFAULT 'unknown' CHECK (remote_type IN
                     ('onsite','hybrid','remote','unknown')),
  employment_type  TEXT,
  term             TEXT,
  posted_at        TEXT,
  closes_at        TEXT,
  description_raw  TEXT,
  description_hash TEXT,
  dedupe_key       TEXT NOT NULL,
  status           TEXT NOT NULL DEFAULT 'open' CHECK (status IN ('open','closed','unknown')),
  first_seen_at    TEXT NOT NULL,
  last_seen_at     TEXT NOT NULL,
  UNIQUE (source_id, external_id)
);
CREATE INDEX idx_jobs_dedupe ON jobs(dedupe_key);
CREATE INDEX idx_jobs_company ON jobs(company_id);

CREATE TABLE job_analysis (
  job_id                 INTEGER PRIMARY KEY REFERENCES jobs(id) ON DELETE CASCADE,
  required_skills        TEXT NOT NULL DEFAULT '[]' CHECK (json_valid(required_skills)),
  preferred_skills       TEXT NOT NULL DEFAULT '[]' CHECK (json_valid(preferred_skills)),
  keywords               TEXT NOT NULL DEFAULT '[]' CHECK (json_valid(keywords)),
  degree_requirements    TEXT,
  graduation_window      TEXT,
  work_auth_requirements TEXT,
  responsibilities       TEXT NOT NULL DEFAULT '[]' CHECK (json_valid(responsibilities)),
  seniority              TEXT,
  method                 TEXT NOT NULL,      -- deterministic | model:<id>
  analysis_version       INTEGER NOT NULL DEFAULT 1,
  task_id                INTEGER REFERENCES tasks(id),
  analyzed_at            TEXT NOT NULL
);

CREATE TABLE eligibility_assessments (
  id                INTEGER PRIMARY KEY,
  job_id            INTEGER NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
  profile_version   INTEGER NOT NULL,
  verdict           TEXT NOT NULL CHECK (verdict IN
                      ('eligible','ineligible','needs_user_confirmation','uncertain')),
  reasons           TEXT NOT NULL DEFAULT '[]' CHECK (json_valid(reasons)),
  blocking_criteria TEXT NOT NULL DEFAULT '[]' CHECK (json_valid(blocking_criteria)),
  unknown_inputs    TEXT NOT NULL DEFAULT '[]' CHECK (json_valid(unknown_inputs)),
  confidence        REAL CHECK (confidence IS NULL OR confidence BETWEEN 0 AND 1),
  task_id           INTEGER REFERENCES tasks(id),
  assessed_at       TEXT NOT NULL,
  UNIQUE (job_id, profile_version),
  -- A3: if any input is unknown the verdict cannot be a confident yes.
  CHECK (unknown_inputs = '[]' OR verdict <> 'eligible')
);

CREATE TABLE match_scores (
  id              INTEGER PRIMARY KEY,
  job_id          INTEGER NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
  profile_version INTEGER NOT NULL,
  score           REAL NOT NULL CHECK (score BETWEEN 0 AND 1),
  components      TEXT NOT NULL DEFAULT '{}' CHECK (json_valid(components)),
  rationale       TEXT,
  top_fact_ids    TEXT NOT NULL DEFAULT '[]' CHECK (json_valid(top_fact_ids)),
  task_id         INTEGER REFERENCES tasks(id),
  scored_at       TEXT NOT NULL,
  UNIQUE (job_id, profile_version)
);

-- ======================================================================== PERSONAL: applications
CREATE TABLE applications (
  id                       INTEGER PRIMARY KEY,
  job_id                   INTEGER NOT NULL UNIQUE REFERENCES jobs(id),
  status                   TEXT NOT NULL CHECK (status IN (
                             'shortlisted','preparing','ready_for_review','submitted',
                             'assessment','interview','offer','accepted','rejected',
                             'withdrawn','no_response')),
  priority                 TEXT NOT NULL DEFAULT 'normal' CHECK (priority IN
                             ('low','normal','high','critical')),
  deadline                 TEXT,
  resume_document_id       INTEGER REFERENCES documents(id),
  cover_letter_document_id INTEGER REFERENCES documents(id),
  answers_document_id      INTEGER REFERENCES documents(id),
  objective_id             INTEGER REFERENCES objectives(id),
  submitted_at             TEXT,
  outcome_at               TEXT,
  notes                    TEXT,
  created_at               TEXT NOT NULL,
  updated_at               TEXT NOT NULL
);

CREATE TABLE application_events (
  id             INTEGER PRIMARY KEY,
  application_id INTEGER NOT NULL REFERENCES applications(id) ON DELETE CASCADE,
  event_type     TEXT NOT NULL,
  from_status    TEXT,
  to_status      TEXT,
  details        TEXT NOT NULL DEFAULT '{}' CHECK (json_valid(details)),
  occurred_at    TEXT NOT NULL
);
CREATE INDEX idx_app_events_app ON application_events(application_id);

-- ======================================================================== OPERATIONAL: orchestration
CREATE TABLE objectives (
  id                 INTEGER PRIMARY KEY,
  kind               TEXT NOT NULL,
  title              TEXT NOT NULL,
  inputs             TEXT NOT NULL DEFAULT '{}' CHECK (json_valid(inputs)),
  definition_of_done TEXT NOT NULL CHECK (json_valid(definition_of_done)),
  priority           TEXT NOT NULL DEFAULT 'normal' CHECK (priority IN
                       ('low','normal','high','critical')),
  budget_usd         REAL CHECK (budget_usd IS NULL OR budget_usd >= 0),
  budget_tokens      INTEGER CHECK (budget_tokens IS NULL OR budget_tokens >= 0),
  status             TEXT NOT NULL DEFAULT 'active' CHECK (status IN
                       ('active','done','stopped','awaiting_human','failed')),
  stop_reason        TEXT,
  created_at         TEXT NOT NULL,
  updated_at         TEXT NOT NULL,
  completed_at       TEXT
);

CREATE TABLE plans (
  id           INTEGER PRIMARY KEY,
  objective_id INTEGER NOT NULL REFERENCES objectives(id) ON DELETE CASCADE,
  version      INTEGER NOT NULL,
  source       TEXT NOT NULL,                -- playbook:<name> | model:<call_id> | manual
  rationale    TEXT,
  graph        TEXT NOT NULL DEFAULT '{}' CHECK (json_valid(graph)),
  superseded_at TEXT,
  created_at   TEXT NOT NULL,
  UNIQUE (objective_id, version)
);

CREATE TABLE tasks (
  id                 INTEGER PRIMARY KEY,
  objective_id       INTEGER NOT NULL REFERENCES objectives(id) ON DELETE CASCADE,
  plan_id            INTEGER NOT NULL REFERENCES plans(id),
  parent_task_id     INTEGER REFERENCES tasks(id),
  worker             TEXT NOT NULL,
  step               TEXT,
  task_type          TEXT NOT NULL,
  title              TEXT,
  input              TEXT NOT NULL DEFAULT '{}' CHECK (json_valid(input)),
  acceptance_criteria TEXT NOT NULL DEFAULT '[]' CHECK (json_valid(acceptance_criteria)),
  priority           TEXT NOT NULL DEFAULT 'normal' CHECK (priority IN
                       ('low','normal','high','critical')),
  important          INTEGER NOT NULL DEFAULT 0 CHECK (important IN (0,1)),
  complexity         REAL CHECK (complexity IS NULL OR complexity BETWEEN 0 AND 1),
  status             TEXT NOT NULL DEFAULT 'pending' CHECK (status IN (
                       'pending','ready','running','reviewing','retry_scheduled','succeeded',
                       'failed','blocked_budget','awaiting_human','cancelled','skipped')),
  attempt            INTEGER NOT NULL DEFAULT 0 CHECK (attempt >= 0),
  max_attempts       INTEGER NOT NULL DEFAULT 3 CHECK (max_attempts >= 1),
  escalations        INTEGER NOT NULL DEFAULT 0 CHECK (escalations >= 0),
  current_tier       INTEGER CHECK (current_tier IS NULL OR current_tier BETWEEN 1 AND 3),
  current_effort     TEXT CHECK (current_effort IS NULL OR current_effort IN
                       ('none','low','medium','high','max')),
  idempotency_key    TEXT UNIQUE,
  lease_owner        TEXT REFERENCES execution_sessions(id),
  lease_expires_at   TEXT,
  budget_usd         REAL CHECK (budget_usd IS NULL OR budget_usd >= 0),
  spent_usd          REAL NOT NULL DEFAULT 0 CHECK (spent_usd >= 0),
  accepted_result_id INTEGER REFERENCES task_results(id),
  last_error         TEXT,
  created_at         TEXT NOT NULL,
  updated_at         TEXT NOT NULL,
  started_at         TEXT,
  finished_at        TEXT
);
CREATE INDEX idx_tasks_status ON tasks(status, priority);
CREATE INDEX idx_tasks_objective ON tasks(objective_id);

CREATE TABLE task_dependencies (
  task_id            INTEGER NOT NULL REFERENCES tasks(id) ON DELETE CASCADE,
  depends_on_task_id INTEGER NOT NULL REFERENCES tasks(id) ON DELETE CASCADE,
  kind               TEXT NOT NULL DEFAULT 'hard' CHECK (kind IN ('hard','soft')),
  PRIMARY KEY (task_id, depends_on_task_id),
  CHECK (task_id <> depends_on_task_id)
);

CREATE TABLE task_results (
  id                     INTEGER PRIMARY KEY,
  task_id                INTEGER NOT NULL REFERENCES tasks(id) ON DELETE CASCADE,
  attempt                INTEGER NOT NULL,
  schema_version         TEXT NOT NULL,
  status                 TEXT NOT NULL CHECK (status IN ('succeeded','partial','failed','needs_input')),
  output                 TEXT NOT NULL DEFAULT '{}' CHECK (json_valid(output)),
  evidence               TEXT NOT NULL DEFAULT '[]' CHECK (json_valid(evidence)),
  assumptions            TEXT NOT NULL DEFAULT '[]' CHECK (json_valid(assumptions)),
  confidence             REAL NOT NULL CHECK (confidence BETWEEN 0 AND 1),
  effective_confidence   REAL CHECK (effective_confidence IS NULL OR effective_confidence BETWEEN 0 AND 1),
  unresolved_questions   TEXT NOT NULL DEFAULT '[]' CHECK (json_valid(unresolved_questions)),
  recommended_next_steps TEXT NOT NULL DEFAULT '[]' CHECK (json_valid(recommended_next_steps)),
  metrics                TEXT NOT NULL DEFAULT '{}' CHECK (json_valid(metrics)),
  created_at             TEXT NOT NULL
);
CREATE INDEX idx_task_results_task ON task_results(task_id);

-- A7: multi-step workers resume from their last completed step.
CREATE TABLE task_checkpoints (
  id         INTEGER PRIMARY KEY,
  task_id    INTEGER NOT NULL REFERENCES tasks(id) ON DELETE CASCADE,
  attempt    INTEGER NOT NULL,
  step       TEXT NOT NULL,
  state      TEXT NOT NULL CHECK (json_valid(state)),
  created_at TEXT NOT NULL,
  UNIQUE (task_id, attempt, step)
);

-- One row per manager/scheduler invocation; owns task leases.
CREATE TABLE execution_sessions (
  id                TEXT PRIMARY KEY,
  started_at        TEXT NOT NULL,
  last_heartbeat_at TEXT,
  ended_at          TEXT,
  end_reason        TEXT,
  host              TEXT
);

CREATE TABLE reviews (
  id            INTEGER PRIMARY KEY,
  task_id       INTEGER NOT NULL REFERENCES tasks(id) ON DELETE CASCADE,
  result_id     INTEGER NOT NULL REFERENCES task_results(id) ON DELETE CASCADE,
  reviewer      TEXT NOT NULL,
  verdict       TEXT NOT NULL CHECK (verdict IN ('accept','revise','escalate','reject')),
  score         REAL CHECK (score IS NULL OR score BETWEEN 0 AND 1),
  rubric_scores TEXT NOT NULL DEFAULT '{}' CHECK (json_valid(rubric_scores)),
  issues        TEXT NOT NULL DEFAULT '[]' CHECK (json_valid(issues)),
  model_call_id INTEGER REFERENCES model_calls(id),
  created_at    TEXT NOT NULL
);

CREATE TABLE manager_decisions (
  id           INTEGER PRIMARY KEY,
  objective_id INTEGER REFERENCES objectives(id) ON DELETE CASCADE,
  task_id      INTEGER REFERENCES tasks(id) ON DELETE CASCADE,
  decision     TEXT NOT NULL CHECK (decision IN (
                 'plan','replan','route','accept','retry_same','retry_with_feedback',
                 'escalate_effort','escalate_tier','skip','cancel','await_human','stop')),
  rationale    TEXT,
  inputs       TEXT NOT NULL DEFAULT '{}' CHECK (json_valid(inputs)),
  created_at   TEXT NOT NULL
);

-- ======================================================================== OPERATIONAL: models & cost
CREATE TABLE model_profiles (
  id                  TEXT PRIMARY KEY,
  provider            TEXT NOT NULL,
  tier                INTEGER NOT NULL CHECK (tier BETWEEN 1 AND 3),
  capabilities        TEXT NOT NULL DEFAULT '[]' CHECK (json_valid(capabilities)),
  context_window      INTEGER,
  effort_levels       TEXT NOT NULL DEFAULT '[]' CHECK (json_valid(effort_levels)),
  input_per_mtok      REAL,
  output_per_mtok     REAL,
  pricing_simulated   INTEGER NOT NULL CHECK (pricing_simulated IN (0,1)),
  pricing_verified_at TEXT,
  enabled             INTEGER NOT NULL CHECK (enabled IN (0,1)),
  config_hash         TEXT,
  updated_at          TEXT NOT NULL
);

CREATE TABLE model_calls (
  id                INTEGER PRIMARY KEY,
  task_id           INTEGER REFERENCES tasks(id) ON DELETE SET NULL,
  attempt           INTEGER,
  model_id          TEXT NOT NULL REFERENCES model_profiles(id),
  provider          TEXT NOT NULL,
  tier              INTEGER,
  effort            TEXT,
  mode              TEXT NOT NULL CHECK (mode IN ('mock','scripted','manual','real')),
  template_id       TEXT,
  template_version  TEXT,
  est_input_tokens  INTEGER,
  est_output_tokens INTEGER,
  input_tokens      INTEGER,
  output_tokens     INTEGER,
  cached_tokens     INTEGER,
  est_cost_usd      REAL,
  cost_usd          REAL,
  cost_is_simulated INTEGER NOT NULL CHECK (cost_is_simulated IN (0,1)),
  latency_ms        INTEGER,
  status            TEXT NOT NULL,
  error             TEXT,
  cache_key         TEXT,
  cache_hit         INTEGER NOT NULL DEFAULT 0 CHECK (cache_hit IN (0,1)),
  created_at        TEXT NOT NULL,
  -- Simulated (non-real) calls must never be recorded as real spend.
  CHECK (mode = 'real' OR cost_is_simulated = 1)
);
CREATE INDEX idx_model_calls_task ON model_calls(task_id);

CREATE TABLE budgets (
  id           INTEGER PRIMARY KEY,
  scope        TEXT NOT NULL CHECK (scope IN ('global_month','objective','task')),
  scope_ref    TEXT NOT NULL,
  period       TEXT NOT NULL DEFAULT '',
  limit_usd    REAL NOT NULL CHECK (limit_usd >= 0),
  limit_tokens INTEGER,
  spent_usd    REAL NOT NULL DEFAULT 0 CHECK (spent_usd >= 0),
  reserved_usd REAL NOT NULL DEFAULT 0 CHECK (reserved_usd >= 0),
  mode         TEXT NOT NULL CHECK (mode IN ('simulated','real')),
  UNIQUE (scope, scope_ref, period, mode)
);

CREATE TABLE cache_entries (
  key        TEXT PRIMARY KEY,
  kind       TEXT NOT NULL,
  value      TEXT,
  blob_key   TEXT,
  created_at TEXT NOT NULL,
  expires_at TEXT,
  hits       INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE routing_stats (
  task_type         TEXT NOT NULL,
  complexity_bucket TEXT NOT NULL,
  model_id          TEXT NOT NULL,
  effort            TEXT NOT NULL,
  attempts          INTEGER NOT NULL DEFAULT 0,
  accepts           INTEGER NOT NULL DEFAULT 0,
  prior_alpha       REAL NOT NULL,
  prior_beta        REAL NOT NULL,
  avg_quality       REAL,
  avg_cost_usd      REAL,
  avg_latency_ms    REAL,
  updated_at        TEXT,
  PRIMARY KEY (task_type, complexity_bucket, model_id, effort),
  CHECK (accepts <= attempts)
);

CREATE TABLE approvals (
  id         INTEGER PRIMARY KEY,
  kind       TEXT NOT NULL CHECK (kind IN ('enable_real_models','budget_increase','network_source','other')),
  scope      TEXT,
  details    TEXT NOT NULL DEFAULT '{}' CHECK (json_valid(details)),
  max_usd    REAL,
  granted_at TEXT NOT NULL,
  expires_at TEXT,
  revoked_at TEXT
);

-- ======================================================================== reporting
CREATE VIEW v_cost_per_success AS
SELECT t.task_type,
       COUNT(DISTINCT CASE WHEN t.status = 'succeeded' THEN t.id END) AS successes,
       COALESCE(SUM(c.cost_usd), 0)                                   AS total_cost_usd,
       COALESCE(SUM(c.cost_usd), 0) * 1.0 /
         NULLIF(COUNT(DISTINCT CASE WHEN t.status = 'succeeded' THEN t.id END), 0)
                                                                     AS cost_per_success_usd
FROM tasks t
LEFT JOIN model_calls c ON c.task_id = t.id
GROUP BY t.task_type;

CREATE VIEW v_outstanding_tasks AS
SELECT t.*, o.title AS objective_title
FROM tasks t JOIN objectives o ON o.id = t.objective_id
WHERE t.status IN ('pending','ready','running','reviewing','retry_scheduled',
                   'blocked_budget','awaiting_human')
  AND o.status IN ('active','awaiting_human');
