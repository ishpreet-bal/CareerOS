-- M3: bounded concurrent execution and the revised priority/downgrade policy.
-- SQLite cannot alter CHECK constraints, so three small tables are rebuilt with their data.

-- ---------------------------------------------------------------- manager decisions
-- New kinds: degrade (automatic, within limits), approve_downgrade (user approved),
-- recover (lease takeover after an interrupted session), worker_error, requeue.
CREATE TABLE manager_decisions_new (
  id           INTEGER PRIMARY KEY,
  objective_id INTEGER REFERENCES objectives(id) ON DELETE CASCADE,
  task_id      INTEGER REFERENCES tasks(id) ON DELETE CASCADE,
  decision     TEXT NOT NULL CHECK (decision IN (
                 'plan','replan','route','accept','retry_same','retry_with_feedback',
                 'escalate_effort','escalate_tier','skip','cancel','await_human','stop',
                 'degrade','approve_downgrade','recover','worker_error','requeue')),
  rationale    TEXT,
  inputs       TEXT NOT NULL DEFAULT '{}' CHECK (json_valid(inputs)),
  created_at   TEXT NOT NULL
);
INSERT INTO manager_decisions_new SELECT * FROM manager_decisions;
DROP TABLE manager_decisions;
ALTER TABLE manager_decisions_new RENAME TO manager_decisions;
CREATE INDEX idx_decisions_task ON manager_decisions(task_id);
CREATE INDEX idx_decisions_objective ON manager_decisions(objective_id);

-- ---------------------------------------------------------------- approvals
CREATE TABLE approvals_new (
  id         INTEGER PRIMARY KEY,
  kind       TEXT NOT NULL CHECK (kind IN ('enable_real_models','budget_increase',
               'network_source','quality_downgrade','other')),
  scope      TEXT,
  details    TEXT NOT NULL DEFAULT '{}' CHECK (json_valid(details)),
  max_usd    REAL,
  granted_at TEXT NOT NULL,
  expires_at TEXT,
  revoked_at TEXT
);
INSERT INTO approvals_new SELECT * FROM approvals;
DROP TABLE approvals;
ALTER TABLE approvals_new RENAME TO approvals;

ALTER TABLE approval_requests ADD COLUMN resolution TEXT;

-- ---------------------------------------------------------------- budgets (+ session scope)
CREATE TABLE budgets_new (
  id           INTEGER PRIMARY KEY,
  scope        TEXT NOT NULL CHECK (scope IN ('global_month','session','objective','task')),
  scope_ref    TEXT NOT NULL,
  period       TEXT NOT NULL DEFAULT '',
  limit_usd    REAL NOT NULL CHECK (limit_usd >= 0),
  limit_tokens INTEGER,
  spent_usd    REAL NOT NULL DEFAULT 0 CHECK (spent_usd >= 0),
  reserved_usd REAL NOT NULL DEFAULT 0 CHECK (reserved_usd >= 0),
  mode         TEXT NOT NULL CHECK (mode IN ('simulated','real')),
  UNIQUE (scope, scope_ref, period, mode)
);
INSERT INTO budgets_new SELECT * FROM budgets;
DROP TABLE budgets;
ALTER TABLE budgets_new RENAME TO budgets;

-- Every reservation is a row, owned by an execution session, so an interrupted session's
-- reservations can be found and settled or released without touching live ones.
CREATE TABLE budget_reservations (
  id          INTEGER PRIMARY KEY,
  session_id  TEXT REFERENCES execution_sessions(id),
  task_id     INTEGER REFERENCES tasks(id) ON DELETE SET NULL,
  budget_ids  TEXT NOT NULL CHECK (json_valid(budget_ids)),
  amount_usd  REAL NOT NULL CHECK (amount_usd >= 0),
  status      TEXT NOT NULL DEFAULT 'held' CHECK (status IN ('held','settled','released')),
  settled_usd REAL,
  created_at  TEXT NOT NULL,
  resolved_at TEXT
);
CREATE INDEX idx_reservations_held ON budget_reservations(status, session_id);

-- ---------------------------------------------------------------- model calls
-- A call row is written 'in_flight' (with its reservation) before the adapter is invoked,
-- and completed afterwards with the response, so an interrupted call is never lost or
-- silently repeated. The idempotency key makes a repeated request return the stored reply.
ALTER TABLE model_calls ADD COLUMN session_id TEXT;
ALTER TABLE model_calls ADD COLUMN idempotency_key TEXT;
ALTER TABLE model_calls ADD COLUMN reservation_id INTEGER REFERENCES budget_reservations(id);
ALTER TABLE model_calls ADD COLUMN response_json TEXT;
CREATE UNIQUE INDEX idx_model_calls_idem ON model_calls(idempotency_key)
  WHERE idempotency_key IS NOT NULL;

-- ---------------------------------------------------------------- tasks
-- A quality floor the user explicitly accepted for this task (approved downgrade).
ALTER TABLE tasks ADD COLUMN approved_floor REAL
  CHECK (approved_floor IS NULL OR approved_floor BETWEEN 0 AND 1);
