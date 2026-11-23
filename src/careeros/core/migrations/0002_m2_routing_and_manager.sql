-- M2: model routing, cost accounting and manager state.

-- How a task is executed and which model the last attempt used (for escalation).
ALTER TABLE tasks ADD COLUMN mode TEXT NOT NULL DEFAULT 'model'
  CHECK (mode IN ('deterministic','model'));
ALTER TABLE tasks ADD COLUMN optional INTEGER NOT NULL DEFAULT 0 CHECK (optional IN (0,1));
ALTER TABLE tasks ADD COLUMN current_model_id TEXT;

-- Calls not tied to a task (e.g. model-assisted planning) still count against an objective.
ALTER TABLE model_calls ADD COLUMN objective_id INTEGER REFERENCES objectives(id);
ALTER TABLE model_calls ADD COLUMN purpose TEXT NOT NULL DEFAULT 'task'
  CHECK (purpose IN ('task','review','planning'));
CREATE INDEX idx_model_calls_objective ON model_calls(objective_id);

-- Requests for the user's decision (budget approvals, clarifications). Nothing is
-- approved automatically: a critical task that does not fit its budget waits here.
CREATE TABLE approval_requests (
  id           INTEGER PRIMARY KEY,
  objective_id INTEGER NOT NULL REFERENCES objectives(id) ON DELETE CASCADE,
  task_id      INTEGER REFERENCES tasks(id) ON DELETE CASCADE,
  kind         TEXT NOT NULL CHECK (kind IN ('budget_increase','clarification','retry_limit')),
  details      TEXT NOT NULL DEFAULT '{}' CHECK (json_valid(details)),
  status       TEXT NOT NULL DEFAULT 'pending' CHECK (status IN ('pending','approved','denied')),
  created_at   TEXT NOT NULL,
  resolved_at  TEXT
);
CREATE INDEX idx_approval_requests_status ON approval_requests(status);
