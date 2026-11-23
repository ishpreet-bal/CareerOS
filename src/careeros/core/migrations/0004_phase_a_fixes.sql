-- Phase A (post-M3 audit): budget overshoot accounting, per-route approvals, consistent
-- completion of approval requests.

-- M1: what was reserved for a call (worst case), the output cap sent to the provider, and
-- any amount by which the settled cost exceeded the reservation (recorded, never silent).
ALTER TABLE model_calls ADD COLUMN reserved_usd REAL;
ALTER TABLE model_calls ADD COLUMN output_cap_tokens INTEGER;
ALTER TABLE model_calls ADD COLUMN overshoot_usd REAL NOT NULL DEFAULT 0
  CHECK (overshoot_usd >= 0);

-- M6: a downgrade the user approved for the *review* of a task applies only to that review.
ALTER TABLE tasks ADD COLUMN approved_review_floor REAL
  CHECK (approved_review_floor IS NULL OR approved_review_floor BETWEEN 0 AND 1);

-- M3: requests made obsolete (objective finished, task no longer blocked) are 'withdrawn'.
CREATE TABLE approval_requests_new (
  id           INTEGER PRIMARY KEY,
  objective_id INTEGER NOT NULL REFERENCES objectives(id) ON DELETE CASCADE,
  task_id      INTEGER REFERENCES tasks(id) ON DELETE CASCADE,
  kind         TEXT NOT NULL CHECK (kind IN ('budget_increase','clarification','retry_limit')),
  details      TEXT NOT NULL DEFAULT '{}' CHECK (json_valid(details)),
  status       TEXT NOT NULL DEFAULT 'pending'
                 CHECK (status IN ('pending','approved','denied','withdrawn')),
  created_at   TEXT NOT NULL,
  resolved_at  TEXT,
  resolution   TEXT
);
INSERT INTO approval_requests_new
  (id, objective_id, task_id, kind, details, status, created_at, resolved_at, resolution)
  SELECT id, objective_id, task_id, kind, details, status, created_at, resolved_at, resolution
  FROM approval_requests;
DROP TABLE approval_requests;
ALTER TABLE approval_requests_new RENAME TO approval_requests;
CREATE INDEX idx_approval_requests_status ON approval_requests(status);
