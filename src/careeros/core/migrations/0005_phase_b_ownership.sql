-- Phase B: task ownership (fencing), late replies, atomic transitions.

-- ---------------------------------------------------------------- ownership
-- owner_generation: incremented by every claim; the fencing token is (task, generation).
-- attempt_open: 1 while the current attempt has not produced an outcome (interrupted,
-- requeued, or blocked before an answer); the next claim then resumes the same attempt.
ALTER TABLE tasks ADD COLUMN owner_generation INTEGER NOT NULL DEFAULT 0
  CHECK (owner_generation >= 0);
ALTER TABLE tasks ADD COLUMN attempt_open INTEGER NOT NULL DEFAULT 0
  CHECK (attempt_open IN (0,1));

-- Backfill attempt_open from each task's real history. The pre-Phase-B code *handed back*
-- an attempt (attempt - 1) when it blocked on budget, requeued for contention or
-- recovered an interrupted task; such attempts are still open, so they are restored as
-- (attempt + 1, open). Running tasks already hold their claimed attempt.
UPDATE tasks SET attempt_open = 1 WHERE status IN ('running','reviewing');

UPDATE tasks SET attempt = attempt + 1, attempt_open = 1 WHERE status = 'blocked_budget';

-- Ready/pending tasks whose most recent outcome-relevant decision handed the attempt back.
UPDATE tasks SET attempt = attempt + 1, attempt_open = 1
WHERE status IN ('ready','pending') AND attempt_open = 0 AND EXISTS (
  SELECT 1 FROM manager_decisions d
  WHERE d.id = (SELECT MAX(d2.id) FROM manager_decisions d2
                WHERE d2.task_id = tasks.id
                  AND d2.decision NOT IN ('route','degrade','plan','replan','skip'))
    AND (d.decision IN ('requeue','recover')
         OR (d.decision = 'retry_same' AND json_extract(d.inputs, '$.recovered') = 1))
);

-- Ready tasks released by an approved *budget* request: the blocked attempt was handed
-- back when it blocked, so it is still open. (An approved retry-limit request closed its
-- attempt: those stay closed.)
UPDATE tasks SET attempt = attempt + 1, attempt_open = 1
WHERE status = 'ready' AND attempt_open = 0 AND EXISTS (
  SELECT 1 FROM approval_requests r
  WHERE r.id = (SELECT MAX(r2.id) FROM approval_requests r2 WHERE r2.task_id = tasks.id)
    AND r.kind = 'budget_increase' AND r.status = 'approved'
) AND NOT EXISTS (  -- and no attempt outcome was recorded after that approval
  SELECT 1 FROM manager_decisions d
  WHERE d.task_id = tasks.id
    AND d.decision IN ('accept','retry_same','retry_with_feedback','escalate_effort',
                       'escalate_tier','worker_error')
    AND d.created_at > (SELECT MAX(r3.resolved_at) FROM approval_requests r3
                        WHERE r3.task_id = tasks.id)
);

-- ---------------------------------------------------------------- audit of writers
ALTER TABLE manager_decisions ADD COLUMN owner_generation INTEGER;
ALTER TABLE task_checkpoints ADD COLUMN owner_generation INTEGER;

-- ---------------------------------------------------------------- model calls
ALTER TABLE model_calls ADD COLUMN owner_generation INTEGER;
-- Fingerprint of the logical request (task type, prompt, schema); replay must match it.
ALTER TABLE model_calls ADD COLUMN request_hash TEXT;
-- Identifier assigned by the provider (distinct from our local idempotency key).
ALTER TABLE model_calls ADD COLUMN provider_request_id TEXT;
CREATE INDEX idx_model_calls_task_gen ON model_calls(task_id, owner_generation, status);

-- A reply that arrived after its call stopped being authoritative (abandoned by recovery,
-- or its task's ownership was lost). Never used as a task result. One row per call:
-- duplicate delivery cannot apply its accounting twice.
CREATE TABLE late_replies (
  id                  INTEGER PRIMARY KEY,
  call_id             INTEGER NOT NULL UNIQUE REFERENCES model_calls(id),
  task_id             INTEGER REFERENCES tasks(id) ON DELETE SET NULL,
  owner_generation    INTEGER,
  session_id          TEXT,
  received_at         TEXT NOT NULL,
  status              TEXT NOT NULL,
  input_tokens        INTEGER NOT NULL DEFAULT 0,
  output_tokens       INTEGER NOT NULL DEFAULT 0,
  actual_cost_usd     REAL NOT NULL,
  reserved_usd        REAL,
  charged_before_usd  REAL NOT NULL,
  adjustment_usd      REAL NOT NULL,
  overshoot_usd       REAL NOT NULL DEFAULT 0 CHECK (overshoot_usd >= 0),
  adjustment_applied  INTEGER NOT NULL DEFAULT 0 CHECK (adjustment_applied IN (0,1)),
  provider_request_id TEXT,
  response_json       TEXT
);

-- ---------------------------------------------------------------- one result per attempt
-- Pre-existing duplicates (possible with the pre-Phase-B late-writer bug) are archived,
-- never deleted. Kept row: the accepted result if any, else the earliest.
CREATE TABLE task_results_duplicates AS SELECT * FROM task_results WHERE 0;
ALTER TABLE task_results_duplicates ADD COLUMN archived_at TEXT;
ALTER TABLE task_results_duplicates ADD COLUMN kept_result_id INTEGER;

CREATE TEMP TABLE _keep AS
  SELECT r.task_id, r.attempt,
         COALESCE(
           (SELECT t.accepted_result_id FROM tasks t
             WHERE t.id = r.task_id AND t.accepted_result_id IN
               (SELECT id FROM task_results x WHERE x.task_id = r.task_id
                                                AND x.attempt = r.attempt)),
           MIN(r.id)) AS keep_id
  FROM task_results r GROUP BY r.task_id, r.attempt HAVING COUNT(*) > 1;

INSERT INTO task_results_duplicates
  SELECT r.*, datetime('now'), k.keep_id FROM task_results r
  JOIN _keep k ON k.task_id = r.task_id AND k.attempt = r.attempt AND r.id <> k.keep_id;
UPDATE reviews SET result_id = (SELECT kept_result_id FROM task_results_duplicates d
                                WHERE d.id = reviews.result_id)
  WHERE result_id IN (SELECT id FROM task_results_duplicates);
DELETE FROM task_results WHERE id IN (SELECT id FROM task_results_duplicates);
DROP TABLE _keep;
CREATE UNIQUE INDEX idx_task_results_attempt ON task_results(task_id, attempt);

-- One review row per reviewed result (archived the same way).
CREATE TABLE reviews_duplicates AS SELECT * FROM reviews WHERE 0;
INSERT INTO reviews_duplicates
  SELECT * FROM reviews WHERE id NOT IN (SELECT MAX(id) FROM reviews GROUP BY task_id, result_id);
DELETE FROM reviews WHERE id IN (SELECT id FROM reviews_duplicates);
CREATE UNIQUE INDEX idx_reviews_result ON reviews(task_id, result_id);
