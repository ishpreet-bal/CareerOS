-- C5: planning as an owned, recoverable phase; atomic plan installation (L9, N3, N5).

-- At most one current plan per objective. Older code could (in a race) leave several:
-- keep the newest version current, supersede the others.
UPDATE plans SET superseded_at = created_at
 WHERE superseded_at IS NULL
   AND id NOT IN (SELECT p.id FROM plans p
                   WHERE p.superseded_at IS NULL
                     AND p.version = (SELECT MAX(q.version) FROM plans q
                                       WHERE q.objective_id = p.objective_id
                                         AND q.superseded_at IS NULL));
CREATE UNIQUE INDEX idx_plans_one_current ON plans(objective_id) WHERE superseded_at IS NULL;

-- Explicit task-to-plan association: a task belongs to the plan version that lists its
-- step. Older code kept tasks on the version that created them when a replan created a
-- new version; carry them over to the current version if it lists their step.
UPDATE tasks
   SET plan_id = (SELECT p.id FROM plans p
                   WHERE p.objective_id = tasks.objective_id AND p.superseded_at IS NULL)
 WHERE EXISTS (SELECT 1 FROM plans p, json_each(p.graph, '$.steps') s
                WHERE p.objective_id = tasks.objective_id AND p.superseded_at IS NULL
                  AND p.id <> tasks.plan_id
                  AND json_extract(s.value, '$.id') = tasks.step);

-- Dependency rows the current plan's graph requires but a partial installation (finding
-- N5) never wrote. Tasks missing altogether are created on the next run (the manager
-- completes the current plan in one transaction before dispatching), and until then the
-- claim refuses any task whose dependency rows do not match its plan.
INSERT OR IGNORE INTO task_dependencies (task_id, depends_on_task_id, kind)
SELECT t.id, d.id, 'hard'
  FROM tasks t
  JOIN plans p ON p.id = t.plan_id AND p.superseded_at IS NULL
  JOIN json_each(p.graph, '$.steps') s ON json_extract(s.value, '$.id') = t.step
  JOIN json_each(s.value, '$.depends_on') dep
  JOIN tasks d ON d.objective_id = t.objective_id AND d.step = dep.value AND d.id <> t.id;

-- Planning runs as a task of kind 'planning' (owned, leased, heartbeated, recoverable).
ALTER TABLE tasks ADD COLUMN kind TEXT NOT NULL DEFAULT 'step'
  CHECK (kind IN ('step','planning'));

-- The user's answer to a clarification request, used when planning resumes (N3).
ALTER TABLE approval_requests ADD COLUMN answer TEXT;
