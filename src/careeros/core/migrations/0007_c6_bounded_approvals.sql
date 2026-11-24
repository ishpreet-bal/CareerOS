-- C6: bounded, expiring, scoped, single-use approvals (L12).

-- Every request has an explicit expiry; NULL (requests created before this migration)
-- means created_at + the configured request lifetime, evaluated at decision time.
ALTER TABLE approval_requests ADD COLUMN expires_at TEXT;

-- Grants become scoped records: the objective, task and request that produced them, the
-- attempt they apply to, and the amounts they authorise. One grant per request at most
-- (database-enforced), so a request can never be consumed twice.
CREATE TABLE approvals_new (
  id             INTEGER PRIMARY KEY,
  kind           TEXT NOT NULL CHECK (kind IN ('enable_real_models','budget_increase',
                   'network_source','quality_downgrade','extra_attempts','other')),
  scope          TEXT,
  details        TEXT NOT NULL DEFAULT '{}' CHECK (json_valid(details)),
  max_usd        REAL,
  granted_at     TEXT NOT NULL,
  expires_at     TEXT,
  revoked_at     TEXT,
  objective_id   INTEGER REFERENCES objectives(id) ON DELETE CASCADE,
  task_id        INTEGER REFERENCES tasks(id) ON DELETE CASCADE,
  request_id     INTEGER REFERENCES approval_requests(id) ON DELETE CASCADE,
  attempt        INTEGER,
  purpose        TEXT,
  extra_usd      REAL CHECK (extra_usd IS NULL OR extra_usd >= 0),
  extra_attempts INTEGER CHECK (extra_attempts IS NULL OR extra_attempts >= 0)
);

-- Grants recorded twice for one request (possible before C1's fix, N1) are archived,
-- not lost: the first grant per request is kept.
CREATE TABLE approvals_duplicates (
  id INTEGER PRIMARY KEY, kind TEXT, scope TEXT, details TEXT, max_usd REAL,
  granted_at TEXT, expires_at TEXT, revoked_at TEXT, request_id INTEGER,
  kept_approval_id INTEGER, archived_at TEXT NOT NULL
);
INSERT INTO approvals_duplicates (id, kind, scope, details, max_usd, granted_at, expires_at,
                                  revoked_at, request_id, kept_approval_id, archived_at)
SELECT a.id, a.kind, a.scope, a.details, a.max_usd, a.granted_at, a.expires_at, a.revoked_at,
       json_extract(a.details, '$.request_id'),
       (SELECT MIN(b.id) FROM approvals b
         WHERE json_extract(b.details, '$.request_id') = json_extract(a.details, '$.request_id')),
       strftime('%Y-%m-%dT%H:%M:%S+00:00', 'now')
  FROM approvals a
 WHERE json_extract(a.details, '$.request_id') IS NOT NULL
   AND a.id > (SELECT MIN(b.id) FROM approvals b
                WHERE json_extract(b.details, '$.request_id')
                    = json_extract(a.details, '$.request_id'));

INSERT INTO approvals_new (id, kind, scope, details, max_usd, granted_at, expires_at,
                           revoked_at, objective_id, task_id, request_id, attempt, purpose,
                           extra_usd, extra_attempts)
SELECT a.id,
       CASE WHEN a.kind = 'other' AND r.kind = 'retry_limit' THEN 'extra_attempts'
            ELSE a.kind END,
       a.scope, a.details, a.max_usd, a.granted_at, a.expires_at, a.revoked_at,
       r.objective_id, r.task_id, r.id,
       (SELECT t.attempt FROM tasks t WHERE t.id = r.task_id),
       COALESCE(json_extract(a.details, '$.route_purpose'),
                CASE WHEN a.kind = 'quality_downgrade' THEN 'task' END),
       CASE WHEN a.kind = 'budget_increase' THEN a.max_usd END,
       NULL
  FROM approvals a
  LEFT JOIN approval_requests r ON r.id = json_extract(a.details, '$.request_id')
 WHERE a.id NOT IN (SELECT id FROM approvals_duplicates);

DROP TABLE approvals;
ALTER TABLE approvals_new RENAME TO approvals;
CREATE UNIQUE INDEX idx_approvals_one_per_request ON approvals(request_id)
  WHERE request_id IS NOT NULL;
CREATE INDEX idx_approvals_scope ON approvals(objective_id, task_id, kind);
