-- C7: factual evidence checks and recorded reviews (L11, N4).

-- Claims of a result (verified / unverified / assumption / uncertain), with evidence ids.
ALTER TABLE task_results ADD COLUMN claims TEXT NOT NULL DEFAULT '[]'
  CHECK (json_valid(claims));

-- Every check a result passed or failed before a decision: the deterministic evidence
-- check and the (model) review, with the policy version, the fingerprints of the payload
-- and of the sources it was judged against, and the reasons. One row per (result, kind):
-- repeated delivery or a resumed attempt updates the row instead of adding one. A review
-- reused for a cache hit is recorded for the new result with ``reused_from``.
CREATE TABLE result_checks (
  id               INTEGER PRIMARY KEY,
  task_id          INTEGER NOT NULL REFERENCES tasks(id) ON DELETE CASCADE,
  attempt          INTEGER NOT NULL,
  result_id        INTEGER NOT NULL REFERENCES task_results(id) ON DELETE CASCADE,
  owner_generation INTEGER,
  kind             TEXT NOT NULL CHECK (kind IN ('evidence','review')),
  checker          TEXT NOT NULL,
  policy_version   TEXT NOT NULL,
  payload_hash     TEXT NOT NULL,
  sources_hash     TEXT NOT NULL,
  passed           INTEGER NOT NULL CHECK (passed IN (0,1)),
  details          TEXT NOT NULL DEFAULT '{}' CHECK (json_valid(details)),
  model_call_id    INTEGER REFERENCES model_calls(id),
  reused_from      INTEGER REFERENCES result_checks(id),
  created_at       TEXT NOT NULL,
  UNIQUE (result_id, kind)
);
CREATE INDEX idx_result_checks_reuse
  ON result_checks(kind, payload_hash, sources_hash, policy_version, passed);
