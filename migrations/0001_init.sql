-- cronus: the jobs and their last attempts. Applied with
-- `wrangler d1 migrations apply <database>` (see package.json), locally
-- with `--local`, and in the tests through applyD1Migrations.
CREATE TABLE jobs (
  id TEXT PRIMARY KEY,
  cron TEXT NOT NULL,
  tz TEXT NOT NULL DEFAULT 'UTC',
  url TEXT NOT NULL,
  method TEXT NOT NULL DEFAULT 'POST',
  headers_json TEXT NOT NULL DEFAULT '{}',
  body TEXT,
  timeout_ms INTEGER NOT NULL DEFAULT 30000,
  attempts INTEGER NOT NULL DEFAULT 3,
  next_run INTEGER,
  pending_scheduled INTEGER,
  pending_attempt INTEGER NOT NULL DEFAULT 0,
  lease_until INTEGER,
  created INTEGER NOT NULL,
  updated INTEGER NOT NULL,
  last_run INTEGER,
  last_status TEXT,
  failures INTEGER NOT NULL DEFAULT 0
);
CREATE INDEX jobs_due ON jobs (next_run);
CREATE INDEX jobs_pending ON jobs (pending_scheduled);

CREATE TABLE attempts (
  job_id TEXT NOT NULL,
  scheduled INTEGER NOT NULL,
  attempt INTEGER NOT NULL,
  mode TEXT NOT NULL,
  at INTEGER NOT NULL,
  status INTEGER,
  duration_ms INTEGER NOT NULL,
  answer TEXT,
  ok INTEGER NOT NULL
);
CREATE INDEX attempts_by_job ON attempts (job_id, at);
