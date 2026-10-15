-- cronus: the jobs and their last attempts. Applied by the Worker itself
-- on first use (CREATE IF NOT EXISTS), or by hand with
-- `wrangler d1 execute cronus --file schema.sql`.
CREATE TABLE IF NOT EXISTS jobs (
  id TEXT PRIMARY KEY,
  cron TEXT NOT NULL,
  tz TEXT NOT NULL DEFAULT 'UTC',
  url TEXT NOT NULL,
  method TEXT NOT NULL DEFAULT 'POST',
  headers_json TEXT NOT NULL DEFAULT '{}',
  body TEXT,
  timeout_ms INTEGER NOT NULL DEFAULT 30000,
  attempts INTEGER NOT NULL DEFAULT 3,
  state TEXT NOT NULL DEFAULT 'active',
  next_run INTEGER,
  pending_scheduled INTEGER,
  pending_attempt INTEGER NOT NULL DEFAULT 0,
  created INTEGER NOT NULL,
  updated INTEGER NOT NULL,
  last_run INTEGER,
  last_status TEXT,
  failures INTEGER NOT NULL DEFAULT 0
);
CREATE INDEX IF NOT EXISTS jobs_due ON jobs (state, next_run);
CREATE INDEX IF NOT EXISTS jobs_pending ON jobs (state, pending_scheduled);

CREATE TABLE IF NOT EXISTS attempts (
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
CREATE INDEX IF NOT EXISTS attempts_by_job ON attempts (job_id, at);
