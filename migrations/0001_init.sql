-- cronus: the jobs, each with its last attempt. Applied with
-- `wrangler d1 migrations apply <database>` (see package.json), locally
-- with `--local`, and in the tests through applyD1Migrations. A later
-- migration must keep the previous Worker working: `npm run deploy`
-- migrates before it deploys.
CREATE TABLE jobs (
  id TEXT PRIMARY KEY,
  cron TEXT NOT NULL,
  tz TEXT NOT NULL DEFAULT 'UTC',
  url TEXT NOT NULL,
  timeout_ms INTEGER NOT NULL DEFAULT 30000,
  attempts INTEGER NOT NULL DEFAULT 3,
  version INTEGER NOT NULL DEFAULT 1,
  next_run INTEGER,
  pending_scheduled INTEGER,
  pending_attempt INTEGER NOT NULL DEFAULT 0,
  lease_until INTEGER,
  created INTEGER NOT NULL,
  updated INTEGER NOT NULL,
  last_run INTEGER,
  last_status TEXT,
  last_http_status INTEGER,
  last_duration_ms INTEGER,
  last_answer TEXT,
  failures INTEGER NOT NULL DEFAULT 0
);
CREATE INDEX jobs_due ON jobs (next_run);
CREATE INDEX jobs_pending ON jobs (pending_scheduled);
