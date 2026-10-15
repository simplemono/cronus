# cronus

A cron scheduler on Cloudflare Workers that calls your URLs on time.
Written in [Squint](https://github.com/squint-cljs/squint), stored in
[D1](https://developers.cloudflare.com/d1/), the calls made from a
[Queue](https://developers.cloudflare.com/queues/), the cron arithmetic
by [croner](https://github.com/Hexagon/croner).

You register a job with a cron expression, a time zone and a URL. Every
minute a Cloudflare cron trigger wakes the Worker, which claims what is
due and queues it; the queue's consumer, the same Worker, makes one
POST per job and retries what failed. Your server only needs an
endpoint that is idempotent; cronus owns the clock.

## Why

Servers that run application code are a poor place for a clock: they
restart, deploy blue/green, scale out, and each of them has to agree on
who fires a job. Cloudflare's cron triggers run on Cloudflare's clock,
and a Worker with a D1 table is enough to keep thousands of schedules.

## Deploy

```sh
git clone https://github.com/simplemono/cronus
cd cronus
npm install
npx wrangler login
npx wrangler d1 create cronus          # copy the database_id into wrangler.toml
npx wrangler queues create cronus      # the calls are made from this queue
npx wrangler secret put CRONUS_SECRET  # the bearer token of the API
npm run deploy                         # applies migrations/, then deploys the Worker
```

The schema lives in `migrations/` and `npm run deploy` applies what is
new with `wrangler d1 migrations apply` before every deployment, so a
later change to the schema reaches an existing database. The previous
Worker keeps running against the migrated database until the new one
is out, so a migration must be one the previous Worker survives: add
columns and tables, do not drop or rename what it reads.

A Workers paid plan ($5/month) is needed for the queue; it also lifts
the free plan's limit of five cron triggers per account. Beyond the
included allowances a firing costs a few queue operations, about a
dollar per million.

For a second deployment (say, a development one), copy `wrangler.toml`
to another name and pass `--config`.

### As a dependency

A deployment can also be a wrangler project of its own that pins cronus
as a git dependency; `npm install` builds it through the `prepare`
script:

```sh
npm install github:simplemono/cronus#<commit>
```

```js
// worker.js
export { default } from "cronus/worker";
```

The project's `wrangler.toml` has the same bindings as cronus's
(`DB`, `QUEUE`, the minute trigger) and points `migrations_dir` at
`node_modules/cronus/migrations`; its deploy script runs
`wrangler d1 migrations apply <database> --remote` before
`wrangler deploy`.

## The API

Every request carries `Authorization: Bearer <CRONUS_SECRET>`.
Ids are opaque strings, URL-encoded in the path, up to 200 characters
without control characters. If you need a hierarchy, use `_` as the
separator (`<tenant>_<owner>_<job>`) and list by prefix.

```sh
C=https://cronus.<account>.workers.dev
H="Authorization: Bearer $CRONUS_SECRET"

# create or replace
curl -X PUT $C/jobs/acme_report -H "$H" -H 'content-type: application/json' -d '{
  "cron": "30 9 * * 1-5",
  "tz": "Europe/Berlin",
  "url": "https://acme.example.com/hooks/report?token=...",
  "timeout_ms": 30000,
  "attempts": 3
}'

curl $C/jobs/acme_report -H "$H"          # the job, its next run, its last attempt
curl "$C/jobs?prefix=acme_" -H "$H"       # list; paginate with limit= and after=<last id>
curl -X DELETE $C/jobs/acme_report -H "$H"
```

| Field | Default | Notes |
| --- | --- | --- |
| `cron` | required | five fields, `*` lists ranges steps, names, `L` `W` `#`, `@hourly` `@daily` `@weekly` `@monthly` `@yearly` |
| `tz` | `UTC` | an IANA zone; "9:00 Europe/Berlin" stays 9:00 across clock changes |
| `url` | required | a valid `http://` or `https://` URL; it is the credential, so put a token in it |
| `timeout_ms` | `30000` | a whole number, 1 s to 5 min |
| `attempts` | `3` | a whole number, 1 to 10 |

A definition that is wrong is refused with 400 and the reason. That is
the whole API: there is no pause (a paused job is a deleted one, your
side keeps the definition), no manual run (call the URL yourself, you
know it), no bulk delete and no tenant concept. `GET` returns the URL,
credential included, to any holder of the secret.

## Delivery

Every firing is a `POST` to the job's URL with the JSON body

```json
{"job": "acme_report", "scheduled": "2026-10-15T07:30:00.000Z", "attempt": 1}
```

which your endpoint can use to be idempotent. Nothing else is sent: no
custom headers, no custom body; a later `data` field of the job would
appear under its own key, never at the top level.

A firing is attempted until the endpoint answers 2xx, up to `attempts`
times, one attempt per minute. A network error, a timeout, a 5xx and a
429 are retried; any other status ends the firing as failed, a 3xx
among them: redirects are not followed, so the URL never travels on.
That is at-least-once delivery: the endpoint may see the same scheduled
time twice, never with a different `scheduled`. While a retry series
runs the schedule waits: an every-minute job whose target is down
fires its scheduled time, then, after the last attempt, the next one.

A scheduled time that is less than an hour overdue (the Worker was
down, or a backlog) is fired once, the minutes in between are not; an
older one is skipped and noted on the job, so an outage does not replay
a day of runs. The minimum interval is one minute. The job keeps its
last attempt: `last_status` (`ok`, `retrying`, `failed`, `skipped`),
`last_http_status`, `last_duration_ms` and `last_answer`, the first 200
characters of the answer; of the answer at most 64 KB are read, the
rest is cancelled.

Clock changes follow croner: a time inside the hour the spring change
skips runs an hour later that day, a time inside the hour the autumn
change repeats runs once.

## How a firing is kept to one

Before a job is queued, the minute leases it: a conditional update that
also counts the attempt and bumps the row's version. The consumer reads
the job again, and a version other than the one the lease wrote, or no
lease at all, means the message is stale and it does nothing: the lease
had expired and a later minute claimed again, a `PUT` replaced the job,
or the outcome is already written. So a minute that overlaps the
previous one never queues the same scheduled time twice, a message the
queue delivers twice calls once, and an edit or a delete during a call
wins over the call's outcome (the next minute may then fire the new
definition while the old call is still in flight, a different scheduled
time). A lease that is never released, because a consumer died
mid-call, expires with the job's timeout plus a minute, and the job is
due again: the attempt count was taken at the lease, so lost outcomes
use up the budget and can never fire forever.

## Limits

A minute claims up to 300 due jobs; what is left waits for the next
minute. The consumer takes one message per invocation, as many in
parallel as Cloudflare scales to, so a slow target delays nothing but
itself. Not supported: seconds, one-shot jobs.

## Development

```sh
npm install
echo 'CRONUS_SECRET=dev-secret' > .dev.vars   # the secret of your local Worker, git-ignored
npm test            # squint compile, node --test on the cron wrapper, vitest in workerd
npm run dev         # migrations into the local D1, wrangler dev --test-scheduled; curl "localhost:8787/__scheduled"
```

Sources are in `src/cronus/`, one namespace per concern: `cron` (the
expression and the next run, on croner), `store` (D1), `deliver` (the
minute and the consumer), `api` (the routes) and `worker` (the three
handlers). Squint compiles them to ES modules in `dist/`. The tests
under `test/unit/` run with Node's test runner, the ones under
`test/worker/` inside workerd with a local D1, a fake queue that hands
the minute's messages to the consumer, and a stub of the outbound
`fetch`.

## License

MIT, see [LICENSE](LICENSE).
