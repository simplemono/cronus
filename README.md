# cronus

A cron scheduler on Cloudflare Workers that calls your URLs on time.
Written in [Squint](https://github.com/squint-cljs/squint), stored in
[D1](https://developers.cloudflare.com/d1/), the cron arithmetic by
[croner](https://github.com/Hexagon/croner).

You register a job with a cron expression, a time zone and a URL. Every
minute a Cloudflare cron trigger wakes the Worker, which fires what is
due and retries what failed. Your server only needs an endpoint that is
idempotent; cronus owns the clock.

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
npx wrangler secret put CRONUS_SECRET  # the bearer token of the API
npm run deploy                         # applies migrations/, then deploys the Worker
```

The schema lives in `migrations/` and `npm run deploy` applies what is
new with `wrangler d1 migrations apply` before every deployment, so a
later change to the schema reaches an existing database. For a second
deployment (say, a development one), copy `wrangler.toml` to another
name and pass `--config`.

A Workers paid plan ($5/month) is sensible: the free plan allows 50
subrequests per invocation, which the D1 statements share with the
calls, so about a dozen jobs per minute, and five cron triggers per
account.

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
  "url": "https://acme.example.com/hooks/report",
  "headers": {"X-Hook-Token": "..."},
  "body": {"report": "daily"},
  "timeout_ms": 30000,
  "attempts": 3
}'

curl $C/jobs/acme_report -H "$H"          # the job, its next run, its last 20 attempts
curl "$C/jobs?prefix=acme_" -H "$H"       # list; paginate with limit= and after=<last id>
curl -X DELETE $C/jobs/acme_report -H "$H"
```

| Field | Default | Notes |
| --- | --- | --- |
| `cron` | required | five fields, `*` lists ranges steps, names, `L` `W`, `@hourly` `@daily` `@weekly` `@monthly` `@yearly` |
| `tz` | `UTC` | an IANA zone; "9:00 Europe/Berlin" stays 9:00 across clock changes |
| `url` | required | a valid `http://` or `https://` URL |
| `method` | `POST` | GET, POST, PUT, PATCH or DELETE |
| `headers` | `{}` | strings only, usable header names and values, 4 KB in all |
| `body` | none | a string or a JSON object, 64 KB; without one a POST sends `{job, scheduled, attempt}` |
| `timeout_ms` | `30000` | a whole number, 1 s to 5 min |
| `attempts` | `3` | a whole number, 1 to 10 |

A definition that is wrong is refused with 400 and the reason, one
that is too big with 413. That is the whole API: there is no pause (a
paused job is a deleted one, your side keeps the definition), no manual
run (call the URL yourself, you hold its headers), no bulk delete and
no tenant concept.

## Delivery

Every call carries three headers your endpoint can use to be idempotent:

```
X-Cronus-Job: acme_report
X-Cronus-Scheduled: 2026-10-15T07:30:00.000Z
X-Cronus-Attempt: 1
```

A firing is attempted until the endpoint answers 2xx, up to `attempts`
times, one attempt per minute. A network error, a timeout, a 5xx and a
429 are retried; any other status ends the firing as failed, a 3xx
among them: redirects are not followed, so the job's headers never go
to another origin. That is at-least-once delivery: the endpoint may see
the same scheduled time twice, never with a different
`X-Cronus-Scheduled`.

A scheduled time that is less than an hour overdue (the Worker was
down, or a minute ran long) is fired once; an older one is skipped and
logged, so an outage does not replay a day of runs. The minimum
interval is one minute. The attempt log keeps the status, the duration
and the first 200 characters of the answer for the last 20 attempts of
each job; of the answer at most 64 KB are read, the rest is cancelled.

Clock changes follow croner: a time inside the hour the spring change
skips runs an hour later that day, a time inside the hour the autumn
change repeats runs once.

## Limits

A minute takes up to 300 due jobs, six at a time, within the 15 minutes
a scheduled handler may run; what is left waits for the next minute.
Before its call a firing leases the job, so a minute that overlaps the
previous one, or a Worker that died mid-call, never fires the same
scheduled time twice; a lease that is never released expires with the
job's timeout and the job is due again. An edit or a delete during a
call wins over the call's outcome. Not supported: seconds, `#`,
one-shot jobs.

## Development

```sh
npm install
npm test            # squint compile, node --test on the cron wrapper, vitest in workerd
npm run dev         # migrations into the local D1, wrangler dev --test-scheduled; curl "localhost:8787/__scheduled"
```

Sources are in `src/cronus/`, one namespace per concern: `cron` (the
expression and the next run, on croner), `store` (D1), `deliver` (one
firing), `api` (the routes) and `worker` (the two handlers). Squint
compiles them to ES modules in `dist/`. The tests under `test/unit/`
run with Node's test runner, the ones under `test/worker/` inside
workerd with a local D1 and a stub of the outbound `fetch`.

## License

MIT, see [LICENSE](LICENSE).
