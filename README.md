# cronus

A cron scheduler on Cloudflare Workers that calls your URLs on time.
Written in [Squint](https://github.com/squint-cljs/squint), stored in
[D1](https://developers.cloudflare.com/d1/).

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
npm run deploy
```

The Worker applies `schema.sql` on first use. For a second deployment
(say, a development one), copy `wrangler.toml` to another name and pass
`--config`.

A Workers paid plan ($5/month) is sensible: the free plan allows 50
subrequests per invocation, so at most 50 jobs per minute, and five
cron triggers per account.

## The API

Every request carries `Authorization: Bearer <CRONUS_SECRET>`.
Ids are opaque strings, URL-encoded in the path. If you need a
hierarchy, use `_` as the separator (`<tenant>_<owner>_<job>`) and list
by prefix.

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
curl -X POST $C/jobs/acme_report/run -H "$H"     # fire now, through the same path
curl -X POST $C/jobs/acme_report/pause -H "$H"
curl -X POST $C/jobs/acme_report/resume -H "$H"
curl -X DELETE $C/jobs/acme_report -H "$H"
```

| Field | Default | Notes |
| --- | --- | --- |
| `cron` | required | five fields, `*` lists ranges steps, names, `@hourly` `@daily` `@weekly` `@monthly` |
| `tz` | `UTC` | an IANA zone; "9:00 Europe/Berlin" stays 9:00 across clock changes |
| `url` | required | `http://` or `https://` |
| `method` | `POST` | GET, POST, PUT, PATCH or DELETE |
| `headers` | `{}` | strings only |
| `body` | none | a string or a JSON object; without one a POST sends `{job, scheduled, attempt}` |
| `timeout_ms` | `30000` | 1 s to 5 min |
| `attempts` | `3` | 1 to 10 |

There is no bulk delete and no tenant concept: list by prefix, delete
by id.

## Delivery

Every call carries three headers your endpoint can use to be idempotent:

```
X-Cronus-Job: acme_report
X-Cronus-Scheduled: 2026-10-15T07:30:00.000Z
X-Cronus-Attempt: 1
```

A firing is attempted until the endpoint answers 2xx, up to `attempts`
times, one attempt per minute. A network error, a timeout, a 5xx and a
429 are retried; any other status ends the firing as failed. That is
at-least-once delivery: the endpoint may see the same scheduled time
twice, never with a different `X-Cronus-Scheduled`.

A scheduled time that is less than an hour overdue (the Worker was
down, or a minute ran long) is fired once; an older one is skipped and
logged, so an outage does not replay a day of runs. The minimum
interval is one minute. Redirects are not followed. The attempt log
keeps the status, the duration and the first 200 characters of the
answer for the last 20 attempts of each job.

## Limits

A minute fires up to 2,000 due jobs, six at a time, within the 15
minutes a scheduled handler may run. At two seconds per call that is
about 2,500 jobs per minute; a job that is due while the previous
minute is still running is picked up by the next minute, never twice
for the same scheduled time. Not supported: seconds, `L` `W` `#`,
one-shot jobs.

## Development

```sh
npm install
npm test            # squint compile, node --test on the cron parser, vitest in workerd
npm run dev         # wrangler dev --test-scheduled; curl "localhost:8787/__scheduled"
```

Sources are in `src/cronus/`, one namespace per concern: `cron` (the
expression and the next run), `store` (D1), `deliver` (one firing),
`api` (the routes) and `worker` (the two handlers). Squint compiles them
to ES modules in `dist/`. The tests under `test/unit/` run with Node's
test runner, the ones under `test/worker/` inside workerd with a local
D1 and a stub of the outbound `fetch`.

## License

MIT, see [LICENSE](LICENSE).
