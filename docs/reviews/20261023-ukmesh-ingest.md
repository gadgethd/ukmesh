# UKMesh ingest review — 2026-10-23

Branch: `burn/ukmesh-ingest-20261023`, based on
`73ab7d4fc63e4968a5fd46af022696ad69d16d9b`.
Implementation: `0ada4674b376bf62f5352ea50c296c9a08729bb0`.
Public tracking: [issue #147](https://github.com/gadgethd/ukmesh/issues/147),
[PR #151](https://github.com/gadgethd/ukmesh/pull/151).
One PR covers the connection-monitor and ingest work because both change the
shared metrics module. The PR is open and unmerged.

## What changed and why

### Connection monitor

The historical failure was an unreadable Mosquitto log after a container lost
its supplementary broker-log group. At this repository baseline the monitor
is audit-only: it no longer grants ownership from broker logs. Its poll loop
still swallowed every filesystem error, and initialization errors only logged.

The monitor now exposes `starting`, `healthy`, `error` and `stopped` state,
last successful poll time and a bounded error description such as
`init:EACCES`, `poll:EACCES` or `audit:42501`. Initialization, stat, stream and
audit-write failures emit structured error logs and phase-labelled counters.
The health gauge is zero until a successful read and while an error persists.
`/readyz` includes `checks.mqttConnectionMonitor` and returns 503 when required
monitor health is not healthy. MQTT-disabled instances do not require it.
The existing blackbox readiness probe and two-minute `BackendNotReady` rule
therefore carry the failure into alerting; liveness remains independent.

Read failures retain the cursor and retry. Missing logs retry initialization
on the existing 30-second cadence. An audit-write failure advances the range
as before, avoiding replay of an entire log range; idle polls cannot clear
that failure, while a subsequent successful audit-write scan can. Log-derived
observations remain untrusted and cannot grant ownership. The factory exists
to exercise the same lifecycle with injected filesystem errors and fixture
writes; production keeps one singleton.

Regression fixtures simulate EACCES both before initialization and during
poll-time stat/stream access. They also verify retry/recovery, no skipped or
replayed successful ranges, sticky audit failures, missing logs, truncation
and cancellation of startup retries on stop. No real permission changes or
production paths are used.

### Ingest SQL timeout

The general database default was 30 seconds, but Compose explicitly gives that
pool 900000 ms for chart work. Both durable packet transactions and RAM-writer
commits inherited it. Advert/node/status writes could also pin ingest on this
long deadline, matching the historical stall mechanism.

A separate two-connection ingest pool now enforces a PostgreSQL
`statement_timeout` of 10000 ms by default, independently of the general and
analytics settings. `MQTT_INGEST_STATEMENT_TIMEOUT_MS` accepts canonical integer
values from 100 to 30000; zero cannot disable it. The client read deadline is
one second later, allowing PostgreSQL to cancel first. Existing bounded
connection acquisition settings also apply. Packet batch policy reads and
transactions, RAM-writer persistence, redelivery probes, node upserts and
status/neighbor writes use this pool. Shutdown closes it with the other pools.
Analytics and general-purpose queries keep their existing configuration.

Timeouts emit `[ingest-db] query timed out` and increment
`meshcore_db_queries_total{pool="ingest",outcome="timed_out"}` with duration and
pool metrics. Logs omit SQL, parameters, names and packet contents. Server
cancellation rolls back transactions. A client read timeout discards the
socket immediately instead of queuing rollback behind potentially hung work;
rollback failure also discards the client while retaining the original error.
Existing durable failure/unacknowledged and RAM pending-entry recovery paths
remain in place.

The native PostgreSQL fixture proves three actual server cancellations:
`pg_sleep(20)` as a standalone query, `pg_sleep(20)` after a transaction's
insert, and a contested advisory lock. SQLSTATE 57014 arrives within two
seconds under a configured 100 ms bound. Earlier transaction writes roll
back, the connection leaves the lock wait, the next query succeeds, and all
three timeouts are counted/logged. `Pool.query` discards errored clients by
driver design; checked-out transaction clients are demonstrably reusable
after successful rollback. This is a cancellation test, not a Promise.race
that merely stops waiting for live SQL.

### Disk pressure

A filesystem alert already exists: `HostDiskSpaceLow` covers node-exporter
host filesystems, including the data mount, excluding tmpfs/overlay/squashfs.
It was strictly below 10% available. It now includes exactly 10% available
(90% usage), retaining the ten-minute holdoff and existing critical severity.
A rule fixture verifies 89% does not fire, 90% remains pending before holdoff,
90% fires after holdoff, recovery clears it, and overlay filesystems stay
excluded. Existing configured-directory-budget health separately warns at
80% and becomes critical at 90%; that is not filesystem free space.
No new deploy-side configuration is required by this change. No deletion,
cache pruning, rotation or capacity policy was changed. Current VPS capacity
was not measured.

## Spam-node conclusion and evidence limits

Read-only local backlog GETs for #408 and #423 clarified the original failures;
#618, #537 and #649 supplied related historical context. Their full descriptions
and private infrastructure details are not reproduced here.

The node named by #408 is **CC-E5FE7B**, an advert flooder, with key rotation
and approximately 19.7k adverts reported on 2026-09-24. It is distinct from
the October rotating-ID channel-message campaigns addressed by merged
[PR #141](https://github.com/gadgethd/ukmesh/pull/141).

**Current activity: unverified.** Neither this checkout nor its synthetic
fixtures contain a fresh last-seen observation for that node. The historical
backlog is not evidence of activity on 2026-10-23. No production database,
public production API, SSH tunnel or VPS was queried. A sanitized existing
offline snapshot was requested; none has been supplied at this checkpoint.
It would be incorrect to claim either that the node is active or that it has
stopped.

**Detector coverage:** #141's message loader selects `packet_type = 5`, so its
rotating-ID rules do **not** directly cover the node's type-4 adverts. Its
existing synthetic small-burst, machine-name-share, busy-day loading and
campaign-persistence regressions all pass in the 85-test spam suite.

The separate `mqtt/spamDetector.ts` advert detector conditionally covers the
historical behavior. Cached seven-day evidence of at least four names and
20 distinct adverts for one full public key produces a suspect verdict.
With an advert flood of at least 30 distinct adverts/minute observed by at
least two observers, the combined rotation/flood score reaches 110 and the
verdict is spam. A read-only local probe replaces the query function with
invented aggregate rows and invokes the actual cache refresh/evaluator:

| Synthetic evidence | Actual result |
| --- | --- |
| Four names, 20000 adverts, 49/minute, three observers | `spam`, score 110, rotation + flood signals |
| Same rotation, no flood evidence | `suspect`, score 65 |
| Flood evidence without qualifying rotation/support | `clean`, score 45, flood signal only |

Consequently, neither lifetime advert count nor #141 alone establishes that
this node is currently detected or blocked. Fresh key/name/window/observer
evidence is necessary. No spam algorithm, thresholds, bans or live records
were changed. The probe is an ignored local experiment, not a production
traffic replay or a newly shipped detector rule.

## Verification: commands and results

All filesystem fixtures and optional tools were inside this worktree. Test
processes did not load production environment files. The native PostgreSQL
cluster used a fresh directory, allocated loopback port and generated fixture
credentials; only that test-owned process was started/stopped. It was PostgreSQL
16.14 from optional `embedded-postgres@16.14.0-beta.17`, installed under ignored
`.ukmesh-tools/ingest-pg`; no tracked dependency or lockfile was changed for it.

Commands below use the repo root unless a different directory is stated.

| Exact command | Result |
| --- | --- |
| `cd backend && env -u DATABASE_URL -u OWNER_DATABASE_URL node --import tsx --test src/mqtt/connectionMonitor.test.ts src/mqtt/brokerLog.test.ts src/metrics.test.ts` | 10 passed, 0 failed, 0 skipped. |
| `cd backend && env -u DATABASE_URL -u OWNER_DATABASE_URL node --import tsx --test src/db/ingest.test.ts src/db/packetBatch.test.ts src/mqtt/durableHandler.test.ts src/mqtt/observationIdentity.test.ts src/ram/writer.test.ts src/platform/config/database.test.ts` | 39 passed, 0 failed, 0 skipped. |
| `npm install --prefix .ukmesh-tools/ingest-pg --cache backend/node_modules/.cache/npm --no-audit --no-fund embedded-postgres@16.14.0-beta.17` | Optional test tool installed locally (replaced the initially installed 18.4 tool); no runtime dependency change. |
| `cd backend && env -u DATABASE_URL -u OWNER_DATABASE_URL -u TEST_INGEST_DATABASE_URL TMPDIR="$PWD/../.ukmesh-tools/tmp" TEST_INGEST_POSTGRES_MODULE="file://$PWD/../.ukmesh-tools/ingest-pg/node_modules/embedded-postgres/dist/index.js" node --import tsx --test src/db/ingest.integration.test.ts` | 1 native integration test passed, 0 failed, 0 skipped; three server cancellation/recovery scenarios, approximately 6 s. |
| `env -u DATABASE_URL -u OWNER_DATABASE_URL -u TEST_ANALYSIS_DATABASE_URL -u TEST_INGEST_DATABASE_URL -u TEST_VACUUM_DATABASE_URL -u TEST_REDIS_URL -u TEST_REDIS_PASSWORD -u TEST_NODE_CLEANUP_POSTGRES_MODULE -u TEST_NODE_CLEANUP_PGLITE_MODULE -u TEST_SPAM_PGLITE_MODULE TMPDIR="$PWD/.ukmesh-tools/tmp" npm --prefix backend test > .ukmesh-tools/backend-test.log 2>&1` | 582 passed, 0 failed, 3 existing opt-in live EXPLAIN checks skipped; approximately 34 s. Shell synthetic alert firing/recovery delivery check also passed. |
| `cd backend && env -u DATABASE_URL -u OWNER_DATABASE_URL -u TEST_ANALYSIS_DATABASE_URL -u TEST_INGEST_DATABASE_URL -u TEST_REDIS_URL -u TEST_REDIS_PASSWORD -u TEST_NODE_CLEANUP_POSTGRES_MODULE -u TEST_NODE_CLEANUP_PGLITE_MODULE TMPDIR="$PWD/../.ukmesh-tools/tmp" TEST_SPAM_PGLITE_MODULE="file://$PWD/node_modules/.cache/spam-sql/node_modules/@electric-sql/pglite/dist/index.js" node --import tsx --test src/spam/*.test.ts > ../.ukmesh-tools/spam-test.log 2>&1` | 85 passed, 0 failed, 0 skipped; includes five isolated PostgreSQL-WASM repository cases; approximately 16 s. |
| `npm --prefix backend run build` | Passed (`tsc`, exit 0); repeated after implementation completed. |
| `.ukmesh-tools/prometheus-3.15.0.linux-amd64/promtool check rules logging/rules/meshcore.yml` | Passed: 27 rules valid. |
| `.ukmesh-tools/prometheus-3.15.0.linux-amd64/promtool test rules logging/rules/meshcore.test.yml` | Passed, including the new exact-90% holdoff/recovery fixture. |
| `env -u DATABASE_URL -u OWNER_DATABASE_URL TMPDIR="$PWD/.ukmesh-tools/tmp" node --import ./backend/node_modules/tsx/dist/loader.mjs .ukmesh-tools/spam-advert-probe.mjs` | Three synthetic aggregate cases matched the verdicts/scores above; all assertions passed, no database connection. |
| `git diff --check` | Passed. |
| `git push --set-upstream origin burn/ukmesh-ingest-20261023` | Branch pushed; PR #151 opened against main. |

The native integration test is also included in the repository's
`test:ingest-integration` command, using CI's isolated loopback test database.
The larger TimescaleDB/Redis integration stack was not run locally; no existing
server or container was used. GitHub CI was queued/running when the PR opened;
that is not claimed as a green CI result.

Earlier verification attempts exposed fixture/setup issues that were corrected:
monitor tests initially imported owner-auth configuration before injecting their
fixture writer; the default writer is now lazy-loaded. The first native probe
wrongly required `Pool.query` to reuse an errored client; inspecting the driver
confirmed eviction, and the corrected probe distinguishes standalone replacement
from transaction reuse. Two local invocations failed to locate their fixture
module/path before executing tests. The first disk-rule run accidentally
attached existing backup expectations to the new scenario; moving the scenario
to the end restored the existing tests. Final results above are the successful
commands, not those earlier failures.

## Deliberately left out, open questions and remaining work

- No deployment, merge, service restart, live configuration edit, VPS access,
  production query, migration on live data, cleanup, secret read or other
  worktree/agent/session. The required handoff is the only new review artifact
  published; no private plan or raw operational audit was copied.
- **Blocked evidence:** determine CC-E5FE7B's current activity and actual verdict
  from an already collected sanitized offline snapshot. Do not interpret the
  fixture results as a current-liveness check. If no such snapshot exists, this
  requires a separately authorized operator action, outside this task envelope.
- Review and merge PR #151 through the normal owner process. Any later rollout
  and verification of live mount permissions, alerts, capacity and timeout rates
  require separate authorization; existing production permissions were not fixed
  by this code-only change.
- The 10-second per-statement timeout and two additional possible database
  connections per process need operational observation after any authorized
  rollout. A failed audit range is signalled but is not replayed automatically.
- The optional #690 CI stretch was not started while the primary current-activity
  evidence remains unresolved. Existing three live EXPLAIN checks remain skipped.
