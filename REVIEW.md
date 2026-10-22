# UKMesh vacuum backlog diagnosis — 2026-10-22

Branch: `burn/vacuum-backlog-20261022`. Base: `bec753b5` (`origin/main`).
Scope: backlog #649 and the supplied `BRIEF-ukmesh-vacuum.md` only.
The previous root review is preserved verbatim in
[`docs/reviews/2026-10-02-burn.md`](docs/reviews/2026-10-02-burn.md).

## Result and diagnosis

The measured critical vacuum alert is a **health-check false positive over
ordinary accumulated update churn**, not evidence that autovacuum is stuck.
The old predicate flags five tables which have not reached PostgreSQL's
configured dead-tuple vacuum triggers. It treats 50,000 dead rows as a backlog
regardless of table size, and also alerts at 10% dead/live while this database
normally starts vacuuming at about 20% of `pg_class.reltuples`.

The exact old and new queries in the same read-only transaction returned
**5 versus 0** tables needing vacuum. The new catalog query's measured
execution time was **15.184 ms**, planning **6.526 ms**. No application table
scan, maintenance operation, DDL or live data/config write was performed.
The unchanged live `/api/health` still returned `database_vacuum_backlog` with
critical severity. This branch has not been deployed and does not claim that
the live alert has cleared.

This diagnosis is about the present observations. The supplied October 12
probe is historical context, not a measurement repeated at that date. These
snapshots cannot prove which writer produced every dead tuple or rule out
intermittent past vacuum contention or physical index bloat.

## Measured evidence and exact queries

Host: `ben@57.129.131.35`, SSH port `32720`. Live container discovered with
read-only `docker ps`: `meshcore-infra-timescaledb-1`. Database and role:
`meshcore`, as documented in README/Compose and confirmed by the connection.
PostgreSQL **16.11**; `transaction_read_only=on` was checked in every diagnostic
snapshot. No credentials or environment files were read or printed.

The committed SQL files are frozen copies of the exact diagnostic and
comparison inputs sent to psql; adjacent text files contain their output:

- [Diagnostic SQL](docs/diagnostics/vacuum-backlog-20261022.sql) and
  [output](docs/diagnostics/vacuum-backlog-20261022.txt), measured
  **2026-10-22 14:25:27 UTC / 15:25:27 BST**.
- [Old/new comparison and EXPLAIN SQL](docs/diagnostics/vacuum-backlog-comparison-20261022.sql)
  and [output](docs/diagnostics/vacuum-backlog-comparison-20261022.txt).

Only trailing spaces from psql's table formatting were stripped from the
output files; query inputs and measured values are preserved.

All five old-check candidates have no per-table reloptions and therefore use
`autovacuum_vacuum_threshold=50`, `autovacuum_vacuum_scale_factor=0.2`:

| Table | Estimated live | Estimated dead | `reltuples` | Vacuum trigger, approximately | Total bytes |
| --- | ---: | ---: | ---: | ---: | ---: |
| `multibyte_path_facts` | 2,473,913 | 285,751 | 2,469,879 | 494,026 | 2,202,386,432 |
| `packet_hourly_stats` | 914,989 | 108,459 | 909,590 | 181,968 | 226,205,696 |
| `observer_region_packet_sightings` | 238,022 | 35,785 | 233,931 | 46,836 | 311,017,472 |
| `owner_packet_share_deliveries` | 241,414 | 27,746 | 234,784 | 47,007 | 415,195,136 |
| `public_feed_deliveries` | 99,444 | 18,531 | 98,132 | 19,676 | 128,917,504 |

`multibyte_path_facts` has **1,801,912,320 table bytes** (including associated
TOAST), **400,474,112 index bytes**, **5** autovacuums in the available counters,
and last autovacuum **2026-10-22 06:07:27.164047 UTC**. Counters show
**2,860,342 updates**, **50 HOT updates**, **86,321 inserts**, **0 deletes**.
Its dead-row estimate stayed **285,751** across the initial 14:21 and 14:25
probes while live rows increased. The repository's fact trigger and backfill
both use `ON CONFLICT ... DO UPDATE`; this is a plausible source of update
churn, not a proven attribution of the historical counters to one writer.

The diagnostic also collected:

- Autovacuum **on**, `track_counts` **on**, **3** maximum workers,
  naptime **10 s**, vacuum cost delay **2 ms**, cost limit **-1** (uses
  vacuum cost limit **200**), work memory **-1** (uses maintenance memory
  **251,903 kB**). Insert trigger **1000 + 0.2 × row estimate**;
  analyze trigger **50 + 0.1 × row estimate**.
- Other table overrides include scale **0.05** for `node_links`,
  `node_identity_links`, `path_motif_priors`, `path_prefix_priors`; `nodes`
  uses vacuum/analyze scale **0.5**. The health query honors overrides.
- **0 transactions older than one minute**, **0 prepared transactions**,
  **0 replication slots**, **0 active vacuum progress rows** in the snapshots.
  The longest displayed transaction with a pinned xmin was about **19 s** in
  the first probe and **11 s** in the committed diagnostic. Blocker arrays
  were empty for those displayed sessions. SQL text/GIDs are omitted.
- Database size **17,721,029,091 bytes**, dead-row estimate **507,348**;
  database XID age **132,622,753**, configured freeze maximum **200,000,000**.
  These are observations, not a comprehensive wraparound/bloat assessment.
  `stats_reset` was null; counters have no claimed measurement interval.

## What changed and threshold decision

`backend/src/health/vacuumBacklog.ts` shares the catalog query between the
health endpoint and the diagnostic emitter. For enabled autovacuum and
statistics, a table counts only when its estimated dead rows exceed:

```text
max(10,000, effective vacuum base + effective vacuum scale × max(reltuples, 0))
```

The effective base/scale are the table's storage parameters or current server
defaults. This follows PostgreSQL 16's documented scheduling inputs rather
than assuming a fixed percentage of `n_live_tup`.
[PostgreSQL 16 autovacuum documentation](https://www.postgresql.org/docs/16/routine-vacuuming.html#AUTOVACUUM).

The 10,000-row noise floor retains the previous minimum scale of actionable
health alerts on small tables. Dead-only tables are still considered; there
is no positive-live-row admission guard. Disabled global/table autovacuum or
disabled statistics collection uses that conservative floor for observed
dead rows. Disabled statistics can make counts stale; this is not a substitute
for separately monitoring statistics collection. The existing severity rule
is preserved: **1–2** qualifying tables warn, **3+** are critical.

No table-specific exemption, larger arbitrary absolute cutoff or alert-code
removal was added. A table above its configured trigger still qualifies,
even during an active vacuum; repeated diagnostics distinguish a transient
eligible table from a persistent backlog. Insert/freeze vacuums are separate
from this dead-row check. Catalog estimates are approximate, and
`oldest_vacuum_at` remains the legacy aggregate minimum of non-null
autovacuum timestamps, not evidence that the worst table is overdue.

No migration or reloption change is appropriate from these measurements.
Lowering the table's 20% trigger solely to satisfy the old health check would
increase vacuum work without evidence of a failed scheduler. There is no new
DDL and therefore no migration rollback SQL to apply.

## Fleet diagnostic and operator runbook

From a checkout of this branch, build once and emit read-only psql input:

```sh
cd backend
npm run build
cd ..
mkdir -p .ukmesh-tools
node backend/dist/tools/diagnoseVacuumBacklog.js > .ukmesh-tools/vacuum-diagnosis.sql
ssh -o BatchMode=yes -o ConnectTimeout=10 -o StrictHostKeyChecking=yes \
  -p 32720 ben@57.129.131.35 \
  "docker exec -i -e PGOPTIONS='-c default_transaction_read_only=on -c statement_timeout=10000 -c lock_timeout=2000 -c application_name=ukmesh-vacuum-diagnosis' meshcore-infra-timescaledb-1 psql -X -v ON_ERROR_STOP=1 -P pager=off -U meshcore -d meshcore" \
  < .ukmesh-tools/vacuum-diagnosis.sql
```

The tool only writes SQL to stdout;
it loads no credentials, opens no database connection, and imports no
application bootstrap/migration code. The emitted SQL uses `BEGIN READ ONLY`,
a 10 s statement limit, 2 s lock limit and 30 s idle transaction limit.
The outer `PGOPTIONS` independently defaults the session to read-only.
Inspect activity metadata, prepared transaction ages, replication horizons
and active vacuum progress alongside the per-table threshold/size output.
Repeat after a normal work interval if a table is above its trigger. No
transaction cancellation, slot removal or vacuum is part of this script.

**Ben-only maintenance, if fresh evidence warrants it; NOT RUN in this task.**
No immediate manual vacuum is indicated by the recorded measurements. If a
real backlog persists, review the fresh diagnostics and any pinned xmin or
worker contention first. Do not automatically kill transactions. After Ben
chooses targeted maintenance, this is the exact bounded command for the
named table, outside any transaction block:

```sh
ssh -o BatchMode=yes -o ConnectTimeout=10 -o StrictHostKeyChecking=yes \
  -p 32720 ben@57.129.131.35 \
  "docker exec -i meshcore-infra-timescaledb-1 psql -X -v ON_ERROR_STOP=1 -P pager=off -U meshcore -d meshcore" <<'SQL'
SET statement_timeout = '15min';
SET lock_timeout = '5s';
SET maintenance_work_mem = '64MB';
VACUUM (VERBOSE, ANALYZE, PARALLEL 0, TRUNCATE FALSE) public.multibyte_path_facts;
SQL
```

The memory/parallel limits reflect the shared-memory failure recorded in the
earlier fleet maintenance incident. Tail truncation is disabled to avoid its
exclusive-lock attempt. Ben should rerun the read-only diagnostic afterward
and compare dead rows, vacuum timestamps/counts, blockers and API incidents.
Do not substitute database-wide vacuum, `VACUUM FULL` or reindexing for this
specific action without a separate diagnosis. Ordinary vacuum marks space
reusable; it does not promise a smaller filesystem footprint.
[PostgreSQL 16 vacuum behavior](https://www.postgresql.org/docs/16/routine-vacuuming.html#VACUUM-FOR-SPACE-RECOVERY).

## Rollback and deliberately left out

Rollback is a code revert of this branch's implementation commit followed by
the owner's separately authorized normal release procedure. No deployment is
performed here. The previous predicate will again flag the measured five
tables. There is no database rollback: no schema, reloption, data, service,
live config or live checkout was changed. A future ordinary vacuum itself
has no inverse SQL; it preserves logical rows.

Left out: automatic VACUUM/DDL, scheduler tuning, new retention/backfill
behavior, physical bloat measurements requiring extensions/table scans,
transaction termination, new dashboards, deploy/restart/merge, and unrelated
health incidents. Open questions are whether long-term fact/index growth
merits a separately measured tuning change, and whether the existing
three-table severity rule should become a sustained/impact-based policy.
The current evidence does not justify either additional change.

## Verification and handoff

Exact local verification commands (from the worktree root unless shown):

```sh
mkdir -p .ukmesh-tools/tmp
cd backend
npm ci --cache "$PWD/../.ukmesh-tools/npm-cache" --no-audit --no-fund
npm run typecheck
npm run build
TMPDIR="$PWD/../.ukmesh-tools/tmp" npm run contract:check
TMPDIR="$PWD/../.ukmesh-tools/tmp" npm test
TMPDIR="$PWD/../.ukmesh-tools/tmp" node --import tsx --test src/api/hopreachCompatibility.test.ts
TMPDIR="$PWD/../.ukmesh-tools/tmp" node --import tsx --test --test-concurrency=4 \
  $(rg --files src -g '*.test.ts' -g '!*.integration.test.ts')
TMPDIR="$PWD/../.ukmesh-tools/tmp" bash ../scripts/test-alert-receiver.sh
cd ..
npm install --prefix .ukmesh-tools/pglite --cache .ukmesh-tools/npm-cache \
  --no-package-lock --ignore-scripts --no-audit --no-fund @electric-sql/pglite@0.5.8
cd backend
TMPDIR="$PWD/../.ukmesh-tools/tmp" \
TEST_VACUUM_PGLITE_MODULE="file://$PWD/../.ukmesh-tools/pglite/node_modules/@electric-sql/pglite/dist/index.js" \
  npm run test:vacuum-integration
cd ..
node backend/dist/tools/diagnoseVacuumBacklog.js > .ukmesh-tools/vacuum-diagnosis-built.sql
cmp docs/diagnostics/vacuum-backlog-20261022.sql .ukmesh-tools/vacuum-diagnosis-built.sql
git diff --check
```

- Typecheck and build: pass.
- OpenAPI contract check: pass, **71 API and 11 operator routes** current.
- Backend suite: **556 passed / 559 total, 0 failed, 3 skipped**. Skips are
  existing opt-in live RF/history/export EXPLAIN tests. The local synthetic
  alert-receiver firing/recovery drill also passed.
- A repeat of default `npm test` returned **555 pass, 1 fail, 3 skips**:
  the unchanged HopReach calibration test expected at least one `COALESCED`
  HTTP response inside its 250 ms fixture window. Its single backend load
  assertion passed. The unchanged test file passed **3/3** in isolation;
  scheduling under the concurrent suite is a plausible cause, not a proven
  root cause. No HopReach source/test change was made in this branch.
- The full suite with `--test-concurrency=4` then passed **556 / 559,
  0 failures, 3 existing skips**; the following alert-receiver drill passed.
  This covers the same unit test files without relying on the failed run.
- Vacuum SQL tests: **21/21 passed, 0 skipped**, using isolated PostgreSQL-WASM
  (PGlite 0.5.8). They execute the production threshold expression against
  synthetic SQL values: all five measured candidates, real backlog,
  equality/one-row boundaries, server defaults, each/both/zero overrides,
  unrelated options, dead-only/unknown estimates, small-table floor and
  disabled global/table vacuum/statistics. The complete catalog query also
  executes under a read-only transaction.
- CI additionally runs `npm run test:vacuum-integration` against its existing
  disposable PostgreSQL service via `TEST_VACUUM_DATABASE_URL`. No new runtime
  or repository package dependency was added. CI status is recorded below
  after the branch is pushed.
- Exact generated diagnostic SQL ran successfully against live PostgreSQL
  16.11; compiled/source output matches the committed SQL byte for byte.
  Old/new query proof: **5 → 0**, with the live endpoint unchanged.

Implementation commit and PR: recorded in the final handoff update below.

## Honest mistake inventory

- The first API probe looked for private `problems`/`database` fields; the
  public projection exposes `incidents`. The corrected probe confirmed the
  critical vacuum incident. No schema or endpoint change was needed.
- TokenSave context timed out after 300 s, and the worktree has no local
  `.tokensave/tokensave.db`; scoped source reads supplied the missing context.
- The first full unit run used default test temporary paths. Subsequent
  verification sets `TMPDIR` inside this worktree; authored code, tooling,
  npm cache and evidence are worktree-local. No live config was touched.
- The default suite repeat exposed an unrelated timing-sensitive HopReach
  test after the first full run passed; both outcomes are recorded above.

PROOF: read-only diagnostic and old/new SQL inputs/outputs committed above;
five below-trigger tables, zero pinned long transactions/prepared xacts/slots,
SQL **21/21**, backend **556 pass / 3 existing skips**, typecheck/build pass;
implementation SHA and PR are in the final handoff update.
