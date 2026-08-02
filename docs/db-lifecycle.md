# DB Lifecycle

## Rules

- `backend/src/db/schema/base.sql`
  - only cheap, idempotent base schema work
- `backend/src/db/migrations.ts`
  - runs additive versioned migrations
- historical backfills must not run on backend startup

## Use the right layer

### Base schema
Use for:
- table creation
- index creation
- safe `ALTER TABLE ... ADD COLUMN IF NOT EXISTS`

Do not use for:
- whole-table `UPDATE` backfills
- historical recomputation
- data repair

### Migrations
Use for:
- additive schema changes that need one-time application
- constraints or indexes that belong to a versioned rollout

### Backfills / maintenance jobs
Use for:
- recomputing derived packet fields
- rebuilding link tables
- recalculating historical summaries

Run historical work deliberately during a low-traffic window. For example,
after building the current backend image, reconstruct the bounded stats
rollups without restarting the live API:

```bash
docker compose build backend
docker compose run --rm --no-deps backend node dist/tools/backfillStatsRollups.js --apply
```

`backfillStatsRollups` uses independent daily/24-hour slices and monotonic
upserts, so a live ingest write cannot be replaced by an older candidate. It
defaults to the same 31 calendar dates used by the longest-hop API and the
eight-day observer-window retention boundary. It is idempotent; use
`--daily-days` or `--observer-days` only when an operator intentionally wants
a different bounded window.

The hourly chart rollup uses a persisted cursor and a pinned end time. Each
historical hour, its replacement aggregate rows, and its next cursor commit in
one transaction. `--hourly-days` defaults to eight; interruption can therefore
resume without skipping or double-counting a slice.

## History contract and retention inventory

Retention is disabled by default. The longest raw-packet dependency is the
120-day path-learning window. Interactive paths use seven days, public and
owner dashboards use at most 30 days, and public-map node freshness uses 28
days. The proposed raw packet and status-sample retention is therefore 180
days. Privacy-filtered hourly/daily chart aggregates, model parameters, current
node/link/coverage state, and privacy state remain longer lived.

Run the exact, read-only inventory before considering compression or deletion:

```bash
docker compose exec backend npm run db:lifecycle
```

It reports exact expired rows, expired/compressed Timescale chunks, relation
bytes, oldest/newest timestamps, and the affected features for every target.
The inventory can be expensive by design; run it away from peak ingest.

Compression and retention are separate, table-at-a-time changes. Both require:

- a named, fresh database backup;
- a successful isolated restore verification from the last 30 days;
- the target in `DATA_LIFECYCLE_RETENTION_TARGETS`;
- the action flag set to `true`; and
- an exact per-table approval argument.

Example compression rollout:

```bash
DATA_LIFECYCLE_COMPRESSION_ENABLED=true
DATA_LIFECYCLE_RETENTION_TARGETS=packets
DATA_LIFECYCLE_BACKUP_REFERENCE=backup-20260729
DATA_LIFECYCLE_RESTORE_VERIFIED_AT=2026-07-29T12:00:00Z
docker compose exec backend npm run db:lifecycle -- \
  --apply-compression --target=packets \
  --approve=apply-data-lifecycle-compression-packets
```

Measure query CPU, ingest WAL, and storage after cold-chunk compression. Only
after aggregate cutover and another inventory may retention be enabled:

```bash
docker compose exec backend npm run db:lifecycle -- \
  --apply-retention --target=packets \
  --approve=apply-data-lifecycle-retention-packets
```

Hypertable retention uses a Timescale policy. Row-table deletion is bounded and
performed by the health worker only for targets explicitly listed in
`DATA_LIFECYCLE_RETENTION_TARGETS`. Failed/pending owner alert deliveries are
not discarded while they remain retryable.

Owner/private data uses the same 180-day raw-packet boundary. An owner export
must be completed before enabling deletion if older raw evidence is required.
Removing raw data is irreversible without the named restore; turning the flag
off stops future policy runs but does not recreate deleted chunks.

## Startup guarantee

Backend startup should be safe against a production-sized database. If a change can lock or scan large tables, it does not belong in startup schema init.

## Compose deployment

`docker compose up -d --build` runs the one-shot `db-migrate` service after
TimescaleDB becomes healthy and before the backend starts. It applies only
unrecorded files from `backend/src/db/migrations/`; after a successful run it
exits with no changes on later deploys.

Existing production services keep `DATABASE_SKIP_SCHEMA_INIT=true`, so they do
not repeat base-schema DDL during ordinary startup. For a manual migration run,
use `docker compose run --rm db-migrate` and inspect its output before starting
new application containers.

## Production network-label cutover

The historical `teesside` and `northeast` labels remain read-compatible until a deliberate cutover. Start with a non-mutating audit:

```bash
scripts/unify-networks.sh audit
```

Before applying, stop or upgrade every writer, create and verify a database/volume snapshot, and record its identifier. The apply command refuses to run if a legacy-labelled packet arrived in the last 15 minutes, if confirmation is absent, or if no backup reference is supplied:

```bash
CONFIRM_NETWORK_UNIFICATION=ukmesh \
BACKUP_REFERENCE='snapshot-2026-07-11T1600Z' \
scripts/unify-networks.sh apply
```

The workflow preserves sighting intervals, updates status history in 50,000-row commits, rewrites packet chunks individually, restores the prior compression-policy state after interruption, and records progress in `network_unification_runs`. Re-running with the same `NETWORK_UNIFICATION_RUN_ID` is safe. Run `scripts/unify-networks.sh verify` after any interrupted maintenance.

The relabel discards the distinction between historical production labels and is not logically reversible. Rollback means stopping all writers, restoring the snapshot named by `BACKUP_REFERENCE`, restoring the matching application version, and only then reopening ingest. Do not attempt a reverse `UPDATE`: the original label cannot be reconstructed reliably after unification.
