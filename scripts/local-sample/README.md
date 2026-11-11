# Local sample database and MQTT replay

This directory contains **local-only test tooling** for the UKMesh sample
dataset. The dataset itself is not committed and must stay out of Git.

The tools live in `backend/src/tools/`:

- `loadSampleDataset.ts` — loads the sample CSVs into a migrated database.
- `replaySampleMqtt.ts` — publishes the sample to MQTT so the whole ingest path
  (broker → backend → database → WebSocket → API → website) can be exercised.

Both tools read the dataset from `--data-dir` and never write it anywhere or
log payload contents.

## 1. Start a disposable database

```bash
docker run --rm --name ukmesh-sample-db \
  -e POSTGRES_USER=meshcore \
  -e POSTGRES_PASSWORD=meshcore \
  -e POSTGRES_DB=meshcore \
  -p 127.0.0.1:5433:5432 \
  timescale/timescaledb:latest-pg16
```

```bash
export DATABASE_URL=postgresql://meshcore:meshcore@127.0.0.1:5433/meshcore
cd backend
npm run db:migrate
```

## 2. Load the sample

```bash
cd backend
DATABASE_URL="$DATABASE_URL" npm run sample:load -- \
  --data-dir ../data/ukmesh-sample-2026-11-10 \
  --reset --fast
```

Use `--dry-run` first to validate the files without touching the database.
`--reset` truncates `packets`, `nodes`, `node_links` and
`node_status_samples` before loading. `--fast` disables user triggers and FK
checks for the load transaction (requires a superuser) and cuts a full load
from minutes to seconds; omit it if you need trigger-maintained derived tables.

What is loaded:

- `packets` — one row per reception; `path` becomes `path_hashes` and
  `summary` becomes `payload._summary`.
- The `packets` load uses PostgreSQL `COPY` through temporary staging tables,
  so 157k rows load in seconds.
- `nodes`, `node_links`, `node_status_samples` — column-for-column.

Not loaded yet: `multibyte_paths` (the app's `multibyte_path_facts` needs the
privacy generation/topic columns, which the sample does not carry). Stats that
depend on that table will be empty until it is derived from `packets`.

## 3. Start a local broker

```bash
docker run --rm --name ukmesh-sample-mqtt \
  -p 127.0.0.1:1883:1883 \
  -p 127.0.0.1:9001:9001 \
  -v "$PWD/scripts/local-sample/mosquitto-test.conf:/mosquitto/config/mosquitto.conf:ro" \
  eclipse-mosquitto:2
```

This broker has anonymous access and is only for localhost. Never expose it.

## 4. Start the backend

Point the backend at the local database, Redis and broker. A minimal example:

```bash
docker run --rm --name ukmesh-sample-redis -p 127.0.0.1:6379:6379 redis:7

cd backend
DATABASE_URL="$DATABASE_URL" \
REDIS_URL=redis://127.0.0.1:6379 \
MQTT_BROKER_URL=ws://127.0.0.1:9001 \
JWT_SECRET=local-development-secret-32-characters-minimum \
OPERATOR_SITE_TOKEN=local-development-token \
OWNER_COOKIE_SECRET=local-development-cookie-secret \
npm run dev
```

## 5. Replay

Dry-run first:

```bash
cd backend
npm run sample:replay -- \
  --data-dir ../data/ukmesh-sample-2026-11-10 \
  --dry-run --limit 10
```

Replay the whole day as fast as the broker accepts it:

```bash
npm run sample:replay -- \
  --data-dir ../data/ukmesh-sample-2026-11-10 \
  --broker ws://127.0.0.1:9001 \
  --speed 0
```

Replay at real time (speed 1), or 10×:

```bash
npm run sample:replay -- --data-dir ../data/ukmesh-sample-2026-11-10 --speed 1
npm run sample:replay -- --data-dir ../data/ukmesh-sample-2026-11-10 --speed 10
```

Other options:

- `--from` / `--to` — ISO timestamps to bound the replay.
- `--limit N` — stop after N messages.
- `--qos 0|1` — default 1.
- `--prefix meshcore-test` — replay as isolated test traffic.
- `--status` — replay node status envelopes instead of packet bytes.
- `--allow-remote` — required for any non-loopback broker.

Status replay resolves each node's IATA from `nodes.csv.gz` and publishes to
`<prefix>/<IATA>/<node_id>/status`.

## Repeat runs and deduplication

The backend's observation identity does not include the packet timestamp, so
replaying the same rows twice into the same database is deduplicated.

For repeatable runs either:

- use `--reset` on the loader, or
- replay with `--prefix meshcore-test` so the test network has its own
  identities.

`meshcore-test` is isolated test traffic and will not appear in public scopes.

## Verify

```sql
SELECT count(*) AS receptions, count(DISTINCT packet_hash) AS packets FROM packets;
SELECT count(*) FROM nodes;
SELECT count(*) FROM node_links;
SELECT count(*) FROM node_status_samples;
```

Then check `/api/packets/recent`, `/api/stats/charts`, `/api/nodes/map` and the
WebSocket feed.

## Keep it out of Git

`data/ukmesh-sample-*` is ignored through the local Git exclude file, not a
committed `.gitignore`. Do not add the dataset to any commit, fixture or test.
The sample contains real user chat; do not print or republish message text.
