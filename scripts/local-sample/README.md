# Reproducible local stack and sample replay

Run all examples from the repository root. Node 26 (see `.nvmrc`), npm,
Docker Engine and Docker Compose are required. Docker must be accessible to
the current user; after joining its group, start a new login session.

```bash
scripts/dev-stack.sh up
scripts/dev-stack.sh status
```

The default **ingest** profile creates invented data, loads only its nodes and
link evidence, then replays 20 previously absent packets and one status sample.
Readiness includes persisted row counts, WebSocket delivery and a nonempty
recent-packets API. The website is at `http://127.0.0.1:5173`; its `/api` and
`/ws` proxy targets `http://127.0.0.1:3100` using `UKMESH_DEV_API_TARGET`.
No private dataset is needed for onboarding.
The backend explicitly allows the local website origins at `127.0.0.1:5173`
and `localhost:5173`; the replay smoke test supplies that browser Origin when
checking WebSocket admission. Other browser origins remain subject to the
backend allowlist.

The script starts digest-pinned TimescaleDB (5433), Redis (6380), Mosquitto
(MQTT 1884 and WebSocket 9002), backend (3100), region snapshot worker
(loopback metrics 9092) and Vite (5173). Startup waits for a fresh, certified
region snapshot, including a valid empty result for an empty public corpus. Container
ports bind to loopback. It uses an allowlisted environment, never reads a
project `.env`, and sets every active role DSN to the owned local database.
The separate `meshcore_owner_auth` database contains structure only, without
owner grants or external forwarding destinations. Application startup does
not create or migrate schema: the script loads `base.sql`, bootstraps roles,
runs the explicit migration tool, then provisions owner-auth.

Logs, PIDs and synthetic fixtures are retained under ignored
`plans/dev-stack/`. A partial startup leaves evidence for investigation.
Existing fixtures are never silently reused or deleted:

```bash
scripts/dev-stack.sh stop       # stop processes and containers; retain data
scripts/dev-stack.sh start      # restart processes against the prepared fixture
scripts/dev-stack.sh reset      # explicitly discard this stack and rebuild
scripts/dev-stack.sh cleanup    # stop and delete only this stack and its DB volume
```

After pulling backend changes, use `start` to apply pending migrations before
rebuilding and restarting the local services. It keeps the prepared data and
owner-auth database, and does not reload, shift or replay the fixture. A failed
migration stops startup before the backend is launched; inspect
`plans/dev-stack/migrate.log`. Base-schema ledger bootstrap approval is limited
to `up` and `reset`.

## Snapshot/API fixture

A snapshot loads all rows before deriving read models. It must be kept
separate from nonduplicate-ingest benchmarks:

```bash
scripts/dev-stack.sh reset --profile snapshot
```

The optional private archive is local-only and must remain ignored. Choose an
explicit timestamp for a shifted local copy so recent/day queries contain
data, and use the same timestamp for comparable runs:

```bash
scripts/dev-stack.sh reset --profile snapshot \
  --data-dir data/ukmesh-sample-2026-11-10 \
  --shift-to 2026-11-12T03:15:00Z --redact-content
```

`--shift-to` moves the latest packet to the supplied ISO timestamp and shifts
packet/status/node/link timestamps by the same offset. It never modifies the
archive. Omit it to retain original times; record the archive checksum,
original/effective window and any age-related gaps with each benchmark.
Do not publish packet contents or private-archive screenshots.
`--redact-content` replaces imported summaries with invented text of the same
character length; use it before browser captures. Record that transformation
with the fixture. The source CSVs remain unchanged.

Loading uses explicit `core`/`mesh` base tables, not the `public` compatibility
views. Node privacy triggers remain enabled in `--fast` mode. A bulk import
invalidates visibility certification; `sample:prepare` explicitly converges
privacy using the actual generation, rebuilds retained paths and multibyte
facts, and refreshes identity/sighting models. The stack then runs the existing
stats rollup backfill. Chart snapshots are populated by backend warm-up/API
reads; verify meaningful data before timing them. Preparation disables the
owned fixture's scheduled Timescale jobs and enables `pg_stat_statements` so
retention/background work does not change comparisons. This is not a runbook
for activating production lifecycle policies.

`node_links` is derived path evidence. It cannot reconstruct original
neighbour MQTT reports; neighbour-path tests need explicitly invented reports.

## Individual tools

These commands run package scripts in `backend`, with an explicit owned
loopback database. Never point integration suites at the snapshot database.

```bash
export DATABASE_URL=postgresql://meshcore:meshcore@127.0.0.1:5433/meshcore
npm --prefix backend run sample:load -- --data-dir ../data/ukmesh-sample-2026-11-10 --dry-run
npm --prefix backend run sample:replay -- --data-dir ../data/ukmesh-sample-2026-11-10 --dry-run --limit 10
```

For an intentional reload, use `sample:load -- --data-dir DIR --reset --fast`,
then `sample:prepare` and the explicit stats backfill before exposing public
reads. `--reset` truncates base packets, retained paths/facts, observation
ledger/dedupe tables, nodes, links and status samples with their FK dependants.
It does not clear a running backend's memory or Redis: stop/recreate the owned
stack for a clean ingest run.

Replay defaults to `ws://127.0.0.1:9002`. Options include `--limit`, `--from`,
`--to`, `--speed` (0 saturation, 1 original timing), `--concurrency` (1–256,
default 16), `--qos` (0 or 1), and `--status`. Prefixes are limited to
`meshcore`, `ukmesh` and `meshcore-test`; the latter is one fixed test scope,
not a fresh identity namespace for every run. Repeated receptions deduplicate.
Use the ingest profile's `--omit-packets` loader mode and reset Redis/backend
state between repeat ingest benchmarks. Broker acknowledgements alone do not
prove persistence; `sample:smoke` checks the complete path.

The replay tool requires `--allow-remote` for a non-loopback broker. The current
local verification workflow never uses that option.

## Regression check

The test creates and drops its own database, bootstraps the complete migrated
schema, generates invented CSVs, and verifies fast/normal import, derived facts,
privacy changes and reset/reload. The supplied DSN is an owned local admin DB:

```bash
TEST_SAMPLE_DATABASE_URL=postgresql://meshcore:meshcore@127.0.0.1:5433/postgres \
  npm --prefix backend run test:sample-integration
```

`data/` and `plans/` must never be committed. Keep private rows, node identities,
topics and chat out of tracked tests, fixtures, reports and telemetry.

## Measurements

Run API/build/Lighthouse measurements sequentially on a prepared snapshot:

```bash
scripts/benchmark-local.sh baseline
```

This refuses an existing evidence directory and holds a local benchmark lock.
Results, dataset checksums, image/resource settings, query deltas and route
reports are stored under ignored `plans/measurements/baseline/`. It never
resets a database. Optional `PLAYWRIGHT_CHROMIUM_EXECUTABLE_PATH` selects an
installed Chrome executable; otherwise install Playwright Chromium and its
system libraries with the documented Playwright setup for your OS. Lighthouse
navigation records LCP/CLS/TBT, resources and request counts; INP requires
separate interaction/field measurement.

For a nonduplicate ingest measurement, explicitly create an empty ingest
fixture and run the package benchmark. The generator supports a larger
`--synthetic-count`; `--replay-limit 0` leaves all receptions absent:

```bash
scripts/dev-stack.sh reset --profile ingest --synthetic-count 2000 --replay-limit 0
export DATABASE_URL=postgresql://meshcore:meshcore@127.0.0.1:5433/meshcore
npm --prefix backend run bench:ingest -- \
  --data-dir "$(cat plans/dev-stack/data-dir)" --limit 2000 --speed 0
```

For a complete repeatable pair, run `scripts/benchmark-ingest-local.sh ingest-baseline`.
This explicitly recreates the owned sample stack twice, generates one invented
2,000-packet source, and runs saturation then 100 packets/second sequentially.
It retains source checksums, image/resource limits, DB CPU and WAL deltas,
steady-state SQL rankings, RSS/heap/event-loop samples and the decode benchmark.
It also records actual read-only public query plans and the lifecycle inventory
after replay. See [backend qualification notes](../../docs/backend-performance.md)
for the initial query budget and measured limits.
Use `--data-dir plans/measurements/ingest-baseline/dataset --flush-ms 0` with a
new label to compare a flush setting against exactly the same source. The default
comparison setting is explicitly 0 ms, matching the backend default; it does
not read a project `.env`. Select `--flush-ms 10` for the former wait interval.
The local broker has bounded per-client queues (10,000 messages / 16 MiB), and
the 2 GiB database pins 100 connections to accommodate the backend's pools.

Reset between saturation and paced runs. `bench:ingest` verifies SQL row counts
and drained queues, records throughput, before/after process/cache metrics and
the enqueue-to-commit histogram's p95 upper bound. It does not label the broker
acknowledgement latency as persistence latency. `--speed 1` reproduces the
synthetic fixture's one-packet-per-second timing. Package `check` commands run
normal correctness checks; benchmarks stay out of the unit suite.

The 2026-11-12 flush comparison used the same 2,000 invented receptions on a
four-CPU local host, Node 26.13.1 and the pinned images above (PostgreSQL limited
to two CPUs / 2 GiB). Immediate microtask flushing measured 79.2 persisted
packets/second for saturation and 77.8 for the paced run, versus 43.3 and 43.2
with a 10 ms wait. Every run persisted exactly 2,000 rows and drained queues.
The enqueue-to-commit p95 histogram bounds were 25 ms for saturation at both
settings and 50 → 25 ms for paced replay. DB CPU, backend CPU and WAL per packet
decreased. Paced replay offered 100/second and required a final drain; this
fixture does not establish sustainable throughput at 100/second or production
capacity. Reproduce both settings before applying them to another workload.

Snapshot benchmarks add a fixed overlay of 50 invented ACK receptions and one
invented observer. The measurement tools re-anchor only those receptions before
recent-API and browser reads, because the API's recent window is five minutes.
Their UUIDs and count stay fixed; the source archive is unchanged. Inventory
counts include this overlay, and it is excluded from clean ingest benchmarks.
This avoids interpreting an expired, empty feed as a performance improvement.

The dashboard renderer benchmark uses its own invented nodes, real WebSocket
transport and production app artifact; it needs neither the sample DB nor MQTT:

```bash
npm --prefix frontend run bench:map -- --label baseline
```

It refuses an existing label, builds into ignored `plans/measurements/map-baseline/`,
then runs three desktop and three mobile samples at each of 1,000 / 3,000 / 6,000
nodes with 500 presence updates per second. It records frame intervals, CDP task
and script durations, heap, received updates and tool/artifact hashes. Mobile
uses 4× CPU throttling; software-rendered results do not predict a physical GPU.
The fixture contains no links, so it does not qualify link-heavy rendering.
`--counts 6000 --mode mobile --runs 1` selects a diagnostic subset. Run it apart
from other load/build benchmarks. CDP task time is not a React commit profile.

The map benchmark also verifies that the current live store has the exact latest
sent presence timestamp for every touched node after the drain period
(`appliedLatest`), in addition to transport reception counts. It inspects the
component's existing refs from the test harness; production adds no debug globals.
