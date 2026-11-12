# Backend performance qualification

Use the [local sample harness](../scripts/local-sample/README.md) to reproduce
measurements. Keep CPU/memory limits, source checksums, replay timing and
background jobs fixed. Measure ingest, HTTP and browser loads sequentially.
The harness stores raw results under ignored `plans/measurements/`.

## Persisted ingest baseline

The 2026-11-12 comparison used Node 26.13.1, a four-CPU local host, the pinned
sample images and 2,000 invented unique ACK receptions. PostgreSQL had two
CPUs, 2 GiB RAM and 100 connections; broker queues were bounded to 10,000
messages / 16 MiB per client. No source packets were preloaded. The selected
implementation is in revision `13111ee`; reports bind the tested source to its
base revision and diff hash.

| Setting | Saturation persisted/s | Paced persisted/s | Paced enqueue-to-commit p95 bound |
| --- | ---: | ---: | ---: |
| 10 ms batch collection | 43.3 | 43.2 | 50 ms |
| Immediate microtask collection | 79.2 | 77.8 | 25 ms |

Every run persisted exactly 2,000 rows and drained SQL/MQTT queues and active
handlers. Saturation p95 remained within the same 25 ms histogram bucket.
Immediate collection preserved synchronous batch coalescing, retry, privacy,
observation identity and acknowledgement boundaries. Backend CPU per packet
fell from 2.49 to 1.99 ms in paced replay; DB CPU fell from 11.66 to 11.30 ms,
and WAL from 9,710 to 9,617 bytes. These are workload-specific measurements.
Paced replay offered 100 packets/s and required a final drain; it does not
establish sustainable throughput at that offered rate.

The acceptance budget for this comparison was at least 20% more persisted
throughput, no worse persistence p95, at most 10% more CPU/WAL per packet,
exact row counts and complete drain. Future changes should repeat both runs
against those same limits before claiming an improvement.

## SQL cost and plans

The paced after-run's `pg_stat_statements` deltas exclude fixture preparation.
The ten highest total-cost statements were:

| Statement | Calls | Total execution ms | Mean ms |
| --- | ---: | ---: | ---: |
| Atomic packet/ledger/path/rollup write | 2,000 | 10,127.1 | 5.064 |
| Scoped path-prefix candidates | 2,000 | 374.2 | 0.187 |
| Visibility readiness state | 126 | 10.9 | 0.087 |
| Packet writer advisory lock | 2,000 | 7.1 | 0.004 |
| Benchmark persisted-row probe | 23 | 4.3 | 0.186 |
| Benchmark query-stat snapshot | 1 | 2.5 | 2.499 |
| Commit | 2,000 | 2.1 | 0.001 |
| Begin | 2,000 | 1.9 | 0.001 |
| Recent canonical observer count | 1 | 1.1 | 1.061 |
| Daily packet count | 1 | 0.5 | 0.493 |

Rank by both total cost and per-call latency: low-frequency benchmark probes
are not ingest bottlenecks. The packet transaction dominates this fixture;
removing its timer wait improved throughput without an index change.

`npm --prefix backend run bench:db -- --output ../plans/measurements/db-plans`
records actual summary, observer-activity and topology queries with their bound
parameters through PostgreSQL's planner. It runs ten `EXPLAIN (ANALYZE, BUFFERS)`
plans inside a read-only transaction and stores the five slowest first. Results
contain SQL diagnostics, not HTTP p95. On the populated 2,000-packet/three-node
fixture, the five slowest plans took 5.625, 1.571, 0.705, 0.418 and 0.336 ms.
This small fixture supports no production index or resolver-cache change.
Repeat with the prepared archive and current scope before making such changes.

## Lifecycle inventory

Run `npm --prefix backend run db:lifecycle` with the owned database DSN. This
read-only command reports exact expired rows, chunk counts and compression,
actual Timescale jobs, their scheduling state and retention-policy matches.
See [the lifecycle runbook](db-lifecycle.md) for receipt/allowlist requirements.

The inspected local schema-136 fixture used TimescaleDB 2.25.1. Its nine
application lifecycle jobs were disabled; the two built-in telemetry/history
jobs remained scheduled. Five hypertables supported compression, but no chunk
was compressed. Packet chunks were one day; path, archive and status/neighbour
chunks were seven days. The retained status job declared 180 days while the
current application retention policy specifies 30 days. Inventory reports
that mismatch explicitly. No lifecycle policy was enabled or changed by these
measurements.

Local settings were 512 MiB shared buffers, about 10 MiB `work_mem`, autovacuum
enabled with ten workers, and 100 connections. These reflect the pinned
image's local memory tuner, not recommended production settings. Preserve
the actual job/configuration inventory in each upgrade and rollback rehearsal.

## Broker correlation bounds

The audit-only broker-log monitor retains at most 4,096 client correlations
by default and physically expires them after 15 minutes, including idle polls.
`MQTT_CONN_MONITOR_MAX_CLIENTS` is clamped to 1–65,536,
`MQTT_CONN_MONITOR_CLIENT_TTL_MS` to 1,000–86,400,000 ms, and
`MQTT_CONN_MONITOR_POLL_MS` to 100–60,000 ms. Nonfinite settings fall back to
the defaults. The previous direct numeric reads could let `NaN` or `Infinity`
disable pruning. `meshcore_mqtt_connection_monitor_clients` exposes retained
count without client or username labels. These observations never authorize
ownership; the owner-auth database remains authoritative.

## Link-worker startup and memory

Link mode retains GDAL, NumPy, terrain sampling and the existing RF model.
Coverage geometry, mainland clipping and raster filtering load on first use.
The support context still refreshes coordinates and link-radius metadata;
its spatial tree is built when a coverage or planned diagnostic needs it and
invalidated when the source context changes. Other worker modes build that
tree during their normal context refresh. First coverage use still pays the
deferred initialization cost.

The November 2026 comparison used the pinned GDAL 3.13.3 image with Python
3.14.4, one worker, two CPUs and a 1 GiB container limit. Each of three
readiness trials began with the same isolated database containing 1,000
invented positioned nodes and 200 links, plus an isolated Redis instance.
Median Docker-start-to-ready time fell from 0.766 to 0.619 seconds (19%),
and steady RSS from 148.9 to 117.0 MiB (21%). Fresh-process module imports
fell from 0.436 to 0.212 seconds (51%).

Three alternating before/after queue trials each processed 264 unique jobs:
200 cached physical links and 64 fresh calculations using a local invented
HGT raster and real GDAL. Every trial had 264 SQL commits, computed metrics
for all 264 links, and no remaining leases. Median throughput was 92.33
versus 92.63 jobs/s. Cached-job p95 was 3.69 versus 3.79 ms, and fresh-terrain
p95 was 38.42 versus 37.63 ms. Job RSS fell from 152.2 to 122.2 MiB. These
short fixture runs establish comparable behavior, not sustained production
capacity. The image suite passed 57 tests; one optional external-repository
SQL-root check was skipped. RF prefix/radial budgets and parity also passed.

Build and run the import probe from the repository root:

```bash
docker build -f viewshed-worker/Dockerfile -t ukmesh-worker-bench:local viewshed-worker
docker run --rm --network none --cpus 2 --memory 1g --entrypoint python \
  ukmesh-worker-bench:local /app/tests/benchmark_worker_import.py --mode link --runs 3
```

The probe discards one warm-up, starts a fresh Python process per run and
prints timings, current/peak RSS, Python/GDAL versions and loaded modules as
JSON. It opens no database or Redis connection. Full worker readiness and
job measurements require the separate populated services described above.
