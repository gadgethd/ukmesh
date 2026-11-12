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

Analysis lease metrics retain at most 4,096 local run observations. A successful
SQL heartbeat refreshes the observation through the earlier of its lease expiry
and hard run deadline. A 30-second sweep physically removes expired entries;
failed terminal writes therefore cannot retain run IDs indefinitely. The gauge
`meshcore_analysis_active_leases` reports these local observations, while
`meshcore_analysis_lease_tracking_drops_total` counts expiry/capacity removal
using fixed workload/outcome labels. Run IDs never become metric labels.
SQL lease and publication fences remain the authority for execution.

The committed-observation dedup and pre-resolve cooldown maps each retain at
most 50,000 entries. Their minute sweeps remain in place after a component
comparison of fresh, half-expired and fully expired maps. Across 20 measured
sweeps per state, manual sweep p95 stayed below 3.2 ms; replacing them with
the metrics-enabled general TTL map raised full-expiry p95 to 32.3 ms.
The existing fully expired sweep used about 0.044 ms CPU/s amortized over its
minute cadence. An incremental-expiry rewrite was therefore not selected.
This component measurement does not replace the sustained ingest soak.

`meshcore_mqtt_dedup_entries`, `meshcore_mqtt_preresolve_history_entries` and
`meshcore_mqtt_preresolve_inflight` expose occupancy without identity labels.
The background pool bounds queued/active pre-resolves and releases rejected,
timed-out or completed requests. The optional cooldown defaults to ten seconds;
`PATH_BETA_PRERESOLVE_MIN_INTERVAL_MS` is clamped to 1 second–24 hours, with
nonfinite settings falling back to the default. Durable duplicate detection
and privacy validation remain unchanged.

Reproduce the component comparison from the repository root:

```bash
npm --prefix backend run bench:mqtt-caches -- --output "$PWD/plans/measurements/mqtt-cache-sweeps.json"
```

It checks equal starting populations and exact expiry counts, discards one
warm-up per case, and records CPU/wall samples, medians and nearest-rank p95.

The occupancy instrumentation also passed fresh 2,000-packet saturation and
100-packets/s offered-rate trials: both persisted every packet and fully
drained, with the same 25 ms persistence p95 bucket. Throughput was 77.43 and
77.77 packets/s, versus 79.20 and 77.79 in the previous zero-flush fixture.
Backend CPU per packet rose by less than 3%, DB CPU by less than 2%, and WAL
by less than 0.2%. Dedup occupancy reached exactly 2,000; these ACK fixtures
do not exercise optional ADV/GRP pre-resolution. The offered rate still
requires a final drain and is not a sustainable-rate claim.

## Spam-detector memory bounds

Each of the five database result sets is limited to 100,000 rows and 16 MiB
of serialized row data. PostgreSQL checks both budgets before returning the
snapshot to Node. These are transfer limits, not a claim about exact JavaScript
heap usage. Established locations and known IDs publish together; identity,
burst and rotation evidence publish together. A failed or oversized refresh
retains the previous complete snapshot. Before the first successful refresh,
that snapshot is empty and the detector has reduced evidence.

`meshcore_spam_detector_refresh_healthy{dataset}` reports the latest attempt;
`meshcore_spam_detector_snapshot_updated_seconds{dataset}` records the last
successful publication. Check both before relying on detector coverage.
Refresh outcomes and retained entry counts use only the fixed `established`
and `identity` dataset labels. Concurrent refresh callers share one operation.
Rotation diagnostics retain four sample names while keeping the full distinct
name and advert counts used by verdicts.

Runtime state retains at most 10,000 trackers, 64 fixed-size name digests per
tracker and the latest 31 arrival timestamps. The flood predicate needs only
whether more than 30 arrivals fit within one minute. Monotonic time controls
arrival/name expiry; wall-clock corrections cannot extend those windows.
The runtime-entry metric reports tracker, arrival and name populations without
identity labels. Five-minute cleanup also removes idle state.

A fresh-process component comparison used 20,000 repeated invented adverts,
one warm-up and three measured runs per implementation. Both reported exactly
19,970 flood signals. Median evaluation wall time fell from 1,837 to 95 ms,
CPU from 1,900 to 126 ms, and post-GC retained heap delta from 264 to 134 KiB.
This isolates detector evaluation and is not persisted-ingest throughput.
An isolated PostgreSQL fixture retained the previous implementation's exact
verdicts/scores/signals for a six-key clone burst, stable locations and key
rotation. Oversized evidence rejected the whole identity refresh and retained
those verdicts. Row-count and UTF-8 byte boundaries also execute against SQL.

Run the SQL checks against a disposable-capable local PostgreSQL endpoint:

```bash
cd backend
TEST_INGEST_DATABASE_URL=postgresql://meshcore:meshcore@127.0.0.1:5433/meshcore \
  node --import tsx --test src/mqtt/spamSnapshot.integration.test.ts
```

The test creates and drops its own named database. The final sustained soak
must still qualify the complete candidate after the last runtime change.

## Resolver context reuse and accounting

Each resolver worker retains at most 16 network contexts for 15 minutes, within
128 MiB of estimated retained data. The estimate walks node, link, learned-model
and champion-prior maps and counts shared object references once. The previous
JSON estimate omitted Map contents. Unserializable default values and invalid
custom weights are rejected rather than charged one byte or poisoning the
cache's total with `NaN`. This is an estimate, not a process RSS ceiling.

A read-only copy of the retained local snapshot contained 6,177 nodes, 44,100
links and 157,801 packets. Four alternating fresh-process trials per version
included one discarded warm-up; each performed one cold and 50 repeated direct
resolutions of a real three-byte path with 14 decoded nodes. Corrected context accounting was
28.7 MiB versus the old 0.9 MiB. All normalized decoded-output hashes matched.
Median cold loading was 189 versus 196 ms, with CPU 228 versus 246 ms. Warm
p95 was 11.5 versus 10.8 ms; these small differences do not establish a speedup.

The existing context cache reduced 14 cold SQL calls to two per repeated call:
packet evidence and the fresh visibility check. The nine prior queries ran only
on context loading. Network/privacy scope, expiry, final publication fences and
caller cancellation remain unchanged. The context is network-wide; observer
and region projections are applied during decoding. Worker messages carry
packet/scope and bounded sticky/held-path inputs, then the decoded result;
they do not clone the complete context between threads.

Offline accuracy replay also matched every eligible held-out case: 389 development
and 412 test routes in both single- and multi-observer modes. Predictions and
three-byte truth checks were identical; this establishes regression parity on
the retained fixture, not production traffic accuracy.

An additional model memo or shared context-transfer mechanism was not selected:
the existing reuse already removes repeated prior loads in this fixture. The
cache-weight correction preserves complete evidence instead of truncating a
model to fit. Oversized contexts can be used by the current operation but are
not retained. Include complete-process memory across the configured worker
count in the final sustained qualification.

## Public node delivery

Node lists, map pages, inferred nodes, links, history and adverts validate the
ready privacy generation before loading and immediately before sending, including
conditional 304 responses. Link-cache keys include generation, network, region
and node identity. Link/inferred refreshes also fence cache publication; their
in-flight scope maps each admit at most 32 operations. Same-generation background
refresh remains available. Search and top-advert responses retain `no-store`;
other public node responses require revalidation. The former map-page 30-second
freshness plus 60-second stale allowance has been removed.

The HTTP regression covers eight route representations, authority failures and
in-flight changes. An actual complete-schema fixture holds selected map rows
while another SQL connection hides a peer. Both the pending map response and
the old cached links are withheld; after privacy convergence, the map excludes
that peer and the current-generation link result is empty.

On the retained archive, a focused four-pair comparison of 1,000-row map pages
pinned one snapshot time, used two request warm-ups, 30 measurements per process,
ten seconds of startup settling and one discarded pair. Median trial p95 was
63.2 versus 65.1 ms with the new privacy checks (+2.9%), within the predeclared
10% or 5 ms allowance. One individual pair exceeded its allowance by 1.41 ms;
pooled 90-request p95 was 63.3 versus 67.1 ms and also passed. All 240 measured
responses had identical normalized data and 297,618 decoded bytes. An earlier
time-relative comparison crossed an online-status boundary; the fixed-snapshot
repeat resolves that parity uncertainty. This records the observed cost of the
privacy fix, not a speedup or a production SLO.

The activity timeline, RF validation, observer health, link history and firmware
aggregates also fence delivery with fresh ready-generation reads. Observer
health remains `no-store`; the others require private revalidation. RF authority
reads use its existing read-only pool and share the 4.5-second request deadline
and cancellation signal, including when a browser disconnects.

## Activity timeline query

The timeline groups raw receiver/source identities and their observation weights
before canonical identity lookup. It retains distinct packet counts, canonical
observer counts, null-observer buckets, weighted node ordering and the 250-node
per-bucket limit. Actual PostgreSQL regressions cover aliases, duplicate
receptions, nulls and a privacy change committed while an HTTP query is held.
Packet hashes are grouped before counting each bucket, allowing PostgreSQL to
use hash aggregation instead of sorting every observation for `COUNT(DISTINCT)`.
The grouping preserves database equality semantics and excludes null hashes.

On a quiet retained corpus of 54,000 invented ACKs, three alternating pairs used
the actual compiled route, two request warm-ups, 20 measurements per trial and
one discarded pair. Median trial p95 fell from 1,342 to 147 ms (89%). Every one
of the 120 measured response hashes matched after removing `generatedAt`.
Both versions used a no-op limiter for this isolated query comparison; deployed
rate limits remain unchanged. PostgreSQL/OS buffers were warm and neither route
had a response cache. The new timing includes both privacy fence reads.

On the later retained 72,000-packet corpus, three alternating pairs compared the
grouped-hash count with the preceding query. Median trial HTTP p95 fell from
193 to 132 ms (32%), including both fresh privacy reads. All 120 normalized
response hashes matched. This quiet comparison does not replace the sustained
load gate or establish production capacity.

Timeline responses also reuse completed snapshots for the same network, window,
bucket and ready privacy generation. They are fresh for 30 seconds and may be
served during one background refresh up to a maximum age of 90 seconds, matching
the earlier HTTP content-cache window. Browser responses still require private
revalidation; every 200/304 checks current authority before and after loading its
payload. A refresh checks authority again before publication. Query/authority
failures never extend snapshot age or publish partial/error data.

The cache retains at most 16 entries and 8 MiB of estimated serialized values.
At most four fills are coalesced or detached; at capacity another cold request
keeps the original awaited SQL path. Physical TTL expiry and fixture shutdown
release retention. `meshcore_activity_timeline_refresh_inflight` and the fixed
`api_activity_timeline` cache label expose work and occupancy without scope IDs.

Three alternating comparisons on the same quiet 72k corpus measured warm HTTP
p95 at 126 → 3 ms (98% lower), with two warm-ups and 20 measured requests per
trial. A separate cold burst of 16 requests on each new router measured p95 at
1,081 → 138 ms (87% lower) and aggregate SQL calls at 16 → 1. All 216 measured
normalized bodies matched. Both servers used the same three-connection pool
and no-op limiter; deployed rate limits remain. These component measurements
do not replace the final sustained replay, polling and freshness checks.

Historic relay reactivation is a best-effort live-map hint after durable ingest.
It skips rows held by another writer and can update them on later traffic.
This avoids waiting or deadlocking on hot relay rows; packet/path/ledger writes
retain their authoritative transaction. A separate SQL connection holding a
relay lock verifies that free rows update and busy rows update after unlock.

## Sustained qualification

Keep the workload, warm-up and numerical acceptance rules with each run under
ignored `plans/measurements/`. A 50,000-entry invalidation cache needs about
28 minutes to fill at 30 unique packets per second. A 40-minute / 72,000-packet
run allows a retention comparison after that capacity has warmed. Check the
broker backlog/drop counters, exact packet/path/ledger persistence and final
queue drain alongside latency, RSS, heap, cache occupancy and map frames.

`bench:ingest` retains at most 15,000 quarter-second samples, covering an hour
plus bounded drain, including cache occupancy. Use separate private diagnostic
instrumentation for post-GC heap and referenced/unreferenced timers; compare
process RSS separately from the main isolate's heap. Temporary timer occupancy
depends on admission bursts, so verify that it returns to its pre-replay idle
level after traffic drains. Confirm browser-origin admission and actual live
WebSocket messages before treating a rendered map as a live-load measurement.

## Region snapshot convergence

Region snapshots rebuild outside HTTP in a separate supervised worker. Source
capacity, SQL/runtime deadlines, process heaps and container memory are bounded;
see [operations](operations.md#region-snapshot-worker). A failed rebuild retains
the complete previous snapshot. Serving still requires the current ready privacy
generation, so an old certificate is withheld until recovery succeeds.

Four alternating fresh-process before/after pairs on an owned retained-snapshot
copy contained 157,801 packets, 6,177 nodes and 58 mixed-width packet groups.
The complete 92-day scoped corpus had 3,798 packets, of which 3,402 decoded.
All five normalized output-table hashes matched across every run (excluding
the publication timestamp). One pair was warm-up. Measured medians were
13.89 versus 13.80 seconds and 199.6 versus 199.2 MiB peak process RSS.
There is no material speed or memory improvement on this fixture; replacing
per-node packet-hash sets with counts preserves exact distinct-packet semantics
without retaining each hash for every node. These measurements qualify this
source population, not execution at the configured maximum capacity.

The full-schema integration test creates its own invented database and proves
recovery after real privacy convergence, complete empty publication, and later
public-data restoration. Capacity rejection, a competing publisher, a concurrent
privacy change during publication, and nonempty decode failure preserve all
five committed tables. The child lifecycle test kills a process that ignores
SIGTERM at its deadline and after cancellation.

```bash
cd backend
TEST_SAMPLE_DATABASE_URL=postgresql://meshcore:meshcore@127.0.0.1:5433/meshcore \
  node --import tsx --test src/regions/snapshotRefresh.integration.test.ts
```

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
