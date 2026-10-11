# UKMesh ingestion review repair report — 2026-10-11

The F01–F17 review repairs are complete for the supported N=1 configuration.
Every finding has a failing-before/passing-after regression, and the final-code
local, compiled-image, paired-service and CI gates pass. Both UKMesh branches
are pushed; three clean CoreScope commits await coordinator relay as requested.
The specification is `/home/ben/ukmesh-fix-context-20261011/SOL-REVIEW.md`.
This report identifies the production-code revision used in the campaign.
The final documentation-only checkpoint has identical runtime build inputs;
its exact hash and fresh CI runs are recorded in PR #125 and the final handoff.

## Branch and validation checkpoint

- Perf/#122: `89680782c3c6a0e517640c9e02fca59f5c8a0bee`, pushed and verified
  against the remote. Typecheck and full backend suite pass **418/418**
  (138.715s). Both workflows [38174016916](https://github.com/gadgethd/ukmesh/actions/runs/38174016916)
  and [38174021349](https://github.com/gadgethd/ukmesh/actions/runs/38174021349)
  concluded success in every job. N=1 is supported; N>1 is explicitly gated.
- RAM/#125 code head: `c02aa32fd9f5a6e6d97e8f553214e7543dcb0fce`,
  pushed and remote-verified. Both workflows
  [38182374772](https://github.com/gadgethd/ukmesh/actions/runs/38182374772)
  and [38182378151](https://github.com/gadgethd/ukmesh/actions/runs/38182378151)
  concluded success in every job. Typecheck/build and **479/479** backend
  tests pass; the final release rerun also passes in 201.590s, including
  the alert receiver. Full ingest integration
  passes **7 PostgreSQL + 3 Redis + 1 WS**, zero skips. The strict owned-ACL
  gate passes exactly **4 operational + 1 compiled-process test**, zero skips;
  runtime suite 152.153s. Loaded resident-memory refusal, real HTTP gauges,
  held SQL/SIGTERM recovery, quarantine refusal, actual retention expiry,
  standalone drain and durable rollback are verified. Compose renders pass
  all ten RAM/public-source settings without outputting secrets.
- CoreScope local head: `07f05cd5562ac27478f4a46b5dd13d157e661162`, clean,
  committed and **not pushed**. Full `cmd/ingestor` passes (234.242s),
  including real PG/Redis integration; full `cmd/server` passes (103.965s).
  Shared-schema tests passed in the preceding unchanged schema wave.

Evidence below is in the newly owned directory
`/home/ben/ukmesh-bench/mqtt-ingest-perf-20261011/verification-sol-20261011/evidence/`.
The campaign image `mqttbench-backend:sol-operational-c02aa32-20261011` carries
revision label `c02aa32` (resolving to the full code head above) and configuration digest
`sha256:c4fd5e12407e419cd2b30e7adc0bcff3808261043d672feead342067a77a87f6`.
`ukmesh-c02aa32-image-integrity.log` proves **11/11**, zero skips (93.434s),
and `ukmesh-c02aa32-image-operational.log` proves **4/4 + 1/1**, zero skips,
including the actual compiled process gate (148.978s). Fixture sources are
pinned with `git show` to that image label and import only shipped modules.
Missing mounts, zero-test exits or skipped fixtures cannot pass.

## Findings and regression evidence

“Fixed” describes the reviewed defect within the qualified N=1 scope. Every finding has a regression that reproduced the reviewed defect.
Later boundary defects also received new failing regressions.

| Finding | Status | Repair and evidence |
|---|---|---|
| F01 | Fixed; actual paired public audit passed | Fresh completed policy/generation fences govern SQL aggregates, captured RAM, WS initial caches and bounded MQTT/WS live batches. SQL snapshot generation is never permission after a change. The final fence follows alias/summary waits. Tests: `F01 old SQL aggregate contributions fail closed across a policy generation change`, `F01 alias lookup cannot deliver a captured generation after authority changes`, actual ready WS observer/path flip, unavailable authority, cached reconnect and **800/800** allowed messages. CoreScope uses a dedicated revocable SQLite projection and private provenance journal; the read-only server fences REST/WS and replaces generation caches. Actual PG/SQLite tests revoke copied observer/source/all 1/2/3-byte prefixes across UK families while preserving unrelated networks; held REST and actual WS tests pass. |
| F02 | Fixed and real-PG verified | Same-client explicit transaction acquires the writer lock in a separate statement, then stable-ID ledger arbitrates every mutation. `F02 contended lock and replay never change historical tables` proves refusal changes no packet/path/hourly/daily/node/sighting/region table and replay changes none. This also addresses §7's lock/snapshot caveat. |
| F03 | Fixed on #122, merged into #125 | Canonical observation identity distinguishes observer, path, hop, scope, direction and raw frame; restart probes use the same identity. Named F03 identity/probe and real-PG regressions pass. |
| F04 | Fixed for supported N=1 | Unsupported shared-subscription N>1 configuration is rejected before connecting. `F04 shared-subscription N>1 is gated until restart and resize recovery is qualified` passes. No multicore recovery claim is made. |
| F05 | Fixed and real PG/Redis verified | Admission creates a stable UUID before Redis's ambiguous boundary; the bounded delivery ledger returns the stored entry on retry. PG's identity ledger preserves distinct stable IDs even with identical natural fields; natural fallback is restricted to legacy entries without IDs. Lost Redis reply, distinct same-ms identities and the wider SQL fixture pass. |
| F06 | Fixed and real PG/Redis verified | Per-entry commit timestamps retain payload/reservation for 120 seconds after commit. REST/WS capture bounded Redis overlap before SQL and deduplicate by stable UUID. `F06 real PG snapshot and Redis expiry preserve the captured handoff` holds an older Repeatable Read snapshot, commits A while admitting B, deletes expired A, and proves captured A plus later B remain discoverable exactly once. |
| F07 | Fixed; actual offline/cursor-loss audit passed | Migration 059 backfills historical/compressed observations under the same writer lock, preserves/assigns UUIDs and commits a coverage fence atomically. CoreScope requires coverage and uses committed-order `seq`, replaying retained history after cursor loss and generation change. Historical, interrupted migration, contended writer, older late commit and lost-cursor regressions pass. Deleted upstream retention is outside recoverable history. |
| F08 | Fixed and actual crash/SQLite failure verified | Both sources wait for tracked batch completion including the active last job; errors leave cursors behind. `TestF08RealPairedSourcesCrashFailureAndRestartBothOrders` reads real PG/Redis, kills an owned child at that boundary, reopens the same SQLite/WAL, injects observation failure and proves safe cursor hold/replay in both orders through the public projection. |
| F09 | Fixed and actual resident-memory verified | Atomic final deletion releases reservation once; bounded Redis-side reconciliation serializes with admission/cleanup. Retained records and ambiguous delivery copies remain reserved. Startup/admission require bounded noeviction Redis and 32 MiB recovery headroom. Real concurrent admission/reconciliation, lost counter, retained replay and lost cleanup reply pass. Encoded quarantine is capped at 16 KiB and exactly 500 records; actual physical allocation fits the separate headroom. Unsafe configuration regression fails on `c683acd`; valid-budget admission also refuses when other resident keys consume the recovery space. |
| F10 | Fixed and real group-loss verified | Supervised startup retries before MQTT intake; failed state is cleared. NOGROUP repairs the group, young foreign PEL is periodically/boundedly reclaimed, and commit/stall failures affect MQTT readiness. F10 group-loss and aging foreign-PEL regressions plus the historical compiled fault gate pass. |
| F11 | Fixed and real-ioredis verified | Real XADD/trim quarantine precedes source cleanup; all read paths share validated decode. Permanent SQL poison is isolated so its valid neighbor commits. Quarantine failure retains source. Real malformed/quarantine, own/reclaimed PEL and SQLSTATE 23514 poison-neighbor tests pass; escaping no longer inflates encoded evidence above its cap. |
| F12 | Fixed and real SQL/RAM verified | UUIDs distinguish overlap from new observations, including repeated sightings by one observer. Canonical observer arrays and RX/TX counts match real SQL across handoff, alias/family scopes and paging. |
| F13 | Fixed and wide real SQL/RAM verified | Bounded whole-stream capture, SQL aggregates for overlap hashes, representative rank/UUID ties, millisecond order, final distinct-hash paging, windows, full/slim/message fields, alias/family scopes and decrypted/JSON summary extraction match SQL. New fallback regression failed before repair. Packet detail and long channel history are explicitly PG views until commit; documented pending lists do not promise those views. |
| F14 | Fixed and loaded HTTP-scrape verified | Follow-up metrics use the served registry. Pending/retained/PEL/physical/reserved gauges are separate; minimum admitted age, zero-capacity read skip and bounded reclaim are tested. Loaded-state and missing-stream gauges are asserted through the actual HTTP metrics server. Commit histogram counts actual inserted observations. Missing-group/null-memory regression reproduced stale gauges before repair. |
| F15 | Fixed; compiled drain and exact campaign oracle passed | MQTT uses one deadline beginning before disconnect. RAM joins the active loop before draining and returns incomplete on deadline/PEL/failure; owned clients close. Unit held-write regressions and actual compiled SIGTERM gate pass: a held INSERT causes exit 1 at two seconds, then recovery waits for release and commits the original UUID with every DB side effect exactly once. The owned campaign uses independent identity/state checks and proves full physical drain; the stock old bench oracle remains unsuitable. |
| F16 | Fixed; actual configuration/ACL/rollback audits passed | Compose passes RAM settings and the filtered bearer HTTP token; durable stays default. The public server receives no raw PG/Redis credential; its private ingestor uses a restricted PG reader, filtered HTTP and the committed ledger/projection. Actual Redis principals deny public/operator raw stream access, separate quarantine review, and HTTP revocation filtering passes. The compiled runtime uses the restricted writer principal and proves quarantine refusal, real retention sweep, zero stream/PEL/reservation and standalone drain before durable rollback. Both final Compose renders pass; actual paired public server has no raw Redis/PG credential. |
| F17 | Fixed and actual restart/both-order verified | CoreScope preserves upstream UUID, enriches duplicates without incrementing derived counts and retains original privacy participants. Schema reapplication preserves distinct upstream identities and repairs actual legacy/missing indexes despite migration markers. F17 schema, both-order identity/count and paired crash/replay tests pass. |

## Code and fixture references

| Findings | Production code | Regression fixtures |
|---|---|---|
| F01 | `backend/src/ram/liveRead.ts`, `publicSource.ts`, `backend/src/privacy/privatePrefixCache.ts`, `backend/src/mqtt/client.ts`, `backend/src/ws/server.ts`; CoreScope `cmd/ingestor/public_projection.go`, `public_live_source.go`, `cmd/server/public_projection.go` | `backend/src/ws/publicAuthorization.integration.test.ts`, `backend/src/db/recentRead.integration.test.ts`, `backend/src/ram/operational.integration.test.ts`; CoreScope public projection/source tests; actual paired audits |
| F02/F05 | `backend/src/db/packetBatch.ts`, migrations 058/059 | `backend/src/db/packetBatch.integration.test.ts`, `backend/src/ram/ram.integration.test.ts` |
| F03/F04 | `backend/src/mqtt/observationIdentity.ts`, `durableHandler.ts`, `client.ts` | observation identity/durable handler tests and real PG ledger fixture |
| F06/F09–F11/F14/F15 | `backend/src/ram/liveStore.ts`, `writer.ts`, `writerRuntime.ts`, `protocol.ts`, `memory.ts`, `drain.ts`; `backend/src/metrics.ts`, `mqtt/shutdownDeadline.ts` | RAM unit/PG/Redis/operational/runtime fixtures and MQTT deadline tests |
| F07/F08/F17 | migration 059; CoreScope `cmd/ingestor/postgres_source.go`, `redis_source.go`, `public_live_source.go`, `ingest_buffer.go`, `source_identity.go`, `internal/dbschema/dbschema.go` | CoreScope source/buffer/paired integration and schema restart fixtures; actual offline audit |
| F12/F13 | `backend/src/ram/liveRead.ts`, `backend/src/db/index.ts` | `backend/src/db/recentRead.integration.test.ts`, `backend/src/ram/liveStore.test.ts` |
| F16 | `docker-compose.yml`, `scripts/test-ram-redis-acl.mjs`, `docs/ram-first-ingestion.md` | actual Compose render, ACL operational and compiled rollback fixtures |

## Recent evidence and relay

- Read/security before/after logs: `ukmesh-ws-authority-baseline-ready.log`,
  `ukmesh-ws-authority-after.log`, `ukmesh-f01-alias-before-valid.log`,
  `ukmesh-summary-fence-focused2.log`, `ukmesh-f13-summary-before.log`,
  `ukmesh-f13-summary-final.log`, and the earlier paging/tie failure logs.
- Resource/operational logs: `ukmesh-f09-headroom-before.log`,
  `ukmesh-f09-quarantine-before.log`, `ukmesh-f14-loaded-before-valid.log`,
  `ukmesh-f14-loaded-after.log`, `ukmesh-operational-final-runtime.log`,
  `ukmesh-operational-final-full-tests2.log`,
  `ukmesh-operational-final-runtime2.log` and
  `ukmesh-operational-final-ingest-integration2.log`.
- CoreScope logs: `core-f01-copied-before.log`,
  `core-f01-untracked-node-before-valid.log`, `core-f01-enrichment-before.log`,
  `core-public-paired-final2.log`, `core-public-final-ingestor2.log`,
  `core-public-final-server2.log`.

Three clean local CoreScope commits await coordinator relay, in this order:

1. `e9afb3a8a17f8b65d1463985df18984c8f8b86f4` — paired source completion,
   current policy checks, committed-order cursor and upstream identity.
2. `ff9eb3b93c9addbb29feb9debcc12e6cb99d5c8d` — historical coverage,
   real paired crash/replay and shared-schema identity repair.
3. `07f05cd5562ac27478f4a46b5dd13d157e661162` — copied-data revocation,
   provenance reconstruction, filtered live source and fenced public server.

The ready three-commit patch is
`evidence/corescope-sol-relay-final-20261011.patch` (4681 lines; SHA-256
`ca17c74daeb01904244bdd4eea74357477a448e63f5d787a50b1f483fdb59768`). Recreate it with:

```sh
git -C /home/ben/corescope-ukmesh/src format-patch --stdout 90c104eab51618894a3b69ab521829f39313e261..07f05cd5562ac27478f4a46b5dd13d157e661162
```

## Paired-service acceptance

The actual paired privacy audit passed. Pending rematerialization closed
CoreScope reads; completed generation 3 removed two copied revoked observers
while three allowed controls remained visible. Actual UKMesh live WS delivered
4/4 allowed controls and zero revoked frames; the filtered source excluded the
same private raw observations. Authority loss returned 503 from CoreScope and
the filtered source. Restoration replayed retained PG ledger history. Separate
public-server configuration contains no raw PG or Redis credential. Evidence:
`whole-stack-paired-privacy.log` and `whole-stack/runs/paired-privacy-audit.json`.
The actual offline/cursor-loss audit also passed after Redis retention expired:
**100 MQTT observations + 1 older-timestamp late commit**, exact upstream UUIDs
**101/101**, recovered into a fresh CoreScope projection with missing cursors.
The late observation has ledger sequence 119 after the prior maximum 118 while
its timestamp is older. The complete projection has **119 observations = 119
journal records = 119 derived observer contributions**. Redis stream, pending,
PEL and reservation are all zero. Real retained cleanup waited 126.384s after
commit; no fake clock or early trim was used. Evidence:
`whole-stack-paired-offline.log` and `whole-stack/runs/paired-offline-audit.json`.
The repeated final-code matrix passed all 14 runs; its results follow below. The current campaign owns every service and run directory.
It measures six enabled pipeline containers (UKMesh, Redis, TimescaleDB,
Mosquitto, CoreScope ingestor and read-only server), plus its Node publisher/
HTTP/PG/WS probe controller and a per-run SQLite reader. Prepared synthetic
frames, container startup and image builds precede the measurement baseline.
All reader tasks, WebSocket clients, publishers and per-run SQLite children
stop and join before the endpoint; retained cleanup has separate CPU, PG,
AOF and physical I/O snapshots. Memory is a simultaneous sum, sampled at about
one second, including page cache; it is not the sum of component maxima.
Docker daemon/host overhead, other VPS applications, optional observed-link
workers and a production-sized history are outside this fixture. The optional
observed-link worker is absent; its bounded post-commit admission reports
`worker_unavailable`. Nothing here qualifies N>1 or a production deployment.

SQLite latency records when the probe can confirm the stable upstream UUID
which PG exposes at commit, so it does not measure early live-copy visibility.
CoreScope REST observes raw frames independently and measures that visible
path. Latencies are matched to the publisher's per-frame timestamp, with
readiness completed first. Three burst/10/s repetitions use alternating mode
order; the 2/s tail case is a single diagnostic pair. Endpoint durations can
differ between modes. PG statistics and container physical I/O cover these
completed windows; checkpoint activity and background maintenance can add
writes. AOF size deltas are reported only if no rewrite occurred, with rewrite,
fsync and write-status fields retained in each raw artifact. These are scoped
synthetic measurements, not packet-attributed physical I/O or whole-VPS savings.

## Final-code measured campaign

All **14 measured runs / 6,840 observations** pass exact hash/observer/path
identity sets, distinct PG UUIDs, packet/ledger/path/hourly/RSSI/SNR/region
side-effect deltas and the daily maximum-hop invariant. Every run's five
readers sees its full expected set. Pending and PEL are zero at commit;
physical stream and reservation are zero only after real retained expiry.
No processed counter or permissive XLEN threshold participates in the oracle.
Each owned backend then exits zero after SIGTERM. The source-labelled
production image and actual CoreScope binaries match the manifest above.

CPU/memory/I/O below use the common completed reader window for each run.
Cleanup uses its own subsequent measured window. Cells are arithmetic means
of the stated repetitions; CPU/memory ranges are observed run ranges, and
latency cells are means of per-run matched p50/p95. The low-tail pair has one
repetition. Raw snapshots and expected/actual identity sets remain in
`whole-stack/runs/<run>.json`. The AOF had no rewrites or delayed-fsync errors
in any measured window.

| Workload | Mode | Repeats | CPU s/1k to joined readers | Backend CPU s/1k | Cleanup CPU s/1k | PG WAL MiB | Redis AOF MiB | Physical writes MiB | Simultaneous memory MiB |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|
| burst 800 | durable | 3 | 64.25 (58.94–69.76) | 14.46 | 0.01 | 6.93 | 0.00 | 134.90 | 542.96–641.66 |
| burst 800 | ram | 3 | 42.81 (39.98–46.01) | 14.19 | 39.52 | 5.26 | 1.98 | 160.55 | 571.95–680.01 |
| sustained 300 | durable | 3 | 96.26 (95.16–97.64) | 27.21 | 0.05 | 2.90 | 0.00 | 52.08 | 623.45–679.05 |
| sustained 300 | ram | 3 | 101.41 (100.00–102.23) | 39.17 | 67.14 | 1.97 | 0.79 | 59.93 | 622.36–694.77 |
| low-tail 120 | durable | 1 | 291.02 (291.02–291.02) | 97.67 | 0.06 | 2.48 | 0.00 | 28.75 | 697.27–697.27 |
| low-tail 120 | ram | 1 | 763.44 (763.44–763.44) | 293.27 | 103.39 | 0.90 | 0.33 | 34.27 | 695.02–695.02 |

| Workload | Mode | UK WS p50 / p95 ms | UK API p50 / p95 ms | PG p50 / p95 ms | Core SQLite ID check p50 / p95 ms | CoreScope REST p50 / p95 ms |
|---|---|---:|---:|---:|---:|---:|
| burst 800 | durable | 19240 / 36336 | 19495 / 36743 | 19389 / 36648 | 20579 / 37046 | 21845 / 37806 |
| burst 800 | ram | 1567 / 2575 | 2109 / 3222 | 6600 / 11651 | 7457 / 13101 | 20924 / 24334 |
| sustained 300 | durable | 68 / 127 | 348 / 541 | 331 / 525 | 935 / 1476 | 1577 / 2564 |
| sustained 300 | ram | 49 / 107 | 432 / 676 | 6339 / 10839 | 6344 / 10844 | 1445 / 3183 |
| low-tail 120 | durable | 67 / 123 | 270 / 432 | 236 / 409 | 781 / 1316 | 1331 / 2011 |
| low-tail 120 | ram | 49 / 89 | 307 / 464 | 30885 / 118420 | 30887 / 118422 | 982 / 1765 |

Through full physical drain, whole-fixture CPU seconds per 1,000 are
**64.26 durable / 82.33 RAM** for burst, **96.30 / 168.56** at 10/s, and
**291.08 / 866.83** for the single 2/s tail pair. Excluding the two
instrumentation processes, six-container totals through drain are respectively
**55.37 / 66.01**, **80.47 / 138.14**, and **221.43 / 700.07**.
Thus the measured cleanup cost is not solely the controller. RAM improves
UKMesh burst live latency and reduces PG WAL/fsyncs, but does not establish a
whole-fixture CPU saving through retained cleanup. At ordinary rates PG
visibility waits for batches; packet detail/history retain that delay. Memory
ranges overlap, and RAM adds AOF writes and CoreScope/Redis bookkeeping.
No production or whole-VPS efficiency improvement is claimed.

Mean PG committed transactions / WAL fsyncs in the common reader window:
burst durable **3,444 / 854**, RAM **1,603 / 67**; sustained durable
**1,667 / 327**, RAM **1,883 / 25**; low-tail durable **1,885 / 140**, RAM
**6,641 / 22**. These include queries and background work, not just packet
transactions. A checkpoint occurred in one of the three durable sustained
windows and in the RAM low-tail window; other measured reader windows had
none. Full-drain PG WAL / Redis AOF growth averages: burst durable
**6.93 / 0 MiB**, RAM **5.68 / 2.29 MiB**; sustained durable **2.90 / 0 MiB**,
RAM **1.97 / 0.91 MiB**; low-tail durable **2.48 / 0 MiB**, RAM
**1.63 / 0.38 MiB**. Physical I/O includes SQLite, filesystem and background
maintenance, so it cannot be attributed solely to packet persistence.

Evidence: `whole-stack-final-code-campaign.log`,
`whole-stack-final-campaign-analysis.md`, `whole-stack/runs/campaign-analysis.json`,
`whole-stack/runs/source-and-service-manifest.json` and the per-run snapshots.
The separate **3,000-observation saturation gate passed**: PG **3,000/3,000**,
CoreScope **3,000/3,000**, WS **3,000/3,000**, exact identities and historical
side effects, and zero final stream/PEL/reservation. The captured actual metrics
scrape records pending **0**, retained **3,000**, PEL **0**, and reservation
**7,224,000 bytes** before retention expiry. A later diagnostic scrape crossed
expiry; it was not counted as a gate. Evidence: `whole-stack-saturation-3000.log`,
`whole-stack/runs/saturation-3000-integrity.json` and its saved metrics.

An independent final audit compares all **9,959** retained upstream ledger UUIDs
with all **9,959** CoreScope UUIDs and checks each run's exact raw/direction/
network/observer/path tuple. The UUID set SHA-256 is identical on both sides:
`93e9060fe4a8201e85180f616db50273c68647514480330cab0c1a102b240e55`.
CoreScope **observations = journal records = derived observer contributions =
9,959**. This includes **9,840** measured/stress observations plus the earlier
119 audit observations. Every tested stream, PEL and reservation is empty.
Evidence: `whole-stack-final-identity-audit.log` and
`whole-stack/runs/final-identity-audit.json`. Public configuration responses
also expose none of the fixture's bearer tokens, passwords or other secrets;
see `whole-stack/runs/public-config-credential-audit.json`.

RAM remains opt-in: ACK precedes PG commit and a Redis loss can lose acknowledged
telemetry. Current policy unavailability/rematerialization deliberately makes
public reads unavailable. Previously delivered/network-in-flight bytes cannot
be recalled. Old untracked/mixed CoreScope data is preserved and fenced; migrate
to a new dedicated projection backed by retained PG history. Best-effort
post-commit enrichment is bounded but not transactional/exactly once. N>1
remains gated. The repeated final-code measurements above qualify only this owned fixture;
no whole-VPS or production savings claim is made. The historical measurements below remain evidence only
for their explicitly older revisions.

## Historical bench measurements — earlier code only

Both Docker images carry the source revision label (`cbf78a8` durable, `022b7c8` RAM). The isolated final-chain marker preceded these builds and runs. The strict gate compares publisher distinct `(packet hash, observer)` observations against packet, path, hourly and, except for the privacy case, region deltas. From the sustained durable run onward, it also saves the exact PostgreSQL identity set for a separate equality audit. Reader containers are stopped before the next scenario's reset. WAL bytes are from `pg_stat_wal`; Redis AOF size is compared only if no rewrite occurred. CPU and memory below cover backend, Redis and TimescaleDB; broker, publisher, reader probes and CoreScope are not included.

| Run | Exact observations | Side-effect deltas | Three-service CPU seconds/1k | PG WAL bytes | Redis AOF bytes | Simultaneous three-service peak |
|---|---:|---|---:|---:|---:|---:|
| Durable 2/s `s1-low` | 360/360 by counts; exact row-key snapshot not yet available | packet/path/hourly/region +360 | 90.363 | unavailable | unavailable | unavailable |
| Durable 10/s `s2-sustained` | 3,000/3,000 exact identity match | packet/path/hourly/region +3,000 | 46.771 | 30,829,367 | 0 | 501.4 MiB |
| Durable 800 burst `s3-burst` | 800/800 exact identity match | packet/path/hourly/region +800 | 39.620 | 7,069,812 | 0 | 530.0 MiB |
| RAM 10/s batch 50 `s2-sustained` | 3,000/3,000 exact identity match | packet/path/hourly/region +3,000 | 60.839† | 28,082,965 | 9,177,589 | 574.0 MiB |
| RAM 800 burst batch 50 `s3-burst` | 800/800 exact identity match | packet/path/hourly/region +800 | 70.962† | 7,996,005 | 2,366,357 | 546.1 MiB |
| RAM 10/s batch 100 `s2-sustained` | 3,000/3,000 exact identity match | packet/path/hourly/region +3,000 | 59.013† | 29,572,769 | 9,174,060 | 571.7 MiB |
| RAM 800 burst batch 100 `s3-burst` | 800/800 exact identity match | packet/path/hourly/region +800 | 67.221† | 8,983,539 | 2,368,489 | 546.0 MiB |
| RAM 10/s batch 250 `s2-sustained` | 3,000/3,000 exact identity match | packet/path/hourly/region +3,000 | 60.392†‡ | 29,194,347 | unavailable | 575.0 MiB |
| RAM 800 burst batch 250 `s3-burst` | 800/800 exact identity match | packet/path/hourly/region +800 | 99.674† | 11,093,658 | 2,493,807 | 515.3 MiB |
| RAM 10/s batch 500 `s2-sustained` | 3,000/3,000 exact identity match | packet/path/hourly/region +3,000 | 63.901† | 27,646,272 | 9,515,048 | 545.1 MiB |
| RAM 800 burst batch 500 `s3-burst` | 800/800 exact identity match | packet/path/hourly/region +800 | 106.671† | 11,118,472 | 3,063,053 | 513.6 MiB |

These numbers are resource observations for isolated completed windows, not whole-stack or production savings. †The normal scenario runner measures CPU through its final drain. RAM retains committed entries for about 120 seconds, so its run can include a longer post-publisher period than durable mode; the sweep's CPU totals alone are not an equal-duration comparison. ‡The stock bench runner timed out at 240 seconds with 100 already committed entries still in retention. A separate post-timeout wait then observed stream=0 and PEL=0 before the next scenario; its first after-snapshot and CPU window ended at the timeout. The bench-only resumed runner uses a 420-second drain wait for larger batches. The original reader probes start asynchronously; a two-second burst can begin before WebSocket connection, so their burst WebSocket coverage is not a delivery guarantee. The clean-warmup rerun below captures publisher-end and all-observations-committed CPU/WAL/AOF snapshots for both variants, then reports retained cleanup separately.

### Historical joined-reader comparison

The clean runner waits for all four reader probes to signal ready before publishing. For RAM, it first waits until all 100 warmup observations are committed with PEL zero, then trims only those retained warmup stream entries while the isolated backend is paused. The comparison endpoint is all expected observations committed, with RAM PEL zero; retained cleanup is measured afterward. CPU includes backend, Redis and TimescaleDB, and excludes broker, publishers and reader containers. WAL comes from `pg_stat_wal`; AOF is the Redis persistence delta. Each row is a single run, so these are workload observations rather than repeatable savings estimates.

| Workload | Variant | Three-service CPU s/1k to commit | PG WAL bytes | Redis AOF bytes | Three-service peak to commit | API seen / p50 | WS seen / matched p50 | PG follower seen / matched p50 |
|---|---|---:|---:|---:|---:|---|---|---|
| 10/s, 3,000 main | Durable | 52.254 | 32,567,045 | 0 | 520.6 MiB | 3,000/3,000; 417 ms | 2,595/3,000; 59 ms | 3,000/3,000; 1,559 ms |
| 10/s, 3,000 main | RAM batch 100 | 54.280 | 27,323,965 | 8,675,821 | 589.1 MiB | 3,000/3,000; 443 ms | 2,450/3,000; 25 ms | 3,000/3,000; 7,640 ms |
| 800 burst | Durable | 40.382 | 9,043,221 | 0 | 560.6 MiB | 800/800; 16,650 ms | 750/800; 16,102 ms | 800/800; 17,824 ms |
| 800 burst | RAM batch 100 | 23.662 | 6,394,221 | 2,053,392 | 530.1 MiB | 800/800; 727 ms | 2/800; 494 ms | 800/800; 7,855 ms |
| 2/s, 360 main | Durable | 100.867 | 4,931,845 | 0 | 527.6 MiB | 360/360; 346 ms | 197/360; 60 ms | 360/360; 1,555 ms |
| 2/s, 360 main | RAM batch 100 | 158.709 | 4,821,330 | 1,156,591 | 530.6 MiB | 360/360; 318 ms | 163/360; 26 ms | 300/360; 27,052 ms |

The six-run log is `/home/ben/ukmesh-bench/mqtt-ingest-perf-20261011/logs/clean-window-comparison-20261011-200123.log`; all six runs passed exact main and warmup PostgreSQL observation identity and packet/path/hourly/region deltas. The burst RAM stream probe saw 800/800 at matched p50 737 ms. Its WebSocket probe was connected 1.36 seconds before publisher start yet recorded only 2/800; no WebSocket delivery improvement is established. The low-rate RAM stream probe saw 360/360 at p50 380 ms. The low-rate RAM PG follower saw only 300/360 in its bounded reader window, with matched p50 27,052 ms; the durable follower saw 360/360. For sustained and low-rate traffic, RAM used more three-service CPU than durable at the commit-complete endpoint. PostgreSQL WAL was lower but Redis AOF was added in all three pairs. These are single-run isolated observations, not whole-stack savings estimates.

## Operational boundary

Production is untouched: no production DB, Redis, broker, configuration,
service, process or deployment was modified. No existing service was restarted,
and no PR was merged or approved. The original campaign's `FINAL-CHAIN-DONE`
was personally observed at 17:39:52 UTC before isolated verification began.
New fixtures use disposable owned DBs/keys/containers/processes and the owned
`verification-sol-20261011` directory; other run directories remain read-only.
One old owned WS diagnostic process was stopped after its evidence was captured.
CoreScope has not been pushed. All requested code and verification work is complete; CoreScope relay is the
coordinator’s next action. The PRs retain their requested draft states.
