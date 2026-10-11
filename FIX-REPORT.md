# UKMesh ingestion review repair report — 2026-10-11

The F01–F17 repair mission remains active. This report replaces the earlier
handoff status tables. Code fixes and completed evidence are recorded below;
the final whole-stack campaign and final-head CI are still required. The
specification is `/home/ben/ukmesh-fix-context-20261011/SOL-REVIEW.md`.

## Branch and validation checkpoint

- Perf/#122: `89680782c3c6a0e517640c9e02fca59f5c8a0bee`, pushed and verified
  against the remote. Typecheck and full backend suite pass **418/418**
  (138.715s). Both workflows [38174016916](https://github.com/gadgethd/ukmesh/actions/runs/38174016916)
  and [38174021349](https://github.com/gadgethd/ukmesh/actions/runs/38174021349)
  concluded success in every job. N=1 is supported; N>1 is explicitly gated.
- RAM/#125: public-read code checkpoint
  `c683acdec8572e6b6dbc5ba07ccdfcbb062f10ce`, pushed and remote-verified.
  Both workflows [38180468916](https://github.com/gadgethd/ukmesh/actions/runs/38180468916)
  and [38180473812](https://github.com/gadgethd/ukmesh/actions/runs/38180473812)
  concluded success in every job. Its typecheck, **476/476** full backend
  suite, **7 PG + 2 Redis + 1 WS** integration tests, and exact compiled
  image gate **10/10**, zero skips, passed.
- New RAM operational/read wave: all local gates pass, awaiting commit/push
  and its own CI. Final typecheck/build and **479/479** backend tests pass
  (193.952s), including the alert receiver. Full ingest integration passes
  **7 PostgreSQL + 3 Redis + 1 WS**, zero skips. The strict owned-ACL gate
  passes exactly **4 operational + 1 compiled-process test**, zero skips;
  runtime suite 152.153s. Loaded resident-memory refusal, real HTTP gauges,
  held SQL/SIGTERM recovery, quarantine refusal, actual retention expiry,
  standalone drain and durable rollback are all verified. The final Compose
  render passes all ten RAM/public-source settings, without outputting secrets.
- CoreScope local head: `07f05cd5562ac27478f4a46b5dd13d157e661162`, clean,
  committed and **not pushed**. Full `cmd/ingestor` passes (234.242s),
  including real PG/Redis integration; full `cmd/server` passes (103.965s).
  Shared-schema tests passed in the preceding unchanged schema wave.

Evidence below is in the newly owned directory
`/home/ben/ukmesh-bench/mqtt-ingest-perf-20261011/verification-sol-20261011/evidence/`.
The image `mqttbench-backend:sol-public-c683acd-20261011` carries source label
`c683acd` and configuration digest
`sha256:d96f200660518a8453fd494513193e0814cfb4bee9a4bfee10aa2e1a44094adc`.
`ukmesh-public-c683acd-image-integrity-pinned.log` proves exactly ten passing,
zero-skipped tests against shipped modules (88.480s). The test source is pinned
with `git show` to the image label. Missing mounts or zero-test exits cannot
pass. A newer operational image is still required.

## Findings and regression evidence

“Implemented” describes code/regression scope; final release gates are listed
separately. Every finding has a regression that reproduced the reviewed defect.
Later boundary defects also received new failing regressions.

| Finding | Status | Repair and evidence |
|---|---|---|
| F01 | Implemented; final paired public audit remains | Fresh completed policy/generation fences govern SQL aggregates, captured RAM, WS initial caches and bounded MQTT/WS live batches. SQL snapshot generation is never permission after a change. The final fence follows alias/summary waits. Tests: `F01 old SQL aggregate contributions fail closed across a policy generation change`, `F01 alias lookup cannot deliver a captured generation after authority changes`, actual ready WS observer/path flip, unavailable authority, cached reconnect and **800/800** allowed messages. CoreScope uses a dedicated revocable SQLite projection and private provenance journal; the read-only server fences REST/WS and replaces generation caches. Actual PG/SQLite tests revoke copied observer/source/all 1/2/3-byte prefixes across UK families while preserving unrelated networks; held REST and actual WS tests pass. |
| F02 | Implemented and real-PG verified | Same-client explicit transaction acquires the writer lock in a separate statement, then stable-ID ledger arbitrates every mutation. `F02 contended lock and replay never change historical tables` proves refusal changes no packet/path/hourly/daily/node/sighting/region table and replay changes none. This also addresses §7's lock/snapshot caveat. |
| F03 | Fixed on #122, merged into #125 | Canonical observation identity distinguishes observer, path, hop, scope, direction and raw frame; restart probes use the same identity. Named F03 identity/probe and real-PG regressions pass. |
| F04 | Fixed for supported N=1 | Unsupported shared-subscription N>1 configuration is rejected before connecting. `F04 shared-subscription N>1 is gated until restart and resize recovery is qualified` passes. No multicore recovery claim is made. |
| F05 | Implemented and real PG/Redis verified | Admission creates a stable UUID before Redis's ambiguous boundary; the bounded delivery ledger returns the stored entry on retry. PG's identity ledger preserves distinct stable IDs even with identical natural fields; natural fallback is restricted to legacy entries without IDs. Lost Redis reply, distinct same-ms identities and the wider SQL fixture pass. |
| F06 | Implemented and real PG/Redis verified | Per-entry commit timestamps retain payload/reservation for 120 seconds after commit. REST/WS capture bounded Redis overlap before SQL and deduplicate by stable UUID. `F06 real PG snapshot and Redis expiry preserve the captured handoff` holds an older Repeatable Read snapshot, commits A while admitting B, deletes expired A, and proves captured A plus later B remain discoverable exactly once. |
| F07 | Implemented; final offline paired audit remains | Migration 059 backfills historical/compressed observations under the same writer lock, preserves/assigns UUIDs and commits a coverage fence atomically. CoreScope requires coverage and uses committed-order `seq`, replaying retained history after cursor loss and generation change. Historical, interrupted migration, contended writer, older late commit and lost-cursor regressions pass. Deleted upstream retention is outside recoverable history. |
| F08 | Implemented and actual crash/SQLite failure verified | Both sources wait for tracked batch completion including the active last job; errors leave cursors behind. `TestF08RealPairedSourcesCrashFailureAndRestartBothOrders` reads real PG/Redis, kills an owned child at that boundary, reopens the same SQLite/WAL, injects observation failure and proves safe cursor hold/replay in both orders through the public projection. |
| F09 | Implemented and actual resident-memory verified | Atomic final deletion releases reservation once; bounded Redis-side reconciliation serializes with admission/cleanup. Retained records and ambiguous delivery copies remain reserved. Startup/admission require bounded noeviction Redis and 32 MiB recovery headroom. Real concurrent admission/reconciliation, lost counter, retained replay and lost cleanup reply pass. Encoded quarantine is capped at 16 KiB and exactly 500 records; actual physical allocation fits the separate headroom. Unsafe configuration regression fails on `c683acd`; valid-budget admission also refuses when other resident keys consume the recovery space. |
| F10 | Implemented and real group-loss verified | Supervised startup retries before MQTT intake; failed state is cleared. NOGROUP repairs the group, young foreign PEL is periodically/boundedly reclaimed, and commit/stall failures affect MQTT readiness. F10 group-loss and aging foreign-PEL regressions plus the historical compiled fault gate pass. |
| F11 | Implemented and real-ioredis verified | Real XADD/trim quarantine precedes source cleanup; all read paths share validated decode. Permanent SQL poison is isolated so its valid neighbor commits. Quarantine failure retains source. Real malformed/quarantine, own/reclaimed PEL and SQLSTATE 23514 poison-neighbor tests pass; escaping no longer inflates encoded evidence above its cap. |
| F12 | Implemented and real SQL/RAM verified | UUIDs distinguish overlap from new observations, including repeated sightings by one observer. Canonical observer arrays and RX/TX counts match real SQL across handoff, alias/family scopes and paging. |
| F13 | Implemented and wide real SQL/RAM verified | Bounded whole-stream capture, SQL aggregates for overlap hashes, representative rank/UUID ties, millisecond order, final distinct-hash paging, windows, full/slim/message fields, alias/family scopes and decrypted/JSON summary extraction match SQL. New fallback regression failed before repair. Packet detail and long channel history are explicitly PG views until commit; documented pending lists do not promise those views. |
| F14 | Implemented and loaded HTTP-scrape verified | Follow-up metrics use the served registry. Pending/retained/PEL/physical/reserved gauges are separate; minimum admitted age, zero-capacity read skip and bounded reclaim are tested. Loaded-state and missing-stream gauges are asserted through the actual HTTP metrics server. Commit histogram counts actual inserted observations. Missing-group/null-memory regression reproduced stale gauges before repair. |
| F15 | Implemented; corrected campaign oracle remains | MQTT uses one deadline beginning before disconnect. RAM joins the active loop before draining and returns incomplete on deadline/PEL/failure; owned clients close. Unit held-write regressions and actual compiled SIGTERM gate pass: a held INSERT causes exit 1 at two seconds, then recovery waits for release and commits the original UUID with every DB side effect exactly once. The stock old bench oracle is unsuitable; the new campaign must use independent identity/state checks. |
| F16 | Implemented; final whole-stack configuration audit remains | Compose passes RAM settings and the filtered bearer HTTP token; durable stays default. Public CoreScope receives no raw Redis URL/DSN and requires the paired PG ledger/projection. Actual Redis principals deny public/operator raw stream access, separate quarantine review, and HTTP revocation filtering passes. The compiled runtime uses the restricted writer principal and proves quarantine refusal, real retention sweep, zero stream/PEL/reservation and standalone drain before durable rollback. Final Compose render passes; whole-stack rerun remains. |
| F17 | Implemented and actual restart/both-order verified | CoreScope preserves upstream UUID, enriches duplicates without incrementing derived counts and retains original privacy participants. Schema reapplication preserves distinct upstream identities and repairs actual legacy/missing indexes despite migration markers. F17 schema, both-order identity/count and paired crash/replay tests pass. |

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
`evidence/corescope-sol-relay-20261011.patch` (4681 lines). Recreate it with:

```sh
git -C /home/ben/corescope-ukmesh/src format-patch --stdout 90c104eab51618894a3b69ab521829f39313e261..07f05cd5562ac27478f4a46b5dd13d157e661162
```

## Remaining acceptance and limitations

Commit/push the locally verified RAM wave, watch both new CI
workflows to actual success, build a source-labelled image and rerun the strict
compiled gates. Then run an independently isolated whole-stack campaign with
actual CoreScope processes, ready/joined readers, exact observation and
side-effect checks, complete measurement windows, Redis AOF, PG WAL and physical
I/O. Compare durable/RAM using the same final code and repeated workloads;
report overhead as well as latency. Update both existing PR descriptions while
preserving #122 open/non-draft and #125 open/draft. Do not merge or deploy.

RAM remains opt-in: ACK precedes PG commit and a Redis loss can lose acknowledged
telemetry. Current policy unavailability/rematerialization deliberately makes
public reads unavailable. Previously delivered/network-in-flight bytes cannot
be recalled. Old untracked/mixed CoreScope data is preserved and fenced; migrate
to a new dedicated projection backed by retained PG history. Best-effort
post-commit enrichment is bounded but not transactional/exactly once. N>1
remains gated. No whole-stack savings or three-repeat performance claim exists
for the new code yet. The historical measurements below remain evidence only
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
CoreScope has not been pushed. The full mission remains active.
