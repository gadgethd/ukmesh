# UKMesh ingestion review repairs — working report (2026-10-11)

Specification: `/home/ben/ukmesh-fix-context-20261011/SOL-REVIEW.md`. This is a working record; **the full repair objective is not yet achieved**. Status below reflects inspected code and completed verification only.

## Branches and gates

**Successor progress, 2026-10-11.** The authoritative remote checkpoints were
perf `8968078` and RAM `e0bddc1`, both matching their local worktrees. Perf
workflows `38174016916` / `38174021349` and RAM checkpoint workflows
`38174696402` / `38174699642` have now concluded **success**.

The next repair wave implements F06 snapshot ordering and F07 historical
coverage and adds a real F08/F17 crash/restart gate. Its UKMesh typecheck and
full backend suite pass **469/469** (`/tmp/ukmesh-sol-ram-final-tests.log`).
The complete ingest integration command passes **6 PostgreSQL + 2 Redis tests**
using a newly created disposable PostgreSQL database and an independently
created Redis fixture (`/tmp/ukmesh-sol-full-ingest-integration.log`).
Focused CoreScope F07/F08/F17 tests pass with real PostgreSQL/Redis/SQLite
(`/tmp/core-sol-f08-integration-after.log`). Full successor Go suites pass:
ingestor **222.972s**, server **116.673s**, shared schema **0.530s**. Logs are
`/tmp/core-sol-full-ingestor.log`, `/tmp/core-sol-full-server.log` and
`/tmp/core-sol-full-dbschema.log`.

RAM `6ed4296e988f1ec69640d02443f551d37c619400` is pushed and verified by
`git ls-remote`. Its workflows
[38176440656](https://github.com/gadgethd/ukmesh/actions/runs/38176440656) and
[38176443793](https://github.com/gadgethd/ukmesh/actions/runs/38176443793)
both concluded **success in all four jobs**, including full Backend and
Workers/Compose/E2E/empty-volume gates. Both PR descriptions are updated; #122 remains
open/non-draft and #125 remains open/draft. #122's title now describes the
supported N=1 repair instead of obsolete multi-connection performance.

CoreScope is locally committed at `ff9eb3b93c9addbb29feb9debcc12e6cb99d5c8d`.
Both `e9afb3a` and `ff9eb3b` await relay; CoreScope was not pushed. Export with
`git -C /home/ben/corescope-ukmesh/src format-patch --stdout 90c104eab51618894a3b69ab521829f39313e261..ff9eb3b93c9addbb29feb9debcc12e6cb99d5c8d`.
The ready patch is `/tmp/corescope-sol-relay-20261011.patch` (2383 lines).
Evidence logs and the patch are also preserved in the new owned
`/home/ben/ukmesh-bench/mqtt-ingest-perf-20261011/verification-sol-20261011/evidence/`
directory so tool-session temporary files are not the sole handoff artifacts.

Both uniquely tagged verification images built successfully: RAM
`mqttbench-backend:sol-handoff-6ed4296-20261011` has image ID
`sha256:563424ed79ac51abc2eb70cf8f2080bec9961e6c0622f1948233bfeaf92b0859`
and revision label `6ed4296`; durable
`mqttbench-backend:sol-durable-8968078-20261011` has image ID
`sha256:f501da065bf98e219c54d335dfad6703d430b51e6437d49a63140341dc72a637`
and label `8968078`. Existing bench services were unchanged. The completed
compiled-regression gate against the RAM image was launched by the new owned
`verification-sol-20261011/run-image-integrity.sh` under the bench directory.
It mounts only tests, executes shipped modules and requires eight passing,
zero-skipped tests; exit-zero/zero-tests cannot pass.
The `6ed4296` image passed **8/8**, zero skips, in **72.190s**, including real
snapshot/expiry, historical/compressed/contended backfill, refused-write,
replay and quarantine cases. Its evidence is
`logs/sol-image-integrity-shared-path-6ed4296-20261011.log` under the bench
directory (`IMAGE-INTEGRITY-END code=0 complete=true`).

F06's new unit failed on the checkpoint (`/tmp/ukmesh-f06-before.log`), then
passed with `readRecentWithPending`. The real fixture captures Redis before
starting SQL, holds a pre-commit Repeatable Read snapshot, commits A while
admitting B, expires retained A from Redis, and proves A still appears exactly
once. A later request sees A and B. REST recent packets and both WS initial
packet/message paths use this ordering; authorization runs after SQL.

F07's regression failed at the checkpoint (`/tmp/ukmesh-f07-before.log`). New
migration 059 backfills pre-ledger history under the writer transaction lock,
assigns missing UUIDs in compressed history once, preserves existing IDs and
publishes a coverage fence atomically. An injected migration-ledger failure
rolls back UUIDs and coverage; retry after a contended writer commit succeeds.
CoreScope requires the fence. Its real PG-to-SQLite test rejects incomplete
coverage, ingests historical and older late commits, and safely replays after
cursor-file loss. History already deleted by PG retention remains unavailable.

The F08 paired-source gate reads actual PostgreSQL ledger and Redis entries,
kills a disposable ingestor test process while its last submitted write is
active with an empty queue, reopens the same SQLite/WAL, injects a SQLite
observation failure and proves both cursor files retain the last safe state.
Successful retry completes both cursors with two observations and exactly two
observer count increments, in both source orders. It exposed a further F17
restart defect: shared schema reapplication rebuilt the broad legacy identity
index and could collapse distinct upstream IDs. The new shared-schema
regression reproduced that deletion (`/tmp/core-sol-f17-restart-before.log`);
schema setup now defers upstream identity indexes to the ingestor migration,
and startup checks/repairs the actual index state despite a done marker.

The first local backend suite failed a real-time 50ms slow-mode assertion
under CPU load; its clock is now deterministic and the full suite passes.
Concurrent fresh suite reruns then hit the existing two-second worker-fixture
startup timeout on both branches. Complete suites are being rerun with two
CPUs per suite (same test files/assertions) to bound test-process contention.
Both complete reruns passed: perf **418/418** in **138.715s**, RAM **469/469**
in **210.111s**, with typechecks and the alert receiver gate passing. Logs are
`evidence/perf-8968078-two-cpu-tests.log` and
`evidence/ram-6ed4296-two-cpu-tests.log` in the owned verification directory.
The first full PG invocation used an already initialized bench database and
failed its final empty-schema fixture; the successful rerun used a new owned
database. One intentionally stopped diagnostic CoreScope test left an owned
fixture schema; that exact schema was removed. No production data or services
were involved. Preliminary image-fixture packaging had missing assets or zero
tests; neither is accepted as a pass. Docker's daemon does not share the tool
session's `/tmp`, so fixtures now use the shared, newly owned bench verification
directory. The corrected image run's exact eight-test result is verified above.

Remaining mission work: F01 copied-data revocation and generation transitions;
F09/F14 memory/accounting and metric semantics; F12/F13 SQL read parity;
F15 held-write runtime shutdown; F16 ACL/public-source isolation; the ready
WebSocket reader gap; final source-labelled builds, integrity/performance
gates, final CI, PR descriptions, relay commit evidence and report audit.
The predecessor handoff below is historical and is superseded for F06–F08/F17
by this successor evidence. The full objective is still active.

| Work | Current evidence |
|---|---|
| UKMesh #122 | Repair code `cbf78a8a6c8d0f11da200eed0bcf9f4161c5dd2f` and handoff commit `89680782c3c6a0e517640c9e02fca59f5c8a0bee` are pushed to `perf/mqtt-ingest-20261011`. The handoff commit archives `TASK-BRIEF.md` and two patch scripts; runtime code is unchanged. Local `npx tsc --noEmit` and backend `npm test` 418/418 pass at this head (`/tmp/ukmesh-perf-handoff-npm-test.log`); earlier 9/9 focused F03/F04/F14/F15 tests passed. [Code-head CI 38154922672](https://github.com/gadgethd/ukmesh/actions/runs/38154922672) succeeded in all four jobs. New handoff-head CI status is below. |
| UKMesh #125 | Ingestion repairs through `022b7c8a238f2265f219cbe59cea606525c8806f` and drain fix `f70fe058bff5ef28d89ddea3f271cfcf66a311b2` are pushed. Handoff commit `8cace8e46f8e157729dfe00e9e29a1eb1dc35bbd` archives the brief, SQL capture helper and patch scripts; merge `d1aa6ebb8405e9172a74f1f83a42782de7ecde25` carries the new perf head, preserving the stack. Local TypeScript and backend `npm test` 468/468 pass at this head (`/tmp/ukmesh-ram-handoff-npm-test.log`); contract, build and focused checks passed on the same runtime code. [Earlier code-head CI 38160316617](https://github.com/gadgethd/ukmesh/actions/runs/38160316617) succeeded in all four jobs, including real-PG, real-Redis, E2E and empty-volume Compose. New handoff-head CI status is below. |
| CoreScope #1 | Local `feat/redis-live-source` commit `e9afb3a` contains F01/F07/F08/F17 repairs. Focused regressions, full `cmd/ingestor` Go suite (150.648s) and full `cmd/server` Go suite (104.999s) pass. CoreScope has not been pushed; credentials are unavailable on this host. |
| Bench | `FINAL-CHAIN-DONE` observed at 17:39:52 UTC. The ingestion matrix used exact code heads `022b7c8` and `cbf78a8`; the standalone drain entrypoint was rebuilt at `f70fe05`. Crash replay (120/120), ambiguous commit replay (120/120), concurrent count and byte admission, public-to-private pending/commit boundary, and all six matched-window scenarios pass their database identity gates. The first default-broker saturation run lost messages; a sized-broker retest passed. Standalone drain, quarantine refusal and durable rollback passed. |

## Finding map

| Finding | Status | Code and regression evidence |
|---|---|---|
| F01 | Partial | #125 fresh bulk policy/generation/rematerialization check in `packetBatch.ts`, `liveRead.ts`, `client.ts`, `ws/privacy.ts`; stale-cache regressions pass. CoreScope PG/Redis sources now recheck current authority per batch and fail closed, but already copied derived data has no immediate revocation barrier. |
| F02 | Implemented; real PG lock/replay regression passes | Separate same-client transaction lock and ledger-derived mutations in `packetBatch.ts`; CI `38159431225` passed the contended-lock/replay test. |
| F03 | Fixed on #122 and merged into #125 | `backend/src/mqtt/observationIdentity.ts`, `client.ts`, `db/packetBatch.ts`; unit and real-PG regression passed on #122 CI. |
| F04 | Fixed on #122 and merged into #125 | `backend/src/mqtt/ingestConfig.ts` rejects N>1 before connect; regression passes. |
| F05 | Implemented for UKMesh; final-head gates pass | Delivery-key replay ledger in `ram/liveStore.ts`, stable observation ID and PG ledger; lost-reply and same-ms unit/PG integration regressions added. Real-PG and real-Redis CI tests pass. Final-head crash and ambiguous SQL-commit replay pass; a deterministic real-Redis XADD followed by a lost reply recovers the original observation ID with stream length one. |
| F06 | Implemented; real snapshot/expiry regression passes | Per-entry commit state, retained overlap and stable-ID dedupe plus `readRecentWithPending` before-SQL capture on REST/WS. `F06 real PG snapshot and Redis expiry preserve the captured handoff` covers concurrent admission and retention expiry. Final image gate remains. |
| F07 | Implemented; historical/late-commit/cursor-loss regressions pass | Migration 059 adds atomic compressed-history backfill and coverage fence without changing 058. CoreScope requires coverage and follows `seq > cursor`. `F07 historical and compressed observations enter the complete ledger atomically` and `TestF07RealLedgerHistoricalCoverageLateCommitAndCursorLoss` pass. Final image/paired offline gate remains. |
| F08 | Implemented; real paired crash/failure and full Go suites pass | Both CoreScope sources use `finishSourceBatch`; `TestF08RealPairedSourcesCrashFailureAndRestartBothOrders` proves active-last-job process crash, same-SQLite/WAL restart, actual observation insert failure, held cursors and successful replay. Full successor ingestor/server/schema suites pass at `ff9eb3b`. |
| F09 | Partial | One-time atomic cleanup, bounded reconciliation and retained accounting in `ram/liveStore.ts`, `writer.ts`; replay, cleanup and real-Redis lost-counter reconciliation tests pass. Final-head concurrent byte admission passes at the enforced 1 MiB minimum. Startup/retention accounting in the final-head bench remains. |
| F10 | Implemented; group-loss integration passes | Supervised startup, NOGROUP repair, periodic bounded foreign PEL reclaim, and stale-PEL/progress readiness in `index.ts`, `ram/writer.ts`, `writerRuntime.ts`; unit regressions and a final-head real-Redis group-destruction/recovery gate pass. |
| F11 | Implemented; final-head fault gate passes | Real ioredis XADD quarantine, common validated decode, and rowwise permanent SQL poison isolation in `ram/writer.ts`, `protocol.ts`; malformed PEL, poison-neighbor and real-Redis quarantine tests pass. A final-head simulated permanent SQLSTATE 23514 in a two-row batch quarantined only the poison row through real ioredis, committed its valid neighbor in PG, and left PEL zero. |
| F12 | Implemented, awaiting integration | Stable-ID aggregate and PG overlap in `ram/liveRead.ts`; same-observer sighting regression passes. |
| F13 | Partial | `ram/liveRead.ts` now matches network-family scope, canonical aliases, recent windows, representative preference and slim DTO; `TestF13 RAM recent rows honor family scope, aliases, window, and slim DTO` passes. A corrected final-head fixture persisted the exact admitted stream entry and matched full and slim pending DTOs to real PostgreSQL recent-query DTOs. Detail/long history are explicitly PG-only; wider paging and field combinations remain unproven. |
| F14 | Partial | #122 metric registration fixed. #125 separates pending/retained/PEL gauges, byte budget and minimum admitted-age ordering; commit batch histogram now counts actual inserted PG rows. A final-head live metric scrape confirms all those series are served under load. Final load validation remains. |
| F15 | Implemented, boundary still partly unit-tested | #122 deadline fixed. #125 shares one shutdown deadline, joins writer loop, detects PEL on drain and closes clients; `drainLifecycle.test.ts` holds an active persist past shutdown. Isolated runtime SIGTERM logged graceful completion in about two seconds and its 300/300 exact identities, side effects, queue, PEL and bytes passed a manual post-restart gate. The bench's stock wait script missed this because `processed` has no zero-valued series after restart; no held active persist was injected into that runtime run. |
| F16 | Partial | Compose variables and CI render assertion, `docs/ram-first-ingestion.md`, and `node dist/ram/drain.js` added. The drain waits for Redis readiness, reuses the ready client for final byte reconciliation, closes path-resolution workers, and refuses a nonempty quarantine as well as nonempty stream/PEL/byte reservation. Isolated standalone drain, quarantine refusal and durable rollback passed on `f70fe05`. Raw-stream Redis ACL isolation remains. |
| F17 | Implemented including restart repair; full Go suites pass | CoreScope preserves upstream UUID and increments observer count once. `ff9eb3b` prevents schema reapply from rebuilding the broad legacy index and deleting distinct IDs, and repairs actual missing/legacy index state despite done migration markers. `TestF17UpstreamIdentitySurvivesSchemaReapply`, both-order identity/count and real crash/restart tests pass. |

## Successor handoff — 2026-10-11 20:55 UTC

The owner requested that a GPT-6.1 Sol successor continue the **full** F01–F17 mission. The statuses below are review requirements, not a claim that #125 is merge-ready. “Complete” means the finding's code and named regression are in place; cross-cutting release gates remain listed separately. No top-level finding is wholly not started, but several required integration and operational gates are.

| Finding | Handoff status | Exact next step if in progress |
|---|---|---|
| F01 | **In progress** | Add an immediate public revocation barrier for **already copied** CoreScope SQLite-derived rows, then test observer/source/prefix privacy flips after copy and during pending/commit with available and unavailable policy authority. Rerun the synthetic post-flip audit after rematerialization completes; its current zero-result audit is inconclusive. |
| F02 | **Complete** | — |
| F03 | **Complete** | — |
| F04 | **Complete** for supported N=1; N>1 is gated | — |
| F05 | **Complete** | — |
| F06 | **In progress** | Add a deterministic PG-snapshot-before-commit and Redis-retention-expiry integration test with a new admission during commit; compare exact observation IDs across the PG/RAM handoff. |
| F07 | **In progress** | Implement and test a safe historical backfill for packets written before migration 058. Prove the paired CoreScope follower reads those rows and late commits across Redis retention/offline periods, including cursor loss. Do not enable Redis-only completeness. |
| F08 | **In progress** | Run a real paired PostgreSQL/Redis-to-SQLite crash/restart test that blocks the last job, injects a write failure, and proves both saved cursors remain behind every incomplete write. |
| F09 | **In progress** | Reconcile byte reservations through startup, retained expiry, replay and interruption against real Redis; prove pending, retained, PEL and quarantine headroom under the configured Redis memory limit. |
| F10 | **Complete** | — |
| F11 | **Complete** | — |
| F12 | **In progress** | Compare RAM/PG merged counts and representative selection to SQL for repeated sightings from the same and multiple observers across a real handoff, including distinct stable IDs. |
| F13 | **In progress** | Extend the real-SQL parity fixture beyond the passed full/slim recent DTO case to paging, sampling, all representative fields, aliases, network-family combinations and documented PG-only detail/history. |
| F14 | **In progress** | Run a controlled loaded metric scrape with known pending/retained/PEL/byte and zero-capacity states, and assert registered series and actual batch/drop semantics at each boundary. |
| F15 | **In progress** | Inject a held active persist into the isolated runtime SIGTERM gate and prove deadline, incomplete result and no lost identities. Fix the bench `wait-drain.sh` post-restart `processed`-series oracle before relying on its `DRAINED` marker. |
| F16 | **In progress** | Prove raw-stream Redis ACL isolation for the CoreScope reader, then repeat Compose render and the already-passed standalone drain/quarantine refusal/durable rollback gate on the resulting final configuration. |
| F17 | **Complete** at code/regression scope | —; real paired-source crash operation is tracked under F08. |

**Handoff commits and push state.** Perf `89680782c3c6a0e517640c9e02fca59f5c8a0bee` adds the task brief and two review patch scripts. RAM `8cace8e46f8e157729dfe00e9e29a1eb1dc35bbd` adds the Sol brief, RAM patch scripts and SQL capture helper; `d1aa6ebb8405e9172a74f1f83a42782de7ecde25` merges the new perf head. Both UKMesh branches were pushed and locally clean before this report edit. The CoreScope remote `feat/redis-live-source` is at `90c104eab51618894a3b69ab521829f39313e261`; **its only unpushed local commit is `e9afb3a8a17f8b65d1463985df18984c8f8b86f4`**: paired PG/Redis completion barriers, current privacy checks for new batches, committed-order cursor, stable upstream identities and deduped observer counts. Export with `git -C /home/ben/corescope-ukmesh/src format-patch -1 e9afb3a8a17f8b65d1463985df18984c8f8b86f4`; the coordinator must relay-push it. No CoreScope worktree changes are uncommitted.

**CI snapshot, 20:55 UTC.** Earlier repair-code runs [#122 38154922672](https://github.com/gadgethd/ukmesh/actions/runs/38154922672) and [#125 38160316617](https://github.com/gadgethd/ukmesh/actions/runs/38160316617) succeeded in all four jobs. New perf handoff-head runs [38174016916](https://github.com/gadgethd/ukmesh/actions/runs/38174016916) and [38174021349](https://github.com/gadgethd/ukmesh/actions/runs/38174021349) were in progress. RAM runs [38173522331](https://github.com/gadgethd/ukmesh/actions/runs/38173522331) and [38173526291](https://github.com/gadgethd/ukmesh/actions/runs/38173526291) were still in progress; the newer RAM handoff-head runs [38174191342](https://github.com/gadgethd/ukmesh/actions/runs/38174191342) and [38174195183](https://github.com/gadgethd/ukmesh/actions/runs/38174195183) were pending. These are **not recorded as green**. Re-query the latest branch heads and runs after this documentation push, then watch the newest full CI to conclusion. Local pre-push gates passed: perf typecheck and 418/418 backend tests (`/tmp/ukmesh-perf-handoff-npm-test.log`); RAM typecheck and 468/468 (`/tmp/ukmesh-ram-handoff-npm-test.log`). Earlier CoreScope full `cmd/ingestor` and `cmd/server` Go suites passed.

**Open handoff threads.** The PR #122 body still cites the repair code head `cbf78a8`; PR #125 still cites `022b7c8` and says the benchmark is running. After final CI, update both existing PR descriptions to the pushed heads and evidence, preserving #122 open/non-draft and #125 draft. `/tmp/ukmesh-pr125-body.md` has a prepared #125 description with `FINAL_HEAD` and `FINAL_CI` placeholders. No PR was merged, approved or deployed. The isolated bench evidence and limitations follow below: ready RAM burst WebSocket reader received 2/800, the low-rate RAM PG follower received 300/360 in its bounded window, the default-broker saturation run lost 315/3,100 before a sized-broker retest passed, and privacy selective filtering after flip remains inconclusive while rematerialization jobs are pending.

## Regression index

| Finding | Named regression or gate |
|---|---|
| F01 | `F01 a privacy flip overrides the loaded public cache for RAM and PG rows`; `F01 known private observer or source suppresses a stale visibility flag`; CoreScope `TestF01CorePublicPolicyOverridesFrozenRedisVisibility`; isolated pending/commit flip. |
| F02 | `F02 contended lock and replay never change historical tables` (real PostgreSQL); `F02 refuses a contended writer before any mutation and gates the hourly predicate`. |
| F03 | `F03 canonical observation key distinguishes hop, path, direction, observer and scope`; `F03 restart probe checks the same identity`; real PostgreSQL #122 CI case. |
| F04 | `F04 shared-subscription N>1 is gated until restart and resize recovery is qualified`. |
| F05 | `F05 lost Redis reply recovers the original observation ID without a second XADD`; `F05 two same-millisecond sightings with distinct hop/path and IDs both persist`; real Redis admission replay and final-head ambiguous SQL-commit replay. |
| F06 | `F06 retained overlap and newer admission remain readable across a commit marker`; `F06 an old pending hole remains discoverable`; `F06 retention starts at commit time`; final-head marker-gap gate passed. |
| F07 | `F07 ledger cursor follows completed writes even when the second observation is older` (real PostgreSQL); CoreScope `TestF07LedgerCursorUsesCommitOrderRatherThanObservedTime`, `TestF07RedisTailRequiresPairedCompletenessSource`. |
| F08 | CoreScope `TestF08TrackedJobReportsActiveCompletionAndWriteFailure`, `TestF08SourceBatchWaitsForLastJobAndReturnsWriteFailure`, historical handler SQLite/observation failure cases. |
| F09 | `F09 interrupted cleanup releases bytes exactly once on retry`; real Redis F05/F09 admission replay and atomic byte reconciliation; final-head concurrent byte cap. |
| F10 | `F10 destroyed group is repaired before another admitted entry is stranded`; `F10 young foreign PEL is reclaimed after it ages without a restart`; final-head NOGROUP gate passed. |
| F11 | `F11 rejects invalid UUIDs, SQL integers, JSON and path arrays before persistence`; `F11 malformed entries survive quarantine failure`; `F11 a permanent SQL poison row is quarantined while a valid neighbor commits`; real ioredis quarantine CI. |
| F12 | `F12 merge deduplicates committed overlap by ID and counts a new same-observer sighting`. |
| F13 | `F13 RAM recent rows honor family scope, aliases, window, and slim DTO`; corrected final-head real-SQL full/slim parity fixture passed. |
| F14 | `F14 follow-up inflight and drop metrics appear in the served registry`; `F14 pending age uses admitted time and skips committed overlap`; `F14 an older reclaimed entry behind a young entry triggers the age flush`. |
| F15 | `F15 one MQTT shutdown deadline includes a blocked disconnect`; `F15 shutdown joins the active persist before attempting to drain`; `F15 an active write past the deadline reports an incomplete shutdown`; `F15 drain reports an uncommitted PEL`. |
| F16 | CI `Validate Compose configuration` RAM render assertion and empty-volume smoke; final-head standalone drain, quarantine-refusal and durable rollback gate passed on `f70fe05`. |
| F17 | CoreScope `TestF17TwoSourceObservationIdentityAndCountInBothOrders`, `TestF17HistoricalSourcesWaitForIdentityIndexMigration`. |

The §7 advisory-lock snapshot caveat is addressed in code by a separate-statement same-client transaction and unique identity ledger; its real-PG lock/replay regression passed in CI. The §4 eleven-item verification and §5 methodology repairs remain incomplete. Current tests do not establish immediate CoreScope revocation, exact RAM/SQL read parity, or whole-stack performance.

## Review §4 issue check

| Prior issue | Current-head evidence and remaining gate |
|---|---|
| 1. Commit before ACK double count | F02 contended-lock/replay PG test passes; F05 stable admission identity and ledger tests pass locally. Final-head ambiguous SQL-commit replay passes 120/120 with 60 paths. The duplicate-publish run acknowledged 1,440 main deliveries but persisted exactly 1,200 distinct main observations, with duplicate-key count zero, max row multiplicity one, and one path per observation. Exact lost Redis admission reply remains separate. |
| 2. Atomic limits | Lua count/byte admission and missing-counter reconciliation have unit and real-Redis CI tests. Final-head concurrent count admission passed 180/180 at cap 180, with 220/220 refusals; byte admission passed 564 entries and 936 refusals under the 1 MiB bound. In a 3,000-message burst with a 2,000-entry RAM cap, the default isolated Mosquitto queue logged outgoing drops: only 2,785/3,100 warmup-plus-main identities reached PG, despite all 3,000 main publisher broker ACKs. With a bench-only `max_queued_messages 10000`, the identical burst passed 3,100/3,100 exact identities, all historical totals, 407 explicit queue-full refusals, no duplicate rows, and zero final queue/PEL/reserved bytes. Broker queue sizing is a deployment requirement; the failed run remains evidence. |
| 3. Queue age | Pending, retained and PEL are separate; the flush trigger uses the earliest admitted entry. The final-head batch-500 live scrape shows pending=203, retained=1,500, PEL=203, reserved bytes=4,138,800 and oldest pending age=20.261 seconds. |
| 4. Privacy | Fresh policy and rematerialization fences cover UKMesh public reads and new CoreScope batches. Final-head isolated observer flip test passed: admitted public, hidden by pending read after flip, committed private. The sustained synthetic privacy run passed 1,600/1,600 PG identities, but its post-flip API/WS audit is inconclusive for selective observer filtering: two rematerialization jobs remained pending, so both public probes saw zero records after the flip (fail closed). Immediate CoreScope revocation remains unresolved. |
| 5. Marker gaps | Per-entry commit state replaces cumulative marker authority; gap and retained-overlap unit tests pass. The final-head admit-during-commit gate observed both rows in the live overlay before and after the first commit, with the second still pending. |
| 6. Multi-observer aggregate | Stable observation IDs prevent observer-membership dedupe errors; alias/window/DTO tests and a corrected real-SQL full/slim parity fixture pass. The isolated three-observer fanout persisted 2,700/2,700 main and 100/100 warmup identities with no duplicate rows. |
| 7. Message type | WS initial message merge limits pending rows to GroupText and scans the bounded whole stream. Long REST channel history remains PG-only. |
| 8. Background work | Post-commit hooks remain best effort and bounded. A crash after PG commit may omit a hook; it is not an exactly-once side effect. |
| 9. Quarantine adapter | Writer uses real ioredis XADD, validates every read path, and acknowledges poison only after quarantine. Real-Redis CI passes; the final-head SQLSTATE 23514 poison-neighbor gate quarantined one row through real ioredis, committed the valid PG neighbor and left PEL zero. |
| 10. Byte drift | Atomic final deletion and Redis-side reconciliation cover the reservation counter. Real-Redis CI passes; startup/restart accounting gate remains. |
| 11. Handoff | Retained PG overlap and committed-order ledger cover late commits after migration. End-to-end two-source offline, retention and cursor-file-loss gates remain. |

## CoreScope relay-push record

Local commit `e9afb3a8a17f8b65d1463985df18984c8f8b86f4` on `feat/redis-live-source` is ready to export with `git format-patch -1 e9afb3a8a17f8b65d1463985df18984c8f8b86f4`. It changes only `cmd/ingestor`: paired PG/Redis sources hold cursors through completed SQLite jobs, follow the UKMesh committed-order ledger, recheck current privacy policy for new batches, preserve upstream observation identity, and dedupe observer counts. Migration to the two identity indexes is asynchronous and gates the sources until ready. Both full Go suites pass. The coordinator must relay-push this local commit; it was not pushed here.

## Existing test observations

- #122 initial full-suite run: 417/418, with a timing failure in the unrelated `path-beta/workerPool` abort test. Focused rerun: 3/3; final full suite: 418/418. Log: `/tmp/ukmesh-perf-fix-npm-test-final.log`.
- #122 CI run `38154922672` completed successfully in Backend, Frontend, Workers and Compose, and Secret scan.
- #125 final code head `022b7c8` final-head bench integration log: `/home/ben/ukmesh-bench/mqtt-ingest-perf-20261011/logs/integration-20261011-174207.log`, exit 0. Crash replay and ambiguous commit each persisted 120/120 distinct observations and 60/60 paths; replay PEL was zero. Atomic count and byte admissions passed. Isolated privacy boundary log: `/tmp/ukmesh-final-privacy-boundary-rerun.log`, exit 0.
- The drain-only code head `f70fe05` passed `npx tsc --noEmit`, OpenAPI contract check, build and the full backend `npm test` (468/468). Full-suite log: `/tmp/ukmesh-final-head-f70fe05-npm-test.log`. Focused lifecycle and worker-pool checks passed 5/5: `/tmp/ukmesh-final-head-f70fe05-focused.log`.
- Live final-head metric scrape: `/home/ben/ukmesh-bench/mqtt-ingest-perf-20261011/logs/final-head-metric-scrape-b500-20261011-1843.txt`. The same scrape reports 2,180 dropped best-effort post-persist follow-ups. This is a measured nontransactional enrichment limitation, not a missing packet/path/hourly/region database row.
- Isolated shutdown manual gate: `/tmp/ukmesh-s9-manual-gate.log`. PostgreSQL packets=300, Redis stream=0, PEL=0 and all queue gauges/byte reservation=0 after SIGTERM and restart; a separate exact-identity audit passed 300/300. The stock bench `wait-drain.sh` did not emit `DRAINED` because the post-restart `processed` counter had no zero-valued series, so its wrapper exit is a benchmark-oracle failure, not evidence of uncommitted observations.
- Final-head fault gates: `/home/ben/ukmesh-bench/mqtt-ingest-perf-20261011/logs/final-head-fault-gates-20261011-195949.log` records lost admission reply, marker gap, NOGROUP repair and SQL poison passes. Its first SQL parity invocation failed because the bench fixture supplied `PacketBatchInput` fields to an `InsertPacketInput` API and persisted a separately timed row. The corrected fixture uses public input and persists the exact admitted entry; `/home/ben/ukmesh-bench/mqtt-ingest-perf-20261011/logs/final-head-sql-parity-corrected-20261011-2001.log` passes full and slim DTO comparison.
- Standalone drain first exposed two real exit-path defects: a Redis command raced connection readiness, and two path-resolution worker threads held the CLI process open after `RAM_DRAIN_COMPLETE`. `f70fe05` connects explicitly, retains the ready Redis client across writer shutdown for the final byte check, and closes that worker pool. On the exact `f70fe05` bench image, `/home/ben/ukmesh-bench/mqtt-ingest-perf-20261011/logs/final-head-drain-20261011-204138.log` records exit-zero drain, refusal of one quarantined entry, zero final stream/quarantine/bytes and a ready durable-mode backend. The negative invocation output is `/home/ben/ukmesh-bench/mqtt-ingest-perf-20261011/logs/final-head-drain-quarantine-negative.log`.

## Final-code isolated bench matrix

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

### Joined-reader clean-window comparison

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

No production database, Redis, broker, service, configuration, deployment or running process has been modified. No PR has been merged or approved. Only the isolated `mqttbench` rig was used after its `FINAL-CHAIN-DONE` marker. CoreScope has not been pushed.
