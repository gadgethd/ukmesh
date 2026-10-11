# UKMesh ingestion review repairs — working report (2026-10-11)

Specification: `/home/ben/ukmesh-fix-context-20261011/SOL-REVIEW.md`. This is a working record; **the full repair objective is not yet achieved**. Status below reflects inspected code and completed verification only.

## Branches and gates

| Work | Current evidence |
|---|---|
| UKMesh #122 | Pushed `cbf78a8a6c8d0f11da200eed0bcf9f4161c5dd2f` to `perf/mqtt-ingest-20261011`. Local `npx tsc --noEmit` passed. Full backend `npm test` passed 418/418 on final rerun; 9/9 focused F03/F04/F14/F15 tests passed. [CI run 38154922672](https://github.com/gadgethd/ukmesh/actions/runs/38154922672) succeeded in all four jobs, including PostgreSQL ingestion integration. PR body updated. |
| UKMesh #125 | Perf merge `ebab933`; repair commits `91cc3c2`, `21b1eda`, `647e6a0` and `8bfc5ce` pushed. Final local TypeScript, contract, build, 468/468 backend unit tests and 21/21 focused writer tests pass after the poison-health and drain-preflight adjustments. [CI run 38159953702](https://github.com/gadgethd/ukmesh/actions/runs/38159953702) has green Backend, Frontend and Secret scan jobs, including 4/4 real-PG and 2/2 real-Redis integration tests; Workers and Compose is still running. The local follow-up remains to be pushed and checked. |
| CoreScope #1 | Local `feat/redis-live-source` commit `e9afb3a` contains F01/F07/F08/F17 repairs. Focused regressions, full `cmd/ingestor` Go suite (150.648s) and full `cmd/server` Go suite (104.999s) pass. CoreScope has not been pushed; credentials are unavailable on this host. |
| Bench | Read-only; no `FINAL-CHAIN-DONE` marker observed yet. No new image or scenario has been run. |

## Finding map

| Finding | Status | Code and regression evidence |
|---|---|---|
| F01 | Partial | #125 fresh bulk policy/generation/rematerialization check in `packetBatch.ts`, `liveRead.ts`, `client.ts`, `ws/privacy.ts`; stale-cache regressions pass. CoreScope PG/Redis sources now recheck current authority per batch and fail closed, but already copied derived data has no immediate revocation barrier. |
| F02 | Implemented; real PG lock/replay regression passes | Separate same-client transaction lock and ledger-derived mutations in `packetBatch.ts`; CI `38159431225` passed the contended-lock/replay test. |
| F03 | Fixed on #122 and merged into #125 | `backend/src/mqtt/observationIdentity.ts`, `client.ts`, `db/packetBatch.ts`; unit and real-PG regression passed on #122 CI. |
| F04 | Fixed on #122 and merged into #125 | `backend/src/mqtt/ingestConfig.ts` rejects N>1 before connect; regression passes. |
| F05 | Partial | Delivery-key replay ledger in `ram/liveStore.ts`, stable observation ID and PG ledger; lost-reply and same-ms unit/PG integration regressions added. Real-PG and real-Redis CI tests pass; final-head ambiguous admission and crash gate remains. |
| F06 | Partial | Per-entry commit state, retained overlap and stable-ID dedupe in `ram/liveStore.ts`, `writer.ts`, `liveRead.ts`; marker-gap and PEL-hole unit regressions pass. Snapshot/retention integration remains. |
| F07 | Implemented for post-migration UKMesh rows; PG integration passes | #125 migration 058 provides a committed-order ledger under the batch advisory lock. CoreScope PG follower uses `seq > cursor`; Redis-only config is rejected and a missing ledger disables the paired source. `TestF07LedgerCursorUsesCommitOrderRatherThanObservedTime` and the late-observed-commit PG regression pass. Pre-ledger history still uses the older time cursor and needs a separate backfill. |
| F08 | Implemented, full Go suites pass | CoreScope `ingest_buffer.go` tracked completion, `source_batch.go`, both sources, and packet-path `main.go` propagate write errors and hold the cursor. F08 blocked-last-job, admission failure, SQLite failure and observation insert failure tests pass. Real two-source crash integration remains. |
| F09 | Partial | One-time atomic cleanup, bounded reconciliation and retained accounting in `ram/liveStore.ts`, `writer.ts`; replay, cleanup and real-Redis lost-counter reconciliation tests pass. Startup/retention accounting in the final-head bench remains. |
| F10 | Implemented, awaiting integration | Supervised startup, NOGROUP repair, periodic bounded foreign PEL reclaim, and stale-PEL/progress readiness in `index.ts`, `ram/writer.ts`, `writerRuntime.ts`; unit regressions pass. |
| F11 | Implemented, awaiting final-head fault gate | Real ioredis XADD quarantine, common validated decode, and rowwise permanent SQL poison isolation in `ram/writer.ts`, `protocol.ts`; malformed PEL, poison-neighbor and real-Redis quarantine tests pass. Poison quarantine also reports writer failure in the local follow-up. |
| F12 | Implemented, awaiting integration | Stable-ID aggregate and PG overlap in `ram/liveRead.ts`; same-observer sighting regression passes. |
| F13 | Partial | `ram/liveRead.ts` now matches network-family scope, canonical aliases, recent windows, representative preference and slim DTO; `TestF13 RAM recent rows honor family scope, aliases, window, and slim DTO` passes. Detail/long history are explicitly PG-only; side-by-side real-SQL fixture still needed. |
| F14 | Partial | #122 metric registration fixed. #125 separates pending/retained/PEL gauges, byte budget and minimum admitted-age ordering; commit batch histogram now counts actual inserted PG rows. Real metric scrape and final load validation remain. |
| F15 | Implemented, awaiting integration | #122 deadline fixed. #125 shares one shutdown deadline, joins writer loop, detects PEL on drain and closes clients; `drainLifecycle.test.ts` holds an active persist past shutdown. Runtime-level drain test remains. |
| F16 | Partial | Compose variables and CI render assertion, `docs/ram-first-ingestion.md`, and `node dist/ram/drain.js` added. Standalone drain now refuses a nonempty quarantine as well as nonempty stream/PEL/byte reservation. Isolated rollback/drain validation and raw-stream Redis ACL isolation remain. |
| F17 | Implemented, full Go suites pass | CoreScope preserves upstream UUID in SQLite and updates observer count only for a newly inserted observation. Async indexes hold paired sources until ready. `TestF17TwoSourceObservationIdentityAndCountInBothOrders` and migration readiness test pass. |

The §7 advisory-lock snapshot caveat is addressed in code by a separate-statement same-client transaction and unique identity ledger; its real-PG lock/replay regression passed in CI. The §4 eleven-item verification and §5 methodology repairs remain to be repeated on final heads. Current tests do not establish immediate CoreScope revocation, exact RAM/SQL read parity, or whole-stack performance.

## Review §4 issue check

| Prior issue | Current-head evidence and remaining gate |
|---|---|
| 1. Commit before ACK double count | F02 contended-lock/replay PG test passes; F05 stable admission identity and ledger tests pass locally. Final-head ambiguous-commit bench replay remains. |
| 2. Atomic limits | Lua count/byte admission and missing-counter reconciliation have unit and real-Redis CI tests. Final-head concurrent admission remains the gate. |
| 3. Queue age | Pending, retained and PEL are separate; the flush trigger uses the earliest admitted entry. Metric scrape under load remains. |
| 4. Privacy | Fresh policy and rematerialization fences cover UKMesh public reads and new CoreScope batches. Immediate CoreScope revocation remains unresolved. |
| 5. Marker gaps | Per-entry commit state replaces cumulative marker authority; gap and retained-overlap unit tests pass. A deterministic admit-during-commit integration test remains. |
| 6. Multi-observer aggregate | Stable observation IDs prevent observer-membership dedupe errors; alias/window/DTO tests pass. Real SQL parity fixture remains. |
| 7. Message type | WS initial message merge limits pending rows to GroupText and scans the bounded whole stream. Long REST channel history remains PG-only. |
| 8. Background work | Post-commit hooks remain best effort and bounded. A crash after PG commit may omit a hook; it is not an exactly-once side effect. |
| 9. Quarantine adapter | Writer uses real ioredis XADD, validates every read path, and acknowledges poison only after quarantine. Real-Redis CI passes; final-head malformed/poison fault gates remain. |
| 10. Byte drift | Atomic final deletion and Redis-side reconciliation cover the reservation counter. Real-Redis CI passes; startup/restart accounting gate remains. |
| 11. Handoff | Retained PG overlap and committed-order ledger cover late commits after migration. End-to-end two-source offline, retention and cursor-file-loss gates remain. |

## CoreScope relay-push record

Local commit `e9afb3a8a17f8b65d1463985df18984c8f8b86f4` on `feat/redis-live-source` is ready to export with `git format-patch -1 e9afb3a8a17f8b65d1463985df18984c8f8b86f4`. It changes only `cmd/ingestor`: paired PG/Redis sources hold cursors through completed SQLite jobs, follow the UKMesh committed-order ledger, recheck current privacy policy for new batches, preserve upstream observation identity, and dedupe observer counts. Migration to the two identity indexes is asynchronous and gates the sources until ready. Both full Go suites pass. The coordinator must relay-push this local commit; it was not pushed here.

## Existing test observations

- #122 initial full-suite run: 417/418, with a timing failure in the unrelated `path-beta/workerPool` abort test. Focused rerun: 3/3; final full suite: 418/418. Log: `/tmp/ukmesh-perf-fix-npm-test-final.log`.
- #122 CI run `38154922672` completed successfully in Backend, Frontend, Workers and Compose, and Secret scan.

## Operational boundary

No production database, Redis, broker, service, configuration, deployment or running process has been modified. No PR has been merged or approved. The bench campaign has only been inspected read-only. CoreScope has not been pushed.
