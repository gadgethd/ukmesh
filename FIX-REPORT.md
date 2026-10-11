# UKMesh ingestion review repairs — working report (2026-10-11)

Specification: `/home/ben/ukmesh-fix-context-20261011/SOL-REVIEW.md`. This is a working record; **the full repair objective is not yet achieved**. Status below reflects inspected code and completed verification only.

## Branches and gates

| Work | Current evidence |
|---|---|
| UKMesh #122 | Pushed `cbf78a8a6c8d0f11da200eed0bcf9f4161c5dd2f` to `perf/mqtt-ingest-20261011`. Local `npx tsc --noEmit` passed. Full backend `npm test` passed 418/418 on final rerun; 9/9 focused F03/F04/F14/F15 tests passed. [CI run 38154922672](https://github.com/gadgethd/ukmesh/actions/runs/38154922672) succeeded in all four jobs, including PostgreSQL ingestion integration. PR body updated. |
| UKMesh #125 | Perf merge `ebab933`; pushed repair checkpoint `91cc3c29c5afb81d5051baa6fd3d42d794d0d79b`. Final local TypeScript, contract, build, 468/468 backend unit tests and 48/48 focused RAM tests pass. [CI run 38157689284](https://github.com/gadgethd/ukmesh/actions/runs/38157689284) has three successful jobs; Backend remains in the combined ingest integration step. A follow-up push splits PG/Redis integration and applies time limits. |
| CoreScope #1 | Local branch head remains `90c104eab51618894a3b69ab521829f39313e261`. F01/F07/F08/F17 changes remain uncommitted. Focused Go regressions and final `cmd/ingestor` full suite pass (`go test -mod=readonly ./...`, 150.648s); `cmd/server` test is in progress. No push is possible from this host. |
| Bench | Read-only; no `FINAL-CHAIN-DONE` marker observed yet. No new image or scenario has been run. |

## Finding map

| Finding | Status | Code and regression evidence |
|---|---|---|
| F01 | Partial | #125 fresh bulk policy/generation/rematerialization check in `packetBatch.ts`, `liveRead.ts`, `client.ts`, `ws/privacy.ts`; stale-cache regressions pass. CoreScope PG/Redis sources now recheck current authority per batch and fail closed, but already copied derived data has no immediate revocation barrier. |
| F02 | Implemented, awaiting real PG CI | Separate same-client transaction lock and ledger-derived mutations in `packetBatch.ts`; unit and PG integration regression added. |
| F03 | Fixed on #122 and merged into #125 | `backend/src/mqtt/observationIdentity.ts`, `client.ts`, `db/packetBatch.ts`; unit and real-PG regression passed on #122 CI. |
| F04 | Fixed on #122 and merged into #125 | `backend/src/mqtt/ingestConfig.ts` rejects N>1 before connect; regression passes. |
| F05 | Partial, awaiting real Redis/PG CI | Delivery-key replay ledger in `ram/liveStore.ts`, stable observation ID and PG ledger; lost-reply and same-ms unit/PG integration regressions added. |
| F06 | Partial | Per-entry commit state, retained overlap and stable-ID dedupe in `ram/liveStore.ts`, `writer.ts`, `liveRead.ts`; marker-gap and PEL-hole unit regressions pass. Snapshot/retention integration remains. |
| F07 | Implemented for post-migration UKMesh rows, awaiting real PG CI | #125 migration 058 provides a committed-order ledger under the batch advisory lock. CoreScope PG follower uses `seq > cursor`; Redis-only config is rejected and a missing ledger disables the paired source. `TestF07LedgerCursorUsesCommitOrderRatherThanObservedTime` passes. Pre-ledger history still uses the older time cursor and needs a separate backfill. |
| F08 | Implemented, awaiting final full Go suite | CoreScope `ingest_buffer.go` tracked completion, `source_batch.go`, both sources, and packet-path `main.go` propagate write errors and hold the cursor. F08 blocked-last-job, admission failure, SQLite failure and observation insert failure tests pass. Real two-source crash integration remains. |
| F09 | Partial, awaiting real Redis CI | One-time atomic cleanup, bounded reconciliation and retained accounting in `ram/liveStore.ts`, `writer.ts`; replay and cleanup unit tests pass. |
| F10 | Implemented, awaiting integration | Supervised startup, NOGROUP repair, periodic bounded foreign PEL reclaim, and stale-PEL/progress readiness in `index.ts`, `ram/writer.ts`, `writerRuntime.ts`; unit regressions pass. |
| F11 | Implemented, awaiting real Redis CI | Real ioredis XADD quarantine, common validated decode, and rowwise permanent SQL poison isolation in `ram/writer.ts`, `protocol.ts`; malformed PEL and poison-neighbor unit tests pass. |
| F12 | Implemented, awaiting integration | Stable-ID aggregate and PG overlap in `ram/liveRead.ts`; same-observer sighting regression passes. |
| F13 | Partial | `ram/liveRead.ts` now matches network-family scope, canonical aliases, recent windows, representative preference and slim DTO; `TestF13 RAM recent rows honor family scope, aliases, window, and slim DTO` passes. Detail/long history are explicitly PG-only; side-by-side real-SQL fixture still needed. |
| F14 | Partial | #122 metric registration fixed. #125 separates pending/retained/PEL gauges, byte budget and minimum admitted-age ordering; commit batch histogram now counts actual inserted PG rows. Real metric scrape and final load validation remain. |
| F15 | Implemented, awaiting integration | #122 deadline fixed. #125 shares one shutdown deadline, joins writer loop, detects PEL on drain and closes clients; `drainLifecycle.test.ts` holds an active persist past shutdown. Runtime-level drain test remains. |
| F16 | Partial | Compose variables and CI render assertion, `docs/ram-first-ingestion.md`, and `node dist/ram/drain.js` added. Isolated rollback/drain validation and raw-stream Redis ACL isolation remain. |
| F17 | Implemented, awaiting final full Go suite | CoreScope preserves upstream UUID in SQLite and updates observer count only for a newly inserted observation. Async indexes hold paired sources until ready. `TestF17TwoSourceObservationIdentityAndCountInBothOrders` and migration readiness test pass. |

The §7 advisory-lock snapshot caveat is addressed in code by a separate-statement same-client transaction and unique identity ledger, with real-PG regression pending CI. The §4 eleven-item verification and §5 methodology repairs remain to be repeated on final heads. Current tests do not establish immediate CoreScope revocation, exact RAM/SQL read parity, or whole-stack performance.

## Existing test observations

- #122 initial full-suite run: 417/418, with a timing failure in the unrelated `path-beta/workerPool` abort test. Focused rerun: 3/3; final full suite: 418/418. Log: `/tmp/ukmesh-perf-fix-npm-test-final.log`.
- #122 CI run `38154922672` completed successfully in Backend, Frontend, Workers and Compose, and Secret scan.

## Operational boundary

No production database, Redis, broker, service, configuration, deployment or running process has been modified. No PR has been merged or approved. The bench campaign has only been inspected read-only. CoreScope has not been pushed.
