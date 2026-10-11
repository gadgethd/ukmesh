# UKMesh ingestion review repairs — working report (2026-10-11)

Specification: `/home/ben/ukmesh-fix-context-20261011/SOL-REVIEW.md`. This is a working record; **the full repair objective is not yet achieved**. Status below reflects inspected code and completed verification only.

## Branches and gates

| Work | Current evidence |
|---|---|
| UKMesh #122 | Pushed `cbf78a8a6c8d0f11da200eed0bcf9f4161c5dd2f` to `perf/mqtt-ingest-20261011`. Local `npx tsc --noEmit` passed. Full backend `npm test` passed 418/418 on final rerun; 9/9 focused F03/F04/F14/F15 tests passed. [CI run 38154922672](https://github.com/gadgethd/ukmesh/actions/runs/38154922672) succeeded in all four jobs, including PostgreSQL ingestion integration. PR body updated. |
| UKMesh #125 | Perf merge committed as `ebab933`; RAM repair code remains an intermediate, unpushed checkpoint. Local TypeScript and 464/464 backend unit tests pass. Real PostgreSQL/Redis CI and final scope/privacy/completeness gates remain. |
| CoreScope #1 | Local branch head remains `90c104eab51618894a3b69ab521829f39313e261`. F08 changes are uncommitted; focused Go regressions pass. One full Go suite passed before the latest edits and must be rerun. No push is possible from this host. |
| Bench | Read-only; no `FINAL-CHAIN-DONE` marker observed yet. No new image or scenario has been run. |

## Finding map

| Finding | Status | Code and regression evidence |
|---|---|---|
| F01 | Partial | #125 fresh bulk policy/generation/rematerialization check in `packetBatch.ts`, `liveRead.ts`, `client.ts`, `ws/privacy.ts`; stale-cache regressions pass. CoreScope and revocation race remain. |
| F02 | Implemented, awaiting real PG CI | Separate same-client transaction lock and ledger-derived mutations in `packetBatch.ts`; unit and PG integration regression added. |
| F03 | Fixed on #122 and merged into #125 | `backend/src/mqtt/observationIdentity.ts`, `client.ts`, `db/packetBatch.ts`; unit and real-PG regression passed on #122 CI. |
| F04 | Fixed on #122 and merged into #125 | `backend/src/mqtt/ingestConfig.ts` rejects N>1 before connect; regression passes. |
| F05 | Partial, awaiting real Redis/PG CI | Delivery-key replay ledger in `ram/liveStore.ts`, stable observation ID and PG ledger; lost-reply and same-ms unit/PG integration regressions added. |
| F06 | Partial | Per-entry commit state, retained overlap and stable-ID dedupe in `ram/liveStore.ts`, `writer.ts`, `liveRead.ts`; marker-gap and PEL-hole unit regressions pass. Snapshot/retention integration remains. |
| F07 | Pending | #125/CoreScope durable completeness source for late commits. |
| F08 | Partial, uncommitted | CoreScope `ingest_buffer.go` tracked completion, `source_batch.go`, `postgres_source.go`, `redis_source.go`, and packet-path `main.go` error propagation. `TestF08TrackedJobReportsActiveCompletionAndWriteFailure`, `TestF08TrackedJobReportsAdmissionFailure`, `TestF08SourceBatchWaitsForLastJobAndReturnsWriteFailure`, and `TestF08HistoricalPacketHandlerPropagatesSQLiteFailure` pass. Full Go suite, source integration and failure audit pending. |
| F09 | Partial, awaiting real Redis CI | One-time atomic cleanup, bounded reconciliation and retained accounting in `ram/liveStore.ts`, `writer.ts`; replay and cleanup unit tests pass. |
| F10 | Partial | Supervised startup, NOGROUP repair and periodic bounded foreign PEL reclaim in `index.ts`, `ram/writer.ts`; unit regressions pass. Progress health remains. |
| F11 | Partial, awaiting real Redis CI | Real ioredis XADD quarantine and common validated decode in `ram/writer.ts`, `protocol.ts`; malformed PEL unit tests pass. Permanent SQL poison isolation remains. |
| F12 | Implemented, awaiting integration | Stable-ID aggregate and PG overlap in `ram/liveRead.ts`; same-observer sighting regression passes. |
| F13 | Pending | #125 scope/alias/window/DTO/read contract. |
| F14 | Partial | #122 metric registration fixed. #125 adds pending/retained/PEL gauges and minimum admitted-age ordering in `metrics.ts`, `ram/writer.ts`; metric semantics audit remains. |
| F15 | Partial | #122 deadline fixed. #125 joins writer loop and detects PEL on drain in `ram/writerRuntime.ts`, `writer.ts`; held-persist and client-close regression remains. |
| F16 | Partial | Compose environment pass-through and CI render assertion added; documentation and verified standalone drain/rollback remain. |
| F17 | Pending | CoreScope stable upstream identity and idempotent derived state. |

The §7 advisory-lock snapshot caveat is addressed in code by a separate-statement same-client transaction and unique identity ledger, with real-PG regression pending CI. The §4 eleven-item verification and §5 methodology repairs remain to be repeated on final heads. Current tests do not establish privacy, handoff or whole-stack performance of RAM mode.

## Existing test observations

- #122 initial full-suite run: 417/418, with a timing failure in the unrelated `path-beta/workerPool` abort test. Focused rerun: 3/3; final full suite: 418/418. Log: `/tmp/ukmesh-perf-fix-npm-test-final.log`.
- #122 CI run `38154922672` completed successfully in Backend, Frontend, Workers and Compose, and Secret scan.

## Operational boundary

No production database, Redis, broker, service, configuration, deployment or running process has been modified. No PR has been merged or approved. The bench campaign has only been inspected read-only. CoreScope has not been pushed.
