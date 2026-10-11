# UKMesh ingestion review repair plan — 2026-10-11

Specification: `/home/ben/ukmesh-fix-context-20261011/SOL-REVIEW.md`.
The full F01–F17 mission remains active. This current plan supersedes the
historical handoff tables, which remain available in git history.

## Completed sequence

1. Repaired #122 F03 observation identity, F04 unsupported N>1 configuration,
   F14 metric registration and F15 one-deadline accounting. Perf head
   `8968078` is pushed/remote-verified; full 418-test suite/typecheck and both
   current CI workflows pass. Merged perf into RAM, retaining the stack.
2. Repaired #125 SQL lock/ledger mutation safety; retry-stable admission UUIDs;
   per-entry retained accounting/handoff; writer supervision, decode/quarantine,
   read counts/contracts and shutdown/drain. Added real refused-write,
   ambiguous admission, snapshot-before-commit/expiry and group-loss gates.
3. Added historical migration 059 and its atomic coverage fence. CoreScope
   committed-order completeness, cursor barriers, stable-ID derived state,
   actual crash/SQLite failure and shared-schema restart repair are committed
   locally. CoreScope `07f05cd` additionally fences/reconstructs copied public
   data, preserves participant provenance and uses a filtered HTTP live source.
   Full ingestor/server suites pass; three commits await coordinator relay.
4. RAM public/read checkpoint `c683acd` is pushed. Both workflows
   `38180468916` / `38180473812` passed every job. Its strict source-labelled
   image gate passed exactly 10/10, zero skips. Current policy now governs
   SQL aggregates, WS caches and bounded live batches; ready WS regression
   covers 800/800 allowed frames and revocation/unavailability.

## Current operational/read wave

F09 startup/admission reserve 32 MiB of Redis recovery headroom; encoded
quarantine has an exact 500-record/16-KiB cap. F14 missing-group/null-memory
handling clears stale gauges and actual HTTP scrape checks known loaded states.
F15/F16 compiled processes hold real PG persistence across SIGTERM, recover the
original UUID, refuse quarantine rollback, wait through actual 120-second
retention and prove empty stream/PEL/reservation before durable mode. Actual
writer/public/quarantine Redis principals are isolated. CI owns those fixtures.
F13 preserves decrypted-summary fallback and exact JSONB summary extraction;
F01 delivery rechecks generation after asynchronous alias/summary enrichment.
New regressions reproduce old failures. Final full backend suite passes
479/479 (193.952s), typecheck/build pass, full ingest integration passes
7 PG + 3 Redis + 1 WS, and the strict actual ACL/runtime gate passes exactly
4+1 tests with zero skips (runtime 152.153s). Loaded resident-memory refusal
and all ten Compose RAM/public-source settings pass. Next commit/push and CI.

## Remaining work, in order

1. Commit/push the locally verified operational/read RAM wave and watch
   its new full CI to actual success. Existing green CI
   is evidence only for its matching revision.
2. Build a uniquely labelled final RAM image; pin compiled fixture sources
   to that label and require exact nonzero test totals with zero skips.
3. Use newly owned isolated services/processes for a whole-stack campaign;
   preserve original services and all other run directories. Replace the
   old processed/XLEN drain oracle with exact expected identities, every
   side-effect total, actual pending/PEL and completion barriers. Wait for
   readers to be ready and stop/join each one at the recorded endpoint.
4. Audit actual paired CoreScope source/server privacy transitions after
   rematerialization, late commit/offline/cursor loss and restricted public
   credentials. Measure backend and whole-stack CPU/memory, Redis AOF,
   PG WAL/checkpoints and physical I/O over the same completed window;
   compare final-code durable/RAM workloads with repetitions.
5. Refresh both existing PR finding maps, hashes, CI links, evidence and
   limitations. Keep #122 open/non-draft and #125 draft. Finalize FIX-REPORT
   and the three-commit CoreScope relay patch, then verify remote UK heads,
   clean local CoreScope commits and green current CI before completion.

## Boundaries

Production remains untouched. No deployments, existing service restarts,
PR merges/approvals or CoreScope pushes. No subagents. The original bench
`FINAL-CHAIN-DONE` marker was observed at 17:39:52 UTC. Owned test processes
and containers began afterward. Durable is the default; RAM is opt-in;
N>1 remains gated. Current-authority failure is deliberate public
unavailability, and already delivered data cannot be recalled. Preserve
old untracked CoreScope DBs; initialize a new dedicated public projection.
