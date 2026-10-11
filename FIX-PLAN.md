# UKMesh ingestion review repair plan — 2026-10-11

Specification: `/home/ben/ukmesh-fix-context-20261011/SOL-REVIEW.md`.
The F01–F17 code and verification work is complete for N=1. This plan supersedes the
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
and all ten Compose RAM/public-source settings pass. The wave is pushed at `c02aa32`; workflows 38182374772 / 38182378151
passed every job. Its pinned labelled image passes exactly 11 + 4 + 1 compiled
tests with zero skips. Actual paired privacy/rematerialization, copied-data
revocation, unavailable authority and private-to-public replay pass.

## Final acceptance

1. The operational code head `c02aa32` is pushed and both full CI workflows
   passed every job. Its labelled image passes exactly 11 integration + 4
   operational + 1 compiled runtime tests, zero skips. Final local release
   typecheck/full backend suite passes 479/479 (201.590s).
2. Actual paired privacy/rematerialization, copied-data revocation, private-to-
   public replay, unavailable authority, restricted public credentials, late
   commit/offline/cursor loss and real retention expiry pass.
3. All 14 measured runs (6,840 observations) and the 3,000-observation saturation
   integrity run pass exact identity/side-effect/reader/drain gates. Independent
   PG/CoreScope UUID sets match 9,959/9,959, with identical journal/derived counts
   and zero final stream/PEL/reservation. Whole-fixture CPU/memory, AOF, WAL,
   checkpoints, transactions and physical I/O are recorded over completed
   windows. Three burst/10/s repetitions and a diagnostic 2/s tail pair show
   latency gains and additional retained-cleanup costs; no production savings
   claim is made.
4. The documentation-only checkpoint has the same runtime build inputs. Its
   own source-pinned image gates and fresh CI are recorded with the exact
   branch hash in PR #125 and the final handoff. Both existing PR descriptions
   carry the final finding map/evidence; #122 stays open/non-draft and #125
   open/draft. The ready three-commit CoreScope relay patch is
   `evidence/corescope-sol-relay-final-20261011.patch`; no CoreScope push.

## Boundaries

Production remains untouched. No deployments, existing service restarts,
PR merges/approvals or CoreScope pushes. No subagents. The original bench
`FINAL-CHAIN-DONE` marker was observed at 17:39:52 UTC. Owned test processes
and containers began afterward. Durable is the default; RAM is opt-in;
N>1 remains gated. Current-authority failure is deliberate public
unavailability, and already delivered data cannot be recalled. Preserve
old untracked CoreScope DBs; initialize a new dedicated public projection.
