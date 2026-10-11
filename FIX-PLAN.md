# UKMesh ingestion repair plan — 2026-10-11

Specification: `/home/ben/ukmesh-fix-context-20261011/SOL-REVIEW.md`, especially findings F01–F17, §§4, 7–9. This plan records implementation order and will be updated as evidence changes. The independent review takes precedence over the earlier fix checklist.

## Boundaries and sequence

Production and running services remain untouched. No deployment, service restart, merge, or CoreScope push. The bench rig remains read-only until a `FINAL-CHAIN-DONE` marker is observed in its final-chain log. Durable ingestion stays the default; RAM ingestion remains opt-in. Existing untracked worktree files are preserved.

1. On `perf/mqtt-ingest-20261011` (#122), add failing regressions, repair F03/F04 and its F14/F15 items, run TypeScript and the full backend suite, commit and push, then watch CI to success. Keep the N=1 supported default and gate N>1 pending pinned broker qualification.
2. Merge the pushed perf head into `experiment/ram-first-ingest-20261011` (#125), preserving the stack and recording the merge in both PR descriptions. Repair shared SQL safety first, then identity/privacy/handoff, then writer/read/recovery/operational contracts. Add a regression for each finding before its fix. Run the full backend and targeted integration gates, commit and push, and watch CI to success.
3. In the local CoreScope `feat/redis-live-source` branch, repair the dependent privacy, completeness, cursor, and idempotency contracts. Add failing Go regressions first, run `go test ./...` from `cmd/ingestor`, and commit clean changes for relay without pushing.
4. After the bench campaign settles, build a uniquely tagged image from the repaired source and run the isolated integration harnesses and identity-checked final-head campaign. Preserve the other campaign's logs. Update PR bodies with finding→fix map, evidence and limitations. Write `FIX-REPORT.md` with hashes, CI URLs, per-finding evidence and confirmation that production was not changed.

## Finding-by-finding implementation

| Finding | Branch | Intended repair and regression |
|---|---|---|
| F01 | #125 + CoreScope | One current-authority public-read contract with batch policy/generation/rematerialization checks, fail-closed behavior and a revocation barrier. Apply to REST merge, WS initial/live and both CoreScope paths; test observer/source/prefix flips during pending and commit, stale/unavailable authority and network scope. Never use frozen `vo` as permission. |
| F02 | #125 | Parenthesize the hourly lock gate and audit every mutation CTE. Acquire advisory lock in a separate statement on one pooled client inside an explicit Read Committed transaction, or enforce equivalent identity ledger. Concurrent PG refusal must change no table. |
| F03 | #122 | Define canonical observation/replay identity including the distinctions used by claim arbitration; use it in restart probe and SQL matching. Test lost PUBACK, older same-hash sighting, hop/path, observer and scope differences, including historical side effects. |
| F04 | #122 | Explicitly gate N>1 shared subscriptions pending restart/resize qualification while retaining N=1. Test unsupported configuration fails safely and default subscription/reconnect remains supported. |
| F05 | #125 | Create a retry-stable observation ID before the first ambiguous Redis write; atomic bounded admission ledger returns the original record on retry. Preserve distinct same-millisecond sightings and enforce identity at PG insertion. Test lost Redis reply and same-ms distinct path/hop. |
| F06 | #125 | Replace cumulative markers with per-entry commit state or a proven contiguous prefix. Keep retained overlap readable, retain from commit time and dedupe against PG by stable observation ID. Test admission during commit, old PEL hole and snapshot-before-commit handoff. |
| F07 | #125 + CoreScope | Provide a durable committed-order/outbox completeness source, or a formally bounded overlap backed by stable identity; ensure late PG commits remain discoverable after Redis retention/offline periods. Test out-of-order commit, offline beyond retention and cursor loss. |
| F08 | CoreScope | Wait for the active job and all submitted writes before either source saves a cursor; propagate write failures and preserve the last safe cursor. Test blocked last job, DB failure and both source orders. |
| F09 | #125 | Atomic one-time per-entry cleanup/accounting, safe reconciliation and separate pending/retained budgets including PEL/quarantine headroom below shared Redis maxmemory. Test interrupted cleanup, replay and concurrent admission/reconciliation. |
| F10 | #125 | Supervise/retry startup; heal missing group, periodically and boundedly reclaim foreign PEL, clear failed singleton state and expose writer progress/readiness. Test NOGROUP, startup failure and young foreign PEL aging into reclaim. |
| F11 | #125 | Use real ioredis `XADD ... MAXLEN ~` for quarantine; validate common decoder on all delivery paths; preserve source until quarantine succeeds; isolate poison rows. Test real client, malformed own/reclaimed PEL and permanent SQL poison. |
| F12 | #125 | Aggregate/dedupe observations by stable ID with PG identity overlap, canonical observer aliases and correct RX/TX counts. Test multiple sightings by the same and new observers during handoff. |
| F13 | #125 | Match SQL network-family scope, aliases, five-minute window, representative fields, full/slim DTOs, and distinct-hash paging. Cover pending detail/channel-history behavior or document explicit pending support. Test side-by-side SQL/RAM fixtures. |
| F14 | #122 + #125 | Register follow-up metrics in the served registry. Separate pending/retained/PEL age/depth, use minimum admitted time, skip zero-capacity new reads, bound reclaim and count actual inserted/deleted work. Test metric scrape and each queue edge case. |
| F15 | #122 + #125 | Start one MQTT shutdown deadline before disconnect. Join RAM writer loop and active write, serialize drain, return explicit complete/incomplete with PEL/outstanding state, close owned clients. Test held persist and failed flush. |
| F16 | #125 | Wire Compose RAM variables, add `docs/ram-first-ingestion` and a verified standalone drain/rollback procedure plus readiness. Test Compose render and backlog before mode rollback. |
| F17 | CoreScope | Preserve upstream observation ID; apply derived state once for a newly accepted observation and define enrichment on duplicate source. Test one Redis+PG observation in both arrival orders and single observer count increment. |

## Cross-cutting acceptance gates

- §7 snapshot caveat: concurrent transaction test that schedules lock release after competing statement start; replay and lock refusal must produce no duplicate packets, paths, hourly or region effects.
- §4 eleven-item verification: use the review's SQL/Redis evidence fixtures and extend the named integration harnesses for refused writes, ambiguous admission, marker gap, privacy flips, real quarantine, group loss and shutdown join.
- §5 methodology: correct the below-minimum byte-limit test; compare expected distinct observation identities and all side-effect totals; stop/join each reader; measure complete windows, Redis AOF and PG WAL/physical I/O, and distinguish backend from whole-stack CPU/RAM. Report unresolved limits honestly.
- Each finding needs a regression that fails on reviewed code and passes on repaired code. Every push gets `npx tsc --noEmit`, full `npm test` in `backend/`, focused suites, and successful CI. CoreScope gets `go test ./...` in `cmd/ingestor`.

## Current state

The perf branch is pushed at `cbf78a8` with F03/F04/F14/F15 regressions. Local `npx tsc --noEmit` and the full 418-test backend suite passed; CI run `38154922672` succeeded. The RAM branch merged that head in `ebab933`; F01/F02/F05/F06/F09/F10/F11/F12/F14/F15/F16 repairs are in progress and 464 unit tests currently pass. The RAM integration gate and final F01/F07/F13/F16 work remain. CoreScope remains at `90c104e` with uncommitted F08 completion-barrier work; the full Go suite passed before the last focused edits and must be rerun.
