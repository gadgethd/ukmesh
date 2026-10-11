# SOL FIX BRIEF — UKMesh ingestion + CoreScope repair round (2026-10-11)

You are **GPT-6.1 Sol at xhigh effort**. You are implementing the repairs demanded by an independent architecture & security review of the UKMesh MQTT ingestion stack. **Read the review first — it is the specification:** `/home/ben/ukmesh-fix-context-20261011/SOL-REVIEW.md` (17 findings F01–F17, §4 eleven-item verification, §7 failure/concurrency analysis incl. the advisory-lock snapshot caveat, §8 prioritized repair plan, §9 acceptance criteria). Evidence fixtures the reviewer built are in `/home/ben/ukmesh-fix-context-20261011/evidence/` (sql-repro.ts, redis-repro.ts, core-buffer.log, unit-tests.log, corescope-tests.log, bench/) — reuse and extend them as regression fixtures. Supporting context: `DESIGN.md` (architecture), `review-fixes-checklist.md` (previous round's claims — do not trust, the review supersedes).

## Mission

Fix **ALL** review findings, in priority order, across three codebases, then update the existing PRs. Keep the existing stacked-branch relationship; do not open replacement PRs; do not merge anything; do not touch production.

### Repositories and workspaces (all on this VPS where you run)

1. **gadgethd/ukmesh** — two git worktrees of the same clone:
   - `/home/ben/ukmesh/.agent-worktrees/mqtt-ingest-perf-20261011` → branch `perf/mqtt-ingest-20261011` (**PR #122**) — #122-scoped fixes here.
   - `/home/ben/ukmesh/.agent-worktrees/ram-ingest-20261011` → branch `experiment/ram-first-ingest-20261011` (**PR #125**, stacked on #122) — #125-scoped fixes here. Your session cwd.
   - git credentials on this VPS can push gadgethd/ukmesh over HTTPS.
2. **hermes-gadget/CoreScope** (fork) — local clone `/home/ben/corescope-ukmesh/src` with branch `feat/redis-live-source` (**CoreScope PR #1**). **You cannot push this repo from the VPS** (credentials resolve to `gadgethd`, 403 on the fork). Commit your CoreScope changes locally and include the exact commit hashes + a `git format-patch`-ready description in your FIX-REPORT; the coordinator will relay-push them. Keep commits clean and self-contained.

### Recommended order (write FIX-PLAN.md first, then execute)

1. **#122 fixes on the perf worktree** (repair independently per review §8): F03 (restart-dedup probe discards a distinct observation — define a canonical observation identity and use it consistently in claim arbitration + restart replay), F04 (N>1 shared-subscription recovery: gate/remove the multicore mode for merge, keep N=1 supported default — implement a delivery-progress-based recovery or an explicit feature gate, per §8 item 2), F14's #122 metrics items (missing `registers:[metricsRegistry]` on follow-up inflight/drop counters), F15's #122 timeout accounting. Commit, push to `perf/mqtt-ingest-20261011`, get CI green.
2. **Update the ram worktree** from the pushed perf branch (merge perf into the ram branch so the stack stays honest — document the merge in the PR bodies), then do **#125 fixes**: F02 (**P0, first**: the hourly-stats predicate precedence bug — parenthesize `locked.ok AND (…)`; audit every side-effect CTE; ideally move the lock acquisition into a separate statement on the same pooled client and explicit transaction per the §7 snapshot caveat, or implement the identity-ledger approach the review sanctions), F01 (**P0**: one public-read authorization contract — authoritative policy/generation/rematerialization fence, batch authority checks, fail-closed on unavailable/stale authority, immediate revocation barrier, no frozen-epoch-as-permission; cover merged reads, WS initial state, WS live broadcast, and both CoreScope paths), F05 (retry-stable observation identity: stable ID before first ambiguous write + bounded admission ledger so a lost-reply retry returns the existing observation; distinguish same-ms distinct sightings), F06 (replace the marker/handoff protocol: readers must not lose observations admitted during a commit; retain from commit time; per-entry commit state or a rigorous committed-prefix; dedupe by stable observation ID against the PG snapshot), F07–F08 (with CoreScope), F09 (atomic/reconcilable byte accounting incl. retained storage; reconcile safely), F10 (supervised writer: group repair, periodic foreign-PEL reclamation, clear failed singleton state, progress health), F11 (**quarantine uses a nonexistent ioredis method `xaddWithLimit`** — use real ioredis `XADD ... MAXLEN ~ ...`; only ack/delete after successful quarantine; share validated decode across new/own/reclaimed paths; validate types/ranges before admission; isolate poison rows), F12–F13 (observation-identity dedupe + full SQL-equivalent contracts: network families, aliases, windows, slim/full DTO, representative fields, sampling/paging), F14 (split pending/retained/PEL gauges; min-admitted order for age trigger; zero-capacity new-read skip; bounded startup reclaim; correct metric registration and semantics), F15 (join the writer loop in drain; explicit complete/incomplete result; quit owned clients), F16 (Compose env wiring for `MQTT_INGEST_MODE`/`MQTT_RAM_*`, `docs/ram-first-ingestion`, verified drain/rollback procedure, readiness), the §7 advisory-lock snapshot caveat, and the §5 methodology code items you can address in code/tests.
3. **CoreScope fixes** (local commits): F01 (current-authorization check + revocation path; or a filtered public source), F07 (late-commit completeness: durable outbox/ledger or a proven lookback with stable identity — cannot rely on the admission-time PG cursor), F08 (completion barriers: advance cursors only after batch completion incl. the active job; propagate write failures; keep cursor behind them — both sources), F17 (idempotent derived state: observer counts only for newly accepted observations; preserve upstream observation identity and define enrichment).

### Hard constraints

- **Production untouched.** No deploys, no service restarts anywhere, no modifications to running databases/Redis/MQTT. The bench rig below is the ONLY place for heavier testing, and it is mid-campaign:
  - Bench rig: `/home/ben/ukmesh-bench/mqtt-ingest-perf-20261011` (isolated compose project `mqttbench` — its own network/volumes/ports; synthetic traffic only). A benchmark campaign runs until roughly 18:30 UTC running `scripts/run-final-chain.sh` logic (`campaign-ram-*.log` in `logs/`). While it runs: **read-only** — do NOT stop, restart, modify or interact with any bench service or its processes; do not edit the bench scripts it is executing (`run-scenario.sh`, `run-campaign-ram.sh`, `run-extras-ram.sh`). After it finishes (settle marker `FINAL-CHAIN-DONE` in `logs/final-chain-*.log`), you may use the rig for your own verification: build your own variant image (`scripts/build-variant.sh <your-tag> <src-dir>`) and run scenarios/integration suites — never disturb other run logs.
  - Integration harnesses to reuse/extend: `publisher/crash-replay-test.cjs`, `publisher/admission-concurrency-test.cjs`, `scripts/run-integration-tests.sh`. Extend them with the review's demanded regression cases: refused-write-changes-no-table (concurrent PG, F02), ambiguous admission returns existing observation (F05), marker-gap (admit during commit — F06), privacy flip during pending + during commit (F01), real-ioredis quarantine path (F11), NOGROUP/group-loss recovery (F10), shutdown-joins-inflight (F15). Deterministic boundary failures, not timed kills.
- Keep **durable mode as the default and fully safe**; RAM mode stays opt-in. Preserve #122's process-local dedupe semantics and the bounded follow-up design the review approved.
- Do not pivot the architecture beyond the review's mandated redesigns (identity, authorization contract, handoff protocol). The §8 "simpler alternatives" (A–D) are the owner's call, not yours to switch to.
- Update PR descriptions for #122 and #125 with a finding→fix map (F01–F17 + the caveat + methodology items), test evidence, and residual limitations. #125 stays draft; #122 stays open non-draft (owner controls merge/draft state). Do not merge, do not approve.
- Work directly — no sub-agents, no MissionDeck, no delegation.

### Testing requirements (evidence, not vibes)

For every finding: at least one new regression test that fails on the old code and passes on the fixed code (unit where possible; harness-level for concurrency/recovery), named after the finding in the commit message. Run on every push: `npx tsc --noEmit` + full `npm test` in `backend/`, plus the focused suites for touched areas. Before declaring done: run the full CI (push → gh run watch) on both ukmesh branches, and the CoreScope `go test ./...` in `cmd/ingestor` on your commits. All green = done; report honestly anything that is not.

### Deliverables

1. `FIX-PLAN.md` (dated; per-finding approach + order) written before the code edits, kept updated.
2. Pushed commits on both ukmesh branches (exact hashes in your report; CI run URLs).
3. Committed (not pushed) CoreScope changes with hashes + patch description for relay.
4. `FIX-REPORT.md` in the ram worktree: per-finding status (fixed / partial with reason / not feasible with reason), code refs, test names, CI results, residual limitations, and explicit confirmation that no production system was modified.
5. Final message: same summary in compact form.

## PROOF

- `git ls-remote origin` shows your new commit hashes at the heads of `perf/mqtt-ingest-20261011` and `experiment/ram-first-ingest-20261011`.
- `FIX-REPORT.md` exists in `/home/ben/ukmesh/.agent-worktrees/ram-ingest-20261011/` with per-finding status.
- CI runs for both branches conclude `success` (report run IDs).
- CoreScope commits present in `/home/ben/corescope-ukmesh/src` (`git log --oneline` of `feat/redis-live-source`) awaiting relay.
