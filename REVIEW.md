# REVIEW — backlog wave B-ukmesh

## Item #553 — replace-container mock harness

**Status:** DONE via coordinator [PR #114](https://github.com/gadgethd/ukmesh/pull/114), merged to `main` as `b40793f` from `fix113-on-main` head `87da64c`.

**Files changed:** `scripts/test-replace-container.sh`, merged from the coordinator-owned branch. No changes for this item were pushed by this worktree to `fix113-on-main`.

**Why:** `replace-container.sh` now calls `check-compose-adoption.sh` and inspects Compose labels before mutation. The isolated test project did not stage that child script, and its Docker mock did not model the label response. The harness now stages the script, supplies configurable valid/invalid label metadata, and covers adoption rejection and accepted cases before mutation.

**Tests run:** At coordinator head `87da64c`, `bash scripts/test-replace-container.sh` passed the 16-case adoption/rollback suite, including infra-compose staging and TimescaleDB drill assertions. All PR #114 CI checks passed before merge. The earlier failure was caused by the isolated fixture omitting the child adoption script and not modeling Compose labels.

**PROOF:** `bash scripts/test-replace-container.sh` (`fix113-on-main` head `87da64c`) -> `/replace-container rollback, compatibility and Compose adoption drills passed \(16\/16\)/`

**Deliberately left out:** No product behavior changed for this harness-only fix. The coordinator owns branch and PR disposition.

**Open questions:** None. The coordinator owns `fix113-on-main`; do not push further to that branch.

## Item #543 — link-worker heartbeat alert

**Status:** ALREADY FIXED on `main` by `b40793f` (the #114 re-land). The newer main implementation supersedes this branch's alternative and is covered by [main's review](https://github.com/gadgethd/ukmesh/blob/main/REVIEW.md). PR #115 was closed as redundant after it became conflicting with `main`.

**Files changed:** On `main`: `backend/src/health/status.ts`, `backend/src/health/workerHeartbeat.ts`, `backend/src/health/workerHeartbeat.test.ts`, `backend/src/metrics.ts`, `logging/rules/meshcore.yml`, `logging/rules/meshcore.test.yml`, and `viewshed-worker/tests/test_link_worker_heartbeat.py`.

**Why:** `ActiveQueueWorkerHeartbeatStale` originally used `MAX(node_links.itm_computed_at)` as if it were process liveness. Main now reads the link worker's Redis heartbeat (`meshcore:link:v3:worker_heartbeat`) on every scrape, treats a missing or invalid heartbeat as unhealthy, and alerts for a queued worker when heartbeat age is over 180 seconds or missing, with the existing three-minute hold. The publisher refreshes every ten seconds with a 45-second TTL. This directly measures worker liveness and preserves the existing metric name.

**Tests run:** Main's review records `node --import tsx --test src/health/workerHeartbeat.test.ts` (5/5), publisher tests `python3 -m unittest discover -s viewshed-worker/tests -p test_link_worker_heartbeat.py -v` (5/5), backend suite 343/343, Prometheus rules validation (24 rules), and 7 rule-test scenario groups. All PR #114 CI checks passed before merge. The superseded PR #115 branch separately passed `npm run typecheck`, metrics tests 2/2, full backend suite 374/374, Prometheus/Alertmanager validation, and CI run #484 on head `ccd5a9b`; it is not the implementation retained on main.

**PROOF:** Main's review documents the Redis publisher cadence/TTL tests, missing and stale-heartbeat rule cases, and the live-queue alert holdoff. The upstream implementation is on `main` at `b40793f`.

**Deliberately left out:** No deployment or live service changes; the brief prohibits them. The alternative implementation from commit `13f03de` was not carried across the conflicting main landing.

**Open questions:** No code work remains for #543 in this wave. Production Redis freshness and alert recovery remain deployment-time checks documented in main's review.

## Item #437 — orphan `runState.test.ts`

**Status:** CLOSED-ALREADY on main; no changes made.

**Files changed:** None. `backend/src/analysis/runState.ts` exports `normalizeRetiredAnalysisState`, and `backend/src/analysis/runState.test.ts` is tracked.

**Why:** The missing export described by the backlog item was restored in commit `9a5e838` before this wave.

**Tests run:** `node --import tsx --test src/analysis/runState.test.ts` passed 3/3; the full backend `npm test` suite passed 374/374.

**PROOF:** `node --import tsx --test src/analysis/runState.test.ts` -> `/# tests 3[\s\S]*# pass 3[\s\S]*# fail 0/`

**Deliberately left out:** No change to already-working analysis run-state behavior.

**Open questions:** None.

## Item #301 — missing `normalizeRetiredAnalysisState` export

**Status:** CLOSED-ALREADY; duplicate of #437.

**Files changed:** None; the export and test are present on main.

**Why:** Commit `9a5e838` restored the export and the current focused test passes.

**Tests run:** `node --import tsx --test src/analysis/runState.test.ts` passed 3/3; the full backend `npm test` suite passed 374/374.

**PROOF:** `git show 9a5e838:backend/src/analysis/runState.ts | rg -n normalizeRetiredAnalysisState` -> `/export \{ normalizeRetiredAnalysisState \} from '\.\/workloadPolicy\.js'/`

**Deliberately left out:** No duplicate fix for the same file/API.

**Open questions:** None.

## Item #536 — stale deploy-tree `runState.test.ts`

**Status:** CLOSED-ALREADY for this repository state; duplicate of #437/#301.

**Files changed:** None; the current repository tracks the test and exports the imported function.

**Why:** The stale untracked deploy-tree copy described in the backlog is not the state of this worktree. The current tracked test imports an existing export and passes.

**Tests run:** `node --import tsx --test src/analysis/runState.test.ts` passed 3/3; the full backend `npm test` suite passed 374/374.

**PROOF:** `node --import tsx --test src/analysis/runState.test.ts` -> `/# tests 3[\s\S]*# pass 3[\s\S]*# fail 0/`

**Deliberately left out:** No deletion or duplicate export was added.

**Open questions:** The item also described an untracked copy on the VPS deploy tree. That external checkout was not inspected or edited; reconcile the local artifact separately when permitted.
