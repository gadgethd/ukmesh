# REVIEW — backlog wave B-ukmesh

## Item #553 — replace-container mock harness

**Status:** DONE; superseded by the coordinator's full harness on `fix113-on-main` at `87da64c`.

**Files changed:** `scripts/test-replace-container.sh` on the coordinator-owned branch. No changes for this item are being pushed to `fix113-on-main`.

**Why:** `replace-container.sh` now calls `check-compose-adoption.sh` and inspects Compose labels before mutation. The isolated test project did not stage that child script, and its Docker mock did not model the label response. The harness now stages the script, supplies configurable valid/invalid label metadata, and covers adoption rejection and accepted cases before mutation.

**Tests run:** At coordinator head `87da64c`, `bash scripts/test-replace-container.sh` passed the 16-case adoption/rollback suite, including infra-compose staging and TimescaleDB drill assertions. The earlier failure was caused by the isolated fixture omitting the child adoption script and not modeling Compose labels.

**PROOF:** `bash scripts/test-replace-container.sh` (`fix113-on-main` head `87da64c`) -> `/replace-container rollback, compatibility and Compose adoption drills passed \(16\/16\)/`

**Deliberately left out:** No product behavior changed for this harness-only fix. The coordinator owns branch and PR disposition.

**Open questions:** None for this backlog item. Do not push further to `fix113-on-main`.

## Item #543 — link-worker heartbeat alert

**Status:** FIXED in [PR #115](https://github.com/gadgethd/ukmesh/pull/115), commit `13f03de`.

**Files changed:** `backend/src/health/status.ts`, `backend/src/metrics.ts`, `logging/rules/meshcore.yml`, `logging/rules/meshcore.test.yml`, and `docs/operations.md`.

**Why:** `ActiveQueueWorkerHeartbeatStale` used `MAX(node_links.itm_computed_at)` as if it were process liveness. It now uses the RF link worker's own `meshcore_worker_heartbeat_timestamp_seconds{worker="link"}` metric and alerts after 600 seconds of staleness with the existing three-minute hold. The backend metric derived from database timestamps is now `meshcore_worker_data_age_seconds`; the health alert and runbook identify snapshot freshness separately from process liveness. The `link`, `path_learning`, `health`, and `link_backfill` values all measure persisted-data recency.

**Tests run:** `npm run typecheck` passed; `node --import tsx --test src/metrics.test.ts` passed 2/2; the full backend `npm test` suite passed 374/374; Prometheus config and rules validation passed; Alertmanager config validation passed. The rule test verifies that stale database data alone does not fire the queued-link liveness alert and that a genuinely stale in-process heartbeat does.

**PROOF:** `docker run --rm -v "$PWD/logging/rules:/rules:ro" --entrypoint /bin/promtool prom/prometheus@sha256:63805ebb8d2b3920190daf1cb14a60871b16fd38bed42b857a3182bc621f4996 test rules /rules/meshcore.test.yml` -> `/SUCCESS/`

**Deliberately left out:** No deployment or live service changes; the brief prohibits them.

**Open questions:** Check out-of-repository dashboards or alerts for consumers of the renamed `meshcore_worker_heartbeat_age_seconds` metric before deployment, and review the latest PR #115 CI result before merge.

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
