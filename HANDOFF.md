# Mission C5d handoff

Status: implemented; production rollout remains gated.
Branch: `fix/ukmesh-ops-pipeline`
Base: fetched `origin/main` at `2fa80fa`.
Push state: not pushed; coordinator relays this branch.

## Commits

- `4c549bb` — `test(viewshed): inject post-commit side-effect failures`
- `1d4d594` — `fix(deploy): rollback on unhealthy or mismatched sites`
- `f48c652` — `fix(alerts): persist forwarding queue across restarts`
- `d93030c` — `perf(stats): add per-network aggregate cutover controls`

## Changes

- #47: the fetched main already contained the Redis completion marker and replay path. Added fault injection at link admission, both notification publishes, and marker persistence to verify retry behavior through the already-calculated path. Per the brief, this new test was not run.
- #54: deployment now waits for a running, healthy container and a successful HTTP response, rejects empty bundle lists and mismatches, and rolls back the prior pin on any failure. Rollback checks the restored service and supports an empty prior pin. Added mocked deploy tests.
- #55: forwarding records are fsynced into the mounted queue before HTTP 202. Startup restores pending records; retries use bounded backoff and dead-letter files. Health JSON exposes queue age, pending/dead-letter counts, last success, and last error. Archive-only mode returns degraded status. Updated the alert runbook and its archive-only test expectation.
- #57: existing aggregate controls now accept comma-separated network names through the Compose-forwarded variables; shadow comparisons can run while reads stay off. Chart scans share one `asOf` value, cancel as a batch on query failure, and enforce duration, timeout, and returned-row budgets. True PostgreSQL rows-scanned counts are not exposed by the current query API; review query plans and DB scan telemetry during the gated shadow period.

PR #99 and #100 have no functional overlap with the changed worker, deploy, alert receiver, or stats repository code. PR #100 edits other sections of `docs/operations.md`. Both PRs also add `HANDOFF.md`; combine their handoff sections when integrating those branches.

## Tests and checks

- From `backend`: `node --import tsx --test src/workers/alertDeliveryQueue.test.ts src/workers/alert-receiver.test.ts src/stats/statsRepository.test.ts` — passed, 18/18.
- From `backend`: `npm run typecheck` — passed.
- From the worktree root: `bash -n scripts/deploy-website.sh scripts/test-deploy-website.sh scripts/test-alert-receiver.sh && bash scripts/test-deploy-website.sh` — passed.
- `git diff --check` — passed.
- `viewshed-worker/tests/test_side_effect_markers.py` was not executed, as required by #47. `scripts/test-alert-receiver.sh` was not run because it starts a receiver process, prohibited by the host constraint.

No services or containers were started, no live DB writes or config edits were made, and no push was attempted.

## Gated items for Ben

- Backfill and catch up hourly rollups, set `STATS_AGGREGATE_SHADOW_ENABLED=ukmesh`, and review clean parity logs before setting `STATS_AGGREGATE_READS_ENABLED=ukmesh`. Keep the shadow window short and review DB scan plans/telemetry because the application result-row guard is not a scanned-row counter.
- Confirm `ALERT_FORWARD_URL` is valid in production and inspect `/healthz` after the approved config rollout. Archive-only operation now reports HTTP 503 health.
- The C5d `HANDOFF.md` must be combined with the handoff content being added by open PRs #99/#100.

Open question: which database scan telemetry should be used to set a production rows-scanned threshold before additional networks switch to aggregates?
