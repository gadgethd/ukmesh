# UKMesh alerts and terrain CI — 2026-10-23

Branch: `burn/ukmesh-alerts-20261023`, based on freshly fetched `origin/main`
`73ab7d4fc63e4968a5fd46af022696ad69d16d9b`. Work stayed in
`/home/ben/worktrees/ukmesh/burn-mqttperf-20261022` and finished before 16:15 BST.

## Changes and evidence

### #683 — ActiveQueueWorkerHeartbeatStale

Current main already obtains the link heartbeat from Redis, rather than
`MAX(node_links.itm_computed_at)`. The producer in
`viewshed-worker/link_queue_v3.py:start_worker_heartbeat` writes Unix seconds
every **10 seconds**, with a **45-second TTL**, on an independent thread started
by `viewshed-worker/worker.py:worker_loop`. The backend's per-scrape collector
reads that key and exports `-1` for expired, invalid or unreadable values.
ITM publication can have the reported roughly 50-minute gaps without implying
that the worker is dead. Raising a process-liveness limit to an hour would hide
real publisher failures.

The rule now uses **age >45 seconds or age <0, sustained for three minutes**,
with nonempty queue and heartbeat still matched on the same `job`/`instance`.
The threshold follows the producer's TTL; the holdoff absorbs brief read
failures and reconnects. A stopped publisher should page in roughly four
minutes of its last write, subject to scrape/evaluation alignment. The runbook
documents the source, cadence and holdoff.

Rule checks cover 100 minutes of healthy heartbeats despite ITM output gaps,
the positive-age TTL boundary, expiry after worker death, holdoff, recovery,
idle queues, and independent reporter instances. The test evaluation interval
now matches the worker rule group's 15 seconds. An isolated control using the
old `>180` rule fails the new positive-age `46` regression; the changed rule
passes all 14 scenario groups.

Commit: `544f6ab75f838e854f217e4ef9a072f98449f644`.

### #695 — deterministic terrain/coverage readiness

The unchanged test failed on both desktop and mobile. Trace inspection then
showed service-worker-owned requests bypassing `page.route`, fetching the real
OpenFreeMap style, sprites, glyphs and vector tiles. This turned a fixture test
into an external-network/rendering race. A visible `Live Map` title also did
not establish that either map source was initialized.

The mocked dashboard suite now blocks service workers. The terrain case
records the first DEM and RF coverage responses, then polls their actual status
with a bounded 45-second readiness assertion before UI interaction. Failure
reports identify which tile is missing or has an unexpected status. Response
listeners replace navigation-time promises that could reject before being
awaited. Both responses must be outside the service worker, return PNGs, and
decode to 256×256 bitmaps; layer state, shareable URL and source-error assertions
remain. The 4,600-node fixture and existing 60-second overall timeout remain.
No test was skipped, and no application or service-worker runtime code changed.
Parallel instrumentation confirmed that DEM decoding can delay the map's `load`
event and RF overlay installation beyond 30 seconds; this step shares the
existing 60-second overall test budget instead of imposing an earlier UI or
30-second resource deadline.

Commit: `1efd63acad645b7df8b25e3133c4c95ccf1e649e`.

### Stretch #438 — owner last-hop prewarm progress

The existing two-window sequential warmer logged only enablement, errors and
completion. It now logs a pass start and progress every ten completed nodes,
including planned node count, owners, refreshed/skipped/failed counts and
elapsed time. Completion uses the same fields. New progress records contain
counts, without per-node identity fields. Scheduling, cache behavior, windows,
concurrency and failure handling are unchanged.

Commit: `3a837a047d755639af388e598a68d7a160d1c70f`.

## Verification

Commands below ran locally; paths are relative to the worktree unless a working
directory is specified. Docker checks used the same pinned Prometheus image as
the repository CI, with networking disabled.

```sh
docker run --rm --network none -v "$PWD/logging/rules:/rules:ro" --entrypoint /bin/promtool prom/prometheus@sha256:63805ebb8d2b3920190daf1cb14a60871b16fd38bed42b857a3182bc621f4996 check rules /rules/meshcore.yml
docker run --rm --network none -v "$PWD/logging/rules:/rules:ro" --entrypoint /bin/promtool prom/prometheus@sha256:63805ebb8d2b3920190daf1cb14a60871b16fd38bed42b857a3182bc621f4996 test rules /rules/meshcore.test.yml
PYTHONPATH=viewshed-worker python3 -m unittest discover -s viewshed-worker/tests -p test_link_worker_heartbeat.py -v
```

Results: **27 rules valid; all 14 rule scenario groups green; 5/5 publisher
lifecycle tests pass**. No live Redis or database was used.

From `backend`:

```sh
node --import tsx --test src/health/workerHeartbeat.test.ts
npm run typecheck
node --import tsx --test src/owner/ownerService.test.ts
node --import tsx ../.ukmesh-tools/alerts-prewarm-check.mjs
```

Results: **6/6 heartbeat tests; typecheck green; 4/4 owner service tests**.
The last command is an ignored, worktree-local additive smoke experiment using
mocked clocks and dependency functions: 11 active nodes, two windows, one
injected refresh failure, correct start/ten-node/completion counts, and no
further refresh after shutdown. It passed and contacted no services.

From `frontend`:

```sh
npm run build
node --import tsx --test src/serviceWorkerRuntime.test.ts
PLAYWRIGHT_PORT_BASE=4463 npx playwright test dashboard.spec.ts --project=dashboard-desktop --project=dashboard-mobile --grep 'RF coverage remains available with 3D terrain' --workers=1 --retries=0 --repeat-each=2 --trace=retain-on-failure --output=../.ukmesh-tools/alerts-terrain-final
PLAYWRIGHT_PORT_BASE=4463 PLAYWRIGHT_JSON_OUTPUT_FILE=../.ukmesh-tools/alerts-terrain-parallel-final.json npx playwright test dashboard.spec.ts --project=dashboard-desktop --project=dashboard-mobile --grep 'RF coverage remains available with 3D terrain' --workers=2 --retries=0 --repeat-each=3 --reporter=list,json --trace=retain-on-failure --output=../.ukmesh-tools/alerts-terrain-parallel-final
PLAYWRIGHT_PORT_BASE=4463 PLAYWRIGHT_JSON_OUTPUT_FILE=../.ukmesh-tools/alerts-dashboard-verified.json npx playwright test dashboard.spec.ts --project=dashboard-desktop --project=dashboard-mobile --workers=2 --retries=0 --reporter=list,json --trace=retain-on-failure --output=../.ukmesh-tools/alerts-dashboard-verified
```

Results: **production build green** (existing large-chunk advisory), **4/4
service-worker runtime tests green**, **4/4 single-worker terrain repeats
green**, and **6/6 final parallel terrain repeats green**, with no retries or
skips. The single-worker repeats and an initial **8/8** dashboard run used the
stricter intermediate 30-second readiness cap. Full parallel dashboard result
at the final 45-second cap: **8/8 green**, zero retries, skips or flaky outcomes.

The original control command used the same terrain grep, two workers,
`--retries=0 --trace=on --output=../.ukmesh-tools/alerts-terrain-baseline`;
**0/2 passed**, both reaching the 60-second timeout. Intermediate runs exposed
the service-worker bypass and the premature title wait before the final fix.
Private trace/smoke artifacts remain ignored under `.ukmesh-tools/`.

## Deliberately left out, open questions and remaining work

- No deploy, VPS access, live service restart, live data operation, migration,
  credential read, Hermes configuration change, agent spawning, branch push or
  PR publication occurred.
- Live alert frequency and the VPS's currently deployed heartbeat source are
  unverified under this lane's boundary. Ben can review/merge these commits and
  choose a release separately; rebaseline the live cadence after that release.
- The separate `HealthWorkerHeartbeatStale` snapshot proxy and unused worker
  freshness gauges retain their existing behavior. They need their own scope;
  this rule change addresses the link queue alert.
- Service-worker offline/cache behavior remains covered by its existing runtime
  tests, independently of the mocked dashboard suite. Live terrain tile serving
  and visual relief were not tested against production.
- Heartbeat freshness proves publisher availability, rather than workload
  completion; existing queue-age alerts continue to cover stalled work.
- No additional implementation work remains for #683, #695 or the logging
  stretch. Deployment and live acceptance remain outside this task.
