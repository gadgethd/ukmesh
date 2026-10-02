# UKMesh fix review — 2026-10-02

Branch: `fix/ukmesh-burn-20261002`. Base: `73ee004` (`ukmesh-w5`).
Scope: the UKMesh burn brief, including the stretch queue. All edits and local
verification stay in this worktree. No deployment, container restart, migration,
live-tree access, or environment change is authorized or performed.

## Baseline and evidence

- Read `knowledge.md`, `AI_MEMORY.md`, and the full supplied brief, and consulted
  OpenViking. Tokensave tools and `.tokensave/tokensave.db` are absent here, so
  structural investigation uses scoped source searches and Git objects.
- The initial worktree was clean. `git symbolic-ref refs/remotes/origin/HEAD`
  fails because the local symbolic ref is absent. `git ls-remote --symref origin
  HEAD` and `gh repo view gadgethd/ukmesh --json defaultBranchRef,nameWithOwner`
  both identify `main` (remote HEAD `74d3ff1`). The requested integration base
  remains `ukmesh-w5`; changing it would change the brief's scope.
- Dependencies were missing; `npm ci` in `backend` and `frontend` installed the
  committed lockfiles without changing them. Initial backend `npm test` after
  installation: **327/327 pass, 0 fail, 0 skipped**.
- The brief describes additional untracked live-checkout code. Neither
  `runState.test.ts` nor `ownerLastHopPrewarm.ts` exists on the initial branch.
  Git history contains the former and its full policy in `d834ba8`, and an
  earlier feed measurement implementation in `333f180`. No live checkout was
  inspected or copied.
- `push.md` is ignored but absent from this worktree and all Git history. An
  optional location question received no answer during the work; OpenViking
  supplies no copy. Its exact discipline cannot be verified. The authorized
  fallback is a clean, reviewed and tested private branch, a non-forced push to
  that branch only, and an exact remote-SHA check. No merge or deployment follows.
- Issue numbers in the brief are not assumed to be GitHub issue IDs:
  `gh issue view 438 --repo gadgethd/ukmesh` finds no such issue. The supplied
  brief is the requirements source.

## 1. Unit suite — #536 / #437 / #301

Decision: option (c), restore the complete intended retired-workload policy,
including the missing export and production callers, from the isolated prior
fix `d834ba8`. The initial `runState.ts` exactly matches that commit's parent.
This is not a dummy export to make a test load: migration 044 explicitly removed
the path-history cache, API and worker; `beginAnalysisRun` must reject a new
retired lease, and `getAnalysisWorkloadStates` must describe retirement while
preserving historical failures for audit. Supported workloads and unexpected
active retired runs continue to expose their actual status and errors.

Evidence: after restoring only the historical test and `workloadPolicy.ts`,
`cd backend && node --import tsx --test src/analysis/runState.test.ts` fails with
`SyntaxError: ... does not provide an export named 'normalizeRetiredAnalysisState'`.
After restoring the source policy wiring, the same command passes **3/3**, with
no database needed. `cd backend && npm test` now passes **330/330**, 0 fail,
0 skipped; `cd backend && npm run typecheck` passes. Final suite results appear
below.

## 2. Owner last-hop prewarm — #438

The named file is absent from the integration base. Recreated a prewarmer around
the existing `ownerService.getOwnerLastHopStrength` cache and wired it after
owner-auth initialization, with lifecycle shutdown draining. This branch exposes
one rolling seven-day result (`ownerRepository.fetchLastHopStrength`), not the
live brief's two separate windows; no speculative window API was added.

Each pass enumerates active, verified owner grants, deduplicates nodes per owner,
and refreshes at most two owner/node pairs concurrently. Optional
`OWNER_LAST_HOP_PREWARM_CONCURRENCY=1` selects sequential work; values above two
remain capped at two. The next pass starts 30 minutes after completion, so cold
passes cannot overlap. Start/end logs and every ten completed nodes report
owners/nodes completed and total, refreshed and failed. A refresh warns at 20s
while still in flight; failures are counted and do not stop the pass.

Forced warm refreshes retain incremental last-hop merging. Foreground reads and
warm refreshes share in-flight work. Cache keys include all owned nodes because
that exclusion set changes the last-hop query; warming a shared node for one
owner must not serve another owner's exclusion scope.

Validation: `cd backend && node --import tsx --test
src/owner/ownerLastHopPrewarm.test.ts src/owner/ownerService.test.ts`:
**8/8 pass**, covering the concurrency cap, progress/failure/owner accounting,
single-flight, deduplication, an in-flight slow failure, shutdown admission,
cache ownership scope and retry. `cd backend && npm run typecheck` passes;
`cd backend && npm test`: **338/338 pass**, 0 fail, 0 skipped.
Production cold-pass time and DB load are not measured.

Continuation verification adds three scheduler regressions: a long pass cannot
overlap and starts its delay only after completion, enumeration failures recover
on the next scheduled pass, and shutdown during enumeration admits no refresh.
`cd backend && node --import tsx --test src/owner/ownerLastHopPrewarm.test.ts
src/owner/ownerService.test.ts`: **11/11 pass**. No scheduler source change was
needed; mocked clocks exercise the real scheduling and shutdown paths.

## 3. Heartbeat alert semantics — #543 / #507

Confirmed `currentWorkers` set `worker="link"` from `MAX(itm_computed_at)`.
Replaced that source with Redis `meshcore:link:v3:worker_heartbeat`. The current
worker's independent thread writes Unix seconds every 10s with a **45s TTL**
(the brief's 39s is a live observation, not the source's configured TTL).
`last_activity_at` still reports ITM write activity separately.

The Prometheus gauge collects Redis on each scrape, rather than freezing the
age until the five-minute health pass. Reads time out at 2s; missing, invalid,
excessively future or unreadable timestamps publish `-1`, replacing any earlier
healthy value. The queue alert now handles `< 0` as well as `> 180`, with the
existing three-minute holdoff. No threshold increase is needed for a genuine
ten-second heartbeat. Empty queues do not trigger this alert.

Validation: `cd backend && node --import tsx --test
src/health/workerHeartbeat.test.ts`: **5/5 pass**, including the real Redis key,
timestamp validation, expiry/failure, bounded timeout, repeated real metric
scrapes and recovery. `npm run typecheck` passes; `npm test`: **343/343 pass**,
0 fail, 0 skipped. `.ukmesh-tools/promtool check rules
logging/rules/meshcore.yml`: **24 rules valid**. `.ukmesh-tools/promtool test rules
logging/rules/meshcore.test.yml`: **7 scenario groups pass**, including batch
gaps, missing heartbeats, holdoff, recovery, idle queues and existing stale cases.

Standalone promtool 3.15.0 was downloaded from the [official release listing](https://prometheus.io/download/?trk=direct)
into an ignored worktree-local tools directory, without starting a container.
Archive SHA256: `2a542df32eac02ee17b9d844fb2aa1de00dafa5476579ba8a3ba862e9d572ea0`,
matching the official listing. Live metric/alert behavior remains deploy-gated.

## 4. UK feed virtualizer — #303

Implemented option (a): natural border-box measurements from `ResizeObserver`,
identity-keyed height storage, prefix offsets and a binary visible-range lookup.
The previous `76px * index` arithmetic placed spacers according to an assumed
height even when metadata wrapped. The 76px estimate now applies only to unseen
rows; rendered rows use their actual fractional-pixel heights.

Stable packet/scope identities retain measurements across filtering and prepends.
Width changes invalidate offscreen measurements. Layout measurement and scroll
anchoring preserve the visible packet through observer enrichment and new
arrivals; a feed at the top remains pinned to live traffic. Both the desktop
internal scroller and mobile document scrolling are supported. Details remain
outside the packet list.

Validation: `cd frontend && npm test`: **99/99 pass**, including **2/2** new
offset/range tests. `cd frontend && PLAYWRIGHT_PORT_BASE=4273 npx playwright test
feed-virtualizer.spec.ts --project=public-desktop`: **2/2 pass** in Chromium,
at 390px and 1280px, checking variable heights, contiguous rows and spacers,
scroll-anchor preservation, enrichment, prepends, resizing, bottom reachability,
filter reset, empty-filter recovery, new arrivals pinned at the top and absence of
page errors. Test-only isolated ports avoid borrowing
other sessions' servers; default ports are unchanged. The in-app Browser runtime
could not connect to its trusted Node service, so these checks use the repository's
Playwright CLI. `cd frontend && npm run build` passes with the existing large-chunk
warning. The complete fixture-based E2E matrix passes **55/55**, with no retries;
production traffic is not tested.

`cd frontend && npm run lint:css` reports **6 pre-existing duplicate selectors**
in unchanged `globals.css`, `map-app.css` and `owner-portal.css`. Their diff against
`73ee004` is empty; this change edits no CSS files.

Continuation adds empty-filter/repopulation and top-pinned arrival assertions at
both widths. Each arrival has a distinct packet identity. Only the initial row
readiness wait increases from 5s to the existing application's 15s bootstrap
budget; row geometry and scroll-anchor assertions retain their previous limits.

Browser verification also uncovered incomplete test-server isolation: mobile and
accessibility tests still navigated to hardcoded ports 4173–4175 even when the
config selected another port range. They now share the config's origins. An
explicit `PLAYWRIGHT_PORT_BASE` refuses to borrow existing servers. The three
Vite test servers also receive separate dependency caches per port range and
site role through a test-only config; the production Vite config is unchanged.
Installed Vite source confirms optimization replaces the shared dependency cache;
concurrent replacement is a source-based explanation for the earlier dynamic
module fetch failure, rather than a proven production cause.

The first full run passed 53/55; both failures passed unchanged in isolation.
After cache isolation, a run passed 54/55 with the feed's 5s initial readiness
wait expiring before geometry assertions. The expanded feed tests and 15s
readiness wait then passed the full **55/55** matrix without retries. A subsequent
fresh-cache four-case check passed both feed widths and tablet topology but the
mobile map-mode test exceeded its 30s total budget. Its unchanged isolated trace
passed in 19.3s and shows two 4,600-node map bootstraps plus an accessibility scan.
That test now has a scoped 45s total budget; its functional assertions are
unchanged. The final fresh-cache check on ports 4303–4305 passes **4/4**, without
retries. These results establish local fixture behavior, not production performance
or the absence of every intermittent browser failure.

## Stretch queue

### #424 — Compose adoption guard

The replacement script previously trusted Compose's selected container without
checking its adoption metadata. A mocked backend with missing labels reached
replacement: before the guard, `TMPDIR="$PWD/.ukmesh-tools/tmp" bash
scripts/test-replace-container.sh` failed with `adoption_empty_stops_before_mutation:
expected Compose adoption rejection (65), got 0`. This reproduces the config gap
locally; it is not a claim about current live Docker state.

Added a read-only `scripts/check-compose-adoption.sh SERVICE` preflight. It requires
exactly one current container with matching project, service, physical working
directory and a config-file list containing this checkout's `docker-compose.yml`.
`replace-container.sh` checks the requested service and backend before signature
checks, pulls, receipts, migration runs or Compose replacement. Gaps stop with
exit 65 and an operator-reconciliation message. No automatic adoption or network
recreation is attempted. Operators can run the preflight separately when reviewing
a deployment; this session runs it only through mocked Docker.

Validation: `bash -n scripts/check-compose-adoption.sh scripts/replace-container.sh
scripts/test-replace-container.sh` passes. `TMPDIR="$PWD/.ukmesh-tools/tmp" bash
scripts/test-replace-container.sh`: **6/6 drills pass** (existing rollback and
compatibility, plus empty labels, missing config-file label, wrong directory and
wrong project). Gap cases assert no pull, migration/replacement or release receipt.
Live adoption and operator reconciliation remain unverified and deploy-gated.

Continuation expands this to **16/16 mocked drills**: wrong-service labels,
absent/multiple containers, null/malformed labels, overlay-only config rejection,
base-plus-overlay acceptance, and separate frontend/backend preflights. Every
adoption rejection also asserts that image signature verification has not begun.
A 10,000-service fixture reproduced an additional bug: `config --services |
grep -Fxq` under `pipefail` rejected an existing backend with exit 65 when the
producer hit SIGPIPE. The check now consumes the entire list with `grep -Fx`
redirected to `/dev/null`. The same large-list drill passes after the fix.

### #447 — Archive/delete inactive nodes of every role

The existing cleanup selected only `(role IS NULL OR role = 2)` and required an
old, non-null `last_mqtt_observer_seen_at`, so roles 1/3 and never-bridged nodes
could not enter its archive/delete path. Added a separate `cleanupInactiveNodes`
pass to the health worker's existing initial/six-hour maintenance schedule.
The original observer policy remains separate.

Inactive selection considers the latest node, MQTT, path, predicted-online and
creation timestamps, and retains nodes with recent network or observer sightings.
Source inspection found packet ingestion updates network sightings independently
of the source node's main timestamp; those rollups therefore also fence deletion.
The threshold stays bounded to 30–365 days. Test-network nodes, recent nodes and
records with wholly unknown age remain protected. No prior bridge or role filter
is required.

The existing transaction/advisory lock archives complete node and visibility
records into `maintenance_removed_records` before any delete, then removes the
two visibility sets and nodes together. Any failure rolls everything back.
Authentication and packet-history tables are outside the cleanup. No schema or
migration change is needed.

Validation: `cd backend && node --import tsx --test
src/maintenance/staleMqttObservers.test.ts`: **6/6 pass**. `cd backend && npm run
typecheck` passes. Supplementary isolated PostgreSQL 18.3 / PGlite 0.5.8 tests
execute the actual selection, advisory lock, archive and delete SQL against
minimal in-memory fixture tables. They verify roles 1/3, never-bridged nodes,
retention by each freshness source, test/unknown-age exclusions, archive readback,
preserved auth/history fixtures, and real transaction rollback on injected archive
and node-delete errors: **3/3 pass**, 0 skipped.

Reproduce without a server or repository dependency change, from the worktree root:

```sh
npm install --prefix .ukmesh-tools/pglite --no-package-lock --ignore-scripts @electric-sql/pglite@0.5.8
cd backend
TEST_NODE_CLEANUP_PGLITE_MODULE="file://$PWD/../.ukmesh-tools/pglite/node_modules/@electric-sql/pglite/dist/index.js" node --import tsx --test src/maintenance/staleMqttObservers.integration.test.ts
```

The optional fixture test is excluded by the normal unit command. It does not
apply project migrations or exercise TimescaleDB, the complete production trigger
chain, concurrent ingestion, production candidate counts or cleanup cost; those
remain unverified. The privacy function tested below is the sole production
trigger definition installed into the fixture.

Continuation found a real privacy interaction omitted by the initial fixture.
The current `sync_private_node_prefixes()` definition in migration 049 treats
deleting a private node as a privacy transition, removes its prefixes and makes
historical packets public. Isolated PostgreSQL tests execute that exact function
against fixture tables (no application migration or server) and reproduced
`is_private: true -> false`, `visibility_ok: false -> true` for both cleanup
policies. Both selectors now retain names containing the privacy marker **or**
identities present in `private_node_prefixes`, including mismatched name/index
states. Age does not revoke the user's privacy choice. Public inactive nodes of
all roles and bridge states still take the archive/delete path.

The SQL fixture now has **7/7 pass**, including the actual privacy trigger for
both policies, the exact 30-day boundary, idempotent repeat passes, archive
readback and transaction rollback. The optional integration command above is
unchanged; `cd backend && node --import tsx --test
src/maintenance/staleMqttObservers.test.ts` remains **6/6 pass**. Removing a
private identity while retaining its privacy decision requires a separate
tombstone design; no schema change is introduced in this constrained branch.

### Other heartbeat proxy signals — alert-semantics follow-up findings

Scoped audit command: `rg -n 'worker_heartbeat_age_seconds|worker_heartbeat_timestamp_seconds|setHeartbeatAge' backend/src viewshed-worker logging`.
`docker-compose.yml:811` mounts `logging/rules` into Prometheus's rules directory;
`logging/prometheus.yml:5` loads `/etc/prometheus/rules/meshcore.yml`. The edited rule is
therefore the repository's active Compose rule source; deployed mounts were not
inspected.

| Signal | Source and cadence | Alert consumer / finding |
| --- | --- | --- |
| `worker="link"` age | Previously `MAX(node_links.itm_computed_at)`; now the independent Redis heartbeat, collected per scrape. | `ActiveQueueWorkerHeartbeatStale` fixed in this branch; DB write activity remains separate. |
| `worker="health"` age | `currentWorkers` reads the previous `MAX(worker_health_snapshots.ts)` before `captureWorkerHealthSnapshot` writes the current snapshot (`backend/src/health/status.ts:378`, `:519`). `backend/src/workers/health.ts:9` schedules the next capture five minutes after completion. | `HealthWorkerHeartbeatStale` still uses `>180` or `<0` for 3m (`logging/rules/meshcore.yml:126`). A normal previous-snapshot age is about 300s and remains frozen between captures. **Source-based inference: another false-positive candidate.** Follow up with a genuine health-process heartbeat and producer/job scoping, plus healthy-cadence regression tests. No live firing frequency is claimed. |
| `worker="path_learning"` age | `MAX(path_model_calibration.updated_at)` from published calibration (`status.ts:377`, `path-learning/rebuild.ts:440`); default rebuild cadence one hour (`workers/path-learning.ts:7`). | No current rule consumes this label. It describes publication freshness, not process liveness; keep those concepts separate before adding alerts. |
| `worker="link_backfill"` age | `MAX(node_links.last_observed)` (`status.ts:385`), the newest observation. | No current rule consumes this label. Observation recency cannot prove a backfill process is alive or making progress. |
| Python `meshcore_worker_heartbeat_timestamp_seconds` | Set by `viewshed-worker/worker_metrics.py:58`. Link updates are on the main loop (`worker.py:2516`); the Redis thread is independent (`link_queue_v3.py:587`). Viewshed's independent thread also updates its exported heartbeat (`worker.py:2483`). | No current rule consumes the timestamp metric. It is unsuitable as a ten-second link-heartbeat substitute without changing its producer cadence. `WorkerMetricsDown` checks exporter `up`, which proves reachability rather than work-loop progress. |

Queue oldest-job age and backup/restore receipt age describe backlog/freshness
explicitly, rather than being renamed as liveness. The health-worker issue and
the two unused proxy labels are filed here for follow-up, as requested; this
branch changes only the link heartbeat semantics.

## Final verification and publication

Source commits: `9a5e838` (retirement), `2d2b425` (prewarm), `236e948`
(heartbeat), `1f2bc76` (feed), `4b30eb0` (Compose guard), `6dda582` (cleanup).
Continuation commits: `ab5d5b8` (cleanup privacy), `56bfcb6` (Compose pipefail),
`2f8f03e` (scheduler regressions), `bbb14a1` (E2E isolation), `fb981e0` (feed
filter/arrival regressions), `62d9a9f` (map-mode cold-start budget). Each change
stays on the private branch.
The final documentation commit records this audit and the validation below.
Full unit suites are rerun after it, before publication.

| Exact command | Result |
| --- | --- |
| `cd backend && npm test` | **350/350 pass**, 0 fail, 0 skipped. Expands to the brief's `node --import tsx --test $(find src -name '*.test.ts' ! -name '*.integration.test.ts' -print)`. |
| `cd frontend && npm test` | **99/99 pass**, 0 fail, 0 skipped. |
| `cd backend && npm run typecheck` | Pass. |
| `cd backend && npm run build` | Pass. |
| `cd frontend && npm run build` | TypeScript + Vite pass; existing chunk-size warning. |
| `cd backend && npm run contract:check` | Pass: **63 API + 11 operator routes** current. |
| `cd frontend && PLAYWRIGHT_PORT_BASE=4273 npx playwright test feed-virtualizer.spec.ts --project=public-desktop` | **2/2 pass**. |
| `cd frontend && PLAYWRIGHT_PORT_BASE=4273 npx playwright test --workers=2` | **55/55 pass** across public desktop, dashboard desktop/mobile and mobile regression projects; no retries. |
| Fresh-cache targeted browser command below | **4/4 pass** with fresh caches; no retries. |
| `.ukmesh-tools/promtool check rules logging/rules/meshcore.yml` | **24 rules valid**. |
| `.ukmesh-tools/promtool test rules logging/rules/meshcore.test.yml` | **7 scenario groups pass**. |
| `bash -n scripts/check-compose-adoption.sh scripts/replace-container.sh scripts/test-replace-container.sh` | Pass. |
| `TMPDIR="$PWD/.ukmesh-tools/tmp" bash scripts/test-replace-container.sh` | **16/16 mocked drills pass**. |
| Optional isolated cleanup integration command above | **7/7 pass**, 0 skipped. |
| `git diff --check 73ee004` | Pass; scoped review found no migration, schema, environment-file, lockfile or unrelated source edits. |
| `cd frontend && npm run lint:css` | Existing **6 duplicate-selector failures** in unchanged files; left out of scope. |

Fresh-cache command, from the worktree root:

```sh
cd frontend
PLAYWRIGHT_PORT_BASE=4303 npx playwright test feed-virtualizer.spec.ts public.spec.ts dashboard.spec.ts --project=public-desktop --project=dashboard-mobile --grep 'measured feed rows|tablet topology|map modes' --workers=2 --trace=retain-on-failure --output=../.ukmesh-tools/cold-final-results
```

Completion audit against the supplied brief:

| Requirement | Reviewable result and evidence |
| --- | --- |
| Retired-workload unit-suite repair | Historical policy and current callers restored; 3 focused tests and the complete backend suite pass. |
| Owner prewarm observability and bounded load | Progress every ten nodes, in-flight 20s warning, configurable cap of two, ownership-scoped single-flight and shutdown; 11 focused tests pass. Live cold-pass time/load remains unmeasured. |
| Link heartbeat source and alert semantics | Per-scrape genuine Redis heartbeat; missing/stale active queues covered by 5 source/metric tests and 7 Prometheus scenario groups. Live alert firing is not claimed. |
| Variable-height UK feed | ResizeObserver measurements and anchored offsets at both widths; 2 frontend geometry tests plus 2 expanded browser cases pass. |
| Compose adoption guard | Local failing mocked reproduction, pre-mutation rejection, 16 passing replacement drills, including the pipefail regression. No live Docker inspection. |
| Inactive-node archive/delete for every role | Role/bridge-independent selection, archive-before-delete transaction and private identity retention; 6 unit and 7 SQL fixture tests pass. No live deletion or migration. |
| Final verification and private-branch publication | Full backend/frontend units run on the final documentation HEAD; non-forced branch push and matching remote SHA required before handoff. |
| Other heartbeat proxies | Source/cadence/alert-consumer findings recorded above; the health-worker false-positive candidate remains a source-based follow-up. |

Publication command: `git push -u origin fix/ukmesh-burn-20261002`.
Verify with `git rev-parse HEAD` and `git ls-remote --heads origin
fix/ukmesh-burn-20261002`; the two full SHAs must match. Remote `ukmesh-w5`
remains `73ee00406e7cb0b7251220abbff73144615a8b90`. No force push is used.
The operator reviews against that integration base; opening a draft PR is optional.
Inspected workflow triggers: branch pushes run CI; `release.yml` requires a
published release or explicit dispatch. No deployment/release workflow is invoked.
Ignored local logs contain TAP/build/browser output, and ignored tooling holds
standalone promtool and PGlite. Neither is part of the shipped runtime.

## Deployment gate, limitations and open questions

The operator must separately review, merge and deploy code/rules. Local tests
cannot prove live DB load, cold-pass wall time, Redis freshness in production,
alert recovery or container adoption. This session will not perform those
operations. No migrations were run, no environment files were changed, and no
live checkout or other burn worktree was accessed. `push.md` remains unavailable;
the exact requested document-specific push discipline is not claimed.
