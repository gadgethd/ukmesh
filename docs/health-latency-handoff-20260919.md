# UKMesh health and latency handoff — 2026-09-19

Branch: `fix/health-latency-bundle`. Base: `1d86e043a515d3c2ca381660866ff03eb2833a0a` (includes the four locally deployed commits named in the brief).

All source changes are in this worktree. No deployments, restarts, session termination, production SQL writes/DDL, baseline replacement, ACL changes, or `.env` edits were performed. The dirty main checkout and other stacks were left alone.

**Open investigation:** the historical idle session cannot be attributed to an individual process from the retained evidence. It had already disappeared before the first inspection. Do not interpret the performance fix or this handoff as proof of an idle-transaction leak fix. The requested full query/application name or saved host socket/process mapping is still needed for that part of #271.

## #271 — MQTT node latency

**Cause.** The original aggregate runs the SQL function `meshcore_canonical_node_id(rx_node_id)` for every reception in the 24-hour packet window. This STABLE function performs a lookup in `node_identity_aliases`. The first EXPLAIN counted 162,458 input receptions; the packet aggregate alone took 6,883 ms of 7,088 ms overall, despite using the existing covering index. The identity-node view took approximately 192 ms. This is primarily repeated function/lookup work, not a missing packet index. The endpoint remained slow with no idle transactions present.

**Fix and files.** `backend/src/repositories/mqttNodes.ts` materializes counts by raw observer first, then canonicalizes that small result and sums aliases. `backend/src/api/routes/misc.ts` uses this exact SQL. The existing scope expansion, 24-hour count, 15-minute status window, privacy-name exclusion, uptime/time ordering, string count type and zero-packet response filter are preserved. Route tests cover production aliases, test isolation and zero-packet filtering.

**Evidence.** [Captured plans and queries](evidence/health-latency-20260919/) include the initial text plans, three paired JSON plans, and `mqtt-verification.json`:

| Trial | Original SELECT | Fixed SELECT |
| --- | ---: | ---: |
| 1 | 6,867.619 ms | 281.524 ms |
| 2 | 6,611.297 ms | 286.269 ms |
| 3 | 6,779.078 ms | 310.512 ms |

The before/after result comparison executes both queries in one statement/snapshot: 20 rows each, **zero differences**, including all selected columns. Median execution improved about 24×. The fixed SELECT was run directly against live data with a read-only connection; it is not deployed HTTP performance. The unchanged live endpoint initially took 7.710498 s; `live-http-before.txt` contains a later HTTP confirmation.

Exact reproduction from the worktree root:

```sh
python3 scripts/verify-health-latency.py mqtt
bash scripts/health-latency-readonly-psql.sh < .health-latency-local/explain-before.sql
bash scripts/health-latency-readonly-psql.sh < .health-latency-local/explain-after.sql
```

The second/third commands were used for the first text plans. Recreate their local input by prefixing the committed `mqtt-nodes-before.sql` / `mqtt-nodes-after.sql` with `EXPLAIN (ANALYZE, BUFFERS, VERBOSE, SETTINGS)`. The Python probe uses the actual exported application SQL for the fixed query and writes the paired plans. Credentials are obtained in memory from the backend container, never printed. PostgreSQL read-only and timeout options are enforced on these connections.

**Idle session ownership.** `idle-transaction.txt` records both `pg_stat_activity` and network membership. PID 3098497 is absent and there are no idle-in-transaction sessions. Docker IPAM identifies `172.30.0.128` as the **bridge gateway**, not any container's address. That identifies a host/gateway-facing connection, potentially NATed, not a unique service. Current backend and tagger addresses are `.156` and `.141`. Retained Timescale container logs since 2026-09-17 10:35Z had zero lines for PID 3098497. The similar tagger SELECT uses `autocommit = True` on both initial connection and reconnect; backend feed SELECTs use pool queries without a surrounding transaction. Those facts do not establish ownership of the vanished session. No speculative transaction fix was made and no session was killed.

Useful read-only capture if it recurs (keep full query text private until reviewed):

```sql
SELECT pid, client_addr, client_port, application_name, backend_start,
       xact_start, state_change, wait_event_type, wait_event
FROM pg_stat_activity
WHERE state = 'idle in transaction' OR pid = 3098497;
```

Correlate the client port with host `ss -tnp '( dport = :5432 )'` and the Docker network mapping while the connection is still present. An IP alone cannot recover a closed NATed session's originating process.

**Risks/deploy.** The daily packet scan still scales with traffic, but uses existing indexes and avoids per-packet identity lookups. No new index or migration is required. Deploy the backend image to activate the query. Historical idle-session attribution remains unverified.

## #302 — readiness and analysis

**Observed readiness cause differs from the brief's premise.** `/readyz` already returned 200 when this investigation began. The approved September 14b baseline has 49 grants (12 configured, 37 database); the current inventory has 53 (14 configured, 39 database). Its stored ACL generation is `f1b98f5d…`, whereas the current desired/rendered/applied generation is `da5f0eb7…`. Equal *current* generations do not mean equality with a historical baseline. Replaying that old baseline reproduces exactly the seven mismatches in the brief. The active September 18 11:44Z replacement already matches, without this branch being deployed. Automatically accepting those grant changes would remove an authorization safeguard.

**Comparison fix and files.** `ownerInventoryBaseline.ts` now compares grant multisets and object fields independently of array/property ordering (including locale-dependent grant order), while preserving duplicate detection and the original v1 checksum format. This corrects a real order-sensitivity defect; it is not presented as the cause of the observed historical drift. `index.ts` adds safe count/generation diagnostics on mismatch. Actual changed grants, counts, generations, ACL readback and errors still fail comparison. Existing readiness predicates and frontend healthchecks were not relaxed.

**Live verification.** `backend/src/tools/verifyOwnerBaseline.ts` captures only live read inputs, then runs this checkout's builder, checksum validation and comparison. It emits no owner identities or credentials. `owner-baseline-verification.json` shows the active baseline **ok=true, mismatches=[]**, and the exact count/generation deltas for every historical file. Tests cover reordering, duplicate grants, real generation drift, preservation of legacy verification methods and tamper detection.

```sh
cd backend
node --import tsx src/tools/verifyOwnerBaseline.ts --all-baselines
```

**Path-history/ukmesh and path-history/test: formally retired.** Migration 044 already removed `path_history_cache` and documents removal of its API/worker. There is no current path-history scheduler. The intended missing legacy score schema is defined in migrations 001/002; `ml_path_prefix_scores` now exists live, while `path_history_cache` is absent. Recreating the retired cache or reviving the old job would contradict the current architecture. The test job's empty selection is another stale record of that retired workload.

`backend/src/analysis/workloadPolicy.ts` explicitly registers retirement. `runState.ts` rejects new leases for it and reports idle historical rows as `unavailable/retired`, with their original failure and reason retained under `retirement.historicalState`. Unexpected active runs are not normalized away. Supported workloads, including failed/timed-out spam runs, keep their true status. No DB rows are deleted or rewritten. `analysis-verification.json` shows both scopes using the corrected mapping against live data. This also supplies the missing `normalizeRetiredAnalysisState` export requested by #301, with the previously untracked test's expectation and additional regression tests.

```sh
python3 scripts/verify-health-latency.py analysis
```

**Spam-analysis/public: repaired, still enabled.** The initial 24-hour sample had 252 complete and 33 timed-out runs (the later committed sample has 255/30 as the window advanced). Successful runs averaged about 3.67 seconds against a 5-second budget. A live read-only pipeline loaded 2,000 messages in 1.17 s and spent 2.23 s building incidents, only 0.52 s of which was origin SQL. Fuzzy text comparison was the main avoidable CPU work.

`spam/similarity.ts` uses a bounded edit-distance calculation. It retains exact scores that can affect the clustering decision and avoids calculating irrelevant larger distances. `spam/cluster.ts` derives the minimum eligible score from the existing thresholds and skips candidate sorting after an exact/signal match. Detection thresholds, candidate/message limits, deadlines, timeout reporting and publication fences remain unchanged.

The full-matrix reference test checks 6,724 message pairs at five thresholds, including URLs, Unicode, short and long messages. On the same live 2,000-message input, old/new incident JSON hashes were identical. The read-only load/build pipeline improved from **3,402 ms to 2,263 ms**; a confirmation was **2,397 ms**. Same-input clustering alone improved from 1,179 ms to 680 ms in that confirmation. See `spam-before.json`, `spam-after.json`, `spam-confirmation.json`.

```sh
HEALTH_AUDIT_COMPARE_SPAM=1 python3 scripts/verify-health-latency.py spam
```

The probe recreates baseline clustering source from the base commit inside the ignored worktree directory. It never acquires an analysis lease, publishes incidents or invokes webhooks. Its timing excludes persistence/lease overhead, so it is not a claim that every deployed run will finish within budget under arbitrary DB load.

**Risks/deploy.** Keep the currently approved inventory file and its existing configuration. Future intentional grant changes still need operator review and an updated approved baseline. Retired job status now has explicit metadata; consumers treating every historical failure as an active outage should use that metadata. Spam timeout handling remains visible. Deploy the backend; no schema migration or workload database cleanup is required.

## #303 — variable-height feed virtualization

**Cause.** Spacer and visible-range arithmetic assumed 76 px despite natural-height observer lines and asynchronously arriving tags. Mobile CSS also makes the *document* scroll, so list-only scroll tracking cannot work there.

**Fix/files.** `frontend/src/hooks/useMeasuredVirtualRows.ts` observes rendered article border boxes and container resizing with `ResizeObserver`. `measuredVirtualRows.ts` builds cumulative offsets and binary-searches the visible range. `UKFeedPage.tsx` adds measurement refs and stable packet keys, uses actual measured spacers, and leaves row content/styles intact. Unseen rows alone use the estimate. Width changes invalidate offscreen measurements, removed keys are pruned, and visible packet anchoring handles height changes and prepends. Both internal list scrolling and document scrolling are supported. Browser anchoring inside the virtualized list is disabled to avoid double compensation.

**Evidence.** Unit tests cover variable/fractional heights, offset boundaries, empty lists, filtering and reordered identities. `feed-virtualizer.spec.ts` passes at 390 px and 1280 px, checking tall wrapped observer rows, tag enrichment, scroll anchors, prepends, crossing the mobile breakpoint, reaching the final row and filtering down to one row. Both runs had zero page errors. Frontend build/typecheck and all 99 unit tests pass.

**Risks/deploy.** Requires `ResizeObserver` in the browser, consistent with the supported modern stack. Unseen rows use estimates until rendered; resizing invalidates their old measurements. Rebuild the website frontend and companion app frontend from the reviewed revision. No uniform-height fallback or row-rendering change was used.

## Stretch #276 and #301

`connectionMonitor.ts` treats access errors as a loss of optional audit observations; broker logs cannot grant ownership. It now warns once per changed error/outage, continues retries, and reports recovery. Poll-time access failures are also visible. The regression test simulates four denied attempts, recovery without restart, then a new outage. It verifies that suppression does not stop retries or suppress the next incident.

The recent-log sample requested 24 hours, but the current backend container's retained logs begin at September 18 11:44Z: about 22 h 35 m were available. In 48,405 log lines there was **one identity-mismatch rejection**, in the September 18 11Z hour, and **zero EACCES lines**. This is a count of logged rejections, not a rate per received envelope. Existing identity rejection enforcement is unchanged. See `recent-log-counts.json`.

#301 is included in the explicit path-history retirement/export/tests above. Another pre-existing build failure was found: `api/routes.ts` passed an unused `withTransaction` dependency not declared or consumed by `OwnerRouteDeps`. Removing that stale import/property makes backend build and typecheck pass; it does not change owner route transaction behavior.

## Validation and exact commands

Captured on Node v22.22.1 / npm 9.2.0. `validation.json` records results. Backend and frontend dependencies were installed with `npm ci --prefix backend` and `npm ci --prefix frontend`.

```sh
npm run typecheck --prefix backend   # pass
npm run build --prefix backend       # pass
npm test --prefix backend            # 328 passed, 0 failed/skipped
npm run build --prefix frontend      # tsc + Vite pass; existing large-chunk warning
npm test --prefix frontend           # 99 passed, 0 failed/skipped
cd frontend
npx playwright test feed-virtualizer.spec.ts --project=public-desktop  # 2 passed
```

For this host, the browser test used these environment variables so downloaded Chromium and missing OS libraries stayed inside the worktree:

```sh
cd frontend
TMPDIR="$PWD/../.health-latency-local/tmp" \
LD_LIBRARY_PATH="$PWD/../.health-latency-local/browser-libs/usr/lib/x86_64-linux-gnu" \
PLAYWRIGHT_BROWSERS_PATH="$PWD/../.health-latency-local/browsers" \
npx playwright test feed-virtualizer.spec.ts --project=public-desktop
```

No production integration tests or migration runner were invoked. All backend unit tests passed, including the previously broken build/import scope. Browser dependency launch failures were resolved locally before the passing browser run; no system packages were installed.

## Deploy day — Ben after review

1. Use the reviewed branch revision in a clean release checkout; preserve the unrelated dirty main WIP.
2. **Migrations/indexes: none for this bundle.** Do not re-create retired path-history tables or rewrite analysis history. Do not refresh the owner baseline just to clear a mismatch; the currently approved September 18 file already matches.
3. Build the backend image (`Dockerfile.backend`, `BACKEND_IMAGE`), UKMesh website image (`Dockerfile.website`, `WEBSITE_IMAGE`) and app image (`Dockerfile.app`, `APP_IMAGE`) from the same reviewed revision with their existing production build arguments.
4. Roll out only `backend`, `website-ukmesh`, and `app-ukmesh` using the normal reviewed deployment procedure. No infrastructure, tagger, path-learning/health worker, nemesh, sec-test or webflasher change is needed for this bundle.
5. Verify both frontend `/readyz` checks, live `/api/mqtt-nodes?network=northeast` timing, the owner verification tool, retired path-history metadata, and the next actual spam run's completion/budget. Check the feed with wrapped observers and tags on desktop/mobile. If an idle transaction reappears, capture client port/application/process evidence before it disappears; this bundle does not claim to fix an unidentified historical owner.

No deploy action in this list has been performed by this session.
