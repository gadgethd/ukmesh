# MeshCore Analytics Audit

Audit date: 2026-07-09

## Scope and Baseline

- Repository: `meshcore-analytics`, a Docker Compose deployment with a Node.js/
  TypeScript API and MQTT ingest path, React/Vite frontend, TimescaleDB, Redis,
  Mosquitto, Nginx, and Python coverage/link workers.
- Existing local modifications were present across the project before this
  audit. They were preserved; this audit adds focused operational changes only.
- The live Compose stack was healthy at audit start. The backend, frontends,
  TimescaleDB, Redis, Mosquitto, coverage/link workers, and health check were
  running.

## Findings and Actions

### Ingest and real-time delivery

- **Unbounded MQTT async work:** each broker message started an independent
  asynchronous handler. A replay or traffic burst could grow pending packet
  decoding, database writes, and memory without a limit.
  - Added a bounded MQTT queue with configurable payload, concurrency, and
    queue-size limits. Overload is rate-limited in logs and deliberately sheds
    QoS 0 messages rather than exhausting the API process.
- **Slow WebSocket consumers:** live-event batches had no byte limit and could
  accumulate indefinitely for a stalled browser.
  - Added maximum client input, queued-event, and socket-buffer limits. A slow
    client is terminated so it can reconnect with a fresh initial state.
- **Redundant viewshed jobs:** active adverts repeatedly queued nodes whose
  coverage already existed; live worker logs showed many skip-only jobs.
  - Added a Redis cooldown after successful enqueue. New nodes still queue, and
    explicit requests or meaningful coordinate changes bypass the cooldown.

### Database and maintenance

- **Migration deployment gap:** production services set
  `DATABASE_SKIP_SCHEMA_INIT=true`; the live migration ledger had not recorded
  migrations `003_spam_suspects.sql` and `006_stats_rollups.sql`.
  - Added the one-shot `db-migrate` Compose service and made backend startup
    wait for it. The runner applies only versioned migration files and does not
    run the base schema on every production boot.
- **Startup coupled to historical spam warmup:** the API listener waited for
  spam-detector cache refreshes, including several multi-day packet
  aggregations. A healthy deploy could therefore fail its HTTP health check for
  minutes before MQTT even connected.
  - Started HTTP/WebSocket serving before MQTT and spam-detector warmup. The
    MQTT client reports its own startup failure and reconnects independently,
    while the site remains available during historical cache refreshes.
- **Large retention delete:** the observer-region packet rollup had 1,239,787
  rows, including 846,043 older than the intended eight-day window. The prior
  cleanup was one unbounded delete and PostgreSQL planned it as a sequential
  scan.
  - Changed cleanup to bounded batches (25,000 rollup rows per health cycle)
    and made the health worker schedule its next cycle only after the current
    one completes. This prevents overlapping maintenance transactions and
    gradually removes the backlog without a long write lock.

### Frontend and delivery efficiency

- **Unnecessary dashboard polling:** the map fetched multibyte path history and
  seven-day inferred-node data every 10 seconds even when the Paths overlay was
  disabled; overlapping interval requests were also possible on a slow API.
  - Live packets and summary stats now use a non-overlapping 10-second poll.
    Inferred nodes refresh every 60 seconds, and path history is fetched only
    while the Paths overlay is enabled. Hidden tabs stop these polls and stale
    requests are aborted.
- **Initial JS cost:** route modules were statically imported. The baseline
  build emitted a 705 KB app chunk plus 751 KB DeckGL and 802 KB MapLibre
  chunks.
  - Lazy-loaded the dashboard and page routes. The new entry is 44 KB (16 KB
    gzip); MapLibre and DeckGL are now requested by the routes that render
    maps, rather than by every route at startup.
- **Docker build hygiene:** image builders used `npm install` and the repository
  had no Docker ignore file, so builds were non-reproducible and could include
  host dependencies or `.env` secrets in context.
  - Switched images to lockfile-based `npm ci` and added `.dockerignore` for
    secrets, node modules, build output, logs, and terrain data.

### Operational follow-up: 2026-07-09

#### Website loading

- **Deploy-wide cache invalidation:** Vite appended `Date.now()` to every
  already-content-hashed asset filename. Every deploy therefore forced a fresh
  download despite Nginx's one-year immutable asset cache policy.
  - Removed the timestamp suffix and retained Vite content hashes only. Assets
    now keep their URL across deployments when their content is unchanged.
- **Map code loaded for non-map work:** the feed detail panel and decoded-path
  statistics view statically imported MapLibre. Opening either route downloaded
  the map engine even if the visitor never opened a path map.
  - Both views now load MapLibre and its CSS only when a path map mounts, with
    cancellation-safe cleanup for route changes.
- **Public entry preloaded an unused Deck bundle:** a manually named Deck chunk
  captured Vite's preload helper, causing public pages to preload about 197 KB
  gzip of Deck despite the dashboard being route-lazy.
  - Isolated the helper in its own 0.6 KB gzip chunk and let Rollup keep Deck
    and MapLibre behind their actual lazy imports. The live public entry now
    preloads only React (about 45 KB gzip) and the helper; it does not preload
    Deck or MapLibre. The feed's MapLibre request remains dynamic.

#### Path resolution

- **Repeated immutable graph construction:** every beta solve rebuilt the same
  coordinate-filtered repeater list, path-hash index, and clash adjacency even
  though `loadContext` already caches source data for 15 minutes. Multi-observer
  and partial-suffix solving magnified this work in the path-history worker.
  - Cached those derived structures with the context and reused them when no
    node exclusions are requested. Excluded-node solves still rebuild their
    own structures, preserving candidate semantics and scoring.
- **Repeated predicted-online writes:** multi-observer resolution issued an
  awaited node-touch query for every observer result.
  - Collects resolved node IDs and performs one deduplicated update after all
    observer paths are computed.
- **Unbounded, failure-prone warmup queue:** MQTT path warmups shared the
  two-worker resolver pool with HTTP queries. Its pending queue was unbounded,
  and a worker crash could leave an in-flight caller unresolved.
  - Added a bounded background queue (128 jobs), interactive-request priority,
    and per-worker job tracking. Worker error/exit now rejects the affected
    job, replaces the worker, and immediately drains queued work. MQTT uses
    this best-effort background API, so bursts cannot monopolise HTTP path
    requests.

## Live Path And Worker Follow-Up: 2026-07-09

- Historical refreshes were treating seven-day inferred paths as current presence and logging every packet. They now suppress predicted-online writes and per-packet logs, and use only 2- and 3-byte path hashes.
- One-byte hashes accounted for 1,878,805 of 2,156,480 sampled seven-day path-bearing rows. Recent 2-byte and 3-byte observations were uniquely resolvable 77.01% and 93.32% of the time, respectively. The learning worker now trains only from strong paths and writes a prior only for globally unique endpoint hashes.
- Learning replacement previously used roughly 120,000-200,000 individual writes per model. It now uses 1,000-row JSON batches in one transaction, preventing partial models and reducing database round trips. The scheduler skips overlapping runs.
- Each multibyte link observation used to reload every positioned repeater and viable pair. Link workers now cache topology for 60 seconds, refresh on a missing physical-job endpoint, and only increment high-confidence multibyte evidence for exact endpoint matches.
- The database still receives both `northeast` and `ukmesh` packets, while request scoping maps the legacy label to `ukmesh`. The staged unification script rewrites millions of rows, so it was not applied without an explicit data-migration decision.

## Verification

- `backend`: `npm run typecheck` passed.
- `backend`: `npm test` passed (60 tests).
- `frontend`: `npm run build` passed. Vite still reports its upstream
  loaders.gl browser-external warning and notes that the on-demand map chunks
  exceed 500 KB; these chunks are now route-lazy by design.
- `docker compose config --quiet` passed before deployment.
- Follow-up backend `npm run typecheck` and frontend `npm run build` passed.
  The known Vite loaders.gl browser-external warning remains upstream and does
  not fail the build.
- A compiled worker-pool exercise verified foreground priority, the bounded
  warmup queue, and queued-job recovery after an intentional worker crash.
- The live `C385A225` multi-observer path response was structurally identical
  on a warm request after excluding only `computedAt`; live traffic added
  observers between the earlier and later snapshots, so the cold response was
  not used as a historical algorithm-quality comparison. The warm API response
  completed in about 11 ms. A separate multi-observer request after context
  warmup completed in about 0.63 seconds.

## Live Deployment Verification

- Built the affected Compose images with lockfile-based installs, then ran
  `docker compose run --rm db-migrate`. It applied
  `003_spam_suspects.sql` and `006_stats_rollups.sql`; the migration ledger now
  contains `001` through `006`.
- Recreated the managed `backend`, `health-worker`, `app-ukmesh`,
  `website-ukmesh`, and `website-dev` containers. The live backend and all
  frontend containers are healthy.
- Verified `GET /healthz`, stats, recent packets, inferred nodes, multibyte
  paths, coverage, app frontend, website frontend, and a WebSocket initial
  state containing 11,953 nodes. No recent backend/health-worker errors were
  present after the final restart.
- The observer-region retention backlog decreased from 846,043 to 671,701
  stale rows while verifying the deployed health worker, confirming that the
  batched cleanup is running.
- Rebuilt and recreated `backend`, `path-history-worker`, `app-ukmesh`,
  `website-ukmesh`, and `website-dev` for the follow-up. All reported healthy
  where a health check is configured; `GET /healthz` remained healthy.
- Confirmed live public HTML preloads only `react` and `vite-preload`; the feed
  chunk's MapLibre import is dynamic, and hashed assets return
  `Cache-Control: public, immutable` with a one-year max age.
- Rebuilt and recreated `backend`, `path-history-worker`,
  `path-learning-worker`, `viewshed-worker`, and `link-worker`. The backend
  health endpoint returned `200 OK`; all recreated services remained up.
- The first batched learning run completed for all live models using verified
  cohorts of 106,557 (`northeast`), 79,904 (`ukmesh`), and 98,821 (`all`)
  packets. Its calibration figures are not directly comparable to the older
  weak-label metric because the training cohort is intentionally stricter.
- `python3 -m py_compile viewshed-worker/worker.py`, backend type checking,
  focused `git diff --check`, and the existing 60 backend tests passed. A
  live link-worker topology load found 4,271 repeaters and 19,725 viable pairs.

## Follow-up Work

- Decide whether to finish the destructive network-label migration or introduce
  a non-destructive compatibility scope. The present `ukmesh` scope omits
  active `northeast`-labelled traffic even though callers are told the network
  is unified.
- Refresh or retire the legacy `ml_path_prefix_scores` data: all 84 live rows
  were last written on 2026-05-09 while the ML container remains disabled.
  Do not gate current resolver behavior on a new threshold until it has a
  contemporary held-out validation run.
- Measure one completed strong-data path-history cycle. If its two scopes do
  not finish inside the hourly interval, reduce the retained packet sample or
  batch packet-observation reads before raising resolver concurrency.

- Let batched retention catch up, then schedule a low-traffic `VACUUM (ANALYZE)`
  of `observer_region_packet_sightings`; reclaiming existing bloat requires an
  operational maintenance window if disk reclamation is needed.
- Redesign the legacy bulk `GET /api/coverage` response before it receives
  significant public traffic. It returned about 128 MB without compression in
  the live check, despite the current map using the per-node coverage endpoint.
  Tiled, paginated, or simplified geometry delivery would reduce this risk.
- Add load tests for MQTT bursts, WebSocket slow-consumer termination, and
  route-specific frontend loading. Existing automated tests focus on spam
  analysis and do not cover these operational paths.
- Consider a dedicated rollup writer/batch upsert if sustained ingest rates
  increase further. Raw packet storage remains authoritative, but current
  per-packet rollup writes should be profiled under expected peak traffic.
- Add deterministic pathing fixtures and an end-to-end benchmark for a full
  path-history cycle. The current live network changes continuously, so it is
  unsuitable for strict before/after resolution-quality comparisons.

## Final Packet Accuracy and Deployment-Safety Pass: 2026-07-09

### Packet identity, decoding, and scope isolation

- **The decoder display hash was only 32 bits.** It was being used as the
  persistence/deduplication identity, so unrelated packets can collide at
  ordinary analytical volumes.
  - Validly framed packets now use an uppercase SHA-256 identity over immutable
    header/payload bytes. Relay-path, route-type, and transport metadata are
    deliberately excluded, so separate receptions of the same transmission
    remain one packet while unrelated payloads do not share the small display
    hash.
- **Multibyte route parsing rewrote wire bytes before decoding.** The current
  decoder understands the native 2- and 3-byte path framing, so that rewrite
  created allocations and could mask malformed metadata.
  - The ingest path now decodes the original bytes, validates path count/size
    against the decoded packet, rejects malformed/truncated frames before they
    affect stored metadata, and rate-limits malformed-packet logging.
- **Test traffic could leak into production analytics/presence.** Topic parsing
  accepted unnormalised observer keys, some explicit test scopes excluded their
  normal topic prefix, and multibyte path evidence could match a public node.
  - MQTT topics, node IDs, test/public API filters, status metadata, and path
    evidence are now scope-aware. Historical `northeast` and `teesside` rows
    remain visible in the public `ukmesh` scope; `test` remains isolated.
- **Direct-route instructions were treated as observed relays.** In MeshCore
  Direct/TransportDirect frames, path entries are destinations yet to be
  traversed, not proof that a node was online.
  - Presence/link evidence now accepts only Flood and TransportFlood route
    types (0 and 1), and only unique 2- or 3-byte repeater prefixes.
- **RX/TX and spam evidence could be distorted.** Decoded payloads dropped the
  MQTT direction, while repeat receptions could inflate advert-spam evidence.
  - Stored decoded payloads retain a validated `rx`/`tx` direction and spam
    evidence counts distinct canonical packet identities.

### Ingest and database efficiency

- The MQTT handler has bounded concurrency, payload size, and queue length;
  overload is rate-limited in logs instead of creating unlimited detached work.
- Packet-related writes now await bounded derived work, preserve the raw packet
  if a derived rollup fails, and combine observer-region rollups where possible.
  Node updates without coordinate changes use one atomic UPSERT rather than a
  transaction and extra pool checkout.
- The per-network/day longest-hop rollup no longer rewrites the same row for
  lower or equal-hop packets. It retains the first packet reaching the daily
  maximum as a stable representative, avoiding needless hot-row locks and WAL.
- Dashboard aggregates no longer silently sample only 50,000 packet rows;
  returned summaries remain bounded, and signal medians now use PostgreSQL's
  exact `percentile_cont(0.5)` rather than averages labelled as medians.

### Migration and configuration reliability

- Redis clients now pass `REDIS_PASSWORD` separately from the URL, so reserved
  password characters cannot invalidate connection strings. All Redis-using
  workers receive the credential and database workers wait for `db-migrate`.
- Viewshed processing is opt-in by default rather than quietly scheduling a
  computationally expensive coverage profile on every deployment.
- Migration `007_planned_coverage_and_rollup_backfill.sql` is deliberately
  DDL-only and uses a five-second lock timeout. It adds the planned-coverage
  column and corrects new-row network defaults without holding table locks
  during an historical scan.
- Historical stats reconstruction moved to the explicit,
  idempotent `stats:backfill-rollups` maintenance tool. It uses independent
  day/24-hour slices, valid receiver IDs, normalized historical identities,
  and monotonic upserts. It must be run only in a low-traffic window; it does
  not run during startup or migration.

### Measured benchmark

The focused decoder benchmark uses representative native 2- and 3-byte path
fixtures and compares the prior compatibility rewrite with the new native decode
plus canonical identity. It is a microbenchmark, not an end-to-end MQTT/DB
throughput claim.

| Run | Result |
| --- | --- |
| Representative legacy compatibility rewrite | 151,139 packets/s (6.616 s / 1,000,000) |
| Representative native decode + canonical identity | 176,453 packets/s (5.667 s / 1,000,000) |
| Measured change | **+14.4% to +16.7%** across isolated Node v18.19.1 million-packet runs |

### Verification and live state

- `backend`: `npm run typecheck`, `npm test`, and `npm run build` passed; the
  final suite has **72 passing tests**.
- `frontend`: production build passed with Vite 6.4.3. The prior loaders.gl
  browser-shim warning is gone; route-lazy MapLibre/Deck chunks over 500 KB
  remain and are a follow-up bundle-splitting opportunity.
- Python worker compilation, Compose configuration validation, shell syntax
  validation, and a transactional temporary-table execution of migration 007
  passed.
- Built deployable `backend`, `db-migrate`, `app-ukmesh`, `website-ukmesh`, and
  `website-dev` images. The maintenance tool's container dry run passed.
- The first migration attempt hit its intentional five-second lock timeout and
  rolled back cleanly; a guarded retry then applied migration 007. The live
  migration ledger, `ukmesh` defaults, and `node_coverage.predicted_links`
  schema were verified afterwards.
- The already-managed live stack remains healthy: backend `/healthz` and
  `/api/health`, local frontends, and public UKMesh/app/test roots returned
  success; worker logs showed no recent fatal/timeout pattern.
- The mesh-health-check container's old `/healthz` probe returned an SPA
  fallback (HTTP 200 HTML), not a health signal. Compose now validates its
  JSON `/api/bootstrap` endpoint instead; that configuration takes effect on
  the next approved recreation of that container.

### Deployment decision and remaining work

The running backend/frontend containers were intentionally not recreated from
the freshly built images. The worktree contained a large set of pre-existing,
uncommitted changes spanning unrelated UI, service, and deleted-file work; a
blind Compose recreation would deploy that unknown aggregate. The service is
running and healthy, but that is not proof that the final uncommitted code is
live.

After the owner reviews/isolates the intended diff, deploy the selected images
in a maintenance window, for example:

```bash
docker compose up -d --build --force-recreate \
  backend path-learning-worker path-history-worker health-worker link-worker \
  app-ukmesh website-ukmesh website-dev mesh-health-check
```

If the optional coverage profile is enabled, recreate `viewshed-worker` through
the same approved deployment. Run the historical rollup reconstruction only
after profiling it during a low-traffic period:

```bash
docker compose run --rm --no-deps backend \
  node dist/tools/backfillStatsRollups.js --apply
```

The earlier read-only plan for the original 30-day daily rollup scan processed
about 8.35 million rows, read about 556,000 blocks, and took 22.8 seconds; that
is why it is no longer an automatic migration. Existing historical 8-character
packet IDs/collisions and legacy test-row contamination are intentionally not
rewritten automatically. Full-window analytics are more accurate but should be
profiled under peak load, and sustained MQTT/WebSocket load tests remain a
recommended follow-up.

Dependency refreshes updated compatible backend transitive packages, React
Router, Lodash, PostCSS, and Vite 6.4.3. After fresh installs, frontend
production dependencies have **zero** `npm audit --omit=dev` findings and the
backend has **two moderate** findings remaining. Those are inherited through
`dockerode` 4.x/UUID and require the Dockerode 5 major upgrade; it should be
handled with a focused Docker-socket compatibility test rather than applied
blindly. The new frontend packages expect Node 20; local Node 18 installation
emits engine warnings, while the project’s Node 20 Docker builds passed.

### Git handoff

- A cleanly separable dependency-only subset was committed and pushed as
  `64d9e43 chore(deps): patch vulnerable runtime dependencies`.
- The branch already has draft/open [PR #9](https://github.com/gadgethd/ukmesh/pull/9),
  stacked on the existing frontend branch, so no duplicate PR was created.
- The broader audit changes remain deliberately uncommitted: they are
  interleaved with a large pre-existing dirty worktree, including deployment
  configuration explicitly excluded from that PR. They need owner review and
  selective staging before any further commit or deployment.
