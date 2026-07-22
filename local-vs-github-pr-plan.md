# Local vs GitHub Change Review

> Repository: `meshcore-analytics` (GitHub remote `origin` → `https://github.com/gadgethd/ukmesh.git`)
> Local branch: `main` · Remote default branch: `main`
> Generated: 2026-06-18 · Source of truth: **local working tree on disk**

## Summary

The local checkout sits on `main` at the **same commit as `origin/main`** (`69acf7a Add beta path neighbor affinity scoring`, 0 ahead / 0 behind). Every difference between disk and GitHub is therefore **uncommitted working-tree change**, not unpushed commits. The delta is large and spans the whole stack:

- **~73 tracked files modified**, **11 tracked files deleted**, and **~24 new untracked files/directories** (≈ 4,100 insertions / 7,800 deletions in tracked files alone).
- Headline functional work:
  1. **Lazy path resolver rewritten** from greedy per-position selection to a **global Viterbi (max-product) decode**, plus a new shared scoring module, an accuracy harness, and beta-resolver multibyte/ML/anchor scoring upgrades.
  2. **Message-spam detection ("Spam Watch")** — a brand-new `backend/src/spam/` subsystem (clustering, sanitization, origin estimation, tests) with a public `/spam` page; plus the older **advert-spoof spam-suspect** detector finally landing in the tree.
  3. **Region-scope / transport-code decoding** for TransportFlood/TransportDirect packets (new DB columns, MQTT decode, stats breakdowns, a one-shot backfill, decoder bump `0.2.x → 0.3.0`).
  4. **Network-scope unification** (`ukmesh` now means `ukmesh + northeast`) and the **"disappearing nodes" fix** via `node_network_sightings`, plus several query-performance bounds.
  5. **Stats dashboard overhaul** (route types, transport codes, observer diversity, signal summary, path-decode trend; removed repeaters-per-day metrics).
  6. **Map rendering**: terrain-aware 3-D path elevation, a multibyte-path overlay, clash path lines, staleness tweaks.
  7. **Frontend restructure**: shared `LoadingIndicator`, CSS consolidated into `styles/globals.css`, dead pages/hooks/utils deleted, new Docs page, nav/SEO updates, an (unwired) Regions page.
  8. **Deployment/ops hardening**: docker-compose `restart: always` + log limits + healthchecks + infra port exposure + Redis password + disabled/removed services; nginx cache header; Anubis terrain-tile bypass; viewshed/ML worker changes.

Most of this is coherent, in-progress work that has been running on the VM but never committed. It needs to be **split into reviewable PRs and committed**, with particular care around a handful of files (`db/index.ts`, `mqtt/client.ts`, `api/routes.ts`, `docker-compose.yml`) that are touched by several unrelated concerns at once.

## Assumptions

- **Comparison baseline**: I compared the working tree against `HEAD`, which equals `origin/main` after `git fetch`. Because local is 0/0 vs the remote default branch, `git diff HEAD` + untracked files **is** the local-vs-GitHub delta. There was no need to diff against a divergent remote.
- **Default branch** confirmed via `git remote show origin` → `HEAD branch: main`. A second local branch `docs/harden-codebase-roadmap` exists on the remote too but is unrelated to this review.
- **Source of truth = disk.** Deletions on disk (e.g. `frontend/src/globals.css`, `AboutPage.tsx`) are treated as intentional removals to be reflected in PRs, not as accidental loss.
- **Files intentionally ignored** (generated / noise / secrets — excluded from PR scope):
  - `vacuum-compressed-chunks.log` (untracked runtime log; should be git-ignored, not committed).
  - `backend/package-lock.json` / `frontend/package-lock.json` — reviewed only for the dependency changes they encode; lockfile line-noise is not itemised.
  - Per `.gitignore`, project context files (`CLAUDE.md`, `AI_MEMORY.md`, `knowledge.md`, `push.md`, `multipath.md`), `.env`, `mosquitto/passwd|acl`, and the `teesside/` frontend tree are not part of this repo's committable surface.
- **`.env` is managed on the VM and never committed**; only `.env.example` (the documented template) is in scope.
- I did **not** build, typecheck, or run tests as part of this review — risk/test notes are based on reading the diffs.

### Commands used

```bash
git remote -v
git fetch origin
git remote show origin | grep -i 'HEAD branch'        # -> main
git rev-list --left-right --count origin/main...HEAD   # -> 0  0
git status --porcelain=v1 | sort
git diff --stat HEAD
git diff HEAD -- <path>           # per-file inspection
git diff HEAD --unified=0/1/2 -- <path>
git check-ignore vacuum-compressed-chunks.log          # -> not ignored
```

---

## Proposed PR Breakdown

> Several backend files (`db/index.ts`, `mqtt/client.ts`, `api/routes.ts`) contain hunks belonging to **multiple** PRs below. The plan assumes hunk-level staging (`git add -p`) when these are split. Where that is impractical, the affected PRs should be merged in the listed order to avoid conflicts.

### PR 1 — Network-scope unification + "disappearing nodes" & query-perf fix (backend foundation)
- **Goal / rationale**: Make `ukmesh` a logical mesh of `ukmesh + northeast`, stop nodes flip-flopping off the map because `nodes.network` is last-writer-wins, and bound several unbounded hypertable scans. Foundational: the new `networks.ts` is imported by many later PRs.
- **Files changed**:
  - `backend/src/networks.ts` *(new)* — `UKMESH_NETWORKS`, `MESH_RESOLVER_NETWORKS`, `networkMatchesScope`, `expandResolverScope`.
  - `backend/src/api/utils/networkFilters.ts`
  - `backend/src/db/index.ts` *(scope-clause hunks only: `buildScopePlaceholders` / `buildNodeScopeClause` / `buildPacketScopeClause`)*
  - `backend/src/ws/server.ts`
  - `backend/src/http/requestScope.ts`
  - `backend/src/health/status.ts` *(3-day bound on `latest_rx` / `active_rx`)*
- **Summary**: Scope params become arrays (`= ANY($n)`) when `ukmesh` is requested; node scope now also matches `node_network_sightings` within 30 days (instead of teesside-only); observer scope rewritten as an uncorrelated `IN (...)` subquery with a 7-day window; `app.ukmesh.com` host now resolves to `ukmesh` scope (was `all`); WS scope matching uses `networkMatchesScope`; health query bounded to 3 days.
- **Risk**: **Medium** — pure SQL/scope-semantics change with real query-plan implications; a regression silently changes which nodes/packets appear.
- **Test plan**: Compare node/packet counts for `?network=ukmesh`, `=northeast`, `=teesside`, `=all`, and `?observer=` before/after; confirm previously-disappearing boundary nodes stay visible; `EXPLAIN ANALYZE` the observer/node-scope queries to confirm the 7-day/30-day bounds keep plans inside recent chunks; verify health endpoint latency dropped.
- **Dependencies**: none (do first).

### PR 2 — Lazy path resolver: greedy → global Viterbi decode (+ beta resolver scoring upgrades)
- **Goal / rationale**: Replace greedy per-position relay selection with a global per-group Viterbi decode (the long-standing "make the lazy resolver the most accurate" goal), share scoring with the beta resolver, and add a measurable accuracy harness.
- **Files changed**:
  - `backend/src/path-shared/scoring.ts` *(new)* — single source of truth for prior key-formats and Viterbi weights.
  - `backend/src/path-lazy/lazyResolver.ts` — rewritten to Viterbi with emission/transition scoring, forward/backward marginals → `ambiguous` flag, synthetic "unresolved" candidate, `ABLATE_LEAKY_PRIORS` diagnostic.
  - `backend/src/path-lazy/lazyResolverLegacy.ts` *(new)* — verbatim greedy copy, harness-only.
  - `backend/src/path-lazy/evaluate.ts` *(new)* — gold-packet accuracy harness (degrades multibyte→1-byte, runs legacy vs Viterbi, stratifies by prior support).
  - `backend/src/path-beta/resolver.ts` — trusted-path pairs, ML 1-byte-prefix scores, direct-observer anchors, multibyte observation counts, trellis candidate cap; removed dead `clashPressure`/`corridorCheck`.
  - `backend/src/path-beta/fallback.ts`, `backend/src/path-beta/types.ts` — multibyte boost + `MlPrefixScore`/trusted-path context types.
  - `backend/src/path-learning/rebuild.ts` — drop unused destructured var (strictness).
  - `backend/src/workers/path-history.ts` — window 72h→168h, max hashes 5k→12k.
- **Summary**: Major resolver-accuracy work; shared weights prevent the two resolvers from drifting.
- **Risk**: **High** — core inference path; changes which relays are drawn on every path overlay and the path-history cache.
- **Test plan**: Run `npx tsx backend/src/path-lazy/evaluate.ts ukmesh` and confirm the "supported" (leakage-resistant) bucket accuracy ≥ legacy; spot-check `/api/path-lazy/resolve` and `/api/path-beta/*` for known packets; confirm `path-history-worker` still populates the cache within the larger window without timeouts.
- **Dependencies**: **PR 1** (`networks.ts` / `expandResolverScope`).

### PR 3 — Region-scope / transport-code decoding (ingest + schema + decoder bump)
- **Goal / rationale**: Decode the 4-byte transport codes on TransportFlood (routeType 0) / TransportDirect (3) packets, tag each packet with its matched region (`#Europe`, …), and persist it.
- **Files changed**:
  - `backend/src/db/migrations/004_region_scope.sql` *(new)* — `ALTER TABLE packets ADD transport_codes TEXT, region_scope TEXT`.
  - `backend/src/mqtt/client.ts` *(region-probe + transportCodes hunks)* — `REGION_PROBE_LIST` from `MESHCORE_REGION_NAMES`, `transportCodeMatchesRegion` probe, plumb `transportCodes`/`regionScope` into insert + WS payload; `handleMessage` now has a top-level `.catch`.
  - `backend/src/types/index.ts` — `transportCodes`, `regionScope`; corrected `routeType` enum comment.
  - `backend/src/db/index.ts` *(insertPacket transport_codes/region_scope columns)*
  - `backend/backfill-transport-codes.mjs` *(new)* — one-shot backfill for existing route 0/3 rows.
  - `backend/package.json` — `@michaelhart/meshcore-decoder ^0.2.7 → ^0.3.0` (provides `calcRegionKey` / `transportCodeMatchesRegion`).
- **Summary**: New optional packet metadata; everything degrades to defaults (`Europe,Global`) if env unset.
- **Risk**: **Medium** — decoder major-ish version bump and a hot-path change in `handleMessage`.
- **Test plan**: Apply migration 004 (additive); confirm new route 0/3 packets get `transport_codes`/`region_scope`; run the backfill on a copy and verify counts; smoke-test the decoder upgrade against a sample of stored `raw_hex`.
- **Dependencies**: independent, but **must land before PR 4's transport-codes stats chart** has data.

### PR 4 — Stats dashboard overhaul (backend + frontend)
- **Goal / rationale**: Replace low-value repeaters-per-day metrics with route-type, transport-code, observer-diversity, signal-quality, and path-decode-trend breakdowns.
- **Files changed**:
  - `backend/src/stats/statsRepository.ts`, `backend/src/stats/statsService.ts`
  - `frontend/src/pages/StatsPage.tsx` (≈ rewritten), `frontend/src/components/LiveStatsSection.tsx`
- **Summary**: New `ROUTE_LABELS`, `decodeTransportCodes`, and aggregate queries; removes `repeatersPerDay` / `activeRepeaters` / `staleRepeaters`.
- **Risk**: **Medium** — large frontend churn; any consumer of the removed summary fields breaks.
- **Test plan**: Load `/stats` for each scope, verify all new cards render and totals are sane; confirm nothing else reads the removed fields.
- **Dependencies**: **PR 3** (transport codes), **PR 9** (`LoadingIndicator`).

### PR 5 — Spam detection backend: Spam Watch (messages) + advert-spoof suspects
- **Goal / rationale**: Land the two spam subsystems. They share `routes/spam.ts`, `db/index.ts` spam functions, and `mqtt/client.ts`, so they're grouped (could be split into 5a advert-spoof / 5b message-incidents if the shared files are hunk-split).
- **Files changed**:
  - **Message-spam (new)**: `backend/src/spam/` (`analyzer.ts`, `cluster.ts`, `config.ts`, `normalize.ts`, `origin.ts`, `repository.ts`, `sanitize.ts`, `similarity.ts`, `spamResolver.ts`, `types.ts` + `*.test.ts`), `backend/src/db/migrations/005_spam_message_incidents.sql`, `backend/src/tools/recomputeSpamMessages.ts`, `backend/src/tools/reresolveSpamOrigins.ts`, `docs/spam-detection.md`, `.env.example` (Spam Watch block).
  - **Advert-spoof (new, older system)**: `backend/src/mqtt/spamDetector.ts`, `backend/src/db/migrations/003_spam_suspects.sql`, `backend/src/tools/recomputeSpamSuspects.ts`, `backend/src/db/index.ts` (`insertOrUpdateSpamSuspect`, `getSpamSuspects`, `getSpamSuspectSummary`, `getSpamPacketObservers`, `getSpamAllObservers`), advert-eval hunk in `backend/src/mqtt/client.ts` (suppress upsert/emit for `verdict==='spam'`).
  - **Shared wiring**: `backend/src/api/routes/spam.ts` *(new — serves `/spam/messages/*` and `/spam/suspects|observers|packet`)*, `backend/src/api/routes.ts` (`registerSpamRoutes`), `backend/src/index.ts` (`initSpamMessageAnalyzer`, `await startMqttClient()`), `backend/package.json` (`spam:*` + `test` scripts), `backend/tsconfig.json` (`noUnusedLocals/Parameters`, exclude `*.test.ts`).
- **Summary**: Public API serves only pre-sanitized incident JSON; raw text/coords/sender names stay local-only in the DB.
- **Risk**: **High** — adds work to the MQTT hot path, can suppress legitimate node upserts if a verdict is wrong, runs a periodic heavy analyzer, and makes `startMqttClient` awaited (startup-order change).
- **Test plan**: `npm test` (spam unit tests); apply migrations 003 + 005 (note: existing deploys used `DATABASE_SKIP_SCHEMA_INIT` + manual ALTERs); verify `/api/spam/messages/*` returns sanitized data only; confirm a legitimate advert still upserts its node (false-positive guard); confirm the analyzer's first pass doesn't block startup.
- **Dependencies**: **PR 1** (`networks.ts`); coordinate with **PR 3** on the shared `mqtt/client.ts` file.

### PR 6 — Private-node redaction
- **Goal / rationale**: Honour the `🚫` opt-out — replace name with "Private Node", deterministically fuzz coords (~500 m), strip identifying fields in API responses.
- **Files changed**: `backend/src/api/utils/privateNode.ts` *(new)*, `backend/src/api/routes/nodes.ts`.
- **Risk**: **Low–Medium** (privacy-positive; verify fuzz is deterministic and applied everywhere a node leaks).
- **Test plan**: Hit `/api/nodes` and the peer endpoint with a `🚫` node present; confirm name/coords/keys are redacted on every path.
- **Dependencies**: none.

### PR 7 — Owner portal: 24 h packet counts + LoRa distance sanity filter
- **Files changed**: `backend/src/owner/ownerRepository.ts`, `backend/src/owner/ownerService.ts`, `frontend/src/pages/OwnerPortalPage.tsx`.
- **Summary**: Adds `packetsSent24h` / `packetsReceived24h`; filters incoming peers with known coords beyond 150 km (LoRa can't reach that far → spoofed/bad coords).
- **Risk**: **Low**.
- **Test plan**: Owner dashboard shows correct 24 h counts; a peer >150 km away is dropped, peers without coords are kept.
- **Dependencies**: **PR 9** (`LoadingIndicator`).

### PR 8 — Map: terrain-aware 3-D path elevation + multibyte-path overlay + clash lines
- **Goal / rationale**: Render path/link lines at true terrain elevation when terrain is on, add a live "multibyte paths" overlay, and surface clash path lines.
- **Files changed**:
  - Frontend: `frontend/src/components/Map/DeckGLOverlay.tsx`, `MapLibreMap.tsx`, `LiveOverlayController.tsx`, `Map/types.ts` (`ClashPathLine`, `LatLonPosition`), `geojsonBuilders.ts` (client vs repeater staleness), `mapConfig.ts` (terrain exaggeration 2→3, DEM `minzoom: 5`), `store/overlayStore.ts` (`clashPathLines`), `hooks/usePacketPathOverlay.ts` (drive overlay off latest message, not packet), `frontend/src/App.tsx` (poll `/api/path-beta/multibyte-paths`, loading indicators, `links:false` sanitize).
  - Backend: `backend/src/api/routes/pathing.ts` (`GET /path-beta/multibyte-paths`), `backend/src/db/index.ts` (`getMultibytePathSegments`), `backend/src/api/routes.ts` (wire dep).
- **Risk**: **Medium** — deck.gl Z-coordinate math and a new backend aggregation query.
- **Test plan**: Toggle terrain and confirm lines lift onto the DEM with no flicker; verify `/api/path-beta/multibyte-paths` segments render and respect scope; confirm clash lines appear in clash mode.
- **Dependencies**: **PR 9** (`LoadingIndicator`); overlaps `api/routes.ts` with PR 5.

### PR 9 — Shared `LoadingIndicator` component + loading states (frontend foundation)
- **Goal / rationale**: One spinner component replacing ad-hoc "Loading…" strings across the app. Imported by ~11 files, so land early.
- **Files changed**: `frontend/src/components/LoadingIndicator.tsx` *(new)*, `frontend/src/styles/globals.css` *(loading-indicator + `app-refresh-status` styles)*, and call-site updates in `Map/NodePopupContent.tsx`, `UKCompanionPage.tsx`, `PacketDetailPanel.tsx`, `UKFeedPage.tsx`, `UKRepeaterSearchPage.tsx` (the App/StatsPage/SpamPage/OwnerPortal call-sites travel with their feature PRs).
- **Risk**: **Low**.
- **Test plan**: Visual check that spinners render in `block` / `overlay` / `inline` variants.
- **Dependencies**: shares `styles/globals.css` with **PR 11** (sequence them).

### PR 10 — Navigation / routing / SEO restructure + dead-page cleanup
- **Goal / rationale**: Add `/docs` & `/spam` routes, redirect retired routes (`/about`, `/mqtt`, `/regions`), refresh nav/footer + SEO/sitemap, and delete now-dead pages/hooks/utils/data.
- **Files changed**:
  - Modified: `frontend/src/main.tsx`, `frontend/src/config/seo.ts`, `frontend/src/pages/shared/SiteLayout.tsx`, `frontend/src/pages/ukmesh/UKLayout.tsx`, `UKHomePage.tsx`, `UKInstallPage.tsx`, `frontend/src/pages/OpenSourcePage.tsx`.
  - **Deleted**: `frontend/src/pages/AboutPage.tsx`, `HealthPage.tsx`, `MqttPage.tsx`, `pages/dev/DevMqttPage.tsx`, `pages/ukmesh/UKMqttPage.tsx`, `hooks/useLinks.ts`, `hooks/usePathLearningModel.ts`, `utils/betaLinks.ts`, `utils/betaPathing.ts`, `data/ni-ring.ts`, `data/uk-mainland.ts`.
- **Summary**: UK nav now surfaces Docs, drops Stats/Flasher from the top nav (Stats moved to footer), `/regions` redirects home (page intentionally not enabled — see PR 11).
- **Risk**: **Medium** — deletions; confirm no remaining imports of the removed modules.
- **Test plan**: `grep` for imports of each deleted file (expect none); click every nav/footer link; verify retired routes redirect; check generated sitemap.
- **Dependencies**: **PR 5** (`SpamPage`) and **PR 11** (`UKBestPracticePage`) provide the components these routes point at — land those page components first or together.

### PR 11 — New content pages: Docs (Best Practice) + Regions (Regions currently disabled)
- **Files changed**: `frontend/src/pages/ukmesh/UKBestPracticePage.tsx` *(new, wired at `/docs`)*, `frontend/src/pages/ukmesh/UKRegionsPage.tsx` *(new, **not** wired — `/regions` redirects home and the component is unimported)*, `frontend/src/pages/ukmesh/regionData.ts` *(new, only used by UKRegionsPage)*.
- **Risk**: **Low** (Docs) / **Low but incomplete** (Regions is dead code until routed).
- **Test plan**: `/docs` renders; decide whether to ship Regions now (wire it) or hold it out of this PR until ready.
- **Dependencies**: **PR 10** (routing/SEO/nav), **PR 9** (`LoadingIndicator` if used).

### PR 12 — Frontend deps / build / CSS consolidation
- **Goal / rationale**: Remove unused `@deck.gl/geo-layers`, consolidate styling into `styles/globals.css`, delete the old root stylesheet.
- **Files changed**: **Deleted** `frontend/src/globals.css` (3,788 lines); `frontend/src/styles/globals.css` (+927, gains the consolidated rules); `frontend/package.json` (drop `@deck.gl/geo-layers`); `frontend/vite.config.ts` (`manualChunks.deck` without geo-layers); `frontend/package-lock.json`.
- **Risk**: **Medium** — a CSS regression is easy to miss; confirm nothing imports the deleted `globals.css` and no code imports `@deck.gl/geo-layers` (it left with `utils/betaPathing.ts` in PR 10).
- **Test plan**: Full visual pass across pages; `grep` for `geo-layers` and `from './globals.css'`; production build succeeds and chunking is intact.
- **Dependencies**: **PR 9 / PR 11** (shared `styles/globals.css`) — merge after them or fold the loading/page styles in here.

### PR 13 — Deployment & ops hardening (compose / nginx / anubis / workers)
- **Goal / rationale**: Production-grade compose (auto-restart, bounded logs, healthchecks, ordered startup), expose infra ports on the VM, password Redis, retire dead services, harden workers.
- **Files changed**:
  - `docker-compose.yml` — `restart: always` + `json-log-limits` everywhere; healthchecks + `condition: service_healthy` deps; infra ports bound via `INFRA_BIND_ADDRESS` (loopback by default); `max_parallel_workers_per_gather=0`; **Redis now requires `REDIS_PASSWORD`**; **commented out `ml-path-learner`**; **removed `website-teesside`, `app-dev`, `website-dev`, `anubis-website-teesside`**; `cloudflared` `extra_hosts` + external `meshcore-beacon-ukmesh` network; consolidated Anubis cookie domains to `ukmesh.com`; `link-worker` radio-bot URL now opt-in (`LINK_WORKER_RADIO_BOT_URL`, empty default).
  - `nginx.website.conf` — `Cache-Control: no-cache…` on the SPA fallback.
  - `anubis/botPolicy.yaml` — allow `^/terrain-tiles/` unchallenged (so Cloudflare can cache tiles).
  - `viewshed-worker/Dockerfile` — pin GDAL to `ubuntu-full-3.12.2@sha256:…`.
  - `viewshed-worker/worker.py` — radio-bot sync optional, `already_calculated` gated on `elevation_m`, **neuter `backfill_elevations`** (reverse-radius estimate deemed unsafe), enqueue nodes missing elevation, `requests` import.
  - `ml-path-learner/worker.py` — `cleanup_training_artifacts` (generation-retention pruning of variant results + model artifacts).
- **Risk**: **High** — most operationally dangerous PR. Redis password + un-passworded `REDIS_URL` default is a live footgun (see Concerns); removing services and changing healthcheck gating can stall the whole stack.
- **Test plan**: Deploy to a staging stack: every service reaches `healthy`; backend/workers connect to Redis with the password; confirm intended services are gone and nothing references them; Cloudflare serves cached terrain tiles; viewshed worker re-queues elevation-less nodes; ML cleanup deletes only old non-active generations. Note the ML worker is **disabled in compose** even though its code changed — confirm that's intended.
- **Dependencies**: none code-wise, but should land **after** the app-level PRs are committed so a rollback of ops config doesn't strand new features.

### PR 14 — Repo hygiene: ignore runtime logs
- **Files changed**: `.gitignore` (add `vacuum-compressed-chunks.log` / `*.log`); do **not** commit `vacuum-compressed-chunks.log`.
- **Risk**: **Low**.
- **Dependencies**: none.

---

## Detailed File-by-File Differences

> PR column references the breakdown above. "M" = modified, "A" = added/untracked, "D" = deleted.

### Backend — core / infra
| File | Type | What changed | Why it matters | PR |
|---|---|---|---|---|
| `backend/src/networks.ts` | A | Scope constants + `expandResolverScope` / `networkMatchesScope` | Single definition of "what is the UK mesh"; imported widely | 1 |
| `backend/src/api/utils/networkFilters.ts` | M | `ukmesh → ANY([ukmesh,northeast])`, `node_network_sightings` 30 d, observer `IN(...)` 7 d | Fixes disappearing nodes + query perf | 1 |
| `backend/src/db/index.ts` | M | Scope clauses (PR1); `insertPacket` transport cols (PR3); spam suspect fns (PR5); `getMultibytePathSegments` (PR8); coord-recalc helpers/threshold | **Touched by 4 PRs** — hunk-split required | 1,3,5,8 |
| `backend/src/ws/server.ts` | M | `networkMatchesScope` in packet/message/node scope checks | WS respects combined ukmesh scope | 1 |
| `backend/src/http/requestScope.ts` | M | `app.ukmesh.com` → `ukmesh` (was `all`); `ForcedScope` type | Default site scope semantics | 1 |
| `backend/src/health/status.ts` | M | 3-day bound on rx scans | 20s+ query → fast | 1 |
| `backend/src/index.ts` | M | `await startMqttClient()`; init spam analyzer; re-cover nodes missing elevation | Startup ordering + spam + viewshed | 5 (with 3,8) |
| `backend/src/api/routes.ts` | M | Register spam + multibyte-paths deps; drop unused import | Route wiring | 5,8 |

### Backend — pathing
| File | Type | What changed | Why | PR |
|---|---|---|---|---|
| `backend/src/path-shared/scoring.ts` | A | Shared weights + key formats | De-dupes resolver scoring | 2 |
| `backend/src/path-lazy/lazyResolver.ts` | M | Greedy → Viterbi decode, marginals, ablation flag | Accuracy goal | 2 |
| `backend/src/path-lazy/lazyResolverLegacy.ts` | A | Frozen greedy copy | Harness comparison only | 2 |
| `backend/src/path-lazy/evaluate.ts` | A | Gold-packet accuracy harness | Measures Viterbi vs greedy | 2 |
| `backend/src/path-beta/resolver.ts` | M | Trusted-path pairs, ML prefix, direct anchors, multibyte; removed dead helpers | Beta path accuracy | 2 |
| `backend/src/path-beta/fallback.ts` | M | Multibyte observed boost | Scoring parity | 2 |
| `backend/src/path-beta/types.ts` | M | `MlPrefixScore`, trusted-path context | Types for above | 2 |
| `backend/src/path-learning/rebuild.ts` | M | Drop unused destructure | Strictness | 2 |
| `backend/src/workers/path-history.ts` | M | 72h→168h, 5k→12k hashes | Wider cache window | 2 |
| `backend/src/api/routes/pathing.ts` | M | `GET /path-beta/multibyte-paths` | Map overlay backend | 8 |

### Backend — region/transport
| File | Type | What | Why | PR |
|---|---|---|---|---|
| `backend/src/db/migrations/004_region_scope.sql` | A | Add `transport_codes`, `region_scope` | Persist new metadata | 3 |
| `backend/src/mqtt/client.ts` | M | Region probe + transportCodes (PR3); advert-spoof eval gating upsert (PR5); top-level catch | **Touched by 2 PRs** | 3,5 |
| `backend/src/types/index.ts` | M | `transportCodes`/`regionScope`; fixed routeType comment | Type surface | 3 |
| `backend/backfill-transport-codes.mjs` | A | One-shot backfill of route 0/3 rows | Migrate existing data | 3 |

### Backend — stats / owner / spam / privacy
| File | Type | What | Why | PR |
|---|---|---|---|---|
| `backend/src/stats/statsRepository.ts` | M | New aggregate queries; removed repeaters-per-day | Stats overhaul | 4 |
| `backend/src/stats/statsService.ts` | M | `ROUTE_LABELS`, `decodeTransportCodes`, new payload fields | Stats overhaul | 4 |
| `backend/src/owner/ownerRepository.ts` | M | 24 h sent/received counts | Owner metrics | 7 |
| `backend/src/owner/ownerService.ts` | M | LoRa >150 km peer filter; expose counts | Data sanity | 7 |
| `backend/src/spam/*` (16 files incl. tests) | A | Message-spam detection subsystem | Spam Watch | 5 |
| `backend/src/mqtt/spamDetector.ts` | A | Advert-spoof signal engine | Suspect detection | 5 |
| `backend/src/db/migrations/003_spam_suspects.sql` | A | `spam_suspects` table | Suspect storage | 5 |
| `backend/src/db/migrations/005_spam_message_incidents.sql` | A | Incidents + members tables | Spam Watch storage | 5 |
| `backend/src/tools/{recomputeSpamSuspects,recomputeSpamMessages,reresolveSpamOrigins}.ts` | A | Operational recompute CLIs | Backfill/recompute | 5 |
| `backend/src/api/routes/spam.ts` | A | `/spam/messages/*` + `/spam/suspects|observers|packet` | Public + suspect API | 5 |
| `docs/spam-detection.md` | A | Tuning/operator docs | Documentation | 5 |
| `backend/src/api/utils/privateNode.ts` | A | `isPrivateNode` / `redactPrivateNode` | Privacy opt-out | 6 |
| `backend/src/api/routes/nodes.ts` | M | Apply redaction to node + peer responses | Privacy | 6 |

### Backend — build/config
| File | Type | What | Why | PR |
|---|---|---|---|---|
| `backend/package.json` | M | decoder `^0.3.0` (PR3); `spam:*` + `test`/`typecheck` scripts (PR5) | Deps + scripts | 3,5 |
| `backend/tsconfig.json` | M | `noUnusedLocals/Parameters`, exclude `*.test.ts` | Strictness + tests | 5 |
| `backend/package-lock.json` | M | Lockfile for decoder bump | Deps | 3 |
| `.env.example` | M | Spam Watch env block | Config docs | 5 |

### Frontend
| File | Type | What | Why | PR |
|---|---|---|---|---|
| `frontend/src/components/LoadingIndicator.tsx` | A | Shared spinner | DRY loading UI | 9 |
| `frontend/src/components/Map/NodePopupContent.tsx` | M | Use `LoadingIndicator` | Loading UI | 9 |
| `frontend/src/pages/ukmesh/{UKCompanionPage,PacketDetailPanel,UKFeedPage,UKRepeaterSearchPage}.tsx` | M | Use `LoadingIndicator` (+ minor) | Loading UI | 9 |
| `frontend/src/App.tsx` | M | Poll `/path-beta/multibyte-paths`; loading overlays; `links:false` sanitize | Overlay + UX | 8 (with 10) |
| `frontend/src/components/Map/DeckGLOverlay.tsx` | M | Terrain Z-elevation; clash lines | 3-D paths | 8 |
| `frontend/src/components/Map/MapLibreMap.tsx` | M | Terrain source/zoom wiring | Terrain | 8 |
| `frontend/src/components/Map/LiveOverlayController.tsx` | M | Position-elevation map; clash lines; terrain flag | 3-D paths | 8 |
| `frontend/src/components/Map/types.ts` | M | `ClashPathLine`, `LatLonPosition` | Types | 8 |
| `frontend/src/components/Map/geojsonBuilders.ts` | M | Client vs repeater staleness window | Map accuracy | 8 |
| `frontend/src/components/Map/mapConfig.ts` | M | Terrain exaggeration 2→3, DEM minzoom 5 | Terrain look | 8 |
| `frontend/src/store/overlayStore.ts` | M | `clashPathLines` state | Overlay | 8 |
| `frontend/src/hooks/usePacketPathOverlay.ts` | M | Drive off latest message | Overlay correctness | 8 |
| `frontend/src/pages/StatsPage.tsx` | M | New stats cards | Stats overhaul | 4 |
| `frontend/src/components/LiveStatsSection.tsx` | M | Loading + stats tweaks | Stats overhaul | 4 |
| `frontend/src/pages/SpamTransparencyPage.tsx` | A | `/spam` Spam Watch page | Spam Watch UI | 5/10 |
| `frontend/src/pages/spam-page.css` | A | Spam page styles | Spam Watch UI | 5/10 |
| `frontend/src/pages/ukmesh/UKBestPracticePage.tsx` | A | `/docs` page | New content | 11 |
| `frontend/src/pages/ukmesh/UKRegionsPage.tsx` | A | Regions page (**unwired**) | Incomplete | 11 |
| `frontend/src/pages/ukmesh/regionData.ts` | A | Region dataset (only used by above) | Incomplete | 11 |
| `frontend/src/pages/OwnerPortalPage.tsx` | M | 24 h counts + loading | Owner UI | 7 |
| `frontend/src/main.tsx` | M | Route table (spam/docs/regions redirects) | Routing | 10 |
| `frontend/src/config/seo.ts` | M | SEO + sitemap for new routes | SEO | 10 |
| `frontend/src/pages/shared/SiteLayout.tsx` | M | Nav/footer; drop Flasher; add Docs/Regions/Stats | Nav | 10 |
| `frontend/src/pages/ukmesh/UKLayout.tsx` | M | Toggle nav flags | Nav | 10 |
| `frontend/src/pages/ukmesh/{UKHomePage,UKInstallPage}.tsx` | M | Minor nav/content | Nav | 10 |
| `frontend/src/pages/OpenSourcePage.tsx` | M | Remove hero section | Cleanup | 10 |
| `frontend/src/pages/{AboutPage,HealthPage,MqttPage}.tsx`, `pages/dev/DevMqttPage.tsx`, `pages/ukmesh/UKMqttPage.tsx` | D | Deleted dead pages | Cleanup | 10 |
| `frontend/src/hooks/{useLinks,usePathLearningModel}.ts`, `utils/{betaLinks,betaPathing}.ts`, `data/{ni-ring,uk-mainland}.ts` | D | Deleted dead hooks/utils/data | Cleanup | 10 |
| `frontend/src/globals.css` | D | Old 3,788-line stylesheet removed | CSS consolidation | 12 |
| `frontend/src/styles/globals.css` | M | +927 lines (loading, spam, regions, etc.) | CSS consolidation | 9/11/12 |
| `frontend/package.json` | M | Remove `@deck.gl/geo-layers` | Dep cleanup | 12 |
| `frontend/vite.config.ts` | M | `manualChunks.deck` without geo-layers | Build | 12 |
| `frontend/package-lock.json` | M | Lockfile | Deps | 12 |

### Ops / workers
| File | Type | What | Why | PR |
|---|---|---|---|---|
| `docker-compose.yml` | M | Restart/log/healthcheck/ports/Redis-pw; remove/disable services; cloudflared hosts | Ops hardening | 13 |
| `nginx.website.conf` | M | SPA `no-cache` header (also missing trailing newline) | Cache control | 13 |
| `anubis/botPolicy.yaml` | M | Allow `/terrain-tiles/` | CDN caching | 13 |
| `viewshed-worker/Dockerfile` | M | Pin GDAL by digest | Reproducible builds | 13 |
| `viewshed-worker/worker.py` | M | Optional radio bot; neuter radius-based elevation backfill; re-queue elevation-less | Correct elevation | 13 |
| `ml-path-learner/worker.py` | M | `cleanup_training_artifacts` retention pruning | DB bloat control | 13 |
| `vacuum-compressed-chunks.log` | A | Runtime log (should be ignored, not committed) | Hygiene | 14 |

---

## Potential Concerns

**Breaking / behavioural**
- **Redis password vs default URL (PR 13)**: compose now starts Redis with `--requirepass ${REDIS_PASSWORD}`, but the backend/worker default `REDIS_URL` is still `redis://redis:6379` (no auth). Unless `REDIS_URL` is set on the VM with the password embedded, every Redis client will fail auth. **Verify `.env` carries a passworded `REDIS_URL` before deploying.**
- **`ukmesh` scope semantics (PR 1)**: every node/packet/WS query that received `ukmesh` now returns `ukmesh + northeast`. This is intended, but any external consumer expecting the old narrow scope changes silently. `app.ukmesh.com` host scope flipped from `all` → `ukmesh`.
- **MQTT startup now awaited (PR 5)**: `await startMqttClient()` + `await initSpamDetector()` mean a slow spam-detector init delays broker connect. Confirm it can't hang ingestion.
- **Advert-spoof can suppress node upserts (PR 5)**: when `verdict === 'spam'` the node is not upserted/emitted. A false positive makes a legitimate node vanish. The detector is uncommitted/under-exercised — review thresholds.
- **Removed stats fields (PR 4)**: `repeatersPerDay`, `activeRepeaters`, `staleRepeaters` are gone from the API payload; ensure no other client/tool consumes them.

**Migrations**
- Migrations **003, 004, 005** are additive but must be applied in order. Existing deploys run with `DATABASE_SKIP_SCHEMA_INIT` and have historically been ALTERed by hand (003 even documents the manual `ALTER TABLE … ADD first_seen`). Confirm the live DB already matches these schemas or apply them deliberately; `backfill-transport-codes.mjs` should run **after** 004.

**Incomplete / unrelated**
- **Regions page is dead code (PR 11)**: `UKRegionsPage.tsx` + `regionData.ts` (≈ 1,770 lines) are added but `/regions` redirects home and the component is never imported. Decide: wire it up, or hold it back until it's ready.
- **ML worker contradiction (PR 13)**: `ml-path-learner/worker.py` gains real cleanup logic, yet the `ml-path-learner` service is **commented out** in compose. The code change is dead until the service is re-enabled — confirm intent.
- **Spam systems coupling**: the new `/spam` page only consumes `/spam/messages/*`; the advert-spoof `/spam/suspects|observers|packet` endpoints are served but unused by the new UI. Decide whether the advert-spoof system is still wanted or is legacy to retire.
- **`nginx.website.conf` / several `.tsx` / `seo.ts` / `main.tsx`** end **without a trailing newline** (`\ No newline at end of file`) — cosmetic, but worth normalising.

**Missing tests**
- Only the spam subsystem ships tests (`backend/src/spam/*.test.ts`). The **Viterbi resolver rewrite (PR 2)** — the highest-risk change — has only the read-only `evaluate.ts` harness, no committed assertions. Consider capturing a small golden-output regression test. Frontend changes (map elevation, stats) have no automated coverage.

**Security / privacy**
- **Infra ports exposed (PR 13)**: Postgres `5432`, Redis `6379`, and Mosquitto `9001` are published on `INFRA_BIND_ADDRESS`, which defaults to loopback. If a LAN interface is configured, confirm it is not internet-reachable and is firewalled to the LAN/VM mesh.
- Privacy redaction (PR 6) and the spam public-JSON sanitization (PR 5) are positive, but both must be verified to cover **every** code path that could leak a `🚫` node or raw spam text/coords.

**Dependency / version risk**
- `@michaelhart/meshcore-decoder ^0.2.7 → ^0.3.0` (PR 3) is a minor bump that the region-decode path depends on; regression-test decoding against stored `raw_hex`.
- Removing `@deck.gl/geo-layers` (PR 12) is only safe because its sole consumer (`utils/betaPathing.ts`) is deleted in PR 10 — **PR 12 must not merge before PR 10**.

**Files touched by multiple PRs (need `git add -p`)**
- `backend/src/db/index.ts` → PRs 1, 3, 5, 8.
- `backend/src/mqtt/client.ts` → PRs 3, 5.
- `backend/src/api/routes.ts` → PRs 5, 8.
- `frontend/src/styles/globals.css` → PRs 9, 11, 12.
- `backend/package.json` → PRs 3, 5.
If hunk-splitting is too fiddly, collapse the sharing PRs (e.g. land 1→3→5→8 in that order on the same file).

---

## Recommended Review Order

1. **PR 14 — ignore runtime logs** (trivial; stops `vacuum-compressed-chunks.log` sneaking into a later commit).
2. **PR 1 — network-scope foundation** (`networks.ts` is imported by 2 and 5; review the SQL carefully).
3. **PR 6 — private-node redaction** (small, self-contained, privacy win).
4. **PR 3 — region/transport decoding** (schema + decoder bump; unblocks stats data).
5. **PR 2 — lazy/beta resolver Viterbi** (highest-risk backend; review after the scope foundation it depends on).
6. **PR 5 — spam detection backend** (large; depends on PR 1, shares files with PR 3).
7. **PR 9 — `LoadingIndicator`** (frontend foundation for 4, 7, 8, 10).
8. **PR 8 — map overlay/elevation** (depends on PR 9; shares `routes.ts`/`db.ts` with 5/8 — land after 5).
9. **PR 4 — stats overhaul** (depends on PR 3 + PR 9).
10. **PR 7 — owner portal** (small; depends on PR 9).
11. **PR 10 — routing/SEO/dead-page cleanup** (needs the new page components from 5 + 11 to exist).
12. **PR 11 — Docs + Regions pages** (decide Regions' fate here; depends on PR 10).
13. **PR 12 — deps/CSS consolidation** (must follow PR 10 for the geo-layers removal; follow 9/11 for `styles/globals.css`).
14. **PR 13 — deployment/ops hardening** (last: highest blast radius; resolve the Redis-password and ML-worker questions before deploying).
