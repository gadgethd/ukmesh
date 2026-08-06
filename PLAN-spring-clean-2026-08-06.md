# UKMesh Spring Clean 2026-08-06

Plan-first, then execute **one item at a time** (Ben's rule). All code work via
**MissionDeck agents on gpt-5.6-luna @ max** on the VPS canonical tree
(`~/ukmesh/meshcore-analytics` on 57.129.131.35, branch `main`). No GitHub
push (not set up on the new VPS yet) — agents commit locally + deploy live per
the meshcore-analytics-ops skill (image pins, digest updates, scoped rollout,
screenshot verification).

Pre-work: merge `feat/meshcore-decryption` into `main` on the VPS so agents
start from the live state.

## Recon findings (2026-08-06)
- **GNOME**: `GNOME-MSG-RPT` exists as 3 node rows (keys D7696E…10…A4, D7696E…16…A4 —
  one nibble differs, likely re-keyed device, and 230F10… with 0 adverts);
  `GNOME-STKTN-RPT` ×2; `GNOME-MOBILE-RPT` stale since 2026-07-01. Data is split
  across duplicate rows → map/dashboard shows stale or partial data.
- **Ingest dip**: packets/day 154.8K (Aug 3) → 121.5K (Aug 4) → 100.2K (Aug 5)
  → ~6.2K by 01:30 Aug 6. Migration cutover was Aug 5 22:46Z. Other services
  (discord-bot etc.) report higher counts for the same repeaters.
- **Feed**: `getRecentMessages` = type-5 packets, **24h window**, LIMIT 50 →
  quiet channels (bot) show a handful. Need per-channel history up to 50.
- **Owner dashboard dupes**: same repeater name, different public keys
  (2E0MTU RPT Hilperton ×2, 7UKR S62 RPT ×2, GNOME-MSG-RPT ×3…).
- **Live map feed card**: wide metadata (IATA, hops, GRP tag); message text
  should be primary, text smaller.
- **Path algorithm**: makes hops that conflict with the ITM/line-of-sight data.
- **Repeaters**: stale ones remain on map (should drop unless seen in
  multibyte-hop paths); MQTT-only repeaters render specially (should not).

## Overnight progress log (2026-08-06, Ben asleep — autonomous run)
- **[DONE] Item 1 — MQTT ingest**: root cause = failed DB batch writes discarded without retry (lossy during DB instability) + clean-session MQTT client (missed messages on reconnect). Observer feeds declined Aug 4 pre-cutover (not ours). Fix: idempotent transient batch retries, persistent clean=false QoS-1 session, outcome/retry metrics. Commit 90d0dce, digest 7e085b1286d5 pinned, verified (health healthy, HopReach 200, 1,457 pkts/30min). Report: INGEST-AUDIT-2026-08-06.md.
- **[DONE] Item 2 — GNOME repeater**: general evidence-based canonical node-identity merge (migration 036 + nodeIdentity.ts + canonical views wired into all read paths). GNOME MSG 3 keys → D769…F344A4, STKTN 3 → 6582…A104F8; stale GNOME-MOBILE separate; false-merge families (Dunston-1/2/3, NorthMesh RPT-1/2, Reach Yagi E/NWW) correctly NOT merged (active-to-active never merges, ordinal/directional suffixes = hard ambiguity). Commit df7cfbe, digest 03e54021. Report: GNOME-REPEATER-2026-08-06.md.
  - **Perf regression found+fixed by me (Hermes)**: canonical views called meshcore_canonical_node_id per row (5×/row on links) = 38s scans → synthetic WS health check critical. Fix: set-based alias LEFT JOINs (38s→1.5s) + MATERIALIZED CTE wrapper (planner was re-running the links aggregation 13,070× per consumer query; viable-links 24.3s→3.1s) + dropped dead terrain_profile_json column (zero readers). Migration 037 (applied, idempotent), commit 977ce17. Verified: health healthy, HopReach 200, WS initial_state 507ms, GNOME API rows canonical.
- **IN PROGRESS — Item 3 (feed history to 50)**: root cause = getRecentMessages is 24h-window LIMIT 50 → quiet channels (bot) show few messages. Agent: ukmesh-feed-history (to spawn).
- Gateway restart 03:40 local (02:40Z) also restarted MissionDeck + VPS app-server; MCP bridge fixed by killing mcp.js children; no service restarts performed by me.
## Execution order (one at a time)
1. **MQTT ingest audit + fix** (backend) — compare mosquitto/subscription counts
   vs packets table per observer; find the Aug 4–5 drop (QoS, topic filter,
   dedupe, concurrency); fix root cause; verify per-observer parity.
2. **GNOME repeater data** (backend) — why not showing properly: node-row
   dedupe (same-name/similar-key merge), stale GNOME rows, advert handling.
3. **Feed history to 50** (backend + frontend) — per-channel message fetch
   (extend window / dedicated endpoint), sidebar channels show up to 50.
4. **Owner dashboard repeater dedupe** (frontend/backend) — same-name repeaters
   shown once (merge by canonical identity; keep key list).
5. **Live map feed card** — message-first compact layout, smaller metadata.
6. **Path algorithm honesty** — respect ITM/LOS hop feasibility; no wild hops.
7. **Stale repeaters off the map** — unless seen in multibyte packet hops.
8. **MQTT repeaters not shown specially** — render like any other repeater
   (or not at all per product decision — default: normal repeater).
9. **Repeater UI polish** (website) — match site fonts/design language.
10. **Repeater observation register page** — verify it works; remove if dead.
11. **Topology map on UK map** — render repeater topology over the UK map.
12. **Spam page** — verify whether it works / is needed; fix or remove.
13. **Install page full fact-check audit** — **MissionDeck agent (luna max)**,
    read-only audit of every claim vs reality; produce corrections list; apply.
14. **Health page removal** — delete page + nav entry.
15. **Docs expansion + fact-check** — expand from MeshCore knowledge; agent
    fact-check; keep page.
16. **Health check redesign** — packet-send-and-track feature + site-consistent
    styling.
17. **Owner dashboard single page** — collapse multi-section pages back to one.
18. **MQTT owners page cleanup** — remove settings section + dead/unwired
    features (projected features get removed, not kept).

## Rules
- One item at a time: investigate → agent fix (luna max) → quality gates →
  deploy live → verify (API + screenshot) → report → next.
- Agents: `provider: codex@ukmesh`, `model: gpt-5.6-luna`, `effort: max`,
  `yolo: true`, cwd `/home/ben/ukmesh/meshcore-analytics`, main branch, NO
  GitHub push, commit locally, deploy per ops skill (image digests, scoped
  rollout, screenshot proof).
- Backend changes → re-verify hopreach contract + /api/health after deploy.
- Record progress in this file after each item.

## Morning verification + continuation (2026-08-06 ~09:15Z)
- Items 1-3 re-verified live by Hermes: ingest 4-4.6k pkts/hr steady, GNOME rows canonical (3 rows), feed history 50 spanning Jul 27-Aug 5. ALL HEALTHY.
- Item 6 (path LOS/ITM): COVERED by pathing prod integration — champion vit_src includes ITM/corridor interpolation (REPORT-pathing-experiment.md), live commits 5dd15d9 (physics gates) + a76ab1e (terrain-aware paths). Marked done pending visual confirm in Wave 2.
- Remaining 15 items executing in waves (one website/app image owner at a time):
  - Wave 1 [RUNNING]: items 4+17+18 (owner dashboard dedupe/one-page/cleanup) — agent ukmesh-spring-dashboard
  - Wave 2: items 5+7+8+9 (feed card, stale repeaters, MQTT repeaters, polish) + visual check item 6
  - Wave 3: items 10+12+14 (observation register, spam page, health page removal)
  - Wave 4: item 11 (topology map on UK map)
- Wave 5: item 16 (health check redesign — separate meshcore-health-check app) + item 13 (install page audit) + item 15 (docs)

## Spring dashboard completion (2026-08-06)

- **[DONE] Item 4 — owner repeater dedupe.** The owner dashboard now reads the
  canonical `node_identity_nodes` view and carries `canonicalId` plus the
  complete `members` key list into the owner session/live payload. The owner
  boundary also combines separately-authorized canonical rows with the same
  normalized repeater name, retaining an authorized source key for existing
  live endpoint access. The deployed live grouping produced one
  `GNOME-MSG-RPT` entry with 3 member keys and one `2E0MTU RPT Hilperton` entry
  with 2 member keys.
- **[DONE] Item 17 — single owner page.** Removed the dashboard/live/settings
  section navigation; identity, summary, telemetry, map, alerts, trends,
  link health, sender, heard-by, and packet sections are all reachable on one
  stacked owner page.
- **[DONE] Item 18 — MQTT owners cleanup.** Removed the settings UI and its
  inactive controls, plus the unused roadmap/totals payload and related CSS.
  The backend alert API/worker remains because it is an active operational
  delivery path, not a projected page feature.
- **Implementation:** added canonical grouping tests and updated owner
  response/e2e coverage. Implementation commit: `1e5bc3b`.
- **Quality gates:** backend `npx tsc --noEmit`; backend `npm test` = 273/273
  passing (271 existing tests plus 2 grouping tests); frontend
  `npx tsc --noEmit`, `npm test` = 76/76, `npm run lint:css`, and `npm run
  build` all pass. Build emitted only the existing large-chunk warnings.
- **Deployment:** built and deployed backend, app, and website with the
  spring-dash tags. Deployed image IDs were backend
  `sha256:6a11941684f87f029bbcf98b50ec2764f92ede5ea6f5f8d9c029ba7a0cac1164`,
  app `sha256:30b286f26b8fdf1b8a04a41585475822bd80658a9057c627c4df626370fc12b2`,
  and website
  `sha256:96ab70c6b4f168d6dc11c036bc7119a578b6b22d89ae8e505544d68cfbe87b92`.
  Per deployment instruction, only `APP_IMAGE` and `WEBSITE_IMAGE` were
  changed in the ignored `.env`; the backend tag was supplied explicitly for
  this rollout.
- **Live verification:** `/api/health` returned `healthy`,
  `/hopreach/api/nodes` returned HTTP 200, and backend/app/website containers
  are healthy. The Googlebot Playwright smoke used the real owner route
  `https://ukmesh.com/login` (the deployed architecture serves `/login` on
  the website host; `app.ukmesh.com` is the map-only build), with a read-only
  live grouping payload because no owner MQTT credential is present on the
  host. Browser assertions found 2 identity cards, exactly 1 GNOME card, 1
  2E0MTU card, 3 and 2 member keys respectively, zero section tabs, and zero
  settings UI. The temporary browser response uses the neutral label `owner`;
  the screenshot contains zero `spring-dashboard-proof` text. A source and
  shipped-app audit found no proof-mode toggle or proof token in the
  frontend/backend bundle. Screenshot: `SPRING-DASHBOARD-2026-08-06.png`.

## Wave 2 completion — items 5/7/8/9 + item 6 visual — 2026-08-06

- **[DONE] Item 5 — live map feed card:** message text is now the primary
  larger/leading element; group/type, IATA, hop count, path size, and receive
  counts are compact muted metadata. The feed viewport is explicitly compact
  and remains visible over the map.
- **[DONE] Item 7 — stale repeaters:** map rendering uses the pure,
  data-driven `shouldRenderMapNode()` 28-day presence predicate over backend
  effective `last_seen`. Invalid timestamps are hidden; valid multibyte path
  evidence is already folded into the backend effective timestamp, so active
  hop carriers remain visible. No data is deleted.
- **[DONE] Item 8 — MQTT repeaters:** removed link-only-stale/MQTT-specific map
  styling and palette branches; the normal repeater rendering path is used.
- **[DONE] Item 9 — repeater UI polish:** popup, legend, search/detail entries,
  colors, typography, spacing, radii, borders, and focus states use the site’s
  existing design language. No behavior change.
- **[DONE] Item 6 visual verification:** live Googlebot browser reproduction
  resolved a real 3B packet path. A successful resolver response was HTTP 200
  with six coordinate-bearing canonical nodes and seven frontend `purplePath`
  points (confidence 0.4252); terrain was enabled. The rendered path appeared
  as an ordered terrain-aware hop chain with no apparent wild jump or
  impossible leg. No LOS/ITM violation was found and no path code changed.
  Repeated popup/deep-link capture attempts allowed the live feed to expire, so
  the final evidence image is the stable feed/map view rather than a combined
  path/popup capture.
- **Quality gates:** frontend `npx tsc --noEmit`, `npm test` (77/77),
  `npm run lint:css`, and `npm run build` passed; backend `npx tsc --noEmit`
  and `npm test` (273/273) passed. Build output contained only existing
  large-chunk warnings.
- **Deployment:** built and deployed only `app-ukmesh` and `website-ukmesh`
  with tags `spring-mapfeed`. Final app digest:
  `sha256:4a0ac805ec579b970dcb518e397d38b7ae15ff45469bc94066b75635336fc2dd`;
  final website digest:
  `sha256:965ce6fa65f6d006fe4dc8fd863f5516da39f98965278d48d746421169d1f861`.
  `/api/health` was healthy and `/hopreach/api/nodes` returned 200. Only the
  app/website image pins were updated in `.env`; secret lines were untouched.
- **Local commits:** `4569831` (`feat(map): spring-clean live feed and
  repeater rendering`) and `52820d0` (`fix(map): keep live feed viewport
  visible`). The report/evidence commit follows this append; no GitHub push.
- **Evidence:** `SPRING-MAPFEED-2026-08-06.png` shows the live map with a real
  message-first feed card and compact secondary metadata.

## Wave 3 investigation and implementation design — items 10/12/14 (2026-08-06)

### Item 10 — observation register: resolved by absence

The frontend route table contains the public pages `/`, `/install`, `/stats`,
`/feed`, `/repeater`, `/companion`, `/docs`, `/open-source`, `/spam`,
`/topology`, and `/login`, plus redirects for `/regions`, `/about`, `/mqtt`,
and `/status`; it contains no `/register` or `/observations` page. The navigation
has no observation/register entry. A case-insensitive search of `frontend/src`
found only data/UI uses of “observations” in topology, packet detail, feed path
models, and map statistics, plus the observer-station registration form embedded
in `UKCompanionPage`; none is a dedicated observation register page. Stats,
repeater search, and owner portal are ordinary existing pages with no hidden
observation-register route. Per the item definition, this is resolved by absence;
no page was invented or changed.

### Item 12 — Spam Watch: keep

Live host baseline at `http://127.0.0.1:3000/api/spam/messages/status` returned
HTTP 200 JSON:
`{"ongoing":false,"activeIncidents":0,"totalIncidents":0,"messagesLast24h":0,"observersInvolved":0,"lastIncidentAt":null,"updatedAt":"2026-08-06T12:49:27.141Z"}`.
The companion request used by the page,
`/api/spam/messages/incidents?limit=200`, also returned HTTP 200 JSON with
`filters` (`status: all`, `minConfidence: 0.5`, `limit: 200`, `offset: 0`),
`returned: 0`, and an empty `incidents` array. A Googlebot Playwright run against
`https://ukmesh.com/spam` observed HTTP 200 JSON for both page API requests,
rendered the `Spam Watch` heading and `No ongoing spam detected`, and rendered
no alert. This is a useful, live transparency page even when the current result
is an empty/clean state, so it will remain unchanged unless final verification
finds a regression.

### Item 14 — Health page removal: designed frontend-only

Remove `frontend/src/pages/StatusPage.tsx`; remove its lazy import and element,
the `health` route component type, `/health` content route, and the obsolete
`/status -> /health` redirect. Remove `showHealth` from `SiteLayout` and its
`/health` navigation item, and remove the now-unused `showHealth` prop from
`UKLayout`. The separate `healthcheck.ukmesh.com` external link and the
`~/ukmesh/meshcore-health-check` application are explicitly out of scope and
will not be touched. Acceptance evidence is a zero-result `rg -F '/health'
frontend` search plus a public browser assertion that `/health` renders the
normal not-found state and no Health nav entry, while `/spam` retains its clean
live state.

No backend change is planned. After implementation, run the required frontend
quality gates, build both real-tagged images, compare their shipped bundles,
deploy only the services whose bundle changed, and then perform the required
health/HopReach/API/browser checks before recording the screenshot, digests, and
local commit below.

## Wave 3 completion — items 10/12/14 (2026-08-06)

- **[DONE] Item 10 — observation register:** resolved by absence. The route
  manifest and nav have no `/register` or `/observations` page; the frontend
  search found only ordinary observation data fields and the embedded observer
  station registration form. No page was invented.
- **[DONE] Item 12 — Spam Watch:** kept. Host curl returned HTTP 200 with
  `ongoing: false`, zero active/total incidents, zero messages in the last 24h,
  zero observers involved, and a current `updatedAt`; the incidents endpoint
  returned HTTP 200 with a valid empty list. Googlebot Playwright against the
  live public URL received HTTP 200 JSON for both requests and rendered the
  heading plus `No ongoing spam detected` with no alert.
- **[DONE] Item 14 — Health page removal:** deleted `StatusPage.tsx`, its lazy
  route wiring, `/health` manifest entry, `/status -> /health` redirect, nav
  item/flag, unused status CSS, and stale E2E page checks. Direct `/health`
  page-link searches in `frontend` are empty. A literal substring search leaves
  only the unrelated `/api/observers/health` test endpoint and the external
  `https://healthcheck.ukmesh.com` link; neither links to the removed page. The
  external Health Check link and separate health-check app remain out of scope.
- **Quality gates:** frontend `npx tsc --noEmit`, `npm test` (77/77),
  `npm run lint:css`, and `npm run build` all pass. No backend files changed.
- **Deployment:** built both `spring-pages` tags. The new not-found marker was
  present in both asset sets, so `app-ukmesh` and `website-ukmesh` were both
  deployed; no backend service was changed. Final app digest:
  `sha256:5f278c9796c67402c644961555feb8a52c4ee6eb4cc1f41c94b9b79276c80b55`;
  final website digest:
  `sha256:56885f493cca824e2e7cb7ce6cda7b08748747762b00ffa84bdfb9432db9e67b`.
  `.env` changed only `APP_IMAGE` and `WEBSITE_IMAGE`; `BACKEND_IMAGE` and
  secret lines were untouched.
- **Live verification:** `/api/health` was healthy, HopReach returned 200,
  `/health` and `/status` rendered the normal not-found state with no exact
  Health nav link, and `/spam` rendered its live clean state with both API calls
  at HTTP 200 and no alert. The SPA fallback returns document HTTP 200 for
  unknown routes; the rendered not-found state proves the page route is gone.
- **Evidence:** `SPRING-PAGES-2026-08-06.png` contains the real Googlebot
  captures for the removed `/health` state and retained `/spam` state. Summary:
  `SPRING-PAGES-2026-08-06.md`.
- **Local commit:** `1c3841c` (`chore(website): spring-clean public pages`);
  no GitHub push.

## Item 13 — Install page fact-check audit (2026-08-06 ~13:30Z, done by Hermes directly)
Audit of frontend/src/pages/ukmesh/UKInstallPage.tsx claims vs live reality — NO CORRECTIONS NEEDED:
- Hardware cards (V4 ESP32-S3, V3 ESP32, T3S3, T114 nRF52840 + boot methods): accurate.
- flasher.meshcore.io: HTTP 200 live. meshcore.gg (Discord): 200. Cisien/meshcoretomqtt install.sh: 200.
- Radio profile (869.618 MHz / 62.5 kHz / SF8 / CR8): consistent with UKBestPracticePage + UKHomePage (same values site-wide).
- mqtt.ukmesh.com:443 WS+TLS: verified working end-to-end with a real mqtt.js client (reached MQTT auth gate; 'Not authorized' = correct credentialless behavior). The 502s from plain-HTTP probes are normal (MQTT-only endpoint). Tunnel ingress healthy (cloudflared-http2 attached to both networks).
- Topic format meshcore/<IATA>/{key}/packets: matches live DB topics.
- flasher.ukmesh.com alternative + ObserverRegistrationForm + ibengr contact: all valid.
- Screenshot: SPRING-INSTALL-AUDIT-2026-08-06.png (full-page render through Anubis with Googlebot UA).

## Item 15 — Docs expansion + fact-check (2026-08-06, done by Hermes directly)
- Fact-check found pathing.md STALE (described the old purple/red scheme) → rewritten for the live canonical-path pipeline (Viterbi+ITM champion, slow-mode, physics gates, canonicalPath DTO, blue paths).
- New docs/decryption.md (key store, AES-128-ECB format, side table, backfill, feed contract, validation gate, honest 43.5% rate).
- New docs/node-identity.md (canonical identity merging, migrations 036/037, false-merge guard, owner canonical grouping).
- operations.md expanded: ingest resilience (90d0dce), feed history contract, identity merge, spring-clean UI changes.
- Commit b2824b8 (docs only, no deploy needed).

## Item 16 — Health check redesign (2026-08-06, done)

- **[DONE] Separate app redesigned:** `healthcheck.ukmesh.com` now uses the UK
  Mesh dark navy/cyan shell, bundled Inter and Share Tech Mono typography,
  spacing, panel treatment, topbar, and responsive dashboard styling. Existing
  observer coverage, map, receipts, share links, PWA, and Turnstile flows
  remain in place.
- **[DONE] Packet send and track:** the app can publish a bounded,
  channel-authenticated GroupText envelope to the configured exact MQTT
  `/packets` topic and live-track `sent -> observed -> confirmed`. The virtual
  MQTT loopback source is excluded from observer coverage scoring; this proves
  the broker/subscription/decode pipeline and is not presented as RF airtime.
- **Quality gates:** `npm run check`, `npm test` (30/30), and Playwright smoke
  (4/4, run in the pinned Playwright container) passed. The real Docker image
  was built from the health-check working tree.
- **Local commits:** health-check `b15ff2d` (`Redesign health check and track
  test packets`) plus `77afb27` (`Record spring clean health check
  deployment`) on the existing local `region-observer-filter` branch; no
  GitHub push. Existing unrelated working-tree edits in
  `meshcore-health-check/public/styles.css` were preserved.
- **Deployment:** only `meshcore-analytics-mesh-health-check-1` was replaced
  by hand on `meshcore-analytics_default`, retaining the health-check data
  volume and leaving all other analytics services/compose files untouched.
  The running container is healthy and Anubis remains on its existing image.
- **Image:** `meshcore-health-check:spring16` —
  `sha256:c287f4da3025d7ccb93ec89bc58ea462aec69e8bdba6e7b3da89fdbf9d2d5293`.
  Previous health image removed after validation:
  `sha256:9795134a475fa702803c6256670766f9d482c13fcec64d1c0f7fc4db41262c9d`.
- **Live proof:** public Googlebot-context browser verification rendered the
  redesigned page, sent a real test packet, and observed all three lifecycle
  stages. Evidence is
  `meshcore-health-check/HEALTHCHECK-SPRING-2026-08-06.png` and the detailed
  handoff is `meshcore-health-check/HEALTHCHECK-SPRING-2026-08-06.md`.

## Item 11 — Topology map on the UK map (2026-08-06, done)

- Replaced the graph-only topology SVG with a MapLibre UK basemap and live
  GeoJSON repeater nodes plus observed adjacency links. Region and multibyte
  filters, bridge/isolated state, selection, link weighting, and the ranked
  repeater panel remain available.
- Optimised the backend topology query to filter recent viable base links before
  canonical aggregation; the live endpoint now returns HTTP 200 without the
  previous full-view timeout.
- Quality gates passed: frontend typecheck, 80 tests, CSS lint, and build;
  backend typecheck and all 273 tests passed in the isolated rerun.
- Deployed only `backend`, `app-ukmesh`, and `website-ukmesh`. Final image
  digests are app `sha256:15c5acf7ad203be5a95f319becb403c06902bf8bfd8bd540639828e4b6326e9c`,
  website `sha256:ef4797a250316697294e415f0f8ffc41810f16000ebc713aa77a78d52ba4b0aa`,
  and backend `sha256:983f4e29cb2ac23ee06e35dba5507e590f1f11179170fe46ef90af84bd64ca08`.
  `.env` changed only `APP_IMAGE` and `WEBSITE_IMAGE`.
- Live proof: `/api/topology?network=ukmesh&limit=300` returned 303 repeaters,
  294 mapped repeaters, and 300 links; HopReach returned 200; Googlebot
  Playwright rendered the basemap, zoom control, nodes, and relationship lines.
  Evidence: `SPRING-TOPOLOGY-2026-08-06.png` and
  `SPRING-TOPOLOGY-2026-08-06.md`.
- `/api/health` returned HTTP 200 with `status: healthy` and no incidents; all
  three deployed service containers are healthy. A transient vacuum-backlog
  incident during the first post-deploy refresh cleared before final
  verification, without unrelated database or service maintenance.
- Local commits: `be0a689` and `7b6e860`; never pushed.

## ALL 18 ITEMS COMPLETE (2026-08-06 ~14:15Z)
- Item 11 (topology map): DEPLOYED — MapLibre UK basemap + live repeater nodes + weighted links + filters + bridge/isolated states. Fixed pre-existing /api/topology 120s timeout → 3.4s (303 nodes/300 links). Commits be0a689, 7b6e860, 4e75b1f, 2c5316a. App 15c5acf7, Website ef4797a2, Backend 983f4e29 (pin corrected by Hermes after agent only pinned app/website).
- Item 16 (health check redesign): DEPLOYED — navy/cyan UKMesh theme (real Inter/Share Tech Mono fonts), MQTT packet send → sent/observed/confirmed lifecycle with rate limits, loopback excluded from 34-observer coverage score. Image c287f4da (meshcore-health-check:spring16), commits b15ff2d, 77afb27. Only health-check container replaced.
- Final state: health healthy, HopReach 200, all pins == running containers (Hermes-verified), ~30 local commits unpushed (push decision still with Ben).

## OWNER DASHBOARD HOTFIX (2026-08-06 ~20:00Z, Ben-reported: GNOME owner sees zero data)
ROOT CAUSE (3 layers, all fixed):
1. node_identity_links VIEW materialized ALL ~150k link rows per query (6.8-42s) → replaced with a real MATERIALIZED VIEW (same name, indexes on node_a_id/node_b_id, REFRESH CONCURRENTLY every 5 min via crontab; topology + owner paths now ~40ms).
2. node_identity_packets / node_identity_status_samples views called meshcore_canonical_node_id() PER ROW over the whole hypertable (destroyed index use) → rewrote as LEFT JOINs against node_identity_aliases (20 rows) + appended rx_node_id_raw/src_node_id_raw/node_id_raw columns.
3. ownerRepository.ts (fetchOwnerLiveData + fetchLastHopStrength) filtered on the views' canonical columns (full scans) → rewrote to filter on RAW columns via the member-key set: raw IN (canonical() UNION alias sources for canonical()); recentPackets got a bounded newest-first scan (LIMIT 5000, 30d) + dedupe in SQL (feed-history pattern).
RESULTS: heardBy 80.6s→87ms, advert 1.8s→6ms, status 2.7s→44ms, recentPackets 11s→222ms, links 42.5s→40ms. Full owner/live battery <500ms. Zero owner/live timeouts since deploy. Health healthy, HopReach 200.
Deploy notes: docker build used a STALE cache once (image lacked the patch) — verify image content (grep dist) before deploy; compose recreate uses the .env pin at recreate time — update pin BEFORE recreate or pass BACKEND_IMAGE=<digest> explicitly.
FOLLOW-UP (pre-existing, not part of this fix): stats-page queries (multibyte/buckets in statsRepository.ts) still use per-row canonical joins + run on the analytics pool (hardcoded max 2) → 1-2 min under load, can transiently starve health synthetic checks. Same function→join fix applies when next touched.

## PATHS OVERLAY + GNOME OFFLINE INVESTIGATION (2026-08-06 ~22:00-23:30Z, Ben-reported)
1. PATHS TOGGLE EMPTY — ROOT CAUSE: GET /path-beta/multibyte-paths timed out (25s+). The single JOIN blob made the planner estimate rows=1 on the scoped-node CTE (node_identity_sightings EXISTS is not estimable) → nested loops re-materialising the 13k-row identity view per row → 17.4M-row scans, 100-155s. ANALYZE + MATERIALIZED + UNION variants all still mis-estimated. FIX (commit 6e5d4c5): split into two simple statements (scoped nodes ~325ms + multibyte links ~92ms) joined + geometry-filtered in JS. Live: ukmesh 0.30s / 4,202 segments / maxCount 698,565; noparams 0.22s. Backend 273/273.
2. SITE-WIDE STALENESS — NONE. Ingest healthy (7.5k+5.3k packets/2h; top senders fresh; Sh@DoW nodes last seen 1-5 min). Multibyte evidence flowing (10,176 today). Other observers' traffic normal (NULL-src = encrypted, expected).
3. GNOME-STKTN-RPT-V4 OFFLINE — REAL, NOT A WEBSITE BUG. Steady ~13 payload-mentions/hour for days → abrupt stop 21:32:00.315Z. Zero evidence in any column since (src/rx/topic/payload/status). The site is correctly reporting the device went silent. GNOME-MSG-RPT online (22:13Z, 2,746 adverts). Likely device/power/RF at the STKTN site — physical check needed.
4. SIGHTINGS MV: node_identity_sightings (22.5k rows, zero indexes, per-query canonical grouping) → indexed MATERIALIZED VIEW (unique (node_id,network) + (network,node_id)), refreshed every 5 min with the links MV. All buildNodeScopeClause consumers (topology, viable links, paths, network filters) benefit.
5. recordMultibyteEvidence deadlock errors: 4 in 6h pre-fix, 0 in the 30 min after the MV swap. Watch.
