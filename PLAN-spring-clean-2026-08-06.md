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
