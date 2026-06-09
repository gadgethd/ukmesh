# Codebase Improvement Roadmap

This roadmap defines the remaining structural improvement work for this repo
(`meshcore-analytics` / ukmesh). It is written so that an implementation agent
(human or AI) can execute any single task safely without guessing, without
global context, and without changing runtime behavior.

Every claim in this document was verified against the repo on **2026-06-09**.
Line counts and symbol names below come from that snapshot. If you are reading
this much later, re-verify line counts before starting a task — files drift.

---

## 1. How to use this document

- Each task in the **Task Catalog** (section 5) is sized to be one PR.
- Tasks are ordered into **waves** (section 6). Do not start a Wave 3 task
  before its listed dependencies are merged.
- Every task has a **Risk** level. High-risk tasks are deliberately last.
- If a task says *"Investigation step required"*, do that investigation and
  record findings in the PR description before editing code.
- If reality contradicts this document (file moved, line count very different,
  symbol renamed), stop, update this document first in its own commit, then
  proceed.

---

## 2. Implementation rules for future agents

These rules are non-negotiable for every task in this roadmap.

1. **Behavior-preserving by default.** Every task here is a structural
   refactor unless explicitly marked otherwise. API response shapes, SQL
   results, MQTT handling, map rendering, and worker output must be
   byte-for-byte equivalent unless the task says otherwise.
2. **One task per PR.** Do not bundle tasks, even small ones.
3. **Move, don't rewrite.** When extracting a function to a new module, copy
   it verbatim, re-point imports, delete the original. Do not "improve" logic,
   rename variables, reformat, or fix perceived bugs in the same PR. If you
   find a real bug, note it in the PR description and leave it.
4. **No new dependencies** without an explicit note in the PR description
   explaining why.
5. **Never touch these files/paths** in roadmap PRs:
   - `.env` (not in git; never add it)
   - `CLAUDE.md`, `AI_MEMORY.md`, `knowledge.md`, `multipath.md`, `push.md`
     (gitignored on purpose — see `.gitignore`; never force-add them)
   - `mosquitto/passwd`, `mosquitto/acl`, `scripts/keys/`
6. **Follow the repo push process.** A `push.md` exists at the repo root on
   the deployment machine (it is intentionally gitignored). If present in your
   working copy, follow it for every push: review diff → check for secrets →
   stage only intended files → validate → commit → push → report.
7. **Validate with the commands that actually exist** (verified in
   `backend/package.json` and `frontend/package.json`):
   - Backend: `npm run build --prefix backend` (runs `tsc`)
   - Frontend: `npm run build --prefix frontend` (runs `tsc && vite build`)
   - Worker: `python3 -m py_compile viewshed-worker/worker.py viewshed-worker/rf/*.py`
   - There is **no test suite in this repo today** (verified: zero
     `*.test.*` / `*.spec.*` files). Task T3 creates one. Until T3 lands,
     "validation" means: compiles + builds + deployed smoke check.
8. **Deployment smoke check** (when you have access to the running stack):
   rebuild the affected service with
   `docker compose build <service> && docker compose up -d <service>`,
   then confirm `curl http://localhost:3000/healthz` returns `ok`.
   Note: `docker compose restart` does **not** re-read `.env`; always use
   `up -d`. If you cannot deploy, say so explicitly in the PR description —
   do not claim a smoke check you did not run.
9. **Stage files explicitly.** Never `git add .` — the working tree on the
   deployment machine routinely carries unrelated local modifications.
10. **Security hygiene.** Never commit DB dumps, packet captures, raw
    telemetry exports, tokens, session secrets, private keys, or operational
    notes containing deployment-specific details. When in doubt, leave it out.

---

## 3. Verified current state (2026-06-09)

### 3.1 What is already done

Earlier phases of this roadmap were completed and are **not** open work:

- **DB lifecycle split** — `backend/src/db/schema/base.sql`,
  `backend/src/db/migrations.ts`, versioned migrations in
  `backend/src/db/migrations/` (`001_ml_tables.sql` … `004_region_scope.sql`),
  asset path resolution in `backend/src/db/assets.ts`. Startup runs base
  schema + migrations through a dedicated no-timeout startup pool
  (`backend/src/db/index.ts`, `initDb()` at line ~171).
- **Route decomposition** — `backend/src/api/routes.ts` is down to **164
  lines** of composition glue. Domain routes live in
  `backend/src/api/routes/` (13 modules: `coverage.ts`, `health.ts`,
  `misc.ts`, `nodeStatus.ts`, `nodes.ts`, `owner.ts`, `pathing.ts`,
  `plannedCoverage.ts`, `radio.ts`, `spam.ts`, `stats.ts`, `telemetry.ts`).
- **Service/repository pattern** for the three main domains:
  - `backend/src/stats/` — `statsService.ts` (507), `statsRepository.ts` (854),
    `maskDecodedPathNodes.ts`
  - `backend/src/owner/` — `ownerService.ts` (409), `ownerRepository.ts` (475),
    `ownerSession.ts`, `ownerAccess.ts`
  - `backend/src/pathing/` — `pathingService.ts`, `pathingRepository.ts`
- **Router bootstrap extraction** — `backend/src/api/bootstrap/caches.ts`,
  `backend/src/api/bootstrap/limiters.ts`.
- **Partial worker split** — `viewshed-worker/rf/config.py` (55),
  `rf/loss.py` (120), `rf/terrain.py` (119) extracted from `worker.py`.
- **Partial frontend map split** — `frontend/src/components/Map/` contains
  `types.ts`, `mapConfig.ts`, `geojsonBuilders.ts` (449),
  `NodePopupContent.tsx` (177) extracted from `MapLibreMap.tsx`.
- **Contributor docs** — `docs/architecture.md`, `docs/contributing.md`,
  `docs/db-lifecycle.md`, `docs/frontend-map.md`, `docs/link-model.md`,
  `docs/pathing.md`.
- **Backend config extraction (partial)** —
  `backend/src/platform/config/database.ts` and
  `backend/src/platform/config/pathing.ts` exist. No other config modules do.

### 3.2 Corrections to earlier roadmap claims

- An earlier progress note recorded `MapLibreMap.tsx` reduced to ~577 lines.
  **It has regrown to 1,165 lines** (features added since: LOS toggle/custom
  LOS, planned-repeater placement and polling, map theme toggle, focus
  handling). Re-decomposition is task T8.
- The earlier "Next Steps" (pathing service/repository extraction, stats and
  owner repository extraction) are complete — see 3.1.

### 3.3 The current large files (the remaining problem)

Verified line counts, largest first:

| File | Lines | What it mixes |
|---|---|---|
| `backend/src/path-beta/resolver.ts` | 2,676 | candidate generation, scoring, anchors, promotion, geometry, priors |
| `viewshed-worker/worker.py` | 1,586 | RF math, viewshed jobs, link jobs, queue loop, DB writes, CLI test mode |
| `frontend/src/pages/OwnerPortalPage.tsx` | 1,260 | owner dashboard data fetch, charts, selector UI, map rendering |
| `backend/src/db/index.ts` | 1,247 | pool setup, startup init, plus ~25 exported domain query helpers |
| `frontend/src/components/Map/MapLibreMap.tsx` | 1,165 | map lifecycle, LOS controller, planned repeaters, refresh scheduling, focus |
| `frontend/src/pages/StatsPage.tsx` | 943 | stats page UI + data handling |
| `backend/src/mqtt/client.ts` | 887 | MQTT connection, topic parsing, packet normalization, dedupe, telemetry extraction |
| `backend/src/path-lazy/lazyResolver.ts` | 771 | lazy path resolution |
| `backend/src/mqtt/spamDetector.ts` | 678 | spam detection on ingest |
| `backend/src/backend-site/routes.ts` | 664 | backend-site routes |
| `backend/src/path-learning/rebuild.ts` | 583 | path-learning rebuild |

Other verified facts used by tasks below:

- **No tests exist anywhere in the repo** (no test files, no test runner in
  either `package.json`).
- Backend `package.json` scripts: `build`, `dev`, `start`, `spam:recompute`
  only. Frontend: `dev`, `build`, `preview` only.
- Frontend has exactly **one store**: `frontend/src/store/overlayStore.ts`
  (directory is `store/`, singular — earlier roadmap text said `stores/`).
- **15 frontend files** call `fetch(` directly (verified by grep). There is
  no `frontend/src/api/` directory; `frontend/src/config/api.ts` exists.
- Backend path domains are split across `backend/src/path-beta/`,
  `path-lazy/`, `path-hash/`, `path-learning/`, `path-shared/`, and
  `pathing/` (the route-facing service/repository pair).
- Background workers live in `backend/src/workers/` (`acl-watcher.ts`,
  `health.ts`, `link-backfill.ts`, `link-recompute.ts`, `path-history.ts`,
  `path-learning.ts`).
- `.gitignore` covers `.env`, mosquitto credentials, key files, and the AI
  context files, but has **no entries** for DB dumps, packet captures, or
  diagnostic exports (task T1).

---

## 4. Target architecture (unchanged goal)

The end state remains: each domain understandable in isolation.

- Backend: thin routes → services → repositories per domain; platform
  concerns (config, db pool, logging) under `backend/src/platform/`.
- Pathing: resolver decomposed into anchors / candidates / scoring /
  promotion / geometry, with reason codes and diagnostics.
- Worker: pure RF math under `viewshed-worker/rf/`, job handlers under
  `viewshed-worker/jobs/`, CLI test tooling under `viewshed-worker/cli/`,
  thin `worker.py` queue loop.
- Frontend: map split into core/controllers/layers/visibility/popups; domain
  API clients under `frontend/src/api/`; stores under `frontend/src/store/`.

Do not try to reach this in one PR. The Task Catalog below is the only
sanctioned path.

---

## 5. Task Catalog

Template per task: files, symbols, problem, steps, must-not-change, edge
cases, risk, validation, sequencing, dependencies.

---

### T1 — Expand `.gitignore` for operational/security artifacts

- **Files to edit:** `.gitignore` only.
- **Current problem (evidence):** `.gitignore` covers `.env`,
  `mosquitto/passwd`, `mosquitto/acl`, `scripts/keys/`, build output, and the
  AI context files — but nothing for DB dumps, packet captures, Redis dumps,
  or ad-hoc diagnostic exports. This repo handles radio infrastructure and
  operational data; an accidental `pg_dump` in the worktree must never be
  committable.
- **Steps:**
  1. Append a clearly-commented block to `.gitignore` with: `*.dump`,
     `*.pcap`, `*.pcapng`, `*.rdb`, `*.trace.json`, `.env.*` (with an
     explicit `!.env.example` exception), and a `tmp-exports/` style
     directory for ad-hoc diagnostic output.
  2. Do **not** add a blanket `*.sql` rule — `backend/src/db/schema/base.sql`,
     `backend/src/db/owner-auth.sql`, and `backend/src/db/migrations/*.sql`
     are tracked source files. If you want dump coverage, scope it (e.g.
     `/*.sql` at repo root only).
  3. Run `git status` before/after to confirm no currently-tracked file
     becomes ignored (`git ls-files -i -c --exclude-standard` must be empty).
- **Must not change:** tracking status of any existing tracked file.
- **Edge cases:** the tracked `.sql` files above; `.env.example` does not
  currently exist — the exception rule is forward-looking, do not create the
  file in this PR.
- **Risk:** Low.
- **Validation:** `git ls-files -i -c --exclude-standard` empty; `git status`
  unchanged for tracked files.
- **Sequencing:** First PR wave. **Dependencies:** none.

---

### T2 — Add "what must never be committed" section to contributing docs

- **Files to edit:** `docs/contributing.md` only.
- **Current problem (evidence):** `docs/contributing.md` exists but the
  security/hygiene rules (no dumps, no captures, no secrets, sanitized
  fixtures only) live only in untracked operational notes. Contributors have
  no in-repo guidance.
- **Steps:** add a short section covering: never commit secrets/dumps/
  captures/telemetry exports; use sanitized fixtures; reduce precision of
  sensitive coordinates in fixtures where exact values aren't required; new
  scripts must not default to writing exports into the repo tree.
- **Must not change:** existing contributing guidance.
- **Edge cases:** keep the section generic — do not include deployment
  hostnames, container topology details, or anything operationally sensitive
  in this public doc.
- **Risk:** Low. **Validation:** docs-only; render check.
- **Sequencing:** First PR wave (can be combined with nothing — own PR).
  **Dependencies:** none (pairs naturally after T1).

---

### T3 — Bootstrap a test harness (backend + worker)

- **Files to edit/create:**
  - `backend/package.json` (add `vitest` devDependency and a `test` script)
  - `backend/src/stats/maskDecodedPathNodes.test.ts` (first real test —
    `maskDecodedPathNodes.ts` is small and pure)
  - `viewshed-worker/requirements-dev.txt` (pytest) and one test, e.g.
    `viewshed-worker/tests/test_loss.py` covering `rf/loss.py`
    (`compute_path_loss(...)` and `compute_path_loss_from_profile(...)`)
- **Current problem (evidence):** zero test files exist in the repo; the
  roadmap goal "heavy domain logic is testable without full-stack deployment"
  is impossible without a runner. Every later extraction task wants
  characterization tests; this task makes that possible.
- **Steps:**
  1. Add vitest to backend devDependencies; add `"test": "vitest run"`.
  2. Write one characterization test for `maskDecodedPathNodes`
     (feed representative inputs, assert current outputs — capture current
     behavior, do not design "correct" behavior).
  3. Add pytest + one test for `viewshed-worker/rf/loss.py` pure functions.
  4. Document `npm test --prefix backend` and `pytest viewshed-worker/tests`
     in `docs/contributing.md`.
- **Must not change:** any runtime code, any production dependency, Docker
  images (dev deps only; confirm `backend/Dockerfile` uses a production
  install or prune — **investigation step required**: check how the backend
  image installs deps before adding devDependencies, and note findings in
  the PR).
- **Edge cases:** vitest needs ESM/TS config that matches the existing
  `tsconfig`; keep config minimal and additive.
- **Risk:** Low (additive only).
- **Validation:** `npm run build --prefix backend` still passes;
  `npm test --prefix backend` passes; `python3 -m pytest viewshed-worker/tests` passes.
- **Sequencing:** First PR wave. **Dependencies:** none.

---

### T4 — Calibration fixture harness (skeleton)

- **Files to create:**
  - `test-fixtures/links/README.md` and one sanitized fixture file
  - `test-fixtures/pathing/README.md` and one sanitized fixture file
  - a backend test that loads a pathing fixture and runs a pure scoring
    function against it (exact function chosen during T12a investigation —
    until then, fixtures + loaders only)
- **Current problem (evidence):** model tuning today is anecdotal ("this
  repeater should hear that one"). There is no `test-fixtures/` directory and
  no stable way to test such claims against the model.
- **Steps:**
  1. Define fixture JSON shapes (link expectation: node pair, expected
     viability/band; pathing expectation: packet hash inputs, expected
     resolved hops / expected-red).
  2. Populate with **sanitized** data only: no session material, no tokens,
     and reduce coordinate precision where exact values aren't needed.
  3. Wire a loader + at least one assertion via the T3 harness.
- **Must not change:** runtime code.
- **Edge cases:** fixtures must not embed raw production exports; see T2
  rules.
- **Risk:** Low (additive).
- **Validation:** `npm test --prefix backend` passes.
- **Sequencing:** First/second PR wave. **Dependencies:** T3.

---

### T5 — Worker RF split, part 2: profile/visibility/geometry pure functions

- **Files to edit/create:**
  - Create `viewshed-worker/rf/geometry.py`, `viewshed-worker/rf/visibility.py`
  - Edit `viewshed-worker/worker.py`, `viewshed-worker/Dockerfile` (only if it
    enumerates `rf/` files rather than copying the directory — **verify
    first**; the rf/ tree is already packaged since the part-1 split)
- **Exact symbols to move** (verified in `worker.py`):
  - To `rf/geometry.py`: `project_xy_km` (line ~154), `node_dist_km` (~161),
    `physical_candidate_radius_km` (~169),
    `is_viewshed_eligible_coordinate` (~134), `weighted_quantile` (~141)
  - To `rf/visibility.py`: `resolve_rf_radial_boundaries` (~473),
    `clip_and_simplify_polygon` (~529), `build_exclusive_strength_geoms`
    (~544)
- **Current problem (evidence):** `worker.py` is 1,586 lines mixing pure RF
  math with queue orchestration and DB writes. The part-1 split (config/loss/
  terrain) proved the pattern; these remaining pure functions still require
  reading queue logic to find.
- **Steps:** copy each function verbatim into the new module; import back
  into `worker.py`; delete originals; keep module-level constants they
  reference either passed as args or imported from `rf/config.py` (do not
  duplicate constants).
- **Must not change:** numeric output of any function; `calculate_viewshed`
  and all `process_*_job` handlers stay in `worker.py` for now (they touch DB
  and Redis — moving them is T6).
- **Edge cases:** functions that read globals refreshed by
  `refresh_rf_calibration` / `refresh_support_context` (e.g.
  `support_penalty_db`, `source_support_radius_m`) — **leave these in
  `worker.py`** in this task; they are stateful, not pure.
- **Risk:** Medium (production worker; behavior must be identical).
- **Validation:** `python3 -m py_compile` on all touched files; pytest from
  T3 if fixtures cover these; deployed smoke: rebuild `viewshed-worker` and
  `link-worker` containers, confirm both boot cleanly and process at least
  one job in logs.
- **Sequencing:** Second wave. **Dependencies:** T3 (recommended, for
  characterization tests of `weighted_quantile` etc.), part-1 split (done).

---

### T6 — Worker jobs + CLI split

- **Files to edit/create:**
  - Create `viewshed-worker/jobs/__init__.py`, `jobs/link_jobs.py`,
    `jobs/coverage_jobs.py`; create `viewshed-worker/cli/test_links.py`
  - Edit `viewshed-worker/worker.py`, `viewshed-worker/Dockerfile`
- **Exact symbols to move** (verified):
  - `jobs/link_jobs.py`: `process_physical_link_job` (~921),
    `process_observation_link_job` (~939), `process_link_job` (~1083),
    `upsert_link_pair` (~832), `ensure_physical_link_metrics` (~856),
    `publish_link_update` (~813), `enqueue_physical_link_jobs_for_node` (~1091)
  - `jobs/coverage_jobs.py`: `calculate_viewshed` (~587),
    `already_calculated` (~740), `store_coverage` (~753),
    `enqueue_uncovered` (~1128), `rebuild_pending_viewshed_set` (~1160)
  - `cli/test_links.py`: `run_test_mode` (~1462), `resolve_node_ref` (~1330),
    `compute_pair_diagnostics` (~1376)
  - `worker.py` keeps: `main`, `worker_loop`, `process_job`, `wait_for_db`,
    the calibration/support refresh functions, and arg parsing.
- **Current problem (evidence):** job handlers and the CLI test mode are
  interleaved with the queue loop in one 1,586-line file; a contributor
  cannot test one link pair without reading queue logic.
- **Steps:** move verbatim; thread `db` / `r_client` handles as parameters
  (they already are parameters on most handlers — verified signatures above);
  keep `process_job` in `worker.py` dispatching to the new modules; preserve
  the existing CLI flags exactly (**investigation step required**: read
  `main()` to enumerate current argparse flags before moving; document them
  in the PR).
- **Must not change:** queue message handling, ack semantics, DB write
  payloads, CLI flag names/output format (the test mode is used
  operationally).
- **Edge cases:** module-level mutable state used by handlers (calibration
  globals) must remain importable from one place — prefer importing
  `worker`-owned refresh state via a small `rf/calibration_state.py` only if
  unavoidable; otherwise pass values explicitly. Note what you chose in the
  PR.
- **Risk:** Medium-High (this is the production job path).
- **Validation:** py_compile all; deployed smoke: rebuild `viewshed-worker` +
  `link-worker`, watch logs for one coverage job and one link job completing;
  run the CLI test mode against one known node pair and confirm output
  matches a pre-refactor capture of the same pair.
- **Sequencing:** Second wave, after T5. **Dependencies:** T5.

---

### T7 — Frontend: centralize map visibility rules

- **Files to edit/create:**
  - Create `frontend/src/components/Map/visibilityRules.ts`
  - Edit `frontend/src/components/Map/geojsonBuilders.ts` (449 lines) and/or
    `MapLibreMap.tsx` — **investigation step required:** grep for the
    stale-node cutoff, links-only-mode visibility, path-focus filtering, and
    clash-mode filtering to find where each rule currently lives
    (candidates: `geojsonBuilders.ts`, `MapLibreMap.tsx`,
    `frontend/src/components/Map/mapConfig.ts`,
    `frontend/src/hooks/useNodes.ts`). Record exact locations in the PR
    before moving anything.
- **Current problem (evidence):** visibility behavior (stale cutoff,
  links-only stale visibility, path-mode hiding, clash relevance) is not in a
  dedicated module; earlier roadmap work flagged this as the remaining
  frontend-map item and it was never done (no `visibility*` file exists under
  `frontend/src/components/Map/` — verified directory listing).
- **Steps:** create one pure module exporting predicate functions
  (node visible? link visible? given mode + timestamps + focus state); move
  rule logic there verbatim; call from existing call sites.
- **Must not change:** which nodes/links render in any mode; opacity values;
  the 14-day stale convention (verify the actual constant during
  investigation — do not assume 14 days, the earlier roadmap asserted it
  without a file citation).
- **Edge cases:** nodes at 0,0 are hidden in some views (owner map — see
  `OwnerPortalPage.tsx` history); path-focus and clash modes interact with
  staleness; theme toggling must not affect visibility.
- **Risk:** Medium.
- **Validation:** `npm run build --prefix frontend`; visual smoke on the dev
  site: default view, links-only mode, path-focus mode, clash mode each
  render the same set of features as before (compare screenshots or feature
  counts).
- **Sequencing:** Second wave. **Dependencies:** none hard; T3 lets you unit
  test the predicates.

---

### T8 — Frontend: re-decompose `MapLibreMap.tsx` (regrown to 1,165 lines)

- **Files to edit/create:**
  - Edit `frontend/src/components/Map/MapLibreMap.tsx`
  - Create `frontend/src/components/Map/losController.ts`,
    `frontend/src/components/Map/plannedRepeaterController.ts`,
    `frontend/src/components/Map/refreshScheduler.ts` (names indicative;
    keep one concern per file)
- **Exact symbols to extract** (verified in `MapLibreMap.tsx`):
  - LOS: `handleToggleLos` (~line 181), `handleCustomLosPoint` (~246),
    `clearLosTimer` (~173) and associated LOS state
  - Planned repeaters: `pollPlannedCoverage` (~264),
    `handleRemovePlannedRepeater` (~280), `placePlannedRepeater` (~290)
  - Refresh: `refreshMapSources` (~367), `scheduleRefresh` (~426),
    `clearFocusTimer` (~360)
  - Stays in component: map init/lifecycle, `toggleMapTheme`,
    event wiring, `toggleCoverageForNode`, `getNode`,
    `handleFocusSamePrefix` (unless it moves cleanly with focus logic).
- **Current problem (evidence):** the component was previously reduced to
  ~577 lines, then regrew to 1,165 as LOS, planned-repeater, and theme
  features were added directly into it. This is the exact failure mode the
  map split was meant to prevent.
- **Steps:** extract each controller as a hook (`useLosController(map, ...)`
  style) or plain module taking explicit deps; move code verbatim; keep
  the component as orchestration. One controller per commit within the PR
  so review is tractable.
- **Must not change:** the imperative MapLibre update flow (sources are
  mutated in place, not re-created); ordering of source updates in
  `refreshMapSources`; LOS terrain exaggeration math (`ANTENNA_H`, `EXAG`
  usage near lines 201–206); planned-coverage polling cadence.
- **Edge cases:** timers (`clearLosTimer`, `clearFocusTimer`, poll
  intervals) must be cleaned up on unmount exactly as before; deck.gl
  overlay (`DeckGLOverlay.tsx`, 371 lines) interacts with the map instance —
  do not change its props contract.
- **Risk:** Medium.
- **Validation:** frontend build; visual smoke: LOS toggle on a node with
  coverage, custom LOS point, place + remove a planned repeater, theme
  toggle, node focus — all behave identically. Service-worker note: if
  testing in a browser against a deployed build, hard-refresh; stale SW app
  caches have masked frontend changes before (cache names were bumped to v4
  for exactly this reason).
- **Sequencing:** Second wave, after T7 (visibility rules out first reduces
  conflict surface). **Dependencies:** T7 recommended, not strict.

---

### T9 — Frontend API client layer

- **Files to edit/create:**
  - Create `frontend/src/api/client.ts` plus domain modules as needed
    (`coverage.ts`, `owner.ts`, `stats.ts`, `pathing.ts`, `nodes.ts`)
  - Edit the call sites: 15 files currently call `fetch(` directly
    (verified by grep across `frontend/src`). Enumerate them with
    `grep -rl "fetch(" frontend/src --include="*.ts" --include="*.tsx"`
    at implementation time and list them in the PR description.
  - `frontend/src/config/api.ts` already exists — **investigation step
    required:** read it first; if it already defines base-URL handling,
    build `client.ts` on top of it rather than duplicating.
- **Current problem (evidence):** raw `fetch` in 15 files means error
  handling, base URLs, and response typing are repeated and drift.
- **Steps:** introduce `client.ts` (thin typed wrapper, same headers/
  credentials behavior as current calls); migrate **one domain per PR**
  starting with the smallest (suggest: coverage or stats). This task is
  therefore a series of small PRs, not one.
- **Must not change:** request URLs, query params, credentials/cookie
  behavior (owner session relies on cookies — owner endpoints carry session
  state; migrate owner last to be safe), polling cadences, error fallbacks.
- **Edge cases:** owner-portal fetches are isolated from `/api/owner/live`
  on purpose (the last-hop chart was deliberately split to its own endpoint
  after timeout incidents — preserve the separate fetch paths and their
  different refresh cadences); WebSocket usage (`useWebSocket.ts`) is out of
  scope.
- **Risk:** Medium (Low per-domain if split as instructed).
- **Validation:** frontend build; smoke each migrated page; network tab
  diff: identical request URLs/params before vs after.
- **Sequencing:** Second/third wave. **Dependencies:** none strict.

---

### T10 — Split `OwnerPortalPage.tsx` (1,260 lines)

- **Files to edit/create:**
  - Edit `frontend/src/pages/OwnerPortalPage.tsx`
  - Create components under `frontend/src/components/Owner/` (e.g.
    `LastHopChart.tsx`, `DirectSenderMap.tsx`, `TelemetryPanels.tsx`) —
    exact split to be proposed in the PR after reading the file.
- **Current problem (evidence):** the page holds the live dashboard, the
  RX-strength-by-last-hop chart + repeater selector, the direct-sender map
  (with its own MapLibre instance, popups, ResizeObserver), and telemetry
  panels in one file.
- **Steps:** extract self-contained render units first (chart + selector;
  direct-sender map); keep data fetching at the page level initially so
  behavior is provably unchanged; move fetches into the T9 API layer only
  if T9 already covers owner endpoints.
- **Must not change:** the separate fetch for the last-hop chart (own
  endpoint, slower refresh cadence — deliberate, see edge cases in T9);
  the ≥10-samples series filter; hiding of `unresolved` series at the
  presentation layer; repeater selector behavior (All + per-repeater);
  0,0-coordinate node hiding on the direct-sender map; ResizeObserver
  resize handling.
- **Edge cases:** owner styling exists in duplicated global stylesheet
  copies (a past change had to touch "both frontend global stylesheet
  copies" — **investigation step required:** locate both before moving any
  styles; do not consolidate them in this PR).
- **Risk:** Medium.
- **Validation:** frontend build; owner-portal smoke with a real session:
  chart renders, selector filters, map interacts, telemetry panels populate.
- **Sequencing:** Third wave. **Dependencies:** T9 (owner domain) optional;
  do not block on it.

---

### T11 — Backend config consolidation (completion)

- **Files to edit/create:**
  - `backend/src/platform/config/` — add modules only where an inventory
    proves scattered constants exist (candidates from earlier roadmap:
    `rf.ts`, `map.ts`, `startup.ts`, `env.ts`).
- **Current problem (evidence):** only `database.ts` and `pathing.ts` exist
  under `platform/config/`. Other behavior-critical constants (cache TTLs in
  `backend/src/api/bootstrap/caches.ts`, resolver thresholds in
  `path-beta/resolver.ts`, worker calibration defaults) remain inline.
- **Steps:**
  1. **Investigation step required (own PR or PR section):** inventory
     inline constants in `api/bootstrap/caches.ts`, `path-beta/resolver.ts`,
     `mqtt/client.ts`, and `workers/*.ts`. Produce a table: constant,
     file:line, proposed config home. Get the inventory reviewed before
     moving anything.
  2. Move constants one domain per PR, preserving exact values and env-var
     override behavior.
- **Must not change:** any default value; any env-var name already read.
- **Edge cases:** constants that look identical but serve different layers
  (e.g. a frontend stale cutoff vs a backend one) must **not** be merged
  into one constant — link them in comments instead.
- **Risk:** Medium.
- **Validation:** backend build; grep proves old inline constants gone;
  deployed smoke.
- **Sequencing:** Second/third wave. **Dependencies:** none strict.

---

### T12 — Decompose `backend/src/path-beta/resolver.ts` (2,676 lines) — staged

This is the highest-risk area in the repo. It ships in **four separate PRs**
(T12a–T12d). Never combine them.

- **Files involved:** `backend/src/path-beta/resolver.ts`,
  `backend/src/path-beta/affinity.ts` (253 lines, recently added neighbor
  affinity scoring), new files under `backend/src/path-beta/`.
- **Current problem (evidence):** one file holds scoring helpers
  (`linkColorPreference`, `radioNeighborPreference`,
  `multibytePathPreference`, `confirmedLinkConfidence`,
  `edgeMetricConfidence`, `strongConfirmedFloor`, `purpleEdgeAllowed`),
  anchor construction (`buildHashMatchedAnchors`,
  `buildDirectObserverAnchorIndex`, `buildResolvableMultibyteAnchors`,
  `directAnchorsForHops`), path expansion (`buildFallbackPrefixPath`,
  `enumeratePrefixContinuations`), observer-echo handling
  (`trimObserverTerminalHop`, `matchesObserverPathHash`,
  `isObserverSelfEchoLoop`), and orchestration — all symbols verified by
  grep at the listed names.
- **T12a — characterization first (no code movement).** Build a regression
  corpus: capture current resolver output (resolved hops, purple/red
  classification, alternatives) for a set of representative packet hashes
  via the existing `/api/path-beta/resolve` endpoint; store sanitized
  fixtures in `test-fixtures/pathing/` (T4 shape). Risk: Low. This corpus is
  the safety net for T12b–d.
- **T12b — extract pure scoring.** Move the scoring/preference functions
  listed above into `path-beta/scoring.ts`, verbatim. Risk: Medium.
  Validation: T12a corpus byte-identical.
- **T12c — extract anchors + geometry/echo helpers.** Move anchor builders
  and observer-echo helpers into `path-beta/anchors.ts` /
  `path-beta/observerEcho.ts`. Risk: Medium-High. Validation: corpus
  byte-identical.
- **T12d — reason codes + diagnostics (additive behavior change).** Add
  explicit reason codes (e.g. `MULTIBYTE_UNIQUE_MATCH`,
  `NO_PHYSICAL_SUPPORT`, `FAILED_LOS_GATE`, `AMBIGUOUS_SHORT_HASH`) to
  resolver output and a `packetTrace` diagnostic entry point. This is the
  only sub-task allowed to extend output shape; it must be **purely
  additive** (new fields only; existing fields unchanged). Risk: High —
  do this last, after T12b/c have soaked in production.
- **Must not change (T12a–c):** resolver output for the corpus; purple/red
  promotion decisions; conservative multibyte attribution (ambiguous hashes
  are deliberately not force-mapped); interaction with
  `path-lazy/` and `path-learning/` consumers — **investigation step
  required:** grep for imports of `path-beta/resolver` across `backend/src`
  before moving exports, and keep every export name stable.
- **Edge cases:** hour-bucketed priors (`currentHourBucket`, `edgeKey`,
  `motifKey`) are time-dependent — corpus captures must pin or tolerate
  bucket boundaries; multi-resolve (`resolve-multi`) shares helpers with
  single resolve.
- **Risk:** High overall.
- **Validation:** corpus diff per sub-PR; backend build; deployed smoke on
  `/api/path-beta/resolve` and `/api/path-beta/resolve-multi` for known
  hashes.
- **Sequencing:** Third wave, in order a→b→c→d. **Dependencies:** T3, T4.

---

### T13 — Split `backend/src/db/index.ts` (1,247 lines) — staged

- **Files involved:** `backend/src/db/index.ts`, new
  `backend/src/db/pool.ts`, plus moves of domain query helpers toward the
  existing repositories.
- **Current problem (evidence):** the file mixes pool/startup lifecycle
  (`query`, `initDb`) with ~25 exported domain helpers (`upsertNode`,
  `insertPacket`, `getNodes`, `getRecentPackets`, `getPacketDetail`,
  `getMultibytePathSegments`, `getViableLinkPairs`,
  `upsertPathHistoryCache`, etc. — all verified). Any DB change forces
  contributors through one giant file.
- **Steps (one PR each):**
  1. **T13a:** extract pool construction + `query` + `initDb` + startup-pool
     handling into `db/pool.ts`; `db/index.ts` re-exports so no import in
     the codebase changes. Risk: Medium (startup path — the no-timeout
     startup pool behavior from the earlier hardening must be preserved
     exactly).
  2. **T13b+:** move domain helper clusters to domain repositories
     (path-history helpers → `pathing/pathingRepository.ts`; packet/node
     reads → a new `db/packetsRepository.ts` or per-domain homes), one
     cluster per PR, with `db/index.ts` re-exporting during transition.
- **Must not change:** `initDb` semantics (base schema then migrations via
  startup pool, no statement timeout); `MIN_LINK_OBSERVATIONS = 5` constant
  value and export; every existing export must remain importable from
  `backend/src/db` until a dedicated cleanup PR removes re-exports.
- **Edge cases:** callers across `mqtt/`, `workers/`, `ws/`, `api/` —
  re-export strategy exists precisely so they don't change in the same PR.
- **Risk:** High (startup + ingest hot path).
- **Validation:** backend build; deployed smoke: backend restart against the
  production-sized DB completes, `healthz` ok, packets visibly ingesting in
  logs, one full migration-runner boot log reviewed.
- **Sequencing:** Third wave. **Dependencies:** T3 recommended.

---

### T14 — Split `backend/src/mqtt/client.ts` (887 lines)

- **Files involved:** `backend/src/mqtt/client.ts`, new
  `backend/src/mqtt/topic.ts`, `backend/src/mqtt/normalize.ts`,
  `backend/src/mqtt/dedupe.ts` (indicative names).
- **Exact symbols** (verified): pure-ish candidates to extract —
  `parseTopic`, `topicIataForNode`, `toNum`, `toRecord`, `readNum`,
  `isEmptyPacketEnvelope`, `extractStatusTelemetry`, `identifyChannel`,
  `buildSummary`, `buildAdvertFallbackPayload`; stateful pieces that stay
  (or move as a unit with their state) — subscriber registry
  (`onPacket`/`onNodeSeen`/`onNodeUpsert`, `emit*`, flush scheduling),
  `isDuplicatePacket`, `tryCountAdvert`.
- **Current problem (evidence):** connection management, topic parsing,
  packet normalization, dedupe, and telemetry extraction live in one file on
  the ingest hot path.
- **Steps:** extract the pure parsing/normalization helpers first (verbatim);
  leave connection lifecycle and subscriber state in `client.ts`;
  `spamDetector.ts` (678 lines) is explicitly out of scope here.
- **Must not change:** dedupe behavior (`isDuplicatePacket` keying by hash +
  observer + hop count), advert counting, emit batching/flush timing,
  reconnect behavior, topic ACL assumptions.
- **Edge cases:** this is live ingest — a regression silently drops packets.
  Deploy during a low-traffic window and watch ingest counters before/after.
- **Risk:** High.
- **Validation:** backend build; characterization tests for `parseTopic`,
  `extractStatusTelemetry`, `buildSummary` via T3 harness **before** the
  move; deployed smoke comparing packets-per-minute before/after.
- **Sequencing:** Third wave, after T13a. **Dependencies:** T3 (tests
  first), T13a (pool extraction reduces churn overlap).

---

### T15 — Sanctioned npm scripts for common tasks

- **Files to edit:** `backend/package.json`, root-level docs
  (`docs/contributing.md`).
- **Current problem (evidence):** backend scripts are only
  `build`/`dev`/`start`/`spam:recompute`; common operations (migrations
  check, link recompute, path trace) require ad-hoc shell knowledge.
- **Steps:** add scripts **only for tools that exist** at implementation
  time. Today that justifies at most: a `db:migrate`-style wrapper if
  `migrations.ts` exposes a callable entry (**investigation step required**:
  check whether migrations can run outside full startup; if not, defer),
  and wrappers for `tools/recomputeSpamSuspects.ts`-style utilities. Path
  trace and link-test scripts depend on T6/T12d outputs — add them in those
  PRs instead.
- **Must not change:** existing script names/behavior.
- **Risk:** Low.
- **Validation:** each new script runs successfully once, documented output.
- **Sequencing:** Rolling — attach script additions to the PR that creates
  the underlying tool. **Dependencies:** per-script (T6, T12d).

---

## 6. PR sequencing plan

**Wave 1 — safe, additive (start immediately, any order):**
1. T1 `.gitignore` hygiene
2. T2 contributing-doc security section
3. T3 test harness bootstrap
4. T4 calibration fixture skeleton (after T3)

**Wave 2 — behavior-preserving extractions (after Wave 1):**
5. T5 worker RF pure functions
6. T6 worker jobs/CLI split (after T5)
7. T7 map visibility rules
8. T8 MapLibreMap re-decomposition (after T7)
9. T9 frontend API clients (series of small PRs)
10. T11 backend config consolidation (inventory first)

**Wave 3 — high-risk core (only after Waves 1–2 are merged and soaked):**
11. T12a→T12d resolver (corpus first, reason codes last)
12. T13a→T13b db/index split
13. T10 OwnerPortalPage split
14. T14 mqtt client split (last — live ingest)

T15 (scripts) rides along with whichever PR creates each tool.

Rationale: Wave 1 builds the safety net; Wave 2 is verbatim moves of pure
code with cheap validation; Wave 3 touches startup, ingest, and the resolver
— the three places where a silent regression costs real data or real
outages.

---

## 7. Per-task checklist (copy into every roadmap PR)

- [ ] Task ID from this roadmap referenced in the PR description
- [ ] Investigation steps (if the task lists any) done and findings recorded
- [ ] Code moved verbatim — no logic, naming, or formatting changes mixed in
- [ ] No new dependencies (or justified explicitly)
- [ ] All existing export names still importable from their old paths
      (re-export if needed)
- [ ] `npm run build --prefix backend` and/or `--prefix frontend` and/or
      `py_compile` pass, as applicable
- [ ] Tests added/updated if T3 harness covers the touched code
- [ ] No secrets, dumps, fixtures with raw production data, or gitignored
      context files staged (`git diff --cached` reviewed)
- [ ] Deployed smoke check done, or explicitly noted as not done and why
- [ ] Rollback note in PR description (see section 10)

---

## 8. Regression testing checklist

Run after deploying any Wave 2/3 change; scope to the touched area:

- **Backend core:** `curl http://localhost:3000/healthz` returns `ok`;
  backend boot log shows base schema + migrations completing without error.
- **Ingest (T13/T14):** packets-per-minute in logs comparable to
  pre-deploy; node upserts occurring; no dedupe anomalies (sudden duplicate
  or zero packet counts).
- **Pathing (T12):** `/api/path-beta/resolve` and `resolve-multi` return
  corpus-identical results for the T12a fixture hashes; `/api/path-beta/history`
  and `/api/path-learning` respond.
- **Stats/owner:** `/api/stats`, `/api/stats/charts` respond within normal
  time; owner portal login → dashboard → last-hop chart all populate.
- **Map (T7/T8):** default, links-only, path-focus, and clash modes render
  the same feature sets; LOS toggle, planned repeater place/remove, theme
  toggle, popups all work; hard-refresh to defeat service-worker caching
  before judging.
- **Worker (T5/T6):** `viewshed-worker` and `link-worker` containers boot
  cleanly; one coverage job and one link job complete in logs; CLI test mode
  output matches a pre-refactor capture for the same node pair.

---

## 9. Live-network / device-data testing checklist

This system ingests live LoRa mesh traffic over MQTT; there is no synthetic
load generator in the repo. For changes touching ingest or the worker:

- [ ] Deploy during a low-traffic window where feasible
- [ ] Capture a 10-minute pre-deploy baseline: packets/min, adverts counted,
      nodes upserted (from backend logs)
- [ ] Compare the same metrics 10 minutes post-deploy
- [ ] Confirm at least one real multi-hop packet resolves in the path view
- [ ] Confirm at least one repeater's coverage/links recompute end-to-end
- [ ] If MQTT credentials/ACL behavior could be affected, verify one owner
      node still authenticates (owner login flow is backed by MQTT
      credentials — see `backend/src/owner/ownerAccess.ts`)

---

## 10. Rollback plan

- Every roadmap PR must be a single revertable commit on `main`
  (squash-merge). Rollback = `git revert <merge-commit>` + rebuild + redeploy
  the affected services with `docker compose build <service> && docker
  compose up -d <service>`.
- **No roadmap PR may include a DB migration.** Structural refactors here
  never need schema changes; if one seems to, the task is mis-scoped — stop
  and split it. (This keeps every rollback a pure code revert.)
- Frontend rollbacks: remember the service-worker cache — if a bad frontend
  build was cached, bump the SW cache names (last bump: `meshcore-app-v4` /
  `meshcore-tiles-v4`) as part of the rollback deploy.
- Worker rollbacks: rebuild both `viewshed-worker` and `link-worker` (they
  share the worker image/code).
- Keep the pre-change image available where practical so rollback does not
  depend on a rebuild succeeding.

---

## 11. Open questions

Record answers in this file (own commit) as they are resolved.

1. **Stale-node cutoff:** earlier roadmap text asserts a 14-day stale rule
   for map visibility but cites no file. T7's investigation must find the
   actual constant and location before extraction.
2. **Backend devDependencies in Docker:** does `backend/Dockerfile` install
   dev deps into the production image? T3 must check before adding vitest.
3. **Migrations as a standalone command:** can
   `backend/src/db/migrations.ts` run outside full backend startup? Decides
   whether T15 can ship a `db:migrate` script.
4. **Resolver consumers:** which modules import from
   `backend/src/path-beta/resolver.ts` (suspects: `path-lazy/`,
   `path-learning/`, `api/routes/pathing.ts`, `workers/path-history.ts`)?
   T12 must enumerate before any export moves.
5. **Duplicated global stylesheets:** past owner-portal work edited "both
   frontend global stylesheet copies". Which two files, and should they be
   consolidated (separate future task, not part of T10)?
6. **`path-lazy` vs `path-beta` long-term:** `lazyResolver.ts` (771),
   `lazyResolverLegacy.ts` (443), and `path-beta/resolver.ts` (2,676)
   coexist. Is `lazyResolverLegacy.ts` still reachable? If dead, removing it
   is a cheap future task — but verify reachability first; do not assume.
7. **`backend-site/routes.ts` (664 lines):** not covered by the earlier
   route decomposition. Does it warrant the same route/service split, or is
   it low-churn enough to leave?
8. **Frontend store strategy:** only `overlayStore.ts` exists. The original
   target listed several stores, but current state may simply not need them
   — decide based on actual state-sharing pain, not the old diagram.

---

## 12. Out of scope for this roadmap

- Feature work (anything that changes what users see or what the model
  computes), except the explicitly additive T12d reason codes.
- DB schema changes and backfills (governed by `docs/db-lifecycle.md`).
- Mosquitto/ACL, tunnel, or deployment-topology changes.
- The Teesside site assets (gitignored; separate project).
