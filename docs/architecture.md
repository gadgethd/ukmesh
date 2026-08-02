# Architecture

`meshcore-analytics` is split across four runtime groups:

- `backend`
  - HTTP API
  - WebSocket live stream
  - DB access
  - owner dashboard/session logic
  - path resolver orchestration
- `frontend`
  - map rendering
  - packet feed
  - owner/stats pages
  - external stores for live state
- `viewshed-worker`
  - coverage generation
  - physical link evaluation
  - radio-neighbour ingestion support
  - RF/path-loss calculations
- operations and edge
  - Mosquitto, PostgreSQL/TimescaleDB, and Redis state
  - Nginx behind Anubis, optionally reached through Cloudflare Tunnel
  - Prometheus, Alertmanager, Grafana, Loki, Alloy, and bounded exporters

## Backend domain layout

- `backend/src/api/`
  - thin HTTP route modules and bootstrap wiring
  - bounded viewport coverage and recent topology contracts
- `backend/src/repositories/`
  - SQL for nodes, planned coverage/publication, topology, RF validation,
    owner alerts, registration, and operator workflows
- `backend/src/operations/`
  - local/operator queue, analysis, model, registration, and audit services
- `backend/src/platform/`
  - runtime configuration
- `backend/src/db/`
  - pool setup, base schema, migrations
- `backend/src/stats/`
  - stats service/repository logic
- `backend/src/owner/`
  - owner auth/session/live service and repository logic
- `backend/src/pathing/`
  - pathing service/repository orchestration
- `backend/src/path-beta/`
  - resolver implementation and worker pool
- `backend/src/api/utils/`
  - route-scoped shared helpers
- `backend/src/api/bootstrap/`
  - cache and limiter construction

## Frontend domain layout

- `frontend/src/components/Map/MapLibreMap.tsx`
  - primary imperative map orchestration
- `frontend/src/components/Map/geojsonBuilders.ts`
  - pure builders for node/link/coverage/clash GeoJSON
- `frontend/src/components/Map/mapConfig.ts`
  - map constants and style config
- `frontend/src/components/Map/NodePopupContent.tsx`
  - popup rendering
- `frontend/src/store/overlayStore.ts`
  - path, replay, planner, selection, and explanation UI state
- `frontend/src/components/Map/NodePopupContent.tsx`
  - selected-node summary rendered in the shared map popup
- `frontend/src/components/app/TimelineControl.tsx`
  - bounded historical activity replay controls
- `frontend/src/components/app/PlannerComparison.tsx`
  - saved scenario comparison, sharing, and overlap estimates
- `frontend/src/hooks/useWatchlist.ts`
  - bounded, network-scoped browser-local saved searches and actionable entries
- `frontend/src/config/publicRoutes.ts`
  - shared runtime route, canonical, sitemap, and build-time SEO metadata
- `frontend/src/styles/map-app.css` and `frontend/src/pages/*.css`
  - route- and domain-scoped styles; `globals.css` is reserved for shared tokens, reset, and legacy shared components
- `frontend/src/hooks/useNodes.ts`
  - live node/packet store
- `frontend/src/hooks/useCoverage.ts`
  - coverage store
- `frontend/src/hooks/useLinkState.ts`
  - link store

## Worker domain layout

- `viewshed-worker/worker.py`
  - queue orchestration and DB write flow
- `viewshed-worker/rf/config.py`
  - RF thresholds and calibration state
- `viewshed-worker/rf/loss.py`
  - path-loss calculation helpers
- `viewshed-worker/rf/terrain.py`
  - tile download, terrain sampling, VRT helpers

## Data flow

1. MQTT packets arrive in the backend ingest path.
2. Backend normalizes packet/node updates and publishes live messages.
3. Frontend stores ingest live node/packet/link updates without routing them through `App` state.
4. Coverage and physical links are computed asynchronously by the worker.
5. Pathing combines physical links, multibyte evidence, and cached history to produce purple/red paths plus evidence explanations.
6. Synthetic journeys independently exercise liveness, readiness, stats, and initial WebSocket state and persist latency/failure history.
7. Prometheus rules flow through Alertmanager into the local alert receiver;
   operator delivery state is visible without exposing the receiver publicly.

## Operational rules

- app startup must not run heavy historical backfills
- liveness must remain independent of MQTT readiness; use `/readyz` for dependency checks
- route modules should stay thin
- repositories own SQL
- services own orchestration and shaping
- worker RF math should stay isolated from queue orchestration
- anonymous APIs use closed privacy DTOs; explicit operator publication is the
  only path from a private planned-node record to `/api/planned-nodes`
- operator APIs are local-only and use short-lived Strict cookies or the
  temporary bearer compatibility path
- immutable application releases use signed digest references and fresh signed
  restore evidence; rollback is to a previously verified digest

## Recorded security and product decisions

- **Network and Feed test scopes:** the production app is locked to `ukmesh`;
  the separate development build is locked to network `test`; neither accepts
  anonymous network `all`. The Feed's `Test` selector is an independent,
  public content-channel label inside the already authorized network response.
  It does not grant access to network `test`.
- **Anonymous coordinates:** exact private-node, observer, path, raw-advert,
  topology, LOS, and unpublished planned-node coordinates are removed by
  server DTOs before caching or fanout. Client masking is defense in depth.
  Owner and operator responses use separate authenticated DTOs. Public packet
  SQL also validates path framing and joins the current private-prefix table
  for historical and new rows; privacy changes therefore take effect without
  rewriting compressed Timescale history.
- **Map delivery:** `/api/nodes/map` uses deterministic cursor pagination with
  explicit page and response-size bounds. A client must follow `nextCursor`
  until `complete` instead of treating a truncated first page as complete.
- **Raw retention:** the proposed raw packet/status window is 180 days, with
  longer aggregate retention. Compression and deletion remain independently
  disabled until history inventory, a fresh signed restore receipt, and the
  activation gates in `docs/db-lifecycle.md` pass.
- **Owner webhook egress:** arbitrary public HTTPS destinations are supported,
  but every connection and redirect is revalidated against the outbound
  address policy, bounded by timeout/body limits, and delivered through the
  durable outbox. Deployments may narrow this with an allowlist or egress
  proxy.
- **Operator transport:** browser operators exchange the local bootstrap token
  for a short-lived `Secure`, `HttpOnly`, `SameSite=Strict` session. The bearer
  path remains temporarily for automation; operator routes remain local-only.
- **Mosquitto authority:** the public backend writes no broker files and has no
  Docker API access. A dedicated broker-local helper atomically reconciles ACL
  state and signals Mosquitto.
- **RF service level:** planned/live work uses bounded count and byte admission,
  leased jobs, deadlines, retry budgets, terminal results, and worker
  saturation/freshness metrics. The measured RF correctness and latency budget
  is recorded in `docs/performance-budgets.md`.
- **Planned nodes:** private proposals are owner/operator-only. Public discovery
  reads only an explicit, reviewable publication record with a minimal DTO and
  bounded pagination.
- **Embedded channel material:** the default key in
  `backend/backfill-transport-codes.mjs` is classified as the documented
  MeshCore public-channel key, not a deployment secret. Private channel keys
  are environment-only and must not be committed.

## Compatibility exceptions

Most containers run read-only with all Linux capabilities dropped and
`no-new-privileges`. PostgreSQL/TimescaleDB, Redis, Mosquitto, Loki, Prometheus,
Alertmanager, and Grafana require writable state volumes supplied explicitly by
Compose. Vendor entrypoints that must initialize those volumes retain only the
documented filesystem access needed for that purpose. They are not granted a
Docker socket or host-root mount.

The health-check application remains non-root, read-only, and capability-free.
A network-isolated one-shot initializer holds only `CHOWN`, `DAC_OVERRIDE`, and
`FOWNER` while it normalizes the dedicated persisted-data volume, then exits
before the application starts.

The viewshed and link workers continue to drain the legacy
`meshcore:viewshed_jobs` and `meshcore:link_jobs` lists during the protocol
transition. All new writes use bounded, leased `meshcore:viewshed:v2:*` and
`meshcore:link:v3:*` state. Remove the legacy readers only after queue metrics
show both legacy lists empty through one full retention window.

`MapLibreMap.tsx` remains the sole imperative owner of the MapLibre instance.
Its size is a documented lifecycle-coordination exception, not a home for new
data shaping or UI. The decomposition boundary and extraction rules are in
`docs/frontend-map.md`.
