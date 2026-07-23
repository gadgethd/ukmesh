# Architecture

`meshcore-analytics` is split across three main runtimes:

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

## Backend domain layout

- `backend/src/api/`
  - thin HTTP route modules and bootstrap wiring
  - bounded viewport coverage and recent topology contracts
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
- `frontend/src/components/app/NodeDetailDrawer.tsx`
  - selected-node details and mobile bottom sheet
- `frontend/src/components/app/TimelineControl.tsx`
  - bounded historical activity replay controls
- `frontend/src/components/app/PlannerComparison.tsx`
  - saved scenario comparison, sharing, and overlap estimates
- `frontend/src/hooks/useWatchlist.ts`
  - bounded browser-local saved searches and watchlist entries
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

## Operational rules

- app startup must not run heavy historical backfills
- liveness must remain independent of MQTT readiness; use `/readyz` for dependency checks
- route modules should stay thin
- repositories own SQL
- services own orchestration and shaping
- worker RF math should stay isolated from queue orchestration
