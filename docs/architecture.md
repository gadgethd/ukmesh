# Architecture

`meshcore-analytics` has five runtime domains:

- `backend`: MQTT ingest, HTTP API, WebSocket fan-out, database access, owner
  sessions, and path-resolution orchestration;
- `frontend`: React application and native MapLibre live/RF layers;
- `hopreach`: canonical whole-region terrain propagation and progressive RF
  raster publication;
- `link-worker`: observed relay-path processing into `node_links`;
- operations and edge: Mosquitto, TimescaleDB, Redis, Nginx/Anubis,
  Prometheus, Alertmanager, Grafana, Loki, Alloy, and bounded exporters.

## HopReach boundary

The complete HopReach v0.1.32 source is vendored at
`third_party/hopreach`, with the exact upstream revision recorded in
`UPSTREAM_COMMIT`. The identical derived source is public as
[`v0.1.32-ukmesh.2`](https://github.com/gadgethd/hopreach/tree/v0.1.32-ukmesh.2)
at commit `f497b3fb72644aa1fb5f5fcce3fe2afca78bdaf6`. The unmodified upstream CPU
raster remains an executable accuracy oracle. The production CPU path retains
the same propagation equations, DEM samples, free-space path loss, 4/3-earth
curvature, knife-edge diffraction, link budget, and margin calculation while
using a conservative site index, batched terrain sampling, and factored
path-invariant calculations. Other UK-specific work covers chunk/progressive
orchestration, the internal data adapter, versioned boundary, deployment
profile, and native map consumer.

The private backend router at `/hopreach` emits CoreScope-compatible repeaters
and observed reach evidence. It accepts only internal, non-forwarded traffic;
Nginx does not proxy it. Scope fields remain empty because UK Mesh has no
reliable region-membership source. No predicted geometry enters calibration.

```text
positioned UK repeaters + observed node_links
                 │
                 ▼
 backend /hopreach compatibility API
                 │ paginated/bounded
                 ▼
 HopReach ── DEM cache + checkpoint + singleton/nightly scheduler
                 │ atomic Standard first, then gated Precision
                 ▼
 rf_coverage_data (last-known-good + current progressive tiles)
                 │ read-only mount
                 ▼
 app Nginx /rf-coverage/* ── React/MapLibre RF overlay
```

The app can fetch only `meta.json`, `progress.json`, and numeric PNG tile
paths. Coverage is not sent through backend JSON or WebSocket messages.

## Backend domain layout

- `backend/src/api/`: thin route modules, including the internal HopReach
  compatibility boundary and static `410 Gone` legacy coverage contracts;
- `backend/src/repositories/`: SQL for nodes, topology, RF validation, owner
  alerts, registration, and operator workflows;
- `backend/src/operations/`: local operator, link-queue, registration, model,
  and audit services;
- `backend/src/db/`: pool setup, base schema, and migrations;
- `backend/src/pathing/` and `backend/src/path-beta/`: pathing orchestration,
  resolver implementation, pool, and caches;
- `backend/src/stats/` and `backend/src/owner/`: stats and owner domains.

## Frontend domain layout

- `frontend/src/components/Map/MapLibreMap.tsx`: sole imperative MapLibre
  lifecycle owner;
- `frontend/src/hooks/useRfCoverage.ts`: last-known-good metadata/progress
  polling and safe tile validation;
- `frontend/src/components/Map/RfCoverageOverlay.tsx`: native image sources and
  nearest-neighbour raster layers below labels and interactive layers;
- `frontend/src/components/Map/RfCoverageStatus.tsx`: tier controls, legend,
  model details, and progress/failure state;
- `frontend/src/hooks/useNodes.ts` and `useLinkState.ts`: live node, packet, and
  observed-link stores;
- `frontend/src/store/overlayStore.ts`: path, replay, selection, and dormant
  rollback-window planning state.

## Data flow

1. MQTT packets enter the backend, are privacy-normalized, persisted, and
   published as bounded live messages.
2. The frontend stores live node/packet/link updates without routing them
   through `App` state.
3. The link worker independently derives genuine observed `node_links`.
4. On its nightly schedule, HopReach pages positioned repeaters and optionally
   loads observed evidence through the private adapter.
5. Standard tiles are atomically published as each completes. Metadata points
   to last-known-good tiles throughout recomputation and restart.
6. After Standard is live and disk checks pass, Precision publishes in the
   same manner.
7. The app polls small metadata/progress documents and refreshes only completed
   raster tiles. Synthetic journeys and Prometheus/Alertmanager provide
   independent operational evidence.

## Operational rules

- application startup does not run historical backfills;
- liveness remains independent of MQTT readiness; dependency checks use
  `/readyz`;
- anonymous APIs use closed privacy DTOs and route modules remain thin;
- RF fidelity takes priority over performance: the upstream oracle, parity
  suite, and benchmark gate must pass before release;
- calibrated RF remains disabled until validated against representative UK
  paths;
- immutable releases use signed digest references and publicly available
  corresponding source;
- the HopReach output/DEM volume is persistent and Precision is resource-gated;
- only one calculator may run, and recovery resumes its durable checkpoint;
- the old coverage producer, worker, frontend, WebSocket message, and live API
  reads are disabled. `node_coverage`, old images, and the old implementation
  remain for one release only as an inactive whole-release rollback path;
- `link-worker` and `node_links` remain live because they represent observed
  evidence, not rejected viewshed geometry.

Most containers run read-only with Linux capabilities dropped and
`no-new-privileges`; stateful vendor containers receive only their explicit
volumes. `MapLibreMap.tsx` remains a documented lifecycle-coordination
exception; extraction rules are in `docs/frontend-map.md`.
