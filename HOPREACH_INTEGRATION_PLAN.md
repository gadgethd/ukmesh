# HopReach Integration with UK-Scale Performance Optimisation

## Summary

Use HopReach as the canonical RF model, while treating UK Mesh's roughly
4,600 positioned repeaters as a separate performance target. Optimisation
work will be benchmarked and validated before production integration. No RF
implementation will be deployed until it matches HopReach's reference
results.

The existing UK Mesh viewshed coverage calculations and polygons will be
retired. HopReach's terrain model, propagation calculations, raster colours,
and coverage assumptions will become the source of truth.

## Performance Changes

- Add a spatial repeater index to the CPU raster path. Each pixel queries only
  a conservative geographic-bin superset, followed by HopReach's exact
  Haversine cutoff. No physically reachable repeater may be omitted.
- Avoid per-pixel allocations by reusing candidate buffers per worker or
  raster row.
- Precompute reusable site coordinates and tile-level candidate lists.
- Compute coverage in resumable geographic chunks. Each chunk must include
  every transmitter within HopReach's link-budget maximum range so its result
  remains equivalent to a monolithic run.
- Publish completed tiles progressively, allowing the map to fill in while
  calculation continues across the rest of the UK.
- Add an optional bulk observed-link endpoint so HopReach can load calibration
  evidence in one bounded request rather than making thousands of individual
  requests. Retain the per-node endpoint as a compatibility fallback.
- Preserve HopReach's propagation, DEM sampling, free-space path loss,
  4/3-earth curvature, knife-edge diffraction, link-budget, and margin
  calculations unchanged.

## Test and Load Plan

- Establish an unmodified upstream baseline before accepting an optimisation.
- Add Go benchmarks for 500-node and UK-sized 4,600-node fixtures at Standard
  and Precision tile sizes.
- Capture CPU, allocation, memory, and tile-completion profiles for the
  upstream reference and optimised implementations.
- Require optimised output to match the upstream/reference implementation
  within a documented floating-point tolerance.
- Add exhaustive and randomised tests proving the spatial candidate index is
  always a conservative superset of the exact Haversine search.
- Test empty areas, dense urban clusters, tile boundaries, national borders,
  high latitudes, maximum-range paths, and repeaters immediately either side
  of a chunk boundary.
- Load-test the UK compatibility API with realistic node pagination,
  concurrent calibration requests, cold-cache operation, and cache hits.
- Run browser tests with thousands of repeater features and progressively
  arriving coverage tiles. Map movement and layer controls must remain
  responsive throughout.
- Use a release gate requiring a materially lower measured raster time and
  memory footprint than upstream on the UK fixture. If the first spatial index
  is insufficient, continue profile-guided optimisation before rollout rather
  than reducing RF fidelity.

## HopReach Source and Data Integration

- Vendor HopReach v0.1.32 at upstream commit
  `61efac0b4678f55496fe08f53eda0c79eb18655b`.
- Keep upstream RF-domain code identifiable and isolate UK-specific adapters
  and orchestration changes to reduce future merge conflicts.
- Add an internal UK Mesh compatibility API supplying HopReach's expected
  repeater, scope, and observed-link formats.
- Build calibration evidence only from genuinely observed packet paths and
  links. Do not feed the rejected predicted coverage data back into HopReach.
- Use a versioned UK operational boundary covering Great Britain, Northern
  Ireland, the Isle of Man, and the Channel Islands. Do not reuse the old
  viewshed coverage calculation or its operational boundary assumptions.
- Keep HopReach and derived RF integration files under its AGPL-3.0 plus
  Commons-Clause terms, preserve its notices, and publish the exact deployed
  corresponding source.

## Coverage Calculation and Scheduling

- Calculate and publish Standard coverage first using HopReach's 2,000-pixel
  setting.
- Continue Precision coverage in the background at 6,000 pixels, DEM zoom 13,
  and 2x supersampling after Standard is available.
- Leave calibrated variants disabled until observed-link calibration has been
  validated against representative UK paths.
- Run calculations under a singleton lock with resumable checkpoints,
  persistent terrain caching, bounded CPU and memory, restart protection, and
  nightly refreshes.
- Preserve last-known-good tiles during a recomputation. A partial or failed
  run must never blank the live layer.
- Require sufficient free disk space before Precision begins and place
  HopReach DEM/output data in a dedicated persistent volume.

## Map and Interfaces

Integrate coverage natively into the existing React and MapLibre map. The
initial release includes coverage only; HopReach's planner, KML tools, plan
sharing, LOS workspace, and flood simulator are out of scope.

The map will consume:

- `GET /rf-coverage/meta.json`
- `GET /rf-coverage/progress.json`
- `GET /rf-coverage/tiles/{tier}/{tile}.png`

The metadata will preserve HopReach-compatible coverage fields and add
backward-compatible run information: run ID, model/source version, completed
and total tiles, tier state, and failure state.

The native UI will provide:

- An `RF Coverage` map toggle.
- Standard and Precision detail selection when each tier is available.
- HopReach's orange-to-green signal-margin legend.
- Frequency, assumptions, generated time, and model/source details.
- A live progress banner with stage, backend, percentage, and ETA.
- Automatic partial-tile refresh so users can watch coverage fill in.

Coverage tiles will use nearest-neighbour rendering and remain below roads,
place names, node markers, and other interactive map labels. Once the first
Standard tile exists, coverage will be enabled for a new browser session;
later manual visibility choices will be persisted.

## Retirement of the Old RF Map

- Stop the old viewshed coverage worker and disable new coverage queue
  production.
- Remove old coverage consumption from the frontend, WebSocket handlers, and
  live API paths.
- Return `410 Gone` from the rejected coverage and planned-coverage contracts
  during the rollback window.
- Retain the `node_coverage` tables and previous production images for one
  release solely as a rollback path; the live application must not read them.
- Preserve the separate observed-link worker and `node_links`, because these
  provide real calibration evidence rather than the rejected coverage map.

## Production Rollout and Cleanup

- Publish the exact licensed source before deploying HopReach-derived code.
- Build immutable revision-labelled backend, HopReach, and frontend images.
- Deploy the compatibility API and background calculator first, followed by
  the native map UI.
- Expose the toggle immediately with progress state; make coverage visible as
  soon as the first valid Standard tile is atomically published.
- Verify the live site makes no requests to the old coverage API and that no
  viewshed coverage worker remains running.
- Remove only verified test containers, temporary repository clones, browser
  test output, obsolete test/audit RF images, and abandoned caches.
- Retain production rollback images, database data, and unrelated user work.

## Acceptance Criteria

- Standard coverage becomes visible progressively on `app.ukmesh.com` while
  the calculation continues in the background.
- The RF toggle, tier selection, legend, progress reporting, and partial-tile
  updates work on desktop and mobile.
- Optimised raster results match the upstream HopReach reference tests.
- UK-scale benchmarks demonstrate a material speed and memory improvement
  without reducing terrain resolution, range, node count, or RF fidelity.
- A stopped or restarted calculator resumes safely and keeps last-known-good
  tiles live.
- Precision begins only after Standard is live and resource checks pass.
- No old UK Mesh viewshed geometry is displayed or used in the new pipeline.

## Assumptions

- UK Mesh remains non-commercial and will publish the complete deployed
  HopReach-derived source.
- The initial production feature is coverage-only: Standard first, then
  Precision in the background.
- Scope-specific controls remain hidden when the source data contains no
  reliable region membership rather than inventing membership.
- RF accuracy takes priority over a performance shortcut; optimisations must
  remove unnecessary work without changing HopReach's physical model.
