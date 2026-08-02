# Frontend performance and resource budgets

Status: approved release guardrail

Baseline date: 2026-07-29

Baseline revision: `b9b2ecdbfaa9688a79a62ccee38d6b406f14deaa`

These limits turn the Phase 0/6 performance requirements into release gates.
They are ceilings, not allocation targets. A change that needs a higher limit
requires a measured before/after result and explicit review; the default
tolerated regression is 10%.

## Browser resources

| Resource | Limit | Enforcement |
| --- | ---: | --- |
| Decoded terrain tiles | 16 entries and 32 MiB | `TerrainTileCache`; closes every evicted bitmap |
| Service-worker tile cache | 6,000 responses and 96 MiB estimated body bytes | persisted metadata and amortized pruning |
| General scoped browser caches | 128 MiB combined estimated bytes | per-cache TTL/LRU/byte limits; debug snapshots |
| Map/terrain soak | less than 10% retained-heap growth after warm-up over 30 minutes | Playwright/DevTools staging soak |
| Map construction | one main map and one owner map per mounted page | browser construction-count regression |

## Requests and jobs

| Work | Limit |
| --- | ---: |
| Common same-origin API concurrency | 8 |
| Terrain image decode concurrency | 4 |
| Feed selected-path concurrency | 1 |
| Link-quality sparkline concurrency | 2, with a 16-item queue |
| Repeater detail concurrency | 4 |
| HopReach metadata/progress polling | 3 seconds while the RF feature is enabled |
| HopReach compatibility page | 500 nodes, 100,000 maximum offset |
| HopReach bulk evidence request | 5,000 keys and 250,000 result rows |
| Ordinary API deadline | 15 seconds |

Requests are scope-fenced by network, observer, privacy generation, and owner
session where applicable. Hidden pages schedule no polling and perform one
refresh when visible again.

## Rendering

| Work | Limit |
| --- | ---: |
| Topology representative fixture | 600 nodes / 1,200 links |
| Topology React snapshots | at most one every 66 ms while visible |
| Representative topology frame time | p95 below 16.7 ms after simulation warm-up |
| Expandable list details | outside fixed-height virtual rows; no overlap at 375, 768, or 1,280 CSS px |
| Coarse-pointer controls | 44 CSS px where layout permits; otherwise WCAG target spacing applies |

## Production frontend assets

All byte limits below use the output of `vite build`. “Raw” means minified
bytes and “gzip” is computed with Node's default gzip settings.

| Asset group | Raw limit | Gzip limit |
| --- | ---: | ---: |
| Main `App-*` chunk | 900 KiB | 260 KiB |
| `maplibre-gl-*` chunk | 820 KiB | 225 KiB |
| Recharts categorical chunk | 400 KiB | 110 KiB |
| All JavaScript | 2,900 KiB | 850 KiB |
| All CSS | 230 KiB | 42 KiB |

Run `npm run budget` after `npm run build`. CSS has a separate structural
gate: no duplicate selector in the same at-rule/cascade context and no
dashboard/page selector in `styles/globals.css`. Responsive declarations in a
distinct media query are intentional and are not counted as duplicates.

## HopReach RF calculator

The indexed CPU path must match the executable upstream v0.1.32 oracle within
`1e-5 dB`. Conservative-superset tests cover exhaustive and randomized
geography, poles, the antimeridian, borders, dense/empty areas, chunk edges,
and maximum-range paths.

| Measure | Release limit | 2026-08-02 local result |
| --- | ---: | ---: |
| UK-4,600 Standard raster-time ratio | no more than 0.70 of upstream | 0.602 |
| UK-4,600 Precision-tile time ratio | no more than 0.70 of upstream | 0.594 |
| Standard peak terrain working-set ratio | no more than 0.70 of upstream | 0.453 |
| Precision peak terrain working-set ratio | no more than 0.70 of upstream | 0.135 |
| Maximum reference parity error | 0.00001 dB | within tolerance |

The measured times are three-run means with identical dimensions and
`GOMAXPROCS=4`. The working-set gate includes unchanged 100 km range padding
around publication tiles and uses the exact float32 Web-Mercator DEM tile
accounting from `demgrid.Load`. `scripts/benchmark-hopreach.sh` captures
allocation output, independent CPU/heap profiles, and a progressive completion
trace. Full evidence and raw-value interpretation are in
`rf-coverage/BENCHMARKS.md` and `docs/rf-coverage-rollout.md`.

## Rollout evidence

Before a browser-cache, map, topology, or chart release:

1. Record the production asset output and `npm run budget` result.
2. Run desktop/mobile Playwright plus axe.
3. Run `SOAK_BASE_URL=https://test.ukmesh.com npm run soak:production` for the
   30-minute terrain/map soak. The harness records post-GC heap, cache/storage
   snapshots, long tasks, request failures, and MapLibre construction count as
   signed deployment evidence.
4. Roll back on privacy drift, cross-scope state, reload loops, cache quota
   overflow, or a limit above. Retain the prior service-worker namespace for
   seven days.
