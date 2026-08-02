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
| Planned coverage polling | 2 seconds initially, visibility gated, one request at a time |
| Planned coverage terminal deadline | 120 seconds |
| Ordinary API deadline | 15 seconds; map/planned job calls may use their documented tighter deadline |

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

## RF worker

The representative prefix fixture uses 24 rays with 1,000 terrain steps per
ray. The vectorized and legacy implementations must remain numerically
identical on that fixture and the model-v7 golden path-loss fixtures must pass.

| Measure | Release limit | 2026-08-02 exact-image result |
| --- | ---: | ---: |
| Batched p50, 24 rays total | informational | 126.775 ms |
| Batched p50 per ray | informational | 5.282 ms |
| Batched p95 per ray | 250 ms | 5.449 ms |
| Peak worker RSS | 1,024 MiB | 103.016 MiB |
| Maximum parity error | 0.05 dB | 0.000 dB |
| Median speedup over legacy | informational | 14.84x |

The measurement came from `viewshed-worker/tests/benchmark_rf.py` in local
image `sha256:64a25c84d44127b1610db39858651ba1a7fbba62a07bb4b3b56fb2949848f70f`,
built from the digest-pinned GDAL base. All 28 worker tests passed with an
isolated Redis instance in the same image.

The maximum synthetic radial fixture uses 360 rays with 1,000 terrain steps
per ray. `viewshed-worker/tests/benchmark_rf_radial.py` measured 1.848 seconds
p50, 1.860 seconds p95, and 161.777 MiB peak RSS across three repetitions in
the same image. Every green, amber, and red boundary contained 361 points,
including the closing point. CI runs a smaller 72-ray by 500-step version so
pull requests exercise the complete radial path without turning CI into a
host-speed contest.

These are kernel budgets, not a promise about end-to-end job latency. Terrain
tile downloads, VRT construction, DTM filtering, database access, and host
contention remain outside the synthetic fixtures. See
`docs/rf-coverage-rollout.md` for end-to-end pilot evidence and the rollout
gate.

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
