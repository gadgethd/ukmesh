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
| `maplibre-*` renderer chunks (excluding worker entry) | 820 KiB | 225 KiB |
| Recharts categorical chunk | 400 KiB | 110 KiB |
| All JavaScript | 2,900 KiB | 850 KiB |
| All CSS | 230 KiB | 42 KiB |

Run `npm run budget` after `npm run build`. CSS has a separate structural
gate: no duplicate selector in the same at-rule/cascade context and no
dashboard/page selector in `styles/globals.css`. Responsive declarations in a
distinct media query are intentional and are not counted as duplicates.

### Build strategy and measurement

The November 2026 build uses Vite's Terser integration with two workers,
two compression passes and ES2020 output, followed by Rolldown's compact
code generation. Both stages preserve legal notices. A parser removes exact
duplicate notices within each chunk; every distinct notice remains in that
chunk, and the complete MapLibre license is also emitted as an asset.
The previous script's 3,200/900 KiB JavaScript allowance has been corrected to
the approved 2,900/850 limits above.

MapLibre and the deck overlays use WebGL2. The resolver selects deck.gl 9.4's
supported `visgl:webgl-only` exports while retaining Vite's default client
conditions. This excludes unused WebGPU branches. A future WebGPU renderer or
dependency upgrade must qualify those conditions and the emitted worker again.
Development continues to use the upstream MapLibre module and worker.

On the same four-CPU Linux VM, Node 26.13.1 and locked dependencies, three
sequential public builds improved from a 13.29 s median to 8.66 s (35%). Each
set began without TypeScript's incremental cache; later builds reused it.
The cache lives under `node_modules/.cache` and can be deleted safely.
GNU time reported up to 1.59 GiB peak process RSS, versus 1.11 GiB before;
parallel compression trades memory for wall time. Avoid competing build or
benchmark loads on a 4 GiB machine. The production Playwright server setup
already builds its three targets sequentially.

The public artifact measured 2,899.5/830.7 KiB raw/gzip JavaScript and the
dashboard 2,705.0/767.2 KiB, with unchanged byte ceilings. The public raw
margin is small: every subsequent change must rerun the budget. These are
local build measurements, not a claim about whole-workflow CI duration.
Measure that separately from Actions job/step timestamps and cache-hit logs.

For a comparable local result, stop development watchers, hold the target and
dependencies fixed, and run this from `frontend` on Linux. Retain all three
times and the budget output; the first build includes cold TypeScript state.

```bash
rm -f node_modules/.cache/tsconfig.tsbuildinfo
for run in 1 2 3; do
  env VITE_BUILD_TARGET=public-website VITE_APP_HOSTNAME=app.ukmesh.com VITE_SITE=ukmesh \
    /usr/bin/time -f 'wall=%e user=%U system=%S max-rss-KiB=%M' npm run build
done
npm run budget
```

### CI isolation and cache reuse

The HopReach RF gates and monitoring validation run in separate Actions jobs.
Neither needs the live Compose browser fixture. The RF job keeps its compiler
and module caches shared between the parity suite and the performance gate;
its benchmarks run on a separate runner from the browser journeys. The live
Compose smoke checks and browser suites remain together, with sequential
frontend builds and explicit cleanup of their shared fixture.

Compare whole-run and job/step timestamps separately from local build timings.
Successful Actions runs before and after the split had critical jobs of 1,856
and 1,434 seconds (31 versus 24 minutes). This operational comparison also
includes two browser fixture timing corrections, and excludes queue time.
All seven jobs passed; production journeys passed 55 tests without retries,
with six region, two development-renderer and six basemap tests also passing.
Aggregate runner time can rise slightly because the isolated jobs repeat setup.
Retain npm and BuildKit cache-hit evidence with the before/after comparison.

### Stats route delivery

The initial stats dependency closure is 213.8 KiB gzip, versus the qualified
460.3 KiB baseline (54% lower). The enforced route ceiling is now 230 KiB;
the global 2,900/850 KiB ceilings remain unchanged. The public artifact after
this change measures 2,892.6/828.0 KiB raw/gzip JavaScript.

Six fresh Chromium 153 navigations with the populated CI fixtures transferred
227,086 script bytes each. Median stats LCP was 3.47 seconds mobile and 0.82
seconds desktop; CLS was 0/0.063 and TBT 3.04/2.68 seconds. All six had
accessibility and SEO scores of 100. These pass the existing navigation gates;
they do not measure field INP or establish a latency improvement against a
different data fixture.

The complete decoded-path dialog loads on selection, including its dialog
framework, while its renderer remains separately lazy. The existing map
boundary supplies loading/error state with a close action; closing restores
the invoking control's focus. Cold-page requests must include neither dialog
nor map/deck modules, and chunk failures must leave the stats page usable.

Stats uses its fixed tab set with native buttons and the
[WAI-ARIA tabs pattern](https://www.w3.org/WAI/ARIA/apg/patterns/tabs/): labelled
tablist/panels, one tab stop, automatic selection, wrapping left/right arrows,
Home/End, and a focusable active panel. URL state and active-section request
cancellation remain intact. The shared React Aria controls remain available to
node popups; this change is scoped to the stats page.

Deferring only the dialog was not enough: shared tab-library code remained in
the cold route and that trial exceeded the global artifact ceiling. The
qualified combination removes those initial dependencies without raising a
budget or removing accessibility assertions.

### Served compression

The locally built website image uses Nginx 1.31.8 from the pinned base. Its
compiled modules include gzip/gzip-static; neither the compiled arguments nor
the installed dynamic modules include Brotli. The updated `brotli-libs` package
is a security dependency and does not enable an HTTP compression module.

A loopback-only check of the actual image served `/stats` as 3,145 bytes with
identity encoding and 959 bytes with gzip, with `Vary: Accept-Encoding` in both
responses. A Brotli-only request received identity; `br, gzip` received gzip.
Gzip remains the qualified origin behavior. These checks do not establish what
an external CDN adds; production edge behavior needs separately authorized
cutover verification. No Brotli module or CDN setting was changed.

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

CI shares temporary Go compiler/module caches between its parity and RF
benchmark containers. The parity command uses `-count=1` so test results are
executed again; compiled dependencies can still be reused. Profile executables
and traces are written under `HOPREACH_PROFILE_DIR`, outside the vendored
checkout. The vendored source is unchanged.

With the pinned Go 1.23 image, two CPUs and a 2 GiB limit, three propagation
test-compilation trials took a 12.35 s median with fresh caches and 0.64 s
with the compiler cache populated. The traced compiler/assembler action count
fell from 255 to zero. This compile-only experiment used `-run '^$'`; it
does not replace the full parity suite or any RF runtime/performance gate.
Whole-workflow timings also include image builds, browser setup and audits,
so compare their individual steps and cache logs before attributing a change.

The deployed app/website Dockerfiles already share identical BuildKit dependency
and runtime-base layers. CI logs confirm cached `npm ci` layers for both
frontend targets. Their separate build entry points remain in place; no new
shared-image publication or build argument contract is needed for that reuse.

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

Public route ceilings are also checked from the emitted Vite manifest by
`npm run budget:routes`. The stats route must have no static MapLibre/deck import;
its decoded-path map is loaded on selection. These route checks complement the
whole-artifact ceilings above.

`npm run lighthouse:ci` checks three fresh mobile and desktop navigations for
home, docs, stats, feed and regions. It serves the production public artifact on
owned loopback port 4186 and uses invented HTTP, WebSocket and basemap fixtures. Recent timestamps are
relative to each request so the populated feed stays within its real time window.
It checks median LCP, CLS and TBT plus accessibility and SEO scores; raw reports
are retained as CI artifacts. The initial navigation ceilings are in
`frontend/scripts/lighthouse-ci.config.json` and are tightened after qualified
structural changes. CI fixtures cover populated rendering; the local sample-stack
benchmark separately measures backend work, real payload sizes and map providers.
Navigation audits do not measure INP. Field Web Vitals need real user interactions
on an authorized deployment.

The public build uses the standard [web-vitals library](https://github.com/GoogleChrome/web-vitals)
to report document LCP/CLS and observed INP through bounded same-origin
telemetry. Reports use the initial fixed public route and omit attribution and
identifiers; soft-navigation metrics are disabled consistently across browsers.
The [operations guide](operations.md#metrics) describes quotas, opt-out,
monitoring panels and p95/cache-hit warnings. Local browser interactions verify
transport; production delivery and field distributions require deployment.

The lightweight regions/topology base imports the native MapLibre stylesheet.
The earlier wrapper omitted it on those public routes, leaving native controls
without their required styles. Regions therefore includes about 10.7 KiB more CSS
than the first baseline; its route ceiling records that restoration. The total
CSS artifact limits still apply. Public native labels use the provider's
[Noto Sans glyphs](https://github.com/hyperknot/openfreemap-styles).

Regions and topology load their map renderers behind local Suspense boundaries.
Their headings, controls and rankings can render while a renderer downloads;
the loading/error placeholder reserves the same map height. A failed chunk leaves
the rest of the page available and offers a reload. Static route-graph budgets
exclude that dynamic renderer, so they are not the total bytes for an interactive
map visit. Lighthouse still exercises the populated route and its map work.
Renderer mounting yields one painted frame to the static content before the
download/evaluation starts; it uses animation frames and cancels on unmount.

The stats page requests `GET /api/stats/charts?sections=overview` initially and
selects `traffic`, `observers`, `paths` or `signal` when its tab changes. The API
accepts up to five distinct comma-separated names, always includes summary and
snapshot metadata, and preserves the complete response when `sections` is absent.
The full canonical snapshot remains the only persisted/cached representation;
projection happens inside the fresh delivery privacy fences. Selected responses
identify section, network, observer and generation. The browser validates nested
values and this identity, cancels obsolete tab/scope requests, and clears old data
and decoded-path selection when authority changes. Live observer polling runs
only on the Observers tab. Stats HTTP responses require revalidation; runtime
configuration and owner/session reads retain `no-store`.

Polling cadences and deadlines live in `frontend/src/config/polling.ts`. The
shared visibility scheduler aborts reads when hidden and refreshes immediately
on return, with bounded backoff and no overlapping requests. Runtime authority
uses the server's 5–300 second cadence and still fails closed on transport or
deadline failure. RF metadata/progress polls run every 3 seconds for visible
calculations, slowing to 30 seconds when complete. Unused layers stop polling;
a visible selected node calculation continues until completion even with the
layer off. One initial metadata discovery preserves automatic availability
when no layer preference was set. Tag enrichment also pauses and cancels across
visibility/scope changes; owner/session freshness and `no-store` remain intact.

Both preloaded fonts appear above the fold (body and navigation/brand). They
use `font-display: swap` and build to content-addressed `/assets/` URLs covered
by the existing immutable nginx policy. Stable `/fonts/` copies remain available
for prior clients during rollback retention. The existing 1200×630 OG PNG is
under 100 KiB; no re-encoding is required for this phase.
