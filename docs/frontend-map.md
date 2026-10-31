# Frontend Map

## Current structure

- `MapLibreMap.tsx`
  - imperative MapLibre lifecycle, event wiring, and source refresh scheduling
- `mapSourceLayers.ts`
  - source/layer creation and exact layer identifiers
- `geojsonBuilders.ts`
  - pure GeoJSON builders and clash calculations
- `mapConfig.ts`
  - map constants and style config
- `maplibre.ts`
  - configured MapLibre exports; Vite bundles the worker with a content hash
- `basemapHealth.ts` and `basemapFallback.ts`
  - tile-success detection, bounded recovery, and independent backup map
- `NodePopupContent.tsx`
  - popup UI
- `LiveOverlayController.tsx`
  - live path overlay controller
- `DeckGLOverlay.tsx`
  - bounded packet-arc animation outside React map rerenders
- `RfCoverageOverlay.tsx` and `RfCoverageStatus.tsx`
  - native progressive raster layers, tier controls, legend, and progress

## State ownership

- live nodes/packets: `useNodes.ts`
- HopReach metadata/progress and last-known-good state: `useRfCoverage.ts`
- links: `useLinkState.ts`
- overlay/path state: `overlayStore.ts`

## Contributor rules

- do not reintroduce React-driven full-map rerenders for live packet/node traffic
- put pure map data shaping in builder modules, not in the top-level map component
- keep map visibility rules explicit and centralized where possible
- keep RF rasters below labels/nodes, use nearest-neighbour rendering, and
  validate all metadata-provided relative tile paths before adding sources

## Documented lifecycle exception

`MapLibreMap.tsx` is intentionally the single owner of one MapLibre instance.
Splitting its lifecycle across React components would duplicate listeners,
sources, or map construction during mount and Strict Mode. The component may
coordinate lifecycle and call extracted helpers, but it must not absorb new
pure computation, fetch policy, presentation markup, or persistent state.

The allowed responsibilities are:

- construct and destroy the map exactly once per mount
- register and unregister map events
- reconcile extracted source/layer definitions
- bridge the external live stores into bounded frame snapshots
- coordinate popups and imperative camera transitions

New code belongs in `mapSourceLayers.ts`, `geojsonBuilders.ts`, overlay
controllers, hooks, or focused UI components as appropriate. A change that
adds more than one coherent lifecycle responsibility must extract an existing
responsibility in the same slice. Regression tests and the bundle/CSS budgets
guard the boundary; map initialization must remain one instance per mount.

## Basemap reliability

Map constructors must import the configured `maplibre.ts` module. Its
`?worker&url` import makes Vite bundle the worker and shared code into one
content-hashed JavaScript asset. Do not copy MapLibre's fixed `.mjs` filenames
into `/assets`: browser caches and older service workers can otherwise pair a
new renderer with an incompatible worker after an upgrade.

Each map session versions the mutable OpenFreeMap style and `/planet` TileJSON
requests, including retries. This bypasses metadata pinned by earlier service
workers without invalidating immutable tile snapshots. Current service workers
also expire metadata after five minutes and retain one canonical metadata entry
per URL, so session nonces cannot grow the cache indefinitely.

The dashboard starts its watchdog before the style request completes. It
requires a successfully decoded vector tile to report a healthy map;
`isSourceLoaded` only means that requests finished and is also true after
failures. New viewport requests restart monitoring. A stalled attempt retries
once, then uses OSM raster streets with the bundled Natural Earth outline
underneath. Backup streets use normal browser HTTP caching and attribution;
automated tests intercept those requests instead of fetching community tiles.

Run `npm run test:basemap` in `frontend` to build and test the production
worker on desktop and mobile. The tests cover stale worker/metadata URLs,
hanging style requests, tile failures, backup rendering and manual recovery.
The CI browser job runs this check on every change.

### Source graph and release targets (October 2026)

MapLibre remains pinned to 6.4.1. `src/plugins/maplibre-source.ts` reconstructs
its unmodified TypeScript/generated shader sources from the integrity-locked
npm package's source maps. The upstream index-array barrel (which source maps
omit because it has no statements) is reproduced explicitly. No network fetch
or mutable upstream checkout occurs during this step. The version check fails
closed on an unqualified library upgrade. Requalify the emitted worker and
source inventory before changing that pin. The generated cache stays under
`node_modules`; the release includes the upstream BSD licence.

The renderer and module worker now use one build graph, so they share hashed
chunks instead of embedding a second bundled copy of their common code.
Library chunk budgets measure their named chunks; every shared dependency and
worker is also counted by the unchanged all-JavaScript ceiling.

`VITE_BUILD_TARGET=app` emits the dashboard routes; `public-website` and
`dev-website` emit the public/owner routes. An unset target retains the combined
local-development behaviour. Use direct `import.meta.env.VITE_BUILD_TARGET`
access so Vite can eliminate the other host's route tree at build time.
`build-target.json` binds budget checks to the artifact. A website must contain
no dashboard App chunk. Both deployed targets enforce the same JS/CSS ceilings
in CI and in their Dockerfiles; no limit was raised for this change.
