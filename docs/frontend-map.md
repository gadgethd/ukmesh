# Frontend Map

## Current structure

- `MapLibreMap.tsx`
  - imperative MapLibre lifecycle, event wiring, and controller coordination
- `liveMapSources.ts`
  - live-store subscriptions, source invalidation, coalescing and presence expiry
- `NodeDock.tsx` and `useNodeLos.ts`
  - node detail presentation and cancellable selected-node LOS work
- `features/map/`
  - pure map/LOS models and scoped node-link loading
- `useLegacyPlanner.tsx`
  - isolated inactive planning compatibility retained for rollback
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

- live nodes/packets: `features/realtime/nodeStore.ts`, adapted by `useNodes.ts`
- HopReach metadata/progress and last-known-good state: `useRfCoverage.ts`
- links: `features/realtime/linkStore.ts`, consumed by map controllers
- overlay/path state: `overlayStore.ts`

## Contributor rules

- do not reintroduce React-driven full-map rerenders for live packet/node traffic
- put pure map data shaping in builder modules, not in the top-level map component
- keep map visibility rules explicit and centralized where possible
- keep RF rasters below labels/nodes, use nearest-neighbour rendering, and
  validate all metadata-provided relative tile paths before adding sources

## Documented lifecycle exception

`MapLibreMap.tsx` owns the dashboard's MapLibre instance.
Splitting its lifecycle across React components would duplicate listeners,
sources, or map construction during mount and Strict Mode. The component may
coordinate lifecycle and call extracted helpers, but it must not absorb new
pure computation, fetch policy, presentation markup, or persistent state.

Public `RegionsMap` and `TopologyMap` use `EmbeddedMap.tsx` for their basemap
lifecycle and retain their native overlay sources, controls, selection and
style-reload handling. This base has no dashboard store subscriptions, deck.gl
layers or recurring source refresh. Resize work is coalesced in an animation
frame; load/recovery deadlines are bounded and removed with the map. Scope and
privacy changes replace the instance. Each instance has one lifecycle owner.
The stats decoded-path map is loaded only after selection; the feed path and
owner maps retain their separate standalone lifecycles.

The allowed responsibilities are:

- construct and destroy the map exactly once per mount
- register and unregister map events
- reconcile extracted source/layer definitions
- bridge the external live stores into bounded frame snapshots
- coordinate popups and imperative camera transitions

The live-source controller owns its subscriptions and timers. Its lifecycle is
started once per mounted coordinator, cancelled on cleanup and reset after a
style replacement. Controllers receive the existing map; they never construct a
second map. Node-link requests, LOS calculations and dock UI have separate
owners, while inactive planning remains behind its retained compatibility hook.

Scope identity includes the resolved network, observer, region and privacy
generation. Scoped resource owners reject obsolete completions and abort work
on session invalidation. An individual component detaches from shared work;
physical requests continue to count against admission limits until they settle.
Former-name search receives the same selected region as the map.

New code belongs in `mapSourceLayers.ts`, `geojsonBuilders.ts`, overlay
controllers, hooks, or focused UI components as appropriate. A change that
adds more than one coherent lifecycle responsibility must extract an existing
responsibility in the same slice. Regression tests and the bundle/CSS budgets
guard the boundary; map initialization must remain one instance per mount.

## Basemap reliability

Map constructors must import the configured `maplibre.ts` module. Production
resolves a virtual module-worker entry into the shared Vite graph;
development uses the upstream `?worker&url` entry. Both give the worker a
content-hashed URL. Do not copy MapLibre's fixed `.mjs` filenames
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

MapLibre is pinned to 6.11.2. `src/plugins/maplibre-source.ts` reconstructs
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
no dashboard App chunk. Both targets enforce JS/CSS ceilings in CI and in their Dockerfiles.

The November candidate preserves MapLibre 6.11.2 and deck.gl 9.4 while meeting
2,900 KiB raw / 850 KiB gzip for all JavaScript, including worker/shared chunks.
The temporary 3,200/900 allowance has been removed. Renderer (820/225), charts
(400/110) and CSS (230/42) ceilings remain unchanged. Terser and compact code
generation preserve legal notices. See [the current budgets and measurements](performance-budgets.md)
and requalify both targets and production browser journeys after build changes.

## Polling ownership

Cadences and request deadlines are centralized in
`frontend/src/config/polling.ts`. `utils/visibilityPoller.ts` owns the timer,
visibility, cancellation and backoff state machine; `useVisibilityPoll` adapts
it to React. Runtime privacy authority uses that same scheduler without jitter
and with its server-provided bounded interval.

`useRfCoverage` polls when coverage is shown or a selected node request needs
progress. It pauses when hidden, refreshes on return and slows completed work
to the idle interval. An initial discovery request preserves automatic network
coverage availability when the user has not set a layer preference. Explicitly
hiding coverage prevents unused repeated discovery.

## Presence source invalidation

Node-store snapshots remain immutable. Each change carries its touched keys and
the preceding snapshot identity; an unmatched identity or authoritative snapshot
forces full invalidation. Ordinary newer timestamps on already-online, fresh
public nodes update the store and popup without replacing native sources.
Changed geometry/metadata, private-mask movement and revived stale/hidden nodes
still rebuild all dependent sources. A single deadline timer handles the next
14-day stale or 28-day hidden boundary and is cleared on map cleanup. Native
features omit reception timestamps; UI details read exact values from the store.

Connection-status timestamp publications are coalesced to at most once a second,
with a trailing exact last-reception value. Socket messages and store updates
are delivered immediately through the existing coordinator. This status timer
is cancelled on connection/scope cleanup.

### React profiling

The November comparison used isolated production profiling builds with React's
[Profiler](https://react.dev/reference/react/Profiler), Chromium 153 and the same
invented grid. Only the four map/store/socket implementation files were reverted
for the baseline. Each case used three fresh browser contexts, three seconds
of warm-up, six seconds of roughly 500 updates/s and 1.5 seconds to drain.
Mobile used four-times CPU throttling. Every sent update was received and each
touched node retained its latest timestamp.

Median React render work associated with committed updates, normalized per
1,000 received updates, was:

| Nodes | Desktop before → after | Mobile before → after |
| ---: | ---: | ---: |
| 1,000 | 115 → 53 ms | 373 → 235 ms |
| 3,000 | 233 → 128 ms | 807 → 497 ms |
| 6,000 | 399 → 231 ms | 1,124 → 1,083 ms |

The mobile 6,000-node render change is small; it does not establish a material
React speedup there. Native frame p95 at 6,000 nodes fell from 66.6 to 16.8 ms
desktop and 150 to 66.7 ms mobile. Profiling adds overhead, so these figures
are separate from uninstrumented frame/CPU measurements. `actualDuration`
measures render work, not DOM mutation or effect duration. Profiling globals
and the special renderer exist only in the ignored measurement copies.

Immutable store snapshots remain in place. A broader per-key subscription or
structural-sharing rewrite was not selected by this change; the mobile residual
cost remains a measured limit rather than a reason to mutate React snapshots.
