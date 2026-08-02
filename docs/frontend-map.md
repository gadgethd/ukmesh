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
