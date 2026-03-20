# Frontend Map

## Current structure

- `MapLibreMap.tsx`
  - imperative map lifecycle and source refresh scheduling
- `geojsonBuilders.ts`
  - pure GeoJSON builders and clash calculations
- `mapConfig.ts`
  - map constants and style config
- `NodePopupContent.tsx`
  - popup UI
- `LiveOverlayController.tsx`
  - live path overlay controller

## State ownership

- live nodes/packets: `useNodes.ts`
- coverage: `useCoverage.ts`
- links: `useLinkState.ts`
- overlay/path state: `overlayStore.ts`

## Contributor rules

- do not reintroduce React-driven full-map rerenders for live packet/node traffic
- put pure map data shaping in builder modules, not in the top-level map component
- keep map visibility rules explicit and centralized where possible
