import type maplibregl from 'maplibre-gl';

/**
 * deck.gl's MapboxOverlay (9.3.x) reads maplibre internals that maplibre-gl v6
 * removed as part of its camera-composition refactor:
 *   - map.transform.height      (free-camera viewport height)
 *   - map.transform.elevation   (terrain camera elevation)
 *   - map.transform._nearZ/_farZ (near/far clipping planes)
 * Upstream tracking: visgl/deck.gl#10501 ([RFC] Support MapLibre GL JS v4.5.1
 * through v6 in MapboxOverlay) — still OPEN, no released deck.gl version
 * supports maplibre v6 yet. Without a compatible surface, mounting a
 * MapboxOverlay with terrain active throws `Cannot read properties of
 * undefined (reading 'elevation')` and the whole view hits the error boundary.
 *
 * This gives deck a minimal compatible `transform` object when the map does
 * not have one. Remove once deck.gl ships maplibre-v6 support (or when this
 * repo upgrades @deck.gl to a version that has it).
 */
export function ensureMapboxOverlayCompatibility(map: maplibregl.Map): void {
  const candidate = map as unknown as { transform?: unknown };
  if (candidate.transform !== undefined) return;
  const canvas = map.getCanvas();
  Object.defineProperty(candidate, 'transform', {
    configurable: true,
    value: {
      height: canvas?.height ?? 1,
      width: canvas?.width ?? 1,
      elevation: 0,
      _nearZ: 0.1,
      _farZ: 10000,
    },
  });
}
