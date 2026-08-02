import type maplibregl from 'maplibre-gl';
import { EMPTY_FC, MAP_OVERLAY_COLORS, type MapTheme } from './mapConfig.js';

type OverlayColors = (typeof MAP_OVERLAY_COLORS)[MapTheme];

function nodeColorExpression(colors: OverlayColors): maplibregl.ExpressionSpecification {
  return [
    'case',
    ['==', ['get', 'hex_clash_state'], 'offender'], colors.clashOffender,
    ['==', ['get', 'hex_clash_state'], 'relay'], colors.clashRelay,
    ['get', 'replay_active'], colors.replay,
    ['get', 'is_link_only_stale'], colors.linkOnlyStale,
    ['get', 'is_inferred'], colors.inferred,
    ['get', 'is_stale'], colors.stale,
    ['!', ['get', 'is_online']], colors.stale,
    ['==', ['get', 'role'], 1], colors.companion,
    ['==', ['get', 'role'], 3], colors.roomServer,
    ['==', ['get', 'role'], 4], colors.sensor,
    colors.repeater,
  ];
}

function nodeOpacityExpression(colors: OverlayColors): maplibregl.ExpressionSpecification {
  return [
    'case',
    ['all', ['get', 'replay_mode'], ['!', ['get', 'replay_active']]], colors.dimmedOpacity,
    ['get', 'is_link_only_stale'], colors.staleOpacity,
    ['get', 'is_stale'], colors.staleOpacity,
    ['!', ['get', 'is_online']], colors.staleOpacity,
    ['get', 'is_inferred'], colors.inferredOpacity,
    1,
  ];
}

function plannedBandColorExpression(colors: OverlayColors): maplibregl.ExpressionSpecification {
  return [
    'match', ['get', 'band'],
    'green', colors.plannedGood,
    'amber', colors.plannedMarginal,
    'red', colors.plannedPoor,
    colors.plannedGood,
  ];
}

/** Re-theme all non-raster overlays without replacing their live sources. */
export function applyMapOverlayTheme(map: maplibregl.Map, theme: MapTheme): void {
  const colors = MAP_OVERLAY_COLORS[theme];
  const setPaint = (layerId: string, property: string, value: unknown) => {
    if (map.getLayer(layerId)) map.setPaintProperty(layerId, property, value);
  };
  setPaint('node-dots', 'circle-color', nodeColorExpression(colors));
  setPaint('node-dots', 'circle-opacity', nodeOpacityExpression(colors));
  setPaint('node-dots', 'circle-stroke-width', 1);
  setPaint('node-dots', 'circle-stroke-color', colors.nodeStroke);
  setPaint('node-dots', 'circle-stroke-opacity', 0.9);
  setPaint('node-dots-selected-halo', 'circle-color', colors.selectedHalo);
  setPaint('node-dots-selected', 'circle-color', colors.selected);
  setPaint('node-dots-selected', 'circle-stroke-color', colors.selectedStroke);
  setPaint('privacy-rings-layer', 'line-color', colors.privacy);
  setPaint('clash-lines-layer', 'line-color', colors.clashLine);
  setPaint('planned-coverage-fill', 'fill-color', plannedBandColorExpression(colors));
  setPaint('planned-coverage-outline', 'line-color', colors.plannedOutline);
  setPaint('planned-pins-halo', 'circle-color', colors.plannedOutline);
  setPaint('planned-pins-dot', 'circle-color', [
    'match', ['get', 'status'],
    'ready', colors.repeater,
    colors.plannedPending,
  ]);
  setPaint('planned-pins-dot', 'circle-stroke-color', colors.selectedStroke);
}

export function installMapSourcesAndLayers(
  map: maplibregl.Map,
  options: { showLinks: boolean; mapLight: boolean },
): void {
  const { showLinks, mapLight } = options;
  const colors = MAP_OVERLAY_COLORS[mapLight ? 'light' : 'dark'];
      // ── Node dots source + layer ───────────────────────────────────────────
      map.addSource('nodes', { type: 'geojson', data: EMPTY_FC });

      map.addLayer({
        id: 'node-dots',
        type: 'circle',
        source: 'nodes',
        filter: ['==', ['get', 'visible'], true],
        paint: {
          'circle-radius': [
            'interpolate', ['linear'], ['zoom'],
            6, 3, 9, 4, 11, 5, 13, 7, 16, 9,
          ],
          'circle-color': nodeColorExpression(colors),
          'circle-opacity': nodeOpacityExpression(colors),
          'circle-stroke-width': 1,
          'circle-stroke-color': colors.nodeStroke,
          'circle-stroke-opacity': 0.9,
        },
      });

      // ── Selected-node highlight (recolour + ring) ──────────────────────────
      // Two circle layers over node-dots, filtered to the selected node id.
      // A soft halo underneath, a bright solid marker on top.
      map.addLayer({
        id: 'node-dots-selected-halo',
        type: 'circle',
        source: 'nodes',
        filter: ['==', ['get', 'node_id'], '__none__'],
        paint: {
          'circle-radius': [
            'interpolate', ['linear'], ['zoom'],
            6, 11, 9, 13, 11, 15, 13, 18, 16, 22,
          ],
          'circle-color': colors.selectedHalo,
          'circle-opacity': 0.16,
          'circle-blur': 0.5,
        },
      });
      map.addLayer({
        id: 'node-dots-selected',
        type: 'circle',
        source: 'nodes',
        filter: ['==', ['get', 'node_id'], '__none__'],
        paint: {
          'circle-radius': [
            'interpolate', ['linear'], ['zoom'],
            6, 5, 9, 6.5, 11, 8, 13, 10, 16, 13,
          ],
          'circle-color': colors.selected,
          'circle-opacity': 1,
          'circle-stroke-color': colors.selectedStroke,
          'circle-stroke-width': 2.5,
          'circle-stroke-opacity': 0.95,
        },
      });

      // ── Privacy rings source + layer ───────────────────────────────────────
      map.addSource('privacy-rings', { type: 'geojson', data: EMPTY_FC });
      map.addLayer({
        id: 'privacy-rings-layer',
        type: 'line',
        source: 'privacy-rings',
        paint: {
          'line-color': colors.privacy,
          'line-width': 1.4,
          'line-opacity': 0.55,
          'line-dasharray': [4, 6],
        },
      });

      // ── Viable links source + layer ───────────────────────────────────────
      map.addSource('viable-links', { type: 'geojson', data: EMPTY_FC });
      map.addLayer({
        id: 'viable-links-layer',
        type: 'line',
        source: 'viable-links',
        layout: {
          visibility: 'none',
          'line-cap': 'round',
          'line-join': 'round',
        },
        paint: {
          'line-color': ['get', 'color'],
          'line-width': ['get', 'width'],
          'line-opacity': ['get', 'opacity'],
        },
      });

      // ── Clash lines source + layer ─────────────────────────────────────────
      map.addSource('clash-lines', { type: 'geojson', data: EMPTY_FC });
      map.addLayer({
        id: 'clash-lines-layer',
        type: 'line',
        source: 'clash-lines',
        layout: { visibility: 'none' },
        paint: {
          'line-color': colors.clashLine,
          'line-width': 2.2,
          'line-opacity': 0.9,
        },
      });

      // ── Planned coverage source + layers ──────────────────────────────────
      map.addSource('planned-coverage', { type: 'geojson', data: EMPTY_FC });
      map.addLayer({
        id: 'planned-coverage-fill',
        type: 'fill',
        source: 'planned-coverage',
        paint: {
          'fill-color': plannedBandColorExpression(colors),
          'fill-opacity': [
            'match', ['get', 'band'],
            'green', 0.30,
            'amber', 0.25,
            'red',   0.20,
            0.25,
          ],
        },
      });
      map.addLayer({
        id: 'planned-coverage-outline',
        type: 'line',
        source: 'planned-coverage',
        paint: {
          'line-color': colors.plannedOutline,
          'line-width': 1.5,
          'line-opacity': 0.6,
        },
      });

      // ── Predicted planned-repeater links source + layer ───────────────────
      // Dashed lines (coloured by predicted path loss) so they read as
      // hypothetical, distinct from the solid observed-link lines. Visibility
      // follows the global Links toggle.
      map.addSource('planned-links', { type: 'geojson', data: EMPTY_FC });
      map.addLayer({
        id: 'planned-links-layer',
        type: 'line',
        source: 'planned-links',
        layout: {
          visibility: showLinks ? 'visible' : 'none',
          'line-cap': 'round',
          'line-join': 'round',
        },
        paint: {
          'line-color': ['get', 'color'],
          'line-width': ['get', 'width'],
          'line-opacity': 0.9,
          'line-dasharray': [2, 1.5],
        },
      });

      // ── Planned repeater pins source + layers ──────────────────────────────
      // Styled to match real repeater nodes in the active theme but visually
      // distinct via a white stroke and glow halo. Shared map labels and glyphs
      // belong to the base style; planned status text stays in the popup so
      // temporary planning markers do not add clutter to the map.
      map.addSource('planned-pins', { type: 'geojson', data: EMPTY_FC });

      // Halo: soft glow behind the pin
      map.addLayer({
        id: 'planned-pins-halo',
        type: 'circle',
        source: 'planned-pins',
        paint: {
          'circle-radius': [
            'interpolate', ['linear'], ['zoom'],
            6, 8, 9, 11, 11, 14, 13, 18, 16, 22,
          ],
          'circle-color': colors.plannedOutline,
          'circle-opacity': [
            'match', ['get', 'status'],
            'ready', 0.20,
            0.10,
          ],
        },
      });

      // Core dot: same size/colour as a real online repeater, white stroke to mark as planned
      map.addLayer({
        id: 'planned-pins-dot',
        type: 'circle',
        source: 'planned-pins',
        paint: {
          'circle-radius': [
            'interpolate', ['linear'], ['zoom'],
            6, 3, 9, 4, 11, 5, 13, 7, 16, 9,
          ],
          'circle-color': [
            'match', ['get', 'status'],
            'ready', colors.repeater,
            colors.plannedPending,
          ],
          'circle-opacity': [
            'match', ['get', 'status'],
            'ready', 1.0,
            0.6,
          ],
          'circle-stroke-color': colors.selectedStroke,
          'circle-stroke-width': 2,
          'circle-stroke-opacity': 0.95,
        },
      });

}
