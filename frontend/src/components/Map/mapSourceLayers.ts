import type maplibregl from 'maplibre-gl';
import { EMPTY_FC } from './mapConfig.js';

export function installMapSourcesAndLayers(
  map: maplibregl.Map,
  options: { showLinks: boolean },
): void {
  const { showLinks } = options;
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
          'circle-color': [
            'case',
            ['==', ['get', 'hex_clash_state'], 'offender'], '#ef4444',
            ['==', ['get', 'hex_clash_state'], 'relay'], '#22c55e',
            ['get', 'replay_active'], '#fbbf24',
            ['get', 'is_link_only_stale'], '#4b5563',
            ['get', 'is_inferred'], '#7dd3fc',
            ['get', 'is_stale'], '#6b7280',
            ['!', ['get', 'is_online']], '#6b7280',
            ['==', ['get', 'role'], 1], '#ff9f43',
            ['==', ['get', 'role'], 3], '#a78bfa',
            ['==', ['get', 'role'], 4], '#34d399',
            '#00c4ff', // repeater (role 2 / default)
          ],
          'circle-opacity': [
            'case',
            ['all', ['get', 'replay_mode'], ['!', ['get', 'replay_active']]], 0.12,
            ['get', 'is_link_only_stale'], 0.22,
            ['get', 'is_stale'], 0.4,
            ['!', ['get', 'is_online']], 0.4,
            ['get', 'is_inferred'], 0.7,
            1.0,
          ],
          'circle-stroke-width': 0,
          'circle-stroke-color': '#00c4ff',
          'circle-stroke-opacity': 0.7,
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
          'circle-color': '#22e0ff',
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
          'circle-color': '#8af4ff',
          'circle-opacity': 1,
          'circle-stroke-color': '#ffffff',
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
          'line-color': '#f59e0b',
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

      // ── Coverage source + layer ────────────────────────────────────────────
      map.addSource('coverage', { type: 'geojson', data: EMPTY_FC });
      map.addLayer({
        id: 'coverage-fill',
        type: 'fill',
        source: 'coverage',
        layout: { visibility: 'none' },
        paint: {
          'fill-color': [
            'match', ['get', 'band'],
            'green', '#22c55e',
            'amber', '#fbbf24',
            'red', '#ef4444',
            '#22c55e',
          ],
          'fill-opacity': [
            'match', ['get', 'band'],
            'green', 0.22,
            'amber', 0.16,
            'red', 0.10,
            0.18,
          ],
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
          'line-color': '#f97316',
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
          'fill-color': [
            'match', ['get', 'band'],
            'green', '#2dd4bf',   // teal-400
            'amber', '#818cf8',   // indigo-400
            'red',   '#c084fc',   // purple-400
            '#2dd4bf',
          ],
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
          'line-color': '#22d3ee', // cyan-400
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
      // Styled to match real repeater nodes (role 2, #00c4ff) but visually
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
          'circle-color': '#22d3ee',
          'circle-opacity': [
            'match', ['get', 'status'],
            'ready', 0.20,
            0.10,
          ],
          'circle-stroke-width': 0,
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
            'ready', '#00c4ff',   // identical to real online repeater
            '#4b5563',            // dark grey while computing
          ],
          'circle-opacity': [
            'match', ['get', 'status'],
            'ready', 1.0,
            0.6,
          ],
          'circle-stroke-color': '#ffffff',
          'circle-stroke-width': 2,
          'circle-stroke-opacity': 0.95,
        },
      });

}
