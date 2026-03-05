import React, { useState, useCallback, useMemo, useEffect, useRef } from 'react';
import { MapContainer, TileLayer, useMap, Pane, Polygon, Polyline } from 'react-leaflet';
import type { LatLngExpression, Map as LeafletMap, Polyline as LeafletPolyline } from 'leaflet';
import type { MeshNode, PacketArc } from '../../hooks/useNodes.js';
import type { NodeCoverage } from '../../hooks/useCoverage.js';
import type { LinkMetrics } from '../../utils/pathing.js';
import { NodeMarker } from './NodeMarker.js';
import { PacketArcLayer } from './PacketArcLayer.js';
import { NodeSearch } from './NodeSearch.js';

// Sync Leaflet view state with deck.gl view state
interface SyncerProps {
  onViewStateChange: (vs: DeckViewState) => void;
}

interface DeckViewState {
  longitude: number;
  latitude:  number;
  zoom:      number;
  pitch:     number;
  bearing:   number;
}

// Leaflet→deck.gl sync component
const LeafletDeckSyncer: React.FC<SyncerProps> = ({ onViewStateChange }) => {
  const map = useMap();

  React.useEffect(() => {
    const sync = () => {
      const center = map.getCenter();
      onViewStateChange({
        longitude: center.lng,
        latitude:  center.lat,
        // deck.gl uses 512px tiles (mapbox convention); Leaflet uses 256px.
        // One zoom level difference = factor of 2 in scale.
        zoom:      map.getZoom() - 1,
        pitch:     0,
        bearing:   0,
      });
    };

    map.on('move', sync);
    map.on('zoom', sync);
    sync();
    return () => { map.off('move', sync); map.off('zoom', sync); };
  }, [map, onViewStateChange]);

  return null;
};

// GeoJSON rings are [lon, lat]; Leaflet wants [lat, lon].
function ringToLatLng(ring: number[][]): LatLngExpression[] {
  return ring.map(([lon, lat]) => [lat, lon] as LatLngExpression);
}

function hasCoords(node: MeshNode | null | undefined): node is MeshNode & { lat: number; lon: number } {
  return typeof node?.lat === 'number' && typeof node?.lon === 'number';
}

// Raw outer rings from each coverage polygon — used for the green coverage display.
// Using raw rings (not a union) with fillRule:'nonzero' means:
//   - overlapping viewsheds: winding numbers add (+1 per CCW ring) → always filled ✓
//   - no opacity stacking: single SVG <path> element, one fill pass ✓
function useCoverageDisplayRings(coverage: NodeCoverage[]): LatLngExpression[][] {
  return useMemo(() => {
    return coverage.flatMap((c) => {
      if (c.geom.type === 'Polygon')
        return [ringToLatLng((c.geom.coordinates as number[][][])[0])];
      if (c.geom.type === 'MultiPolygon')
        return (c.geom.coordinates as number[][][][]).map((poly) => ringToLatLng(poly[0]));
      return [];
    });
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [coverage]);
}

// Dash pattern cycle length in px — must match dashArray below ('6 9' = 15px).
const DASH_CYCLE = 15;
// Pixels to advance per animation frame (~60fps → ~18px/s ≈ 1.2 cycles/s).
const DASH_STEP  = 0.3;

interface MapViewProps {
  nodes:           Map<string, MeshNode>;
  arcs:            PacketArc[];
  activeNodes:     Set<string>;
  coverage:        NodeCoverage[];
  showPackets:     boolean;
  showCoverage:    boolean;
  showClientNodes: boolean;
  showLinks:       boolean;
  viablePairsArr:  [string, string][];
  linkMetrics:     Map<string, LinkMetrics>;
  packetPath:      [number, number][] | null;
  betaPath:        [number, number][] | null;
  showBetaPaths:   boolean;
  pathOpacity:     number;
  onMapReady?:     (m: LeafletMap) => void;
}

// Default UK centre (Teesside area)
const DEFAULT_CENTER: [number, number] = [54.57, -1.23];
const DEFAULT_ZOOM = 11;

export const MapView: React.FC<MapViewProps> = ({
  nodes, arcs, activeNodes, coverage, showPackets, showCoverage, showClientNodes,
  showLinks, viablePairsArr, linkMetrics, packetPath, betaPath, showBetaPaths, pathOpacity, onMapReady,
}) => {
  const [map, setMap] = useState<LeafletMap | null>(null);

  useEffect(() => {
    if (map && onMapReady) onMapReady(map);
  }, [map, onMapReady]);

  const [deckViewState, setDeckViewState] = useState<DeckViewState>({
    longitude: DEFAULT_CENTER[1],
    latitude:  DEFAULT_CENTER[0],
    zoom:      DEFAULT_ZOOM,
    pitch:     0,
    bearing:   0,
  });

  const handleViewStateChange = useCallback((vs: unknown) => {
    setDeckViewState(vs as DeckViewState);
  }, []);

  // Refs to Leaflet Polyline instances for direct SVG attribute animation
  const regularPathRef = useRef<LeafletPolyline | null>(null);
  const betaPathRef    = useRef<LeafletPolyline | null>(null);
  const aniFrameRef    = useRef<number | null>(null);

  // Animate marching dashes by incrementing stroke-dashoffset directly on the
  // Leaflet SVG path element. CSS animation is unreliable here because Leaflet
  // calls _updateStyle (setAttribute) on every prop change, which can interrupt
  // CSS keyframe animations. Direct DOM manipulation in an rAF loop is stable.
  const hasRegular = !!packetPath;
  const hasBeta    = !!(showBetaPaths && betaPath);

  useEffect(() => {
    if (!hasRegular && !hasBeta) {
      if (aniFrameRef.current !== null) {
        cancelAnimationFrame(aniFrameRef.current);
        aniFrameRef.current = null;
      }
      return;
    }

    let offset = 0;
    const tick = () => {
      offset = (offset + DASH_STEP) % DASH_CYCLE;
      // Negative dashoffset = dashes march forward (source → destination)
      const val = String(-offset);
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      const rp = (regularPathRef.current as any)?._path as SVGPathElement | null;
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      const bp = (betaPathRef.current as any)?._path as SVGPathElement | null;
      if (hasRegular && rp) rp.setAttribute('stroke-dashoffset', val);
      if (hasBeta    && bp) bp.setAttribute('stroke-dashoffset', val);
      aniFrameRef.current = requestAnimationFrame(tick);
    };

    aniFrameRef.current = requestAnimationFrame(tick);
    return () => {
      if (aniFrameRef.current !== null) {
        cancelAnimationFrame(aniFrameRef.current);
        aniFrameRef.current = null;
      }
    };
  }, [hasRegular, hasBeta]); // eslint-disable-line react-hooks/exhaustive-deps

  const FOURTEEN_DAYS_MS = 14 * 24 * 60 * 60 * 1000;
  const allNodesWithPos = useMemo(() => Array.from(nodes.values()).filter(
    (n) => hasCoords(n)
      && (Date.now() - new Date(n.last_seen).getTime()) < FOURTEEN_DAYS_MS
      && !n.name?.includes('🚫')
  ), [nodes]); // eslint-disable-line react-hooks/exhaustive-deps
  const nodesWithPos   = useMemo(() => allNodesWithPos.filter((n) => n.role === undefined || n.role === 2), [allNodesWithPos]);
  const clientNodesArr = useMemo(() => allNodesWithPos.filter((n) => n.role === 1 || n.role === 3), [allNodesWithPos]);

  const coverageRings = useCoverageDisplayRings(coverage);
  const coverageByNodeId = useMemo(() => {
    const m = new Map<string, NodeCoverage>();
    for (const c of coverage) m.set(c.node_id, c);
    return m;
  }, [coverage]);

  const linkKey = (a: string, b: string) => (a < b ? `${a}:${b}` : `${b}:${a}`);

  const linkColor = (pathLossDb: number | null | undefined) => {
    if (pathLossDb == null) return '#d1d5db';
    if (pathLossDb <= 120) return '#22c55e';
    if (pathLossDb <= 135) return '#fbbf24';
    return '#ef4444';
  };

  // Resolve viable link pairs to lat/lon polyline positions
  const linkLines = useMemo(() => {
    if (!showLinks || viablePairsArr.length === 0) return [];
    const lines: Array<{
      key: string;
      positions: [number, number][];
      observedCount: number;
      pathLossDb: number | null | undefined;
      countAToB: number;
      countBToA: number;
    }> = [];
    for (const [aId, bId] of viablePairsArr) {
      const a = nodes.get(aId);
      const b = nodes.get(bId);
      if (
        hasCoords(a)
        && hasCoords(b)
        && (Date.now() - new Date(a.last_seen).getTime()) < FOURTEEN_DAYS_MS
        && (Date.now() - new Date(b.last_seen).getTime()) < FOURTEEN_DAYS_MS
        && !a.name?.includes('🚫')
        && !b.name?.includes('🚫')
        && (a.role === undefined || a.role === 2)
        && (b.role === undefined || b.role === 2)
      ) {
        const key = linkKey(aId, bId);
        const metrics = linkMetrics.get(key);
        lines.push({
          key,
          positions: [[a.lat, a.lon], [b.lat, b.lon]],
          observedCount: metrics?.observed_count ?? 0,
          pathLossDb: metrics?.itm_path_loss_db,
          countAToB: metrics?.count_a_to_b ?? 0,
          countBToA: metrics?.count_b_to_a ?? 0,
        });
      }
    }
    const zoom = map?.getZoom() ?? DEFAULT_ZOOM;
    if (zoom >= 9 || lines.length <= 400) return lines;
    return lines
      .sort((a, b) => (b.observedCount - a.observedCount))
      .slice(0, 400);
  }, [showLinks, viablePairsArr, nodes, linkMetrics, map]);

  return (
    <div className="map-area">
      <NodeSearch nodes={nodes} map={map} />
      <MapContainer
        ref={setMap}
        center={DEFAULT_CENTER}
        zoom={DEFAULT_ZOOM}
        style={{ width: '100%', height: '100%' }}
        zoomControl={false}
        attributionControl={false}
      >
        {/* CartoDB Dark Matter tiles */}
        <TileLayer
          url="https://{s}.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}{r}.png"
          attribution='&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> &copy; <a href="https://carto.com/attributions">CARTO</a>'
          subdomains="abcd"
          maxZoom={19}
          keepBuffer={6}
          updateWhenIdle={false}
        />

        {/* Sync Leaflet map position to deck.gl */}
        <LeafletDeckSyncer onViewStateChange={handleViewStateChange} />

        {/* Coverage — raw outer rings from each viewshed, fillRule:'nonzero'.
            nonzero means overlapping CCW rings sum winding numbers (+1 each)
            so all covered areas fill regardless of how many viewsheds overlap. */}
        {showCoverage && coverageRings.length > 0 && (
          <Pane name="coveragePane" style={{ zIndex: 350 }}>
            <Polygon
              positions={coverageRings as LatLngExpression[][]}
              pathOptions={{
                fillColor:   '#1ec850',
                fillOpacity: 0.22,
                weight:      0,
                fillRule:    'nonzero',
              }}
              interactive={false}
            />
          </Pane>
        )}

        {/* Confirmed link lines — ITM-viable node pairs */}
        {showLinks && linkLines.length > 0 && (
          <Pane name="linksPane" style={{ zIndex: 400 }}>
            {linkLines.map((line) => {
              const obs = Math.max(1, line.observedCount);
              const strength = Math.log10(obs + 1);
              const opacity = Math.min(0.85, 0.35 + strength * 0.22);
              const weight = Math.min(3.2, 1.0 + strength * 1.1);
              return (
              <Polyline
                key={line.key}
                positions={line.positions}
                pathOptions={{
                  color: linkColor(line.pathLossDb),
                  weight,
                  opacity,
                }}
                interactive={false}
              />
              );
            })}
          </Pane>
        )}

        {/* Repeater markers — Leaflet default marker pane at zIndex 600 */}
        {nodesWithPos.map((node) => (
          <NodeMarker
            key={node.node_id}
            node={node}
            isActive={activeNodes.has(node.node_id)}
            nodeCoverage={coverageByNodeId.get(node.node_id)}
          />
        ))}

        {/* Companion radio + room server markers (toggled via filter) */}
        {showClientNodes && clientNodesArr.map((node) => (
          <NodeMarker
            key={node.node_id}
            node={node}
            isActive={activeNodes.has(node.node_id)}
            nodeCoverage={coverageByNodeId.get(node.node_id)}
          />
        ))}

        {/* Live packet path — marching dashes from source → observer */}
        {packetPath && (
          <Polyline
            ref={regularPathRef}
            positions={packetPath}
            pathOptions={{
              color:     '#00c4ff',
              weight:    2,
              dashArray: '6 9',
              opacity:   pathOpacity,
            }}
          />
        )}

        {/* Beta path — coverage-validated, unambiguous hop resolution */}
        {showBetaPaths && betaPath && (
          <Polyline
            ref={betaPathRef}
            positions={betaPath}
            pathOptions={{
              color:     '#a855f7',
              weight:    2,
              dashArray: '6 9',
              opacity:   pathOpacity,
            }}
          />
        )}
      </MapContainer>

      {/* deck.gl overlay — arcs only */}
      <PacketArcLayer
        arcs={arcs}
        showArcs={showPackets}
        viewState={deckViewState}
      />
    </div>
  );
};
