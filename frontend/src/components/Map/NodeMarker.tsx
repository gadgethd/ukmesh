import React, { useState, useEffect, useRef } from 'react';
import { Circle, CircleMarker, Popup, Polygon, Pane } from 'react-leaflet';
import type { LatLngExpression } from 'leaflet';
import type { MeshNode } from '../../hooks/useNodes.js';
import type { NodeCoverage } from '../../hooks/useCoverage.js';
import { HIDDEN_NODE_MASK_RADIUS_METERS, isProhibitedMapNode, isValidMapCoord } from '../../utils/pathing.js';

const SEVEN_DAYS_MS  = 7  * 24 * 60 * 60 * 1000;
const PREVIEW_TTL_MS = 20_000;

type MarkerVariant = 'repeater' | 'companion' | 'room' | 'inferred';
type HexClashState = 'offender' | 'clear';

// Resolve the SVG stroke/fill colour for a CircleMarker based on node state and role
function markerColor(variant: MarkerVariant, isOnline: boolean, isStale: boolean, hexClashState?: HexClashState): string {
  if (hexClashState === 'offender') return '#ef4444';
  if (hexClashState === 'clear')    return '#22c55e';
  if (isStale)    return '#ff4444';
  if (!isOnline)  return '#666';
  if (variant === 'companion') return '#ff9800';
  if (variant === 'room')      return '#ce93d8';
  if (variant === 'inferred')  return 'rgba(109,220,122,0.9)';
  return '#00c4ff'; // repeater default
}

function timeAgo(iso: string): string {
  const secs = Math.floor((Date.now() - new Date(iso).getTime()) / 1000);
  if (secs < 60)    return `${secs}s ago`;
  if (secs < 3600)  return `${Math.floor(secs / 60)}m ago`;
  if (secs < 86400) return `${Math.floor(secs / 3600)}h ago`;
  return `${Math.floor(secs / 86400)}d ago`;
}

const ROLE_LABELS: Record<number, string> = {
  1: 'Companion Radio',
  2: 'Repeater',
  3: 'Room Server',
  4: 'Sensor',
};

function roleVariant(role: number | undefined): MarkerVariant {
  if (role === 1) return 'companion';
  if (role === 3) return 'room';
  return 'repeater';
}

function isRepeaterNode(role: number | undefined): boolean {
  return role === undefined || role === 2;
}

function ringToLatLng(ring: number[][]): LatLngExpression[] {
  return ring.map(([lon, lat]) => [lat, lon] as LatLngExpression);
}

function coverageToPolygons(geom: { type: string; coordinates: unknown } | null | undefined): LatLngExpression[][][] {
  if (!geom) return [];
  if (geom.type === 'Polygon') {
    const polygon = geom.coordinates as number[][][];
    return [polygon.map((ring) => ringToLatLng(ring))];
  }
  if (geom.type === 'MultiPolygon') {
    const multiPolygon = geom.coordinates as number[][][][];
    return multiPolygon.map((polygon) => polygon.map((ring) => ringToLatLng(ring)));
  }
  return [];
}

interface NodeLink {
  peer_id: string; peer_name: string | null; observed_count: number;
  itm_path_loss_db: number | null;
  count_this_to_peer: number; count_peer_to_this: number;
}

interface Props {
  node:          MeshNode;
  displayPosition?: [number, number];
  circleCenterPosition?: [number, number];
  isActive:      boolean;
  isInferred?:   boolean;
  nodeCoverage?: NodeCoverage;
  markerSize?:   number;
  isHighlighted?: boolean;
  isRestoring?: boolean;
  samePrefixRepeaterCount?: number;
  samePrefixActive?: boolean;
  onToggleSamePrefix?: (nodeId: string, enabled: boolean) => void;
  hexClashState?: HexClashState;
}

export const NodeMarker: React.FC<Props> = React.memo(({
  node,
  displayPosition,
  circleCenterPosition,
  isActive: _isActive,
  isInferred = false,
  nodeCoverage,
  markerSize: _markerSize,
  isHighlighted = false,
  isRestoring: _isRestoring,
  samePrefixRepeaterCount: _samePrefixRepeaterCount,
  samePrefixActive: _samePrefixActive,
  onToggleSamePrefix: _onToggleSamePrefix,
  hexClashState,
}) => {
  const [showPreview, setShowPreview] = useState(false);
  const [links, setLinks]             = useState<NodeLink[] | null>(null);
  const timerRef = useRef<ReturnType<typeof setTimeout> | null>(null);

  useEffect(() => () => { if (timerRef.current) clearTimeout(timerRef.current); }, []);

  const handleShowCoverage = () => {
    setShowPreview(true);
    if (timerRef.current) clearTimeout(timerRef.current);
    timerRef.current = setTimeout(() => setShowPreview(false), PREVIEW_TTL_MS);
  };

  const prohibited = isProhibitedMapNode(node);
  const markerLat = displayPosition?.[0] ?? node.lat;
  const markerLon = displayPosition?.[1] ?? node.lon;
  const circleLat = circleCenterPosition?.[0] ?? markerLat;
  const circleLon = circleCenterPosition?.[1] ?? markerLon;
  if (!isValidMapCoord(markerLat, markerLon)) return null;

  const lat = markerLat as number;
  const lon = markerLon as number;
  const ageMs   = Date.now() - new Date(node.last_seen).getTime();
  const isStale = ageMs > SEVEN_DAYS_MS;
  const variant = (isInferred || node.is_inferred) ? 'inferred' : roleVariant(node.role);

  const fallbackName = ROLE_LABELS[node.role ?? 2] ?? 'Unknown Device';
  const displayName = prohibited ? `Redacted ${fallbackName}` : (node.name ?? `Unknown ${fallbackName}`);

  const statusLabel = isStale
    ? 'STALE'
    : node.is_online ? 'ONLINE' : 'OFFLINE';
  const statusColor = isStale
    ? 'var(--danger)'
    : node.is_online ? 'var(--online)' : 'var(--offline)';

  const previewBands = showPreview && nodeCoverage ? {
    red: coverageToPolygons(nodeCoverage.strength_geoms?.red ?? nodeCoverage.geom),
    amber: coverageToPolygons(nodeCoverage.strength_geoms?.amber),
    green: coverageToPolygons(nodeCoverage.strength_geoms?.green),
  } : { red: [], amber: [], green: [] };
  const isRepeater = isRepeaterNode(node.role);

  // Simple popup content for repeaters - just name and coords (respecting privacy)
  const repeaterPopupContent = (
    <div className="node-popup">
      <div className="node-popup__name">{displayName}</div>
      {node.public_key && (
        <div className="node-popup__row">
          <span>Public key</span>
          <span className="node-popup__mono">{node.public_key}</span>
        </div>
      )}
      <div className="node-popup__row">
        <span>Status</span>
        <span style={{ color: statusColor }}>{statusLabel}</span>
      </div>
      <div className="node-popup__row">
        <span>Position</span>
        <span>{prohibited ? 'Redacted' : `${lat.toFixed(5)}, ${lon.toFixed(5)}`}</span>
      </div>
      {prohibited && (
        <div className="node-popup__row">
          <span>Location</span>
          <span>Redacted within 1 mile radius</span>
        </div>
      )}
    </div>
  );

  const color = markerColor(variant, node.is_online, isStale, hexClashState);
  const radius = isHighlighted ? 5 : 3;

  return (
    <>
      <CircleMarker
        center={[lat, lon]}
        radius={radius}
        pathOptions={{ color, fillColor: color, fillOpacity: 0.7, weight: 1 }}
      >
        <Popup eventHandlers={!isRepeater ? {
          add: () => {
            if (links !== null) return;
            fetch(`/api/nodes/${node.node_id}/links`)
              .then((r) => r.json())
              .then((data: NodeLink[]) => setLinks(data))
              .catch(() => setLinks([]));
          },
        } : undefined}>
          {isRepeater ? repeaterPopupContent : (
            <div className="node-popup">
              <div className="node-popup__name">{displayName}</div>
              {node.public_key && (
                <div className="node-popup__row">
                  <span>Public key</span>
                  <span className="node-popup__mono">{node.public_key}</span>
                </div>
              )}
              {node.role !== undefined && node.role !== 2 && (
                <div className="node-popup__row">
                  <span>Type</span>
                  <span>{ROLE_LABELS[node.role] ?? 'Unknown'}</span>
                </div>
              )}
              {(isInferred || node.is_inferred) && (
                <>
                  <div className="node-popup__row">
                    <span>Type</span>
                    <span>{node.is_inferred ? 'Inferred repeater' : 'Inferred active'}</span>
                  </div>
                  {node.inferred_prefix && (
                    <div className="node-popup__row">
                      <span>Prefix</span>
                      <span>{node.inferred_prefix}</span>
                    </div>
                  )}
                  {(node.inferred_packet_count || node.inferred_observations) && (
                    <div className="node-popup__row">
                      <span>Evidence</span>
                      <span>{node.inferred_packet_count ?? 0} packet(s) / {node.inferred_observations ?? 0} sighting(s)</span>
                    </div>
                  )}
                  {(node.inferred_prev_name || node.inferred_next_name) && (
                    <div className="node-popup__row">
                      <span>Between</span>
                      <span>{node.inferred_prev_name ?? 'unknown'} · {node.inferred_next_name ?? 'unknown'}</span>
                    </div>
                  )}
                </>
              )}
              <div className="node-popup__row">
                <span>Status</span>
                <span style={{ color: statusColor }}>{statusLabel}</span>
              </div>
              {node.hardware_model && (
                <div className="node-popup__row">
                  <span>Hardware</span>
                  <span>{node.hardware_model}</span>
                </div>
              )}
              <div className="node-popup__row">
                <span>Last seen</span>
                <span>{timeAgo(node.last_seen)}</span>
              </div>
              {node.advert_count !== undefined && (
                <div className="node-popup__row">
                  <span>Times seen</span>
                  <span>{node.advert_count}</span>
                </div>
              )}
              <div className="node-popup__row">
                <span>Position</span>
                <span>{prohibited ? 'Redacted' : `${lat.toFixed(5)}, ${lon.toFixed(5)}`}</span>
              </div>
              {node.elevation_m !== undefined && node.elevation_m !== null && (
                <div className="node-popup__row">
                  <span>Elevation</span>
                  <span>{Math.round(node.elevation_m)} m ASL</span>
                </div>
              )}
              {nodeCoverage && (
                <button
                  className={`node-popup__coverage-btn${showPreview ? ' node-popup__coverage-btn--active' : ''}`}
                  onClick={handleShowCoverage}
                >
                  {showPreview ? 'Showing coverage…' : 'Preview coverage'}
                </button>
              )}
              {links === null && <div className="node-popup__neighbours-loading">Loading neighbours…</div>}
              {links !== null && links.length > 0 && (
                <div className="node-popup__neighbours">
                  <div className="node-popup__neighbours-title">Confirmed neighbours</div>
                  {links.map((lk) => {
                    const tx = lk.count_this_to_peer > 0;
                    const rx = lk.count_peer_to_this > 0;
                    const arrow = tx && rx ? '↔' : tx ? '→' : '←';
                    return (
                      <div key={lk.peer_id} className="node-popup__neighbour-row">
                        <span className="node-popup__neighbour-name">{arrow} {lk.peer_name ?? lk.peer_id.slice(0, 8)}</span>
                        <span className="node-popup__neighbour-meta">
                          {lk.observed_count}× seen
                          {lk.itm_path_loss_db != null && <> &middot; {Math.round(lk.itm_path_loss_db)} dB</>}
                        </span>
                      </div>
                    );
                  })}
                </div>
              )}
            </div>
          )}
        </Popup>
      </CircleMarker>

      {prohibited && (
        <Circle
          center={[circleLat as number, circleLon as number]}
          radius={HIDDEN_NODE_MASK_RADIUS_METERS}
          pathOptions={{
            color: '#f59e0b',
            weight: 1.4,
            opacity: 0.55,
            fillColor: '#f59e0b',
            fillOpacity: 0.05,
            dashArray: '4 6',
          }}
          interactive={false}
        />
      )}

      {(previewBands.red.length > 0 || previewBands.amber.length > 0 || previewBands.green.length > 0) && (
        <Pane name={`cov-preview-${node.node_id}`} style={{ zIndex: 351 }}>
          {previewBands.red.length > 0 && (
            <Polygon
              positions={previewBands.red as unknown as LatLngExpression[][]}
              pathOptions={{
                fillColor:   '#ef4444',
                fillOpacity: 0.12,
                weight:      0,
                fillRule:    'nonzero',
              }}
              interactive={false}
            />
          )}
          {previewBands.amber.length > 0 && (
            <Polygon
              positions={previewBands.amber as unknown as LatLngExpression[][]}
              pathOptions={{
                fillColor:   '#f59e0b',
                fillOpacity: 0.18,
                weight:      0,
                fillRule:    'nonzero',
              }}
              interactive={false}
            />
          )}
          {previewBands.green.length > 0 && (
            <Polygon
              positions={previewBands.green as unknown as LatLngExpression[][]}
              pathOptions={{
                fillColor:   '#22c55e',
                fillOpacity: 0.28,
                weight:      0,
                fillRule:    'nonzero',
              }}
              interactive={false}
            />
          )}
        </Pane>
      )}
    </>
  );
});
