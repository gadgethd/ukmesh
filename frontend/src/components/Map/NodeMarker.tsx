import React, { useState, useEffect, useRef } from 'react';
import { Marker, Popup, Polygon, Pane } from 'react-leaflet';
import L from 'leaflet';
import type { LatLngExpression } from 'leaflet';
import type { MeshNode } from '../../hooks/useNodes.js';
import type { NodeCoverage } from '../../hooks/useCoverage.js';
import { isValidMapCoord } from '../../utils/pathing.js';

const SEVEN_DAYS_MS  = 7  * 24 * 60 * 60 * 1000;
const PREVIEW_TTL_MS = 20_000;

type MarkerVariant = 'repeater' | 'companion' | 'room' | 'inferred';
type HexClashState = 'offender' | 'clear';

// Build a custom Leaflet icon from HTML
function buildIcon(
  isOnline: boolean,
  isActive: boolean,
  isStale: boolean,
  variant: MarkerVariant,
  markerSize: number,
  isRestoring: boolean,
  hexClashState?: HexClashState,
): L.DivIcon {
  const size = Math.max(4, Math.round(markerSize));
  const border = size >= 10 ? 2 : 1;
  const classes = [
    'node-marker',
    isStale               ? 'node-marker--stale'     : '',
    !isOnline && !isStale ? 'node-marker--offline'   : '',
    isActive && !isStale  ? 'node-marker--active'    : '',
    // Colour variant only shown when online and fresh
    isOnline && !isStale && variant === 'companion' ? 'node-marker--companion' : '',
    isOnline && !isStale && variant === 'room'      ? 'node-marker--room'      : '',
    isOnline && !isStale && variant === 'inferred'  ? 'node-marker--inferred'  : '',
    isRestoring ? 'node-marker--restore' : '',
    hexClashState === 'offender' ? 'node-marker--hex-offender' : '',
    hexClashState === 'clear' ? 'node-marker--hex-clear' : '',
  ].filter(Boolean).join(' ');
  const html = `
    <div class="${classes}" style="--marker-size:${size}px; --marker-border:${border}px;">
      <div class="node-marker__core"></div>
      <div class="node-marker__pulse"></div>
    </div>`;
  return L.divIcon({
    html,
    className: '',
    iconSize:    [size, size],
    iconAnchor:  [Math.round(size / 2), Math.round(size / 2)],
    popupAnchor: [0, -10],
  });
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

function roleZIndexOffset(role: number | undefined): number {
  if (role === 3) return 100;
  if (role === 1) return 200;
  return 300;
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
  isActive,
  isInferred = false,
  nodeCoverage,
  markerSize = 12,
  isHighlighted = false,
  isRestoring = false,
  samePrefixRepeaterCount,
  samePrefixActive = false,
  onToggleSamePrefix,
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

  if (!isValidMapCoord(node.lat, node.lon)) return null;

  const lat = node.lat as number;
  const lon = node.lon as number;
  const ageMs   = Date.now() - new Date(node.last_seen).getTime();
  const isStale = ageMs > SEVEN_DAYS_MS;
  const variant = (isInferred || node.is_inferred) ? 'inferred' : roleVariant(node.role);

  const fallbackName = ROLE_LABELS[node.role ?? 2] ?? 'Unknown Device';

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
  const showSamePrefixRow = (node.role === undefined || node.role === 2) && typeof samePrefixRepeaterCount === 'number';

  return (
    <>
      <Marker
        position={[lat, lon]}
        icon={buildIcon(node.is_online, isActive || isHighlighted, isStale, variant, markerSize, isRestoring, hexClashState)}
        zIndexOffset={roleZIndexOffset(node.role)}
      >
        <Popup eventHandlers={{
          add: () => {
            if (links !== null) return; // already fetched
            fetch(`/api/nodes/${node.node_id}/links`)
              .then((r) => r.json())
              .then((data: NodeLink[]) => setLinks(data))
              .catch(() => setLinks([]));
          },
        }}>
          <div className="node-popup">
            <div className="node-popup__name">{node.name ?? `Unknown ${fallbackName}`}</div>
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
              <span>{lat.toFixed(5)}, {lon.toFixed(5)}</span>
            </div>
            {node.elevation_m !== undefined && node.elevation_m !== null && (
              <div className="node-popup__row">
                <span>Elevation</span>
                <span>{Math.round(node.elevation_m)} m ASL</span>
              </div>
            )}
            {showSamePrefixRow && (
              <div className="node-popup__row node-popup__row--inline">
                <span>Same 2-hex repeaters</span>
                <span className="node-popup__inline-value">
                  {samePrefixRepeaterCount}
                  <button
                    className={`node-popup__inline-btn${samePrefixActive ? ' node-popup__inline-btn--active' : ''}`}
                    onClick={() => onToggleSamePrefix?.(node.node_id, !samePrefixActive)}
                  >
                    {samePrefixActive ? 'Hide' : 'Show locations'}
                  </button>
                </span>
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
        </Popup>
      </Marker>

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
