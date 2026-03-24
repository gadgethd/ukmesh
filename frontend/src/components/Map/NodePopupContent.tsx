import React from 'react';
import { SEVEN_DAYS_MS } from './mapConfig.js';
import type { NodeFeatureProps, NodeLink } from './types.js';

const GPU_ROLE_LABELS: Record<number, string> = {
  1: 'Companion Radio', 2: 'Repeater', 3: 'Room Server', 4: 'Sensor',
};

function timeAgo(iso: string): string {
  const secs = Math.floor((Date.now() - new Date(iso).getTime()) / 1000);
  if (secs < 60) return `${secs}s ago`;
  if (secs < 3600) return `${Math.floor(secs / 60)}m ago`;
  if (secs < 86400) return `${Math.floor(secs / 3600)}h ago`;
  return `${Math.floor(secs / 86400)}d ago`;
}

export const NodePopupContent: React.FC<{
  props: NodeFeatureProps;
  lat: number;
  lon: number;
  links: NodeLink[] | null;
  coverageActive: boolean;
  coverageLoading: boolean;
  coverageMessage: string | null;
  onToggleCoverage: (nodeId: string) => void;
  onFocusSamePrefix: (nodeId: string) => void;
  samePrefixCount: number;
  losActive: boolean;
  losLoading: boolean;
  onToggleLos: (nodeId: string) => void;
}> = ({
  props,
  lat,
  lon,
  links,
  coverageActive,
  coverageLoading,
  coverageMessage,
  onToggleCoverage,
  onFocusSamePrefix,
  samePrefixCount,
  losActive,
  losLoading,
  onToggleLos,
}) => {
  const isRepeater = props.role === undefined || props.role === 2;
  const ageMs = Date.now() - new Date(props.last_seen).getTime();
  const isStale = ageMs > SEVEN_DAYS_MS;
  const statusLabel = isStale ? 'STALE' : props.is_online ? 'ONLINE' : 'OFFLINE';
  const statusColor = isStale ? 'var(--danger)' : props.is_online ? 'var(--online)' : 'var(--offline)';
  const fallbackName = GPU_ROLE_LABELS[props.role ?? 2] ?? 'Unknown Device';
  const displayName = props.is_prohibited
    ? `Redacted ${fallbackName}`
    : (props.name ?? `Unknown ${fallbackName}`);

  return (
    <div className="node-popup">
      <div className="node-popup__name">{displayName}</div>
      {props.public_key && (
        <div className="node-popup__row">
          <span>Public key</span>
          <span className="node-popup__mono">{props.public_key}</span>
        </div>
      )}
      {!isRepeater && props.role !== undefined && (
        <div className="node-popup__row">
          <span>Type</span>
          <span>{GPU_ROLE_LABELS[props.role] ?? 'Unknown'}</span>
        </div>
      )}
      <div className="node-popup__row">
        <span>Status</span>
        <span style={{ color: statusColor }}>{statusLabel}</span>
      </div>
      {props.hardware_model && (
        <div className="node-popup__row">
          <span>Hardware</span>
          <span>{props.hardware_model}</span>
        </div>
      )}
      <div className="node-popup__row">
        <span>Last seen</span>
        <span>{timeAgo(props.last_seen)}</span>
      </div>
      {props.advert_count !== null && props.advert_count !== undefined && (
        <div className="node-popup__row">
          <span>Times seen</span>
          <span>{props.advert_count}</span>
        </div>
      )}
      <div className="node-popup__row">
        <span>Position</span>
        <span>{props.is_prohibited ? 'Redacted' : `${lat.toFixed(5)}, ${lon.toFixed(5)}`}</span>
      </div>
      {props.is_prohibited && (
        <div className="node-popup__row">
          <span>Location</span>
          <span>Redacted within 1 mile radius</span>
        </div>
      )}
      {props.elevation_m !== null && props.elevation_m !== undefined && (
        <div className="node-popup__row">
          <span>Elevation</span>
          <span>{Math.round(props.elevation_m)} m ASL</span>
        </div>
      )}
      {isRepeater && !props.is_prohibited && (
        <div className="node-popup__row" style={{ marginTop: 6 }}>
          <button
            type="button"
            className={`node-popup__coverage-btn${coverageActive ? ' node-popup__coverage-btn--active' : ''}`}
            onClick={() => onToggleCoverage(props.node_id)}
            disabled={coverageLoading}
          >
            {coverageLoading ? 'Loading coverage…' : coverageActive ? 'Hide coverage' : 'Show coverage'}
          </button>
        </div>
      )}
      {coverageMessage && (
        <div className="node-popup__row">
          <span>Coverage</span>
          <span>{coverageMessage}</span>
        </div>
      )}
      {isRepeater && !props.is_prohibited && (
        <div className="node-popup__row" style={{ marginTop: 6 }}>
          <button
            type="button"
            className={`node-popup__coverage-btn${losActive ? ' node-popup__coverage-btn--active' : ''}`}
            onClick={() => onToggleLos(props.node_id)}
            disabled={losLoading}
          >
            {losLoading ? 'Loading LOS…' : losActive ? 'Hide LOS' : 'Show LOS'}
          </button>
        </div>
      )}
      {isRepeater && samePrefixCount > 1 && (
        <div className="node-popup__row" style={{ marginTop: 6 }}>
          <button
            type="button"
            className="node-popup__action-btn"
            onClick={() => onFocusSamePrefix(props.node_id)}
          >
            Focus same-prefix nodes
          </button>
        </div>
      )}
      {!isRepeater && links === null && (
        <div className="node-popup__neighbours-loading">Loading neighbours…</div>
      )}
      {!isRepeater && links !== null && links.length > 0 && (
        <div className="node-popup__neighbours">
          <div className="node-popup__neighbours-title">Confirmed neighbours</div>
          {links.map((lk) => {
            const tx = lk.count_this_to_peer > 0;
            const rx = lk.count_peer_to_this > 0;
            const arrow = tx && rx ? '↔' : tx ? '→' : '←';
            return (
              <div key={lk.peer_id} className="node-popup__neighbour-row">
                <span className="node-popup__neighbour-name">
                  {arrow} {lk.peer_name ?? lk.peer_id.slice(0, 8)}
                </span>
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
  );
};
