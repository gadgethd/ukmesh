import React from 'react';
import { LoadingIndicator } from '../LoadingIndicator.js';
import { LINK_AMBER_THRESHOLD_DB, LINK_GREEN_THRESHOLD_DB } from './mapConfig.js';
import type { PredictedLink } from './types.js';

function linkColor(pathLoss: number | null): string {
  if (pathLoss == null) return 'var(--text-dim, #9ca3af)';
  if (pathLoss <= LINK_GREEN_THRESHOLD_DB) return '#22c55e';
  if (pathLoss <= LINK_AMBER_THRESHOLD_DB) return '#fbbf24';
  return '#ef4444';
}

/**
 * Popup for a user-placed planned (hypothetical) repeater. Mirrors the normal
 * node popup but surfaces the placement coordinates, computation status, the
 * server-predicted links to nearby repeaters, and a remove action.
 */
export const PlannedRepeaterPopup: React.FC<{
  planId: string;
  lat: number;
  lon: number;
  status: 'queued' | 'ready' | 'error';
  links: PredictedLink[] | undefined;
  getPeerName: (peerId: string) => string;
  onRemove: (planId: string) => void;
  losActive: boolean;
  losLoading: boolean;
  onToggleLos: (planId: string) => void;
}> = ({ lat, lon, status, links, getPeerName, planId, onRemove, losActive, losLoading, onToggleLos }) => {
  const statusLabel = status === 'ready' ? 'READY' : status === 'error' ? 'FAILED' : 'COMPUTING';
  const statusColor = status === 'ready' ? 'var(--online)' : status === 'error' ? 'var(--danger)' : 'var(--text-dim, #9ca3af)';

  return (
    <div className="node-popup">
      <div className="node-popup__name">Planned Repeater</div>
      <div className="node-popup__row">
        <span>Status</span>
        <span style={{ color: statusColor }}>{statusLabel}</span>
      </div>
      <div className="node-popup__row">
        <span>Position</span>
        <span>{lat.toFixed(5)}, {lon.toFixed(5)}</span>
      </div>

      {status === 'queued' && (
        <div className="node-popup__neighbours-loading">
          <LoadingIndicator label="Computing coverage & links..." variant="inline" />
        </div>
      )}

      {status === 'ready' && (
        <div className="node-popup__neighbours">
          <div className="node-popup__neighbours-title">
            Predicted links{links && links.length > 0 ? ` (${links.length})` : ''}
          </div>
          {!links || links.length === 0 ? (
            <div className="node-popup__neighbour-row">
              <span className="node-popup__neighbour-meta">No viable links to nearby repeaters</span>
            </div>
          ) : (
            links.map((link) => (
              <div key={link.peer_id} className="node-popup__neighbour-row">
                <span className="node-popup__neighbour-name">
                  <span style={{ color: linkColor(link.itm_path_loss_db) }}>●</span>{' '}
                  {link.peer_name ?? getPeerName(link.peer_id)}
                </span>
                <span className="node-popup__neighbour-meta">
                  {link.distance_km != null && <>{link.distance_km.toFixed(1)} km</>}
                  {link.itm_path_loss_db != null && <> &middot; {Math.round(link.itm_path_loss_db)} dB</>}
                </span>
              </div>
            ))
          )}
          <div className="node-popup__row" style={{ marginTop: 4 }}>
            <span className="node-popup__neighbour-meta">
              Sight-lines are shown automatically; enable the Links toggle for the flat map links.
            </span>
          </div>
        </div>
      )}

      {status === 'ready' && (
        <div className="node-popup__row" style={{ marginTop: 6 }}>
          <button
            type="button"
            className={`node-popup__coverage-btn${losActive ? ' node-popup__coverage-btn--active' : ''}`}
            onClick={() => onToggleLos(planId)}
            disabled={losLoading}
          >
            {losLoading ? <LoadingIndicator label="Loading LOS..." variant="inline" /> : losActive ? 'Hide LOS' : 'Show LOS'}
          </button>
        </div>
      )}

      <div className="node-popup__row" style={{ marginTop: 6 }}>
        <button
          type="button"
          className="node-popup__action-btn"
          onClick={() => onRemove(planId)}
        >
          Remove planned repeater
        </button>
      </div>
    </div>
  );
};
