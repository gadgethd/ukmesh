import React, { useState } from 'react';
import { LoadingIndicator } from '../LoadingIndicator.js';
import { NODE_STALE_AFTER_MS } from './mapConfig.js';
import type { NodeFeatureProps, NodeLink } from './types.js';
import { LinkQualitySparkline } from './LinkQualitySparkline.js';
import { Tab, TabList, TabPanel, Tabs } from '../ui/Tabs.js';

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
  /** Hide the internal name heading (the docked panel renders its own header). */
  hideName?: boolean;
  onFocusSamePrefix: (nodeId: string) => void;
  samePrefixCount: number;
  losActive: boolean;
  losLoading: boolean;
  onToggleLos: (nodeId: string) => void;
  network?: string;
  observer?: string;
  privacyGeneration: number;
}> = ({
  props,
  lat,
  lon,
  links,
  hideName = false,
  onFocusSamePrefix,
  samePrefixCount,
  losActive,
  losLoading,
  onToggleLos,
  network,
  observer,
  privacyGeneration,
}) => {
  const isRepeater = props.role === undefined || props.role === 2;
  const ageMs = Date.now() - new Date(props.last_seen).getTime();
  const isStale = ageMs > NODE_STALE_AFTER_MS;
  const statusLabel = isStale ? 'STALE' : props.is_online ? 'ONLINE' : 'OFFLINE';
  const statusColor = isStale ? 'var(--danger)' : props.is_online ? 'var(--online)' : 'var(--offline-text)';
  const fallbackName = GPU_ROLE_LABELS[props.role ?? 2] ?? 'Unknown Device';
  const displayName = props.is_prohibited
    ? `Redacted ${fallbackName}`
    : (props.name ?? `Unknown ${fallbackName}`);
  const [tab, setTab] = useState<'info' | 'links' | 'activity' | 'path' | 'status'>('info');

  return (
    <div className="node-popup">
      {!hideName && <div className="node-popup__name">{displayName}</div>}
      <Tabs
        selectedKey={tab}
        onSelectionChange={(key) => setTab(key as typeof tab)}
        className="node-popup__tab-system"
      >
      <TabList className="node-popup__tabs" aria-label="Node details">
        {(['info', 'links', 'activity', 'path', 'status'] as const).map((value) => (
          <Tab id={value} key={value}>
            {value[0].toUpperCase() + value.slice(1)}
          </Tab>
        ))}
      </TabList>
      <TabPanel id="info">
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
          <span>Adverts seen</span>
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
      </TabPanel>
      <TabPanel id="status">
        <div className="node-popup__row"><span>Status</span><span style={{ color: statusColor }}>{statusLabel}</span></div>
        {props.hardware_model && <div className="node-popup__row"><span>Hardware</span><span>{props.hardware_model}</span></div>}
        <div className="node-popup__row"><span>Freshness</span><span>{timeAgo(props.last_seen)}</span></div>
      </TabPanel>
      <TabPanel id="activity">
        <div className="node-popup__row"><span>Last seen</span><span>{timeAgo(props.last_seen)}</span></div>
        <div className="node-popup__row"><span>Adverts</span><span>{props.advert_count ?? 'No samples'}</span></div>
      </TabPanel>
      <TabPanel id="path">
      {isRepeater && !props.is_prohibited && (
        <div className="node-popup__row" style={{ marginTop: 6 }}>
          <button
            type="button"
            className={`node-popup__coverage-btn${losActive ? ' node-popup__coverage-btn--active' : ''}`}
            onClick={() => onToggleLos(props.node_id)}
            disabled={losLoading}
          >
            {losLoading ? <LoadingIndicator label="Loading LOS..." variant="inline" /> : losActive ? 'Hide LOS' : 'Show LOS'}
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
      </TabPanel>
      <TabPanel id="links">
      {links === null && (
        <div className="node-popup__neighbours-loading">
          <LoadingIndicator label="Loading neighbours..." variant="inline" />
        </div>
      )}
      {links !== null && links.length > 0 && (
        <div className="node-popup__neighbours">
          <div className="node-popup__neighbours-title">Confirmed neighbours</div>
          {links.slice(0, 8).map((lk) => {
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
                <LinkQualitySparkline
                  source={props.node_id}
                  target={lk.peer_id}
                  network={network}
                  observer={observer}
                  privacyGeneration={privacyGeneration}
                />
              </div>
            );
          })}
        </div>
      )}
      {links !== null && links.length === 0 && <div className="node-popup__muted">No confirmed neighbours.</div>}
      </TabPanel>
      </Tabs>
    </div>
  );
};
