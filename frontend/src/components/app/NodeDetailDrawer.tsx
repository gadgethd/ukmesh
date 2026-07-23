import React, { useEffect, useMemo, useState } from 'react';
import { useNodeMap } from '../../hooks/useNodes.js';
import { withScopeParams } from '../../utils/api.js';
import { LoadingIndicator } from '../LoadingIndicator.js';
import { useWatchlist } from '../../hooks/useWatchlist.js';

type NodeLink = {
  peer_id: string;
  peer_name: string | null;
  observed_count: number;
  itm_path_loss_db: number | null;
  count_this_to_peer: number;
  count_peer_to_this: number;
};

type Props = {
  nodeId: string | null;
  network?: string;
  observer?: string;
  onClose: () => void;
};

const ROLE_NAMES: Record<number, string> = { 1: 'Companion', 2: 'Repeater', 3: 'Room server', 4: 'Sensor' };

function relativeTime(value: string): string {
  const seconds = Math.max(0, Math.round((Date.now() - Date.parse(value)) / 1_000));
  if (seconds < 60) return `${seconds}s ago`;
  if (seconds < 3_600) return `${Math.floor(seconds / 60)}m ago`;
  if (seconds < 86_400) return `${Math.floor(seconds / 3_600)}h ago`;
  return `${Math.floor(seconds / 86_400)}d ago`;
}

export const NodeDetailDrawer: React.FC<Props> = ({ nodeId, network, observer, onClose }) => {
  const nodes = useNodeMap();
  const node = nodeId ? nodes.get(nodeId) ?? nodes.get(nodeId.toLowerCase()) ?? null : null;
  const [links, setLinks] = useState<NodeLink[]>([]);
  const [historyCount, setHistoryCount] = useState<number | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const watchlist = useWatchlist();

  useEffect(() => {
    if (!nodeId) return undefined;
    const controller = new AbortController();
    setLoading(true);
    setError(null);
    const linkEndpoint = withScopeParams(`/api/nodes/${encodeURIComponent(nodeId)}/links`, { network, observer });
    const historyEndpoint = withScopeParams(`/api/nodes/${encodeURIComponent(nodeId)}/history?hours=24`, { network, observer });
    void Promise.all([
      fetch(linkEndpoint, { signal: controller.signal }).then((response) => response.ok ? response.json() as Promise<NodeLink[]> : []),
      fetch(historyEndpoint, { signal: controller.signal }).then((response) => response.ok ? response.json() as Promise<unknown> : []),
    ]).then(([nextLinks, history]) => {
      setLinks(Array.isArray(nextLinks) ? nextLinks : []);
      setHistoryCount(Array.isArray(history) ? history.length : 0);
    }).catch((reason: unknown) => {
      if ((reason as DOMException).name !== 'AbortError') setError('Recent node details are temporarily unavailable.');
    }).finally(() => setLoading(false));
    return () => controller.abort();
  }, [network, nodeId, observer]);

  const directionalObservations = useMemo(
    () => links.reduce((sum, link) => sum + Number(link.count_this_to_peer || 0) + Number(link.count_peer_to_this || 0), 0),
    [links],
  );

  if (!nodeId) return null;
  return (
    <aside className="node-drawer" aria-label="Node details">
      <header className="node-drawer__header">
        <div>
          <span className={`node-drawer__status${node?.is_online ? ' node-drawer__status--online' : ''}`}>
            {node?.is_online ? 'Online' : 'Offline'}
          </span>
          <h2>{node?.name ?? `Node ${nodeId.slice(0, 8)}`}</h2>
        </div>
        <button type="button" onClick={onClose} aria-label="Close node details">×</button>
      </header>
      <div className="node-drawer__identity">
        <code>{nodeId}</code>
        <dl>
          <div><dt>Role</dt><dd>{ROLE_NAMES[node?.role ?? 2] ?? 'Unknown'}</dd></div>
          <div><dt>Last heard</dt><dd>{node?.last_seen ? relativeTime(node.last_seen) : 'Unknown'}</dd></div>
          <div><dt>Hardware</dt><dd>{node?.hardware_model ?? 'Not reported'}</dd></div>
          <div><dt>Elevation</dt><dd>{node?.elevation_m != null ? `${Math.round(node.elevation_m)} m` : 'Unknown'}</dd></div>
        </dl>
      </div>
      <button type="button" className="node-drawer__watch" onClick={() => watchlist.toggle('node', nodeId, node?.name ?? nodeId.slice(0, 8))}>
        {watchlist.isWatched('node', nodeId) ? '★ Watching node' : '☆ Watch node'}
      </button>
      {loading && <LoadingIndicator label="Loading recent evidence…" variant="inline" />}
      {error && <p className="node-drawer__error" role="status">{error}</p>}
      {!loading && (
        <>
          <div className="node-drawer__metrics">
            <div><strong>{links.length}</strong><span>viable neighbours</span></div>
            <div><strong>{directionalObservations.toLocaleString()}</strong><span>link observations</span></div>
            <div><strong>{historyCount ?? 0}</strong><span>24h history samples</span></div>
          </div>
          <section className="node-drawer__links">
            <h3>Strongest neighbours</h3>
            {links.length === 0 ? <p>No recent viable relationships.</p> : (
              <ol>
                {links.slice(0, 8).map((link) => (
                  <li key={link.peer_id}>
                    <span>{link.peer_name ?? link.peer_id.slice(0, 8)}</span>
                    <strong>{Number(link.observed_count).toLocaleString()}</strong>
                    <small>{link.itm_path_loss_db == null ? 'unmodelled' : `${link.itm_path_loss_db.toFixed(1)} dB`}</small>
                  </li>
                ))}
              </ol>
            )}
          </section>
        </>
      )}
      <p className="node-drawer__freshness">Live status may be newer than the historical relationship data.</p>
    </aside>
  );
};
