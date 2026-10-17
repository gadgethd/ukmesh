import { useMemo, useState } from 'react';

export interface NodeLink {
  peer_id: string;
  peer_name: string | null;
  observed_count: number;
  itm_path_loss_db: number | null;
  count_this_to_peer: number;
  count_peer_to_this: number;
  last_observed: string | null;
  recent_count_72h: number;
}

// Shared with the repeater page's existing last-seen and packet timestamps.
export function timeAgo(iso: string, now = Date.now()): string {
  const timestamp = Date.parse(iso);
  if (!Number.isFinite(timestamp)) return 'Unknown';
  const secs = Math.max(0, Math.floor((now - timestamp) / 1000));
  if (secs < 60) return `${secs}s ago`;
  if (secs < 3600) return `${Math.floor(secs / 60)}m ago`;
  if (secs < 86400) return `${Math.floor(secs / 3600)}h ago`;
  return `${Math.floor(secs / 86400)}d ago`;
}

function contactTime(link: NodeLink): number {
  const timestamp = link.last_observed ? Date.parse(link.last_observed) : NaN;
  return Number.isFinite(timestamp) ? timestamp : -Infinity;
}

type NeighbourSort = 'contact' | 'frequency';

export function sortNeighbourLinks(links: NodeLink[], sort: NeighbourSort): NodeLink[] {
  return [...links].sort((a, b) => {
    if (sort === 'frequency' && a.recent_count_72h !== b.recent_count_72h) {
      return b.recent_count_72h - a.recent_count_72h;
    }
    const aTime = contactTime(a);
    const bTime = contactTime(b);
    if (aTime !== bTime) return aTime > bTime ? -1 : 1;
    return a.peer_id.localeCompare(b.peer_id);
  });
}

export function NeighbourContact({ link, now = Date.now() }: { link: NodeLink; now?: number }) {
  const timestamp = contactTime(link);
  const known = Number.isFinite(timestamp);
  const age = Math.max(0, now - timestamp);
  const colour = !known ? 'var(--text-muted)'
    : age < 86400_000 ? 'var(--online)'
      : age <= 7 * 86400_000 ? 'var(--amber)' : 'var(--danger)';
  // Last contact is any-direction link-worker evidence, not "last direct".
  // Recent counts are public src/rx packet rows, including relayed traffic.
  // A healthy but quiet link is deliberately RARE even with many lifetime hits.
  const frequency = link.recent_count_72h >= 10 ? 'ACTIVE'
    : link.recent_count_72h >= 1 ? 'OCCASIONAL' : 'RARE';
  const frequencyColour = frequency === 'ACTIVE' ? 'var(--online)'
    : frequency === 'OCCASIONAL' ? 'var(--amber)' : 'var(--text-muted)';

  return (
    <div className="repeater-details-card__neighbour-stats" style={{ display: 'grid', gap: 6 }}>
      <span style={{ display: 'flex', alignItems: 'center', gap: 6, flexWrap: 'wrap' }}>
        <span aria-hidden="true" style={{ width: 7, height: 7, borderRadius: '50%', background: colour, flexShrink: 0 }} />
        <span>Last contact: {known
          ? <time dateTime={link.last_observed!} title={new Date(timestamp).toUTCString()}>{timeAgo(link.last_observed!, now)}</time>
          : 'Unknown'}</span>
      </span>
      <span style={{ display: 'flex', alignItems: 'baseline', gap: 6, flexWrap: 'wrap' }}>
        <strong style={{ color: frequencyColour, fontSize: 11, letterSpacing: '0.04em' }}>{frequency}</strong>
        <span>{link.recent_count_72h} {link.recent_count_72h === 1 ? 'contact' : 'contacts'} in last 72h</span>
      </span>
    </div>
  );
}

export function ConfirmedNeighbours({ links }: { links: NodeLink[] }) {
  const [sort, setSort] = useState<NeighbourSort>('contact');
  const sortedLinks = useMemo(() => sortNeighbourLinks(links, sort), [links, sort]);

  if (links.length === 0) {
    return <p className="repeater-details-card__empty-msg">No neighbours found for this node.</p>;
  }

  return (
    <>
      <p className="repeater-details-card__desc">
        Nodes with confirmed two-way communication. Contact includes relayed traffic.
        {' '}Active: 10+ contacts in 72h · Occasional: 1–9 · Rare: 0.
      </p>
      <label className="repeater-details-card__desc" style={{ display: 'flex', alignItems: 'center', gap: 8, flexWrap: 'wrap' }}>
        Sort neighbours
        <select
          value={sort}
          onChange={(event) => setSort(event.target.value as NeighbourSort)}
          style={{ padding: '8px 10px', minHeight: 40, borderRadius: 'var(--radius)', border: '1px solid var(--border)', background: 'var(--bg-secondary)', color: 'var(--text-primary)', font: 'inherit' }}
        >
          <option value="contact">Last contact</option>
          <option value="frequency">Most frequent</option>
        </select>
      </label>
      <div className="repeater-details-card__neighbours">
        {sortedLinks.map(link => (
          <div key={link.peer_id} className="repeater-details-card__neighbour" style={{ minWidth: 0, overflowWrap: 'anywhere' }}>
            <div className="repeater-details-card__neighbour-main">
              <span className="repeater-details-card__neighbour-name">
                {link.peer_name || `${link.peer_id.slice(0, 12)}...`}
              </span>
              <span className="repeater-details-card__neighbour-id">
                {link.peer_id.slice(0, 16)}…{link.peer_id.slice(-8)}
              </span>
            </div>
            <NeighbourContact link={link} />
            <div className="repeater-details-card__neighbour-stats">
              <span title="Lifetime observations">{link.observed_count}× seen</span>
              {link.itm_path_loss_db !== null && <span> · {Math.round(link.itm_path_loss_db)} dB loss</span>}
              <span> · TX: {link.count_this_to_peer}</span>
              <span> · RX: {link.count_peer_to_this}</span>
            </div>
          </div>
        ))}
      </div>
    </>
  );
}
