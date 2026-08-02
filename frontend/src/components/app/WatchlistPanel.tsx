import React, { useState } from 'react';
import { useWatchlist, type WatchEntry } from '../../hooks/useWatchlist.js';
import { getCurrentSite } from '../../config/site.js';

export function watchEntryHref(entry: WatchEntry): string {
  const site = getCurrentSite();
  const target = entry.category === 'node' || entry.category === 'search'
    ? new URL(site.appUrl)
    : new URL(site.appHomeUrl);
  if (entry.category === 'node') target.searchParams.set('node', entry.id);
  if (entry.category === 'search') target.searchParams.set('search', entry.id);
  if (entry.category === 'region' || entry.category === 'observer') {
    target.pathname = '/stats';
    target.searchParams.set('tab', 'observers');
    target.searchParams.set('region', entry.id);
  }
  if (entry.category === 'packet_type') {
    target.pathname = '/feed';
    target.searchParams.set('type', entry.id);
  }
  if (entry.category === 'spam_incident') {
    target.pathname = '/spam';
    target.searchParams.set('incident', entry.id);
  }
  target.searchParams.set('network', entry.scope);
  return target.toString();
}

export const WatchlistPanel: React.FC = () => {
  const { entries, remove } = useWatchlist();
  const [open, setOpen] = useState(() => {
    try {
      const stored = localStorage.getItem('meshcore-watchlist-collapsed-v1');
      if (stored != null) return stored !== '1';
      return !window.matchMedia('(max-width: 640px)').matches;
    } catch { return true; }
  });
  return (
    <details
      className="watchlist-panel"
      open={open}
      onToggle={(event) => {
        const next = event.currentTarget.open;
        setOpen(next);
        try { localStorage.setItem('meshcore-watchlist-collapsed-v1', next ? '0' : '1'); }
        catch { /* persistence is best-effort */ }
      }}
    >
      <summary>Watchlist <span>{entries.length}</span></summary>
      {entries.length === 0 ? <p>Star nodes, searches, regions, packet types, or spam incidents to keep them here.</p> : (
        <ul>{entries.map((entry) => (
          <li key={`${entry.category}:${entry.id}`}>
            <a href={watchEntryHref(entry)}>
              <small>{entry.category.replace(/_/g, ' ')} · {entry.scope}</small>
              {entry.label}
            </a>
            <button type="button" onClick={() => remove(entry.category, entry.id, entry.scope)} aria-label={`Remove ${entry.label} from watchlist`}>×</button>
          </li>
        ))}</ul>
      )}
    </details>
  );
};
