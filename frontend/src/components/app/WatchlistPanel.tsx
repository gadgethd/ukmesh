import React, { useState } from 'react';
import { useWatchlist } from '../../hooks/useWatchlist.js';

export const WatchlistPanel: React.FC = () => {
  const { entries, remove } = useWatchlist();
  const [open, setOpen] = useState(() => {
    try { return localStorage.getItem('meshcore-watchlist-collapsed-v1') !== '1'; }
    catch { return true; }
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
            <span><small>{entry.category.replace(/_/g, ' ')}</small>{entry.label}</span>
            <button type="button" onClick={() => remove(entry.category, entry.id)} aria-label={`Remove ${entry.label} from watchlist`}>×</button>
          </li>
        ))}</ul>
      )}
    </details>
  );
};
