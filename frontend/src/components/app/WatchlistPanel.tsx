import React from 'react';
import { useWatchlist } from '../../hooks/useWatchlist.js';

export const WatchlistPanel: React.FC = () => {
  const { entries, remove } = useWatchlist();
  return (
    <details className="watchlist-panel">
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
