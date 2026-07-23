import React, { useState, useMemo, useEffect, useRef } from 'react';
import type maplibregl from 'maplibre-gl';
import type { MeshNode } from '../../hooks/useNodes.js';
import { useNodeMap } from '../../hooks/useNodes.js';
import { isValidMapCoord } from '../../utils/pathing.js';
import { useWatchlist } from '../../hooks/useWatchlist.js';

interface NodeSearchProps {
  map: maplibregl.Map | null;
  onNodeSelect?: (nodeId: string) => void;
}

export const NodeSearch: React.FC<NodeSearchProps> = ({ map, onNodeSelect }) => {
  const nodes = useNodeMap();
  const [query, setQuery] = useState('');
  const [open, setOpen] = useState(false);
  const containerRef = useRef<HTMLDivElement>(null);
  const watchlist = useWatchlist();

  const results = useMemo(() => {
    if (!query.trim()) return [];
    const q = query.toLowerCase();
    return Array.from(nodes.values())
      .filter((n) => {
        if (!isValidMapCoord(n.lat, n.lon)) return false;
        const nameMatch = n.name && !n.name.includes('🚫') && n.name.toLowerCase().includes(q);
        const keyMatch = n.public_key && n.public_key.toLowerCase().includes(q);
        return nameMatch || keyMatch;
      })
      .slice(0, 6);
  }, [query, nodes]);

  useEffect(() => {
    const handler = (e: MouseEvent) => {
      if (containerRef.current && !containerRef.current.contains(e.target as Node))
        setOpen(false);
    };
    document.addEventListener('mousedown', handler);
    return () => document.removeEventListener('mousedown', handler);
  }, []);

  const select = (node: MeshNode) => {
    // MapLibre flyTo: center is [lon, lat]
    map?.flyTo({ center: [node.lon!, node.lat!], zoom: 15 });
    onNodeSelect?.(node.node_id);
    setQuery('');
    setOpen(false);
  };

  return (
    <div ref={containerRef} className="node-search">
      <input
        className="node-search__input"
        type="text"
        placeholder="Search nodes…"
        value={query}
        onChange={(e) => { setQuery(e.target.value); setOpen(true); }}
        onFocus={() => { if (query) setOpen(true); }}
        onKeyDown={(e) => { if (e.key === 'Escape') setOpen(false); }}
      />
      {open && results.length > 0 && (
        <div className="node-search__results">
          {results.map((node) => (
            <button type="button" key={node.node_id} className="node-search__result" onClick={() => select(node)}>
              <span className="node-search__result-name">{node.name}</span>
              {node.public_key && (
                <span className="node-search__result-key">{node.public_key.slice(0, 8)}…</span>
              )}
            </button>
          ))}
          <button type="button" className="node-search__save" onClick={() => { watchlist.toggle('search', query.trim().toLowerCase(), query.trim()); setOpen(false); }}>
            {watchlist.isWatched('search', query.trim().toLowerCase()) ? '★ Saved search' : '☆ Save this search'}
          </button>
        </div>
      )}
    </div>
  );
};
