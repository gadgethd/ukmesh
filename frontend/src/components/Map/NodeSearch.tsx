import React, { useState, useMemo, useEffect, useRef } from 'react';
import type { Map as LeafletMap } from 'leaflet';
import type { MeshNode } from '../../hooks/useNodes.js';
import { isValidMapCoord } from '../../utils/pathing.js';

interface NodeSearchProps {
  nodes: Map<string, MeshNode>;
  map: LeafletMap | null;
}

export const NodeSearch: React.FC<NodeSearchProps> = ({ nodes, map }) => {
  const [query, setQuery] = useState('');
  const [open, setOpen]   = useState(false);
  const containerRef      = useRef<HTMLDivElement>(null);

  const results = useMemo(() => {
    if (!query.trim()) return [];
    const q = query.toLowerCase();
    return Array.from(nodes.values())
      .filter((n) => isValidMapCoord(n.lat, n.lon) && n.name && !n.name.includes('🚫') && n.name.toLowerCase().includes(q))
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
    map?.flyTo([node.lat!, node.lon!], 15);
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
            <div key={node.node_id} className="node-search__result" onMouseDown={() => select(node)}>
              {node.name}
            </div>
          ))}
        </div>
      )}
    </div>
  );
};
