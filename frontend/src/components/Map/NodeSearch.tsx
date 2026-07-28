import React, { useState, useMemo, useEffect, useRef } from 'react';
import Fuse from 'fuse.js';
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
  const [nearbyNodes, setNearbyNodes] = useState<MeshNode[] | null>(null);
  const [geoStatus, setGeoStatus] = useState<string | null>(null);

  const searchableNodes = useMemo(() => Array.from(nodes.values()).filter(
    (node) => isValidMapCoord(node.lat, node.lon) && !node.name?.includes('🚫'),
  ), [nodes]);
  const fuse = useMemo(() => new Fuse(searchableNodes, {
    keys: [
      { name: 'name', weight: 0.55 },
      { name: 'iata', weight: 0.3 },
      { name: 'node_id', weight: 0.1 },
      { name: 'public_key', weight: 0.05 },
    ],
    threshold: 0.35,
    ignoreLocation: true,
    includeScore: true,
  }), [searchableNodes]);

  const results = useMemo(() => {
    if (!query.trim()) return nearbyNodes ?? [];
    const q = query.trim().toLowerCase();
    const prefixMatches = searchableNodes.filter((node) =>
      node.name?.toLowerCase().startsWith(q)
      || node.iata?.toLowerCase().startsWith(q)
      || node.node_id.toLowerCase().startsWith(q)
      || node.public_key?.toLowerCase().startsWith(q),
    );
    const fuzzyMatches = fuse.search(q).map((result) => result.item);
    return Array.from(new Map([...prefixMatches, ...fuzzyMatches].map((node) => [node.node_id, node])).values())
      .slice(0, 6);
  }, [fuse, nearbyNodes, query, searchableNodes]);

  useEffect(() => {
    const handler = (e: MouseEvent) => {
      if (containerRef.current && !containerRef.current.contains(e.target as Node))
        setOpen(false);
    };
    document.addEventListener('mousedown', handler);
    return () => document.removeEventListener('mousedown', handler);
  }, []);

  useEffect(() => {
    const focusSearch = () => {
      const input = containerRef.current?.querySelector<HTMLInputElement>('input');
      input?.focus();
      setOpen(true);
    };
    window.addEventListener('meshcore:focus-search', focusSearch);
    return () => window.removeEventListener('meshcore:focus-search', focusSearch);
  }, []);

  const findNearby = () => {
    if (!navigator.geolocation) {
      setGeoStatus('Location is not supported by this browser.');
      setOpen(true);
      return;
    }
    setGeoStatus('Requesting location permission…');
    setOpen(true);
    navigator.geolocation.getCurrentPosition(
      ({ coords }) => {
        const distanceKm = (node: MeshNode) => {
          const lat1 = coords.latitude * Math.PI / 180;
          const lat2 = node.lat! * Math.PI / 180;
          const dLat = lat2 - lat1;
          const dLon = (node.lon! - coords.longitude) * Math.PI / 180;
          const a = Math.sin(dLat / 2) ** 2 + Math.cos(lat1) * Math.cos(lat2) * Math.sin(dLon / 2) ** 2;
          return 6371 * 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
        };
        const nearest = searchableNodes
          .filter((node) => node.role === undefined || node.role === 2)
          .sort((a, b) => distanceKm(a) - distanceKm(b))
          .slice(0, 6);
        setNearbyNodes(nearest);
        setGeoStatus(nearest.length ? 'Nearest repeaters' : 'No nearby repeaters found.');
        map?.flyTo({ center: [coords.longitude, coords.latitude], zoom: 11 });
      },
      (error) => setGeoStatus(error.code === error.PERMISSION_DENIED
        ? 'Location permission was denied.'
        : 'Unable to determine your location.'),
      { enableHighAccuracy: false, timeout: 10_000, maximumAge: 300_000 },
    );
  };

  const select = (node: MeshNode) => {
    // MapLibre flyTo: center is [lon, lat]
    map?.flyTo({ center: [node.lon!, node.lat!], zoom: 15 });
    onNodeSelect?.(node.node_id);
    setQuery('');
    setNearbyNodes(null);
    setGeoStatus(null);
    setOpen(false);
  };

  return (
    <div ref={containerRef} className="node-search">
      <div className="node-search__controls">
        <input
          className="node-search__input"
          type="text"
          placeholder="Name, IATA or prefix…"
          value={query}
          onChange={(e) => { setQuery(e.target.value); setNearbyNodes(null); setGeoStatus(null); setOpen(true); }}
          onFocus={() => { if (query || nearbyNodes) setOpen(true); }}
          onKeyDown={(e) => { if (e.key === 'Escape') setOpen(false); }}
        />
        <button type="button" className="node-search__nearby" onClick={findNearby} title="Find nearby repeaters" aria-label="Find nearby repeaters">⌖</button>
      </div>
      {open && (results.length > 0 || geoStatus) && (
        <div className="node-search__results">
          {geoStatus && <div className="node-search__status">{geoStatus}</div>}
          {results.map((node) => (
            <button type="button" key={node.node_id} className="node-search__result" onClick={() => select(node)}>
              <span className="node-search__result-name">{node.name}</span>
              <span className="node-search__result-key">{node.iata ?? node.public_key?.slice(0, 8) ?? node.node_id.slice(0, 8)}</span>
            </button>
          ))}
          {query.trim() && <button type="button" className="node-search__save" onClick={() => { watchlist.toggle('search', query.trim().toLowerCase(), query.trim()); setOpen(false); }}>
            {watchlist.isWatched('search', query.trim().toLowerCase()) ? '★ Saved search' : '☆ Save this search'}
          </button>}
        </div>
      )}
    </div>
  );
};
