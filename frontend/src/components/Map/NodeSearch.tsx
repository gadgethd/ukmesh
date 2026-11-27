import React, { useState, useMemo, useEffect, useRef } from 'react';
import Fuse from 'fuse.js';
import type * as maplibregl from 'maplibre-gl';
import type { MeshNode } from '../../types/domain.js';
import { useNodeMetadata } from '../../hooks/useNodes.js';
import { PROHIBITED_NODE_MARKER, isProhibitedMapNode, isValidMapCoord } from '../../utils/pathing.js';
import { scopeKey, type PublicScope } from '../../utils/scope.js';
import { FOCUS_SEARCH_EVENT } from '../../utils/appEvents.js';
import { useNodeNameSearch } from '../../hooks/useNodeNameSearch.js';
import { useWatchlist } from '../../hooks/useWatchlist.js';
import { Combobox, type ComboboxOption } from '../ui/Combobox.js';
import { nearestRepeaters } from '../../utils/exploreLinks.js';

interface NodeSearchProps {
  map: maplibregl.Map | null;
  scope: PublicScope;
  onNodeSelect?: (nodeId: string) => void;
}

export const NodeSearch: React.FC<NodeSearchProps> = ({ map, scope, onNodeSelect }) => {
  const nodes = useNodeMetadata();
  const restoredQuery = new URLSearchParams(window.location.search).get('search')?.trim() ?? '';
  const [query, setQuery] = useState(restoredQuery);
  const nameMatches = useNodeNameSearch(query, scope);
  const [open, setOpen] = useState(Boolean(restoredQuery));
  const inputRef = useRef<HTMLInputElement>(null);
  const watchlist = useWatchlist();
  const nearbyScope = scopeKey(scope);
  const [nearby, setNearby] = useState<{ scope: string; nodes: MeshNode[] } | null>(null);
  const nearbyNodes = nearby?.scope === nearbyScope ? nearby.nodes : null;
  const locationRequest = useRef(0);
  const [geoStatus, setGeoStatus] = useState<string | null>(null);
  useEffect(() => {
    setGeoStatus(null);
    return () => { locationRequest.current += 1; };
  }, [nearbyScope]);

  const searchableNodes = useMemo(() => Array.from(nodes.values()).filter(
    (node) => isValidMapCoord(node.lat, node.lon) && !node.name?.includes(PROHIBITED_NODE_MARKER),
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
    const matches = new Map(nameMatches.filter(node => isValidMapCoord(node.lat, node.lon)
      && !isProhibitedMapNode(node)).map((node) => [node.node_id, node]));
    for (const node of [...prefixMatches, ...fuzzyMatches]) {
      if (!matches.has(node.node_id)) matches.set(node.node_id, node);
    }
    return Array.from(matches.values())
      .slice(0, 6);
  }, [fuse, nearbyNodes, query, searchableNodes, nameMatches]);

  useEffect(() => {
    const focusSearch = () => {
      inputRef.current?.focus();
      setOpen(true);
    };
    window.addEventListener(FOCUS_SEARCH_EVENT, focusSearch);
    return () => window.removeEventListener(FOCUS_SEARCH_EVENT, focusSearch);
  }, []);

  useEffect(() => {
    if (!restoredQuery) return;
    inputRef.current?.focus();
    setOpen(true);
  }, [restoredQuery]);

  const findNearby = () => {
    const request = ++locationRequest.current;
    if (!navigator.geolocation) {
      setGeoStatus('Location is not supported by this browser.');
      setOpen(true);
      return;
    }
    setGeoStatus('Requesting location permission…');
    setOpen(true);
    navigator.geolocation.getCurrentPosition(
      ({ coords }) => {
        if (request !== locationRequest.current) return;
        const nearest = nearestRepeaters(searchableNodes, coords, 6);
        setNearby({ scope: nearbyScope, nodes: nearest });
        setGeoStatus(nearest.length ? 'Nearest repeaters' : 'No nearby repeaters found.');
      },
      (error) => {
        if (request !== locationRequest.current) return;
        setGeoStatus(error.code === error.PERMISSION_DENIED
          ? 'Location access was denied. Search by name or observer code.'
          : 'Could not get your location. Search by name or observer code.');
      },
      { enableHighAccuracy: false, timeout: 10_000, maximumAge: 300_000 },
    );
  };

  const select = (node: MeshNode) => {
    // MapLibre flyTo: center is [lon, lat]
    map?.flyTo({ center: [node.lon!, node.lat!], zoom: 15 });
    onNodeSelect?.(node.node_id);
    setQuery('');
    locationRequest.current += 1;
    setNearby(null);
    setGeoStatus(null);
    setOpen(false);
    const url = new URL(window.location.href);
    url.searchParams.delete('search');
    window.history.replaceState(null, '', url);
  };

  const options = useMemo<ComboboxOption[]>(() => results.map((node) => ({
    id: node.node_id,
    label: node.name ?? node.node_id,
    content: (
      <>
        <span className="node-search__result-name">{node.name ?? 'Unnamed node'}</span>
        <span className="node-search__result-key">
          {node.iata ?? node.public_key?.slice(0, 8) ?? node.node_id.slice(0, 8)}
        </span>
      </>
    ),
  })), [results]);

  return (
    <div className="node-search">
      <div className="node-search__controls">
        <Combobox
          label="Search map nodes"
          value={query}
          onValueChange={(value) => {
            setQuery(value);
            locationRequest.current += 1;
            setNearby(null);
            setGeoStatus(null);
            setOpen(true);
          }}
          onSelectionChange={(id) => {
            const node = results.find((entry) => entry.node_id === id);
            if (node) select(node);
          }}
          options={options}
          placeholder="Name, observer code or key prefix"
          isOpen={open && (results.length > 0 || Boolean(geoStatus) || Boolean(query.trim()))}
          onOpenChange={setOpen}
          inputRef={inputRef}
          className="node-search__combobox"
          inputClassName="node-search__input"
          popoverClassName="node-search__results"
          optionClassName="node-search__result"
          emptyContent={geoStatus ?? (query.trim() ? 'No matching nodes' : 'Type to search nodes')}
          footer={(
            <>
              {geoStatus && results.length > 0 ? <div className="node-search__status">{geoStatus}</div> : null}
              {query.trim() ? (
                <button
                  type="button"
                  className="node-search__save"
                  onClick={() => {
                    watchlist.toggle('search', query.trim().toLowerCase(), query.trim());
                    setOpen(false);
                  }}
                >
                  {watchlist.isWatched('search', query.trim().toLowerCase()) ? '★ Saved search' : '☆ Save this search'}
                </button>
              ) : null}
            </>
          )}
        />
        <button type="button" className="node-search__nearby" onClick={findNearby} title="Find nearby repeaters" aria-label="Find nearby repeaters">⌖</button>
      </div>
    </div>
  );
};
