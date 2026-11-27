import '../../styles/repeater.css';
import React, { useState, useEffect, useMemo, useRef } from 'react';
import { Link, useNavigationType, useSearchParams } from 'react-router';
import { LoadingIndicator } from '../../components/LoadingIndicator.js';
import { getCurrentSite } from '../../config/site.js';
import { useRuntimeFeatures } from '../../config/runtimeFeatures.js';
import { useNodeLookup } from '../../hooks/useNodeNameSearch.js';
import { fetchJson, withScopeParams } from '../../utils/api.js';
import { ScopedCache } from '../../utils/scopedCache.js';
import { scopeKey } from '../../utils/scope.js';
import { Combobox, type ComboboxOption } from '../../components/ui/Combobox.js';
import { ConfirmedNeighbours, type NodeLink } from '../../components/ConfirmedNeighbours.js';
import { timeAgo } from '../../utils/time.js';
import { isPublicRepeater, liveMapLink, nearestRepeaters } from '../../utils/exploreLinks.js';
import type { MeshNode } from '../../types/domain.js';

interface PacketHistory {
  time: string;
  packet_hash: string;
  src_node_id: string;
  topic: string;
  packet_type: number;
  hop_count: number;
  rssi: number;
  snr: number;
}

interface AdvertPacket {
  time: string;
  packet_hash: string;
}

interface NodeStatusLatest {
  radio: string | null;
}

interface TopAdvertNode {
  node_id: string;
  name?: string;
  iata?: string;
  role?: number;
  lat?: number;
  lon?: number;
  last_seen: string;
  is_online: boolean;
  advert_count?: number;
  adverts_in_window: number;
  last_advert_at: string;
}

type PublicMapPage = {
  nodes: MeshNode[];
  page: {
    snapshot: string;
    nextCursor: string | null;
    complete: boolean;
    returned: number;
    rowLimit: number;
  };
};
const NODE_DETAIL_TTL_MS = 5 * 60_000;
const MAP_PAGE_LIMIT = 2000;
const MAP_MAX_PAGES = 100;
function detailCache<Row>(name: string, limitMiB: number) {
  return new ScopedCache<Row[]>({
    name, ttlMs: NODE_DETAIL_TTL_MS, maxEntries: 128, maxBytes: limitMiB * 1024 * 1024, maxInflight: 4,
  });
}

const neighbourCache = detailCache<NodeLink>('repeater-neighbours', 3);
const historyCache = detailCache<PacketHistory>('repeater-packet-history', 6);
const advertCache = detailCache<AdvertPacket>('repeater-advert-history', 3);
const radioCache = detailCache<NodeStatusLatest>('repeater-radio-status', 1);

function formatRadio(radio: unknown): string | null {
  if (typeof radio !== 'string' || !radio.trim()) return null;
  const parts = radio.split(',').map(part => part.trim());
  if (parts.length !== 4 || !parts.every(part => /^\d+(\.\d+)?$/.test(part))) return radio;
  return `${parts[0]} MHz · ${parts[1]} kHz · SF${parts[2]} · CR${parts[3]}`;
}

function useRepeaterDetail<Row>(path: string | undefined, scope: string, cache: ScopedCache<Row[]>, maxMiB: number) {
  const [attempt, setAttempt] = useState(0);
  const [result, setResult] = useState<{ key: string; rows: Row[] | null }>();
  const key = JSON.stringify([scope, path, attempt]);
  useEffect(() => {
    cache.activateScope(scope);
    if (!path) return;
    const controller = new AbortController();
    void cache.getOrLoad(scope, path, signal => fetchJson<Row[]>(
      path, { signal, cache: 'no-store' },
      { timeoutMs: 8_000, maxBytes: maxMiB * 1024 * 1024, validate: Array.isArray },
    ), controller.signal).then(rows => {
      if (!controller.signal.aborted) setResult({ key, rows });
    }).catch(() => {
      if (!controller.signal.aborted) setResult({ key, rows: null });
    });
    return () => controller.abort();
  }, [cache, key, maxMiB, path, scope]);
  return { rows: result?.key === key ? result.rows : undefined, retry: () => setAttempt(value => value + 1) };
}

async function loadCompleteNodeSnapshot(
  signal: AbortSignal,
  network: string,
  observer: string | undefined,
): Promise<MeshNode[]> {
  const nodes = new Map<string, MeshNode>();
  let snapshot: string | null = null;
  let cursor: string | null = null;
  const seenCursors = new Set<string>();

  for (let pageNumber = 0; pageNumber < MAP_MAX_PAGES; pageNumber += 1) {
    const params = new URLSearchParams({
      network,
      fields: 'node_id,name,lat,lon,iata,role,last_seen,is_online,hardware_model,advert_count,elevation_m',
      limit: String(MAP_PAGE_LIMIT),
    });
    if (observer) params.set('observer', observer);
    if (snapshot) params.set('snapshot', snapshot);
    if (cursor) params.set('cursor', cursor);
    const payload = await fetchJson<PublicMapPage>(
      `/api/nodes/map?${params}`,
      { signal, cache: 'no-store' },
      { timeoutMs: 20_000, maxBytes: 12 * 1024 * 1024 },
    );
    if (!payload || !Array.isArray(payload.nodes) || !payload.page) {
      throw new Error('Repeater index returned an invalid page');
    }
    if (snapshot && payload.page.snapshot !== snapshot) {
      throw new Error('Repeater index snapshot changed while paging');
    }
    snapshot = payload.page.snapshot;
    for (const node of payload.nodes) {
      if (node && typeof node.node_id === 'string') {
        nodes.set(node.node_id, { ...node, public_key: node.node_id });
      }
    }
    if (payload.page.complete) return [...nodes.values()];
    if (!payload.page.nextCursor || seenCursors.has(payload.page.nextCursor)) {
      throw new Error('Repeater index did not provide a forward cursor');
    }
    seenCursors.add(payload.page.nextCursor);
    cursor = payload.page.nextCursor;
  }
  throw new Error('Repeater index exceeded its page budget');
}

function predictNextAdvert<T extends { time: string; packet_hash: string }>(packets: T[]): { nextAdvert: Date; avgInterval: number; samples: number } | null {
  if (packets.length < 2) return null;

  const sorted = [...packets].sort((a, b) => new Date(a.time).getTime() - new Date(b.time).getTime());

  // First deduplicate by packet_hash (same advert received by different nodes)
  const byHash = new Map<string, T>();
  for (const pkt of sorted) {
    if (!byHash.has(pkt.packet_hash)) {
      byHash.set(pkt.packet_hash, pkt);
    }
  }
  let unique = Array.from(byHash.values()).sort((a, b) =>
    new Date(a.time).getTime() - new Date(b.time).getTime()
  );

  // Filter out packets that are within 30 seconds of each other
  // (these are duplicates from different observers receiving the same packet)
  const MIN_INTERVAL = 30;
  const filtered: T[] = [];
  for (const pkt of unique) {
    if (filtered.length === 0) {
      filtered.push(pkt);
    } else {
      const lastTime = new Date(filtered[filtered.length - 1].time).getTime();
      const thisTime = new Date(pkt.time).getTime();
      if ((thisTime - lastTime) / 1000 >= MIN_INTERVAL) {
        filtered.push(pkt);
      }
    }
  }
  unique = filtered;

  if (unique.length < 2) return null;

  // Take last 10 unique packets
  const recent = unique.slice(-10);

  const intervals: number[] = [];
  for (let i = 1; i < recent.length; i++) {
    const prev = new Date(recent[i - 1].time).getTime();
    const curr = new Date(recent[i].time).getTime();
    const interval = (curr - prev) / 1000; // convert to seconds
    if (interval > 0 && interval < 259200) { // ignore invalid intervals (> 3 days)
      intervals.push(interval);
    }
  }

  if (intervals.length < 1) return null;

  // Filter out outliers - only use intervals within 50% of median
  // This captures the recent consistent pattern and excludes old outlier intervals
  const sortedIntervals = [...intervals].sort((a, b) => a - b);
  const medianIdx = Math.floor(sortedIntervals.length / 2);
  const median = sortedIntervals.length % 2 === 0
    ? (sortedIntervals[medianIdx - 1] + sortedIntervals[medianIdx]) / 2
    : sortedIntervals[medianIdx];

  const filteredIntervals = intervals.filter(i =>
    i >= median * 0.5 && i <= median * 1.5
  );

  // Use filtered intervals if we have enough, otherwise fall back to all intervals
  const intervalsToUse = filteredIntervals.length >= 2 ? filteredIntervals : intervals;

  const avgInterval = intervalsToUse.reduce((a, b) => a + b, 0) / intervalsToUse.length;
  const lastPacketTime = new Date(recent[recent.length - 1].time).getTime();
  const nextAdvert = new Date(lastPacketTime + avgInterval * 1000);

  return { nextAdvert, avgInterval, samples: intervals.length };
}

function formatInterval(seconds: number): string {
  if (seconds < 60) return `${Math.round(seconds)}s`;
  if (seconds < 3600) return `${Math.round(seconds / 60)}m`;
  return `${Math.round(seconds / 3600)}h`;
}

function formatTimeUntil(date: Date): string {
  const secs = Math.floor((date.getTime() - Date.now()) / 1000);
  if (secs <= 0) return 'now';
  if (secs < 60) return `${secs}s`;
  if (secs < 3600) return `${Math.floor(secs / 60)}m`;
  return `${Math.floor(secs / 3600)}h ${Math.floor((secs % 3600) / 60)}m`;
}

export const UKRepeaterSearchPage: React.FC = () => {
  const site = getCurrentSite();
  const runtimeFeatures = useRuntimeFeatures();
  const network = site.networkFilter ?? site.network;
  const observer = site.observerId;
  const publicScope = { network, observer, privacyGeneration: runtimeFeatures.privacyGeneration };
  const detailScopeKey = scopeKey(publicScope);
  const [params, setParams] = useSearchParams();
  const navigation = useNavigationType();
  const urlQuery = (params.get('q') ?? '').slice(0, 160);
  const [searchQuery, setSearchQuery] = useState(urlQuery);
  const requested = params.getAll('node');
  const nodeKey = requested.length === 1 && /^[a-f0-9]{64}$/i.test(requested[0] ?? '') ? requested[0] : undefined;
  const [searchAttempt, setSearchAttempt] = useState(0);
  const lookup = useNodeLookup(searchQuery, publicScope, searchAttempt);
  const [showResults, setShowResults] = useState(false);
  const [indexAttempt, setIndexAttempt] = useState(0);
  const [index, setIndex] = useState<{ key: string; attempt: number; nodes: MeshNode[] | null }>();
  const currentIndex = index?.key === detailScopeKey && index.attempt === indexAttempt ? index.nodes : undefined;
  const [top, setTop] = useState<{ key: string; rows: TopAdvertNode[] }>();
  const topAdverts = top?.key === detailScopeKey ? top.rows : [];
  const [copiedKey, setCopiedKey] = useState<string>();
  const [copyFailed, setCopyFailed] = useState<string>();
  const detailHeading = useRef<HTMLHeadingElement>(null);
  const pendingFocus = useRef<string | undefined>(undefined);
  const locationRequest = useRef(0);
  const [nearby, setNearby] = useState<{ key: string; status: string; coords?: { latitude: number; longitude: number } }>();
  const currentNearby = nearby?.key === detailScopeKey ? nearby : undefined;

  useEffect(() => {
    if (navigation !== 'REPLACE') setSearchQuery(urlQuery);
    if (navigation === 'POP') setShowResults(Boolean(urlQuery) && !nodeKey);
  }, [navigation, urlQuery, nodeKey]);

  useEffect(() => {
    if (runtimeFeatures.privacyGeneration < 1) return;
    const controller = new AbortController();
    loadCompleteNodeSnapshot(controller.signal, network, observer).then(nodes => {
      if (!controller.signal.aborted) setIndex({ key: detailScopeKey, attempt: indexAttempt, nodes });
    }).catch(() => {
      if (!controller.signal.aborted) setIndex({ key: detailScopeKey, attempt: indexAttempt, nodes: null });
    });
    return () => controller.abort();
  }, [detailScopeKey, network, observer, runtimeFeatures.privacyGeneration, indexAttempt]);

  useEffect(() => {
    if (runtimeFeatures.privacyGeneration < 1) return;
    const controller = new AbortController();
    void fetchJson<TopAdvertNode[]>(
      withScopeParams('/api/nodes/top-adverts?hours=24&limit=10', { network, observer }),
      { signal: controller.signal, cache: 'default' },
      { timeoutMs: 10_000, maxBytes: 2 * 1024 * 1024, validate: Array.isArray },
    ).then(rows => {
      if (!controller.signal.aborted) setTop({ key: detailScopeKey, rows });
    }).catch(() => {});
    return () => controller.abort();
  }, [detailScopeKey, network, observer, runtimeFeatures.privacyGeneration]);

  useEffect(() => () => { locationRequest.current += 1; }, [detailScopeKey]);

  const matchesNode = (node: MeshNode) => node.node_id.toLowerCase() === nodeKey?.toLowerCase() && isPublicRepeater(node);
  const visibleNode = nodeKey ? lookup.nodes.find(matchesNode) ?? currentIndex?.find(matchesNode) : undefined;
  const directLookup = useNodeLookup(visibleNode ? '' : nodeKey ?? '', publicScope, indexAttempt);
  const selectedNode = visibleNode ?? directLookup.nodes.find(matchesNode);
  const selectedId = selectedNode?.node_id;
  useEffect(() => {
    if (!selectedId || selectedId !== pendingFocus.current) return;
    detailHeading.current?.focus();
    pendingFocus.current = undefined;
  }, [selectedId]);
  const nodePath = selectedNode && '/api/nodes/' + encodeURIComponent(selectedNode.node_id);
  const requestScope = { network, observer };
  const neighbours = useRepeaterDetail(nodePath && withScopeParams(nodePath + '/links', requestScope), detailScopeKey, neighbourCache, 2);
  const packetHistory = useRepeaterDetail(nodePath && withScopeParams(nodePath + '/history?hours=24', requestScope), detailScopeKey, historyCache, 4);
  const advertHistory = useRepeaterDetail(nodePath && withScopeParams(nodePath + '/adverts?hours=168', requestScope), detailScopeKey, advertCache, 2);
  const latestStatus = useRepeaterDetail(selectedId && withScopeParams('/api/node-status/latest', { network, observer: selectedId }),
    detailScopeKey, radioCache, 0.25);
  const radio = formatRadio(latestStatus.rows?.[0]?.radio);
  const links = neighbours.rows ?? [];
  const history = packetHistory.rows ?? [];
  const adverts = advertHistory.rows;
  const near = useMemo(() => currentNearby?.coords && currentIndex
    ? nearestRepeaters(currentIndex, currentNearby.coords, 10) : undefined, [currentNearby, currentIndex]);

  const searchResults = useMemo(() => {
    const q = searchQuery.trim().toLowerCase();
    if (!q) return [];
    const matches = new Map(lookup.nodes.filter(isPublicRepeater).map(node => [node.node_id, node]));
    for (const node of currentIndex ?? []) {
      if (matches.size >= 10) break;
      if (!matches.has(node.node_id) && isPublicRepeater(node) && (node.name?.toLowerCase().includes(q)
        || node.public_key?.toLowerCase().includes(q) || node.iata?.toLowerCase().includes(q))) {
        matches.set(node.node_id, node);
      }
    }
    return [...matches.values()];
  }, [searchQuery, currentIndex, lookup.nodes]);
  const searchOptions = useMemo<ComboboxOption[]>(() => searchResults.map((node) => ({
    id: node.node_id,
    label: node.name ?? node.node_id,
    content: (
      <>
        <span className="repeater-search-box__result-name">{node.name || 'Unknown'}</span>
        <span className="repeater-search-box__result-meta">
          {node.iata ? `${node.iata} · ` : ''}
          {node.public_key?.slice(0, 16)}... · {node.is_online ? 'Online' : 'Offline'}
        </span>
      </>
    ),
  })), [searchResults]);

  const prediction = useMemo(() => adverts?.length ? predictNextAdvert(adverts) : null, [adverts]);

  const selectNode = (node: string) => {
    if (node === selectedId) detailHeading.current?.focus();
    else pendingFocus.current = node;
    const next = new URLSearchParams(params);
    next.set('node', node);
    if (next.toString() !== params.toString()) setParams(next);
    setShowResults(false);
    setNearby(undefined);
  };

  const changeQuery = (query: string) => {
    const nextQuery = query.slice(0, 160);
    setSearchQuery(nextQuery);
    const next = new URLSearchParams(params);
    if (nextQuery) next.set('q', nextQuery);
    else next.delete('q');
    setParams(next, { replace: true });
    setNearby(undefined);
    locationRequest.current += 1;
    setShowResults(Boolean(nextQuery.trim()));
  };

  const findNearby = () => {
    setShowResults(false);
    const request = ++locationRequest.current;
    if (!navigator.geolocation) {
      setNearby({ key: detailScopeKey, status: 'Location is unavailable. Search by name or observer code.' });
      return;
    }
    setNearby({ key: detailScopeKey, status: 'Waiting for location permission…' });
    navigator.geolocation.getCurrentPosition(({ coords }) => {
      if (locationRequest.current !== request) return;
      setNearby({ key: detailScopeKey, status: 'Finding nearby repeaters…',
        coords: { latitude: coords.latitude, longitude: coords.longitude } });
    }, error => {
      if (locationRequest.current !== request) return;
      setNearby({ key: detailScopeKey, status: error.code === error.PERMISSION_DENIED
        ? 'Location access was denied. Search by name or observer code.'
        : 'Could not get your location. Search by name or observer code.' });
    }, { enableHighAccuracy: false, timeout: 10_000, maximumAge: 300_000 });
  };

  const copyPublicKey = async () => {
    if (!selectedNode?.public_key) return;
    const key = JSON.stringify([detailScopeKey, selectedNode.node_id]);
    try {
      await navigator.clipboard.writeText(selectedNode.public_key);
      setCopyFailed(undefined);
      setCopiedKey(key);
    } catch {
      setCopiedKey(undefined);
      setCopyFailed(key);
    }
  };
  const copyIdentity = selectedNode && JSON.stringify([detailScopeKey, selectedNode.node_id]);
  const canLoad = runtimeFeatures.privacyGeneration > 0;
  const loadingNodes = canLoad && currentIndex === undefined;
  const nodesError = !canLoad || currentIndex === null;
  const retryIndex = () => {
    if (!canLoad) window.location.reload();
    else setIndexAttempt(value => value + 1);
  };
  const resolvingNode = Boolean(nodeKey) && (directLookup.status === 'loading' || loadingNodes);

  return (
    <>
      <section className="site-section repeater-page">
        <div className="site-content">
          <h1>Repeater search</h1>
          <div className="repeater-search-tools">
          <Combobox
              label="Search repeaters"
              value={searchQuery}
              onValueChange={changeQuery}
              onSelectionChange={selectNode}
              options={searchOptions}
              placeholder="Name, observer code or key prefix"
              className="repeater-search-box"
              inputClassName="repeater-search-box__input"
              popoverClassName="repeater-search-box__results"
              optionClassName="repeater-search-box__result"
              isOpen={showResults}
              onOpenChange={setShowResults}
              emptyContent={lookup.status === 'loading' || loadingNodes ? (
                  <div className="repeater-search-box__no-results">
                    <LoadingIndicator label="Loading repeaters..." variant="inline" />
                  </div>
                ) : nodesError || lookup.status === 'error' ? (
                  <div className="repeater-search-box__no-results" role="alert">
                    Could not complete repeater search.
                  </div>
                ) : searchQuery && searchResults.length === 0 ? (
                  <div className="repeater-search-box__no-results">
                    No repeaters found matching "{searchQuery}"
                  </div>
                ) : 'Type to search repeaters'}
              footer={lookup.status === 'error' ? (
                  <button className="site-btn site-btn--ghost" type="button" onClick={() => {
                    if (!canLoad) window.location.reload();
                    else setSearchAttempt(value => value + 1);
                  }}>Retry search</button>
                ) : searchResults.length > 0 ? (
                  <div className="repeater-search-box__count">
                    {searchResults.length} result{searchResults.length !== 1 ? 's' : ''}
                  </div>
                ) : null}
          />
            <button type="button" className="site-btn site-btn--ghost" onClick={findNearby}>Find nearby</button>
          </div>
          {currentNearby && <p className="repeater-search-feedback" role="status">{near
            ? near.length ? 'Nearest repeaters with public positions.' : 'No repeaters with public positions are available.'
            : currentNearby.coords && nodesError ? 'Could not load nearby repeaters. Search by name or observer code.' : currentNearby.status}</p>}
          {near && near.length > 0 && <ol className="repeater-top-list__items repeater-nearby" aria-label="Nearby repeaters">
            {near.map(node => <li key={node.node_id}>
              <button type="button" className="repeater-top-list__item" onClick={() => selectNode(node.node_id)}>
                <span className="repeater-top-list__name">{node.name || node.node_id.slice(0, 16)}</span>
                {node.iata && <span className="repeater-top-list__adverts">{node.iata}</span>}
              </button>
            </li>)}
          </ol>}

          {!selectedNode ? (
            <div className="repeater-details-card">
              {requested.length && !resolvingNode ? (
                <div className="repeater-details-card__empty" role="status">
                  <h2>{directLookup.status === 'error' ? 'Could not open this repeater' : 'Repeater unavailable'}</h2>
                  <p>{directLookup.status === 'error' ? 'Try loading the repeater again, or look it up by name.'
                    : 'This link does not match a public repeater in the current network. Search for another repeater above.'}</p>
                  {directLookup.status === 'error' && <button type="button" className="site-btn site-btn--ghost"
                    onClick={retryIndex}>Retry repeater</button>}
                </div>
              ) : loadingNodes || resolvingNode ? (
                <LoadingIndicator label="Loading repeater index..." variant="block" />
              ) : nodesError ? (
                <div className="repeater-details-card__empty" role="alert">
                  <h3>Repeater index unavailable</h3>
                  <p>Search by name or try loading the full repeater index again.</p>
                  <button type="button" className="site-btn site-btn--ghost" onClick={retryIndex}>Retry repeaters</button>
                </div>
              ) : (
                <div className="repeater-details-card__empty">
                  <svg className="repeater-details-card__empty-icon" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.5">
                    <circle cx="11" cy="11" r="8" />
                    <path d="m21 21-4.35-4.35" />
                  </svg>
                  <h3>Select a Repeater</h3>
                  <p>Search for a repeater above, or pick one from the most active below.</p>
                  {topAdverts.length > 0 ? (
                    <div className="repeater-top-list">
                      <p className="repeater-top-list__heading">Most adverts in the last 24 hours</p>
                      <ol className="repeater-top-list__items">
                        {topAdverts.map((entry, index) => (
                          <li key={entry.node_id}>
                            <button
                              type="button"
                              className="repeater-top-list__item"
                              onClick={() => selectNode(entry.node_id)}
                            >
                              <span className="repeater-top-list__rank">{index + 1}</span>
                              <span className="repeater-top-list__name">
                                {entry.name || entry.node_id.slice(0, 16)}
                              </span>
                              <span className="repeater-top-list__adverts">
                                {entry.adverts_in_window.toLocaleString()} advert{entry.adverts_in_window !== 1 ? 's' : ''}
                                <span className={`repeater-top-list__status ${entry.is_online ? 'repeater-top-list__status--online' : 'repeater-top-list__status--offline'}`}>
                                  {entry.is_online ? 'Online' : 'Offline'}
                                </span>
                              </span>
                            </button>
                          </li>
                        ))}
                      </ol>
                    </div>
                  ) : null}
                </div>
              )}
            </div>
          ) : (
            <div className="repeater-details-card">
              <div className="repeater-details-card__header">
                <div className="repeater-details-card__identity">
                  <h2 ref={detailHeading} tabIndex={-1}>{selectedNode.name || 'Unnamed repeater'}</h2>
                  <span>Last heard {timeAgo(selectedNode.last_seen, { empty: 'unknown' })}</span>
                </div>
                <span className={`repeater-details-card__status ${selectedNode.is_online ? 'repeater-details-card__status--online' : 'repeater-details-card__status--offline'}`}>
                  {selectedNode.is_online ? 'Online' : 'Offline'}
                </span>
                <a className="site-btn site-btn--primary" href={liveMapLink(site.mapHomeUrl, { node: {
                  id: selectedNode.node_id, lat: selectedNode.lat, lon: selectedNode.lon,
                } })}>View on Live Map</a>
                <Link className="site-btn site-btn--ghost" to={`/health-check?${new URLSearchParams({ node: selectedNode.node_id })}`}>
                  Health Check
                </Link>
              </div>

              <div className="repeater-details-card__section">
                <h3>
                  Details
                </h3>
                <div className="repeater-details-card__grid">
                  <div className="repeater-details-card__field">
                    <span className="repeater-details-card__label">Public Key</span>
                    <span className="repeater-details-card__value repeater-details-card__value--mono">
                      {selectedNode.public_key
                        ? `${selectedNode.public_key.slice(0, 16)}…${selectedNode.public_key.slice(-8)}`
                        : 'N/A'}
                    </span>
                    {selectedNode.public_key && (
                      <button
                        type="button"
                        className="repeater-details-card__copy-btn"
                        onClick={copyPublicKey}
                      >
                        {copiedKey === copyIdentity ? 'Copied' : 'Copy public key'}
                      </button>
                    )}
                    {copyFailed === copyIdentity && <span role="status">Could not copy the public key.</span>}
                  </div>
                  {[
                    ['Position', selectedNode.lat != null && selectedNode.lon != null
                      ? `${selectedNode.lat.toFixed(5)}, ${selectedNode.lon.toFixed(5)}` : 'Unknown'],
                    ['Elevation', selectedNode.elevation_m != null ? `${Math.round(selectedNode.elevation_m)} m` : 'N/A'],
                    ['Network', selectedNode.iata || 'N/A'], ['Hardware', selectedNode.hardware_model || 'Unknown'],
                    ...(radio ? [['Radio', radio]] : []),
                    ['Advert count', selectedNode.advert_count?.toLocaleString() || '0'],
                  ].map(([label, value]) => <div className="repeater-details-card__field" key={label}>
                    <span className="repeater-details-card__label">{label}</span>
                    <span className="repeater-details-card__value">{value}</span>
                  </div>)}
                  {prediction && (
                    <div className="repeater-details-card__field">
                      <span className="repeater-details-card__label">Predicted Next Advert</span>
                      <span className="repeater-details-card__value">
                        {prediction.samples >= 3 ? formatTimeUntil(prediction.nextAdvert) : 'Collecting data…'}
                      </span>
                      <span className="repeater-details-card__meta">
                        ~{formatInterval(prediction.avgInterval)} interval ({prediction.samples} samples{prediction.samples < 3 ? ' — need 3+' : ''})
                      </span>
                    </div>
                  )}
                  {adverts === null && <div role="status">
                    <p>Could not load advert history.</p>
                    <button type="button" className="site-btn site-btn--ghost" onClick={advertHistory.retry}>Retry adverts</button>
                  </div>}
                </div>
              </div>

              <>
                  <div className="repeater-details-card__section">
                    <h3>
                      Confirmed Neighbours {links.length > 0 && <span className="repeater-details-card__count-badge">{links.length}</span>}
                    </h3>
                    {neighbours.rows === undefined ? <LoadingIndicator label="Loading neighbours…" variant="inline" />
                      : neighbours.rows === null ? <div role="status"><p>Could not load neighbours.</p>
                        <button type="button" className="site-btn site-btn--ghost" onClick={neighbours.retry}>Retry neighbours</button>
                      </div> : <ConfirmedNeighbours links={links} />}
                  </div>

                  <div className="repeater-details-card__section">
                    <h3>
                      Recent Packets {history.length > 0 && <span className="repeater-details-card__count-badge">{history.length}</span>}
                    </h3>
                    {packetHistory.rows === undefined ? <LoadingIndicator label="Loading packet history…" variant="inline" />
                      : packetHistory.rows === null ? <div role="status"><p>Could not load packet history.</p>
                        <button type="button" className="site-btn site-btn--ghost" onClick={packetHistory.retry}>Retry packet history</button>
                      </div> : history.length === 0 ? (
                      <p className="repeater-details-card__empty-msg">No packet history available for this node.</p>
                    ) : (
                      <>
                        <p className="repeater-details-card__desc">Last 24 hours of packet activity</p>
                        <div className="repeater-details-card__table-wrap">
                          <table className="repeater-details-card__table">
                            <thead>
                              <tr>
                                <th scope="col">Time</th>
                                <th scope="col">Hops</th>
                                <th scope="col">RSSI</th>
                                <th scope="col">SNR</th>
                                <th scope="col">From</th>
                              </tr>
                            </thead>
                            <tbody>
                              {history.slice(0, 50).map((pkt, idx) => (
                                <tr key={idx}>
                                  <td>{timeAgo(pkt.time)}</td>
                                  <td>{pkt.hop_count ?? '-'}</td>
                                  <td>{pkt.rssi ?? '-'}</td>
                                  <td>{pkt.snr ?? '-'}</td>
                                  <td>{pkt.src_node_id?.slice(0, 12) || '-'}...</td>
                                </tr>
                              ))}
                            </tbody>
                          </table>
                        </div>
                      </>
                    )}
                  </div>
              </>
            </div>
          )}
        </div>
      </section>
    </>
  );
};
