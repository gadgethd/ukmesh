import React, { useMemo, useCallback, useEffect, useState, useRef } from 'react';
import './feed-page.css';
import { getCurrentSite } from '../../config/site.js';
import { useWebSocket } from '../../hooks/useWebSocket.js';
import {
  nodeStore,
  useMessages,
  useNodes,
} from '../../hooks/useNodes.js';
import { useAppMessageHandler } from '../../hooks/useAppMessageHandler.js';
import {
  ApiResponseError,
  chartStatsEndpoint,
  fetchJson,
  uncachedEndpoint,
  withScopeParams,
} from '../../utils/api.js';
import { useRuntimeFeatures } from '../../config/runtimeFeatures.js';
import { useVisibilityPoll } from '../../hooks/useVisibilityPoll.js';
import type { LazyPathResult } from './PacketDetailPanel.js';
import { FeedMapPanel } from './FeedPathViews.js';
import { FeedDialogs } from './FeedDialogs.js';
import {
  FEED_PATH_MAX_CONCURRENCY,
  LAZY_SETTLE_MS,
  MAX_PACKETS,
  PATH_REQUEST_TIMEOUT_MS,
  TYPE_LABELS,
  aggregatedPacketToFeedPacket,
  feedPathCache,
  feedPathCacheKey,
  mergeFeedPacketObservations,
  packetMatchesMessageScope,
  packetObserverIatas,
  packetObserverIds,
  packetSummary,
  timeAgo,
  type FeedPacket,
  type MessageScope,
} from './feedModel.js';
import {
  feedConnectionDot,
  feedConnectionLabel,
  feedConnectionStatus,
  initialPathTreeStatus,
  validatedRegionSelection,
  type PathTreeStatus,
} from './feedState.js';

export type { FeedPacket } from './feedModel.js';
export { FEED_PATH_MAX_CONCURRENCY } from './feedModel.js';
export const UKFeedPage: React.FC = () => {
  const site = getCurrentSite();
  const { privacyGeneration } = useRuntimeFeatures();
  const scope = useMemo(() => ({ network: site.networkFilter, observer: site.observerId }), [site.networkFilter, site.observerId]);
  const scopeKey = `${scope.network ?? 'all'}|${scope.observer ?? 'all'}|privacy-${privacyGeneration}`;
  const scopeEpochRef = useRef<{ key: string; epoch: number } | null>(null);
  if (scopeEpochRef.current?.key !== scopeKey) {
    scopeEpochRef.current = { key: scopeKey, epoch: nodeStore.reset(scopeKey) };
  }
  const scopeEpoch = scopeEpochRef.current.epoch;
  const [selectedIata, setSelectedIata] = useState<string>(() => localStorage.getItem('uk-feed-iata') ?? 'all');
  const [selectedMessageScope, setSelectedMessageScope] = useState<MessageScope>(() => {
    const stored = localStorage.getItem('uk-feed-message-scope');
    return stored === 'public' || stored === 'test' ? stored : 'all';
  });
  const [messagesOnly, setMessagesOnly] = useState<boolean>(() => localStorage.getItem('uk-feed-messages-only') === '1');
  const [regionOptions, setRegionOptions] = useState<string[]>([]);
  const [regionOptionsStatus, setRegionOptionsStatus] = useState<'loading' | 'ready' | 'failed'>('loading');
  const [selectedPacketHash, setSelectedPacketHash] = useState<string | null>(null);
  const selectedPacketHashRef = useRef<string | null>(selectedPacketHash);
  const selectedPacketRef = useRef<FeedPacket | null>(null);
  const [pathTreeOpen, setPathTreeOpen] = useState(false);
  const [pathTreeStatus, setPathTreeStatus] = useState<PathTreeStatus>('idle');
  const [searchQuery, setSearchQuery] = useState<string>('');
  const [selectedPacketType, setSelectedPacketType] = useState<string | null>(() => {
    const requested = new URLSearchParams(window.location.search).get('type')?.trim().toLowerCase();
    return requested && (/^(?:[0-9]{1,3}|unknown)$/.test(requested)) ? requested : null;
  });
  const [now, setNow] = useState(() => Date.now());
  const [viewportHeight, setViewportHeight] = useState(() => window.innerHeight);
  const [listScrollTop, setListScrollTop] = useState(0);
  const [detailOpen, setDetailOpen] = useState(false);
  const packetListRef = useRef<HTMLDivElement>(null);

  const {
    nodes: nodeMap,
    packets: packetsList,
  } = useNodes();
  const messagesList = useMessages();

  const handleWSMessage = useAppMessageHandler({
    epoch: scopeEpoch,
    handleInitialState: (data) => nodeStore.handleInitialState(data, scopeEpoch),
    handlePacket: (data) => nodeStore.handlePacket(data, scopeEpoch),
    handleNodeUpdate: (data) => nodeStore.handleNodeUpdate(data, scopeEpoch),
    handleNodeUpdateBatch: (data) => nodeStore.handleNodeUpdateBatch(data, scopeEpoch),
    handleNodeUpsert: (data) => nodeStore.handleNodeUpsert(data, scopeEpoch),
    handleNodeUpsertBatch: (data) => nodeStore.handleNodeUpsertBatch(data, scopeEpoch),
    applyInitialViablePairs: () => {},
    applyInitialViableLinks: () => {},
    applyLinkUpdate: () => {},
    applyLinkUpdateBatch: () => {},
  });

  const wsConnection = useWebSocket(handleWSMessage, scope, scopeEpoch);

  // Persist filters
  useEffect(() => { localStorage.setItem('uk-feed-iata', selectedIata); }, [selectedIata]);
  useEffect(() => { localStorage.setItem('uk-feed-message-scope', selectedMessageScope); }, [selectedMessageScope]);
  useEffect(() => { localStorage.setItem('uk-feed-messages-only', messagesOnly ? '1' : '0'); }, [messagesOnly]);

  // Clock tick for live connection indicator and stats
  useEffect(() => {
    const timer = window.setInterval(() => setNow(Date.now()), 5000);
    return () => window.clearInterval(timer);
  }, []);

  useEffect(() => {
    const handleResize = () => setViewportHeight(window.innerHeight);
    window.addEventListener('resize', handleResize);
    return () => window.removeEventListener('resize', handleResize);
  }, []);

  // Selected-packet path cache. Requests are demand-driven and scope-fenced.
  const [, setLazyCacheVersion] = useState(0);
  const settleTimerRef = useRef<ReturnType<typeof setTimeout> | null>(null);
  const pathRequestControllersRef = useRef<Map<string, AbortController>>(new Map());
  const fetchSelectedLazyPathRef = useRef<(packet: FeedPacket, skipSettle?: boolean) => Promise<void>>(
    async () => {},
  );

  useEffect(() => {
    setRegionOptionsStatus('loading');
    setRegionOptions([]);
  }, [scopeKey]);

  useVisibilityPoll(async (signal) => {
    const json = await fetchJson<{
      observerRegions?: Array<{ iata?: string | null; activeObservers?: number; observers?: number }>;
    }>(
      uncachedEndpoint(chartStatsEndpoint(scope)),
      { cache: 'no-store', signal },
      { timeoutMs: PATH_REQUEST_TIMEOUT_MS, maxBytes: 4 * 1024 * 1024 },
    );
    if (signal.aborted) return;
    const values = (json.observerRegions ?? [])
      .map((region) => String(region.iata ?? '').trim().toUpperCase())
      .filter((iata) => /^[A-Z0-9]{2,8}$/.test(iata));
    setRegionOptions(Array.from(new Set(values)).sort((a, b) => a.localeCompare(b)));
    setRegionOptionsStatus('ready');
  }, {
    scopeKey: `feed-regions:${scopeKey}`,
    intervalMs: 60_000,
    timeoutMs: PATH_REQUEST_TIMEOUT_MS,
    onError: () => setRegionOptionsStatus('failed'),
  });

  // Convert live packets to FeedPacket format for display
  const packets: FeedPacket[] = useMemo(() => {
    return packetsList.slice(0, MAX_PACKETS).map(aggregatedPacketToFeedPacket);
  }, [packetsList]);

  const messagePackets: FeedPacket[] = useMemo(() => {
    return messagesList.slice(0, MAX_PACKETS).map(aggregatedPacketToFeedPacket);
  }, [messagesList]);

  const retainedMessagePackets = useMemo(() => {
    const byHash = new Map<string, FeedPacket>();
    for (const packet of [...messagePackets, ...packets]) {
      if (packet.packet_type !== 2 && packet.packet_type !== 5) continue;
      const existing = byHash.get(packet.packet_hash);
      if (!existing) {
        byHash.set(packet.packet_hash, packet);
        continue;
      }
      byHash.set(packet.packet_hash, mergeFeedPacketObservations(existing, packet));
    }
    return Array.from(byHash.values()).sort(
      (a, b) => Date.parse(b.first_seen_time ?? b.time) - Date.parse(a.first_seen_time ?? a.time),
    );
  }, [messagePackets, packets]);

  const availableIatas = useMemo(() => {
    const values = new Set(regionOptions);
    for (const packet of packets) {
      for (const iata of packetObserverIatas(packet, nodeMap)) values.add(iata);
    }
    if (regionOptionsStatus !== 'ready' && selectedIata !== 'all') values.add(selectedIata);
    return Array.from(values).sort((a, b) => a.localeCompare(b));
  }, [nodeMap, packets, regionOptions, regionOptionsStatus, selectedIata]);

  useEffect(() => {
    const validated = validatedRegionSelection(
      selectedIata,
      availableIatas,
      regionOptionsStatus,
    );
    if (validated !== selectedIata) setSelectedIata(validated);
  }, [availableIatas, regionOptionsStatus, selectedIata]);

  const filteredPackets = useMemo(() => {
    const messageViewActive = selectedMessageScope !== 'all' || messagesOnly;
    let result = messageViewActive ? retainedMessagePackets : packets;
    if (selectedMessageScope !== 'all') {
      result = result.filter((packet) => packetMatchesMessageScope(packet, selectedMessageScope));
    }
    if (selectedIata !== 'all') {
      result = result.filter((packet) => packetObserverIatas(packet, nodeMap).includes(selectedIata));
    }
    if (messagesOnly) {
      result = result.filter((packet) => packet.packet_type === 2 || packet.packet_type === 5);
    }
    if (selectedPacketType) {
      result = result.filter((packet) => String(packet.packet_type ?? 'unknown') === selectedPacketType);
    }
    if (searchQuery.trim()) {
      const q = searchQuery.trim().toLowerCase();
      result = result.filter(
        (p) =>
          p.packet_hash.toLowerCase().startsWith(q) ||
          packetSummary(p, nodeMap).toLowerCase().includes(q) ||
          (p.src_node_id?.toLowerCase().startsWith(q) ?? false) ||
          (p.rx_node_id?.toLowerCase().startsWith(q) ?? false),
      );
    }
    return result;
  }, [messagesOnly, nodeMap, packets, retainedMessagePackets, searchQuery, selectedIata, selectedMessageScope, selectedPacketType]);

  const activeObserverCount = useMemo(() => {
    const ids = new Set<string>();
    for (const packet of filteredPackets) {
      const observerIds = packetObserverIds(packet);
      for (const observerId of observerIds) {
        if (!observerId) continue;
        if (selectedIata !== 'all') {
          const packetIatas = packetObserverIatas(packet, nodeMap);
          if (!packetIatas.includes(selectedIata)) continue;
        }
        ids.add(observerId);
      }
    }
    return ids.size;
  }, [filteredPackets, nodeMap, selectedIata]);

  const latestPacket = filteredPackets[0] ?? null;
  const globalLatestPacket = packets[0] ?? null; // unfiltered, for connection status
  const recentPackets = useMemo(() => {
    return filteredPackets.slice(0, MAX_PACKETS);
  }, [filteredPackets]);
  const virtualRows = useMemo(() => {
    const rowHeight = 76;
    const visibleHeight = Math.max(320, viewportHeight - 200);
    const start = Math.max(0, Math.floor(listScrollTop / rowHeight) - 5);
    const end = Math.min(recentPackets.length, start + Math.ceil(visibleHeight / rowHeight) + 10);
    return {
      rows: recentPackets.slice(start, end),
      start,
      top: start * rowHeight,
      bottom: Math.max(0, (recentPackets.length - end) * rowHeight),
    };
  }, [listScrollTop, recentPackets, viewportHeight]);

  // Always derive selectedPacket from the live list so new MQTT observers are picked up
  const selectedPacket = useMemo(
    () => selectedPacketHash
      ? (packets.find((p) => p.packet_hash === selectedPacketHash)
        ?? retainedMessagePackets.find((p) => p.packet_hash === selectedPacketHash)
        ?? null)
      : null,
    [selectedPacketHash, packets, retainedMessagePackets],
  );
  const selectedPathCacheKey = selectedPacket
    ? feedPathCacheKey(selectedPacket)
    : null;
  const selectedLazyPath = selectedPathCacheKey
    ? (feedPathCache.get(scopeKey, selectedPathCacheKey) ?? null)
    : null;
  const selectedPathRequestKey = selectedPathCacheKey
    ? `${scopeKey}:${selectedPathCacheKey}`
    : null;
  selectedPacketRef.current = selectedPacket;

  useEffect(() => {
    selectedPacketHashRef.current = selectedPacketHash;
    setPathTreeOpen(false);
    if (settleTimerRef.current !== null) {
      clearTimeout(settleTimerRef.current);
      settleTimerRef.current = null;
    }
    for (const [key, controller] of pathRequestControllersRef.current) {
      if (key !== selectedPathRequestKey) {
        controller.abort();
        pathRequestControllersRef.current.delete(key);
      }
    }
    setPathTreeStatus('idle');
  }, [selectedPacketHash, selectedPathRequestKey]);

  useEffect(() => {
    if (!selectedPacket) {
      setPathTreeStatus('idle');
    } else {
      const next = initialPathTreeStatus(
        Boolean(selectedPacket.path_hashes?.length),
        selectedLazyPath !== null,
      );
      if (next !== 'idle') setPathTreeStatus(next);
    }
  }, [selectedLazyPath, selectedPacket]);

  useEffect(() => {
    feedPathCache.invalidateScope(scopeKey);
    setLazyCacheVersion((version) => version + 1);
    return () => {
      if (settleTimerRef.current !== null) clearTimeout(settleTimerRef.current);
      for (const controller of pathRequestControllersRef.current.values()) controller.abort();
      pathRequestControllersRef.current.clear();
      feedPathCache.invalidateScope(scopeKey);
    };
  }, [scopeKey]);

  const fetchSelectedLazyPath = useCallback(async (
    packet: FeedPacket,
    skipSettle = false,
  ) => {
    const hash = packet.packet_hash;
    const pathKey = feedPathCacheKey(packet);
    const requestKey = `${scopeKey}:${pathKey}`;
    const setStatusForSelected = (status: PathTreeStatus) => {
      if (selectedPacketHashRef.current === hash) setPathTreeStatus(status);
    };

    if (!packet.path_hashes?.length) {
      setStatusForSelected('unavailable');
      return;
    }

    if (feedPathCache.peek(scopeKey, pathKey)) {
      setStatusForSelected('ready');
      return;
    }

    if (!skipSettle) {
      const packetAgeMs = Math.max(0, Date.now() - Date.parse(packet.time));
      const remainingMs = Math.max(0, LAZY_SETTLE_MS - packetAgeMs);
      if (remainingMs > 0) {
        setStatusForSelected('settling');
        if (settleTimerRef.current !== null) clearTimeout(settleTimerRef.current);
        settleTimerRef.current = setTimeout(() => {
          settleTimerRef.current = null;
          const current = selectedPacketRef.current;
          if (current?.packet_hash === hash) {
            void fetchSelectedLazyPathRef.current(current, true);
          }
        }, remainingMs);
        return;
      }
    }

    if (pathRequestControllersRef.current.has(requestKey)) {
      setStatusForSelected('loading');
      return;
    }

    while (pathRequestControllersRef.current.size >= FEED_PATH_MAX_CONCURRENCY) {
      const oldest = pathRequestControllersRef.current.entries().next().value as
        | [string, AbortController]
        | undefined;
      if (!oldest) break;
      oldest[1].abort();
      pathRequestControllersRef.current.delete(oldest[0]);
    }
    const controller = new AbortController();
    pathRequestControllersRef.current.set(requestKey, controller);
    setStatusForSelected('loading');

    try {
      const result = await fetchJson<LazyPathResult>(
        withScopeParams(`/api/path-lazy/resolve?hash=${encodeURIComponent(hash)}`, scope),
        { cache: 'no-store', signal: controller.signal },
        { timeoutMs: PATH_REQUEST_TIMEOUT_MS, maxBytes: 4 * 1024 * 1024 },
      );
      if (controller.signal.aborted || scopeEpochRef.current?.epoch !== scopeEpoch) return;
      feedPathCache.set(scopeKey, pathKey, result);
      setLazyCacheVersion((version) => version + 1);
      setStatusForSelected('ready');
    } catch (error) {
      if (error instanceof ApiResponseError && error.status === 404) {
        setStatusForSelected('unavailable');
        return;
      }
      if (!controller.signal.aborted && (error as DOMException).name !== 'AbortError') {
        setStatusForSelected('error');
      }
    } finally {
      if (pathRequestControllersRef.current.get(requestKey) === controller) {
        pathRequestControllersRef.current.delete(requestKey);
      }
    }
  }, [scope, scopeEpoch, scopeKey]);
  fetchSelectedLazyPathRef.current = fetchSelectedLazyPath;

  const openPathTree = useCallback(() => {
    if (!selectedPacket) return;
    selectedPacketHashRef.current = selectedPacket.packet_hash;
    setPathTreeOpen(true);
    void fetchSelectedLazyPath(selectedPacket);
  }, [fetchSelectedLazyPath, selectedPacket]);

  const connStatus = feedConnectionStatus(
    wsConnection.readyState,
    wsConnection.lastMessageAt,
    globalLatestPacket !== null,
    now,
  );
  const connDot = feedConnectionDot(connStatus);
  const connLabel = feedConnectionLabel(connStatus);

  // Network activity stats (rolling window over cached packets)
  const networkStats = useMemo(() => {
    const cutoff1min = now - 60_000;
    const cutoff5min = now - 300_000;
    let packetsLastMin = 0;
    const activeSenders = new Set<string>();
    const typeCounts: Record<number, number> = {};
    for (const p of packets) {
      const ts = Date.parse(p.time);
      if (ts >= cutoff1min) {
        packetsLastMin++;
        if (p.packet_type != null) typeCounts[p.packet_type] = (typeCounts[p.packet_type] ?? 0) + 1;
      }
      if (ts >= cutoff5min && p.src_node_id) activeSenders.add(p.src_node_id);
    }
    const topTypes = (Object.entries(typeCounts) as [string, number][])
      .sort(([, a], [, b]) => b - a)
      .slice(0, 5)
      .map(([type, count]) => ({ type: Number(type), label: TYPE_LABELS[Number(type)] ?? `T${type}`, count }));
    return { packetsLastMin, activeSenderCount: activeSenders.size, topTypes };
  }, [packets, now]);

  return (
    <>

      <div className={`uk-feed-layout${selectedPacketHash ? ' uk-feed-layout--has-selection' : ''}`}>

        {/* ── Channels sidebar ───────────────────────────────────────── */}
        <nav className="uk-feed-channels">
          <div className="uk-feed-channels__header">Channels</div>
          <button
            type="button"
            className={`uk-feed-channel-item${selectedMessageScope === 'all' ? ' uk-feed-channel-item--active' : ''}`}
            onClick={() => setSelectedMessageScope('all')}
          >
            All
          </button>
          <button
            type="button"
            className={`uk-feed-channel-item${selectedMessageScope === 'public' ? ' uk-feed-channel-item--active' : ''}`}
            onClick={() => setSelectedMessageScope('public')}
          >
            Public
          </button>
          <button
            type="button"
            className={`uk-feed-channel-item${selectedMessageScope === 'test' ? ' uk-feed-channel-item--active' : ''}`}
            onClick={() => setSelectedMessageScope('test')}
          >
            Test
          </button>

          <div className="uk-feed-channels__divider" />
          <div className="uk-feed-channels__header">Regions</div>
          <button
            type="button"
            className={`uk-feed-channel-item${selectedIata === 'all' ? ' uk-feed-channel-item--active' : ''}`}
            onClick={() => setSelectedIata('all')}
          >
            All
          </button>
          {availableIatas.map((iata) => (
            <button
              type="button"
              key={iata}
              className={`uk-feed-channel-item${selectedIata === iata ? ' uk-feed-channel-item--active' : ''}`}
              onClick={() => setSelectedIata(iata)}
            >
              {iata}
            </button>
          ))}

          <div className="uk-feed-channels__divider" />
          <label className="uk-feed-channel-toggle">
            <input
              type="checkbox"
              checked={messagesOnly}
              onChange={(e) => setMessagesOnly(e.target.checked)}
            />
            Msgs only
          </label>
        </nav>

        {/* ── Mobile stats bar (hidden on desktop) ──────────────────── */}
        <div className="uk-feed-mobile-stats">
          <div className="uk-feed-stats__row">
            <span className={`uk-feed-live-dot uk-feed-live-dot--${connDot}`} />
            <span className="uk-feed-stats__label">
              {connLabel}
            </span>
            <span className="uk-feed-stats__sep">·</span>
            <span className="uk-feed-stats__label">{activeObserverCount} observer{activeObserverCount !== 1 ? 's' : ''}</span>
            <span className="uk-feed-stats__sep">·</span>
            <span className="uk-feed-stats__label">{networkStats.packetsLastMin} pkt/min</span>
            <span className="uk-feed-stats__sep">·</span>
            <span className="uk-feed-stats__label">last: {timeAgo(globalLatestPacket?.time)}</span>
          </div>
          {networkStats.topTypes.length > 0 && (
            <div className="uk-feed-type-tags">
              {networkStats.topTypes.map(({ label, count, type }) => (
                <span key={type} className="uk-feed-type-tag">{label} <strong>{count}</strong></span>
              ))}
            </div>
          )}
        </div>

        {/* ── Chat (packet list) ─────────────────────────────────────── */}
        <div className="uk-feed-chat">
          <div className="uk-feed-chat__header">
            <input
              type="search"
              className="uk-feed-search"
              aria-label="Search packets"
              placeholder="Search packets…"
              value={searchQuery}
              onChange={(e) => setSearchQuery(e.target.value)}
            />
            {selectedPacketType && (
              <button
                type="button"
                className="uk-feed-channel-item uk-feed-channel-item--active"
                onClick={() => {
                  setSelectedPacketType(null);
                  const url = new URL(window.location.href);
                  url.searchParams.delete('type');
                  window.history.replaceState(null, '', url);
                }}
              >
                Type {TYPE_LABELS[Number(selectedPacketType)] ?? selectedPacketType} ×
              </button>
            )}
          </div>
          <div
            className="uk-feed-packets-list"
            ref={packetListRef}
            onScroll={(event) => setListScrollTop(event.currentTarget.scrollTop)}
          >
            {recentPackets.length > 0 ? <>
              <div aria-hidden="true" style={{ height: virtualRows.top }} />
              {virtualRows.rows.map((packet, virtualIndex) => {
              const iatas = packetObserverIatas(packet, nodeMap);
              const observerDisplay = iatas.length === 0 ? 'unknown' : iatas.join(' · ');
              const isSelected = selectedPacketHash === packet.packet_hash;
              return (
                <React.Fragment key={`${packet.packet_hash}-${packet.time}-${virtualRows.start + virtualIndex}`}>
                  <article
                    className={`uk-feed-packet-row${isSelected ? ' uk-feed-packet-row--selected' : ''}`}
                    onClick={() => setSelectedPacketHash(isSelected ? null : packet.packet_hash)}
                    role="button"
                    tabIndex={0}
                    aria-pressed={isSelected}
                    aria-label={`${isSelected ? 'Collapse' : 'Open'} packet ${packet.packet_hash}`}
                    onKeyDown={(event) => {
                      if (event.key !== 'Enter' && event.key !== ' ') return;
                      event.preventDefault();
                      setSelectedPacketHash(isSelected ? null : packet.packet_hash);
                    }}
                  >
                    <div className="uk-feed-packet-row__meta">
                      <span>{new Date(packet.time).toLocaleTimeString()}</span>
                      <span>{packet.packet_type != null ? (TYPE_LABELS[packet.packet_type] ?? `T${packet.packet_type}`) : '—'}</span>
                      <span className="uk-feed-packet-row__hops">{packet.hop_count != null ? `${packet.hop_count} hop${packet.hop_count !== 1 ? 's' : ''}` : '—'}</span>
                      <span className="uk-feed-packet-row__hash dev-status-mono">{packet.packet_hash}</span>
                      <span className="uk-feed-packet-row__observer">{observerDisplay}</span>
                    </div>
                    <p className="uk-feed-packet-row__summary">{packetSummary(packet, nodeMap)}</p>
                  </article>
                </React.Fragment>
              );
            })}
              <div aria-hidden="true" style={{ height: virtualRows.bottom }} />
            </> : (
              <p className="dev-status-empty">No public packets have arrived yet.</p>
            )}
          </div>
          {/* Selected details live outside the fixed-height virtual rows. This
              keeps expansion height from corrupting spacer calculations. */}
          {selectedPacket && (
            <section className="uk-feed-mobile-selection" aria-label="Selected packet summary">
              <div className="uk-feed-inline-map">
                <FeedMapPanel
                  key={selectedPacket.packet_hash}
                  packet={selectedPacket}
                  nodeMap={nodeMap}
                  cachedLazyPath={selectedLazyPath}
                  isLoading={pathTreeStatus === 'settling' || pathTreeStatus === 'loading'}
                />
              </div>
              <div className="uk-feed-mobile-detail">
                <div className="uk-feed-stats__selected-meta">
                  <code className="feed-detail__hash">{selectedPacket.packet_hash}</code>
                  <span className="feed-detail__badge">
                    {selectedPacket.packet_type != null
                      ? (TYPE_LABELS[selectedPacket.packet_type] ?? `T${selectedPacket.packet_type}`)
                      : '—'}
                  </span>
                  {selectedPacket.hop_count != null && (
                    <span className="feed-detail__badge feed-detail__badge--muted">
                      {selectedPacket.hop_count} hop{selectedPacket.hop_count !== 1 ? 's' : ''}
                    </span>
                  )}
                </div>
                <p className="uk-feed-stats__selected-summary">{packetSummary(selectedPacket, nodeMap)}</p>
                <div className="uk-feed-stats__actions">
                  <button type="button" className="uk-feed-stats__tree-toggle" onClick={openPathTree}>
                    Repeater tree
                  </button>
                </div>
              </div>
            </section>
          )}
        </div>

        {/* ── Right column: map + stats ──────────────────────────────── */}
        <div className="uk-feed-right">

          {/* Map panel */}
          <div className="uk-feed-map">
            <FeedMapPanel
              key={selectedPacket?.packet_hash ?? 'none'}
              packet={selectedPacket}
              nodeMap={nodeMap}
              cachedLazyPath={selectedLazyPath}
              isLoading={pathTreeStatus === 'settling' || pathTreeStatus === 'loading'}
            />
          </div>

          {/* Stats panel */}
          <div className="uk-feed-stats">
            <div className="uk-feed-stats__row">
              <span className={`uk-feed-live-dot uk-feed-live-dot--${connDot}`} />
              <span className="uk-feed-stats__label">
                {connLabel}
              </span>
              <span className="uk-feed-stats__sep">·</span>
              <span className="uk-feed-stats__label">{activeObserverCount} observer{activeObserverCount !== 1 ? 's' : ''}</span>
              <span className="uk-feed-stats__sep">·</span>
              <span className="uk-feed-stats__label">{networkStats.packetsLastMin} pkt/min</span>
              <span className="uk-feed-stats__sep">·</span>
              <span className="uk-feed-stats__label">last: {timeAgo(globalLatestPacket?.time)}</span>
            </div>
            {networkStats.topTypes.length > 0 && (
              <div className="uk-feed-type-tags">
                {networkStats.topTypes.map(({ label, count, type }) => (
                  <span key={type} className="uk-feed-type-tag">{label} <strong>{count}</strong></span>
                ))}
              </div>
            )}
            {selectedPacket && (
              <div className="uk-feed-stats__selected">
                <div className="uk-feed-stats__selected-meta">
                  <code className="feed-detail__hash">{selectedPacket.packet_hash}</code>
                  <span className="feed-detail__badge">
                    {selectedPacket.packet_type != null ? (TYPE_LABELS[selectedPacket.packet_type] ?? `T${selectedPacket.packet_type}`) : '—'}
                  </span>
                  {selectedPacket.hop_count != null && (
                    <span className="feed-detail__badge feed-detail__badge--muted">
                      {selectedPacket.hop_count} hop{selectedPacket.hop_count !== 1 ? 's' : ''}
                    </span>
                  )}
                  <button type="button" className="uk-feed-stats__close" onClick={() => setSelectedPacketHash(null)} aria-label="Clear selected packet">✕</button>
                </div>
                <p className="uk-feed-stats__selected-summary">{packetSummary(selectedPacket, nodeMap)}</p>
                <div className="uk-feed-stats__actions">
                  <button
                    type="button"
                    className="uk-feed-stats__tree-toggle"
                    onClick={openPathTree}
                  >
                    Repeater tree
                  </button>
                  <button type="button" className="uk-feed-stats__tree-toggle" onClick={() => setDetailOpen(true)}>
                    Packet details
                  </button>
                </div>
              </div>
            )}
            {latestPacket && !selectedPacket && (
              <p className="uk-feed-stats__latest">Latest: {packetSummary(latestPacket, nodeMap)}</p>
            )}
          </div>
        </div>

      </div>
      <FeedDialogs
        scopeKey={scopeKey}
        packet={selectedPacket}
        nodeMap={nodeMap}
        lazyPath={selectedLazyPath}
        pathTreeStatus={pathTreeStatus}
        pathTreeOpen={pathTreeOpen}
        detailOpen={detailOpen}
        network={site.networkFilter ?? site.network}
        observer={site.observerId}
        onClosePathTree={() => setPathTreeOpen(false)}
        onCloseDetail={() => setDetailOpen(false)}
        onRetryPath={() => {
          if (selectedPacket) void fetchSelectedLazyPath(selectedPacket, true);
        }}
      />
    </>
  );
};
