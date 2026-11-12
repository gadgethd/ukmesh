import { POLLING } from '../../config/polling.js';
import { parseStatsSection } from '../../features/stats/sections.js';
import React, { useMemo, useEffect, useState, useRef } from 'react';
import './feed-page.css';
import { getCurrentSite } from '../../config/site.js';
import { useWebSocket } from '../../hooks/useWebSocket.js';
import { useRealtimeScopeSession } from '../../hooks/useRealtimeScopeSession.js';
import { scopeKey as publicScopeKey } from '../../utils/scope.js';
import {
  nodeStore,
  useMessages,
  useNodeMap,
  usePackets,
} from '../../hooks/useNodes.js';
import { mapMessageRows, type RecentPacketRow } from '../../features/packets/liveFeed.js';
import { useAppMessageHandler } from '../../hooks/useAppMessageHandler.js';
import { useMessageTags } from '../../hooks/useMessageTags.js';
import { messageTagKind, messageTagTitle } from '../../features/packets/liveFeed.js';
import {
  fetchJson,
  uncachedEndpoint,
  withScopeParams,
} from '../../utils/api.js';
import { useRuntimeFeatures } from '../../config/runtimeFeatures.js';
import { useVisibilityPoll } from '../../hooks/useVisibilityPoll.js';
import { useMeasuredVirtualRows } from '../../hooks/useMeasuredVirtualRows.js';
import { FeedMapPanel } from './FeedPathViews.js';
import { FeedDialogs } from './FeedDialogs.js';
import { timeAgo } from '../../utils/time.js';
import { PACKET_TYPE_LABELS } from '../../config/packetTypes.js';
import { useFeedPathSelection } from '../../hooks/useFeedPathSelection.js';
import {
  MESSAGE_SCOPE_CHANNELS,
  MAX_PACKETS,
  aggregatedPacketToFeedPacket,
  mergeFeedPacketObservations,
  formatFeedTimestamp,
  packetMatchesMessageScope,
  packetObserverIatas,
  packetObserverIds,
  packetSummary,
  type FeedPacket,
  type MessageScope,
} from '../../features/packets/feedModel.js';
import {
  feedConnectionDot,
  feedConnectionLabel,
  feedConnectionStatus,
  validatedRegionSelection,
} from '../../features/packets/feedState.js';

export const UKFeedPage: React.FC = () => {
  const site = getCurrentSite();
  const { privacyGeneration } = useRuntimeFeatures();
  const scope = useMemo(() => ({ network: site.networkFilter, observer: site.observerId }), [site.networkFilter, site.observerId]);
  const scopeKey = publicScopeKey({ ...scope, privacyGeneration });
  const session = useRealtimeScopeSession({ ...scope, privacyGeneration });
  const scopeEpoch = session.nodeEpoch;
  const [selectedIata, setSelectedIata] = useState<string>(() => localStorage.getItem('uk-feed-iata') ?? 'all');
  const [selectedMessageScope, setSelectedMessageScope] = useState<MessageScope>(() => {
    const stored = localStorage.getItem('uk-feed-message-scope');
    return stored === 'all' || (stored !== null && (MESSAGE_SCOPE_CHANNELS as readonly string[]).includes(stored))
      ? (stored as MessageScope)
      : 'all';
  });
  const [messagesOnly, setMessagesOnly] = useState<boolean>(() => localStorage.getItem('uk-feed-messages-only') === '1');
  const [regionOptions, setRegionOptions] = useState<string[]>([]);
  const [regionOptionsStatus, setRegionOptionsStatus] = useState<'loading' | 'ready' | 'failed'>('loading');
  const [selectedPacketHash, setSelectedPacketHash] = useState<string | null>(null);
  const [searchQuery, setSearchQuery] = useState<string>('');
  const [selectedPacketType, setSelectedPacketType] = useState<string | null>(() => {
    const requested = new URLSearchParams(window.location.search).get('type')?.trim().toLowerCase();
    return requested && (/^(?:[0-9]{1,3}|unknown)$/.test(requested)) ? requested : null;
  });
  const [now, setNow] = useState(() => Date.now());
  const [detailOpen, setDetailOpen] = useState(false);
  const [historicalMessages, setHistoricalMessages] = useState<FeedPacket[]>([]);
  const [historyStatus, setHistoryStatus] = useState<'idle' | 'loading' | 'ready' | 'error'>('idle');
  const [historyError, setHistoryError] = useState<string | null>(null);
  const packetListRef = useRef<HTMLDivElement>(null);
  const historyRequestRef = useRef<AbortController | null>(null);

  const nodeMap = useNodeMap();
  const packetsList = usePackets();
  const messagesList = useMessages();

  const handleWSMessage = useAppMessageHandler({
    epoch: scopeEpoch,
    handleInitialState: (data) => nodeStore.handleInitialState(data, scopeEpoch),
    handlePacket: (data) => nodeStore.handlePacket(data, scopeEpoch),
    handleNodeUpdate: (data) => nodeStore.handleNodeUpdate(data, scopeEpoch),
    handleNodeUpdateBatch: (data) => nodeStore.handleNodeUpdateBatch(data, scopeEpoch),
    handleNodeUpsert: (data) => nodeStore.handleNodeUpsert(data, scopeEpoch),
    handleNodeUpsertBatch: (data) => nodeStore.handleNodeUpsertBatch(data, scopeEpoch),
  });

  const wsConnection = useWebSocket(handleWSMessage, scope, scopeEpoch, session.ready);

  // Live tag enrichment: tags land ~2s after a packet, so patch rows by hash.
  useMessageTags(scope, scopeKey, session.ready);

  useEffect(() => {
    historyRequestRef.current?.abort();
    historyRequestRef.current = null;
    if (!session.ready || selectedMessageScope === 'all') {
      setHistoricalMessages([]);
      setHistoryStatus('idle');
      setHistoryError(null);
      return;
    }

    const controller = new AbortController();
    historyRequestRef.current = controller;
    setHistoricalMessages([]);
    setHistoryStatus('loading');
    setHistoryError(null);
    const endpoint = withScopeParams(
      `/api/feed/messages?channel=${encodeURIComponent(selectedMessageScope)}&limit=50`,
      scope,
    );
    void fetchJson<RecentPacketRow[]>(
      uncachedEndpoint(endpoint),
      { cache: 'no-store', signal: controller.signal },
      { timeoutMs: 45_000, maxBytes: 4 * 1024 * 1024 },
    ).then((rows) => {
      if (controller.signal.aborted || historyRequestRef.current !== controller) return;
      setHistoricalMessages(mapMessageRows(rows).map(aggregatedPacketToFeedPacket));
      setHistoryStatus('ready');
    }).catch((error: unknown) => {
      if (controller.signal.aborted || historyRequestRef.current !== controller) return;
      setHistoryStatus('error');
      setHistoryError(error instanceof Error ? error.message : 'History unavailable');
    });

    return () => {
      controller.abort();
      if (historyRequestRef.current === controller) historyRequestRef.current = null;
    };
  }, [scope, scopeKey, selectedMessageScope, session.ready]);

  useEffect(() => { localStorage.setItem('uk-feed-iata', selectedIata); }, [selectedIata]);
  useEffect(() => { localStorage.setItem('uk-feed-message-scope', selectedMessageScope); }, [selectedMessageScope]);
  useEffect(() => { localStorage.setItem('uk-feed-messages-only', messagesOnly ? '1' : '0'); }, [messagesOnly]);

  useEffect(() => {
    const timer = window.setInterval(() => setNow(Date.now()), POLLING.feedClock);
    return () => window.clearInterval(timer);
  }, []);

  useEffect(() => {
    setRegionOptionsStatus('loading');
    setRegionOptions([]);
  }, [scopeKey]);

  useVisibilityPoll(async (signal) => {
    const payload = await fetchJson<unknown>(
      withScopeParams('/api/stats/charts?sections=observers', scope),
      { cache: 'no-cache', signal },
      { timeoutMs: POLLING.ordinaryTimeout, maxBytes: 4 * 1024 * 1024 },
    );
    if (signal.aborted) return;
    const selected = parseStatsSection(payload, 'observers', { network: scope.network ?? 'ukmesh',
      observer: scope.observer ?? null, generation: privacyGeneration });
    if (!selected) throw new Error('Observer regions no longer match the feed scope');
    const json = selected.data;
    const values = (json.observerRegions ?? [])
      .map((region) => String(region.iata ?? '').trim().toUpperCase())
      .filter((iata) => /^[A-Z0-9]{2,8}$/.test(iata));
    setRegionOptions(Array.from(new Set(values)).sort((a, b) => a.localeCompare(b)));
    setRegionOptionsStatus('ready');
  }, {
    enabled: privacyGeneration > 0,
    scopeKey: `feed-regions:${scopeKey}`,
    intervalMs: POLLING.feedRegions,
    timeoutMs: POLLING.ordinaryTimeout,
    onError: () => setRegionOptionsStatus('failed'),
  });

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

  const selectedChannelMessages = useMemo(() => {
    if (selectedMessageScope === 'all') return retainedMessagePackets;
    const byHash = new Map<string, FeedPacket>();
    for (const packet of [...historicalMessages, ...retainedMessagePackets]) {
      if (packet.packet_type !== 5 || !packetMatchesMessageScope(packet, selectedMessageScope)) continue;
      const current = byHash.get(packet.packet_hash);
      byHash.set(
        packet.packet_hash,
        current ? mergeFeedPacketObservations(current, packet) : packet,
      );
    }
    return Array.from(byHash.values())
      .sort((a, b) => Date.parse(b.first_seen_time ?? b.time) - Date.parse(a.first_seen_time ?? a.time))
      .slice(0, 50);
  }, [historicalMessages, retainedMessagePackets, selectedMessageScope]);

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
    let result = messageViewActive
      ? (selectedMessageScope === 'all' ? retainedMessagePackets : selectedChannelMessages)
      : packets;
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
  }, [messagesOnly, nodeMap, packets, retainedMessagePackets, searchQuery, selectedChannelMessages, selectedIata, selectedMessageScope, selectedPacketType]);

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
  const packetKeys = useMemo(() => recentPackets.map(packet => `${packet.packet_hash}-${packet.time}`), [recentPackets]);
  const virtualRows = useMeasuredVirtualRows(packetKeys, packetListRef);

  // Always derive selectedPacket from the live list so new MQTT observers are picked up
  const selectedPacket = useMemo(
    () => selectedPacketHash
      ? (packets.find((p) => p.packet_hash === selectedPacketHash)
        ?? retainedMessagePackets.find((p) => p.packet_hash === selectedPacketHash)
        ?? null)
      : null,
    [selectedPacketHash, packets, retainedMessagePackets],
  );
  const { selectedLazyPath, pathTreeStatus, pathTreeOpen, setPathTreeOpen,
    fetchSelectedLazyPath, openPathTree } = useFeedPathSelection({ ...scope, privacyGeneration }, scopeEpoch, selectedPacket);

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
      .map(([type, count]) => ({ type: Number(type), label: PACKET_TYPE_LABELS[Number(type)] ?? `T${type}`, count }));
    return { packetsLastMin, activeSenderCount: activeSenders.size, topTypes };
  }, [packets, now]);

  return (
    <>
      <h1 className="ui-visually-hidden">Live feed</h1>

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
          {MESSAGE_SCOPE_CHANNELS.map((channel) => (
            <button
              key={channel}
              type="button"
              className={`uk-feed-channel-item${selectedMessageScope === channel ? ' uk-feed-channel-item--active' : ''}`}
              onClick={() => setSelectedMessageScope(channel)}
            >
              {channel === 'public' ? 'Public' : channel}
            </button>
          ))}

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
                Type {PACKET_TYPE_LABELS[Number(selectedPacketType)] ?? selectedPacketType} ×
              </button>
            )}
            {selectedMessageScope !== 'all' && (
              <span className={`uk-feed-history-status uk-feed-history-status--${historyStatus}`} aria-live="polite">
                {historyStatus === 'loading'
                  ? `Loading ${selectedMessageScope} history…`
                  : historyStatus === 'error'
                    ? (historyError ?? 'History unavailable')
                    : `${selectedMessageScope} history · ${selectedChannelMessages.length} messages`}
              </span>
            )}
          </div>
          <div
            className="uk-feed-packets-list"
            ref={packetListRef}
            onScroll={virtualRows.onScroll}
          >
            {recentPackets.length > 0 ? <>
              <div aria-hidden="true" style={{ height: virtualRows.top }} />
              {recentPackets.slice(virtualRows.start, virtualRows.end).map((packet, virtualIndex) => {
              const iatas = packetObserverIatas(packet, nodeMap);
              const observerDisplay = iatas.length === 0 ? 'unknown' : iatas.join(' · ');
              const isSelected = selectedPacketHash === packet.packet_hash;
              return (
                <React.Fragment key={packetKeys[virtualRows.start + virtualIndex]}>
                  <article
                    ref={virtualRows.measureRow(packetKeys[virtualRows.start + virtualIndex]!)}
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
                      <span>{formatFeedTimestamp(packet.time)}</span>
                      <span>{packet.packet_type != null ? (PACKET_TYPE_LABELS[packet.packet_type] ?? `T${packet.packet_type}`) : '—'}</span>
                      <span className="uk-feed-packet-row__hops">{packet.hop_count != null ? `${packet.hop_count} hop${packet.hop_count !== 1 ? 's' : ''}` : '—'}</span>
                      <span className="uk-feed-packet-row__hash dev-status-mono">{packet.packet_hash}</span>
                      <span className="uk-feed-packet-row__observer">{observerDisplay}</span>
                    </div>
                    {messageTagKind(packet) && (
                      <div className="uk-feed-packet-row__tags">
                        <span className="msg-tag-chip" title={messageTagTitle(packet)}>
                          {messageTagKind(packet)}
                        </span>
                      </div>
                    )}
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
          {/* Selected details stay outside the measured list so expanding a
              path does not change packet-row scroll anchors. */}
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
                      ? (PACKET_TYPE_LABELS[selectedPacket.packet_type] ?? `T${selectedPacket.packet_type}`)
                      : '—'}
                  </span>
                  {selectedPacket.hop_count != null && (
                    <span className="feed-detail__badge feed-detail__badge--muted">
                      {selectedPacket.hop_count} hop{selectedPacket.hop_count !== 1 ? 's' : ''}
                    </span>
                  )}
                  {messageTagKind(selectedPacket) && (
                    <span className="msg-tag-chip" title={messageTagTitle(selectedPacket)}>
                      {messageTagKind(selectedPacket)}
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
                    {selectedPacket.packet_type != null ? (PACKET_TYPE_LABELS[selectedPacket.packet_type] ?? `T${selectedPacket.packet_type}`) : '—'}
                  </span>
                  {selectedPacket.hop_count != null && (
                    <span className="feed-detail__badge feed-detail__badge--muted">
                      {selectedPacket.hop_count} hop{selectedPacket.hop_count !== 1 ? 's' : ''}
                    </span>
                  )}
                  {messageTagKind(selectedPacket) && (
                    <span className="msg-tag-chip" title={messageTagTitle(selectedPacket)}>
                      {messageTagKind(selectedPacket)}
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
