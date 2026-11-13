import { POLLING } from '../../config/polling.js';
import { parseStatsSection } from '../../features/stats/sections.js';
import React, { useMemo, useEffect, useLayoutEffect, useState, useRef } from 'react';
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
import {
  fetchJson,
  uncachedEndpoint,
  withScopeParams,
} from '../../utils/api.js';
import { useRuntimeFeatures } from '../../config/runtimeFeatures.js';
import { useVisibilityPoll } from '../../hooks/useVisibilityPoll.js';
import { useMediaQuery } from '../../hooks/useMediaQuery.js';
import { FeedPacketRows } from './FeedPacketRows.js';
import { FeedLiveStatus } from './FeedLiveStatus.js';
import { LoadingIndicator } from '../../components/LoadingIndicator.js';
import { FeedMapPanel, SelectedPacketSummary } from './FeedPathViews.js';
import { FeedDialogs } from './FeedDialogs.js';
import { PACKET_TYPE_LABELS } from '../../config/packetTypes.js';
import { useFeedPathSelection } from '../../hooks/useFeedPathSelection.js';
import {
  MESSAGE_SCOPE_CHANNELS,
  MAX_PACKETS,
  aggregatedPacketToFeedPacket,
  mergeFeedPacketObservations,
  packetMatchesMessageScope,
  packetObserverIatas,
  packetObserverIds,
  packetSummary,
  type FeedPacket,
  type MessageScope,
} from '../../features/packets/feedModel.js';
import {
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
  const compact = useMediaQuery('(max-width: 860px)');
  const [detailOpen, setDetailOpen] = useState(false);
  const historyKey = JSON.stringify([scopeKey, scopeEpoch, selectedMessageScope]);
  const [history, setHistory] = useState<{ key: string; rows: FeedPacket[]; status: 'loading' | 'ready' | 'error'; error: string | null } | null>(null);
  const [historyAttempt, setHistoryAttempt] = useState(0);
  const historicalMessages = session.ready && history?.key === historyKey ? history.rows : [];
  const historyStatus = session.ready && history?.key === historyKey ? history.status : selectedMessageScope === 'all' ? 'idle' : 'loading';
  const historyError = history?.key === historyKey ? history.error : null;
  const historyRequestRef = useRef<AbortController | null>(null);

  const nodeMap = useNodeMap();
  const packetsList = usePackets();
  const messagesList = useMessages();

  useLayoutEffect(() => { setSelectedPacketHash(null); setDetailOpen(false); }, [scopeKey, scopeEpoch]);

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
      setHistory(null);
      return;
    }

    const controller = new AbortController();
    historyRequestRef.current = controller;
    setHistory(previous => ({ key: historyKey, rows: previous?.key === historyKey ? previous.rows : [], status: 'loading', error: null }));
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
      setHistory({ key: historyKey, rows: mapMessageRows(rows).map(aggregatedPacketToFeedPacket), status: 'ready', error: null });
    }).catch((error: unknown) => {
      if (controller.signal.aborted || historyRequestRef.current !== controller) return;
      setHistory(previous => ({ key: historyKey, rows: previous?.key === historyKey ? previous.rows : [], status: 'error', error: error instanceof Error ? error.message : 'History unavailable' }));
    });

    return () => {
      controller.abort();
      if (historyRequestRef.current === controller) historyRequestRef.current = null;
    };
  }, [scope, historyKey, selectedMessageScope, session.ready, historyAttempt]);

  useEffect(() => { localStorage.setItem('uk-feed-iata', selectedIata); }, [selectedIata]);
  useEffect(() => { localStorage.setItem('uk-feed-message-scope', selectedMessageScope); }, [selectedMessageScope]);
  useEffect(() => { localStorage.setItem('uk-feed-messages-only', messagesOnly ? '1' : '0'); }, [messagesOnly]);



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
  const recentPackets = useMemo(() => {
    return filteredPackets.slice(0, MAX_PACKETS);
  }, [filteredPackets]);


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
              <span className={`uk-feed-history-status uk-feed-history-status--${historyStatus}`} role={historyStatus === 'error' ? 'alert' : 'status'}>
                {historyStatus === 'loading'
                  ? `Loading ${selectedMessageScope} history…`
                  : historyStatus === 'error'
                    ? `${historyError ?? 'History unavailable'}.${historicalMessages.length ? ' Showing the last received history.' : ''}`
                    : `${selectedMessageScope} history · ${selectedChannelMessages.length} messages`}
                {historyStatus === 'error' && <button type="button" className="site-btn site-btn--ghost" onClick={() => setHistoryAttempt(value => value + 1)}>Retry history</button>}
              </span>
            )}
          </div>
          <FeedPacketRows key={scopeKey} packets={recentPackets} nodes={nodeMap}
            selectedHash={selectedPacketHash} select={setSelectedPacketHash}
            empty={<div className="dev-status-empty" role="status">
              {!session.ready || wsConnection.readyState === 'connecting'
                ? <LoadingIndicator label="Connecting to live feed..." variant="inline" />
                : wsConnection.readyState === 'disconnected' && packets.length === 0
                  ? <><p>The feed is offline. Reconnection will retry automatically.</p><button type="button" className="site-btn site-btn--ghost" onClick={() => window.location.reload()}>Reload feed</button></>
                  : packets.length || retainedMessagePackets.length
                    ? <><p>No packets match the current filters{selectedIata !== 'all' ? ` in ${selectedIata}` : ''}.</p>
                      {(searchQuery || selectedPacketType || messagesOnly || selectedMessageScope !== 'all') && <button type="button" className="site-btn site-btn--ghost" onClick={() => {
                        setSearchQuery(''); setSelectedPacketType(null); setMessagesOnly(false); setSelectedMessageScope('all');
                        const url = new URL(window.location.href); url.searchParams.delete('type'); window.history.replaceState(null, '', url);
                      }}>Clear filters</button>}</>
                    : <p>No public packets have arrived for this scope yet.</p>}
            </div>} />
          {/* Selected details stay outside the measured list so expanding a
              path does not change packet-row scroll anchors. */}
          {selectedPacket && compact && (
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
              <SelectedPacketSummary packet={selectedPacket} nodeMap={nodeMap}
                clear={() => setSelectedPacketHash(null)} openPath={openPathTree} openDetails={() => setDetailOpen(true)} className="uk-feed-mobile-detail" />
            </section>
          )}
        </div>

        {/* ── Right column: map + stats ──────────────────────────────── */}
        <div className="uk-feed-right">

          {/* Map panel */}
          <div className="uk-feed-map">
            <FeedMapPanel
              key={selectedPacket?.packet_hash ?? 'none'}
              packet={compact ? null : selectedPacket}
              nodeMap={nodeMap}
              cachedLazyPath={selectedLazyPath}
              isLoading={pathTreeStatus === 'settling' || pathTreeStatus === 'loading'}
            />
          </div>

          {/* Status owns its clock; packet rows do not subscribe to it. */}
          <FeedLiveStatus packets={packets} connection={wsConnection} observerCount={activeObserverCount}>
            {selectedPacket && <SelectedPacketSummary packet={selectedPacket} nodeMap={nodeMap}
                clear={() => setSelectedPacketHash(null)} openPath={openPathTree} openDetails={() => setDetailOpen(true)} />}
            {latestPacket && !selectedPacket && (
              <p className="uk-feed-stats__latest">Latest: {packetSummary(latestPacket, nodeMap)}</p>
            )}
          </FeedLiveStatus>
        </div>

      </div>
      {selectedPacket && (pathTreeOpen || detailOpen) && <FeedDialogs
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
      />}
    </>
  );
};
