import React, { useMemo, useCallback, useEffect, useState, useRef } from 'react';
import { getCurrentSite } from '../../config/site.js';
import { useWebSocket, type WSMessage } from '../../hooks/useWebSocket.js';
import { useNodes, type MeshNode, type LivePacketData } from '../../hooks/useNodes.js';
import type { RecentPacketRow } from '../../hooks/packetFeed.js';
import { chartStatsEndpoint, uncachedEndpoint } from '../../utils/api.js';
import { PathMap } from './PacketDetailPanel.js';
import type { LazyPathResult, LazyPath } from './PacketDetailPanel.js';

export type FeedPacket = {
  time: string;
  packet_hash: string;
  topic?: string;
  rx_node_id?: string | null;
  src_node_id?: string | null;
  packet_type?: number | null;
  hop_count?: number | null;
  rssi?: number | null;
  snr?: number | null;
  payload?: Record<string, unknown>;
  observer_node_ids?: string[];
  rx_count?: number;
  tx_count?: number;
  summary?: string | null;
  path_hashes?: string[] | null;
};

const TYPE_LABELS: Record<number, string> = {
  0: 'REQ',
  1: 'RSP',
  2: 'DM',
  3: 'ACK',
  4: 'ADV',
  5: 'GRP',
  6: 'DAT',
  7: 'ANON',
  8: 'PATH',
  9: 'TRC',
  11: 'CTL',
};

const MAX_PACKETS = 500;
const MAX_CHANNEL_MESSAGES = 30;

function timeAgo(ts?: string | null): string {
  if (!ts) return 'never';
  const ageMs = Math.max(0, Date.now() - Date.parse(ts));
  const sec = Math.floor(ageMs / 1000);
  if (sec < 60) return `${sec}s ago`;
  if (sec < 3600) return `${Math.floor(sec / 60)}m ago`;
  if (sec < 86400) return `${Math.floor(sec / 3600)}h ago`;
  return `${Math.floor(sec / 86400)}d ago`;
}


function packetSummary(packet: FeedPacket, nodeMap?: Map<string, MeshNode>): string {
  if (typeof packet.summary === 'string' && packet.summary.trim()) return packet.summary.trim();
  const payload = packet.payload ?? {};
  const appData = payload['appData'] as Record<string, unknown> | undefined;
  const type = packet.packet_type;

  if (type === 4) {
    const name = typeof appData?.['name'] === 'string' ? appData['name'].trim() : null;
    return name ? `Node advertisement — ${name}` : 'Node advertisement';
  }
  if (type === 3) return 'Acknowledgement';
  if (type === 8) {
    const count = Array.isArray(payload['pathHashes']) ? payload['pathHashes'].length : (packet.path_hashes?.length ?? null);
    return count != null ? `Path trace (${count} hops)` : 'Path trace';
  }
  if (type === 9) return 'Trace';
  if (type === 0) return 'Request';
  if (type === 1) return 'Response';
  if (type === 2) {
    const srcNode = packet.src_node_id ? nodeMap?.get(packet.src_node_id) : undefined;
    const srcName = srcNode?.name ?? (packet.src_node_id ? `${packet.src_node_id.slice(0, 8)}…` : null);
    return srcName ? `Encrypted DM from ${srcName}` : 'Encrypted direct message';
  }
  if (type === 5) {
    const candidate = [
      typeof appData?.['text'] === 'string' ? appData['text'] : undefined,
      typeof payload['summary'] === 'string' ? payload['summary'] : undefined,
    ].find((v) => typeof v === 'string' && v.trim());
    return String(candidate ?? 'Group message');
  }

  const candidate = [
    typeof appData?.['name'] === 'string' ? appData['name'] : undefined,
    typeof appData?.['text'] === 'string' ? appData['text'] : undefined,
    typeof payload['summary'] === 'string' ? payload['summary'] : undefined,
  ].find((value) => typeof value === 'string' && value.trim());
  return String(candidate ?? 'No decoded summary');
}

function packetObserverIds(packet: FeedPacket): string[] {
  return packet.observer_node_ids?.length
    ? packet.observer_node_ids.filter(Boolean)
    : (packet.rx_node_id ? [packet.rx_node_id] : []);
}

function packetTopicIata(packet: FeedPacket): string | null {
  const topic = String(packet.payload?.topic ?? packet.topic ?? '').trim();
  if (!topic) return null;
  const parts = topic.split('/');
  if (parts.length < 2) return null;
  const iata = String(parts[1] ?? '').trim().toUpperCase();
  return /^[A-Z0-9]{2,8}$/.test(iata) ? iata : null;
}

function packetChannel(packet: FeedPacket): string | null {
  const summary = typeof packet.summary === 'string' ? packet.summary.trim() : null;
  if (summary?.startsWith('[')) {
    const end = summary.indexOf(']');
    if (end > 1) {
      const name = summary.slice(1, end);
      if (!name.toLowerCase().includes('encrypt')) return name;
    }
  }
  return null;
}

function packetObserverIatas(packet: FeedPacket, nodeMap: Map<string, MeshNode>): string[] {
  const values = new Set<string>();
  for (const observerId of packetObserverIds(packet)) {
    const iata = nodeMap.get(observerId)?.iata?.trim().toUpperCase();
    if (iata) values.add(iata);
  }
  if (values.size > 0) return Array.from(values);
  // Fallback: extract IATA from MQTT topic (reliable even for new/uncached nodes)
  const topicIata = packetTopicIata(packet);
  if (topicIata) return [topicIata];
  return [];
}

// ── Feed map panel (top-right quadrant) ───────────────────────────────────────

const FeedMapPanel: React.FC<{
  packet: FeedPacket | null;
  nodeMap: Map<string, MeshNode>;
  cachedLazyPath: LazyPathResult | null;
}> = ({ packet, nodeMap, cachedLazyPath }) => {
  const observerPositions = useMemo((): [number, number][] => {
    if (!packet) return [];
    const candidates: string[] = packet.observer_node_ids?.length
      ? packet.observer_node_ids
      : (packet.rx_node_id ? [packet.rx_node_id] : []);
    const positions: [number, number][] = [];
    const seen = new Set<string>();
    for (const id of candidates) {
      if (!id || seen.has(id)) continue;
      seen.add(id);
      const node = nodeMap.get(id);
      if (node?.lat != null && node?.lon != null) positions.push([node.lat, node.lon]);
    }
    return positions;
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [packet?.observer_node_ids, packet?.rx_node_id, nodeMap]);

  if (!packet) {
    return (
      <div className="uk-feed-map-placeholder">
        Select a packet to see its path
      </div>
    );
  }

  return (
    <PathMap
      results={[]}
      observerPositions={observerPositions}
      lazyPaths={(cachedLazyPath?.paths ?? []) as LazyPath[]}
      nodeMap={nodeMap}
    />
  );
};

export const UKFeedPage: React.FC = () => {
  const site = getCurrentSite();
  const scope = useMemo(() => ({ network: site.networkFilter, observer: site.observerId }), [site.networkFilter, site.observerId]);
  const [selectedIata, setSelectedIata] = useState<string>(() => localStorage.getItem('uk-feed-iata') ?? 'all');
  const [selectedChannel, setSelectedChannel] = useState<string | null>(null);
  const [messagesOnly, setMessagesOnly] = useState<boolean>(() => localStorage.getItem('uk-feed-messages-only') === '1');
  const [regionOptions, setRegionOptions] = useState<string[]>([]);
  const [selectedPacketHash, setSelectedPacketHash] = useState<string | null>(null);
  const [searchQuery, setSearchQuery] = useState<string>('');
  const [now, setNow] = useState(() => Date.now());

  // Use useNodes hook like the main App does
  const {
    nodes: nodeMap,
    packets: packetsList,
    handleInitialState,
    handlePacket,
    handleNodeUpdate,
    handleNodeUpsert,
  } = useNodes();

  const handleWSMessage = useCallback((msg: WSMessage) => {
    if (msg.type === 'initial_state') {
      const data = msg.data as {
        nodes?: MeshNode[];
        packets?: RecentPacketRow[];
      };
      if (data.nodes && data.packets) {
        handleInitialState({ nodes: data.nodes, packets: data.packets });
      }
      return;
    }

    if (msg.type === 'packet') {
      handlePacket(msg.data as LivePacketData);
      return;
    }

    if (msg.type === 'node_update') {
      handleNodeUpdate(msg.data as { nodeId: string; ts: number });
      return;
    }

    if (msg.type === 'node_upsert') {
      handleNodeUpsert(msg.data as Partial<MeshNode> & { node_id: string });
    }
  }, [handleInitialState, handleNodeUpdate, handleNodeUpsert, handlePacket]);

  // Connect to WebSocket
  useWebSocket(handleWSMessage, scope);

  // Persist filters
  useEffect(() => { localStorage.setItem('uk-feed-iata', selectedIata); }, [selectedIata]);
  useEffect(() => { localStorage.setItem('uk-feed-messages-only', messagesOnly ? '1' : '0'); }, [messagesOnly]);

  // Clock tick for live connection indicator and stats
  useEffect(() => {
    const timer = window.setInterval(() => setNow(Date.now()), 5000);
    return () => window.clearInterval(timer);
  }, []);

  // ── Background lazy path cache ────────────────────────────────────────────
  // Tracks observer keys and settle timers for all visible packets so that
  // lazy paths are ready before the user clicks on a packet.
  const LAZY_SETTLE_MS = 10_000;
  const [lazyCache, setLazyCache] = useState<Map<string, LazyPathResult>>(() => new Map());
  const lazyCacheRef = useRef<Map<string, LazyPathResult>>(new Map());
  const lazyTimersRef = useRef<Map<string, ReturnType<typeof setTimeout>>>(new Map());
  const observerKeysRef = useRef<Map<string, string>>(new Map());

  useEffect(() => {
    let cancelled = false;

    const loadObserverRegions = async () => {
      try {
        const response = await fetch(uncachedEndpoint(chartStatsEndpoint(scope)), { cache: 'no-store' });
        if (!response.ok) return;
        const json = await response.json() as {
          observerRegions?: Array<{ iata?: string | null; activeObservers?: number; observers?: number }>;
        };
        const values = (json.observerRegions ?? [])
          .map((region) => String(region.iata ?? '').trim().toUpperCase())
          .filter((iata) => /^[A-Z0-9]{2,8}$/.test(iata));
        if (!cancelled) setRegionOptions(Array.from(new Set(values)).sort((a, b) => a.localeCompare(b)));
      } catch {
        // Leave the dropdown populated from live packet traffic only if stats fetch fails.
      }
    };

    void loadObserverRegions();
    const timer = window.setInterval(() => {
      void loadObserverRegions();
    }, 60_000);

    return () => {
      cancelled = true;
      window.clearInterval(timer);
    };
  }, [scope]);

  // Convert live packets to FeedPacket format for display
  const packets: FeedPacket[] = useMemo(() => {
    return packetsList.slice(0, MAX_PACKETS).map(p => ({
      time: new Date(p.ts).toISOString(),
      packet_hash: p.packetHash,
      rx_node_id: p.rxNodeId ?? null,
      src_node_id: p.srcNodeId ?? null,
      topic: p.topic,
      packet_type: p.packetType ?? null,
      hop_count: p.hopCount ?? null,
      rssi: null,
      snr: null,
      payload: p as any,
      observer_node_ids: p.observerIds,
      rx_count: p.rxCount,
      tx_count: p.txCount,
      summary: p.summary ?? null,
      path_hashes: (p.path as string[] | undefined) ?? null,
    }));
  }, [packetsList]);

  const availableIatas = useMemo(() => {
    return regionOptions;
  }, [regionOptions]);

  const availableChannels = useMemo(() => {
    const seen = new Set<string>();
    for (const p of packets) {
      const ch = packetChannel(p);
      if (ch) seen.add(ch);
    }
    return Array.from(seen).sort((a, b) => a.localeCompare(b));
  }, [packets]);

  // Per-channel message history (last 30 per channel, newest first)
  const channelMessages = useMemo(() => {
    const map = new Map<string, FeedPacket[]>();
    for (const p of packets) {
      const ch = packetChannel(p);
      if (!ch) continue;
      const list = map.get(ch) ?? [];
      if (list.length < MAX_CHANNEL_MESSAGES) {
        list.push(p);
        map.set(ch, list);
      }
    }
    return map;
  }, [packets]);

  useEffect(() => {
    if (selectedIata === 'all') return;
    if (!availableIatas.includes(selectedIata)) {
      setSelectedIata('all');
    }
  }, [availableIatas, selectedIata]);

  const filteredPackets = useMemo(() => {
    let result = packets;
    if (selectedChannel !== null) {
      result = result.filter((packet) => packetChannel(packet) === selectedChannel);
    }
    if (selectedIata !== 'all') {
      result = result.filter((packet) => packetObserverIatas(packet, nodeMap).includes(selectedIata));
    }
    if (messagesOnly) {
      result = result.filter((packet) => packet.packet_type === 2 || packet.packet_type === 5);
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
  }, [messagesOnly, nodeMap, packets, selectedIata, selectedChannel, searchQuery]);

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
    if (selectedChannel !== null) {
      // Show channel history (last 30), optionally filtered by region + search
      let msgs = channelMessages.get(selectedChannel) ?? [];
      if (selectedIata !== 'all') {
        msgs = msgs.filter((p) => packetObserverIatas(p, nodeMap).includes(selectedIata));
      }
      if (searchQuery.trim()) {
        const q = searchQuery.trim().toLowerCase();
        msgs = msgs.filter((p) =>
          packetSummary(p, nodeMap).toLowerCase().includes(q) ||
          p.packet_hash.toLowerCase().startsWith(q),
        );
      }
      return msgs;
    }
    return searchQuery.trim() ? filteredPackets : filteredPackets.slice(0, 20);
  }, [selectedChannel, channelMessages, selectedIata, nodeMap, searchQuery, filteredPackets]);

  // Always derive selectedPacket from the live list so new MQTT observers are picked up
  const selectedPacket = useMemo(
    () => selectedPacketHash ? (packets.find((p) => p.packet_hash === selectedPacketHash) ?? null) : null,
    [selectedPacketHash, packets],
  );

  // Live connection status
  const lastPacketAgeMs = globalLatestPacket ? now - Date.parse(globalLatestPacket.time) : Infinity;
  const connStatus = lastPacketAgeMs < 10_000 ? 'live' : lastPacketAgeMs < 60_000 ? 'stale' : 'dead';

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

  // Watch visible packets: start/reset settle timer when observer set changes.
  // When settled, fetch lazy path and store in cache — ready before user clicks.
  useEffect(() => {
    const network = site.networkFilter ?? null;

    for (const packet of recentPackets) {
      if (!packet.path_hashes?.length) continue; // skip packets with no path hashes
      const hash = packet.packet_hash;
      const key = (packet.observer_node_ids ?? []).slice().sort().join(',') || (packet.rx_node_id ?? '');

      const prevKey = observerKeysRef.current.get(hash);
      if (prevKey === key) continue; // no change, timer already running or done
      observerKeysRef.current.set(hash, key);

      // Cancel any existing timer for this hash (propagation still ongoing)
      const existing = lazyTimersRef.current.get(hash);
      if (existing != null) clearTimeout(existing);

      // Clear stale cache entry since observers changed
      if (lazyCacheRef.current.has(hash)) {
        lazyCacheRef.current.delete(hash);
        setLazyCache(new Map(lazyCacheRef.current));
      }

      const timer = setTimeout(() => {
        lazyTimersRef.current.delete(hash);
        const netParam = network ? `&network=${encodeURIComponent(network)}` : '';
        fetch(`/api/path-lazy/resolve?hash=${hash}${netParam}`, { cache: 'no-store' })
          .then((r) => (r.ok ? r.json() as Promise<LazyPathResult> : null))
          .catch(() => null)
          .then((result) => {
            if (!result) return;
            lazyCacheRef.current.set(hash, result);
            setLazyCache(new Map(lazyCacheRef.current));
          });
      }, LAZY_SETTLE_MS);

      lazyTimersRef.current.set(hash, timer);
    }
  }, [recentPackets, site.networkFilter]);

  return (
    <>
      <section className="site-page-hero">
        <div className="site-content">
          <h1 className="site-page-hero__title">Public Feed</h1>
          <p className="site-page-hero__sub">
            Live MQTT observer activity across the public UK Mesh feed.
          </p>
        </div>
      </section>

      <div className="uk-feed-layout">

        {/* ── Channels sidebar ───────────────────────────────────────── */}
        <nav className="uk-feed-channels">
          <div className="uk-feed-channels__header">Channels</div>
          <button
            className={`uk-feed-channel-item${selectedChannel === null ? ' uk-feed-channel-item--active' : ''}`}
            onClick={() => setSelectedChannel(null)}
          >
            All
          </button>
          {availableChannels.map((ch) => (
            <button
              key={ch}
              className={`uk-feed-channel-item${selectedChannel === ch ? ' uk-feed-channel-item--active' : ''}`}
              onClick={() => setSelectedChannel(ch)}
            >
              {ch}
            </button>
          ))}

          <div className="uk-feed-channels__divider" />
          <div className="uk-feed-channels__header">Regions</div>
          <button
            className={`uk-feed-channel-item${selectedIata === 'all' ? ' uk-feed-channel-item--active' : ''}`}
            onClick={() => setSelectedIata('all')}
          >
            All
          </button>
          {availableIatas.map((iata) => (
            <button
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
              placeholder="Search packets…"
              value={searchQuery}
              onChange={(e) => setSearchQuery(e.target.value)}
            />
          </div>
          <div className="uk-feed-packets-list">
            {recentPackets.length > 0 ? recentPackets.map((packet) => {
              const iatas = packetObserverIatas(packet, nodeMap);
              const observerDisplay = iatas.length === 0 ? 'unknown' : iatas.join(' · ');
              const isSelected = selectedPacketHash === packet.packet_hash;
              return (
                <article
                  className={`uk-feed-packet-row${isSelected ? ' uk-feed-packet-row--selected' : ''}`}
                  key={`${packet.packet_hash}-${packet.time}`}
                  onClick={() => setSelectedPacketHash(isSelected ? null : packet.packet_hash)}
                >
                  <div className="uk-feed-packet-row__meta">
                    <span>{new Date(packet.time).toLocaleTimeString()}</span>
                    <span>{packet.packet_type != null ? (TYPE_LABELS[packet.packet_type] ?? `T${packet.packet_type}`) : '—'}</span>
                    <span>{packet.hop_count != null ? `${packet.hop_count} hop${packet.hop_count !== 1 ? 's' : ''}` : '—'}</span>
                    <span className="dev-status-mono">{packet.packet_hash}</span>
                    <span className="uk-feed-packet-row__observer">{observerDisplay}</span>
                  </div>
                  <p className="uk-feed-packet-row__summary">{packetSummary(packet, nodeMap)}</p>
                </article>
              );
            }) : (
              <p className="dev-status-empty">No public packets have arrived yet.</p>
            )}
          </div>
        </div>

        {/* ── Right column: map + stats ──────────────────────────────── */}
        <div className="uk-feed-right">

          {/* Map panel */}
          <div className="uk-feed-map">
            <FeedMapPanel
              packet={selectedPacket}
              nodeMap={nodeMap}
              cachedLazyPath={selectedPacket ? (lazyCache.get(selectedPacket.packet_hash) ?? null) : null}
            />
          </div>

          {/* Stats panel */}
          <div className="uk-feed-stats">
            <div className="uk-feed-stats__row">
              <span className={`uk-feed-live-dot uk-feed-live-dot--${connStatus}`} />
              <span className="uk-feed-stats__label">
                {connStatus === 'live' ? 'Live' : globalLatestPacket ? 'Active' : 'Waiting…'}
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
                  <button className="uk-feed-stats__close" onClick={() => setSelectedPacketHash(null)}>✕</button>
                </div>
                <p className="uk-feed-stats__selected-summary">{packetSummary(selectedPacket, nodeMap)}</p>
              </div>
            )}
            {latestPacket && !selectedPacket && (
              <p className="uk-feed-stats__latest">Latest: {packetSummary(latestPacket, nodeMap)}</p>
            )}
          </div>
        </div>

      </div>
    </>
  );
};
