import React, { useMemo, useCallback, useEffect, useState } from 'react';
import { getCurrentSite } from '../../config/site.js';
import { useWebSocket, type WSMessage } from '../../hooks/useWebSocket.js';
import { useNodes, type MeshNode, type LivePacketData } from '../../hooks/useNodes.js';
import type { RecentPacketRow } from '../../hooks/packetFeed.js';
import { chartStatsEndpoint, uncachedEndpoint } from '../../utils/api.js';

type FeedPacket = {
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

const MAX_PACKETS = 100;

function timeAgo(ts?: string | null): string {
  if (!ts) return 'never';
  const ageMs = Math.max(0, Date.now() - Date.parse(ts));
  const sec = Math.floor(ageMs / 1000);
  if (sec < 60) return `${sec}s ago`;
  if (sec < 3600) return `${Math.floor(sec / 60)}m ago`;
  if (sec < 86400) return `${Math.floor(sec / 3600)}h ago`;
  return `${Math.floor(sec / 86400)}d ago`;
}

function shortNode(id?: string | null): string {
  if (!id) return 'unknown';
  return `${id.slice(0, 8)}…${id.slice(-6)}`;
}

function packetSummary(packet: FeedPacket): string {
  if (typeof packet.summary === 'string' && packet.summary.trim()) return packet.summary.trim();
  const payload = packet.payload ?? {};
  const appData = payload['appData'] as Record<string, unknown> | undefined;
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

function packetObserverIatas(packet: FeedPacket, nodeMap: Map<string, MeshNode>): string[] {
  const topicIata = packetTopicIata(packet);
  if (topicIata) return [topicIata];

  const values = new Set<string>();
  for (const observerId of packetObserverIds(packet)) {
    const iata = String(nodeMap.get(observerId)?.iata ?? '').trim().toUpperCase();
    if (iata) values.add(iata);
  }
  return Array.from(values);
}

export const UKFeedPage: React.FC = () => {
  const site = getCurrentSite();
  const scope = useMemo(() => ({ network: site.networkFilter, observer: site.observerId }), [site.networkFilter, site.observerId]);
  const [selectedIata, setSelectedIata] = useState<string>('all');
  const [messagesOnly, setMessagesOnly] = useState<boolean>(false);
  const [regionOptions, setRegionOptions] = useState<string[]>([]);
  
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
      packet_type: p.packetType ?? null,
      hop_count: p.hopCount ?? null,
      rssi: null,
      snr: null,
      payload: p as any,
      observer_node_ids: p.observerIds,
      rx_count: p.rxCount,
      tx_count: p.txCount,
      summary: p.summary ?? null,
    }));
  }, [packetsList]);

  const availableIatas = useMemo(() => {
    return regionOptions;
  }, [regionOptions]);

  useEffect(() => {
    if (selectedIata === 'all') return;
    if (!availableIatas.includes(selectedIata)) {
      setSelectedIata('all');
    }
  }, [availableIatas, selectedIata]);

  const filteredPackets = useMemo(() => {
    let result = packets;
    if (selectedIata !== 'all') {
      result = result.filter((packet) => packetObserverIatas(packet, nodeMap).includes(selectedIata));
    }
    if (messagesOnly) {
      result = result.filter((packet) => packet.packet_type === 2 || packet.packet_type === 5);
    }
    return result;
  }, [messagesOnly, nodeMap, packets, selectedIata]);

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

  const latestPacket = filteredPackets[0];
  const latestObserver = latestPacket?.rx_node_id ? nodeMap.get(latestPacket.rx_node_id) : undefined;
  const recentPackets = useMemo(() => filteredPackets.slice(0, 20), [filteredPackets]);

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

      <div className="site-content site-prose">
        <section className="prose-section">
          <div className="dev-status-page uk-feed-page">
            <div className="dev-status-grid uk-feed-grid">
              <section className="dev-status-card">
                <h2>Status</h2>
                <div className="dev-status-list">
                  <div>
                    <span>Region</span>
                    <strong className="uk-feed-region-control">
                      <select
                        className="uk-feed-region-select"
                        value={selectedIata}
                        onChange={(event) => setSelectedIata(event.target.value)}
                        aria-label="Filter feed by observer region"
                      >
                        <option value="all">All regions</option>
                        {availableIatas.map((iata) => (
                          <option key={iata} value={iata}>{iata}</option>
                        ))}
                      </select>
                      <span className="uk-feed-region-caret" aria-hidden="true">v</span>
                    </strong>
                  </div>
                  <div>
                    <span>Messages only</span>
                    <strong>
                      <label className="uk-feed-checkbox-label">
                        <input
                          type="checkbox"
                          checked={messagesOnly}
                          onChange={(e) => setMessagesOnly(e.target.checked)}
                        />
                      </label>
                    </strong>
                  </div>
                  <div><span>Feed</span><strong>{latestPacket ? 'Receiving packets' : 'Waiting for packets'}</strong></div>
                  <div><span>Observers active</span><strong>{activeObserverCount.toLocaleString()}</strong></div>
                  <div><span>Last packet</span><strong>{latestPacket ? timeAgo(latestPacket.time) : 'never'}</strong></div>
                </div>
              </section>

              <section className="dev-status-card">
                <h2>Latest packet</h2>
                <div className="dev-status-list">
                  <div><span>Observer</span><strong>{latestObserver?.name ?? shortNode(latestPacket?.rx_node_id)}</strong></div>
                  <div><span>Type</span><strong>{latestPacket?.packet_type != null ? (TYPE_LABELS[latestPacket.packet_type] ?? `T${latestPacket.packet_type}`) : '—'}</strong></div>
                  <div><span>Signal</span><strong>{latestPacket?.rssi != null || latestPacket?.snr != null ? `${latestPacket.rssi ?? '—'} / ${latestPacket.snr ?? '—'}` : '—'}</strong></div>
                  <div><span>Hash</span><strong className="dev-status-mono">{latestPacket?.packet_hash ?? '—'}</strong></div>
                </div>
                <p className="dev-status-note">{latestPacket ? packetSummary(latestPacket) : 'No public packets have arrived yet.'}</p>
              </section>
            </div>

            <section className="dev-status-card uk-feed-packets-card">
              <h2>Live packets</h2>
              <div className="uk-feed-packets-list">
                {recentPackets.length > 0 ? recentPackets.map((packet) => {
                  const iatas = packetObserverIatas(packet, nodeMap);
                  const observerDisplay = iatas.length === 0 ? 'unknown' : iatas.join(' · ');
                  return (
                    <article className="uk-feed-packet-row" key={`${packet.packet_hash}-${packet.time}`}>
                      <div className="uk-feed-packet-row__meta">
                        <span>{new Date(packet.time).toLocaleTimeString()}</span>
                        <span>{packet.packet_type != null ? (TYPE_LABELS[packet.packet_type] ?? `T${packet.packet_type}`) : '—'}</span>
                        <span>{packet.hop_count != null ? `${packet.hop_count} hop${packet.hop_count !== 1 ? 's' : ''}` : '—'}</span>
                        <span>{packet.rssi != null || packet.snr != null ? `${packet.rssi ?? '—'} / ${packet.snr ?? '—'}` : '—'}</span>
                        <span className="dev-status-mono">{packet.packet_hash}</span>
                        <span className="uk-feed-packet-row__observer">{observerDisplay}</span>
                      </div>
                      <p className="uk-feed-packet-row__summary">{packetSummary(packet)}</p>
                    </article>
                  );
                }) : (
                  <p className="dev-status-empty">No public packets have arrived yet.</p>
                )}
              </div>
            </section>
          </div>
        </section>
      </div>
    </>
  );
};
