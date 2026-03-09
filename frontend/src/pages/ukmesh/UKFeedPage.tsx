import React, { useEffect, useMemo, useState } from 'react';
import { getCurrentSite } from '../../config/site.js';
import { uncachedEndpoint, withScopeParams } from '../../utils/api.js';

type FeedNode = {
  node_id: string;
  name?: string | null;
  iata?: string | null;
  lat?: number | null;
  lon?: number | null;
  last_seen?: string | null;
  is_online?: boolean;
};

type FeedPacket = {
  time: string;
  packet_hash: string;
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

export const UKFeedPage: React.FC = () => {
  const site = getCurrentSite();
  const scope = useMemo(() => ({ network: site.networkFilter, observer: site.observerId }), [site.networkFilter, site.observerId]);
  const [nodes, setNodes] = useState<FeedNode[]>([]);
  const [packets, setPackets] = useState<FeedPacket[]>([]);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    let cancelled = false;

    const load = async () => {
      try {
        const [nodesRes, packetsRes] = await Promise.all([
          fetch(uncachedEndpoint(withScopeParams('/api/nodes', scope)), { cache: 'no-store' }),
          fetch(uncachedEndpoint(withScopeParams('/api/packets/recent?limit=100', scope)), { cache: 'no-store' }),
        ]);

        if (!nodesRes.ok || !packetsRes.ok) {
          throw new Error(`HTTP ${nodesRes.status}/${packetsRes.status}`);
        }

        const [nodesJson, packetsJson] = await Promise.all([
          nodesRes.json() as Promise<FeedNode[]>,
          packetsRes.json() as Promise<FeedPacket[]>,
        ]);

        if (cancelled) return;
        setNodes(nodesJson);
        setPackets(packetsJson);
        setError(null);
      } catch (err) {
        if (cancelled) return;
        setError((err as Error).message);
      }
    };

    void load();
    const timer = setInterval(() => void load(), 5 * 60_000);
    return () => {
      cancelled = true;
      clearInterval(timer);
    };
  }, [scope]);

  const nodeMap = useMemo(() => new Map(nodes.map((node) => [node.node_id, node])), [nodes]);

  const activeObserverCount = useMemo(() => {
    const ids = new Set<string>();
    for (const packet of packets) {
      const observerIds = packet.observer_node_ids?.length
        ? packet.observer_node_ids
        : (packet.rx_node_id ? [packet.rx_node_id] : []);
      for (const observerId of observerIds) {
        if (observerId) ids.add(observerId);
      }
    }
    return ids.size;
  }, [packets]);

  const latestPacket = packets[0];
  const latestObserver = latestPacket?.rx_node_id ? nodeMap.get(latestPacket.rx_node_id) : undefined;
  const recentPackets = useMemo(() => packets.slice(0, 10), [packets]);

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
        {error && <p className="prose-note">Feed API error: {error}</p>}

        <section className="prose-section">
          <div className="dev-status-page uk-feed-page">
            <div className="dev-status-grid uk-feed-grid">
              <section className="dev-status-card">
                <h2>Status</h2>
                <div className="dev-status-list">
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
                {recentPackets.length > 0 ? recentPackets.map((packet) => (
                  <article className="uk-feed-packet-row" key={`${packet.packet_hash}-${packet.time}`}>
                    <div className="uk-feed-packet-row__meta">
                      <span>{new Date(packet.time).toLocaleTimeString()}</span>
                      <span>{packet.packet_type != null ? (TYPE_LABELS[packet.packet_type] ?? `T${packet.packet_type}`) : '—'}</span>
                      <span>{packet.rssi != null || packet.snr != null ? `${packet.rssi ?? '—'} / ${packet.snr ?? '—'}` : '—'}</span>
                      <span className="dev-status-mono">{packet.packet_hash}</span>
                      <span>{packet.rx_node_id ? (nodeMap.get(packet.rx_node_id)?.iata ?? shortNode(packet.rx_node_id)) : 'unknown'}</span>
                    </div>
                    <p className="uk-feed-packet-row__summary">{packetSummary(packet)}</p>
                  </article>
                )) : (
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
