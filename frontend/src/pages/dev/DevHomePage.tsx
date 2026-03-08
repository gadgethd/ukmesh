import React, { useEffect, useMemo, useState } from 'react';
import { getCurrentSite } from '../../config/site.js';
import { uncachedEndpoint, withScopeParams } from '../../utils/api.js';

type DevNode = {
  node_id: string;
  name?: string;
  iata?: string;
  last_seen: string;
  is_online: boolean;
  hardware_model?: string;
  public_key?: string;
};

type DevPacket = {
  time: string;
  packet_hash: string;
  rx_node_id?: string;
  src_node_id?: string;
  packet_type?: number;
  hop_count?: number;
  rssi?: number;
  snr?: number;
  payload?: Record<string, unknown>;
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

function shortNode(id?: string): string {
  if (!id) return 'unknown';
  return `${id.slice(0, 8)}…${id.slice(-6)}`;
}

function packetSummary(packet: DevPacket): string {
  const payload = packet.payload ?? {};
  const appData = payload['appData'] as Record<string, unknown> | undefined;
  const candidate = [
    typeof appData?.['name'] === 'string' ? appData['name'] : undefined,
    typeof payload['origin'] === 'string' ? payload['origin'] : undefined,
    typeof appData?.['text'] === 'string' ? appData['text'] : undefined,
    typeof payload['summary'] === 'string' ? payload['summary'] : undefined,
  ].find(Boolean);
  return String(candidate ?? 'No decoded summary');
}

export const DevHomePage: React.FC = () => {
  const site = getCurrentSite();
  const scope = useMemo(() => ({ network: site.networkFilter, observer: site.observerId }), [site.networkFilter, site.observerId]);
  const [nodes, setNodes] = useState<DevNode[]>([]);
  const [packets, setPackets] = useState<DevPacket[]>([]);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    let cancelled = false;

    const load = async () => {
      try {
        const [nodesRes, packetsRes] = await Promise.all([
          fetch(uncachedEndpoint(withScopeParams('/api/nodes', scope)), { cache: 'no-store' }),
          fetch(uncachedEndpoint(withScopeParams('/api/packets/recent?limit=15&raw=1', scope)), { cache: 'no-store' }),
        ]);

        if (!nodesRes.ok || !packetsRes.ok) {
          throw new Error(`HTTP ${nodesRes.status}/${packetsRes.status}`);
        }

        const [nodesJson, packetsJson] = await Promise.all([
          nodesRes.json() as Promise<DevNode[]>,
          packetsRes.json() as Promise<DevPacket[]>,
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
    const timer = setInterval(() => void load(), 3000);
    return () => {
      cancelled = true;
      clearInterval(timer);
    };
  }, [scope]);

  const nodeMap = useMemo(() => new Map(nodes.map((node) => [node.node_id, node])), [nodes]);
  const observerIds = useMemo(() => {
    const ids = new Set<string>();
    for (const packet of packets) {
      if (packet.rx_node_id) ids.add(packet.rx_node_id);
    }
    return [...ids];
  }, [packets]);

  const latestPacket = packets[0];
  const latestObserver = latestPacket?.rx_node_id ? nodeMap.get(latestPacket.rx_node_id) : undefined;
  const recentPackets = useMemo(() => packets.slice(0, 15), [packets]);

  return (
    <section className="dev-status-page">
      <div className="dev-status-page__header">
        <h1>{site.displayName}</h1>
        <p>Isolated MQTT test feed status.</p>
        <span className="dev-status-page__last-seen">
          Last seen: {latestPacket ? timeAgo(latestPacket.time) : 'never'}
        </span>
      </div>

      {error && <p className="prose-note">Diagnostics fetch error: {error}</p>}

      <div className="dev-status-grid">
        <section className="dev-status-card">
          <h2>Status</h2>
          <div className="dev-status-list">
            <div><span>Feed</span><strong>{latestPacket ? 'Receiving packets' : 'Waiting for packets'}</strong></div>
            <div><span>Observers seen</span><strong>{observerIds.length.toLocaleString()}</strong></div>
            <div><span>Last packet</span><strong>{latestPacket ? timeAgo(latestPacket.time) : 'never'}</strong></div>
            <div><span>Isolation</span><strong>network=test</strong></div>
          </div>
        </section>

        <section className="dev-status-card">
          <h2>Latest packet</h2>
          <div className="dev-status-list">
            <div><span>Observer</span><strong>{latestObserver?.name ?? 'Unknown observer'}</strong></div>
            <div><span>Type</span><strong>{latestPacket?.packet_type !== undefined ? (TYPE_LABELS[latestPacket.packet_type] ?? `T${latestPacket.packet_type}`) : '—'}</strong></div>
            <div><span>Signal</span><strong>{latestPacket?.rssi !== undefined || latestPacket?.snr !== undefined ? `${latestPacket.rssi ?? '—'} / ${latestPacket.snr ?? '—'}` : '—'}</strong></div>
            <div><span>Hash</span><strong className="dev-status-mono">{latestPacket?.packet_hash ?? '—'}</strong></div>
          </div>
          <p className="dev-status-note">{latestPacket ? packetSummary(latestPacket) : 'No test packets have arrived yet.'}</p>
        </section>
      </div>

      <section className="dev-status-card dev-status-card--wide dev-status-card--fixed">
        <h2>Live packets</h2>
        <div className="dev-status-table-wrap">
          <table className="dev-status-table">
            <thead>
              <tr>
                <th>Time</th>
                <th>Type</th>
                <th>Observer</th>
                <th>Signal</th>
                <th>Hash</th>
                <th>Summary</th>
              </tr>
            </thead>
            <tbody>
              {recentPackets.length > 0 ? recentPackets.map((packet) => (
                <tr key={`${packet.packet_hash}-${packet.time}`}>
                  <td>{new Date(packet.time).toLocaleTimeString()}</td>
                  <td>{packet.packet_type !== undefined ? (TYPE_LABELS[packet.packet_type] ?? `T${packet.packet_type}`) : '—'}</td>
                  <td>{packet.rx_node_id ? (nodeMap.get(packet.rx_node_id)?.name ?? shortNode(packet.rx_node_id)) : 'unknown'}</td>
                  <td>{packet.rssi !== undefined || packet.snr !== undefined ? `${packet.rssi ?? '—'} / ${packet.snr ?? '—'}` : '—'}</td>
                  <td className="dev-status-mono">{packet.packet_hash}</td>
                  <td>{packetSummary(packet)}</td>
                </tr>
              )) : (
                <tr>
                  <td colSpan={6} className="dev-status-empty">No test packets have arrived yet.</td>
                </tr>
              )}
            </tbody>
          </table>
        </div>
      </section>
    </section>
  );
};
