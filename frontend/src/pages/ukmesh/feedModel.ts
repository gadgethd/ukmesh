import type { AggregatedPacket, MeshNode } from '../../hooks/useNodes.js';
import { ScopedCache } from '../../utils/scopedCache.js';
import type { LazyPathNode, LazyPathResult } from './PacketDetailPanel.js';

export type FeedPacket = {
  time: string;
  first_seen_time?: string;
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
  iata?: string | null;
  observer_iatas?: string[];
  rx_count?: number;
  tx_count?: number;
  summary?: string | null;
  path_hashes?: string[] | null;
};

export const TYPE_LABELS: Record<number, string> = {
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

export const MAX_PACKETS = 500;
export type MessageScope = 'all' | 'public' | 'test';
export type PathTreeBranchNode = LazyPathNode & {
  treeKey: string;
  branchIndexes: Set<number>;
  children: PathTreeBranchNode[];
};

export const LAZY_SETTLE_MS = 10_000;
export const PATH_REQUEST_TIMEOUT_MS = 15_000;
export const FEED_PATH_MAX_CONCURRENCY = 1;
export const feedPathCache = new ScopedCache<LazyPathResult>({
  name: 'feed-selected-paths',
  ttlMs: 5 * 60_000,
  maxEntries: 128,
  maxBytes: 16 * 1024 * 1024,
  maxInflight: FEED_PATH_MAX_CONCURRENCY,
});

export function packetObserverIds(packet: FeedPacket): string[] {
  return packet.observer_node_ids?.length
    ? packet.observer_node_ids.filter(Boolean)
    : (packet.rx_node_id ? [packet.rx_node_id] : []);
}

export function feedPathCacheKey(packet: FeedPacket): string {
  const observers = packetObserverIds(packet).map((id) => id.toUpperCase()).sort().join(',');
  return `${packet.packet_hash.toUpperCase()}:${observers}`;
}

export function timeAgo(ts?: string | null): string {
  if (!ts) return 'never';
  const ageMs = Math.max(0, Date.now() - Date.parse(ts));
  const seconds = Math.floor(ageMs / 1000);
  if (seconds < 60) return `${seconds}s ago`;
  if (seconds < 3600) return `${Math.floor(seconds / 60)}m ago`;
  if (seconds < 86400) return `${Math.floor(seconds / 3600)}h ago`;
  return `${Math.floor(seconds / 86400)}d ago`;
}

export function packetSummary(packet: FeedPacket, nodeMap?: Map<string, MeshNode>): string {
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
    const count = Array.isArray(payload['pathHashes'])
      ? payload['pathHashes'].length
      : (packet.path_hashes?.length ?? null);
    return count != null ? `Path trace (${count} hops)` : 'Path trace';
  }
  if (type === 9) return 'Trace';
  if (type === 0) return 'Request';
  if (type === 1) return 'Response';
  if (type === 2) {
    const source = packet.src_node_id ? nodeMap?.get(packet.src_node_id) : undefined;
    const sourceName = source?.name ?? (packet.src_node_id ? `${packet.src_node_id.slice(0, 8)}…` : null);
    return sourceName ? `Encrypted DM from ${sourceName}` : 'Encrypted direct message';
  }
  if (type === 5) {
    const candidate = [
      typeof appData?.['text'] === 'string' ? appData['text'] : undefined,
      typeof payload['summary'] === 'string' ? payload['summary'] : undefined,
    ].find((value) => typeof value === 'string' && value.trim());
    return String(candidate ?? 'Group message');
  }

  const candidate = [
    typeof appData?.['name'] === 'string' ? appData['name'] : undefined,
    typeof appData?.['text'] === 'string' ? appData['text'] : undefined,
    typeof payload['summary'] === 'string' ? payload['summary'] : undefined,
  ].find((value) => typeof value === 'string' && value.trim());
  return String(candidate ?? 'No decoded summary');
}

export function packetMatchesMessageScope(packet: FeedPacket, scope: MessageScope): boolean {
  if (scope === 'all') return true;
  const channel = packetChannel(packet)?.trim().toLowerCase();
  return Boolean(channel && channel === scope);
}

function packetChannel(packet: FeedPacket): string | null {
  const summary = typeof packet.summary === 'string' ? packet.summary.trim() : null;
  if (!summary?.startsWith('[')) return null;
  const end = summary.indexOf(']');
  if (end <= 1) return null;
  const name = summary.slice(1, end);
  return name.toLowerCase().includes('encrypt') ? null : name;
}

function packetTopicIata(packet: FeedPacket): string | null {
  const topic = String(packet.payload?.topic ?? packet.topic ?? '').trim();
  if (!topic) return null;
  const parts = topic.split('/');
  if (parts.length < 2) return null;
  const iata = String(parts[1] ?? '').trim().toUpperCase();
  return /^[A-Z0-9]{2,8}$/.test(iata) ? iata : null;
}

export function aggregatedPacketToFeedPacket(packet: AggregatedPacket): FeedPacket {
  return {
    time: new Date(packet.ts).toISOString(),
    first_seen_time: new Date(packet.firstSeenTs ?? packet.ts).toISOString(),
    packet_hash: packet.packetHash,
    rx_node_id: packet.rxNodeId ?? null,
    src_node_id: packet.srcNodeId ?? null,
    topic: packet.topic,
    packet_type: packet.packetType ?? null,
    hop_count: packet.hopCount ?? null,
    rssi: null,
    snr: null,
    payload: packet as unknown as Record<string, unknown>,
    observer_node_ids: packet.observerIds,
    iata: packet.observerIatas[0] ?? null,
    observer_iatas: packet.observerIatas,
    rx_count: packet.rxCount,
    tx_count: packet.txCount,
    summary: packet.summary ?? null,
    path_hashes: (packet.path as string[] | undefined) ?? null,
  };
}

export function packetObserverIatas(packet: FeedPacket, nodeMap: Map<string, MeshNode>): string[] {
  const values = new Set<string>();
  for (const value of [...(packet.observer_iatas ?? []), packet.iata]) {
    const iata = String(value ?? '').trim().toUpperCase();
    if (/^[A-Z0-9]{2,8}$/.test(iata)) values.add(iata);
  }
  if (values.size > 0) return Array.from(values);
  for (const observerId of packetObserverIds(packet)) {
    const iata = nodeMap.get(observerId)?.iata?.trim().toUpperCase();
    if (iata) values.add(iata);
  }
  if (values.size > 0) return Array.from(values);
  const topicIata = packetTopicIata(packet);
  return topicIata ? [topicIata] : [];
}
