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
/** Decrypted/visible channel scopes — matches the [Channel] summary prefix (lowercased).
 * Kept in sync with backend/src/mqtt/channelRegistry.ts VALIDATED_CHANNELS. */
export const MESSAGE_SCOPE_CHANNELS = [
  'public',
  'test',
  'bot',
  'yorkshire',
  'liverpool',
  'london',
  'nottingham',
  'northeast',
  'kent',
  'wales',
  'cornwall',
  'devon',
  'dorset',
  'cumbria',
  'yorks',
  'leicester',
  'dartford',
  'uckfield',
  'huddersfield',
  'derbyshire',
  'lincolnshire',
  'surrey',
  'midlands',
  'hamradio',
  'mesh',
  'public2',
  'test2',
  'thenorf',
  'g8py',
  'echo',
  'denhaag',
  'dublin',
  'glasgow',
  'york',
  'ireland',
  'scilly',
  'brentwood',
  'marple',
  'uk',
] as const;
export type MessageScope = 'all' | (typeof MESSAGE_SCOPE_CHANNELS)[number];
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

function finiteTimestamp(value: string | undefined): number | null {
  if (!value) return null;
  const parsed = Date.parse(value);
  return Number.isFinite(parsed) ? parsed : null;
}

function mergedStrings(values: Array<string | null | undefined>): string[] {
  return Array.from(new Set(values.map((value) => String(value ?? '').trim()).filter(Boolean)));
}

function mergedIatas(values: Array<string | null | undefined>): string[] {
  const normalized = values
    .map((value) => String(value ?? '').trim().toUpperCase())
    .filter((value) => /^[A-Z0-9]{2,8}$/.test(value));
  return Array.from(new Set(normalized));
}

export function mergeFeedPacketObservations(current: FeedPacket, next: FeedPacket): FeedPacket {
  const currentTime = finiteTimestamp(current.time);
  const nextTime = finiteTimestamp(next.time);
  const latest = nextTime != null && (currentTime == null || nextTime >= currentTime) ? next : current;
  const firstSeenCandidates = [
    finiteTimestamp(current.first_seen_time ?? current.time),
    finiteTimestamp(next.first_seen_time ?? next.time),
  ].filter((value): value is number => value != null);
  const observerIatas = mergedIatas([
    ...(current.observer_iatas ?? []),
    current.iata,
    ...(next.observer_iatas ?? []),
    next.iata,
  ]);
  return {
    ...latest,
    first_seen_time: firstSeenCandidates.length > 0
      ? new Date(Math.min(...firstSeenCandidates)).toISOString()
      : (latest.first_seen_time ?? latest.time),
    observer_node_ids: mergedStrings([
      ...(current.observer_node_ids ?? []),
      current.rx_node_id,
      ...(next.observer_node_ids ?? []),
      next.rx_node_id,
    ]),
    observer_iatas: observerIatas,
    iata: mergedIatas([latest.iata])[0] ?? observerIatas[0] ?? null,
    rx_count: Math.max(current.rx_count ?? 0, next.rx_count ?? 0),
    tx_count: Math.max(current.tx_count ?? 0, next.tx_count ?? 0),
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
