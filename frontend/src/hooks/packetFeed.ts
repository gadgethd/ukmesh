import type { AggregatedPacket, LivePacketData, MeshNode } from './useNodes.js';
import { canonicalNodeId, canonicalOptionalNodeId } from '../utils/nodeIds.js';

export const FEED_MAX_PACKETS = 50;
export const FEED_MAX_MESSAGES = 200;

export type RecentPacketRow = {
  time: string;
  packet_hash: string;
  rx_node_id?: string;
  observer_node_ids?: string[] | null;
  iata?: string | null;
  observer_iatas?: Array<string | null> | null;
  src_node_id?: string;
  topic?: string;
  packet_type?: number;
  hop_count?: number;
  path_hash_size_bytes?: number;
  summary?: string | null;
  payload?: Record<string, unknown>;
  advert_count?: number | null;
  path_hashes?: string[] | null;
  rx_count?: number | null;
  tx_count?: number | null;
};

export function normalizeIatas(values: Array<string | null | undefined>): string[] {
  const normalized = new Set<string>();
  for (const value of values) {
    const iata = String(value ?? '').trim().toUpperCase();
    if (/^[A-Z0-9]{2,8}$/.test(iata)) normalized.add(iata);
  }
  return Array.from(normalized);
}

export function packetRowObserverIatas(row: RecentPacketRow): string[] {
  const explicit = normalizeIatas([...(row.observer_iatas ?? []), row.iata]);
  if (explicit.length > 0) return explicit;
  const topicIata = String(row.topic ?? '').split('/')[1];
  return normalizeIatas([topicIata]);
}

export function aggregatedPacketObserverIataLabel(
  packet: Pick<AggregatedPacket, 'rxNodeId'> & Partial<Pick<AggregatedPacket, 'observerIatas'>>,
  nodeMap: Map<string, Pick<MeshNode, 'iata'>>,
): string | undefined {
  const explicit = normalizeIatas(packet.observerIatas ?? []);
  if (explicit.length > 0) return explicit.join(' · ');
  const fallback = packet.rxNodeId ? nodeMap.get(packet.rxNodeId)?.iata : undefined;
  return normalizeIatas([fallback])[0];
}

export function extractPacketSummary(payload?: Record<string, unknown>): string | undefined {
  if (!payload) return undefined;
  const persisted = payload['_summary'];
  if (typeof persisted === 'string' && persisted.trim() !== '') return persisted;
  const appData = payload['appData'] as Record<string, unknown> | undefined;
  if (typeof appData?.['name'] === 'string') return appData['name'];

  const decrypted = payload['decrypted'] as Record<string, unknown> | undefined;
  if (decrypted) {
    const sender = typeof decrypted['sender'] === 'string' ? decrypted['sender'] : undefined;
    const message = typeof decrypted['message'] === 'string' ? decrypted['message'] : undefined;
    if (sender && message) return `${sender}: ${message}`;
    if (message) return message;
  }

  if (typeof payload['checksum'] === 'string') return `ACK ${(payload['checksum'] as string).slice(0, 4)}`;
  if (typeof payload['pathLength'] === 'number') return `${payload['pathLength']} hop path`;
  const pathHashes = payload['pathHashes'];
  if (Array.isArray(pathHashes)) return `trace ${pathHashes.length} hops`;

  return undefined;
}

export function packetInfoScore(packet: Pick<AggregatedPacket, 'packetType' | 'srcNodeId' | 'summary' | 'hopCount' | 'path' | 'advertCount'>): number {
  let score = 0;
  if (packet.summary) score += 4;
  if (packet.srcNodeId) score += 3;
  if (packet.packetType === 4) score += 2;
  else if (packet.packetType !== undefined) score += 1;
  if (packet.hopCount !== undefined) score += 1;
  if (packet.path && packet.path.length > 0) score += 1;
  if ((packet.advertCount ?? 0) > 0) score += 1;
  return score;
}

export function mergeAggregatedPacket(current: AggregatedPacket, next: AggregatedPacket): AggregatedPacket {
  const mergedCandidate: AggregatedPacket = {
    ...current,
    firstSeenTs: Math.min(current.firstSeenTs ?? current.ts, next.firstSeenTs ?? next.ts),
    packetType: next.packetType ?? current.packetType,
    rxNodeId: next.rxNodeId ?? current.rxNodeId,
    observerIds: Array.from(new Set([...current.observerIds, ...next.observerIds])),
    observerIatas: normalizeIatas([...current.observerIatas, ...next.observerIatas]),
    srcNodeId: next.srcNodeId ?? current.srcNodeId,
    summary: next.summary ?? current.summary,
    hopCount: next.hopCount ?? current.hopCount,
    pathHashSizeBytes: next.pathHashSizeBytes ?? current.pathHashSizeBytes,
    path: next.path ?? current.path,
    rxCount: Math.max(current.rxCount, next.rxCount),
    txCount: Math.max(current.txCount, next.txCount),
    ts: Math.max(current.ts, next.ts),
    advertCount: Math.max(current.advertCount ?? 0, next.advertCount ?? 0) || undefined,
  };

  if (packetInfoScore(mergedCandidate) >= packetInfoScore(current)) return mergedCandidate;
  return {
    ...current,
    firstSeenTs: Math.min(current.firstSeenTs ?? current.ts, next.firstSeenTs ?? next.ts),
    observerIds: Array.from(new Set([...current.observerIds, ...next.observerIds])),
    observerIatas: normalizeIatas([...current.observerIatas, ...next.observerIatas]),
    rxCount: Math.max(current.rxCount, next.rxCount),
    txCount: Math.max(current.txCount, next.txCount),
    ts: Math.max(current.ts, next.ts),
    pathHashSizeBytes: next.pathHashSizeBytes ?? current.pathHashSizeBytes,
    advertCount: Math.max(current.advertCount ?? 0, next.advertCount ?? 0) || undefined,
  };
}

export function mapRecentRows(rows: RecentPacketRow[]): AggregatedPacket[] {
  const mapped = new Map<string, AggregatedPacket>();
  for (const row of rows) {
    const summary = row.summary ?? extractPacketSummary(row.payload);
    const observerIds = Array.from(new Set([
      ...(row.observer_node_ids ?? []),
      ...(row.rx_node_id ? [row.rx_node_id] : []),
    ].map(canonicalNodeId).filter(Boolean)));
    const packetHash = row.packet_hash.trim().toUpperCase();
    const next: AggregatedPacket = {
      id: packetHash,
      packetHash,
      packetType: row.packet_type,
      firstSeenTs: new Date(row.time).getTime(),
      rxNodeId: canonicalOptionalNodeId(row.rx_node_id),
      observerIds,
      observerIatas: packetRowObserverIatas(row),
      srcNodeId: canonicalOptionalNodeId(row.src_node_id),
      topic: row.topic,
      summary,
      hopCount: row.hop_count,
      pathHashSizeBytes: row.path_hash_size_bytes ?? undefined,
      path: row.path_hashes?.map(canonicalNodeId).filter(Boolean),
      rxCount: Number(row.rx_count ?? 1),
      txCount: Number(row.tx_count ?? 0),
      ts: new Date(row.time).getTime(),
      advertCount: row.advert_count ?? undefined,
    };
    const current = mapped.get(packetHash);
    mapped.set(packetHash, current ? mergeAggregatedPacket(current, next) : next);
  }
  return Array.from(mapped.values())
    .sort((a, b) => b.ts - a.ts)
    .slice(0, FEED_MAX_PACKETS);
}

export function mapMessageRows(rows: RecentPacketRow[]): AggregatedPacket[] {
  const type5 = rows.filter((r) => r.packet_type === 5);
  const mapped = new Map<string, AggregatedPacket>();
  for (const row of type5) {
    const summary = row.summary ?? extractPacketSummary(row.payload);
    const observerIds = Array.from(new Set([
      ...(row.observer_node_ids ?? []),
      ...(row.rx_node_id ? [row.rx_node_id] : []),
    ].map(canonicalNodeId).filter(Boolean)));
    const packetHash = row.packet_hash.trim().toUpperCase();
    const next: AggregatedPacket = {
      id: packetHash,
      packetHash,
      packetType: row.packet_type,
      firstSeenTs: new Date(row.time).getTime(),
      rxNodeId: canonicalOptionalNodeId(row.rx_node_id),
      observerIds,
      observerIatas: packetRowObserverIatas(row),
      srcNodeId: canonicalOptionalNodeId(row.src_node_id),
      topic: row.topic,
      summary,
      hopCount: row.hop_count,
      pathHashSizeBytes: row.path_hash_size_bytes ?? undefined,
      path: row.path_hashes?.map(canonicalNodeId).filter(Boolean),
      rxCount: Number(row.rx_count ?? 1),
      txCount: Number(row.tx_count ?? 0),
      ts: new Date(row.time).getTime(),
      advertCount: row.advert_count ?? undefined,
    };
    const current = mapped.get(packetHash);
    mapped.set(packetHash, current ? mergeAggregatedPacket(current, next) : next);
  }
  return Array.from(mapped.values())
    .sort((a, b) => b.ts - a.ts)
    .slice(0, FEED_MAX_MESSAGES);
}

/** Merge two message lists (type=5 only). Client-side messages take priority so
 *  a WS reconnect with a stale server cache doesn't wipe recently received messages. */
export function mergeMessages(clientMessages: AggregatedPacket[], serverMessages: AggregatedPacket[]): AggregatedPacket[] {
  const merged = new Map<string, AggregatedPacket>();
  for (const m of serverMessages) merged.set(m.packetHash, m);
  for (const m of clientMessages) {
    const existing = merged.get(m.packetHash);
    merged.set(m.packetHash, existing ? mergeAggregatedPacket(existing, m) : m);
  }
  return Array.from(merged.values())
    .sort((a, b) => b.ts - a.ts)
    .slice(0, FEED_MAX_MESSAGES);
}

export function mergePackets(existing: AggregatedPacket[], incoming: AggregatedPacket[]): AggregatedPacket[] {
  const merged = new Map<string, AggregatedPacket>();
  for (const packet of existing) merged.set(packet.packetHash, packet);
  for (const next of incoming) {
    const current = merged.get(next.packetHash);
    merged.set(next.packetHash, current ? mergeAggregatedPacket(current, next) : next);
  }
  return Array.from(merged.values())
    .sort((a, b) => b.ts - a.ts)
    .slice(0, FEED_MAX_PACKETS);
}

export function createAggregatedPacketFromLive(packet: LivePacketData): AggregatedPacket {
  return {
    id: packet.id,
    packetHash: packet.packetHash,
    packetType: packet.packetType,
    firstSeenTs: packet.ts,
    rxNodeId: packet.rxNodeId,
    observerIds: packet.rxNodeId ? [packet.rxNodeId] : [],
    observerIatas: normalizeIatas([packet.iata]),
    srcNodeId: packet.srcNodeId,
    topic: packet.topic,
    summary: packet.summary ?? extractPacketSummary(packet.payload),
    hopCount: packet.hopCount,
    pathHashSizeBytes: packet.pathHashSizeBytes,
    path: packet.path,
    rxCount: packet.direction !== 'tx' ? 1 : 0,
    txCount: packet.direction === 'tx' ? 1 : 0,
    ts: packet.ts,
    advertCount: packet.advertCount,
  };
}
