import type { AggregatedPacket, LivePacketData } from './useNodes.js';

export const FEED_MAX_PACKETS = 12;

export type RecentPacketRow = {
  time: string;
  packet_hash: string;
  rx_node_id?: string;
  observer_node_ids?: string[] | null;
  src_node_id?: string;
  packet_type?: number;
  hop_count?: number;
  summary?: string | null;
  payload?: Record<string, unknown>;
  advert_count?: number | null;
  path_hashes?: string[] | null;
  rx_count?: number | null;
  tx_count?: number | null;
};

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
    packetType: next.packetType ?? current.packetType,
    rxNodeId: next.rxNodeId ?? current.rxNodeId,
    observerIds: Array.from(new Set([...current.observerIds, ...next.observerIds])),
    srcNodeId: next.srcNodeId ?? current.srcNodeId,
    summary: next.summary ?? current.summary,
    hopCount: next.hopCount ?? current.hopCount,
    path: next.path ?? current.path,
    rxCount: Math.max(current.rxCount, next.rxCount),
    txCount: Math.max(current.txCount, next.txCount),
    ts: Math.max(current.ts, next.ts),
    advertCount: Math.max(current.advertCount ?? 0, next.advertCount ?? 0) || undefined,
  };

  if (packetInfoScore(mergedCandidate) >= packetInfoScore(current)) return mergedCandidate;
  return {
    ...current,
    observerIds: Array.from(new Set([...current.observerIds, ...next.observerIds])),
    rxCount: Math.max(current.rxCount, next.rxCount),
    txCount: Math.max(current.txCount, next.txCount),
    ts: Math.max(current.ts, next.ts),
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
    ]));
    const next: AggregatedPacket = {
      id: row.packet_hash,
      packetHash: row.packet_hash,
      packetType: row.packet_type,
      rxNodeId: row.rx_node_id,
      observerIds,
      srcNodeId: row.src_node_id,
      summary,
      hopCount: row.hop_count,
      path: row.path_hashes ?? undefined,
      rxCount: Number(row.rx_count ?? 1),
      txCount: Number(row.tx_count ?? 0),
      ts: new Date(row.time).getTime(),
      advertCount: row.advert_count ?? undefined,
    };
    const current = mapped.get(row.packet_hash);
    mapped.set(row.packet_hash, current ? mergeAggregatedPacket(current, next) : next);
  }
  return Array.from(mapped.values())
    .sort((a, b) => b.ts - a.ts)
    .slice(0, FEED_MAX_PACKETS);
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
    rxNodeId: packet.rxNodeId,
    observerIds: packet.rxNodeId ? [packet.rxNodeId] : [],
    srcNodeId: packet.srcNodeId,
    summary: packet.summary ?? extractPacketSummary(packet.payload),
    hopCount: packet.hopCount,
    path: packet.path,
    rxCount: packet.direction !== 'tx' ? 1 : 0,
    txCount: packet.direction === 'tx' ? 1 : 0,
    ts: packet.ts,
    advertCount: packet.advertCount,
  };
}
