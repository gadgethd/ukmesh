import type { PacketBatchInput } from './packetBatch.js';

export function observerRegionFromTopic(topic: string): string | null {
  const [prefix = '', rawRegion = ''] = topic.split('/');
  if (prefix === 'meshcore-test') return null;
  return rawRegion.trim().toUpperCase() || 'UNK';
}

export type InsertPacketInput = {
  packetHash: string;
  rxNodeId?: string;
  srcNodeId?: string;
  topic: string;
  packetType?: number;
  routeType?: number;
  hopCount?: number;
  rssi?: number;
  snr?: number;
  payload?: Record<string, unknown>;
  summary?: string;
  rawHex: string;
  advertCount?: number;
  pathHashes?: string[];
  pathHashSizeBytes?: number;
  network?: string;
  transportCodes?: string;
  regionScope?: string;
};

export function buildPacketBatchInput(p: InsertPacketInput): PacketBatchInput {
  const inferredPathHashSizeBytes = (() => {
    if (typeof p.pathHashSizeBytes === 'number' && Number.isFinite(p.pathHashSizeBytes) && p.pathHashSizeBytes > 0) {
      return Math.trunc(p.pathHashSizeBytes);
    }
    const first = p.pathHashes?.[0];
    if (!first) return null;
    const len = String(first).trim().length;
    return len === 2 || len === 4 || len === 6 ? len / 2 : null;
  })();
  const storedPayload = p.payload
    ? (p.summary ? { ...p.payload, _summary: p.summary } : p.payload)
    : (p.summary ? { _summary: p.summary } : null);
  const companionSender = (() => {
    if (p.packetType !== 5 || !storedPayload) return null;
    const decrypted = storedPayload['decrypted'];
    if (!decrypted || typeof decrypted !== 'object' || Array.isArray(decrypted)) return null;
    const sender = (decrypted as Record<string, unknown>)['sender'];
    if (typeof sender !== 'string') return null;
    const normalized = sender.trim();
    return normalized.length > 0 ? normalized.slice(0, 256) : null;
  })();
  const network = p.network ?? 'ukmesh';
  const topicPrefix = p.topic.split('/', 1)[0]?.trim() || '';
  const iata = observerRegionFromTopic(p.topic);
  return {
    time: new Date(),
    packetHash: p.packetHash,
    rxNodeId: p.rxNodeId ?? null,
    srcNodeId: p.srcNodeId ?? null,
    topic: p.topic,
    topicPrefix,
    iata,
    packetType: p.packetType ?? null,
    routeType: p.routeType ?? null,
    hopCount: p.hopCount ?? null,
    rssi: p.rssi ?? null,
    snr: p.snr ?? null,
    payloadJson: storedPayload ? JSON.stringify(storedPayload) : null,
    companionSender,
    rawHex: p.rawHex,
    advertCount: p.advertCount ?? null,
    pathHashes: p.pathHashes?.map((hash) => String(hash).trim().toUpperCase()) ?? null,
    pathHashSizeBytes: inferredPathHashSizeBytes,
    network,
    transportCodes: p.transportCodes ?? null,
    regionScope: p.regionScope ?? null,
  };
}
