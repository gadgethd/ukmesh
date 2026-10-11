/**
 * RAM-first MQTT ingestion — protocol + configuration.
 *
 * The RAM mode admits packets into a bounded Redis Stream after validation,
 * identity handling and privacy classification, then acknowledges the broker
 * (early ACK). A background writer consumes the stream with a consumer group
 * and persists batches through the same SQL the durable path uses. Durable
 * mode (the default) never touches this module.
 *
 * Entry identity: every admitted packet carries an `observationId` generated
 * at admission so consumers see a stable identity across the RAM → PostgreSQL
 * handoff (matches the `packets.observation_id` column).
 */
import type { PacketBatchInput } from '../db/packetBatch.js';

export type RamIngestMode = 'durable' | 'ram';

function boundedEnvInteger(name: string, fallback: number, min: number, max: number): number {
  const parsed = Number(process.env[name]);
  if (!Number.isFinite(parsed)) return fallback;
  return Math.min(max, Math.max(min, Math.trunc(parsed)));
}

export function getRamIngestMode(): RamIngestMode {
  const value = String(process.env['MQTT_INGEST_MODE'] ?? 'durable').trim().toLowerCase();
  return value === 'ram' ? 'ram' : 'durable';
}

export function getRamStreamKey(): string {
  const value = process.env['MQTT_RAM_STREAM_KEY']?.trim();
  return value && value.length > 0 ? value : 'meshcore:ingest:pending';
}

export function getRamConsumerGroup(): string {
  const value = process.env['MQTT_RAM_CONSUMER_GROUP']?.trim();
  return value && value.length > 0 ? value : 'ingest-writers';
}

/**
 * Conservative per-entry overhead added to the raw value length when
 * accounting the queue byte budget (stream id, field name, listpack entry
 * overhead and allocator slack). The budget must never underestimate.
 */
export const RAM_ENTRY_OVERHEAD_BYTES = 512;

// Separate working space for bounded quarantine, PEL/markers and allocator
// slack. The queue reservation is an application budget, not a RAM measure.
export const RAM_REDIS_HEADROOM_BYTES = 32 * 1_048_576;
export const RAM_QUARANTINE_MAX_ENTRIES = 500;
export const RAM_QUARANTINE_ENTRY_MAX_BYTES = 16_384;

/** Reserve space for both stream payload and ambiguous-reply delivery record. */
export function accountedRamEntryBytes(value: string): number {
  return Buffer.byteLength(value) * 2 + RAM_ENTRY_OVERHEAD_BYTES;
}

export function getRamQueueBytesKey(): string {
  return `${getRamStreamKey()}:bytes`;
}

export function getRamAdmissionKey(deliveryKey: string): string {
  return `${getRamStreamKey()}:delivery:${deliveryKey}`;
}

export function getRamCommittedKey(): string {
  return `${getRamStreamKey()}:committed`;
}

/** Bounded dead-letter stream for undecodable entries (recovery evidence). */
export function getRamQuarantineKey(): string {
  return `${getRamStreamKey()}:quarantine`;
}

export function getRamConsumerName(): string {
  const value = process.env['MQTT_RAM_CONSUMER_NAME']?.trim();
  return value && value.length > 0 ? value : 'backend';
}

export function getRamBatchSize(): number {
  return boundedEnvInteger('MQTT_RAM_BATCH_SIZE', 100, 1, 2_000);
}

export function getRamMaxAgeMs(): number {
  return boundedEnvInteger('MQTT_RAM_MAX_AGE_MS', 120_000, 1_000, 600_000);
}

export function getRamQueueMaxPackets(): number {
  return boundedEnvInteger('MQTT_RAM_QUEUE_MAX_PACKETS', 5_000, 100, 10_000);
}

export function getRamQueueMaxBytes(): number {
  return boundedEnvInteger('MQTT_RAM_QUEUE_MAX_BYTES', 32 * 1_048_576, 1_048_576, 64 * 1_048_576);
}

/** One admitted packet waiting for batch persistence. */
export type RamPendingEntry = {
  observationId: string;
  /** Broker delivery identity used to recover an ambiguous Redis reply. */
  deliveryKey?: string;
  admittedAtMs: number;
  /** Admission-time classification (bounded staleness; DB re-classifies at write). */
  isPrivate: boolean;
  visibilityOk: boolean;
  input: PacketBatchInput;
};

/** Commit boundary marker: everything at or below `through` is committed. */
export type RamCommitMarker = {
  kind: 'commit';
  through: string;
  atMs: number;
  packets: number;
};

export type RamStreamValue =
  | { kind: 'entry'; entry: RamPendingEntry }
  | { kind: 'commit'; marker: RamCommitMarker }
  | null;

const ENTRY_FIELD = 'e';

export function entryFieldName(): string {
  return ENTRY_FIELD;
}

export function encodePendingEntry(entry: RamPendingEntry): string {
  return JSON.stringify({
    v: 1,
    k: 'p',
    o: entry.observationId,
    dk: entry.deliveryKey,
    a: entry.admittedAtMs,
    ip: entry.isPrivate ? 1 : 0,
    vo: entry.visibilityOk ? 1 : 0,
    d: { ...entry.input, time: entry.input.time.toISOString() },
  });
}

export function encodeCommitMarker(marker: RamCommitMarker): string {
  return JSON.stringify({ v: 1, k: 'c', t: marker.through, a: marker.atMs, n: marker.packets });
}

export function decodeRamStreamValue(raw: string | undefined | null): RamStreamValue {
  if (!raw) return null;
  let parsed: unknown;
  try {
    parsed = JSON.parse(raw);
  } catch {
    return null;
  }
  if (typeof parsed !== 'object' || parsed === null) return null;
  const record = parsed as Record<string, unknown>;
  if (record['v'] !== 1) return null;
  if (record['k'] === 'c') {
    const through = record['t'];
    const atMs = record['a'];
    const packets = record['n'];
    if (typeof through !== 'string' || typeof atMs !== 'number') return null;
    return {
      kind: 'commit',
      marker: {
        kind: 'commit',
        through,
        atMs,
        packets: typeof packets === 'number' ? packets : 0,
      },
    };
  }
  if (record['k'] !== 'p') return null;
  const observationId = record['o'];
  const deliveryKey = record['dk'];
  const admittedAtMs = record['a'];
  const d = record['d'];
  if (typeof observationId !== 'string'
    || !/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i.test(observationId)) return null;
  if (typeof admittedAtMs !== 'number' || !Number.isFinite(admittedAtMs)) return null;
  if ((record['ip'] !== 0 && record['ip'] !== 1)
    || (record['vo'] !== 0 && record['vo'] !== 1)) return null;
  if (deliveryKey !== undefined && (typeof deliveryKey !== 'string' || deliveryKey.length > 128)) return null;
  if (typeof d !== 'object' || d === null) return null;
  const input = d as Record<string, unknown>;
  const boundedString = (value: unknown, max: number): boolean =>
    typeof value === 'string' && value.length <= max;
  const nullableString = (value: unknown, max: number): boolean =>
    value === null || boundedString(value, max);
  const nullableInt = (value: unknown): boolean =>
    value === null || (Number.isInteger(value) && Number(value) >= -2_147_483_648
      && Number(value) <= 2_147_483_647);
  const nullableNumber = (value: unknown): boolean =>
    value === null || (typeof value === 'number' && Number.isFinite(value));
  if (!boundedString(input['packetHash'], 256) || String(input['packetHash']).length === 0) return null;
  if (!boundedString(input['rawHex'], 262_144) || !/^(?:[0-9a-fA-F]{2})*$/.test(String(input['rawHex']))) return null;
  if (!boundedString(input['topic'], 512) || String(input['topic']).length === 0) return null;
  if (!boundedString(input['topicPrefix'], 64)
    || !boundedString(input['network'], 64) || String(input['network']).length === 0) return null;
  for (const field of ['rxNodeId', 'srcNodeId', 'iata', 'companionSender', 'transportCodes', 'regionScope']) {
    if (!nullableString(input[field], 256)) return null;
  }
  for (const field of ['packetType', 'routeType', 'hopCount', 'advertCount', 'pathHashSizeBytes']) {
    if (!nullableInt(input[field])) return null;
  }
  if (!nullableNumber(input['rssi']) || !nullableNumber(input['snr'])) return null;
  if (input['pathHashes'] !== null) {
    if (!Array.isArray(input['pathHashes']) || input['pathHashes'].length > 256
      || !input['pathHashes'].every((hash) => typeof hash === 'string'
        && /^[0-9a-fA-F]{2,6}$/.test(hash))) return null;
  }
  if (input['payloadJson'] !== null) {
    if (!boundedString(input['payloadJson'], 262_144)) return null;
    try {
      const payload = JSON.parse(String(input['payloadJson'])) as unknown;
      if (!payload || typeof payload !== 'object' || Array.isArray(payload)) return null;
    } catch { return null; }
  }
  if (typeof input['time'] !== 'string') return null;
  const time = new Date(input['time']);
  if (Number.isNaN(time.getTime())) return null;
  return {
    kind: 'entry',
    entry: {
      observationId,
      ...(typeof deliveryKey === 'string' && deliveryKey.length > 0 ? { deliveryKey } : {}),
      admittedAtMs,
      isPrivate: record['ip'] === 1,
      visibilityOk: record['vo'] === 1,
      input: { ...(input as unknown as PacketBatchInput), time },
    },
  };
}
