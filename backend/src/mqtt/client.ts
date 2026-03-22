import mqtt from 'mqtt';
import { MeshCoreDecoder } from '@michaelhart/meshcore-decoder';
import type {
  AdvertPayload, GroupTextPayload, TextMessagePayload,
  TracePayload, PathPayload, AckPayload,
} from '@michaelhart/meshcore-decoder';
import { insertNodeStatusSample, insertPacket, upsertNode, incrementAdvertCount, query } from '../db/index.js';
import { invalidateResolveCache, setResolveCache, getStickyNodeMap, mergeStickyNodes } from '../path-beta/resolveCache.js';
import { resolvePool } from '../path-beta/resolvePool.js';
import type { LivePacket } from '../types/index.js';
import { decodePacketCompat } from './decodePacket.js';

type PacketCallback      = (packet: LivePacket) => void;
type NodeCallback        = (nodeId: string, meta?: { network?: string; observerId?: string }) => void;
type NodeUpsertCallback  = (node: Record<string, unknown>) => void;

const subscribers:       PacketCallback[]     = [];
const nodeSubscribers:   NodeCallback[]       = [];
const upsertSubscribers: NodeUpsertCallback[] = [];

export function onPacket(cb: PacketCallback)         { subscribers.push(cb); }
export function onNodeSeen(cb: NodeCallback)         { nodeSubscribers.push(cb); }
export function onNodeUpsert(cb: NodeUpsertCallback) { upsertSubscribers.push(cb); }

function emit(packet: LivePacket) {
  for (const cb of subscribers) cb(packet);
}

// Debounced node update batching — 500 ms window
// Coalesces rapid repeated emits for the same nodeId into a single callback
// invocation, cutting Redis publishes 10–100× during active advert bursts.
const NODE_DEBOUNCE_MS = 500;
const pendingNodeUpdates  = new Map<string, { network?: string; observerId?: string }>();
const pendingNodeUpserts  = new Map<string, Record<string, unknown>>();
let nodeFlushTimer: ReturnType<typeof setTimeout> | null = null;

function flushPendingNodeEmits(): void {
  nodeFlushTimer = null;
  for (const [nodeId, meta] of pendingNodeUpdates) {
    for (const cb of nodeSubscribers) cb(nodeId, meta);
  }
  pendingNodeUpdates.clear();
  for (const [, node] of pendingNodeUpserts) {
    for (const cb of upsertSubscribers) cb(node);
  }
  pendingNodeUpserts.clear();
}

function scheduleNodeFlush(): void {
  if (nodeFlushTimer !== null) return;
  nodeFlushTimer = setTimeout(flushPendingNodeEmits, NODE_DEBOUNCE_MS);
}

function emitNode(nodeId: string, meta?: { network?: string; observerId?: string }): void {
  pendingNodeUpdates.set(nodeId, meta ?? {});
  scheduleNodeFlush();
}

function emitNodeUpsert(node: Record<string, unknown>): void {
  const nodeId = String(node['node_id'] ?? '');
  if (nodeId) {
    // Merge into pending, keeping latest data for this nodeId
    pendingNodeUpserts.set(nodeId, node);
  } else {
    // No nodeId to deduplicate on — fire immediately
    for (const cb of upsertSubscribers) cb(node);
  }
  scheduleNodeFlush();
}

/**
 * Topic formats:
 *   meshcore/{IATA}/{OBSERVER_PUBLIC_KEY}/{packets|status}
 *   ukmesh/{IATA}/{OBSERVER_PUBLIC_KEY}/{packets|status} (legacy, accepted during migration)
 *   meshcore-test/{IATA}/{OBSERVER_PUBLIC_KEY}/{packets|status} (isolated dev/test ingest)
 *
 * Public-network assignment is derived from observer IATA:
 *   - MME => teesside
 *   - all other IATA => ukmesh/global
 *
 * Any non-public topic prefix is treated as isolated test/dev traffic so it can
 * live in a separate schema/backend without contaminating the public dataset.
 *
 * mctomqtt JSON structure:
 *   status:  { origin, origin_id, model, firmware_version, radio, client_version }
 *   packets: { raw (hex), hash, packet_type, SNR, RSSI, score, route, len,
 *              payload_len, direction, origin, origin_id, timestamp, type }
 *   All numeric values arrive as strings from mctomqtt regex groups.
 */
const DEFAULT_TOPIC_PREFIXES = ['meshcore', 'ukmesh', 'meshcore-test'];
const PUBLIC_TOPIC_PREFIXES = new Set(['meshcore', 'ukmesh']);
const BLOCKED_PUBLIC_IATAS = new Set(['TST']);
const TOPIC_PREFIXES = new Set(
  String(process.env['MQTT_TOPIC_PREFIXES'] ?? DEFAULT_TOPIC_PREFIXES.join(','))
    .split(',')
    .map((value) => value.trim().toLowerCase())
    .filter(Boolean),
);
const TEESSIDE_IATA = (process.env['TEESSIDE_IATA'] ?? 'MME').trim().toUpperCase();

interface TopicParts {
  iata:        string;
  observerKey: string;
  suffix:      string;
  network:     string;
}

function parseTopic(topic: string): TopicParts | null {
  const parts = topic.split('/');
  if (parts.length !== 4) return null;
  const prefix = parts[0]?.toLowerCase();
  if (!prefix || !TOPIC_PREFIXES.has(prefix)) return null;
  const iata = (parts[1] ?? '').trim().toUpperCase();
  if (!iata || !/^[A-Z0-9]{2,8}$/.test(iata)) return null;
  if (PUBLIC_TOPIC_PREFIXES.has(prefix) && BLOCKED_PUBLIC_IATAS.has(iata)) return null;
  const network = PUBLIC_TOPIC_PREFIXES.has(prefix)
    ? (iata === TEESSIDE_IATA ? 'teesside' : 'ukmesh')
    : 'test';
  return { iata, observerKey: parts[2]!, suffix: parts[3]!, network };
}

/** Coerce a string or number field to number, returning undefined if not parseable. */
function toNum(v: unknown): number | undefined {
  if (typeof v === 'number') return v;
  if (typeof v === 'string' && v.trim() !== '') {
    const n = Number(v);
    return isNaN(n) ? undefined : n;
  }
  return undefined;
}

function isEmptyPacketEnvelope(json: Record<string, unknown>, rawHex: string, packetType: number | undefined): boolean {
  const hash = String(json['hash'] ?? '').trim();
  const declaredLen = toNum(json['len']);
  const payloadLen = toNum(json['payload_len']);

  return rawHex.trim() === ''
    && !hash
    && packetType == null
    && (declaredLen ?? 0) <= 0
    && (payloadLen ?? 0) <= 0;
}

function toRecord(value: unknown): Record<string, unknown> | undefined {
  return value && typeof value === 'object' && !Array.isArray(value)
    ? value as Record<string, unknown>
    : undefined;
}

function readNum(obj: Record<string, unknown> | undefined, ...keys: string[]): number | undefined {
  if (!obj) return undefined;
  for (const key of keys) {
    if (!(key in obj)) continue;
    const n = toNum(obj[key]);
    if (n != null) return n;
  }
  return undefined;
}

type StatusTelemetrySample = {
  batteryMv?: number;
  uptimeSecs?: number;
  txAirSecs?: number;
  rxAirSecs?: number;
  channelUtilization?: number;
  airUtilTx?: number;
  stats?: Record<string, unknown>;
};

function extractStatusTelemetry(
  json: Record<string, unknown>,
  options?: { allowRawStatsOnly?: boolean },
): StatusTelemetrySample | null {
  const stats = toRecord(json['stats']);
  const hasRawStats = Boolean(stats && Object.keys(stats).length > 0);
  const batteryMv = readNum(stats, 'battery_mv', 'batteryMv');
  const uptimeSecs = (() => {
    const direct = readNum(stats, 'uptime_secs', 'uptimeSecs');
    if (direct != null) return direct;
    const uptimeMs = readNum(stats, 'uptime_ms', 'uptimeMs');
    if (uptimeMs == null) return undefined;
    return Math.floor(uptimeMs / 1000);
  })();
  const txAirSecs = readNum(stats, 'tx_air_secs', 'txAirSecs');
  const rxAirSecs = readNum(stats, 'rx_air_secs', 'rxAirSecs');
  const channelUtilization = readNum(
    stats,
    'channel_utilization',
    'channel_utilization_pct',
    'channel_util',
    'channelUtil',
    'channelUtilization',
  );
  const airUtilTx = readNum(
    stats,
    'air_util_tx',
    'air_util_tx_pct',
    'tx_air_util',
    'tx_air_utilization',
    'airUtilTx',
  );

  if (
    batteryMv == null
    && uptimeSecs == null
    && txAirSecs == null
    && rxAirSecs == null
    && channelUtilization == null
    && airUtilTx == null
  ) {
    if (options?.allowRawStatsOnly && hasRawStats) {
      return {
        batteryMv,
        uptimeSecs,
        txAirSecs,
        rxAirSecs,
        channelUtilization,
        airUtilTx,
        stats,
      };
    }
    return null;
  }

  return {
    batteryMv,
    uptimeSecs,
    txAirSecs,
    rxAirSecs,
    channelUtilization,
    airUtilTx,
    stats,
  };
}

/** In-flight pre-resolve tracking — prevents duplicate concurrent resolutions for the same hash. */
const preResolveInFlight = new Set<string>();

/**
 * Per-observer packet dedup — prevents relay copies of the same packet from being
 * ingested multiple times when they arrive at the same observer with the same hop count.
 * Keyed by "packetHash:observerKey:hopCount". Entries expire after 120 seconds.
 */
const seenPackets = new Map<string, number>();
const SEEN_PACKETS_MAX = 50_000;
const SEEN_PACKETS_TTL_MS = 120_000;

setInterval(() => {
  const cutoff = Date.now() - SEEN_PACKETS_TTL_MS;
  for (const [k, ts] of seenPackets) {
    if (ts < cutoff) seenPackets.delete(k);
  }
}, 60_000).unref();

function isDuplicatePacket(packetHash: string, observerKey: string, hopCount: number | undefined): boolean {
  const key = `${packetHash}:${observerKey}:${hopCount ?? '?'}`;
  if (seenPackets.has(key)) return true;
  if (seenPackets.size >= SEEN_PACKETS_MAX) {
    const oldest = seenPackets.keys().next().value;
    if (oldest !== undefined) seenPackets.delete(oldest);
  }
  seenPackets.set(key, Date.now());
  return false;
}

/**
 * Dedup map for advert counts — prevents relay copies of the same advert packet
 * from incrementing the count multiple times. Keyed by decoded message hash.
 * Entries expire after 60 seconds (well beyond any realistic relay window).
 */
const countedAdvertHashes = new Map<string, number>();
const COUNTED_ADVERT_HASHES_MAX = 10_000;
const COUNTED_ADVERT_TTL_MS = 60_000;

setInterval(() => {
  const cutoff = Date.now() - COUNTED_ADVERT_TTL_MS;
  for (const [h, ts] of countedAdvertHashes) {
    if (ts < cutoff) countedAdvertHashes.delete(h);
  }
}, 30_000).unref();

function tryCountAdvert(hash: string): boolean {
  if (countedAdvertHashes.has(hash)) return false;
  if (countedAdvertHashes.size >= COUNTED_ADVERT_HASHES_MAX) {
    const oldest = countedAdvertHashes.keys().next().value;
    if (oldest !== undefined) countedAdvertHashes.delete(oldest);
  }
  countedAdvertHashes.set(hash, Date.now());
  return true;
}

/**
 * Channel registry — built once at startup.
 * MESHCORE_CHANNEL_SECRETS supports 'name:hex' or bare 'hex' entries (comma-separated).
 * The default public channel is always included.
 */
interface ChannelEntry {
  name:     string;
  secret:   string;
  keyStore: ReturnType<typeof MeshCoreDecoder.createKeyStore>;
}

const channelEntries: ChannelEntry[] = [
  {
    name:     'Public',
    secret:   '8b3387e9c5cdea6ac9e5edbaa115cd72',
    keyStore: MeshCoreDecoder.createKeyStore({ channelSecrets: ['8b3387e9c5cdea6ac9e5edbaa115cd72'] }),
  },
  ...(process.env['MESHCORE_CHANNEL_SECRETS']
    ?.split(',').map((s) => s.trim()).filter(Boolean)
    .map((entry) => {
      const colon  = entry.indexOf(':');
      const name   = colon > 0 ? entry.slice(0, colon)  : entry.slice(0, 6);
      const secret = colon > 0 ? entry.slice(colon + 1) : entry;
      return { name, secret, keyStore: MeshCoreDecoder.createKeyStore({ channelSecrets: [secret] }) };
    }) ?? []),
];

// Combined keyStore used for decryption (all secrets, single decode call per packet)
const keyStore = MeshCoreDecoder.createKeyStore({
  channelSecrets: channelEntries.map((e) => e.secret),
});

// Small cache so relay copies of the same GroupText don't trigger re-decodes
const channelCache = new Map<string, string | null>();
const CHANNEL_CACHE_MAX = 200;

/** Identify which channel a GroupText was sent on by trying each single-key keyStore. */
function identifyChannel(rawHex: string): string | undefined {
  // Single channel — must be it, no re-decode needed
  if (channelEntries.length === 1) return channelEntries[0]!.name;

  if (channelCache.has(rawHex)) return channelCache.get(rawHex) ?? undefined;

  let result: string | undefined;
  for (const entry of channelEntries) {
    const { decoded: d } = decodePacketCompat(rawHex, entry.keyStore);
    const p = d?.payload?.decoded as GroupTextPayload | undefined;
    if (p?.decrypted) { result = entry.name; break; }
  }

  if (channelCache.size >= CHANNEL_CACHE_MAX) {
    channelCache.delete(channelCache.keys().next().value!);
  }
  channelCache.set(rawHex, result ?? null);
  return result;
}

/** Build a short human-readable summary from a decoded payload. */
function buildSummary(payloadType: number, decoded: unknown, rawHex?: string): string | undefined {
  if (!decoded) return undefined;

  switch (payloadType) {
    case 4: {
      const p = decoded as AdvertPayload;
      const name = p.appData?.name;
      return name ? `${name}` : undefined;
    }
    case 5: {
      const p = decoded as GroupTextPayload;
      if (p.decrypted) {
        const sender  = p.decrypted.sender ?? '?';
        const channel = rawHex ? identifyChannel(rawHex) : undefined;
        const prefix  = channel ? `[${channel}] ` : '';
        return `${prefix}${sender}: ${p.decrypted.message}`;
      }
      return '[encrypted]';
    }
    case 2: {
      const p = decoded as TextMessagePayload;
      if (p.decrypted?.message) return `${p.decrypted.message}`;
      return '[encrypted DM]';
    }
    case 3: {
      const p = decoded as AckPayload;
      return `ACK ${p.checksum.slice(0, 4)}`;
    }
    case 8: {
      const p = decoded as PathPayload;
      return `${p.pathLength} hop path`;
    }
    case 9: {
      const p = decoded as TracePayload;
      return `trace ${p.pathHashes.length} hops`;
    }
    default:
      return undefined;
  }
}

function buildAdvertFallbackPayload(originId: string, originName?: string): Record<string, unknown> {
  const appData: Record<string, unknown> = {
    flags: 0,
    deviceRole: 2,
    hasLocation: false,
    hasName: Boolean(originName),
  };
  if (originName) appData['name'] = originName;
  return {
    type: 4,
    version: 0,
    isValid: false,
    publicKey: originId,
    appData,
  };
}


export function startMqttClient(): void {
  const brokerUrl = process.env['MQTT_BROKER_URL'] ?? 'ws://mosquitto:9001';
  const redactedUrl = brokerUrl.replace(/\/\/[^@]*@/, '//***:***@');
  console.log(`[mqtt] connecting to ${redactedUrl}`);
  console.log(`[mqtt] channels: ${channelEntries.map((e) => e.name).join(', ')}`);
  console.log(`[mqtt] topic prefixes: ${Array.from(TOPIC_PREFIXES).join(', ')}`);

  const client = mqtt.connect(brokerUrl, {
    reconnectPeriod: 5000,
    connectTimeout: 30000,
    clientId: `meshcore-analytics-${Math.random().toString(16).slice(2, 8)}`,
    username: process.env['MQTT_USERNAME'],
    password: process.env['MQTT_PASSWORD'],
  });

  client.on('connect', () => {
    console.log('[mqtt] connected');
    for (const prefix of TOPIC_PREFIXES) {
      client.subscribe(`${prefix}/#`, { qos: 0 }, (err) => {
        if (err) console.error(`[mqtt] subscribe error (${prefix}/#)`, err.message);
        else      console.log(`[mqtt] subscribed to ${prefix}/#`);
      });
    }
  });

  client.on('error',     (err) => console.error('[mqtt] error', err.message));
  client.on('reconnect', ()    => console.log('[mqtt] reconnecting…'));
  client.on('message',   (topic: string, rawPayload: Buffer) => {
    void handleMessage(topic, rawPayload);
  });
}

async function handleMessage(topic: string, rawPayload: Buffer): Promise<void> {
  const topicParts = parseTopic(topic);
  if (!topicParts) return;

  const { iata, observerKey, suffix, network } = topicParts;
  const rawStr = rawPayload.toString('utf8').trim();

  let json: Record<string, unknown>;
  try {
    json = JSON.parse(rawStr) as Record<string, unknown>;
  } catch {
    console.warn(`[mqtt] non-JSON payload on topic ${topic}: ${rawStr.slice(0, 80)}`);
    return;
  }

  const origin   = json['origin']           as string | undefined;
  const originId = json['origin_id']        as string | undefined;

  if (suffix === 'status') {
    const model    = json['model']            as string | undefined;
    const firmware = json['firmware_version'] as string | undefined;

    const nodeId = originId ?? observerKey;
    upsertNode(nodeId, {
      name:            origin,
      iata,
      publicKey:       originId,
      hardwareModel:   (model    && model    !== 'unknown') ? model    : undefined,
      firmwareVersion: (firmware && firmware !== 'unknown') ? firmware : undefined,
      network,
      allowTestOverride: network === 'test' && nodeId === observerKey,
    }).catch((err: Error) => console.error('[mqtt] upsertNode error:', err.message));
    const telemetry = extractStatusTelemetry(json, {
      allowRawStatsOnly: network === 'test',
    });
    if (telemetry) {
      insertNodeStatusSample({
        nodeId,
        network,
        batteryMv: telemetry.batteryMv,
        uptimeSecs: telemetry.uptimeSecs,
        txAirSecs: telemetry.txAirSecs,
        rxAirSecs: telemetry.rxAirSecs,
        channelUtilization: telemetry.channelUtilization,
        airUtilTx: telemetry.airUtilTx,
        stats: telemetry.stats,
      }).catch((err: Error) => console.error('[mqtt] insertNodeStatusSample error:', err.message));
    }
    emitNode(nodeId, { network, observerId: observerKey });
    return;
  }

  if (suffix !== 'packets') return;

  const packetHash = (json['hash'] as string | undefined) ?? crypto.randomUUID();
  const packetType = toNum(json['packet_type']);
  const rssi       = toNum(json['RSSI']);
  const snr        = toNum(json['SNR']);
  const rawHex     = (json['raw'] as string | undefined) ?? '';
  const direction  = json['direction'] as string | undefined;

  if (isEmptyPacketEnvelope(json, rawHex, packetType)) {
    return;
  }

  let resolvedPacketType = packetType;

  let innerPayload: Record<string, unknown> | undefined;
  let decodedHash: string | undefined;
  let decodedHops: number | undefined;
  let decodedPathHashSizeBytes: number | undefined;
  let decodedRouteType: number | undefined;
  let summary:     string | undefined;
  let srcNodeId:   string | undefined;
  let advertCount: number | undefined;

  let path: string[] | undefined;

  if (rawHex) {
    try {
      const { decoded, pathHashes, pathHashCount, pathHashSize, routeType: decodedRT } = decodePacketCompat(rawHex, keyStore);

      if (decoded) {
        resolvedPacketType = decoded.payloadType ?? resolvedPacketType;
        decodedHash = decoded.messageHash;
        decodedHops = pathHashCount ?? decoded.pathLength;
        decodedPathHashSizeBytes = pathHashSize;
        decodedRouteType = decodedRT;
        if (pathHashes && pathHashes.length > 0) {
          path = pathHashes;
        }

        if (
          decodedPathHashSizeBytes != null
          && decodedPathHashSizeBytes > 1
          && decodedHops != null
          && (decodedPathHashSizeBytes * decodedHops) > 64
        ) {
          console.warn(
            `[mqtt] dropping impossible multibyte path metadata: hash=${decodedHash ?? 'unknown'} size=${decodedPathHashSizeBytes} hops=${decodedHops} raw=${rawHex.slice(0, 24)}…`,
          );
          decodedHops = undefined;
          decodedPathHashSizeBytes = undefined;
          path = undefined;
        }

        const decodedInner = decoded.payload?.decoded;
        summary = buildSummary(decoded.payloadType, decodedInner, rawHex);

        if (decoded.payloadType === 4) {
          const inner     = decodedInner as unknown as Record<string, unknown> | undefined;
          const appData   = inner?.['appData'] as Record<string, unknown> | undefined;
          const loc       = appData?.['location'] as Record<string, number> | undefined;
          const senderKey = inner?.['publicKey'] as string | undefined;
          const nodeId    = senderKey ?? observerKey;

          if (network !== 'test') {
            upsertNode(nodeId, {
              name:      appData?.['name']       as string | undefined,
              lat:       loc?.['latitude'],
              lon:       loc?.['longitude'],
              role:      appData?.['deviceRole'] as number | undefined,
              iata,
              publicKey: senderKey,
              network,
            }).catch((err: Error) => console.error('[mqtt] upsertNode error:', err.message));

            if (decodedHash && tryCountAdvert(decodedHash)) {
              incrementAdvertCount(nodeId).catch((err: Error) => console.error('[mqtt] incrementAdvertCount error:', err.message));
            }

            emitNodeUpsert({
              node_id:     nodeId,
              name:        appData?.['name']       as string | undefined,
              lat:         loc?.['latitude'],
              lon:         loc?.['longitude'],
              role:        appData?.['deviceRole'] as number | undefined,
              iata,
              network,
              observer_id: observerKey,
              public_key:  senderKey,
              last_seen:   new Date().toISOString(),
              is_online:   true,
              advert_count: advertCount,
            });
          }

          innerPayload = inner;
          srcNodeId = senderKey;
        } else if (decoded.payloadType === 5) {
          innerPayload = decodedInner as unknown as Record<string, unknown> | undefined;
        } else if (decoded.payloadType === 7) {
          const inner = decodedInner as unknown as Record<string, unknown> | undefined;
          srcNodeId = inner?.['senderPublicKey'] as string | undefined;
        } else if (decoded.payloadType === 1) {
          // Router/Advert packets - extract origin_id from payload
          const inner = decodedInner as unknown as Record<string, unknown> | undefined;
          srcNodeId = inner?.['origin_id'] as string | undefined;
        }
      }
    } catch {
      // Decode failed — fall back to mctomqtt fields
    }
  }

  const hasDecodedAdvertPayload = Boolean(
    innerPayload
      && typeof innerPayload === 'object'
      && 'appData' in innerPayload
      && srcNodeId
  );
  const useTxAdvertFallback = direction === 'tx'
    && resolvedPacketType === 4
    && Boolean(originId)
    && !hasDecodedAdvertPayload;

  if (useTxAdvertFallback && originId) {
    srcNodeId = originId;
    summary ??= origin;
    innerPayload = buildAdvertFallbackPayload(originId, origin);
  }

  // For Router packets (type 1), also try originId as fallback
  if (!srcNodeId && resolvedPacketType === 1 && originId) {
    srcNodeId = originId;
  }

  if (resolvedPacketType == null) {
    return;
  }

  const finalHash = decodedHash ?? (json['hash'] as string | undefined) ?? crypto.randomUUID();

  if (isDuplicatePacket(finalHash, observerKey, decodedHops)) {
    return;
  }

  upsertNode(observerKey, {
    iata,
    network,
    allowTestOverride: network === 'test',
  }).catch((err: Error) => console.error('[mqtt] upsertNode error:', err.message));
  emitNode(observerKey, { network, observerId: observerKey });

  if (useTxAdvertFallback && originId) {
    upsertNode(originId, {
      name: origin,
      iata,
      publicKey: originId,
      network,
    }).catch((err: Error) => console.error('[mqtt] upsertNode error:', err.message));
    if (tryCountAdvert(finalHash)) {
      incrementAdvertCount(originId).catch((err: Error) => console.error('[mqtt] incrementAdvertCount error:', err.message));
    }
    emitNodeUpsert({
      node_id: originId,
      name: origin,
      iata,
      network,
      observer_id: observerKey,
      public_key: originId,
      last_seen: new Date().toISOString(),
      is_online: true,
      advert_count: advertCount,
    });
  }

  {
    const livePacket: LivePacket = {
      id:         crypto.randomUUID(),
      packetHash: finalHash,
      rxNodeId:   observerKey,
      srcNodeId,
      topic,
      network,
      packetType: resolvedPacketType,
      routeType:  decodedRouteType,
      hopCount:   decodedHops,
      pathHashSizeBytes: decodedPathHashSizeBytes,
      direction,
      summary,
      payload:    innerPayload ?? json,
      path,
      advertCount,
      ts:         Date.now(),
    };
    emit(livePacket);
  }

  try {
    await insertPacket({
      packetHash: finalHash,
      rxNodeId:   observerKey,
      srcNodeId,
      topic,
      packetType: resolvedPacketType,
      routeType:  decodedRouteType,
      hopCount:   decodedHops,
      pathHashSizeBytes: decodedPathHashSizeBytes,
      rssi,
      snr,
      payload:    innerPayload ?? json,
      summary,
      rawHex,
      advertCount,
      pathHashes: path,
      network,
    });
    invalidateResolveCache(finalHash);

    // Pre-resolve path for ADV/GRP packets so the cache is warm before the browser requests it.
    // Only runs if no resolution is already in-flight for this hash.
    if ((resolvedPacketType === 4 || resolvedPacketType === 5) && !preResolveInFlight.has(finalHash)) {
      preResolveInFlight.add(finalHash);
      const ck = `m|${finalHash}|${network}`;
      // Pass any previously resolved high-confidence hop assignments so the solver reuses them
      const stickyEntry = getStickyNodeMap(finalHash, network);
      const stickyMap = stickyEntry ? Object.fromEntries(stickyEntry.hashToNodeId) : undefined;
      const stickyAgeFraction = stickyEntry?.ageFraction;
      resolvePool.run<{ stickyUpdates?: Record<string, string> } | null>({ type: 'resolveMulti', packetHash: finalHash, network, ...(stickyMap ? { stickyMap, stickyAgeFraction } : {}) })
        .then((result) => {
          if (result) {
            const { stickyUpdates, ...cacheableResult } = result;
            setResolveCache(ck, cacheableResult);
            if (stickyUpdates && Object.keys(stickyUpdates).length > 0) {
              mergeStickyNodes(finalHash, network, stickyUpdates);
            }
          }
        })
        .catch(() => { /* best-effort */ })
        .finally(() => { preResolveInFlight.delete(finalHash); });
    }
  } catch (err) {
    console.error('[mqtt] db insert failed', (err as Error).message);
  }
}

export async function backfillHistoricalLinks(
  queueFn: (
    rxNodeId: string,
    srcNodeId: string | undefined,
    path: string[],
    hopCount: number | undefined,
    pathHashSizeBytes: number | undefined,
  ) => void,
): Promise<void> {
  const res = await query<{
    rx_node_id: string; src_node_id: string | null; hop_count: number | null; raw_hex: string;
  }>(
    `SELECT DISTINCT ON (packet_hash)
       rx_node_id, src_node_id, hop_count, raw_hex
     FROM packets
     WHERE rx_node_id IS NOT NULL AND raw_hex IS NOT NULL AND raw_hex != ''
     ORDER BY packet_hash, time DESC`,
  );

  let queued = 0;
  for (const row of res.rows) {
    try {
      const compat = decodePacketCompat(row.raw_hex, keyStore);
      if ((compat.pathHashSize ?? 1) > 1 && compat.pathHashes && compat.pathHashes.length > 0) {
        queueFn(
          row.rx_node_id,
          row.src_node_id ?? undefined,
          compat.pathHashes,
          compat.pathHashCount,
          compat.pathHashSize,
        );
        queued++;
      }
    } catch {
      // Skip undecipherable packets
    }
  }
  console.log(`[app] historical link backfill: queued ${queued} packets`);
}
