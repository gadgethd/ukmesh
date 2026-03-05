import mqtt from 'mqtt';
import { MeshCoreDecoder } from '@michaelhart/meshcore-decoder';
import type {
  AdvertPayload, GroupTextPayload, TextMessagePayload,
  TracePayload, PathPayload, AckPayload,
} from '@michaelhart/meshcore-decoder';
import { insertPacket, upsertNode, incrementAdvertCount, query } from '../db/index.js';
import type { LivePacket } from '../types/index.js';

type PacketCallback      = (packet: LivePacket) => void;
type NodeCallback        = (nodeId: string) => void;
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
function emitNode(nodeId: string) {
  for (const cb of nodeSubscribers) cb(nodeId);
}
function emitNodeUpsert(node: Record<string, unknown>) {
  for (const cb of upsertSubscribers) cb(node);
}

/**
 * Topic formats:
 *   meshcore/{IATA}/{OBSERVER_PUBLIC_KEY}/{packets|status}
 *   ukmesh/{IATA}/{OBSERVER_PUBLIC_KEY}/{packets|status} (legacy, accepted during migration)
 *
 * Network assignment is now derived from observer IATA:
 *   - MME => teesside
 *   - all other IATA => ukmesh/global
 *
 * mctomqtt JSON structure:
 *   status:  { origin, origin_id, model, firmware_version, radio, client_version }
 *   packets: { raw (hex), hash, packet_type, SNR, RSSI, score, route, len,
 *              payload_len, direction, origin, origin_id, timestamp, type }
 *   All numeric values arrive as strings from mctomqtt regex groups.
 */
const TOPIC_PREFIXES = new Set(['meshcore', 'ukmesh']);
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
  if (!iata) return null;
  const network = iata === TEESSIDE_IATA ? 'teesside' : 'ukmesh';
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

/**
 * Dedup map for advert counts — prevents relay copies of the same advert packet
 * from incrementing the count multiple times. Keyed by decoded message hash.
 * Entries expire after 60 seconds (well beyond any realistic relay window).
 */
const countedAdvertHashes = new Map<string, number>();

function tryCountAdvert(hash: string): boolean {
  const now = Date.now();
  for (const [h, ts] of countedAdvertHashes) {
    if (now - ts > 60_000) countedAdvertHashes.delete(h);
  }
  if (countedAdvertHashes.has(hash)) return false;
  countedAdvertHashes.set(hash, now);
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

/** Identify which channel a GroupText was sent on by trying each single-key keyStore. */
function identifyChannel(rawHex: string): string | undefined {
  for (const entry of channelEntries) {
    const d = MeshCoreDecoder.decode(rawHex, { keyStore: entry.keyStore });
    const p = d?.payload?.decoded as GroupTextPayload | undefined;
    if (p?.decrypted) return entry.name;
  }
  return undefined;
}

/** Build a short human-readable summary from a decoded payload. */
function buildSummary(payloadType: number, decoded: unknown, rawHex?: string): string | undefined {
  if (!decoded) return undefined;

  switch (payloadType) {
    case 4: { // Advert
      const p = decoded as AdvertPayload;
      const name = p.appData?.name;
      return name ? `${name}` : undefined;
    }
    case 5: { // GroupText
      const p = decoded as GroupTextPayload;
      if (p.decrypted) {
        const sender  = p.decrypted.sender ?? '?';
        const channel = rawHex ? identifyChannel(rawHex) : undefined;
        const prefix  = channel ? `[${channel}] ` : '';
        return `${prefix}${sender}: ${p.decrypted.message}`;
      }
      return '[encrypted]';
    }
    case 2: { // TextMessage (direct/private)
      const p = decoded as TextMessagePayload;
      if (p.decrypted?.message) return `${p.decrypted.message}`;
      return '[encrypted DM]';
    }
    case 3: { // Ack
      const p = decoded as AckPayload;
      return `ACK ${p.checksum.slice(0, 4)}`;
    }
    case 8: { // Path
      const p = decoded as PathPayload;
      return `${p.pathLength} hop path`;
    }
    case 9: { // Trace
      const p = decoded as TracePayload;
      return `trace ${p.pathHashes.length} hops`;
    }
    default:
      return undefined;
  }
}


export function startMqttClient(): void {
  const brokerUrl = process.env['MQTT_BROKER_URL'] ?? 'ws://mosquitto:9001';
  console.log(`[mqtt] connecting to ${brokerUrl}`);
  console.log(`[mqtt] channels: ${channelEntries.map((e) => e.name).join(', ')}`);

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

  // ── Status: update observer node metadata ─────────────────────────────────
  if (suffix === 'status') {
    const origin   = json['origin']           as string | undefined;
    const originId = json['origin_id']        as string | undefined;
    const model    = json['model']            as string | undefined;
    const firmware = json['firmware_version'] as string | undefined;

    const nodeId = originId ?? observerKey;
    void upsertNode(nodeId, {
      name:            origin,
      iata,
      publicKey:       originId,
      hardwareModel:   (model    && model    !== 'unknown') ? model    : undefined,
      firmwareVersion: (firmware && firmware !== 'unknown') ? firmware : undefined,
      network,
    });
    emitNode(nodeId);
    return;
  }

  // ── Packets only below ─────────────────────────────────────────────────────
  if (suffix !== 'packets') return;

  // mctomqtt fields (all arrive as strings)
  const packetHash = (json['hash'] as string | undefined) ?? crypto.randomUUID();
  const packetType = toNum(json['packet_type']);
  const rssi       = toNum(json['RSSI']);
  const snr        = toNum(json['SNR']);
  const rawHex     = (json['raw'] as string | undefined) ?? '';
  const direction  = json['direction'] as string | undefined; // 'rx' | 'tx'

  // ── Decode raw hex with keyStore for all packet types ─────────────────────
  let innerPayload: Record<string, unknown> | undefined;
  let decodedHash: string | undefined;
  let decodedHops: number | undefined;
  let summary:     string | undefined;
  let srcNodeId:   string | undefined;
  let advertCount: number | undefined;

  let path: string[] | undefined;

  if (rawHex) {
    try {
      const decoded = MeshCoreDecoder.decode(rawHex, { keyStore });

      if (decoded) {
        decodedHash = decoded.messageHash;
        decodedHops = decoded.pathLength;
        if (decoded.path && Array.isArray(decoded.path) && decoded.path.length > 0) {
          path = decoded.path as string[];
        }

        const decodedInner = decoded.payload?.decoded;
        summary = buildSummary(decoded.payloadType, decodedInner, rawHex);

        if (decoded.payloadType === 4) {
          // Advert: upsert the advertising node
          const inner     = decodedInner as unknown as Record<string, unknown> | undefined;
          const appData   = inner?.['appData'] as Record<string, unknown> | undefined;
          const loc       = appData?.['location'] as Record<string, number> | undefined;
          const senderKey = inner?.['publicKey'] as string | undefined;
          const nodeId    = senderKey ?? observerKey;

          void upsertNode(nodeId, {
            name:      appData?.['name']       as string | undefined,
            lat:       loc?.['latitude'],
            lon:       loc?.['longitude'],
            role:      appData?.['deviceRole'] as number | undefined,
            iata,
            publicKey: senderKey,
            network,
          });

          // Only count each unique advert once — relay copies share the same decoded hash.
          if (decodedHash && tryCountAdvert(decodedHash)) {
            advertCount = await incrementAdvertCount(nodeId);
          }

          // Push full node data to frontend so map updates immediately
          emitNodeUpsert({
            node_id:     nodeId,
            name:        appData?.['name']       as string | undefined,
            lat:         loc?.['latitude'],
            lon:         loc?.['longitude'],
            role:        appData?.['deviceRole'] as number | undefined,
            iata,
            public_key:  senderKey,
            last_seen:   new Date().toISOString(),
            is_online:   true,
            advert_count: advertCount,
          });

          innerPayload = inner;
          srcNodeId = senderKey;
        } else if (decoded.payloadType === 5) {
          // GroupText: store decoded payload so sender name is queryable from DB
          innerPayload = decodedInner as unknown as Record<string, unknown> | undefined;
        } else if (decoded.payloadType === 7) {
          // AnonRequest: sender public key is exposed in clear
          const inner = decodedInner as unknown as Record<string, unknown> | undefined;
          srcNodeId = inner?.['senderPublicKey'] as string | undefined;
        }
      }
    } catch {
      // Decode failed — fall back to mctomqtt fields
    }
  }

  // Use decoder hash as primary key — it's derived from packet content and is the
  // same for TX and RX of the same packet, enabling frontend deduplication.
  // Fall back to mctomqtt hash (RX only) when decode fails, then UUID.
  // The 16-char mctomqtt hash remains accessible via payload->>'hash' in the DB.
  const finalHash = decodedHash ?? (json['hash'] as string | undefined) ?? crypto.randomUUID();

  // Keep observer last-seen current
  void upsertNode(observerKey, { iata, network });
  emitNode(observerKey);

  {
    const livePacket: LivePacket = {
      id:         crypto.randomUUID(),
      packetHash: finalHash,
      rxNodeId:   observerKey,
      srcNodeId,
      topic,
      packetType,
      hopCount:   decodedHops,
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
      packetType,
      routeType:  undefined,
      hopCount:   decodedHops,
      rssi,
      snr,
      payload:    innerPayload ?? json,
      rawHex,
      advertCount,
      pathHashes: path,
      network,
    });
  } catch (err) {
    console.error('[mqtt] db insert failed', (err as Error).message);
  }
}

/**
 * Decode all historical packets with raw_hex and queue link jobs for those with
 * relay path data. Called once at startup when node_links is empty.
 */
export async function backfillHistoricalLinks(
  queueFn: (rxNodeId: string, srcNodeId: string | undefined, path: string[], hopCount: number | undefined) => void,
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
      const decoded = MeshCoreDecoder.decode(row.raw_hex, { keyStore });
      if (decoded?.path && Array.isArray(decoded.path) && decoded.path.length > 0) {
        queueFn(row.rx_node_id, row.src_node_id ?? undefined, decoded.path as string[], decoded.pathLength);
        queued++;
      }
    } catch {
      // Skip undecipherable packets
    }
  }
  console.log(`[app] historical link backfill: queued ${queued} packets`);
}
