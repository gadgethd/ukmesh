import mqtt, { type MqttClient } from 'mqtt';
import { calcRegionKey, transportCodeMatchesRegion } from '@michaelhart/meshcore-decoder';
import { insertNodeNeighborSample, insertNodeStatusSample, insertPacket, upsertNode, query, insertOrUpdateSpamSuspect, recordMultibyteEvidence } from '../db/index.js';
import { closePacketBatch, flush as flushPacketBatch } from '../db/packetBatch.js';
import { evaluateAdvert, initSpamDetector } from './spamDetector.js';
import { invalidateResolveCache, setResolveCache, getStickyNodeMap, mergeStickyNodes, getHeldPath, setHeldPath } from '../path-beta/resolveCache.js';
import { resolvePool } from '../path-beta/resolvePool.js';
import { scheduleSlowResolution } from '../path-beta/slowMode.js';
import { pathingConfig } from '../platform/config/pathing.js';
import type { LivePacket } from '../types/index.js';
import { decodePacketCompat, verifyAdvertSignature } from './decodePacket.js';
import { buildChannelEntries, buildCombinedKeyStore, buildSummary } from './channelRegistry.js';
import { shouldDiscardUnverifiedTxAdvert, statusEnvelopeTargetsObserver } from './identityBinding.js';
import { extractNeighborNodes } from './neighborPayload.js';
import { parseMqttTopic } from './topic.js';
import { createDurableMqttHandleMessage } from './durableHandler.js';
import { waitForInFlightPacket } from './dedupeRegistry.js';
import { admitPacketToRam } from '../ram/liveStore.js';
import { getRamIngestMode } from '../ram/protocol.js';
import { persistStatusTelemetrySample } from './statusTelemetry.js';
import {
  boundedNetworkMetricLabel,
  mqttIngestAckDuration,
  mqttIngestActive,
  mqttIngestOutcomesTotal,
  mqttMessagesTotal,
  postPersistDroppedTotal,
  postPersistInFlightGauge,
} from '../metrics.js';

type PacketCallback      = (packet: LivePacket, upstreamEnvelope?: Record<string, unknown>) => void;
type NodeCallback        = (nodeId: string, meta?: { network?: string; observerId?: string }) => void;
type NodeUpsertCallback  = (node: Record<string, unknown>) => void;

const subscribers:       PacketCallback[]     = [];
const nodeSubscribers:   NodeCallback[]       = [];
const upsertSubscribers: NodeUpsertCallback[] = [];

type MqttRuntimeStatus = {
  state: 'starting' | 'connected' | 'disconnected' | 'reconnecting' | 'error';
  changedAt: string;
  lastError?: string;
};

let mqttRuntimeStatus: MqttRuntimeStatus = { state: 'starting', changedAt: new Date().toISOString() };
const mqttClients: MqttClient[] = [];
let mqttStopping = false;
const MQTT_INGEST_MODE = getRamIngestMode();
let mqttDurabilityFailure: string | undefined;

function setMqttRuntimeStatus(state: MqttRuntimeStatus['state'], lastError?: string): void {
  mqttRuntimeStatus = {
    state,
    changedAt: new Date().toISOString(),
    ...(lastError ? { lastError: lastError.slice(0, 200) } : {}),
  };
}

export function getMqttRuntimeStatus(): MqttRuntimeStatus {
  return mqttDurabilityFailure
    ? { ...mqttRuntimeStatus, state: 'error', lastError: mqttDurabilityFailure }
    : { ...mqttRuntimeStatus };
}

export function markMqttPersistenceHealthy(): void {
  mqttDurabilityFailure = undefined;
}

export function onPacket(cb: PacketCallback)         { subscribers.push(cb); }
export function onNodeSeen(cb: NodeCallback)         { nodeSubscribers.push(cb); }
export function onNodeUpsert(cb: NodeUpsertCallback) { upsertSubscribers.push(cb); }

function emit(packet: LivePacket, upstreamEnvelope?: Record<string, unknown>) {
  for (const cb of subscribers) cb(packet, upstreamEnvelope);
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
 *   meshcore/{IATA}/{OBSERVER_PUBLIC_KEY}/{packets|status|neighbors|neighbours}
 *   ukmesh/{IATA}/{OBSERVER_PUBLIC_KEY}/{packets|status|neighbors|neighbours} (legacy, accepted during migration)
 *   meshcore-test/{IATA}/{OBSERVER_PUBLIC_KEY}/{packets|status|neighbors} (isolated dev/test ingest)
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
 *   neighbors/neighbours: { nodes: [...] } (both spellings accepted — the
 *   official MeshCore firmware publishes the UK spelling, the MQTT fork the US)
 *   packets: { raw (hex), hash, packet_type, SNR, RSSI, score, route, len,
 *              payload_len, direction, origin, origin_id, timestamp, type }
 *   All numeric values arrive as strings from mctomqtt regex groups.
 */
const DEFAULT_TOPIC_PREFIXES = ['meshcore', 'ukmesh', 'meshcore-test'];
// Test-marker IATAs are isolated to the 'test' network scope regardless of
// topic prefix: persisted for analysis, excluded from every public scope.
const BLOCKED_PUBLIC_IATAS = new Set(['TST']);
const TOPIC_PREFIXES = new Set(
  String(process.env['MQTT_TOPIC_PREFIXES'] ?? DEFAULT_TOPIC_PREFIXES.join(','))
    .split(',')
    .map((value) => value.trim().toLowerCase())
    .filter(Boolean),
);

// Region probe list — built once at startup from MESHCORE_REGION_NAMES env var.
// Names are normalised to '#Name' by the decoder; keys are precomputed SHA256 slices.
// Only used for packets with routeType 0 (TransportFlood) or 3 (TransportDirect).
const REGION_PROBE_LIST: Array<{ name: string }> = (
  process.env['MESHCORE_REGION_NAMES'] ?? 'Europe,Global'
)
  .split(',')
  .map((s) => s.trim())
  .filter(Boolean)
  .map((name) => {
    calcRegionKey(name); // pre-warm internal key derivation (crypto-js is lazy)
    return { name: name.startsWith('#') ? name : `#${name}` };
  });

function topicIataForNode(nodeId: string | undefined, observerKey: string, iata: string): string | undefined {
  if (!nodeId) return undefined;
  return nodeId.trim().toUpperCase() === observerKey.trim().toUpperCase() ? iata : undefined;
}

/** Coerce a string or number field to number, returning undefined if not parseable. */
function toNum(v: unknown): number | undefined {
  if (typeof v === 'number') return Number.isFinite(v) ? v : undefined;
  if (typeof v === 'string' && v.trim() !== '') {
    const n = Number(v);
    return Number.isFinite(n) ? n : undefined;
  }
  return undefined;
}

/** Accept only a stable, bounded upstream packet identifier as a fallback. */
function upstreamPacketHash(value: unknown): string | undefined {
  if (typeof value !== 'string') return undefined;
  const hash = value.trim();
  return /^[0-9A-Fa-f]{16,128}$/.test(hash) ? hash.toUpperCase() : undefined;
}

function normalizeNodeId(value: unknown): string | undefined {
  if (typeof value !== 'string') return undefined;
  const nodeId = value.trim().toUpperCase();
  return /^[0-9A-F]{64}$/.test(nodeId) ? nodeId : undefined;
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

/** In-flight pre-resolve tracking — prevents duplicate concurrent resolutions for the same hash/network. */
const preResolveInFlight = new Set<string>();
const preResolveLastStarted = new Map<string, number>();
const PRE_RESOLVE_HISTORY_MAX = 50_000;
const PRE_RESOLVE_ENABLED = process.env['PATH_BETA_PRERESOLVE_ENABLED'] === '1';
const PRE_RESOLVE_MIN_INTERVAL_MS = Math.max(
  1_000,
  Number(process.env['PATH_BETA_PRERESOLVE_MIN_INTERVAL_MS'] ?? 10_000),
);

setInterval(() => {
  const cutoff = Date.now() - PRE_RESOLVE_MIN_INTERVAL_MS * 6;
  for (const [key, ts] of preResolveLastStarted) {
    if (ts < cutoff) preResolveLastStarted.delete(key);
  }
}, 60_000).unref();

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

function packetDedupeKey(packetHash: string, observerKey: string, hopCount: number | undefined): string {
  return `${packetHash}:${observerKey}:${hopCount ?? '?'}`;
}

function isDuplicatePacket(packetHash: string, observerKey: string, hopCount: number | undefined): boolean {
  return seenPackets.has(packetDedupeKey(packetHash, observerKey, hopCount));
}

function rememberPacket(packetHash: string, observerKey: string, hopCount: number | undefined): void {
  if (seenPackets.size >= SEEN_PACKETS_MAX) {
    const oldest = seenPackets.keys().next().value;
    if (oldest !== undefined) seenPackets.delete(oldest);
  }
  seenPackets.set(packetDedupeKey(packetHash, observerKey, hopCount), Date.now());
}

/**
 * Packets currently being persisted, keyed like `seenPackets`. A second copy
 * of the same packet arriving while the first is still in flight (possible
 * once more than one connection can ingest concurrently) waits for the first
 * attempt: success resolves this copy as a duplicate, failure leaves nothing
 * committed so this copy persists on its own.
 */
const inFlightPackets = new Map<string, Promise<void>>();
// Best-effort post-persist follow-ups are bounded: a stalled database must
// not grow the in-flight set without limit. Over the bound they are dropped
// (safe: everything here is explicitly best-effort) and counted.
const POST_PERSIST_MAX_INFLIGHT = 64;
let postPersistInFlight = 0;

/**
 * Database-level duplicate confirmation for QoS 1 redeliveries — the in-memory
 * filters do not survive a restart. Bounded to the retained packet window so
 * the probe stays on the packet_hash index.
 */
async function packetAlreadyPersisted(
  packetHash: string,
  rxNodeId: string,
  topic: string,
  network: string,
): Promise<boolean> {
  const result = await query<{ present: boolean }>(
    `SELECT EXISTS (
       SELECT 1
         FROM packets
        WHERE packet_hash = $1
          AND rx_node_id = $2
          AND topic = $3
          AND network = $4
          AND time > NOW() - INTERVAL '30 days'
     ) AS present`,
    [packetHash, rxNodeId, topic, network],
  );
  return Boolean(result.rows[0]?.present);
}

/**
 * Channel registry — built once at startup.
 * MESHCORE_CHANNEL_SECRETS supports 'name:hex' or bare 'hex' entries (comma-separated).
 * The default public channel is always included.
 */
const channelEntries = buildChannelEntries(process.env['MESHCORE_CHANNEL_SECRETS']);

// Combined keyStore used for decryption (all secrets, single decode call per packet)
const keyStore = buildCombinedKeyStore(channelEntries);

function boundedEnvInteger(name: string, fallback: number, min: number, max: number): number {
  const parsed = Number(process.env[name]);
  if (!Number.isFinite(parsed)) return fallback;
  return Math.min(max, Math.max(min, Math.trunc(parsed)));
}

const MQTT_MAX_PAYLOAD_BYTES = boundedEnvInteger('MQTT_MAX_PAYLOAD_BYTES', 64 * 1024, 1_024, 1_048_576);
const MQTT_CLIENT_ID = String(
  process.env['MQTT_CLIENT_ID'] ?? 'meshcore-analytics-ingest',
).trim() || 'meshcore-analytics-ingest';
/**
 * Number of ingest connections to the broker. Each connection is a strictly
 * serial durable pipeline (mqtt.js dispatches the next packet only after the
 * previous one is acknowledged), so this is the supported way to run more
 * than one persistence flow; messages are distributed between the connections
 * with a shared subscription. 1 preserves the historical single-connection
 * behaviour exactly.
 */
const MQTT_INGEST_CONNECTIONS = boundedEnvInteger('MQTT_INGEST_CONNECTIONS', 1, 1, 16);
const MQTT_INGEST_SHARE_GROUP = String(
  process.env['MQTT_INGEST_SHARE_GROUP'] ?? 'meshcore-ingest',
).trim() || 'meshcore-ingest';

let activeMqttIngests = 0;
let droppedMqttMessages = 0;
let lastMqttDropLogAt = 0;

function reportDroppedMqttMessage(
  reason: string,
  outcome: 'oversized_payload' | 'oversized_topic',
): void {
  mqttIngestOutcomesTotal.inc({ outcome });
  droppedMqttMessages += 1;
  const now = Date.now();
  if (now - lastMqttDropLogAt < 5_000) return;
  console.warn(`[mqtt] dropped ${droppedMqttMessages} message(s): ${reason}`);
  droppedMqttMessages = 0;
  lastMqttDropLogAt = now;
}

function handleIncomingMqttPacket(packet: { topic: string; payload: Buffer | string; dup?: boolean }): Promise<void> {
  const startedAt = process.hrtime.bigint();
  const rawPayload = Buffer.isBuffer(packet.payload)
    ? packet.payload
    : Buffer.from(packet.payload);
  if (rawPayload.length > MQTT_MAX_PAYLOAD_BYTES) {
    reportDroppedMqttMessage(
      `payload exceeds ${MQTT_MAX_PAYLOAD_BYTES} bytes`,
      'oversized_payload',
    );
    return Promise.resolve();
  }
  if (packet.topic.length > 512) {
    reportDroppedMqttMessage('topic exceeds 512 characters', 'oversized_topic');
    return Promise.resolve();
  }

  activeMqttIngests += 1;
  mqttIngestActive.set(activeMqttIngests);
  return handleMessage(packet.topic, rawPayload, packet.dup === true)
    .then(() => mqttIngestOutcomesTotal.inc({ outcome: 'processed' }))
    .finally(() => {
      mqttIngestAckDuration.observe(Number(process.hrtime.bigint() - startedAt) / 1e9);
      activeMqttIngests -= 1;
      mqttIngestActive.set(activeMqttIngests);
    });
}

function reportMqttIngestFailure(client: MqttClient, error: Error): void {
  mqttIngestOutcomesTotal.inc({ outcome: 'failure' });
  mqttDurabilityFailure = error.message.slice(0, 200) || 'MQTT persistence failed';
  console.error('[mqtt] durable ingest failed; leaving delivery unacknowledged:', error.message);
  // MQTT.js forwards handleMessage errors to its parser callback, which does
  // not send PUBACK. Close the stream so the persistent broker session
  // redelivers the unacknowledged QoS 1 message instead of leaving it inflight.
  if (!mqttStopping && client.connected) client.stream.destroy();
}


export async function startMqttClient(): Promise<void> {
  if (mqttStopping) return;
  // Must complete before connecting to MQTT — the broker replays buffered
  // messages immediately on connect, and knownNodeIds must be populated first.
  await initSpamDetector();
  if (mqttStopping) return;

  const brokerUrl = process.env['MQTT_BROKER_URL'] ?? 'ws://mosquitto:9001';
  const redactedUrl = brokerUrl.replace(/\/\/[^@]*@/, '//***:***@');
  console.log(`[mqtt] connecting to ${redactedUrl}`);
  console.log(`[mqtt] channels: ${channelEntries.map((e) => e.name).join(', ')}`);
  console.log(`[mqtt] topic prefixes: ${Array.from(TOPIC_PREFIXES).join(', ')}`);
  console.log(`[mqtt] durable ingest maxPayload=${MQTT_MAX_PAYLOAD_BYTES}`);
  console.log(`[mqtt] persistent session clientId=${MQTT_CLIENT_ID} subscriptionQos=1`);

  console.log(`[mqtt] connections=${MQTT_INGEST_CONNECTIONS}`);
  for (let index = 0; index < MQTT_INGEST_CONNECTIONS; index += 1) {
    const connectionLabel = MQTT_INGEST_CONNECTIONS > 1
      ? ` (connection ${index + 1}/${MQTT_INGEST_CONNECTIONS})`
      : '';
    const client = mqtt.connect(brokerUrl, {
      reconnectPeriod: 5000,
      connectTimeout: 30000,
      clean: false,
      clientId: MQTT_INGEST_CONNECTIONS > 1 ? `${MQTT_CLIENT_ID}-${index + 1}` : MQTT_CLIENT_ID,
      username: process.env['MQTT_USERNAME'],
      password: process.env['MQTT_PASSWORD'],
    });
    mqttClients.push(client);
    client.handleMessage = createDurableMqttHandleMessage(
      handleIncomingMqttPacket,
      (error) => reportMqttIngestFailure(client, error),
    );

    client.on('connect', () => {
      setMqttRuntimeStatus('connected');
      console.log(`[mqtt] connected${connectionLabel}`);
      for (const prefix of TOPIC_PREFIXES) {
        // A shared subscription spreads messages across the ingest
        // connections; a single connection keeps the historical direct
        // subscription and its topic-ACL behaviour exactly.
        const topicFilter = MQTT_INGEST_CONNECTIONS > 1
          ? `$share/${MQTT_INGEST_SHARE_GROUP}/${prefix}/#`
          : `${prefix}/#`;
        client.subscribe(topicFilter, { qos: 1 }, (err) => {
          if (err) console.error(`[mqtt] subscribe error (${topicFilter})`, err.message);
          else      console.log(`[mqtt] subscribed to ${topicFilter}${connectionLabel}`);
        });
      }
    });

    client.on('error', (err) => {
      setMqttRuntimeStatus('error', err.message);
      console.error('[mqtt] error', err.message);
    });
    client.on('offline', () => {
      if (!mqttClients.some((candidate) => candidate.connected)) {
        setMqttRuntimeStatus('disconnected');
      }
    });
    client.on('close', () => {
      if (!mqttClients.some((candidate) => candidate.connected)) {
        setMqttRuntimeStatus('disconnected');
      }
      void flushPacketBatch().catch((err: unknown) => {
        console.error('[mqtt] packet batch close flush failed:', (err as Error).message);
      });
    });
    client.on('reconnect', () => {
      setMqttRuntimeStatus('reconnecting');
      console.log(`[mqtt] reconnecting…${connectionLabel}`);
    });
  }
}

export async function stopMqttClient(timeoutMs = 20_000): Promise<void> {
  mqttStopping = true;
  const clients = mqttClients.splice(0, mqttClients.length);
  for (const client of clients) {
    client.removeAllListeners('reconnect');
    await client.endAsync(false).catch((error: unknown) => {
      console.error('[mqtt] disconnect failed:', error instanceof Error ? error.message : error);
    });
  }
  const deadline = Date.now() + timeoutMs;
  while (activeMqttIngests > 0 && Date.now() < deadline) {
    await new Promise((resolve) => setTimeout(resolve, 25));
  }
  if (activeMqttIngests > 0) {
    throw new Error(
      `MQTT_DRAIN_TIMEOUT:active=${activeMqttIngests}`,
    );
  }
  if (nodeFlushTimer) {
    clearTimeout(nodeFlushTimer);
    flushPendingNodeEmits();
  }
  await closePacketBatch();
  setMqttRuntimeStatus('disconnected');
}

async function handleMessage(topic: string, rawPayload: Buffer, redelivered = false): Promise<void> {
  const topicParts = parseMqttTopic(topic, TOPIC_PREFIXES, BLOCKED_PUBLIC_IATAS);
  if (!topicParts) {
    mqttIngestOutcomesTotal.inc({ outcome: 'invalid_topic' });
    return;
  }

  const { iata, observerKey, suffix, network } = topicParts;
  mqttMessagesTotal.inc({ network: boundedNetworkMetricLabel(network) });
  const rawStr = rawPayload.toString('utf8').trim();

  let json: Record<string, unknown>;
  try {
    const parsed = JSON.parse(rawStr) as unknown;
    if (!parsed || typeof parsed !== 'object' || Array.isArray(parsed)) throw new Error('payload_not_object');
    json = parsed as Record<string, unknown>;
  } catch {
    mqttIngestOutcomesTotal.inc({ outcome: 'invalid_json' });
    console.warn(`[mqtt] non-JSON payload on topic ${topic}: ${rawStr.slice(0, 80)}`);
    return;
  }

  const origin   = typeof json['origin'] === 'string' ? json['origin'] : undefined;
  if (suffix === 'status') {
    const model    = json['model']            as string | undefined;
    const firmware = json['firmware_version'] as string | undefined;

    if (!statusEnvelopeTargetsObserver(observerKey, json)) {
      mqttIngestOutcomesTotal.inc({ outcome: 'status_identity_mismatch' });
      console.warn('[mqtt] rejected status envelope with identity mismatch');
      return;
    }
    const nodeId = observerKey;
    const nodeIata = topicIataForNode(nodeId, observerKey, iata);
    const writes: Promise<unknown>[] = [upsertNode(nodeId, {
      name:            origin,
      iata:            nodeIata,
      publicKey:       observerKey,
      hardwareModel:   (model    && model    !== 'unknown') ? model    : undefined,
      firmwareVersion: (firmware && firmware !== 'unknown') ? firmware : undefined,
      network,
      allowTestOverride: network === 'test' && nodeId === observerKey,
      mqttObserver: true,
    })];
    writes.push(persistStatusTelemetrySample(
      json,
      { nodeId, network, allowRawStatsOnly: network === 'test' },
      insertNodeStatusSample,
    ));
    const writeResults = await Promise.allSettled(writes);
    const writeErrors: unknown[] = [];
    for (const result of writeResults) {
      if (result.status === 'rejected') {
        mqttIngestOutcomesTotal.inc({ outcome: 'status_persist_failure' });
        console.error(
          '[mqtt] status persistence error:',
          result.reason instanceof Error ? result.reason.message : String(result.reason),
        );
        writeErrors.push(result.reason);
      }
    }
    if (writeErrors.length > 0) throw writeErrors[0];
    markMqttPersistenceHealthy();
    emitNode(nodeId, { network, observerId: observerKey });
    return;
  }

  if (suffix === 'neighbors' || suffix === 'neighbours') {
    const neighbors = extractNeighborNodes(json);
    if (!neighbors) {
      mqttIngestOutcomesTotal.inc({ outcome: 'invalid_neighbors' });
      return;
    }
    try {
      await insertNodeNeighborSample({ nodeId: observerKey, network, neighbors });
      mqttIngestOutcomesTotal.inc({ outcome: 'neighbors_persisted' });
      markMqttPersistenceHealthy();
    } catch (error) {
      mqttIngestOutcomesTotal.inc({ outcome: 'neighbors_persist_failure' });
      console.error('[mqtt] neighbor persistence error:', error instanceof Error ? error.message : error);
      throw error;
    }
    return;
  }

  if (suffix !== 'packets') return;

  const packetType = toNum(json['packet_type']);
  const rssi       = toNum(json['RSSI']);
  const snr        = toNum(json['SNR']);
  const rawHex     = typeof json['raw'] === 'string' ? json['raw'] : '';
  const directionValue = typeof json['direction'] === 'string'
    ? json['direction'].trim().toLowerCase()
    : undefined;
  const direction = directionValue === 'rx' || directionValue === 'tx' ? directionValue : undefined;

  if (isEmptyPacketEnvelope(json, rawHex, packetType)) {
    mqttIngestOutcomesTotal.inc({ outcome: 'empty_packet' });
    return;
  }

  let resolvedPacketType = packetType;

  let innerPayload: Record<string, unknown> | undefined;
  let canonicalPacketId: string | undefined;
  let decodedHops: number | undefined;
  let decodedPathHashSizeBytes: number | undefined;
  let decodedRouteType: number | undefined;
  let decodedTransportCodes: string | undefined;
  let decodedRegionScope: string | undefined;
  let summary:     string | undefined;
  let srcNodeId:   string | undefined;
  let advertCount: number | undefined;

  let path: string[] | undefined;

  if (rawHex) {
    try {
      const compat = decodePacketCompat(rawHex, keyStore);
      const {
        decoded,
        pathHashes,
        pathHashCount,
        pathHashSize,
        routeType: decodedRT,
        transportCodes,
      } = compat;

      // A malformed path-length framing can shift the payload boundary. Do not
      // let metadata or payload decoded from such a frame affect persistence.
      if (decoded && compat.metadataValid) {
        resolvedPacketType = decoded.payloadType ?? resolvedPacketType;
        canonicalPacketId = compat.canonicalPacketId;
        decodedHops = pathHashCount;
        decodedPathHashSizeBytes = pathHashSize;
        decodedRouteType = decodedRT;
        if (pathHashes && pathHashes.length > 0) {
          path = pathHashes;
        }

        // Region scope — only present on TransportFlood (0) / TransportDirect (3) packets
        if (transportCodes && decoded.payload?.raw && REGION_PROBE_LIST.length > 0) {
          decodedTransportCodes = transportCodes;
          const tcBuf = Buffer.from(transportCodes, 'hex');
          if (tcBuf.length >= 2) {
            const tc0 = tcBuf[0]! | (tcBuf[1]! << 8);
            for (const { name } of REGION_PROBE_LIST) {
              if (transportCodeMatchesRegion(name, decoded.payloadType, decoded.payload.raw, tc0)) {
                decodedRegionScope = name;
                break;
              }
            }
          }
        }

        if (
          decodedPathHashSizeBytes != null
          && decodedPathHashSizeBytes > 1
          && decodedHops != null
          && (decodedPathHashSizeBytes * decodedHops) > 64
        ) {
          console.warn(
            `[mqtt] dropping impossible multibyte path metadata: hash=${canonicalPacketId ?? 'unknown'} size=${decodedPathHashSizeBytes} hops=${decodedHops} raw=${rawHex.slice(0, 24)}…`,
          );
          decodedHops = undefined;
          decodedPathHashSizeBytes = undefined;
          path = undefined;
        }

        const decodedInner = decoded.payload?.decoded;
        summary = buildSummary(decoded.payloadType, decodedInner, rawHex, channelEntries);

        if (decoded.payloadType === 4) {
          const inner     = decodedInner as unknown as Record<string, unknown> | undefined;
          const appData   = inner?.['appData'] as Record<string, unknown> | undefined;
          const loc       = appData?.['location'] as Record<string, number> | undefined;
          const senderKey = normalizeNodeId(inner?.['publicKey']);
          const nodeId    = senderKey ?? observerKey;
          const nodeIata  = topicIataForNode(nodeId, observerKey, iata);
          const advertSignatureValid = await verifyAdvertSignature(decoded);

          if (!advertSignatureValid) {
            mqttIngestOutcomesTotal.inc({ outcome: 'invalid_advert_signature' });
            console.warn(
              `[mqtt] ignoring advert identity with invalid signature: key=${senderKey?.slice(0, 16) ?? 'unknown'}…`,
            );
            summary = undefined;
          }

          if (advertSignatureValid && network !== 'test') {
            const advertName = appData?.['name'] as string | undefined;
            const advertLat  = loc?.['latitude'];
            const advertLon  = loc?.['longitude'];

            const evalResult = advertName && senderKey
              ? await evaluateAdvert({
                  name: advertName,
                  publicKey: senderKey,
                  srcNodeId: nodeId,
                  lat: advertLat,
                  lon: advertLon,
                  hopCount: decodedHops,
                  payloadTimestamp: inner?.['timestamp'] as number | undefined,
                  network,
                })
              : null;

            const isSpam    = evalResult?.verdict === 'spam';
            const isSuspect = evalResult?.verdict === 'suspect';

            if (evalResult && (isSpam || isSuspect)) {
              insertOrUpdateSpamSuspect({
                srcNodeId:    nodeId,
                spoofedName:  advertName ?? '',
                publicKey:    senderKey,
                claimedLat:   advertLat,
                claimedLon:   advertLon,
                canonicalKey: evalResult.canonicalKey,
                verdict:      evalResult.verdict,
                signals:      evalResult.signals,
                totalScore:   evalResult.totalScore,
                network,
              }).catch((err: Error) => console.error('[spam-detect] insertOrUpdateSpamSuspect error:', err.message));
              console.warn(
                `[spam-detect] ${evalResult.verdict.toUpperCase()} score=${evalResult.totalScore} ` +
                `name="${advertName}" key=${senderKey?.slice(0, 16)}… ` +
                `signals=${evalResult.signals.map((s) => s.name).join(',')}`
              );
            }

            if (!isSpam) {
              try {
                const nodeUpdate = await upsertNode(nodeId, {
                  name:      advertName,
                  lat:       advertLat,
                  lon:       advertLon,
                  role:      appData?.['deviceRole'] as number | undefined,
                  iata:      nodeIata,
                  publicKey: senderKey,
                  network,
                  advertHash: canonicalPacketId,
                });
                advertCount = nodeUpdate.advertCount;

                emitNodeUpsert({
                  node_id:      nodeId,
                  name:         advertName,
                  lat:          advertLat,
                  lon:          advertLon,
                  role:         appData?.['deviceRole'] as number | undefined,
                  iata,
                  network,
                  observer_id:  observerKey,
                  public_key:   senderKey,
                  last_seen:    new Date().toISOString(),
                  is_online:    true,
                  advert_count: advertCount,
                  coordinates_changed: nodeUpdate.coordinatesChanged,
                });
              } catch (err) {
                mqttIngestOutcomesTotal.inc({ outcome: 'advert_persist_failure' });
                console.error('[mqtt] advert node upsert failed:', (err as Error).message);
              }
            }
          }

          innerPayload = advertSignatureValid ? inner : undefined;
          srcNodeId = advertSignatureValid ? senderKey : undefined;
        } else if (decoded.payloadType === 5) {
          innerPayload = decodedInner as unknown as Record<string, unknown> | undefined;
        } else if (decoded.payloadType === 7) {
          const inner = decodedInner as unknown as Record<string, unknown> | undefined;
          srcNodeId = normalizeNodeId(inner?.['senderPublicKey']);
        } else if (decoded.payloadType === 1) {
          // Router/Advert packets - extract origin_id from payload
          const inner = decodedInner as unknown as Record<string, unknown> | undefined;
          srcNodeId = normalizeNodeId(inner?.['origin_id']);
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
  const discardUnverifiedTxAdvert = shouldDiscardUnverifiedTxAdvert({
    direction,
    packetType: resolvedPacketType,
    decodedAdvertPayload: hasDecodedAdvertPayload,
  });

  if (discardUnverifiedTxAdvert) {
    srcNodeId = undefined;
    summary = undefined;
    innerPayload = undefined;
  }

  if (resolvedPacketType == null) {
    mqttIngestOutcomesTotal.inc({ outcome: 'missing_packet_type' });
    return;
  }

  // Decoder messageHash is deliberately not used here: it is only a 32-bit
  // display hash and collides at normal packet volumes. The canonical identity
  // comes from validated route/path-invariant wire content.
  const finalHash = canonicalPacketId ?? upstreamPacketHash(json['hash']) ?? crypto.randomUUID();

  const dedupeKey = packetDedupeKey(finalHash, observerKey, decodedHops);

  if (isDuplicatePacket(finalHash, observerKey, decodedHops)) {
    // This marker is written only after the matching packet is committed.
    // A broker redelivery after a lost PUBACK confirms the durable outcome.
    markMqttPersistenceHealthy();
    mqttIngestOutcomesTotal.inc({ outcome: 'duplicate_packet' });
    return;
  }

  // Arbitration against concurrent copies of this packet. The claim below is
  // registered inside waitForInFlightPacket in the same microtask as its
  // re-check, so after a failed predecessor exactly one waiter becomes the
  // successor and the rest queue behind it (never two concurrent retries).
  let claimResolve!: () => void;
  let claimReject!: (error: unknown) => void;
  const claim = new Promise<void>((resolve, reject) => {
    claimResolve = resolve;
    claimReject = reject;
  });
  claim.catch(() => { /* waiters observe failure via their own handlers */ });

  const claimState = await waitForInFlightPacket(inFlightPackets, dedupeKey, claim);
  if (claimState === 'duplicate') {
    claimResolve(); // never registered; settle for tidiness
    markMqttPersistenceHealthy();
    mqttIngestOutcomesTotal.inc({ outcome: 'duplicate_packet' });
    return;
  }

  // Decoded payloads omit mctomqtt's envelope direction. Keep that metadata in
  // the stored JSON so RX/TX counts do not classify every decoded packet as RX.
  const packetPayload = innerPayload && direction
    ? { ...innerPayload, direction }
    : (innerPayload ?? json);

  const livePacket: LivePacket = {
    id:         crypto.randomUUID(),
    packetHash: finalHash,
    rxNodeId:   observerKey,
    srcNodeId,
    topic,
    iata,
    network,
    packetType: resolvedPacketType,
    routeType:  decodedRouteType,
    hopCount:   decodedHops,
    pathHashSizeBytes: decodedPathHashSizeBytes,
    direction,
    summary,
    payload:    packetPayload,
    path,
    advertCount,
    transportCodes: decodedTransportCodes,
    regionScope:    decodedRegionScope,
    ts:         Date.now(),
  };

  const persistOnce = async (): Promise<void> => {
    const persistInput = {
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
      payload:    packetPayload,
      summary,
      rawHex,
      advertCount,
      pathHashes: path,
      network,
      transportCodes: decodedTransportCodes,
      regionScope:    decodedRegionScope,
    };
    const visibility = MQTT_INGEST_MODE === 'ram'
      ? await admitPacketToRam(persistInput)
      : await insertPacket(persistInput);
    rememberPacket(finalHash, observerKey, decodedHops);
    if (MQTT_INGEST_MODE !== 'ram') markMqttPersistenceHealthy();
    emitNode(observerKey, { network, observerId: observerKey });
    livePacket.isPrivate = visibility.isPrivate;
    livePacket.visibilityOk = visibility.visibilityOk;
    try {
      emit(livePacket, json);
    } catch (error) {
      console.error(
        '[mqtt] packet subscriber failed after durable ingest:',
        error instanceof Error ? error.message : String(error),
      );
    }
    invalidateResolveCache(finalHash);
    mqttIngestOutcomesTotal.inc({
      outcome: MQTT_INGEST_MODE === 'ram' ? 'packet_admitted' : 'packet_persisted',
    });
  };

  const ingest = (async () => {
    // Claimed above. After a restart there is no in-memory dedup state to
    // match against: a QoS 1 redelivery whose original commit survived the
    // crash must not be persisted or counted a second time. Confirm against
    // the retained window before writing (inside the claim so the registered
    // claim promise covers this path too).
    if (redelivered && await packetAlreadyPersisted(finalHash, observerKey, topic, network)) {
      markMqttPersistenceHealthy();
      mqttIngestOutcomesTotal.inc({ outcome: 'duplicate_packet' });
      return;
    }
    try {
      await persistOnce();
    } catch (err) {
      mqttIngestOutcomesTotal.inc({
        outcome: MQTT_INGEST_MODE === 'ram' ? 'packet_admission_failure' : 'packet_persist_failure',
      });
      console.error(
        MQTT_INGEST_MODE === 'ram' ? '[mqtt] ram admission failed' : '[mqtt] db insert failed',
        err instanceof Error ? err.message : String(err),
      );
      throw err;
    }
    // Best-effort live-map follow-ups run once the durable write is done and
    // outside the acknowledgement path: PUBACK is released as soon as the
    // write commits and these resolve asynchronously. The in-flight count is
    // bounded so a database stall cannot grow them without limit; over the
    // bound they are dropped (best-effort) and counted.
    if (postPersistInFlight >= POST_PERSIST_MAX_INFLIGHT) {
      postPersistDroppedTotal.inc();
    } else {
      postPersistInFlight++;
      postPersistInFlightGauge.set(postPersistInFlight);
      void runPostPersistFollowUps({
        finalHash,
        decodedPathHashSizeBytes,
        path,
        decodedRouteType,
        network,
        resolvedPacketType,
      }).catch((err: unknown) => {
        console.error(
          '[mqtt] post-persist follow-up failed:',
          err instanceof Error ? err.message : String(err),
        );
      }).finally(() => {
        postPersistInFlight--;
        postPersistInFlightGauge.set(postPersistInFlight);
      });
    }
  })();

  void ingest.then(
    () => claimResolve(),
    (err: unknown) => claimReject(err),
  );
  try {
    await ingest;
  } finally {
    if (inFlightPackets.get(dedupeKey) === claim) inFlightPackets.delete(dedupeKey);
  }
}

/**
 * Best-effort live-map work that does not need to hold the QoS 1
 * acknowledgement: multibyte path evidence, slow-mode scheduling and cache
 * warm-up all resolve after the durable write and run concurrently with the
 * next message's persistence.
 */
async function runPostPersistFollowUps(args: {
  finalHash: string;
  decodedPathHashSizeBytes: number | undefined;
  path: string[] | undefined;
  decodedRouteType: number | undefined;
  network: string;
  resolvedPacketType: number | undefined;
}): Promise<void> {
  const { finalHash, decodedPathHashSizeBytes, path, decodedRouteType, network, resolvedPacketType } = args;

  // A repeater appearing in a multibyte (2–3 byte) path hash almost certainly
  // relayed this packet. Resolve only a unique historic row with stored
  // coordinates, then broadcast the full row so clients which no longer hold
  // the stale node can restore it at its original location. Best-effort.
  if (decodedPathHashSizeBytes != null && decodedPathHashSizeBytes >= 2 && path && path.length > 0) {
    try {
      const historicNodes = await recordMultibyteEvidence(
        path,
        decodedPathHashSizeBytes,
        new Date(),
        decodedRouteType,
        network,
      );
      for (const node of historicNodes) emitNodeUpsert({ ...node });
    } catch (err) {
      console.error('[mqtt] recordMultibyteEvidence error:', (err as Error).message);
    }
    // Slow mode: schedule ONE final multi-observer resolution after the
    // propagation window closes so the complete observer set is included
    // (idempotent per packet hash; best-effort).
    if (path.length >= pathingConfig.slowModeMinPathHops) {
      scheduleSlowResolution(finalHash, network);
    }
  }

  // Pre-resolve path for ADV/GRP packets so the cache is warm before the browser requests it.
  // Bursty multi-observer packets can arrive repeatedly for the same hash; throttle those
  // warmups so live ingest cannot monopolise the shared path resolver pool.
  if (!PRE_RESOLVE_ENABLED || (resolvedPacketType !== 4 && resolvedPacketType !== 5)) return;
  const preResolveKey = `${finalHash}|${network}`;
  const lastPreResolve = preResolveLastStarted.get(preResolveKey) ?? 0;
  if (preResolveInFlight.has(preResolveKey) || Date.now() - lastPreResolve < PRE_RESOLVE_MIN_INTERVAL_MS) {
    return;
  }
  preResolveInFlight.add(preResolveKey);
  if (
    !preResolveLastStarted.has(preResolveKey)
    && preResolveLastStarted.size >= PRE_RESOLVE_HISTORY_MAX
  ) {
    const oldest = preResolveLastStarted.keys().next().value;
    if (oldest !== undefined) preResolveLastStarted.delete(oldest);
  }
  preResolveLastStarted.set(preResolveKey, Date.now());
  const ck = `m|${finalHash}|${network}`;
  // Pass any previously resolved high-confidence hop assignments so the solver reuses them
  const stickyEntry = getStickyNodeMap(finalHash, network);
  const stickyMap = stickyEntry ? Object.fromEntries(stickyEntry.hashToNodeId) : undefined;
  const stickyAgeFraction = stickyEntry?.ageFraction;
  const heldPath = getHeldPath(finalHash, network);
  resolvePool.runBackground<{
    stickyUpdates?: Record<string, string>;
    canonicalPath?: Array<{ nodeId?: string | null }>;
  }>({ type: 'resolveMulti', packetHash: finalHash, network, ...(stickyMap ? { stickyMap, stickyAgeFraction } : {}), ...(heldPath ? { heldPath } : {}) })
    .then((result) => {
      if (result) {
        const { stickyUpdates, ...cacheableResult } = result;
        setResolveCache(ck, cacheableResult);
        if (stickyUpdates && Object.keys(stickyUpdates).length > 0) {
          mergeStickyNodes(finalHash, network, stickyUpdates);
        }
        const heldNodes = result.canonicalPath?.map((hop) => hop.nodeId ?? '').filter(Boolean) ?? [];
        if (heldNodes.length > 0 && heldNodes.length === result.canonicalPath?.length) {
          setHeldPath(finalHash, network, { path: heldNodes, resolvedAt: Date.now(), physical: true });
        }
      }
    })
    .catch(() => { /* best-effort */ })
    .finally(() => { preResolveInFlight.delete(preResolveKey); });
}

export async function backfillHistoricalLinks(
  queueFn: (
    packetHash: string,
    rxNodeId: string,
    srcNodeId: string | undefined,
    path: string[],
    hopCount: number | undefined,
    pathHashSizeBytes: number | undefined,
  ) => void | Promise<unknown>,
  options: { windowEnd?: Date; batchSize?: number } = {},
): Promise<void> {
  const windowEnd = options.windowEnd ?? new Date();
  const batchSize = Math.min(10_000, Math.max(100, Math.floor(options.batchSize ?? 2_000)));
  let cursor = '';
  let queued = 0;
  while (true) {
    const res = await query<{
      packet_hash: string; rx_node_id: string; src_node_id: string | null; hop_count: number | null; raw_hex: string;
    }>(
      `SELECT DISTINCT ON (packet_hash)
         packet_hash, rx_node_id, src_node_id, hop_count, raw_hex
       FROM packets
       WHERE time < $1
         AND packet_hash > $2
         AND rx_node_id IS NOT NULL
         AND raw_hex IS NOT NULL
         AND raw_hex != ''
       ORDER BY packet_hash, time DESC
       LIMIT $3`,
      [windowEnd, cursor, batchSize],
    );
    if (res.rows.length === 0) break;
    for (const row of res.rows) {
      let compat: ReturnType<typeof decodePacketCompat>;
      try {
        compat = decodePacketCompat(row.raw_hex, keyStore);
      } catch {
        // Skip undecipherable packets
        continue;
      }
      if (compat.metadataValid && (compat.pathHashSize ?? 1) > 1 && compat.pathHashes && compat.pathHashes.length > 0) {
        // Admission failures are operational failures, not decode failures.
        // Let the privileged caller abort rather than publish a partial graph.
        await queueFn(
          row.packet_hash,
          row.rx_node_id,
          row.src_node_id ?? undefined,
          compat.pathHashes,
          compat.pathHashCount,
          compat.pathHashSize,
        );
        queued++;
      }
    }
    cursor = res.rows.at(-1)!.packet_hash;
    if (res.rows.length < batchSize) break;
  }
  console.log(`[app] historical link backfill: queued ${queued} packets`);
}
