import type { IngestEvents } from './events.js';
import type { CommittedFollowUps } from './committedFollowUps.js';
import { createPublicPolicyReader } from '../privacy/policyReader.js';
import { boundedEnvInteger } from '../platform/config/boundedNumber.js';
import { createHash } from 'node:crypto';
import { calcRegionKey, transportCodeMatchesRegion } from '@michaelhart/meshcore-decoder';
import { insertNodeNeighborSample, insertNodeStatusSample, upsertNode } from '../repositories/nodeState.js';
import { insertPacket } from '../db/packetPersistence.js';
import { insertOrUpdateSpamSuspect } from '../spam/suspectRepository.js';
import { query, ingestQuery } from '../db/runtime.js';
import { evaluateAdvert } from '../mqtt/spamDetector.js';
import { invalidateResolveCache } from '../path-beta/resolveCache.js';
import type { LivePacket } from '../types/index.js';
import { decodePacketCompat, verifyAdvertSignature } from '../mqtt/decodePacket.js';
import { buildChannelEntries, buildCombinedKeyStore, buildSummary } from '../mqtt/channelRegistry.js';
import { shouldDiscardUnverifiedTxAdvert, statusEnvelopeTargetsObserver } from '../mqtt/identityBinding.js';
import { extractNeighborNodes } from '../mqtt/neighborPayload.js';
import { parseMqttTopic } from '../mqtt/topic.js';
import { validPacketIntegers } from '../mqtt/envelopeSafety.js';
import { isPermanentPersistenceError } from '../platform/persistenceErrors.js';
import { waitForInFlightPacket } from '../mqtt/dedupeRegistry.js';
import { admitPacketToRam } from '../ram/liveStore.js';
import { getRamIngestMode } from '../ram/protocol.js';
import { persistStatusTelemetrySample, persistStatusWrites, statusObservationTime } from '../mqtt/statusTelemetry.js';
import { buildPersistedPacketPayload, ingestFailureAction, packetEnvelopeViolations, statusEnvelopeViolations, stripDerivedPacketFields, type IntegerDomainViolation } from '../mqtt/envelopeSafety.js';
import { fallbackPacketIdentity, ramMqttDeliveryKey, observationIdentityKey, packetAlreadyPersisted, type ObservationIdentity } from '../db/observationIdentity.js';
import { createPublicBatch } from '../privacy/publicBatch.js';
import { boundedNetworkMetricLabel } from '../metrics/labels.js';
import { mqttIngestAckDuration, mqttIngestActive, mqttIngestOutcomesTotal, mqttMessagesTotal, mqttDedupEntries } from '../metrics/ingest.js';
import { publicLiveOutcomesTotal } from '../metrics/websocket.js';

export function createPacketProcessor(deps: { clientId: string; events: IngestEvents; markMqttPersistenceHealthy: () => void; committedFollowUps: CommittedFollowUps }) {
  const { emitCommittedPacket: emit, emitNode, emitNodeUpsert } = deps.events;
  const schedulePostPersistFollowUps = deps.committedFollowUps.schedule;
  const markMqttPersistenceHealthy = deps.markMqttPersistenceHealthy;
  let closed = false;
  const MQTT_CLIENT_ID = deps.clientId;
  const MQTT_INGEST_MODE = getRamIngestMode();
  const publicPackets = createPublicBatch<{ packet: LivePacket; publish: () => void }>({
    maxItems: 10_000, maxBytes: 8 * 1_048_576,
    loadPolicy: createPublicPolicyReader(ingestQuery),
    refused: (outcome, count) => publicLiveOutcomesTotal.inc({ phase: 'mqtt', outcome }, count),
    deliver: (items, policy) => {
      for (const { packet, publish } of items) {
        const current = policy.classify({ network: packet.network ?? '',
          rxNodeId: packet.rxNodeId ?? null, srcNodeId: packet.srcNodeId ?? null,
          pathHashes: packet.path ?? null, pathHashSizeBytes: packet.pathHashSizeBytes ?? null });
        publicLiveOutcomesTotal.inc({ phase: 'mqtt', outcome: current.visibilityOk ? 'authorized' : 'private' });
        if (current.visibilityOk) { packet.isPrivate = false; packet.visibilityOk = true; publish(); }
      }
    },
  });
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

  /**
   * Process-local observation dedup. The same identity is used for claim arbitration,
   * batch collapse and the persisted DUP probe. Entries expire after 120 seconds.
   */
  const seenPackets = new Map<string, number>();
  const SEEN_PACKETS_MAX = 50_000;
  const SEEN_PACKETS_TTL_MS = 120_000;

  let dedupSweepTimer: ReturnType<typeof setInterval> | undefined;

  function startDedupMaintenance() {
    if (closed || dedupSweepTimer) return;
  dedupSweepTimer = setInterval(() => {
    const cutoff = Date.now() - SEEN_PACKETS_TTL_MS;
    for (const [k, ts] of seenPackets) {
      if (ts < cutoff) seenPackets.delete(k);
    }
    mqttDedupEntries.set(seenPackets.size);
  }, 60_000).unref();

  }

  function isDuplicatePacket(key: string): boolean {
    return seenPackets.has(key);
  }

  function rememberPacket(key: string): void {
    if (seenPackets.size >= SEEN_PACKETS_MAX) {
      const oldest = seenPackets.keys().next().value;
      if (oldest !== undefined) seenPackets.delete(oldest);
    }
    seenPackets.set(key, Date.now());
    mqttDedupEntries.set(seenPackets.size);
  }

  /**
   * Packets currently being persisted, keyed like `seenPackets`. A second copy
   * of the same packet arriving while the first is still in flight (possible
   * once more than one connection can ingest concurrently) waits for the first
   * attempt: success resolves this copy as a duplicate, failure leaves nothing
   * committed so this copy persists on its own.
   */
  const inFlightPackets = new Map<string, Promise<void>>();
  /**
   * Channel registry — built once at startup.
   * MESHCORE_CHANNEL_SECRETS supports 'name:hex' or bare 'hex' entries (comma-separated).
   * The default public channel is always included.
   */
  const channelEntries = buildChannelEntries(process.env['MESHCORE_CHANNEL_SECRETS']);

  // Combined keyStore used for decryption (all secrets, single decode call per packet)
  const keyStore = buildCombinedKeyStore(channelEntries);

  const MQTT_MAX_PAYLOAD_BYTES = boundedEnvInteger('MQTT_MAX_PAYLOAD_BYTES', 64 * 1024, 1_024, 1_048_576);
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

  /**
   * A deterministic data error cannot be fixed by redelivery: the same insert
   * fails on every attempt while the PUBACK is withheld, which is an ingest
   * livelock for that topic. Count the drop, log it, and acknowledge so the
   * broker discards the message. Transient failures never reach this path.
   */
  function quarantineEnvelope(kind: 'packet' | 'status', violations: IntegerDomainViolation[]): void {
    mqttIngestOutcomesTotal.inc({ outcome: `${kind}_quarantined` });
    console.warn(
      `[mqtt] quarantined ${kind} envelope with out-of-domain field(s): ` +
      violations.map((violation) => `${violation.field}=${String(violation.value)} (${violation.reason})`).join(', '),
    );
  }

  function quarantinePersistenceFailure(error: unknown): void {
    mqttIngestOutcomesTotal.inc({ outcome: 'packet_quarantined' });
    const message = error instanceof Error ? error.message : String(error);
    console.error('[mqtt] quarantined packet after deterministic persistence error:', message);
  }

  function handleIncomingMqttPacket(packet: { topic: string; payload: Buffer | string; dup?: boolean; messageId?: number; qos?: number; retain?: boolean }): Promise<void> {
    if (closed) return Promise.reject(new Error('MQTT_PACKET_PROCESSOR_CLOSED'));
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
    return handleMessage(packet.topic, rawPayload, packet.dup === true, packet.messageId, packet.qos ?? 1, packet.retain === true)
      .catch(async (error: unknown) => {
        if (!isPermanentPersistenceError(error)) throw error;
        // Quarantine evidence must commit before this delivery can be acknowledged.
        await query(`INSERT INTO ops.ingest_rejections AS ir (message_hash, topic, reason, payload_bytes)
          VALUES ($1, $2, $3, $4) ON CONFLICT (message_hash) DO UPDATE
          SET last_seen_at = NOW(), occurrences = ir.occurrences + 1`, [
          createHash('sha256').update(packet.topic).update(rawPayload).digest('hex'),
          packet.topic.replaceAll('\u0000', '').slice(0, 512),
          `SQLSTATE:${(error as { code: string }).code}`, rawPayload.length,
        ]);
        mqttIngestOutcomesTotal.inc({ outcome: 'packet_quarantined' });
      })
      .then(() => mqttIngestOutcomesTotal.inc({ outcome: 'processed' }))
      .finally(() => {
        mqttIngestAckDuration.observe(Number(process.hrtime.bigint() - startedAt) / 1e9);
        activeMqttIngests -= 1;
        mqttIngestActive.set(activeMqttIngests);
      });
  }

  async function handleMessage(
    topic: string,
    rawPayload: Buffer,
    redelivered = false,
    messageId?: number,
    qos = 1,
    retained = false,
  ): Promise<void> {
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
      const observedAt = statusObservationTime(json, { retained });
      if (!observedAt) return;
      const recentStatus = Date.now() - Date.parse(observedAt) <= 15 * 60_000;
      const nodeIata = topicIataForNode(nodeId, observerKey, iata);
      const writes: Promise<unknown>[] = recentStatus ? [upsertNode(nodeId, {
        name:            origin,
        iata:            nodeIata,
        publicKey:       observerKey,
        hardwareModel:   (model    && model    !== 'unknown') ? model    : undefined,
        firmwareVersion: (firmware && firmware !== 'unknown') ? firmware : undefined,
        network,
        allowTestOverride: network === 'test' && nodeId === observerKey,
        mqttObserver: true,
      })] : [];
      writes.push(persistStatusTelemetrySample(
        json,
        { nodeId, network, allowRawStatsOnly: network === 'test', retained },
        async (sample) => {
          // Validate the values actually destined for integer columns after the
          // deployed firmware normalization; raw stats retain fractional detail.
          const violations = statusEnvelopeViolations({ stats: {
            battery_mv: sample.batteryMv, uptime_secs: sample.uptimeSecs,
            tx_air_secs: sample.txAirSecs, rx_air_secs: sample.rxAirSecs,
          } });
          if (violations.length) { quarantineEnvelope('status', violations); return; }
          await insertNodeStatusSample(sample);
        },
      ));
      try {
        await persistStatusWrites(writes, (code) => {
          mqttIngestOutcomesTotal.inc({ outcome: 'status_telemetry_rejected' });
          console.warn('[mqtt] rejected status data:', { nodeId, code });
        });
      } catch (error) {
        mqttIngestOutcomesTotal.inc({ outcome: 'status_persist_failure' });
        console.error('[mqtt] status persistence error:', error instanceof Error ? error.message : error);
        throw error;
      }
      markMqttPersistenceHealthy();
      if (recentStatus) emitNode(nodeId, { network, observerId: observerKey });
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

    if (!validPacketIntegers({ packetType: resolvedPacketType, routeType: decodedRouteType,
      hopCount: decodedHops, pathHashSizeBytes: decodedPathHashSizeBytes, advertCount })) {
      throw Object.assign(new Error('Invalid packet integer field'), { code: '22003' });
    }
    if (resolvedPacketType == null) {
      mqttIngestOutcomesTotal.inc({ outcome: 'missing_packet_type' });
      return;
    }

    // Validate against the integer columns the values are inserted into before
    // any write: a fractional packet_type (22P02) or an int4 overflow (22003)
    // fails on every redelivery. Rejecting here keeps the bad row out of the
    // shared packet batch, where one failing row would reject its batch-mates.
    const packetViolations = packetEnvelopeViolations({
      packetType: resolvedPacketType,
      hopCount: decodedHops,
      pathHashSizeBytes: decodedPathHashSizeBytes,
      advertCount,
      snr,
      rssi,
    });
    if (packetViolations.length > 0) {
      quarantineEnvelope('packet', packetViolations);
      return;
    }

    // Decoder messageHash is deliberately not used here: it is only a 32-bit
    // display hash and collides at normal packet volumes. The canonical identity
    // comes from validated route/path-invariant wire content.
    const finalHash = canonicalPacketId ?? upstreamPacketHash(json['hash']) ?? fallbackPacketIdentity(rawPayload);

    const observationIdentity: ObservationIdentity = {
      packetHash: finalHash,
      rxNodeId: observerKey,
      topic,
      network,
      hopCount: decodedHops ?? null,
      routeType: decodedRouteType ?? null,
      pathHashes: path ?? null,
      pathHashSizeBytes: decodedPathHashSizeBytes ?? null,
      direction: direction ?? null,
    };
    const dedupeKey = observationIdentityKey(observationIdentity);
    const ramDeliveryKey = MQTT_INGEST_MODE === 'ram'
      ? ramMqttDeliveryKey({ clientId: MQTT_CLIENT_ID, messageId, qos, topic,
        payload: rawPayload, observationKey: dedupeKey })
      : null;

    if (isDuplicatePacket(dedupeKey)) {
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
    // Without a verified decode the envelope itself is stored, so publisher-owned
    // `_summary`/`decrypted` keys are removed rather than persisted as if the
    // decoder had produced them (they also feed `companion_sender`).
    const packetPayload = buildPersistedPacketPayload(innerPayload, json, direction);
    // Live consumers receive the upstream envelope too; hand them the same
    // sanitised view so a forged summary is never forwarded as decoded content.
    const upstreamEnvelope = stripDerivedPacketFields(json);

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
        forwarding: { packet: livePacket, upstreamEnvelope },
        packetHash: finalHash,
        packetUid: canonicalPacketId ?? null,
        identityVersion: canonicalPacketId ? 'wire-sha256-v1' : null,
        identityProvenance: canonicalPacketId ? 'meshcore-decoded-wire' : 'no-verified-wire-content',
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
        ? await admitPacketToRam(persistInput, { key: ramDeliveryKey!, redelivered })
        : await insertPacket(persistInput);
      rememberPacket(dedupeKey);
      if (MQTT_INGEST_MODE !== 'ram') markMqttPersistenceHealthy();
      livePacket.isPrivate = visibility.isPrivate;
      livePacket.visibilityOk = visibility.visibilityOk;
      const publishLive = (): void => {
        emitNode(observerKey, { network, observerId: observerKey });
        try {
          emit(livePacket, upstreamEnvelope);
        } catch (error) {
          console.error(
            '[mqtt] packet subscriber failed after ingest:',
            error instanceof Error ? error.message : String(error),
          );
        }
      };
      if (MQTT_INGEST_MODE === 'ram') {
        publicPackets.enqueue({ packet: livePacket, publish: publishLive },
          Buffer.byteLength(JSON.stringify([livePacket, upstreamEnvelope])));
      } else publishLive();
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
      if (redelivered && await packetAlreadyPersisted(
        (sql, params) => ingestQuery<{ present: boolean }>(sql, params),
        observationIdentity,
      )) {
        markMqttPersistenceHealthy();
        mqttIngestOutcomesTotal.inc({ outcome: 'duplicate_packet' });
        return;
      }
      try {
        await persistOnce();
      } catch (err) {
        // A deterministic data error (SQLSTATE class 22 data_exception / class 23
        // integrity_constraint_violation) fails identically on every redelivery.
        // Withholding the PUBACK for it is an ingest livelock, so quarantine the
        // message, remember the observation (a lost-PUBACK replay is then a
        // duplicate rather than a second attempt) and acknowledge. Transient
        // failures still leave the message unacknowledged for redelivery.
        if (ingestFailureAction(err) === 'ack_quarantine') {
          quarantinePersistenceFailure(err);
          rememberPacket(dedupeKey);
          return;
        }
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
      // RAM mode releases the MQTT acknowledgement at admission, far earlier
      // than the batch commit. Commit-dependent live-map work must not run
      // against rows that are not in PostgreSQL yet, so it is deferred to the
      // RAM writer's post-commit hook (see ram/writerRuntime.ts). The durable
      // path keeps running it right here, after the committed write.
      if (MQTT_INGEST_MODE !== 'ram') {
        schedulePostPersistFollowUps({
          finalHash,
          decodedPathHashSizeBytes,
          path,
          decodedRouteType,
          network,
          resolvedPacketType,
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

  return {
    process: handleIncomingMqttPacket,
    start: startDedupMaintenance,
    activeCount: () => activeMqttIngests,
    topicPrefixes: TOPIC_PREFIXES as ReadonlySet<string>,
    channelNames: channelEntries.map(entry => entry.name),
    maxPayloadBytes: MQTT_MAX_PAYLOAD_BYTES,
    async close() {
      if (closed) return;
      closed = true;
      if (dedupSweepTimer) clearInterval(dedupSweepTimer);
      dedupSweepTimer = undefined;
      seenPackets.clear(); mqttDedupEntries.set(0);
      await publicPackets.close();
    },
  };
}
