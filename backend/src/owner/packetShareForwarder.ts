import mqtt, { type MqttClient } from 'mqtt';
import { randomUUID } from 'node:crypto';
import type { LivePacket } from '../types/index.js';
import {
  buildPacketSharePayload,
  destinationConfigured,
  isForwardablePacket,
  isValidPacketShareBrokerUrl,
  packetShareEventKey,
  renderPacketShareTopic,
  type PacketShareDestinationRow,
} from './packetShareConfig.js';
import { decryptPacketShareSecret, hasPacketShareEncryptionKey } from './packetShareCrypto.js';
import type { PacketShareDeliveryRow, PacketShareRepository } from './packetShareRepository.js';
import { packetShareOutcomesTotal, packetShareQueueDepth } from '../metrics.js';

const DEFAULT_POLL_INTERVAL_MS = 5_000;
const DEFAULT_BATCH_SIZE = 25;
const PUBLISH_TIMEOUT_MS = 10_000;

export type PacketSharePublisher = {
  publish: (topic: string, payload: string) => Promise<void>;
  close: () => Promise<void>;
};

export type PacketSharePublisherFactory = (
  destination: PacketShareDestinationRow,
) => PacketSharePublisher;

function positiveIntegerSetting(name: string, fallback: number, max: number): number {
  const parsed = Number(process.env[name]);
  return Number.isFinite(parsed) && parsed > 0 ? Math.min(max, Math.floor(parsed)) : fallback;
}

function destinationUrl(destination: PacketShareDestinationRow): URL {
  if (!destination.broker_url) throw new Error('destination broker URL is missing');
  if (!isValidPacketShareBrokerUrl(destination.broker_url)) {
    throw new Error('destination broker URL must use mqtt, mqtts, ws, or wss without credentials');
  }
  const url = new URL(destination.broker_url.trim());
  return url;
}

function destinationFingerprint(destination: PacketShareDestinationRow): string {
  return JSON.stringify([
    destination.broker_url,
    destination.topic_template,
    destination.broker_username,
    destination.broker_password_ciphertext,
  ]);
}

function connectMqttPublisher(destination: PacketShareDestinationRow): PacketSharePublisher {
  let client: MqttClient | null = null;
  let connecting: Promise<MqttClient> | null = null;

  const closeClient = async (force = true): Promise<void> => {
    const current = client;
    client = null;
    connecting = null;
    if (current) await current.endAsync(force).catch(() => undefined);
  };

  const connect = async (): Promise<MqttClient> => {
    if (client?.connected) return client;
    if (connecting) return connecting;
    const url = destinationUrl(destination);
    const encryptedPassword = destination.broker_password_ciphertext?.trim() || '';
    const password = encryptedPassword
      ? decryptPacketShareSecret(encryptedPassword)
      : undefined;
    const created = mqtt.connect(url.toString(), {
      clientId: `meshcore-share-${randomUUID().replaceAll('-', '').slice(0, 24)}`,
      username: destination.broker_username?.trim() || undefined,
      password,
      clean: true,
      reconnectPeriod: 0,
      connectTimeout: PUBLISH_TIMEOUT_MS,
    });
    client = created;
    created.on('error', (error) => {
      console.error(`[packet-share] MQTT destination ${destination.destination_id} error:`, error.message);
    });
    connecting = new Promise<MqttClient>((resolve, reject) => {
      let settled = false;
      let timer: NodeJS.Timeout;
      const cleanup = () => {
        clearTimeout(timer);
        created.removeListener('connect', onConnect);
        created.removeListener('close', onClose);
      };
      const finish = (error?: Error) => {
        if (settled) return;
        settled = true;
        cleanup();
        if (error) {
          if (client === created) client = null;
          created.end(true);
          reject(error);
        } else {
          resolve(created);
        }
      };
      const onConnect = () => finish();
      const onClose = () => finish(new Error('destination MQTT connection closed before connect'));
      timer = setTimeout(() => finish(new Error('destination MQTT connection timed out')), PUBLISH_TIMEOUT_MS);
      created.once('connect', onConnect);
      created.once('close', onClose);
    }).finally(() => {
      connecting = null;
    });
    return connecting;
  };

  return {
    async publish(topic: string, payload: string): Promise<void> {
      const current = await connect();
      await current.publishAsync(topic, payload, { qos: 1 });
      if (!current.connected) {
        await closeClient();
        throw new Error('destination MQTT connection closed after publish');
      }
    },
    close: closeClient,
  };
}

export type PacketShareForwarderDeps = {
  repository: PacketShareRepository;
  publisherFactory?: PacketSharePublisherFactory;
  pollIntervalMs?: number;
  batchSize?: number;
};

export function createPacketShareForwarder(deps: PacketShareForwarderDeps) {
  const publisherFactory = deps.publisherFactory ?? connectMqttPublisher;
  const pollIntervalMs = deps.pollIntervalMs ?? positiveIntegerSetting(
    'OWNER_PACKET_SHARE_POLL_INTERVAL_MS', DEFAULT_POLL_INTERVAL_MS, 60_000,
  );
  const batchSize = deps.batchSize ?? positiveIntegerSetting(
    'OWNER_PACKET_SHARE_BATCH_SIZE', DEFAULT_BATCH_SIZE, 100,
  );
  const publishers = new Map<string, { publisher: PacketSharePublisher; fingerprint: string }>();
  let pollTimer: NodeJS.Timeout | null = null;
  let processing: Promise<void> | null = null;
  let stopping = false;

  async function publisherFor(destination: PacketShareDestinationRow): Promise<PacketSharePublisher> {
    const fingerprint = destinationFingerprint(destination);
    const existing = publishers.get(destination.destination_id);
    if (existing?.fingerprint === fingerprint) return existing.publisher;
    if (existing) {
      publishers.delete(destination.destination_id);
      await existing.publisher.close().catch(() => undefined);
    }
    const created = publisherFactory(destination);
    publishers.set(destination.destination_id, { publisher: created, fingerprint });
    return created;
  }

  async function enqueuePacket(packet: LivePacket, upstreamEnvelope: Record<string, unknown>): Promise<void> {
    if (!isForwardablePacket(packet)) {
      packetShareOutcomesTotal.inc({ outcome: 'skipped' });
      return;
    }
    const targets = await deps.repository.activeTargets(packet.rxNodeId!);
    if (targets.length < 1) return;

    for (const target of targets) {
      if (!destinationConfigured(target, hasPacketShareEncryptionKey())) {
        packetShareOutcomesTotal.inc({ outcome: 'unconfigured' });
        continue;
      }
      let destinationTopic: string;
      try {
        destinationTopic = renderPacketShareTopic(target.topic_template!, packet);
      } catch (error) {
        packetShareOutcomesTotal.inc({ outcome: 'invalid_destination' });
        console.error('[packet-share] invalid destination topic:', error instanceof Error ? error.message : error);
        continue;
      }
      try {
        const eventKey = packetShareEventKey(target.owner_username, target.destination_id, packet);
        const payload = buildPacketSharePayload(packet, upstreamEnvelope);
        const inserted = await deps.repository.enqueueDelivery({
          ownerUsername: target.owner_username,
          nodeId: packet.rxNodeId!,
          destinationId: target.destination_id,
          eventKey,
          packet,
          destinationTopic,
          payload: payload as unknown as Record<string, unknown>,
        });
        packetShareOutcomesTotal.inc({ outcome: inserted ? 'queued' : 'duplicate' });
      } catch (error) {
        packetShareOutcomesTotal.inc({ outcome: 'queue_error' });
        console.error('[packet-share] queue error:', error instanceof Error ? error.message : error);
      }
    }
    void processDue();
  }

  async function deliver(row: PacketShareDeliveryRow, claimToken: string): Promise<void> {
    const destination = await deps.repository.getDestination(row.destination_id);
    if (!destination || !destinationConfigured(destination, hasPacketShareEncryptionKey())) {
      await deps.repository.markFailed(row.id, claimToken, row.attempts, 'destination is missing or incomplete');
      packetShareOutcomesTotal.inc({ outcome: 'delivery_error' });
      return;
    }
    try {
      const publisher = await publisherFor(destination);
      await publisher.publish(row.destination_topic, JSON.stringify(row.payload));
      await deps.repository.markSucceeded(row.id, claimToken);
      packetShareOutcomesTotal.inc({ outcome: 'delivered' });
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      const entry = publishers.get(row.destination_id);
      if (entry) {
        publishers.delete(row.destination_id);
        await entry.publisher.close().catch(() => undefined);
      }
      await deps.repository.markFailed(row.id, claimToken, row.attempts, message);
      packetShareOutcomesTotal.inc({ outcome: 'delivery_error' });
    }
  }

  async function processDue(): Promise<void> {
    if (stopping) return;
    if (processing) {
      await processing;
      return;
    }
    processing = (async () => {
      try {
        await deps.repository.reclaimExpiredClaims();
        const claimToken = randomUUID();
        const rows = await deps.repository.claimDue(batchSize, claimToken);
        packetShareQueueDepth.set(rows.length);
        await Promise.all(rows.map((row) => deliver(row, claimToken)));
      } catch (error) {
        packetShareOutcomesTotal.inc({ outcome: 'worker_error' });
        console.error('[packet-share] worker error:', error instanceof Error ? error.message : error);
      } finally {
        packetShareQueueDepth.set(0);
        processing = null;
      }
    })();
    await processing;
  }

  function start(): void {
    if (pollTimer || stopping) return;
    stopping = false;
    pollTimer = setInterval(() => void processDue(), pollIntervalMs);
    pollTimer.unref();
    void processDue();
  }

  async function stop(): Promise<void> {
    stopping = true;
    if (pollTimer) clearInterval(pollTimer);
    pollTimer = null;
    if (processing) await processing.catch(() => undefined);
    const current = [...publishers.values()].map(({ publisher }) => publisher);
    publishers.clear();
    await Promise.all(current.map((publisher) => publisher.close().catch(() => undefined)));
  }

  return { enqueuePacket, processDue, start, stop };
}
