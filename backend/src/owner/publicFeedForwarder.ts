import mqtt, { type MqttClient } from 'mqtt';
import { withDeadline } from '../platform/deadline.js';
import { randomUUID } from 'node:crypto';
import type { LivePacket } from '../types/index.js';
import {
  buildPublicFeedPacket,
  getPublicFeedConfig,
  isPublicFeedPacketTopic,
  publicFeedEventKey,
  publicFeedPacketIsEligible,
  type PublicFeedConfig,
} from './publicFeedConfig.js';
import type { PublicFeedDeliveryRow, PublicFeedRepository, PublicFeedAuthorizationSnapshot } from './publicFeedRepository.js';
import { publicFeedOutcomesTotal, publicFeedQueueDepth, publicFeedOldestAgeSeconds } from '../metrics.js';

const DEFAULT_POLL_INTERVAL_MS = 5_000;
const DEFAULT_BATCH_SIZE = 25;
const DEFAULT_OWNERSHIP_REFRESH_INTERVAL_MS = 15_000;
const PUBLISH_TIMEOUT_MS = 10_000;

export type PublicFeedPublisher = {
  publish: (topic: string, payload: string) => Promise<void>;
  close: () => Promise<void>;
};

export type PublicFeedPublisherFactory = (config: PublicFeedConfig) => PublicFeedPublisher;

function positiveIntegerSetting(name: string, fallback: number, max: number): number {
  const parsed = Number(process.env[name]);
  return Number.isFinite(parsed) && parsed > 0 ? Math.min(max, Math.floor(parsed)) : fallback;
}

function configUrl(config: PublicFeedConfig): URL {
  if (!config.brokerUrl) throw new Error('public feed broker URL is missing');
  const url = new URL(config.brokerUrl);
  if (!['mqtt:', 'mqtts:', 'ws:', 'wss:'].includes(url.protocol) || !url.hostname) {
    throw new Error('public feed broker URL is invalid');
  }
  return url;
}

function configFingerprint(config: PublicFeedConfig): string {
  return JSON.stringify([config.brokerUrl, config.username, config.password]);
}

function connectPublicFeedPublisher(config: PublicFeedConfig): PublicFeedPublisher {
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
    const url = configUrl(config);
    if (!config.username || !config.password) throw new Error('public feed publisher credentials are missing');
    const created = mqtt.connect(url.toString(), {
      clientId: `meshcore-public-feed-${randomUUID().replaceAll('-', '').slice(0, 24)}`,
      username: config.username,
      password: config.password,
      clean: true,
      reconnectPeriod: 0,
      connectTimeout: PUBLISH_TIMEOUT_MS,
    });
    client = created;
    created.on('error', (error) => {
      console.error('[public-feed] MQTT publisher error:', error.message);
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
      const onClose = () => finish(new Error('public feed MQTT connection closed before connect'));
      timer = setTimeout(() => finish(new Error('public feed MQTT connection timed out')), PUBLISH_TIMEOUT_MS);
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
      // CoreScope subscribes at QoS 0. The feed is an observation stream, so
      // retaining messages or requesting broker redelivery is undesirable.
      await current.publishAsync(topic, payload, { qos: 0, retain: false });
      if (!current.connected) {
        await closeClient();
        throw new Error('public feed MQTT connection closed after publish');
      }
    },
    close: closeClient,
  };
}

export type PublicFeedForwarderDeps = {
  repository: PublicFeedRepository;
  getOwnerAuthorizationSnapshot: () => Promise<PublicFeedAuthorizationSnapshot[]>;
  publisherFactory?: PublicFeedPublisherFactory;
  pollIntervalMs?: number;
  batchSize?: number;
  ownershipRefreshIntervalMs?: number;
  publishTimeoutMs?: number;
  deliveryTimeoutMs?: number;
  closeTimeoutMs?: number;
};

export function createPublicFeedForwarder(deps: PublicFeedForwarderDeps) {
  const publisherFactory = deps.publisherFactory ?? connectPublicFeedPublisher;
  const pollIntervalMs = deps.pollIntervalMs ?? positiveIntegerSetting(
    'PUBLIC_FEED_POLL_INTERVAL_MS', DEFAULT_POLL_INTERVAL_MS, 60_000,
  );
  const batchSize = deps.batchSize ?? positiveIntegerSetting(
    'PUBLIC_FEED_BATCH_SIZE', DEFAULT_BATCH_SIZE, 100,
  );
  const ownershipRefreshIntervalMs = deps.ownershipRefreshIntervalMs ?? positiveIntegerSetting(
    'PUBLIC_FEED_OWNERSHIP_REFRESH_INTERVAL_MS', DEFAULT_OWNERSHIP_REFRESH_INTERVAL_MS, 300_000,
  );
  const publishTimeoutMs = deps.publishTimeoutMs ?? 10_000;
  const deliveryTimeoutMs = deps.deliveryTimeoutMs ?? 25_000;
  const closePublisher = (publisher: PublicFeedPublisher) => withDeadline(
    publisher.close(), deps.closeTimeoutMs ?? 2_000, 'public feed cleanup',
  ).catch(() => undefined);
  const publishers = new Map<string, { publisher: PublicFeedPublisher; fingerprint: string }>();
  let pollTimer: NodeJS.Timeout | null = null;
  let ownershipTimer: NodeJS.Timeout | null = null;
  let processing: Promise<void> | null = null;
  let ownershipRefresh: Promise<void> | null = null;
  let stopping = false;

  async function publisherFor(config: PublicFeedConfig): Promise<PublicFeedPublisher> {
    const fingerprint = configFingerprint(config);
    const existing = publishers.get('public-feed');
    if (existing?.fingerprint === fingerprint) return existing.publisher;
    if (existing) {
      publishers.delete('public-feed');
      await closePublisher(existing.publisher);
    }
    const created = publisherFactory(config);
    publishers.set('public-feed', { publisher: created, fingerprint });
    return created;
  }

  async function refreshOwnership(): Promise<void> {
    if (stopping) return;
    if (ownershipRefresh) {
      await ownershipRefresh;
      return;
    }
    ownershipRefresh = (async () => {
      try {
        const snapshot = await deps.getOwnerAuthorizationSnapshot();
        await deps.repository.syncOwnership(snapshot);
      } catch (error) {
        publicFeedOutcomesTotal.inc({ outcome: 'ownership_sync_error' });
        console.error('[public-feed] ownership refresh failed:', error instanceof Error ? error.message : error);
      } finally {
        ownershipRefresh = null;
      }
    })();
    await ownershipRefresh;
  }

  async function enqueuePacket(packet: LivePacket, upstreamEnvelope: Record<string, unknown>): Promise<void> {
    if (!publicFeedPacketIsEligible(packet)) {
      publicFeedOutcomesTotal.inc({ outcome: 'skipped' });
      return;
    }
    const config = getPublicFeedConfig();
    if (!config.configured) {
      publicFeedOutcomesTotal.inc({ outcome: 'unconfigured' });
      return;
    }
    if (!await deps.repository.hasActiveNode(packet.rxNodeId!)) return;

    let outbound: ReturnType<typeof buildPublicFeedPacket>;
    try {
      outbound = buildPublicFeedPacket(packet, upstreamEnvelope);
    } catch (error) {
      publicFeedOutcomesTotal.inc({ outcome: 'invalid_packet' });
      console.error('[public-feed] packet rejected:', error instanceof Error ? error.message : error);
      return;
    }

    try {
      const inserted = await deps.repository.enqueueDelivery({
        nodeId: packet.rxNodeId!,
        eventKey: publicFeedEventKey(packet),
        packet,
        destinationTopic: outbound.topic,
        payload: outbound.payload,
      });
      publicFeedOutcomesTotal.inc({ outcome: inserted ? 'queued' : 'duplicate' });
    } catch (error) {
      publicFeedOutcomesTotal.inc({ outcome: 'queue_error' });
      console.error('[public-feed] queue error:', error instanceof Error ? error.message : error);
      throw error;
      return;
    }
    void processDue();
  }

  async function deliver(row: PublicFeedDeliveryRow, claimToken: string, signal: AbortSignal): Promise<void> {
    // Drop deliveries created by an older deployment before they can publish
    // to the canonical ingest namespace. The curated public namespace is the
    // only destination this worker is allowed to emit.
    if (!isPublicFeedPacketTopic(row.destination_topic)) {
      await deps.repository.markDiscarded(row.id, claimToken, 'public feed destination namespace is invalid');
      publicFeedOutcomesTotal.inc({ outcome: 'invalid_destination' });
      return;
    }
    if (!await deps.repository.hasActiveNode(row.node_id)) {
      await deps.repository.markDiscarded(row.id, claimToken, 'public feed is no longer enabled for this node');
      publicFeedOutcomesTotal.inc({ outcome: 'disabled' });
      return;
    }
    const config = getPublicFeedConfig();
    if (!config.configured) {
      await deps.repository.markFailed(row.id, claimToken, row.attempts, 'public feed broker is not configured');
      publicFeedOutcomesTotal.inc({ outcome: 'unconfigured' });
      return;
    }
    try {
      const publisher = await publisherFor(config);
      signal.throwIfAborted();
      await withDeadline(publisher.publish(row.destination_topic, JSON.stringify(row.payload)), publishTimeoutMs, 'public feed publish');
      signal.throwIfAborted();
      await deps.repository.markSucceeded(row.id, claimToken);
      publicFeedOutcomesTotal.inc({ outcome: 'delivered' });
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      const entry = publishers.get('public-feed');
      if (entry) {
        publishers.delete('public-feed');
        await closePublisher(entry.publisher);
      }
      await deps.repository.markFailed(row.id, claimToken, row.attempts, message);
      publicFeedOutcomesTotal.inc({ outcome: 'delivery_error' });
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
        await deps.repository.discardInactiveDeliveries();
        await deps.repository.reclaimExpiredClaims();
        if (!getPublicFeedConfig().configured) return;
        const claimToken = randomUUID();
        const rows = await deps.repository.claimDue(batchSize, claimToken);
        await Promise.all(rows.map(async (row) => {
          const cancellation = new AbortController();
          try { await withDeadline(deliver(row, claimToken, cancellation.signal), deliveryTimeoutMs, 'public feed delivery'); }
          catch (error) {
            cancellation.abort();
            publicFeedOutcomesTotal.inc({ outcome: 'delivery_error' });
            await withDeadline(deps.repository.markFailed(row.id, claimToken, row.attempts,
              error instanceof Error ? error.message : String(error)), 2_000, 'public feed claim release').catch(() => undefined);
          }
        }));
      } catch (error) {
        publicFeedOutcomesTotal.inc({ outcome: 'worker_error' });
        console.error('[public-feed] worker error:', error instanceof Error ? error.message : error);
      } finally {
        try {
          const stats = await deps.repository.queueStats?.();
          if (stats) { publicFeedQueueDepth.set(stats.count); publicFeedOldestAgeSeconds.set(stats.oldestAgeSeconds); }
        } catch { publicFeedQueueDepth.set(-1); publicFeedOldestAgeSeconds.set(-1); }
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
    ownershipTimer = setInterval(() => void refreshOwnership(), ownershipRefreshIntervalMs);
    ownershipTimer.unref();
    void refreshOwnership();
    void processDue();
  }

  async function stop(): Promise<void> {
    stopping = true;
    if (pollTimer) clearInterval(pollTimer);
    if (ownershipTimer) clearInterval(ownershipTimer);
    pollTimer = null;
    ownershipTimer = null;
    if (processing) await processing.catch(() => undefined);
    if (ownershipRefresh) await ownershipRefresh.catch(() => undefined);
    const current = [...publishers.values()].map(({ publisher }) => publisher);
    publishers.clear();
    await Promise.all(current.map((publisher) => closePublisher(publisher)));
  }

  return { enqueuePacket, processDue, refreshOwnership, start, stop };
}
