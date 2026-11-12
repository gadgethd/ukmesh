import { createMqttPublisher, type MessagePublisher } from '../forwarding/mqttPublisher.js';
import { createPublisherCache } from '../forwarding/publisherCache.js';
import { createClaimedDeliveryLoop } from '../forwarding/claimedDeliveryLoop.js';
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
import { publicFeedOutcomesTotal, publicFeedQueueDepth, publicFeedOldestAgeSeconds } from '../metrics/owner.js';
import { positiveIntegerSetting } from '../platform/config/boundedNumber.js';

const DEFAULT_POLL_INTERVAL_MS = 5_000;
const DEFAULT_BATCH_SIZE = 25;
const DEFAULT_OWNERSHIP_REFRESH_INTERVAL_MS = 15_000;
const PUBLISH_TIMEOUT_MS = 10_000;

export type PublicFeedPublisher = MessagePublisher;

export type PublicFeedPublisherFactory = (config: PublicFeedConfig) => PublicFeedPublisher;

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
  return createMqttPublisher({
    connection() {
      const url = configUrl(config);
      if (!config.username || !config.password) throw new Error('public feed publisher credentials are missing');
      return { url: url.toString(), options: {
        clientId: `meshcore-public-feed-${randomUUID().replaceAll('-', '').slice(0, 24)}`,
        username: config.username,
        password: config.password,
        clean: true,
        reconnectPeriod: 0,
        connectTimeout: PUBLISH_TIMEOUT_MS,
      } };
    },
    connectionLabel: 'public feed MQTT',
    publishLabel: 'public feed publish',
    // The curated observation stream remains QoS 0 and nonretained.
    publishOptions: { qos: 0, retain: false },
    connectTimeoutMs: PUBLISH_TIMEOUT_MS,
    logError: error => console.error('[public-feed] MQTT publisher error:', error.message),
  });
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
  const publishers = createPublisherCache(deps.closeTimeoutMs ?? 2_000, 'public feed cleanup');
  let ownershipTimer: NodeJS.Timeout | null = null;
  let ownershipRefresh: Promise<void> | null = null;
  let stopping = false;

  function publisherFor(config: PublicFeedConfig): Promise<PublicFeedPublisher> {
    return publishers.get('public-feed', configFingerprint(config), () => publisherFactory(config));
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
      await publishers.discard('public-feed');
      await deps.repository.markFailed(row.id, claimToken, row.attempts, message);
      publicFeedOutcomesTotal.inc({ outcome: 'delivery_error' });
    }
  }

  const deliveryLoop = createClaimedDeliveryLoop<PublicFeedDeliveryRow>({
    pollIntervalMs,
    deliveryTimeoutMs,
    async claim(claimToken) {
      await deps.repository.discardInactiveDeliveries();
      await deps.repository.reclaimExpiredClaims();
      return getPublicFeedConfig().configured ? deps.repository.claimDue(batchSize, claimToken) : [];
    },
    deliver,
    deliveryLabel: () => 'public feed delivery',
    async aborted(row, claimToken, error) {
      publicFeedOutcomesTotal.inc({ outcome: 'delivery_error' });
      await withDeadline(deps.repository.markFailed(row.id, claimToken, row.attempts,
        error instanceof Error ? error.message : String(error)), 2_000, 'public feed claim release').catch(() => undefined);
    },
    failed(error) {
      publicFeedOutcomesTotal.inc({ outcome: 'worker_error' });
      console.error('[public-feed] worker error:', error instanceof Error ? error.message : error);
    },
    async settled() {
      try {
        const stats = await deps.repository.queueStats?.();
        if (stats) { publicFeedQueueDepth.set(stats.count); publicFeedOldestAgeSeconds.set(stats.oldestAgeSeconds); }
      } catch { publicFeedQueueDepth.set(-1); publicFeedOldestAgeSeconds.set(-1); }
    },
  });
  const { processDue } = deliveryLoop;

  function start(): void {
    if (ownershipTimer || stopping) return;
    ownershipTimer = setInterval(() => void refreshOwnership(), ownershipRefreshIntervalMs);
    ownershipTimer.unref();
    void refreshOwnership();
    deliveryLoop.start();
  }

  async function stop(): Promise<void> {
    stopping = true;
    if (ownershipTimer) clearInterval(ownershipTimer);
    ownershipTimer = null;
    await deliveryLoop.stop();
    if (ownershipRefresh) await ownershipRefresh.catch(() => undefined);
    await publishers.close();
  }

  return { enqueuePacket, processDue, refreshOwnership, start, stop };
}
