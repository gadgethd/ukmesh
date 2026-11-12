import { createMqttPublisher, type MessagePublisher } from '../forwarding/mqttPublisher.js';
import { createPublisherCache } from '../forwarding/publisherCache.js';
import { createClaimedDeliveryLoop } from '../forwarding/claimedDeliveryLoop.js';
import { randomUUID } from 'node:crypto';
import type { LivePacket } from '../types/index.js';
import {
  buildPacketSharePayload,
  destinationConfigured,
  destinationAcceptsPacket,
  destinationAllowedForOwner,
  isForwardablePacket,
  isValidPacketShareBrokerUrl,
  packetShareEventKey,
  renderDestinationTopic,
  type PacketShareDestinationRow,
} from './packetShareConfig.js';
import { hasPacketShareEncryptionKey } from './packetShareCrypto.js';
import { packetShareCredentials } from './packetShareAuth.js';
import type {
  OwnerNodeGrantPredicate,
  OwnerNodeGrantState,
  PacketShareDeliveryRow,
  PacketShareRepository,
} from './packetShareRepository.js';
import { packetShareOutcomesTotal, packetShareQueueDepth } from '../metrics/owner.js';
import { positiveIntegerSetting } from '../platform/config/boundedNumber.js';
import { withDeadline } from '../platform/deadline.js';

const DEFAULT_POLL_INTERVAL_MS = 5_000;
const DEFAULT_BATCH_SIZE = 25;
const PUBLISH_TIMEOUT_MS = 10_000;
const DEFAULT_PUBLISH_ACK_TIMEOUT_MS = 10_000;
const DEFAULT_DELIVER_TIMEOUT_MS = 30_000;

export type PacketSharePublisher = MessagePublisher;

export type PacketSharePublisherFactory = (
  destination: PacketShareDestinationRow,
) => PacketSharePublisher;

// A hung publisher promise must never be able to block the single-flight
// delivery loop. The 2026-10-12 wedge: a QoS-1 publish on a silently-dead
// connection never settled, freezing delivery for every destination until
// the process was restarted.

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
    destination.auth_mode,
    destination.auth_audience,
  ]);
}

function connectMqttPublisher(
  destination: PacketShareDestinationRow,
  publishAckTimeoutMs: number,
): PacketSharePublisher {
  return createMqttPublisher({
    async connection() {
      const url = destinationUrl(destination);
      const credentials = await packetShareCredentials(destination);
      return { url: url.toString(), expiresAt: credentials.expiresAt, options: {
        clientId: `meshcore-share-${randomUUID().replaceAll('-', '').slice(0, 24)}`,
        username: credentials.username,
        password: credentials.password,
        clean: true,
        reconnectPeriod: 0,
        connectTimeout: PUBLISH_TIMEOUT_MS,
      } };
    },
    connectionLabel: 'destination MQTT',
    publishLabel: 'destination MQTT publish',
    publishOptions: { qos: 1 },
    connectTimeoutMs: PUBLISH_TIMEOUT_MS,
    publishTimeoutMs: publishAckTimeoutMs,
    discardOnPublishFailure: true,
    logError: error => console.error(`[packet-share] MQTT destination ${destination.destination_id} error:`, error.message),
  });
}

export type PacketShareForwarderDeps = {
  repository: PacketShareRepository;
  /**
   * Live owner-node grant predicate. Production wiring passes the same
   * owner-auth resolution the owner routes use; the repository also receives
   * it. Both layers check because either one alone is bypassable.
   */
  isOwnerNodeGrantActive?: OwnerNodeGrantPredicate;
  publisherFactory?: PacketSharePublisherFactory;
  pollIntervalMs?: number;
  batchSize?: number;
  publishAckTimeoutMs?: number;
  deliverTimeoutMs?: number;
};

export function createPacketShareForwarder(deps: PacketShareForwarderDeps) {
  const publishAckTimeoutMs = deps.publishAckTimeoutMs ?? positiveIntegerSetting(
    'OWNER_PACKET_SHARE_PUBLISH_TIMEOUT_MS', DEFAULT_PUBLISH_ACK_TIMEOUT_MS, 120_000,
  );
  const deliverTimeoutMs = deps.deliverTimeoutMs ?? positiveIntegerSetting(
    'OWNER_PACKET_SHARE_DELIVER_TIMEOUT_MS', DEFAULT_DELIVER_TIMEOUT_MS, 600_000,
  );
  const publisherFactory: PacketSharePublisherFactory = deps.publisherFactory
    ?? ((destination) => connectMqttPublisher(destination, publishAckTimeoutMs));
  const pollIntervalMs = deps.pollIntervalMs ?? positiveIntegerSetting(
    'OWNER_PACKET_SHARE_POLL_INTERVAL_MS', DEFAULT_POLL_INTERVAL_MS, 60_000,
  );
  const batchSize = deps.batchSize ?? positiveIntegerSetting(
    'OWNER_PACKET_SHARE_BATCH_SIZE', DEFAULT_BATCH_SIZE, 100,
  );
  const publishers = createPublisherCache(2_000, 'publisher cleanup');

  /**
   * Resolve the live grant for one (owner, node) pair. An owner-auth outage is
   * not authority to forward, but it is also not proof of revocation: the
   * caller treats "unknown" as retryable and "inactive" as terminal.
   */
  async function ownerGrantState(ownerUsername: string, nodeId: string): Promise<OwnerNodeGrantState> {
    if (deps.isOwnerNodeGrantActive) {
      try {
        return (await deps.isOwnerNodeGrantActive(ownerUsername, nodeId)) ? 'active' : 'inactive';
      } catch (error) {
        console.error(
          '[packet-share] owner grant lookup failed:',
          error instanceof Error ? error.message : error,
        );
        return 'unknown';
      }
    }
    if (typeof deps.repository.ownerGrantState === 'function') {
      return deps.repository.ownerGrantState(ownerUsername, nodeId);
    }
    // No authority source wired: suppress (retryable), never assume grant.
    return 'unknown';
  }

  function publisherFor(destination: PacketShareDestinationRow): Promise<PacketSharePublisher> {
    return publishers.get(destination.destination_id, destinationFingerprint(destination), () => publisherFactory(destination));
  }

  async function enqueuePacket(packet: LivePacket, upstreamEnvelope: Record<string, unknown>): Promise<void> {
    if (!isForwardablePacket(packet)) {
      packetShareOutcomesTotal.inc({ outcome: 'skipped' });
      return;
    }
    const targets = await deps.repository.activeTargets(packet.rxNodeId!);
    if (targets.length < 1) return;

    for (const target of targets) {
      const grantState = await ownerGrantState(target.owner_username, packet.rxNodeId!);
      if (grantState !== 'active') {
        if (grantState === 'inactive') {
          try {
            await deps.repository.deadLetterOwnerNodeDeliveries?.(target.owner_username, packet.rxNodeId!);
          } catch (error) {
            console.error('[packet-share] dead-letter failed:', error instanceof Error ? error.message : error);
          }
        }
        packetShareOutcomesTotal.inc({ outcome: 'blocked' });
        continue;
      }
      if (!destinationAllowedForOwner(target, target.owner_username, packet.rxNodeId!)
        || !destinationAcceptsPacket(target, packet, upstreamEnvelope)) continue;
      if (!destinationConfigured(target, hasPacketShareEncryptionKey())) {
        packetShareOutcomesTotal.inc({ outcome: 'unconfigured' });
        continue;
      }
      let destinationTopic: string;
      try {
        destinationTopic = renderDestinationTopic(target, packet);
      } catch (error) {
        packetShareOutcomesTotal.inc({ outcome: 'invalid_destination' });
        console.error('[packet-share] invalid destination topic:', error instanceof Error ? error.message : error);
        continue;
      }
      try {
        const eventKey = packetShareEventKey(target.owner_username, target.destination_id, packet);
        const payload = buildPacketSharePayload(packet, upstreamEnvelope, target);
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
        throw error;
      }
    }
    void processDue();
  }

  async function deliver(row: PacketShareDeliveryRow, claimToken: string, signal: AbortSignal): Promise<void> {
    const grantState = await ownerGrantState(row.owner_username, row.node_id);
    signal.throwIfAborted();
    if (grantState === 'inactive') {
      // Revocation is terminal for everything already queued for this node.
      try {
        await deps.repository.deadLetterOwnerNodeDeliveries?.(row.owner_username, row.node_id);
      } catch (error) {
        console.error('[packet-share] dead-letter failed:', error instanceof Error ? error.message : error);
      }
      packetShareOutcomesTotal.inc({ outcome: 'blocked' });
      return;
    }
    if (grantState === 'unknown') {
      // No positive grant, so no publish: release the claim on the normal
      // backoff instead of destroying it on an owner-auth blip.
      await deps.repository.markFailed(row.id, claimToken, row.attempts, 'owner grant check unavailable');
      packetShareOutcomesTotal.inc({ outcome: 'delivery_error' });
      return;
    }
    const destination = await deps.repository.getDestination(row.destination_id);
    signal.throwIfAborted();
    if (!destination || !destinationAllowedForOwner(destination, row.owner_username, row.node_id)
      || !destinationConfigured(destination, hasPacketShareEncryptionKey())) {
      await deps.repository.markFailed(row.id, claimToken, row.attempts, 'destination is missing or incomplete');
      packetShareOutcomesTotal.inc({ outcome: 'delivery_error' });
      return;
    }
    try {
      const publisher = await publisherFor(destination);
      signal.throwIfAborted();
      await withDeadline(
        publisher.publish(row.destination_topic, JSON.stringify(row.payload)),
        publishAckTimeoutMs,
        'destination MQTT publish',
      );
      signal.throwIfAborted();
      await deps.repository.markSucceeded(row.id, claimToken);
      signal.throwIfAborted();
      packetShareOutcomesTotal.inc({ outcome: 'delivered' });
    } catch (error) {
      // The loop owns claim release after its deadline. A late attempt must
      // neither disturb the retry's publisher nor transition the expired claim.
      if (signal.aborted) throw error;
      const message = error instanceof Error ? error.message : String(error);
      await publishers.discard(row.destination_id);
      signal.throwIfAborted();
      await deps.repository.markFailed(row.id, claimToken, row.attempts, message);
      packetShareOutcomesTotal.inc({ outcome: 'delivery_error' });
    }
  }

  const deliveryLoop = createClaimedDeliveryLoop<PacketShareDeliveryRow>({
    pollIntervalMs,
    deliveryTimeoutMs: deliverTimeoutMs,
    async claim(claimToken) {
      await deps.repository.reclaimExpiredClaims();
      const rows = await deps.repository.claimDue(batchSize, claimToken);
      packetShareQueueDepth.set(rows.length);
      return rows;
    },
    deliver,
    deliveryLabel: row => `delivery timed out (row ${row.id}, ${row.destination_id})`,
    async aborted(row, claimToken, error) {
      // A hung delivery releases its claim for retry without wedging other destinations.
      const message = error instanceof Error ? error.message : String(error);
      console.error('[packet-share] delivery aborted:', message);
      await deps.repository.markFailed(row.id, claimToken, row.attempts, message).catch(() => undefined);
      packetShareOutcomesTotal.inc({ outcome: 'delivery_error' });
    },
    failed(error) {
      packetShareOutcomesTotal.inc({ outcome: 'worker_error' });
      console.error('[packet-share] worker error:', error instanceof Error ? error.message : error);
    },
    settled: () => packetShareQueueDepth.set(0),
  });
  const { processDue } = deliveryLoop;
  function start(): void { deliveryLoop.start(); }

  async function stop(): Promise<void> {
    await deliveryLoop.stop();
    await publishers.close();
  }

  return { enqueuePacket, processDue, start, stop };
}
