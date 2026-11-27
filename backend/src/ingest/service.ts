import express from 'express';
import { publicReadinessPayload } from '../owner/ownerReadiness.js';
import { setTimeout as delay } from 'node:timers/promises';
import { ingestQuery, query } from '../db/runtime.js';
import type { DatabaseQueryFn } from '../db/contracts.js';
import { initializePacketPersistence } from '../db/packetPersistence.js';
import type { LifecycleCoordinator } from '../lifecycle/coordinator.js';
import { stopSpamDetector } from '../mqtt/spamDetector.js';
import { getOwnerAclReadiness, getOwnerAuthorizationSnapshot, getOwnerNodeIdsForUsername } from '../owner/ownerAuth.js';
import { createForwardingOutbox } from '../owner/forwardingOutbox.js';
import { createPacketShareForwarder } from '../owner/packetShareForwarder.js';
import { createPacketShareRepository } from '../owner/packetShareRepository.js';
import { createPublicFeedForwarder } from '../owner/publicFeedForwarder.js';
import { createPublicFeedRepository } from '../owner/publicFeedRepository.js';
import { closeQueuePublisher, queueLinkJob, stopQueueAdmission } from '../queue/publisher.js';
import { getRamIngestMode } from '../ram/protocol.js';
import { createLivePublisher } from '../ws/liveBus.js';
import type { PathObservation } from '../path-beta/observation.js';
import { mqttIngestEnabled, type ReadIngestHealth } from './health.js';
import { createIngestRuntime } from './runtime.js';

export function startIngestService(options: {
  lifecycle: LifecycleCoordinator;
  shutdownTimeLeft: () => number;
  observePath?: (hint: PathObservation) => void;
}) {
  const { lifecycle, shutdownTimeLeft } = options;
  const enabled = mqttIngestEnabled();
  const ramMode = getRamIngestMode() === 'ram';
  initializePacketPersistence();
  const live = createLivePublisher();
  const runtime = createIngestRuntime({ observePath: options.observePath ?? live.pathObservation });
  const { startMqttClient, stopMqttClient, getMqttRuntimeStatus, publishMqtt } = runtime.mqtt;
  const { startRamWriter, closeRamWriter } = runtime.writer;
  const { onPacket, onNodeSeen, onNodeUpsert } = runtime.events;

  const isOwnerNodeGrantActive = async (username: string, nodeId: string) =>
    (await getOwnerNodeIdsForUsername(username)).includes(nodeId.trim().toUpperCase());
  const writeForwarding: DatabaseQueryFn = (sql, params) => query(sql, params, AbortSignal.timeout(5_000));
  const packetShare = createPacketShareForwarder({
    repository: createPacketShareRepository(writeForwarding, undefined, { isOwnerNodeGrantActive }),
    isOwnerNodeGrantActive,
  });
  const publicFeed = createPublicFeedForwarder({
    repository: createPublicFeedRepository(writeForwarding), getOwnerAuthorizationSnapshot,
    // The public namespace is on the ingest broker and is writable only by this principal.
    publisherFactory: () => ({ publish: publishMqtt, close: async () => undefined }),
  });
  const forwarding = createForwardingOutbox(async (packet, envelope) => {
    await Promise.all([packetShare.enqueuePacket(packet, envelope), publicFeed.enqueuePacket(packet, envelope)]);
  });
  const starting = new AbortController();
  const retryDelay = (ms: number) => delay(ms, undefined, { signal: starting.signal, ref: false });

  async function startForwarding() {
    while (!starting.signal.aborted) {
      try {
        const acl = await getOwnerAclReadiness();
        const ready = acl.renderedGeneration && acl.desiredGeneration === acl.renderedGeneration && acl.lastError === null
          && (process.env['OWNER_ACL_MODE']?.trim().toLowerCase() === 'shadow' || acl.appliedGeneration === acl.renderedGeneration);
        if (ready && !starting.signal.aborted) {
          forwarding.start();
          packetShare.start();
          publicFeed.start();
          return;
        }
      } catch (error) {
        console.error('[ingest] owner authorization unavailable, forwarding will retry:', (error as Error).message);
      }
      await retryDelay(5_000);
    }
  }

  async function startTransport() {
    if (ramMode) {
      let wait = 1_000;
      while (!starting.signal.aborted) {
        try { await startRamWriter(); break; }
        catch (error) {
          console.error('[ram] writer startup failed, retrying:', (error as Error).message);
          await retryDelay(wait);
          wait = Math.min(wait * 2, 30_000);
        }
      }
    }
    // RAM mode may acknowledge at admission only once the durable writer is running.
    if (enabled && !starting.signal.aborted) await startMqttClient();
  }

  onPacket((packet) => {
    live.packet(packet);
    // The RAM writer admits link work after commit because the worker reads from PostgreSQL.
    if (!ramMode && packet.path?.length && packet.rxNodeId) {
      void queueLinkJob(packet.packetHash, packet.rxNodeId, packet.srcNodeId, packet.path, packet.hopCount, packet.pathHashSizeBytes)
        .then((admission) => {
          if (admission && ['full', 'oversized', 'worker_unavailable'].includes(admission.status)) {
            console.warn('[link-queue] live observation not admitted', admission.status, packet.packetHash);
          }
        }).catch((error: Error) => console.error('[link-queue] live admission failed', error.message));
    }
  });
  onNodeSeen(live.nodeSeen);
  onNodeUpsert(live.nodeUpsert);

  const logStartup = (name: string) => (error: unknown) => {
    if (!starting.signal.aborted) console.error(`[ingest] ${name} startup failed:`, (error as Error).message);
  };
  if (!enabled) console.warn('[mqtt] ingest disabled by MQTT_INGEST_ENABLED');
  const transportStartup = startTransport().catch(logStartup('transport'));
  const forwardingStartup = startForwarding().catch(logStartup('forwarding'));
  lifecycle.register({ name: 'ingest-admission', stage: 10, close: async () => {
    starting.abort();
    stopQueueAdmission();
    await stopMqttClient(shutdownTimeLeft());
  } });
  lifecycle.register({ name: 'spam-detector', stage: 10, close: stopSpamDetector });
  lifecycle.register({ name: 'forwarding-outbox', stage: 25, close: async () => {
    await forwardingStartup;
    await forwarding.stop();
  } });
  lifecycle.register({ name: 'ram-ingest-writer', stage: 30, close: async () => {
    await transportStartup;
    await closeRamWriter(shutdownTimeLeft());
  } });
  lifecycle.register({ name: 'owner-packet-share-forwarder', stage: 30, close: packetShare.stop });
  lifecycle.register({ name: 'public-feed-forwarder', stage: 30, close: publicFeed.stop });
  lifecycle.register({ name: 'committed-followups', stage: 35, close: runtime.committedFollowUps.close });
  lifecycle.register({ name: 'ingest-events', stage: 36, close: runtime.events.close });
  lifecycle.register({ name: 'live-publisher', stage: 38, close: live.close });
  lifecycle.register({ name: 'queue-publisher', stage: 38, close: closeQueuePublisher });

  const readHealth: ReadIngestHealth = async () => {
    const mqtt = getMqttRuntimeStatus();
    const deliveryReady = mqtt.delivery?.state === 'healthy' || mqtt.delivery?.state === 'disabled';
    return { ready: !lifecycle.isDraining && (!enabled || (mqtt.state === 'connected' && deliveryReady)),
      state: enabled ? mqtt.state : 'disabled', deliveryStalled: mqtt.delivery?.state === 'stalled' };
  };
  let databaseCheck: Promise<boolean> | undefined;
  let checkedAt = 0;
  let databaseReady = false;
  return { readHealth, async checkDatabase() {
    if (Date.now() - checkedAt < 5_000) return databaseReady;
    databaseCheck ??= ingestQuery('SELECT 1').then(() => true, () => false).then((ready) => {
      databaseReady = ready;
      checkedAt = Date.now();
      databaseCheck = undefined;
      return ready;
    });
    return databaseCheck;
  } };
}

export function startIngestWorker(options: Pick<Parameters<typeof startIngestService>[0], 'lifecycle' | 'shutdownTimeLeft'>) {
  const intake = startIngestService(options);
  const app = express();
  app.get('/healthz', (_req, res) => res.json({ status: 'ok' }));
  app.get('/readyz', async (_req, res) => {
    const [health, database] = await Promise.all([intake.readHealth(), intake.checkDatabase()]);
    const ready = database && health.ready;
    const body = publicReadinessPayload(ready, database, health.state);
    res.status(ready ? 200 : 503).json({ ...body, checks: { ...body.checks,
      mqtt: { ...body.checks.mqtt, deliveryStalled: health.deliveryStalled } } });
  });
  const server = app.listen(Number(process.env['PORT'] ?? 3000), process.env['HTTP_HOST'] ?? '0.0.0.0');
  options.lifecycle.register({ name: 'ingest-health-listener', stage: 20, close: () => {
    server.closeIdleConnections();
    return new Promise<void>((resolve, reject) => server.close((error) => error ? reject(error) : resolve()));
  } });
}
