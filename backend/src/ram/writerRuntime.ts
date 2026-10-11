/**
 * RAM-first MQTT ingestion — writer runtime wiring.
 *
 * Kept separate from writer.ts so the writer core (and its unit tests) do not
 * import database or MQTT client modules. This module is only imported by
 * index.ts in the running application.
 */
import { Redis } from 'ioredis';
import { getRedisConnectionOptions, getRedisUrl } from '../platform/config/redis.js';
import { query, withTransaction } from '../db/index.js';
import { persistEntriesCore, refreshAdmissionPrivacyCache } from '../db/packetBatch.js';
import { markMqttPersistenceFailed, markMqttPersistenceHealthy, schedulePostPersistFollowUps } from '../mqtt/client.js';
import { queueLinkJob } from '../queue/publisher.js';
import { closeRamStore, reconcileRamQueueBytes, refreshPendingStats } from './liveStore.js';
import {
  getRamBatchSize,
  getRamConsumerGroup,
  getRamConsumerName,
  getRamMaxAgeMs,
  getRamStreamKey,
} from './protocol.js';
import { createRamWriter, type RamWriterRedis } from './writer.js';
import { joinAndDrainRamWriter } from './drainLifecycle.js';
import { validateRamRedisHeadroom } from './memory.js';

let singleton: ReturnType<typeof createRamWriter> | null = null;
let writerLoop: Promise<void> | null = null;
let writerRedis: Redis | null = null;

/** Bound on the stale-cache window for admission-time visibility flags. */
const PRIVACY_REFRESH_INTERVAL_MS = 30_000;
let lastPrivacyRefreshAt = 0;
let lastBytesReconcileAt = 0;
let pendingSince = 0;

function productionRedis(): Redis {
  const client = new Redis(getRedisUrl(), {
    ...getRedisConnectionOptions(),
    maxRetriesPerRequest: 2,
  });
  client.on('error', (error: Error) => {
    console.error('[ram] writer redis error', error.message);
  });
  return client;
}

export async function startRamWriter(): Promise<void> {
  if (singleton) return;
  await reconcileRamQueueBytes();
  const redis = productionRedis();
  try { validateRamRedisHeadroom(await redis.info('memory')); }
  catch (error) { redis.disconnect(); throw error; }
  const writer = createRamWriter({
    redis: redis as unknown as RamWriterRedis,
    persist: async (entries, idempotent) => {
      const rows = await persistEntriesCore(entries, query, idempotent, withTransaction);
      return Number(rows[0]?.inserted_count ?? 0);
    },
    now: () => Date.now(),
    sleep: (ms) => new Promise((resolve) => setTimeout(resolve, ms)),
    batchSize: getRamBatchSize,
    maxAgeMs: getRamMaxAgeMs,
    streamKey: getRamStreamKey,
    group: getRamConsumerGroup,
    consumer: getRamConsumerName,
    statsTick: async () => {
      const depth = await refreshPendingStats();
      const now = Date.now();
      const stalledAfter = Math.max(180_000, getRamMaxAgeMs() * 2);
      if (depth && depth.pending > 0) {
        if (pendingSince === 0) pendingSince = now;
        if (now - pendingSince > stalledAfter || depth.oldestPelAgeSeconds * 1000 > stalledAfter) {
          markMqttPersistenceFailed(new Error(`RAM_WRITER_STALLED:pending=${depth.pending} pel=${depth.pel}`));
        }
      } else {
        pendingSince = 0;
      }
      if (now - lastBytesReconcileAt >= 30_000) {
        await reconcileRamQueueBytes();
        lastBytesReconcileAt = now;
      }
      if (now - lastPrivacyRefreshAt >= PRIVACY_REFRESH_INTERVAL_MS) {
        lastPrivacyRefreshAt = now;
        await refreshAdmissionPrivacyCache().catch((error: unknown) => {
          console.error(
            '[ram] privacy cache refresh failed',
            error instanceof Error ? error.message : error,
          );
        });
      }
    },
    onCommitted: (entries) => {
      pendingSince = 0;
      // Commit-dependent live-map work, mirroring the durable post-persist
      // path: the link queue and the live-map follow-ups both read packet
      // rows from PostgreSQL, so they run here — after the batch commit.
      for (const { entry } of entries) {
        const input = entry.input;
        if (input.pathHashes && input.pathHashes.length > 0 && input.rxNodeId) {
          void queueLinkJob(
            input.packetHash,
            input.rxNodeId,
            input.srcNodeId ?? undefined,
            input.pathHashes,
            input.hopCount ?? undefined,
            input.pathHashSizeBytes ?? undefined,
          ).then((admission) => {
            if (admission && ['full', 'oversized', 'worker_unavailable'].includes(admission.status)) {
              console.warn('[link-queue] live observation not admitted', admission.status, input.packetHash);
            }
          }).catch((error: Error) => console.error('[link-queue] live admission failed', error.message));
        }
        schedulePostPersistFollowUps({
          finalHash: input.packetHash,
          decodedPathHashSizeBytes: input.pathHashSizeBytes ?? undefined,
          path: input.pathHashes ?? undefined,
          decodedRouteType: input.routeType ?? undefined,
          network: input.network,
          resolvedPacketType: input.packetType ?? undefined,
        });
      }
    },
    markHealthy: markMqttPersistenceHealthy,
    markFailed: markMqttPersistenceFailed,
    log: (message, error) => {
      console.error(message, error instanceof Error ? error.message : (error ?? ''));
    },
  });
  try {
    await writer.start();
  } catch (error) {
    await redis.quit().catch(() => undefined);
    throw error;
  }
  singleton = writer;
  writerRedis = redis;
  writerLoop = (async () => {
    while (writer.running) {
      try {
        await writer.tickOnce();
      } catch (error) {
        markMqttPersistenceFailed(error);
        console.error('[ram] writer loop error', error instanceof Error ? error.message : error);
        await new Promise((resolve) => setTimeout(resolve, 1_000));
      }
    }
  })();
  void writerLoop.catch(() => undefined);
}

export async function closeRamWriter(deadlineMs = 30_000): Promise<void> {
  if (!singleton) return;
  const writer = singleton;
  const deadline = Date.now() + deadlineMs;
  try {
    await joinAndDrainRamWriter(writer, writerLoop ?? Promise.resolve(), deadline);
  } finally {
    singleton = null;
    writerLoop = null;
    const owned = writerRedis;
    writerRedis = null;
    if (owned) {
      if (Date.now() >= deadline) owned.disconnect();
      else await owned.quit().catch(() => owned.disconnect());
    }
    await closeRamStore();
  }
}
