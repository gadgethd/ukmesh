/**
 * RAM-first MQTT ingestion — writer runtime wiring.
 *
 * Kept separate from writer.ts so the writer core (and its unit tests) do not
 * import database or MQTT client modules. This module is only imported by
 * index.ts in the running application.
 */
import { Redis } from 'ioredis';
import { getRedisConnectionOptions, getRedisUrl } from '../platform/config/redis.js';
import { query } from '../db/index.js';
import { persistEntriesCore, refreshAdmissionPrivacyCache } from '../db/packetBatch.js';
import { markMqttPersistenceHealthy, schedulePostPersistFollowUps } from '../mqtt/client.js';
import { queueLinkJob } from '../queue/publisher.js';
import { refreshPendingStats } from './liveStore.js';
import {
  getRamBatchSize,
  getRamConsumerGroup,
  getRamConsumerName,
  getRamMaxAgeMs,
  getRamStreamKey,
} from './protocol.js';
import { createRamWriter, type RamWriterRedis } from './writer.js';

let singleton: ReturnType<typeof createRamWriter> | null = null;

/** Bound on the stale-cache window for admission-time visibility flags. */
const PRIVACY_REFRESH_INTERVAL_MS = 30_000;
let lastPrivacyRefreshAt = 0;

function productionRedis(): RamWriterRedis {
  const client = new Redis(getRedisUrl(), {
    ...getRedisConnectionOptions(),
    maxRetriesPerRequest: 2,
  });
  client.on('error', (error: Error) => {
    console.error('[ram] writer redis error', error.message);
  });
  return client as unknown as RamWriterRedis;
}

export async function startRamWriter(): Promise<void> {
  if (singleton) return;
  singleton = createRamWriter({
    redis: productionRedis(),
    persist: async (entries, idempotent) => {
      await persistEntriesCore(entries, query, idempotent);
    },
    now: () => Date.now(),
    sleep: (ms) => new Promise((resolve) => setTimeout(resolve, ms)),
    batchSize: getRamBatchSize,
    maxAgeMs: getRamMaxAgeMs,
    streamKey: getRamStreamKey,
    group: getRamConsumerGroup,
    consumer: getRamConsumerName,
    statsTick: async () => {
      await refreshPendingStats();
      const now = Date.now();
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
    log: (message, error) => {
      console.error(message, error instanceof Error ? error.message : (error ?? ''));
    },
  });
  await singleton.start();
  const loop = (async () => {
    while (singleton && singleton.running) {
      try {
        await singleton.tickOnce();
      } catch (error) {
        console.error('[ram] writer loop error', error instanceof Error ? error.message : error);
        await new Promise((resolve) => setTimeout(resolve, 1_000));
      }
    }
  })();
  void loop.catch(() => undefined);
}

export async function closeRamWriter(deadlineMs = 30_000): Promise<void> {
  if (!singleton) return;
  const writer = singleton;
  singleton = null;
  await writer.drain(deadlineMs);
}
