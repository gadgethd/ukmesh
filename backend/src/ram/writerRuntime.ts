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
import { persistEntriesCore } from '../db/packetBatch.js';
import { markMqttPersistenceHealthy } from '../mqtt/client.js';
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
    statsTick: refreshPendingStats,
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
