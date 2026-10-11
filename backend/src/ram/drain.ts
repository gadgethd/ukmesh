/** Standalone RAM backlog drain for a stopped MQTT admission source.
 * Run with the same DATABASE_URL, REDIS_URL/PASSWORD and stream settings as
 * the backend. Exit 0 only after every stream entry has committed and aged
 * past the reader-overlap retention interval, then been deleted by the writer.
 */
import { Redis } from 'ioredis';
import { query, closeDb, withTransaction } from '../db/index.js';
import { configurePacketBatch } from '../db/packetBatch.js';
import { getRedisConnectionOptions, getRedisUrl } from '../platform/config/redis.js';
import { getRamQuarantineKey, getRamStreamKey } from './protocol.js';
import { closeRamStore, reconcileRamQueueBytes } from './liveStore.js';
import { startRamWriter, closeRamWriter } from './writerRuntime.js';

async function main(): Promise<void> {
  process.env['MQTT_INGEST_MODE'] = 'ram';
  const deadlineMs = Number(process.env['MQTT_RAM_DRAIN_DEADLINE_MS'] ?? 600_000);
  if (!Number.isSafeInteger(deadlineMs) || deadlineMs < 130_000 || deadlineMs > 3_600_000) {
    throw new Error('MQTT_RAM_DRAIN_DEADLINE_MS must be 130000..3600000');
  }
  const stopAt = Date.now() + deadlineMs;
  configurePacketBatch(query, withTransaction);
  const redis = new Redis(getRedisUrl(), {
    ...getRedisConnectionOptions(), maxRetriesPerRequest: 2,
    enableOfflineQueue: false,
  });
  let started = false;
  try {
    await query('SELECT 1');
    await redis.ping();
    await startRamWriter();
    started = true;
    const stream = getRamStreamKey();
    while (Date.now() < stopAt) {
      const count = await redis.xlen(stream);
      if (count === 0) {
        await closeRamWriter(Math.max(1, stopAt - Date.now()));
        started = false;
        if (await redis.xlen(stream) !== 0) {
          throw new Error('RAM_DRAIN_INCOMPLETE: stream received new entries during drain');
        }
        if (await reconcileRamQueueBytes() !== 0) {
          throw new Error('RAM_DRAIN_INCOMPLETE: byte counter is not zero');
        }
        const quarantined = await redis.xlen(getRamQuarantineKey());
        if (quarantined > 0) {
          throw new Error(`RAM_DRAIN_INCOMPLETE: ${quarantined} quarantined entries require review`);
        }
        console.log('RAM_DRAIN_COMPLETE: stream empty, writer PEL empty, retention swept');
        return;
      }
      await new Promise((resolve) => setTimeout(resolve, 1_000));
    }
    throw new Error(`RAM_DRAIN_INCOMPLETE: deadline reached with ${await redis.xlen(stream)} entries`);
  } finally {
    if (started) {
      await closeRamWriter(Math.max(1, stopAt - Date.now())).catch((error: unknown) => {
        console.error('[ram] final drain incomplete', error);
      });
    }
    redis.disconnect();
    await closeRamStore();
    await closeDb();
  }
}

main().catch((error: unknown) => {
  console.error(error instanceof Error ? error.message : error);
  process.exitCode = 1;
});
