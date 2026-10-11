import assert from 'node:assert/strict';
import test from 'node:test';
import { Redis } from 'ioredis';
import {
  admitPacketToRam,
  configureRamStore,
  reconcileRamQueueBytes,
} from './liveStore.js';
import {
  accountedRamEntryBytes,
  getRamQueueBytesKey,
  getRamQuarantineKey,
  getRamStreamKey,
} from './protocol.js';
import { createRamWriter } from './writer.js';

const redisUrl = process.env['TEST_REDIS_URL'];
const localRedis = redisUrl && ['127.0.0.1', 'localhost'].includes(new URL(redisUrl).hostname);

async function fixture(t: { after: (callback: () => Promise<void>) => void }): Promise<Redis> {
  process.env['MQTT_INGEST_MODE'] = 'ram';
  process.env['MQTT_RAM_STREAM_KEY'] = `meshcore:ingest:integration:${process.pid}:${Date.now()}`;
  const client = new Redis(redisUrl!, { maxRetriesPerRequest: 1, enableOfflineQueue: false });
  await client.ping();
  configureRamStore(client);
  t.after(async () => {
    configureRamStore(null);
    const keys = await client.keys(`${getRamStreamKey()}*`);
    if (keys.length) await client.del(...keys);
    await client.quit();
  });
  return client;
}

const packet = () => ({
  packetHash: 'AB'.repeat(32), rxNodeId: 'CD'.repeat(32),
  topic: `meshcore/ZZZ/${'CD'.repeat(32)}/packets`,
  rawHex: 'DEADBEEF', packetType: 4, routeType: 1, hopCount: 2,
  pathHashes: ['ABCD'], pathHashSizeBytes: 2, network: 'ukmesh',
});

test('real Redis F05/F09 admission replay and atomic byte reconciliation', {
  skip: localRedis ? false : 'TEST_REDIS_URL must point to a loopback fixture',
}, async (t) => {
  const client = await fixture(t);
  const first = await admitPacketToRam(packet(), { key: 'delivery-42', redelivered: false });
  const replay = await admitPacketToRam(packet(), { key: 'delivery-42', redelivered: true });
  assert.equal(replay.observationId, first.observationId);
  assert.equal(await client.xlen(getRamStreamKey()), 1);
  const raw = await client.xrange(getRamStreamKey(), '-', '+');
  const encoded = raw[0]?.[1]?.[1];
  assert.ok(encoded);
  await client.set(getRamQueueBytesKey(), '1');
  assert.equal(await reconcileRamQueueBytes(), accountedRamEntryBytes(encoded));
  assert.equal(Number(await client.get(getRamQueueBytesKey())), accountedRamEntryBytes(encoded));
});

test('real ioredis F11 quarantine preserves malformed source until bounded XADD succeeds', {
  skip: localRedis ? false : 'TEST_REDIS_URL must point to a loopback fixture',
}, async (t) => {
  const client = await fixture(t);
  const badId = await client.xadd(getRamStreamKey(), '*', 'e', 'not-json');
  assert.ok(badId);
  const writer = createRamWriter({
    redis: client,
    persist: async () => undefined,
    now: () => Date.now(),
    sleep: async () => undefined,
    batchSize: () => 1,
    maxAgeMs: () => 1,
    streamKey: getRamStreamKey,
    group: () => 'integration-writer',
    consumer: () => 'test',
    statsTick: null,
    markHealthy: () => undefined,
    onCommitted: null,
    log: () => undefined,
  });
  await writer.start();
  await writer.tickOnce();
  assert.equal((await client.xrange(getRamQuarantineKey(), '-', '+')).length, 1);
  assert.equal((await client.xrange(getRamStreamKey(), badId, badId)).length, 0);
});
