import assert from 'node:assert/strict';
import test from 'node:test';
import { once } from 'node:events';
import { Redis } from 'ioredis';
import {
  admitPacketToRam,
  configureRamStore,
  reconcileRamQueueBytes,
  refreshPendingStats,
} from './liveStore.js';
import {
  accountedRamEntryBytes,
  getRamQueueBytesKey,
  getRamQuarantineKey,
  getRamStreamKey,
  getRamConsumerGroup,
} from './protocol.js';
import { createRamWriter } from './writer.js';
import { startMetricsServer, closeMetricsServer } from '../metricsServer.js';

const redisUrl = process.env['TEST_REDIS_URL'];
const localRedis = redisUrl && ['127.0.0.1', 'localhost'].includes(new URL(redisUrl).hostname);

async function fixture(t: { after: (callback: () => Promise<void>) => void }): Promise<Redis> {
  process.env['MQTT_INGEST_MODE'] = 'ram';
  process.env['MQTT_RAM_STREAM_KEY'] = `meshcore:ingest:integration:${process.pid}:${Date.now()}`;
  const client = new Redis(redisUrl!, {
    lazyConnect: true,
    connectTimeout: 5_000,
    maxRetriesPerRequest: 1,
    enableOfflineQueue: false,
  });
  try {
    await client.connect();
    await client.ping();
  } catch (error) {
    client.disconnect();
    throw error;
  }
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

test('F14 real loaded and empty queue gauges match pending, retained, PEL and bytes', {
  skip: localRedis ? false : 'TEST_REDIS_URL must point to a loopback fixture',
}, async (t) => {
  const client = await fixture(t);
  const key = getRamStreamKey(), group = getRamConsumerGroup();
  await client.xgroup('CREATE', key, group, '0', 'MKSTREAM');
  const metrics=startMetricsServer(0);await once(metrics,'listening');
  t.after(closeMetricsServer);
  const address=metrics.address();assert.ok(address&&typeof address!=='string');
  const gauge = async (suffix: string) => {
    const response=await fetch(`http://127.0.0.1:${address.port}/metrics`);
    assert.equal(response.status,200);
    const metric=(await response.text()).match(new RegExp(`^meshcore_ram_ingest_${suffix} ([0-9.e+-]+)$`,'m'));
    assert.ok(metric,`${suffix} must be exposed by the actual HTTP metrics server`);
    return Number(metric[1]);
  };
  await Promise.all(Array.from({length:6},(_,i)=>admitPacketToRam({...packet(),packetHash:i.toString(16).padStart(64,'0')})));
  const rows = await client.xrange(key,'-','+');
  await client.xreadgroup('GROUP',group,'loaded-metrics','COUNT',2,'STREAMS',key,'>');
  await client.hset(`${key}:committed`,rows[2]![0],String(Date.now()),rows[3]![0],String(Date.now()));
  await refreshPendingStats();
  assert.equal(await gauge('queue_depth'),6);
  assert.equal(await gauge('pending_depth'),4);
  assert.equal(await gauge('retained_depth'),2);
  assert.equal(await gauge('pel_depth'),2);
  assert.ok(Number(await gauge('queue_bytes'))>0);
  assert.equal(await gauge('queue_reserved_bytes'),rows.reduce((n,row)=>n+accountedRamEntryBytes(row[1][1]!),0));
  await client.xack(key,group,...rows.map(([id])=>id));
  await client.xdel(key,...rows.map(([id])=>id));
  await client.del(key,`${key}:committed`);
  await reconcileRamQueueBytes();
  await refreshPendingStats();
  for(const suffix of ['queue_depth','pending_depth','retained_depth','pel_depth','queue_bytes','queue_reserved_bytes','oldest_pending_age_seconds','oldest_pel_age_seconds']) {
    assert.equal(await gauge(suffix),0,`empty ${suffix} must not retain its loaded value`);
  }
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
  await client.del(getRamQueueBytesKey());
  await admitPacketToRam({ ...packet(), packetHash: 'EF'.repeat(32) }, { key: 'delivery-43', redelivered: false });
  const afterRecovery = await client.xrange(getRamStreamKey(), '-', '+');
  const expected = afterRecovery.reduce((sum, row) => sum + accountedRamEntryBytes(row[1][1]!), 0);
  assert.equal(Number(await client.get(getRamQueueBytesKey())), expected,
    'F09 admission reconciles a lost counter before reserving a second entry');
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
