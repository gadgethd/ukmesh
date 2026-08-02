import assert from 'node:assert/strict';
import test from 'node:test';
import { Redis } from 'ioredis';
import { admitViewshedV2Job } from './publisher.js';

const testRedisUrl = process.env['TEST_REDIS_URL'];

test('viewshed v2 producer admission coalesces in-flight position updates durably', {
  skip: testRedisUrl ? false : 'TEST_REDIS_URL is required',
}, async () => {
  const redis = new Redis(testRedisUrl!);
  try {
    await redis.flushdb();
    await redis.set('meshcore:viewshed:worker_heartbeat', '1', 'EX', 30);

    const first = await admitViewshedV2Job(redis, 'fixture-node', 51.5, -1.2, false);
    assert.equal(first, 'accepted');
    assert.equal(await redis.hget('meshcore:viewshed:v2:states', 'fixture-node'), 'queued');
    assert.deepEqual(
      await redis.hmget('meshcore:viewshed:v2:counters', 'count', 'bytes'),
      ['1', String(Buffer.byteLength(await redis.hget('meshcore:viewshed:v2:payloads', 'fixture-node') ?? ''))],
    );

    await redis.hset('meshcore:viewshed:v2:states', 'fixture-node', 'in_flight');
    const second = await admitViewshedV2Job(redis, 'fixture-node', 52.0, -1.2, true);
    assert.equal(second, 'coalesced');
    assert.equal(await redis.sismember('meshcore:viewshed:v2:dirty', 'fixture-node'), 1);
    const payload = JSON.parse(
      await redis.hget('meshcore:viewshed:v2:payloads', 'fixture-node') ?? '{}',
    ) as { lat?: number };
    assert.equal(payload.lat, 52);
  } finally {
    await redis.quit();
  }
});
