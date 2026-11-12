import assert from 'node:assert/strict';
import { Redis } from 'ioredis';
import { admitLinkV3Job, LINK_V3_KEYS, linkObservationIdentity, linkPhysicalIdentity, linkQueueLimits } from './dist/queue/linkQueueV3.js';

const redis = new Redis(process.env.TEST_REDIS_URL, { password: process.env.TEST_REDIS_PASSWORD, maxRetriesPerRequest: 1 });
const generation = 'link_rebuild_0123456789abcdef';
function observation(packetHash, scoped = false) {
  const identity = linkObservationIdentity(packetHash, 'receiver', scoped ? generation : undefined);
  return { version: 3, type: 'observe', job_id: identity.jobId, dedupe_key: identity.dedupeKey,
    logical_job_id: linkObservationIdentity(packetHash, 'receiver').jobId, packet_hash: packetHash,
    rx_node_id: 'receiver', src_node_id: 'fixture-\u00e9', path_hashes: ['A1B2'], path_hash_size_bytes: 2,
    ...(scoped ? { generation } : {}) };
}
try {
  switch (process.argv[2]) {
    case 'admit-observe': {
      assert.equal((await admitLinkV3Job(redis, observation('first'))).status, 'worker_unavailable');
      await redis.set(LINK_V3_KEYS.workerHeartbeat, '1', 'EX', 60);
      const payload = observation('first');
      assert.deepEqual(await admitLinkV3Job(redis, payload), { status: 'accepted', jobId: payload.job_id });
      assert.equal(Number(await redis.hget(LINK_V3_KEYS.counters, 'bytes')), Buffer.byteLength(JSON.stringify(payload)));
      break;
    }
    case 'verify-observe':
      assert.equal(await redis.hget(LINK_V3_KEYS.states, observation('first').job_id), 'complete');
      assert.equal(await redis.hget(LINK_V3_KEYS.counters, 'count'), '0');
      break;
    case 'verify-physical': {
      const id = await redis.hget(LINK_V3_KEYS.dedupe, 'physical:live:a:z');
      assert.ok(id?.startsWith('lp_'));
      const payload = JSON.parse(await redis.hget(LINK_V3_KEYS.payloads, id));
      assert.equal(payload.node_a_id, 'a'); assert.equal(payload.node_b_id, 'z');
      assert.equal(Number(await redis.hget(LINK_V3_KEYS.bytes, id)), Buffer.byteLength(await redis.hget(LINK_V3_KEYS.payloads, id)));
      const identity = linkPhysicalIdentity('z', 'a');
      const next = { version: 3, type: 'physical_pair', job_id: identity.jobId, dedupe_key: identity.dedupeKey, node_a_id: 'a', node_b_id: 'z' };
      assert.equal((await admitLinkV3Job(redis, next)).status, 'worker_unavailable');
      assert.deepEqual(await admitLinkV3Job(redis, next, { requireHealthyWorker: false }), { status: 'coalesced', jobId: id });
      break;
    }
    case 'admit-rebuild':
      await redis.set(LINK_V3_KEYS.workerHeartbeat, '1', 'EX', 60);
      await redis.set(LINK_V3_KEYS.rebuild, 'owned-rebuild-token');
      assert.equal((await admitLinkV3Job(redis, observation('frozen'))).status, 'accepted');
      assert.equal((await admitLinkV3Job(redis, observation('scoped', true))).status, 'accepted');
      assert.equal(await redis.llen(LINK_V3_KEYS.deferred), 1);
      assert.equal(await redis.llen(LINK_V3_KEYS.ready), 1);
      break;
    case 'verify-drained':
      assert.deepEqual(await redis.hmget(LINK_V3_KEYS.counters, 'count', 'bytes'), ['0', '0']);
      assert.equal(await redis.zcard(LINK_V3_KEYS.leases), 0);
      assert.equal(await redis.llen(LINK_V3_KEYS.deferred), 0);
      break;
    case 'limits': {
      const value = process.env.LINK_QUEUE_V3_MAX_JOBS;
      const expected = value === '' ? 1 : value === '1.9' ? 1 : value === 'garbage' ? 5000 : value === '9999999' ? 100000 : 5000;
      assert.equal(linkQueueLimits.maxJobs, expected);
      break;
    }
    default: throw Error('unknown contract phase');
  }
  console.log(`link-v3 TypeScript ${process.argv[2]} passed`);
} finally {
  redis.disconnect();
}
