import assert from 'node:assert/strict';
import test from 'node:test';
import {
  createPublicFeedRepository,
  getActivePublicFeedNodeIds,
  getActivePublicFeedReaderUsernames,
} from './publicFeedRepository.js';

const nodeId = 'A'.repeat(64);

test('active public-feed readers require opt-in and a visible packet in the last 24 hours', async () => {
  let sql = '';
  const readers = await getActivePublicFeedReaderUsernames(async (text) => {
    sql = text;
    return { rows: [{ owner_username: 'alice' }] };
  });

  assert.deepEqual([...readers], ['alice']);
  assert.match(sql, /settings\.enabled = TRUE/);
  assert.match(sql, /packet\.time > NOW\(\) - INTERVAL '24 hours'/);
  assert.match(sql, /packet\.visibility_ok IS TRUE/);
  assert.match(sql, /packet\.is_private IS NOT TRUE/);
  assert.match(sql, /packet\.network IS DISTINCT FROM 'test'/);
});

test('active public-feed node IDs require opt-in and recent eligible activity', async () => {
  let sql = '';
  const nodeIds = await getActivePublicFeedNodeIds(async (text) => {
    sql = text;
    return { rows: [{ node_id: 'b'.repeat(64) }, { node_id: 'A'.repeat(64) }] };
  });

  assert.deepEqual(nodeIds, ['A'.repeat(64), 'B'.repeat(64)]);
  assert.match(sql, /settings\.enabled = TRUE/);
  assert.match(sql, /packet\.time > NOW\(\) - INTERVAL '24 hours'/);
  assert.match(sql, /packet\.visibility_ok IS TRUE/);
  assert.match(sql, /packet\.is_private IS NOT TRUE/);
  assert.match(sql, /packet\.network IS DISTINCT FROM 'test'/);
});

test('owner public-feed status reports recent eligibility separately from opt-in state', async () => {
  let params: unknown[] | undefined;
  const repository = createPublicFeedRepository(async (sql, queryParams) => {
    assert.match(sql, /recent_activity/);
    params = queryParams;
    return {
      rows: [{
        enabled: true,
        recent_packet_count: 2,
        last_forwarded_at: '2026-09-20T12:00:00.000Z',
      }],
    };
  });

  assert.deepEqual(await repository.accountStatus('alice', [nodeId]), {
    enabled: true,
    nodeCount: 1,
    eligible: true,
    recentPacketCount: 2,
    lastForwardedAt: '2026-09-20T12:00:00.000Z',
  });
  assert.deepEqual(params, ['alice', [nodeId]]);
});
