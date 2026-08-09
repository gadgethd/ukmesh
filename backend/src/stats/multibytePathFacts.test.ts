import assert from 'node:assert/strict';
import test from 'node:test';
import {
  backfillMultibyteObservationIds,
  backfillMultibytePathFacts,
  multibyteFactBackfillSql,
  multibyteFactsCoverWindow,
  multibyteObservationIdBatchSql,
} from './multibytePathFacts.js';

test('multibyte fact backfill keys every observation row instead of packet hashes', () => {
  const sql = multibyteFactBackfillSql();
  assert.match(sql, /p\.observation_id/);
  assert.match(sql, /p\.observation_id IS NOT NULL/);
  assert.match(sql, /ON CONFLICT \(observation_id\)/);
  assert.doesNotMatch(sql, /ON CONFLICT \(packet_hash\)/);
  assert.match(sql, /meshcore_decode_multibyte_path/);
});

test('historical observation ids are populated oldest-first in bounded physical-row batches', async () => {
  const sql = multibyteObservationIdBatchSql();
  assert.match(sql, /p\.observation_id IS NULL/);
  assert.match(sql, /ORDER BY p\.time ASC, p\.tableoid ASC, p\.ctid ASC/);
  assert.match(sql, /LIMIT \$3::integer/);
  assert.match(sql, /SET observation_id = gen_random_uuid\(\)/);
  assert.doesNotMatch(sql, /ALTER COLUMN observation_id SET DEFAULT/i);

  const calls: unknown[][] = [];
  const affected = await backfillMultibyteObservationIds(async (_text, params) => {
    calls.push(params ?? []);
    return { rows: [{ affected_rows: 500 }] };
  }, {
    windowStart: new Date('2026-08-01T00:00:00.000Z'),
    cutoff: new Date('2026-08-09T00:00:00.000Z'),
    batchSize: 500,
  });
  assert.equal(affected, 500);
  assert.deepEqual(calls, [[
    '2026-08-01T00:00:00.000Z',
    '2026-08-09T00:00:00.000Z',
    500,
  ]]);
});

test('bounded backfill pins cutoff and privacy generation', async () => {
  const calls: Array<{ sql: string; params?: unknown[] }> = [];
  const query = async (sql: string, params?: unknown[]) => {
    calls.push({ sql, params });
    return { rows: [{ affected_rows: 42 }] };
  };
  const windowStart = new Date('2026-08-01T00:00:00.000Z');
  const cutoff = new Date('2026-08-09T00:00:00.000Z');
  const result = await backfillMultibytePathFacts(query, {
    windowStart,
    cutoff,
    visibilityGeneration: 7,
  });
  assert.equal(result.affectedRows, 42);
  assert.deepEqual(calls[0]?.params, [windowStart.toISOString(), cutoff.toISOString(), 7]);
});

test('coverage fence rejects a state from another privacy generation', async () => {
  const calls: unknown[][] = [];
  const ready = await multibyteFactsCoverWindow(async (_sql, params) => {
    calls.push(params ?? []);
    return { rows: [{ ready: false }] };
  }, {
    windowStart: new Date('2026-08-02T00:00:00.000Z'),
    visibilityGeneration: 9,
  });
  assert.equal(ready, false);
  assert.deepEqual(calls, [[9, '2026-08-02T00:00:00.000Z']]);
});
