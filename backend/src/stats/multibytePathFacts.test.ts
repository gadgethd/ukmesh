import assert from 'node:assert/strict';
import test from 'node:test';
import {
  backfillMultibyteObservationIds,
  backfillMultibytePathFacts,
  listMultibyteFactChunks,
  multibyteFactBackfillSql,
  multibyteFactsCoverWindow,
  multibyteObservationIdBatchSql,
  selectMultibyteFactChunkBatch,
  splitMultibyteFactWindow,
} from './multibytePathFacts.js';

test('multibyte fact backfill keys every observation row instead of packet hashes', () => {
  const sql = multibyteFactBackfillSql();
  assert.match(sql, /p\.observation_id/);
  assert.match(sql, /p\.observation_id IS NOT NULL/);
  assert.match(sql, /ON CONFLICT \(observation_id\)/);
  assert.doesNotMatch(sql, /ON CONFLICT \(packet_hash\)/);
  assert.match(sql, /meshcore_decode_multibyte_path/);
});

test('off-peak backfill selects an explicit bounded chunk batch', () => {
  const chunks = [
    { chunkSchema: 'ts', chunkName: 'c1', rangeStart: new Date(1), rangeEnd: new Date(2), wasCompressed: true },
    { chunkSchema: 'ts', chunkName: 'c2', rangeStart: new Date(2), rangeEnd: new Date(3), wasCompressed: false },
  ];
  assert.deepEqual(selectMultibyteFactChunkBatch(chunks, 1, 1), [chunks[1]]);
  assert.throws(() => selectMultibyteFactChunkBatch(chunks, -1, 1), /INVALID_MULTIBYTE_FACT_CHUNK_INDEX/);
  assert.throws(() => selectMultibyteFactChunkBatch(chunks, 0, 5), /INVALID_MULTIBYTE_FACT_CHUNK_LIMIT/);
});

test('off-peak fact writes split one selected chunk into bounded statement windows', () => {
  const windows = splitMultibyteFactWindow(
    new Date('2026-08-01T22:00:00.000Z'),
    new Date('2026-08-02T12:30:00.000Z'),
    360,
  );
  assert.deepEqual(windows.map((window) => ({
    windowStart: window.windowStart.toISOString(),
    cutoff: window.cutoff.toISOString(),
  })), [
    { windowStart: '2026-08-01T22:00:00.000Z', cutoff: '2026-08-02T04:00:00.000Z' },
    { windowStart: '2026-08-02T04:00:00.000Z', cutoff: '2026-08-02T10:00:00.000Z' },
    { windowStart: '2026-08-02T10:00:00.000Z', cutoff: '2026-08-02T12:30:00.000Z' },
  ]);
  assert.throws(
    () => splitMultibyteFactWindow(new Date(2), new Date(1), 360),
    /INVALID_MULTIBYTE_FACT_WINDOW/,
  );
  assert.throws(
    () => splitMultibyteFactWindow(new Date(1), new Date(2), 0),
    /INVALID_MULTIBYTE_FACT_WINDOW_MINUTES/,
  );
});

test('historical observation ids are populated oldest-first in bounded physical-row batches', async () => {
  const sql = multibyteObservationIdBatchSql();
  assert.match(sql, /p\.observation_id IS NULL/);
  assert.match(sql, /ORDER BY p\.time ASC/);
  assert.doesNotMatch(sql, /ORDER BY p\.time ASC, p\.tableoid/);
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
  assert.match(calls[0]?.sql ?? '', /LEAST\(multibyte_path_fact_state\.covered_from/);
  assert.match(calls[0]?.sql ?? '', /GREATEST\(multibyte_path_fact_state\.covered_through/);
});

test('chunk inventory is ordered and clipped to the bounded requested window', async () => {
  const chunks = await listMultibyteFactChunks(async (_sql, params) => {
    assert.deepEqual(params, ['2026-08-01T12:00:00.000Z', '2026-08-09T14:00:00.000Z']);
    return { rows: [
      {
        chunk_schema: '_timescaledb_internal',
        chunk_name: '_hyper_1_70_chunk',
        range_start: new Date('2026-07-30T00:00:00.000Z'),
        range_end: new Date('2026-08-06T00:00:00.000Z'),
        is_compressed: true,
      },
      {
        chunk_schema: '_timescaledb_internal',
        chunk_name: '_hyper_1_90_chunk',
        range_start: new Date('2026-08-06T00:00:00.000Z'),
        range_end: new Date('2026-08-13T00:00:00.000Z'),
        is_compressed: false,
      },
    ] };
  }, {
    windowStart: new Date('2026-08-01T12:00:00.000Z'),
    cutoff: new Date('2026-08-09T14:00:00.000Z'),
  });
  assert.deepEqual(chunks.map((chunk) => ({
    ...chunk,
    rangeStart: chunk.rangeStart.toISOString(),
    rangeEnd: chunk.rangeEnd.toISOString(),
  })), [
    {
      chunkSchema: '_timescaledb_internal',
      chunkName: '_hyper_1_70_chunk',
      rangeStart: '2026-08-01T12:00:00.000Z',
      rangeEnd: '2026-08-06T00:00:00.000Z',
      wasCompressed: true,
    },
    {
      chunkSchema: '_timescaledb_internal',
      chunkName: '_hyper_1_90_chunk',
      rangeStart: '2026-08-06T00:00:00.000Z',
      rangeEnd: '2026-08-09T14:00:00.000Z',
      wasCompressed: false,
    },
  ]);
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
