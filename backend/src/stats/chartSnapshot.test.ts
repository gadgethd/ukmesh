import assert from 'node:assert/strict';
import test from 'node:test';
import type { QueryResultRow } from 'pg';
import {
  CHART_SNAPSHOT_MAX_BYTES,
  CHART_SNAPSHOT_SCHEMA_VERSION,
  loadStoredChartSnapshot,
  saveStoredChartSnapshot,
  validateChartSnapshotPayload,
} from './chartSnapshot.js';

const NOW = Date.parse('2026-07-30T12:00:00Z');

function payload(scope = 'ukmesh', generatedAt = '2026-07-30T11:59:00Z') {
  return {
    snapshot: { status: 'complete', scope, generatedAt, visibilityGeneration: 7 },
    packetsPerHour: [],
  };
}

test('chart snapshot validation requires a complete, current, matching bounded payload', () => {
  assert.equal(
    validateChartSnapshotPayload(payload(), 'ukmesh', 6 * 60 * 60_000, NOW)?.generatedAtMs,
    Date.parse('2026-07-30T11:59:00Z'),
  );
  assert.equal(validateChartSnapshotPayload(payload('test'), 'ukmesh', 6 * 60 * 60_000, NOW), null);
  assert.equal(
    validateChartSnapshotPayload(payload('ukmesh', '2026-07-30T05:59:59Z'), 'ukmesh', 6 * 60 * 60_000, NOW),
    null,
  );
  assert.equal(
    validateChartSnapshotPayload(payload('ukmesh', '2026-07-30T12:01:01Z'), 'ukmesh', 6 * 60 * 60_000, NOW),
    null,
  );
  assert.equal(
    validateChartSnapshotPayload({
      ...payload(),
      oversized: 'x'.repeat(CHART_SNAPSHOT_MAX_BYTES),
    }, 'ukmesh', 6 * 60 * 60_000, NOW),
    null,
  );
  assert.equal(
    validateChartSnapshotPayload(payload(), 'ukmesh', 6 * 60 * 60_000, NOW, 8),
    null,
  );
  assert.ok(validateChartSnapshotPayload(payload(), 'ukmesh', 6 * 60 * 60_000, NOW, 7));
});

test('chart snapshot store pins the schema version and refuses older overwrites', async () => {
  const calls: Array<{ text: string; params?: unknown[] }> = [];
  const query = async <T extends QueryResultRow = QueryResultRow>(
    text: string,
    params?: unknown[],
  ): Promise<{ rows: T[] }> => {
    calls.push({ text, params });
    if (text.includes('SELECT scope_key')) {
      return {
        rows: [{
          scope_key: 'ukmesh',
          schema_version: CHART_SNAPSHOT_SCHEMA_VERSION,
          visibility_generation: '7',
          generated_at: '2026-07-30T11:59:00Z',
          payload: payload(),
        }] as T[],
      };
    }
    if (text.includes('INSERT INTO stats_chart_snapshots')) {
      return { rows: [{ scope_key: 'ukmesh' }] as T[] };
    }
    return { rows: [] };
  };

  const loaded = await loadStoredChartSnapshot(query, 'ukmesh', 7);
  assert.equal(loaded?.schema_version, CHART_SNAPSHOT_SCHEMA_VERSION);
  assert.deepEqual(calls[0]?.params, ['ukmesh', CHART_SNAPSHOT_SCHEMA_VERSION, 7]);

  assert.equal(
    await saveStoredChartSnapshot(query, 'ukmesh', payload(), 7, 6 * 60 * 60_000, NOW),
    true,
  );
  const write = calls[1];
  assert.ok(write);
  assert.match(write.text, /ON CONFLICT \(scope_key\) DO UPDATE/);
  assert.match(write.text, /generated_at <= EXCLUDED\.generated_at/);
  assert.match(write.text, /FROM public_visibility_state/);
  assert.match(write.text, /generation = \$3/);
  assert.equal(write.params?.[0], 'ukmesh');
  assert.equal(write.params?.[1], CHART_SNAPSHOT_SCHEMA_VERSION);
  assert.equal(write.params?.[2], 7);
  assert.equal(write.params?.[3], '2026-07-30T11:59:00.000Z');
});

test('chart snapshot store rejects invalid scope keys before querying', async () => {
  let calls = 0;
  const query = async <T extends QueryResultRow = QueryResultRow>(): Promise<{ rows: T[] }> => {
    calls += 1;
    return { rows: [] };
  };
  await assert.rejects(loadStoredChartSnapshot(query, '../private', 7), /scope is invalid/);
  assert.equal(calls, 0);
});
