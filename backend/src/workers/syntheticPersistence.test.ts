import assert from 'node:assert/strict';
import test from 'node:test';
import { persistSyntheticCheckResults } from './syntheticPersistence.js';

test('synthetic results are persisted in one jsonb recordset statement', async () => {
  const calls: Array<{ text: string; params?: unknown[] }> = [];
  await persistSyntheticCheckResults([
    { name: 'a', status: 'ok', latencyMs: 10, detail: 'first' },
    { name: 'b', status: 'failed', latencyMs: 20, detail: 'second' },
    { name: 'c', status: 'ok', latencyMs: 30, detail: 'third' },
    { name: 'd', status: 'ok', latencyMs: 40, detail: 'fourth' },
  ], async (text, params) => {
    calls.push({ text, params });
    return {};
  });

  assert.equal(calls.length, 1);
  assert.match(calls[0]!.text, /jsonb_to_recordset/);
  const payload = JSON.parse(String(calls[0]!.params?.[0])) as unknown[];
  assert.equal(payload.length, 4);
  assert.deepEqual(payload[1], {
    name: 'b',
    status: 'failed',
    latency_ms: 20,
    detail: 'second',
  });
});
