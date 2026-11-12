import assert from 'node:assert/strict';
import test from 'node:test';
import type { DatabaseQueryFn } from '../db/index.js';
import { regionRebuildSettings } from './rebuildSettings.js';
import { RegionSnapshotRefresher } from './snapshotRefresh.js';

const settings = regionRebuildSettings({});
test('region tuning keeps finite source, execution and poll limits', () => {
  for (const value of ['NaN','Infinity','bad','']) {
    const env = Object.fromEntries(['WINDOW_DAYS','MAX_PACKETS','MAX_NODES','STATEMENT_TIMEOUT_MS','RUN_TIMEOUT_MS','POLL_MS','MAX_AGE_MS'].map(key => ['REGION_SCOPE_'+key,value]));
    assert.deepEqual(regionRebuildSettings(env), settings);
  }
  assert.equal(regionRebuildSettings({ REGION_SCOPE_ALLOW_EMPTY: '1' }).allowEmpty, true);
  assert.equal(regionRebuildSettings({ REGION_SCOPE_RUN_TIMEOUT_MS: '0' }).runTimeoutMs, 5000);
  assert.equal(regionRebuildSettings({ REGION_SCOPE_MAX_PACKETS: '1e99' }).maxPackets, 2_000_000);
});

test('region refresh skips unavailable authority, coalesces calls and verifies actual publication', async t => {
  t.mock.method(console, 'error', () => undefined);
  let ready = false, generation = 1, certified = 0, calls = 0;
  let release!: () => void;
  const gate = new Promise<void>(resolve => { release = resolve; });
  const query: DatabaseQueryFn = async sql => ({ rows: (sql.includes('FROM ops.public_visibility_state')
    ? ready ? [{ generation: String(generation) }] : []
    : [{ visibility_generation: String(certified), window_days: 92, finished_at: new Date(), fresh: true }]) as never });
  const refresh = new RegionSnapshotRefresher({ query, settings, run: async () => { calls++; await gate; certified = generation; } });
  const signal = new AbortController().signal;
  assert.equal(await refresh.runOnce(signal), 'unavailable');
  assert.equal(calls, 0);
  ready = true;
  const first = refresh.runOnce(signal), second = refresh.runOnce(signal);
  assert.equal(first, second);
  release();
  assert.equal(await first, 'rebuilt');
  assert.equal(calls, 1);
  assert.equal(await refresh.runOnce(signal), 'fresh');
  generation++;
  assert.equal(await refresh.runOnce(signal), 'rebuilt');
  assert.equal(calls, 2);
});

test('an uncertified child success backs off, but a newer ready generation can repair immediately', async t => {
  t.mock.method(console, 'error', () => undefined);
  let now = 0, generation = 1, certified = 0, calls = 0;
  const query: DatabaseQueryFn = async sql => ({ rows: (sql.includes('FROM ops.public_visibility_state')
    ? [{ generation: String(generation) }]
    : [{ visibility_generation: String(certified), window_days: 92, finished_at: new Date(), fresh: true }]) as never });
  const refresh = new RegionSnapshotRefresher({ query, settings, now: () => now,
    run: async () => { calls++; if (generation === 2) certified = generation; } });
  const signal = new AbortController().signal;
  assert.equal(await refresh.runOnce(signal), 'failed');
  assert.equal(await refresh.runOnce(signal), 'backoff');
  assert.equal(calls, 1);
  now += settings.pollMs;
  assert.equal(await refresh.runOnce(signal), 'failed');
  generation = 2;
  assert.equal(await refresh.runOnce(signal), 'rebuilt');
  assert.equal(calls, 3);
  assert.equal(await refresh.runOnce(signal), 'fresh');
});
