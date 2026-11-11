import assert from 'node:assert/strict';
import test from 'node:test';
import { mkdtemp, rm } from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import { createSyntheticAlertEvaluator } from './syntheticAlerts.js';
import { DurableAlertQueue } from './alertDeliveryQueue.js';
import { handleCheckResults } from './synthetic-monitor.js';

test('DB archival failure cannot suppress alert evaluation', async () => {
  let evaluated = false;
  await handleCheckResults([{ name: 'dependency_readiness', status: 'failed', latencyMs: 1, detail: 'DB unavailable' }], {
    evaluate: async () => { evaluated = true; },
    persist: async () => { assert.ok(evaluated); throw new Error('database offline'); },
  });
  assert.ok(evaluated);
});

test('HTTP failure retries from disk after restart, with recovery state preserved', async (t) => {
  const directory = await mkdtemp(path.join(os.tmpdir(), 'synthetic-notifications-'));
  t.after(() => rm(directory, { recursive: true, force: true }));
  let now = Date.now();
  let attempts = 0;
  const options = { directory: path.join(directory, 'queue'), maxItems: 100, maxAttempts: 5,
    backoffBaseMs: 10, backoffCapMs: 100, now: () => now,
    send: async () => { attempts++; throw new Error('HTTP 503'); } };
  const queue = new DurableAlertQueue(options);
  await queue.initialize();
  const evaluator = await createSyntheticAlertEvaluator({ queue, statePath: path.join(directory, 'state.json'), threshold: 1 });
  await evaluator([{ name: 'stats_api', status: 'failed', latencyMs: 2, detail: 'failed' }]);
  await queue.processDue();
  assert.equal(attempts, 1);
  assert.equal(queue.metrics().pendingCount, 1);
  const delivered: string[] = [];
  const restarted = new DurableAlertQueue({ ...options, send: async (receipt) => { delivered.push(receipt.status); } });
  await restarted.initialize();
  const recoveredEvaluator = await createSyntheticAlertEvaluator({ queue: restarted, statePath: path.join(directory, 'state.json'), threshold: 1 });
  await recoveredEvaluator([{ name: 'stats_api', status: 'ok', latencyMs: 2, detail: 'ok' }]);
  now += 1_000;
  await restarted.processDue();
  assert.deepEqual(delivered, ['firing', 'recovery']);
  assert.equal(restarted.metrics().pendingCount, 0);
});
