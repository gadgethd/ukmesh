import assert from 'node:assert/strict';
import test from 'node:test';
import pg from 'pg';
import { Histogram } from 'prom-client';
import { createOperationDiagnostics, metricDifferences } from './operationDiagnostics.js';

test('diagnostics use selected counters and numeric pool options without opening sessions or exposing credentials', async () => {
  const metrics = Object.fromEntries(['dbPoolWaitDuration', 'dbExecutionDuration', 'dbQueryDuration'].map((name, index) => [name,
    new Histogram({ name: 'diagnostic_test_' + index, help: 'test only', labelNames: ['pool', 'outcome'], registers: [] })]));
  const pools = { pool: new pg.Pool({ connectionString: 'postgresql://private:private-secret@127.0.0.1/db', max: 8 }),
    analyticsPool: new pg.Pool({ max: 2 }), ingestPool: new pg.Pool({ max: 2 }) };
  try {
    const loaded: string[] = [];
    const diagnostics = await createOperationDiagnostics(async file => { loaded.push(file); return metrics; }, pools);
    const before = await diagnostics.metrics();
    for (const histogram of Object.values(metrics)) histogram.observe({ pool: 'oltp', outcome: 'success' }, .003);
    const changes = metricDifferences(before, await diagnostics.metrics());
    assert.equal(changes.length, 6);
    assert.deepEqual(changes.filter(value => value.metric.endsWith('_count')).map(value => value.value), [1, 1, 1]);
    assert.deepEqual(changes.filter(value => value.metric.endsWith('_sum')).map(value => value.value), [.003, .003, .003]);
    assert.deepEqual(loaded, ['metrics/database.ts']);
    assert.equal(diagnostics.profile.pool.max, 8);
    assert.equal(JSON.stringify(diagnostics.profile).includes('private'), false);
    assert.deepEqual(diagnostics.pools().ingestPool, { total: 0, idle: 0, waiting: 0 });
  } finally { await Promise.all(Object.values(pools).map(pool => pool.end())); }
});

test('counter differences preserve labels, admit new series and reject reset, missing or nonfinite telemetry', () => {
  const first = { metric: 'duration_sum', labels: { pool: 'oltp', outcome: 'success' }, value: 1 };
  const next = { metric: 'duration_sum', labels: { outcome: 'success', pool: 'oltp' }, value: 2 };
  assert.equal(metricDifferences([first], [next])[0].value, 1);
  assert.equal(metricDifferences([], [first])[0].value, 1);
  assert.throws(() => metricDifferences([first], [{ ...next, value: .5 }]), /regressed/);
  assert.throws(() => metricDifferences([first], []), /disappeared/);
  assert.throws(() => metricDifferences([], [{ ...first, value: Infinity }]), /regressed/);
});
