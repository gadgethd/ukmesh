import assert from 'node:assert/strict';
import test from 'node:test';
import { compareAccuracy } from './pathAccuracy.js';

const result = {
  summary: { snapshotSha256: 'frozen', split: 'test', mode: 'single' },
  cases: [{ packetHash: 'packet', receiver: 'rx', source: null, truth: ['A', 'B'], prediction: ['A', 'C'] }],
};

test('accuracy comparisons reject changed snapshots, missing cases or changed labels', () => {
  assert.throws(() => compareAccuracy(result, { ...result, summary: { ...result.summary, snapshotSha256: 'different' } }), /snapshotSha256/);
  assert.throws(() => compareAccuracy(result, { ...result, cases: [] }), /cases/);
  assert.throws(() => compareAccuracy(result, { ...result, cases: [{ ...result.cases[0]!, truth: ['A', 'C'] }] }), /ground truth/);
});

test('paired comparison counts a correction without excluding the previously wrong hop', () => {
  const comparison = compareAccuracy(result, { ...result, cases: [{ ...result.cases[0]!, prediction: ['A', 'B'] }] });
  assert.equal(comparison.improved, 1);
  assert.equal(comparison.regressed, 0);
  assert.equal(comparison.hopGainPercentagePoints, 50);
  assert.equal(comparison.routeGainPercentagePoints, 100);
});
