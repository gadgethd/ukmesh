import assert from 'node:assert/strict';
import test from 'node:test';
import { beginAnalysisRun, normalizeRetiredAnalysisState } from './runState.js';
import { analysisRetirement } from './workloadPolicy.js';

const failed = {
  workload: 'path-history',
  lastStatus: 'failed',
  lastTerminalReason: 'relation "ml_path_prefix_scores" does not exist',
  lastError: 'relation "ml_path_prefix_scores" does not exist',
};

test('retired path history is unavailable with its original failure retained for audit', () => {
  assert.deepEqual(normalizeRetiredAnalysisState(failed), {
    lastStatus: 'unavailable', lastTerminalReason: 'retired', lastError: null,
  });
  assert.equal(analysisRetirement(failed)?.historicalState.lastError, failed.lastError);
  assert.match(analysisRetirement(failed)!.reason, /migration 044/);
});

test('retirement does not hide supported failures or unexpected active runs', () => {
  for (const workload of ['spam-analysis', 'path-learning', 'unknown']) {
    assert.equal(normalizeRetiredAnalysisState({ ...failed, workload }).lastStatus, 'failed');
    assert.equal(analysisRetirement({ ...failed, workload }), null);
  }
  const active = { ...failed, activeRunId: 'unexpected' };
  assert.equal(normalizeRetiredAnalysisState(active).lastError, failed.lastError);
  assert.equal(analysisRetirement(active)?.unexpectedActiveRun, true);
});

test('retired workloads cannot acquire a new lease in either production or test', async () => {
  for (const scope of ['ukmesh', 'test']) {
    await assert.rejects(beginAnalysisRun({
      workload: 'path-history', scope, windowStart: new Date(0), windowEnd: new Date(), totalItems: 0,
    }), /ANALYSIS_WORKLOAD_RETIRED:path-history/);
  }
});
