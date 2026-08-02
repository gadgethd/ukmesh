import assert from 'node:assert/strict';
import test from 'node:test';
import {
  PATH_HISTORY_REFRESH_INTERVAL_MS,
  pathHistoryNextDelayMs,
  pathHistoryRetryIntervalMs,
} from './pathHistorySchedule.js';

test('path history retries active leases promptly with bounded configuration', () => {
  assert.equal(pathHistoryRetryIntervalMs(undefined), 60_000);
  assert.equal(pathHistoryRetryIntervalMs('1000'), 10_000);
  assert.equal(pathHistoryRetryIntervalMs('900000'), 300_000);
  assert.equal(pathHistoryNextDelayMs(true, 45_000), 45_000);
  assert.equal(pathHistoryNextDelayMs(false, 45_000), PATH_HISTORY_REFRESH_INTERVAL_MS);
});
