import assert from 'node:assert/strict';
import test from 'node:test';
import {
  PATH_HISTORY_REFRESH_INTERVAL_MS,
  pathHistoryRetryDelayMs,
  pathHistoryNextDelayMs,
  pathHistoryRetryIntervalMs,
} from './pathHistorySchedule.js';

test('path history retries with bounded exponential backoff', () => {
  assert.equal(pathHistoryRetryIntervalMs(undefined), 300_000);
  assert.equal(pathHistoryRetryIntervalMs('1000'), 300_000);
  assert.equal(pathHistoryRetryIntervalMs('900000'), 900_000);
  assert.equal(pathHistoryRetryDelayMs(1, 300_000), 300_000);
  assert.equal(pathHistoryRetryDelayMs(2, 300_000), 600_000);
  assert.equal(pathHistoryRetryDelayMs(3, 300_000), 1_200_000);
  assert.equal(pathHistoryRetryDelayMs(4, 300_000), 1_800_000);
  assert.equal(pathHistoryRetryDelayMs(10, 300_000), 1_800_000);
  assert.equal(pathHistoryNextDelayMs(true, 300_000, 2), 600_000);
  assert.equal(pathHistoryNextDelayMs(false, 300_000, 2), PATH_HISTORY_REFRESH_INTERVAL_MS);
});
