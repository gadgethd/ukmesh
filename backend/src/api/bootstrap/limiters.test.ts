import assert from 'node:assert/strict';
import test from 'node:test';
import { parseApiRateLimitMax } from './limiters.js';

test('global API rate limit keeps a safe production default and bounded overrides', () => {
  assert.equal(parseApiRateLimitMax(undefined), 120);
  assert.equal(parseApiRateLimitMax('1'), 1);
  assert.equal(parseApiRateLimitMax('1000000'), 1_000_000);
  for (const invalid of ['0', '-1', '1.5', 'NaN', '1000001']) {
    assert.throws(() => parseApiRateLimitMax(invalid), /API_RATE_LIMIT_MAX/);
  }
});
