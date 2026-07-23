import assert from 'node:assert/strict';
import test from 'node:test';
import { getRedisConnectionOptions, getRedisUrl } from './redis.js';

test('Redis connection options pass a password separately from the URL', () => {
  const previousUrl = process.env['REDIS_URL'];
  const previousPassword = process.env['REDIS_PASSWORD'];
  try {
    process.env['REDIS_URL'] = 'redis://redis:6379/2';
    process.env['REDIS_PASSWORD'] = 'has:@/reserved characters';
    assert.equal(getRedisUrl(), 'redis://redis:6379/2');
    assert.deepEqual(getRedisConnectionOptions(), { password: 'has:@/reserved characters' });

    delete process.env['REDIS_PASSWORD'];
    assert.deepEqual(getRedisConnectionOptions(), {});
  } finally {
    if (previousUrl === undefined) delete process.env['REDIS_URL'];
    else process.env['REDIS_URL'] = previousUrl;
    if (previousPassword === undefined) delete process.env['REDIS_PASSWORD'];
    else process.env['REDIS_PASSWORD'] = previousPassword;
  }
});
