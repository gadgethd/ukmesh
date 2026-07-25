import assert from 'node:assert/strict';
import test from 'node:test';
import { isBackendSiteLocalAddress } from './routes.js';

test('operator routes trust only the actual loopback peer', () => {
  assert.equal(isBackendSiteLocalAddress('127.0.0.1'), true);
  assert.equal(isBackendSiteLocalAddress('::1'), true);
  assert.equal(isBackendSiteLocalAddress('::ffff:127.0.0.1'), true);
  assert.equal(isBackendSiteLocalAddress('10.0.0.5'), false);
  assert.equal(isBackendSiteLocalAddress('172.20.0.2'), false);
  assert.equal(isBackendSiteLocalAddress('192.168.1.10'), false);
  assert.equal(isBackendSiteLocalAddress('203.0.113.10'), false);
});
