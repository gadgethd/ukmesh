import assert from 'node:assert/strict';
import test from 'node:test';
import { decryptOwnerSession, encryptOwnerSession } from './ownerSession.js';

test('new owner cookies carry identity and expiry but no authorization snapshot', () => {
  process.env['OWNER_COOKIE_SECRET'] = 'test-only-owner-cookie-secret';
  const token = encryptOwnerSession({
    v: 2,
    mqttUsername: 'owner-a',
    exp: 123456,
  });
  assert.deepEqual(decryptOwnerSession(token), {
    v: 2,
    mqttUsername: 'owner-a',
    exp: 123456,
  });
  assert.equal(token.includes('nodeIds'), false);
});
