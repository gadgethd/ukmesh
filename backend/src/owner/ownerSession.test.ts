import assert from 'node:assert/strict';
import test from 'node:test';
import { decryptOwnerSession, encryptOwnerSession } from './ownerSession.js';

test('owner session v2 carries identity and expiry but no authorization snapshot', () => {
  const previous = process.env['OWNER_COOKIE_SECRET'];
  process.env['OWNER_COOKIE_SECRET'] = 'test-owner-cookie-secret-at-least-32-bytes';
  try {
    const token = encryptOwnerSession({
      v: 2,
      mqttUsername: 'owner',
      exp: 2_000_000_000_000,
    });
    const session = decryptOwnerSession(token);
    assert.deepEqual(session, {
      v: 2,
      mqttUsername: 'owner',
      exp: 2_000_000_000_000,
    });
    assert.doesNotMatch(token, /owner/);
  } finally {
    if (previous === undefined) delete process.env['OWNER_COOKIE_SECRET'];
    else process.env['OWNER_COOKIE_SECRET'] = previous;
  }
});
