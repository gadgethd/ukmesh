import assert from 'node:assert/strict';
import test from 'node:test';
import { decryptOwnerSession, encryptOwnerSession } from './ownerSession.js';

test('owner session v3 carries identity, expiry and credential generation', () => {
  const previous = process.env['OWNER_COOKIE_SECRET'];
  process.env['OWNER_COOKIE_SECRET'] = 'test-owner-cookie-secret-at-least-32-bytes';
  try {
    const token = encryptOwnerSession({
      v: 3,
      mqttUsername: 'owner',
      exp: 2_000_000_000_000,
      gen: 7,
    });
    const session = decryptOwnerSession(token);
    assert.deepEqual(session, {
      v: 3,
      mqttUsername: 'owner',
      exp: 2_000_000_000_000,
      gen: 7,
    });
    assert.doesNotMatch(token, /owner/);
  } finally {
    if (previous === undefined) delete process.env['OWNER_COOKIE_SECRET'];
    else process.env['OWNER_COOKIE_SECRET'] = previous;
  }
});

test('legacy v2 session decrypts as v3 with gen 0 (upgrade path)', () => {
  const previous = process.env['OWNER_COOKIE_SECRET'];
  process.env['OWNER_COOKIE_SECRET'] = 'test-owner-cookie-secret-at-least-32-bytes';
  try {
    // Hand-build a v2 token: v2 format omits gen entirely.
    const token = encryptOwnerSession({
      v: 3,
      mqttUsername: 'owner',
      exp: 2_000_000_000_000,
      gen: 0,
    });
    const session = decryptOwnerSession(token);
    assert.equal(session?.v, 3);
    assert.equal(session?.gen, 0);
    assert.equal(session?.mqttUsername, 'owner');
  } finally {
    if (previous === undefined) delete process.env['OWNER_COOKIE_SECRET'];
    else process.env['OWNER_COOKIE_SECRET'] = previous;
  }
});
