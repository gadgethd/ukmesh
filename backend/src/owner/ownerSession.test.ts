import { createCipheriv, createHash, randomBytes } from 'node:crypto';
import assert from 'node:assert/strict';
import test from 'node:test';
import { decryptOwnerSession, encryptOwnerSession } from './ownerSession.js';

const SESSION_ID = '11111111-1111-4111-8111-111111111111';

function legacyToken(payload: Record<string, unknown>): string {
  const iv = randomBytes(12);
  const cipher = createCipheriv(
    'aes-256-gcm',
    createHash('sha256').update('test-owner-cookie-secret-at-least-32-bytes').digest(),
    iv,
  );
  const encrypted = Buffer.concat([
    cipher.update(Buffer.from(JSON.stringify(payload), 'utf8')),
    cipher.final(),
  ]);
  return `${iv.toString('base64url')}.${cipher.getAuthTag().toString('base64url')}.${encrypted.toString('base64url')}`;
}

test('owner session v4 carries identity, session id, expiry and credential generation', () => {
  const previous = process.env['OWNER_COOKIE_SECRET'];
  process.env['OWNER_COOKIE_SECRET'] = 'test-owner-cookie-secret-at-least-32-bytes';
  try {
    const token = encryptOwnerSession({
      v: 4,
      mqttUsername: 'owner',
      sid: SESSION_ID,
      exp: 2_000_000_000_000,
      gen: 7,
    });
    const session = decryptOwnerSession(token);
    assert.deepEqual(session, {
      v: 4,
      mqttUsername: 'owner',
      sid: SESSION_ID,
      exp: 2_000_000_000_000,
      gen: 7,
    });
    assert.doesNotMatch(token, /owner/);
  } finally {
    if (previous === undefined) delete process.env['OWNER_COOKIE_SECRET'];
    else process.env['OWNER_COOKIE_SECRET'] = previous;
  }
});

test('legacy v1/v2/v3 sessions are rejected instead of being upgraded', () => {
  const previous = process.env['OWNER_COOKIE_SECRET'];
  process.env['OWNER_COOKIE_SECRET'] = 'test-owner-cookie-secret-at-least-32-bytes';
  try {
    for (const payload of [
      { v: 1, mqttUsername: 'owner', nodeIds: ['A1'.repeat(32)], exp: 2_000_000_000_000 },
      { v: 2, mqttUsername: 'owner', exp: 2_000_000_000_000 },
      { v: 3, mqttUsername: 'owner', exp: 2_000_000_000_000, gen: 0 },
    ]) {
      assert.equal(decryptOwnerSession(legacyToken(payload)), null);
    }
  } finally {
    if (previous === undefined) delete process.env['OWNER_COOKIE_SECRET'];
    else process.env['OWNER_COOKIE_SECRET'] = previous;
  }
});

test('owner session rejects malformed session ids', () => {
  const previous = process.env['OWNER_COOKIE_SECRET'];
  process.env['OWNER_COOKIE_SECRET'] = 'test-owner-cookie-secret-at-least-32-bytes';
  try {
    const token = encryptOwnerSession({
      v: 4,
      mqttUsername: 'owner',
      sid: 'not-a-uuid',
      exp: 2_000_000_000_000,
      gen: 0,
    });
    assert.equal(decryptOwnerSession(token), null);
  } finally {
    if (previous === undefined) delete process.env['OWNER_COOKIE_SECRET'];
    else process.env['OWNER_COOKIE_SECRET'] = previous;
  }
});
