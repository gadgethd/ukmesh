import assert from 'node:assert/strict';
import test from 'node:test';
import {
  closeOwnerAuthDb,
  createOwnerSession,
  initOwnerAuthDb,
  isOwnerSessionActive,
  revokeOwnerSession,
  revokeOwnerSessionsForUsername,
} from './ownerAuth.js';

const configured = Boolean(process.env['TEST_OWNER_DATABASE_URL'] || process.env['OWNER_DATABASE_URL']);

test('owner sessions persist, revoke, reject mismatched users, expire, cap, and replay DDL idempotently', {
  skip: configured ? false : 'TEST_OWNER_DATABASE_URL or OWNER_DATABASE_URL required',
}, async () => {
  await initOwnerAuthDb();
  await initOwnerAuthDb();

  const first = await createOwnerSession('integration-owner', 60_000);
  assert.match(first.sessionId, /^[0-9a-f-]{36}$/i);
  assert.equal(await isOwnerSessionActive(first.sessionId, 'integration-owner'), true);
  assert.equal(await isOwnerSessionActive(first.sessionId, 'different-owner'), false);

  await revokeOwnerSession(first.sessionId);
  assert.equal(await isOwnerSessionActive(first.sessionId, 'integration-owner'), false);

  const expiring = await createOwnerSession('integration-expiry', 1);
  await new Promise((resolve) => setTimeout(resolve, 20));
  assert.equal(await isOwnerSessionActive(expiring.sessionId, 'integration-expiry'), false);

  const maxPerUser = Math.min(
    100,
    Math.max(2, Number(process.env['OWNER_SESSION_MAX_PER_USER'] ?? 20) || 20),
  );
  const capped = await Promise.all(
    Array.from({ length: maxPerUser + 1 }, () => createOwnerSession('integration-cap', 60_000)),
  );
  assert.equal(await isOwnerSessionActive(capped[0]!.sessionId, 'integration-cap'), false);
  assert.equal(await isOwnerSessionActive(capped.at(-1)!.sessionId, 'integration-cap'), true);

  await revokeOwnerSessionsForUsername('integration-cap');
  assert.equal(await isOwnerSessionActive(capped.at(-1)!.sessionId, 'integration-cap'), false);

  await closeOwnerAuthDb();
});
