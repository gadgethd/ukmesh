import assert from 'node:assert/strict';
import test from 'node:test';
import { getNodeIdsForUserInAcl } from '../mqtt/aclManager.js';

// ownerAccess imports the owner-auth pool module, which builds its connection
// string at import time. This file never issues a query (the verified-grant
// reader is injected below), but a valid URL must exist so the module can load
// without a live Postgres.
process.env['DATABASE_URL'] ??= 'postgresql://127.0.0.1:1/meshcore_test';
const { __setOwnerGrantSourceForTests, resolveOwnerNodeIds } = await import('./ownerAccess.js');

const NODE_A = 'A1'.repeat(32);
const NODE_B = 'B2'.repeat(32);
const SHADOW_OWNER = 'shadow-owner';

// A realistic broker ACL write stanza: exactly what the legacy renderer leaves
// behind after granting publish scope for a node. It proves a credential may
// write, which must never be mistaken for proof that it owns the node.
const ACL_WITH_WRITE_STANZA = [
  `user ${SHADOW_OWNER}`,
  `topic write meshcore/+/${NODE_A}/packets`,
  `topic write meshcore/+/${NODE_A}/status`,
  `topic write meshcore/+/${NODE_A}/neighbors`,
  `topic write meshcore/+/${NODE_A}/neighbours`,
  '',
].join('\n');

async function withOwnerEnv<T>(
  values: Record<string, string | undefined>,
  fn: () => Promise<T>,
): Promise<T> {
  const previous = new Map(Object.keys(values).map((key) => [key, process.env[key]]));
  for (const [key, value] of Object.entries(values)) {
    if (value === undefined) delete process.env[key];
    else process.env[key] = value;
  }
  try {
    return await fn();
  } finally {
    for (const [key, value] of previous) {
      if (value === undefined) delete process.env[key];
      else process.env[key] = value;
    }
  }
}

test('an empty verified grant list denies authority in shadow mode despite ACL and config write scope', async () => {
  // Both legacy fallback sources are present and non-empty.
  assert.deepEqual(getNodeIdsForUserInAcl(ACL_WITH_WRITE_STANZA, SHADOW_OWNER), [NODE_A]);

  __setOwnerGrantSourceForTests(async () => []);
  await withOwnerEnv({
    OWNER_AUTHORIZATION_MODE: 'shadow',
    OWNER_MQTT_USERNAME_MAP: `${SHADOW_OWNER}=${NODE_A}`,
  }, async () => {
    assert.deepEqual(await resolveOwnerNodeIds(SHADOW_OWNER), []);
  });
});

test('an empty verified grant list denies authority in enforce mode despite ACL and config write scope', async () => {
  __setOwnerGrantSourceForTests(async () => []);
  await withOwnerEnv({
    OWNER_AUTHORIZATION_MODE: 'enforce',
    OWNER_MQTT_USERNAME_MAP: `${SHADOW_OWNER}=${NODE_A}`,
  }, async () => {
    assert.deepEqual(await resolveOwnerNodeIds(SHADOW_OWNER), []);
  });
});

test('a verified grant still authorises its node in shadow mode', async () => {
  __setOwnerGrantSourceForTests(async () => [NODE_A.toLowerCase()]);
  await withOwnerEnv({
    OWNER_AUTHORIZATION_MODE: 'shadow',
    OWNER_MQTT_USERNAME_MAP: undefined,
  }, async () => {
    assert.deepEqual(await resolveOwnerNodeIds(SHADOW_OWNER), [NODE_A]);
  });
});

test('only verified grants authorise; write scope for another node is not merged in', async () => {
  __setOwnerGrantSourceForTests(async () => [NODE_B]);
  await withOwnerEnv({
    OWNER_AUTHORIZATION_MODE: 'shadow',
    OWNER_MQTT_USERNAME_MAP: `${SHADOW_OWNER}=${NODE_A}`,
  }, async () => {
    assert.deepEqual(await resolveOwnerNodeIds(SHADOW_OWNER), [NODE_B]);
  });
});

test('an unknown authorization mode is rejected instead of silently authorising', async () => {
  __setOwnerGrantSourceForTests(async () => []);
  await withOwnerEnv({ OWNER_AUTHORIZATION_MODE: 'off' }, async () => {
    await assert.rejects(
      resolveOwnerNodeIds('bad-mode-owner'),
      /INVALID_OWNER_AUTHORIZATION_MODE:off/,
    );
  });
});
