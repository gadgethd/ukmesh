import assert from 'node:assert/strict';
import test from 'node:test';
import { randomBytes, randomUUID } from 'node:crypto';
import pg from 'pg';

const testOwnerDatabaseUrl = String(process.env['TEST_OWNER_DATABASE_URL'] ?? '').trim();

function adminUrlFor(ownerUrl: string): string {
  const url = new URL(ownerUrl);
  url.pathname = '/postgres';
  return url.toString();
}

if (testOwnerDatabaseUrl) {
  // ownerAuth.ts reads its pool configuration at import time. Point it at the
  // explicit isolated test database and derive the admin connection from the
  // same URL so ambient OWNER_DATABASE_URL/DATABASE_URL can never leak in.
  process.env['OWNER_DATABASE_URL'] = testOwnerDatabaseUrl;
  process.env['DATABASE_URL'] = adminUrlFor(testOwnerDatabaseUrl);
  process.env['OWNER_SESSION_MAX_PER_USER'] = '2';
}

const skip = testOwnerDatabaseUrl ? false : 'TEST_OWNER_DATABASE_URL required';

type OwnerAuthModule = typeof import('./ownerAuth.js');
let ownerAuthPromise: Promise<OwnerAuthModule> | undefined;
let schemaInitialized = false;

async function ownerAuth(): Promise<OwnerAuthModule> {
  ownerAuthPromise ??= import('./ownerAuth.js');
  const module = await ownerAuthPromise;
  if (!schemaInitialized) {
    await module.initOwnerAuthDb();
    await module.initOwnerAuthDb();
    schemaInitialized = true;
  }
  return module;
}

async function withTestClient<T>(run: (client: pg.Client) => Promise<T>): Promise<T> {
  const client = new pg.Client({ connectionString: testOwnerDatabaseUrl });
  await client.connect();
  try {
    return await run(client);
  } finally {
    await client.end().catch(() => undefined);
  }
}

test.after(async () => {
  if (!ownerAuthPromise) return;
  const module = await ownerAuthPromise;
  await module.closeOwnerAuthDb();
});

function uniqueName(prefix: string): string {
  return `${prefix}-${randomUUID()}`;
}

function nodeId(): string {
  return randomBytes(32).toString('hex').toUpperCase();
}

async function waitForAdvisoryLockWait(timeoutMs = 10_000): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    const result = await withTestClient((client) => client.query<{ waiting: boolean }>(
      `SELECT EXISTS (
         SELECT 1
           FROM pg_stat_activity
          WHERE datname = current_database()
            AND state = 'active'
            AND wait_event_type = 'Lock'
            AND query LIKE '%pg_advisory_xact_lock%'
       ) AS waiting`,
    ));
    if (result.rows[0]?.waiting === true) return;
    await new Promise((resolve) => setTimeout(resolve, 25));
  }
  throw new Error('owner session creation never blocked on the per-user advisory lock');
}

test('owner schema DDL is idempotent', { skip }, async () => {
  const { initOwnerAuthDb } = await ownerAuth();
  await initOwnerAuthDb();
});

test('owner sessions persist, revoke, reject mismatched users, and expire', { skip }, async () => {
  const {
    createOwnerSession,
    isOwnerSessionActive,
    revokeOwnerSession,
  } = await ownerAuth();
  const user = uniqueName('integration-core');

  const first = await createOwnerSession(user, 60_000);
  assert.match(first.sessionId, /^[0-9a-f-]{36}$/i);
  assert.equal(await isOwnerSessionActive(first.sessionId, user), true);
  assert.equal(await isOwnerSessionActive(first.sessionId, `${user}-other`), false);

  await revokeOwnerSession(first.sessionId);
  assert.equal(await isOwnerSessionActive(first.sessionId, user), false);

  const expiring = await createOwnerSession(user, 1);
  await new Promise((resolve) => setTimeout(resolve, 20));
  assert.equal(await isOwnerSessionActive(expiring.sessionId, user), false);
});

test('owner session cap revokes the oldest session and keeps the newest', { skip }, async () => {
  const { createOwnerSession, isOwnerSessionActive } = await ownerAuth();
  const user = uniqueName('integration-cap');

  const first = await createOwnerSession(user, 60_000);
  const second = await createOwnerSession(user, 60_000);
  const third = await createOwnerSession(user, 60_000);

  assert.equal(await isOwnerSessionActive(first.sessionId, user), false);
  assert.equal(await isOwnerSessionActive(second.sessionId, user), true);
  assert.equal(await isOwnerSessionActive(third.sessionId, user), true);
});

test('a login delayed on the per-user lock cannot be pruned by its own creation', { skip }, async () => {
  const { createOwnerSession, isOwnerSessionActive } = await ownerAuth();
  const user = uniqueName('integration-lock');
  const oldest = await createOwnerSession(user, 60_000);

  const holder = new pg.Client({ connectionString: testOwnerDatabaseUrl });
  await holder.connect();
  try {
    await holder.query('SELECT pg_advisory_lock(hashtext($1))', [`owner-session:${user}`]);
    const pending = createOwnerSession(user, 60_000);
    await waitForAdvisoryLockWait();

    // These two logins win the cap while the delayed transaction is blocked.
    // The delayed transaction's NOW() (transaction start) predates both.
    const whileWaitingA = randomUUID();
    const whileWaitingB = randomUUID();
    const expiresAt = Date.now() + 60_000;
    await holder.query(
      `INSERT INTO owner_sessions (session_id, mqtt_username, expires_at)
       VALUES ($1, $2, to_timestamp($3 / 1000.0))`,
      [whileWaitingA, user, expiresAt],
    );
    await holder.query('SELECT pg_sleep(0.01)');
    await holder.query(
      `INSERT INTO owner_sessions (session_id, mqtt_username, expires_at)
       VALUES ($1, $2, to_timestamp($3 / 1000.0))`,
      [whileWaitingB, user, expiresAt],
    );
    await holder.query('SELECT pg_advisory_unlock(hashtext($1))', [`owner-session:${user}`]);

    const delayed = await pending;

    // The delayed session was inserted after whileWaitingB, so a later login
    // must rank it above whileWaitingB rather than trusting its transaction start.
    const newest = await createOwnerSession(user, 60_000);
    assert.equal(await isOwnerSessionActive(newest.sessionId, user), true);
    assert.equal(await isOwnerSessionActive(delayed.sessionId, user), true);
    assert.equal(await isOwnerSessionActive(whileWaitingB, user), false);

    assert.equal(await isOwnerSessionActive(whileWaitingA, user), false);
    assert.equal(await isOwnerSessionActive(oldest.sessionId, user), false);
  } finally {
    await holder.end().catch(() => undefined);
  }
});

test('revoking the last effective grant revokes sessions despite a legacy unverified row', { skip }, async () => {
  const {
    createOwnerSession,
    getOwnerNodeIdsForUsername,
    isOwnerSessionActive,
    revokeOwnerGrant,
    upsertVerifiedOwnerGrant,
  } = await ownerAuth();
  const user = uniqueName('integration-unverified');
  const grantedNode = nodeId();

  await upsertVerifiedOwnerGrant(user, grantedNode, {
    verificationMethod: 'operator-database',
    actor: 'integration-test',
  });
  const session = await createOwnerSession(user, 60_000);
  await withTestClient((client) => client.query(
    'INSERT INTO owner_account_nodes (mqtt_username, node_id) VALUES ($1, $2)',
    [user, nodeId()],
  ));

  await revokeOwnerGrant(user, grantedNode, {
    actor: 'integration-test',
    reason: 'regression coverage',
  });

  assert.deepEqual(await getOwnerNodeIdsForUsername(user), []);
  assert.equal(await isOwnerSessionActive(session.sessionId, user), false);
});

test('a failed session revocation rolls back the grant revocation atomically', { skip }, async () => {
  const {
    createOwnerSession,
    getOwnerNodeIdsForUsername,
    isOwnerSessionActive,
    revokeOwnerGrant,
    upsertVerifiedOwnerGrant,
  } = await ownerAuth();
  const user = uniqueName('integration-atomic');
  const grantedNode = nodeId();

  await upsertVerifiedOwnerGrant(user, grantedNode, {
    verificationMethod: 'operator-database',
    actor: 'integration-test',
  });
  const session = await createOwnerSession(user, 60_000);

  try {
    await withTestClient(async (client) => {
      await client.query('DROP TRIGGER IF EXISTS owner_sessions_test_fail ON owner_sessions');
      await client.query(
        `CREATE OR REPLACE FUNCTION owner_sessions_test_fail() RETURNS trigger
         LANGUAGE plpgsql AS $$
         BEGIN
           IF OLD.mqtt_username = TG_ARGV[0] THEN
             RAISE EXCEPTION 'injected owner session revocation failure';
           END IF;
           RETURN NEW;
         END
         $$`,
      );
      await client.query(
        `CREATE TRIGGER owner_sessions_test_fail
           BEFORE UPDATE ON owner_sessions
           FOR EACH ROW EXECUTE FUNCTION owner_sessions_test_fail(${client.escapeLiteral(user)})`,
      );
    });

    await assert.rejects(
      revokeOwnerGrant(user, grantedNode, {
        actor: 'integration-test',
        reason: 'atomicity coverage',
      }),
      /injected owner session revocation failure/,
    );

    assert.deepEqual(await getOwnerNodeIdsForUsername(user), [grantedNode]);
    assert.equal(await isOwnerSessionActive(session.sessionId, user), true);
  } finally {
    await withTestClient(async (client) => {
      await client.query('DROP TRIGGER IF EXISTS owner_sessions_test_fail ON owner_sessions');
      await client.query('DROP FUNCTION IF EXISTS owner_sessions_test_fail()');
    }).catch(() => undefined);
  }
});
