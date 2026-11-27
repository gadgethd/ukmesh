import assert from 'node:assert/strict';
import test from 'node:test';
import { randomBytes, randomUUID } from 'node:crypto';
import pg from 'pg';
import type { Request, Response } from 'express';
import { bootstrapBaseSchemaFixture, POST_MOVE_SEARCH_PATH } from './fixtures/baseSchema.js';
import { createOwnerSessionGuard } from '../api/ownerSessionGuard.js';
import { decryptOwnerSession, encryptOwnerSession, getOwnerSession } from '../owner/ownerSession.js';

const testOwnerDatabaseUrl = String(process.env['TEST_OWNER_DATABASE_URL'] ?? '').trim();
const testRedisUrl = String(process.env['TEST_REDIS_URL'] ?? '').trim();

function isLoopbackRedisUrl(raw: string): boolean {
  try {
    const parsed = new URL(raw);
    return ['127.0.0.1', 'localhost', '[::1]', '::1'].includes(parsed.hostname);
  } catch {
    return false;
  }
}

const isolatedRedisUrl = isLoopbackRedisUrl(testRedisUrl) ? testRedisUrl : '';

function adminUrlFor(ownerUrl: string): string {
  const url = new URL(ownerUrl);
  url.pathname = '/postgres';
  return url.toString();
}

// The consolidated database keeps owner-auth structure in core (073/075); the
// owner queries are unqualified and rely on the role's search_path, which a
// plain test connection does not carry.
function withOwnerSearchPath(url: string): string {
  const parsed = new URL(url);
  parsed.searchParams.set('options', '-c search_path=mesh,core,analytics,ops,public');
  return parsed.toString();
}

if (testOwnerDatabaseUrl) {
  // ownerAuth.ts reads its pool configuration at import time. Point it at the
  // explicit isolated test database and derive the admin connection from the
  // same URL so ambient OWNER_DATABASE_URL/DATABASE_URL can never leak in.
  process.env['OWNER_DATABASE_URL'] = withOwnerSearchPath(testOwnerDatabaseUrl);
  process.env['DATABASE_URL'] = adminUrlFor(testOwnerDatabaseUrl);
  process.env['OWNER_SESSION_MAX_PER_USER'] = '2';
  // Credential-generation tests must only touch an isolated loopback Redis;
  // never inherit a deployment REDIS_URL that could revoke real sessions.
  if (isolatedRedisUrl) process.env['REDIS_URL'] = isolatedRedisUrl;
  process.env['OWNER_COOKIE_SECRET'] ??= 'owner-auth-integration-test-cookie-secret';
}

// P1-9 removed runtime schema DDL, so this suite owns its fixture the same way
// the other integration suites do: create the database if the service did not,
// seed the shipped base schema, and replay the migration band (073/075 create
// core.owner_*).
async function provisionOwnerFixture(): Promise<void> {
  const admin = new pg.Pool({ connectionString: adminUrlFor(testOwnerDatabaseUrl), max: 1 });
  const name = new URL(testOwnerDatabaseUrl).pathname.replace(/^\//, '');
  try {
    const found = await admin.query('SELECT 1 FROM pg_database WHERE datname = $1', [name]);
    if (!found.rowCount) {
      await admin.query(`CREATE DATABASE "${name}"`);
    }
  } finally {
    await admin.end();
  }
  const pool = new pg.Pool({ connectionString: testOwnerDatabaseUrl, options: POST_MOVE_SEARCH_PATH });
  try {
    await bootstrapBaseSchemaFixture(pool);
  } finally {
    await pool.end();
  }
}

test.before(async () => {
  if (!testOwnerDatabaseUrl) return;
  await provisionOwnerFixture();
});

const skip = testOwnerDatabaseUrl ? false : 'TEST_OWNER_DATABASE_URL required';
const rotationSkip = skip || (isolatedRedisUrl ? false : 'TEST_REDIS_URL must point to a loopback fixture');

type OwnerAuthModule = typeof import('./ownerAuth.js');
let ownerAuthPromise: Promise<OwnerAuthModule> | undefined;
let schemaInitialized = false;
let ownerAccessPromise: Promise<typeof import('../owner/ownerAccess.js')> | undefined;

async function ownerAccess(): Promise<typeof import('../owner/ownerAccess.js')> {
  ownerAccessPromise ??= import('../owner/ownerAccess.js');
  return ownerAccessPromise;
}


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
  const client = new pg.Client({ connectionString: withOwnerSearchPath(testOwnerDatabaseUrl) });
  await client.connect();
  try {
    return await run(client);
  } finally {
    await client.end().catch(() => undefined);
  }
}

test.after(async () => {
  if (ownerAccessPromise) {
    const access = await ownerAccessPromise;
    await access.closeOwnerCredentialGenerationClient().catch(() => undefined);
  }
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

test('credential rotation revokes the server session and invalidates the old cookie', { skip: rotationSkip }, async () => {
  const {
    createOwnerSession,
    isOwnerSessionActive,
  } = await ownerAuth();
  const {
    bumpOwnerCredentialGeneration,
    getOwnerCredentialGeneration,
  } = await ownerAccess();
  const user = uniqueName('integration-rotation');
  const cookieName = 'meshcore_owner_session';
  const grantedNode = nodeId();

  const generationBefore = await getOwnerCredentialGeneration(user);
  const oldSession = await createOwnerSession(user, 60_000);
  const oldCookie = encryptOwnerSession({
    v: 4,
    mqttUsername: user,
    sid: oldSession.sessionId,
    exp: oldSession.expiresAt,
    gen: generationBefore,
  });

  await bumpOwnerCredentialGeneration(user);

  assert.equal(await isOwnerSessionActive(oldSession.sessionId, user), false);
  assert.equal(await getOwnerCredentialGeneration(user), generationBefore + 1);

  function recorder() {
    const state: { statusCode: number; body: unknown; cleared: string[] } = {
      statusCode: 200,
      body: undefined,
      cleared: [],
    };
    const response = {
      clearCookie(name: string) { state.cleared.push(name); return response; },
      status(code: number) { state.statusCode = code; return response; },
      json(body: unknown) { state.body = body; return response; },
    } as unknown as Response;
    return { response, state };
  }

  const guard = createOwnerSessionGuard({
    ownerCookieName: cookieName,
    getOwnerSession: (req) => getOwnerSession(req, cookieName),
    getOwnerCredentialGeneration,
    isOwnerSessionActive,
    touchOwnerSession: async () => undefined,
    resolveOwnerNodeIds: async () => [grantedNode],
  });

  const oldRequest = { headers: { cookie: `${cookieName}=${oldCookie}` } } as unknown as Request;
  const oldResponse = recorder();
  assert.equal(await guard(oldRequest, oldResponse.response), null);
  assert.equal(oldResponse.state.statusCode, 401);
  assert.deepEqual(oldResponse.state.body, { error: 'Credentials have been rotated — please log in again' });

  // A fresh login reads the bumped generation before minting its server row,
  // which is what makes the new cookie valid while the old one stays dead.
  const freshGeneration = await getOwnerCredentialGeneration(user);
  const freshSession = await createOwnerSession(user, 60_000);
  const freshCookie = encryptOwnerSession({
    v: 4,
    mqttUsername: user,
    sid: freshSession.sessionId,
    exp: freshSession.expiresAt,
    gen: freshGeneration,
  });
  assert.equal(decryptOwnerSession(freshCookie)?.gen, generationBefore + 1);
  assert.equal(await isOwnerSessionActive(freshSession.sessionId, user), true);

  const freshRequest = { headers: { cookie: `${cookieName}=${freshCookie}` } } as unknown as Request;
  const freshResponse = recorder();
  const nodeIds = await guard(freshRequest, freshResponse.response);
  assert.deepEqual(nodeIds, [grantedNode]);
  assert.equal(freshResponse.state.statusCode, 200);
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

  const holder = new pg.Client({ connectionString: withOwnerSearchPath(testOwnerDatabaseUrl) });
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
