import assert from 'node:assert/strict';
import { randomUUID } from 'node:crypto';
import test from 'node:test';
import pg from 'pg';
import { createDisposableDatabase } from '../db/fixtures/disposableDatabase.js';
import { createDatabaseRuntime } from '../db/databaseRuntime.js';
import {
  collectLookupDatabaseIdentity, configureLookupReadOnlyEnvironment, readLookupSessionIdentity,
} from './lookupBenchmarkSession.js';

const endpoint = process.env['TEST_INGEST_DATABASE_URL'];

test('actual distinct logins preserve role permissions, read-only options and separate owner database', {
  skip: endpoint ? false : 'owned PostgreSQL fixture required', timeout: 30_000,
}, async t => {
  const main = await createDisposableDatabase(t, endpoint!, 'lookup_role_main');
  const owner = await createDisposableDatabase(t, endpoint!, 'lookup_role_owner');
  const admin = new pg.Client({ connectionString: main.url });
  const created: string[] = [];
  const names = Object.fromEntries(['http', 'worker', 'ingest', 'owner'].map(role =>
    [role, 'lookup_' + role + '_' + randomUUID().replaceAll('-', '').slice(0, 12)]));
  const password = randomUUID();
  let runtime: ReturnType<typeof createDatabaseRuntime> | undefined;
  let inspector: ReturnType<typeof createDatabaseRuntime> | undefined;
  await admin.connect();
  try {
    for (const name of Object.values(names)) {
      await admin.query(`CREATE ROLE "${name}" LOGIN NOSUPERUSER NOBYPASSRLS NOINHERIT PASSWORD '${password}'`);
      created.push(name);
    }
    await admin.query(`REVOKE CONNECT ON DATABASE "${main.name}", "${owner.name}" FROM PUBLIC`);
    for (const role of ['http', 'worker', 'ingest']) {
      await admin.query(`GRANT CONNECT ON DATABASE "${main.name}" TO "${names[role]}"`);
    }
    await admin.query(`GRANT CONNECT ON DATABASE "${owner.name}" TO "${names['owner']}"`);
    await admin.query('CREATE TABLE public.role_visible (value integer)');
    await admin.query('INSERT INTO public.role_visible VALUES (17)');
    await admin.query('CREATE SCHEMA role_private');
    await admin.query('CREATE TABLE role_private.secret (value integer)');
    await admin.query(`GRANT USAGE ON SCHEMA public TO "${names['http']}", "${names['worker']}"`);
    await admin.query(`GRANT SELECT ON public.role_visible TO "${names['http']}", "${names['worker']}"`);
    await admin.query(`GRANT INSERT ON public.role_visible TO "${names['worker']}"`);
    const urlFor = (role: string) => {
      const url = new URL(role === 'owner' ? owner.url : main.url);
      url.username = names[role]!; url.password = password;
      url.searchParams.set('options', '-c application_name=lookup_role_fixture -c default_transaction_read_only=off');
      return url.href;
    };
    const env: NodeJS.ProcessEnv = {
      DATABASE_URL: urlFor('http'), MESHCORE_HTTP_DATABASE_URL: urlFor('http'),
      MESHCORE_WORKER_DATABASE_URL: urlFor('worker'), MESHCORE_INGEST_DATABASE_URL: urlFor('ingest'),
      OWNER_DATABASE_URL: urlFor('owner'), DATABASE_ROLE_ENFORCEMENT: 'true',
      DATABASE_POOL_MAX: '8', OWNER_DATABASE_POOL_MAX: '2',
    };
    configureLookupReadOnlyEnvironment(env['DATABASE_URL']!, env);
    runtime = createDatabaseRuntime(env);
    const setup = {
      http: await readLookupSessionIdentity(runtime.query, 'http', env),
      worker: await readLookupSessionIdentity(runtime.analyticsQuery, 'worker', env),
    };
    assert.equal(setup.http.current_user, names['http']);
    assert.equal(setup.worker.current_user, names['worker']);
    assert.equal((await runtime.query('SELECT value FROM public.role_visible')).rows[0]!['value'], 17);
    await assert.rejects(runtime.query('SELECT * FROM role_private.secret'), { code: '42501' });
    await assert.rejects(runtime.analyticsQuery('INSERT INTO public.role_visible VALUES (18)'), { code: '25006' });
    const proof = await collectLookupDatabaseIdentity(runtime, setup, env);
    assert.equal(proof.scope, 'application-role-readonly');
    assert.equal(proof.expected['owner']!.database, owner.name);
    assert.equal(proof.after.worker!.current_user, names['worker']);
    assert.equal(proof.after.http!.default_transaction_read_only, 'on');
    assert.equal(runtime.ingestPool.totalCount, 0, 'unused ingest pool is not opened for proof');
    assert.equal(JSON.stringify(proof).includes(password), false);
    const inspectorEnv: NodeJS.ProcessEnv = { DATABASE_URL: main.url };
    configureLookupReadOnlyEnvironment(main.url, inspectorEnv);
    inspector = createDatabaseRuntime(inspectorEnv);
    const legacy = await readLookupSessionIdentity(inspector.query, 'http', inspectorEnv);
    assert.equal(legacy.current_user, decodeURIComponent(new URL(main.url).username));
    const enforcedInspector = { ...inspectorEnv, DATABASE_ROLE_ENFORCEMENT: 'true' };
    await assert.rejects(readLookupSessionIdentity(inspector.query, 'http', enforcedInspector), /identity differs/);
    const wrongRole = { ...env, MESHCORE_HTTP_DATABASE_URL: env['MESHCORE_WORKER_DATABASE_URL'] };
    await assert.rejects(readLookupSessionIdentity(runtime.query, 'http', wrongRole), /identity differs/);
  } finally {
    const errors: unknown[] = [];
    for (const close of [() => inspector?.close(), () => runtime?.close(), () => admin.end(), () => owner.dispose(), () => main.dispose()]) {
      try { await close(); } catch (error) { errors.push(error); }
    }
    // Dispose the owned databases before removing their unique roles. Reconnect
    // through the administrative test endpoint; no retained store is involved.
    const cleanup = new pg.Client({ connectionString: endpoint! });
    try {
      await cleanup.connect();
      for (const name of created) {
        try { await cleanup.query(`DROP ROLE "${name}"`); } catch (error) { errors.push(error); }
      }
    } finally { await cleanup.end(); }
    if (errors.length) throw new AggregateError(errors, 'owned role fixture cleanup failed');
  }
});
