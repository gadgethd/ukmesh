/** Guard the new full-stack fixtures without widening legacy lookup ownership. */
import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import pg from 'pg';
import { assertLocalDatabaseUrl } from './sampleData.js';

type SelectedFixture = { token?: string; targets?: { ukmesh?: string } | string };
type FixtureManifest = { token?: string; ready?: boolean; databases?: {
  label?: string; database?: string; cluster?: string; container?: string; restored?: boolean;
}[] };
type DatabaseIdentity = { database: string; cluster: string; ownership: string | null };

export function validateLeanFixtureSelection(endpoint: string, selected: SelectedFixture, fixture: FixtureManifest) {
  assertLocalDatabaseUrl(endpoint);
  const url = new URL(endpoint);
  assert.equal(url.port, '5433', 'full-stack fixtures require their existing local database port');
  assert.ok(!url.search, 'fixture connections must not override database runtime settings');
  const token = selected.token;
  assert.ok(token && /^[a-z0-9_]{1,38}$/.test(token), 'selected fixture token is invalid');
  assert.equal(fixture.token, token, 'fixture manifest does not match selection');
  assert.equal(fixture.ready, true, 'fixture restore is incomplete');
  const database = decodeURIComponent(url.pathname.slice(1));
  assert.match(database, /^lean_(?:fixture|1x|10x|soak)_[a-z0-9_]{1,38}$/);
  assert.ok(database.endsWith('_' + token), 'fixture database is not owned by selected token');
  assert.equal(typeof selected.targets === 'object' && selected.targets.ukmesh, database,
    'database is not the active selected fixture');
  const store = fixture.databases?.find(record => record.label === 'ukmesh');
  assert.equal(store?.database, database, 'fixture database differs from ownership manifest');
  assert.equal(store?.container, 'ukmesh-sample-db', 'fixture cluster is outside local ownership');
  assert.equal(store?.restored, true, 'fixture database restore is incomplete');
  assert.ok(store?.cluster && /^\d+$/.test(store.cluster), 'fixture cluster identity is invalid');
  return { database, cluster: store.cluster, ownership: 'ukmesh-owned-fixture:' + token };
}

export function validateLeanFixtureIdentity(expected: DatabaseIdentity, actual: DatabaseIdentity) {
  assert.deepEqual(actual, expected, 'connected database does not match the owned fixture identity');
}

export async function assertOwnedLeanFixture(endpoint: string): Promise<void> {
  const root = fileURLToPath(new URL('../../../', import.meta.url));
  const state = path.join(root, 'plans/lean-runtime/runtime');
  const selected = JSON.parse(await readFile(path.join(state, 'selected-fixture.json'), 'utf8')) as SelectedFixture;
  assert.ok(selected.token && /^[a-z0-9_]{1,38}$/.test(selected.token), 'owned fixture selection required');
  const fixture = JSON.parse(await readFile(path.join(state, 'fixtures', selected.token, 'manifest.json'), 'utf8')) as FixtureManifest;
  const expected = validateLeanFixtureSelection(endpoint, selected, fixture);
  const client = new pg.Client({ connectionString: endpoint, connectionTimeoutMillis: 5_000,
    statement_timeout: 5_000, application_name: 'ukmesh-fixture-ownership-check' });
  try {
    await client.connect();
    const actual = (await client.query<DatabaseIdentity>(`SELECT current_database() AS database,
      (SELECT system_identifier::text FROM pg_control_system()) AS cluster,
      shobj_description(oid,'pg_database') AS ownership FROM pg_database WHERE datname=current_database()`)).rows[0];
    assert.ok(actual, 'connected fixture database identity is unavailable');
    validateLeanFixtureIdentity(expected, actual);
  } finally {
    await client.end();
  }
}
