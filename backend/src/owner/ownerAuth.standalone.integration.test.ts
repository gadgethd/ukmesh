import assert from 'node:assert/strict';
import { randomUUID } from 'node:crypto';
import { readFileSync } from 'node:fs';
import test from 'node:test';
import pg from 'pg';
import { createDisposableDatabase } from '../db/fixtures/disposableDatabase.js';

const endpoint = process.env['TEST_OWNER_DATABASE_URL'];

test('owner sessions use the existing public table in the standalone owner database', {
  skip: endpoint ? false : 'TEST_OWNER_DATABASE_URL required',
}, async (t) => {
  const origin = new URL(endpoint!);
  origin.pathname = '/postgres';
  const fixture = await createDisposableDatabase(t, origin.href, 'standalone_owner');
  const client = new pg.Client({ connectionString: fixture.url });
  fixture.addCleanup(() => client.end());
  await client.connect();
  await client.query(readFileSync(new URL('../db/owner-auth.sql', import.meta.url), 'utf8'));

  const username = 'standalone-owner';
  const existing = randomUUID();
  await client.query(`
    INSERT INTO public.owner_sessions (session_id, mqtt_username, expires_at)
    VALUES ($1, $2, NOW() + INTERVAL '1 hour')
  `, [existing, username]);

  const ownerUrl = new URL(fixture.url);
  ownerUrl.searchParams.set('options', '-c search_path=pg_catalog');
  process.env['OWNER_DATABASE_URL'] = ownerUrl.href;
  process.env['DATABASE_URL'] = fixture.url;
  const auth = await import('./ownerAuth.js');
  fixture.addCleanup(() => auth.closeOwnerAuthDb());

  assert.equal(await auth.isOwnerSessionActive(existing, username), true);
  const current = await auth.createOwnerSession(username, 60_000);
  assert.equal(await auth.isOwnerSessionActive(current.sessionId, username), true);
  await auth.revokeOwnerSession(existing);
  assert.equal(await auth.isOwnerSessionActive(existing, username), false);
});
