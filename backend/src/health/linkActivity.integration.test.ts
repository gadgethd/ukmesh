import assert from 'node:assert/strict';
import test from 'node:test';
import pg from 'pg';
import { createDisposableDatabase } from '../db/fixtures/disposableDatabase.js';
import { createHealthRepository } from './repository.js';

const endpoint = process.env['TEST_INGEST_DATABASE_URL'];

test('health link activity retains exact counts, boundaries and timestamps', {
  skip: endpoint ? false : 'owned loopback TEST_INGEST_DATABASE_URL required', timeout: 30_000,
}, async t => {
  const fixture = await createDisposableDatabase(t, endpoint!, 'health_link_activity');
  const client = new pg.Client({ connectionString: fixture.url });
  fixture.addCleanup(() => client.end());
  await client.connect();
  await client.query(`BEGIN;
    CREATE SCHEMA core;
    CREATE TABLE core.node_links (itm_computed_at timestamptz, last_observed timestamptz NOT NULL)`);
  try {
    const repository = createHealthRepository((sql, params) => client.query(sql, params));
    const load = async () => (await repository.loadLinkActivity()).rows[0];
    assert.deepEqual(await load(), {
      recent_count: '0', last_computed: null, links: '0', last_observed: null,
    });
    await client.query(`INSERT INTO core.node_links VALUES
      (NOW()-INTERVAL '59 minutes',NOW()-INTERVAL '10 minutes'),
      (NOW()-INTERVAL '1 hour',NOW()-INTERVAL '5 minutes'),
      (NULL,NOW()+INTERVAL '2 days'),
      (NOW()+INTERVAL '1 day',NOW()-INTERVAL '2 days')`);
    const times = (await client.query(`SELECT
      (NOW()+INTERVAL '1 day')::text AS computed,
      (NOW()+INTERVAL '2 days')::text AS observed`)).rows[0];
    assert.deepEqual(await load(), {
      recent_count: '2', last_computed: times.computed, links: '4', last_observed: times.observed,
    });
    await client.query('UPDATE core.node_links SET itm_computed_at=NULL');
    assert.deepEqual(await load(), {
      recent_count: '0', last_computed: null, links: '4', last_observed: times.observed,
    });
  } finally { await client.query('ROLLBACK'); }
});
