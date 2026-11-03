import assert from 'node:assert/strict';
import test from 'node:test';
import pg from 'pg';
import { runMigrations } from './migrations.js';

const databaseUrl = process.env['TEST_EXTERNAL_CONSUMERS_DATABASE_URL']
  ?? process.env['TEST_INGEST_DATABASE_URL'];
const local = databaseUrl && ['127.0.0.1', 'localhost', '[::1]'].includes(new URL(databaseUrl).hostname);

const compatibilityViews = [
  ['packet_visibility_materialization_state', 'ops.packet_visibility_materialization_state'],
  ['private_node_prefixes', 'core.private_node_prefixes'],
  ['nodes', 'core.nodes'],
  ['node_links', 'core.node_links'],
  ['node_status_samples', 'core.node_status_samples'],
  ['observers', 'core.observers'],
] as const;

test('external consumer compatibility views survive migrations and are readable', {
  skip: local ? false : 'explicit loopback TEST_EXTERNAL_CONSUMERS_DATABASE_URL or TEST_INGEST_DATABASE_URL required',
  timeout: 300_000,
}, async (t) => {
  const admin = new pg.Pool({ connectionString: databaseUrl, max: 1 });
  const name = `external_consumers_${process.pid}_${Date.now()}`;
  const url = new URL(databaseUrl!);
  url.pathname = `/${name}`;
  await admin.query(`CREATE DATABASE "${name}"`);
  const pool = new pg.Pool({ connectionString: url.toString(), max: 2 });
  t.after(async () => {
    await pool.end();
    await admin.query(`DROP DATABASE IF EXISTS "${name}" WITH (FORCE)`);
    await admin.end();
  });

  await runMigrations(pool);

  for (const [viewName, target] of compatibilityViews) {
    const relation = await pool.query<{ table_type: string }>(`
      SELECT table_type FROM information_schema.tables
       WHERE table_schema = 'public' AND table_name = $1
    `, [viewName]);
    assert.equal(relation.rows[0]?.table_type, 'VIEW', `public.${viewName} must remain a view`);
    await pool.query(`SELECT * FROM public."${viewName}" LIMIT 0`);
    const definition = await pool.query<{ definition: string }>(`
      SELECT pg_get_viewdef('public.${viewName}'::regclass, true) AS definition
    `);
    assert.match(definition.rows[0]!.definition, new RegExp(target.split('.')[1]!),
      `public.${viewName} must still select from ${target}`);
  }
});
