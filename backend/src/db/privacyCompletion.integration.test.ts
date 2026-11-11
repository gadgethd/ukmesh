import assert from 'node:assert/strict';
import test from 'node:test';
import pg from 'pg';
import { processNextPrivacyRematerialization } from '../workers/privacy-rematerialization.js';

const endpoint = process.env.TEST_INGEST_DATABASE_URL;

test('node-scoped privacy completion preserves unrelated generation gaps and re-requests', {
  skip: endpoint ? false : 'owned PostgreSQL required', timeout: 30000,
}, async (t) => {
  assert.ok(['localhost', '127.0.0.1'].includes(new URL(endpoint!).hostname));
  const admin = new pg.Pool({ connectionString: endpoint, max: 1 });
  const database = `privacy_completion_${process.pid}_${Date.now()}`;
  await admin.query(`CREATE DATABASE "${database}"`);
  const url = new URL(endpoint!); url.pathname = `/${database}`;
  const pool = new pg.Pool({ connectionString: url.href, max: 2 });
  t.after(async () => {
    await pool.end();
    await admin.query(`DROP DATABASE "${database}" WITH (FORCE)`);
    await admin.end();
  });
  await pool.query(`CREATE EXTENSION IF NOT EXISTS timescaledb;
    CREATE TABLE public_visibility_state(singleton boolean PRIMARY KEY, generation bigint, updated_at timestamptz);
    CREATE TABLE packet_visibility_materialization_state(singleton boolean PRIMARY KEY, visibility_generation bigint, updated_at timestamptz);
    CREATE TABLE privacy_rematerialization_queue(id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
      node_id text, network text, target text, status text, attempts int DEFAULT 0,
      requested_at timestamptz DEFAULT NOW(), started_at timestamptz, finished_at timestamptz,
      claim_token uuid, claim_expires_at timestamptz, last_error text);`);
  for (const state of ['current', 'stale', 'missing', 're-requested'] as const) {
    await pool.query(`TRUNCATE public_visibility_state, packet_visibility_materialization_state, privacy_rematerialization_queue;
      INSERT INTO public_visibility_state VALUES(true,10,NOW());`);
    if (state !== 'missing') await pool.query(
      'INSERT INTO packet_visibility_materialization_state VALUES(true,$1,NOW())', [state === 'stale' ? 9 : 10]);
    await pool.query(`INSERT INTO privacy_rematerialization_queue(node_id,network,target,status)
      VALUES($1,'ukmesh','packets','pending')`, ['A'.repeat(64)]);
    const executor = state !== 're-requested' ? pool : {
      connect: () => pool.connect(),
      query: async (sql: string, params?: unknown[]) => {
        const result = await pool.query(sql, params);
        if (sql.includes('FROM timescaledb_information.chunks')) await pool.query(
          "UPDATE privacy_rematerialization_queue SET requested_at=started_at + INTERVAL '1 second'");
        return result;
      },
    };
    assert.equal(await processNextPrivacyRematerialization(executor as pg.Pool, { chunkPauseMs: 0 }), true);
    const visibility = (await pool.query(`SELECT p.generation::int AS public_generation,
      m.visibility_generation::int AS materialized_generation FROM public_visibility_state p
      LEFT JOIN packet_visibility_materialization_state m USING(singleton)`)).rows[0];
    assert.deepEqual(visibility, {
      public_generation: state === 're-requested' ? 10 : 11,
      materialized_generation: state === 'missing' ? null : state === 'stale' ? 9 : state === 'current' ? 11 : 10,
    }, state);
    assert.equal((await pool.query('SELECT status FROM privacy_rematerialization_queue')).rows[0].status,
      state === 're-requested' ? 'pending' : 'done', state);
  }
});
