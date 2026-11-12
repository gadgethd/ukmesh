import assert from 'node:assert/strict';
import test from 'node:test';
import pg from 'pg';
import { bootstrapBaseSchemaFixture, POST_MOVE_SEARCH_PATH } from '../db/fixtures/baseSchema.js';

const endpoint = process.env['TEST_INGEST_DATABASE_URL'];
const loopback = endpoint && ['127.0.0.1', 'localhost'].includes(new URL(endpoint).hostname);

test('empty selected training window retains published priors and completes the all-network fallback', {
  skip: loopback ? false : 'owned loopback PostgreSQL fixture required', timeout: 60_000,
}, async () => {
  const admin = new pg.Pool({ connectionString: endpoint, max: 1 });
  const name = `meshcore_learning_${process.pid}_${Date.now()}`;
  await admin.query(`CREATE DATABASE "${name}"`);
  const url = new URL(endpoint!); url.pathname = `/${name}`;
  const pool = new pg.Pool({ connectionString: url.toString(), max: 2, options: POST_MOVE_SEARCH_PATH });
  let closeDb: (() => Promise<void>) | undefined;
  const previous = new Map<string, string | undefined>();
  try {
    await bootstrapBaseSchemaFixture(pool);
    await pool.query(`INSERT INTO analytics.path_position_prefix_priors(network, prefix, position, node_id, count, probability)
      VALUES ('all', 'AA', 0, 'AA0011', 7, 1)`);
    for (const key of ['DATABASE_URL', 'MESHCORE_HTTP_DATABASE_URL', 'MESHCORE_WORKER_DATABASE_URL']) {
      previous.set(key, process.env[key]); process.env[key] = url.toString();
    }
    const { rebuildPathLearningModels } = await import('./rebuild.js');
    ({ closeDb } = await import('../db/runtime.js'));
    await rebuildPathLearningModels();
    assert.deepEqual((await pool.query(`SELECT count::int AS count FROM analytics.path_position_prefix_priors
      WHERE network = 'all' AND prefix = 'AA' AND node_id = 'AA0011'`)).rows, [{ count: 7 }]);
    const runs = await pool.query(`SELECT scope, status FROM analytics.analysis_runs WHERE workload = 'path-learning'`);
    assert.equal(runs.rows.length, 1);
    assert.equal(runs.rows[0]?.status, 'complete');
    assert.ok(String(runs.rows[0]?.scope).includes('all'));
  } finally {
    await closeDb?.(); await pool.end();
    for (const [key, value] of previous) {
      if (value === undefined) delete process.env[key]; else process.env[key] = value;
    }
    await admin.query(`DROP DATABASE "${name}" WITH (FORCE)`); await admin.end();
  }
});
