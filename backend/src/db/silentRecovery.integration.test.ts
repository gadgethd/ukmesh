import assert from 'node:assert/strict';
import fs from 'node:fs';
import test from 'node:test';
import pg from 'pg';
import { runMigrations } from './migrations.js';
import { persistEntriesCore, type QueryFn } from './packetBatch.js';
import { buildPacketBatchInput } from './packetInput.js';
import { processNextPrivacyRematerialization } from '../workers/privacy-rematerialization.js';
import { createForwardingOutbox } from '../owner/forwardingOutbox.js';
import { RECLAIM_EXPIRED_ALERT_DELIVERIES_SQL } from '../owner/alertRules.js';
import { PrivatePrefixCache } from '../privacy/privatePrefixCache.js';

const databaseUrl = process.env['TEST_INGEST_DATABASE_URL'];
test('recovery migration: atomic forwarding, hypertable capacity, and abandoned privacy claims', {
  skip: !databaseUrl && 'TEST_INGEST_DATABASE_URL is not configured', timeout: 120_000,
}, async (t) => {
  const admin = new pg.Pool({ connectionString: databaseUrl, max: 1 });
  const name = `meshcore_recovery_${process.pid}_${Date.now()}`;
  await admin.query(`CREATE DATABASE "${name}"`);
  const url = new URL(databaseUrl!); url.pathname = `/${name}`;
  const pool = new pg.Pool({ connectionString: url.toString(), max: 3 });
  t.after(async () => { await pool.end(); await admin.query(`DROP DATABASE "${name}" WITH (FORCE)`); await admin.end(); });
  await pool.query(fs.readFileSync(new URL('./schema/base.sql', import.meta.url), 'utf8'));
  await runMigrations(pool);
  const execute: QueryFn = (sql, params) => pool.query(sql, params);
  const transaction = async <T>(work: (query: QueryFn) => Promise<T>) => {
    const client = await pool.connect();
    try { await client.query('BEGIN'); const value = await work((sql, params) => client.query(sql, params)); await client.query('COMMIT'); return value; }
    catch (error) { await client.query('ROLLBACK'); throw error; }
    finally { client.release(); }
  };
  await pool.query("INSERT INTO owner_public_feed_settings (owner_username, enabled) VALUES ('fixture', TRUE)");
  await pool.query("INSERT INTO owner_public_feed_nodes (owner_username, node_id) VALUES ('fixture', $1)", ['A'.repeat(64)]);
  const input = buildPacketBatchInput({ packetHash: 'recovery-valid', rxNodeId: 'A'.repeat(64), topic: `meshcore/LHR/${'A'.repeat(64)}/packets`,
    packetType: 5, rawHex: '0102', pathHashes: ['ABCD'], pathHashSizeBytes: 2, network: 'ukmesh',
    forwarding: { packet: { id: 'fixture', packetHash: 'recovery-valid', topic: 'fixture', ts: Date.now() }, upstreamEnvelope: { raw: '0102' } } });
  await persistEntriesCore([{ input, observationId: null }], execute, false, transaction);
  assert.equal((await pool.query('SELECT COUNT(*)::int AS count FROM packet_forwarding_outbox')).rows[0].count, 1);
  await assert.rejects(persistEntriesCore([{ input: { ...input, packetHash: 'bad', packetType: 5.5 }, observationId: null }], execute, false, transaction));
  assert.equal((await pool.query('SELECT COUNT(*)::int AS count FROM packet_forwarding_outbox')).rows[0].count, 1, 'rolled back packet cannot leave a forwarding intent');
  const policy = new PrivatePrefixCache();
  policy.replace(1, []);
  const deps = { execute: pool.query.bind(pool), loadPolicy: async () => policy };
  const failedHandoff = createForwardingOutbox(async () => { throw new Error('destination queue unavailable'); }, deps);
  await failedHandoff.processDue();
  const pending = (await pool.query('SELECT attempts, last_error FROM packet_forwarding_outbox')).rows[0];
  assert.equal(pending.attempts, 1);
  assert.match(pending.last_error, /destination queue unavailable/);
  await pool.query('UPDATE packet_forwarding_outbox SET next_attempt_at = NOW()');
  let forwarded = 0;
  const restarted = createForwardingOutbox(async () => { forwarded++; }, deps);
  await restarted.processDue();
  assert.equal(forwarded, 1);
  assert.equal((await pool.query('SELECT COUNT(*)::int AS count FROM packet_forwarding_outbox')).rows[0].count, 0);
  const fixture = await pool.connect();
  try {
    await fixture.query(`CREATE TEMP TABLE owner_alert_deliveries (
      id INT, status TEXT, attempts INT, claim_token TEXT, claim_expires_at TIMESTAMPTZ,
      next_attempt_at TIMESTAMPTZ, last_error TEXT, updated_at TIMESTAMPTZ)`);
    await fixture.query(`INSERT INTO owner_alert_deliveries (id, status, attempts, claim_token, claim_expires_at)
      VALUES (1, 'delivering', 5, 'old', NOW() - INTERVAL '1 second'),
             (2, 'delivering', 4, 'old', NOW() - INTERVAL '1 second')`);
    await fixture.query(RECLAIM_EXPIRED_ALERT_DELIVERIES_SQL);
    assert.deepEqual((await fixture.query('SELECT id, status, claim_token FROM owner_alert_deliveries ORDER BY id')).rows,
      [{ id: 1, status: 'dead_lettered', claim_token: null }, { id: 2, status: 'failed', claim_token: null }]);
  } finally { fixture.release(true); }
  const sizes = await pool.query(`SELECT approximate_row_count('packet_paths'::regclass)::text AS rows,
    hypertable_size('packet_paths'::regclass)::text AS bytes,
    pg_total_relation_size('packet_paths'::regclass)::text AS parent`);
  assert.ok(Number(sizes.rows[0].bytes) > Number(sizes.rows[0].parent));
  assert.ok(Number(sizes.rows[0].rows) >= 0);
  await pool.query(`INSERT INTO privacy_rematerialization_queue (node_id, target, status, attempts, started_at, claim_token, claim_expires_at)
    VALUES ($1, 'packets', 'processing', 1, NOW() - INTERVAL '1 hour', gen_random_uuid(), NOW() - INTERVAL '1 minute')`, ['B'.repeat(64)]);
  assert.equal(await processNextPrivacyRematerialization(pool, { chunkPauseMs: 0, chunkStatementTimeoutMs: 5_000 }), false, 'expired lease enters retry backoff');
  await pool.query("UPDATE privacy_rematerialization_queue SET finished_at = NOW() - INTERVAL '1 day'");
  assert.equal(await processNextPrivacyRematerialization(pool, { chunkPauseMs: 0, chunkStatementTimeoutMs: 5_000 }), true);
  const recovered = (await pool.query('SELECT status, attempts, claim_token FROM privacy_rematerialization_queue')).rows[0];
  assert.equal(recovered.status, 'done');
  assert.equal(recovered.attempts, 2);
  assert.equal(recovered.claim_token, null);
});
