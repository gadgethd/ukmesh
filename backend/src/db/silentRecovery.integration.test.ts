import assert from 'node:assert/strict';
import test from 'node:test';
import pg from 'pg';
import { bootstrapBaseSchemaFixture, POST_MOVE_SEARCH_PATH } from './fixtures/baseSchema.js';
import { persistEntriesCore, type QueryFn } from './packetBatch.js';
import { buildPacketBatchInput } from './packetInput.js';
import { processNextPrivacyRematerialization, reconcileAbandonedPrivacyClaims } from '../workers/privacy-rematerialization.js';
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
  const pool = new pg.Pool({ connectionString: url.toString(), max: 3, options: POST_MOVE_SEARCH_PATH });
  t.after(async () => { await pool.end(); await admin.query(`DROP DATABASE "${name}" WITH (FORCE)`); await admin.end(); });
  // A fresh deployment seeds base.sql and the operator roles before the first
  // `db:migrate`; without both preconditions the ledger guard correctly
  // refuses a non-empty, unledgered database and this suite can never run.
  await bootstrapBaseSchemaFixture(pool);
  const execute: QueryFn = (sql, params) => pool.query(sql, params);
  const transaction = async <T>(work: (query: QueryFn) => Promise<T>) => {
    const client = await pool.connect();
    try { await client.query('BEGIN'); const value = await work((sql, params) => client.query(sql, params)); await client.query('COMMIT'); return value; }
    catch (error) { await client.query('ROLLBACK'); throw error; }
    finally { client.release(); }
  };
  await pool.query("INSERT INTO core.owner_public_feed_settings (owner_username, enabled) VALUES ('fixture', TRUE)");
  await pool.query("INSERT INTO core.owner_public_feed_nodes (owner_username, node_id) VALUES ('fixture', $1)", ['A'.repeat(64)]);
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
  // The reclaim SQL is schema-qualified (`core.owner_alert_deliveries`), so a
  // TEMP stand-in can never shadow it — an earlier version of this suite
  // asserted against a temp table that the statement never touched. Seed the
  // real table in the disposable database instead.
  const rule = await pool.query<{ id: string }>(`
    INSERT INTO core.owner_alert_rules (owner_username, node_id, rule_type, threshold, channels)
    VALUES ('fixture', $1, 'offline_minutes', 60, '{"webhook":"https://example.invalid/hook"}'::jsonb)
    RETURNING id::text`, ['A'.repeat(64)]);
  await pool.query(`
    INSERT INTO core.owner_alert_deliveries
      (rule_id, event_key, destination_host, payload, status, attempts, claim_token, claim_expires_at)
    VALUES ($1, 'expired-5', 'example.invalid', '{}'::jsonb, 'delivering', 5, 'old', NOW() - INTERVAL '1 second'),
           ($1, 'expired-4', 'example.invalid', '{}'::jsonb, 'delivering', 4, 'old', NOW() - INTERVAL '1 second')`,
  [rule.rows[0]!.id]);
  await pool.query(RECLAIM_EXPIRED_ALERT_DELIVERIES_SQL);
  assert.deepEqual((await pool.query(
    'SELECT attempts, status, claim_token, claim_expires_at FROM core.owner_alert_deliveries ORDER BY attempts DESC',
  )).rows, [
    { attempts: 5, status: 'dead_lettered', claim_token: null, claim_expires_at: null },
    { attempts: 4, status: 'failed', claim_token: null, claim_expires_at: null },
  ]);
  const sizes = await pool.query(`SELECT approximate_row_count('packet_paths'::regclass)::text AS rows,
    hypertable_size('packet_paths'::regclass)::text AS bytes,
    pg_total_relation_size('packet_paths'::regclass)::text AS parent`);
  assert.ok(Number(sizes.rows[0].bytes) > Number(sizes.rows[0].parent));
  assert.ok(Number(sizes.rows[0].rows) >= 0);
  await pool.query(`INSERT INTO ops.privacy_rematerialization_queue (node_id, target, status, attempts, started_at, claim_token, claim_expires_at)
    VALUES ($1, 'packets', 'processing', 1, NOW() - INTERVAL '1 hour', gen_random_uuid(), NOW() - INTERVAL '1 minute')`, ['B'.repeat(64)]);
  assert.equal(await processNextPrivacyRematerialization(pool, { chunkPauseMs: 0, chunkStatementTimeoutMs: 5_000 }), false, 'expired lease enters retry backoff');
  // The abandoned claim itself must be aged out: token cleared, lease cleared,
  // and the row returned to the retryable failed state.
  const aged = await pool.query<{
    status: string; attempts: number; claim_token: string | null; claim_expires_at: string | null; last_error: string;
  }>(`SELECT status, attempts, claim_token, claim_expires_at, last_error
        FROM ops.privacy_rematerialization_queue WHERE node_id = $1`, ['B'.repeat(64)]);
  assert.equal(aged.rows[0]!.status, 'failed');
  assert.equal(aged.rows[0]!.attempts, 1);
  assert.equal(aged.rows[0]!.claim_token, null);
  assert.equal(aged.rows[0]!.claim_expires_at, null);
  assert.match(aged.rows[0]!.last_error, /worker lease expired/);

  // A claim that has burnt every attempt is reclaimed too, but is not put back
  // into the claimable set: it becomes a visible terminal row.
  await pool.query(`INSERT INTO ops.privacy_rematerialization_queue (node_id, target, status, attempts, started_at, claim_token, claim_expires_at)
    VALUES ($1, 'packets', 'processing', 8, NOW() - INTERVAL '1 hour', gen_random_uuid(), NOW() - INTERVAL '1 minute')`, ['C'.repeat(64)]);
  assert.deepEqual(await reconcileAbandonedPrivacyClaims(pool, 8), { reclaimed: 1, exhausted: 1 });
  const exhausted = await pool.query<{ status: string; claim_token: string | null; last_error: string }>(
    `SELECT status, claim_token, last_error FROM ops.privacy_rematerialization_queue WHERE node_id = $1`,
    ['C'.repeat(64)]);
  assert.equal(exhausted.rows[0]!.status, 'failed');
  assert.equal(exhausted.rows[0]!.claim_token, null);
  assert.match(exhausted.rows[0]!.last_error, /attempts exhausted \(8\/8\)/);

  await pool.query("UPDATE ops.privacy_rematerialization_queue SET finished_at = NOW() - INTERVAL '1 day'");
  assert.equal(await processNextPrivacyRematerialization(pool, { chunkPauseMs: 0, chunkStatementTimeoutMs: 5_000 }), true);
  const recovered = (await pool.query(
    'SELECT status, attempts, claim_token FROM ops.privacy_rematerialization_queue WHERE node_id = $1',
    ['B'.repeat(64)],
  )).rows[0];
  assert.equal(recovered.status, 'done');
  assert.equal(recovered.attempts, 2);
  assert.equal(recovered.claim_token, null);
  const terminal = (await pool.query(
    'SELECT status, attempts FROM ops.privacy_rematerialization_queue WHERE node_id = $1',
    ['C'.repeat(64)],
  )).rows[0];
  assert.deepEqual(terminal, { status: 'failed', attempts: 8 }, 'an exhausted claim must never be retried');
});
