import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import test, { type TestContext } from 'node:test';
import pg from 'pg';
import { buildIncidents } from './analyzer.js';
import { DEFAULT_SPAM_MESSAGE_CONFIG as CFG } from './config.js';
import { normalizeMessage } from './normalize.js';
import { countLogicalMessages, getPublicIncident, getPublicIncidents, loadRecentMessages, persistIncidents } from './repository.js';
import type { MessageRecord } from './types.js';

// Optional PostgreSQL-WASM fixture: in-memory SQL only, no existing server/URL.
// Same opt-in module pattern as the repository's cleanup integration tests.
const moduleUrl = process.env['TEST_SPAM_PGLITE_MODULE'];
const options = { skip: moduleUrl ? false : 'TEST_SPAM_PGLITE_MODULE is not configured' };
const MIN = 60_000;

async function fixture(t: TestContext) {
  const { PGlite } = await import(moduleUrl!);
  const db = await PGlite.create();
  t.after(() => db.close());
  await db.exec(`
    CREATE TABLE nodes (node_id TEXT PRIMARY KEY, name TEXT, lat DOUBLE PRECISION, lon DOUBLE PRECISION);
    CREATE TABLE path_history_cache (id INTEGER);
    CREATE TABLE packets (
      packet_hash TEXT, time TIMESTAMPTZ, network TEXT, packet_type INTEGER,
      payload JSONB, rx_node_id TEXT, src_node_id TEXT, is_private BOOLEAN DEFAULT FALSE,
      hop_count INTEGER, rssi DOUBLE PRECISION, snr DOUBLE PRECISION
    );
  `);
  for (const migration of [
    '005_spam_message_incidents.sql', '012_analysis_run_protocol.sql',
    '015_public_visibility_generation.sql', '024_analysis_run_leases.sql',
  ]) {
    await db.exec(await readFile(new URL(`../db/migrations/${migration}`, import.meta.url), 'utf8'));
  }
  const run = {
    runId: 'synthetic-spam-run', workload: 'spam-analysis', scope: 'public', leaseToken: 'synthetic-lease',
    privacyGeneration: 1, modelGeneration: 'spam-analysis-v2',
  };
  await db.query(`
    INSERT INTO analysis_runs (run_id, workload, scope, status, window_start, window_end,
                               lease_token, privacy_generation, model_generation)
      VALUES ($1, $2, $3, 'running', NOW() - INTERVAL '24 hours', NOW(), $4, $5, $6);
  `, [run.runId, run.workload, run.scope, run.leaseToken, run.privacyGeneration, run.modelGeneration]);
  await db.query(`
    INSERT INTO analysis_workload_state (workload, scope, active_run_id, active_lease_token,
                                         active_lease_expires_at, active_run_deadline_at)
      VALUES ($1, $2, $3, $4, NOW() + INTERVAL '5 minutes', NOW() + INTERVAL '5 minutes');
  `, [run.workload, run.scope, run.runId, run.leaseToken]);
  let failMemberInsert = false;
  let releases = 0;
  const query = async (sql: string, params?: unknown[]) => {
    if (failMemberInsert && sql.includes('INSERT INTO spam_message_members')) throw new Error('synthetic member failure');
    const result = await db.query(sql, params);
    return { rows: result.rows, rowCount: result.affectedRows ?? result.rows.length };
  };
  t.mock.method(pg.Pool.prototype, 'query', query);
  t.mock.method(pg.Pool.prototype, 'connect', async () => ({ query, release() { releases += 1; } }));
  return { db, run, failMembers() { failMemberInsert = true; }, releases: () => releases };
}

function burst(count: number, lastSeen: number, offset: number): MessageRecord[] {
  return Array.from({ length: count }, (_, i) => {
    const text = 'Synthetic recurring campaign at https://campaign.example.org';
    return {
      id: `fixture-${offset + i}`, network: 'ukmesh', sender: 'FixtureRelay', text,
      norm: normalizeMessage(text), channelHash: '80', channelLabel: 'Public',
      observedAt: lastSeen - (count - 1 - i) * 10_000,
      observers: [{ observerId: 'synthetic-observer', lat: 53, lon: -1, hopCount: 1 }],
    };
  });
}

test('PostgreSQL publishes one merged campaign and serves row/JSON/timeline agreement', options, async (t) => {
  const f = await fixture(t);
  const now = Date.now();
  const old = burst(17, now - 180 * MIN, 0);
  const fresh = burst(8, now, 17);
  const items = buildIncidents([...old, ...fresh], now, CFG);
  assert.deepEqual(await persistIncidents(items, CFG, f.run), { upserted: 1, active: 1, removed: 0, lifecycleExpired: 0 });
  const rows = await f.db.query('SELECT * FROM spam_message_incidents');
  assert.equal(rows.rows.length, 1);
  const row = rows.rows[0];
  assert.equal(row.message_count, 25);
  assert.equal(row.last_seen.getTime(), now);
  assert.equal(row.status, row.public_json.status);
  assert.equal(row.message_count, row.public_json.messageCount);
  assert.equal(row.observer_count, row.public_json.observerCount);
  assert.equal(row.last_seen.getTime(), Date.parse(row.public_json.lastSeen));
  const listed = await getPublicIncidents({ status: 'active', minConfidence: CFG.publicMinScore });
  assert.equal(listed.length, 1);
  assert.equal(listed[0]!.messageCount, 25);
  const detail = await getPublicIncident(items[0]!.incident.key);
  assert.equal(detail!.timeline.length, 25);
  assert.equal(detail!.timeline.at(-1)!.observedAt, new Date(now).toISOString());

  // A later rolling/bounded window may no longer contain the oldest burst.
  await persistIncidents(buildIncidents(fresh, now, CFG), CFG, f.run);
  const rolled = (await f.db.query('SELECT first_seen, public_json FROM spam_message_incidents')).rows[0];
  assert.equal(rolled.first_seen.getTime(), old[0]!.observedAt);
  assert.equal(Date.parse(rolled.public_json.firstSeen), rolled.first_seen.getTime());
  assert.equal(rolled.public_json.lastSeen, new Date(now).toISOString());
  assert.equal(f.releases(), 2);
});

test('PostgreSQL ignores older closed snapshots without replacing newer campaign members', options, async (t) => {
  const f = await fixture(t);
  const now = Date.now();
  const old = burst(17, now - 180 * MIN, 0);
  const fresh = burst(8, now, 17);
  const items = buildIncidents([...old, ...fresh], now, CFG);
  await persistIncidents(items, CFG, f.run);
  const before = await getPublicIncident(items[0]!.incident.key);
  const stale = buildIncidents(old, now, CFG);
  assert.equal(stale[0]!.status, 'closed');
  assert.deepEqual(await persistIncidents(stale, CFG, f.run), { upserted: 0, active: 0, removed: 0, lifecycleExpired: 0 });
  assert.deepEqual(await getPublicIncident(items[0]!.incident.key), before);

  await f.db.query(`UPDATE spam_message_incidents SET last_seen = NOW() - INTERVAL '1 hour'`);
  const expired = await persistIncidents([], CFG, f.run);
  assert.equal(expired.lifecycleExpired, 1);
  assert.equal((await getPublicIncident(items[0]!.incident.key))!.status, 'closed');
});

test('PostgreSQL keeps merged campaign counts while strictly bounding sampled evidence', options, async (t) => {
  const f = await fixture(t);
  const now = Date.now();
  const old = burst(17, now - 180 * MIN, 0);
  const fresh = burst(8, now, 17);
  const items = buildIncidents([...old, ...fresh], now, CFG);
  for (const limit of [1, 2, 3, 8]) {
    await persistIncidents(items, { ...CFG, maxEvidenceMembersPerIncident: limit }, f.run);
    const detail = await getPublicIncident(items[0]!.incident.key);
    assert.equal(detail!.messageCount, 25, 'campaign counts describe all qualifying bursts');
    assert.equal(detail!.timeline.length, limit, 'persist only the configured evidence sample');
    assert.equal(detail!.timeline.at(-1)!.observedAt, new Date(now).toISOString());
    if (limit >= 2) assert.equal(detail!.timeline[0]!.observedAt, new Date(old[0]!.observedAt).toISOString());
  }
});

test('PostgreSQL rolls back row/member changes and still enforces the publication fence', options, async (t) => {
  const f = await fixture(t);
  const now = Date.now();
  const items = buildIncidents(burst(8, now, 0), now, CFG);
  await persistIncidents(items, CFG, f.run);
  const before = await getPublicIncident(items[0]!.incident.key);
  f.failMembers();
  await assert.rejects(persistIncidents(buildIncidents(burst(9, now + MIN, 0), now + MIN, CFG), CFG, f.run), /synthetic member failure/);
  assert.deepEqual(await getPublicIncident(items[0]!.incident.key), before);
  await f.db.query(`UPDATE analysis_workload_state SET active_lease_token = 'replaced-lease'`);
  await assert.rejects(persistIncidents(items, CFG, f.run), /analysis publication fence rejected/);
  assert.deepEqual(await getPublicIncident(items[0]!.incident.key), before);
  assert.equal(f.releases(), 3);
});

test('PostgreSQL loader retains a rotating burst on a busy day and reports cap reductions', options, async (t) => {
  const f = await fixture(t);
  await f.db.exec(`
    INSERT INTO packets (packet_hash, time, network, packet_type, payload)
      SELECT 'noise-' || i, NOW() - INTERVAL '1 hour', 'ukmesh', 5,
             jsonb_build_object('decrypted', jsonb_build_object('sender', 'FixtureTester', 'message', 'test'),
                                '_summary', '[test] synthetic connectivity traffic', 'channelHash', '81')
        FROM generate_series(1, 2900) i;
    INSERT INTO packets (packet_hash, time, network, packet_type, payload)
      SELECT 'rotating-' || i, NOW() - INTERVAL '2 minutes' + i * INTERVAL '10 seconds', 'ukmesh', 5,
             jsonb_build_object('decrypted', jsonb_build_object(
               'sender', (ARRAY['R7K2X9PM', '5BQ8T3NW', 'Z6D4V1CK', '8JF2G9SL', 'N1X8R4BT', '6WK3P9DZ', 'T9L5B2CQ'])[i],
               'message', 'Synthetic promotion at https://community.example.org'),
               '_summary', '[Public] synthetic promo', 'channelHash', '80')
        FROM generate_series(1, 7) i;
  `);
  const window = { start: new Date(Date.now() - 24 * 60 * MIN), end: new Date() };
  assert.equal(await countLogicalMessages(window), 2_907);
  const loaded = await loadRecentMessages(CFG, window);
  assert.equal(loaded.length, 2_907);
  const items = buildIncidents(loaded, Date.now(), CFG);
  assert.equal(items.length, 1);
  assert.equal(items[0]!.publicJson.messageCount, 7);

  const warnings: string[] = [];
  t.mock.method(console, 'warn', (message: string) => warnings.push(message));
  const capped = await loadRecentMessages({ ...CFG, maxMessagesPerRun: 2_000 }, window);
  assert.equal(capped.length, 2_000);
  assert.equal(buildIncidents(capped, Date.now(), CFG).length, 0, 'the former cap really does lose this burst');
  assert.match(warnings[0]!, /phase=messages limit=2000 observed_at_least=2907 omitted_at_least=907/);
});
