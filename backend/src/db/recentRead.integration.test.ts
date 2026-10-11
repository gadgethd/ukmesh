import assert from 'node:assert/strict';
import fs from 'node:fs';
import test from 'node:test';
import pg from 'pg';
import { Redis } from 'ioredis';
import { closeDb, getRecentMessages, getRecentPackets } from './index.js';
import { runMigrations } from './migrations.js';
import { configurePacketBatch, persistEntriesCore, type QueryFn } from './packetBatch.js';
import { admitPacketToRam, configureRamStore, readPendingEntries } from '../ram/liveStore.js';
import type { InsertPacketInput } from './packetInput.js';
import { readRecentWithPending, mergePendingPacketsIntoRecentRows } from '../ram/liveRead.js';
import { getRamStreamKey } from '../ram/protocol.js';
import { processNextPrivacyRematerialization } from '../workers/privacy-rematerialization.js';

const url = process.env['TEST_INGEST_DATABASE_URL'];
const redisURL = process.env['TEST_REDIS_URL'];
const local = (value?: string) => value && ['localhost', '127.0.0.1'].includes(new URL(value).hostname);
const json = (value: unknown) => JSON.parse(JSON.stringify(value));

test('F01/F05/F12/F13 real SQL and RAM contracts agree across scoped paging, stable IDs and privacy generations', {
  skip: local(url) && local(redisURL) ? false : 'loopback PostgreSQL and Redis fixtures required',
}, async (t) => {
  const admin = new pg.Pool({ connectionString: url, max: 1 });
  const name = `meshcore_read_${process.pid}_${Date.now()}`;
  await admin.query(`CREATE DATABASE "${name}"`);
  const ownedURL = new URL(url!);
  ownedURL.pathname = `/${name}`;
  const pool = new pg.Pool({ connectionString: ownedURL.toString(), max: 3 });
  const redis = new Redis(redisURL!, { lazyConnect: true, enableOfflineQueue: false });
  const oldMode = process.env['MQTT_INGEST_MODE'];
  const oldKey = process.env['MQTT_RAM_STREAM_KEY'];
  process.env['MQTT_INGEST_MODE'] = 'ram';
  process.env['MQTT_RAM_STREAM_KEY'] = `meshcore:read:${process.pid}:${Date.now()}`;
  const key = getRamStreamKey();
  t.after(async () => {
    configureRamStore(null);
    configurePacketBatch(null);
    const keys = await redis.keys(`${key}*`);
    if (keys.length) await redis.del(...keys);
    await redis.quit();
    await closeDb();
    await pool.end();
    await admin.query(`DROP DATABASE IF EXISTS "${name}" WITH (FORCE)`);
    await admin.end();
    if (oldMode === undefined) delete process.env['MQTT_INGEST_MODE']; else process.env['MQTT_INGEST_MODE'] = oldMode;
    if (oldKey === undefined) delete process.env['MQTT_RAM_STREAM_KEY']; else process.env['MQTT_RAM_STREAM_KEY'] = oldKey;
  });
  await redis.connect();
  await pool.query(fs.readFileSync(new URL('./schema/base.sql', import.meta.url), 'utf8'));
  await runMigrations(pool);
  const query: QueryFn = (sql, params) => pool.query(sql, params);
  const transaction = async <T>(work: (query: QueryFn) => Promise<T>) => {
    const client = await pool.connect();
    try {
      await client.query('BEGIN');
      const result = await work((sql, params) => client.query(sql, params));
      await client.query('COMMIT');
      return result;
    } catch (error) { await client.query('ROLLBACK'); throw error; }
    finally { client.release(); }
  };
  configureRamStore(redis);
  configurePacketBatch(query, transaction);
  const admit = async (input: InsertPacketInput & { time: Date }) => {
    t.mock.timers.enable({ apis: ['Date'], now: input.time.getTime() });
    try {
      const accepted = await admitPacketToRam(input);
      const stored = (await readPendingEntries(10)).find(({ entry }) => entry.observationId === accepted.observationId)!.entry;
      return { input: stored.input, observationId: stored.observationId };
    } finally { t.mock.timers.reset(); }
  };
  const alias = 'AA'.repeat(32), canonical = 'BB'.repeat(32), other = 'CD'.repeat(32);
  await pool.query(`INSERT INTO node_identity_aliases
    (source_node_id, canonical_node_id, confidence, reason) VALUES ($1, $2, 'high', 'fixture')`, [alias, canonical]);
  while (await processNextPrivacyRematerialization(pool, { chunkPauseMs: 0 })) { /* finish fixture policy */ }
  const aliases = async () => new Map([[alias, canonical]]);
  const observed = Date.now();
  const base = { packetHash: '80'.repeat(32), rxNodeId: alias, topic: `meshcore/ZZZ/${alias}/packets`,
    network: 'northeast', rawHex: '00', packetType: 5, pathHashes: [] as string[],
    time: new Date(observed - 4_000), payload: { direction: 'rx', _summary: 'encrypted' } };
  const committed = await admit(base);
  await persistEntriesCore([committed], query, true, transaction);
  const older = { input: { ...committed.input, time: new Date(observed - 120_000) },
    observationId: '00000000-0000-4000-8000-000000000042' };
  const pageLeader = await admit({ ...base, packetHash: '81'.repeat(32), time: new Date(observed - 3_500) });
  await persistEntriesCore([older, pageLeader], query, true, transaction);
  const pending = [
    await admit({ ...base, time: new Date(observed - 2_000), payload: { direction: 'tx', _summary: 'newer' } }),
    await admit({ ...base, rxNodeId: other, srcNodeId: other,
      time: new Date(observed - 3_000), payload: { appData: { text: 'decoded' }, direction: 'rx', _summary: 'decoded' } }),
    await admit({ ...base, packetHash: '81'.repeat(32), time: new Date(observed - 3_500) }),
    await admit({ ...base, packetHash: '82'.repeat(32), network: 'othernet' }),
    await admit({ ...base, packetHash: '83'.repeat(32), time: new Date(observed - 301_000) }),
  ];
  const merged = new Map<string, unknown>();
  for (const scope of [undefined, 'ukmesh', 'northeast', 'teesside', 'othernet']) {
    for (const observer of [undefined, alias, canonical, other]) {
      for (const fields of ['full', 'slim'] as const) {
        for (const limit of [1, 5]) {
          const opts = { limit, network: scope, observer, fields, resolveAliases: aliases, nowMs: observed };
          merged.set(JSON.stringify(opts), json(await readRecentWithPending(
            (hashes) => getRecentPackets(limit, scope, observer, fields, undefined, query, hashes), opts)));
        }
      }
    }
  }
  const messages = json(await readRecentWithPending((hashes) => getRecentMessages(5, 'ukmesh', undefined, undefined, query, hashes),
    { limit: 5, network: 'ukmesh', rowKind: 'messages', packetTypes: [5], resolveAliases: aliases, nowMs: observed }));
  await persistEntriesCore(pending, query, true, transaction);
  await redis.del(key); // compare against the complete SQL projection, without overlap
  for (const [serialized, expected] of merged) {
    const opts = JSON.parse(serialized);
    const actual = await readRecentWithPending(
      () => getRecentPackets(opts.limit, opts.network, opts.observer, opts.fields, undefined, query),
      { ...opts, resolveAliases: aliases, nowMs: observed });
    assert.deepEqual(json(actual), expected, serialized);
  }
  assert.deepEqual(json(await readRecentWithPending(() => getRecentMessages(5, 'ukmesh', undefined, undefined, query),
    { limit: 5, network: 'ukmesh', rowKind: 'messages', packetTypes: [5], resolveAliases: aliases, nowMs: observed })), messages);
  const snapshot = await pool.connect();
  try {
    await snapshot.query('BEGIN ISOLATION LEVEL REPEATABLE READ');
    const before = await getRecentPackets(5, 'ukmesh', undefined, 'full', undefined,
      (sql, params) => snapshot.query(sql, params));
    assert.equal(before[0]?.rx_count, 3);
    await pool.query('UPDATE public_visibility_state SET generation = generation + 1 WHERE singleton');
    await pool.query(`UPDATE packet_visibility_materialization_state SET visibility_generation =
      (SELECT generation FROM public_visibility_state WHERE singleton) WHERE singleton`);
    assert.deepEqual(await mergePendingPacketsIntoRecentRows(before, { limit: 5, network: 'ukmesh' }), [],
      'fresh authority rejects a genuine older SQL aggregate snapshot');
    await snapshot.query('COMMIT');
  } finally { await snapshot.query('ROLLBACK'); snapshot.release(); }
});
