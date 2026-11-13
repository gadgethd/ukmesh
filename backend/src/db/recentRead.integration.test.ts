import { createPublicPolicyReader } from '../privacy/policyReader.js';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import test from 'node:test';
import pg from 'pg';
import { createDisposableDatabase } from './fixtures/disposableDatabase.js';
import { Redis } from 'ioredis';
import { getRecentMessages, getRecentPackets } from '../repositories/packetReads.js';
import { closeDb } from './runtime.js';
import { bootstrapBaseSchemaFixture, POST_MOVE_SEARCH_PATH } from './fixtures/baseSchema.js';
import { configurePacketBatch, persistEntriesCore, type QueryFn } from './packetBatch.js';
import { admitPacketToRam, configureRamStore, readPendingEntries } from '../ram/liveStore.js';
import type { InsertPacketInput } from './packetInput.js';
import { readRecentWithPending, mergePendingPacketsIntoRecentRows, resolvePendingSummaries, type PendingSummary } from '../ram/liveRead.js';
import { getRamStreamKey } from '../ram/protocol.js';
import { processNextPrivacyRematerialization } from '../workers/privacy-rematerialization.js';

const url = process.env['TEST_INGEST_DATABASE_URL'];
const redisURL = process.env['TEST_REDIS_URL'];
const local = (value?: string) => value && ['localhost', '127.0.0.1'].includes(new URL(value).hostname);
const json = (value: unknown) => JSON.parse(JSON.stringify(value));

test('F01/F05/F12/F13 real SQL and RAM contracts agree across scoped paging, stable IDs and privacy generations', {
  skip: local(url) && local(redisURL) ? false : 'loopback PostgreSQL and Redis fixtures required',
}, async (t) => {
  const fixture = await createDisposableDatabase(t, url!, 'meshcore_read');
  const pool = new pg.Pool({ connectionString: fixture.url, max: 3, options: POST_MOVE_SEARCH_PATH });
  fixture.addCleanup(() => pool.end());
  fixture.addCleanup(() => closeDb());
  const redis = new Redis(redisURL!, { lazyConnect: true, enableOfflineQueue: false });
  const oldMode = process.env['MQTT_INGEST_MODE'];
  const oldKey = process.env['MQTT_RAM_STREAM_KEY'];
  process.env['MQTT_INGEST_MODE'] = 'ram';
  process.env['MQTT_RAM_STREAM_KEY'] = `meshcore:read:${process.pid}:${Date.now()}`;
  const key = getRamStreamKey();
  fixture.addCleanup(async () => {
    try {
      const keys = await redis.keys(`${key}*`);
      if (keys.length) await redis.del(...keys);
    } finally { redis.disconnect(); }
  });
  fixture.addCleanup(() => {
    if (oldMode === undefined) delete process.env['MQTT_INGEST_MODE']; else process.env['MQTT_INGEST_MODE'] = oldMode;
    if (oldKey === undefined) delete process.env['MQTT_RAM_STREAM_KEY']; else process.env['MQTT_RAM_STREAM_KEY'] = oldKey;
  });
  fixture.addCleanup(() => { configurePacketBatch(null); });
  fixture.addCleanup(() => { configureRamStore(null); });
  await redis.connect();
  await bootstrapBaseSchemaFixture(pool);
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
  await pool.query(`INSERT INTO core.node_identity_aliases
    (source_node_id, canonical_node_id, confidence, reason) VALUES ($1, $2, 'high', 'fixture')`, [alias, canonical]);
  while (await processNextPrivacyRematerialization(pool, { chunkPauseMs: 0 })) { /* finish fixture policy */ }
  const loadPolicy = createPublicPolicyReader(query);
  const aliases = async () => new Map([[alias, canonical]]);
  const summaries = (entries: PendingSummary[]) => resolvePendingSummaries(entries,query);
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
    await admit({ ...base, packetHash: '84'.repeat(32), payload: {}, time:new Date(observed-2_500) }),
    await admit({ ...base, packetHash: '85'.repeat(32), payload: {_summary:42}, time:new Date(observed-2_600) }),
    await admit({ ...base, packetHash: '86'.repeat(32), payload: {_summary:{z:false,a:'text'}}, time:new Date(observed-2_700) }),
  ];
  await pool.query('INSERT INTO packet_decryptions(packet_hash,decrypted,summary) VALUES ($1,$2,$3)',
    ['84'.repeat(32),{},'decrypted fallback summary']);
  const merged = new Map<string, unknown>();
  for (const scope of [undefined, 'ukmesh', 'northeast', 'teesside', 'othernet']) {
    for (const observer of [undefined, alias, canonical, other]) {
      for (const fields of ['full', 'slim'] as const) {
        for (const limit of [1, 5]) {
          const opts = { limit, network: scope, observer, fields, loadPolicy, resolveAliases: aliases, resolveSummaries:summaries, nowMs: observed };
          merged.set(JSON.stringify(opts), json(await readRecentWithPending(
            (hashes) => getRecentPackets(limit, scope, observer, fields, undefined, query, hashes), opts)));
        }
      }
    }
  }
  const messages = json(await readRecentWithPending((hashes) => getRecentMessages(5, 'ukmesh', undefined, undefined, query, hashes),
    { limit: 5, network: 'ukmesh', rowKind: 'messages', packetTypes: [5], loadPolicy, resolveAliases: aliases, resolveSummaries:summaries, nowMs: observed }));
  await persistEntriesCore(pending, query, true, transaction);
  await redis.del(key); // compare against the complete SQL projection, without overlap
  for (const [serialized, expected] of merged) {
    const opts = JSON.parse(serialized);
    const actual = await readRecentWithPending(
      () => getRecentPackets(opts.limit, opts.network, opts.observer, opts.fields, undefined, query),
      { ...opts, loadPolicy, resolveAliases: aliases, nowMs: observed });
    assert.deepEqual(json(actual), expected, serialized);
  }
  assert.deepEqual(json(await readRecentWithPending(() => getRecentMessages(5, 'ukmesh', undefined, undefined, query),
    { limit: 5, network: 'ukmesh', rowKind: 'messages', packetTypes: [5], loadPolicy, resolveAliases: aliases, nowMs: observed })), messages);
  await t.test('bounded page and RAM overlap keep complete observation metadata', async () => {
    const complete = await getRecentPackets(50, 'ukmesh', undefined, 'full', undefined, query);
    assert.ok(complete.length > 2, 'fixture spans more than one page');
    assert.deepEqual(json(await getRecentPackets(2, 'ukmesh', undefined, 'full', undefined, query)), json(complete.slice(0, 2)));
    const oldest = complete.at(-1)!;
    const overlap = await getRecentPackets(1, 'ukmesh', undefined, 'full', undefined, query, [oldest.packet_hash]);
    assert.deepEqual(json(overlap), json([complete[0], oldest]));
    assert.equal(complete.find(row => row.packet_hash === '80'.repeat(32))?.rx_count, 3,
      'choosing a page must not truncate older observations of its identities');
  });
  const snapshot = await pool.connect();
  try {
    await snapshot.query('BEGIN ISOLATION LEVEL REPEATABLE READ');
    const before = await getRecentPackets(5, 'ukmesh', undefined, 'full', undefined,
      (sql, params) => snapshot.query(sql, params));
    assert.equal(before.find(row=>row.packet_hash==='80'.repeat(32))?.rx_count, 3);
    await pool.query('UPDATE ops.public_visibility_state SET generation = generation + 1 WHERE singleton');
    await pool.query(`UPDATE ops.packet_visibility_materialization_state SET visibility_generation =
      (SELECT generation FROM ops.public_visibility_state WHERE singleton) WHERE singleton`);
    assert.deepEqual(await mergePendingPacketsIntoRecentRows(before, { limit: 5, network: 'ukmesh', loadPolicy }), [],
      'fresh authority rejects a genuine older SQL aggregate snapshot');
    await snapshot.query('COMMIT');
  } finally { await snapshot.query('ROLLBACK'); snapshot.release(); }
});
