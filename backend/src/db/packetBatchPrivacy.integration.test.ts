import assert from 'node:assert/strict';
import { randomBytes, randomUUID } from 'node:crypto';
import test from 'node:test';
import pg from 'pg';
import { bootstrapBaseSchemaFixture, POST_MOVE_SEARCH_PATH } from './fixtures/baseSchema.js';
import { createDisposableDatabase } from './fixtures/disposableDatabase.js';
import { configurePacketBatch, persistEntriesCore, refreshAdmissionPrivacyCache,
  type PacketBatchInput, type QueryFn } from './packetBatch.js';
import { processNextPrivacyRematerialization } from '../workers/privacy-rematerialization.js';

const endpoint = process.env['TEST_INGEST_DATABASE_URL'];

test('packet batch uses current privacy for stale nonzero caches and every public contribution', {
  skip: endpoint ? false : 'owned loopback PostgreSQL fixture required', timeout: 120_000,
}, async t => {
  const fixture = await createDisposableDatabase(t, endpoint!, 'packet_privacy');
  const pool = new pg.Pool({ connectionString: fixture.url, max: 3,
    application_name: 'packet-privacy-native', options: POST_MOVE_SEARCH_PATH,
    statement_timeout: 10_000 });
  fixture.addCleanup(() => pool.end());
  await bootstrapBaseSchemaFixture(pool);
  const query: QueryFn = (sql, params) => pool.query(sql, params);
  const transaction = async <T>(work: (execute: QueryFn) => Promise<T>): Promise<T> => {
    const client = await pool.connect();
    try {
      await client.query('BEGIN');
      const result = await work((sql, params) => client.query(sql, params));
      await client.query('COMMIT');
      return result;
    } catch (error) {
      await client.query('ROLLBACK');
      throw error;
    } finally { client.release(); }
  };
  const prime = async () => {
    configurePacketBatch(query, transaction);
    await refreshAdmissionPrivacyCache();
  };
  const prepare = async (label: string, privateNode = false) => {
    const receiver = randomBytes(32).toString('hex').toUpperCase();
    const network = `privacy_${randomBytes(6).toString('hex')}`;
    await pool.query('INSERT INTO core.nodes(node_id,name,network) VALUES($1,$2,$3)',
      [receiver, label + (privateNode ? ' 🚫' : ''), network]);
    await rematerialize();
    const input: PacketBatchInput = { time: new Date(), packetHash: randomBytes(32).toString('hex'),
      rxNodeId: receiver, srcNodeId: null, topic: `meshcore/ZZZ/${receiver}/packets`,
      topicPrefix: 'meshcore', iata: 'ZZZ', packetType: 3, routeType: 1, hopCount: 1,
      rssi: -80, snr: 8, payloadJson: '{}', companionSender: null, rawHex: '00',
      advertCount: null, pathHashes: [receiver.slice(0, 4)], pathHashSizeBytes: 2,
      network, transportCodes: null, regionScope: null };
    return input;
  };
  const rematerialize = async () => {
    while (await processNextPrivacyRematerialization(pool, { chunkPauseMs: 0 })) { /* owned scratch only */ }
  };
  const advancePublicGeneration = async () => {
    await pool.query(`WITH changed AS (
      UPDATE ops.public_visibility_state SET generation=generation+1 WHERE singleton RETURNING generation
    ) UPDATE ops.packet_visibility_materialization_state SET visibility_generation=changed.generation
      FROM changed WHERE singleton`);
  };
  const persist = async (input: PacketBatchInput, observationId = randomUUID()) =>
    (await persistEntriesCore([{ input, observationId }], query, true, transaction))[0]!;
  const contributions = async (input: PacketBatchInput) => (await pool.query(`SELECT
    (SELECT count(*)::int FROM mesh.packets WHERE network=$1) packets,
    (SELECT count(*)::int FROM mesh.packet_paths WHERE network=$1) paths,
    (SELECT coalesce(sum(packet_count),0)::int FROM mesh.packet_hourly_stats WHERE network=$1) hourly,
    (SELECT count(*)::int FROM mesh.packet_daily_stats WHERE network=$1) daily,
    (SELECT count(*)::int FROM core.observer_region_packet_sightings WHERE network=$1) regions`,
  [input.network])).rows[0];
  const stored = async (input: PacketBatchInput) => (await pool.query(
    'SELECT is_private,visibility_ok FROM mesh.packets WHERE packet_hash=$1', [input.packetHash])).rows;
  const persistAcrossPrivacyChange = async (input: PacketBatchInput,
    change: (client: pg.PoolClient) => Promise<unknown>) => {
    const holder = await pool.connect();
    let pending: ReturnType<typeof persist> | undefined;
    try {
      await holder.query('BEGIN');
      const holderPid = (await holder.query('SELECT pg_backend_pid() AS pid')).rows[0].pid;
      await change(holder);
      pending = persist(input); void pending.catch(() => undefined);
      let waiting = false;
      const deadline = performance.now() + 2_000;
      while (!waiting && performance.now() < deadline) {
        waiting = (await pool.query(`SELECT EXISTS(SELECT 1 FROM pg_stat_activity
          WHERE application_name='packet-privacy-native' AND wait_event_type='Lock'
            AND $1=ANY(pg_blocking_pids(pid))) waiting`, [holderPid])).rows[0].waiting;
        if (!waiting) await new Promise(resolve => setTimeout(resolve, 10));
      }
      assert.equal(waiting, true, 'ingest must wait for the uncommitted privacy fence');
      await holder.query('COMMIT');
      const result = await pending; await refreshAdmissionPrivacyCache();
      return result;
    } finally {
      await holder.query('ROLLBACK'); holder.release();
      await pending?.catch(() => undefined);
    }
  };

  await t.test('public identity generation changes retain the first public result and rollups', async () => {
    const input = await prepare('public before identity fence');
    await prime(); await advancePublicGeneration();
    const result = await persist(input);
    await refreshAdmissionPrivacyCache();
    assert.equal(result.prefix_cache_fresh, false, 'exercise a warm cache from an earlier valid generation');
    assert.deepEqual({ stored: await stored(input), isPrivate: result.is_private,
      visibilityOk: result.visibility_ok, contributions: await contributions(input) },
    { stored: [{ is_private: false, visibility_ok: true }], isPrivate: false, visibilityOk: true,
      contributions: { packets: 1, paths: 1, hourly: 1, daily: 1, regions: 1 } });
  });

  await t.test('a newly private observer cannot use the stale public cache', async () => {
    const input = await prepare('public before opt out');
    await prime();
    await pool.query('UPDATE core.nodes SET name=$2 WHERE node_id=$1', [input.rxNodeId, 'private now 🚫']);
    await rematerialize();
    const result = await persist(input); await refreshAdmissionPrivacyCache();
    assert.equal(result.prefix_cache_fresh, false);
    assert.deepEqual({ isPrivate: result.is_private, visibilityOk: result.visibility_ok },
      { isPrivate: true, visibilityOk: false });
    assert.deepEqual(await stored(input), [{ is_private: true, visibility_ok: false }]);
    assert.deepEqual(await contributions(input), { packets: 1, paths: 1, hourly: 1, daily: 0, regions: 0 });
  });

  await t.test('restored public observers retain their first result and contributions', async () => {
    const input = await prepare('private before restoration', true);
    await prime();
    await pool.query('UPDATE core.nodes SET name=$2 WHERE node_id=$1', [input.rxNodeId, 'public now']);
    await rematerialize();
    const result = await persist(input); await refreshAdmissionPrivacyCache();
    assert.equal(result.prefix_cache_fresh, false);
    assert.deepEqual({ isPrivate: result.is_private, visibilityOk: result.visibility_ok },
      { isPrivate: false, visibilityOk: true });
    assert.deepEqual(await stored(input), [{ is_private: false, visibility_ok: true }]);
    assert.deepEqual(await contributions(input), { packets: 1, paths: 1, hourly: 1, daily: 1, regions: 1 });
  });

  await t.test('invalid paths remain invisible and duplicate delivery adds no contributions', async () => {
    const input = await prepare('invalid path'); input.pathHashes = ['ZZ'];
    await prime(); await advancePublicGeneration();
    const id = randomUUID(), result = await persist(input, id); await refreshAdmissionPrivacyCache();
    assert.equal(result.visibility_ok, false);
    assert.equal((await stored(input))[0]?.visibility_ok, false);
    const once = await contributions(input);
    assert.deepEqual(once, { packets: 1, paths: 1, hourly: 1, daily: 0, regions: 0 });
    await persist(input, id);
    assert.deepEqual(await contributions(input), once);
  });

  await t.test('a committed privacy change wins when ingest waits on its generation fence', async () => {
    const input = await prepare('concurrent public'); await prime();
    const result = await persistAcrossPrivacyChange(input, holder => holder.query(
      'UPDATE core.nodes SET name=$2 WHERE node_id=$1', [input.rxNodeId, 'concurrent private 🚫']));
    assert.equal(result.visibility_ok, false);
    assert.deepEqual(await stored(input), [{ is_private: true, visibility_ok: false }]);
    assert.deepEqual(await contributions(input), { packets: 1, paths: 1, hourly: 1, daily: 0, regions: 0 });
    await rematerialize();
  });

  await t.test('a direct prefix generation update fences the first later classification', async () => {
    const input = await prepare('concurrent prefix'); await prime();
    const result = await persistAcrossPrivacyChange(input, holder => holder.query(`
      INSERT INTO core.private_node_prefixes(node_id,network,prefix_size_bytes,prefix)
      VALUES($1,$2,2,$3)`, [input.rxNodeId, input.network, input.rxNodeId!.slice(0, 4)]));
    assert.equal(result.visibility_ok, false);
    assert.deepEqual(await stored(input), [{ is_private: true, visibility_ok: false }]);
    assert.deepEqual(await contributions(input), { packets: 1, paths: 1, hourly: 1, daily: 0, regions: 0 });
  });

  await t.test('missing authority rolls the entire batch back', async () => {
    const input = await prepare('missing authority'); await prime();
    const generation = (await pool.query('SELECT generation FROM ops.public_visibility_state WHERE singleton')).rows[0].generation;
    try {
      await pool.query('DELETE FROM ops.public_visibility_state WHERE singleton');
      await assert.rejects(persist(input), /PACKET_BATCH_RESULT_MISMATCH/);
      assert.deepEqual(await contributions(input), { packets: 0, paths: 0, hourly: 0, daily: 0, regions: 0 });
    } finally {
      await pool.query('INSERT INTO ops.public_visibility_state(singleton,generation) VALUES(true,$1)', [generation]);
    }
  });

  await t.test('invalid authority generations never authorize the fallback', async () => {
    for (const generation of [0, -1, null]) {
      const input = await prepare('invalid authority'); await prime();
      input.pathHashes = null; input.pathHashSizeBytes = null;
      const previous = (await pool.query('SELECT generation FROM ops.public_visibility_state WHERE singleton')).rows[0].generation;
      try {
        await pool.query('ALTER TABLE ops.public_visibility_state ALTER COLUMN generation DROP NOT NULL');
        await pool.query('UPDATE ops.public_visibility_state SET generation=$1 WHERE singleton', [generation]);
        const result = await persist(input);
        assert.equal(result.is_private, true); assert.equal(result.visibility_ok, false);
        assert.equal((await contributions(input)).daily, 0);
        assert.equal((await contributions(input)).regions, 0);
      } finally {
        await pool.query('UPDATE ops.public_visibility_state SET generation=$1 WHERE singleton', [previous]);
        await pool.query('ALTER TABLE ops.public_visibility_state ALTER COLUMN generation SET NOT NULL');
        await refreshAdmissionPrivacyCache();
      }
    }
  });
});
