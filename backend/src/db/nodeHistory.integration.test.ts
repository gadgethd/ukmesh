import assert from 'node:assert/strict';
import test from 'node:test';
import pg from 'pg';
import { createDisposableDatabase } from './fixtures/disposableDatabase.js';
import { bootstrapBaseSchemaFixture, POST_MOVE_SEARCH_PATH } from './fixtures/baseSchema.js';
import { createDatabaseRuntime } from './databaseRuntime.js';
import { pool as defaultPool } from './runtime.js';
import { getNodeHistory } from '../repositories/nodeReads.js';
import { convergePacketVisibility } from '../privacy/convergence.js';
import { readPublicResponseGeneration } from '../privacy/responseGeneration.js';

const endpoint = process.env['TEST_SAMPLE_DATABASE_URL'];

test('prepared history retains real rows, aliases, privacy and boundaries after generic plan reuse', {
  skip: endpoint ? false : 'owned TEST_SAMPLE_DATABASE_URL required', timeout: 180_000,
}, async t => {
  const fixture = await createDisposableDatabase(t, endpoint!, 'history_behavior');
  const writer = new pg.Pool({ connectionString: fixture.url, max: 2, options: POST_MOVE_SEARCH_PATH });
  fixture.addCleanup(() => writer.end());
  await bootstrapBaseSchemaFixture(writer);
  // The full-runtime access owner grants these API reads after migrations.
  // Reproduce only that allowlist in this unique fixture; this is effective
  // read-behavior coverage, not production permission qualification.
  assert.equal((await writer.query('SELECT current_database() AS name')).rows[0].name, fixture.name);
  await writer.query(`GRANT USAGE ON SCHEMA core, mesh, ops TO meshcore_api;
    GRANT SELECT ON core.node_identity_aliases, mesh.packets,
      ops.packet_visibility_materialization_state, ops.public_visibility_state,
      ops.privacy_rematerialization_queue TO meshcore_api`);
  const readUrl = new URL(fixture.url);
  readUrl.searchParams.set('options', '-c default_transaction_read_only=on');
  const reader = createDatabaseRuntime({ DATABASE_URL: readUrl.href, DATABASE_POOL_MAX: '1' });
  fixture.addCleanup(() => reader.close());
  // Keep the real default-runtime query, preparation registry and physical
  // client implementation. Only checkout targets this unique fixture DB.
  const checkout = t.mock.method(defaultPool, 'connect', () => reader.pool.connect());
  fixture.addCleanup(() => checkout.mock.restore());
  let transactionOpen = false;
  fixture.addCleanup(async () => { if (transactionOpen) await reader.query('ROLLBACK'); });
  await reader.query('SET ROLE meshcore_api');
  await reader.query('BEGIN READ ONLY');
  transactionOpen = true;
  try {
    const identity = (await reader.query(`SELECT NOW()::text AS at, pg_backend_pid() AS pid,
      current_database() AS database, current_user AS role,
      current_setting('transaction_read_only') AS read_only`)).rows[0]!;
    assert.equal(identity['database'], fixture.name);
    assert.equal(identity['role'], 'meshcore_api');
    assert.equal(identity['read_only'], 'on');
    // NOW is fixed by this owned READ COMMITTED transaction, so exact lower
    // and upper time boundaries do not depend on test execution speed.
    const reference = String(identity['at']);
    const referenceMs = new Date(reference).getTime();
    assert.ok(Number.isFinite(referenceMs));
    const id = (hex: string) => hex.repeat(64);
    const [subject, subjectAlias, source, sourceAlias, other, privateNode, addedAlias, capNode]
      = ['A', 'B', 'C', 'D', 'E', 'F', '6', '7'].map(id);
    for (const [nodeId, name] of [
      [subject, 'History subject'], [subjectAlias, 'History alias'],
      [source, 'History origin'], [sourceAlias, 'Origin alias'], [other, 'Other receiver'],
      [privateNode, 'Private fixture 🚫'], [addedAlias, 'Later alias'], [capNode, 'Capped history'],
    ]) await writer.query(`INSERT INTO core.nodes(node_id,name,network,role)
      VALUES($1,$2,'ukmesh',2)`, [nodeId, name]);
    for (const [from, to] of [[subjectAlias, subject], [sourceAlias, source]]) {
      await writer.query(`INSERT INTO core.node_identity_aliases
        (source_node_id,canonical_node_id,confidence,reason)
        VALUES($1,$2,'high','owned history behavior fixture')`, [from, to]);
    }
    type Row = {
      label: string; offsetMs: number; hash: string; rx: string | null; src: string | null;
      canonicalSource: string | null; network: string; topicPrefix: string; topic: string;
      packetType: number; hops: number; rssi: number | null; snr: number | null;
      payload: { label: string; nested: { text: string; flags: (boolean | null)[] } } | null;
    };
    const rows = new Map<string, Row>();
    let sequence = 0;
    async function add(label: string, offsetMs: number, rx: string | null, src: string | null,
      options: Partial<Row> = {}) {
      const row: Row = { label, offsetMs, rx, src, canonicalSource: src,
        hash: (++sequence).toString(16).padStart(64, '0'), network: 'ukmesh',
        topicPrefix: 'meshcore', topic: 'meshcore/ZZZ/history-fixture/packets',
        packetType: 3, hops: 2, rssi: -91.5, snr: 4.25,
        payload: { label, nested: { text: 'café 🚲', flags: [true, null, false] } }, ...options };
      rows.set(label, row);
      await writer.query(`INSERT INTO mesh.packets
        (time,packet_hash,rx_node_id,src_node_id,network,topic_prefix,topic,
         packet_type,hop_count,rssi,snr,payload,path_hash_size_bytes,path_hashes)
        VALUES($1::timestamptz + $2::bigint * INTERVAL '1 millisecond',
          $3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,1,$14::text[])`,
      [reference, offsetMs, row.hash, rx, src, row.network, row.topicPrefix, row.topic,
        row.packetType, row.hops, row.rssi, row.snr, row.payload,
        label === 'invalid-path' ? ['ZZ'] : []]);
      return row;
    }
    await add('upper-bound', 0, subject!, source!);
    await add('unknown-source', -50_000, subject!, null, { rssi: null, snr: null, payload: null });
    await add('null-receiver-origin', -60_000, null, subject!);
    await add('north', -90_000, subjectAlias!, source!, { network: 'northeast' });
    await add('teesside-origin', -100_000, other!, subject!, { network: 'teesside' });
    await add('reception-and-origin-once', -120_000, subjectAlias!, subject!);
    const shared = await add('shared-hash-first', -150_000, subject!, source!);
    await add('shared-hash-second', -151_000, subjectAlias!, source!, { hash: shared.hash });
    await add('origin', -180_000, other!, subject!);
    await add('alias-reception', -240_000, subjectAlias!, sourceAlias!, { canonicalSource: source! });
    await add('reception', -300_000, subject!, source!);
    await add('two-hours', -2 * 3_600_000, subject!, source!);
    await add('inside-lower-bound', -24 * 3_600_000 + 1, subject!, source!);
    await add('lower-bound', -24 * 3_600_000, subject!, source!);
    await add('old', -25 * 3_600_000, subject!, source!);
    await add('future', 1, subject!, source!);
    await add('unrelated', -20_000, other!, source!);
    await add('private-origin', -21_000, subject!, privateNode!);
    await add('invalid-path', -22_000, subject!, source!);
    await add('test-topic', -23_000, subject!, source!, { topicPrefix: 'meshcore-test' });
    await add('fallback-test-topic', -24_000, subject!, source!, {
      topicPrefix: '', topic: 'meshcore-test/ZZZ/history-fixture/packets',
    });
    await add('test-network', -70_000, subject!, source!, { network: 'test', topicPrefix: 'meshcore-test' });
    await add('other-network', -80_000, subject!, source!, { network: 'differentnet' });
    await add('new-alias-reception', -30_000, addedAlias!, source!);
    const capLabels: string[] = [];
    for (let index = 1; index <= 505; index++) {
      const label = `cap-${index}`;
      capLabels.push(label);
      await add(label, -600_000 - index * 1_000, capNode!, source!);
    }
    const expected = (labels: string[], overrides: Record<string, string> = {}) => labels.map(label => {
      const row = rows.get(label)!;
      return { time: new Date(referenceMs + row.offsetMs), packet_hash: row.hash,
        src_node_id: overrides[label] ?? row.canonicalSource, topic: row.topic,
        packet_type: row.packetType, hop_count: row.hops, rssi: row.rssi, snr: row.snr, payload: row.payload };
    });
    const public24 = ['upper-bound', 'unknown-source', 'null-receiver-origin', 'north',
      'teesside-origin', 'reception-and-origin-once', 'shared-hash-first', 'shared-hash-second',
      'origin', 'alias-reception', 'reception', 'two-hours', 'inside-lower-bound'];
    const generation = async () => String((await writer.query(
      'SELECT generation::text FROM ops.public_visibility_state WHERE singleton')).rows[0].generation);
    const visibility = async () => (await writer.query(`SELECT state.generation::text AS current,
      materialized.visibility_generation::text AS materialized
      FROM ops.public_visibility_state state
      JOIN ops.packet_visibility_materialization_state materialized USING(singleton)
      WHERE state.singleton`)).rows[0]!;
    const authority = () => readPublicResponseGeneration(sql => writer.query(sql));
    const converge = async () => {
      await convergePacketVisibility(writer, { apply: true, expectedGeneration: await generation() });
      return readPublicResponseGeneration(sql => writer.query(sql));
    };
    await converge();
    for (let index = 0; index < 8; index++) {
      const hours = index % 2 ? 1 : 24;
      const request = index % 2 ? ` ${subjectAlias!.toLowerCase()} ` : subject!;
      assert.deepEqual(await getNodeHistory(request, hours, 'ukmesh'),
        expected(hours === 1 ? public24.slice(0, -2) : public24));
    }
    const definitions = async () => (await reader.query(`SELECT name,
      generic_plans::int AS generic, custom_plans::int AS custom
      FROM pg_prepared_statements WHERE starts_with(name,'read_v1_')`)).rows;
    let prepared = await definitions();
    assert.equal(prepared.length, 1, 'actual history opts into one definition on the checked-out client');
    const historyName = String(prepared[0]!['name']);
    assert.equal(Number(prepared[0]!['generic']) + Number(prepared[0]!['custom']), 8);
    // Isolated behavior coverage, not a production setting or performance
    // assertion: force a real generic execution before changing policy/aliases.
    await reader.query('SET LOCAL plan_cache_mode=force_generic_plan');
    assert.deepEqual(await getNodeHistory(subject!, 24, 'ukmesh'), expected(public24));
    prepared = await definitions();
    assert.ok(Number(prepared[0]!['generic']) > 0);

    await t.test('hours, both time boundaries, networks, duplicate exclusion and full cap remain exact', async () => {
      assert.deepEqual(await getNodeHistory(subject!, 48, 'ukmesh'), expected([...public24, 'lower-bound', 'old']));
      assert.deepEqual(await getNodeHistory(subject!, 24, 'northeast'), expected(['north']));
      assert.deepEqual(await getNodeHistory(subject!, 24, 'test'), expected(['test-network']));
      assert.deepEqual(await getNodeHistory(subject!, 24, 'differentnet'), expected(['other-network']));
      assert.deepEqual(await getNodeHistory(id('0'), 24, 'ukmesh'), []);
      assert.deepEqual(await getNodeHistory(capNode!, 24, 'ukmesh'), expected(capLabels.slice(0, 500)));
    });
    await t.test('existing generic definition sees newly committed aliases and canonical DTO values', async () => {
      const before = await generation();
      await writer.query(`UPDATE core.node_identity_aliases SET canonical_node_id=$1
        WHERE source_node_id=$2`, [other, sourceAlias]);
      await writer.query(`INSERT INTO core.node_identity_aliases
        (source_node_id,canonical_node_id,confidence,reason)
        VALUES($1,$2,'high','owned added history alias')`, [addedAlias, subject]);
      assert.ok(BigInt(await generation()) > BigInt(before));
      const refenced = await visibility();
      assert.equal(refenced.current, refenced.materialized, 'alias-only changes immediately refence valid packet bits');
      assert.equal(await authority(), Number(refenced.current));
      const aliasRows = expected(['upper-bound', 'new-alias-reception', ...public24.slice(1)],
        { 'alias-reception': other! });
      assert.deepEqual(await getNodeHistory(subject!, 24, 'ukmesh'), aliasRows,
        'the existing generic definition sees committed alias membership and canonical values immediately');
      await converge();
      assert.deepEqual(await getNodeHistory(addedAlias!, 24, 'ukmesh'), aliasRows);
    });
    await t.test('privacy revocation and restoration are fresh after generic execution', async () => {
      await writer.query("UPDATE core.nodes SET name='History private 🚫' WHERE node_id=$1", [source]);
      await assert.rejects(authority(), /PUBLIC_VISIBILITY_GENERATION_UNAVAILABLE/);
      const safeRows = expected([
        'unknown-source', 'null-receiver-origin', 'teesside-origin', 'reception-and-origin-once',
        'origin', 'alias-reception',
      ], { 'alias-reception': other! });
      assert.deepEqual(await getNodeHistory(subject!, 24, 'ukmesh'), safeRows,
        'repository filtering removes affected rows while HTTP authority refuses pending rematerialization');
      await converge();
      assert.deepEqual(await getNodeHistory(subject!, 24, 'ukmesh'), safeRows);
      await writer.query("UPDATE core.nodes SET name='History origin' WHERE node_id=$1", [source]);
      await writer.query('DELETE FROM core.node_identity_aliases WHERE source_node_id=$1', [addedAlias]);
      await assert.rejects(authority(), /PUBLIC_VISIBILITY_GENERATION_UNAVAILABLE/);
      await converge();
      assert.deepEqual(await getNodeHistory(subject!, 24, 'ukmesh'), expected(public24, { 'alias-reception': other! }));
    });
    await t.test('direct owned prefix changes stale materialization and globally fence prepared history', async () => {
      const key = [source, 'ukmesh', 1, 'CC'];
      assert.equal((await writer.query(`SELECT COUNT(*)::int AS n FROM core.private_node_prefixes
        WHERE node_id=$1 AND network=$2 AND prefix_size_bytes=$3 AND prefix=$4`, key)).rows[0].n, 0);
      await writer.query(`INSERT INTO core.private_node_prefixes
        (node_id,network,prefix_size_bytes,prefix) VALUES($1,$2,$3,$4)`, key);
      let state = await visibility();
      assert.notEqual(state.current, state.materialized, 'direct prefix insertion deliberately leaves packet bits stale');
      await assert.rejects(authority(), /PUBLIC_VISIBILITY_GENERATION_UNAVAILABLE/);
      assert.deepEqual(await getNodeHistory(subject!, 24, 'ukmesh'), []);
      assert.equal((await writer.query(`DELETE FROM core.private_node_prefixes
        WHERE node_id=$1 AND network=$2 AND prefix_size_bytes=$3 AND prefix=$4`, key)).rowCount, 1);
      state = await visibility();
      assert.notEqual(state.current, state.materialized, 'deleting the owned prefix cannot certify stale materialization');
      await assert.rejects(authority(), /PUBLIC_VISIBILITY_GENERATION_UNAVAILABLE/);
      assert.deepEqual(await getNodeHistory(subject!, 24, 'ukmesh'), []);
      await converge();
      assert.deepEqual(await getNodeHistory(subject!, 24, 'ukmesh'), expected(public24, { 'alias-reception': other! }));
    });
    const final = (await definitions()).find(row => row['name'] === historyName)!;
    assert.ok(Number(final['generic']) > Number(prepared[0]!['generic']), 'policy/alias cases used the same real generic definition');
    assert.equal((await reader.query('SELECT pg_backend_pid() AS pid')).rows[0]!['pid'], identity['pid']);
  } finally {
    await reader.query('ROLLBACK');
    transactionOpen = false;
  }
});
