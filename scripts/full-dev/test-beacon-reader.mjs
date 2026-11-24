import assert from 'node:assert/strict';
import {spawnSync} from 'node:child_process';
import {createHash, randomUUID} from 'node:crypto';
import {readFileSync} from 'node:fs';
import {createRequire} from 'node:module';
import test from 'node:test';

const require = createRequire(new URL('../../backend/package.json', import.meta.url));
const {Pool} = require('pg');
const endpoint = process.env.BEACON_TEST_DATABASE_URL;
const script = readFileSync(new URL('./beacon-reader.sql', import.meta.url), 'utf8');

// Same disposable database/role owner as test-corescope-reader.mjs. The source
// SQL itself, including its fixture and credential guards, is applied by psql.
test('Beacon history credential exposes only current public GroupText and exact source identity', {
  skip: !endpoint && 'BEACON_TEST_DATABASE_URL is required', timeout: 30_000,
}, async () => {
  const adminURL = new URL(endpoint);
  assert.equal(adminURL.hostname, '127.0.0.1');
  assert.equal(adminURL.port, '5433', 'use the owned sample database, never production');
  assert.match(adminURL.username, /^[a-z_][a-z0-9_]*$/);
  const token = randomUUID().replaceAll('-', '').slice(0, 24);
  const database = 'lean_verify_' + token;
  const role = 'audit_beacon_' + token;
  const password = randomUUID();
  const admin = new Pool({connectionString: adminURL.href, max: 1});
  let fixture, reader, createdDatabase = false, createdRole = false;
  try {
    await admin.query(`CREATE DATABASE ${database}`);
    createdDatabase = true;
    await admin.query(`COMMENT ON DATABASE ${database} IS 'ukmesh-owned-fixture:${token}'`);
    await admin.query(`CREATE ROLE ${role} LOGIN NOINHERIT PASSWORD '${password}'`);
    createdRole = true;
    const fixtureURL = new URL(adminURL);
    fixtureURL.pathname = '/' + database;
    fixture = new Pool({connectionString: fixtureURL.href, max: 1});
    await fixture.query(`
      REVOKE CREATE ON SCHEMA public FROM PUBLIC;
      CREATE SCHEMA mesh; CREATE SCHEMA core; CREATE SCHEMA ops;
      CREATE TABLE mesh.packets (
        time timestamptz NOT NULL, observation_id uuid NOT NULL, network text,
        iata text, packet_uid bytea, identity_version text, identity_provenance text,
        packet_hash text, packet_type integer, topic text, rx_node_id text,
        src_node_id text, path_hashes text[], path_hash_size_bytes integer,
        rssi real, snr real, payload jsonb, raw_hex text,
        visibility_ok boolean, is_private boolean, UNIQUE(time, observation_id)
      );
      CREATE TABLE mesh.ingest_observation_ledger (
        seq bigserial PRIMARY KEY, observation_id uuid UNIQUE, observed_at timestamptz, network text
      );
      CREATE INDEX ON mesh.ingest_observation_ledger (network, seq);
      CREATE TABLE mesh.ingest_observation_ledger_coverage (singleton boolean PRIMARY KEY, version integer);
      CREATE TABLE core.private_node_prefixes (node_id text, network text, prefix_size_bytes integer, prefix text);
      CREATE TABLE ops.public_visibility_state (singleton boolean PRIMARY KEY, generation bigint);
      CREATE TABLE ops.packet_visibility_materialization_state (singleton boolean PRIMARY KEY, visibility_generation bigint);
      CREATE TABLE ops.privacy_rematerialization_queue (node_id text, status text);
      INSERT INTO mesh.ingest_observation_ledger_coverage VALUES (true, 1);
      INSERT INTO ops.public_visibility_state VALUES (true, 7);
      INSERT INTO ops.packet_visibility_materialization_state VALUES (true, 7);
      INSERT INTO core.private_node_prefixes VALUES (repeat('B',64),'ukmesh',1,'BB');
      CREATE VIEW public.packets AS SELECT * FROM mesh.packets;
      CREATE SCHEMA corescope_public;
      CREATE VIEW corescope_public.packets AS SELECT * FROM mesh.packets;
    `);
    const valid = readFileSync(new URL('../../backend/src/db/migrations/051_reset_readiness.sql', import.meta.url), 'utf8')
      .match(/CREATE OR REPLACE FUNCTION meshcore_path_is_valid\([\s\S]*?\$\$;/)?.[0];
    const privatePath = readFileSync(new URL('../../backend/src/db/migrations/114_repair_private_prefix_functions.sql', import.meta.url), 'utf8')
      .match(/CREATE OR REPLACE FUNCTION public\.meshcore_path_matches_private\([\s\S]*?\$function\$;/)?.[0];
    assert.ok(valid && privatePath, 'exercise the existing authoritative privacy functions');
    await fixture.query(valid + '\n' + privatePath);
    const raw = '1500AABBCCDDEEFF';
    const uid = createHash('sha256').update(Buffer.from('14AABBCCDDEEFF', 'hex')).digest();
    const observedAt = '2026-11-24T01:02:03.123456Z';
    const rows = [
      {status: 'candidate-for-decode'},
      {status: 'candidate-for-decode'}, // distinct hearing; same receiver, raw and timestamp
      {status: 'missing-uid', uid: null},
      {status: 'invalid-uid', uid: Buffer.alloc(31)},
      {status: 'unsupported-identity-version', version: 'future-v2'},
      {status: 'invalid-observer-key', receiver: 'AB'},
      {status: 'missing-raw', raw: null},
      {status: 'invalid-raw-hex', raw: 'ABC'},
      {status: 'invalid-raw-hex', raw: 'GG'},
      {hidden: true, type: 4},
      {hidden: true, source: 'B'.repeat(64)},
      {hidden: true, paths: ['BB']},
      {hidden: true, paths: ['ABCD']}, // stale true flag cannot validate a malformed path
      {hidden: true, visibility: false},
      {hidden: true, private: true},
      {status: 'candidate-for-decode', network: 'isolated-test-network'},
    ];
    for (const row of rows) {
      row.id = randomUUID();
      await fixture.query(`INSERT INTO mesh.packets VALUES (
        $1,$2,$3,'LON',$4,$5,'meshcore-decoded-wire','legacy-alias',$6,
        'meshcore/LON/fixture/packets',$7,$8,$9,1,-70,3,
        '{"direction":"rx","secret":"not-exported"}',$10,$11,$12
      )`, [observedAt, row.id, row.network ?? 'ukmesh',
        Object.hasOwn(row, 'uid') ? row.uid : uid, row.version ?? 'wire-sha256-v1', row.type ?? 5,
        row.receiver ?? 'C'.repeat(64), row.source ?? 'A'.repeat(64), row.paths ?? [],
        Object.hasOwn(row, 'raw') ? row.raw : raw, row.visibility ?? true, row.private ?? false]);
      await fixture.query(`INSERT INTO mesh.ingest_observation_ledger (observation_id,observed_at,network)
        VALUES ($1,$2,$3)`, [row.id, observedAt, row.network ?? 'ukmesh']);
    }
    const provision = () => spawnSync('sg', ['docker', '-c',
      `docker exec -i ukmesh-sample-db psql -X -q -v ON_ERROR_STOP=1 -v beacon_reader_role=${role} -U ${adminURL.username} -d ${database}`],
    {input: script, encoding: 'utf8', timeout: 15_000,
      env: {PATH: process.env.PATH, HOME: process.env.HOME, LANG: 'C.UTF-8'}});
    const applied = provision();
    assert.ifError(applied.error);
    assert.equal(applied.status, 0, applied.stderr);
    const readerURL = new URL(fixtureURL);
    readerURL.username = role;
    readerURL.password = password;
    reader = new Pool({connectionString: readerURL.href, max: 1});
    assert.deepEqual((await reader.query('SELECT current_user,session_user')).rows,
      [{current_user: role, session_user: role}]);
    assert.deepEqual((await reader.query('SELECT * FROM source_state')).rows, [{
      source_contract_version: 1, generation: '7', visibility_generation: '7',
      ledger_coverage_version: 1, ready: true,
    }]);
    const visible = await reader.query(`SELECT *,
      to_char(time AT TIME ZONE 'UTC','YYYY-MM-DD"T"HH24:MI:SS.US"Z"') AS exact_time FROM packets
      WHERE network = ANY($1::text[]) ORDER BY seq`, [['ukmesh']]);
    assert.deepEqual(visible.rows.map(row => row.observation_id), rows.filter(row => !row.hidden && !row.network).map(row => row.id));
    assert.deepEqual(visible.rows.map(row => row.raw_input_status), rows.filter(row => !row.hidden && !row.network).map(row => row.status));
    assert.deepEqual(visible.rows.slice(0, 2).map(row => row.raw_hex), [raw, raw]);
    assert.ok(visible.rows.slice(2).every(row => row.raw_hex === null));
    assert.deepEqual(visible.rows[0].packet_uid, uid);
    assert.equal(visible.rows[0].identity_version, 'wire-sha256-v1');
    assert.equal(visible.rows[0].identity_provenance, 'meshcore-decoded-wire');
    assert.equal(visible.rows[0].packet_hash, 'legacy-alias');
    assert.equal(visible.rows[0].exact_time, observedAt);
    assert.equal(visible.rows[0].direction, 'rx');
    assert.ok(!visible.fields.some(field => ['payload','secret'].includes(field.name)));
    const page = await reader.query(`SELECT seq,observation_id FROM ingest_observation_ledger
      WHERE network=ANY($1::text[]) AND seq>$2 ORDER BY seq LIMIT 1`, [['ukmesh'], visible.rows[0].seq]);
    assert.deepEqual(page.rows.map(row => row.observation_id), [rows[1].id]);
    assert.equal((await reader.query('SELECT * FROM packets WHERE network=$1', ['isolated-test-network'])).rowCount, 1);
    for (const statement of [
      'SELECT raw_hex FROM mesh.packets', 'SELECT * FROM public.packets',
      'SELECT * FROM mesh.ingest_observation_ledger', 'SELECT * FROM core.private_node_prefixes',
      'SELECT * FROM corescope_public.packets', 'SELECT * FROM ops.public_visibility_state',
      'INSERT INTO mesh.packets DEFAULT VALUES', 'CREATE TABLE beacon_public.forbidden(id integer)',
      'CREATE TABLE public.forbidden(id integer)', `SET ROLE ${adminURL.username}`,
    ]) await assert.rejects(reader.query(statement), {code: '42501'}, statement);
    const writes = await reader.query("SELECT has_table_privilege('beacon_public.packets', 'INSERT,UPDATE,DELETE,TRUNCATE') AS allowed");
    assert.equal(writes.rows[0].allowed, false);
    await assert.rejects(reader.query('INSERT INTO packets DEFAULT VALUES'));
    const unavailable = async reason => {
      assert.equal((await reader.query('SELECT ready FROM source_state')).rows[0].ready, false, reason);
      assert.equal((await reader.query('SELECT * FROM packets')).rowCount, 0, reason);
      assert.equal((await reader.query('SELECT * FROM ingest_observation_ledger')).rowCount, 0, reason);
    };
    await fixture.query('UPDATE ops.packet_visibility_materialization_state SET visibility_generation=8');
    await unavailable('stale materialization');
    await fixture.query('UPDATE ops.packet_visibility_materialization_state SET visibility_generation=7');
    await fixture.query('UPDATE mesh.ingest_observation_ledger_coverage SET version=2');
    await unavailable('unknown ledger version');
    await fixture.query('UPDATE mesh.ingest_observation_ledger_coverage SET version=1');
    for (const status of ['pending','processing','failed']) {
      await fixture.query('INSERT INTO ops.privacy_rematerialization_queue VALUES ($1,$2)', ['private-id', status]);
      await unavailable(status);
      await fixture.query('TRUNCATE ops.privacy_rematerialization_queue');
    }
    await fixture.query("INSERT INTO core.private_node_prefixes VALUES (repeat('A',64),'ukmesh',1,'AA')");
    assert.equal((await reader.query('SELECT * FROM packets WHERE network=$1', ['ukmesh'])).rowCount, 0,
      'current private-prefix policy revokes even unchanged public flags');
    await fixture.query(`GRANT SELECT(raw_hex) ON mesh.packets TO ${role}`);
    const rejected = provision();
    assert.equal(rejected.status, 3);
    assert.match(rejected.stderr, /raw relation privileges/);
    await fixture.query(`REVOKE SELECT(raw_hex) ON mesh.packets FROM ${role}`);
    await fixture.query(`COMMENT ON DATABASE ${database} IS 'not-owned'`);
    const wrongMarker = provision();
    assert.equal(wrongMarker.status, 3);
    assert.match(wrongMarker.stderr, /ownership marker/);
  } finally {
    if (reader) await reader.end();
    if (fixture) await fixture.end();
    if (createdDatabase) await admin.query(`DROP DATABASE ${database} WITH (FORCE)`);
    if (createdRole) await admin.query(`DROP ROLE ${role}`);
    await admin.end();
  }
});
