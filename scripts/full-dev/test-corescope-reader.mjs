import assert from 'node:assert/strict';
import {spawnSync} from 'node:child_process';
import {mkdirSync, readFileSync, writeFileSync} from 'node:fs';
import {createRequire} from 'node:module';
import {randomUUID} from 'node:crypto';
import test from 'node:test';

const require = createRequire(new URL('../../backend/package.json', import.meta.url));
const {Pool} = require('pg');
const endpoint = process.env.CORESCOPE_TEST_DATABASE_URL;
const script = readFileSync(new URL('./corescope-reader.sql', import.meta.url), 'utf8');
const source = readFileSync(new URL('../../third_party/corescope/cmd/ingestor/redis_source.go', import.meta.url), 'utf8');
const policyStart = source.indexOf('func loadCorePublicPolicy(');
const policySQL = source.slice(policyStart).match(/QueryRowContext\(ctx, `([\s\S]*?)`\)/)?.[1];
assert.ok(policySQL, 'use the unchanged CoreScope policy query');

test('restricted CoreScope credential reads public projections and revocation metadata', {
  skip: !endpoint && 'CORESCOPE_TEST_DATABASE_URL is required', timeout: 30_000,
}, async () => {
  const adminURL = new URL(endpoint);
  assert.equal(adminURL.hostname, '127.0.0.1');
  assert.equal(adminURL.port, '5433', 'use the owned sample database, never production');
  const token = randomUUID().replaceAll('-', '').slice(0, 24);
  const database = 'lean_verify_' + token;
  const role = 'audit_core_' + token;
  const admin = new Pool({connectionString: adminURL.href, max: 1});
  let fixture;
  let reader;
  let createdDatabase = false;
  let createdRole = false;
  try {
    await admin.query(`CREATE DATABASE ${database}`);
    createdDatabase = true;
    await admin.query(`COMMENT ON DATABASE ${database} IS 'ukmesh-owned-fixture:${token}'`);
    await admin.query(`CREATE ROLE ${role} LOGIN NOINHERIT PASSWORD 'fixture-core-only'`);
    createdRole = true;
    const fixtureURL = new URL(adminURL);
    fixtureURL.pathname = '/' + database;
    fixture = new Pool({connectionString: fixtureURL.href, max: 1});
    await fixture.query(`
      REVOKE CREATE ON SCHEMA public FROM PUBLIC;
      CREATE SCHEMA mesh; CREATE SCHEMA core; CREATE SCHEMA ops;
      CREATE TABLE mesh.packets (
        time timestamptz, topic text, raw_hex text, snr real, rssi real, payload jsonb,
        observation_id uuid PRIMARY KEY, network text, rx_node_id text, src_node_id text,
        path_hashes text[], path_hash_size_bytes integer, visibility_ok boolean, is_private boolean
      );
      CREATE TABLE core.nodes (node_id text PRIMARY KEY, public_key text, name text);
      CREATE TABLE core.private_node_prefixes (
        node_id text, network text, prefix_size_bytes integer, prefix text
      );
      CREATE TABLE mesh.ingest_observation_ledger (
        seq bigserial PRIMARY KEY, observation_id uuid, observed_at timestamptz, network text
      );
      CREATE TABLE mesh.ingest_observation_ledger_coverage (singleton boolean, version integer);
      CREATE TABLE ops.public_visibility_state (singleton boolean, generation bigint);
      CREATE TABLE ops.packet_visibility_materialization_state (singleton boolean, visibility_generation bigint);
      CREATE TABLE ops.privacy_rematerialization_queue (node_id text, status text);
      INSERT INTO mesh.ingest_observation_ledger_coverage VALUES (true, 1);
      INSERT INTO ops.public_visibility_state VALUES (true, 1);
      INSERT INTO ops.packet_visibility_materialization_state VALUES (true, 1);
      CREATE VIEW public.packets AS SELECT * FROM mesh.packets;
      CREATE VIEW public.nodes AS SELECT * FROM core.nodes;
      CREATE VIEW public.private_node_prefixes AS SELECT * FROM core.private_node_prefixes;
    `);
    const valid = readFileSync(new URL('../../backend/src/db/migrations/051_reset_readiness.sql', import.meta.url), 'utf8')
      .match(/CREATE OR REPLACE FUNCTION meshcore_path_is_valid\([\s\S]*?\$\$;/)?.[0];
    const privatePath = readFileSync(new URL('../../backend/src/db/migrations/114_repair_private_prefix_functions.sql', import.meta.url), 'utf8')
      .match(/CREATE OR REPLACE FUNCTION public\.meshcore_path_matches_private\([\s\S]*?\$function\$;/)?.[0];
    assert.ok(valid && privatePath);
    await fixture.query(valid + '\n' + privatePath);
    await fixture.query(`
      INSERT INTO core.nodes VALUES
        (repeat('A',64),repeat('A',64),'public source'),
        (repeat('B',64),repeat('B',64),'private source'),
        (repeat('C',64),repeat('C',64),'public receiver'),
        (repeat('D',64),repeat('D',64),'private marker 🚫');
      INSERT INTO core.private_node_prefixes VALUES (repeat('B',64),'ukmesh',1,'BB');
      INSERT INTO mesh.packets VALUES
        (now(),'meshcore/LON/fixture/packets','1122',1,2,'{"direction":"rx","secret":"not-exported"}',
         '11111111-1111-4111-8111-111111111111','northeast',repeat('C',64),repeat('A',64),'{}',1,true,false),
        (now(),'meshcore/LON/private/packets','deadbeef',1,2,'{"direction":"rx","secret":"private"}',
         '22222222-2222-4222-8222-222222222222','northeast',repeat('C',64),repeat('B',64),'{}',1,true,false),
        (now(),'meshcore/LON/invalid/packets','c0ffee',1,2,'{"direction":"rx"}',
         '33333333-3333-4333-8333-333333333333','ukmesh',repeat('C',64),repeat('A',64),'{ABCD}',1,true,false);
      INSERT INTO mesh.ingest_observation_ledger (observation_id,observed_at,network)
        SELECT observation_id,time,network FROM mesh.packets ORDER BY observation_id;
    `);
    const provision = () => spawnSync('sg', ['docker', '-c',
      `docker exec -i ukmesh-sample-db psql -X -q -v ON_ERROR_STOP=1 -v corescope_reader_role=${role} -U meshcore -d ${database}`],
    {input: script, encoding: 'utf8', timeout: 15_000,
      env: {PATH: process.env.PATH, HOME: process.env.HOME, LANG: 'C.UTF-8'}});
    const apply = provision();
    assert.ifError(apply.error);
    assert.equal(apply.status, 0, apply.stderr);
    const readerURL = new URL(fixtureURL);
    readerURL.username = role;
    readerURL.password = 'fixture-core-only';
    const wrongURL = new URL(readerURL);
    wrongURL.password = 'wrong-fixture-password';
    const wrong = new Pool({connectionString: wrongURL.href, max: 1});
    try { await assert.rejects(wrong.query('SELECT 1'), {code: '28P01'}); }
    finally { await wrong.end(); }
    reader = new Pool({connectionString: readerURL.href, max: 1});
    assert.deepEqual((await reader.query('SELECT current_user, session_user')).rows,
      [{current_user: role, session_user: role}]);
    assert.equal((await reader.query(
      "SELECT to_regclass('packets') = 'corescope_public.packets'::regclass AS expected"
    )).rows[0].expected, true);
    const visible = await reader.query('SELECT raw_hex,payload FROM packets');
    assert.deepEqual(visible.rows, [{raw_hex: '1122', payload: {direction: 'rx'}}]);
    const plans = {};
    for (const [name, query] of Object.entries({
      packets: 'SELECT * FROM packets LIMIT 500',
      nodes: 'SELECT public_key,name FROM nodes LIMIT 500',
      ledger: `WITH candidates AS MATERIALIZED (
        SELECT observation_id, observed_at, seq FROM ingest_observation_ledger
        WHERE seq > 0 ORDER BY seq LIMIT 500
      ) SELECT p.time,p.raw_hex,c.seq FROM candidates c
        CROSS JOIN LATERAL (SELECT * FROM packets p
          WHERE p.observation_id=c.observation_id AND p.time=c.observed_at LIMIT 1) p
        WHERE EXISTS (SELECT 1 FROM ingest_observation_ledger_coverage WHERE singleton=true AND version=1)
        ORDER BY c.seq`,
    })) {
      const explained = await reader.query('EXPLAIN (ANALYZE,BUFFERS,VERBOSE,FORMAT JSON) ' + query);
      plans[name] = explained.rows[0]['QUERY PLAN'];
      let fencePlans = 0;
      const visit = node => {
        if (node['Subplan Name']?.startsWith('InitPlan') && node['Output']?.some(
          expression => expression.includes('privacy_ready')
        )) {
          fencePlans++;
          assert.ok(node['Actual Loops'] <= 1, 'privacy fence is evaluated once per statement');
        }
        for (const child of node.Plans ?? []) visit(child);
      };
      visit(plans[name][0].Plan);
      assert.ok(fencePlans > 0, 'privacy readiness remains a statement-level fence');
    }
    const evidence = new URL('../../plans/lean-runtime/', import.meta.url);
    mkdirSync(evidence, {recursive: true, mode: 0o700});
    writeFileSync(new URL('corescope-reader-plans.json', evidence), JSON.stringify(plans, null, 2) + '\n', {mode: 0o600});
    assert.deepEqual((await reader.query('SELECT * FROM packets')).fields.map(field => field.name), [
      'time', 'topic', 'raw_hex', 'snr', 'rssi', 'payload', 'observation_id', 'network',
      'rx_node_id', 'src_node_id', 'path_hashes', 'path_hash_size_bytes',
    ]);
    assert.deepEqual((await reader.query('SELECT name FROM nodes ORDER BY name')).rows.map(row => row.name),
      ['public receiver', 'public source']);
    assert.equal((await reader.query('SELECT * FROM ingest_observation_ledger')).rowCount, 1);
    const policy = await reader.query({text: policySQL, rowMode: 'array'});
    assert.deepEqual(policy.rows[0].slice(0, 3), ['1', '1', false]);
    assert.equal(policy.rows[0][3][0].node_id, 'B'.repeat(64));
    assert.deepEqual((await reader.query('SELECT * FROM private_node_prefixes')).fields.map(field => field.name),
      ['node_id', 'network', 'prefix_size_bytes', 'prefix']);
    const denied = [
      'SELECT * FROM mesh.packets', 'SELECT * FROM core.nodes',
      'SELECT * FROM core.private_node_prefixes', 'SELECT * FROM mesh.ingest_observation_ledger',
      'SELECT * FROM public.packets', 'SELECT * FROM public.nodes',
      'SELECT * FROM public.private_node_prefixes',
      'INSERT INTO packets DEFAULT VALUES', 'UPDATE private_node_prefixes SET prefix=\'AA\'',
      'CREATE TABLE corescope_public.forbidden (id integer)',
      'CREATE TABLE public.forbidden (id integer)', 'SET ROLE meshcore',
    ];
    for (const statement of denied) {
      await assert.rejects(reader.query(statement), {code: '42501'}, statement);
    }
    await fixture.query('UPDATE ops.packet_visibility_materialization_state SET visibility_generation=2');
    for (const relation of ['packets', 'nodes', 'ingest_observation_ledger']) {
      assert.equal((await reader.query(`SELECT * FROM ${relation}`)).rowCount, 0, 'stale ' + relation);
    }
    await fixture.query('UPDATE ops.packet_visibility_materialization_state SET visibility_generation=1');
    await fixture.query('TRUNCATE mesh.ingest_observation_ledger_coverage');
    assert.equal((await reader.query('SELECT * FROM packets')).rowCount, 0, 'incomplete historical coverage');
    await fixture.query('INSERT INTO mesh.ingest_observation_ledger_coverage VALUES (true, 1)');
    await fixture.query("INSERT INTO ops.privacy_rematerialization_queue VALUES ('sensitive-private-id','processing')");
    for (const status of ['pending', 'processing', 'failed']) {
      await fixture.query('UPDATE ops.privacy_rematerialization_queue SET status=$1', [status]);
      assert.equal((await reader.query('SELECT * FROM packets')).rowCount, 0, status + ' rematerialization');
      assert.deepEqual((await reader.query('SELECT * FROM privacy_rematerialization_queue')).rows, [{status}]);
    }
    await assert.rejects(reader.query('SELECT node_id FROM privacy_rematerialization_queue'), {code: '42703'});
    await fixture.query('TRUNCATE ops.privacy_rematerialization_queue');
    await fixture.query("INSERT INTO core.private_node_prefixes VALUES (repeat('A',64),'ukmesh',1,'AA')");
    await fixture.query('UPDATE ops.public_visibility_state SET generation=2; UPDATE ops.packet_visibility_materialization_state SET visibility_generation=2');
    assert.equal((await reader.query('SELECT * FROM packets')).rowCount, 0, 'current policy revokes frozen public flags');
    assert.equal((await reader.query('SELECT * FROM ingest_observation_ledger')).rowCount, 0);
    assert.deepEqual((await reader.query('SELECT name FROM nodes')).rows, [{name: 'public receiver'}]);
    assert.equal((await reader.query({text: policySQL, rowMode: 'array'})).rows[0][3].length, 2,
      'existing adapter receives complete revocation policy');
    await fixture.query(`GRANT SELECT (raw_hex) ON mesh.packets TO ${role}`);
    const rejected = provision();
    assert.equal(rejected.status, 3);
    assert.match(rejected.stderr, /raw relation privileges/,
      'provisioning rejects a credential that already has raw content access');
    await fixture.query(`REVOKE SELECT (raw_hex) ON mesh.packets FROM ${role}`);
  } finally {
    if (reader) await reader.end();
    if (fixture) await fixture.end();
    if (createdDatabase) await admin.query(`DROP DATABASE ${database} WITH (FORCE)`);
    if (createdRole) await admin.query(`DROP ROLE ${role}`);
    await admin.end();
  }
});
