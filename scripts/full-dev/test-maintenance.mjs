#!/usr/bin/env node
// Verify compressed-boundary cleanup through an actual nonowner TCP login.
import assert from 'node:assert/strict';
import { createRequire } from 'node:module';
import { randomUUID } from 'node:crypto';
const { Pool } = createRequire(new URL('../../backend/package.json', import.meta.url))('pg');
const [database] = process.argv.slice(2);
if (!/^lean_(?:verify|fixture)_[a-z0-9_]{1,38}$/.test(database ?? '')) {
  throw new Error('Maintenance proof requires an owned small fixture database');
}
const settings = {host:'127.0.0.1',port:5433,database,max:1,
  connectionTimeoutMillis:3000,statement_timeout:30000,application_name:'ukmesh-owned-maintenance-proof'};
const admin = new Pool({...settings,user:'meshcore',password:'meshcore'});
const worker = new Pool({...settings,user:'meshcore_worker',password:'local-full-dev-only'});
const api = new Pool({...settings,user:'meshcore_api',password:'local-full-dev-only'});
const prefix = 'owned-maint-' + randomUUID();
const oldHash = prefix + '-expired', youngHash = prefix + '-retained';
let chunk, originallyCompressed, seeded = false, passed = false;
async function forbidden(connection, statement) {
  await connection.query('BEGIN');
  try { await assert.rejects(() => connection.query(statement),{code:'42501'}); }
  finally { await connection.query('ROLLBACK'); }
}
try {
  const identity = await admin.query("SELECT shobj_description(oid,'pg_database') AS marker FROM pg_database WHERE datname=current_database()");
  assert.match(identity.rows[0]?.marker ?? '', /^ukmesh-owned-fixture:/);
  const {rows:[count]} = await admin.query('SELECT COUNT(*)::int AS n FROM mesh.packets');
  assert.ok(count.n <= 10_000, 'Use the small sample fixture for compressed-boundary proof');
  const metadata = await admin.query(`SELECT p.prosecdef, p.proconfig,
    pg_get_userbyid(p.proowner) AS owner,
    has_function_privilege('meshcore_api',p.oid,'EXECUTE') AS api_exec,
    has_function_privilege('meshcore_worker',p.oid,'EXECUTE') AS worker_exec
    FROM pg_proc p WHERE p.oid='ops.prune_expired_hypertable(regclass,interval)'::regprocedure`);
  assert.equal(metadata.rows[0].prosecdef,true);
  assert.deepEqual(metadata.rows[0].proconfig,['search_path=pg_catalog']);
  assert.notEqual(metadata.rows[0].owner,'meshcore_worker');
  assert.equal(metadata.rows[0].api_exec,false);
  assert.equal(metadata.rows[0].worker_exec,true);
  await forbidden(api,"SELECT ops.prune_expired_hypertable('mesh.packets','30 days')");
  await forbidden(worker,"SELECT ops.prune_expired_hypertable('mesh.packet_paths','30 days')");
  await forbidden(worker,"SELECT ops.prune_expired_hypertable('core.nodes','30 days')");
  await forbidden(worker,"SELECT ops.prune_expired_hypertable('mesh.packets','1 day')");
  await forbidden(worker,"SELECT ops.prune_expired_hypertable('mesh.packets','1 month')");
  const calendar = await admin.query(`SELECT instant,
      instant-interval '1 month' > instant-interval '30 days' AS actually_shorter
    FROM (VALUES ('2027-01-14'::timestamptz),('2027-02-14'::timestamptz),
      ('2027-03-14'::timestamptz)) AS dates(instant)`);
  assert.deepEqual(calendar.rows.map(row=>row.actually_shorter),[false,false,true]);
  await forbidden(worker,'ALTER TABLE mesh.packets ADD COLUMN owned_forbidden integer');

  const {rows:[clock]} = await admin.query("SELECT now()-interval '30 days' AS cutoff");
  const old = new Date(clock.cutoff.getTime()-60_000), young = new Date(clock.cutoff.getTime()+60_000);
  await admin.query('BEGIN');
  try {
    await admin.query(`INSERT INTO mesh.packets(time,packet_hash,topic,network)
      VALUES($1,$2,'owned/maintenance','test'),($3,$4,'owned/maintenance','test')`, [old,oldHash,young,youngHash]);
    await admin.query(`INSERT INTO mesh.packet_paths(time,packet_hash,topic,network)
      VALUES($1,$2,'owned/maintenance','test')`, [old,oldHash]);
    await admin.query('COMMIT'); seeded=true;
  } catch(error) { await admin.query('ROLLBACK'); throw error; }
  const chunks = await admin.query(`SELECT format('%I.%I',chunk_schema,chunk_name) AS relation,
      is_compressed FROM timescaledb_information.chunks
    WHERE hypertable_schema='mesh' AND hypertable_name='packets'
      AND range_start <= $1 AND range_end > $2`, [old,young]);
  assert.equal(chunks.rows.length,1,'Both sides of the cutoff must share a boundary chunk');
  chunk=chunks.rows[0].relation; originallyCompressed=chunks.rows[0].is_compressed;
  await admin.query('SELECT compress_chunk($1::regclass,if_not_compressed=>true)',[chunk]);
  await worker.query('BEGIN');
  try {
    const login = await worker.query('SELECT session_user AS username');
    assert.equal(login.rows[0].username,'meshcore_worker');
    await worker.query("SELECT ops.prune_expired_hypertable('mesh.packets','30 days')");
    const rows = await worker.query('SELECT packet_hash FROM mesh.packets WHERE packet_hash=ANY($1::text[])',[ [oldHash,youngHash] ]);
    assert.deepEqual(rows.rows.map(row=>row.packet_hash),[youngHash]);
    const paths = await worker.query('SELECT COUNT(*)::int AS n FROM mesh.packet_paths WHERE packet_hash=$1',[oldHash]);
    assert.equal(paths.rows[0].n,1,'Raw-path history must survive raw-packet retention');
    const compression = await worker.query(`SELECT is_compressed FROM timescaledb_information.chunks
      WHERE format('%I.%I',chunk_schema,chunk_name)=$1`,[chunk]);
    assert.equal(compression.rows[0]?.is_compressed,true);
    await worker.query("SELECT ops.prune_expired_hypertable('core.node_status_samples','30 days')");
  } finally { await worker.query('ROLLBACK'); }
  passed=true;
} catch(error) {
  console.error(JSON.stringify({passed:false,code:error.code??error.name,message:error.message}));
  process.exitCode=1;
} finally {
  try {
    if(seeded) {
      if(chunk) await admin.query('SELECT decompress_chunk($1::regclass,if_compressed=>true)',[chunk]);
      await admin.query('DELETE FROM mesh.packets WHERE packet_hash=ANY($1::text[])',[[oldHash,youngHash]]);
      await admin.query('DELETE FROM mesh.packet_paths WHERE packet_hash=$1',[oldHash]);
      if(chunk&&originallyCompressed) await admin.query('SELECT compress_chunk($1::regclass,if_not_compressed=>true)',[chunk]);
    }
  } catch(error) {
    console.error(JSON.stringify({passed:false,stage:'fixture-cleanup',code:error.code??error.name}));
    process.exitCode=1;
  } finally { await Promise.all([admin.end(),worker.end(),api.end()]); }
}
if(passed&&!process.exitCode) {
  console.log(JSON.stringify({passed:true,checks:['actual-nonowner-login','fixed-definer-search-path',
    'API-execute-denied','unrelated-targets-denied','short-retention-denied',
    'calendar-month-retention-denied','calendar-boundaries-verified','ownership-denied',
    'compressed-boundary-expired-removed','retained-side-preserved','raw-path-history-preserved',
    'compression-restored','second-automatic-target-permitted',
    'rollback-protected-fixture','seed-cleanup-complete']}));
}
