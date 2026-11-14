#!/usr/bin/env node
// Native privilege and maintenance proof against the selected owned fixture.
import assert from 'node:assert/strict';
import { createRequire } from 'node:module';
import { randomUUID } from 'node:crypto';
import { execFileSync } from 'node:child_process';
import { fileURLToPath } from 'node:url';
const { Pool } = createRequire(new URL('../../backend/package.json',import.meta.url))('pg');
const [database] = process.argv.slice(2);
if(!/^lean_beacon_[a-z0-9_]{1,38}$/.test(database??'')) throw new Error('Owned Beacon fixture required');
const settings={host:'127.0.0.1',port:5435,database,max:1,connectionTimeoutMillis:3000,
  statement_timeout:15000,application_name:'ukmesh-owned-beacon-permission-proof'};
const admin=new Pool({...settings,user:'beacon_dev',password:'local-companion-only'});
const runtime=new Pool({...settings,user:'beacon_runtime',password:'local-full-dev-only'});
const maint=new Pool({...settings,user:'beacon_maint',password:'local-full-dev-only'});
const records=[];
const probe='owned_acl_'+randomUUID().replaceAll('-','');
let created=false;
async function check(name,work) {
  try { await work(); records.push({name,passed:true}); }
  catch(error) { records.push({name,passed:false,code:error.code??error.name,message:error.message}); }
}
async function denied(connection,statement) {
  const client=await connection.connect();
  try {
    await client.query('BEGIN');
    try { await assert.rejects(()=>client.query(statement),{code:'42501'}); }
    finally { await client.query('ROLLBACK'); }
  } finally { client.release(); }
}
try {
  const marker=await admin.query("SELECT shobj_description(oid,'pg_database') AS marker FROM pg_database WHERE datname=current_database()");
  assert.equal(marker.rows[0]?.marker,'ukmesh-owned-fixture:'+database.slice('lean_beacon_'.length));
  await admin.query('CREATE TABLE public.'+probe+'(id integer)'); created=true;
  // Reapplying the real helper must not expose an unrelated table that appeared
  // after its previous run. This distinguishes a named list from ALL TABLES.
  execFileSync('sg',['docker','-c','bash scripts/companion-dev/beacon-access.sh '+database],{
    cwd:fileURLToPath(new URL('../..',import.meta.url)),stdio:'pipe',timeout:30000,
    env:{PATH:process.env.PATH,HOME:process.env.HOME,LANG:'C.UTF-8'},
  });
  const {rows:[metadata]}=await admin.query('SELECT count(*)::int AS count FROM schema_migrations JOIN schema_migration_integrity USING(filename)');
  assert.ok(metadata.count>0,'operator metadata must be populated');
  records.push({name:'named grants reject newly introduced unrelated relation',passed:true});
  for(const [name,connection] of [['beacon_runtime',runtime],['beacon_maint',maint]]) {
    await check(name+' actual login and unprivileged role attributes',async()=>{
      const {rows:[row]}=await connection.query(`SELECT session_user AS username,current_schema() AS schema,
        rolsuper,rolcreatedb,rolcreaterole,rolreplication,rolbypassrls,rolinherit
        FROM pg_roles WHERE rolname=current_user`);
      assert.equal(row.username,name); assert.equal(row.schema,'public');
      for(const key of ['rolsuper','rolcreatedb','rolcreaterole','rolreplication','rolbypassrls','rolinherit']) assert.equal(row[key],false,key);
    });
    await check(name+' SELECT-only canonical migration metadata',async()=>{
      const {rows:[row]}=await connection.query('SELECT count(*)::int AS count FROM schema_migrations JOIN schema_migration_integrity USING(filename)');
      assert.equal(row.count,metadata.count);
    });
    for(const [label,statement] of [
      ['raw table ownership denied','ALTER TABLE public.packets ADD COLUMN owned_forbidden boolean'],
      ['persistent schema DDL denied','CREATE TABLE public.owned_forbidden(id integer)'],
      ['schema creation denied','CREATE SCHEMA owned_forbidden'],
      ['migration ledger writes denied','UPDATE schema_migrations SET applied_at=applied_at WHERE false'],
      ['migration checksum writes denied','UPDATE schema_migration_integrity SET checksum_sha256=checksum_sha256 WHERE false'],
      ['unrelated relation reads denied','SELECT * FROM public.'+probe],
      ['unrelated relation writes denied','INSERT INTO public.'+probe+' VALUES(1)'],
    ]) await check(name+' '+label,()=>denied(connection,statement));
    await check(name+' database creation denied',async()=>{
      // CREATE DATABASE cannot run in a transaction. A unique owned name keeps
      // cleanup bounded if this negative control ever discovers a regression.
      try {
        await assert.rejects(()=>connection.query('CREATE DATABASE '+probe),{code:'42501'});
      } finally {
        const {rows:[row]}=await admin.query('SELECT EXISTS(SELECT 1 FROM pg_database WHERE datname=$1) AS found',[probe]);
        if(row.found) await admin.query('DROP DATABASE '+probe);
      }
    });
    const targets=await admin.query('SELECT datname FROM pg_database WHERE datallowconn AND datname<>current_database()');
    for(const {datname} of targets.rows) await check(name+' cross-database CONNECT denied: '+datname,async()=>{
      const other=new Pool({...settings,database:datname,user:name,password:'local-full-dev-only'});
      try {await assert.rejects(()=>other.query('SELECT 1'),{code:'42501'});}
      finally {await other.end();}
    });
  }
  await check('runtime owns no business relation',async()=>{
    const {rows}=await runtime.query(`SELECT c.relname FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
      WHERE n.nspname='public' AND c.relkind IN ('r','v','m') AND c.relowner=current_user::regrole`);
    assert.equal(rows.length,0);
  });
  await check('maintenance ownership bounded to three derived relations',async()=>{
    const {rows}=await maint.query(`SELECT c.relname FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
      WHERE n.nspname='public' AND c.relkind IN ('r','v','m') AND c.relowner=current_user::regrole ORDER BY c.relname`);
    assert.deepEqual(rows.map(row=>row.relname),['channel_message_iatas','mv_radio_presets','packet_sites']);
  });
  await check('maintenance sees cross-role transaction statistics',async()=>{
    assert.equal((await maint.query("SELECT pg_has_role(current_user,'pg_read_all_stats','USAGE') AS ok")).rows[0].ok,true);
    await maint.query('SELECT backend_xid,xact_start FROM pg_stat_activity WHERE datname=current_database()');
  });
  await check('maintenance cannot assume statistics role',()=>denied(maint,'SET ROLE pg_read_all_stats'));
  await check('maintenance temporary rollup staging works',async()=>{
    const client=await maint.connect();
    try {await client.query('BEGIN');await client.query('CREATE TEMP TABLE owned_rollup_stage(id integer) ON COMMIT DROP');}
    finally {try {await client.query('ROLLBACK');}finally{client.release();}}
  });
  await check('runtime temporary DDL denied',()=>denied(runtime,'CREATE TEMP TABLE owned_forbidden(id integer)'));
  for(const relation of ['packet_sites','channel_message_iatas']) await check('maintenance VACUUM ANALYZE '+relation+' performs work without permission notices',async()=>{
    const client=await maint.connect(), notices=[];
    const notice=value=>notices.push(value);
    client.on('notice',notice);
    try {
      await client.query('VACUUM (ANALYZE) public.'+relation);
      assert.equal(notices.filter(value=>['WARNING','ERROR'].includes(value.severity)).length,0);
    }finally{client.off('notice',notice);client.release();}
  });
  await check('maintenance refreshes derived radio presets concurrently',()=>maint.query('REFRESH MATERIALIZED VIEW CONCURRENTLY public.mv_radio_presets'));
  await check('wrong runtime password is rejected',async()=>{
    const wrong=new Pool({...settings,user:'beacon_runtime',password:'wrong-owned-password'});
    try {await assert.rejects(()=>wrong.query('SELECT 1'),{code:'28P01'});}
    finally {await wrong.end();}
  });
} finally {
  try {if(created) await admin.query('DROP TABLE public.'+probe);}
  finally {await Promise.all([admin.end(),runtime.end(),maint.end()]);}
}
const passed=records.every(record=>record.passed);
console.log(JSON.stringify({passed,checks:records},null,2));
if(!passed) process.exitCode=1;
