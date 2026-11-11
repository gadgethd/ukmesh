import assert from 'node:assert/strict';
import fs from 'node:fs';
import test from 'node:test';
import pg from 'pg';
import { refreshNodeIdentityAliases } from './nodeIdentity.js';

const endpoint = process.env.TEST_INGEST_DATABASE_URL;

test('identity refresh publishes a validated generation and reuses only fresh, matching results', {skip: endpoint ? false : 'owned PostgreSQL required', timeout: 30000}, async (t) => {
  assert.ok(['localhost', '127.0.0.1'].includes(new URL(endpoint!).hostname));
  const schema = `identity_refresh_${process.pid}`;
  const admin = new pg.Pool({connectionString: endpoint});
  await admin.query(`CREATE SCHEMA ${schema}`);
  const pool = new pg.Pool({connectionString: endpoint, options: `-c search_path=${schema},public`, max: 2});
  t.after(async () => {await pool.end(); await admin.query(`DROP SCHEMA ${schema} CASCADE`); await admin.end();});
  await pool.query(`CREATE TABLE public_visibility_state(singleton boolean primary key, generation bigint); INSERT INTO public_visibility_state VALUES(true,1);
    CREATE TABLE nodes(node_id text,name text,lat float,lon float,role int,advert_count int,last_seen timestamptz,network text,hardware_model text,firmware_version text);
    CREATE TABLE node_status_samples(node_id text,time timestamptz);
    CREATE TABLE packets(rx_node_id text,src_node_id text,packet_type int,time timestamptz);
    CREATE TABLE node_identity_aliases(source_node_id text primary key,canonical_node_id text,confidence text,reason text,evidence jsonb,source_kind text,updated_at timestamptz);
    CREATE TABLE node_identity_match_evidence(node_a_id text,node_b_id text,decision text,confidence text,reason text,evidence jsonb,source_kind text,updated_at timestamptz,score int,primary key(node_a_id,node_b_id));`);
  const migration = fs.readFileSync(new URL('./migrations/069_node_identity_refresh_state.sql', import.meta.url), 'utf8');
  await pool.query('BEGIN'); await pool.query(migration); await pool.query('COMMIT');
  await pool.query(`INSERT INTO nodes(node_id,name,role,network) VALUES($1,'Unambiguous fixture',2,'ukmesh')`,['A'.repeat(64)]);
  const first = await refreshNodeIdentityAliases(pool, {maxAgeMs:1800000});
  assert.equal(first.nodesConsidered,1); assert.equal(first.reusedRecentRefresh,undefined);
  const second = await refreshNodeIdentityAliases(pool, {maxAgeMs:1800000});
  assert.equal(second.reusedRecentRefresh,true);
  await pool.query('UPDATE public_visibility_state SET generation=2');
  const changed = await refreshNodeIdentityAliases(pool, {maxAgeMs:1800000});
  assert.equal(changed.reusedRecentRefresh,undefined);
  // A privacy transition after the read snapshot must reject publication.
  let changedDuringRead=false;
  const intercepted={connect:async()=>{const client=await pool.connect();return {query:async (sql:string,params?:unknown[])=>{
    const result=await client.query(sql,params);
    if(sql.includes('FROM nodes')&&!changedDuringRead){changedDuringRead=true;await pool.query('UPDATE public_visibility_state SET generation=3');}
    return result;
  },release:(destroy?:boolean)=>client.release(destroy)};}};
  await assert.rejects(refreshNodeIdentityAliases(intercepted),/visibility changed/);
  assert.equal((await pool.query('SELECT visibility_generation::text generation FROM node_identity_refresh_state')).rows[0].generation,'2');
  assert.equal((await pool.query("SELECT COUNT(*)::int n FROM pg_locks WHERE locktype='advisory' AND pid<>pg_backend_pid() AND objid = ((hashtext('meshcore-node-identity-refresh')::bigint & 4294967295)::oid) AND objsubid=1 AND database=(SELECT oid FROM pg_database WHERE datname=current_database())")).rows[0].n,0);
});
