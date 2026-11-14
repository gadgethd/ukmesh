#!/usr/bin/env node
// Actual TCP logins with rollback-only writes on an owned fixture database.
import assert from 'node:assert/strict';
import { createRequire } from 'node:module';
import { randomUUID } from 'node:crypto';
const { Pool } = createRequire(new URL('../../backend/package.json', import.meta.url))('pg');
const [database, ownerDatabase] = process.argv.slice(2);
const owned = /^lean_(?:verify|fixture|1x|10x|soak)_([a-z0-9_]{1,38})$/;
const token = owned.exec(database ?? '')?.[1];
if (!token || ownerDatabase !== 'lean_owner_' + token) {
  throw new Error('Permission writes require separate owned fixture databases');
}
const reports = [];
function pool(role, target = database, password = 'local-full-dev-only') {
  return new Pool({host: '127.0.0.1', port: 5433, database: target, user: role,
    password, max: 1, connectionTimeoutMillis: 3000, statement_timeout: 10000,
    application_name: 'ukmesh-owned-permission-proof'});
}
async function allowed(role, name, work, target = database) {
  const connection = pool(role, target);
  try {
    const client = await connection.connect();
    try {
      const { rows } = await client.query('SELECT session_user AS username');
      assert.equal(rows[0].username, role);
      const attributes = await client.query(`SELECT rolsuper,rolcreatedb,rolcreaterole,
        rolreplication,rolbypassrls,rolinherit FROM pg_roles WHERE rolname=current_user`);
      assert.ok(Object.values(attributes.rows[0]).every(value=>value===false));
      await client.query('BEGIN');
      try { await work(client); } finally { await client.query('ROLLBACK'); }
    } finally { client.release(); }
    reports.push({role, target, name, allowed: true, passed: true});
  } finally { await connection.end(); }
}
async function denied(role, name, query, target = database, password = 'local-full-dev-only', code = '42501') {
  const connection = pool(role, target, password);
  try {
    await assert.rejects(async () => {
      const client = await connection.connect();
      try {
        await client.query('BEGIN');
        await client.query(query);
      } finally {
        try { await client.query('ROLLBACK'); }
        finally { client.release(); }
      }
    }, error => error.code === code);
    reports.push({role, target, name, allowed: false, expected_code: code, passed: true});
  } finally { await connection.end(); }
}

try {
  let inactive;
  for (const [role, target] of [['meshcore_api', database], ['meshcore_owner', ownerDatabase]]) {
    const connection = pool(role, target);
    try {
      const {rows:[identity]} = await connection.query(`SELECT current_database() AS database,
        shobj_description(oid,'pg_database') AS marker FROM pg_database WHERE datname=current_database()`);
      assert.equal(identity.database, target);
      assert.equal(identity.marker, 'ukmesh-owned-fixture:' + token);
      if (target === database) {
        const {rows:[retained]} = await connection.query(`SELECT datname FROM pg_database
          WHERE datname ~ '^lean_(fixture|1x|10x|soak)_[a-z0-9_]{1,38}$'
            AND datname<>current_database()
            AND shobj_description(oid,'pg_database') = 'ukmesh-owned-fixture:' ||
              regexp_replace(datname,'^lean_(fixture|1x|10x|soak)_','')
          ORDER BY datname LIMIT 1`);
        assert.ok(retained, 'a retained inactive owned fixture is required for cross-database proof');
        inactive = retained.datname;
      }
    } finally { await connection.end(); }
  }
  await allowed('meshcore_api', 'read public API inputs and write bounded UI state', async db => {
    await db.query('SELECT node_id FROM core.nodes LIMIT 1');
    await db.query('SELECT packet_hash FROM mesh.packets LIMIT 1');
    await db.query("INSERT INTO ops.frontend_error_events(kind,message) VALUES('permission-proof','owned rollback fixture')");
    await db.query("INSERT INTO core.owner_public_feed_settings(owner_username,enabled) VALUES($1,false)", ['proof-' + randomUUID()]);
    await db.query('UPDATE core.nodes SET last_predicted_online_at=last_predicted_online_at WHERE false');
    await db.query('SELECT generation FROM ops.public_visibility_state WHERE singleton=true FOR SHARE');
  });
  await allowed('meshcore_ingest', 'write nodes, privacy triggers, packet facts and ledger guards', async db => {
    const node = randomUUID().replaceAll('-','').toUpperCase().padEnd(64,'0');
    const observation = randomUUID(), hash = randomUUID();
    await db.query("INSERT INTO core.nodes(node_id,name,network) VALUES($1,'owned permission proof','test')", [node]);
    await db.query("INSERT INTO mesh.packets(time,packet_hash,rx_node_id,topic,network,path_hashes,path_hash_size_bytes,observation_id) VALUES(now(),$1,$2,'owned/proof','test',ARRAY['FFFF'],2,$3)", [hash,node,observation]);
    await db.query("INSERT INTO mesh.packet_paths(time,packet_hash,rx_node_id,topic,network,path_hashes,path_hash_size_bytes,observation_id) VALUES(now(),$1,$2,'owned/proof','test',ARRAY['FFFF'],2,$3)", [hash,node,observation]);
    await db.query("INSERT INTO mesh.ingest_observation_ledger(observation_id,observed_at,packet_hash,network) VALUES($1,now(),$2,'test')", [observation,hash]);
    const facts = await db.query('SELECT COUNT(*)::int AS count FROM mesh.multibyte_path_facts WHERE observation_id=$1', [observation]);
    assert.equal(facts.rows[0].count,1);
    await db.query("UPDATE core.nodes SET name='owned private 🚫' WHERE node_id=$1", [node]);
    const prefixes = await db.query('SELECT COUNT(*)::int AS count FROM core.private_node_prefixes WHERE node_id=$1', [node]);
    assert.equal(prefixes.rows[0].count,3);
  });
  await allowed('meshcore_worker', 'worker status and native classification state', async db => {
    await db.query('CREATE TEMP TABLE owned_learning_stage(network text) ON COMMIT DROP');
    await db.query("INSERT INTO owned_learning_stage VALUES('owned-proof')");
    assert.equal((await db.query('SELECT network FROM owned_learning_stage')).rows[0].network,'owned-proof');
    await db.query('SELECT packet_hash FROM mesh.packets LIMIT 1');
    await db.query("UPDATE ops.tagger_state SET updated_at=now() WHERE id=1");
    await db.query("INSERT INTO ops.operational_check_results(check_name,status,latency_ms) VALUES('owned-proof','ok',1)");
    await db.query('DELETE FROM core.region_scope_stats WHERE false');
    await db.query('UPDATE mesh.packets SET visibility_ok=visibility_ok WHERE false');
    await db.query('SELECT generation FROM ops.public_visibility_state WHERE singleton=true FOR UPDATE');
  });
  await allowed('meshcore_worker', 'analytics backfill upserts derived facts and coverage', async db => {
    const observation = randomUUID(), hash = randomUUID();
    const {rows:[{generation}]} = await db.query('SELECT generation FROM ops.public_visibility_state WHERE singleton=true');
    const insert = `INSERT INTO mesh.multibyte_path_facts(observation_id,observed_at,packet_hash,network,
      topic,path_hashes,path_hash_size_bytes,visibility_ok,is_private,visibility_generation,fully_decoded)
      VALUES($1,now(),$2,'test',$3,ARRAY['FFFF'],2,true,false,$4,false)
      ON CONFLICT(observation_id) DO UPDATE SET topic=EXCLUDED.topic RETURNING topic`;
    await db.query(insert, [observation,hash,'owned/proof/initial',generation]);
    const {rows:[fact]} = await db.query(insert, [observation,hash,'owned/proof/updated',generation]);
    assert.equal(fact.topic,'owned/proof/updated');
    const {rows:[state]} = await db.query(`INSERT INTO mesh.multibyte_path_fact_state
      (singleton,visibility_generation,covered_from,covered_through,row_count)
      VALUES(true,$1,now(),now(),1)
      ON CONFLICT(singleton) DO UPDATE SET updated_at=EXCLUDED.updated_at RETURNING singleton`,[generation]);
    assert.equal(state.singleton,true);
  });
  await allowed('meshcore_worker', 'analytics publishes generation-fenced chart snapshots', async db => {
    const scope = 'owned-proof-'+randomUUID();
    const {rows:[{generation}]} = await db.query('SELECT generation FROM ops.public_visibility_state WHERE singleton=true');
    const publish = `INSERT INTO analytics.stats_chart_snapshots
      (scope_key,schema_version,visibility_generation,generated_at,payload)
      SELECT $1,2,$2,now(),$3::jsonb FROM ops.public_visibility_state WHERE singleton=true AND generation=$2
      ON CONFLICT(scope_key) DO UPDATE SET payload=EXCLUDED.payload RETURNING payload`;
    await db.query(publish,[scope,generation,JSON.stringify({proof:'initial'})]);
    const {rows:[saved]} = await db.query(publish,[scope,generation,JSON.stringify({proof:'confirmed'})]);
    assert.equal(saved.payload.proof,'confirmed');
  });
  await allowed('meshcore_api', 'spam analysis starts, renews and completes a fenced lease', async db => {
    const scope = 'owned-proof-'+randomUUID(), run = randomUUID(), lease = randomUUID();
    const {rows:[{generation}]} = await db.query('SELECT generation FROM ops.public_visibility_state WHERE singleton=true');
    await db.query("INSERT INTO analytics.analysis_workload_state(workload,scope) VALUES('spam-analysis',$1)",[scope]);
    await db.query("SELECT active_run_id FROM analytics.analysis_workload_state WHERE workload='spam-analysis' AND scope=$1 FOR UPDATE",[scope]);
    await db.query(`INSERT INTO analytics.analysis_runs(run_id,workload,scope,status,window_start,window_end,
      lease_owner,lease_token,lease_expires_at,run_deadline_at,privacy_generation,model_generation)
      VALUES($1,'spam-analysis',$2,'running',now(),now(),'owned-proof',$3,now()+interval '1 minute',
        now()+interval '2 minutes',$4,'spam-analysis-v2')`,[run,scope,lease,generation]);
    await db.query(`UPDATE analytics.analysis_workload_state SET active_run_id=$1,active_lease_token=$2,
      active_lease_expires_at=now()+interval '1 minute',active_run_deadline_at=now()+interval '2 minutes'
      WHERE workload='spam-analysis' AND scope=$3`,[run,lease,scope]);
    const {rows} = await db.query(`UPDATE analytics.analysis_runs AS run SET heartbeat_at=clock_timestamp()
      FROM analytics.analysis_workload_state AS state WHERE run.run_id=state.active_run_id
      AND state.workload='spam-analysis' AND state.scope=$1 AND run.lease_token=$2
      AND state.active_lease_token=$2 AND run.privacy_generation=$3 RETURNING run.run_id`,[scope,lease,generation]);
    assert.equal(rows[0]?.run_id,run);
    await db.query("UPDATE analytics.analysis_runs SET status='complete',completed_at=now() WHERE run_id=$1",[run]);
    await db.query(`UPDATE analytics.analysis_workload_state SET active_run_id=NULL,last_status='complete'
      WHERE workload='spam-analysis' AND scope=$1`,[scope]);
    assert.equal((await db.query("SELECT last_status FROM analytics.analysis_workload_state WHERE workload='spam-analysis' AND scope=$1",[scope])).rows[0].last_status,'complete');
  });
  await allowed('meshcore_owner', 'create session and owner account in isolated auth store', async db => {
    const username = 'proof-' + randomUUID();
    await db.query('INSERT INTO core.owner_accounts(mqtt_username) VALUES($1)', [username]);
    await db.query("INSERT INTO owner_sessions(session_id,mqtt_username,expires_at) VALUES($1,$2,now()+interval '1 minute')", [randomUUID(),username]);
    await db.query('SELECT mqtt_username FROM owner_accounts WHERE mqtt_username=$1', [username]);
  }, ownerDatabase);
  await allowed('meshcore_monitor', 'monitor statistics without measurement rows', async db => {
    await db.query('SELECT numbackends FROM pg_stat_database WHERE datname=current_database()');
    assert.equal((await db.query("SELECT pg_has_role(current_user,'pg_monitor','USAGE') AS permitted")).rows[0].permitted,true);
  });
  await allowed('meshcore_migrate', 'additive DDL under migration credential only', async db => {
    await db.query('CREATE TABLE ops.owned_permission_probe(id integer)');
  });
  await allowed('corescope_ro', 'CoreScope reader actual login and public policy read', async db => {
    assert.equal((await db.query('SELECT count(*)::int AS count FROM corescope_public.public_visibility_state')).rows[0].count, 1);
  });
  for (const role of ['meshcore_api','meshcore_ingest','meshcore_worker']) {
    await denied(role,'runtime cannot create schema objects','CREATE TABLE public.owned_permission_probe(id integer)');
    if (role !== 'meshcore_worker') {
      await denied(role,'runtime cannot create temporary objects','CREATE TEMP TABLE owned_permission_probe(id integer)');
    }
    await denied(role,'runtime cannot assume migration role','SET ROLE meshcore_migrate');
    await denied(role,'runtime cannot read private policy table','SELECT * FROM ops.node_privacy LIMIT 0');
    await denied(role,'runtime cannot enter owner store','SELECT 1',ownerDatabase);
  }
  await denied('meshcore_api','API cannot write packet observations',"INSERT INTO mesh.packets(packet_hash,topic) VALUES('owned-probe','owned/proof')");
  await denied('meshcore_api','API cannot change node consent',"UPDATE core.nodes SET name=name WHERE false");
  await denied('meshcore_api','API cannot write derived analytics facts',
    'INSERT INTO mesh.multibyte_path_facts SELECT * FROM mesh.multibyte_path_facts WHERE false');
  await denied('meshcore_api','spam lease capability cannot delete analysis history',
    'DELETE FROM analytics.analysis_runs WHERE false');
  await denied('meshcore_worker','analytics fact backfill cannot delete observation facts',
    'DELETE FROM mesh.multibyte_path_facts WHERE false');
  await denied('meshcore_ingest','ingest cannot write owner forwarding policy',"UPDATE core.owner_public_feed_settings SET enabled=true WHERE false");
  await denied('meshcore_owner','owner cannot enter measurement store','SELECT 1');
  await denied('meshcore_monitor','monitor cannot read raw packets','SELECT * FROM mesh.packets LIMIT 0');
  for (const role of ['meshcore_api','meshcore_ingest','meshcore_worker','meshcore_owner','corescope_ro']) {
    await denied(role,'runtime cannot enter retained inactive fixture','SELECT 1',inactive);
    await denied(role,'runtime cannot enter system database','SELECT 1','postgres');
  }
  await denied('meshcore_api','wrong password fails actual login','SELECT 1',database,'wrong-owned-fixture-password','28P01');
  console.log(JSON.stringify({passed:true,at:new Date().toISOString(),
    targets:{main:database,owner:ownerDatabase,inactive},checks:reports},null,2));
} catch (error) {
  console.error(JSON.stringify({passed:false,code:error.code ?? error.name,checks:reports},null,2));
  process.exitCode=1;
}
