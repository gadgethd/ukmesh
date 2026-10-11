import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import os from 'node:os';
import net from 'node:net';
import { spawn, type ChildProcess } from 'node:child_process';
import { once } from 'node:events';
import test from 'node:test';
import pg from 'pg';
import { Redis } from 'ioredis';
import { runMigrations } from '../db/migrations.js';
import { configurePacketBatch } from '../db/packetBatch.js';
import { admitPacketToRam, configureRamStore } from './liveStore.js';
import { accountedRamEntryBytes } from './protocol.js';

const pgURL=process.env['TEST_INGEST_DATABASE_URL'], redisURL=process.env['TEST_REDIS_URL'];
const local=(url?:string)=>url&&['localhost','127.0.0.1'].includes(new URL(url).hostname);
const sleep=(ms:number)=>new Promise(resolve=>setTimeout(resolve,ms));
async function until(check:()=>Promise<boolean>|boolean, timeout=20_000, label='boundary') {
  const end=Date.now()+timeout;
  while(Date.now()<end){if(await check())return;await sleep(50);}
  throw new Error(`Timed out waiting for ${label}`);
}
async function port() {
  const server=net.createServer();server.listen(0,'127.0.0.1');await once(server,'listening');
  const address=server.address();assert.ok(address&&typeof address!=='string');
  await new Promise<void>((resolve,reject)=>server.close(error=>error?reject(error):resolve()));
  return address.port;
}

test('F15/F16 actual SIGTERM joins held PostgreSQL persistence, recovers identities and drains before durable rollback', {
  skip:local(pgURL)&&local(redisURL)?false:'owned loopback PostgreSQL and Redis fixtures required', timeout:230_000,
},async t=>{
  const admin=new pg.Pool({connectionString:pgURL,max:1});
  const name=`meshcore_runtime_${process.pid}_${Date.now()}`;
  await admin.query(`CREATE DATABASE "${name}"`);
  const url=new URL(pgURL!);url.pathname='/'+name;
  const pool=new pg.Pool({connectionString:url.toString(),max:3});
  const redis=new Redis(redisURL!,{lazyConnect:true,enableOfflineQueue:false});
  const key=`meshcore:acl:runtime:${name}`,group='owned-runtime';
  const files=fs.mkdtempSync(path.join(os.tmpdir(),'meshcore-runtime-'));
  fs.writeFileSync(path.join(files,'acl'),'user backend\ntopic readwrite meshcore/#\n',{mode:0o600});
  const children:ChildProcess[]=[];
  const old=Object.fromEntries(['MQTT_INGEST_MODE','MQTT_RAM_STREAM_KEY','MQTT_RAM_QUEUE_MAX_BYTES'].map(name=>[name,process.env[name]]));
  let holder:pg.PoolClient|undefined;
  t.after(async()=>{
    for(const child of children)if(child.exitCode===null&&child.signalCode===null){child.kill('SIGKILL');await once(child,'exit').catch(()=>undefined);}
    if(holder){await holder.query('ROLLBACK').catch(()=>undefined);holder.release();}
    configureRamStore(null);configurePacketBatch(null);
    const keys=await redis.keys(key+'*');if(keys.length)await redis.del(...keys);
    await redis.quit();await pool.end();
    await admin.query(`DROP DATABASE IF EXISTS "${name}" WITH (FORCE)`);await admin.end();
    fs.rmSync(files,{recursive:true,force:true});
    for(const [name,value] of Object.entries(old)){if(value===undefined)delete process.env[name];else process.env[name]=value;}
  });
  await redis.connect();
  await pool.query(fs.readFileSync(new URL('../db/schema/base.sql',import.meta.url),'utf8'));
  await runMigrations(pool);
  await pool.query(`CREATE FUNCTION hold_owned_packet() RETURNS trigger LANGUAGE plpgsql AS $$
    BEGIN PERFORM pg_advisory_xact_lock(918204,17); RETURN NEW; END $$;
    CREATE TRIGGER hold_owned_packet BEFORE INSERT ON packets FOR EACH ROW EXECUTE FUNCTION hold_owned_packet()`);
  holder=await pool.connect();await holder.query('BEGIN');await holder.query('SELECT pg_advisory_xact_lock(918204,17)');
  process.env['MQTT_INGEST_MODE']='ram';process.env['MQTT_RAM_STREAM_KEY']=key;
  process.env['MQTT_RAM_QUEUE_MAX_BYTES']=String(1048576);
  configureRamStore(redis);configurePacketBatch((sql,params)=>pool.query(sql,params));
  const accepted=await admitPacketToRam({packetHash:'F1'.repeat(32),rxNodeId:'CD'.repeat(32),srcNodeId:'AB'.repeat(32),
    topic:`meshcore/ZZZ/${'CD'.repeat(32)}/packets`,network:name,rawHex:'00',packetType:5,routeType:0,hopCount:2,
    pathHashes:['ABCD','BEEF'],pathHashSizeBytes:2,payload:{_summary:'owned-runtime-fixture'}});
  const initial=await redis.xrange(key,'-','+');
  const bytes=accountedRamEntryBytes(initial[0]![1][1]!);
  // Simulate a crash that lost only the companion counter before startup.
  await redis.del(key+':bytes');
  const evidence=process.env['TEST_EVIDENCE_DIR'];
  const launch=async(mode:'ram'|'durable'|'drain',deadline=7_000)=>{
    const httpPort=await port(),metricsPort=await port();
    const applicationName=`${name}-process-${children.length+1}`;
    const env={...process.env,DATABASE_URL:url.toString(),OWNER_DATABASE_URL:url.toString(),
      DATABASE_SKIP_SCHEMA_INIT:'1',DATABASE_APPLICATION_NAME:applicationName,DATABASE_STATEMENT_TIMEOUT_MS:'8000',
      REDIS_URL:process.env['TEST_REDIS_RUNTIME_URL']??redisURL!,REDIS_PASSWORD:'',MQTT_INGEST_ENABLED:'0',WS_ENABLED:'0',
      MQTT_INGEST_MODE:mode==='drain'?'ram':mode,MQTT_RAM_STREAM_KEY:key,MQTT_RAM_CONSUMER_GROUP:group,
      MQTT_RAM_CONSUMER_NAME:'owned-runtime',MQTT_RAM_BATCH_SIZE:'1',MQTT_RAM_MAX_AGE_MS:'1000',
      MQTT_RAM_QUEUE_MAX_BYTES:String(1048576),MQTT_RAM_DRAIN_DEADLINE_MS:'180000',
      OWNER_AUTHORIZATION_MODE:'shadow',OWNER_ACL_MODE:'shadow',
      MOSQUITTO_ACL_PATH:path.join(files,'acl'),OWNER_ACL_SHADOW_PATH:path.join(files,'shadow'),
      PATH_RESOLVE_WORKERS:'1',
      PORT:String(httpPort),METRICS_PORT:String(metricsPort),SHUTDOWN_DEADLINE_MS:String(deadline)};
    const compiled=process.env['TEST_BACKEND_ENTRY']??path.resolve('dist/index.js');
    assert.ok(fs.existsSync(compiled),'build the actual backend before running the runtime gate');
    const entry=mode==='drain'?path.join(path.dirname(compiled),'ram/drain.js'):compiled;
    const child=spawn(process.execPath,[entry],{env,stdio:['ignore','pipe','pipe']});
    children.push(child);let output='';
    let log:fs.WriteStream|undefined;
    if(evidence)log=fs.createWriteStream(path.join(evidence,`${name}-${mode}-${children.length}.log`));
    child.stdout!.on('data',chunk=>{output+=chunk;log?.write(chunk);});
    child.stderr!.on('data',chunk=>{output+=chunk;log?.write(chunk);});
    const done=new Promise<{code:number|null,signal:NodeJS.Signals|null}>(resolve=>child.once('exit',(code,signal)=>{log?.end();resolve({code,signal});}));
    return {child,done,output:()=>output,httpPort,metricsPort,applicationName};
  };
  const counts=async()=>(await pool.query(`SELECT
    (SELECT COUNT(*)::int FROM packets WHERE network=$1) packets,
    (SELECT COUNT(*)::int FROM ingest_observation_ledger WHERE network=$1) ledger,
    (SELECT COUNT(*)::int FROM packet_paths WHERE network=$1) paths,
    (SELECT COALESCE(SUM(packet_count),0)::int FROM packet_hourly_stats WHERE network=$1) hourly,
    (SELECT COUNT(*)::int FROM packet_daily_stats WHERE network=$1) daily,
    (SELECT COUNT(*)::int FROM observer_region_packet_sightings WHERE network=$1) regions`,[name])).rows[0];
  const blocked=async(applicationName:string)=>Number((await pool.query(`SELECT COUNT(*) n FROM pg_stat_activity
    WHERE datname=$1 AND application_name=$2 AND wait_event='advisory'`,[name,applicationName])).rows[0].n)>0;
  const first=await launch('ram',2_000);
  await until(()=>blocked(first.applicationName),25_000,'held real INSERT');
  assert.equal(Number(await redis.get(key+':bytes')),bytes,'writer startup reconciles the lost reservation');
  assert.equal((await redis.xpending(key,group))[0],1);
  const loadedMetrics=await (await fetch(`http://127.0.0.1:${first.metricsPort}/metrics`)).text();
  for(const [suffix,value] of [['pending_depth',1],['retained_depth',0],['pel_depth',1],['queue_reserved_bytes',bytes]]) {
    assert.match(loadedMetrics,new RegExp(`^meshcore_ram_ingest_${suffix} ${value}$`,'m'));
  }
  assert.deepEqual(await counts(),{packets:0,ledger:0,paths:0,hourly:0,daily:0,regions:0});
  const signalAt=Date.now();first.child.kill('SIGTERM');
  const failed=await Promise.race([first.done,sleep(6_000).then(()=>{throw new Error('SIGTERM exceeded the shared deadline');})]);
  assert.equal(failed.code,1);assert.equal(failed.signal,null);
  assert.ok(Date.now()-signalAt<5_000);
  assert.doesNotMatch(first.output(),/graceful shutdown complete/);
  assert.match(first.output(),/graceful shutdown (?:failed|timed out)|RAM_DRAIN_TIMEOUT/);
  assert.deepEqual(await counts(),{packets:0,ledger:0,paths:0,hourly:0,daily:0,regions:0});
  assert.equal((await redis.xpending(key,group))[0],1);
  assert.equal(Number(await redis.get(key+':bytes')),bytes);
  await until(async()=>!await blocked(first.applicationName),15_000,'aborted first database connection');
  const recovered=await launch('ram');
  await until(()=>blocked(recovered.applicationName),25_000,'held replay INSERT');
  recovered.child.kill('SIGTERM');await sleep(250);
  assert.equal(recovered.child.exitCode,null,'successful drain waits for the actual write');
  assert.deepEqual(await counts(),{packets:0,ledger:0,paths:0,hourly:0,daily:0,regions:0});
  await holder.query('COMMIT');holder.release();holder=undefined;
  const success=await recovered.done;
  assert.equal(success.code,0,recovered.output());assert.match(recovered.output(),/graceful shutdown complete/);
  const expected={packets:1,ledger:1,paths:1,hourly:1,daily:1,regions:1};
  assert.deepEqual(await counts(),expected);
  assert.equal((await pool.query('SELECT observation_id::text FROM packets WHERE network=$1',[name])).rows[0].observation_id,accepted.observationId);
  assert.equal((await redis.xpending(key,group))[0],0);
  assert.equal(await redis.xlen(key),1,'committed overlap remains reserved until expiry');
  assert.equal(Number(await redis.get(key+':bytes')),bytes);
  // Quarantine blocks rollback even when the source is empty. After review,
  // only its separate operator may clear it and rerun the production drainer.
  await redis.xadd(key+':quarantine','*','q','owned-operator-review');
  const drain=await launch('drain');
  const refused=await drain.done;
  assert.equal(refused.code,1,drain.output());assert.match(drain.output(),/quarantined entries require review/);
  assert.doesNotMatch(drain.output(),/RAM_DRAIN_COMPLETE/);
  assert.equal(await redis.xlen(key),0);assert.equal((await redis.xpending(key,group))[0],0);
  assert.equal(Number(await redis.get(key+':bytes')),0);
  await redis.del(key+':quarantine');
  const drained=await launch('drain');
  assert.equal((await drained.done).code,0,drained.output());assert.match(drained.output(),/RAM_DRAIN_COMPLETE/);
  const durable=await launch('durable');
  await until(()=>durable.output().includes('[app] listening'),25_000,'durable rollback listener');
  assert.doesNotMatch(durable.output(),/\[ram\] writer started/);
  const scrape=await fetch(`http://127.0.0.1:${durable.metricsPort}/metrics`);assert.equal(scrape.status,200);
  const metrics=await scrape.text();assert.match(metrics,/meshcore_mqtt_post_persist_inflight/);
  durable.child.kill('SIGTERM');assert.equal((await durable.done).code,0,durable.output());
  assert.deepEqual(await counts(),expected,'rollback preserves every observation and side effect exactly once');
});
