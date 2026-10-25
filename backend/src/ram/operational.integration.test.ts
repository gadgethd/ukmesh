import assert from 'node:assert/strict';
import http from 'node:http';
import { once } from 'node:events';
import test from 'node:test';
import { pathToFileURL } from 'node:url';
import express from 'express';
import { Redis } from 'ioredis';
import { configurePacketBatch } from '../db/packetBatch.js';
import { createRamWriter } from './writer.js';
import { accountedRamEntryBytes, decodeRamStreamValue, RAM_REDIS_HEADROOM_BYTES,
  RAM_QUARANTINE_MAX_ENTRIES, RAM_QUARANTINE_ENTRY_MAX_BYTES } from './protocol.js';
import { PrivatePrefixCache } from '../privacy/privatePrefixCache.js';
import { createPublicRamSource } from './publicSource.js';

const urls = ['TEST_REDIS_ACL_URL','TEST_REDIS_ADMIN_URL','TEST_REDIS_PUBLIC_URL','TEST_REDIS_OPERATOR_URL'].map(name=>process.env[name]);
const ready = urls.every(url=>url&&['127.0.0.1','localhost'].includes(new URL(url).hostname));
const packet = (i:number)=>({packetHash:i.toString(16).padStart(64,'0'),rxNodeId:'CD'.repeat(32),
  topic:`meshcore/ZZZ/${'CD'.repeat(32)}/packets`,rawHex:'DEADBEEF',packetType:4,network:'ukmesh',payload:{data:'x'.repeat(8_000)}});

async function fixture(t: test.TestContext, budget:number) {
  const clients=urls.map(url=>new Redis(url!,{lazyConnect:true,enableReadyCheck:false,enableOfflineQueue:false,maxRetriesPerRequest:1}));
  await Promise.all(clients.map(async client=>{await client.connect();await client.ping();}));
  const [writer,admin,publicClient,operator]=clients as [Redis,Redis,Redis,Redis];
  const old=Object.fromEntries(['MQTT_INGEST_MODE','MQTT_RAM_STREAM_KEY','MQTT_RAM_QUEUE_MAX_BYTES','MQTT_RAM_QUEUE_MAX_PACKETS'].map(name=>[name,process.env[name]]));
  process.env['MQTT_INGEST_MODE']='ram';
  const key=`meshcore:acl:${process.pid}:${Date.now()}`;
  process.env['MQTT_RAM_STREAM_KEY']=key;
  process.env['MQTT_RAM_QUEUE_MAX_BYTES']=String(budget);
  process.env['MQTT_RAM_QUEUE_MAX_PACKETS']='1000';
  const source=process.env['TEST_RAM_SOURCE_DIR'];
  const store = source ? await import(pathToFileURL(source+'/ram/liveStore.ts').href) : await import('./liveStore.js');
  const batch = source ? await import(pathToFileURL(source+'/db/packetBatch.ts').href) : {configurePacketBatch};
  batch.configurePacketBatch(async()=>({rows:[{generation:'1',materialized_generation:'1',remat_active:false,prefixes:[]}]}));
  store.configureRamStore(writer);
  t.after(async()=>{
    store.configureRamStore(null);batch.configurePacketBatch(null);
    const keys=await admin.keys(key+'*');if(keys.length)await admin.del(...keys);
    await Promise.all(clients.map(client=>client.quit()));
    for(const [name,value] of Object.entries(old)){if(value===undefined)delete process.env[name];else process.env[name]=value;}
  });
  return {writer,admin,publicClient,operator,key,store};
}

test('F09 actual Redis refuses unsafe headroom before admitting an observation', {skip:ready?false:'owned loopback ACL Redis fixture required'}, async t=>{
  const {writer,admin,key,store}=await fixture(t,32*1048576);
  await assert.rejects(store.admitPacketToRam(packet(1)),/redis_headroom/);
  assert.equal(await writer.xlen(key),0);
  process.env['MQTT_RAM_QUEUE_MAX_BYTES']=String(1048576);
  await admin.set(key+':other-private-load','x'.repeat(18*1048576));
  await assert.rejects(store.admitPacketToRam(packet(2)),/redis_headroom/,
    'a valid application budget also refuses admission when other resident keys consume recovery space');
  assert.equal(await writer.xlen(key),0);
});

test('F09 concurrent reconciliation, retained replay and lost cleanup reply conserve reservations', {skip:ready?false:'owned loopback ACL Redis fixture required'}, async t=>{
  const {writer,admin,key,store}=await fixture(t,1048576);
  const outcomes=await Promise.allSettled([
    ...Array.from({length:200},(_,i)=>store.admitPacketToRam(packet(i))),
    ...Array.from({length:8},()=>store.reconcileRamQueueBytes()),
  ]);
  const admitted=outcomes.slice(0,200).filter(result=>result.status==='fulfilled').length;
  assert.ok(admitted>0&&admitted<200,'supported byte budget must refuse some admissions');
  const reserved=async()=>{
    const rows=await writer.xrange(key,'-','+');
    const expected=rows.reduce((sum,row)=>sum+accountedRamEntryBytes(row[1][1]!),0);
    assert.equal(Number(await writer.get(key+':bytes')),expected);
    assert.ok(expected<=1048576);
    return {rows,expected};
  };
  const before=await reserved();
  await admin.del(key+':bytes'); // crash/lost counter before writer startup
  await store.reconcileRamQueueBytes();
  await reserved();
  const keys=await admin.keys(key+'*');
  const physical=(await Promise.all(keys.map(key=>admin.memory('USAGE',key)))).reduce((sum,value)=>sum+Number(value??0),0);
  assert.ok(physical<before.expected+RAM_REDIS_HEADROOM_BYTES,'payload/ledger overhead exceeds reserved recovery space');
  let now=Date.now()+120_001,lostReply=true;
  const proxy=new Proxy(writer,{get(target,property){
    if(property==='eval')return async (...args:Parameters<Redis['eval']>)=>{
      const result=await target.eval(...args);
      if(lostReply&&String(args[0]).includes("local at = tonumber(redis.call('HGET'")){lostReply=false;throw new Error('lost cleanup reply after Redis commit');}
      return result;
    };
    const value=Reflect.get(target,property);return typeof value==='function'?value.bind(target):value;
  }});
  const identities=new Set<string>();
  const running=createRamWriter({redis:proxy,persist:async entries=>{let novel=0;for(const entry of entries){if(!identities.has(entry.observationId!)){identities.add(entry.observationId!);novel++;}}return novel;},
    now:()=>now,sleep:async()=>undefined,batchSize:()=>2000,maxAgeMs:()=>1,streamKey:()=>key,group:()=> 'operational',consumer:()=> 'owned',statsTick:null,markHealthy:()=>undefined,onCommitted:null,log:()=>undefined});
  await running.start();await running.tickOnce();
  assert.equal(identities.size,admitted);
  assert.equal((await writer.xpending(key,'operational'))[0],0,'all persisted records are acknowledged');
  assert.equal(await writer.hlen(key+':committed'),admitted,'each committed record has its retention timestamp');
  assert.equal((await reserved()).expected,before.expected,'commit retains the payload reservation');
  now+=120_001;
  await running.sweepRetained();
  assert.equal(lostReply,false,'lost-reply boundary was exercised');
  await reserved();
  await running.sweepRetained();
  await store.reconcileRamQueueBytes();
  assert.equal((await reserved()).rows.length,0);
  assert.equal((await writer.xpending(key,'operational'))[0],0);
});

test('F16 real Redis principals isolate raw/quarantine while public HTTP rechecks authorization', {skip:ready?false:'owned loopback ACL Redis fixture required'}, async t=>{
  const {writer,publicClient,operator,key,store}=await fixture(t,1048576);
  await store.admitPacketToRam({...packet(1),payload:{}});
  for(const client of [publicClient,operator]){
    await assert.rejects(client.xrange(key,'-','+'),/NOPERM/);
    await assert.rejects(client.xread('STREAMS',key,'0-0'),/NOPERM/);
  }
  await writer.xadd(key+':quarantine','*','q','private-evidence');
  await assert.rejects(publicClient.xrange(key+':quarantine','-','+'),/NOPERM/);
  assert.equal((await operator.xrange(key+':quarantine','-','+')).length,1);
  const policy=new PrivatePrefixCache();policy.replace(1,[]);
  const app=express();app.use('/public',createPublicRamSource({token:()=> 'owned-fixture-token-with-at-least-32-characters',loadPolicy:async()=>policy,
    read:async()=>(await writer.xrange(key,'-','+')).map(([id,fields])=>{const decoded=decodeRamStreamValue(fields[1]);assert.ok(decoded&&decoded.kind==='entry');return {id,entry:decoded.entry};})}));
  const server=http.createServer(app);server.listen(0,'127.0.0.1');await once(server,'listening');
  t.after(()=>new Promise<void>((resolve,reject)=>server.close(error=>error?reject(error):resolve())));
  const address=server.address();assert.ok(address&&typeof address!=='string');
  const response=()=>fetch(`http://127.0.0.1:${address.port}/public/entries`,{headers:{authorization:'Bearer owned-fixture-token-with-at-least-32-characters'}});
  assert.equal((await (await response()).json()).entries.length,1);
  policy.replace(2,[{node_id:'CD'.repeat(32),network:'teesside',prefix_size_bytes:1,prefix:'CD'}]);
  const revoked=await (await response()).json();
  assert.deepEqual(revoked.entries,[]);assert.notEqual(revoked.nextCursor,'0-0');
});

test('F09/F11 real quarantine has an exact count and encoded-byte cap below reserved headroom', {skip:ready?false:'owned loopback ACL Redis fixture required'}, async t=>{
  const {writer,admin,key,store}=await fixture(t,16*1048576);
  // Model existing corrupted records, which admission validation would refuse.
  const pipeline=writer.pipeline();
  for(let i=0;i<RAM_QUARANTINE_MAX_ENTRIES+50;i++)pipeline.xadd(key,'*','e','x'.repeat(10_000));
  const injected=await pipeline.exec();assert.ok(injected);assert.ok(injected.every(([error])=>error===null));
  await store.reconcileRamQueueBytes();
  const running=createRamWriter({redis:writer,persist:async()=>{throw new Error('malformed rows must never reach PG');},
    now:()=>Date.now(),sleep:async()=>undefined,batchSize:()=>1000,maxAgeMs:()=>1,streamKey:()=>key,
    group:()=> 'quarantine-bound',consumer:()=> 'owned',statsTick:null,markHealthy:()=>undefined,onCommitted:null,log:()=>undefined});
  await running.start();await running.tickOnce();
  const evidence=await writer.xrange(key+':quarantine','-','+');
  assert.equal(evidence.length,RAM_QUARANTINE_MAX_ENTRIES);
  assert.ok(evidence.every(([,fields])=>Buffer.byteLength(fields[1]!)<=RAM_QUARANTINE_ENTRY_MAX_BYTES));
  assert.equal(await writer.xlen(key),0);assert.equal((await writer.xpending(key,'quarantine-bound'))[0],0);
  assert.equal(await store.reconcileRamQueueBytes(),0);
  const keys=await admin.keys(key+'*');
  const physical=(await Promise.all(keys.map(key=>admin.memory('USAGE',key)))).reduce((sum,value)=>sum+Number(value??0),0);
  assert.ok(physical<RAM_REDIS_HEADROOM_BYTES,'quarantine/PEL/marker allocation fits separate recovery space');
});
