import assert from 'node:assert/strict';
import http from 'node:http';
import { once } from 'node:events';
import { fileURLToPath } from 'node:url';
import type { AddressInfo } from 'node:net';
import test from 'node:test';
import { readDemandPolicy,runMatchedDemand } from './lookupDemand.js';
import { createLookupRequestFleet } from './lookupHttpRequests.js';
import type { LookupHttpCase,LookupHttpResult } from './lookupHttpRequests.js';

const policy=await readDemandPolicy(fileURLToPath(new URL('../../../scripts/full-dev/qualification/read_protocol.json',import.meta.url)));
const short={...policy,requests:12,duration_seconds:.15};
const definitions:LookupHttpCase[]=policy.case_order.map(name=>({name,route:'/'+name,
  ...(name==='packet-missing'?{expected:404}:{}),populated:false}));
const hash='a'.repeat(64);
function harness() {
  let at=1000, early=false;
  const clock={now:()=>at,pause:async(ms:number)=>{if(early){early=false;return;}at+=ms;}};
  const start=(definition:LookupHttpCase,actor:number)=>{
    const timing={startedAtMonoMs:at,socketAtMonoMs:at as number|null,connectedAtMonoMs:at as number|null,actor:`127.0.1.${actor+1}`};
    const value:LookupHttpResult={...timing,ms:0,bytes:2,status:definition.expected??200,digest:hash,dataDigest:hash,
      entries:1,receivedAtMonoMs:at};
    return {timing,done:Promise.resolve(value)};
  };
  return {clock,start,get:()=>at,advance:(ms:number)=>{at+=ms;},early:()=>{early=true;}};
}

test('shared policy drives every planned index, cyclic case, actor and exact schedule',async()=>{
  const h=harness();const value=await runMatchedDemand(short,definitions,900,1000,h.start,h.clock);
  assert.equal(value.passed,true);assert.equal(value.samples.length,12);
  assert.deepEqual(value.window,{startMs:100,endMs:250});
  assert.deepEqual(value.samples.map(r=>r.scheduledMs),Array.from({length:12},(_,i)=>100+i*12.5));
  assert.deepEqual(value.samples.map(r=>r.name),policy.case_order);
  assert.equal(value.admission.planned,12);assert.equal(value.admission.attempted,12);
  assert.equal(value.admission.completed,12);assert.equal(value.admission.undelivered,0);
  assert.ok(h.get()>=1150,'full fixed observation window remains covered');
});

test('early scheduled timers are rechecked rather than dispatching early',async()=>{
  const h=harness();h.early();const value=await runMatchedDemand(short,definitions,900,1000,h.start,h.clock);
  assert.equal(value.passed,true);
  assert.ok(value.samples.every(row=>row.socketMs!>=row.scheduledMs));
});

test('actual socket queue delay invalidates even an on-time request token and retains every planned row',async()=>{
  const h=harness();
  const value=await runMatchedDemand(short,definitions,900,1000,(definition,actor)=>{
    const started=h.get();h.advance(101);
    const pending=h.start(definition,actor);pending.timing.startedAtMonoMs=started;return pending;
  },h.clock);
  assert.equal(value.passed,false);assert.equal(value.samples.length,12);
  assert.ok(value.samples[0]!.latenessMs!>=101);assert.equal(value.samples[0]!.startMs,100);
  assert.ok(value.admission.undelivered>0);
});

test('pacing waits for delayed socket assignment and never compresses subsequent admissions',async()=>{
  const h=harness();let calls=0;
  const value=await runMatchedDemand(short,definitions,900,1000,(definition,actor)=>{
    const pending=h.start(definition,actor),started=h.get();
    if(calls++===0){
      pending.timing.socketAtMonoMs=null;
      const assigned=Promise.resolve().then(()=>{h.advance(20);pending.timing.socketAtMonoMs=h.get();pending.timing.connectedAtMonoMs=h.get();return h.get();});
      return {timing:pending.timing,assigned,done:assigned.then(async at=>({...await pending.done,
        startedAtMonoMs:started,socketAtMonoMs:at,connectedAtMonoMs:at,receivedAtMonoMs:at,ms:at-started}))};
    }
    return pending;
  },h.clock);
  assert.equal(value.samples.length,12);
  assert.ok(value.samples[1]!.socketMs!-value.samples[0]!.socketMs!>=10);
  assert.equal(value.intervalViolation,false);
});

test('eight active validation slots drain without retries or unhandled late failures',async()=>{
  const h=harness();let active=0,maximum=0,attempts=0;
  const value=await runMatchedDemand(short,definitions,900,1000,(definition,actor)=>{
    const pending=h.start(definition,actor);active++;maximum=Math.max(maximum,active);const index=attempts++;
    return {timing:pending.timing,done:new Promise<LookupHttpResult>((resolve,reject)=>setImmediate(()=>{
      active--;if(index===3)reject(new Error('injected response failure'));else pending.done.then(resolve,reject);
    }))};
  },h.clock);
  assert.equal(maximum,8);assert.equal(active,0);assert.equal(attempts,12);
  assert.equal(value.admission.completed,12);assert.equal(value.admission.errors,1);
  assert.equal(value.samples.filter(r=>r.error).length,1);assert.equal(value.passed,false);
});

test('response deadlines and per-case evidence stay mandatory even for a resolved transport',async()=>{
  for(const change of ['deadline','status','digest','bytes']){
    const h=harness();
    const value=await runMatchedDemand(short,definitions,900,1000,(definition,actor)=>{
      const request=h.start(definition,actor);
      return {...request,done:request.done.then(value=>({...value,...(change==='deadline'?{ms:30001}:
        change==='status'?{status:500}:change==='digest'?{digest:'bad'}:{bytes:8388609})}))};
    },h.clock);
    assert.equal(value.passed,false,change);assert.equal(value.admission.errors,12,change);
    assert.equal(value.samples.length,12);
  }
});

test('synchronous HTTP startup errors reject both admission and response promises',async t=>{
  const failure=new Error('injected socket startup failure');
  t.mock.method(http,'get',()=>{throw failure;});
  const fleet=createLookupRequestFleet();
  try{
    const request=fleet.start(1,definitions[0]!,0);
    await Promise.all([assert.rejects(request.done,error=>error===failure),
      assert.rejects(request.assigned!,error=>error===failure)]);
  }finally{await fleet.close();}
});

test('real local HTTP keeps existing content/status/hash bounds and records actual socket admission', {timeout:10000},async()=>{
  let calls=0;
  const server=http.createServer((request,response)=>{
    calls++;
    if(request.url==='/bad'){response.end('{broken');return;}
    if(request.url==='/packet-missing'){response.writeHead(404);response.end('{"error":"missing"}');return;}
    if(request.url==='/packet-detail'){response.end('{"packetHash":"fixture"}');return;}
    response.end('{"nodes":[1],"page":{"snapshot":"fixed"}}');
  });
  const fleet=createLookupRequestFleet();
  try{
    server.listen(0,'127.0.0.1');await once(server,'listening');const port=(server.address() as AddressInfo).port;
    for(const definition of definitions)for(let n=0;n<3;n++)await fleet.request(port,definition);
    const origin=performance.now();
    const value=await runMatchedDemand(short,definitions,origin,origin,(definition,actor)=>fleet.start(port,definition,actor));
    assert.equal(value.admission.completed,12);assert.equal(value.admission.errors,0);
    assert.equal(calls,48);assert.equal(value.samples[9]!.status,404);assert.equal(value.samples[9]!.entries,null);
    assert.equal(value.samples[8]!.entries,null);
    for(const row of value.samples){
      assert.ok(row.startMs!<=row.socketMs!&&row.socketMs!<=row.connectedMs!&&row.connectedMs!<=row.endMs!);
      assert.ok(row.endMs!<=row.completedMs!);assert.ok(Math.abs(row.ms!-(row.endMs!-row.startMs!))<1e-6);
      assert.match(row.digest!,/^[a-f0-9]{64}$/);
    }
    await assert.rejects(fleet.request(port,{name:'bad',route:'/bad'}),/not valid JSON/);
    assert.equal(calls,49);
    const stats=await fleet.readStatistics();assert.equal(stats.calls,49);assert.equal(stats.freshHashes,49);
    assert.equal(stats.errors,1);assert.ok(stats.maxEntries<=8);
  }finally{await fleet.close();server.closeAllConnections();await new Promise<void>(resolve=>server.close(()=>resolve()));}
});
