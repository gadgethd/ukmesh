// PRIVATE prospective regression. Every connection is an injected fake.
// Root runs the first test against the unchanged owner for the expected RED.
import assert from 'node:assert/strict';
import {createHash} from 'node:crypto';
import {EventEmitter} from 'node:events';
import test from 'node:test';
import {observeBrokerQuiescence} from './broker-quiescence.mjs';

const endpoint='mqtt://127.0.0.1:1884';
const topics={store:'$SYS/broker/store/messages/count',retained:'$SYS/broker/retained messages/count',
  queued:'$SYS/broker/packet/out/count',disconnected:'$SYS/broker/clients/disconnected',uptime:'$SYS/broker/uptime'};
const turn=()=>new Promise(resolve=>setImmediate(resolve));
function fixture(script,{beforeUnsubAck}={}) {
  const calls=[];
  let client,settings;
  function send(key,value,retain=true) {
    client.emit('message',topics[key]??key,Buffer.from(String(value)),{retain});
  }
  function factory(_address,options) {
    settings=options;client=new EventEmitter();
    client.stream={destroy:()=>calls.push('destroy')};
    client.subscribeAsync=async filters=>{
      calls.push('subscribe');
      setImmediate(()=>{Promise.resolve().then(()=>script(send)).catch(error=>client.emit('error',error));});
      return filters.map(topic=>({topic,qos:0}));
    };
    client.unsubscribeAsync=async()=>{calls.push('unsubscribe');await beforeUnsubAck?.(send);};
    client.endAsync=async force=>{calls.push(['endAsync',force]);client.emit('close');};
    client.end=force=>{calls.push(['end',force]);client.emit('close');};
    queueMicrotask(()=>client.emit('connect',{sessionPresent:false}));
    return client;
  }
  return {factory,calls,get settings(){return settings;}};
}
function cached(send,{store=52,retained=store,queued=0,disconnected=0}={}) {
  for(const [key,value] of Object.entries({store,retained,queued,disconnected}))send(key,value);
  send('uptime','0 seconds');
}
function marker(send,seconds) {send('uptime',`${seconds} seconds`,false);}
function completed(send) {for(const seconds of [8,18,28,38])marker(send,seconds);}
const exactTrace=[['store','2',true],['retained','2',true],['queued','0',true],['uptime','0 seconds',true],
  ['uptime','8 seconds',false],['disconnected','0',false],['store','52',false],['retained','52',false]];

test('actual eight-packet mixed SYS batch becomes three coherent completed epochs',async()=>{
  // Bind the fake transcript to the actual failed observer's packet digest.
  const digest=createHash('sha256');let bytes=0;
  for(const [key,text,retain] of exactTrace) {
    const topic=topics[key],body=Buffer.from(text);bytes+=body.length;
    digest.update(JSON.stringify([topic.length,body.length,retain]));digest.update(topic);digest.update(body);
  }
  assert.equal(bytes,26);
  assert.equal(digest.digest('hex'),'e31e054f81bfef59f3adfd951fb144ae6b0e2c012823025e306bb0346d99b36c');
  const f=fixture(send=>{
    for(const row of exactTrace)send(...row);
    for(const seconds of [18,28,38])marker(send,seconds);
  });
  const result=await observeBrokerQuiescence(endpoint,f.factory);
  assert.equal(result.passed,true,JSON.stringify({errors:result.errors,latest:result.latestMetrics}));
  assert.equal(result.epochs.length,3);
  assert.deepEqual(result.epochs.map(row=>row.uptimeSeconds),[8,18,28]);
  assert.deepEqual(result.epochs.map(row=>row.closedBy.uptimeSeconds),[18,28,38]);
  for(const row of result.epochs) {
    assert.ok(row.messageIndex<row.closedBy.messageIndex);
    assert.ok(row.offsetMs<=row.closedBy.offsetMs);
    assert.match(row.at,/^\d{4}-\d{2}-\d{2}T/);assert.match(row.closedBy.at,/^\d{4}-\d{2}-\d{2}T/);
    assert.equal(row.metricObservations.store.retained,false);
    assert.equal(row.metricObservations.retained.retained,false);
    assert.equal(row.metricObservations.queued.retained,true);
  }
  assert.ok(result.epochs.every(row=>row.metrics.store===52&&row.metrics.retained===52
    &&row.metrics.queued===0&&row.metrics.disconnected===0));
  assert.equal(result.maxWindowMs,45000);
  assert.equal(result.traffic.retainedNonSys,0);
  assert.equal(f.settings.clean,true);assert.equal(f.settings.reconnectPeriod,0);
  assert.equal(f.calls.filter(row=>row==='unsubscribe').length,1);
  assert.deepEqual(f.calls.at(-1),['endAsync',false]);
});

test('split field updates across asynchronous turns use only the closed producer tuple',async()=>{
  for(const [next,order] of [[60,['store','retained']],[40,['retained','store']]]) {
    const f=fixture(async send=>{
      cached(send);marker(send,8);
      send(order[0],next,false);await turn();send(order[1],next,false);await turn();
      for(const seconds of [18,28,38])marker(send,seconds);
    });
    const result=await observeBrokerQuiescence(endpoint,f.factory);
    assert.equal(result.passed,true,JSON.stringify(result.errors));
    assert.equal(result.epochs.length,3);
    assert.ok(result.epochs.every(row=>row.metrics.store===next&&row.metrics.retained===next));
  }
});

test('a genuine completed-epoch mismatch remains failed after later matching recovery',async()=>{
  const f=fixture(send=>{
    cached(send);marker(send,8);send('store',53,false);marker(send,18);
    send('retained',53,false);marker(send,28);marker(send,38);
  });
  const result=await observeBrokerQuiescence(endpoint,f.factory);
  assert.equal(result.passed,false);assert.ok(result.errors.includes('stored-nonretained-messages'));
  assert.equal(result.latestMetrics.store,result.latestMetrics.retained);
  assert.equal(result.cleanup.closed,true);
});

test('a late retained cache packet cannot certify or overwrite an already-open live batch',async()=>{
  const f=fixture(send=>{
    cached(send);marker(send,8);send('store',53,false);send('retained',53,true);
    for(const seconds of [18,28,38])marker(send,seconds);
  });
  const result=await observeBrokerQuiescence(endpoint,f.factory);
  assert.equal(result.passed,false);assert.equal(result.cleanup.closed,true);
});

test('positive queue/disconnected and retained application messages remain immediately sticky',async()=>{
  for(const [key,value,error] of [['queued',1,'queued-packets'],['disconnected',1,'disconnected-durable-sessions'],
    ['private/topic','private-payload','retained-application-message']]) {
    for(const beforeTuple of [true,false]) {
      const f=fixture(send=>{
        if(beforeTuple)send(key,value,key==='private/topic');
        cached(send);marker(send,8);
        if(!beforeTuple)send(key,value,key==='private/topic');
        if(key!=='private/topic')send(key,0,false);
        for(const seconds of [18,28,38])marker(send,seconds);
      });
      const result=await observeBrokerQuiescence(endpoint,f.factory);
      assert.equal(result.passed,false);assert.ok(result.errors.includes(error));
      assert.doesNotMatch(JSON.stringify(result),/private\/topic|private-payload/);
      assert.equal(result.cleanup.closed,true);
    }
  }
});

test('the existing own UNSUBACK includes final partial-batch relation and unsafe observations',async()=>{
  for(const kind of ['balanced','mismatch','closedThenRecovered','queued','disconnected','retained']) {
    const f=fixture(send=>{cached(send);completed(send);},{beforeUnsubAck:async send=>{
      if(kind==='balanced'||kind==='mismatch') {
        send('store',51,false);await turn();
        if(kind==='balanced')send('retained',51,false);
      } else if(kind==='closedThenRecovered') {
        send('store',53,false);marker(send,48);send('retained',53,false);
      } else if(kind==='retained')send('private/topic','private-payload');
      else send(kind,1,false);
    }});
    const result=await observeBrokerQuiescence(endpoint,f.factory);
    assert.equal(result.passed,kind==='balanced',JSON.stringify({kind,errors:result.errors}));
    if(kind==='mismatch'||kind==='closedThenRecovered')assert.ok(result.errors.includes('stored-nonretained-messages'));
    if(kind==='closedThenRecovered')assert.equal(result.latestMetrics.store,result.latestMetrics.retained);
    if(kind==='queued')assert.ok(result.errors.includes('queued-packets'));
    if(kind==='disconnected')assert.ok(result.errors.includes('disconnected-durable-sessions'));
    if(kind==='retained')assert.ok(result.errors.includes('retained-application-message'));
    assert.equal(result.epochs.length,3);
    assert.ok(result.epochs.every(row=>row.metrics.store===52&&row.metrics.retained===52));
    assert.equal(result.finalSnapshot.afterOwnUnsuback,true);
    if(kind==='balanced') {
      assert.equal(result.finalSnapshot.metrics.store,51);assert.equal(result.finalSnapshot.metrics.retained,51);
      assert.equal(result.epochs[0].metricObservations.store.retained,true);
    }
    assert.equal(result.cleanup.closed,true);assert.equal(result.cleanup.forced,false);
  }
});

test('three live markers close only two epochs and cannot qualify before the unchanged deadline',async t=>{
  t.mock.timers.enable({apis:['setTimeout']});
  const f=fixture(send=>{cached(send);for(const seconds of [8,18,28])marker(send,seconds);});
  const pending=observeBrokerQuiescence(endpoint,f.factory);
  await turn();await turn();t.mock.timers.tick(45001);await turn();t.mock.timers.tick(45001);
  const result=await pending;
  assert.equal(result.passed,false);assert.equal(result.epochs.length,2);
  assert.ok(result.errors.includes('observation-deadline'));
  assert.ok(result.errors.includes('missing-live-epochs'));assert.equal(result.maxWindowMs,45000);
  assert.equal(result.cleanup.closed,true);
});
