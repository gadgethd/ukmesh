import assert from 'node:assert/strict';
import {EventEmitter} from 'node:events';
import test from 'node:test';
import {observeBrokerQuiescence} from './broker-quiescence.mjs';

const endpoint='mqtt://127.0.0.1:1884';
const topics={store:'$SYS/broker/store/messages/count',retained:'$SYS/broker/retained messages/count',
  queued:'$SYS/broker/packet/out/count',disconnected:'$SYS/broker/clients/disconnected',uptime:'$SYS/broker/uptime'};
const turn=()=>new Promise(resolve=>setImmediate(resolve));
function fixture(script,options={}) {
  let client,settings,address;
  const calls=[];
  function factory(url,config) {
    address=url;settings=config;client=new EventEmitter();
    client.stream={destroy:()=>calls.push('destroy')};
    client.subscribeAsync=async(filters,subscription)=>{
      calls.push(['subscribe',[...filters],subscription]);
      if(options.subscribeError)throw new Error('private backend details');
      setImmediate(()=>script(send,client));
      return filters.map(topic=>({topic,qos:options.denied?128:0}));
    };
    client.unsubscribeAsync=async filters=>{
      calls.push(['unsubscribe',[...filters]]);
      options.beforeUnsubAck?.(send,client);
    };
    client.endAsync=async force=>{
      calls.push(['endAsync',force]);
      if(options.endHangs)return new Promise(()=>{});
      client.emit('close');
    };
    client.end=force=>{calls.push(['end',force]);client.emit('close');};
    queueMicrotask(()=>{if(!options.noConnect)client.emit('connect',{sessionPresent:options.sessionPresent??false});});
    return client;
  }
  function send(key,value,retain=true) {
    client.emit('message',topics[key]??key,Buffer.from(String(value)),{retain});
  }
  return {factory,calls,get client(){return client;},get settings(){return settings;},get address(){return address;}};
}
function baseline(send,override={}) {
  for(const [key,value] of Object.entries({store:50,retained:50,queued:0,disconnected:0,...override}))send(key,value);
  send('uptime','100 seconds');
}
function live(send,epochs=[101,111,121,131]) {for(const seconds of epochs)send('uptime',`${seconds} seconds`,false);}
async function expire(t,promise) {
  await turn();t.mock.timers.tick(45001);await turn();t.mock.timers.tick(45001);
  return promise;
}

test('three completed live epochs accept cached unchanged metrics and close only their clean session',async()=>{
  const f=fixture(send=>{baseline(send);live(send);});
  const result=await observeBrokerQuiescence(endpoint,f.factory);
  assert.equal(result.passed,true);assert.equal(result.epochs.length,3);
  assert.deepEqual(result.epochs.map(row=>row.uptimeSeconds),[101,111,121]);
  assert.deepEqual(result.epochs.map(row=>row.closedBy.uptimeSeconds),[111,121,131]);
  assert.equal(f.settings.clean,true);assert.equal(f.settings.reconnectPeriod,0);assert.equal(f.settings.resubscribe,false);
  assert.match(f.settings.clientId,/^ukmesh-quiescence-[a-f0-9-]{36}$/);
  assert.equal(result.cleanup.closed,true);assert.equal(result.cleanup.forced,false);
  assert.equal(f.calls.filter(row=>Array.isArray(row)&&row[0]==='subscribe').length,1);
  assert.equal(f.calls.filter(row=>Array.isArray(row)&&row[0]==='unsubscribe').length,1);
  assert.deepEqual(f.calls.at(-1),['endAsync',false]);
});

test('queue, store mismatch and even empty disconnected durable sessions refuse permanently',async()=>{
  for(const [override,error] of [[{queued:1},'queued-packets'],[{disconnected:1},'disconnected-durable-sessions']]) {
    const f=fixture(send=>{baseline(send,override);baseline(send);live(send);});
    const result=await observeBrokerQuiescence(endpoint,f.factory);
    assert.equal(result.passed,false);assert.ok(result.errors.includes(error));assert.equal(result.cleanup.closed,true);
  }
  // A relational predicate is evaluated at a complete producer boundary. Later
  // equality cannot erase that unsafe epoch; intermediate paired fields can.
  const f=fixture(send=>{
    baseline(send,{store:51});live(send,[101,111]);
    send('store',50,false);live(send,[121,131]);
  });
  const result=await observeBrokerQuiescence(endpoint,f.factory);
  assert.equal(result.passed,false);assert.ok(result.errors.includes('stored-nonretained-messages'));
  assert.equal(result.latestMetrics.store,result.latestMetrics.retained);
  assert.equal(result.cleanup.closed,true);
});

test('malformed, nonadvancing and backward system metrics fail closed',async()=>{
  for(const script of [send=>{baseline(send);send('queued','01');live(send);},
    send=>{baseline(send);send('store','9007199254740992');live(send);},
    send=>{baseline(send);send('uptime','101');},
    send=>{baseline(send);live(send,[101,101]);},
    send=>{baseline(send);live(send,[99]);}]) {
    const f=fixture(script),result=await observeBrokerQuiescence(endpoint,f.factory);
    assert.equal(result.passed,false);assert.equal(result.cleanup.closed,true);
  }
});

test('retained application data is unsafe and evidence never contains its topic or payload',async()=>{
  const f=fixture(send=>{baseline(send);send('private/secret-node','private-payload');live(send);});
  const result=await observeBrokerQuiescence(endpoint,f.factory);
  assert.equal(result.passed,false);assert.equal(result.traffic.retainedNonSys,1);
  assert.ok(result.errors.includes('retained-application-message'));
  assert.doesNotMatch(JSON.stringify(result),/private\/secret-node|private-payload/);
  assert.match(result.traffic.sha256,/^[a-f0-9]{64}$/);
});

test('an unsafe message before the own UNSUBACK is still included',async()=>{
  const f=fixture(send=>{baseline(send);live(send);},{beforeUnsubAck:send=>send('private/topic','secret')});
  const result=await observeBrokerQuiescence(endpoint,f.factory);
  assert.equal(result.passed,false);assert.equal(result.traffic.retainedNonSys,1);
});

test('missing metrics, retained-only and incomplete epochs time out and always close',async t=>{
  t.mock.timers.enable({apis:['setTimeout']});
  for(const script of [send=>{baseline(send);},send=>{send('uptime','100 seconds');live(send);},
    send=>{baseline(send);live(send,[101,111]);}]) {
    const f=fixture(script),result=await expire(t,observeBrokerQuiescence(endpoint,f.factory));
    assert.equal(result.passed,false);assert.ok(result.errors.includes('observation-deadline'));
    assert.equal(result.cleanup.closed,true);
  }
});

test('connection, subscription and unexpected close errors retain redacted failed evidence',async t=>{
  t.mock.timers.enable({apis:['setTimeout']});
  for(const options of [{noConnect:true},{subscribeError:true},{denied:true},{sessionPresent:true}]) {
    const f=fixture(send=>{baseline(send);live(send);},options);
    const pending=observeBrokerQuiescence(endpoint,f.factory);
    const result=options.noConnect?await expire(t,pending):await pending;
    assert.equal(result.passed,false);assert.equal(result.cleanup.closed,true);
    assert.doesNotMatch(JSON.stringify(result),/private backend details/);
  }
  for(const event of ['error','close']) {
    const f=fixture((_send,client)=>client.emit(event,new Error('private cause')));
    const result=await observeBrokerQuiescence(endpoint,f.factory);
    assert.equal(result.passed,false);assert.equal(result.cleanup.closed,true);
  }
});

test('a hung graceful close is bounded, forced, and cannot pass',async t=>{
  t.mock.timers.enable({apis:['setTimeout']});
  const f=fixture(send=>{baseline(send);live(send);},{endHangs:true});
  const result=await expire(t,observeBrokerQuiescence(endpoint,f.factory));
  assert.equal(result.passed,false);assert.equal(result.cleanup.forced,true);
  assert.ok(result.errors.includes('close-failed'));assert.ok(f.calls.includes('destroy'));
  assert.ok(f.calls.some(row=>Array.isArray(row)&&row[0]==='end'&&row[1]===true));
});

test('invalid or credentialed endpoints fail without dispatching a client',async()=>{
  let calls=0;const factory=()=>{calls++;throw new Error('must not run');};
  for(const address of ['mqtt://example.com:1884','mqtt://:secret@127.0.0.1:1884','mqtt://127.0.0.1:1884/path',
    'mqtt://127.0.0.1:1884?secret=value','mqtts://127.0.0.1:1884','mqtt://127.0.0.1:1884#value',
    'mqtt://@127.0.0.1:1884','mqtt://127.0.0.1:65536']) {
    const result=await observeBrokerQuiescence(address,factory);
    assert.equal(result.passed,false);assert.equal(result.endpoint,null);
    assert.doesNotMatch(JSON.stringify(result),/secret|example\.com/);
  }
  assert.equal(calls,0);
});

test('bounded input metadata and even non-Error factory failures refuse without leaking payloads',async()=>{
  for(const script of [(_send,client)=>client.emit('message','private/topic',Buffer.alloc(8*1024*1024+1),{retain:false}),
    (_send,client)=>client.emit('message',topics.store,Buffer.from('0'),{}),
    send=>{baseline(send);for(let index=0;index<65537;index++)send('private/topic','',false);}]) {
    const f=fixture(script),result=await observeBrokerQuiescence(endpoint,f.factory);
    assert.equal(result.passed,false);assert.equal(result.cleanup.closed,true);
    assert.doesNotMatch(JSON.stringify(result),/private\/topic/);
  }
  const result=await observeBrokerQuiescence(endpoint,()=>{throw null;});
  assert.equal(result.passed,false);assert.ok(result.errors.includes('observer-error'));
});
