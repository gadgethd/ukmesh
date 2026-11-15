// Broker-only observation. The caller owns broker identity and target locks.
import {createHash,randomUUID} from 'node:crypto';
import {createRequire} from 'node:module';
import {pathToFileURL} from 'node:url';
const require=createRequire(new URL('../../backend/package.json',import.meta.url));
const {connect}=require('mqtt');

const MAX_MS=45000,MAX_MESSAGES=65536,MAX_BYTES=8*1024*1024;
const TOPICS=Object.freeze({
  store:'$SYS/broker/store/messages/count',
  retained:'$SYS/broker/retained messages/count',
  queued:'$SYS/broker/packet/out/count',
  disconnected:'$SYS/broker/clients/disconnected',
  uptime:'$SYS/broker/uptime',
});
const FILTERS=Object.freeze([...Object.values(TOPICS),'#']);
const NUMERIC=new Map(Object.entries(TOPICS).map(([key,topic])=>[topic,key]));

function localEndpoint(endpoint) {
  const match=typeof endpoint==='string'&&/^mqtt:\/\/127\.0\.0\.1:([1-9]\d*)$/.exec(endpoint);
  if(!match||Number(match[1])<1024||Number(match[1])>65535)throw new Error('invalid-loopback-endpoint');
  return `mqtt://127.0.0.1:${Number(match[1])}`;
}

/** One clean session; never publishes or touches any other client's session. */
export async function observeBrokerQuiescence(endpoint,clientFactory=connect) {
  const started=performance.now(),startedAt=new Date().toISOString();
  const errors=[],epochs=[],values={},observed={},digest=createHash('sha256');
  const traffic={messages:0,bytes:0,retainedNonSys:0};
  const clientId=`ukmesh-quiescence-${randomUUID()}`;
  let address,client,connected=false,subscribed=false,closing=false,closed=false,forced=false;
  let initialized=false,previousUptime=null,pendingEpoch=null,finalSnapshot=null,timer,finish;
  const completed=new Promise(resolve=>{finish=resolve;});
  const elapsed=()=>performance.now()-started;
  const remaining=()=>Math.max(0,MAX_MS-elapsed());
  function fail(code) {if(!errors.includes(code)&&errors.length<16)errors.push(code);finish();}
  function inspect() {
    if(!Object.keys(TOPICS).every(key=>Object.hasOwn(values,key)))return false;
    initialized=true;
    if(values.queued!==0)fail('queued-packets');
    if(values.store!==values.retained)fail('stored-nonretained-messages');
    if(values.disconnected!==0)fail('disconnected-durable-sessions');
    return true;
  }
  function snapshot() {
    const now=elapsed();
    return {metrics:{store:values.store,retained:values.retained,queued:values.queued,disconnected:values.disconnected},
      metricAgesMs:Object.fromEntries(Object.entries(observed).map(([name,row])=>[name,now-row.offsetMs])),
      metricObservations:Object.fromEntries(Object.entries(observed).map(([name,row])=>[name,{...row}]))};
  }
  function receive(topic,payload,packet={}) {
    try {
      if(!Buffer.isBuffer(payload)||typeof topic!=='string'||typeof packet.retain!=='boolean')return fail('malformed-message');
      if(payload.length>MAX_BYTES)return fail('message-byte-bound');
      if(++traffic.messages>MAX_MESSAGES)return fail('message-count-bound');
      traffic.bytes+=payload.length;
      digest.update(JSON.stringify([topic.length,payload.length,packet.retain===true]));
      digest.update(topic);digest.update(payload);
      if(!topic.startsWith('$SYS/')) {
        if(packet.retain===true){traffic.retainedNonSys++;fail('retained-application-message');}
        return;
      }
      const key=NUMERIC.get(topic);
      if(!key)return fail('unexpected-system-topic');
      if(payload.length>64)return fail('malformed-system-metric');
      const text=payload.toString('utf8');
      const match=key==='uptime'?/^(0|[1-9]\d*) seconds$/.exec(text):/^(0|[1-9]\d*)$/.exec(text);
      if(!match)return fail('malformed-system-metric');
      const value=Number(match[1]);if(!Number.isSafeInteger(value))return fail('malformed-system-metric');
      if(key==='uptime'&&Object.hasOwn(values,key)&&value<values[key])return fail('backward-uptime');
      // Initial retained delivery cannot replace a newer live producer update.
      if(packet.retain&&previousUptime!==null)return fail('out-of-phase-retained-metric');
      values[key]=value;observed[key]={offsetMs:elapsed(),retained:packet.retain===true};
      if(Object.keys(TOPICS).every(name=>Object.hasOwn(values,name)))initialized=true;
      // These predicates concern one metric and are unsafe even before a full tuple.
      if(key==='queued'&&value!==0)fail('queued-packets');
      if(key==='disconnected'&&value!==0)fail('disconnected-durable-sessions');
      if(key==='uptime'&&packet.retain!==true) {
        if(previousUptime!==null&&value<=previousUptime)return fail('nonadvancing-uptime');
        previousUptime=value;
        if(subscribed) {
          const marker={at:new Date().toISOString(),offsetMs:elapsed(),uptimeSeconds:value,messageIndex:traffic.messages};
          // sys_tree__update queues uptime BEFORE its changed metric publications.
          // The next live marker closes that complete producer update, in FIFO order.
          if(pendingEpoch) {
            if(!inspect())fail('missing-system-metrics');
            else if(epochs.length<3) {
              epochs.push({...pendingEpoch,closedBy:{...marker},...snapshot()});
              if(epochs.length===3)finish();
            }
          }
          pendingEpoch=marker;
        }
      }
    } catch {fail('observation-error');}
  }
  async function bounded(promise,code) {
    let deadline;
    try {
      const result=await Promise.race([promise,new Promise((_,reject)=>{
        deadline=setTimeout(()=>reject(new Error(code)),remaining());
      })]);
      if(remaining()===0)throw new Error(code);
      return result;
    } finally {clearTimeout(deadline);}
  }
  try {
    address=localEndpoint(endpoint);
    timer=setTimeout(()=>fail('observation-deadline'),remaining());
    client=clientFactory(address,{clientId,clean:true,reconnectPeriod:0,resubscribe:false,
      connectTimeout:5000,queueQoSZero:false});
    client.on('message',receive);
    client.on('error',()=>fail('client-error'));
    client.on('close',()=>{closed=true;if(!closing)fail('unexpected-close');});
    const connection=await bounded(new Promise((resolve,reject)=>{
      client.once('connect',packet=>{connected=true;resolve(packet);});
      client.once('error',()=>reject(new Error('client-error')));
      client.once('close',()=>reject(new Error('unexpected-close')));
    }),'connect-deadline');
    if(connection?.sessionPresent!==false)fail('unexpected-session');
    const grants=await bounded(client.subscribeAsync(FILTERS,{qos:0}),'subscribe-deadline');
    if(!Array.isArray(grants)||grants.length!==FILTERS.length
        ||grants.some((grant,index)=>grant.topic!==FILTERS[index]||grant.qos!==0))fail('subscription-refused');
    else subscribed=true;
    await completed;
    if(!initialized)fail('missing-system-metrics');
    if(epochs.length!==3)fail('missing-live-epochs');
    // Own UNSUBACK is a wire barrier, retaining every earlier observation.
    if(subscribed&&remaining()>0) {
      await bounded(client.unsubscribeAsync(FILTERS),'unsubscribe-deadline');
      // This final partial producer update is validated, never counted as an epoch.
      finalSnapshot={at:new Date().toISOString(),offsetMs:elapsed(),afterOwnUnsuback:true,...snapshot()};
      if(!inspect())fail('missing-system-metrics');
    }
  } catch(error) {fail(error instanceof Error&&['invalid-loopback-endpoint','connect-deadline','subscribe-deadline','unsubscribe-deadline','client-error','unexpected-close'].includes(error.message)?error.message:'observer-error');}
  finally {
    closing=true;
    if(client) {
      try {
        if(remaining()===0)throw new Error('close-deadline');
        await bounded(client.endAsync(false),'close-deadline');closed=true;
      } catch {
        fail('close-failed');forced=true;
        try {client.end(true);client.stream?.destroy();}catch {fail('forced-close-failed');}
      }
    }
    clearTimeout(timer);
  }
  if(!closed)fail('client-not-closed');
  return {version:1,passed:errors.length===0,startedAt,finishedAt:new Date().toISOString(),
    durationMs:elapsed(),maxWindowMs:MAX_MS,endpoint:address??null,clientId,
    requiredTopics:TOPICS,initialized,epochs,finalSnapshot,latestMetrics:values,metricObservations:observed,
    traffic:{...traffic,sha256:digest.digest('hex')},errors,
    cleanup:{connected,subscribed,closed,forced},
    interpretation:'Observed broker aggregate quiescence only; each of three completed live epochs is closed by the next advancing uptime (four live markers). Unchanged SYS gauges remain cached. Final partial update is checked after own UNSUBACK. Caller must bind broker identity, target lock and application drain.'};
}

if(process.argv[1]&&import.meta.url===pathToFileURL(process.argv[1]).href) {
  if(process.argv.length!==2)throw new Error('CLI accepts no endpoint override');
  const result=await observeBrokerQuiescence('mqtt://127.0.0.1:1884');
  console.log(JSON.stringify(result));if(!result.passed)process.exitCode=1;
}
