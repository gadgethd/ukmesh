// Synthetic data only; the owner supplies a separately created loopback broker.
import assert from 'node:assert/strict';
import {createRequire} from 'node:module';
import {pathToFileURL} from 'node:url';
const require=createRequire(new URL('../../backend/package.json',import.meta.url));
const {connect}=require('mqtt');

export async function bounded(promise,message) {
  let timer;
  try {
    return await Promise.race([promise,new Promise((_,reject)=>{
      timer=setTimeout(()=>reject(new Error(message)),6000);
    })]);
  } finally { clearTimeout(timer); }
}

export async function main(argv) {
  const [mode,address,token]=argv,url=new URL(address);
  assert.ok(['prepare','verify'].includes(mode));
  assert.match(token,/^[a-f0-9]{32}$/);
  assert.equal(url.protocol,'mqtt:');assert.equal(url.hostname,'127.0.0.1');
  assert.equal(url.username,'');assert.equal(url.password,'');
  assert.ok(Number(url.port)>1024 && ![1884,1885,9002,9003].includes(Number(url.port)));
  const topic=`ukmesh-owned-broker/${token}`,clients=new Set();
  const messages=Array.from({length:3},(_,i)=>`owned-message-${i}`);
  async function open(id,clean,receive=()=>{}) {
    const client=connect(address,{clientId:id,clean,reconnectPeriod:0,resubscribe:false,connectTimeout:5000});
    clients.add(client);client.on('message',receive);
    const packet=await bounded(new Promise((resolve,reject)=>{
      client.once('connect',resolve);client.once('error',reject);
    }),'owned broker connection deadline');
    return {client,packet};
  }
  async function close(client) {await client.endAsync(false);clients.delete(client);}
  try {
    if(mode==='prepare') {
      const {client,packet}=await open(`persist-${token}`,false);
      assert.equal(packet.sessionPresent,false);
      await client.subscribeAsync(topic+'/queue',{qos:1});await close(client);
      const {client:publisher}=await open(`publisher-${token}`,true);
      for(const message of messages)await publisher.publishAsync(topic+'/queue',message,{qos:1});
      await publisher.publishAsync(topic+'/retained','owned-retained',{qos:1,retain:true});
      await close(publisher);
      console.log(JSON.stringify({passed:true,queued_qos1:messages.length,retained_published:1,all_clients_closed:clients.size===0}));
    } else {
      const received=[];
      let ready;const complete=new Promise(resolve=>{ready=resolve;});
      const {client,packet}=await open(`persist-${token}`,false,(name,body,wire)=>{
        received.push({topic:name,value:body.toString(),qos:wire.qos});
        if(received.length>=messages.length)ready();
      });
      assert.equal(packet.sessionPresent,true,'durable session must survive restart');
      await bounded(complete,'saved queue missing');
      await bounded(client.unsubscribeAsync(topic+'/queue'),'queue unsubscribe deadline');
      await close(client);
      assert.deepEqual(received,messages.map(value=>({topic:topic+'/queue',value,qos:1})));
      let found;const retained=new Promise(resolve=>{found=resolve;});
      const {client:reader}=await open(`retained-${token}`,true,(name,body,wire)=>found({topic:name,value:body.toString(),retained:wire.retain}));
      await reader.subscribeAsync(topic+'/retained',{qos:1});
      const value=await bounded(retained,'saved retained message missing');
      assert.deepEqual(value,{topic:topic+'/retained',value:'owned-retained',retained:true});
      await close(reader);
      console.log(JSON.stringify({passed:true,session_present:true,queued_qos1_restored:received.length,retained_restored:1,all_clients_closed:clients.size===0}));
    }
  } finally {
    for(const client of clients)await client.endAsync(true);
  }
}

if(process.argv[1] && import.meta.url===pathToFileURL(process.argv[1]).href)await main(process.argv.slice(2));
