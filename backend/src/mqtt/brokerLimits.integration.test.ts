import assert from 'node:assert/strict';
import test from 'node:test';
import net from 'node:net';
import mqttPacket from 'mqtt-packet';

const endpoint = process.env['TEST_MQTT_BROKER_URL'];

test('owned broker accepts normal QoS 1 traffic and closes oversized frames without PUBACK', {
  skip: endpoint ? false : 'TEST_MQTT_BROKER_URL must point to an owned broker with the candidate limits',
  timeout: 10_000,
}, async (t) => {
  const url = new URL(endpoint!);
  assert.equal(url.protocol, 'mqtt:');
  assert.ok(['127.0.0.1', 'localhost'].includes(url.hostname));
  const socket = net.connect(Number(url.port), url.hostname);
  const parser = mqttPacket.parser();
  const acknowledgements: number[] = [];
  let connected = false;
  t.after(() => socket.destroy());
  const closed = new Promise<void>((resolve, reject) => {
    socket.on('error', (error: NodeJS.ErrnoException) => {
      if (error.code !== 'ECONNRESET' && error.code !== 'EPIPE') reject(error);
    });
    socket.on('close', () => resolve());
    parser.on('error', reject);
  });
  socket.on('data', (data) => parser.parse(data));
  parser.on('packet', (packet) => {
    if (packet.cmd === 'connack') {
      assert.equal(packet.returnCode, 0);
      connected = true;
      socket.write(mqttPacket.generate({ cmd: 'publish', topic: 'owned/normal',
        payload: Buffer.alloc(8_192), qos: 1, messageId: 1, dup: false, retain: false }));
    }
    if (packet.cmd === 'puback') {
      acknowledgements.push(packet.messageId);
      if (packet.messageId === 1) {
        socket.write(mqttPacket.generate({ cmd: 'publish', topic: 'owned/oversized',
          payload: Buffer.alloc(2 * 1_048_576), qos: 1, messageId: 2, dup: false, retain: false }));
      }
    }
  });
  socket.on('connect', () => socket.write(mqttPacket.generate({ cmd: 'connect',
    protocolId: 'MQTT', protocolVersion: 4, clean: true, clientId: `owned-limit-test-${process.pid}`, keepalive: 5 })));
  await closed;
  assert.ok(connected);
  assert.deepEqual(acknowledgements, [1]);
});

test('owned broker preserves a thirty-minute measured-rate backlog and exposes finite byte overflow', {
  skip: endpoint ? false : 'owned candidate broker required', timeout: 90000,
}, async () => {
  const { connectAsync } = await import('mqtt');
  const url = new URL(endpoint!);
  assert.ok(['127.0.0.1','localhost'].includes(url.hostname));
  const prefix = `owned-capacity-${process.pid}-${Date.now()}`;
  const publisher = await connectAsync(endpoint!, {clientId: `${prefix}-publisher`, reconnectPeriod: 0});
  const until = async (predicate:()=>boolean, ms=15000) => {
    const deadline=Date.now()+ms;
    while(!predicate()) {if(Date.now()>deadline)throw new Error('owned broker delivery deadline');await new Promise(r=>setTimeout(r,25));}
  };
  try {
    for (const [name,count,bytes,expectAll] of [['outage',6000,600,true],['overflow',400,131072,false]] as const) {
      const id = `${prefix}-${name}`, topic = `owned/${id}`;
      let subscriber=await connectAsync(endpoint!, {clientId:id,clean:false,reconnectPeriod:0});
      await subscriber.subscribeAsync(topic,{qos:1});await subscriber.endAsync();
      for(let i=0;i<count;i++) {
        const payload=Buffer.alloc(bytes);payload.writeUInt32BE(i);
        await publisher.publishAsync(topic,payload,{qos:1});
      }
      subscriber=await connectAsync(endpoint!, {clientId:id,clean:false,reconnectPeriod:0});
      const received:number[]=[];subscriber.on('message',(_topic,payload)=>received.push(payload.readUInt32BE(0)));
      try {
        if(expectAll) {await until(()=>received.length===count);assert.deepEqual(received,Array.from({length:count},(_,i)=>i));}
        else {await until(()=>received.length>=240);await new Promise(r=>setTimeout(r,1200));assert.ok(received.length<270,`byte bound failed: ${received.length}`);assert.equal(new Set(received).size,received.length);}
        console.log(JSON.stringify({scenario:name,published:count,payloadBytes:bytes,received:received.length}));
      } finally {await subscriber.endAsync();const clean=await connectAsync(endpoint!,{clientId:id,clean:true,reconnectPeriod:0});await clean.endAsync();}
    }
    const monitor=await connectAsync(endpoint!,{clientId:`${prefix}-monitor`,reconnectPeriod:0});
    let dropped=0;monitor.on('message',(_topic,payload)=>{dropped=Number(payload.toString());});
    try {await monitor.subscribeAsync('$SYS/broker/publish/messages/dropped');await until(()=>dropped>0,5000);assert.ok(dropped>=130);console.log(JSON.stringify({droppedCounter:dropped}));}
    finally {await monitor.endAsync();}
  } finally {await publisher.endAsync();}
});
