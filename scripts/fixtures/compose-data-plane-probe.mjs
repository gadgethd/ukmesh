import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import { setTimeout as delay } from 'node:timers/promises';
import WebSocket from 'ws';
import { connectAsync } from 'mqtt';

const { MQTT_E2E_OBSERVER: observer, MQTT_E2E_PUBLISHER: username, MQTT_E2E_PASSWORD: password } = process.env;
const [mode, nonce, firstArgument, countArgument] = process.argv.slice(2);
const observerId = observer?.toLowerCase();
assert.match(observerId ?? '', /^[a-f0-9]{64}$/);

function createPackets() {
  assert.match(nonce ?? '', /^[a-f0-9]{8}$/);
  const first = Number(firstArgument), count = Number(countArgument);
  assert.ok(Number.isSafeInteger(first) && first >= 0 && first < 256);
  assert.ok(Number.isSafeInteger(count) && count >= 1 && first + count <= 256);
  return Array.from({ length: count }, (_, index) => {
    const ack = Buffer.alloc(4);
    ack.writeUInt32BE((Number.parseInt(nonce, 16) + first + index) >>> 0);
    // An ACK on the existing two-hop, two-byte path. Its identity covers
    // the payload type/version and the four-byte ACK value.
    return {
      raw: `0D42AABBCCDD${ack.toString('hex').toUpperCase()}`,
      hash: createHash('sha256').update(Buffer.from([0x0c])).update(ack).digest('hex').toUpperCase(),
    };
  });
}

function withFeed(onPacket, action) {
  const socket = new WebSocket('ws://website-ukmesh:8080/ws?network=test');
  let acceptInitial, rejectInitial;
  const initial = new Promise((resolve, reject) => { acceptInitial = resolve; rejectInitial = reject; });
  socket.once('error', rejectInitial);
  socket.once('close', () => rejectInitial(new Error('WebSocket closed before initial state')));
  socket.on('message', raw => {
    for (const line of String(raw).split('\n').filter(Boolean)) {
      const message = JSON.parse(line);
      if (message.type === 'initial_state') acceptInitial(message.data);
      if (message.type === 'packet') onPacket(message.data);
    }
  });
  return initial.then(action).finally(() => socket.terminate());
}

async function publish(packets, seedObserver) {
  assert.ok(username && password, 'owned broker publisher credentials required');
  const client = await connectAsync('ws://mosquitto:9001', {
    username, password, reconnectPeriod: 0, connectTimeout: 10_000,
  });
  const topic = `meshcore-test/CI/${observer}`;
  try {
    if (seedObserver) {
      await client.publishAsync(`${topic}/status`, JSON.stringify({
        origin: 'CI E2E observer', origin_id: observer, stats: { battery_mv: 4100, uptime_secs: 60 },
      }), { qos: 1 });
    }
    for (const { raw } of packets) {
      await client.publishAsync(`${topic}/packets`, JSON.stringify({
        packet_type: 3, RSSI: -75, SNR: 8, direction: 'rx', raw,
      }), { qos: 1 });
      if (packets.length > 1) await delay(25);
    }
    process.stdout.write(JSON.stringify(packets.map(({ hash }) => hash)) + '\n');
  } finally {
    await client.endAsync();
  }
}

const deadline = setTimeout(() => {
  console.error(`timed out in data-plane probe ${mode}`);
  process.exit(1);
}, 30_000);
try {
  if (mode === 'api') {
    const response = await fetch('http://website-ukmesh:8080/api/runtime-config', { signal: AbortSignal.timeout(5000) });
    assert.ok(response.ok, 'public runtime config should remain available with ingest stopped');
    assert.ok(await response.json());
    const ready = await fetch('http://backend:3000/readyz', { signal: AbortSignal.timeout(5000) });
    assert.equal(ready.status, 503, 'aggregate readiness should report the stopped ingest service');
    await withFeed(() => {}, snapshot => assert.ok(Array.isArray(snapshot.packets)));
  } else if (mode === 'publish') {
    await publish(createPackets(), false);
  } else if (mode === 'fanout') {
    const packets = createPackets();
    assert.equal(packets.length, 1);
    let acceptPacket;
    const packet = new Promise(resolve => { acceptPacket = resolve; });
    await withFeed(message => {
      if (String(message?.rxNodeId ?? '').toLowerCase() === observerId && message.packetHash === packets[0].hash) {
        acceptPacket();
      }
    }, async () => {
      await publish(packets, true);
      await packet;
    });
  } else {
    assert.fail('expected api, publish or fanout mode');
  }
} finally {
  clearTimeout(deadline);
}
