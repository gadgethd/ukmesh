import assert from 'node:assert/strict';
import WebSocket from 'ws';
import { connectAsync } from 'mqtt';

const observer = process.env.MQTT_E2E_OBSERVER;
const observerId = observer?.toLowerCase();
assert.match(observerId ?? '', /^[a-f0-9]{64}$/);
const socket = new WebSocket('ws://website-ukmesh:8080/ws?network=test');
let client;
const deadline = setTimeout(() => {
  console.error('timed out proving MQTT persistence and WebSocket fanout');
  process.exit(1);
}, 15_000);
let initialResolve, packetResolve, fail;
const initial = new Promise((resolve, reject) => { initialResolve = resolve; fail = reject; });
const packet = new Promise((resolve) => { packetResolve = resolve; });
socket.on('error', error => fail(error));
socket.on('message', raw => {
  for (const line of String(raw).split('\n').filter(Boolean)) {
    const message = JSON.parse(line);
    if (message.type === 'initial_state') initialResolve();
    if (message.type === 'packet' && String(message.data?.rxNodeId ?? '').toLowerCase() === observerId) packetResolve();
  }
});
try {
  await initial;
  client = await connectAsync('ws://mosquitto:9001', {
    username: process.env.MQTT_E2E_PUBLISHER, password: process.env.MQTT_E2E_PASSWORD,
    reconnectPeriod: 0, connectTimeout: 10_000,
  });
  await client.publishAsync(`meshcore-test/CI/${observer}/status`, JSON.stringify({
    origin: 'CI E2E observer', origin_id: observer, stats: { battery_mv: 4100, uptime_secs: 60 },
  }), { qos: 1 });
  await client.publishAsync(`meshcore-test/CI/${observer}/packets`, JSON.stringify({
    packet_type: 1, RSSI: -75, SNR: 8, direction: 'rx', raw: '0D42AABBCCDDDEADBEEF',
  }), { qos: 1 });
  await packet;
  console.log('mqtt-packet-fanout-ok');
} finally {
  clearTimeout(deadline);
  socket.terminate();
  if (client) await client.endAsync();
}
