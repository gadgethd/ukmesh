import mqtt from 'mqtt';
import { WebSocket } from 'ws';
import { randomBytes } from 'node:crypto';

type Options = {
  baseUrl: string;
  durationSeconds: number;
  concurrency: number;
  mqttMessages: number;
  slowWsClients: number;
  maxP95Ms: number;
};

function optionValue(name: string): string | undefined {
  const index = process.argv.indexOf(name);
  return index >= 0 ? process.argv[index + 1] : undefined;
}

const options: Options = {
  baseUrl: (optionValue('--base-url') ?? process.env['LOAD_BASE_URL'] ?? 'http://localhost:3000').replace(/\/$/, ''),
  durationSeconds: Math.max(1, Number(optionValue('--duration') ?? 15)),
  concurrency: Math.max(1, Number(optionValue('--concurrency') ?? 20)),
  mqttMessages: Math.max(0, Number(optionValue('--mqtt-messages') ?? 0)),
  slowWsClients: Math.max(0, Number(optionValue('--slow-ws-clients') ?? 0)),
  maxP95Ms: Math.max(1, Number(optionValue('--max-p95-ms') ?? 1_500)),
};

function percentile(values: number[], fraction: number): number {
  if (values.length === 0) return 0;
  const sorted = [...values].sort((a, b) => a - b);
  return sorted[Math.min(sorted.length - 1, Math.floor(sorted.length * fraction))] ?? 0;
}

async function runHttpLoad(): Promise<{ requests: number; errors: number; p95Ms: number }> {
  const endAt = Date.now() + options.durationSeconds * 1_000;
  const latencies: number[] = [];
  let errors = 0;
  const worker = async () => {
    while (Date.now() < endAt) {
      const started = performance.now();
      try {
        const response = await fetch(`${options.baseUrl}/api/stats?network=ukmesh`);
        if (!response.ok) errors += 1;
        await response.arrayBuffer();
      } catch {
        errors += 1;
      }
      latencies.push(Math.round(performance.now() - started));
    }
  };
  await Promise.all(Array.from({ length: options.concurrency }, () => worker()));
  return { requests: latencies.length, errors, p95Ms: percentile(latencies, 0.95) };
}

async function publishMqttBurst(): Promise<number> {
  if (options.mqttMessages === 0) return 0;
  const brokerUrl = process.env['MQTT_BROKER_URL'] ?? 'ws://localhost:9001';
  const client = mqtt.connect(brokerUrl, {
    username: process.env['MQTT_USERNAME'],
    password: process.env['MQTT_PASSWORD'],
    reconnectPeriod: 0,
  });
  await new Promise<void>((resolve, reject) => {
    const timer = setTimeout(() => reject(new Error('MQTT connection timeout')), 10_000);
    client.once('connect', () => { clearTimeout(timer); resolve(); });
    client.once('error', reject);
  });
  const observer = 'F'.repeat(64);
  for (let index = 0; index < options.mqttMessages; index += 1) {
    const payload = JSON.stringify({
      raw: randomBytes(64).toString('hex'),
      hash: randomBytes(16).toString('hex'),
      packet_type: '5',
      direction: 'rx',
    });
    client.publish(`meshcore-test/TST/${observer}/packets`, payload, { qos: 0 });
  }
  await new Promise<void>((resolve, reject) => client.end(false, {}, (error) => (error ? reject(error) : resolve())));
  return options.mqttMessages;
}

async function holdSlowWebSockets(): Promise<number> {
  if (options.slowWsClients === 0) return 0;
  const wsUrl = options.baseUrl.replace(/^http/, 'ws');
  const clients = await Promise.all(Array.from({ length: options.slowWsClients }, () => new Promise<WebSocket>((resolve, reject) => {
    const socket = new WebSocket(`${wsUrl}/ws?network=ukmesh`);
    const timer = setTimeout(() => reject(new Error('WebSocket connection timeout')), 10_000);
    socket.once('open', () => {
      clearTimeout(timer);
      const internalSocket = (socket as unknown as { _socket?: { pause: () => void } })._socket;
      internalSocket?.pause();
      resolve(socket);
    });
    socket.once('error', reject);
  })));
  await new Promise((resolve) => setTimeout(resolve, options.durationSeconds * 1_000));
  for (const socket of clients) socket.terminate();
  return clients.length;
}

async function main(): Promise<void> {
  console.log('[load] options', options);
  const [http, mqttPublished, slowWsClients] = await Promise.all([
    runHttpLoad(),
    publishMqttBurst(),
    holdSlowWebSockets(),
  ]);
  const result = { ...http, mqttPublished, slowWsClients };
  console.log('[load] result', result);
  const errorRate = http.requests > 0 ? http.errors / http.requests : 1;
  if (errorRate > 0.01 || http.p95Ms > options.maxP95Ms) process.exitCode = 1;
}

main().catch((err) => {
  console.error('[load] failed', err);
  process.exit(1);
});
