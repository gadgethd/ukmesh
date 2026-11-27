import { createReadStream } from 'node:fs';
import { readFile, rename, writeFile } from 'node:fs/promises';
import { createGunzip } from 'node:zlib';
import { createInterface } from 'node:readline';
import { setTimeout as sleep } from 'node:timers/promises';
import mqtt from 'mqtt';

const speed = Number(process.env.REPLAY_SPEED);
const interval = 1000 / Number(process.env.REPLAY_RATE);
if (!Number.isFinite(speed) || speed <= 0 || !Number.isFinite(interval) || interval < 20) {
  throw new Error('Invalid archive replay pace');
}
const directory = '/progress';
const manifest = JSON.parse(await readFile('/archive/manifest.json', 'utf-8'));
const client = mqtt.connect('mqtt://broker:1883', { clientId: 'ukmesh-archive-player', reconnectPeriod: 1500 });
let bridge = false;
let cursor = 0;
let cycle = 0;
try {
  const state = JSON.parse(await readFile(`${directory}/state.json`, 'utf-8'));
  if (state.archive === manifest.digest) {
    cursor = state.cursor;
    cycle = state.cycle;
  }
} catch (error) {
  if (error.code !== 'ENOENT') throw error;
}

client.on('connect', () => client.subscribe('local/archive/bridge', { qos: 1 }));
client.on('message', (_, payload) => { bridge = payload.toString() === '1'; });
client.on('close', () => { bridge = false; });
client.on('error', () => { bridge = false; });

async function save() {
  const state = { archive: manifest.digest, cycle, cursor, updatedAt: new Date().toISOString(), connected: bridge };
  await writeFile(`${directory}/next.json`, JSON.stringify(state), { mode: 0o600 });
  await rename(`${directory}/next.json`, `${directory}/state.json`);
}

let stopping = false;
for (const signal of ['SIGINT', 'SIGTERM']) process.on(signal, () => { stopping = true; });

while (!stopping) {
  const rows = createInterface({ input: createReadStream('/archive/events.jsonl.gz').pipe(createGunzip()), crlfDelay: Infinity });
  let rowNumber = 0;
  let previousAt;
  for await (const line of rows) {
    if (stopping) break;
    if (++rowNumber <= cursor) continue;
    const [at, topic, payload] = JSON.parse(line);
    if (previousAt !== undefined) await sleep(Math.max(interval, (at - previousAt) / speed));
    previousAt = at;
    while (!bridge && !stopping) await sleep(250);
    if (stopping) break;
    payload.timestamp = new Date().toISOString();
    await client.publishAsync(topic, JSON.stringify(payload), { qos: 1, retain: false });
    cursor = rowNumber;
    if (cursor % 32 === 0) await save();
  }
  if (stopping) break;
  if (rowNumber !== manifest.events) throw new Error('Archive event count changed');
  cycle++;
  cursor = 0;
  await save();
  await sleep(interval);
}
await save();
await client.endAsync();
