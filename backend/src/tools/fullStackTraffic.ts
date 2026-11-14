/** Invented MeshCore traffic; wire layout follows MeshCore docs/payloads.md,
 * Mesh::createAdvert and Utils::encryptThenMAC. No received traffic is reused. */
import assert from 'node:assert/strict';
import { createCipheriv, createHash, createHmac, createPrivateKey, createPublicKey, randomUUID, sign } from 'node:crypto';
import { mkdir, readFile, writeFile } from 'node:fs/promises';
import path from 'node:path';
import { pathToFileURL } from 'node:url';
import { setTimeout as wait } from 'node:timers/promises';
import mqtt from 'mqtt';
import { MeshCoreDecoder } from '@michaelhart/meshcore-decoder';
import { PUBLIC_CHANNELS } from '../mqtt/channelRegistry.js';
import { decodePacketCompat } from '../mqtt/decodePacket.js';
import { assertOwnedLeanFixture } from './ownedLookupFixture.js';
import { option, requiredOption } from './sampleData.js';

const channel = PUBLIC_CHANNELS.find(entry => entry.name.toLowerCase().replace(/^#/, '') === 'test');
assert.ok(channel, 'public test channel must exist');
const channelKey = Buffer.from(channel.secret, 'hex');
const keyStore = MeshCoreDecoder.createKeyStore({ channelSecrets: [channel.secret] });

type TrafficEvent = { offsetMs: number; topic: string; envelope: Record<string, unknown> };
export type FullStackTraffic = {
  version: 1; synthetic: true; seed: string; at: string; cycles: number; intervalMs: number; healthCode?: string;
  nodeIds: string[]; observerIds: string[]; statusObserverId: string; ukmeshHashes: string[]; radioHashes: string[]; channelHashes: string[];
  events: TrafficEvent[];
  expected: { ukmesh: { packets: number; paths: number; facts: number; ledger: number; statuses: number };
    corescope: { transmissions: number; observations: number; statuses: number };
    beacon: { packets: number; observations: number; messages: number; statuses: number } };
};

function identity(seed: string, index: number) {
  const bytes = createHash('sha256').update(`UKMesh invented qualification actor:${seed}:${index}`).digest();
  const key = createPrivateKey({ key: Buffer.concat([Buffer.from('302e020100300506032b657004220420', 'hex'), bytes]),
    format: 'der', type: 'pkcs8' });
  const publicKey = createPublicKey(key).export({ format: 'der', type: 'spki' }).subarray(-32);
  return { key, publicKey };
}

export function signedAdvert(seed: string, index: number, epoch: number): { raw: string; nodeId: string } {
  const actor = identity(seed, index);
  const timestamp = Buffer.alloc(4); timestamp.writeUInt32LE(epoch);
  const position = Buffer.alloc(8);
  position.writeInt32LE(Math.round((51.5 + index * .01) * 1e6), 0);
  position.writeInt32LE(Math.round((-.1 + index * .01) * 1e6), 4);
  const app = Buffer.concat([Buffer.from([0x92]), position, Buffer.from(`Lean ${index} ${seed.slice(-6)}`)]);
  const signature = sign(null, Buffer.concat([actor.publicKey, timestamp, app]), actor.key);
  return { raw: Buffer.concat([Buffer.from([0x11, 0]), actor.publicKey, timestamp, signature, app]).toString('hex').toUpperCase(),
    nodeId: actor.publicKey.toString('hex').toUpperCase() };
}

export function encryptedTestMessage(text: string, epoch: number): string {
  const timestamp = Buffer.alloc(4); timestamp.writeUInt32LE(epoch);
  const plain = Buffer.concat([timestamp, Buffer.from([0]), Buffer.from('Qualification: ' + text)]);
  const padded = Buffer.alloc(Math.ceil(plain.length / 16) * 16); plain.copy(padded);
  const cipher = createCipheriv('aes-128-ecb', channelKey, null); cipher.setAutoPadding(false);
  const encrypted = Buffer.concat([cipher.update(padded), cipher.final()]);
  const secret = Buffer.alloc(32); channelKey.copy(secret);
  const mac = createHmac('sha256', secret).update(encrypted).digest().subarray(0, 2);
  const hash = createHash('sha256').update(channelKey).digest().subarray(0, 1);
  return Buffer.concat([Buffer.from([0x15, 0]), hash, mac, encrypted]).toString('hex').toUpperCase();
}

function hashes(raw: string) {
  const decoded = decodePacketCompat(raw, keyStore);
  assert.ok(decoded.metadataValid && decoded.canonicalPacketId, 'invented packet must decode');
  const radioHash = createHash('sha256').update(Buffer.from([decoded.decoded!.payloadType]))
    .update(Buffer.from(raw.slice(4), 'hex')).digest('hex').slice(0, 16);
  return { ukmesh: decoded.canonicalPacketId, radio: radioHash };
}

export function buildFullStackTraffic(seed: string, at: string, cycles: number, intervalMs: number,
  healthCode?: string): FullStackTraffic {
  assert.match(seed, /^[a-zA-Z0-9_-]{1,48}$/);
  assert.ok(Number.isSafeInteger(cycles) && cycles >= 1 && cycles <= 120);
  assert.ok(Number.isSafeInteger(intervalMs) && intervalMs >= 1000 && intervalMs <= 60000);
  assert.ok(Number.isFinite(Date.parse(at)));
  if (healthCode) assert.match(healthCode, /^[a-zA-Z0-9_-]{1,64}$/);
  const observers = [2, 3].map(index => identity(seed, index).publicKey.toString('hex').toUpperCase());
  const result: FullStackTraffic = { version: 1, synthetic: true, seed, at, cycles, intervalMs, healthCode,
    nodeIds: [], observerIds: observers, statusObserverId: observers[0]!, ukmeshHashes: [], radioHashes: [], channelHashes: [], events: [],
    expected: { ukmesh: { packets: cycles * 4, paths: 0, facts: 0, ledger: cycles * 4, statuses: cycles },
      corescope: { transmissions: cycles * 3, observations: cycles * 4, statuses: cycles },
      beacon: { packets: cycles * 3, observations: cycles * 4, messages: cycles, statuses: cycles } } };
  const packet = (raw: string, observer: string, offsetMs: number) => {
    result.events.push({ offsetMs, topic: `meshcore/LHR/${observer}/packets`, envelope: {
      raw, direction: 'rx', type: 'PACKET', origin_id: observer, origin: 'Qualification observer', RSSI: -80, SNR: 8 } });
  };
  for (let cycle = 0; cycle < cycles; cycle++) {
    const offset = cycle * intervalMs;
    const epoch = Math.floor((Date.parse(at) + offset) / 1000);
    for (let actor = 0; actor < 2; actor++) {
      const advert = signedAdvert(seed, actor, epoch);
      if (!cycle) result.nodeIds.push(advert.nodeId);
      const proof = hashes(advert.raw);
      result.ukmeshHashes.push(proof.ukmesh); result.radioHashes.push(proof.radio);
      packet(advert.raw, observers[0]!, offset + actor * 50);
    }
    const message = encryptedTestMessage(cycle === 0 && healthCode ? healthCode : `DEV qualification ${seed} ${cycle}`, epoch);
    const proof = hashes(message);
    result.ukmeshHashes.push(proof.ukmesh); result.radioHashes.push(proof.radio); result.channelHashes.push(proof.ukmesh);
    packet(message, observers[0]!, offset + 100);
    packet(message, observers[0]!, offset + 150); // Deliberate same-observer redelivery.
    packet(message, observers[1]!, offset + 200);
    result.events.push({ offsetMs: offset + 250, topic: `meshcore/LHR/${observers[0]}/status`, envelope: {
      status: 'online', origin: 'Qualification observer', origin_id: observers[0], radio: '868.1,62.5,7,5',
      model: 'Invented DEV observer', firmware_version: 'qualification', stats: { battery_mv: 4100,
        uptime_secs: 3600 + cycle, tx_air_secs: 10 + cycle, rx_air_secs: 20 + cycle } } });
  }
  return result;
}

export async function readFullStackTraffic(file: string): Promise<FullStackTraffic> {
  const input = JSON.parse(await readFile(file, 'utf8')) as FullStackTraffic;
  const expected = buildFullStackTraffic(input.seed, input.at, input.cycles, input.intervalMs, input.healthCode);
  assert.deepEqual(input, JSON.parse(JSON.stringify(expected)), 'traffic must match its invented deterministic manifest');
  return input;
}

export async function publishFullStackTraffic(traffic: FullStackTraffic, broker: string,
  signal?: AbortSignal): Promise<number> {
  const endpoint = new URL(broker);
  assert.ok(['localhost', '127.0.0.1', '[::1]'].includes(endpoint.hostname), 'owned loopback broker required');
  assert.ok(['9002', '1884'].includes(endpoint.port), 'full-stack fixture broker port required');
  const client = mqtt.connect(broker, { clientId: 'ukmesh-full-qualification-' + randomUUID(), reconnectPeriod: 0,
    connectTimeout: 10000, clean: true });
  await new Promise<void>((resolve, reject) => { client.once('connect', () => resolve()); client.once('error', reject); });
  const cancel = () => { client.end(true); };
  signal?.addEventListener('abort', cancel, { once: true });
  const started = performance.now();
  let sent = 0;
  try {
    for (const event of traffic.events) {
      signal?.throwIfAborted();
      const delay = event.offsetMs - (performance.now() - started);
      if (delay > 0) await wait(delay, undefined, { signal });
      let timer: ReturnType<typeof setTimeout> | undefined;
      try {
        await Promise.race([
          client.publishAsync(event.topic, JSON.stringify({ ...event.envelope, timestamp: new Date().toISOString() }), { qos: 1 }),
          new Promise<never>((_resolve, reject) => { timer = setTimeout(() => reject(new Error('auxiliary MQTT ACK timed out')), 15000); }),
        ]);
      } finally { if (timer) clearTimeout(timer); }
      sent++;
    }
  } finally { signal?.removeEventListener('abort', cancel); await client.endAsync(true); }
  return sent;
}

async function main() {
  const args = process.argv.slice(2);
  const output = path.resolve(requiredOption(args, 'output'));
  const traffic = buildFullStackTraffic(option(args, 'seed') ?? randomUUID(), option(args, 'as-of') ?? new Date().toISOString(),
    Number(option(args, 'cycles') ?? 1), Number(option(args, 'interval-ms') ?? 1000), option(args, 'health-code'));
  await mkdir(output, { recursive: true, mode: 0o700 });
  await writeFile(path.join(output, 'traffic.json'), JSON.stringify(traffic, null, 2) + '\n', { flag: 'wx', mode: 0o600 });
  if (args.includes('--generate-only')) return;
  await assertOwnedLeanFixture(process.env['DATABASE_URL'] ?? '');
  const brokerAcknowledgements = await publishFullStackTraffic(traffic, option(args, 'broker') ?? 'ws://127.0.0.1:9002');
  await writeFile(path.join(output, 'published.json'), JSON.stringify({ brokerAcknowledgements,
    expected: traffic.expected, note: 'Broker ACKs are not application commit proof.' }, null, 2) + '\n', { flag: 'wx', mode: 0o600 });
  console.log(JSON.stringify({ brokerAcknowledgements, output }));
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  main().catch(error => { console.error(error instanceof Error ? error.message : 'traffic failed'); process.exitCode = 1; });
}
