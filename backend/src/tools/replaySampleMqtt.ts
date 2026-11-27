/** Replay the local UKMesh sample dataset into MQTT.
 *
 * Usage:
 *   npm run sample:replay -- --data-dir ../data/ukmesh-sample-2026-11-10 \
 *     --broker ws://127.0.0.1:9001 --speed 0
 *
 * Packet mode republishes each reception as the observer JSON envelope
 * (raw bytes plus wire metadata) on its original topic. Status mode
 * reconstructs status envelopes from node telemetry.
 *
 * Safety: refuses non-loopback brokers unless --allow-remote is passed. Never
 * logs payload contents.
 */
import mqtt, { type MqttClient } from 'mqtt';
import path from 'node:path';
import { pathToFileURL } from 'node:url';
import {
  csvJson,
  defaultDataDir,
  flag,
  gzipCsvRecords,
  option,
  parseIntOption,
  parseTimeMs,
  readGzipCsv,
  rowObject,
} from './sampleData.js';

type Row = Record<string, string>;
type Publish = { atMs: number; topic: string; payload: Buffer; label: string };
export type ReplayEnvelopeFormat = 'legacy' | 'observer';

const DEFAULT_BROKER = 'ws://127.0.0.1:9002';

function rowsOf(table: { headers: string[]; rows: string[][] }): Row[] {
  return table.rows.map((row) => rowObject(table.headers, row));
}

function isLoopback(url: URL): boolean {
  return url.hostname === 'localhost'
    || url.hostname === '127.0.0.1'
    || url.hostname === '::1'
    || url.hostname === '[::1]';
}

function rewritePrefix(topic: string, prefix: string | undefined): string {
  if (!prefix) return topic;
  const parts = topic.split('/');
  parts[0] = prefix;
  return parts.join('/');
}

function sleep(ms: number): Promise<void> {
  return ms <= 0 ? Promise.resolve() : new Promise((resolve) => setTimeout(resolve, ms));
}

async function connect(broker: string, username?: string, password?: string): Promise<MqttClient> {
  const client = mqtt.connect(broker, {
    clientId: `ukmesh-sample-replay-${process.pid}`,
    username,
    password,
    reconnectPeriod: 0,
    connectTimeout: 10_000,
    clean: true,
  });
  await new Promise<void>((resolve, reject) => {
    const timer = setTimeout(() => reject(new Error('MQTT connect timed out')), 15_000);
    client.once('connect', () => {
      clearTimeout(timer);
      resolve();
    });
    client.once('error', (error) => {
      clearTimeout(timer);
      reject(error);
    });
  });
  return client;
}

async function publishAll(
  client: MqttClient,
  publishes: readonly Publish[] | AsyncIterable<Publish>,
  count: number,
  speed: number,
  qos: 0 | 1,
  concurrency: number,
  format: ReplayEnvelopeFormat,
): Promise<{ sent: number; failed: number }> {
  let sent = 0;
  let failed = 0;
  let index = 0;
  const started = Date.now();

  const send = async (publish: Publish): Promise<void> => {
    try {
      const payload = format === 'observer'
        ? Buffer.from(JSON.stringify({ ...JSON.parse(publish.payload.toString()), timestamp: new Date().toISOString() }))
        : publish.payload;
      await client.publishAsync(publish.topic, payload, { qos });
      sent += 1;
    } catch (error) {
      failed += 1;
      if (failed <= 10) {
        console.error(`[sample:replay] publish failed (${publish.label}):`, (error as Error).message);
      }
    }
    if ((sent + failed) % 2000 === 0) {
      const elapsed = Math.max(1, Date.now() - started);
      console.log(`[sample:replay] ${sent + failed}/${count} (${Math.round((sent + failed) / (elapsed / 1000))}/s)`);
    }
  };

  if (speed <= 0) {
    if (!('length' in publishes)) throw new Error('streamed replay requires a positive speed');
    const workers = Array.from({ length: Math.max(1, concurrency) }, async () => {
      while (index < publishes.length) {
        const publish = publishes[index]!;
        index += 1;
        await send(publish);
      }
    });
    await Promise.all(workers);
    return { sent, failed };
  }

  let firstAt: number | undefined;
  for await (const publish of publishes) {
    firstAt ??= publish.atMs;
    const target = started + (publish.atMs - firstAt) / speed;
    await sleep(target - Date.now());
    await send(publish);
  }
  return { sent, failed };
}

export function packetReplayEnvelope(row: Row, rawHex: string, format: ReplayEnvelopeFormat,
  receivedAt = new Date().toISOString()): Record<string, unknown> {
  const envelope: Record<string, unknown> = { raw: rawHex, direction: 'rx' };
  if (format === 'observer') {
    envelope['type'] = 'PACKET';
    envelope['timestamp'] = receivedAt;
    envelope['origin_id'] = row['rx_node_id'];
  }
  for (const field of ['packet_type', 'route_type', 'hop_count', 'path_hash_size_bytes'] as const) {
    const value = Number(row[field]);
    if (Number.isFinite(value)) envelope[field] = format === 'observer' ? String(value) : value;
  }
  for (const [field, key] of [['rssi', 'RSSI'], ['snr', 'SNR']] as const) {
    const value = Number(row[field]);
    if (row[field] && Number.isFinite(value)) envelope[key] = value;
  }
  return envelope;
}

async function* replayRows(file: string, required?: readonly string[]): AsyncGenerator<Row> {
  let headers: string[] | undefined;
  for await (const row of gzipCsvRecords(file)) {
    if (headers) {
      if (required && row.length !== headers.length) throw new Error('paired replay CSV width mismatch');
      yield rowObject(headers, row);
    } else {
      if (required?.some(key => row.indexOf(key) < 0 || row.indexOf(key) !== row.lastIndexOf(key))) {
        throw new Error('paired replay requires unique input columns');
      }
      headers = row;
    }
  }
}

/** Owned soak inputs are written together and replayed in their verified order. */
export async function* pairedReplayPackets(directory: string, count: number) {
  const keys = ['time', 'packet_hash', 'rx_node_id'];
  const packets = replayRows(path.join(directory, 'packets.csv.gz'), [...keys, 'topic', 'network']);
  const rawPackets = replayRows(path.join(directory, 'raw_packets.csv.gz'), [...keys, 'raw_hex']);
  let previousTime = -Infinity;
  try {
    for (let index = 0; index < count; index++) {
      const [packet, raw] = await Promise.all([packets.next(), rawPackets.next()]);
      if (packet.done || raw.done) throw new Error('paired replay requires every requested row');
      const row = packet.value;
      const rawHex = raw.value['raw_hex'];
      const atMs = Date.parse(row['time'] ?? '');
      if (!Number.isFinite(atMs) || atMs < previousTime || !row['topic'] || !rawHex
        || !keys.every(key => row[key] === raw.value[key])) {
        throw new Error(`unmatched or unordered paired replay row ${index + 1}`);
      }
      previousTime = atMs;
      yield { row, atMs, rawHex };
    }
    const [packet, raw] = await Promise.all([packets.next(), rawPackets.next()]);
    if (!packet.done || !raw.done) throw new Error('paired replay requires exactly the requested rows');
  } finally {
    await Promise.all([packets.return(undefined), rawPackets.return(undefined)]);
  }
}

async function* pairedPublishes(directory: string, count: number, format: ReplayEnvelopeFormat) {
  for await (const { row, atMs, rawHex } of pairedReplayPackets(directory, count)) {
    const envelope = packetReplayEnvelope(row, rawHex, format);
    yield { atMs, topic: row['topic']!, label: row['packet_hash']!,
      payload: Buffer.from(JSON.stringify(envelope), 'utf8') };
  }
}

async function packetPublishes(directory: string, prefix: string | undefined, fromMs?: number, toMs?: number,
  limit?: number, format: ReplayEnvelopeFormat = 'legacy'): Promise<Publish[]> {
  const rawByKey = new Map<string, string>();
  for await (const row of replayRows(path.join(directory, 'raw_packets.csv.gz'))) {
    rawByKey.set(`${row['time']}|${row['packet_hash']}|${row['rx_node_id']}`, row['raw_hex'] ?? '');
  }
  const publishes: Publish[] = [];
  let missingRaw = 0;
  for await (const row of replayRows(path.join(directory, 'packets.csv.gz'))) {
    if (limit !== undefined && publishes.length >= limit) continue;
    const atMs = Date.parse(row['time'] ?? '');
    if (!Number.isFinite(atMs)) continue;
    if (fromMs !== undefined && atMs < fromMs) continue;
    if (toMs !== undefined && atMs >= toMs) continue;
    const key = `${row['time']}|${row['packet_hash']}|${row['rx_node_id']}`;
    const rawHex = rawByKey.get(key);
    if (!rawHex) {
      missingRaw += 1;
      continue;
    }
    const topic = rewritePrefix(row['topic'] ?? '', prefix);
    if (!topic) continue;
    const envelope = packetReplayEnvelope(row, rawHex, format);
    publishes.push({
      atMs,
      topic,
      payload: Buffer.from(JSON.stringify(envelope), 'utf8'),
      label: row['packet_hash'] ?? 'packet',
    });
  }
  if (missingRaw > 0) {
    console.warn(`[sample:replay] ${missingRaw} packet rows had no matching raw row`);
  }
  return publishes.sort((a, b) => a.atMs - b.atMs);
}

async function statusPublishes(directory: string, prefix: string | undefined, fromMs?: number, toMs?: number, limit?: number): Promise<Publish[]> {
  const [nodes, samples] = await Promise.all([
    readGzipCsv(path.join(directory, 'nodes.csv.gz')),
    readGzipCsv(path.join(directory, 'node_status_samples.csv.gz')),
  ]);
  const iataByNode = new Map<string, string>();
  for (const node of rowsOf(nodes)) {
    const nodeId = node['node_id'] ?? '';
    const iata = node['iata'] ?? '';
    if (nodeId && iata) iataByNode.set(nodeId.toUpperCase(), iata.toUpperCase());
  }
  const publishes: Publish[] = [];
  let missingIata = 0;
  for (const row of rowsOf(samples)) {
    const atMs = Date.parse(row['time'] ?? '');
    if (!Number.isFinite(atMs)) continue;
    if (fromMs !== undefined && atMs < fromMs) continue;
    if (toMs !== undefined && atMs >= toMs) continue;
    const nodeId = (row['node_id'] ?? '').toUpperCase();
    const iata = iataByNode.get(nodeId);
    if (!iata) {
      missingIata += 1;
      continue;
    }
    const storedStats = csvJson(row['stats'] ?? '');
    const stats: Record<string, unknown> = storedStats && typeof storedStats === 'object' && !Array.isArray(storedStats)
      ? { ...storedStats } : {};
    for (const field of ['battery_mv', 'uptime_secs', 'tx_air_secs', 'rx_air_secs', 'channel_utilization', 'air_util_tx']) {
      if (row[field] !== undefined && row[field] !== '') stats[field] = Number(row[field]);
    }
    const payload = { status: row['status'] || 'online', stats };
    const base = prefix ?? (row['network'] === 'test' ? 'meshcore-test' : 'meshcore');
    publishes.push({
      atMs,
      topic: `${base}/${iata}/${nodeId}/status`,
      payload: Buffer.from(JSON.stringify(payload), 'utf8'),
      label: `status ${nodeId}`,
    });
    if (limit !== undefined && publishes.length >= limit) break;
  }
  if (missingIata > 0) console.warn(`[sample:replay] ${missingIata} status rows had no node IATA`);
  return publishes.sort((a, b) => a.atMs - b.atMs);
}

async function publishReplay(broker: string, publishes: readonly Publish[] | AsyncIterable<Publish>,
  count: number, speed: number, qos: 0 | 1, concurrency: number, format: ReplayEnvelopeFormat,
  username?: string, password?: string): Promise<void> {
  const client = await connect(broker, username, password);
  try {
    const { sent, failed } = await publishAll(client, publishes, count, speed, qos, concurrency, format);
    console.log(`[sample:replay] done: sent=${sent} failed=${failed}`);
    if (failed > 0) throw new Error(`${failed} MQTT publications failed`);
    if (sent !== count) throw new Error(`expected ${count} MQTT acknowledgements, received ${sent}`);
  } finally {
    await client.endAsync();
  }
}

export async function replayPairedSoakPackets(directory: string, broker: URL, count: number, speed: number): Promise<void> {
  if (!isLoopback(broker) || !Number.isSafeInteger(count) || count < 1 || !Number.isFinite(speed) || speed <= 0) {
    throw new Error('paired replay requires a loopback broker, positive count and speed');
  }
  console.log(`[sample:replay] ${count} paired packet messages`);
  await publishReplay(broker.href, pairedPublishes(directory, count, 'observer'), count, speed, 1, 1, 'observer');
}

/** The fixture controller verifies the owned targets and seed cohort before calling this helper. */
export async function replayOwnedSeedBatch(rows: readonly { row: Row; rawHex: string }[]): Promise<void> {
  if (rows.length < 1 || rows.length > 360) throw new Error('invalid fixture seed batch size');
  const publishes = rows.map(({ row, rawHex }, index) => ({
    atMs: index * 1_000,
    topic: row['topic']!,
    label: row['packet_hash']!,
    payload: Buffer.from(JSON.stringify(packetReplayEnvelope(row, rawHex, 'observer'))),
  }));
  await publishReplay(DEFAULT_BROKER, publishes, rows.length, 30, 1, 1, 'observer');
}

export async function replaySampleMqtt(args: string[]): Promise<void> {
  const directory = path.resolve(option(args, 'data-dir') ?? defaultDataDir());
  const envelopeFormat = option(args, 'envelope-format') ?? 'legacy';
  if (envelopeFormat !== 'legacy' && envelopeFormat !== 'observer') throw new Error('--envelope-format must be legacy or observer');
  const brokerText = option(args, 'broker') ?? DEFAULT_BROKER;
  const broker = new URL(brokerText);
  if (!isLoopback(broker) && !flag(args, 'allow-remote')) {
    throw new Error('refusing non-loopback broker without --allow-remote');
  }
  const speed = parseIntOption(args, 'speed', 1);
  const qos = parseIntOption(args, 'qos', 1);
  if (qos !== 0 && qos !== 1) throw new Error('--qos must be 0 or 1');
  const concurrency = parseIntOption(args, 'concurrency', 16);
  const limit = option(args, 'limit') ? parseIntOption(args, 'limit', 0) : undefined;
  const fromMs = parseTimeMs(option(args, 'from'), 'from');
  const toMs = parseTimeMs(option(args, 'to'), 'to');
  const prefix = option(args, 'prefix');
  if (speed < 0) throw new Error('--speed must be nonnegative');
  if (!Number.isSafeInteger(concurrency) || concurrency < 1 || concurrency > 256) {
    throw new Error('--concurrency must be an integer between 1 and 256');
  }
  if (limit !== undefined && (!Number.isSafeInteger(limit) || limit < 1)) {
    throw new Error('--limit must be a positive integer');
  }
  if (prefix && !['meshcore', 'ukmesh', 'meshcore-test'].includes(prefix)) {
    throw new Error('--prefix must be meshcore, ukmesh or meshcore-test');
  }
  const statusMode = flag(args, 'status');
  const dryRun = flag(args, 'dry-run');

  const publishes = statusMode
    ? await statusPublishes(directory, prefix, fromMs, toMs, limit)
    : await packetPublishes(directory, prefix, fromMs, toMs, limit, envelopeFormat);
  if (publishes.length === 0) throw new Error('no rows matched the replay filters');
  console.log(
    `[sample:replay] ${publishes.length} ${statusMode ? 'status' : 'packet'} messages`
      + ` from ${new Date(publishes[0]!.atMs).toISOString()}`
      + ` to ${new Date(publishes[publishes.length - 1]!.atMs).toISOString()}`,
  );
  if (dryRun) return;

  await publishReplay(brokerText, publishes, publishes.length, speed, qos, concurrency, envelopeFormat,
    option(args, 'username'),
    option(args, 'password'),
  );
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  replaySampleMqtt(process.argv.slice(2)).catch((error: unknown) => {
    console.error('[sample:replay] failed:', error instanceof Error ? error.message : error);
    process.exitCode = 1;
  });
}
