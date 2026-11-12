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
  option,
  parseIntOption,
  parseTimeMs,
  readGzipCsv,
  rowObject,
} from './sampleData.js';

type Row = Record<string, string>;
type Publish = { atMs: number; topic: string; payload: Buffer; label: string };

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
  publishes: readonly Publish[],
  speed: number,
  qos: 0 | 1,
  concurrency: number,
): Promise<{ sent: number; failed: number }> {
  let sent = 0;
  let failed = 0;
  let index = 0;
  const started = Date.now();
  const firstAt = publishes[0]?.atMs ?? 0;

  const send = async (publish: Publish): Promise<void> => {
    try {
      await client.publishAsync(publish.topic, publish.payload, { qos });
      sent += 1;
    } catch (error) {
      failed += 1;
      if (failed <= 10) {
        console.error(`[sample:replay] publish failed (${publish.label}):`, (error as Error).message);
      }
    }
    if ((sent + failed) % 2000 === 0) {
      const elapsed = Math.max(1, Date.now() - started);
      console.log(`[sample:replay] ${sent + failed}/${publishes.length} (${Math.round((sent + failed) / (elapsed / 1000))}/s)`);
    }
  };

  if (speed <= 0) {
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

  for (const publish of publishes) {
    const target = started + (publish.atMs - firstAt) / speed;
    await sleep(target - Date.now());
    await send(publish);
  }
  return { sent, failed };
}

async function packetPublishes(directory: string, prefix: string | undefined, fromMs?: number, toMs?: number, limit?: number): Promise<Publish[]> {
  const [packets, raw] = await Promise.all([
    readGzipCsv(path.join(directory, 'packets.csv.gz')),
    readGzipCsv(path.join(directory, 'raw_packets.csv.gz')),
  ]);
  {
    const rawByKey = new Map<string, string>();
    for (const row of rowsOf(raw)) {
      rawByKey.set(`${row['time']}|${row['packet_hash']}|${row['rx_node_id']}`, row['raw_hex'] ?? '');
    }
    const publishes: Publish[] = [];
    let missingRaw = 0;
    for (const row of rowsOf(packets)) {
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
      const envelope: Record<string, unknown> = {
        raw: rawHex,
        direction: 'rx',
      };
      const packetType = Number(row['packet_type']);
      if (Number.isFinite(packetType)) envelope['packet_type'] = packetType;
      const routeType = Number(row['route_type']);
      if (Number.isFinite(routeType)) envelope['route_type'] = routeType;
      const hopCount = Number(row['hop_count']);
      if (Number.isFinite(hopCount)) envelope['hop_count'] = hopCount;
      const hashSize = Number(row['path_hash_size_bytes']);
      if (Number.isFinite(hashSize)) envelope['path_hash_size_bytes'] = hashSize;
      const rssi = Number(row['rssi']);
      if (row['rssi'] && Number.isFinite(rssi)) envelope['RSSI'] = rssi;
      const snr = Number(row['snr']);
      if (row['snr'] && Number.isFinite(snr)) envelope['SNR'] = snr;
      publishes.push({
        atMs,
        topic,
        payload: Buffer.from(JSON.stringify(envelope), 'utf8'),
        label: row['packet_hash'] ?? 'packet',
      });
      if (limit !== undefined && publishes.length >= limit) break;
    }
    if (missingRaw > 0) {
      console.warn(`[sample:replay] ${missingRaw} packet rows had no matching raw row`);
    }
    return publishes.sort((a, b) => a.atMs - b.atMs);
  }
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

export async function replaySampleMqtt(args: string[]): Promise<void> {
  const directory = path.resolve(option(args, 'data-dir') ?? defaultDataDir());
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
    : await packetPublishes(directory, prefix, fromMs, toMs, limit);
  if (publishes.length === 0) throw new Error('no rows matched the replay filters');
  console.log(
    `[sample:replay] ${publishes.length} ${statusMode ? 'status' : 'packet'} messages`
      + ` from ${new Date(publishes[0]!.atMs).toISOString()}`
      + ` to ${new Date(publishes[publishes.length - 1]!.atMs).toISOString()}`,
  );
  if (dryRun) return;

  const client = await connect(
    brokerText,
    option(args, 'username'),
    option(args, 'password'),
  );
  try {
    const result = await publishAll(client, publishes, speed, qos as 0 | 1, concurrency);
    console.log(`[sample:replay] done: sent=${result.sent} failed=${result.failed}`);
    if (result.failed > 0) throw new Error(`${result.failed} MQTT publications failed`);
  } finally {
    await client.endAsync();
  }
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  replaySampleMqtt(process.argv.slice(2)).catch((error: unknown) => {
    console.error('[sample:replay] failed:', error instanceof Error ? error.message : error);
    process.exitCode = 1;
  });
}
