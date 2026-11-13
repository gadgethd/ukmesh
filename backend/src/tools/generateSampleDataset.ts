/** Deterministic, invented data for local onboarding. Never reads the archive. */
import { createHash } from 'node:crypto';
import { mkdir, writeFile } from 'node:fs/promises';
import path from 'node:path';
import { pathToFileURL } from 'node:url';
import { gzipSync } from 'node:zlib';
import { calcTransportCodeForRegion, MeshCoreDecoder } from '@michaelhart/meshcore-decoder';
import { decodePacketCompat } from '../mqtt/decodePacket.js';
import { option, requiredOption } from './sampleData.js';

function csv(rows: unknown[][]): string {
  return rows.map((row) => row.map((value) => {
    const text = value == null ? '' : String(value);
    return /[",\r\n]/.test(text) ? `"${text.replaceAll('"', '""')}"` : text;
  }).join(',')).join('\n') + '\n';
}

export async function generateSampleDataset(args: string[]): Promise<void> {
  const directory = path.resolve(requiredOption(args, 'data-dir'));
  const count = Number(option(args, 'count') ?? 200);
  const sequenceStart = Number(option(args, 'sequence-start') ?? 0);
  if (!Number.isSafeInteger(count) || count < 1 || count > 1_000_000) {
    throw new Error('--count must be an integer between 1 and 1000000');
  }
  if (!Number.isSafeInteger(sequenceStart) || sequenceStart < 0 || sequenceStart + count > 0x1_0000_0000) {
    throw new Error('--sequence-start and count must fit an unsigned 32-bit ACK sequence');
  }
  const end = Date.parse(option(args, 'as-of') ?? new Date().toISOString());
  if (!Number.isFinite(end)) throw new Error('--as-of must be an ISO timestamp');
  const at = (index: number) => new Date(end - (count - index) * 1000).toISOString();
  const receiver = '0102' + '11'.repeat(30);
  const first = 'AABB' + '22'.repeat(30);
  const second = 'CCDD' + '33'.repeat(30);
  const keyStore = MeshCoreDecoder.createKeyStore({ channelSecrets: [] });
  const packets: unknown[][] = [[
    'time', 'packet_hash', 'packet_type', 'route_type', 'hop_count', 'path_hash_size_bytes',
    'path', 'src_node_id', 'rx_node_id', 'rssi', 'snr', 'iata', 'network',
    'region_scope', 'transport_codes', 'topic', 'summary',
  ]];
  const raw: unknown[][] = [['time', 'packet_hash', 'rx_node_id', 'raw_hex']];
  for (let index = 0; index < count; index += 1) {
    const payload = (index + sequenceStart).toString(16).padStart(8, '0').toUpperCase();
    const scoped = index % 2 === 0;
    const codes = Buffer.alloc(4);
    codes.writeUInt16LE(calcTransportCodeForRegion('gb', 3, Buffer.from(payload, 'hex')));
    const hex = (scoped ? '0C' + codes.toString('hex').toUpperCase() : '0D') + '42AABBCCDD' + payload;
    const decoded = decodePacketCompat(hex, keyStore);
    if (!decoded.metadataValid || !decoded.canonicalPacketId) throw new Error('invalid synthetic packet');
    packets.push([at(index), decoded.canonicalPacketId, 3, scoped ? 0 : 1, 2, 2,
      'AABB>CCDD', '', receiver, -80, 8, 'LHR', 'ukmesh', scoped ? '#gb' : '', scoped ? codes.toString('hex').toUpperCase() : '',
      `meshcore/LHR/${receiver}/packets`, 'Synthetic ACK']);
    raw.push([at(index), decoded.canonicalPacketId, receiver, hex]);
  }
  const files: Record<string, unknown[][]> = {
    nodes: [
      ['node_id', 'name', 'lat', 'lon', 'iata', 'role', 'last_seen', 'is_online', 'network', 'public_key'],
      [receiver, 'Synthetic observer', 51.5, -0.1, 'LHR', 2, at(count), true, 'ukmesh', receiver],
      [first, 'Synthetic west', 51.51, -0.12, 'LHR', 2, at(count), true, 'ukmesh', first],
      [second, 'Synthetic east', 51.52, -0.08, 'LHR', 2, at(count), true, 'ukmesh', second],
    ],
    packets,
    raw_packets: raw,
    node_links: [
      ['node_a_id', 'node_b_id', 'observed_count', 'last_observed', 'itm_path_loss_db',
        'itm_viable', 'count_a_to_b', 'count_b_to_a', 'multibyte_observed_count', 'force_viable'],
      [first, second, count, at(count), 100, true, count, 0, count, false],
    ],
    node_status_samples: [
      ['time', 'node_id', 'network', 'battery_mv', 'uptime_secs', 'tx_air_secs',
        'rx_air_secs', 'channel_utilization', 'air_util_tx', 'status', 'stats'],
      [at(count), receiver, 'ukmesh', 4100, 3600, 10, 20, 0.1, 0.05, 'online', '{}'],
    ],
  };
  await mkdir(directory, { recursive: true });
  const checksums: Record<string, string> = {};
  for (const [name, rows] of Object.entries(files)) {
    const compressed = gzipSync(csv(rows));
    await writeFile(path.join(directory, `${name}.csv.gz`), compressed, { flag: 'wx' });
    checksums[`${name}.csv.gz`] = createHash('sha256').update(compressed).digest('hex');
  }
  await writeFile(path.join(directory, 'fixture.json'), JSON.stringify({
    version: 1, synthetic: true, count, seed: 'sequential-ack-v1', sequenceStart,
    from: at(0), to: at(count), checksums,
    gaps: ['Invented link evidence is not an original neighbour MQTT report.'],
  }, null, 2) + '\n', { flag: 'wx' });
  console.log(`[sample:generate] wrote ${count} invented packets to ${directory}`);
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  generateSampleDataset(process.argv.slice(2)).catch((error: unknown) => {
    console.error('[sample:generate]', error instanceof Error ? error.message : 'failed');
    process.exitCode = 1;
  });
}
