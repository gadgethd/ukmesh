import assert from 'node:assert/strict';
import { mkdtemp, rm, writeFile } from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import test from 'node:test';
import { gzipSync } from 'node:zlib';
import { MeshCoreDecoder } from '@michaelhart/meshcore-decoder';
import { decodePacketCompat } from '../mqtt/decodePacket.js';
import { packetReplayEnvelope, replaySampleMqtt } from './replaySampleMqtt.js';

const row = { packet_type: '3', route_type: '1', hop_count: '2', path_hash_size_bytes: '2',
  rx_node_id: '0102' + '11'.repeat(30), rssi: '-80', snr: '8' };
const raw = '0D42AABBCCDDDEADBEEF';

test('legacy replay retains its existing numeric envelope contract', () => {
  assert.deepEqual(packetReplayEnvelope(row, raw, 'legacy'), { raw, direction: 'rx',
    packet_type: 3, route_type: 1, hop_count: 2, path_hash_size_bytes: 2, RSSI: -80, SNR: 8 });
});

test('observer replay metadata meets Beacon string fields without changing decoded packet identity', () => {
  const timestamp = '2026-11-14T00:00:00.000Z';
  const envelope = JSON.parse(JSON.stringify(packetReplayEnvelope(row, raw, 'observer', timestamp)));
  assert.equal(typeof envelope.packet_type, 'string');
  assert.equal(envelope.type, 'PACKET');
  assert.equal(envelope.timestamp, timestamp);
  assert.equal(envelope.origin_id, row.rx_node_id);
  const decoded = decodePacketCompat(envelope.raw, MeshCoreDecoder.createKeyStore({ channelSecrets: [] }));
  assert.equal(decoded.metadataValid, true);
  assert.equal(decoded.decoded?.payloadType, 3);
  assert.equal(decoded.pathHashCount, 2);
  assert.equal(decoded.pathHashSize, 2);
  assert.equal(decoded.canonicalPacketId, decodePacketCompat(raw,
    MeshCoreDecoder.createKeyStore({ channelSecrets: [] })).canonicalPacketId);
});

test('streamed replay joins raw frames by identity and keeps selection before chronological ordering', async t => {
  const directory = await mkdtemp(path.join(os.tmpdir(), 'ukmesh-replay-'));
  t.after(() => rm(directory, { recursive: true, force: true }));
  const stdout = t.mock.method(console, 'log', () => {});
  const first = '2026-11-14T00:00:00.000Z';
  const second = '2026-11-14T00:00:01.000Z';
  const third = '2026-11-14T00:00:02.000Z';
  const packetRows = `time,packet_hash,rx_node_id,topic
${third},C,R,meshcore/LHR/R/packets
${first},A,R,meshcore/LHR/R/packets
${second},B,R,meshcore/LHR/R/packets
`;
  await writeFile(path.join(directory, 'packets.csv.gz'), gzipSync(packetRows));
  await writeFile(path.join(directory, 'raw_packets.csv.gz'), gzipSync(`time,packet_hash,rx_node_id,raw_hex
${second},B,R,${raw}
${third},C,R,${raw}
${first},A,R,${raw}
`));
  const args = ['--data-dir', directory, '--limit', '2', '--dry-run'];
  await replaySampleMqtt(args);
  assert.deepEqual(stdout.mock.calls.map(call => call.arguments), [[
    `[sample:replay] 2 packet messages from ${first} to ${third}`,
  ]]);
  await writeFile(path.join(directory, 'packets.csv.gz'), gzipSync(packetRows + '"unfinished'));
  await assert.rejects(replaySampleMqtt(args), /unterminated quoted CSV field/);
});
