import assert from 'node:assert/strict';
import test from 'node:test';
import { MeshCoreDecoder } from '@michaelhart/meshcore-decoder';
import { decodePacketCompat } from '../mqtt/decodePacket.js';
import { packetReplayEnvelope } from './replaySampleMqtt.js';

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
