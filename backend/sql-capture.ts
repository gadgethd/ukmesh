// Captures the persistEntriesCore SQL text for direct PREPARE validation against Postgres.
import { configurePacketBatch, enqueuePacket, flush } from './src/db/packetBatch.js';

let captured = '';
configurePacketBatch(async (text: string) => {
  if (text.includes('jsonb_agg(jsonb_build_object')) {
    return { rows: [{ generation: '7', prefixes: [] }] };
  }
  captured = text;
  return {
    rows: [{ row_id: 0, is_private: false, visibility_ok: true, prefix_cache_fresh: true, lock_ok: true }],
  };
});

const resultPromise = enqueuePacket({
  time: new Date('2026-10-11T12:00:00.000Z'),
  packetHash: 'AA'.repeat(32),
  rxNodeId: 'BB'.repeat(32),
  srcNodeId: 'CC'.repeat(32),
  topic: 'meshcore/EMA/OBS/packets',
  topicPrefix: 'meshcore',
  iata: 'EMA',
  packetType: 5,
  routeType: 0,
  hopCount: 1,
  rssi: -80,
  snr: 5,
  payloadJson: '{"test":true}',
  companionSender: null,
  rawHex: '00',
  advertCount: null,
  pathHashes: ['AABB'],
  pathHashSizeBytes: 2,
  network: 'ukmesh',
  transportCodes: null,
  regionScope: null,
});
await flush();
await resultPromise;
if (!captured) {
  console.error('NO SQL CAPTURED');
  process.exit(1);
}
process.stdout.write(captured);
