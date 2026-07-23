import { MeshCoreDecoder } from '@michaelhart/meshcore-decoder';
import { decodePacketCompat } from '../mqtt/decodePacket.js';

const keyStore = MeshCoreDecoder.createKeyStore({ channelSecrets: [] });
const iterations = Math.max(10_000, Number(process.env['BENCH_ITERATIONS'] ?? 200_000));
const packets = [
  // ACK/Flood with two 2-byte relay hashes.
  '0D42AABBCCDDDEADBEEF',
  // ACK/Flood with two 3-byte relay hashes.
  '0D82010203A1A2A3DEADBEEF',
  // ACK/TransportFlood with two 2-byte relay hashes.
  '0C1122334442AABBCCDDDEADBEEF',
];

function bytesToHex(bytes: Uint8Array): string {
  return Array.from(bytes, (byte) => byte.toString(16).padStart(2, '0')).join('').toUpperCase();
}

/**
 * The pre-0.3 decoder workaround retained only for an apples-to-apples
 * benchmark. It parses paths, rewrites the packed path length to a one-byte
 * count, then decodes the rebuilt hex string.
 */
function legacyCompatDecode(rawHex: string): void {
  const bytes = Uint8Array.from(Buffer.from(rawHex, 'hex'));
  const routeType = bytes[0]! & 0x03;
  let offset = 1;
  if (routeType === 0 || routeType === 3) offset += 4;
  const encodedLength = bytes[offset]!;
  const pathHashCount = encodedLength & 0x3f;
  const pathHashSize = (encodedLength >> 6) + 1;
  const pathStart = offset + 1;
  const pathByteLength = pathHashCount * pathHashSize;

  // Match the old parser's path grouping work as well as its wire rewrite.
  for (let index = 0; index < pathHashCount; index += 1) {
    const start = pathStart + index * pathHashSize;
    void bytesToHex(bytes.subarray(start, start + pathHashSize));
  }

  if (encodedLength >= 64 && pathByteLength <= 63) {
    const compat = new Uint8Array(bytes);
    compat[offset] = pathByteLength;
    MeshCoreDecoder.decode(bytesToHex(compat), { keyStore });
    return;
  }
  MeshCoreDecoder.decode(rawHex, { keyStore });
}

function nativeDecode(rawHex: string): void {
  const result = decodePacketCompat(rawHex, keyStore);
  if (!result.metadataValid || !result.canonicalPacketId) {
    throw new Error('benchmark fixture is not a valid packet');
  }
}

function benchmark(label: string, decode: (rawHex: string) => void): number {
  for (let index = 0; index < 10_000; index += 1) decode(packets[index % packets.length]!);

  const startedAt = process.hrtime.bigint();
  for (let index = 0; index < iterations; index += 1) decode(packets[index % packets.length]!);
  const elapsedSeconds = Number(process.hrtime.bigint() - startedAt) / 1_000_000_000;
  const packetsPerSecond = iterations / elapsedSeconds;
  console.log(`${label}: ${packetsPerSecond.toFixed(0)} packets/s (${elapsedSeconds.toFixed(3)} s for ${iterations.toLocaleString()})`);
  return packetsPerSecond;
}

console.log(`Packet decode benchmark (${iterations.toLocaleString()} iterations; Node ${process.version})`);
const legacyPacketsPerSecond = benchmark('legacy compatibility rewrite', legacyCompatDecode);
const nativePacketsPerSecond = benchmark('native decoder + canonical identity', nativeDecode);
const change = ((nativePacketsPerSecond / legacyPacketsPerSecond) - 1) * 100;
console.log(`native change: ${change >= 0 ? '+' : ''}${change.toFixed(1)}%`);
