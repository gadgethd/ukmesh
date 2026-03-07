import { MeshCoreDecoder } from '@michaelhart/meshcore-decoder';

type KeyStore = ReturnType<typeof MeshCoreDecoder.createKeyStore>;

type PathMeta = {
  encodedLengthByte: number;
  pathLengthOffset: number;
  pathHashCount: number;
  pathHashSize: number;
  pathByteLength: number;
  pathHashes: string[];
};

export type CompatDecodedPacket = {
  decoded: ReturnType<typeof MeshCoreDecoder.decode>;
  pathHashes?: string[];
  pathHashCount?: number;
  pathHashSize?: number;
};

function bytesToHex(bytes: Uint8Array): string {
  return Array.from(bytes, (b) => b.toString(16).padStart(2, '0')).join('').toUpperCase();
}

function hexToBytes(rawHex: string): Uint8Array | null {
  const hex = rawHex.trim();
  if (!hex || (hex.length % 2) !== 0 || !/^[0-9A-Fa-f]+$/.test(hex)) return null;
  return Uint8Array.from(Buffer.from(hex, 'hex'));
}

function parsePathMeta(rawHex: string): PathMeta | null {
  const bytes = hexToBytes(rawHex);
  if (!bytes || bytes.length < 2) return null;

  const routeType = bytes[0]! & 0x03;
  let offset = 1;
  if (routeType === 0 || routeType === 3) offset += 4;
  if (bytes.length <= offset) return null;

  const encodedLengthByte = bytes[offset]!;
  const pathHashCount = encodedLengthByte & 0x3f;
  const pathHashSize = (encodedLengthByte >> 6) + 1;
  if (pathHashSize > 3) return null;

  const pathByteLength = pathHashCount * pathHashSize;
  const pathStart = offset + 1;
  const pathEnd = pathStart + pathByteLength;
  if (bytes.length < pathEnd) return null;

  const pathHashes: string[] = [];
  for (let i = 0; i < pathHashCount; i++) {
    const start = pathStart + (i * pathHashSize);
    const end = start + pathHashSize;
    pathHashes.push(bytesToHex(bytes.subarray(start, end)));
  }

  return {
    encodedLengthByte,
    pathLengthOffset: offset,
    pathHashCount,
    pathHashSize,
    pathByteLength,
    pathHashes,
  };
}

function buildCompatRawHex(rawHex: string, meta: PathMeta): string {
  if (meta.encodedLengthByte < 64) return rawHex;
  const bytes = hexToBytes(rawHex);
  if (!bytes) return rawHex;
  const compat = new Uint8Array(bytes);
  compat[meta.pathLengthOffset] = meta.pathByteLength;
  return bytesToHex(compat);
}

export function decodePacketCompat(rawHex: string, keyStore: KeyStore): CompatDecodedPacket {
  const meta = parsePathMeta(rawHex);
  const compatRawHex = meta ? buildCompatRawHex(rawHex, meta) : rawHex;
  const decoded = MeshCoreDecoder.decode(compatRawHex, { keyStore });
  return {
    decoded,
    pathHashes: meta?.pathHashes ?? (decoded.path as string[] | null) ?? undefined,
    pathHashCount: meta?.pathHashCount ?? decoded.pathLength ?? undefined,
    pathHashSize: meta?.pathHashSize ?? 1,
  };
}
