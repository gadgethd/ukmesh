import { createHash } from 'node:crypto';
import { MeshCoreDecoder } from '@michaelhart/meshcore-decoder';

type KeyStore = ReturnType<typeof MeshCoreDecoder.createKeyStore>;

let malformedPacketCount = 0;
let lastMalformedPacketLogAt = 0;

function reportMalformedPacket(reason: string, rawHex: string): void {
  malformedPacketCount += 1;
  const now = Date.now();
  if (now - lastMalformedPacketLogAt < 5_000) return;
  console.warn(`[decode] dropped ${malformedPacketCount} malformed packet(s): ${reason}; sample=${rawHex.slice(0, 16)}…`);
  malformedPacketCount = 0;
  lastMalformedPacketLogAt = now;
}

type PathMeta = {
  bytes: Uint8Array;
  routeType: number;
  transportCodes: string | undefined;
  pathHashCount: number;
  pathHashSize: number;
  payloadOffset: number;
};

export type CompatDecodedPacket = {
  decoded?: ReturnType<typeof MeshCoreDecoder.decode>;
  pathHashes?: string[];
  pathHashCount?: number;
  pathHashSize?: number;
  routeType?: number;
  transportCodes?: string;
  /** True only when the wire framing and decoder agree on route/path metadata. */
  metadataValid: boolean;
  /** SHA-256 of route/path-invariant wire content; safe to use as a packet identity. */
  canonicalPacketId?: string;
};

function hexToBytes(rawHex: string): Uint8Array | null {
  const hex = rawHex.trim();
  if (!hex || (hex.length % 2) !== 0 || !/^[0-9A-Fa-f]+$/.test(hex)) return null;
  return Buffer.from(hex, 'hex');
}

function parsePathMeta(rawHex: string): PathMeta | null {
  const bytes = hexToBytes(rawHex);
  if (!bytes || bytes.length < 2) return null;

  const routeType = bytes[0]! & 0x03;
  let offset = 1;

  // Extract transport codes when present (routeType 0 = FLOOD, 3 = DIRECT with codes)
  let transportCodes: string | undefined;
  if (routeType === 0 || routeType === 3) {
    if (bytes.length < offset + 4) return null;
    transportCodes = Buffer.from(bytes.subarray(offset, offset + 4)).toString('hex').toUpperCase();
    offset += 4;
  }

  if (bytes.length <= offset) return null;

  const encodedLengthByte = bytes[offset]!;
  const pathHashCount = encodedLengthByte & 0x3f;
  const pathHashSize = (encodedLengthByte >> 6) + 1;

  // Reserved hash-size mode (0b11 → 4 bytes) — reject fully (#3)
  if (pathHashSize > 3) {
    reportMalformedPacket(`reserved path hash size mode ${pathHashSize}`, rawHex);
    return null;
  }

  const pathByteLength = pathHashCount * pathHashSize;
  const pathStart = offset + 1;
  const pathEnd = pathStart + pathByteLength;

  // Warn on truncated path data (#2)
  if (bytes.length < pathEnd) {
    reportMalformedPacket(
      `truncated path (need ${pathByteLength} path bytes, got ${bytes.length - pathStart})`,
      rawHex,
    );
    return null;
  }

  // A packet with no payload cannot be decoded meaningfully. Treat it as invalid
  // framing rather than deriving an identity from just routing metadata.
  if (bytes.length <= pathEnd) {
    reportMalformedPacket('missing payload', rawHex);
    return null;
  }

  return {
    bytes,
    routeType,
    transportCodes,
    pathHashCount,
    pathHashSize,
    payloadOffset: pathEnd,
  };
}

function decodedMetadataMatches(meta: PathMeta, decoded: ReturnType<typeof MeshCoreDecoder.decode>): boolean {
  if (!decoded.isValid) return false;
  if (
    decoded.routeType !== meta.routeType
    || decoded.pathLength !== meta.pathHashCount
    || decoded.pathHashSize !== meta.pathHashSize
  ) return false;

  const decodedPath = decoded.path;
  if (meta.pathHashCount === 0) {
    return decodedPath == null || decodedPath.length === 0;
  }
  return Array.isArray(decodedPath)
    && decodedPath.length === meta.pathHashCount
    && decodedPath.every((hash) => /^[0-9A-F]+$/.test(hash) && hash.length === meta.pathHashSize * 2);
}

/**
 * Hash only immutable packet content: payload type/version plus payload bytes.
 * Route type, transport codes, and the mutable relay path are intentionally
 * excluded, so receptions of the same transmission share an identity without
 * relying on the decoder's 32-bit display hash.
 */
function canonicalPacketId(meta: PathMeta): string | undefined {
  const { bytes } = meta;
  if (bytes.length <= meta.payloadOffset) return undefined;

  const hash = createHash('sha256');
  hash.update(Uint8Array.of(bytes[0]! & 0xFC));
  hash.update(bytes.subarray(meta.payloadOffset));
  return hash.digest('hex').toUpperCase();
}

export function decodePacketCompat(rawHex: string, keyStore: KeyStore): CompatDecodedPacket {
  const meta = parsePathMeta(rawHex);
  if (!meta) return { metadataValid: false };

  let decoded: ReturnType<typeof MeshCoreDecoder.decode>;
  try {
    // meshcore-decoder >=0.3.0 natively understands the high path-length bits
    // used by 2- and 3-byte hashes. Do not rewrite the wire packet before decode.
    decoded = MeshCoreDecoder.decode(rawHex, { keyStore });
  } catch {
    return { metadataValid: false };
  }

  const metadataValid = decodedMetadataMatches(meta, decoded);
  return {
    decoded,
    metadataValid,
    pathHashes: metadataValid ? decoded.path ?? undefined : undefined,
    pathHashCount: metadataValid ? meta.pathHashCount : undefined,
    pathHashSize: metadataValid ? meta.pathHashSize : undefined,
    routeType: metadataValid ? meta.routeType : undefined,
    transportCodes: metadataValid ? meta.transportCodes : undefined,
    canonicalPacketId: metadataValid ? canonicalPacketId(meta) : undefined,
  };
}
