import { transportCodeMatchesRegion } from '@michaelhart/meshcore-decoder';

/**
 * First transport code (uint16, little-endian) from a 4-byte transport-codes
 * hex string, as extracted from a route-type 0/3 packet. Returns null when the
 * hex is too short or malformed.
 */
export function transportCode0Of(transportCodesHex: string): number | null {
  const hex = transportCodesHex.trim();
  if (!/^[0-9A-Fa-f]{4,}$/.test(hex)) return null;
  const low = Number.parseInt(hex.slice(0, 2), 16);
  const high = Number.parseInt(hex.slice(2, 4), 16);
  return low | (high << 8);
}

/**
 * Attribute a decoded scoped packet (route 0/3, transport codes present) to one
 * of the candidate public region names, or null when no candidate matches.
 *
 * The match is `HMAC-SHA256(SHA256("#name")[0:16], payloadType || payload)`
 * truncated to a little-endian uint16, compared against the packet's first
 * transport code — the same derivation the firmware uses when flooding within
 * a region, executed per candidate name (see @michaelhart/meshcore-decoder
 * region-transport, which mirrors TransportKeyStore::getAutoKeyFor()).
 */
export function decodeRegionForPacket(
  probeNames: readonly string[],
  payloadType: number | null | undefined,
  payloadRawHex: string | null | undefined,
  transportCode0: number | null,
): string | null {
  if (transportCode0 == null || payloadType == null || !payloadRawHex) return null;
  for (const name of probeNames) {
    if (transportCodeMatchesRegion(name, payloadType, payloadRawHex, transportCode0)) {
      return name;
    }
  }
  return null;
}
