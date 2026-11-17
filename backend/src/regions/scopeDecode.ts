import { createHmac } from 'node:crypto';
import { calcRegionKey, hexToBytes } from '@michaelhart/meshcore-decoder';

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
 * a region (see @michaelhart/meshcore-decoder
 * region-transport, which mirrors TransportKeyStore::getAutoKeyFor()).
 */
export function decodeRegionForPacket(
  probeNames: readonly string[],
  payloadType: number | null | undefined,
  payloadRawHex: string | null | undefined,
  transportCode0: number | null,
): string | null {
  if (transportCode0 == null || payloadType == null || !payloadRawHex) return null;
  return createRegionProbe(probeNames)(payloadType, payloadRawHex, transportCode0);
}

/** A rebuild has fixed names. Reuse each reached key and parse each payload
 * once; native HMAC avoids repeated CryptoJS byte/hex/word conversions. Derive
 * keys lazily to preserve first-match and malformed-input error ordering. */
export function createRegionProbe(probeNames: readonly string[]): (
  payloadType: number | null | undefined,
  payloadRawHex: string | null | undefined,
  transportCode0: number | null,
) => string | null {
  const probes: Array<{ name: string; key?: Uint8Array }> = probeNames.map(name => ({ name }));
  return (payloadType, payloadRawHex, transportCode0) => {
    if (transportCode0 == null || payloadType == null || !payloadRawHex || probes.length === 0) return null;
    const payload = hexToBytes(payloadRawHex);
    const message = Buffer.allocUnsafe(1 + payload.length);
    message[0] = payloadType & 0xff;
    message.set(payload, 1);
    for (const probe of probes) {
      const key = probe.key ??= calcRegionKey(probe.name);
      let code = createHmac('sha256', key).update(message).digest().readUInt16LE(0);
      // Firmware reserves both values; apply the same nudges as the decoder.
      if (code === 0) code = 1;
      else if (code === 0xffff) code = 0xfffe;
      if (code === (transportCode0 & 0xffff)) return probe.name;
    }
    return null;
  };
}
