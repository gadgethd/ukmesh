import { createDecipheriv, createHash } from 'node:crypto';
import { MQTT_CACHE_POLICIES } from './cachePolicies.js';
import { PUBLIC_CHANNEL_METADATA } from '../contracts/publicData.generated.js';
/**
 * Channel registry — explicitly public defaults plus confidential environment keys.
 *
 * PUBLIC_CHANNELS contains only recovered/community-known material intentionally
 * classified as public. MESHCORE_CHANNEL_SECRETS ('name:hex' or bare 'hex',
 * comma-separated) adds confidential keys without committing their values.
 *
 * Also hosts the summary + channel-identification helpers so the ingest
 * pipeline and offline tools (e.g. backfillDecrypt) share one implementation.
 */
import { Buffer } from 'node:buffer';
import { ChannelCrypto, MeshCoreDecoder } from '@michaelhart/meshcore-decoder';
import type { GroupTextPayload } from '@michaelhart/meshcore-decoder';
import { decodePacketCompat } from './decodePacket.js';
import { BoundedTtlMap } from '../cache/boundedTtlMap.js';

export type ChannelClassification = 'public' | 'confidential';

export interface ChannelEntry {
  classification: ChannelClassification;
  name: string;
  secret: string;
  keyStore: ReturnType<typeof MeshCoreDecoder.createKeyStore>;
}

/**
 * Every committed value is intentionally public: recovered/community-known
 * channels from the 2026-08-06 derivation audit, validated against real UK Mesh
 * group text. Confidential channel keys belong in MESHCORE_CHANNEL_SECRETS.
 */
const PUBLIC_CHANNEL_KEYS: Readonly<Record<string, string>> = {
  'public': '8b3387e9c5cdea6ac9e5edbaa115cd72',
  'test': '9cd8fcf22a47333b591d96a2b848b73f',
  'bot': 'eb50a1bcb3e4e5d7bf69a57c9dada211',
  'yorkshire': 'ea19e9567eec1b148c174f0cb9d04dd7',
  'liverpool': '2a223c8f7b4190b329f46d54d3a9c350',
  'london': '9881d2b7ab9105a41a8d0f6ba449447e',
  'nottingham': '90f554aa06b30c705e7ed3bdd59b38e2',
  'northeast': '3b1ca0ae6003193eb9f91984eefef5dc',
  'repeaters': '89db441e2814dccf0dbd2e8cc5f501a3',
  'kent': 'dea8bb392991eeee26806e4d2cdf8dd4',
  'wales': '809573d8134fa262d284400a788f63d9',
  'cornwall': '88b113ee8130e832d0bf15a5c7be0ddd',
  'crawley': 'adee277d04fe73f1bfc43ce8a19f351e',
  'devon': 'cbd7337937131f7db971cfdb24431ae3',
  'dorset': 'd2de225d13760cf2e13aa368ef9e5b6b',
  'cumbria': 'ec3be8009f600059d6cd1dca32c74e1c',
  'yorks': '81eb980a4884e5859bb42b135261200e',
  'leicester': '2ca4f626387ca26ec7c2ae45641f9628',
  'dartford': '848469a776c39f8bf2b774def5cd697b',
  'uckfield': '15ab8ecee7b54d18adffd4b83be33682',
  'huddersfield': '549a4cd97d96c130d359748c72f0e321',
  'derbyshire': '4c3e09555ad74fde17f3a82febb610c4',
  'lincolnshire': '2db23106132a517f0b9ea47b4d822c4f',
  'surrey': '5e12bb2d2b8f660ab179676cbc663cd1',
  'midlands': 'ba3707a0760d156169aea2a6c82e2d6a',
  'hamradio': '83c8b01997654265938da8765cbc7db9',
  'mesh': '5b664cde0b08b220612113db980650f3',
  'public2': '8b4b705b080c0d943b1c80f6b3ef6b6d',
  'test2': '9f86d081884c7d659a2feaa0c55ad015',
  'thenorf': '1f19cc976112b77f623ec970e4805e87',
  'g8py': 'f4624be48a6c80309b4c2f6f3eb36e99',
  'echo': '5d25cf40b1f5b4a7eb0cf9703634a948',
  'denhaag': '84eed36f62c7b22527dbf8883585ad14',
  'dublin': '8792638977132bc05a1f72d6bb913694',
  'glasgow': 'ea6b521f794e7a0e4ff41fe5d0e3f838',
  'york': 'd90a1f4941a7892855e473c8f9a10195',
  'ireland': '1b2a12acc5db1517d9d407946756b1da',
  'scilly': 'eb9ca9764bd08712f596a054e32e336c',
  'brentwood': 'b4e5450da06c21e5338b91b3b18206ad',
  'marple': '3238ea75dcefd77a31ce907037901f61',
  'uk': '22b2eed34b5cc429ce1dc5e88635ff84',
};

const publicSlugs = new Set<string>(PUBLIC_CHANNEL_METADATA.map(channel => channel.slug));
if (Object.keys(PUBLIC_CHANNEL_KEYS).length !== publicSlugs.size
  || Object.keys(PUBLIC_CHANNEL_KEYS).some(slug => !publicSlugs.has(slug))) {
  throw new Error('PUBLIC_CHANNEL_KEY_METADATA_MISMATCH');
}

export const PUBLIC_CHANNELS: ReadonlyArray<{ name: string; secret: string }> =
  PUBLIC_CHANNEL_METADATA.map(({ label, slug }) => ({ name: label, secret: PUBLIC_CHANNEL_KEYS[slug]! }));

const normalizeChannelName = (name: string) => name.trim().toLowerCase();
const configuredPublicNames = (value?: string) => (value ?? '').split(',').map(normalizeChannelName).filter(Boolean);

/** Build the channel entry list: committed defaults + env extras (dedup by secret). */
export function buildChannelEntries(envValue?: string): ChannelEntry[] {
  const entries: ChannelEntry[] = PUBLIC_CHANNELS.map(({ name, secret }) => ({
    classification: 'public',
    name,
    secret,
    keyStore: MeshCoreDecoder.createKeyStore({ channelSecrets: [secret] }),
  }));
  const seen = new Set(entries.map((e) => e.secret.toLowerCase()));
  for (const raw of (envValue ?? '').split(',').map((s) => s.trim()).filter(Boolean)) {
    const colon = raw.indexOf(':');
    const name = colon > 0 ? raw.slice(0, colon) : raw.slice(0, 6);
    const secret = colon > 0 ? raw.slice(colon + 1) : raw;
    if (seen.has(secret.toLowerCase())) continue;
    seen.add(secret.toLowerCase());
    entries.push({
      classification: 'confidential',
      name,
      secret,
      keyStore: MeshCoreDecoder.createKeyStore({ channelSecrets: [secret] }),
    });
  }
  return entries;
}

const channelHashCache = new BoundedTtlMap<string, string[]>(MQTT_CACHE_POLICIES.channelHashCache, { weightOf: (key, value) => key.length * 2 + value.length * 4 });

/** Return the wire-level channel hashes for a configured channel label. */
export function channelHashesForName(name: string, envValue = process.env['MESHCORE_CHANNEL_SECRETS']): string[] {
  const normalizedName = name.trim().toLowerCase();
  const cacheKey = `${envValue ?? ''}\u0000${normalizedName}`;
  const cached = channelHashCache.get(cacheKey);
  if (cached) return cached;

  const hashes = Array.from(new Set(
    buildChannelEntries(envValue)
      .filter((entry) => entry.name.trim().toLowerCase() === normalizedName)
      .map((entry) => ChannelCrypto.calculateChannelHash(entry.secret).toLowerCase()),
  ));
  channelHashCache.set(cacheKey, hashes);
  return hashes;
}

/** Combined keyStore used for decryption — one decode call per packet tries every secret. */
export function buildCombinedKeyStore(entries: ChannelEntry[]) {
  return MeshCoreDecoder.createKeyStore({ channelSecrets: entries.map((e) => e.secret) });
}

/**
 * Channel names whose plaintext may be published by the public API.
 *
 * Committed PUBLIC_CHANNELS are public by definition. Environment-supplied
 * secrets are confidential unless their name is explicitly added to
 * PUBLIC_DECRYPT_CHANNELS (comma-separated, default empty = deny).
 */
export function isPublicChannelName(
  name: string,
  allowlist = process.env['PUBLIC_DECRYPT_CHANNELS'],
): boolean {
  const normalized = normalizeChannelName(name);
  if (PUBLIC_CHANNELS.some((channel) => normalizeChannelName(channel.name) === normalized)) return true;
  return configuredPublicNames(allowlist).includes(normalized);
}

/** Exact publication registry for the credentialed Beacon consumer. */
export function beaconPublicationRegistry(secretsEnv?: string, allowlist?: string) {
  const optedIn = new Set(configuredPublicNames(allowlist));
  const registeredNames = new Set(PUBLIC_CHANNELS.map(({ name }) => normalizeChannelName(name)));
  const canonicalIds = new Map<string, string>(PUBLIC_CHANNEL_METADATA.map(({ label, slug }) => [normalizeChannelName(label), slug]));
  const usedIds = new Set<string>();
  const channels = [];
  for (const { classification, name, secret } of buildChannelEntries(secretsEnv)) {
    const normalized = normalizeChannelName(name);
    if (classification === 'confidential' && !optedIn.has(normalized)) continue;
    if (classification === 'confidential' && registeredNames.has(normalized)) {
      throw new Error('BEACON_PUBLIC_CHANNEL_NAME_CONFLICT');
    }
    if (!/^[0-9a-f]{32}$/i.test(secret)) throw new Error('BEACON_PUBLIC_CHANNEL_KEY_INVALID');
    const id = classification === 'public'
      ? canonicalIds.get(normalized)!
      : createHash('sha256').update(normalized, 'utf8').digest('hex');
    if (usedIds.has(id)) throw new Error('BEACON_PUBLIC_CHANNEL_NAME_CONFLICT');
    usedIds.add(id);
    channels.push({ id, name: name.trim(),
      keyFingerprint: createHash('sha256').update(Buffer.from(secret, 'hex')).digest('hex'),
      secretHex: secret.toLowerCase(),
    });
  }
  channels.sort((left, right) => left.id < right.id ? -1 : left.id === right.id ? 0 : 1);
  const descriptors = channels.map(({ id, name, keyFingerprint }) => ({ id, name, keyFingerprint }));
  const registryVersion = createHash('sha256').update(JSON.stringify(descriptors)).digest('hex');
  return { registryVersion, channels };
}

/** Wire-level hashes whose plaintext the public API may return. */
export function publicChannelHashes(
  secretsEnv = process.env['MESHCORE_CHANNEL_SECRETS'],
  allowlist = process.env['PUBLIC_DECRYPT_CHANNELS'],
): Set<string> {
  const hashes = new Set<string>();
  for (const entry of buildChannelEntries(secretsEnv)) {
    if (entry.classification === 'public' || isPublicChannelName(entry.name, allowlist)) {
      hashes.add(ChannelCrypto.calculateChannelHash(entry.secret).toLowerCase());
    }
  }
  return hashes;
}

export function isPublicChannelHash(
  hash: string,
  secretsEnv = process.env['MESHCORE_CHANNEL_SECRETS'],
  allowlist = process.env['PUBLIC_DECRYPT_CHANNELS'],
): boolean {
  const normalized = hash.trim().toLowerCase();
  return normalized.length > 0 && publicChannelHashes(secretsEnv, allowlist).has(normalized);
}

/**
 * Strip plaintext from a packet payload unless its channel is explicitly public.
 * Keeps the pre-computed summary for public channels only; confidential channel
 * content never leaves the database through the public packet-detail API.
 */
export function sanitizePublicPacketPayload(
  payload: Record<string, unknown> | null,
  secretsEnv = process.env['MESHCORE_CHANNEL_SECRETS'],
  allowlist = process.env['PUBLIC_DECRYPT_CHANNELS'],
): Record<string, unknown> | null {
  if (!payload || typeof payload !== 'object' || Array.isArray(payload)) return payload;
  const decrypted = payload['decrypted'];
  if (!decrypted || typeof decrypted !== 'object' || Array.isArray(decrypted)) return payload;
  const channelHash = String((decrypted as Record<string, unknown>)['channelHash'] ?? '');
  if (channelHash && isPublicChannelHash(channelHash, secretsEnv, allowlist)) return payload;
  const { decrypted: _decrypted, _summary: _summary, ...rest } = payload;
  return rest;
}

// Small cache so relay copies of the same GroupText don't trigger re-decodes
const channelCache = new BoundedTtlMap<string, string | null>(MQTT_CACHE_POLICIES.channelCache, { weightOf: (key, value) => key.length * 2 + (value?.length ?? 0) * 2 });

/** Identify which channel a GroupText was sent on by trying each single-key keyStore. */
export function identifyChannel(rawHex: string, entries: ChannelEntry[]): string | undefined {
  // Single channel — must be it, no re-decode needed
  if (entries.length === 1) return entries[0]!.name;

  if (channelCache.has(rawHex)) return channelCache.get(rawHex) ?? undefined;

  let result: string | undefined;
  for (const entry of entries) {
    const { decoded: d, metadataValid } = decodePacketCompat(rawHex, entry.keyStore);
    if (!metadataValid) continue;
    const p = d?.payload?.decoded as GroupTextPayload | undefined;
    if (p?.decrypted) { result = entry.name; break; }
  }

  channelCache.set(rawHex, result ?? null);
  return result;
}

// Channel decryption and the signed ending identify the format only.
export const MESHOS_SIGNED_MARKER = 'MeshOS-signed (unverified)';

/**
 * Structural MeshOS signature detector.
 *
 * Mirrors MeshCore-SignatureFreeZone `src/helpers/SignatureFreeZone.h`: MeshOS
 * appends a signed ending *inside* the encrypted plaintext —
 *   [timestamp 4B][flags 1B]["<Name>: <text>"][0x00]["S" 0x01][signer key id 8B]
 *   [Ed25519 signature 64B][zero pad <= 15B]
 * — so the packet still passes MeshCore's HMAC check and only the tail marks it
 * as signed. Detection is structural only; the signature is not verified here.
 */
export function isMeshosSignedPlaintext(plaintext: Buffer): boolean {
  const len = plaintext.length;
  if (len < 6) return false;
  let i = 5; // skip timestamp(4) + flags(1)
  while (i < len && plaintext[i] !== 0x00) i += 1;
  if (i >= len) return false;
  const ext = i + 1;
  const extEnd = ext + 74; // 0x53 0x01 + 8B key id + 64B signature
  if (extEnd > len) return false;
  if (plaintext[ext] !== 0x53 || plaintext[ext + 1] !== 0x01) return false;
  if (len - extEnd > 15) return false;
  for (let j = extEnd; j < len; j += 1) {
    if (plaintext[j] !== 0x00) return false;
  }
  return true;
}

/** Decrypt a group-text ciphertext with the channel secret (no padding removal). */
function decryptGroupPlaintext(secret: Buffer, ciphertextHex: string): Buffer | undefined {
  const ciphertext = Buffer.from(ciphertextHex, 'hex');
  if (ciphertext.length < 16 || ciphertext.length % 16 !== 0) return undefined;
  try {
    const decipher = createDecipheriv('aes-128-ecb', secret, null);
    decipher.setAutoPadding(false);
    return Buffer.concat([decipher.update(ciphertext), decipher.final()]);
  } catch {
    return undefined;
  }
}

/** True when the decrypted group text carries the MeshOS signed ending. */
export function hasMeshosSignature(p: GroupTextPayload, entries: ChannelEntry[], channel?: string): boolean {
  if (typeof p.ciphertext !== 'string' || typeof p.channelHash !== 'string' || !p.decrypted) return false;
  const expectedHash = p.channelHash.trim().toUpperCase();
  if (expectedHash.length !== 2) return false;

  for (const entry of entries) {
    if (channel !== undefined && entry.name !== channel) continue;
    if (ChannelCrypto.calculateChannelHash(entry.secret).toUpperCase() !== expectedHash) continue;
    const plaintext = decryptGroupPlaintext(Buffer.from(entry.secret, 'hex'), p.ciphertext);
    if (plaintext && isMeshosSignedPlaintext(plaintext)) return true;
  }
  return false;
}

/** Build a short human-readable summary from a decoded payload. */
export function buildSummary(payloadType: number, decoded: unknown, rawHex?: string, entries?: ChannelEntry[]): string | undefined {
  if (!decoded) return undefined;

  switch (payloadType) {
    case 4: {
      const p = decoded as { appData?: { name?: string } };
      const name = p.appData?.name;
      return name ? `${name}` : undefined;
    }
    case 5: {
      const p = decoded as GroupTextPayload;
      if (p.decrypted) {
        const sender  = p.decrypted.sender ?? '?';
        const channel = rawHex && entries ? identifyChannel(rawHex, entries) : undefined;
        const prefix  = channel ? `[${channel}] ` : '';
        const signed  = entries ? hasMeshosSignature(p, entries, channel) : false;
        const suffix  = signed ? ` · ${MESHOS_SIGNED_MARKER}` : '';
        return `${prefix}${sender}: ${p.decrypted.message}${suffix}`;
      }
      return '[encrypted]';
    }
    case 2: {
      const p = decoded as { decrypted?: { message?: string } };
      if (p.decrypted?.message) return `${p.decrypted.message}`;
      return '[encrypted DM]';
    }
    case 3: {
      const p = decoded as { checksum: string };
      return `ACK ${p.checksum.slice(0, 4)}`;
    }
    case 8: {
      const p = decoded as { pathLength: number };
      return `${p.pathLength} hop path`;
    }
    case 9: {
      const p = decoded as { pathHashes: unknown[] };
      return `trace ${p.pathHashes.length} hops`;
    }
    default:
      return undefined;
  }
}
