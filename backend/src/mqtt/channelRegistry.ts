/**
 * Channel registry — the single source of truth for MeshCore channel secrets.
 *
 * VALIDATED_CHANNELS are the default secrets baked into the service (all
 * recovered/community-known channels, verified to decrypt real group text).
 * MESHCORE_CHANNEL_SECRETS (env, 'name:hex' or bare 'hex', comma-separated)
 * APPENDS extra channels on top — useful for community-shared secrets that
 * shouldn't be committed.
 *
 * Also hosts the summary + channel-identification helpers so the ingest
 * pipeline and offline tools (e.g. backfillDecrypt) share one implementation.
 */
import { ChannelCrypto, MeshCoreDecoder } from '@michaelhart/meshcore-decoder';
import type { GroupTextPayload } from '@michaelhart/meshcore-decoder';
import { decodePacketCompat } from './decodePacket.js';
import { BoundedTtlMap } from '../cache/boundedTtlMap.js';

export interface ChannelEntry {
  name: string;
  secret: string;
  keyStore: ReturnType<typeof MeshCoreDecoder.createKeyStore>;
}

/**
 * Default channels — recovered via the meshcore wordlist/derivation audit
 * (2026-08-06) and validated: each key decrypts real human-readable group
 * text (sane epoch timestamp + printable text) on the UK Mesh network.
 */
export const VALIDATED_CHANNELS: ReadonlyArray<{ name: string; secret: string }> = [
  { name: 'Public',    secret: '8b3387e9c5cdea6ac9e5edbaa115cd72' },
  { name: 'test',      secret: '9cd8fcf22a47333b591d96a2b848b73f' },
  { name: 'bot',       secret: 'eb50a1bcb3e4e5d7bf69a57c9dada211' },
  { name: 'yorkshire', secret: 'ea19e9567eec1b148c174f0cb9d04dd7' },
  { name: 'liverpool', secret: '2a223c8f7b4190b329f46d54d3a9c350' },
  { name: 'london',    secret: '9881d2b7ab9105a41a8d0f6ba449447e' },
  { name: 'nottingham', secret: '90f554aa06b30c705e7ed3bdd59b38e2' },
  { name: 'northeast', secret: '3b1ca0ae6003193eb9f91984eefef5dc' },
  { name: 'repeaters', secret: '89db441e2814dccf0dbd2e8cc5f501a3' },
  { name: 'kent',      secret: 'dea8bb392991eeee26806e4d2cdf8dd4' },
  { name: 'wales',     secret: '809573d8134fa262d284400a788f63d9' },
  { name: 'cornwall',  secret: '88b113ee8130e832d0bf15a5c7be0ddd' },
  { name: 'crawley',   secret: 'adee277d04fe73f1bfc43ce8a19f351e' },
  { name: 'devon',     secret: 'cbd7337937131f7db971cfdb24431ae3' },
  { name: 'dorset',    secret: 'd2de225d13760cf2e13aa368ef9e5b6b' },
  { name: 'cumbria',   secret: 'ec3be8009f600059d6cd1dca32c74e1c' },
  { name: 'yorks',     secret: '81eb980a4884e5859bb42b135261200e' },
  { name: 'leicester', secret: '2ca4f626387ca26ec7c2ae45641f9628' },
  { name: 'dartford',  secret: '848469a776c39f8bf2b774def5cd697b' },
  { name: 'uckfield',  secret: '15ab8ecee7b54d18adffd4b83be33682' },
  { name: 'huddersfield', secret: '549a4cd97d96c130d359748c72f0e321' },
  { name: 'derbyshire', secret: '4c3e09555ad74fde17f3a82febb610c4' },
  { name: 'lincolnshire', secret: '2db23106132a517f0b9ea47b4d822c4f' },
  { name: 'surrey',    secret: '5e12bb2d2b8f660ab179676cbc663cd1' },
  { name: 'midlands',  secret: 'ba3707a0760d156169aea2a6c82e2d6a' },
  { name: 'hamradio',  secret: '83c8b01997654265938da8765cbc7db9' },
  { name: 'mesh',      secret: '5b664cde0b08b220612113db980650f3' },
  { name: 'public2',   secret: '8b4b705b080c0d943b1c80f6b3ef6b6d' },
  { name: 'test2',     secret: '9f86d081884c7d659a2feaa0c55ad015' },
  { name: 'thenorf',   secret: '1f19cc976112b77f623ec970e4805e87' },
  { name: 'g8py',      secret: 'f4624be48a6c80309b4c2f6f3eb36e99' },
  { name: 'echo',      secret: '5d25cf40b1f5b4a7eb0cf9703634a948' },
  { name: 'denhaag',   secret: '84eed36f62c7b22527dbf8883585ad14' },
  { name: 'dublin',    secret: '8792638977132bc05a1f72d6bb913694' },
  { name: 'glasgow',   secret: 'ea6b521f794e7a0e4ff41fe5d0e3f838' },
  { name: 'york',      secret: 'd90a1f4941a7892855e473c8f9a10195' },
  { name: 'ireland',   secret: '1b2a12acc5db1517d9d407946756b1da' },
  { name: 'scilly',    secret: 'eb9ca9764bd08712f596a054e32e336c' },
  { name: 'brentwood', secret: 'b4e5450da06c21e5338b91b3b18206ad' },
  { name: 'marple',    secret: '3238ea75dcefd77a31ce907037901f61' },
  { name: 'uk',        secret: '22b2eed34b5cc429ce1dc5e88635ff84' },
];

/** Build the channel entry list: committed defaults + env extras (dedup by secret). */
export function buildChannelEntries(envValue?: string): ChannelEntry[] {
  const entries: ChannelEntry[] = VALIDATED_CHANNELS.map(({ name, secret }) => ({
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
    entries.push({ name, secret, keyStore: MeshCoreDecoder.createKeyStore({ channelSecrets: [secret] }) });
  }
  return entries;
}

const channelHashCache = new BoundedTtlMap<string, string[]>({
  name: 'mqtt_channel_hashes',
  maxEntries: 64,
  maxWeight: 64 * 1024,
  ttlMs: 30 * 60_000,
  weightOf: (key, value) => key.length * 2 + value.length * 4,
});

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

// Small cache so relay copies of the same GroupText don't trigger re-decodes
const channelCache = new BoundedTtlMap<string, string | null>({
  name: 'mqtt_channels',
  maxEntries: 200,
  maxWeight: 2 * 1024 * 1024,
  ttlMs: 10 * 60_000,
  weightOf: (key, value) => key.length * 2 + (value?.length ?? 0) * 2,
});

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
        return `${prefix}${sender}: ${p.decrypted.message}`;
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
