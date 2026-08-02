/**
 * One-shot backfill: decode raw_hex for all route_type 0/3 packets and populate
 * transport_codes + region_scope where currently NULL.
 *
 * Run from backend/:
 *   node backfill-transport-codes.mjs
 *
 * Env vars used (same as backend):
 *   DATABASE_URL, MESHCORE_CHANNEL_SECRETS, MESHCORE_REGION_NAMES
 */

import pg from 'pg';
import { createRequire } from 'module';
const require = createRequire(import.meta.url);

const {
  MeshCoreDecoder,
  calcRegionKey,
  transportCodeMatchesRegion,
} = require('@michaelhart/meshcore-decoder');

const { Pool } = pg;

// ── Config ──────────────────────────────────────────────────────────────────

const DATABASE_URL = process.env.DATABASE_URL;
if (!DATABASE_URL) {
  console.error('DATABASE_URL env var is required');
  process.exit(1);
}

// This is the documented MeshCore public-channel material, not an application
// credential. Private/custom channel material must only enter through the
// environment and must never be committed.
const DEFAULT_PUBLIC_CHANNEL_KEY = '8b3387e9c5cdea6ac9e5edbaa115cd72';
const CHANNEL_SECRETS = [DEFAULT_PUBLIC_CHANNEL_KEY];
if (process.env.MESHCORE_CHANNEL_SECRETS) {
  for (const entry of process.env.MESHCORE_CHANNEL_SECRETS.split(',').map((s) => s.trim()).filter(Boolean)) {
    const colon = entry.indexOf(':');
    CHANNEL_SECRETS.push(colon > 0 ? entry.slice(colon + 1) : entry);
  }
}

const keyStore = MeshCoreDecoder.createKeyStore({ channelSecrets: CHANNEL_SECRETS });

const REGION_NAMES = (process.env.MESHCORE_REGION_NAMES ?? 'Europe,Global')
  .split(',').map((s) => s.trim()).filter(Boolean)
  .map((name) => (name.startsWith('#') ? name : `#${name}`));

console.log(`[backfill] region probe list: ${REGION_NAMES.join(', ')}`);

// ── Decoder helpers (mirrors decodePacket.ts logic) ─────────────────────────

function hexToBytes(rawHex) {
  const hex = rawHex.trim();
  if (!hex || hex.length % 2 !== 0) return null;
  return Buffer.from(hex, 'hex');
}

function bytesToHex(buf) {
  return buf.toString('hex').toUpperCase();
}

function parseTransportCodes(rawHex) {
  const bytes = hexToBytes(rawHex);
  if (!bytes || bytes.length < 2) return null;

  const routeType = bytes[0] & 0x03;
  if (routeType !== 0 && routeType !== 3) return null;

  if (bytes.length < 5) return null; // 1 route byte + 4 transport code bytes
  const tcHex = bytesToHex(bytes.subarray(1, 5));

  // Decode with compat wrapper to get payload.raw for region probing
  // Build compat raw hex: rewrite path length byte to the old 1-byte encoding
  const encodedLengthByte = bytes[5];
  if (encodedLengthByte === undefined) return { tcHex, payloadRaw: null, payloadType: null };

  const pathHashCount = encodedLengthByte & 0x3f;
  const pathHashSize  = (encodedLengthByte >> 6) + 1;
  if (pathHashSize > 3) return { tcHex, payloadRaw: null, payloadType: null };

  const pathByteLength = pathHashCount * pathHashSize;
  if (pathByteLength > 63) return { tcHex, payloadRaw: null, payloadType: null };

  // Build compat hex for the library decoder (replaces encoded length byte with plain byte count)
  const compat = Buffer.from(bytes);
  compat[5] = pathByteLength;
  const compatHex = bytesToHex(compat);

  let decoded;
  try {
    decoded = MeshCoreDecoder.decode(compatHex, { keyStore });
  } catch {
    return { tcHex, payloadRaw: null, payloadType: null };
  }

  return {
    tcHex,
    payloadRaw: decoded?.payload?.raw ?? null,
    payloadType: decoded?.payloadType ?? null,
  };
}

function probeRegion(tcHex, payloadType, payloadRaw) {
  if (!tcHex || payloadType == null || !payloadRaw) return null;
  const tcBuf = Buffer.from(tcHex, 'hex');
  if (tcBuf.length < 2) return null;
  const tc0 = tcBuf[0] | (tcBuf[1] << 8);
  for (const name of REGION_NAMES) {
    if (transportCodeMatchesRegion(name, payloadType, payloadRaw, tc0)) {
      return name;
    }
  }
  return null;
}

// ── Main ────────────────────────────────────────────────────────────────────

const pool = new Pool({ connectionString: DATABASE_URL });

async function run() {
  const { rows } = await pool.query(
    `SELECT packet_hash, raw_hex
     FROM packets
     WHERE route_type IN (0, 3)
       AND raw_hex IS NOT NULL
       AND raw_hex != ''
       AND transport_codes IS NULL
       AND time > NOW() - INTERVAL '31 days'
     ORDER BY time DESC`,
  );

  console.log(`[backfill] ${rows.length} packets to process`);
  if (rows.length === 0) { await pool.end(); return; }

  let updated = 0;
  let withCodes = 0;
  let withRegion = 0;
  const BATCH = 200;

  for (let i = 0; i < rows.length; i += BATCH) {
    const batch = rows.slice(i, i + BATCH);
    const values = [];

    for (const row of batch) {
      const result = parseTransportCodes(row.raw_hex);
      if (!result) {
        values.push({ hash: row.packet_hash, tcHex: null, region: null });
        continue;
      }

      withCodes++;
      const region = probeRegion(result.tcHex, result.payloadType, result.payloadRaw);
      if (region) withRegion++;

      values.push({ hash: row.packet_hash, tcHex: result.tcHex, region });
    }

    // Bulk update using unnest
    const hashes    = values.map((v) => v.hash);
    const tcHexes   = values.map((v) => v.tcHex);
    const regions   = values.map((v) => v.region);

    const client = await pool.connect();
    try {
      await client.query('SET timescaledb.max_tuples_decompressed_per_dml_transaction = 0');
      await client.query(
        `UPDATE packets AS p
           SET transport_codes = v.tc,
               region_scope    = v.region
         FROM unnest($1::text[], $2::text[], $3::text[]) AS v(hash, tc, region)
         WHERE p.packet_hash = v.hash
           AND p.route_type IN (0, 3)
           AND p.transport_codes IS NULL`,
        [hashes, tcHexes, regions],
      );
    } finally {
      client.release();
    }

    updated += batch.length;
    if (updated % 500 === 0 || updated === rows.length) {
      console.log(`[backfill] ${updated}/${rows.length} processed — ${withCodes} with codes, ${withRegion} region matches so far`);
    }
  }

  console.log(`[backfill] done. ${withCodes} packets had transport codes, ${withRegion} matched a region.`);

  // Summary by region
  const { rows: summary } = await pool.query(
    `SELECT region_scope, COUNT(*) AS n
       FROM packets
      WHERE route_type IN (0, 3)
        AND region_scope IS NOT NULL
        AND time > NOW() - INTERVAL '31 days'
      GROUP BY region_scope
      ORDER BY n DESC`,
  );
  if (summary.length > 0) {
    console.log('[backfill] region breakdown:');
    for (const r of summary) console.log(`  ${r.region_scope}: ${r.n}`);
  } else {
    console.log('[backfill] no region matches found — region names to probe may not match the firmware config');
  }

  await pool.end();
}

run().catch((err) => { console.error('[backfill] fatal:', err); process.exit(1); });
