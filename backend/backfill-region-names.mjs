/**
 * One-shot backfill: re-probe the REGION NAME for packets that already have
 * transport_codes populated but region_scope NULL — i.e. history that predates
 * a MESHCORE_REGION_NAMES extension.
 *
 * Companion to backfill-transport-codes.mjs: that script only processes rows
 * where transport_codes IS NULL (all already filled), so extending the region
 * name list needs this re-probe pass instead.
 *
 * Run from backend/:
 *   node backfill-region-names.mjs
 *
 * Env vars used (same as backend):
 *   DATABASE_URL, MESHCORE_CHANNEL_SECRETS, MESHCORE_REGION_NAMES
 *
 * Safe to re-run: only touches rows with region_scope IS NULL; decode is
 * deterministic; window matches the original backfill (31 days).
 */

import pg from 'pg';
import { createRequire } from 'module';
const require = createRequire(import.meta.url);

const {
  MeshCoreDecoder,
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

console.log(`[backfill-names] region probe list: ${REGION_NAMES.join(', ')}`);

// ── Decoder helpers (mirrors backfill-transport-codes.mjs) ──────────────────

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
    `SELECT DISTINCT ON (packet_hash) packet_hash, raw_hex
       FROM packets
      WHERE route_type IN (0, 3)
        AND raw_hex IS NOT NULL
        AND raw_hex != ''
        AND transport_codes IS NOT NULL
        AND region_scope IS NULL
        AND time > NOW() - INTERVAL '31 days'
      ORDER BY packet_hash, time DESC`,
  );

  console.log(`[backfill-names] ${rows.length} distinct packets to re-probe`);
  if (rows.length === 0) { await pool.end(); return; }

  let processed = 0;
  let matches = 0;
  let noCode = 0;
  let totalRowsUpdated = 0;
  const byName = new Map();
  const BATCH = 200;

  for (let i = 0; i < rows.length; i += BATCH) {
    const batch = rows.slice(i, i + BATCH);
    const values = [];

    for (const row of batch) {
      const result = parseTransportCodes(row.raw_hex);
      if (!result) { noCode++; continue; }

      const region = probeRegion(result.tcHex, result.payloadType, result.payloadRaw);
      if (!region) continue;

      matches++;
      byName.set(region, (byName.get(region) ?? 0) + 1);
      values.push({ hash: row.packet_hash, region });
    }

    if (values.length > 0) {
      const hashes  = values.map((v) => v.hash);
      const regions = values.map((v) => v.region);

      const client = await pool.connect();
      try {
        await client.query('SET timescaledb.max_tuples_decompressed_per_dml_transaction = 0');
        const r = await client.query(
          `UPDATE packets AS p
             SET region_scope = v.region
             FROM unnest($1::text[], $2::text[]) AS v(hash, region)
             WHERE p.packet_hash = v.hash
               AND p.route_type IN (0, 3)
               AND p.region_scope IS NULL`,
          [hashes, regions],
        );
        totalRowsUpdated += r.rowCount ?? 0;
      } finally {
        client.release();
      }
    }

    processed += batch.length;
    if (processed % 1000 === 0 || processed === rows.length) {
      console.log(`[backfill-names] ${processed}/${rows.length} probed — ${matches} matches, ${totalRowsUpdated} rows updated`);
    }
  }

  console.log(`[backfill-names] done. ${matches} packets matched a region, ${noCode} undecodable.`);
  if (byName.size > 0) {
    console.log('[backfill-names] matches by name:');
    for (const [name, n] of [...byName.entries()].sort((a, b) => b[1] - a[1])) {
      console.log(`  ${name}: ${n} packets`);
    }
  }

  // Summary by region (31-day window, all rows)
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
    console.log('[backfill-names] full region breakdown:');
    for (const r of summary) console.log(`  ${r.region_scope}: ${r.n}`);
  }

  await pool.end();
}

run().catch((err) => { console.error('[backfill-names] fatal:', err); process.exit(1); });
