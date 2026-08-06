/**
 * Backfill decryption — decrypt stored raw group-text packets retroactively.
 *
 * Keyset-paginated on packet_hash (uses packets_hash_idx) so each batch is a
 * small range scan instead of re-scanning the whole table. Resumable via
 * --cursor=<packet_hash>. Idempotent: skips hashes already decrypted.
 *
 * Usage (inside the backend image):
 *   node dist/tools/backfillDecrypt.js [--days=90] [--batch=500] [--cursor=<hash>]
 */
import { buildChannelEntries, buildCombinedKeyStore, buildSummary } from '../mqtt/channelRegistry.js';
import { decodePacketCompat } from '../mqtt/decodePacket.js';
import { pool } from '../db/index.js';

function log(msg: string): void {
  const line = `[backfill ${new Date().toISOString()}] ${msg}`;
  process.stdout.write(line + '\n');
}

const args = process.argv.slice(2);
const daysArg = args.find((a) => a.startsWith('--days='));
const batchArg = args.find((a) => a.startsWith('--batch='));
const cursorArg = args.find((a) => a.startsWith('--cursor='));
const DAYS = daysArg ? Number(daysArg.split('=')[1]) : 90;
const BATCH = batchArg ? Number(batchArg.split('=')[1]) : 500;
let cursor = cursorArg ? cursorArg.split('=')[1] : '';

const channelEntries = buildChannelEntries(process.env['MESHCORE_CHANNEL_SECRETS']);
const keyStore = buildCombinedKeyStore(channelEntries);
log(`channels=${channelEntries.length} days=${DAYS} batch=${BATCH} cursor=${cursor || 'start'}`);

process.on('unhandledRejection', (err) => {
  log(`FATAL unhandledRejection: ${err instanceof Error ? err.stack : String(err)}`);
  process.exit(1);
});

async function fetchBatch(gt: string, limit: number): Promise<Array<{ packet_hash: string; raw_hex: string }>> {
  const res = await pool.query(
    `SELECT DISTINCT packet_hash, raw_hex
       FROM packets
      WHERE packet_type = 5
        AND raw_hex IS NOT NULL
        AND time > NOW() - ($1::int * INTERVAL '1 day')
        AND payload->>'decrypted' IS NULL
        AND packet_hash > $2
      ORDER BY packet_hash
      LIMIT $3`,
    [DAYS, gt, limit],
  );
  return res.rows as Array<{ packet_hash: string; raw_hex: string }>;
}

async function applyDecryptions(rows: Array<{ packet_hash: string; decrypted: unknown; summary: string }>): Promise<void> {
  await pool.query(
    `UPDATE packets
        SET payload = payload || jsonb_build_object(
              'decrypted', $1::jsonb,
              '_summary',  $2::text
            )
      WHERE packet_hash = ANY($3::text[])`,
    [JSON.stringify(rows.map((r) => r.decrypted)), JSON.stringify(rows.map((r) => r.summary)), rows.map((r) => r.packet_hash)],
  );
}

async function main(): Promise<void> {
  let processed = 0;
  let decrypted = 0;
  let failed = 0;
  const startedAt = Date.now();

  for (;;) {
    const rows = await fetchBatch(cursor, BATCH);
    if (rows.length === 0) break;

    const updates: Array<{ packet_hash: string; decrypted: unknown; summary: string }> = [];
    for (const row of rows) {
      processed += 1;
      try {
        const { decoded, metadataValid } = decodePacketCompat(row.raw_hex, keyStore);
        if (!metadataValid || !decoded?.payload?.decoded) { failed += 1; continue; }
        const inner = decoded.payload.decoded as { decrypted?: { sender?: string; message?: string } } | undefined;
        if (!inner?.decrypted) { failed += 1; continue; }
        const summary = buildSummary(decoded.payloadType, inner, row.raw_hex, channelEntries);
        updates.push({ packet_hash: row.packet_hash, decrypted: inner.decrypted, summary: summary ?? '[decrypted]' });
        decrypted += 1;
      } catch (err) {
        failed += 1;
      }
    }

    if (updates.length > 0) {
      await applyDecryptions(updates);
    }
    cursor = rows[rows.length - 1]!.packet_hash;

    if (processed % (BATCH * 10) === 0 || rows.length < BATCH) {
      log(`progress: ${processed} scanned, ${decrypted} decrypted, ${failed} undecryptable — ${Math.round((Date.now() - startedAt) / 1000)}s — cursor=${cursor.slice(0, 12)}…`);
    }
  }

  log(`DONE — scanned=${processed} decrypted=${decrypted} undecryptable=${failed} in ${Math.round((Date.now() - startedAt) / 1000)}s`);
  await pool.end();
}

main().catch((err) => {
  log(`FATAL: ${err instanceof Error ? err.stack : String(err)}`);
  process.exit(1);
});
