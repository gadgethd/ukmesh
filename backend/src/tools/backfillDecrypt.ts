/**
 * Backfill decryption — decrypt stored raw group-text packets retroactively.
 *
 * Two-phase design:
 *  Phase A: materialise the candidate list ONCE into backfill_candidates
 *           (single DISTINCT scan of the 90-day type-5 corpus).
 *  Phase B: keyset-paginate the candidates table (pk index — instant),
 *           decrypt each raw packet, INSERT into packet_decryptions.
 *
 * Results live in packet_decryptions (joined by the feed queries); the
 * packets hypertable is never rewritten (TimescaleDB UPDATEs seq-scan all
 * chunks). Idempotent + resumable: candidates exclude already-decrypted
 * hashes; decryptions INSERT uses ON CONFLICT DO NOTHING.
 *
 * Usage (inside the backend image):
 *   node dist/tools/backfillDecrypt.js [--days=90] [--batch=500]
 */
import { buildChannelEntries, buildCombinedKeyStore, buildSummary } from '../mqtt/channelRegistry.js';
import { decodePacketCompat } from '../mqtt/decodePacket.js';
import { pool } from '../db/index.js';

function log(msg: string): void {
  process.stdout.write(`[backfill ${new Date().toISOString()}] ${msg}\n`);
}

const args = process.argv.slice(2);
const daysArg = args.find((a) => a.startsWith('--days='));
const batchArg = args.find((a) => a.startsWith('--batch='));
const DAYS = daysArg ? Number(daysArg.split('=')[1]) : 90;
const BATCH = batchArg ? Number(batchArg.split('=')[1]) : 500;

const channelEntries = buildChannelEntries(process.env['MESHCORE_CHANNEL_SECRETS']);
const keyStore = buildCombinedKeyStore(channelEntries);
log(`channels=${channelEntries.length} days=${DAYS} batch=${BATCH}`);

process.on('unhandledRejection', (err) => {
  log(`FATAL unhandledRejection: ${err instanceof Error ? err.stack : String(err)}`);
  process.exit(1);
});

async function phaseA(): Promise<number> {
  log('phase A: materialising candidate hashes (single scan)…');
  await pool.query('DROP TABLE IF EXISTS backfill_candidates');
  await pool.query(
    `CREATE TABLE backfill_candidates AS
       SELECT DISTINCT ON (packet_hash) packet_hash, upper(raw_hex) AS raw_hex
         FROM packets p
        WHERE packet_type = 5
          AND raw_hex IS NOT NULL
          AND time > NOW() - ($1::int * INTERVAL '1 day')
          AND NOT EXISTS (SELECT 1 FROM packet_decryptions d WHERE d.packet_hash = p.packet_hash)
        ORDER BY packet_hash`,
    [DAYS],
  );
  await pool.query('ALTER TABLE backfill_candidates ADD PRIMARY KEY (packet_hash)');
  const res = await pool.query('SELECT count(*)::int AS n FROM backfill_candidates');
  log(`phase A done — ${res.rows[0]!.n} candidates`);
  return res.rows[0]!.n as number;
}

async function fetchBatch(gt: string, limit: number): Promise<Array<{ packet_hash: string; raw_hex: string }>> {
  const res = await pool.query(
    `SELECT packet_hash, raw_hex FROM backfill_candidates
      WHERE packet_hash > $1 ORDER BY packet_hash LIMIT $2`,
    [gt, limit],
  );
  return res.rows as Array<{ packet_hash: string; raw_hex: string }>;
}

async function applyDecryptions(rows: Array<{ packet_hash: string; decrypted: unknown; summary: string }>): Promise<void> {
  await pool.query(
    `INSERT INTO packet_decryptions (packet_hash, decrypted, summary)
     SELECT * FROM jsonb_to_recordset($1::jsonb) AS x(packet_hash text, decrypted jsonb, summary text)
     ON CONFLICT (packet_hash) DO NOTHING`,
    [JSON.stringify(rows.map((r) => ({ packet_hash: r.packet_hash, decrypted: r.decrypted, summary: r.summary })))],
  );
}

async function main(): Promise<void> {
  const startedAt = Date.now();
  let cursor = '';
  let processed = 0;
  let decrypted = 0;
  let failed = 0;

  const total = await phaseA();

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
      } catch {
        failed += 1;
      }
    }

    if (updates.length > 0) {
      await applyDecryptions(updates);
    }
    cursor = rows[rows.length - 1]!.packet_hash;

    if (processed % (BATCH * 20) === 0 || rows.length < BATCH) {
      const pct = total > 0 ? ` ${Math.round((100 * processed) / total)}%` : '';
      log(`progress: ${processed}/${total}${pct} — ${decrypted} decrypted, ${failed} undecryptable — ${Math.round((Date.now() - startedAt) / 1000)}s`);
    }
  }

  await pool.query('DROP TABLE IF EXISTS backfill_candidates');
  log(`DONE — scanned=${processed} decrypted=${decrypted} undecryptable=${failed} in ${Math.round((Date.now() - startedAt) / 1000)}s`);
  await pool.end();
}

main().catch((err) => {
  log(`FATAL: ${err instanceof Error ? err.stack : String(err)}`);
  process.exit(1);
});
