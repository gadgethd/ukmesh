import { evaluateAdvert, refreshSpamDetectorCaches } from '../mqtt/spamDetector.js';
import { pool, query, replaceSpamSuspects, type SpamSuspectRow } from '../db/index.js';
import { UKMESH_NETWORKS } from '../networks.js';

type CandidateRow = {
  src_node_id: string;
  spoofed_name: string;
  public_key: string;
  claimed_lat: number | null;
  claimed_lon: number | null;
  hop_count: number | null;
  payload_timestamp: string | null;
  network: string | null;
};

const APPLY = process.argv.includes('--apply');
const WINDOW_DAYS = Number(process.env['SPAM_RECOMPUTE_WINDOW_DAYS'] ?? 7);

function toNumber(value: unknown): number | undefined {
  if (value == null) return undefined;
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : undefined;
}

async function main(): Promise<void> {
  console.log(`[spam-recompute] loading detector evidence over ${WINDOW_DAYS} day(s)`);
  await refreshSpamDetectorCaches();

  const candidates = await query<CandidateRow>(`
    SELECT DISTINCT ON (p.src_node_id)
      p.src_node_id,
      p.payload->>'_summary' AS spoofed_name,
      upper(p.payload->>'publicKey') AS public_key,
      CASE
        WHEN p.payload->'appData'->'location'->>'latitude' ~ '^-?[0-9]+(\\.[0-9]+)?$'
        THEN (p.payload->'appData'->'location'->>'latitude')::double precision
        ELSE NULL
      END AS claimed_lat,
      CASE
        WHEN p.payload->'appData'->'location'->>'longitude' ~ '^-?[0-9]+(\\.[0-9]+)?$'
        THEN (p.payload->'appData'->'location'->>'longitude')::double precision
        ELSE NULL
      END AS claimed_lon,
      p.hop_count,
      p.payload->>'timestamp' AS payload_timestamp,
      p.network
    FROM packets p
    WHERE p.packet_type = 4
      AND p.time > NOW() - ($1 * INTERVAL '1 day')
      AND p.payload ? 'publicKey'
      AND p.payload->>'publicKey' ~* '^[0-9a-f]{64}$'
      AND p.payload->>'_summary' IS NOT NULL
    ORDER BY p.src_node_id, p.time DESC
  `, [WINDOW_DAYS]);

  const suspects: SpamSuspectRow[] = [];
  const verdictCounts = new Map<string, number>();
  const signalCounts = new Map<string, number>();

  for (const row of candidates.rows) {
    const result = await evaluateAdvert({
      name: row.spoofed_name,
      publicKey: row.public_key,
      srcNodeId: row.src_node_id,
      lat: row.claimed_lat ?? undefined,
      lon: row.claimed_lon ?? undefined,
      hopCount: row.hop_count ?? undefined,
      payloadTimestamp: toNumber(row.payload_timestamp),
      network: row.network ?? 'ukmesh',
    });

    verdictCounts.set(result.verdict, (verdictCounts.get(result.verdict) ?? 0) + 1);
    for (const signal of result.signals) {
      signalCounts.set(signal.name, (signalCounts.get(signal.name) ?? 0) + 1);
    }

    if (result.verdict === 'clean') continue;
    suspects.push({
      srcNodeId: row.src_node_id,
      spoofedName: row.spoofed_name,
      publicKey: row.public_key,
      claimedLat: row.claimed_lat ?? undefined,
      claimedLon: row.claimed_lon ?? undefined,
      canonicalKey: result.canonicalKey,
      verdict: result.verdict,
      signals: result.signals,
      totalScore: result.totalScore,
      network: row.network ?? 'ukmesh',
    });
  }

  const current = await query<{ verdict: string; count: string }>(
    `SELECT verdict, COUNT(*) AS count FROM spam_suspects GROUP BY verdict ORDER BY verdict`
  );

  console.log(`[spam-recompute] evaluated ${candidates.rowCount} current advert identities`);
  console.log('[spam-recompute] current table:', Object.fromEntries(current.rows.map((row) => [row.verdict, Number(row.count)])));
  console.log('[spam-recompute] recomputed verdicts:', Object.fromEntries(verdictCounts));
  console.log('[spam-recompute] retained suspects:', {
    spam: suspects.filter((row) => row.verdict === 'spam').length,
    suspect: suspects.filter((row) => row.verdict === 'suspect').length,
    total: suspects.length,
  });
  console.log('[spam-recompute] signal counts:', Object.fromEntries(Array.from(signalCounts.entries()).sort((a, b) => b[1] - a[1])));

  if (!APPLY) {
    console.log('[spam-recompute] dry run only; rerun with --apply to replace spam_suspects');
    return;
  }

  await replaceSpamSuspects(UKMESH_NETWORKS, suspects);
  console.log(`[spam-recompute] replaced spam_suspects with ${suspects.length} row(s)`);
}

main()
  .catch((err: unknown) => {
    console.error('[spam-recompute] failed:', err instanceof Error ? err.message : err);
    process.exitCode = 1;
  })
  .finally(async () => {
    await pool.end();
  });
