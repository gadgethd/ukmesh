import { pool, query } from '../db/index.js';

type CountRow = { count: string };

function argValue(flag: string): string | undefined {
  const index = process.argv.indexOf(flag);
  return index >= 0 ? process.argv[index + 1] : undefined;
}

function boundedPositiveInteger(flag: string, fallback: number, max: number): number {
  const raw = argValue(flag);
  if (raw == null) return fallback;
  const value = Number(raw);
  if (!Number.isInteger(value) || value < 1 || value > max) {
    throw new Error(`${flag} must be an integer between 1 and ${max}`);
  }
  return value;
}

/**
 * Reconstruct small stats rollups without putting a history scan in a schema
 * migration transaction. Each time slice is an independent, idempotent UPSERT,
 * allowing normal ingest to proceed between short database statements.
 *
 *   node dist/tools/backfillStatsRollups.js                 # describe only
 *   node dist/tools/backfillStatsRollups.js --apply
 *   node dist/tools/backfillStatsRollups.js --apply --daily-days 31 --observer-days 8
 */
async function backfillDailyStats(days: number): Promise<void> {
  const dayResult = await query<{ day: string }>('SELECT CURRENT_DATE::text AS day');
  const baseDay = dayResult.rows[0]?.day;
  if (!baseDay) throw new Error('database did not return CURRENT_DATE');
  let written = 0;
  for (let daysAgo = days - 1; daysAgo >= 0; daysAgo -= 1) {
    const result = await query<CountRow>(
      `WITH daily_max AS (
         SELECT DISTINCT ON (p.network, p.time::date)
                p.network,
                p.time::date AS day,
                p.hop_count AS max_hop_count,
                p.packet_hash AS max_hop_hash,
                p.time AS max_hop_seen_at
         FROM packets p
         WHERE p.time >= $1::date - $2::integer
           AND p.time < $1::date - ($2::integer - 1)
           AND p.hop_count IS NOT NULL
         ORDER BY p.network, p.time::date, p.hop_count DESC, p.time DESC, p.packet_hash DESC
       ), inserted AS (
         INSERT INTO packet_daily_stats
           (network, day, max_hop_count, max_hop_hash, max_hop_seen_at, updated_at)
         SELECT network, day, max_hop_count, max_hop_hash, max_hop_seen_at, NOW()
         FROM daily_max
         ON CONFLICT (network, day) DO UPDATE SET
           max_hop_count = EXCLUDED.max_hop_count,
           max_hop_hash = EXCLUDED.max_hop_hash,
           max_hop_seen_at = EXCLUDED.max_hop_seen_at,
           updated_at = NOW()
         -- A live ingest write can race this maintenance pass. Only replace a
         -- rollup with an objectively better daily candidate, never with an
         -- older/lower historical value selected before that live write.
         WHERE packet_daily_stats.max_hop_count IS NULL
            OR EXCLUDED.max_hop_count > packet_daily_stats.max_hop_count
            OR (
              EXCLUDED.max_hop_count = packet_daily_stats.max_hop_count
              AND (
                EXCLUDED.max_hop_seen_at > COALESCE(packet_daily_stats.max_hop_seen_at, '-infinity'::timestamptz)
                OR (
                  EXCLUDED.max_hop_seen_at = packet_daily_stats.max_hop_seen_at
                  AND EXCLUDED.max_hop_hash > COALESCE(packet_daily_stats.max_hop_hash, '')
                )
              )
            )
         RETURNING 1
       )
       SELECT COUNT(*)::text AS count FROM inserted`,
      [baseDay, daysAgo],
    );
    written += Number(result.rows[0]?.count ?? 0);
  }
  console.log(`[stats-rollup-backfill] daily max rows upserted: ${written}`);
}

async function backfillObserverRegionRollups(days: number): Promise<void> {
  // Pin the rolling-window boundary so adjacent slices neither overlap nor leave
  // a gap if the job crosses a clock boundary.
  const nowResult = await query<{ now: string }>('SELECT NOW()::text AS now');
  const windowEnd = new Date(nowResult.rows[0]?.now ?? Date.now());
  let packetRows = 0;
  let observerRows = 0;

  for (let daysAgo = days - 1; daysAgo >= 0; daysAgo -= 1) {
    const start = new Date(windowEnd.getTime() - (daysAgo + 1) * 86_400_000);
    const end = new Date(windowEnd.getTime() - daysAgo * 86_400_000);
    const params = [start.toISOString(), end.toISOString()];

    const packetResult = await query<CountRow>(
      `WITH sightings AS (
         SELECT
           p.network,
           COALESCE(NULLIF(TRIM(UPPER(split_part(p.topic, '/', 2))), ''), 'UNK') AS iata,
           UPPER(p.packet_hash) AS packet_hash,
           MIN(p.time) AS first_seen,
           MAX(p.time) AS last_seen
         FROM packets p
         WHERE p.time > $1::timestamptz
           AND p.time <= $2::timestamptz
           AND p.packet_hash IS NOT NULL
           AND p.rx_node_id ~ '^[0-9A-Fa-f]{64}$'
           AND p.network IS DISTINCT FROM 'test'
           AND split_part(p.topic, '/', 1) <> 'meshcore-test'
         GROUP BY p.network, COALESCE(NULLIF(TRIM(UPPER(split_part(p.topic, '/', 2))), ''), 'UNK'), UPPER(p.packet_hash)
       ), inserted AS (
         INSERT INTO observer_region_packet_sightings
           (network, iata, packet_hash, first_seen, last_seen)
         SELECT network, iata, packet_hash, first_seen, last_seen
         FROM sightings
         ON CONFLICT (network, iata, packet_hash) DO UPDATE SET
           first_seen = LEAST(observer_region_packet_sightings.first_seen, EXCLUDED.first_seen),
           last_seen = GREATEST(observer_region_packet_sightings.last_seen, EXCLUDED.last_seen)
         RETURNING 1
       )
       SELECT COUNT(*)::text AS count FROM inserted`,
      params,
    );
    packetRows += Number(packetResult.rows[0]?.count ?? 0);

    const observerResult = await query<CountRow>(
      `WITH sightings AS (
         SELECT
           p.network,
           COALESCE(NULLIF(TRIM(UPPER(split_part(p.topic, '/', 2))), ''), 'UNK') AS iata,
           UPPER(p.rx_node_id) AS rx_node_id,
           MIN(p.time) AS first_seen,
           MAX(p.time) AS last_seen
         FROM packets p
         WHERE p.time > $1::timestamptz
           AND p.time <= $2::timestamptz
           AND p.rx_node_id ~ '^[0-9A-Fa-f]{64}$'
           AND p.network IS DISTINCT FROM 'test'
           AND split_part(p.topic, '/', 1) <> 'meshcore-test'
         GROUP BY p.network, COALESCE(NULLIF(TRIM(UPPER(split_part(p.topic, '/', 2))), ''), 'UNK'), UPPER(p.rx_node_id)
       ), inserted AS (
         INSERT INTO observer_region_observer_sightings
           (network, iata, rx_node_id, first_seen, last_seen)
         SELECT network, iata, rx_node_id, first_seen, last_seen
         FROM sightings
         ON CONFLICT (network, iata, rx_node_id) DO UPDATE SET
           first_seen = LEAST(observer_region_observer_sightings.first_seen, EXCLUDED.first_seen),
           last_seen = GREATEST(observer_region_observer_sightings.last_seen, EXCLUDED.last_seen)
         RETURNING 1
       )
       SELECT COUNT(*)::text AS count FROM inserted`,
      params,
    );
    observerRows += Number(observerResult.rows[0]?.count ?? 0);
  }

  console.log(`[stats-rollup-backfill] observer-region packet rows upserted: ${packetRows}`);
  console.log(`[stats-rollup-backfill] observer-region observer rows upserted: ${observerRows}`);
}

async function main(): Promise<void> {
  const apply = process.argv.includes('--apply');
  const dailyDays = boundedPositiveInteger('--daily-days', 31, 366);
  const observerDays = boundedPositiveInteger('--observer-days', 8, 31);

  console.log(
    `[stats-rollup-backfill] daily=${dailyDays} calendar day(s), observer=${observerDays} rolling day(s), apply=${apply}`,
  );
  if (!apply) {
    console.log('[stats-rollup-backfill] dry run only; rerun with --apply during a low-traffic window');
    return;
  }

  await backfillDailyStats(dailyDays);
  await backfillObserverRegionRollups(observerDays);
}

main()
  .catch((err: unknown) => {
    console.error('[stats-rollup-backfill] failed:', err instanceof Error ? err.message : err);
    process.exitCode = 1;
  })
  .finally(async () => {
    await pool.end();
  });
