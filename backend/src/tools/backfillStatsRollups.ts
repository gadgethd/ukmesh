import { pool, query } from '../db/index.js';
import { publicPacketPrivacySql } from '../api/utils/networkFilters.js';

const PUBLIC_PACKET_PRIVACY_SQL = publicPacketPrivacySql('p');

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

async function describeHourlyBackfill(days: number): Promise<void> {
  const result = await query<{
    window_start: string;
    window_end: string;
    source_rows: string;
    source_hours: string;
    existing_rollup_rows: string;
  }>(
    `WITH bounds AS (
       SELECT date_trunc('hour', NOW()) AS window_end,
              date_trunc('hour', NOW()) - ($1::integer * INTERVAL '1 day') AS window_start
     )
     SELECT
       b.window_start::text,
       b.window_end::text,
       (SELECT COUNT(*)::text
          FROM packets p
         WHERE p.time >= b.window_start
           AND p.time < b.window_end
           AND (
             p.network = 'test'
             OR COALESCE(NULLIF(p.topic_prefix, ''), split_part(p.topic, '/', 1)) <> 'meshcore-test'
           )) AS source_rows,
       (SELECT COUNT(DISTINCT date_trunc('hour', p.time))::text
          FROM packets p
         WHERE p.time >= b.window_start
           AND p.time < b.window_end
           AND (
             p.network = 'test'
             OR COALESCE(NULLIF(p.topic_prefix, ''), split_part(p.topic, '/', 1)) <> 'meshcore-test'
           )) AS source_hours,
       (SELECT COUNT(*)::text
          FROM packet_hourly_stats s
         WHERE s.hour >= b.window_start
           AND s.hour < b.window_end) AS existing_rollup_rows
     FROM bounds b`,
    [days],
  );
  console.log('[stats-rollup-backfill] hourly dry-run', result.rows[0]);
}

async function backfillHourlyStats(days: number, pauseMs: number): Promise<void> {
  const jobName = 'packet-hourly-stats-v1';
  const checkpoint = await query<{
    cursor_value: string;
    window_end: string;
    rows_processed: string;
    completed_at: string | null;
  }>(
    `SELECT cursor_value, window_end::text, rows_processed::text, completed_at::text
       FROM maintenance_backfill_checkpoints
      WHERE job_name = $1`,
    [jobName],
  );
  if (checkpoint.rows[0]?.completed_at) {
    console.log('[stats-rollup-backfill] hourly checkpoint is already complete', checkpoint.rows[0]);
    return;
  }

  const bounds = checkpoint.rows[0]
    ? {
        cursor: new Date(checkpoint.rows[0].cursor_value),
        windowEnd: new Date(checkpoint.rows[0].window_end),
      }
    : await query<{ cursor: string; window_end: string }>(
        `SELECT
           (date_trunc('hour', NOW()) - ($1::integer * INTERVAL '1 day'))::text AS cursor,
           date_trunc('hour', NOW())::text AS window_end`,
        [days],
      ).then((result) => ({
        cursor: new Date(result.rows[0]!.cursor),
        windowEnd: new Date(result.rows[0]!.window_end),
      }));
  if (
    !Number.isFinite(bounds.cursor.getTime())
    || !Number.isFinite(bounds.windowEnd.getTime())
    || bounds.cursor >= bounds.windowEnd
  ) {
    throw new Error('invalid hourly backfill checkpoint bounds');
  }

  let cursor = bounds.cursor;
  let processed = Number(checkpoint.rows[0]?.rows_processed ?? 0);
  while (cursor < bounds.windowEnd) {
    const sliceEnd = new Date(
      Math.min(bounds.windowEnd.getTime(), cursor.getTime() + 60 * 60_000),
    );
    const client = await pool.connect();
    try {
      await client.query('BEGIN');
      await client.query(`SET LOCAL lock_timeout = '5s'`);
      await client.query(`SET LOCAL statement_timeout = '2min'`);
      await client.query(
        `DELETE FROM packet_hourly_stats
          WHERE hour >= $1::timestamptz AND hour < $2::timestamptz`,
        [cursor.toISOString(), sliceEnd.toISOString()],
      );
      const inserted = await client.query<CountRow>(
        `WITH source AS (
           SELECT
             p.network,
             date_trunc('hour', p.time) AS hour,
             COALESCE(p.packet_type, -1) AS packet_type,
             COALESCE(p.hop_count, -1) AS hop_count,
             COALESCE(p.route_type, -1) AS route_type,
             COALESCE(NULLIF(TRIM(p.transport_codes), ''), '') AS transport_code,
             COALESCE(NULLIF(TRIM(p.region_scope), ''), '') AS region_scope,
             COUNT(*)::bigint AS packet_count,
             COALESCE(SUM(p.rssi), 0)::double precision AS rssi_sum,
             COUNT(p.rssi)::bigint AS rssi_count,
             COALESCE(SUM(p.snr), 0)::double precision AS snr_sum,
             COUNT(p.snr)::bigint AS snr_count
           FROM packets p
           WHERE p.time >= $1::timestamptz
             AND p.time < $2::timestamptz
             AND (
               p.network = 'test'
               OR COALESCE(NULLIF(p.topic_prefix, ''), split_part(p.topic, '/', 1)) <> 'meshcore-test'
             )
           GROUP BY 1, 2, 3, 4, 5, 6, 7
         ), inserted AS (
           INSERT INTO packet_hourly_stats (
             network, hour, packet_type, hop_count, route_type,
             transport_code, region_scope, packet_count,
             rssi_sum, rssi_count, snr_sum, snr_count, updated_at
           )
           SELECT
             network, hour, packet_type, hop_count, route_type,
             transport_code, region_scope, packet_count,
             rssi_sum, rssi_count, snr_sum, snr_count, NOW()
           FROM source
           RETURNING packet_count
         )
         SELECT COALESCE(SUM(packet_count), 0)::text AS count FROM inserted`,
        [cursor.toISOString(), sliceEnd.toISOString()],
      );
      processed += Number(inserted.rows[0]?.count ?? 0);
      const complete = sliceEnd >= bounds.windowEnd;
      await client.query(
        `INSERT INTO maintenance_backfill_checkpoints (
           job_name, cursor_value, window_end, rows_processed, updated_at, completed_at
         )
         VALUES ($1, $2, $3, $4, NOW(), CASE WHEN $5 THEN NOW() ELSE NULL END)
         ON CONFLICT (job_name) DO UPDATE SET
           cursor_value = EXCLUDED.cursor_value,
           window_end = EXCLUDED.window_end,
           rows_processed = EXCLUDED.rows_processed,
           updated_at = NOW(),
           completed_at = EXCLUDED.completed_at`,
        [jobName, sliceEnd.toISOString(), bounds.windowEnd.toISOString(), processed, complete],
      );
      await client.query('COMMIT');
    } catch (error) {
      await client.query('ROLLBACK');
      throw error;
    } finally {
      client.release();
    }
    cursor = sliceEnd;
    if (pauseMs > 0 && cursor < bounds.windowEnd) {
      await new Promise((resolve) => setTimeout(resolve, pauseMs));
    }
  }
  console.log(`[stats-rollup-backfill] hourly complete; source rows processed=${processed}`);
}

const MAX_CUTOVER_GAP_HOURS = 48;

/**
 * Reconcile every hour between the completed historical checkpoint and now
 * after the aggregate-writing backend is live. This closes a deployment gap
 * without reopening the completed history job. One short writer lock per hour
 * makes each delete/rebuild atomic with packet-batch increments: transactions
 * already in flight finish first, while later transactions increment the
 * reconstructed rows after the lock is released.
 */
async function catchUpCurrentHour(): Promise<void> {
  const bounds = await query<{
    checkpoint_end: string | null;
    current_hour: string;
    window_end: string;
  }>(
    `SELECT (
       SELECT window_end::text
         FROM maintenance_backfill_checkpoints
        WHERE job_name = 'packet-hourly-stats-v1'
          AND completed_at IS NOT NULL
     ) AS checkpoint_end,
     date_trunc('hour', NOW())::text AS current_hour,
     NOW()::text AS window_end`,
  );
  const row = bounds.rows[0];
  if (!row?.current_hour || !row.window_end) {
    throw new Error('database did not return cutover bounds');
  }
  const currentHour = new Date(row.current_hour);
  const windowEnd = new Date(row.window_end);
  const checkpointEnd = row.checkpoint_end ? new Date(row.checkpoint_end) : currentHour;
  if (
    !Number.isFinite(currentHour.getTime())
    || !Number.isFinite(windowEnd.getTime())
    || !Number.isFinite(checkpointEnd.getTime())
    || currentHour > windowEnd
  ) {
    throw new Error('database returned invalid cutover bounds');
  }
  const gapHours = Math.max(0, (currentHour.getTime() - checkpointEnd.getTime()) / 3_600_000);
  if (gapHours > MAX_CUTOVER_GAP_HOURS) {
    throw new Error(
      `cutover gap is ${gapHours.toFixed(1)} hours; maximum is ${MAX_CUTOVER_GAP_HOURS}`,
    );
  }

  let cursor = checkpointEnd < currentHour ? checkpointEnd : currentHour;
  let processed = 0;
  let slices = 0;
  while (cursor < windowEnd) {
    const sliceEnd = new Date(
      Math.min(windowEnd.getTime(), cursor.getTime() + 3_600_000),
    );
    const client = await pool.connect();
    try {
      await client.query('BEGIN');
      await client.query(`SET LOCAL lock_timeout = '5s'`);
      await client.query(`SET LOCAL statement_timeout = '30s'`);
      await client.query('LOCK TABLE packet_hourly_stats IN SHARE ROW EXCLUSIVE MODE');
      await client.query(
        `DELETE FROM packet_hourly_stats
          WHERE hour >= $1::timestamptz AND hour < $2::timestamptz`,
        [cursor.toISOString(), sliceEnd.toISOString()],
      );
      const inserted = await client.query<CountRow>(
        `WITH source AS (
           SELECT
             p.network,
             date_trunc('hour', p.time) AS hour,
             COALESCE(p.packet_type, -1) AS packet_type,
             COALESCE(p.hop_count, -1) AS hop_count,
             COALESCE(p.route_type, -1) AS route_type,
             COALESCE(NULLIF(TRIM(p.transport_codes), ''), '') AS transport_code,
             COALESCE(NULLIF(TRIM(p.region_scope), ''), '') AS region_scope,
             COUNT(*)::bigint AS packet_count,
             COALESCE(SUM(p.rssi), 0)::double precision AS rssi_sum,
             COUNT(p.rssi)::bigint AS rssi_count,
             COALESCE(SUM(p.snr), 0)::double precision AS snr_sum,
             COUNT(p.snr)::bigint AS snr_count
           FROM packets p
           WHERE p.time >= $1::timestamptz
             AND p.time < $2::timestamptz
             AND (
               p.network = 'test'
               OR COALESCE(NULLIF(p.topic_prefix, ''), split_part(p.topic, '/', 1)) <> 'meshcore-test'
             )
           GROUP BY 1, 2, 3, 4, 5, 6, 7
         ), inserted AS (
           INSERT INTO packet_hourly_stats (
             network, hour, packet_type, hop_count, route_type,
             transport_code, region_scope, packet_count,
             rssi_sum, rssi_count, snr_sum, snr_count, updated_at
           )
           SELECT
             network, hour, packet_type, hop_count, route_type,
             transport_code, region_scope, packet_count,
             rssi_sum, rssi_count, snr_sum, snr_count, NOW()
           FROM source
           RETURNING packet_count
         )
         SELECT COALESCE(SUM(packet_count), 0)::text AS count FROM inserted`,
        [cursor.toISOString(), sliceEnd.toISOString()],
      );
      await client.query('COMMIT');
      processed += Number(inserted.rows[0]?.count ?? 0);
      slices += 1;
    } catch (error) {
      await client.query('ROLLBACK');
      throw error;
    } finally {
      client.release();
    }
    cursor = sliceEnd;
  }
  console.log(
    `[stats-rollup-backfill] cutover gap reconciled; slices=${slices} source rows=${processed} window_end=${row.window_end}`,
  );
}

/**
 * Reconstruct small stats rollups without putting a history scan in a schema
 * migration transaction. Each time slice is an independent, idempotent UPSERT,
 * allowing normal ingest to proceed between short database statements.
 *
 *   node dist/tools/backfillStatsRollups.js                 # describe only
 *   node dist/tools/backfillStatsRollups.js --apply
 *   node dist/tools/backfillStatsRollups.js --apply --catch-up-current-hour
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
           AND ${PUBLIC_PACKET_PRIVACY_SQL}
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
           AND ${PUBLIC_PACKET_PRIVACY_SQL}
           AND p.network IS DISTINCT FROM 'test'
           AND COALESCE(NULLIF(p.topic_prefix, ''), split_part(p.topic, '/', 1)) <> 'meshcore-test'
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
           AND ${PUBLIC_PACKET_PRIVACY_SQL}
           AND p.network IS DISTINCT FROM 'test'
           AND COALESCE(NULLIF(p.topic_prefix, ''), split_part(p.topic, '/', 1)) <> 'meshcore-test'
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
  const catchUpCutoverHour = process.argv.includes('--catch-up-current-hour');
  const dailyDays = boundedPositiveInteger('--daily-days', 31, 366);
  const observerDays = boundedPositiveInteger('--observer-days', 8, 31);
  const hourlyDays = boundedPositiveInteger('--hourly-days', 8, 366);
  const pauseMs = boundedPositiveInteger('--pause-ms', 50, 5_000);

  console.log(
    `[stats-rollup-backfill] hourly=${hourlyDays} day(s), daily=${dailyDays} calendar day(s), observer=${observerDays} rolling day(s), apply=${apply}, catchUpCurrentHour=${catchUpCutoverHour}`,
  );
  if (catchUpCutoverHour) {
    if (!apply) throw new Error('--catch-up-current-hour requires --apply');
    await catchUpCurrentHour();
    return;
  }
  if (!apply) {
    await describeHourlyBackfill(hourlyDays);
    console.log('[stats-rollup-backfill] dry run only; rerun with --apply during a low-traffic window');
    return;
  }

  await backfillHourlyStats(hourlyDays, pauseMs);
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
