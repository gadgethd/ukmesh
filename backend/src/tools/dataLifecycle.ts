import { pool, query } from '../db/index.js';
import {
  assertDataLifecycleGate,
  compressionPolicy,
  DATA_COMPRESSION_POLICIES,
  DATA_LIFECYCLE_POLICIES,
  lifecyclePolicy,
} from '../db/dataLifecycle.js';

function argValue(name: string): string | undefined {
  const prefix = `${name}=`;
  return process.argv.find((arg) => arg.startsWith(prefix))?.slice(prefix.length);
}

async function inventory(): Promise<void> {
  const version = await query<{ extversion: string }>(
    `SELECT extversion FROM pg_extension WHERE extname = 'timescaledb'`,
  );
  console.log('[data-lifecycle] TimescaleDB', version.rows[0]?.extversion ?? 'unavailable');
  for (const policy of DATA_LIFECYCLE_POLICIES) {
    const result = await query<{
      expired_rows: string;
      oldest_at: string | null;
      newest_at: string | null;
      total_bytes: string;
    }>(
      `SELECT
         COUNT(*) FILTER (
           WHERE ${policy.timestampColumn} < NOW() - $1::interval
         )::text AS expired_rows,
         MIN(${policy.timestampColumn})::text AS oldest_at,
         MAX(${policy.timestampColumn})::text AS newest_at,
         pg_total_relation_size($2::regclass)::text AS total_bytes
       FROM ${policy.table}`,
      [policy.retention, policy.table],
    );
    let chunks: { expired_chunks: string; compressed_chunks: string } | null = null;
    if (policy.kind === 'hypertable') {
      chunks = await query<{ expired_chunks: string; compressed_chunks: string }>(
        `SELECT
           COUNT(*) FILTER (
             WHERE range_end < NOW() - $2::interval
           )::text AS expired_chunks,
           COUNT(*) FILTER (WHERE is_compressed)::text AS compressed_chunks
         FROM timescaledb_information.chunks
         WHERE hypertable_schema = 'public'
           AND hypertable_name = $1`,
        [policy.table, policy.retention],
      ).then((value) => value.rows[0] ?? {
        expired_chunks: '0',
        compressed_chunks: '0',
      });
    }
    console.log('[data-lifecycle] inventory', {
      table: policy.table,
      retention: policy.retention,
      exactExpiredRows: Number(result.rows[0]?.expired_rows ?? 0),
      expiredChunks: Number(chunks?.expired_chunks ?? 0),
      compressedChunks: Number(chunks?.compressed_chunks ?? 0),
      oldestAt: result.rows[0]?.oldest_at ?? null,
      newestAt: result.rows[0]?.newest_at ?? null,
      totalBytes: Number(result.rows[0]?.total_bytes ?? 0),
      featureImpact: policy.featureImpact,
    });
  }
  for (const policy of DATA_COMPRESSION_POLICIES) {
    if (DATA_LIFECYCLE_POLICIES.some((candidate) => candidate.table === policy.table)) continue;
    const result = await query<{
      oldest_at: string | null;
      newest_at: string | null;
      total_bytes: string;
      compressed_chunks: string;
    }>(
      `SELECT MIN(time)::text AS oldest_at,
              MAX(time)::text AS newest_at,
              pg_total_relation_size($1::regclass)::text AS total_bytes,
              (SELECT COUNT(*)::text
                 FROM timescaledb_information.chunks
                WHERE hypertable_schema = 'public'
                  AND hypertable_name = $2
                  AND is_compressed) AS compressed_chunks
         FROM ${policy.table}`,
      [policy.table, policy.table],
    );
    console.log('[data-lifecycle] inventory', {
      table: policy.table,
      retention: null,
      compressAfter: policy.compressAfter,
      compressedChunks: Number(result.rows[0]?.compressed_chunks ?? 0),
      oldestAt: result.rows[0]?.oldest_at ?? null,
      newestAt: result.rows[0]?.newest_at ?? null,
      totalBytes: Number(result.rows[0]?.total_bytes ?? 0),
      featureImpact: ['compression-only; packet path rows are never deleted'],
    });
  }
}

async function applyCompression(target: string, approval: string | undefined): Promise<void> {
  assertDataLifecycleGate({
    action: 'compression',
    target,
    approval,
  });
  const policy = compressionPolicy(target);
  const client = await pool.connect();
  try {
    await client.query('BEGIN');
    await client.query(`SET LOCAL lock_timeout = '5s'`);
    await client.query(`SET LOCAL statement_timeout = '30s'`);
    await client.query(
      `ALTER TABLE ${policy.table} SET (
         timescaledb.compress,
         timescaledb.compress_segmentby = '${policy.compressionSegmentBy}',
         timescaledb.compress_orderby = '${policy.compressionOrderBy}'
       )`,
    );
    await client.query(
      `SELECT add_compression_policy(
         $1::regclass,
         $2::interval,
         if_not_exists => TRUE
       )`,
      [policy.table, policy.compressAfter],
    );
    await client.query('COMMIT');
  } catch (error) {
    await client.query('ROLLBACK');
    throw error;
  } finally {
    client.release();
  }
  console.log('[data-lifecycle] compression policy enabled', {
    table: policy.table,
    compressAfter: policy.compressAfter,
  });
}

async function applyRetention(target: string, approval: string | undefined): Promise<void> {
  assertDataLifecycleGate({
    action: 'retention',
    target,
    approval,
  });
  const policy = lifecyclePolicy(target);
  if (policy.kind === 'hypertable') {
    await query(
      `SELECT add_retention_policy(
         $1::regclass,
         $2::interval,
         if_not_exists => TRUE
       )`,
      [policy.table, policy.retention],
    );
  } else {
    console.log(
      '[data-lifecycle] row-table retention is enabled for bounded health-worker deletion',
      { table: policy.table, retention: policy.retention },
    );
  }
  console.log('[data-lifecycle] retention enabled', {
    table: policy.table,
    retention: policy.retention,
    restoreReceipt: process.env['DATA_LIFECYCLE_RESTORE_RECEIPT_PATH'],
  });
}

async function main(): Promise<void> {
  const compression = process.argv.includes('--apply-compression');
  const retention = process.argv.includes('--apply-retention');
  if (compression && retention) {
    throw new Error('choose one lifecycle action per invocation');
  }
  if (!compression && !retention) {
    await inventory();
    return;
  }
  const target = argValue('--target');
  if (!target) throw new Error('--target=<table> is required');
  const approval = argValue('--approve');
  if (compression) {
    compressionPolicy(target);
    await applyCompression(target, approval);
  } else {
    lifecyclePolicy(target);
    await applyRetention(target, approval);
  }
}

main()
  .catch((error: unknown) => {
    console.error('[data-lifecycle] failed:', error instanceof Error ? error.message : error);
    process.exitCode = 1;
  })
  .finally(async () => {
    await pool.end();
  });
