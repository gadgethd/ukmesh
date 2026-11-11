import type pg from 'pg';

export class PrivacyConvergenceError extends Error {}

type Target = 'packets' | 'packet_paths';
type TableState = { target: Target; rows: string; mismatches: string };
export type ConvergenceState = {
  generation: string | null;
  materializedGeneration: string | null;
  queue: { status: string; count: string }[];
  tables: TableState[];
  needsConvergence: boolean;
};
export type ConvergenceOptions = {
  apply?: boolean;
  expectedGeneration?: string;
  statementTimeoutMs?: number;
};

// Fixed identifiers only. Both packet stores use the authoritative DB policy,
// including framing validation and the shared UK privacy-network scope.
// Normalize legacy lowercase hashes before calling the path helper, just as
// the packet INSERT classifier compares UPPER(packet_prefix) in migration 046.
const privateMatch = `meshcore_path_matches_private(network, rx_node_id, src_node_id,
  ARRAY(SELECT UPPER(hash) FROM unnest(path_hashes) AS hash), path_hash_size_bytes)`;
const validAndPublic = `(meshcore_path_is_valid(path_hashes, path_hash_size_bytes) IS TRUE) AND NOT ${privateMatch}`;

function classification(target: Target): string {
  return `SELECT row_to_classify.*,
    ${privateMatch} AS expected_private,
    ${validAndPublic} AS expected_visible
    FROM ${target} row_to_classify`;
}

type PolicyState = Pick<ConvergenceState, 'generation' | 'materializedGeneration' | 'queue'>;

async function inspectPolicy(client: pg.PoolClient): Promise<PolicyState> {
  const visibility = await client.query<{ generation: string; materialized_generation: string | null }>(`
    SELECT visibility.generation::text, materialized.visibility_generation::text AS materialized_generation
      FROM public_visibility_state visibility
      LEFT JOIN packet_visibility_materialization_state materialized USING (singleton)
     WHERE visibility.singleton = TRUE
  `);
  const queue = await client.query<{ status: string; count: string }>(`
    SELECT status, COUNT(*)::text AS count FROM privacy_rematerialization_queue
     GROUP BY status ORDER BY status
  `);
  return {
    generation: visibility.rows[0]?.generation ?? null,
    materializedGeneration: visibility.rows[0]?.materialized_generation ?? null,
    queue: queue.rows,
  };
}

async function inspect(client: pg.PoolClient, policy?: PolicyState): Promise<ConvergenceState> {
  const state = policy ?? await inspectPolicy(client);
  const tables: TableState[] = [];
  for (const target of ['packets', 'packet_paths'] as const) {
    const { rows } = await client.query<{ rows: string; mismatches: string }>(`
      SELECT COUNT(*)::text AS rows, COUNT(*) FILTER (
        WHERE is_private IS DISTINCT FROM expected_private
           OR visibility_ok IS DISTINCT FROM expected_visible
      )::text AS mismatches FROM (${classification(target)}) classified
    `);
    tables.push({ target, ...rows[0] });
  }
  return {
    ...state, tables,
    needsConvergence: state.generation === null || state.generation !== state.materializedGeneration
      || state.queue.some((row) => row.status !== 'done' && row.count !== '0')
      || tables.some((table) => table.mismatches !== '0'),
  };
}

export function validateGeneration(value: string | undefined): string {
  if (!value || !/^[1-9][0-9]*$/.test(value)
    || BigInt(value) >= BigInt(Number.MAX_SAFE_INTEGER)) {
    throw new PrivacyConvergenceError('EXPECTED_GENERATION_REQUIRED: use the positive generation reported by dry-run');
  }
  return value;
}

/**
 * Dry-run is a read-only, consistent snapshot. Apply deliberately takes a
 * maintenance lock on every policy/queue writer and both packet stores, then
 * recomputes and verifies ALL retained rows before publishing one generation.
 * Lock/statement timeouts or any verification error roll the whole repair back.
 */
export async function convergePacketVisibility(pool: Pick<pg.Pool, 'connect'>, options: ConvergenceOptions = {}) {
  const apply = options.apply === true;
  if (apply) validateGeneration(options.expectedGeneration);
  const timeout = options.statementTimeoutMs ?? (apply ? 900_000 : 30_000);
  if (!Number.isInteger(timeout) || timeout < 1_000 || timeout > 3_600_000) {
    throw new PrivacyConvergenceError('STATEMENT_TIMEOUT_OUT_OF_RANGE: expected 1000..3600000 milliseconds');
  }
  const client = await pool.connect();
  try {
    await client.query(apply ? 'BEGIN' : 'BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY');
    await client.query("SET LOCAL lock_timeout = '5s'");
    await client.query("SELECT set_config('statement_timeout', $1, TRUE)", [`${timeout}ms`]);
    // TimescaleDB 2.25.1 can fail a parallel readback after compressed DML
    // with "subplan ... was not initialized". Keep this maintenance scan
    // serial, as in the application's DB service configuration. LOCAL means
    // neither the server nor a later pooled query inherits the setting.
    await client.query('SET LOCAL max_parallel_workers_per_gather = 0');
    if (apply) {
      // Queue locking excludes claims and completion. Policy table locking
      // excludes node transitions, direct prefixes and alias invalidations.
      // Packet locks also exclude writers that do not pin the generation.
      // A competing transaction can cause a bounded deadlock/lock timeout;
      // aborting is safe, and the operator may retry after it finishes.
      await client.query(`LOCK TABLE nodes, private_node_prefixes, node_identity_aliases,
        privacy_rematerialization_queue, packets, packet_paths IN SHARE ROW EXCLUSIVE MODE`);
      await client.query('SELECT generation FROM public_visibility_state WHERE singleton = TRUE FOR UPDATE');
    }
    const policy = await inspectPolicy(client);
    // Refuse before expensive historical scans while maintenance locks are
    // held: a running worker or stale approval must not wait for a full audit.
    if (apply && policy.generation !== options.expectedGeneration) {
      throw new PrivacyConvergenceError('VISIBILITY_GENERATION_CHANGED: rerun dry-run and review the current generation');
    }
    if (apply && policy.queue.some((row) => row.status === 'processing' && row.count !== '0')) {
      throw new PrivacyConvergenceError('PRIVACY_REMATERIALIZATION_PROCESSING: wait for the active worker; orphaned claims require operator investigation');
    }
    const before = await inspect(client, policy);
    if (!apply) {
      await client.query('COMMIT');
      return { mode: 'dry-run' as const, before, updated: null };
    }
    if (!before.needsConvergence) {
      await client.query('COMMIT');
      return { mode: 'apply' as const, before, after: before, updated: { packets: 0, packet_paths: 0, queue: 0 } };
    }
    await client.query('SET LOCAL timescaledb.max_tuples_decompressed_per_dml_transaction = 0');
    const updated = { packets: 0, packet_paths: 0, queue: 0 };
    for (const target of ['packets', 'packet_paths'] as const) {
      // Compare to the very same expressions used for inspection. No blanket
      // flag resets, time cutoffs, node filters, or skipped compressed chunks.
      const result = await client.query(`UPDATE ${target} row_to_classify
        SET is_private = ${privateMatch}, visibility_ok = ${validAndPublic}
        WHERE is_private IS DISTINCT FROM ${privateMatch}
           OR visibility_ok IS DISTINCT FROM (${validAndPublic})`);
      updated[target] = result.rowCount ?? 0;
    }
    const verified = await inspect(client);
    if (verified.tables.some((table) => table.mismatches !== '0')) {
      throw new PrivacyConvergenceError('PRIVACY_CONVERGENCE_READBACK_FAILED');
    }
    const completed = await client.query(`UPDATE privacy_rematerialization_queue
      SET status = 'done', finished_at = NOW(), last_error = NULL
      WHERE status IN ('pending', 'failed')`);
    updated.queue = completed.rowCount ?? 0;
    await client.query(`WITH next_generation AS (
      UPDATE public_visibility_state SET generation = generation + 1, updated_at = NOW()
       WHERE singleton = TRUE RETURNING generation
    ) INSERT INTO packet_visibility_materialization_state (singleton, visibility_generation, updated_at)
      SELECT TRUE, generation, NOW() FROM next_generation
      ON CONFLICT (singleton) DO UPDATE
        SET visibility_generation = EXCLUDED.visibility_generation, updated_at = EXCLUDED.updated_at`);
    const after = await inspect(client);
    if (after.needsConvergence) throw new PrivacyConvergenceError('PRIVACY_CONVERGENCE_PUBLISH_FAILED');
    await client.query('COMMIT');
    return { mode: 'apply' as const, before, after, updated };
  } catch (error) {
    await client.query('ROLLBACK').catch(() => {});
    throw error;
  } finally {
    client.release();
  }
}
