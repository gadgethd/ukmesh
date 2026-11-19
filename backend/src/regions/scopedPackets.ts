import type pg from 'pg';
import { publicPacketPrivacySql } from '../platform/query/networkFilters.js';
import { UKMESH_NETWORK_SQL_LIST } from '../networks.js';
import type { regionRebuildSettings } from './rebuildSettings.js';

export interface ScopedPacketRow {
  packet_hash: string;
  t0: Date;
  t1: Date;
  raw: string | null;
  elems: string[] | null;
  observers: string[] | null;
}

// Keep each packet's path union local instead of globally sorting expanded paths.
// Native array text round-trips variable lengths, dimensions, nulls and escaping.
export const SCOPED_PACKETS_SQL = `WITH scoped_observations AS (
  SELECT p.packet_hash, p.time, p.raw_hex, p.rx_node_id, p.path_hashes, p.route_type
    FROM mesh.packets p
        WHERE p.network IN (${UKMESH_NETWORK_SQL_LIST})
          AND ${publicPacketPrivacySql('p')}
          AND p.route_type IN (0, 3)
          AND p.time > now() - make_interval(days => $1)
), packet_observation_groups AS (
  SELECT p.packet_hash,
         array_agg(p.path_hashes::text) AS paths,
         min(p.time) AS t0,
         max(p.time) AS t1,
         (array_agg(p.raw_hex ORDER BY p.time DESC)
                 FILTER (WHERE p.route_type IN (0, 3) AND p.raw_hex <> ''))[1] AS raw,
         (array_agg(DISTINCT p.rx_node_id) FILTER (WHERE p.rx_node_id IS NOT NULL))[1:40] AS observers
    FROM scoped_observations p
   GROUP BY p.packet_hash
)
SELECT observations.packet_hash, observations.t0, observations.t1,
       observations.raw,
       (SELECT (array_agg(DISTINCT e) FILTER (WHERE e IS NOT NULL))[1:80]
          FROM unnest(observations.paths) AS path(value)
          CROSS JOIN LATERAL unnest(path.value::text[]) AS e) AS elems,
       observations.observers
  FROM packet_observation_groups observations
 ORDER BY observations.packet_hash
 LIMIT $2
`;

const PACKET_BATCH_SIZE = 2_048;

/** The separate connection is already budgeted alongside the region publisher. */
export async function readScopedPackets(
  pool: Pick<pg.Pool, 'connect'>,
  settings: Pick<ReturnType<typeof regionRebuildSettings>, 'windowDays' | 'maxPackets' | 'statementTimeoutMs'>,
  visit: (row: ScopedPacketRow) => void,
): Promise<number> {
  const client = await pool.connect();
  let remainingMs = settings.statementTimeoutMs;
  let discard = false;
  const execute = async (text: string, values: unknown[] | undefined, timeoutMs: number) => {
    const query: pg.QueryConfig & { query_timeout: number } = { text, values, query_timeout: timeoutMs };
    try {
      return (await client.query<ScopedPacketRow>(query)).rows;
    } catch (error) {
      // A client timeout can leave the server busy. Closing this session
      // abandons the read-only transaction without queuing another query.
      discard = true;
      throw error;
    }
  };
  // The original query's budget is shared across batches. Row reduction runs
  // between calls and still counts against the enclosing rebuild deadline.
  const withinBudget = async (text: string, values?: unknown[]) => {
    if (remainingMs <= 0) throw new Error('REGION_SCOPE_READ_DEADLINE');
    const started = performance.now();
    const rows = await execute(text, values, Math.ceil(remainingMs) + 1_000);
    remainingMs -= performance.now() - started;
    if (remainingMs < -1_000) throw new Error('REGION_SCOPE_READ_DEADLINE');
    return rows;
  };
  const read = async (text: string, values?: unknown[]) => {
    await withinBudget("SELECT set_config('statement_timeout', $1, true)", [`${Math.ceil(remainingMs)}ms`]);
    return withinBudget(text, values);
  };
  try {
    await withinBudget('BEGIN READ ONLY');
    await withinBudget('SET LOCAL cursor_tuple_fraction = 1');
    await read(`DECLARE scoped_region_packets NO SCROLL CURSOR FOR ${SCOPED_PACKETS_SQL}`,
      [settings.windowDays, settings.maxPackets + 1]);
    let count = 0;
    while (true) {
      const rows = await read(`FETCH FORWARD ${PACKET_BATCH_SIZE} FROM scoped_region_packets`);
      count += rows.length;
      if (count > settings.maxPackets) throw new Error('REGION_SCOPE_PACKET_CAPACITY');
      for (const row of rows) visit(row);
      if (rows.length < PACKET_BATCH_SIZE) return count;
    }
  } finally {
    try {
      // The read-only transaction owns and closes the non-HOLD cursor.
      if (!discard) await execute('ROLLBACK', undefined, 1_000);
    } finally {
      client.release(discard);
    }
  }
}
