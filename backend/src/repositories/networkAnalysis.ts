import pg, { type QueryResultRow } from 'pg';
import { databaseConfig } from '../platform/config/database.js';
import type { NetworkFilters } from '../api/utils/networkFilters.js';

type QueryFn = <T extends QueryResultRow = QueryResultRow>(
  text: string,
  params?: unknown[],
  signal?: AbortSignal,
) => Promise<{ rows: T[] }>;

export type TopologyRow = {
  node_a_id: string;
  node_b_id: string;
  name_a: string | null;
  name_b: string | null;
  lat_a: number | null;
  lon_a: number | null;
  lat_b: number | null;
  lon_b: number | null;
  iata_a?: string | null;
  iata_b?: string | null;
  observed_count: string | number;
  multibyte_observed_count: string | number;
  last_observed: string;
  itm_path_loss_db: number | null;
};

export type StandaloneNodeRow = {
  node_id: string;
  name: string | null;
  lat: number | null;
  lon: number | null;
  iata?: string | null;
};

type CombinedTopologyRow = {
  row_kind: number;
  row_order: string | number;
  node_a_id: string | null;
  node_b_id: string | null;
  name_a: string | null;
  name_b: string | null;
  lat_a: number | null;
  lon_a: number | null;
  lat_b: number | null;
  lon_b: number | null;
  iata_a: string | null;
  iata_b: string | null;
  observed_count: string | number | null;
  multibyte_observed_count: string | number | null;
  last_observed: string | null;
  itm_path_loss_db: number | null;
  standalone_node_id: string | null;
  standalone_name: string | null;
  standalone_lat: number | null;
  standalone_lon: number | null;
  standalone_iata: string | null;
};

// The canonical link view deliberately materializes a complete identity
// projection. That is useful for broad analytics, but topology only needs
// recent viable rows. Filtering the base link table first keeps this public
// endpoint bounded as historical link evidence grows.
const RECENT_CANONICAL_LINK_CTES = `
WITH recent_links AS MATERIALIZED (
  SELECT
    COALESCE(la.canonical_node_id, upper(btrim(nl.node_a_id))) AS raw_a,
    COALESCE(lb.canonical_node_id, upper(btrim(nl.node_b_id))) AS raw_b,
    nl.observed_count,
    nl.last_observed,
    nl.itm_path_loss_db,
    nl.multibyte_observed_count
  FROM node_links nl
  LEFT JOIN node_identity_aliases la
    ON la.source_node_id = upper(btrim(nl.node_a_id))
  LEFT JOIN node_identity_aliases lb
    ON lb.source_node_id = upper(btrim(nl.node_b_id))
  WHERE (nl.itm_viable = true OR nl.force_viable = true)
    AND nl.last_observed > NOW() - INTERVAL '30 days'
), canonical_links AS (
  SELECT
    LEAST(raw_a, raw_b) AS node_a_id,
    GREATEST(raw_a, raw_b) AS node_b_id,
    observed_count,
    last_observed,
    itm_path_loss_db,
    multibyte_observed_count
  FROM recent_links
  WHERE raw_a <> raw_b
)`;

/** Loads the complete topology input in one snapshot and one database query. */
export async function combinedTopologyRows(
  query: QueryFn,
  filters: NetworkFilters,
  limit: number,
): Promise<{ links: TopologyRow[]; standalone: StandaloneNodeRow[] }> {
  const limitParam = `$${filters.params.length + 1}`;
  const result = await query<CombinedTopologyRow>(
    `${RECENT_CANONICAL_LINK_CTES},
     aggregated_links AS (
       SELECT
         node_a_id,
         node_b_id,
         SUM(observed_count)::integer AS observed_count,
         SUM(multibyte_observed_count)::integer AS multibyte_observed_count,
         MAX(last_observed)::text AS last_observed,
         MIN(itm_path_loss_db) AS itm_path_loss_db
       FROM canonical_links
       GROUP BY node_a_id, node_b_id
     ),
     eligible_nodes AS MATERIALIZED (
       SELECT n.node_id, n.name, n.lat, n.lon, n.iata
       FROM node_identity_nodes n
       WHERE (n.role IS NULL OR n.role = 2)
         AND (n.name IS NULL OR n.name NOT LIKE '%🚫%')
         ${filters.nodesAlias('n')}
     ),
     selected_links AS MATERIALIZED (
       SELECT
         nl.node_a_id,
         nl.node_b_id,
         a.name AS name_a,
         b.name AS name_b,
         a.lat AS lat_a,
         a.lon AS lon_a,
         a.iata AS iata_a,
         b.lat AS lat_b,
         b.lon AS lon_b,
         b.iata AS iata_b,
         nl.observed_count,
         nl.multibyte_observed_count,
         nl.last_observed,
         nl.itm_path_loss_db,
         row_number() OVER (
           ORDER BY nl.multibyte_observed_count DESC, nl.observed_count DESC, nl.last_observed DESC
         ) AS row_order
       FROM aggregated_links nl
       JOIN eligible_nodes a ON a.node_id = nl.node_a_id
       JOIN eligible_nodes b ON b.node_id = nl.node_b_id
       ORDER BY nl.multibyte_observed_count DESC, nl.observed_count DESC, nl.last_observed DESC
       LIMIT ${limitParam}
     ),
     recent_node_ids AS (
       SELECT node_a_id AS node_id FROM canonical_links
       UNION
       SELECT node_b_id AS node_id FROM canonical_links
     ),
     standalone_nodes AS MATERIALIZED (
       SELECT
         n.node_id,
         n.name,
         n.lat,
         n.lon,
         n.iata,
         row_number() OVER (ORDER BY n.last_seen DESC) AS row_order
       FROM node_identity_nodes n
       WHERE n.last_seen > NOW() - INTERVAL '30 days'
         AND (n.role IS NULL OR n.role = 2)
         AND (n.name IS NULL OR n.name NOT LIKE '%🚫%')
         ${filters.nodesAlias('n')}
         AND NOT EXISTS (
           SELECT 1
           FROM recent_node_ids r
           WHERE r.node_id = n.node_id
         )
       ORDER BY n.last_seen DESC
       LIMIT 100
     )
     SELECT
       0 AS row_kind,
       link.row_order,
       link.node_a_id,
       link.node_b_id,
       link.name_a,
       link.name_b,
       link.lat_a,
       link.lon_a,
       link.lat_b,
       link.lon_b,
       link.iata_a,
       link.iata_b,
       link.observed_count,
       link.multibyte_observed_count,
       link.last_observed,
       link.itm_path_loss_db,
       NULL::text AS standalone_node_id,
       NULL::text AS standalone_name,
       NULL::double precision AS standalone_lat,
       NULL::double precision AS standalone_lon,
       NULL::text AS standalone_iata
     FROM selected_links link
     UNION ALL
     SELECT
       1 AS row_kind,
       node.row_order,
       NULL::text,
       NULL::text,
       NULL::text,
       NULL::text,
       NULL::double precision,
       NULL::double precision,
       NULL::double precision,
       NULL::double precision,
       NULL::text,
       NULL::text,
       NULL::integer,
       NULL::integer,
       NULL::text,
       NULL::double precision,
       node.node_id,
       node.name,
       node.lat,
       node.lon,
       node.iata
     FROM standalone_nodes node
     ORDER BY row_kind, row_order`,
    [...filters.params, limit],
  );

  const links: TopologyRow[] = [];
  const standalone: StandaloneNodeRow[] = [];
  for (const row of result.rows) {
    if (row.row_kind === 0) {
      if (!row.node_a_id || !row.node_b_id || row.last_observed == null) {
        throw new Error('INVALID_COMBINED_TOPOLOGY_LINK');
      }
      links.push({
        node_a_id: row.node_a_id,
        node_b_id: row.node_b_id,
        name_a: row.name_a,
        name_b: row.name_b,
        lat_a: row.lat_a,
        lon_a: row.lon_a,
        lat_b: row.lat_b,
        lon_b: row.lon_b,
        iata_a: row.iata_a,
        iata_b: row.iata_b,
        observed_count: row.observed_count ?? 0,
        multibyte_observed_count: row.multibyte_observed_count ?? 0,
        last_observed: row.last_observed,
        itm_path_loss_db: row.itm_path_loss_db,
      });
    } else if (row.row_kind === 1) {
      if (!row.standalone_node_id) throw new Error('INVALID_COMBINED_TOPOLOGY_NODE');
      standalone.push({
        node_id: row.standalone_node_id,
        name: row.standalone_name,
        lat: row.standalone_lat,
        lon: row.standalone_lon,
        iata: row.standalone_iata,
      });
    } else {
      throw new Error('INVALID_COMBINED_TOPOLOGY_ROW_KIND');
    }
  }
  return { links, standalone };
}

export async function topologyRows(
  query: QueryFn,
  filters: NetworkFilters,
  limit: number,
) {
  const limitParam = `$${filters.params.length + 1}`;
  return query<TopologyRow>(
    `${RECENT_CANONICAL_LINK_CTES},
     aggregated_links AS (
       SELECT
         node_a_id,
         node_b_id,
         SUM(observed_count)::integer AS observed_count,
         SUM(multibyte_observed_count)::integer AS multibyte_observed_count,
         MAX(last_observed)::text AS last_observed,
         MIN(itm_path_loss_db) AS itm_path_loss_db
       FROM canonical_links
       GROUP BY node_a_id, node_b_id
     ),
     eligible_nodes AS MATERIALIZED (
       SELECT n.node_id, n.name, n.lat, n.lon, n.iata
       FROM node_identity_nodes n
       WHERE (n.role IS NULL OR n.role = 2)
         AND (n.name IS NULL OR n.name NOT LIKE '%🚫%')
         ${filters.nodesAlias('n')}
     )
     SELECT
       nl.node_a_id,
       nl.node_b_id,
       a.name AS name_a,
       b.name AS name_b,
       a.lat AS lat_a,
       a.lon AS lon_a,
       a.iata AS iata_a,
       b.lat AS lat_b,
       b.lon AS lon_b,
       b.iata AS iata_b,
       nl.observed_count,
       nl.multibyte_observed_count,
       nl.last_observed,
       nl.itm_path_loss_db
     FROM aggregated_links nl
     JOIN eligible_nodes a ON a.node_id = nl.node_a_id
     JOIN eligible_nodes b ON b.node_id = nl.node_b_id
     ORDER BY nl.multibyte_observed_count DESC, nl.observed_count DESC, nl.last_observed DESC
     LIMIT ${limitParam}`,
    [...filters.params, limit],
  );
}

export async function standaloneTopologyRows(query: QueryFn, filters: NetworkFilters) {
  return query<StandaloneNodeRow>(
    `${RECENT_CANONICAL_LINK_CTES},
     recent_node_ids AS (
       SELECT node_a_id AS node_id FROM canonical_links
       UNION
       SELECT node_b_id AS node_id FROM canonical_links
     )
     SELECT n.node_id, n.name, n.lat, n.lon, n.iata
     FROM node_identity_nodes n
     WHERE n.last_seen > NOW() - INTERVAL '30 days'
       AND (n.role IS NULL OR n.role = 2)
       AND (n.name IS NULL OR n.name NOT LIKE '%🚫%')
       ${filters.nodesAlias('n')}
       AND NOT EXISTS (
         SELECT 1
         FROM recent_node_ids r
         WHERE r.node_id = n.node_id
       )
     ORDER BY n.last_seen DESC
     LIMIT 100`,
    filters.params,
  );
}

export const RF_VALIDATION_DB_TIMEOUT_MS = 3_500;

// RF validation must not inherit the shared pool's potentially unlimited
// analytics timeout. This small, lazy pool is read-only and bounds execution
// on the server even if the requesting browser or backend disconnects.
export const rfValidationPool = new pg.Pool({
  connectionString: process.env['DATABASE_URL'],
  application_name: `${databaseConfig.applicationName}-rf-validation`,
  options: `-c default_transaction_read_only=on${databaseConfig.schema ? ` -c search_path=${databaseConfig.schema},public` : ''}`,
  max: 2,
  connectionTimeoutMillis: 500,
  idleTimeoutMillis: 10_000,
  allowExitOnIdle: true,
  statement_timeout: RF_VALIDATION_DB_TIMEOUT_MS,
  query_timeout: RF_VALIDATION_DB_TIMEOUT_MS + 250,
});
rfValidationPool.on('error', (error) => {
  console.error('[rf-validation] database connection error', error.message);
});

export async function queryRfValidation<T extends QueryResultRow = QueryResultRow>(
  text: string,
  params?: unknown[],
  signal?: AbortSignal,
): Promise<{ rows: T[] }> {
  signal?.throwIfAborted();
  const client = await rfValidationPool.connect();
  let released = false;
  let onAbort: (() => void) | undefined;
  try {
    signal?.throwIfAborted();
    const aborted = new Promise<never>((_resolve, reject) => {
      if (!signal) return;
      onAbort = () => {
        // Destroy rather than return a connection with an in-flight query.
        // The server timeout remains the independent cancellation backstop.
        released = true;
        client.release(true);
        reject(signal.reason);
      };
      signal.addEventListener('abort', onAbort, { once: true });
    });
    return await Promise.race([client.query<T>(text, params), aborted]);
  } finally {
    if (onAbort) signal?.removeEventListener('abort', onAbort);
    if (!released) client.release(signal?.aborted === true);
  }
}

type RfValidationRow = {
  node_a_id: string;
  node_b_id: string;
  name_a: string | null;
  name_b: string | null;
  observed_count: string | number;
  multibyte_observed_count: string | number;
  itm_path_loss_db: number | null;
  itm_viable: boolean | null;
  force_viable: boolean;
  last_observed: string;
  classification: string;
};

export async function rfValidationRows(
  query: QueryFn,
  filters: NetworkFilters,
  limit: number,
  signal?: AbortSignal,
): Promise<{ rows: RfValidationRow[] }> {
  // The identity-node view estimates ~1 eligible row where production has
  // thousands. Joining it twice to identity links caused millions of nested
  // loop comparisons. Resolve scope once, then pass the actual ids into the
  // link query so its hash joins have accurate input cardinality.
  const nodes = await query<{ node_id: string; name: string | null }>(
    `SELECT n.node_id, n.name
     FROM node_identity_nodes n
     WHERE (n.role IS NULL OR n.role = 2)
       AND (n.name IS NULL OR n.name NOT LIKE '%🚫%')
       ${filters.nodesAlias('n')}`,
    filters.params,
    signal,
  );
  signal?.throwIfAborted();
  const names = new Map(nodes.rows.map((node) => [node.node_id, node.name]));
  if (names.size === 0) return { rows: [] };
  const result = await query<Omit<RfValidationRow, 'name_a' | 'name_b'>>(
    `WITH eligible_ids AS MATERIALIZED (
       SELECT unnest($1::text[]) AS node_id
     ), recent_links AS MATERIALIZED (
       SELECT
         COALESCE(la.canonical_node_id, UPPER(BTRIM(nl.node_a_id))) AS raw_a,
         COALESCE(lb.canonical_node_id, UPPER(BTRIM(nl.node_b_id))) AS raw_b,
         nl.observed_count, nl.multibyte_observed_count, nl.itm_path_loss_db,
         nl.itm_viable, nl.force_viable, nl.last_observed
       FROM node_links nl
       LEFT JOIN node_identity_aliases la
         ON la.source_node_id = UPPER(BTRIM(nl.node_a_id))
       LEFT JOIN node_identity_aliases lb
         ON lb.source_node_id = UPPER(BTRIM(nl.node_b_id))
       WHERE nl.last_observed > NOW() - INTERVAL '30 days'
     ), aggregated_links AS (
       SELECT LEAST(raw_a, raw_b) AS node_a_id,
         GREATEST(raw_a, raw_b) AS node_b_id,
         SUM(observed_count)::integer AS observed_count,
         SUM(multibyte_observed_count)::integer AS multibyte_observed_count,
         MIN(itm_path_loss_db) AS itm_path_loss_db,
         BOOL_OR(itm_viable) AS itm_viable,
         BOOL_OR(force_viable) AS force_viable,
         MAX(last_observed) AS last_observed
       FROM recent_links
       WHERE raw_a <> raw_b
         AND raw_a IN (SELECT node_id FROM eligible_ids)
         AND raw_b IN (SELECT node_id FROM eligible_ids)
       GROUP BY LEAST(raw_a, raw_b), GREATEST(raw_a, raw_b)
     )
     SELECT node_a_id, node_b_id,
       observed_count, multibyte_observed_count, itm_path_loss_db,
       itm_viable, force_viable, last_observed::text,
       CASE
         WHEN force_viable THEN 'operator_override'
         WHEN itm_viable = false AND multibyte_observed_count > 0 THEN 'observed_unexpected'
         WHEN itm_viable = false AND observed_count >= 10 THEN 'observed_unexpected'
         WHEN itm_viable = true AND observed_count <= 2 AND last_observed < NOW() - INTERVAL '7 days' THEN 'weak_model_evidence'
         ELSE 'match'
       END AS classification
     FROM aggregated_links
     ORDER BY
       CASE
         WHEN itm_viable = false AND multibyte_observed_count > 0 THEN 0
         WHEN force_viable THEN 1
         WHEN itm_viable = false AND observed_count >= 10 THEN 2
         WHEN itm_viable = true AND observed_count <= 2 THEN 3
         ELSE 4
       END,
       multibyte_observed_count DESC, observed_count DESC, node_a_id, node_b_id
     LIMIT $2`,
    [[...names.keys()], Math.max(1, Math.min(250, Math.trunc(limit) || 100))],
    signal,
  );
  signal?.throwIfAborted();
  // Enrich only the bounded result, and fail closed if an endpoint is absent
  // from the public scope (including when a test/custom query supplies rows).
  return { rows: result.rows
    .filter((row) => names.has(row.node_a_id) && names.has(row.node_b_id))
    .slice(0, limit)
    .map((row) => ({ ...row, name_a: names.get(row.node_a_id)!, name_b: names.get(row.node_b_id)! })) };
}
