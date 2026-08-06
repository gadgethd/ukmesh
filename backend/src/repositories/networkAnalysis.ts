import type { QueryResultRow } from 'pg';
import type { NetworkFilters } from '../api/utils/networkFilters.js';

type QueryFn = <T extends QueryResultRow = QueryResultRow>(
  text: string,
  params?: unknown[],
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

export async function rfValidationRows(
  query: QueryFn,
  filters: NetworkFilters,
  limit: number,
) {
  const limitParam = `$${filters.params.length + 1}`;
  return query<{
    node_a_id: string;
    node_b_id: string;
    name_a: string | null;
    name_b: string | null;
    observed_count: string;
    multibyte_observed_count: string;
    itm_path_loss_db: number | null;
    itm_viable: boolean | null;
    force_viable: boolean;
    last_observed: string;
    classification: string;
  }>(
    `SELECT
       nl.node_a_id, nl.node_b_id, a.name AS name_a, b.name AS name_b,
       nl.observed_count, nl.multibyte_observed_count, nl.itm_path_loss_db,
       nl.itm_viable, nl.force_viable, nl.last_observed::text,
       CASE
         WHEN nl.force_viable THEN 'operator_override'
         WHEN nl.itm_viable = false AND nl.multibyte_observed_count > 0 THEN 'observed_unexpected'
         WHEN nl.itm_viable = false AND nl.observed_count >= 10 THEN 'observed_unexpected'
         WHEN nl.itm_viable = true AND nl.observed_count <= 2 AND nl.last_observed < NOW() - INTERVAL '7 days' THEN 'weak_model_evidence'
         ELSE 'match'
       END AS classification
     FROM node_identity_links nl
     JOIN node_identity_nodes a ON a.node_id = nl.node_a_id
     JOIN node_identity_nodes b ON b.node_id = nl.node_b_id
     WHERE nl.last_observed > NOW() - INTERVAL '30 days'
       AND (a.name IS NULL OR a.name NOT LIKE '%🚫%')
       AND (b.name IS NULL OR b.name NOT LIKE '%🚫%')
       AND (a.role IS NULL OR a.role = 2)
       AND (b.role IS NULL OR b.role = 2)
       ${filters.nodesAlias('a')}
       ${filters.nodesAlias('b')}
     ORDER BY
       CASE
         WHEN nl.itm_viable = false AND nl.multibyte_observed_count > 0 THEN 0
         WHEN nl.force_viable THEN 1
         WHEN nl.itm_viable = false AND nl.observed_count >= 10 THEN 2
         WHEN nl.itm_viable = true AND nl.observed_count <= 2 THEN 3
         ELSE 4
       END,
       nl.multibyte_observed_count DESC,
       nl.observed_count DESC
     LIMIT ${limitParam}`,
    [...filters.params, limit],
  );
}
