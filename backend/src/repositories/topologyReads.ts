import { pool } from '../db/runtime.js';
import { query } from '../db/runtime.js';
import { UKMESH_NETWORKS } from '../networks.js';
import { regionMemberIdsSql } from '../regions/filter.js';
import { regionNodeSourceSql } from '../regions/nodeSource.js';
import { buildScopePlaceholders } from '../db/readScope.js';
import { buildNodeScopeClause } from '../db/readScope.js';

const MAX_MULTIBYTE_PATH_SEGMENT_KM = 150;

export type MultibytePathSegmentRow = {
  positions: [[number, number], [number, number]];
  count: number;
};

export async function getMultibytePathSegments(network?: string, observer?: string, region?: string): Promise<{
  maxCount: number;
  segments: MultibytePathSegmentRow[];
}> {
  const scope = buildScopePlaceholders(1, network, observer, region);
  const params: unknown[] = [...scope.params];

  // Two simple statements joined in JS. A single JOIN blob makes the planner
  // guess rows=1 on the scoped-node CTE (the sightings EXISTS is not
  // estimable) and pick nested loops that re-materialise the identity view
  // per row: 100s+. Splitting keeps both statements simple enough that the
  // planner cannot mis-estimate them (measured: ~325ms + ~92ms).
  const [nodesRes, linksRes] = await Promise.all([
    pool.query<{
      node_id: string;
      lat: number | null;
      lon: number | null;
      name: string | null;
    }>(
      `SELECT node_id, lat, lon, name
       FROM ${scope.regionParam ? regionNodeSourceSql(scope.regionParam) : 'core.node_identity_nodes'} n
       WHERE 1=1${buildNodeScopeClause(scope, 'n')}`,
      params,
    ),
    pool.query<{
      node_a_id: string;
      node_b_id: string;
      multibyte_observed_count: number;
    }>(
      `SELECT node_a_id, node_b_id, multibyte_observed_count
       FROM core.node_identity_links
       WHERE multibyte_observed_count > 0
         AND itm_viable = true
         ${scope.regionParam ? `AND node_a_id IN (${regionMemberIdsSql('$1')})
           AND node_b_id IN (${regionMemberIdsSql('$1')})` : ''}`,
      scope.regionParam ? [region] : undefined,
    ),
  ]);

  const nodesById = new Map(nodesRes.rows.map((row) => [row.node_id, row]));
  const segments: Array<{
    positions: [[number, number], [number, number]];
    count: number;
  }> = [];
  for (const link of linksRes.rows) {
    const a = nodesById.get(link.node_a_id);
    const b = nodesById.get(link.node_b_id);
    if (!a || !b) continue;
    if (a.lat == null || a.lon == null || b.lat == null || b.lon == null) continue;
    if (a.lat < -90 || a.lat > 90 || b.lat < -90 || b.lat > 90) continue;
    if (a.lon < -180 || a.lon > 180 || b.lon < -180 || b.lon > 180) continue;
    if (Math.abs(a.lat) < 5 && Math.abs(a.lon) < 5) continue;
    if (Math.abs(b.lat) < 5 && Math.abs(b.lon) < 5) continue;
    if ((a.name ?? '').includes('🚫') || (b.name ?? '').includes('🚫')) continue;
    const distKm = Math.sqrt(
      Math.pow((a.lat - b.lat) * 111, 2)
        + Math.pow(
          (a.lon - b.lon) * 111 * Math.cos(((a.lat + b.lat) / 2) * (Math.PI / 180)),
          2,
        ),
    );
    if (distKm > MAX_MULTIBYTE_PATH_SEGMENT_KM) continue;
    segments.push({
      positions: [
        [a.lat, a.lon],
        [b.lat, b.lon],
      ],
      count: link.multibyte_observed_count,
    });
  }
  segments.sort((x, y) => y.count - x.count);

  const maxCount = segments.reduce((max, segment) => Math.max(max, segment.count), 0);
  return { maxCount, segments };
}

export type ViableLinkRow = {
  node_a_id: string;
  node_b_id: string;
  observed_count: number;
  multibyte_observed_count: number;
  neighbor_report_count: number;
  neighbor_best_snr_db: number | null;
  itm_viable: boolean | null;
  itm_path_loss_db: number | null;
  count_a_to_b: number;
  count_b_to_a: number;
  last_observed: string;
};

/** Returns viable links with metrics so UI can render precomputed styles immediately. */
export async function getViableLinks(
  network?: string,
  observer?: string,
  signal?: AbortSignal,
  region?: string,
): Promise<ViableLinkRow[]> {
  if (region) {
    const nodes = await getNodes(network, observer, 'full', signal, region);
    const ids = nodes.filter((node) => !node.name?.includes('🚫')).map((node) => node.node_id);
    const result = await query<ViableLinkRow>(
      `SELECT nl.node_a_id, nl.node_b_id, nl.observed_count, nl.multibyte_observed_count,
         COALESCE(nr.neighbor_report_count, 0) AS neighbor_report_count,
         nr.neighbor_best_snr_db, nl.itm_viable, nl.itm_path_loss_db,
         nl.count_a_to_b, nl.count_b_to_a, nl.last_observed::text AS last_observed
       FROM core.node_identity_links nl
       LEFT JOIN LATERAL (
         SELECT SUM(sample_count)::int AS neighbor_report_count, MAX(best_snr_db) AS neighbor_best_snr_db
         FROM core.node_identity_link_radio_reports r
         WHERE r.node_a_id = nl.node_a_id AND r.node_b_id = nl.node_b_id
       ) nr ON TRUE
       WHERE nl.node_a_id = ANY($1::text[]) AND nl.node_b_id = ANY($1::text[])
         AND (nl.itm_viable = true OR nl.force_viable = true)`,
      [ids], signal,
    );
    return result.rows;
  }
  if (network && !observer) {
    const scopedNetworks = network === 'ukmesh' ? UKMESH_NETWORKS : [network];
    // Keep the identity scope and link scan as separate statements. The
    // identity view's representative-rank predicate is estimated at one row;
    // combining it with both link endpoints makes PostgreSQL form the scoped
    // node Cartesian product and perform ~174M composite-index probes for
    // ukmesh. Each simple statement is fast even with that bad estimate, and
    // the in-memory intersection preserves the exact endpoint scope.
    const [nodesRes, linksRes] = await Promise.all([
      query<{ node_id: string }>(
        `SELECT DISTINCT node_id
         FROM core.node_identity_nodes
         WHERE network = ANY($1::text[])
           AND (name IS NULL OR name NOT LIKE '%🚫%')`,
        [scopedNetworks],
        signal,
      ),
      query<ViableLinkRow>(
        `SELECT
           nl.node_a_id,
           nl.node_b_id,
           nl.observed_count,
           nl.multibyte_observed_count,
           COALESCE(nr.neighbor_report_count, 0) AS neighbor_report_count,
           nr.neighbor_best_snr_db,
           nl.itm_viable,
           nl.itm_path_loss_db,
           nl.count_a_to_b,
           nl.count_b_to_a,
           nl.last_observed::text AS last_observed
         FROM core.node_identity_links nl
         LEFT JOIN (
           SELECT node_a_id, node_b_id,
             SUM(sample_count)::int AS neighbor_report_count,
             MAX(best_snr_db) AS neighbor_best_snr_db
           FROM core.node_identity_link_radio_reports
           GROUP BY node_a_id, node_b_id
         ) nr ON nr.node_a_id = nl.node_a_id AND nr.node_b_id = nl.node_b_id
         WHERE (nl.itm_viable = true OR nl.force_viable = true)`,
        undefined,
        signal,
      ),
    ]);
    const scopedNodeIds = new Set(nodesRes.rows.map((row) => row.node_id));
    return linksRes.rows.filter((link) => (
      scopedNodeIds.has(link.node_a_id) && scopedNodeIds.has(link.node_b_id)
    ));
  }

  const scope = buildScopePlaceholders(1, network, observer);
  const params: unknown[] = [...scope.params];

  const res = await query<ViableLinkRow>(
    `SELECT
       nl.node_a_id,
       nl.node_b_id,
       nl.observed_count,
       nl.multibyte_observed_count,
       COALESCE(nr.neighbor_report_count, 0) AS neighbor_report_count,
       nr.neighbor_best_snr_db,
       nl.itm_viable,
       nl.itm_path_loss_db,
       nl.count_a_to_b,
       nl.count_b_to_a,
       nl.last_observed::text AS last_observed
     FROM core.node_identity_links nl
     LEFT JOIN (
       SELECT node_a_id, node_b_id,
         SUM(sample_count)::int AS neighbor_report_count,
         MAX(best_snr_db) AS neighbor_best_snr_db
       FROM core.node_identity_link_radio_reports
       GROUP BY node_a_id, node_b_id
     ) nr ON nr.node_a_id = nl.node_a_id AND nr.node_b_id = nl.node_b_id
     JOIN core.node_identity_nodes a ON a.node_id = nl.node_a_id
     JOIN core.node_identity_nodes b ON b.node_id = nl.node_b_id
     WHERE (nl.itm_viable = true OR nl.force_viable = true)
       AND (a.name IS NULL OR a.name NOT LIKE '%🚫%')
       AND (b.name IS NULL OR b.name NOT LIKE '%🚫%')
       ${buildNodeScopeClause(scope, 'a')}
       ${buildNodeScopeClause(scope, 'b')}`,
    params,
    signal,
  );
  return res.rows;
}
import { getNodes } from './nodeReads.js';
