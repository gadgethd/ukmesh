import type { QueryResultRow } from 'pg';
import { clamp, linkKey } from './geometry.js';
import type { NeighborAffinityMetrics } from './types.js';

type QueryFn = <T extends QueryResultRow = QueryResultRow>(
  text: string,
  params?: unknown[],
) => Promise<{ rows: T[] }>;

type AffinityRow = {
  node_a_id: string;
  node_b_id: string;
  observed_count: string | number;
  observer_count: string | number;
  avg_snr: string | number | null;
  last_seen: string | null;
};

const DEFAULT_WINDOW_DAYS = 14;
const DEFAULT_MIN_OBSERVATIONS = 3;
const DEFAULT_MAX_EDGES = 20_000;
const HALF_LIFE_DAYS = 7;

function envInt(name: string, fallback: number, min: number, max: number): number {
  const parsed = Number(process.env[name] ?? fallback);
  if (!Number.isFinite(parsed)) return fallback;
  return Math.min(max, Math.max(min, Math.trunc(parsed)));
}

function affinityScore(row: AffinityRow, nowMs = Date.now()): NeighborAffinityMetrics {
  const count = Math.max(0, Number(row.observed_count ?? 0));
  const observerCount = Math.max(0, Number(row.observer_count ?? 0));
  const avgSnr = row.avg_snr == null ? null : Number(row.avg_snr);
  const lastSeenMs = row.last_seen ? new Date(row.last_seen).getTime() : NaN;
  const ageDays = Number.isFinite(lastSeenMs)
    ? Math.max(0, (nowMs - lastSeenMs) / 86_400_000)
    : DEFAULT_WINDOW_DAYS;

  const countScore = clamp(Math.log1p(count) / Math.log1p(100), 0, 1);
  const observerScore = clamp(0.7 + observerCount * 0.15, 0.7, 1);
  const recencyScore = clamp(Math.pow(0.5, ageDays / HALF_LIFE_DAYS), 0.08, 1);
  const snrScore = avgSnr == null || !Number.isFinite(avgSnr)
    ? 0.82
    : avgSnr >= 8 ? 1.10
      : avgSnr >= 2 ? 1.00
        : avgSnr >= 0 ? 0.92
          : avgSnr >= -4 ? 0.82
            : 0.68;

  return {
    count,
    observerCount,
    avgSnr: avgSnr == null || !Number.isFinite(avgSnr) ? null : avgSnr,
    lastSeen: row.last_seen,
    score: clamp(countScore * observerScore * recencyScore * snrScore, 0, 1),
  };
}

export async function buildNeighborAffinityMap(
  network: string,
  query: QueryFn,
): Promise<Map<string, NeighborAffinityMetrics>> {
  const windowDays = envInt('PATH_BETA_AFFINITY_WINDOW_DAYS', DEFAULT_WINDOW_DAYS, 1, 90);
  const minObservations = envInt('PATH_BETA_AFFINITY_MIN_OBSERVATIONS', DEFAULT_MIN_OBSERVATIONS, 1, 100);
  const maxEdges = envInt('PATH_BETA_AFFINITY_MAX_EDGES', DEFAULT_MAX_EDGES, 100, 200_000);

  const result = await query<AffinityRow>(
    `WITH node_prefixes AS (
       SELECT
         CASE WHEN $1 = 'all' THEN 'all' ELSE network END AS scope,
         UPPER(LEFT(node_id, 2)) AS prefix,
         node_id
       FROM nodes
       WHERE LENGTH(node_id) >= 2
         AND ($1 = 'all' OR network = $1)
       UNION ALL
       SELECT
         CASE WHEN $1 = 'all' THEN 'all' ELSE network END AS scope,
         UPPER(LEFT(node_id, 4)) AS prefix,
         node_id
       FROM nodes
       WHERE LENGTH(node_id) >= 4
         AND ($1 = 'all' OR network = $1)
       UNION ALL
       SELECT
         CASE WHEN $1 = 'all' THEN 'all' ELSE network END AS scope,
         UPPER(LEFT(node_id, 6)) AS prefix,
         node_id
       FROM nodes
       WHERE LENGTH(node_id) >= 6
         AND ($1 = 'all' OR network = $1)
     ),
     prefix_candidates AS (
       SELECT scope, prefix, COUNT(*)::int AS candidate_count, MIN(node_id) AS node_id
       FROM node_prefixes
       GROUP BY scope, prefix
     ),
     scoped_packets AS (
       SELECT
         CASE WHEN $1 = 'all' THEN 'all' ELSE p.network END AS scope,
         p.packet_type,
         p.src_node_id,
         p.rx_node_id,
         p.path_hashes,
         p.snr,
         p.time
       FROM packets p
       WHERE p.time > NOW() - ($2::int * INTERVAL '1 day')
         AND p.rx_node_id IS NOT NULL
         AND ($1 = 'all' OR p.network = $1)
     ),
     direct_edges AS (
       SELECT
         LEAST(src_node_id, rx_node_id) AS node_a_id,
         GREATEST(src_node_id, rx_node_id) AS node_b_id,
         rx_node_id AS observer_id,
         snr,
         time
       FROM scoped_packets
       WHERE packet_type = 4
         AND src_node_id IS NOT NULL
         AND rx_node_id IS NOT NULL
         AND src_node_id <> rx_node_id
         AND (path_hashes IS NULL OR cardinality(path_hashes) = 0)
     ),
     prefix_edges AS (
       SELECT
         scope,
         src_node_id AS known_node_id,
         UPPER(path_hashes[1]) AS prefix,
         rx_node_id AS observer_id,
         snr,
         time
       FROM scoped_packets
       WHERE packet_type = 4
         AND src_node_id IS NOT NULL
         AND path_hashes IS NOT NULL
         AND cardinality(path_hashes) > 0
       UNION ALL
       SELECT
         scope,
         rx_node_id AS known_node_id,
         UPPER(path_hashes[array_length(path_hashes, 1)]) AS prefix,
         rx_node_id AS observer_id,
         snr,
         time
       FROM scoped_packets
       WHERE rx_node_id IS NOT NULL
         AND path_hashes IS NOT NULL
         AND cardinality(path_hashes) > 0
     ),
     resolved_prefix_edges AS (
       SELECT
         LEAST(pe.known_node_id, pc.node_id) AS node_a_id,
         GREATEST(pe.known_node_id, pc.node_id) AS node_b_id,
         pe.observer_id,
         pe.snr,
         pe.time
       FROM prefix_edges pe
       JOIN prefix_candidates pc
         ON pc.scope = pe.scope
        AND pc.prefix = pe.prefix
        AND pc.candidate_count = 1
       WHERE pe.known_node_id IS NOT NULL
         AND pc.node_id IS NOT NULL
         AND pe.known_node_id <> pc.node_id
         AND LENGTH(pe.prefix) IN (2, 4, 6)
     ),
     all_edges AS (
       SELECT * FROM direct_edges
       UNION ALL
       SELECT * FROM resolved_prefix_edges
     )
     SELECT
       node_a_id,
       node_b_id,
       COUNT(*) AS observed_count,
       COUNT(DISTINCT observer_id) AS observer_count,
       AVG(snr) FILTER (WHERE snr IS NOT NULL) AS avg_snr,
       MAX(time)::text AS last_seen
     FROM all_edges
     WHERE node_a_id IS NOT NULL
       AND node_b_id IS NOT NULL
       AND node_a_id <> node_b_id
     GROUP BY node_a_id, node_b_id
     HAVING COUNT(*) >= $3::int
     ORDER BY observed_count DESC, last_seen DESC
     LIMIT $4::int`,
    [network, windowDays, minObservations, maxEdges],
  );

  const map = new Map<string, NeighborAffinityMetrics>();
  const nowMs = Date.now();
  for (const row of result.rows) {
    map.set(linkKey(row.node_a_id, row.node_b_id), affinityScore(row, nowMs));
  }
  return map;
}

export function buildNeighborAffinityAdjacency(
  affinity: Map<string, NeighborAffinityMetrics>,
): Map<string, Set<string>> {
  const adjacency = new Map<string, Set<string>>();
  for (const key of affinity.keys()) {
    const [aId, bId] = key.split(':');
    if (!aId || !bId || aId === bId) continue;
    if (!adjacency.has(aId)) adjacency.set(aId, new Set());
    if (!adjacency.has(bId)) adjacency.set(bId, new Set());
    adjacency.get(aId)!.add(bId);
    adjacency.get(bId)!.add(aId);
  }
  return adjacency;
}

function mutualNeighborPreference(
  adjacency: Map<string, Set<string>>,
  fromId: string,
  toId: string,
): number {
  const fromNeighbors = adjacency.get(fromId);
  const toNeighbors = adjacency.get(toId);
  if (!fromNeighbors || !toNeighbors || fromNeighbors.size < 1 || toNeighbors.size < 1) return 0;

  let intersection = 0;
  for (const id of fromNeighbors) {
    if (id !== fromId && id !== toId && toNeighbors.has(id)) intersection += 1;
  }
  if (intersection < 1) return 0;

  const union = new Set<string>([...fromNeighbors, ...toNeighbors]);
  union.delete(fromId);
  union.delete(toId);
  const jaccard = union.size > 0 ? intersection / union.size : 0;
  const supportBonus = intersection >= 3 ? 0.035 : intersection >= 2 ? 0.02 : 0;
  return clamp(jaccard * 0.16 + supportBonus, 0, 0.18);
}

export function neighborAffinityPreference(
  affinity: Map<string, NeighborAffinityMetrics>,
  adjacency: Map<string, Set<string>>,
  fromId: string,
  toId: string,
): number {
  const metrics = affinity.get(linkKey(fromId, toId));
  const direct = (() => {
    if (!metrics) return 0;
    const countBonus = metrics.count >= 30 ? 0.04 : metrics.count >= 10 ? 0.025 : 0;
    const observerBonus = metrics.observerCount >= 3 ? 0.03 : metrics.observerCount >= 2 ? 0.015 : 0;
    return clamp(metrics.score * 0.20 + countBonus + observerBonus, 0, 0.28);
  })();
  const mutual = mutualNeighborPreference(adjacency, fromId, toId);
  return clamp(direct + mutual, 0, 0.32);
}
