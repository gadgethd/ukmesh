import type pg from 'pg';
import { mapSuppressedNodes } from '../metrics.js';
import { nodeEffectiveLastSeenSql } from './presence.js';
import { mapVisiblePredicate } from './publicMap.js';

const DAY_MS = 86_400_000;
const EARTH_RADIUS_M = 6_371_000;

export type MapSuppressionConfig = {
  radiusM: number;
  staleAfterDays: number;
  activeWithinDays: number;
};

export const DEFAULT_MAP_SUPPRESSION_CONFIG: Readonly<MapSuppressionConfig> = {
  radiusM: 30,
  staleAfterDays: 14,
  activeWithinDays: 7,
};

export type MapSuppressionNode = {
  node_id: string;
  network: string | null;
  lat: number | null;
  lon: number | null;
  // The caller supplies effective last seen, including RX/status/path evidence.
  last_seen: Date | string | null;
};

export type NodeMapSuppression = {
  node_id: string;
  suppressed_by_node_id: string;
  reason: 'stale-colocated';
  distance_m: number;
  stale_age_days: number;
  freshest_age_days: number;
};

function validateConfig(config: Readonly<MapSuppressionConfig>): void {
  if (Object.values(config).some((value) => !Number.isFinite(value) || value < 0)
      || config.staleAfterDays <= config.activeWithinDays) {
    throw new Error('Invalid map suppression thresholds: require staleAfterDays > activeWithinDays >= 0 and radiusM >= 0');
  }
}

export function mapSuppressionConfig(
  env: NodeJS.ProcessEnv = process.env,
): MapSuppressionConfig {
  const config = {
    radiusM: Number(env['MAP_STALE_DUPLICATE_RADIUS_M'] ?? DEFAULT_MAP_SUPPRESSION_CONFIG.radiusM),
    staleAfterDays: Number(env['MAP_STALE_DUPLICATE_STALE_AFTER_DAYS'] ?? DEFAULT_MAP_SUPPRESSION_CONFIG.staleAfterDays),
    activeWithinDays: Number(env['MAP_STALE_DUPLICATE_ACTIVE_WITHIN_DAYS'] ?? DEFAULT_MAP_SUPPRESSION_CONFIG.activeWithinDays),
  };
  validateConfig(config);
  return config;
}

function distanceMeters(a: MapSuppressionNode, b: MapSuppressionNode): number {
  const radians = Math.PI / 180;
  const dLat = (b.lat! - a.lat!) * radians;
  const dLon = (b.lon! - a.lon!) * radians;
  const haversine = Math.sin(dLat / 2) ** 2
    + Math.cos(a.lat! * radians) * Math.cos(b.lat! * radians) * Math.sin(dLon / 2) ** 2;
  return 2 * EARTH_RADIUS_M * Math.asin(Math.sqrt(Math.min(1, haversine)));
}

/** Each stale node is compared directly with its freshest in-radius neighbour.
 * Names, keys and roles do not establish identity here; callers supply map-visible nodes. */
export function computeNodeMapSuppressions(
  nodes: readonly MapSuppressionNode[],
  referenceMs: number,
  config: Readonly<MapSuppressionConfig> = DEFAULT_MAP_SUPPRESSION_CONFIG,
): NodeMapSuppression[] {
  validateConfig(config);
  if (!Number.isFinite(referenceMs)) throw new Error('Invalid map suppression reference time');
  const positioned = nodes.flatMap((node) => {
    const seen = node.last_seen instanceof Date
      ? node.last_seen.getTime()
      : node.last_seen == null ? NaN : Date.parse(node.last_seen);
    if (!node.network || node.lat == null || node.lon == null
        || !Number.isFinite(node.lat) || !Number.isFinite(node.lon)
        || Math.abs(node.lat) > 90 || Math.abs(node.lon) > 180
        || (Math.abs(node.lat) < 5 && Math.abs(node.lon) < 5)
        || !Number.isFinite(seen) || seen > referenceMs) return [];
    return [{ node, age: (referenceMs - seen) / DAY_MS }];
  });
  const fresh = positioned.filter(({ age }) => age <= config.activeWithinDays)
    .sort((a, b) => a.age - b.age || a.node.node_id.localeCompare(b.node.node_id));
  const suppressions: NodeMapSuppression[] = [];
  for (const stale of positioned) {
    if (stale.age < config.staleAfterDays) continue;
    for (const active of fresh) {
      if (stale.node.node_id === active.node.node_id
          || stale.node.network !== active.node.network) continue;
      const distance = distanceMeters(stale.node, active.node);
      // Coordinate subtraction can put an exact 30 m boundary a few
      // nanometres above it. Allow only floating-point noise at that edge.
      if (distance - config.radiusM > 1e-8) continue;
      suppressions.push({
        node_id: stale.node.node_id,
        suppressed_by_node_id: active.node.node_id,
        reason: 'stale-colocated',
        distance_m: distance,
        stale_age_days: stale.age,
        freshest_age_days: active.age,
      });
      break;
    }
  }
  return suppressions;
}

type DbClient = {
  query<T extends pg.QueryResultRow = pg.QueryResultRow>(sql: string, params?: unknown[]): Promise<{ rows: T[] }>;
  release(): void;
};

export async function refreshNodeMapSuppressions(
  pool: { connect(): Promise<DbClient> },
): Promise<{ nodesConsidered: number; suppressedNodes: number }> {
  const config = mapSuppressionConfig();
  const client = await pool.connect();
  try {
    await client.query('BEGIN');
    await client.query(`SELECT pg_advisory_xact_lock(hashtext('meshcore-node-map-suppressions-refresh'))`);
    const { rows: [clock] } = await client.query<{ computed_at: Date }>('SELECT NOW() AS computed_at');
    const { rows } = await client.query<MapSuppressionNode>(
      `SELECT n.node_id, n.network, n.lat, n.lon,
              ${nodeEffectiveLastSeenSql('n')} AS last_seen
         FROM node_identity_nodes n
        WHERE ${mapVisiblePredicate('n')}`,
    );
    const suppressions = computeNodeMapSuppressions(rows, clock!.computed_at.getTime(), config);
    await client.query('DELETE FROM node_map_suppressions');
    if (suppressions.length > 0) {
      await client.query(
        `INSERT INTO node_map_suppressions
           (node_id, suppressed_by_node_id, reason, distance_m, stale_age_days, freshest_age_days)
         SELECT node_id, suppressed_by_node_id, reason, distance_m, stale_age_days, freshest_age_days
           FROM jsonb_to_recordset($1::jsonb) AS s(
             node_id text, suppressed_by_node_id text, reason text,
             distance_m double precision, stale_age_days double precision, freshest_age_days double precision
           )`,
        [JSON.stringify(suppressions)],
      );
    }
    await client.query('COMMIT');
    mapSuppressedNodes.set(suppressions.length);
    const result = { nodesConsidered: rows.length, suppressedNodes: suppressions.length };
    console.log('[node-map] refreshed suppressions', result);
    return result;
  } catch (error) {
    await client.query('ROLLBACK');
    throw error;
  } finally {
    client.release();
  }
}
