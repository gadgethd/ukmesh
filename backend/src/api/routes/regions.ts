import type { Router } from 'express';
import type { QueryResultRow } from 'pg';
import { resolvePublicNetworkScope } from '../../http/requestScope.js';
import type { NetworkFilters } from '../utils/networkFilters.js';
import { isPrivateNode } from '../utils/privateNode.js';

type QueryFn = <T extends QueryResultRow = QueryResultRow>(
  text: string,
  params?: unknown[],
) => Promise<{ rows: T[] }>;

export type RegionConfidence = 'confirmed' | 'probable' | 'single';

export type RegionMemberDto = {
  nodeId: string;
  name: string | null;
  role: number | null;
  lat: number | null;
  lon: number | null;
  packets: number;
  isBridge: boolean;
};

export type RegionSummaryDto = {
  name: string;
  packetsTotal: number;
  memberCount: number;
  repeaterMemberCount: number;
  confidence: RegionConfidence;
  firstSeen: string | null;
  lastSeen: string | null;
  members: RegionMemberDto[];
};

export type RegionRepeaterDto = {
  nodeId: string;
  name: string | null;
  role: number | null;
  lat: number | null;
  lon: number | null;
  status: 'green' | 'red';
  evidence: string;
  scopedPackets: number;
  unscopedPackets7d: number;
  regions: Record<string, number>;
};

export type RegionScopeDto = {
  nodeId: string;
  name: string | null;
  role: number | null;
  lat: number | null;
  lon: number | null;
  status: 'green' | 'red' | 'unobserved';
  evidence: string;
  scopedPackets: number;
  unscopedPackets7d: number;
  regions: Array<{ region: string; packets: number }>;
};

/**
 * Evidence tiers for a region's decoded packet count. The 2026-10 audit
 * calibrated the chance-match floor at ~0.2 false hits per candidate name over
 * the whole corpus: >=4 matches is cryptographically solid, 2-3 is plausible
 * but not proven, a single match may be chance.
 */
export function confidenceFor(packetsTotal: number): RegionConfidence {
  if (packetsTotal >= 4) return 'confirmed';
  if (packetsTotal >= 2) return 'probable';
  return 'single';
}

/**
 * Display envelope for the UK regions layer. The mesh dataset also carries
 * nodes from other deployments (Nordic SkyMesh, continental Europe) and a
 * small number of junk/duplicate rows with placeholder coordinates, so the
 * public UK map only renders nodes inside the UK. Deliberately coarse
 * (GB + Northern Ireland); the Channel Islands are excluded.
 */
export const UK_DISPLAY_ENVELOPE = {
  minLat: 49.5,
  maxLat: 61.2,
  minLon: -8.8,
  maxLon: 2.2,
} as const;

export function isWithinUkDisplayEnvelope(lat: number | null, lon: number | null): boolean {
  if (lat == null || lon == null) return false;
  if (!Number.isFinite(lat) || !Number.isFinite(lon)) return false;
  // Reject null-island and similar placeholder coordinates.
  if (Math.abs(lat) < 1e-6 && Math.abs(lon) < 1e-6) return false;
  return lat >= UK_DISPLAY_ENVELOPE.minLat && lat <= UK_DISPLAY_ENVELOPE.maxLat
    && lon >= UK_DISPLAY_ENVELOPE.minLon && lon <= UK_DISPLAY_ENVELOPE.maxLon;
}

/** True only for rows carrying coordinates that fall outside the UK envelope. */
function excludedByLocation(lat: number | null, lon: number | null): boolean {
  return lat != null && lon != null && !isWithinUkDisplayEnvelope(lat, lon);
}

interface RunRow extends QueryResultRow {
  finished_at: Date | string | null;
  window_days: number | null;
}

export interface RegionStatRow extends QueryResultRow {
  region: string;
  packets_total: number;
  member_count: number;
  repeater_member_count: number;
  first_seen: Date | string | null;
  last_seen: Date | string | null;
}

export interface RegionMemberRow extends QueryResultRow {
  region: string;
  node_id: string;
  packets: number;
  name: string | null;
  role: number | null;
  lat: number | null;
  lon: number | null;
}

export interface RegionRepeaterRow extends QueryResultRow {
  node_id: string;
  status: string;
  evidence: string;
  scoped_packets: number;
  unscoped_packets_7d: number;
  regions: Record<string, number> | null;
  name: string | null;
  role: number | null;
  lat: number | null;
  lon: number | null;
}

export interface RegionScopeNodeRow extends QueryResultRow {
  node_id: string;
  name: string | null;
  role: number | null;
  lat: number | null;
  lon: number | null;
}

interface ScopeMemberRow extends QueryResultRow {
  region: string;
  packets: number;
}

function isoOrNull(value: Date | string | null | undefined): string | null {
  if (value == null) return null;
  const date = value instanceof Date ? value : new Date(value);
  return Number.isFinite(date.getTime()) ? date.toISOString() : null;
}

function numberOrNull(value: number | null | undefined): number | null {
  return value == null ? null : Number(value);
}

function normalizeRegions(value: Record<string, number> | null): Record<string, number> {
  // node-postgres parses jsonb, but be defensive about non-numeric members.
  const regions: Record<string, number> = {};
  for (const [name, count] of Object.entries(value ?? {})) {
    const numeric = Number(count);
    if (Number.isFinite(numeric) && numeric > 0) regions[name] = numeric;
  }
  return regions;
}

/**
 * Universal "bridge" repeaters are configured to relay scoped traffic for most
 * of the network's regions (the live distribution puts them at 17+ regions,
 * with a clean gap down to 11 for ordinary multi-region repeaters), so their
 * position carries no signal about any single region's footprint. Region
 * shapes exclude bridge members; member lists keep them — they are real relays.
 */
export const BRIDGE_MIN_REGIONS = 12;

export function shapeRegionSummaryPayload(
  run: RunRow | null,
  stats: RegionStatRow[],
  memberRows: RegionMemberRow[],
): { generatedAt: string | null; windowDays: number | null; regions: RegionSummaryDto[] } {
  const membersByRegion = new Map<string, RegionMemberDto[]>();
  for (const row of memberRows) {
    if (isPrivateNode(row.name)) continue;
    if (excludedByLocation(row.lat, row.lon)) continue;
    const list = membersByRegion.get(row.region) ?? [];
    list.push({
      nodeId: row.node_id,
      name: row.name,
      role: numberOrNull(row.role),
      lat: numberOrNull(row.lat),
      lon: numberOrNull(row.lon),
      packets: Number(row.packets),
      isBridge: false,
    });
    membersByRegion.set(row.region, list);
  }
  // A node appears at most once per region list, so counting occurrences
  // across lists yields its distinct-region membership count.
  const regionsPerNode = new Map<string, number>();
  for (const list of membersByRegion.values()) {
    for (const member of list) {
      regionsPerNode.set(member.nodeId, (regionsPerNode.get(member.nodeId) ?? 0) + 1);
    }
  }
  for (const list of membersByRegion.values()) {
    for (const member of list) {
      member.isBridge = (regionsPerNode.get(member.nodeId) ?? 0) >= BRIDGE_MIN_REGIONS;
    }
  }
  return {
    generatedAt: isoOrNull(run?.finished_at),
    windowDays: run?.window_days == null ? null : Number(run.window_days),
    regions: stats.map((row) => ({
      name: row.region,
      packetsTotal: Number(row.packets_total),
      memberCount: Number(row.member_count),
      repeaterMemberCount: Number(row.repeater_member_count),
      confidence: confidenceFor(Number(row.packets_total)),
      firstSeen: isoOrNull(row.first_seen),
      lastSeen: isoOrNull(row.last_seen),
      members: membersByRegion.get(row.region) ?? [],
    })),
  };
}

export function shapeRegionRepeatersPayload(
  run: RunRow | null,
  rows: RegionRepeaterRow[],
): { generatedAt: string | null; repeaters: RegionRepeaterDto[] } {
  return {
    generatedAt: isoOrNull(run?.finished_at),
    repeaters: rows
      .filter((row) => !isPrivateNode(row.name))
      .filter((row) => !excludedByLocation(row.lat, row.lon))
      .map((row) => ({
        nodeId: row.node_id,
        name: row.name,
        role: numberOrNull(row.role),
        lat: numberOrNull(row.lat),
        lon: numberOrNull(row.lon),
        status: row.status === 'green' ? ('green' as const) : ('red' as const),
        evidence: row.evidence,
        scopedPackets: Number(row.scoped_packets),
        unscopedPackets7d: Number(row.unscoped_packets_7d),
        regions: normalizeRegions(row.regions),
      })),
  };
}

export function shapeRegionScopePayload(
  nodeRow: RegionScopeNodeRow | null,
  statusRow: RegionRepeaterRow | null,
  memberRows: ScopeMemberRow[],
): RegionScopeDto | null {
  if (!nodeRow && !statusRow) return null;
  const nodeId = (statusRow?.node_id ?? nodeRow?.node_id ?? '').toUpperCase();
  let name = statusRow?.name ?? nodeRow?.name ?? null;
  let lat = statusRow?.lat ?? nodeRow?.lat ?? null;
  let lon = statusRow?.lon ?? nodeRow?.lon ?? null;
  if (isPrivateNode(name)) {
    // Public geometry must never be derived from opted-out nodes.
    name = 'Private Node';
    lat = null;
    lon = null;
  }
  return {
    nodeId,
    name,
    role: numberOrNull(statusRow?.role ?? nodeRow?.role),
    lat: numberOrNull(lat),
    lon: numberOrNull(lon),
    status: statusRow ? (statusRow.status === 'green' ? 'green' : 'red') : 'unobserved',
    evidence: statusRow?.evidence ?? 'none',
    scopedPackets: Number(statusRow?.scoped_packets ?? 0),
    unscopedPackets7d: Number(statusRow?.unscoped_packets_7d ?? 0),
    regions: memberRows.map((row) => ({ region: row.region, packets: Number(row.packets) })),
  };
}

type RegionsRouteDeps = {
  query: QueryFn;
  networkFilters: (network?: string, observer?: string) => NetworkFilters;
  limiter: ReturnType<typeof import('express-rate-limit').rateLimit>;
};

const REGIONS_CACHE_CONTROL = 'public, max-age=300, stale-while-revalidate=900';

// Nodes can briefly hold more than one row per network; pick the most recently
// seen row for stable public metadata. The network predicate comes from the
// shared networkFilters helper so the public ukmesh scope keeps the historical
// northeast/teesside labels in scope (matching every other public route).
function nodeMetaLateral(alias: string, filters: NetworkFilters): string {
  return `LEFT JOIN LATERAL (
    SELECT n.name, n.role, n.lat, n.lon
      FROM nodes n
     WHERE n.node_id = ${alias}.node_id ${filters.nodesAlias('n')}
     ORDER BY n.last_seen DESC NULLS LAST
     LIMIT 1
  ) meta ON TRUE`;
}

export function registerRegionsRoutes(router: Router, deps: RegionsRouteDeps): void {
  router.get('/regions/summary', deps.limiter, async (req, res) => {
    try {
      const network = resolvePublicNetworkScope(req.query['network'], req.headers);
      const filters = deps.networkFilters(network);
      const [run, stats, members] = await Promise.all([
        deps.query<RunRow>('SELECT finished_at, window_days FROM region_scope_run LIMIT 1'),
        deps.query<RegionStatRow>(
          `SELECT region, packets_total, member_count, repeater_member_count, first_seen, last_seen
             FROM region_scope_stats
            ORDER BY packets_total DESC, region ASC`,
        ),
        deps.query<RegionMemberRow>(
          `SELECT m.region, m.node_id, m.packets, meta.name, meta.role, meta.lat, meta.lon
             FROM region_scope_members m
             ${nodeMetaLateral('m', filters)}
            ORDER BY m.region ASC, m.packets DESC, m.node_id ASC`,
          [...filters.params],
        ),
      ]);
      res.setHeader('Cache-Control', REGIONS_CACHE_CONTROL);
      res.json(shapeRegionSummaryPayload(run.rows[0] ?? null, stats.rows, members.rows));
    } catch (err) {
      console.error('[api] GET /regions/summary', (err as Error).message);
      res.status(500).json({ error: 'Internal server error' });
    }
  });

  router.get('/regions/repeaters', deps.limiter, async (req, res) => {
    try {
      const network = resolvePublicNetworkScope(req.query['network'], req.headers);
      const filters = deps.networkFilters(network);
      const [run, repeaters] = await Promise.all([
        deps.query<RunRow>('SELECT finished_at, window_days FROM region_scope_run LIMIT 1'),
        deps.query<RegionRepeaterRow>(
          `SELECT s.node_id, s.status, s.evidence, s.scoped_packets, s.unscoped_packets_7d, s.regions,
                  meta.name, meta.role, meta.lat, meta.lon
             FROM repeater_scope_status s
             ${nodeMetaLateral('s', filters)}
            ORDER BY s.scoped_packets DESC, s.node_id ASC`,
          [...filters.params],
        ),
      ]);
      res.setHeader('Cache-Control', REGIONS_CACHE_CONTROL);
      res.json(shapeRegionRepeatersPayload(run.rows[0] ?? null, repeaters.rows));
    } catch (err) {
      console.error('[api] GET /regions/repeaters', (err as Error).message);
      res.status(500).json({ error: 'Internal server error' });
    }
  });

  router.get('/repeaters/:id/scope', deps.limiter, async (req, res) => {
    const rawId = String(req.params['id'] ?? '').trim().toUpperCase();
    if (!/^[0-9A-F]{64}$/.test(rawId)) {
      res.status(400).json({ error: 'Invalid repeater id' });
      return;
    }
    try {
      const network = resolvePublicNetworkScope(req.query['network'], req.headers);
      const filters = deps.networkFilters(network);
      const idParam = `$${filters.params.length + 1}`;
      const [status, members, node] = await Promise.all([
        deps.query<RegionRepeaterRow>(
          `SELECT s.node_id, s.status, s.evidence, s.scoped_packets, s.unscoped_packets_7d, s.regions,
                  meta.name, meta.role, meta.lat, meta.lon
             FROM repeater_scope_status s
             ${nodeMetaLateral('s', filters)}
            WHERE s.node_id = ${idParam}`,
          [...filters.params, rawId],
        ),
        deps.query<ScopeMemberRow>(
          `SELECT region, packets
             FROM region_scope_members
            WHERE node_id = $1
            ORDER BY packets DESC, region ASC`,
          [rawId],
        ),
        deps.query<RegionScopeNodeRow>(
          `SELECT n.node_id, n.name, n.role, n.lat, n.lon
             FROM nodes n
            WHERE n.node_id = ${idParam} ${filters.nodesAlias('n')}
            ORDER BY n.last_seen DESC NULLS LAST
            LIMIT 1`,
          [...filters.params, rawId],
        ),
      ]);
      const payload = shapeRegionScopePayload(
        node.rows[0] ?? null,
        status.rows[0] ?? null,
        members.rows,
      );
      if (!payload) {
        res.status(404).json({ error: 'Repeater not found' });
        return;
      }
      res.setHeader('Cache-Control', REGIONS_CACHE_CONTROL);
      res.json(payload);
    } catch (err) {
      console.error('[api] GET /repeaters/:id/scope', (err as Error).message);
      res.status(500).json({ error: 'Internal server error' });
    }
  });
}
