// Region-scope model helpers for the public /regions page.
//
// Pure functions only — the map overlay builders and page components consume
// these; tests run them under node without a DOM.

export type RegionConfidence = 'confirmed' | 'probable' | 'single';

export type RegionMember = {
  nodeId: string;
  name: string | null;
  role: number | null;
  lat: number | null;
  lon: number | null;
  packets: number;
  /** True for universal bridge repeaters that relay most of the network's regions. */
  isBridge?: boolean;
};

export type RegionSummary = {
  name: string;
  packetsTotal: number;
  memberCount: number;
  repeaterMemberCount: number;
  confidence: RegionConfidence;
  firstSeen: string | null;
  lastSeen: string | null;
  members: RegionMember[];
};

export type RegionRepeater = {
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

export type RegionSummaryPayload = {
  generatedAt: string | null;
  windowDays: number | null;
  regions: RegionSummary[];
};

export type RegionRepeatersPayload = {
  generatedAt: string | null;
  repeaters: RegionRepeater[];
};

export type UnattributedClusterNode = {
  nodeId: string;
  name: string | null;
  lat: number;
  lon: number;
  packets: number;
};

/**
 * A footprint of scoped packets that no public region name explains. It has no
 * `name` on purpose: private regions derive their key from stored material, so
 * the name is not recoverable from traffic. The map shows where the traffic is
 * heard, not a guessed label.
 */
export type UnattributedCluster = {
  id: string;
  lat: number;
  lon: number;
  packets: number;
  nodeCount: number;
  firstSeen: string | null;
  lastSeen: string | null;
  topNodes: UnattributedClusterNode[];
};

export type UnattributedPayload = {
  generatedAt: string | null;
  windowDays: number | null;
  clusters: UnattributedCluster[];
  totals: { clusters: number; packets: number; nodes: number };
};

const REGION_PALETTE = [
  '#22d3ee', '#a78bfa', '#34d399', '#fbbf24', '#f472b6', '#60a5fa',
  '#f87171', '#4ade80', '#facc15', '#c084fc', '#38bdf8', '#fb923c',
] as const;

/** Stable per-region colour: same name always maps to the same palette entry. */
export function regionColor(name: string): string {
  let hash = 0;
  for (let index = 0; index < name.length; index += 1) {
    hash = (hash * 31 + name.charCodeAt(index)) >>> 0;
  }
  return REGION_PALETTE[hash % REGION_PALETTE.length]!;
}

/** Mirrors the backend evidence tiers (>=4 confirmed, 2-3 probable, 1 single). */
export function confidenceFor(packets: number): RegionConfidence {
  if (packets >= 4) return 'confirmed';
  if (packets >= 2) return 'probable';
  return 'single';
}

/**
 * Display threshold for the default map and list: regions with fewer than
 * three decoded packets are chance-match grade — the 2026-10 calibration
 * measured ~15% of candidate names picking up a single spurious hit over the
 * whole corpus, so 1-2 observations stay hidden unless the user opts in.
 */
export const REGION_ESTABLISHED_MIN_PACKETS = 3;

export function isEstablished(region: Pick<RegionSummary, 'packetsTotal'>): boolean {
  return region.packetsTotal >= REGION_ESTABLISHED_MIN_PACKETS;
}

/** Regions for the default view; low-confidence ones appear only when asked for. */
export function regionsForDisplay(
  regions: readonly RegionSummary[],
  includeLowConfidence: boolean,
): RegionSummary[] {
  return includeLowConfidence ? [...regions] : regions.filter(isEstablished);
}

export function compactNumber(value: number): string {
  return new Intl.NumberFormat('en-GB', { notation: 'compact', maximumFractionDigits: 1 }).format(value);
}

export type Coordinate = [number, number];

function hasCoordinates(member: { lat: number | null; lon: number | null }): member is { lat: number; lon: number } {
  const { lat, lon } = member;
  return lat != null && lon != null
    && Number.isFinite(lat) && Number.isFinite(lon)
    && lat >= -90 && lat <= 90
    && lon >= -180 && lon <= 180
    && !(Math.abs(lat) < 1e-6 && Math.abs(lon) < 1e-6);
}

/**
 * Andrew's monotone chain over [lon, lat] points. Returns the hull vertices
 * counter-clockwise without repeating the first point; collinear inputs yield
 * fewer than three vertices (callers treat that as "no polygon").
 */
export function convexHull(points: ReadonlyArray<Coordinate>): Coordinate[] {
  const unique = [...new Map(points.map(([x, y]) => [`${x},${y}`, [x, y] as Coordinate])).values()]
    .sort((a, b) => a[0] - b[0] || a[1] - b[1]);
  if (unique.length <= 2) return unique;
  const cross = (o: Coordinate, a: Coordinate, b: Coordinate): number =>
    (a[0] - o[0]) * (b[1] - o[1]) - (a[1] - o[1]) * (b[0] - o[0]);
  const lower: Coordinate[] = [];
  for (const point of unique) {
    while (lower.length >= 2 && cross(lower[lower.length - 2]!, lower[lower.length - 1]!, point) <= 0) lower.pop();
    lower.push(point);
  }
  const upper: Coordinate[] = [];
  for (let index = unique.length - 1; index >= 0; index -= 1) {
    const point = unique[index]!;
    while (upper.length >= 2 && cross(upper[upper.length - 2]!, upper[upper.length - 1]!, point) <= 0) upper.pop();
    upper.push(point);
  }
  lower.pop();
  upper.pop();
  return lower.concat(upper);
}

/**
 * Closed GeoJSON ring for a region: the convex hull of its resolved relay
 * members (repeater-role members preferred). Universal bridge repeaters are
 * excluded — they relay most regions' traffic, so their positions carry no
 * signal about this region's footprint. Null when fewer than three located
 * non-bridge members remain or the hull degenerates to a line.
 */
export function regionRing(region: RegionSummary): Coordinate[] | null {
  const located = region.members.filter((member) => member.isBridge !== true && hasCoordinates(member));
  const repeaterMembers = located.filter((member) => member.role === 2);
  const source = repeaterMembers.length >= 3 ? repeaterMembers : located;
  if (source.length < 3) return null;
  const hull = convexHull(source.map((member) => [member.lon, member.lat] as Coordinate));
  if (hull.length < 3) return null;
  const ring = hull.map(([lon, lat]) => [Number(lon.toFixed(5)), Number(lat.toFixed(5))] as Coordinate);
  return [...ring, ring[0]!];
}

/** Node ids of a region's resolved relay members, for map filtering. */
export function memberIdSet(region: RegionSummary): Set<string> {
  return new Set(region.members.map((member) => member.nodeId));
}

/**
 * Footprint radius for an unattributed cluster: a 10 km floor so a cluster is
 * visible at country zoom, growing with the square root of its packet count and
 * capped well below the clustering radius (60 km) so neighbouring clusters stay
 * distinguishable.
 */
export function unattributedRadiusKm(packets: number): number {
  return Math.min(48, 10 + Math.sqrt(Math.max(0, packets)) * 1.1);
}

/** Closed GeoJSON ring (lon/lat) approximating a cluster footprint. */
export function unattributedRing(cluster: UnattributedCluster, segments = 48): Coordinate[] {
  const radiusKm = unattributedRadiusKm(cluster.packets);
  const dLat = radiusKm / 111.32;
  const dLon = radiusKm / (111.32 * Math.max(0.2, Math.cos((cluster.lat * Math.PI) / 180)));
  const ring: Coordinate[] = [];
  for (let index = 0; index < segments; index += 1) {
    const angle = (index / segments) * Math.PI * 2;
    ring.push([
      Number((cluster.lon + Math.cos(angle) * dLon).toFixed(5)),
      Number((cluster.lat + Math.sin(angle) * dLat).toFixed(5)),
    ]);
  }
  ring.push(ring[0]!);
  return ring;
}

export function formatGeneratedAt(value: string | null): string | null {
  if (!value) return null;
  const date = new Date(value);
  if (!Number.isFinite(date.getTime())) return null;
  return date.toLocaleString('en-GB', {
    day: 'numeric', month: 'short', hour: '2-digit', minute: '2-digit',
  });
}
