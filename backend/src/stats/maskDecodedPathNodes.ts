const PROHIBITED_NODE_MARKER = '🚫';
const HIDDEN_NODE_MASK_RADIUS_MILES = 1;

function isProhibitedMapNode(node: { name?: string | null } | null | undefined): boolean {
  return Boolean(node?.name?.includes(PROHIBITED_NODE_MARKER));
}

function hashSeed(input: string): number {
  let hash = 2166136261;
  for (let i = 0; i < input.length; i++) {
    hash ^= input.charCodeAt(i);
    hash = Math.imul(hash, 16777619);
  }
  return hash >>> 0;
}

function seededUnitPair(seed: string): [number, number] {
  const distanceUnit = hashSeed(`${seed}:distance`) / 0xffffffff;
  const bearingUnit = hashSeed(`${seed}:bearing`) / 0xffffffff;
  return [distanceUnit, bearingUnit];
}

function stablePointWithinMiles(
  lat: number,
  lon: number,
  seed: string,
  radiusMiles = HIDDEN_NODE_MASK_RADIUS_MILES,
): [number, number] {
  const radiusKm = radiusMiles * 1.609344;
  const [distanceUnit, bearingUnit] = seededUnitPair(seed);
  const distanceKm = Math.sqrt(distanceUnit) * radiusKm;
  const bearing = bearingUnit * Math.PI * 2;
  const latRad = lat * (Math.PI / 180);
  const dLat = (distanceKm / 111) * Math.cos(bearing);
  const lonScale = Math.max(0.01, Math.cos(latRad));
  const dLon = (distanceKm / (111 * lonScale)) * Math.sin(bearing);
  return [lat + dLat, lon + dLon];
}

export function maskDecodedPathNodes(
  rawNodes: Array<{
    ord: number;
    node_id: string | null;
    name: string | null;
    lat: number | null;
    lon: number | null;
    last_seen?: string | null;
  }> | null | undefined,
): Array<{
  ord: number;
  node_id: string | null;
  name: string | null;
  lat: number | null;
  lon: number | null;
}> {
  if (!Array.isArray(rawNodes)) return [];
  return rawNodes.map((node) => {
    if (!node || typeof node !== 'object') return node;
    if (!isProhibitedMapNode(node)) {
      return {
        ord: Number(node.ord ?? 0),
        node_id: node.node_id ?? null,
        name: node.name ?? null,
        lat: node.lat ?? null,
        lon: node.lon ?? null,
      };
    }
    if (typeof node.lat !== 'number' || typeof node.lon !== 'number') {
      return {
        ord: Number(node.ord ?? 0),
        node_id: node.node_id ?? null,
        name: 'Redacted repeater',
        lat: node.lat ?? null,
        lon: node.lon ?? null,
      };
    }
    const activityKey = node.last_seen ?? 'unknown';
    const seed = `${node.node_id ?? 'unknown'}|${activityKey}`;
    const [maskedLat, maskedLon] = stablePointWithinMiles(node.lat, node.lon, seed);
    return {
      ord: Number(node.ord ?? 0),
      node_id: node.node_id ?? null,
      name: 'Redacted repeater',
      lat: maskedLat,
      lon: maskedLon,
    };
  });
}
