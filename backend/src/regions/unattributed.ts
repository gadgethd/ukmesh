// Geographic clustering for region-scoped packets that no public region name
// explains.
//
// A packet whose transport code matches no candidate name is still real
// traffic: it is scoped to *some* region, just one whose name cannot be
// derived (private key material, or a non-geographic public name). Grouping
// those packets by where they were heard gives an honest, name-free view of
// the gap — "unattributed traffic around Bristol", not a guessed region name.
//
// The clustering is deliberately simple and deterministic: each packet is
// reduced to the centroid of the nodes that carried it (observers plus
// resolved relay hops), packets are greedily assigned to the nearest cluster
// within RADIUS_KM, then clusters closer than that radius are merged. A cluster
// is only reported once it holds MIN_PACKETS packets, so a handful of
// one-off receptions never becomes a shape on the map.

export type LocatedNode = {
  nodeId: string;
  lat: number;
  lon: number;
  name?: string | null;
};

export type UnattributedPacket = {
  hash: string;
  nodes: readonly LocatedNode[];
  firstSeen?: Date | string | null;
  lastSeen?: Date | string | null;
};

export type UnattributedClusterNode = {
  nodeId: string;
  name: string | null;
  lat: number;
  lon: number;
  packets: number;
};

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

export type UnattributedClusterOptions = {
  radiusKm?: number;
  minPackets?: number;
  maxTopNodes?: number;
};

export type UnattributedClusterResult = {
  clusters: UnattributedCluster[];
  /** Packets that landed in a reported cluster. */
  clusteredPackets: number;
  /** Packets with no located node at all; they cannot be placed on a map. */
  unlocatedPackets: number;
  /** Packets in clusters below minPackets (deliberately not reported). */
  discardedPackets: number;
};

export const DEFAULT_CLUSTER_RADIUS_KM = 60;
export const DEFAULT_CLUSTER_MIN_PACKETS = 10;
export const DEFAULT_CLUSTER_MAX_TOP_NODES = 5;

const EARTH_RADIUS_KM = 6371.0088;

/** Great-circle distance in kilometres. */
export function haversineKm(
  aLat: number,
  aLon: number,
  bLat: number,
  bLon: number,
): number {
  const toRadians = Math.PI / 180;
  const dLat = (bLat - aLat) * toRadians;
  const dLon = (bLon - aLon) * toRadians;
  const lat1 = aLat * toRadians;
  const lat2 = bLat * toRadians;
  const h = Math.sin(dLat / 2) ** 2 + Math.cos(lat1) * Math.cos(lat2) * Math.sin(dLon / 2) ** 2;
  return 2 * EARTH_RADIUS_KM * Math.asin(Math.min(1, Math.sqrt(h)));
}

interface WorkingCluster {
  latSum: number;
  lonSum: number;
  weight: number;
  packets: number;
  nodes: Map<string, { node: LocatedNode; count: number }>;
  firstMs: number | null;
  lastMs: number | null;
}

function toMs(value: Date | string | null | undefined): number | null {
  if (value == null) return null;
  const date = value instanceof Date ? value : new Date(value);
  const ms = date.getTime();
  return Number.isFinite(ms) ? ms : null;
}

function toIso(ms: number | null): string | null {
  return ms == null ? null : new Date(ms).toISOString();
}

function round5(value: number): number {
  return Number(value.toFixed(5));
}

function centreOf(cluster: WorkingCluster): { lat: number; lon: number } {
  return { lat: cluster.latSum / cluster.weight, lon: cluster.lonSum / cluster.weight };
}

function isLocatable(node: LocatedNode): boolean {
  return Number.isFinite(node.lat) && Number.isFinite(node.lon)
    && Math.abs(node.lat) <= 90 && Math.abs(node.lon) <= 180
    // Null-island style placeholders carry no signal.
    && !(Math.abs(node.lat) < 1e-6 && Math.abs(node.lon) < 1e-6);
}

function absorb(target: WorkingCluster, source: WorkingCluster): void {
  target.latSum += source.latSum;
  target.lonSum += source.lonSum;
  target.weight += source.weight;
  target.packets += source.packets;
  for (const [nodeId, entry] of source.nodes) {
    const existing = target.nodes.get(nodeId);
    if (existing) existing.count += entry.count;
    else target.nodes.set(nodeId, { node: entry.node, count: entry.count });
  }
  if (source.firstMs != null && (target.firstMs == null || source.firstMs < target.firstMs)) {
    target.firstMs = source.firstMs;
  }
  if (source.lastMs != null && (target.lastMs == null || source.lastMs > target.lastMs)) {
    target.lastMs = source.lastMs;
  }
}

/**
 * Cluster unattributed packets into geographic footprints.
 *
 * Deterministic: identical input (same order) always yields identical clusters
 * and ids, and ids are assigned by descending packet count (`u1` is the
 * busiest cluster).
 */
export function clusterUnattributedPackets(
  packets: readonly UnattributedPacket[],
  options: UnattributedClusterOptions = {},
): UnattributedClusterResult {
  const radiusKm = options.radiusKm ?? DEFAULT_CLUSTER_RADIUS_KM;
  const minPackets = options.minPackets ?? DEFAULT_CLUSTER_MIN_PACKETS;
  const maxTopNodes = options.maxTopNodes ?? DEFAULT_CLUSTER_MAX_TOP_NODES;

  const working: WorkingCluster[] = [];
  let unlocatedPackets = 0;

  for (const packet of packets) {
    const located = packet.nodes.filter(isLocatable);
    if (located.length === 0) {
      unlocatedPackets += 1;
      continue;
    }
    const unique = new Map<string, LocatedNode>();
    for (const node of located) {
      if (!unique.has(node.nodeId)) unique.set(node.nodeId, node);
    }
    const points = [...unique.values()];
    const lat = points.reduce((sum, node) => sum + node.lat, 0) / points.length;
    const lon = points.reduce((sum, node) => sum + node.lon, 0) / points.length;

    let bestIndex = -1;
    let bestDistance = Number.POSITIVE_INFINITY;
    for (let index = 0; index < working.length; index += 1) {
      const centre = centreOf(working[index]!);
      const distance = haversineKm(lat, lon, centre.lat, centre.lon);
      if (distance <= radiusKm && distance < bestDistance) {
        bestDistance = distance;
        bestIndex = index;
      }
    }

    let target: WorkingCluster;
    if (bestIndex >= 0) {
      target = working[bestIndex]!;
    } else {
      target = {
        latSum: 0, lonSum: 0, weight: 0, packets: 0,
        nodes: new Map(), firstMs: null, lastMs: null,
      };
      working.push(target);
    }
    target.latSum += lat;
    target.lonSum += lon;
    target.weight += 1;
    target.packets += 1;
    for (const node of points) {
      const existing = target.nodes.get(node.nodeId);
      if (existing) existing.count += 1;
      else target.nodes.set(node.nodeId, { node, count: 1 });
    }
    const first = toMs(packet.firstSeen);
    const last = toMs(packet.lastSeen);
    if (first != null && (target.firstMs == null || first < target.firstMs)) target.firstMs = first;
    if (last != null && (target.lastMs == null || last > target.lastMs)) target.lastMs = last;
  }

  // Greedy assignment can leave two clusters that overlap; merge until stable.
  let merged = true;
  while (merged) {
    merged = false;
    outer: for (let a = 0; a < working.length; a += 1) {
      const centreA = centreOf(working[a]!);
      for (let b = a + 1; b < working.length; b += 1) {
        const centreB = centreOf(working[b]!);
        if (haversineKm(centreA.lat, centreA.lon, centreB.lat, centreB.lon) <= radiusKm) {
          absorb(working[a]!, working[b]!);
          working.splice(b, 1);
          merged = true;
          break outer;
        }
      }
    }
  }

  const kept = working.filter((cluster) => cluster.packets >= minPackets);
  const discardedPackets = working
    .filter((cluster) => cluster.packets < minPackets)
    .reduce((sum, cluster) => sum + cluster.packets, 0);

  kept.sort((a, b) => b.packets - a.packets || b.weight - a.weight);

  const clusters: UnattributedCluster[] = kept.map((cluster, index) => {
    const centre = centreOf(cluster);
    const topNodes: UnattributedClusterNode[] = [...cluster.nodes.values()]
      .sort((a, b) => b.count - a.count || a.node.nodeId.localeCompare(b.node.nodeId))
      .slice(0, maxTopNodes)
      .map((entry) => ({
        nodeId: entry.node.nodeId,
        name: entry.node.name ?? null,
        lat: round5(entry.node.lat),
        lon: round5(entry.node.lon),
        packets: entry.count,
      }));
    return {
      id: `u${index + 1}`,
      lat: round5(centre.lat),
      lon: round5(centre.lon),
      packets: cluster.packets,
      nodeCount: cluster.nodes.size,
      firstSeen: toIso(cluster.firstMs),
      lastSeen: toIso(cluster.lastMs),
      topNodes,
    };
  });

  return {
    clusters,
    clusteredPackets: clusters.reduce((sum, cluster) => sum + cluster.packets, 0),
    unlocatedPackets,
    discardedPackets,
  };
}
