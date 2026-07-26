/**
 * Cluster-level spam origin resolver.
 *
 * Given a spam incident's repeated transmissions (each heard by many observers,
 * every reception carrying the ordered relay path_hashes), this walks each
 * transmission's relay chain to its source repeater and finds the geographic
 * consensus across all of them, weighting transmissions heard by a close
 * observer (low hop count) highest.
 *
 * Uses path_hashes embedded in packets by relaying nodes, combined with
 * geographic anchoring from MQTT observer positions, to reconstruct the chain.
 *
 * Scoring weights and prior key-formats live in ../path-shared/scoring.ts so
 * the lazy and beta resolvers cannot silently drift apart.
 */
import { MAX_HOP_KM } from '../path-shared/scoring.js';
import { expandResolverScope } from '../networks.js';
import {
  buildNodePathHashIndex,
  getNodesForPathHash,
  normalizePathHash,
  type NodePathHashIndex,
} from '../path-hash/utils.js';
import type { OriginEstimate, ConfidenceLevel } from './types.js';
import type { SpamMessageConfig } from './config.js';
import { nearestRegion, levelFor } from './origin.js';

type QueryFn = <T extends Record<string, unknown> = Record<string, unknown>>(
  text: string,
  params?: unknown[],
) => Promise<{ rows: T[] }>;

function networkScope(network: string | null): string[] | null {
  return network == null ? null : expandResolverScope(network);
}

function distKm(
  a: { lat: number; lon: number },
  b: { lat: number; lon: number },
): number {
  const R = 6371;
  const dLat = (b.lat - a.lat) * (Math.PI / 180);
  const dLon = (b.lon - a.lon) * (Math.PI / 180);
  const sinLat = Math.sin(dLat / 2) ** 2;
  const sinLon = Math.sin(dLon / 2) ** 2;
  const cosA = Math.cos(a.lat * (Math.PI / 180));
  const cosB = Math.cos(b.lat * (Math.PI / 180));
  return 2 * R * Math.asin(Math.sqrt(sinLat + cosA * cosB * sinLon));
}

// ===========================================================================
// Cluster-level origin resolution for spam incidents.
//
// A spam flood is transmitted many times; each transmission is relayed across
// the mesh and heard by many observers, every reception carrying the ordered
// relay path. The FIRST repeater in a path is the source of truth: the spammer
// was in direct range of it. We walk each transmission's relay chain to its
// source repeater, then find the geographic consensus across all of them —
// weighting transmissions heard by a CLOSE observer (low hop count) highest,
// since their paths are shortest and resolve most reliably.
// ===========================================================================

interface FirstHopVote {
  nodeId: string;
  name: string | null;
  lat: number;
  lon: number;
  ambiguous: boolean;
  /** Closeness weight from the observer that heard this transmission. */
  weight: number;
}

interface RepeaterAgg {
  nodeId: string;
  name: string | null;
  lat: number;
  lon: number;
  weight: number;
  votes: number;
  ambiguous: number;
}

type NodeLoc = { node_id: string; name: string | null; lat: number; lon: number };

interface ReceptionRow extends Record<string, unknown> {
  rx_node_id: string;
  hop_count: number;
  path_hashes: string[];
  olat: number;
  olon: number;
}

/**
 * Walk a relay path backward from the observer to the source, choosing at each
 * hop the candidate node that has a confirmed RF link (`node_links`) to the
 * previously-resolved node — else the nearest candidate within link range. Each
 * consecutive (AA,BB) hash pair is only realisable where two such repeaters sit
 * within range, so the adjacency chain disambiguates the 1-byte hashes and pins
 * the FIRST repeater (nearest the source). Returns null if the chain breaks.
 */
function walkChainToSource(
  pathHashes: string[],
  observerId: string,
  observer: { lat: number; lon: number },
  index: NodePathHashIndex<NodeLoc>,
  adjacency: Map<string, Set<string>>,
): { first: NodeLoc; linkQuality: number } | null {
  let prevId = observerId;
  let prevLoc = observer;
  let first: NodeLoc | null = null;
  let linkSteps = 0;
  let steps = 0;
  for (let i = pathHashes.length - 1; i >= 0; i--) {
    const cands = getNodesForPathHash(index, normalizePathHash(pathHashes[i]!));
    if (cands.length === 0) return null;
    steps += 1;
    const linked = cands.filter((c) => adjacency.get(prevId)?.has(c.node_id));
    const pool = linked.length > 0 ? linked : cands.filter((c) => distKm(prevLoc, c) <= MAX_HOP_KM);
    if (pool.length === 0) return null;
    if (linked.length > 0) linkSteps += 1;
    let chosen = pool[0]!;
    let bestD = distKm(prevLoc, chosen);
    for (const c of pool) {
      const d = distKm(prevLoc, c);
      if (d < bestD) {
        bestD = d;
        chosen = c;
      }
    }
    prevId = chosen.node_id;
    prevLoc = { lat: chosen.lat, lon: chosen.lon };
    first = chosen;
  }
  return first ? { first, linkQuality: steps > 0 ? linkSteps / steps : 0 } : null;
}

/**
 * Resolve a spam cluster's coarse origin from relay paths.
 *
 * Method: a flood is heard by many observers, each reception carrying its
 * ordered relay path. We take the lowest-hop receptions (closest to the source)
 * and walk each path BACKWARD from the observer, hop by hop, choosing at each
 * step the candidate that has a confirmed RF link (`node_links`) to the previous
 * node — else the nearest candidate within range. Because a consecutive (AA,BB)
 * hash pair is only realisable where two such repeaters are within range, the
 * adjacency chain disambiguates the 1-byte hashes; the chain's FIRST repeater is
 * the one the spammer transmitted into. The "random deviations" in the middle of
 * the paths give many independent chains that converge on the same source. We
 * take the geographic consensus of the first repeaters.
 *
 * Returns null when paths can't be resolved confidently — the caller then falls
 * back to the observer-signal estimate.
 */
export async function resolveSpamOrigin(
  packetHashes: string[],
  network: string | null,
  query: QueryFn,
  cfg: SpamMessageConfig,
): Promise<OriginEstimate | null> {
  const ids = [...new Set(packetHashes)].filter((h) => h.length > 0);
  if (ids.length === 0) return null;
  const scope = networkScope(network);

  // 1. Receptions that carry a path, with the observer's id, hop, location.
  const recRes = await query<ReceptionRow>(
    `SELECT p.rx_node_id, p.hop_count, p.path_hashes, n.lat AS olat, n.lon AS olon
       FROM packets p
       JOIN nodes n ON n.node_id = p.rx_node_id
      WHERE p.packet_hash = ANY($1)
        AND ($2::text[] IS NULL OR p.network = ANY($2))
        AND p.path_hashes IS NOT NULL AND cardinality(p.path_hashes) > 0
        AND p.hop_count IS NOT NULL
        AND n.lat IS NOT NULL AND n.lon IS NOT NULL AND n.lat <> 0 AND n.lon <> 0
        AND (n.name IS NULL OR n.name NOT LIKE '%🚫%')
        AND NOT EXISTS (
          SELECT 1
          FROM nodes private_node
          WHERE private_node.name LIKE '%🚫%'
            AND (
              private_node.node_id IN (p.rx_node_id, p.src_node_id)
              OR EXISTS (
                SELECT 1
                FROM unnest(COALESCE(p.path_hashes, ARRAY[]::text[])) AS path_hash
                WHERE p.path_hash_size_bytes BETWEEN 1 AND 3
                  AND UPPER(private_node.node_id) LIKE UPPER(path_hash) || '%'
              )
            )
        )`,
    [ids, scope],
  );
  if (recRes.rows.length === 0) return null;

  // Keep only the closest cohort: the lowest-hop receptions sit nearest the
  // source, so their first repeater localises it; distant receptions don't.
  const minHop = Math.min(...recRes.rows.map((r) => Number(r.hop_count)));
  const cohort = recRes.rows
    .filter((r) => Number(r.hop_count) <= minHop + cfg.originNearHopSlack)
    .sort((a, b) => Number(a.hop_count) - Number(b.hop_count))
    .slice(0, Math.max(1, cfg.originPathMaxPackets));

  // 2. Located nodes across the physical mesh, indexed by path-hash prefix.
  const nodeRes = await query<NodeLoc>(
    `SELECT node_id, name, lat, lon FROM nodes
      WHERE ($1::text[] IS NULL OR network = ANY($1))
        AND lat IS NOT NULL AND lon IS NOT NULL AND lat <> 0 AND lon <> 0
        AND (name IS NULL OR name NOT LIKE '%🚫%')
        AND length(node_id) >= 2`,
    [scope],
  );
  if (nodeRes.rows.length === 0) return null;
  const index = buildNodePathHashIndex(nodeRes.rows, [2, 4, 6]);

  // 3. Confirmed RF links (node_links) among the candidate universe + observers,
  //    so each hop can be resolved to the node that actually links to the next.
  const universe = new Set<string>();
  for (const r of cohort) {
    universe.add(r.rx_node_id);
    for (const h of r.path_hashes) {
      for (const c of getNodesForPathHash(index, normalizePathHash(h))) universe.add(c.node_id);
    }
  }
  const adjacency = new Map<string, Set<string>>();
  if (universe.size > 0) {
    const uni = [...universe];
    const linkRes = await query<{ node_a_id: string; node_b_id: string }>(
      `SELECT node_a_id, node_b_id FROM node_links
        WHERE node_a_id = ANY($1) AND node_b_id = ANY($1)
          AND (observed_count > 0 OR itm_viable = true OR force_viable = true)`,
      [uni],
    );
    for (const l of linkRes.rows) {
      if (!adjacency.has(l.node_a_id)) adjacency.set(l.node_a_id, new Set());
      if (!adjacency.has(l.node_b_id)) adjacency.set(l.node_b_id, new Set());
      adjacency.get(l.node_a_id)!.add(l.node_b_id);
      adjacency.get(l.node_b_id)!.add(l.node_a_id);
    }
  }

  // 4. Walk each close reception's path backward from the observer along the
  //    adjacency chain; the chain's first repeater is the source's repeater.
  const votes: FirstHopVote[] = [];
  for (const r of cohort) {
    const obs = { lat: Number(r.olat), lon: Number(r.olon) };
    const hop = Number(r.hop_count);
    const walk = walkChainToSource(r.path_hashes, r.rx_node_id, obs, index, adjacency);
    if (!walk) continue;
    votes.push({
      nodeId: walk.first.node_id,
      name: walk.first.name,
      lat: walk.first.lat,
      lon: walk.first.lon,
      // A chain mostly built from confirmed links is trustworthy; a mostly
      // geographic chain is weaker evidence.
      ambiguous: walk.linkQuality < 0.5,
      weight: (1 / (1 + Math.max(0, hop))) * (0.5 + 0.5 * walk.linkQuality),
    });
  }

  if (votes.length < cfg.originPathMinVotes) return null;

  // Aggregate per repeater, weighting by closeness x certainty.
  const byNode = new Map<string, RepeaterAgg>();
  let totalW = 0;
  for (const v of votes) {
    const w = v.weight * (v.ambiguous ? cfg.originPathAmbiguousWeight : 1);
    totalW += w;
    const e =
      byNode.get(v.nodeId) ??
      { nodeId: v.nodeId, name: v.name, lat: v.lat, lon: v.lon, weight: 0, votes: 0, ambiguous: 0 };
    e.weight += w;
    e.votes += 1;
    if (v.ambiguous) e.ambiguous += 1;
    byNode.set(v.nodeId, e);
  }
  if (totalW <= 0) return null;

  // The strongest near-source repeater anchors the consensus; nearby repeaters
  // refine it; far outliers (ambiguous mis-resolves elsewhere) are dropped.
  const top = [...byNode.values()].sort((a, b) => b.weight - a.weight)[0]!;
  let sw = 0;
  let slat = 0;
  let slon = 0;
  const inliers: RepeaterAgg[] = [];
  for (const e of byNode.values()) {
    if (distKm({ lat: top.lat, lon: top.lon }, { lat: e.lat, lon: e.lon }) <= cfg.originPathClusterKm) {
      inliers.push(e);
      sw += e.weight;
      slat += e.weight * e.lat;
      slon += e.weight * e.lon;
    }
  }
  if (sw <= 0) return null;
  const lat = slat / sw;
  const lon = slon / sw;

  let wd2 = 0;
  for (const e of inliers) {
    const d = distKm({ lat, lon }, { lat: e.lat, lon: e.lon });
    wd2 += e.weight * d * d;
  }
  const rmsKm = Math.sqrt(wd2 / sw);

  const inlierVotes = inliers.reduce((a, e) => a + e.votes, 0);
  const inlierAmbig = inliers.reduce((a, e) => a + e.ambiguous, 0);
  const dominance = sw / totalW; // share of weight in the consensus cluster
  const ambigShare = inlierAmbig / Math.max(1, inlierVotes);

  // Radius: ~one link range around the near-source repeater + cluster spread.
  let radiusKm = Math.max(cfg.originMinRadiusKm, rmsKm + cfg.originPerHopKm);
  radiusKm = Math.ceil(radiusKm / 5) * 5;

  // Per-hash ambiguity matters less when many closest receivers all resolve to
  // the same place — unanimous geographic agreement IS the disambiguation.
  const ambigPenalty = 0.25 * ambigShare * (1 - dominance);
  let confidence = 0.25 + 0.45 * dominance + Math.min(0.15, 0.03 * inlierVotes) - ambigPenalty;
  // Long chains (no close observer) compound per-hop error — temper confidence by
  // how close the nearest receiver actually was. A 2-hop reception is trusted; a
  // source only heard 5–7 hops out is honestly less certain even if chains agree.
  const chainReliability = Math.max(0.4, 1 - Math.max(0, minHop - 2) * 0.1);
  confidence = Math.max(0, Math.min(1, confidence * chainReliability));

  const region = nearestRegion(lat, lon, cfg.regionSnapKm);
  const level: ConfidenceLevel = levelFor(confidence, cfg.originMinObservers, cfg);

  const reasons: string[] = [
    `${inlierVotes} relay paths from the closest receivers agree on the source repeater`,
    `walked each path back from its closest observer (${minHop} hop${minHop === 1 ? '' : 's'}) along confirmed RF-link adjacency`,
  ];
  if (minHop > cfg.originNearHopMax) {
    reasons.push(`nearest receiver was ${minHop} hops out — longer chain, lower certainty`);
  }
  if (dominance < 0.6) reasons.push('close receivers saw different first hops — area widened');
  if (ambigShare > 0.3) reasons.push('parts of the chain fell back to geography — treat as approximate');

  return {
    lat,
    lon,
    radiusKm,
    region,
    confidence,
    level,
    observerCount: inlierVotes,
    reasons,
  };
}
