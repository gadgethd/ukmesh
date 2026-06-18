/**
 * Lazy path resolver — called after propagation has settled.
 *
 * Uses path_hashes embedded in packets by relaying nodes, combined with
 * geographic anchoring from MQTT observer positions, to reconstruct the
 * relay chain.
 *
 * Observers (rx_node_id) are included as definitive known-position nodes
 * at the end of each path they belong to.  Observers with incompatible
 * path_hashes (no shared prefix) produce separate paths.
 *
 * Resolution is a global per-group Viterbi (max-product) decode over the
 * group's canonical hash chain — NOT greedy per-position selection. Each hop
 * position is a trellis column of candidate nodes (the nodes whose IDs start
 * with that hash, bounded to the observer region); the decoder finds the
 * highest-scoring *coherent chain* using:
 *
 *   Emission (node fit):      prefix priors, ML prefix scores, direct-receiver
 *                             observer-anchor proximity.
 *   Transition (hop fit):     hop-distance feasibility (infeasible = -Inf),
 *                             edge/transition/2-gram-motif priors, node_links.
 *
 * Forward/backward marginals from the same trellis drive the per-node
 * `ambiguous` flag (a position is ambiguous when an alternative node yields a
 * near-equal best chain). A synthetic "unresolved" candidate with a small
 * baseline score sits in every column so the decoder leaves a hop unresolved
 * rather than guess on geography alone.
 *
 * Key constraints:
 *
 * 1. OBSERVER BOUNDING BOX: All relay nodes must lie within the geographic
 *    region defined by the MQTT observer positions (+ MAX_HOP_KM padding).
 *
 * 2. DIRECT-RECEIVER ANCHORS: An observer with hop_count=N received directly
 *    from relay[N-1].  That relay must be within MAX_HOP_KM of that observer
 *    (enforced as a hard emission gate when an anchor exists for the position).
 *
 * 3. OBSERVER NODES: Each observer with a known position is inserted into the
 *    path at position = path_hashes.length (i.e. right after the last relay
 *    it heard through).  Observer positions are ground-truth.
 *
 * Scoring weights and prior key-formats live in ../path-shared/scoring.ts so
 * the lazy and beta resolvers cannot silently drift apart.
 */
import {
  MAX_HOP_KM,
  SCORE,
  ML_DOMINANT_THRESHOLD,
  DIST_DECAY_KM,
  NULL_BASELINE,
  AMBIG_DELTA,
  MAX_COL,
  transitionKey,
  motif2Key,
} from '../path-shared/scoring.js';
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

export type SpamPathNode = {
  position: number;
  hash: string;
  nodeId: string | null;
  name: string | null;
  lat: number | null;
  lon: number | null;
  appearances: number;
  totalObservations: number;
  ambiguous: boolean;
  isObserver: boolean;
};

export type SpamPath = {
  canonicalPath: SpamPathNode[];
  coordinates: Array<[number, number]>;
  matchedHops: number;
  totalHops: number;
  observerIds: string[];
};

export type SpamPathResult = {
  packetHash: string;
  observerCount: number;
  paths: SpamPath[];
};

// Diagnostics only: when set, the node-id-keyed + ML signals (transition / motif
// / edge / node_links / ML prefix scores) are forced to zero. Used by the
// accuracy harness to measure the leakage-free floor (structural + prefix-prior
// + geographic anchoring only). Never set in production.
const ABLATE_LEAKY_PRIORS = process.env['LAZY_ABLATE_PRIORS'] === '1';

function expectedHashHexLength(pathHashSizeBytes: number | null): number | null {
  if (pathHashSizeBytes !== 1 && pathHashSizeBytes !== 2 && pathHashSizeBytes !== 3) return null;
  return pathHashSizeBytes * 2;
}

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

function minDistToSet(
  pt: { lat: number; lon: number },
  anchors: Array<{ lat: number; lon: number }>,
): number {
  let min = Infinity;
  for (const a of anchors) min = Math.min(min, distKm(pt, a));
  return min;
}

type Bounds = { minLat: number; maxLat: number; minLon: number; maxLon: number };

function inBounds(pt: { lat: number; lon: number }, b: Bounds): boolean {
  return pt.lat >= b.minLat && pt.lat <= b.maxLat &&
         pt.lon >= b.minLon && pt.lon <= b.maxLon;
}

type ObsEntry = {
  rx_node_id: string;
  path_hashes: string[];          // normalized uppercase
  path_hash_size_bytes: number | null;
};

type ObsGroup = {
  canonicalHashes: string[];      // longest hash sequence in group
  members: ObsEntry[];
};

/**
 * Group observers whose path_hash sequences are prefix-compatible into
 * a single group.  Observers with no path_hashes are excluded.
 */
function groupByPathHashes(entries: ObsEntry[]): ObsGroup[] {
  // Longest sequences first so that groups start with the richest member.
  const sorted = [...entries]
    .filter((e) => e.path_hashes.length > 0)
    .sort((a, b) => b.path_hashes.length - a.path_hashes.length);

  const groups: ObsGroup[] = [];

  for (const entry of sorted) {
    const h = entry.path_hashes;
    const match = groups.find((g) => {
      const short = h.length <= g.canonicalHashes.length ? h : g.canonicalHashes;
      const long  = h.length >  g.canonicalHashes.length ? h : g.canonicalHashes;
      return short.every((x, i) => long[i] === x);
    });

    if (match) {
      match.members.push(entry);
      if (h.length > match.canonicalHashes.length) match.canonicalHashes = h;
    } else {
      groups.push({ canonicalHashes: h, members: [entry] });
    }
  }

  return groups;
}

export async function spamResolvePath(
  packetHash: string,
  network: string | null,
  query: QueryFn,
): Promise<SpamPathResult | null> {
  const scopedNetworks = networkScope(network);

  // ── 1. Canonical path-hash observations (one richest row per observer) ──
  const canonicalObs = await query<{
    rx_node_id: string;
    path_hashes: string[] | null;
    path_hash_size_bytes: number | null;
  }>(
    `SELECT DISTINCT ON (rx_node_id)
            rx_node_id, path_hashes, path_hash_size_bytes
       FROM packets
      WHERE packet_hash = $1
        AND ($2::text[] IS NULL OR network = ANY($2))
        AND rx_node_id IS NOT NULL
      ORDER BY rx_node_id,
               COALESCE(cardinality(path_hashes), 0) DESC,
               COALESCE(path_hash_size_bytes, 0) DESC,
               time ASC`,
    [packetHash, scopedNetworks],
  );

  if (canonicalObs.rows.length === 0) return null;

  const totalObs = canonicalObs.rows.length;
  const allObserverIds = canonicalObs.rows.map((r) => r.rx_node_id);

  // ── 2. All (rx_node_id, hop_count) rows — NOT deduplicated ──────────────
  const allHopRows = await query<{
    rx_node_id: string;
    hop_count: number | null;
  }>(
    `SELECT rx_node_id, hop_count
       FROM packets
      WHERE packet_hash = $1
        AND ($2::text[] IS NULL OR network = ANY($2))
        AND rx_node_id IS NOT NULL
        AND hop_count IS NOT NULL`,
    [packetHash, scopedNetworks],
  );

  // ── 3. Observer node positions + names ──────────────────────────────────
  const obsNodeResult = await query<{
    node_id: string;
    lat: number | null;
    lon: number | null;
    name: string | null;
  }>(
    `SELECT node_id, lat, lon, name FROM nodes
      WHERE node_id = ANY($1)
        AND lat IS NOT NULL AND lon IS NOT NULL
        AND lat != 0 AND lon != 0`,
    [allObserverIds],
  );

  const observerPositions = new Map<string, { lat: number; lon: number; name: string | null }>();
  for (const row of obsNodeResult.rows) {
    if (row.lat != null && row.lon != null) {
      observerPositions.set(row.node_id, { lat: row.lat, lon: row.lon, name: row.name ?? null });
    }
  }

  // ── 4. Bounding box from all observer positions ─────────────────────────
  let observerBounds: Bounds | null = null;
  const obsCoords = [...observerPositions.values()];
  if (obsCoords.length > 0) {
    const lats = obsCoords.map((p) => p.lat);
    const lons = obsCoords.map((p) => p.lon);
    const padLat = MAX_HOP_KM / 111;
    const midLat = (Math.min(...lats) + Math.max(...lats)) / 2;
    const padLon = MAX_HOP_KM / (111 * Math.cos(midLat * (Math.PI / 180)));
    observerBounds = {
      minLat: Math.min(...lats) - padLat,
      maxLat: Math.max(...lats) + padLat,
      minLon: Math.min(...lons) - padLon,
      maxLon: Math.max(...lons) + padLon,
    };
  }

  // ── 5. Group observers by prefix-compatible path_hashes ─────────────────
  const obsEntries: ObsEntry[] = canonicalObs.rows.map((row) => {
    const expectedHexLen = expectedHashHexLength(row.path_hash_size_bytes);
    const pathHashSizeBytes = expectedHexLen == null ? null : row.path_hash_size_bytes;
    let hashes = (row.path_hashes ?? [])
      .map((h) => String(h).trim().toUpperCase())
      .filter((h) => h.length > 0);
    if (expectedHexLen != null) {
      hashes = hashes.filter((h) => h.length === expectedHexLen);
    }
    // Trim the observer's own trailing hash (meshcore appends the receiver's
    // own prefix as the last path_hashes entry). Mirrors trimObserverTerminalHop
    // in resolver.ts.
    const rxId = row.rx_node_id.toUpperCase();
    if (hashes.length > 0) {
      const lastHash = hashes[hashes.length - 1]!;
      if (rxId.startsWith(lastHash)) {
        hashes = hashes.slice(0, -1);
      }
    }
    return {
      rx_node_id: row.rx_node_id,
      path_hashes: hashes,
      path_hash_size_bytes: pathHashSizeBytes,
    };
  });

  const groups = groupByPathHashes(obsEntries);
  if (groups.length === 0) return null;

  // ── 6. Batch node lookup for all canonical hashes across all groups ──────
  const allUniqueHashes = [...new Set(groups.flatMap((g) => g.canonicalHashes))];

  const nodesByHash = new Map<string, Array<{ nodeId: string; name: string | null; lat: number; lon: number }>>();

  if (allUniqueHashes.length > 0) {
    const whereClauses = allUniqueHashes.map((_, i) => `upper(node_id) LIKE $${i + 2}`);
    const nodeResult = await query<{
      node_id: string;
      name: string | null;
      lat: number | null;
      lon: number | null;
    }>(
      `SELECT node_id, name, lat, lon
         FROM nodes
        WHERE ($1::text[] IS NULL OR network = ANY($1))
          AND (${whereClauses.join(' OR ')})`,
      [scopedNetworks, ...allUniqueHashes.map((h) => h + '%')],
    );

    for (const node of nodeResult.rows) {
      if (node.lat == null || node.lon == null || node.lat === 0 || node.lon === 0) continue;
      if (observerBounds && !inBounds(node as { lat: number; lon: number }, observerBounds)) continue;
      const id = node.node_id.toUpperCase();
      for (const hash of allUniqueHashes) {
        if (id.startsWith(hash)) {
          if (!nodesByHash.has(hash)) nodesByHash.set(hash, []);
          nodesByHash.get(hash)!.push({ nodeId: node.node_id, name: node.name, lat: node.lat!, lon: node.lon! });
          break;
        }
      }
    }
  }

  // ── 7. Global cross-group direct anchor map ─────────────────────────────
  // Key: `${position}:${hash}`.  An observer with hop_count = P+1 received
  // directly from the relay at position P, so its position anchors that relay.
  // By keying on (position, hash) we share anchors across groups that use the
  // same relay at a given position, improving resolution of common prefix hops.
  const obsEntryByNodeId = new Map(obsEntries.map((e) => [e.rx_node_id, e]));
  const globalDirectAnchors = new Map<string, Array<{ lat: number; lon: number; nodeId: string }>>();

  for (const row of allHopRows.rows) {
    const hc = Number(row.hop_count);
    if (!Number.isFinite(hc) || hc < 1) continue;
    const pos = hc - 1;
    const obsPos = observerPositions.get(row.rx_node_id);
    if (!obsPos) continue;
    const entry = obsEntryByNodeId.get(row.rx_node_id);
    if (!entry || pos >= entry.path_hashes.length) continue;
    const hash = entry.path_hashes[pos]!;
    const key = `${pos}:${hash}`;
    if (!globalDirectAnchors.has(key)) globalDirectAnchors.set(key, []);
    const existing = globalDirectAnchors.get(key)!;
    if (!existing.some((e) => e.lat === obsPos.lat && e.lon === obsPos.lon)) {
      existing.push({ lat: obsPos.lat, lon: obsPos.lon, nodeId: row.rx_node_id });
    }
  }

  // ── 8. Fetch recorded links + path-learning priors ──────────────────────
  // Prior data comes from historical resolutions and tells us which node
  // typically appears at a hash position (prefix prior), and which node→node
  // hops are real (transition / edge / motif priors + node_links).
  const allCandidateNodeIds: string[] = [];
  for (const candidates of nodesByHash.values()) {
    for (const c of candidates) allCandidateNodeIds.push(c.nodeId);
  }
  for (const obsId of observerPositions.keys()) allCandidateNodeIds.push(obsId);

  // Derive 2-char (1-byte) prefixes for ML score lookup
  const allUnique2charHashes = [...new Set(allUniqueHashes.map((h) => h.slice(0, 2)))];

  const [linkResult, prefixPriorRows, edgePriorRows, mlScoreRows, transitionPriorRows, motifPriorRows, calibrationRows] =
    await Promise.all([
      allCandidateNodeIds.length > 0
        ? query<{ node_a_id: string; node_b_id: string }>(
            `SELECT node_a_id, node_b_id FROM node_links
              WHERE (node_a_id = ANY($1) OR node_b_id = ANY($1))
                AND observed_count >= 2`,
            [allCandidateNodeIds],
          )
        : Promise.resolve({ rows: [] as { node_a_id: string; node_b_id: string }[] }),
      allUniqueHashes.length > 0
        ? query<{ prefix: string; prev_prefix: string | null; node_id: string; max_prob: string }>(
            `SELECT prefix, prev_prefix, node_id, MAX(probability)::text as max_prob
               FROM path_prefix_priors
              WHERE ($1::text[] IS NULL OR network = ANY($1))
                AND prefix = ANY($2)
              GROUP BY prefix, prev_prefix, node_id`,
            [scopedNetworks, allUniqueHashes],
          )
        : Promise.resolve({ rows: [] as { prefix: string; prev_prefix: string | null; node_id: string; max_prob: string }[] }),
      allCandidateNodeIds.length > 0
        ? query<{ from_node_id: string; to_node_id: string; best_score: string }>(
            `SELECT from_node_id, to_node_id, MAX(score)::text as best_score
               FROM path_edge_priors
              WHERE ($1::text[] IS NULL OR network = ANY($1))
                AND (from_node_id = ANY($2) OR to_node_id = ANY($2))
              GROUP BY from_node_id, to_node_id`,
            [scopedNetworks, allCandidateNodeIds],
          )
        : Promise.resolve({ rows: [] as { from_node_id: string; to_node_id: string; best_score: string }[] }),
      allUnique2charHashes.length > 0
        ? query<{ hash_2char: string; node_id: string; score: string; observation_count: string }>(
            `SELECT hash_2char, node_id, score::text, observation_count::text
               FROM ml_path_prefix_scores
              WHERE ($1::text[] IS NULL OR network = ANY($1)) AND hash_2char = ANY($2) AND score >= 0.80`,
            [scopedNetworks, allUnique2charHashes],
          )
        : Promise.resolve({ rows: [] as { hash_2char: string; node_id: string; score: string; observation_count: string }[] }),
      allCandidateNodeIds.length > 0
        ? query<{ from_node_id: string; to_node_id: string; prob: string }>(
            `SELECT from_node_id, to_node_id, MAX(probability)::text as prob
               FROM path_transition_priors
              WHERE ($1::text[] IS NULL OR network = ANY($1))
                AND (from_node_id = ANY($2) OR to_node_id = ANY($2))
              GROUP BY from_node_id, to_node_id`,
            [scopedNetworks, allCandidateNodeIds],
          )
        : Promise.resolve({ rows: [] as { from_node_id: string; to_node_id: string; prob: string }[] }),
      allCandidateNodeIds.length > 0
        ? query<{ node_ids: string; prob: string }>(
            `SELECT node_ids, MAX(probability)::text as prob
               FROM path_motif_priors
              WHERE ($1::text[] IS NULL OR network = ANY($1))
                AND motif_len = 2
                AND split_part(node_ids, '>', 1) = ANY($2)
                AND split_part(node_ids, '>', 2) = ANY($2)
              GROUP BY node_ids`,
            [scopedNetworks, allCandidateNodeIds],
          )
        : Promise.resolve({ rows: [] as { node_ids: string; prob: string }[] }),
      query<{ network: string; confidence_scale: string; confidence_bias: string; recommended_threshold: string }>(
        `SELECT network, confidence_scale::text, confidence_bias::text, recommended_threshold::text
           FROM path_model_calibration
          WHERE ($1::text[] IS NULL OR network = ANY($1) OR network = 'all')`,
        [scopedNetworks],
      ),
    ]);

  // Build known-links set
  const knownLinks = new Set<string>();
  for (const row of linkResult.rows) {
    const mn = row.node_a_id < row.node_b_id ? row.node_a_id : row.node_b_id;
    const mx = row.node_a_id < row.node_b_id ? row.node_b_id : row.node_a_id;
    knownLinks.add(`${mn}:${mx}`);
  }

  function hasLink(a: string, b: string): boolean {
    if (ABLATE_LEAKY_PRIORS) return false;
    const mn = a < b ? a : b;
    const mx = a < b ? b : a;
    return knownLinks.has(`${mn}:${mx}`);
  }

  // Prefix-prior probability: "HASH|PREV_HASH" → Map<nodeId, probability>.
  // MAX(probability) aggregates across receiver_region (best-case evidence).
  const prefixProb = new Map<string, Map<string, number>>();
  for (const row of prefixPriorRows.rows) {
    const key = `${row.prefix.toUpperCase()}|${(row.prev_prefix ?? '').toUpperCase()}`;
    if (!prefixProb.has(key)) prefixProb.set(key, new Map());
    const p = Number(row.max_prob) || 0;
    const inner = prefixProb.get(key)!;
    inner.set(row.node_id, Math.max(inner.get(row.node_id) ?? 0, p));
  }

  function getPrefixProb(nodeId: string, hash: string, prevHash: string | null): number {
    const h = hash.toUpperCase();
    const withPrev = prefixProb.get(`${h}|${(prevHash ?? '').toUpperCase()}`)?.get(nodeId);
    if (withPrev != null) return withPrev;
    return prefixProb.get(`${h}|`)?.get(nodeId) ?? 0;
  }

  // Directed edge score: from (source-side) → to (receiver-side).
  const edgeDirected = new Map<string, number>();
  for (const row of edgePriorRows.rows) {
    const k = transitionKey(row.from_node_id, row.to_node_id);
    edgeDirected.set(k, Math.max(edgeDirected.get(k) ?? 0, Number(row.best_score) || 0));
  }
  const getEdgeDirected = (a: string, b: string): number =>
    ABLATE_LEAKY_PRIORS ? 0 : edgeDirected.get(transitionKey(a, b)) ?? 0;

  // Directed transition probability.
  const transitionProb = new Map<string, number>();
  for (const row of transitionPriorRows.rows) {
    const k = transitionKey(row.from_node_id, row.to_node_id);
    transitionProb.set(k, Math.max(transitionProb.get(k) ?? 0, Number(row.prob) || 0));
  }
  const getTransitionProb = (a: string, b: string): number =>
    ABLATE_LEAKY_PRIORS ? 0 : transitionProb.get(transitionKey(a, b)) ?? 0;

  // Directed 2-gram motif probability ("FROM>TO").
  const motif2Prob = new Map<string, number>();
  for (const row of motifPriorRows.rows) {
    motif2Prob.set(row.node_ids, Math.max(motif2Prob.get(row.node_ids) ?? 0, Number(row.prob) || 0));
  }
  const getMotif2 = (a: string, b: string): number =>
    ABLATE_LEAKY_PRIORS ? 0 : motif2Prob.get(motif2Key(a, b)) ?? 0;

  // ML prefix scores: `${hash_2char}:${node_id}` → score
  const mlScores = new Map<string, number>();
  for (const row of mlScoreRows.rows) {
    mlScores.set(`${row.hash_2char.toUpperCase()}:${row.node_id}`, Number(row.score) || 0);
  }
  const getMlScore = (hash: string, nodeId: string): number =>
    ABLATE_LEAKY_PRIORS ? 0 : mlScores.get(`${hash.slice(0, 2).toUpperCase()}:${nodeId}`) ?? 0;

  // Model calibration (prefer exact network, fall back to 'all').
  let confidenceScale = 1;
  let confidenceBias = 0;
  if (calibrationRows.rows.length > 0) {
    const preferred = scopedNetworks
      ? calibrationRows.rows.find((r) => scopedNetworks.includes(r.network))
      : undefined;
    const chosen = preferred ?? calibrationRows.rows.find((r) => r.network === 'all') ?? calibrationRows.rows[0]!;
    confidenceScale = Number(chosen.confidence_scale) || 1;
    confidenceBias = Number(chosen.confidence_bias) || 0;
  }

  // ── 9. Decode each group into a SpamPath via a per-group Viterbi ─────────
  type Cand = { nodeId: string | null; name: string | null; lat: number | null; lon: number | null };
  const NULL_CAND: Cand = { nodeId: null, name: null, lat: null, lon: null };

  type ResolvedEntry = {
    hash: string;
    nodeId: string | null;
    name: string | null;
    lat: number | null;
    lon: number | null;
    ambiguous: boolean;
  };

  // Emission: how well `cand` fits the hash on its own.
  function emission(hash: string, prevHash: string | null, cand: Cand, anchors: Array<{ lat: number; lon: number }>): number {
    if (cand.nodeId === null) return NULL_BASELINE;
    const positioned = cand.lat != null && cand.lon != null;
    // Hard direct-receiver gate: when an anchor exists for this position the
    // relay must be positioned and within one hop of the observer.
    if (anchors.length > 0) {
      if (!positioned) return -Infinity;
      if (minDistToSet({ lat: cand.lat!, lon: cand.lon! }, anchors) > MAX_HOP_KM) return -Infinity;
    }
    let e = 0;
    e += SCORE.prefix * getPrefixProb(cand.nodeId, hash, prevHash);
    const ml = getMlScore(hash, cand.nodeId);
    e += ml >= ML_DOMINANT_THRESHOLD ? Math.min(SCORE.mlDominantCap, ml * SCORE.mlDominantCap)
                                     : Math.min(SCORE.mlWeakCap, ml * SCORE.mlWeakCap);
    if (anchors.length > 0 && positioned) {
      const d = minDistToSet({ lat: cand.lat!, lon: cand.lon! }, anchors);
      e += SCORE.anchor * Math.max(0, 1 - d / MAX_HOP_KM);
    }
    return e;
  }

  // Transition: how well `cur` (receiver-side) follows `prev` (source-side).
  function transition(prev: Cand, cur: Cand): number {
    if (prev.nodeId === null || cur.nodeId === null) return 0; // unresolved wildcard, neutral
    let t = 0;
    if (prev.lat != null && prev.lon != null && cur.lat != null && cur.lon != null) {
      const d = distKm({ lat: prev.lat, lon: prev.lon }, { lat: cur.lat, lon: cur.lon });
      if (d > MAX_HOP_KM) return -Infinity; // physically impossible hop
      t += SCORE.dist * Math.exp(-d / DIST_DECAY_KM);
    }
    t += SCORE.edge * getEdgeDirected(prev.nodeId, cur.nodeId);
    t += SCORE.transition * getTransitionProb(prev.nodeId, cur.nodeId);
    t += SCORE.motif * getMotif2(prev.nodeId, cur.nodeId);
    if (hasLink(prev.nodeId, cur.nodeId)) t += SCORE.link;
    return t;
  }

  function buildColumn(pos: number, hash: string, prevHash: string | null): Cand[] {
    const raw = nodesByHash.get(hash) ?? [];
    let cands: Cand[] = raw.map((c) => ({ nodeId: c.nodeId, name: c.name, lat: c.lat, lon: c.lon }));
    if (cands.length > MAX_COL) {
      // Keep the strongest by standalone evidence when a 1-byte prefix is shared widely.
      const anchors = globalDirectAnchors.get(`${pos}:${hash}`) ?? [];
      cands = cands
        .map((c) => ({ c, s: emission(hash, prevHash, c, anchors) }))
        .sort((a, b) => b.s - a.s)
        .slice(0, MAX_COL)
        .map((x) => x.c);
    }
    cands.push(NULL_CAND);
    return cands;
  }

  // Run the max-product Viterbi over canonicalHashes[0..N-1] and return the
  // best coherent assignment per position (+ ambiguity from marginal gaps).
  function decodeChain(canonicalHashes: string[]): Map<number, ResolvedEntry> {
    const N = canonicalHashes.length;
    const resolved = new Map<number, ResolvedEntry>();
    if (N === 0) return resolved;

    const cols: Cand[][] = [];
    const emiss: number[][] = [];
    const anchorsByPos: Array<Array<{ lat: number; lon: number }>> = [];
    for (let pos = 0; pos < N; pos++) {
      const hash = canonicalHashes[pos]!;
      const prevHash = pos > 0 ? (canonicalHashes[pos - 1] ?? null) : null;
      const anchors = globalDirectAnchors.get(`${pos}:${hash}`) ?? [];
      anchorsByPos.push(anchors);
      const col = buildColumn(pos, hash, prevHash);
      cols.push(col);
      emiss.push(col.map((c) => emission(hash, prevHash, c, anchors)));
    }

    // Forward max-product: best score of a prefix ending at cols[pos][j].
    // The NULL_CAND in every column (neutral, finite transitions) guarantees a
    // connected path, so an infeasible real→real hop simply isn't taken — the
    // chain routes through the unresolved wildcard instead of teleporting.
    const fwd: number[][] = cols.map((c) => new Array(c.length).fill(-Infinity));
    for (let j = 0; j < cols[0]!.length; j++) fwd[0]![j] = emiss[0]![j]!;
    for (let pos = 1; pos < N; pos++) {
      for (let j = 0; j < cols[pos]!.length; j++) {
        const ej = emiss[pos]![j]!;
        if (!isFinite(ej)) continue;
        let best = -Infinity;
        for (let k = 0; k < cols[pos - 1]!.length; k++) {
          if (!isFinite(fwd[pos - 1]![k]!)) continue;
          const t = transition(cols[pos - 1]![k]!, cols[pos]![j]!);
          if (!isFinite(t)) continue;
          const total = fwd[pos - 1]![k]! + t;
          if (total > best) best = total;
        }
        if (isFinite(best)) fwd[pos]![j] = best + ej;
      }
    }

    // Backward max-product: best score of a suffix starting at cols[pos][j].
    const bwd: number[][] = cols.map((c) => new Array(c.length).fill(-Infinity));
    for (let j = 0; j < cols[N - 1]!.length; j++) bwd[N - 1]![j] = 0;
    for (let pos = N - 2; pos >= 0; pos--) {
      for (let j = 0; j < cols[pos]!.length; j++) {
        let best = -Infinity;
        for (let k = 0; k < cols[pos + 1]!.length; k++) {
          const ek = emiss[pos + 1]![k]!;
          if (!isFinite(ek) || !isFinite(bwd[pos + 1]![k]!)) continue;
          const t = transition(cols[pos]![j]!, cols[pos + 1]![k]!);
          if (!isFinite(t)) continue;
          const total = t + ek + bwd[pos + 1]![k]!;
          if (total > best) best = total;
        }
        bwd[pos]![j] = isFinite(best) ? best : 0; // suffix may end here
      }
    }

    for (let pos = 0; pos < N; pos++) {
      const hash = canonicalHashes[pos]!;
      // Best-path-through marginal for each candidate at this position.
      let bestJ = -1, bestM = -Infinity, secondNodeM = -Infinity;
      for (let j = 0; j < cols[pos]!.length; j++) {
        if (!isFinite(fwd[pos]![j]!) || !isFinite(bwd[pos]![j]!)) continue;
        const m = fwd[pos]![j]! + bwd[pos]![j]!;
        if (m > bestM) { secondNodeM = bestM; bestM = m; bestJ = j; }
        else if (m > secondNodeM) { secondNodeM = m; }
      }
      const best = bestJ >= 0 ? cols[pos]![bestJ]! : NULL_CAND;
      const ambiguous = best.nodeId !== null && isFinite(secondNodeM) && (bestM - secondNodeM) < AMBIG_DELTA;
      resolved.set(pos, {
        hash,
        nodeId: best.nodeId,
        name: best.name,
        lat: best.lat,
        lon: best.lon,
        ambiguous,
      });
    }

    return resolved;
  }

  const paths: SpamPath[] = [];

  for (const group of groups) {
    const { canonicalHashes, members } = group;
    const maxHops = canonicalHashes.length;

    // Per-hash appearance weights from this group's members (for diagnostics).
    const positionScores = new Map<number, Map<string, number>>();
    for (const member of members) {
      const weight = Math.max(1, member.path_hash_size_bytes ?? 1);
      for (let i = 0; i < member.path_hashes.length; i++) {
        const h = member.path_hashes[i]!;
        if (!positionScores.has(i)) positionScores.set(i, new Map());
        const scores = positionScores.get(i)!;
        scores.set(h, (scores.get(h) ?? 0) + weight);
      }
    }

    const canonicalHashEntries: Array<{ position: number; hash: string; appearances: number }> = [];
    for (let i = 0; i < maxHops; i++) {
      const h = canonicalHashes[i];
      if (!h) continue;
      const scores = positionScores.get(i);
      const appearances = scores ? Math.ceil(scores.get(h) ?? 1) : 1;
      canonicalHashEntries.push({ position: i, hash: h, appearances });
    }

    const resolved = decodeChain(canonicalHashes);

    // ── Build canonicalPath: relay nodes + observer nodes ──────────────────
    const canonicalPath: SpamPathNode[] = canonicalHashEntries.map(({ position, hash, appearances }) => {
      const r = resolved.get(position)!;
      return {
        position,
        hash,
        nodeId: r.nodeId,
        name: r.name,
        lat: r.lat,
        lon: r.lon,
        appearances,
        totalObservations: totalObs,
        ambiguous: r.ambiguous,
        isObserver: false,
      };
    });

    // Add observer nodes at position = path_hashes.length (after their last relay).
    const obsAtPosition = new Map<number, ObsEntry[]>();
    for (const member of members) {
      const pos = member.path_hashes.length;
      if (!obsAtPosition.has(pos)) obsAtPosition.set(pos, []);
      obsAtPosition.get(pos)!.push(member);
    }

    for (const [obsPosition, obsMembers] of obsAtPosition) {
      for (const obs of obsMembers) {
        const obsPos = observerPositions.get(obs.rx_node_id);
        // Validate distance from the relay immediately before this observer
        let validLat: number | null = null;
        let validLon: number | null = null;
        if (obsPos) {
          const prevRelay = resolved.get(obsPosition - 1);
          const distOk = !prevRelay?.lat || !prevRelay?.lon ||
            distKm(obsPos, { lat: prevRelay.lat, lon: prevRelay.lon }) <= MAX_HOP_KM;
          if (distOk) { validLat = obsPos.lat; validLon = obsPos.lon; }
        }
        canonicalPath.push({
          position: obsPosition,
          hash: obs.rx_node_id,
          nodeId: obs.rx_node_id,
          name: obsPos?.name ?? null,
          lat: validLat,
          lon: validLon,
          appearances: 1,
          totalObservations: totalObs,
          ambiguous: false,
          isObserver: true,
        });
      }
    }

    // Sort by position so relay and observer nodes are interleaved correctly
    canonicalPath.sort((a, b) => a.position - b.position || (a.isObserver ? 1 : -1));

    const coordinates: Array<[number, number]> = canonicalPath
      .filter((n) => n.lat != null && n.lon != null)
      .map((n) => [n.lat!, n.lon!]);

    const matchedHops = canonicalPath.filter((n) => !n.isObserver && n.nodeId !== null && !n.ambiguous).length;

    paths.push({
      canonicalPath,
      coordinates,
      matchedHops,
      totalHops: maxHops,
      observerIds: members.map((m) => m.rx_node_id),
    });
  }

  if (paths.length === 0) return null;

  // confidenceScale/confidenceBias are loaded for future per-path confidence
  // reporting; node selection does not depend on them. Referenced here to keep
  // them live until the confidence field is surfaced.
  void confidenceScale; void confidenceBias;

  return { packetHash, observerCount: totalObs, paths };
}

// ===========================================================================
// Cluster-level origin resolution for spam incidents.
//
// A spam flood is transmitted many times; each transmission is relayed across
// the mesh and heard by many observers, every reception carrying the ordered
// relay path. The FIRST repeater in a path is the source of truth: the spammer
// was in direct range of it. We resolve the path of each transmission (reusing
// the per-packet resolver above), take that first repeater, and find the
// geographic consensus across all of them — weighting transmissions heard by a
// CLOSE observer (low hop count) highest, since their paths are shortest and
// resolve most reliably.
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
        AND n.lat IS NOT NULL AND n.lon IS NOT NULL AND n.lat <> 0 AND n.lon <> 0`,
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
