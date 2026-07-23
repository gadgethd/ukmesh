/**
 * LEGACY greedy lazy path resolver — kept ONLY for the accuracy harness so the
 * old (pre-Viterbi) behaviour can be measured against the new decoder on the
 * exact same gold packets in one process. Not wired into any route.
 *
 * This is a verbatim copy of the original lazyResolver.ts greedy algorithm
 * (per-position pickBest + propagation + post-validation) prior to the global
 * decoder rewrite. Do not extend it; see lazyResolver.ts for the live path.
 */
import type { LazyPath, LazyPathNode, LazyPathResult } from './lazyResolver.js';
import { expandResolverScope } from '../networks.js';

type QueryFn = <T extends Record<string, unknown> = Record<string, unknown>>(
  text: string,
  params?: unknown[],
) => Promise<{ rows: T[] }>;

const MAX_HOP_KM = 150;
// Mirror lazyResolver's diagnostics flag so the harness can ablate both sides
// equally and isolate the pure structural (greedy-vs-Viterbi) contribution.
const ABLATE_LEAKY_PRIORS = process.env['LAZY_ABLATE_PRIORS'] === '1';

function expectedHashHexLength(pathHashSizeBytes: number | null): number | null {
  if (pathHashSizeBytes !== 1 && pathHashSizeBytes !== 2 && pathHashSizeBytes !== 3) return null;
  return pathHashSizeBytes * 2;
}

function networkScope(network: string | null): string[] | null {
  return network == null ? null : expandResolverScope(network);
}

function distKm(a: { lat: number; lon: number }, b: { lat: number; lon: number }): number {
  const R = 6371;
  const dLat = (b.lat - a.lat) * (Math.PI / 180);
  const dLon = (b.lon - a.lon) * (Math.PI / 180);
  const sinLat = Math.sin(dLat / 2) ** 2;
  const sinLon = Math.sin(dLon / 2) ** 2;
  const cosA = Math.cos(a.lat * (Math.PI / 180));
  const cosB = Math.cos(b.lat * (Math.PI / 180));
  return 2 * R * Math.asin(Math.sqrt(sinLat + cosA * cosB * sinLon));
}

function minDistToSet(pt: { lat: number; lon: number }, anchors: Array<{ lat: number; lon: number }>): number {
  let min = Infinity;
  for (const a of anchors) min = Math.min(min, distKm(pt, a));
  return min;
}

type Bounds = { minLat: number; maxLat: number; minLon: number; maxLon: number };

function inBounds(pt: { lat: number; lon: number }, b: Bounds): boolean {
  return pt.lat >= b.minLat && pt.lat <= b.maxLat && pt.lon >= b.minLon && pt.lon <= b.maxLon;
}

type ObsEntry = { rx_node_id: string; path_hashes: string[]; path_hash_size_bytes: number | null };
type ObsGroup = { canonicalHashes: string[]; members: ObsEntry[] };

function groupByPathHashes(entries: ObsEntry[]): ObsGroup[] {
  const sorted = [...entries]
    .filter((e) => e.path_hashes.length > 0)
    .sort((a, b) => b.path_hashes.length - a.path_hashes.length);
  const groups: ObsGroup[] = [];
  for (const entry of sorted) {
    const h = entry.path_hashes;
    const match = groups.find((g) => {
      const short = h.length <= g.canonicalHashes.length ? h : g.canonicalHashes;
      const long = h.length > g.canonicalHashes.length ? h : g.canonicalHashes;
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

export async function lazyResolvePathLegacy(
  packetHash: string,
  network: string | null,
  query: QueryFn,
): Promise<LazyPathResult | null> {
  const scopedNetworks = networkScope(network);

  const canonicalObs = await query<{
    rx_node_id: string; path_hashes: string[] | null; path_hash_size_bytes: number | null;
  }>(
    `SELECT DISTINCT ON (rx_node_id) rx_node_id, path_hashes, path_hash_size_bytes
       FROM packets
      WHERE packet_hash = $1 AND ($2::text[] IS NULL OR network = ANY($2)) AND rx_node_id IS NOT NULL
      ORDER BY rx_node_id, COALESCE(cardinality(path_hashes), 0) DESC, COALESCE(path_hash_size_bytes, 0) DESC, time ASC`,
    [packetHash, scopedNetworks],
  );
  if (canonicalObs.rows.length === 0) return null;

  const totalObs = canonicalObs.rows.length;
  const allObserverIds = canonicalObs.rows.map((r) => r.rx_node_id);

  const allHopRows = await query<{ rx_node_id: string; hop_count: number | null }>(
    `SELECT rx_node_id, hop_count FROM packets
      WHERE packet_hash = $1 AND ($2::text[] IS NULL OR network = ANY($2)) AND rx_node_id IS NOT NULL AND hop_count IS NOT NULL`,
    [packetHash, scopedNetworks],
  );

  const obsNodeResult = await query<{ node_id: string; lat: number | null; lon: number | null; name: string | null }>(
    `SELECT node_id, lat, lon, name FROM nodes
      WHERE node_id = ANY($1) AND lat IS NOT NULL AND lon IS NOT NULL AND lat != 0 AND lon != 0`,
    [allObserverIds],
  );
  const observerPositions = new Map<string, { lat: number; lon: number; name: string | null }>();
  for (const row of obsNodeResult.rows) {
    if (row.lat != null && row.lon != null) observerPositions.set(row.node_id, { lat: row.lat, lon: row.lon, name: row.name ?? null });
  }

  let observerBounds: Bounds | null = null;
  const obsCoords = [...observerPositions.values()];
  if (obsCoords.length > 0) {
    const lats = obsCoords.map((p) => p.lat);
    const lons = obsCoords.map((p) => p.lon);
    const padLat = MAX_HOP_KM / 111;
    const midLat = (Math.min(...lats) + Math.max(...lats)) / 2;
    const padLon = MAX_HOP_KM / (111 * Math.cos(midLat * (Math.PI / 180)));
    observerBounds = {
      minLat: Math.min(...lats) - padLat, maxLat: Math.max(...lats) + padLat,
      minLon: Math.min(...lons) - padLon, maxLon: Math.max(...lons) + padLon,
    };
  }

  const obsEntries: ObsEntry[] = canonicalObs.rows.map((row) => {
    const expectedHexLen = expectedHashHexLength(row.path_hash_size_bytes);
    const pathHashSizeBytes = expectedHexLen == null ? null : row.path_hash_size_bytes;
    let hashes = (row.path_hashes ?? []).map((h) => String(h).trim().toUpperCase()).filter((h) => h.length > 0);
    if (expectedHexLen != null) hashes = hashes.filter((h) => h.length === expectedHexLen);
    const rxId = row.rx_node_id.toUpperCase();
    if (hashes.length > 0) {
      const lastHash = hashes[hashes.length - 1]!;
      if (rxId.startsWith(lastHash)) hashes = hashes.slice(0, -1);
    }
    return { rx_node_id: row.rx_node_id, path_hashes: hashes, path_hash_size_bytes: pathHashSizeBytes };
  });

  const groups = groupByPathHashes(obsEntries);
  if (groups.length === 0) return null;

  const allUniqueHashes = [...new Set(groups.flatMap((g) => g.canonicalHashes))];
  const nodesByHash = new Map<string, Array<{ nodeId: string; name: string | null; lat: number; lon: number }>>();
  if (allUniqueHashes.length > 0) {
    const whereClauses = allUniqueHashes.map((_, i) => `upper(node_id) LIKE $${i + 2}`);
    const nodeResult = await query<{ node_id: string; name: string | null; lat: number | null; lon: number | null }>(
      `SELECT node_id, name, lat, lon FROM nodes
        WHERE ($1::text[] IS NULL OR network = ANY($1)) AND (${whereClauses.join(' OR ')})`,
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

  function pickBest(
    hash: string,
    anchors: Array<{ lat: number; lon: number }>,
    neighborIds: string[] = [],
    prevHash: string | null = null,
    resolvedNeighborIds: string[] = [],
  ): { nodeId: string; name: string | null; lat: number; lon: number; ambiguous: boolean } | null {
    const candidates = nodesByHash.get(hash) ?? [];
    if (candidates.length === 0) return null;
    type ScoredCandidate = {
      nodeId: string; name: string | null; lat: number; lon: number;
      dist: number; tier: number; priorCount: number; edgeScore: number; mlScore: number;
    };
    const hash2char = hash.slice(0, 2).toUpperCase();
    const scored: ScoredCandidate[] = [];
    for (const c of candidates) {
      const dist = anchors.length > 0 ? minDistToSet(c, anchors) : Infinity;
      if (anchors.length > 0 && dist > MAX_HOP_KM) continue;
      const mlScore = ABLATE_LEAKY_PRIORS ? 0 : (mlScores.get(`${hash2char}:${c.nodeId}`) ?? 0);
      const priorCount = getPrefixPriorCount(c.nodeId, hash, prevHash);
      const edgeScore = (!ABLATE_LEAKY_PRIORS && resolvedNeighborIds.length > 0)
        ? Math.max(...resolvedNeighborIds.map((nId) => getEdgePriorScore(c.nodeId, nId)), 0) : 0;
      const hasLinkEvidence = neighborIds.length > 0 && neighborIds.some((nId) => hasLink(c.nodeId, nId));
      let tier = 3;
      const bestRivalPriorCount = candidates
        .filter((r) => r.nodeId !== c.nodeId)
        .reduce((max, r) => Math.max(max, getPrefixPriorCount(r.nodeId, hash, prevHash)), 0);
      const mlObsCount = mlObsCounts.get(`${hash2char}:${c.nodeId}`) ?? 0;
      const mlDominant = mlScore >= 0.85 && (bestRivalPriorCount === 0 || mlObsCount >= bestRivalPriorCount / 5);
      if (mlDominant) tier = -1;
      else if (priorCount > 0) tier = 0;
      else if (edgeScore > 0) tier = 1;
      else if (hasLinkEvidence) tier = 2;
      scored.push({ nodeId: c.nodeId, name: c.name, lat: c.lat, lon: c.lon, dist, tier, priorCount, edgeScore, mlScore });
    }
    if (scored.length === 0) return null;
    scored.sort((a, b) =>
      a.tier - b.tier || b.priorCount - a.priorCount || b.edgeScore - a.edgeScore
      || (a.tier === -1 && b.tier === -1 ? b.mlScore - a.mlScore : 0) || a.dist - b.dist);
    const best = scored[0]!;
    if (anchors.length === 0) {
      if (candidates.length === 1) return { ...candidates[0]!, ambiguous: false };
      if (best.tier <= 1) return { nodeId: best.nodeId, name: best.name, lat: best.lat, lon: best.lon, ambiguous: false };
      if (best.tier === 2 && (scored[1] == null || scored[1].tier > 2)) {
        return { nodeId: best.nodeId, name: best.name, lat: best.lat, lon: best.lon, ambiguous: false };
      }
      return null;
    }
    if (best.tier === 3 && scored.length > 2) return null;
    const second = scored[1];
    const ambiguous = second != null && second.tier === best.tier && (
      best.tier === -1 ? (second.mlScore >= best.mlScore - 0.05)
        : best.tier >= 2 ? (second.dist - best.dist) < 20 : second.priorCount >= best.priorCount * 0.8);
    return { nodeId: best.nodeId, name: best.name, lat: best.lat, lon: best.lon, ambiguous };
  }

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

  const allCandidateNodeIds: string[] = [];
  for (const candidates of nodesByHash.values()) for (const c of candidates) allCandidateNodeIds.push(c.nodeId);
  for (const obsId of observerPositions.keys()) allCandidateNodeIds.push(obsId);
  const allUnique2charHashes = [...new Set(allUniqueHashes.map((h) => h.slice(0, 2)))];

  const [linkResult, prefixPriorRows, edgePriorRows, mlScoreRows] = await Promise.all([
    allCandidateNodeIds.length > 0
      ? query<{ node_a_id: string; node_b_id: string }>(
          `SELECT node_a_id, node_b_id FROM node_links WHERE (node_a_id = ANY($1) OR node_b_id = ANY($1)) AND observed_count >= 2`,
          [allCandidateNodeIds])
      : Promise.resolve({ rows: [] as { node_a_id: string; node_b_id: string }[] }),
    allUniqueHashes.length > 0
      ? query<{ prefix: string; prev_prefix: string | null; node_id: string; total_count: string }>(
          `SELECT prefix, prev_prefix, node_id, SUM(count)::text as total_count FROM path_prefix_priors
            WHERE ($1::text[] IS NULL OR network = ANY($1)) AND prefix = ANY($2) GROUP BY prefix, prev_prefix, node_id`,
          [scopedNetworks, allUniqueHashes])
      : Promise.resolve({ rows: [] as { prefix: string; prev_prefix: string | null; node_id: string; total_count: string }[] }),
    allCandidateNodeIds.length > 0
      ? query<{ from_node_id: string; to_node_id: string; best_score: string }>(
          `SELECT from_node_id, to_node_id, MAX(score)::text as best_score FROM path_edge_priors
            WHERE ($1::text[] IS NULL OR network = ANY($1)) AND (from_node_id = ANY($2) OR to_node_id = ANY($2)) GROUP BY from_node_id, to_node_id`,
          [scopedNetworks, allCandidateNodeIds])
      : Promise.resolve({ rows: [] as { from_node_id: string; to_node_id: string; best_score: string }[] }),
    allUnique2charHashes.length > 0
      ? query<{ hash_2char: string; node_id: string; score: string; observation_count: string }>(
          `SELECT hash_2char, node_id, score::text, observation_count::text FROM ml_path_prefix_scores
            WHERE ($1::text[] IS NULL OR network = ANY($1)) AND hash_2char = ANY($2) AND score >= 0.80`,
          [scopedNetworks, allUnique2charHashes])
      : Promise.resolve({ rows: [] as { hash_2char: string; node_id: string; score: string; observation_count: string }[] }),
  ]);

  const knownLinks = new Set<string>();
  for (const row of linkResult.rows) {
    const mn = row.node_a_id < row.node_b_id ? row.node_a_id : row.node_b_id;
    const mx = row.node_a_id < row.node_b_id ? row.node_b_id : row.node_a_id;
    knownLinks.add(`${mn}:${mx}`);
  }
  function hasLink(a: string, b: string): boolean {
    if (ABLATE_LEAKY_PRIORS) return false;
    const mn = a < b ? a : b; const mx = a < b ? b : a;
    return knownLinks.has(`${mn}:${mx}`);
  }
  const prefixPriors = new Map<string, Map<string, number>>();
  for (const row of prefixPriorRows.rows) {
    const key = `${row.prefix.toUpperCase()}|${(row.prev_prefix ?? '').toUpperCase()}`;
    if (!prefixPriors.has(key)) prefixPriors.set(key, new Map());
    const nodeCount = Number(row.total_count) || 0;
    const existing = prefixPriors.get(key)!.get(row.node_id) ?? 0;
    prefixPriors.get(key)!.set(row.node_id, existing + nodeCount);
  }
  function getPrefixPriorCount(nodeId: string, hash: string, prevHash: string | null): number {
    const key = `${hash.toUpperCase()}|${(prevHash ?? '').toUpperCase()}`;
    return prefixPriors.get(key)?.get(nodeId) ?? 0;
  }
  const edgePriors = new Map<string, number>();
  for (const row of edgePriorRows.rows) {
    const mn = row.from_node_id < row.to_node_id ? row.from_node_id : row.to_node_id;
    const mx = row.from_node_id < row.to_node_id ? row.to_node_id : row.from_node_id;
    const k = `${mn}:${mx}`;
    edgePriors.set(k, Math.max(edgePriors.get(k) ?? 0, Number(row.best_score) || 0));
  }
  function getEdgePriorScore(a: string, b: string): number {
    const mn = a < b ? a : b; const mx = a < b ? b : a;
    return edgePriors.get(`${mn}:${mx}`) ?? 0;
  }
  const mlScores = new Map<string, number>();
  const mlObsCounts = new Map<string, number>();
  for (const row of mlScoreRows.rows) {
    const key = `${row.hash_2char.toUpperCase()}:${row.node_id}`;
    mlScores.set(key, Number(row.score) || 0);
    mlObsCounts.set(key, Number(row.observation_count) || 0);
  }

  const paths: LazyPath[] = [];
  for (const group of groups) {
    const { canonicalHashes, members } = group;
    const maxHops = canonicalHashes.length;
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
    type ResolvedEntry = { hash: string; nodeId: string | null; name: string | null; lat: number | null; lon: number | null; ambiguous: boolean };
    const resolved = new Map<number, ResolvedEntry>();

    for (const { position, hash } of canonicalHashEntries) {
      const anchors = globalDirectAnchors.get(`${position}:${hash}`) ?? [];
      const neighborIds = anchors.map((a) => a.nodeId);
      const prevHash = position > 0 ? (canonicalHashes[position - 1] ?? null) : null;
      const result = pickBest(hash, anchors, neighborIds, prevHash, []);
      resolved.set(position, {
        hash, nodeId: result?.nodeId ?? null, name: result?.name ?? null,
        lat: result?.lat ?? null, lon: result?.lon ?? null,
        ambiguous: result?.ambiguous ?? (nodesByHash.get(hash)?.length ?? 0) > 1,
      });
    }

    let pass2Changed = true;
    while (pass2Changed) {
      pass2Changed = false;
      const orders = [canonicalHashEntries, [...canonicalHashEntries].reverse()];
      for (const order of orders) {
        for (const { position, hash } of order) {
          const current = resolved.get(position)!;
          if (current.nodeId !== null && !current.ambiguous) continue;
          const neighborAnchors: Array<{ lat: number; lon: number }> = [];
          const neighborIds: string[] = [];
          const prev = resolved.get(position - 1);
          if (prev?.lat != null && prev?.lon != null) neighborAnchors.push({ lat: prev.lat, lon: prev.lon });
          if (prev?.nodeId) neighborIds.push(prev.nodeId);
          const next = resolved.get(position + 1);
          if (next?.lat != null && next?.lon != null) neighborAnchors.push({ lat: next.lat, lon: next.lon });
          if (next?.nodeId) neighborIds.push(next.nodeId);
          const directAnchors = globalDirectAnchors.get(`${position}:${hash}`) ?? [];
          for (const a of directAnchors) neighborIds.push(a.nodeId);
          const allAnchors = [...directAnchors, ...neighborAnchors];
          const prevHash = position > 0 ? (canonicalHashes[position - 1] ?? null) : null;
          const resolvedNeighborIds: string[] = [];
          if (prev?.nodeId) resolvedNeighborIds.push(prev.nodeId);
          if (next?.nodeId) resolvedNeighborIds.push(next.nodeId);
          if (allAnchors.length === 0 && prevHash === null && resolvedNeighborIds.length === 0) continue;
          const result = pickBest(hash, allAnchors, neighborIds, prevHash, resolvedNeighborIds);
          if (result && current.nodeId === null) {
            resolved.set(position, { hash, nodeId: result.nodeId, name: result.name, lat: result.lat, lon: result.lon, ambiguous: result.ambiguous });
            pass2Changed = true;
          }
        }
      }
    }

    let changed = true;
    while (changed) {
      changed = false;
      for (const { position } of canonicalHashEntries) {
        const cur = resolved.get(position);
        if (!cur?.lat || !cur?.lon) continue;
        const prev = resolved.get(position - 1);
        const next = resolved.get(position + 1);
        const prevOk = !prev?.lat || distKm({ lat: cur.lat, lon: cur.lon }, { lat: prev.lat!, lon: prev.lon! }) <= MAX_HOP_KM;
        const nextOk = !next?.lat || distKm({ lat: cur.lat, lon: cur.lon }, { lat: next.lat!, lon: next.lon! }) <= MAX_HOP_KM;
        if (!prevOk || !nextOk) {
          resolved.set(position, { hash: cur.hash, nodeId: null, name: null, lat: null, lon: null, ambiguous: false });
          changed = true;
        }
      }
    }

    const canonicalPath: LazyPathNode[] = canonicalHashEntries.map(({ position, hash, appearances }) => {
      const r = resolved.get(position)!;
      return {
        position, hash, nodeId: r.nodeId, name: r.name, lat: r.lat, lon: r.lon,
        appearances, totalObservations: totalObs, ambiguous: r.ambiguous, isObserver: false,
      };
    });
    const obsAtPosition = new Map<number, ObsEntry[]>();
    for (const member of members) {
      const pos = member.path_hashes.length;
      if (!obsAtPosition.has(pos)) obsAtPosition.set(pos, []);
      obsAtPosition.get(pos)!.push(member);
    }
    for (const [obsPosition, obsMembers] of obsAtPosition) {
      for (const obs of obsMembers) {
        const obsPos = observerPositions.get(obs.rx_node_id);
        let validLat: number | null = null; let validLon: number | null = null;
        if (obsPos) {
          const prevRelay = resolved.get(obsPosition - 1);
          const distOk = !prevRelay?.lat || !prevRelay?.lon || distKm(obsPos, { lat: prevRelay.lat, lon: prevRelay.lon }) <= MAX_HOP_KM;
          if (distOk) { validLat = obsPos.lat; validLon = obsPos.lon; }
        }
        canonicalPath.push({
          position: obsPosition, hash: obs.rx_node_id, nodeId: obs.rx_node_id, name: obsPos?.name ?? null,
          lat: validLat, lon: validLon, appearances: 1, totalObservations: totalObs, ambiguous: false, isObserver: true,
        });
      }
    }
    canonicalPath.sort((a, b) => a.position - b.position || (a.isObserver ? 1 : -1));
    const coordinates: Array<[number, number]> = canonicalPath
      .filter((n) => n.lat != null && n.lon != null).map((n) => [n.lat!, n.lon!]);
    const matchedHops = canonicalPath.filter((n) => !n.isObserver && n.nodeId !== null && !n.ambiguous).length;
    paths.push({ canonicalPath, coordinates, matchedHops, totalHops: maxHops, observerIds: members.map((m) => m.rx_node_id) });
  }
  if (paths.length === 0) return null;
  return { packetHash, observerCount: totalObs, paths };
}
