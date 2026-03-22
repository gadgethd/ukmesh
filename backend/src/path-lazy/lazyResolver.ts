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
 * Key constraints:
 *
 * 1. OBSERVER BOUNDING BOX: All relay nodes must lie within the geographic
 *    region defined by the MQTT observer positions (+ MAX_HOP_KM padding).
 *
 * 2. DIRECT-RECEIVER ANCHORS: An observer with hop_count=N received directly
 *    from relay[N-1].  That relay must be within MAX_HOP_KM of that observer.
 *
 * 3. NEIGHBOR ANCHORS: For positions not covered by direct-receiver anchors,
 *    pass 2 uses already-resolved adjacent relays (and adjacent observer nodes)
 *    as geographic constraints.
 *
 * 4. POST-VALIDATION: Adjacent resolved relay nodes are checked for impossible
 *    hops (> MAX_HOP_KM). Outlier nodes are removed until stable.
 *
 * 5. OBSERVER NODES: Each observer with a known position is inserted into the
 *    path at position = path_hashes.length (i.e. right after the last relay
 *    it heard through).  Observer positions are ground-truth and are never
 *    removed by post-validation.
 */

type QueryFn = <T extends Record<string, unknown> = Record<string, unknown>>(
  text: string,
  params?: unknown[],
) => Promise<{ rows: T[] }>;

export type LazyPathNode = {
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

export type LazyPath = {
  canonicalPath: LazyPathNode[];
  coordinates: Array<[number, number]>;
  matchedHops: number;
  totalHops: number;
  observerIds: string[];
};

export type LazyPathResult = {
  packetHash: string;
  observerCount: number;
  paths: LazyPath[];
};

// Generous but realistic upper bound for a single LoRa hop.
const MAX_HOP_KM = 150;

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

export async function lazyResolvePath(
  packetHash: string,
  network: string | null,
  query: QueryFn,
): Promise<LazyPathResult | null> {

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
        AND ($2::text IS NULL OR network = $2)
        AND rx_node_id IS NOT NULL
      ORDER BY rx_node_id,
               COALESCE(cardinality(path_hashes), 0) DESC,
               time ASC`,
    [packetHash, network],
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
        AND ($2::text IS NULL OR network = $2)
        AND rx_node_id IS NOT NULL
        AND hop_count IS NOT NULL`,
    [packetHash, network],
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
    let hashes = (row.path_hashes ?? []).map((h) => h.toUpperCase());
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
      path_hash_size_bytes: row.path_hash_size_bytes,
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
        WHERE ($1::text IS NULL OR network = $1)
          AND (${whereClauses.join(' OR ')})`,
      [network, ...allUniqueHashes.map((h) => h + '%')],
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

  // ── 7. Geographic resolution helper ─────────────────────────────────────
  // neighborIds: node IDs of adjacent already-resolved nodes (prev + next relay
  // or observer). When provided, candidates with a confirmed link to any of
  // these are strongly preferred over candidates that are merely within range.
  function pickBest(
    hash: string,
    anchors: Array<{ lat: number; lon: number }>,
    neighborIds: string[] = [],
  ): { nodeId: string; name: string | null; lat: number; lon: number; ambiguous: boolean } | null {
    const candidates = nodesByHash.get(hash) ?? [];
    if (candidates.length === 0) return null;

    if (anchors.length === 0) {
      if (candidates.length === 1) return { ...candidates[0]!, ambiguous: false };
      // No distance anchor but we have link data — use it alone.
      if (neighborIds.length > 0) {
        const linked = candidates.filter((c) => neighborIds.some((nId) => hasLink(c.nodeId, nId)));
        if (linked.length === 1) return { ...linked[0]!, ambiguous: false };
      }
      return null;
    }

    const scored = candidates
      .map((c) => ({ ...c, dist: minDistToSet(c, anchors) }))
      .filter((c) => c.dist <= MAX_HOP_KM)
      .sort((a, b) => a.dist - b.dist);

    if (scored.length === 0) return null;

    // Prefer candidates that have a confirmed link to an adjacent resolved node.
    // This eliminates impossible hops where a geometrically-close node is chosen
    // that has never been observed communicating with the neighbour.
    if (neighborIds.length > 0) {
      const linked = scored.filter((c) => neighborIds.some((nId) => hasLink(c.nodeId, nId)));
      if (linked.length > 0) {
        const best = linked[0]!;
        const closeSecond = linked[1] != null && (linked[1].dist - best.dist) < 20;
        return { nodeId: best.nodeId, name: best.name, lat: best.lat, lon: best.lon, ambiguous: closeSecond };
      }
      // No link-confirmed candidate within range — fall back to distance-only.
    }

    const best = scored[0]!;
    const closeSecond = scored[1] != null && (scored[1].dist - best.dist) < 20;
    return { nodeId: best.nodeId, name: best.name, lat: best.lat, lon: best.lon, ambiguous: closeSecond };
  }

  // ── 8. Global cross-group direct anchor map ─────────────────────────────
  // Key: `${position}:${hash}`.  An observer with hop_count = P+1 received
  // directly from the relay at position P, so its position anchors that relay.
  // By keying on (position, hash) we can share anchors across groups that
  // share the same relay at a given position, dramatically improving resolution
  // for the common prefix hops.
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

  // ── 8.5 Fetch recorded links for candidate + observer nodes ─────────────
  // Used in pickBest to prefer candidates that have a confirmed radio link
  // to an adjacent resolved node over candidates that are merely within range.
  const allCandidateNodeIds: string[] = [];
  for (const candidates of nodesByHash.values()) {
    for (const c of candidates) allCandidateNodeIds.push(c.nodeId);
  }
  for (const obsId of observerPositions.keys()) allCandidateNodeIds.push(obsId);

  const knownLinks = new Set<string>(); // normalised "${min}:${max}"
  if (allCandidateNodeIds.length > 0) {
    const linkResult = await query<{ node_a_id: string; node_b_id: string }>(
      `SELECT node_a_id, node_b_id FROM node_links
        WHERE (node_a_id = ANY($1) OR node_b_id = ANY($1))
          AND observed_count >= 2`,
      [allCandidateNodeIds],
    );
    for (const row of linkResult.rows) {
      const mn = row.node_a_id < row.node_b_id ? row.node_a_id : row.node_b_id;
      const mx = row.node_a_id < row.node_b_id ? row.node_b_id : row.node_a_id;
      knownLinks.add(`${mn}:${mx}`);
    }
  }

  function hasLink(a: string, b: string): boolean {
    const mn = a < b ? a : b;
    const mx = a < b ? b : a;
    return knownLinks.has(`${mn}:${mx}`);
  }

  // ── 9. Resolve each group into a LazyPath ───────────────────────────────
  const paths: LazyPath[] = [];

  for (const group of groups) {
    const { canonicalHashes, members } = group;
    const maxHops = canonicalHashes.length;

    // Build positionScores from this group's members' path_hashes
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

    // Canonical hash entries for relay positions 0..maxHops-1
    const canonicalHashEntries: Array<{ position: number; hash: string; appearances: number }> = [];
    for (let i = 0; i < maxHops; i++) {
      const h = canonicalHashes[i];
      if (!h) continue;
      const scores = positionScores.get(i);
      const appearances = scores ? Math.ceil(scores.get(h) ?? 1) : 1;
      canonicalHashEntries.push({ position: i, hash: h, appearances });
    }

    type ResolvedEntry = {
      hash: string;
      nodeId: string | null;
      name: string | null;
      lat: number | null;
      lon: number | null;
      ambiguous: boolean;
    };

    const resolved = new Map<number, ResolvedEntry>();

    // Pass 1: resolve relay nodes using direct-receiver observer anchors.
    // Uses the global anchor map so observers from other groups that share
    // the same relay hash at the same position also contribute.
    for (const { position, hash } of canonicalHashEntries) {
      const anchors = globalDirectAnchors.get(`${position}:${hash}`) ?? [];
      // The observers that anchor this relay are its immediate neighbours —
      // pass their IDs so pickBest can prefer link-confirmed candidates.
      const neighborIds = anchors.map((a) => a.nodeId);
      const result = pickBest(hash, anchors, neighborIds);
      resolved.set(position, {
        hash,
        nodeId: result?.nodeId ?? null,
        name: result?.name ?? null,
        lat: result?.lat ?? null,
        lon: result?.lon ?? null,
        ambiguous: result?.ambiguous ?? (nodesByHash.get(hash)?.length ?? 0) > 1,
      });
    }

    // Pass 2: propagate resolution using neighboring resolved nodes.
    // Runs repeatedly (both forward and backward) until no new positions resolve.
    // This allows resolution to propagate through chains of unresolved hops, e.g.
    // if only the tail is anchored, backward propagation fills in earlier positions.
    let pass2Changed = true;
    while (pass2Changed) {
      pass2Changed = false;
      // Run forward then backward each iteration for bidirectional propagation
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

          // Also include the global direct anchor for this position/hash — it may
          // not have been enough on its own in pass 1 but combined with neighbors it helps.
          // Observer nodeIds from direct anchors are also valid neighbours.
          const directAnchors = globalDirectAnchors.get(`${position}:${hash}`) ?? [];
          for (const a of directAnchors) neighborIds.push(a.nodeId);
          const allAnchors = [...directAnchors, ...neighborAnchors];
          if (allAnchors.length === 0) continue;

          const result = pickBest(hash, allAnchors, neighborIds);
          if (result && current.nodeId === null) {
            // Only count as progress when a previously-unresolved position gets resolved.
            // Ambiguous→unambiguous transitions happen via post-validation, not here.
            resolved.set(position, {
              hash,
              nodeId: result.nodeId,
              name: result.name,
              lat: result.lat,
              lon: result.lon,
              ambiguous: result.ambiguous,
            });
            pass2Changed = true;
          }
        }
      }
    }

    // Post-validation: remove relay nodes creating impossible adjacent hops
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

    // ── Build canonicalPath: relay nodes + observer nodes ──────────────────
    const canonicalPath: LazyPathNode[] = canonicalHashEntries.map(({ position, hash, appearances }) => {
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
    // Multiple observers at the same position each get their own node row.
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

  return { packetHash, observerCount: totalObs, paths };
}
