import type pg from 'pg';

export type IdentityConfidence = 'high' | 'medium';

export type IdentityNode = {
  node_id: string;
  name: string | null;
  lat: number | null;
  lon: number | null;
  role: number | null;
  advert_count: number | null;
  last_seen: Date | string | null;
  network: string | null;
  hardware_model: string | null;
  firmware_version: string | null;
};

export type IdentityEvidence = {
  statusCount: number;
  statusFirstAt: number | null;
  statusLastAt: number | null;
  selfAdvertCount: number;
  selfFirstAt: number | null;
  selfLastAt: number | null;
  pairAdvertCount: number;
  pairPacketCount: number;
  pairFirstAt: number | null;
  pairLastAt: number | null;
};

export type IdentityPairAssessment = {
  nodeAId: string;
  nodeBId: string;
  accepted: boolean;
  confidence: IdentityConfidence | 'low';
  score: number;
  reason: string;
  reasons: string[];
  evidence: Record<string, unknown>;
};

export type IdentityAlias = {
  sourceNodeId: string;
  canonicalNodeId: string;
  confidence: IdentityConfidence;
  reason: string;
  evidence: Record<string, unknown>;
};

export type IdentityRefreshResult = {
  nodesConsidered: number;
  candidatePairs: number;
  acceptedPairs: number;
  aliasesWritten: number;
  ambiguousPairs: number;
  canonicalGroups: number;
};

const MAX_NAME_DISTANCE_METERS = 3_000;
const MAX_HANDOVER_GAP_MS = 14 * 24 * 60 * 60 * 1_000;
const MAX_EVIDENCE_AGE_DAYS = 365;

type PairKey = `${string}:${string}`;

type PairEvidence = {
  pairAdvertCount: number;
  pairPacketCount: number;
  pairFirstAt: number | null;
  pairLastAt: number | null;
};

export type IdentityEvidenceMaps = {
  statuses: Map<string, { count: number; firstAt: number | null; lastAt: number | null }>;
  selfAdverts: Map<string, { count: number; firstAt: number | null; lastAt: number | null }>;
  pairs: Map<PairKey, PairEvidence>;
};

type IdentityEdge = IdentityPairAssessment & {
  nodeA: IdentityNode;
  nodeB: IdentityNode;
};

type IdentityGroup = {
  members: IdentityNode[];
  edges: IdentityEdge[];
};

function timeMs(value: unknown): number | null {
  if (value == null) return null;
  const timestamp = value instanceof Date ? value.getTime() : Date.parse(String(value));
  return Number.isFinite(timestamp) ? timestamp : null;
}

function validPosition(node: Pick<IdentityNode, 'lat' | 'lon'>): boolean {
  return node.lat != null
    && node.lon != null
    && Number.isFinite(node.lat)
    && Number.isFinite(node.lon)
    && Math.abs(node.lat) <= 90
    && Math.abs(node.lon) <= 180
    && !(Math.abs(node.lat) < 1e-9 && Math.abs(node.lon) < 1e-9);
}

function distanceMeters(a: IdentityNode, b: IdentityNode): number | null {
  if (!validPosition(a) || !validPosition(b)) return null;
  const midLat = ((a.lat! + b.lat!) / 2) * Math.PI / 180;
  const dLat = (b.lat! - a.lat!) * 111_320;
  const dLon = (b.lon! - a.lon!) * 111_320 * Math.cos(midLat);
  return Math.sqrt(dLat ** 2 + dLon ** 2);
}

function normalizedNameTokens(value: string | null): string[] {
  if (!value) return [];
  return value
    .normalize('NFKC')
    .toUpperCase()
    .replace(/V\d+(?:\.\d+)?/g, ' ')
    .replace(/[^\p{L}\p{N}]+/gu, ' ')
    .trim()
    .split(/\s+/)
    .filter(Boolean)
    .map((token) => token === 'RPT' ? 'REPEATER' : token);
}

/**
 * A shared site name with a changed ordinal/directional suffix is commonly a
 * pair of real colocated repeaters, not a key rotation. Keep that distinction
 * as an explicit veto for evidence that is weaker than a near-identical key.
 * Version suffixes (V2/V3/V4) are removed by normalizedNameTokens and are not
 * treated as ordinals here.
 */
function nameVariantConflict(a: string | null, b: string | null): boolean {
  const tokens = (value: string | null) => {
    if (!value) return [] as string[];
    return value
      .normalize('NFKC')
      .toUpperCase()
      .replace(/V\d+(?:\.\d+)?/g, ' ')
      .replace(/[^\p{L}\p{N}]+/gu, ' ')
      .trim()
      .split(/\s+/)
      .filter(Boolean)
      .map((token) => token === 'RPT' ? 'REPEATER' : token);
  };
  const left = tokens(a);
  const right = tokens(b);
  if (left.length < 2 || right.length < 2 || left.length !== right.length) return false;
  return left.slice(0, -1).join('|') === right.slice(0, -1).join('|')
    && left[left.length - 1] !== right[right.length - 1];
}

function identityNameBucket(tokens: string[]): string {
  const significant = tokens.filter((token) => !['REPEATER', 'NODE'].includes(token));
  return (significant.length > 0 ? significant : tokens).slice(0, 2).join('|');
}

function nameSimilarity(a: string | null, b: string | null): number {
  const aTokens = normalizedNameTokens(a);
  const bTokens = normalizedNameTokens(b);
  if (aTokens.length === 0 || bTokens.length === 0) return 0;
  const aSet = new Set(aTokens);
  const bSet = new Set(bTokens);
  const intersection = [...aSet].filter((token) => bSet.has(token)).length;
  const union = new Set([...aSet, ...bSet]).size;
  return union > 0 ? intersection / union : 0;
}

function namesMatch(a: string | null, b: string | null): boolean {
  if (!a || !b || a.includes('🚫') || b.includes('🚫')) return false;
  const aTokens = normalizedNameTokens(a);
  const bTokens = normalizedNameTokens(b);
  if (aTokens.length === 0 || bTokens.length === 0) return false;
  if (identityNameBucket(aTokens) !== identityNameBucket(bTokens)) return false;
  const similarity = nameSimilarity(a, b);
  // A one-token name is deliberately permitted only when another independent
  // identity signal (key, position, or packet evidence) is also present.
  return similarity >= 0.6;
}

function publicNetworkClass(network: string | null): string | null {
  if (!network || network === 'test') return null;
  if (network === 'ukmesh' || network === 'northeast' || network === 'teesside') return 'public';
  return network;
}

function repeaterLike(node: IdentityNode): boolean {
  return node.role == null || node.role === 2;
}

function activePositionedRepeater(node: IdentityNode): boolean {
  return node.role === 2 && validPosition(node) && Number(node.advert_count ?? 0) > 0;
}

function roleCompatible(a: IdentityNode, b: IdentityNode): boolean {
  if (!repeaterLike(a) || !repeaterLike(b)) return false;
  return a.role == null || b.role == null || a.role === b.role;
}

function nearKeyAndPosition(a: IdentityNode, b: IdentityNode): boolean {
  const keyDistance = hexNibbleDistance(a.node_id, b.node_id);
  const distance = distanceMeters(a, b);
  return keyDistance <= 1 && distance != null && distance <= MAX_NAME_DISTANCE_METERS;
}

function uniqueActiveRepeaterId(nodes: IdentityNode[]): string | null {
  const representatives: IdentityNode[] = [];
  for (const candidate of nodes.filter(activePositionedRepeater)) {
    if (!representatives.some((representative) => nearKeyAndPosition(candidate, representative))) {
      representatives.push(candidate);
    }
  }
  return representatives.length === 1 ? representatives[0]!.node_id : null;
}

function hexNibbleDistance(a: string, b: string): number {
  const left = a.toUpperCase();
  const right = b.toUpperCase();
  if (!/^[0-9A-F]{64}$/.test(left) || !/^[0-9A-F]{64}$/.test(right)) return 64;
  let distance = 0;
  for (let index = 0; index < left.length; index += 1) {
    if (left[index] !== right[index]) distance += 1;
  }
  return distance;
}

function keyFor(a: string, b: string): PairKey {
  return a < b ? `${a}:${b}` : `${b}:${a}`;
}

function intervalGapMs(
  aStart: number | null,
  aEnd: number | null,
  bStart: number | null,
  bEnd: number | null,
): number | null {
  if (aStart == null || aEnd == null || bStart == null || bEnd == null) return null;
  if (aStart <= bEnd && bStart <= aEnd) return 0;
  return aStart > bEnd ? aStart - bEnd : bStart - aEnd;
}

function nodeActivityInterval(node: IdentityNode, evidence: IdentityEvidence): [number | null, number | null] {
  const nodeLast = timeMs(node.last_seen);
  const first = evidence.selfFirstAt ?? evidence.pairFirstAt ?? timeMs(node.last_seen);
  const last = Math.max(
    evidence.selfLastAt ?? 0,
    evidence.pairLastAt ?? 0,
    nodeLast ?? 0,
  ) || null;
  return [first, last];
}

function evidenceRecord(
  a: IdentityNode,
  b: IdentityNode,
  evidenceA: IdentityEvidence,
  evidenceB: IdentityEvidence,
  pair: PairEvidence,
  details: Record<string, unknown>,
): Record<string, unknown> {
  return {
    node_a: a.node_id,
    node_b: b.node_id,
    name_a: a.name,
    name_b: b.name,
    name_similarity: nameSimilarity(a.name, b.name),
    key_nibble_distance: hexNibbleDistance(a.node_id, b.node_id),
    distance_m: distanceMeters(a, b),
    status_a: evidenceA,
    status_b: evidenceB,
    packet_pair: pair,
    ...details,
  };
}

/**
 * Explain and score one possible identity edge. This is intentionally pure so
 * the merge rule can be tested without a database.
 */
export function assessIdentityPair(
  a: IdentityNode,
  b: IdentityNode,
  evidenceA: IdentityEvidence,
  evidenceB: IdentityEvidence,
  pair: PairEvidence = {
    pairAdvertCount: 0,
    pairPacketCount: 0,
    pairFirstAt: null,
    pairLastAt: null,
  },
  options: { uniqueActiveRepeaterId?: string | null } = {},
): IdentityPairAssessment {
  const reasons: string[] = [];
  const keyDistance = hexNibbleDistance(a.node_id, b.node_id);
  const distance = distanceMeters(a, b);
  const nameMatch = namesMatch(a.name, b.name);
  const samePublicNetwork = publicNetworkClass(a.network) !== null
    && publicNetworkClass(a.network) === publicNetworkClass(b.network);
  const roleMatch = roleCompatible(a, b);
  const statusA = evidenceA.statusCount > 0;
  const statusB = evidenceB.statusCount > 0;
  const directAdvertPair = pair.pairAdvertCount >= 3 && pair.pairPacketCount >= 3;
  const selfA = evidenceA.selfAdvertCount >= 3;
  const selfB = evidenceB.selfAdvertCount >= 3;
  const [aStart, aEnd] = nodeActivityInterval(a, evidenceA);
  const [bStart, bEnd] = nodeActivityInterval(b, evidenceB);
  const activityGap = intervalGapMs(aStart, aEnd, bStart, bEnd);
  const handover = selfA
    && selfB
    && activityGap != null
    && activityGap <= MAX_HANDOVER_GAP_MS
    && ((aEnd != null && bStart != null && aEnd < bStart)
      || (bEnd != null && aStart != null && bEnd < aStart));
  const variantConflict = nameVariantConflict(a.name, b.name);
  const metadataOnlyA = a.role == null && !validPosition(a) && Number(a.advert_count ?? 0) === 0;
  const metadataOnlyB = b.role == null && !validPosition(b) && Number(b.advert_count ?? 0) === 0;
  if (!samePublicNetwork) reasons.push('different-network-family');
  if (!roleMatch) reasons.push('incompatible-role');
  if (!nameMatch) reasons.push('name-not-similar');
  if (keyDistance <= 1) reasons.push('one-nibble-key-rotation');
  if (distance != null && distance <= MAX_NAME_DISTANCE_METERS) reasons.push('same-position');
  if (directAdvertPair) reasons.push('observer-advert-pair');
  if (handover) reasons.push('temporal-self-advert-handover');
  if (variantConflict) reasons.push('ordinal-or-direction-variant');
  if (statusA && activePositionedRepeater(b)) reasons.push('status-to-positioned-repeater');
  if (statusB && activePositionedRepeater(a)) reasons.push('status-to-positioned-repeater');

  // A key rotation with matching advert self-reception, or direct observer →
  // advert observations, is stronger than name similarity alone. A one-nibble
  // key match additionally requires the same physical position.
  const nearKeyAndPositionMatch = nearKeyAndPosition(a, b);
  const acceptedHigh = samePublicNetwork
    && roleMatch
    && nameMatch
    && (
      nearKeyAndPositionMatch
      || (directAdvertPair
        && !variantConflict
        && (metadataOnlyA || metadataOnlyB)
        && (statusA || statusB)
        && (activePositionedRepeater(a) || activePositionedRepeater(b)))
      || (handover
        && !variantConflict
        && (statusA || statusB)
        && (activePositionedRepeater(a) || activePositionedRepeater(b)))
    );

  // A metadata-only, stale MQTT status row may be folded into a unique active
  // repeater with the same identity name. The caller supplies that uniqueness;
  // generic names with multiple positioned repeaters never reach this branch.
  const uniqueActivePair = options.uniqueActiveRepeaterId != null
    && (metadataOnlyA || metadataOnlyB)
    && (activePositionedRepeater(a) || activePositionedRepeater(b));
  const acceptedMedium = samePublicNetwork
    && roleMatch
    && nameMatch
    && !variantConflict
    && uniqueActivePair
    && (statusA || statusB)
    && options.uniqueActiveRepeaterId === (activePositionedRepeater(a) ? a.node_id : b.node_id);

  const accepted = acceptedHigh || acceptedMedium;
  const confidence: IdentityPairAssessment['confidence'] = acceptedHigh
    ? 'high'
    : acceptedMedium
      ? 'medium'
      : 'low';
  const score = [
    nameMatch,
    samePublicNetwork,
    roleMatch,
    keyDistance <= 1,
    distance != null && distance <= MAX_NAME_DISTANCE_METERS,
    directAdvertPair,
    handover,
    statusA || statusB,
  ].filter(Boolean).length;

  return {
    nodeAId: a.node_id,
    nodeBId: b.node_id,
    accepted,
    confidence,
    score,
    reason: accepted ? reasons.join('+') : (reasons.length > 0 ? reasons.join('+') : 'insufficient-evidence'),
    reasons,
    evidence: evidenceRecord(a, b, evidenceA, evidenceB, pair, {
      activity_gap_ms: activityGap,
      direct_observer_advert_pair: directAdvertPair,
      temporal_handover: handover,
      name_variant_conflict: variantConflict,
      metadata_only_alias: acceptedMedium,
    }),
  };
}

function findRoot(parent: Map<string, string>, nodeId: string): string {
  let root = nodeId;
  while (parent.get(root) !== root) root = parent.get(root)!;
  let current = nodeId;
  while (parent.get(current) !== current) {
    const next = parent.get(current)!;
    parent.set(current, root);
    current = next;
  }
  return root;
}

function canJoinGroups(
  a: IdentityGroup,
  b: IdentityGroup,
): boolean {
  const positioned = [...a.members, ...b.members].filter(validPosition);
  for (let left = 0; left < positioned.length; left += 1) {
    for (let right = left + 1; right < positioned.length; right += 1) {
      const distance = distanceMeters(positioned[left]!, positioned[right]!);
      if (distance != null && distance > MAX_NAME_DISTANCE_METERS) return false;
    }
  }
  return true;
}

function chooseCanonicalNode(nodes: IdentityNode[]): IdentityNode {
  return [...nodes].sort((a, b) => {
    const activeDiff = Number(activePositionedRepeater(a)) - Number(activePositionedRepeater(b));
    if (activeDiff !== 0) return -activeDiff;
    const advertDiff = Number(b.advert_count ?? 0) - Number(a.advert_count ?? 0);
    if (advertDiff !== 0) return advertDiff;
    const lastDiff = (timeMs(b.last_seen) ?? 0) - (timeMs(a.last_seen) ?? 0);
    if (lastDiff !== 0) return lastDiff;
    return a.node_id.localeCompare(b.node_id);
  })[0]!;
}

export type IdentityGroupingResult = {
  aliases: IdentityAlias[];
  assessments: IdentityPairAssessment[];
  groups: IdentityGroup[];
};

/** Build accepted components from the candidate evidence graph. */
export function buildIdentityGroups(
  nodes: IdentityNode[],
  evidence: IdentityEvidenceMaps,
): IdentityGroupingResult {
  const byBucket = new Map<string, IdentityNode[]>();
  for (const node of nodes) {
    if (publicNetworkClass(node.network) == null || !repeaterLike(node)) continue;
    const tokens = normalizedNameTokens(node.name);
    if (tokens.length === 0) continue;
    const bucket = `${publicNetworkClass(node.network)}:${identityNameBucket(tokens)}`;
    const values = byBucket.get(bucket) ?? [];
    values.push(node);
    byBucket.set(bucket, values);
  }

  const assessments: IdentityPairAssessment[] = [];
  const edges: IdentityEdge[] = [];
  for (const bucketNodes of byBucket.values()) {
    for (let left = 0; left < bucketNodes.length; left += 1) {
      for (let right = left + 1; right < bucketNodes.length; right += 1) {
        const a = bucketNodes[left]!;
        const b = bucketNodes[right]!;
        const evidenceA = evidence.statuses.has(a.node_id)
          ? {
              statusCount: evidence.statuses.get(a.node_id)!.count,
              statusFirstAt: evidence.statuses.get(a.node_id)!.firstAt,
              statusLastAt: evidence.statuses.get(a.node_id)!.lastAt,
              selfAdvertCount: evidence.selfAdverts.get(a.node_id)?.count ?? 0,
              selfFirstAt: evidence.selfAdverts.get(a.node_id)?.firstAt ?? null,
              selfLastAt: evidence.selfAdverts.get(a.node_id)?.lastAt ?? null,
              pairAdvertCount: 0,
              pairPacketCount: 0,
              pairFirstAt: null,
              pairLastAt: null,
            }
          : {
              statusCount: 0,
              statusFirstAt: null,
              statusLastAt: null,
              selfAdvertCount: evidence.selfAdverts.get(a.node_id)?.count ?? 0,
              selfFirstAt: evidence.selfAdverts.get(a.node_id)?.firstAt ?? null,
              selfLastAt: evidence.selfAdverts.get(a.node_id)?.lastAt ?? null,
              pairAdvertCount: 0,
              pairPacketCount: 0,
              pairFirstAt: null,
              pairLastAt: null,
            };
        const evidenceB = evidence.statuses.has(b.node_id)
          ? {
              statusCount: evidence.statuses.get(b.node_id)!.count,
              statusFirstAt: evidence.statuses.get(b.node_id)!.firstAt,
              statusLastAt: evidence.statuses.get(b.node_id)!.lastAt,
              selfAdvertCount: evidence.selfAdverts.get(b.node_id)?.count ?? 0,
              selfFirstAt: evidence.selfAdverts.get(b.node_id)?.firstAt ?? null,
              selfLastAt: evidence.selfAdverts.get(b.node_id)?.lastAt ?? null,
              pairAdvertCount: 0,
              pairPacketCount: 0,
              pairFirstAt: null,
              pairLastAt: null,
            }
          : {
              statusCount: 0,
              statusFirstAt: null,
              statusLastAt: null,
              selfAdvertCount: evidence.selfAdverts.get(b.node_id)?.count ?? 0,
              selfFirstAt: evidence.selfAdverts.get(b.node_id)?.firstAt ?? null,
              selfLastAt: evidence.selfAdverts.get(b.node_id)?.lastAt ?? null,
              pairAdvertCount: 0,
              pairPacketCount: 0,
              pairFirstAt: null,
              pairLastAt: null,
            };
        const pair = evidence.pairs.get(keyFor(a.node_id, b.node_id)) ?? {
          pairAdvertCount: 0,
          pairPacketCount: 0,
          pairFirstAt: null,
          pairLastAt: null,
        };
        const activeId = uniqueActiveRepeaterId(bucketNodes);
        const assessment = assessIdentityPair(a, b, evidenceA, evidenceB, pair, {
          uniqueActiveRepeaterId: activeId,
        });
        assessments.push(assessment);
        if (assessment.accepted) edges.push({ ...assessment, nodeA: a, nodeB: b });
      }
    }
  }

  const parent = new Map(nodes.map((node) => [node.node_id, node.node_id]));
  const groups = new Map<string, IdentityGroup>();
  for (const node of nodes) groups.set(node.node_id, { members: [node], edges: [] });

  const sortedEdges = [...edges].sort((a, b) => {
    const confidenceDiff = Number(b.confidence === 'high') - Number(a.confidence === 'high');
    return confidenceDiff || b.score - a.score || a.nodeAId.localeCompare(b.nodeAId);
  });
  for (const edge of sortedEdges) {
    const rootA = findRoot(parent, edge.nodeAId);
    const rootB = findRoot(parent, edge.nodeBId);
    if (rootA === rootB) continue;
    const groupA = groups.get(rootA)!;
    const groupB = groups.get(rootB)!;
    if (!canJoinGroups(groupA, groupB)) continue;
    parent.set(rootB, rootA);
    groupA.members.push(...groupB.members);
    groupA.edges.push(...groupB.edges, edge);
    groups.delete(rootB);
  }

  const aliases: IdentityAlias[] = [];
  const acceptedPairKeys = new Set<string>();
  for (const group of groups.values()) {
    if (group.members.length < 2) continue;
    const canonical = chooseCanonicalNode(group.members);
    const groupConfidence: IdentityConfidence = group.edges.some((edge) => edge.confidence === 'high')
      ? 'high'
      : 'medium';
    const groupEvidence = {
      canonical_node_id: canonical.node_id,
      members: group.members.map((member) => member.node_id).sort(),
      edges: group.edges.map((edge) => edge.evidence),
    };
    for (const edge of group.edges) acceptedPairKeys.add(keyFor(edge.nodeAId, edge.nodeBId));
    for (const member of group.members) {
      if (member.node_id === canonical.node_id) continue;
      const memberEdges = group.edges.filter((edge) => (
        edge.nodeAId === member.node_id || edge.nodeBId === member.node_id
      ));
      const memberConfidence: IdentityConfidence = memberEdges.some((edge) => edge.confidence === 'high')
        ? 'high'
        : 'medium';
      aliases.push({
        sourceNodeId: member.node_id,
        canonicalNodeId: canonical.node_id,
        confidence: memberEdges.length > 0 ? memberConfidence : groupConfidence,
        reason: (memberEdges.length > 0 ? memberEdges : group.edges)
          .map((edge) => edge.reason)
          .join(';'),
        evidence: {
          ...groupEvidence,
          supporting_edges: (memberEdges.length > 0 ? memberEdges : group.edges)
            .map((edge) => edge.evidence),
        },
      });
    }
  }

  // Mark rejected/uncertain same-name candidates as ambiguous for operator
  // review, but never make an alias from name similarity alone.
  const finalAssessments = assessments.map((assessment) => ({
    ...assessment,
    accepted: acceptedPairKeys.has(keyFor(assessment.nodeAId, assessment.nodeBId)),
    confidence: acceptedPairKeys.has(keyFor(assessment.nodeAId, assessment.nodeBId))
      ? assessment.confidence
      : 'low' as const,
  }));
  return {
    aliases,
    assessments: finalAssessments,
    groups: [...groups.values()].filter((group) => group.members.length > 1),
  };
}

type DbClient = {
  query<T extends pg.QueryResultRow = pg.QueryResultRow>(text: string, values?: unknown[]): Promise<{ rows: T[]; rowCount: number | null }>;
  release(): void;
};

export async function refreshNodeIdentityAliases(
  pool: { connect(): Promise<DbClient> },
): Promise<IdentityRefreshResult> {
  const client = await pool.connect();
  const refreshStartedAt = new Date();
  try {
    await client.query('BEGIN');
    await client.query(`SELECT pg_advisory_xact_lock(hashtext('meshcore-node-identity-refresh'))`);

    const nodesResult = await client.query<IdentityNode>(
      `SELECT node_id, name, lat, lon, role, advert_count, last_seen,
              network, hardware_model, firmware_version
         FROM nodes
        WHERE network IS DISTINCT FROM 'test'`,
    );
    const nodes = nodesResult.rows.map((row) => ({
      ...row,
      node_id: row.node_id.toUpperCase(),
    }));
    const candidatePairs: Array<[string, string]> = [];
    const candidateIds = new Set<string>();
    const buckets = new Map<string, IdentityNode[]>();
    for (const node of nodes) {
      if (!repeaterLike(node) || !node.name || node.name.includes('🚫')) continue;
      const tokens = normalizedNameTokens(node.name);
      if (tokens.length === 0) continue;
      const bucket = `${publicNetworkClass(node.network)}:${identityNameBucket(tokens)}`;
      const bucketNodes = buckets.get(bucket) ?? [];
      bucketNodes.push(node);
      buckets.set(bucket, bucketNodes);
    }
    for (const bucketNodes of buckets.values()) {
      for (let left = 0; left < bucketNodes.length; left += 1) {
        for (let right = left + 1; right < bucketNodes.length; right += 1) {
          const a = bucketNodes[left]!;
          const b = bucketNodes[right]!;
          if (!namesMatch(a.name, b.name) || !roleCompatible(a, b)) continue;
          candidatePairs.push([a.node_id, b.node_id]);
          candidateIds.add(a.node_id);
          candidateIds.add(b.node_id);
        }
      }
    }

    const ids = [...candidateIds];
    const statuses = new Map<string, { count: number; firstAt: number | null; lastAt: number | null }>();
    const selfAdverts = new Map<string, { count: number; firstAt: number | null; lastAt: number | null }>();
    const pairs = new Map<PairKey, PairEvidence>();
    if (ids.length > 0) {
      const statusResult = await client.query<{
        node_id: string;
        count: string;
        first_at: Date | string | null;
        last_at: Date | string | null;
      }>(
        `SELECT node_id, COUNT(*)::text AS count,
                MIN(time) AS first_at, MAX(time) AS last_at
           FROM node_status_samples
          WHERE node_id = ANY($1::text[])
          GROUP BY node_id`,
        [ids],
      );
      for (const row of statusResult.rows) {
        statuses.set(row.node_id, {
          count: Number(row.count ?? 0),
          firstAt: timeMs(row.first_at),
          lastAt: timeMs(row.last_at),
        });
      }

      const selfResult = await client.query<{
        node_id: string;
        count: string;
        first_at: Date | string | null;
        last_at: Date | string | null;
      }>(
        `SELECT rx_node_id AS node_id, COUNT(*)::text AS count,
                MIN(time) AS first_at, MAX(time) AS last_at
           FROM packets
          WHERE rx_node_id = src_node_id
            AND packet_type = 4
            AND rx_node_id = ANY($1::text[])
            AND time > NOW() - ($2::text || ' days')::interval
          GROUP BY rx_node_id`,
        [ids, String(MAX_EVIDENCE_AGE_DAYS)],
      );
      for (const row of selfResult.rows) {
        selfAdverts.set(row.node_id, {
          count: Number(row.count ?? 0),
          firstAt: timeMs(row.first_at),
          lastAt: timeMs(row.last_at),
        });
      }

      const pairResult = await client.query<{
        rx_node_id: string;
        src_node_id: string;
        packet_count: string;
        advert_count: string;
        first_at: Date | string | null;
        last_at: Date | string | null;
      }>(
        `SELECT rx_node_id, src_node_id,
                COUNT(*)::text AS packet_count,
                COUNT(*) FILTER (WHERE packet_type = 4)::text AS advert_count,
                MIN(time) AS first_at, MAX(time) AS last_at
           FROM packets
          WHERE rx_node_id = ANY($1::text[])
            AND src_node_id = ANY($1::text[])
            AND rx_node_id <> src_node_id
            AND time > NOW() - ($2::text || ' days')::interval
          GROUP BY rx_node_id, src_node_id`,
        [ids, String(MAX_EVIDENCE_AGE_DAYS)],
      );
      for (const row of pairResult.rows) {
        const key = keyFor(row.rx_node_id, row.src_node_id);
        const current = pairs.get(key) ?? {
          pairAdvertCount: 0,
          pairPacketCount: 0,
          pairFirstAt: null,
          pairLastAt: null,
        };
        current.pairAdvertCount += Number(row.advert_count ?? 0);
        current.pairPacketCount += Number(row.packet_count ?? 0);
        const firstAt = timeMs(row.first_at);
        const lastAt = timeMs(row.last_at);
        current.pairFirstAt = current.pairFirstAt == null || (firstAt != null && firstAt < current.pairFirstAt)
          ? firstAt
          : current.pairFirstAt;
        current.pairLastAt = current.pairLastAt == null || (lastAt != null && lastAt > current.pairLastAt)
          ? lastAt
          : current.pairLastAt;
        pairs.set(key, current);
      }
    }

    const grouped = buildIdentityGroups(nodes, { statuses, selfAdverts, pairs });

    await client.query(`DELETE FROM node_identity_aliases WHERE source_kind = 'automatic'`);
    if (grouped.aliases.length > 0) {
      const params: unknown[] = [];
      const values = grouped.aliases.map((alias, index) => {
        const offset = index * 6;
        params.push(
          alias.sourceNodeId,
          alias.canonicalNodeId,
          alias.confidence,
          alias.reason,
          JSON.stringify(alias.evidence),
          refreshStartedAt,
        );
        return `($${offset + 1}, $${offset + 2}, $${offset + 3}, $${offset + 4}, $${offset + 5}::jsonb, 'automatic', $${offset + 6})`;
      }).join(', ');
      await client.query(
        `INSERT INTO node_identity_aliases
           (source_node_id, canonical_node_id, confidence, reason, evidence, source_kind, updated_at)
         VALUES ${values}
         ON CONFLICT (source_node_id) DO UPDATE SET
           canonical_node_id = EXCLUDED.canonical_node_id,
           confidence = EXCLUDED.confidence,
           reason = EXCLUDED.reason,
           evidence = EXCLUDED.evidence,
           updated_at = EXCLUDED.updated_at
         WHERE node_identity_aliases.source_kind = 'automatic'`,
        params,
      );
    }

    await client.query(
      `DELETE FROM node_identity_match_evidence
        WHERE source_kind = 'automatic' AND updated_at < $1`,
      [refreshStartedAt],
    );
    if (grouped.assessments.length > 0) {
      // Keep each statement well below PostgreSQL's bind-parameter limit. A
      // common-name bucket can legitimately produce thousands of ambiguous
      // pairs, all of which are retained as audit evidence.
      const rowsPerWrite = 250;
      for (let start = 0; start < grouped.assessments.length; start += rowsPerWrite) {
        const chunk = grouped.assessments.slice(start, start + rowsPerWrite);
        const params: unknown[] = [];
        const values = chunk.map((assessment, index) => {
          const [nodeAId, nodeBId] = assessment.nodeAId < assessment.nodeBId
            ? [assessment.nodeAId, assessment.nodeBId]
            : [assessment.nodeBId, assessment.nodeAId];
          const offset = index * 9;
          params.push(
            nodeAId,
            nodeBId,
            assessment.accepted ? 'accepted' : 'ambiguous',
            assessment.confidence,
            assessment.reason,
            JSON.stringify(assessment.evidence),
            'automatic',
            refreshStartedAt,
            assessment.score,
          );
          return `($${offset + 1}, $${offset + 2}, $${offset + 3}, $${offset + 4}, $${offset + 5}, $${offset + 6}::jsonb, $${offset + 7}, $${offset + 8}, $${offset + 9})`;
        }).join(', ');
        await client.query(
          `INSERT INTO node_identity_match_evidence
             (node_a_id, node_b_id, decision, confidence, reason, evidence, source_kind, updated_at, score)
           VALUES ${values}
           ON CONFLICT (node_a_id, node_b_id) DO UPDATE SET
             decision = EXCLUDED.decision,
             confidence = EXCLUDED.confidence,
             reason = EXCLUDED.reason,
             evidence = EXCLUDED.evidence,
             updated_at = EXCLUDED.updated_at,
             score = EXCLUDED.score
           WHERE node_identity_match_evidence.source_kind = 'automatic'`,
          params,
        );
      }
    }

    await client.query('COMMIT');
    return {
      nodesConsidered: nodes.length,
      candidatePairs: candidatePairs.length,
      acceptedPairs: grouped.assessments.filter((assessment) => assessment.accepted).length,
      aliasesWritten: grouped.aliases.length,
      ambiguousPairs: grouped.assessments.filter((assessment) => !assessment.accepted).length,
      canonicalGroups: grouped.groups.length,
    };
  } catch (error) {
    await client.query('ROLLBACK');
    throw error;
  } finally {
    client.release();
  }
}
