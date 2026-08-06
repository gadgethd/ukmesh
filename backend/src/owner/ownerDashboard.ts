export type OwnerDashboardRow = {
  canonical_id: string;
  name: string | null;
  network: string;
  last_seen: string | null;
  advert_count: number | null;
  lat: number | null;
  lon: number | null;
  iata: string | null;
  role: number | null;
  members: string[];
};

export type OwnerDashboardNode = {
  // Keep an authorized source key here so existing owner live endpoints can
  // continue to authorize the selected entry without broadening access.
  node_id: string;
  canonicalId: string;
  members: string[];
  name: string | null;
  network: string;
  last_seen: string | null;
  advert_count: number;
  lat: number | null;
  lon: number | null;
  iata: string | null;
  role: number | null;
};

function normalizedKey(value: string): string {
  return value.trim().toUpperCase();
}

function normalizedName(value: string | null): string | null {
  const normalized = value?.trim().replace(/\s+/g, ' ').toUpperCase() ?? '';
  return normalized || null;
}

function timestampMs(value: string | null): number {
  if (!value) return Number.NEGATIVE_INFINITY;
  const parsed = Date.parse(value);
  return Number.isFinite(parsed) ? parsed : Number.NEGATIVE_INFINITY;
}

function compareRows(left: OwnerDashboardRow, right: OwnerDashboardRow): number {
  return timestampMs(right.last_seen) - timestampMs(left.last_seen)
    || Number(right.advert_count ?? 0) - Number(left.advert_count ?? 0)
    || normalizedKey(left.canonical_id).localeCompare(normalizedKey(right.canonical_id));
}

/**
 * Converts canonical identity rows into the owner-facing display list.
 *
 * The canonical view normally provides one row per identity. Exact same-name
 * rows are also combined here because owner authorization can retain older
 * keys that the evidence-based global merge intentionally leaves separate.
 * Their keys stay visible in `members`, while the freshest row supplies the
 * display position and metadata.
 */
export function groupOwnerNodes(
  rows: OwnerDashboardRow[],
  authorizedNodeIds: string[],
): OwnerDashboardNode[] {
  const authorized = authorizedNodeIds.map(normalizedKey);
  const groups = new Map<string, OwnerDashboardRow[]>();

  for (const row of rows) {
    const canonicalId = normalizedKey(row.canonical_id);
    const displayName = normalizedName(row.name);
    const groupKey = displayName ? `name:${displayName}` : `canonical:${canonicalId}`;
    const group = groups.get(groupKey) ?? [];
    group.push({
      ...row,
      canonical_id: canonicalId,
      members: Array.from(new Set([
        canonicalId,
        ...(Array.isArray(row.members) ? row.members : []).map(normalizedKey),
      ])).filter(Boolean),
    });
    groups.set(groupKey, group);
  }

  const result = Array.from(groups.values()).map((group) => {
    const ordered = [...group].sort(compareRows);
    const representative = ordered[0]!;
    const members = Array.from(new Set(group.flatMap((row) => row.members))).sort();
    const accessNodeId = authorized.find((nodeId) => members.includes(nodeId))
      ?? representative.canonical_id;

    return {
      node_id: accessNodeId,
      canonicalId: representative.canonical_id,
      members,
      name: representative.name,
      network: representative.network,
      last_seen: representative.last_seen,
      advert_count: group.reduce((sum, row) => sum + Number(row.advert_count ?? 0), 0),
      lat: representative.lat,
      lon: representative.lon,
      iata: representative.iata,
      role: representative.role,
    } satisfies OwnerDashboardNode;
  });

  return result.sort((left, right) => (
    timestampMs(right.last_seen) - timestampMs(left.last_seen)
      || (left.name ?? '').localeCompare(right.name ?? '')
      || left.canonicalId.localeCompare(right.canonicalId)
  ));
}
