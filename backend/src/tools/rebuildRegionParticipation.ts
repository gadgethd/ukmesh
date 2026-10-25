/**
 * Rebuild the region-scope participation tables.
 *
 * For every distinct transport-routed (route 0/3) packet in the window:
 *   - decode the region via the candidate-name transport-code probe
 *   - collect relay path elements (union across ALL observation rows)
 *   - resolve elements to nodes (unique prefix match only; drop ambiguous)
 * Then tally node -> region -> distinct packets, plus unscoped relay evidence
 * (route 1, multibyte, 7d) for the green/red classification, and refresh:
 *   region_scope_stats, region_scope_members, repeater_scope_status,
 *   region_scope_run
 *
 * Membership floor: a node counts as a region member only when it relayed at
 * least MEMBER_MIN_SHARE_PCT (regions/memberShare.ts, currently 10%) of that
 * region's scoped packets. Token relays (the odd packet out of thousands) are
 * excluded from member lists, member counts and per-node region maps so the
 * region shapes and lists reflect each region's actual relay pattern.
 *
 * Run from backend/ (dist build):  node dist/tools/rebuildRegionParticipation.js
 * or via:  npm run regions:rebuild
 *
 * Env:
 *   DATABASE_URL                 required (same as the backend)
 *   REGION_SCOPE_WINDOW_DAYS     scoped window in days (default 92, 7..365)
 *   MESHCORE_REGION_NAMES        extra probe names (comma-separated)
 *   MESHCORE_CHANNEL_SECRETS     extra channel secrets for the decoder keyStore
 *   REGION_SCOPE_DRY_RUN=1       compute + log but do not write
 *
 * All four tables are refreshed wholesale inside ONE transaction (delete +
 * insert), so readers never observe an empty or partial state. If the scoped
 * corpus is non-empty but nothing decodes, the tool aborts without writing
 * (a broken probe list must never wipe good data).
 */

import pg from 'pg';
import { MeshCoreDecoder } from '@michaelhart/meshcore-decoder';
import { decodePacketCompat } from '../mqtt/decodePacket.js';
import { MEMBER_MIN_SHARE_PCT, meetsMemberShare } from '../regions/memberShare.js';
import { regionProbeNamesWithEnv } from '../regions/regionNames.js';
import { decodeRegionForPacket, transportCode0Of } from '../regions/scopeDecode.js';

const { Pool } = pg;

// Documented MeshCore public channel material, not an application credential.
const DEFAULT_PUBLIC_CHANNEL_KEY = '8b3387e9c5cdea6ac9e5edbaa115cd72';
const DEFAULT_WINDOW_DAYS = 92;
const UNRESOLVED_ELEMENT_LOG_LIMIT = 12;

interface ScopedPacketRow {
  packet_hash: string;
  t0: Date;
  t1: Date;
  raw: string | null;
  elems: string[] | null;
}

interface NodeMetaRow {
  node_id: string;
  role: number | null;
}

interface UnscopedElementRow {
  e: string;
  c: string;
}

function boundedWindowDays(raw: string | undefined): number {
  const value = Number(raw ?? DEFAULT_WINDOW_DAYS);
  if (!Number.isFinite(value)) return DEFAULT_WINDOW_DAYS;
  return Math.max(7, Math.min(365, Math.trunc(value)));
}

function buildChannelSecrets(): string[] {
  const secrets = [DEFAULT_PUBLIC_CHANNEL_KEY];
  for (const entry of (process.env['MESHCORE_CHANNEL_SECRETS'] ?? '')
    .split(',').map((value) => value.trim()).filter(Boolean)) {
    const colon = entry.indexOf(':');
    secrets.push(colon > 0 ? entry.slice(colon + 1) : entry);
  }
  return secrets;
}

function resolveElement(prefixIndex: Map<string, string[]>, element: string): string | null {
  const normalized = element.trim().toUpperCase();
  if (!/^[0-9A-F]{2,6}$/.test(normalized)) return null;
  const candidates = prefixIndex.get(normalized);
  if (!candidates || candidates.length === 0) return null;
  const hits = candidates.filter((candidate) => candidate.startsWith(normalized));
  return hits.length === 1 ? hits[0] : null;
}

async function main(): Promise<void> {
  if (!process.env['DATABASE_URL']) throw new Error('DATABASE_URL is required');
  const windowDays = boundedWindowDays(process.env['REGION_SCOPE_WINDOW_DAYS']);
  const dryRun = process.env['REGION_SCOPE_DRY_RUN'] === '1';
  const probeNames = regionProbeNamesWithEnv(process.env['MESHCORE_REGION_NAMES']);
  const keyStore = MeshCoreDecoder.createKeyStore({ channelSecrets: buildChannelSecrets() });

  const pool = new Pool({
    connectionString: process.env['DATABASE_URL'],
    application_name: 'meshcore-regions-rebuild',
    max: 2,
    statement_timeout: 0,
    query_timeout: 0,
  });

  try {
    const startedAt = Date.now();
    console.log(`[regions] probe list: ${probeNames.length} names; window ${windowDays}d${dryRun ? ' (dry run)' : ''}`);

    const { rows: scopedRows } = await pool.query<ScopedPacketRow>(
      `SELECT p.packet_hash,
              min(p.time) AS t0,
              max(p.time) AS t1,
              (array_agg(p.raw_hex ORDER BY p.time DESC)
                 FILTER (WHERE p.route_type IN (0, 3) AND p.raw_hex <> ''))[1] AS raw,
              (array_agg(DISTINCT e) FILTER (WHERE e IS NOT NULL))[1:80] AS elems
         FROM packets p
         LEFT JOIN LATERAL unnest(p.path_hashes) AS e ON TRUE
        WHERE p.route_type IN (0, 3)
          AND p.time > now() - make_interval(days => $1)
        GROUP BY p.packet_hash`,
      [windowDays],
    );
    console.log(`[regions] scoped packets: ${scopedRows.length} (${Math.round((Date.now() - startedAt) / 1000)}s)`);
    if (scopedRows.length === 0) throw new Error('no scoped packets found in window; refusing to wipe tables');

    const regionTotals = new Map<string, number>();
    const regionFirstSeen = new Map<string, Date>();
    const regionLastSeen = new Map<string, Date>();
    const packetRegion = new Map<string, string | null>();
    let decodedCount = 0;
    for (const row of scopedRows) {
      let region: string | null = null;
      if (row.raw) {
        const compat = decodePacketCompat(row.raw, keyStore);
        if (compat.metadataValid && compat.transportCodes) {
          const tc0 = transportCode0Of(compat.transportCodes);
          region = decodeRegionForPacket(
            probeNames,
            compat.decoded?.payloadType,
            compat.decoded?.payload?.raw,
            tc0,
          );
        }
      }
      packetRegion.set(row.packet_hash, region);
      if (region) {
        decodedCount += 1;
        regionTotals.set(region, (regionTotals.get(region) ?? 0) + 1);
        const first = regionFirstSeen.get(region);
        if (!first || row.t0 < first) regionFirstSeen.set(region, row.t0);
        const last = regionLastSeen.get(region);
        if (!last || row.t1 > last) regionLastSeen.set(region, row.t1);
      }
    }
    const unknownCount = scopedRows.length - decodedCount;
    console.log(`[regions] decoded ${decodedCount}/${scopedRows.length} (${unknownCount} unknown)`);
    if (decodedCount === 0) throw new Error('no scoped packets decoded; refusing to wipe tables');

    const { rows: nodeRows } = await pool.query<NodeMetaRow>(
      'SELECT node_id, role FROM nodes',
    );
    const nodeRoles = new Map<string, number | null>();
    const prefixIndex = new Map<string, string[]>();
    for (const row of nodeRows) {
      const nodeId = (row.node_id ?? '').trim().toUpperCase();
      if (!/^[0-9A-F]+$/.test(nodeId)) continue;
      nodeRoles.set(nodeId, row.role);
      for (const length of [2, 4, 6]) {
        const key = nodeId.slice(0, length);
        const list = prefixIndex.get(key);
        if (list) list.push(nodeId);
        else prefixIndex.set(key, [nodeId]);
      }
    }

    const scopedByNode = new Map<string, Map<string, Set<string>>>();
    const scopedTotal = new Map<string, Set<string>>();
    const elementStats = new Map<string, number>();
    const unresolvedSamples: string[] = [];
    for (const row of scopedRows) {
      const region = packetRegion.get(row.packet_hash) ?? null;
      for (const element of row.elems ?? []) {
        if (!element) continue;
        const lengthKey = `elem_len_${element.length}`;
        elementStats.set(lengthKey, (elementStats.get(lengthKey) ?? 0) + 1);
        const nodeId = resolveElement(prefixIndex, element);
        if (!nodeId) {
          const unresolvedKey = `unresolved_len_${element.length}`;
          elementStats.set(unresolvedKey, (elementStats.get(unresolvedKey) ?? 0) + 1);
          if (unresolvedSamples.length < UNRESOLVED_ELEMENT_LOG_LIMIT) unresolvedSamples.push(element);
          continue;
        }
        let total = scopedTotal.get(nodeId);
        if (!total) {
          total = new Set<string>();
          scopedTotal.set(nodeId, total);
        }
        total.add(row.packet_hash);
        if (region) {
          let byRegion = scopedByNode.get(nodeId);
          if (!byRegion) {
            byRegion = new Map<string, Set<string>>();
            scopedByNode.set(nodeId, byRegion);
          }
          let hashes = byRegion.get(region);
          if (!hashes) {
            hashes = new Set<string>();
            byRegion.set(region, hashes);
          }
          hashes.add(row.packet_hash);
        }
      }
    }
    console.log(`[regions] element stats: ${JSON.stringify(Object.fromEntries([...elementStats.entries()].sort()))}`);

    const { rows: unscopedRows } = await pool.query<UnscopedElementRow>(
      `SELECT e, count(DISTINCT p.packet_hash) AS c
         FROM packets p, unnest(p.path_hashes) AS e
        WHERE p.route_type = 1
          AND p.path_hash_size_bytes >= 2
          AND p.time > now() - interval '7 days'
          AND length(e) >= 4
        GROUP BY e`,
    );
    const unscoped = new Map<string, number>();
    for (const row of unscopedRows) {
      if (!row.e) continue;
      const nodeId = resolveElement(prefixIndex, row.e);
      if (nodeId) unscoped.set(nodeId, (unscoped.get(nodeId) ?? 0) + Number(row.c));
    }
    console.log(`[regions] unscoped relay evidence (7d): ${unscoped.size} nodes, ${[...unscoped.values()].reduce((sum, value) => sum + value, 0)} relay counts`);

    // Region view (members = relays with resolved ids carrying at least
    // MEMBER_MIN_SHARE_PCT of the region's scoped packets).
    interface MemberOutput {
      region: string;
      nodeId: string;
      packets: number;
      role: number | null;
    }
    const members: MemberOutput[] = [];
    const regionView = new Map<string, { memberCount: number; repeaterMemberCount: number }>();
    let memberRowsTotal = 0;
    for (const [nodeId, byRegion] of scopedByNode) {
      for (const [region, hashes] of byRegion) {
        memberRowsTotal += 1;
        if (!meetsMemberShare(hashes.size, regionTotals.get(region) ?? 0)) continue;
        const role = nodeRoles.get(nodeId) ?? null;
        members.push({ region, nodeId, packets: hashes.size, role });
        const view = regionView.get(region) ?? { memberCount: 0, repeaterMemberCount: 0 };
        view.memberCount += 1;
        if (role === 2) view.repeaterMemberCount += 1;
        regionView.set(region, view);
      }
    }
    console.log(`[regions] member share floor >=${MEMBER_MIN_SHARE_PCT}%: kept ${members.length}/${memberRowsTotal} member rows`);

    // Node classification over every node with any relay evidence.
    interface ClassificationOutput {
      nodeId: string;
      status: 'green' | 'red';
      evidence: 'scoped' | 'unscoped-only' | 'none';
      scopedPackets: number;
      unscopedPackets7d: number;
      regions: Record<string, number>;
    }
    const classification: ClassificationOutput[] = [];
    const candidateIds = new Set<string>([...scopedTotal.keys(), ...unscoped.keys()]);
    for (const nodeId of candidateIds) {
      const scoped = scopedTotal.get(nodeId);
      const byRegion = scopedByNode.get(nodeId);
      const regions: Record<string, number> = {};
      if (byRegion) {
        for (const [region, hashes] of [...byRegion.entries()].sort((a, b) => b[1].size - a[1].size)) {
          if (!meetsMemberShare(hashes.size, regionTotals.get(region) ?? 0)) continue;
          regions[region] = hashes.size;
        }
      }
      classification.push({
        nodeId,
        status: scoped ? 'green' : 'red',
        evidence: scoped ? 'scoped' : (unscoped.get(nodeId) ?? 0) > 0 ? 'unscoped-only' : 'none',
        scopedPackets: scoped ? scoped.size : 0,
        unscopedPackets7d: unscoped.get(nodeId) ?? 0,
        regions,
      });
    }

    const greenCount = classification.filter((entry) => entry.status === 'green').length;
    const redCount = classification.length - greenCount;
    console.log(`[regions] regions (scoped packets -> members >=${MEMBER_MIN_SHARE_PCT}%):`);
    for (const [region, total] of [...regionTotals.entries()].sort((a, b) => b[1] - a[1])) {
      const view = regionView.get(region) ?? { memberCount: 0, repeaterMemberCount: 0 };
      console.log(`  ${region}: packets=${total} members=${view.memberCount} repeaters=${view.repeaterMemberCount}`);
    }
    console.log(`[regions] classification: green=${greenCount} red=${redCount}`);
    if (unresolvedSamples.length > 0) {
      console.log(`[regions] unresolved element samples: ${unresolvedSamples.join(', ')}`);
    }

    if (dryRun) {
      console.log(JSON.stringify({
        status: 'dry-run',
        scopedPackets: scopedRows.length,
        decodedPackets: decodedCount,
        regions: regionTotals.size,
        members: members.length,
        classification: classification.length,
      }));
      return;
    }

    const client = await pool.connect();
    try {
      await client.query('BEGIN');
      await client.query('DELETE FROM region_scope_members');
      await client.query('DELETE FROM region_scope_stats');
      await client.query('DELETE FROM repeater_scope_status');
      await client.query('DELETE FROM region_scope_run');

      const regionNames = [...regionTotals.keys()];
      await client.query(
        `INSERT INTO region_scope_stats
           (region, packets_total, member_count, repeater_member_count, first_seen, last_seen)
         SELECT region, packets_total, member_count, repeater_member_count, first_seen, last_seen
           FROM unnest($1::text[], $2::int[], $3::int[], $4::int[], $5::timestamptz[], $6::timestamptz[])
             AS t(region, packets_total, member_count, repeater_member_count, first_seen, last_seen)`,
        [
          regionNames,
          regionNames.map((region) => regionTotals.get(region) ?? 0),
          regionNames.map((region) => regionView.get(region)?.memberCount ?? 0),
          regionNames.map((region) => regionView.get(region)?.repeaterMemberCount ?? 0),
          regionNames.map((region) => regionFirstSeen.get(region) ?? null),
          regionNames.map((region) => regionLastSeen.get(region) ?? null),
        ],
      );

      const MEMBER_BATCH = 2_000;
      for (let offset = 0; offset < members.length; offset += MEMBER_BATCH) {
        const batch = members.slice(offset, offset + MEMBER_BATCH);
        await client.query(
          `INSERT INTO region_scope_members (region, node_id, packets)
           SELECT region, node_id, packets
             FROM unnest($1::text[], $2::text[], $3::int[]) AS t(region, node_id, packets)`,
          [
            batch.map((member) => member.region),
            batch.map((member) => member.nodeId),
            batch.map((member) => member.packets),
          ],
        );
      }

      const STATUS_BATCH = 2_000;
      for (let offset = 0; offset < classification.length; offset += STATUS_BATCH) {
        const batch = classification.slice(offset, offset + STATUS_BATCH);
        await client.query(
          `INSERT INTO repeater_scope_status
             (node_id, status, evidence, scoped_packets, unscoped_packets_7d, regions)
           SELECT node_id, status, evidence, scoped_packets, unscoped_packets_7d, regions::jsonb
             FROM unnest($1::text[], $2::text[], $3::text[], $4::int[], $5::int[], $6::text[])
               AS t(node_id, status, evidence, scoped_packets, unscoped_packets_7d, regions)`,
          [
            batch.map((entry) => entry.nodeId),
            batch.map((entry) => entry.status),
            batch.map((entry) => entry.evidence),
            batch.map((entry) => entry.scopedPackets),
            batch.map((entry) => entry.unscopedPackets7d),
            batch.map((entry) => JSON.stringify(entry.regions)),
          ],
        );
      }

      await client.query(
        `INSERT INTO region_scope_run (id, finished_at, window_days, scoped_packets, decoded_packets)
         VALUES (TRUE, now(), $1, $2, $3)`,
        [windowDays, scopedRows.length, decodedCount],
      );
      await client.query('COMMIT');
    } catch (error) {
      await client.query('ROLLBACK').catch(() => undefined);
      throw error;
    } finally {
      client.release();
    }

    console.log(JSON.stringify({
      status: 'complete',
      scopedPackets: scopedRows.length,
      decodedPackets: decodedCount,
      regions: regionTotals.size,
      members: members.length,
      classification: classification.length,
      elapsedSeconds: Math.round((Date.now() - startedAt) / 1000),
    }));
  } finally {
    await pool.end();
  }
}

main().catch((error: unknown) => {
  console.error('[regions] failed:', error instanceof Error ? error.message : error);
  process.exit(1);
});
