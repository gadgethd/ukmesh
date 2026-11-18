import { createHash } from 'node:crypto';

export const SOAK_COUNTER_DEADLINE_MS = 30_000;
export type SoakTotals = { packets: number; paths: number; facts: number; ledger: number };

const wanted = `wanted AS MATERIALIZED (
  SELECT DISTINCT packet_hash FROM unnest($1::text[]) AS input(packet_hash)
)`;

const parentKeys = (since: string) => `observations AS MATERIALIZED (
  SELECT p.time, p.packet_hash, p.network, p.rx_node_id, p.topic, p.observation_id
    FROM wanted w CROSS JOIN LATERAL (
      SELECT time, packet_hash, network, rx_node_id, topic, observation_id
        FROM mesh.packets
        WHERE packet_hash=w.packet_hash AND (${since}::timestamptz IS NULL OR time >= ${since}::timestamptz)
        OFFSET 0
    ) p
), path_keys AS MATERIALIZED (
  SELECT DISTINCT time, packet_hash, network, COALESCE(rx_node_id,'') AS rx_node_id, topic FROM observations
), observation_keys AS MATERIALIZED (
  SELECT DISTINCT observation_id, packet_hash, network FROM observations WHERE observation_id IS NOT NULL
)`;

const parentPaths = `path_keys k JOIN mesh.packet_paths p
  ON p.time=k.time AND p.packet_hash=k.packet_hash AND p.network=k.network
    AND COALESCE(p.rx_node_id,'')=k.rx_node_id AND p.topic=k.topic`;
const parentFacts = `observation_keys k JOIN mesh.multibyte_path_facts f
  ON f.observation_id=k.observation_id AND f.packet_hash=k.packet_hash AND f.network=k.network`;
const parentLedger = `observation_keys k JOIN mesh.ingest_observation_ledger l
  ON l.observation_id=k.observation_id AND l.packet_hash=k.packet_hash AND l.network=k.network`;

/** Exact persisted children of each owned packet observation. */
export const SOAK_WORKSET_COUNTS_SQL = `WITH ${wanted}, ${parentKeys('$2')}
SELECT (SELECT count(*)::integer FROM observations) AS packets,
  (SELECT count(*)::integer FROM ${parentPaths}) AS paths,
  (SELECT count(*)::integer FROM ${parentFacts}) AS facts,
  (SELECT count(*)::integer FROM ${parentLedger}) AS ledger`;

const parentAuditCounts = `${parentKeys('$4')}, packets AS (
  SELECT packet_hash, count(*)::integer AS n, count(DISTINCT topic)::integer AS observers
    FROM observations GROUP BY packet_hash
), paths AS (
  SELECT k.packet_hash, count(*)::integer AS n FROM ${parentPaths} GROUP BY k.packet_hash
), facts AS (
  SELECT k.packet_hash, count(*)::integer AS n FROM ${parentFacts} GROUP BY k.packet_hash
), ledger AS (
  SELECT k.packet_hash, count(*)::integer AS n FROM ${parentLedger} GROUP BY k.packet_hash
)`;

const globalAuditCounts = `packets AS (
  SELECT packet_hash, count(*)::integer AS n, count(DISTINCT topic)::integer AS observers
    FROM mesh.packets JOIN wanted USING(packet_hash) GROUP BY packet_hash
), paths AS (
  SELECT packet_hash, count(*)::integer AS n FROM mesh.packet_paths JOIN wanted USING(packet_hash) GROUP BY packet_hash
), facts AS (
  SELECT packet_hash, count(*)::integer AS n FROM mesh.multibyte_path_facts JOIN wanted USING(packet_hash) GROUP BY packet_hash
), ledger AS (
  SELECT packet_hash, count(*)::integer AS n FROM mesh.ingest_observation_ledger JOIN wanted USING(packet_hash) GROUP BY packet_hash
)`;

const auditSQL = (counts: string) => `WITH ${wanted}, ${counts}, per_hash AS (
  SELECT w.packet_hash, a.packet_hash IS NOT NULL AS expects_path,
    1 + (multi.packet_hash IS NOT NULL)::integer AS expected_observers,
    coalesce(p.observers,0) AS observers, coalesce(p.n,0) AS packets, coalesce(t.n,0) AS paths,
    coalesce(f.n,0) AS facts, coalesce(l.n,0) AS ledger
  FROM wanted w
  LEFT JOIN (SELECT DISTINCT unnest($2::text[]) AS packet_hash) a USING(packet_hash)
  LEFT JOIN (SELECT DISTINCT unnest($3::text[]) AS packet_hash) multi USING(packet_hash)
  LEFT JOIN packets p USING(packet_hash)
  LEFT JOIN paths t USING(packet_hash)
  LEFT JOIN facts f USING(packet_hash)
  LEFT JOIN ledger l USING(packet_hash)
), mismatches AS MATERIALIZED (
  SELECT packet_hash, packets, paths, facts, ledger, observers, expected_observers FROM per_hash
  WHERE packets <> expected_observers OR observers <> expected_observers OR ledger <> expected_observers
    OR paths <> expects_path::integer OR facts <> expects_path::integer
)
SELECT coalesce(sum(packets),0)::integer AS packets, coalesce(sum(paths),0)::integer AS paths,
  coalesce(sum(facts),0)::integer AS facts, coalesce(sum(ledger),0)::integer AS ledger,
  (SELECT count(*)::integer FROM mismatches) AS mismatch_count,
  (SELECT coalesce(json_agg(m), '[]'::json) FROM
    (SELECT * FROM mismatches ORDER BY packet_hash LIMIT 20) m) AS mismatches
FROM per_hash`;

/** Timed reconciliation prunes to the publication window and uses child-key indexes. */
export const SOAK_PARENT_AUDIT_SQL = auditSQL(parentAuditCounts);

/** Independent reconciliation finds children without a matching packet parent. */
export const SOAK_HASH_AUDIT_SQL = auditSQL(globalAuditCounts);


export function soakWorkset(ackHashes: string[], auxiliaryHashes: string[] = []) {
  const normalized = (values: string[]) => [...new Set(values.map(value => {
    if (!/^[a-fA-F0-9]{64}$/.test(value)) throw new Error('invalid soak workset packet hash');
    return value.toUpperCase();
  }))].sort();
  const ack=normalized(ackHashes),auxiliary=normalized(auxiliaryHashes),hashes=normalized([...ack,...auxiliary]);
  return { hashes, ackHashes: ack, countsScope: { kind:'owned-packet-derived-observation-workset-v1',
    hashCount:hashes.length,ackHashCount:ack.length,auxiliaryHashCount:auxiliary.length,
    sha256:createHash('sha256').update(JSON.stringify(hashes),'utf8').digest('hex'),
    description:'All packet observations for owned ACK and auxiliary hashes; paths by distinct natural key; facts/ledger by parent observation UUID plus hash/network. Independent orphan UUID rows without packet parents are outside this scope. Not global totals.' } };
}

type Query = (sql: string, params: unknown[]) => Promise<{ rows: unknown[] }>;
const totalKeys = ['packets', 'paths', 'facts', 'ledger'] as const;

export async function countSoakWorkset(query: Query, hashes: string[], options: {
  since?: Date; now?: () => number;
} = {}): Promise<SoakTotals> {
  return totals(await checkedRow(query, SOAK_WORKSET_COUNTS_SQL, [hashes, options.since ?? null],
    options.now ?? (() => performance.now())));
}

export async function auditSoakWorksetHashes(query: Query, hashes: string[], ackHashes: string[],
  dualObserverHashes: string[], now = () => performance.now()) {
  return audit(query, SOAK_HASH_AUDIT_SQL, [hashes, ackHashes, dualObserverHashes], hashes.length, now);
}

export async function auditSoakWorksetParents(query: Query, hashes: string[], ackHashes: string[],
  dualObserverHashes: string[], since: Date, now = () => performance.now()) {
  return audit(query, SOAK_PARENT_AUDIT_SQL, [hashes, ackHashes, dualObserverHashes, since], hashes.length, now);
}

async function audit(query: Query, sql: string, parameters: unknown[], worksetSize: number, now: () => number) {
  const row = await checkedRow(query, sql, parameters, now);
  const mismatchCount = row['mismatch_count'];
  const mismatches = row['mismatches'];
  if (typeof mismatchCount !== 'number' || !Number.isSafeInteger(mismatchCount) || mismatchCount < 0
    || mismatchCount > worksetSize || !Array.isArray(mismatches) || mismatches.length !== Math.min(mismatchCount, 20)) {
    throw new Error('owned soak audit returned invalid reconciliation');
  }
  return { totals: totals(row), mismatchCount, mismatches };
}

async function checkedRow(query: Query, sql: string, params: unknown[], now: () => number): Promise<Record<string, unknown>> {
  const began = now();
  const result = await query(sql, params);
  if (now() - began > SOAK_COUNTER_DEADLINE_MS) throw new Error('owned soak counter exceeded30-second logical deadline');
  return (result.rows[0] ?? {}) as Record<string, unknown>;
}

function totals(row: Record<string, unknown>): SoakTotals {
  const result = {} as SoakTotals;
  for (const key of totalKeys) {
    const value = row[key];
    if (typeof value !== 'number' || !Number.isSafeInteger(value) || value < 0) {
      throw new Error('owned soak counter returned invalid exact totals');
    }
    result[key] = value;
  }
  return result;
}
