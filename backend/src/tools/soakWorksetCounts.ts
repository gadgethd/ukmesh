import { createHash } from 'node:crypto';

export const SOAK_COUNTER_DEADLINE_MS = 30_000;
export type SoakTotals = { packets: number; paths: number; facts: number; ledger: number };

/** All observations for the owned hashes, then their exact persisted children.
 * The lateral fence keeps work proportional to wanted hash probes. Child keys
 * use existing indexes; this is not an independent orphan-row/history audit. */
export const SOAK_WORKSET_COUNTS_SQL = `WITH wanted AS MATERIALIZED (
  SELECT DISTINCT packet_hash FROM unnest($1::text[]) AS input(packet_hash)
), observations AS MATERIALIZED (
  SELECT p.time, p.packet_hash, p.network, p.rx_node_id, p.topic, p.observation_id
    FROM wanted w CROSS JOIN LATERAL (
      SELECT time, packet_hash, network, rx_node_id, topic, observation_id
        FROM mesh.packets WHERE packet_hash=w.packet_hash OFFSET 0
    ) p
), path_keys AS MATERIALIZED (
  SELECT DISTINCT time, packet_hash, network, COALESCE(rx_node_id,'') AS rx_node_id, topic FROM observations
), observation_keys AS MATERIALIZED (
  SELECT DISTINCT observation_id, packet_hash, network FROM observations WHERE observation_id IS NOT NULL
)
SELECT (SELECT count(*)::integer FROM observations) AS packets,
  (SELECT count(*)::integer FROM path_keys k JOIN mesh.packet_paths p
    ON p.time=k.time AND p.packet_hash=k.packet_hash AND p.network=k.network
      AND COALESCE(p.rx_node_id,'')=k.rx_node_id AND p.topic=k.topic) AS paths,
  (SELECT count(*)::integer FROM observation_keys k JOIN mesh.multibyte_path_facts f
    ON f.observation_id=k.observation_id AND f.packet_hash=k.packet_hash AND f.network=k.network) AS facts,
  (SELECT count(*)::integer FROM observation_keys k JOIN mesh.ingest_observation_ledger l
    ON l.observation_id=k.observation_id AND l.packet_hash=k.packet_hash AND l.network=k.network) AS ledger`;

/** Once-only final integrity audit also sees orphan/mismatched child UUIDs.
 * Keep this distinct from the indexed parent-key drain polls and their scope. */
export const SOAK_HASH_AUDIT_SQL = `WITH wanted AS MATERIALIZED (
  SELECT DISTINCT packet_hash FROM unnest($1::text[]) AS input(packet_hash)
)
SELECT (SELECT count(*)::integer FROM mesh.packets p JOIN wanted w USING(packet_hash)) AS packets,
  (SELECT count(*)::integer FROM mesh.packet_paths p JOIN wanted w USING(packet_hash)) AS paths,
  (SELECT count(*)::integer FROM mesh.multibyte_path_facts f JOIN wanted w USING(packet_hash)) AS facts,
  (SELECT count(*)::integer FROM mesh.ingest_observation_ledger l JOIN wanted w USING(packet_hash)) AS ledger`;

export function soakWorkset(ackHashes: string[], auxiliaryHashes: string[] = []) {
  const normalized = (values: string[]) => [...new Set(values.map(value => {
    if (!/^[a-fA-F0-9]{64}$/.test(value)) throw new Error('invalid soak workset packet hash');
    return value.toUpperCase();
  }))].sort();
  const ack=normalized(ackHashes),auxiliary=normalized(auxiliaryHashes),hashes=normalized([...ack,...auxiliary]);
  return { hashes, countsScope: { kind:'owned-packet-derived-observation-workset-v1',
    hashCount:hashes.length,ackHashCount:ack.length,auxiliaryHashCount:auxiliary.length,
    sha256:createHash('sha256').update(JSON.stringify(hashes),'utf8').digest('hex'),
    description:'All packet observations for owned ACK and auxiliary hashes; paths by distinct natural key; facts/ledger by parent observation UUID plus hash/network. Independent orphan UUID rows without packet parents are outside this scope. Not global totals.' } };
}

export async function countSoakWorkset(query: (sql: string, params: unknown[]) => Promise<{ rows: unknown[] }>,
  hashes: string[], now: () => number = () => performance.now()): Promise<SoakTotals> {
  return exactCounts(query,SOAK_WORKSET_COUNTS_SQL,hashes,now);
}

export async function auditSoakWorksetHashes(query: (sql: string, params: unknown[]) => Promise<{ rows: unknown[] }>,
  hashes: string[], now: () => number = () => performance.now()): Promise<SoakTotals> {
  return exactCounts(query,SOAK_HASH_AUDIT_SQL,hashes,now);
}

async function exactCounts(query: (sql: string, params: unknown[]) => Promise<{ rows: unknown[] }>,
  sql:string,hashes:string[],now:()=>number):Promise<SoakTotals>{
  const began=now();
  const result=await query(sql,[hashes]);
  if(now()-began>SOAK_COUNTER_DEADLINE_MS)throw new Error('owned soak counter exceeded30-second logical deadline');
  const row=result.rows[0] as SoakTotals | undefined;
  if(!row || (['packets','paths','facts','ledger'] as const).some(key=>!Number.isSafeInteger(row[key])||row[key]<0)) {
    throw new Error('owned soak counter returned invalid exact totals');
  }
  return row;
}
