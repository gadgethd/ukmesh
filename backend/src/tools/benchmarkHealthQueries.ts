/** Read-only parity/timing comparison against a pre-refactor Git revision.
 * Usage: npx tsx src/tools/benchmarkHealthQueries.ts CONTAINER BASELINE_COMMIT
 * Uses the container's existing psql credentials without printing them. */
import assert from 'node:assert/strict';
import { execFileSync } from 'node:child_process';
import { INGEST_HEALTH_SQL, PATH_HASH_HEALTH_SQL } from '../health/packetDiagnostics.js';

const [container, revision] = process.argv.slice(2);
if (!container || !revision || !/^[a-zA-Z0-9_.-]+$/.test(container) || !/^[a-f0-9]{7,40}$/i.test(revision)) {
  throw new Error('Usage: benchmarkHealthQueries.ts CONTAINER BASELINE_COMMIT');
}
const baseline = execFileSync('git', ['show', `${revision}:backend/src/health/status.ts`], { encoding: 'utf8' });
function extract(pattern: RegExp): string {
  const match = baseline.match(pattern)?.[1];
  if (!match) throw new Error('Baseline health query not found');
  return match;
}
const oldIngest = extract(/`(WITH latest_rx AS \([\s\S]*?FROM active_rx)`/);
const oldWidths = extract(/`(SELECT length\(h\)::text AS hash_hex_len,[\s\S]*?GROUP BY 1)`/);
const oldMultibyte = extract(/`(SELECT\s+MAX\(time\) FILTER \([\s\S]*?AND network IS DISTINCT FROM 'test')`/);
const oldPaths = `WITH widths AS (${oldWidths}), multibyte AS (${oldMultibyte})
  SELECT COALESCE((SELECT hop_count FROM widths WHERE hash_hex_len = '2'), '0') AS one_byte,
         COALESCE((SELECT hop_count FROM widths WHERE hash_hex_len = '4'), '0') AS two_byte,
         COALESCE((SELECT hop_count FROM widths WHERE hash_hex_len = '6'), '0') AS three_byte,
         latest_multibyte_at, multibyte_packets_24h FROM multibyte`;
const queries = { oldIngest, newIngest: INGEST_HEALTH_SQL, oldPaths, newPaths: PATH_HASH_HEALTH_SQL };
const sql = [
  'BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY;',
  "SET LOCAL statement_timeout = '30s';",
  "SET LOCAL idle_in_transaction_session_timeout = '30s';",
  ...(['ingest', 'paths'] as const).map((name) => {
    const oldSql = name === 'ingest' ? oldIngest : oldPaths;
    const newSql = name === 'ingest' ? INGEST_HEALTH_SQL : PATH_HASH_HEALTH_SQL;
    return `WITH before AS (${oldSql}), after AS (${newSql})
      SELECT json_build_object('parity', '${name}', 'different_rows', COUNT(*))
      FROM ((SELECT * FROM before EXCEPT ALL SELECT * FROM after)
        UNION ALL (SELECT * FROM after EXCEPT ALL SELECT * FROM before)) diff;`;
  }),
  // Alternate ordering to reduce systematic warm-cache bias.
  ...Array.from({ length: 3 }, (_, run) => {
    const entries = Object.entries(queries);
    if (run % 2) entries.reverse();
    return entries.flatMap(([name, query]) => [
      `SELECT json_build_object('query', '${name}', 'run', ${run + 1});`,
      `EXPLAIN (ANALYZE, TIMING OFF, SUMMARY ON, FORMAT JSON) ${query};`,
    ]).join('\n');
  }),
  'ROLLBACK;',
].join('\n');
const output = execFileSync('docker', [
  'exec', '-i', container, 'sh', '-c',
  'psql -X -qAt -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" -d "$POSTGRES_DB"',
], { input: sql, encoding: 'utf8', timeout: 180_000, maxBuffer: 16 * 1024 * 1024 });
// Emit only aggregate parity and timing, never packet data or query plans.
for (const line of output.split('\n')) {
  if (line.startsWith('{"parity"')) {
    const result = JSON.parse(line) as { different_rows: number };
    assert.equal(result.different_rows, 0, line);
    console.log(line);
  } else if (line.startsWith('{"query"') || /"(?:Planning|Execution) Time":/.test(line)) {
    console.log(line.trim());
  }
}
