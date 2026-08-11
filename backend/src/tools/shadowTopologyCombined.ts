import { pool } from '../db/index.js';
import { networkFilters } from '../api/utils/networkFilters.js';
import {
  combinedTopologyRows,
  standaloneTopologyRows,
  topologyRows,
} from '../repositories/networkAnalysis.js';
import { buildTopologyDto } from '../api/routes/topology.js';

const network = process.argv[2] ?? 'ukmesh';
const limit = Math.max(50, Math.min(500, Number(process.argv[3] ?? 300) || 300));
const generatedAt = new Date('2026-08-09T00:00:00.000Z');
const client = await pool.connect();
try {
  await client.query('BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY');
  const visibility = await client.query<{ generation: string }>(
    'SELECT generation::text AS generation FROM public_visibility_state WHERE singleton = TRUE',
  );
  const query = client.query.bind(client);
  const legacyFilters = networkFilters(network);
  const legacyLinks = await topologyRows(query, legacyFilters, limit);
  const legacyStandalone = await standaloneTopologyRows(query, legacyFilters);
  const combined = await combinedTopologyRows(query, networkFilters(network), limit);
  const legacyDto = buildTopologyDto({
    links: legacyLinks.rows,
    standalone: legacyStandalone.rows,
  }, limit, generatedAt);
  const combinedDto = buildTopologyDto(combined, limit, generatedAt);
  const legacyJson = JSON.stringify(legacyDto);
  const combinedJson = JSON.stringify(combinedDto);
  console.log(JSON.stringify({
    network,
    limit,
    visibilityGeneration: Number(visibility.rows[0]?.generation),
    exact: legacyJson === combinedJson,
    legacyBytes: Buffer.byteLength(legacyJson),
    combinedBytes: Buffer.byteLength(combinedJson),
    links: combined.links.length,
    standalone: combined.standalone.length,
  }));
  if (legacyJson !== combinedJson) process.exitCode = 1;
  await client.query('ROLLBACK');
} catch (error) {
  await client.query('ROLLBACK').catch(() => {});
  throw error;
} finally {
  client.release();
  await pool.end();
}
