/** Read-only, complete graph parity and cache-budget check on owned lookup fixtures. */
import assert from 'node:assert/strict';
import { writeFile } from 'node:fs/promises';
import { loadContext } from '../path-beta/resolver.js';
import { pathContextWeight, PATH_CONTEXT_MAX_WEIGHT } from '../path-beta/contextWeight.js';
import { undirectedLinkKey } from '../path-shared/priorKeys.js';
import { query, closeDb } from '../db/runtime.js';
import type { LinkMetrics } from '../path-beta/types.js';
import { expandResolverScope } from '../networks.js';
import { assertLocalDatabaseUrl, requiredOption } from './sampleData.js';
import type { LinkMetricRow } from '../path-beta/linkMetricsIndex.js';

const endpoint = process.env['DATABASE_URL'] ?? '';
assertLocalDatabaseUrl(endpoint);
assert.match(new URL(endpoint).pathname, /^\/lookup_(1x|10x|soak)$/);
try {
  const context = await loadContext('ukmesh');
  const rows = (await query<LinkMetricRow>(`SELECT nl.node_a_id, nl.node_b_id, nl.observed_count,
    nl.multibyte_observed_count, nl.itm_path_loss_db, nl.itm_viable, nl.count_a_to_b, nl.count_b_to_a
    FROM core.node_links nl JOIN core.nodes a ON a.node_id=nl.node_a_id JOIN core.nodes b ON b.node_id=nl.node_b_id
    WHERE (nl.itm_viable IS NOT NULL OR nl.force_viable=true)
      AND (a.name IS NULL OR a.name NOT LIKE '%🚫%') AND (b.name IS NULL OR b.name NOT LIKE '%🚫%')
      AND a.network=ANY($1) AND b.network=ANY($1)`, [expandResolverScope('ukmesh')])).rows;
  const reference = new Map<string, LinkMetrics>();
  for (const row of rows) reference.set(undirectedLinkKey(row.node_a_id, row.node_b_id), {
    observed_count: Number(row.observed_count ?? 0), multibyte_observed_count: Number(row.multibyte_observed_count ?? 0),
    itm_path_loss_db: row.itm_path_loss_db == null ? null : Number(row.itm_path_loss_db), itm_viable: row.itm_viable ?? null,
    count_a_to_b: row.count_a_to_b == null ? null : Number(row.count_a_to_b),
    count_b_to_a: row.count_b_to_a == null ? null : Number(row.count_b_to_a),
  });
  assert.equal(context.linkMetrics.size, reference.size);
  for (const [key, expected] of reference) assert.deepEqual(context.linkMetrics.get(key), expected, key);
  const compactBytes = pathContextWeight('ukmesh', context);
  const formerBytes = pathContextWeight('ukmesh', { ...context, linkMetrics: reference });
  assert.ok(compactBytes <= PATH_CONTEXT_MAX_WEIGHT, 'complete context must fit the unchanged cache budget');
  const record = { passed: true, nodes: context.nodesById.size, links: reference.size, compactBytes,
    formerBytes, formerWeightCapped: formerBytes > PATH_CONTEXT_MAX_WEIGHT, budgetBytes: PATH_CONTEXT_MAX_WEIGHT,
    accounting: 'Estimated retained bytes, including the actual learning model; former estimate stops when over budget.' };
  await writeFile(requiredOption(process.argv.slice(2), 'output'), JSON.stringify(record, null, 2) + '\n');
  console.log(JSON.stringify(record));
} finally { await closeDb(); }
