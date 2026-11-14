/** Compare selected revisions' real statistics/export handlers on owned stores. */
import { mkdir, writeFile } from 'node:fs/promises';
import path from 'node:path';
import http from 'node:http';
import { option } from './sampleData.js';
import { configureLookupReadOnlyEnvironment } from './lookupBenchmarkSession.js';
import { withOperationServer } from './operationRoutes.js';
import { lookupSourceIdentity } from './lookupSource.js';
import { LOOKUP_RESPONSE_MAX_BYTES } from './lookupResponseBody.js';
import { OPERATION_CASES, readOperation, validateOperationResponse } from './operationResponse.js';
import type { OperationSnapshot } from './operationResponse.js';

const args = process.argv.slice(2), source = path.resolve(option(args, 'source-root') ?? 'src');
const output = path.resolve(option(args, 'output') ?? '../plans/database-lookups/operations');
const sourceIdentity = await lookupSourceIdentity(source, option(args, 'reference-revision') ?? null);
configureLookupReadOnlyEnvironment(process.env['DATABASE_URL'] ?? '', process.env);
const records: Array<Record<string, unknown>> = [];
let failure: unknown;
let chartSnapshot: OperationSnapshot | undefined;
try {
  await withOperationServer(source, async (port, snapshot) => {
      chartSnapshot = snapshot;
      const agent = new http.Agent({ keepAlive: true, maxSockets: 1 });
      let lastExport = -Infinity;
      try {
        for (let trial = 0; trial < 3; trial++) {
          for (const definition of OPERATION_CASES) {
            if (definition.format === 'csv' || definition.format === 'geojson') {
              const delay = 13_000 - (performance.now() - lastExport);
              if (delay > 0) await new Promise(resolve => setTimeout(resolve, delay));
            }
            const started = performance.now();
            const record: Record<string, unknown> = { name: definition.name, trial, passed: false };
            try {
              const result = await readOperation(port, definition.route, agent);
              Object.assign(record, { ms: result.ms, status: result.status, contentType: result.type },
                validateOperationResponse(result.body, definition.format, result.status, result.type, snapshot));
              record['passed'] = result.ms <= 30_000;
            } catch (error) {
              record['error'] = error instanceof Error ? error.message : String(error);
              record['ms'] ??= performance.now() - started;
            }
            records.push(record);
            if (!record['passed']) throw new Error(`operation ${definition.name} failed; see retained response evidence`);
            if (definition.format === 'csv' || definition.format === 'geojson') lastExport = performance.now();
          }
        }
      } finally { agent.destroy(); }
  });
} catch (error) { failure = error; }
const after = await lookupSourceIdentity(source, sourceIdentity.revision);
const sourceStable = JSON.stringify(sourceIdentity) === JSON.stringify(after);
const identityChecks = Array.from({ length: 3 }, (_, trial) => {
  const csv = records.find(row => row['trial'] === trial && row['name'] === 'nodes-csv');
  const geo = records.find(row => row['trial'] === trial && row['name'] === 'nodes-geojson');
  return { trial, passed: Boolean(csv?.['identityDigest']) && csv?.['identityDigest'] === geo?.['identityDigest'] };
});
const passed = !failure && sourceStable && records.length === 12 && records.every(row => row['passed'])
  && identityChecks.every(row => row.passed);
await mkdir(output, { recursive: true });
await writeFile(path.join(output, 'operations.json'), JSON.stringify({ passed, at: new Date().toISOString(),
  settings: { trials: 3, deadlineMs: 30_000, maxResponseBytes: LOOKUP_RESPONSE_MAX_BYTES, exportSpacingMs: 13_000,
    readOnly: true, httpConcurrency: 1 }, chartSnapshot, sourceIdentity, sourceIdentityAfter: after, sourceStable, records, identityChecks,
  error: failure instanceof Error ? failure.message : failure ? String(failure) : null }, null, 2) + '\n');
if (!passed) throw new Error('statistics/export qualification failed; see saved evidence');
