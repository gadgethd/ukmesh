/** Compare selected revisions' real statistics/export handlers on owned stores. */
import { mkdir, writeFile } from 'node:fs/promises';
import path from 'node:path';
import http from 'node:http';
import { performance } from 'node:perf_hooks';
import { option } from './sampleData.js';
import { configureLookupReadOnlyEnvironment } from './lookupBenchmarkSession.js';
import { withOperationServer } from './operationRoutes.js';
import { lookupSourceIdentity } from './lookupSource.js';
import { LOOKUP_RESPONSE_MAX_BYTES } from './lookupResponseBody.js';
import { OPERATION_CASES, readOperation, validateOperationResponse } from './operationResponse.js';
import type { OperationSnapshot } from './operationResponse.js';
import { createOperationPair, operationSamples, OPERATION_PAIR_POLICY as policy } from './operationPair.js';
import { metricDifferences } from './operationDiagnostics.js';
import { createExportPacer, exportIdentityChecks } from './operationExports.js';

const args = process.argv.slice(2), source = path.resolve(option(args, 'source-root') ?? 'src');
const output = path.resolve(option(args, 'output') ?? '../plans/database-lookups/operations');
const sourceIdentity = await lookupSourceIdentity(source, option(args, 'reference-revision') ?? null);
const pairDirectory = option(args, 'pair-directory'), pairToken = option(args, 'pair-token'), pairLabel = option(args, 'pair-label');
if ([pairDirectory, pairToken, pairLabel].some(Boolean) && ![pairDirectory, pairToken, pairLabel].every(Boolean)) throw new Error('complete operation pair identity required');
const pair = pairDirectory && pairToken && pairLabel
  ? await createOperationPair(pairDirectory, pairToken, pairLabel, sourceIdentity) : undefined;
configureLookupReadOnlyEnvironment(process.env['DATABASE_URL'] ?? '', process.env);
const records: Array<Record<string, unknown>> = [];
let failure: unknown;
let chartSnapshot: OperationSnapshot | undefined;
let executorProfile: unknown;
let databaseIdentity: unknown;
const actors = Array.from({ length: policy.clients }, (_, index) => `127.0.${pairLabel === 'baseline' ? 6 : pairLabel === 'candidate' ? 7 : 8}.${index + 1}`);
try {
  await withOperationServer(source, async (port, snapshot, diagnostics) => {
      if (!diagnostics) throw new Error('selected-source operation diagnostics required');
      chartSnapshot = snapshot;
      executorProfile = diagnostics.profile;
      const agents = actors.map(localAddress => new http.Agent({ keepAlive: true, maxSockets: 1, localAddress }));
      const exportPacer = createExportPacer(actors);
      let requestId = 0;
      try {
        for (let trial = 0; trial < policy.trials; trial++) {
          for (const definition of OPERATION_CASES) {
           for (let sample = 0; sample < operationSamples(definition.name); sample++) {
            const actor = requestId++ % actors.length;
            const exporting = definition.format === 'csv' || definition.format === 'geojson';
            if (exporting) await exportPacer.wait(actors[actor]);
            const metricsBefore = await diagnostics.metrics();
            const poolsBefore = diagnostics.pools();
            const paired = pair ? await pair.wait(trial, definition.name, snapshot, sample) : undefined;
            const cpuBefore = process.cpuUsage(), loopBefore = performance.eventLoopUtilization();
            const started = performance.now();
            const record: Record<string, unknown> = { name: definition.name, trial, sample, actor: actors[actor], passed: false,
              startedAt: new Date().toISOString(), ...(paired ? { paired } : {}) };
            try {
              const result = await readOperation(port, definition.route, agents[actor]);
              record['finishedAt'] = new Date().toISOString();
              const cpu = process.cpuUsage(cpuBefore), eventLoop = performance.eventLoopUtilization(loopBefore);
              const metricsAfter = await diagnostics.metrics();
              Object.assign(record, { ms: result.ms, status: result.status, contentType: result.type,
                transport: result.transport, diagnostics: { cpu, eventLoop, database: metricDifferences(metricsBefore, metricsAfter),
                  poolsBefore, poolsAfter: diagnostics.pools(),
                  databaseScope: 'selected-source process; one HTTP request active; execution includes driver wall time' } },
                validateOperationResponse(result.body, definition.format, result.status, result.type, snapshot));
              record['passed'] = result.ms <= policy.responseDeadlineMs && result.transport.localAddress === actors[actor];
            } catch (error) {
              record['finishedAt'] ??= new Date().toISOString();
              record['error'] = error instanceof Error ? error.message : String(error);
              record['ms'] ??= performance.now() - started;
            }
            records.push(record);
            if (!record['passed']) throw new Error(`operation ${definition.name} failed; see retained response evidence`);
            if (exporting) exportPacer.complete(actors[actor]);
           }
          }
        }
      } finally { for (const agent of agents) agent.destroy(); }
  }, { diagnostics: true, onDatabaseIdentity: value => { databaseIdentity = value; } });
} catch (error) { failure = error; }
const after = await lookupSourceIdentity(source, sourceIdentity.revision);
const sourceStable = JSON.stringify(sourceIdentity) === JSON.stringify(after);
const identityChecks = exportIdentityChecks(records);
const passed = !failure && sourceStable && records.length === policy.trials * OPERATION_CASES.reduce((total, item) => total + operationSamples(item.name), 0) && records.every(row => row['passed'])
  && identityChecks.every(row => row.passed);
await mkdir(output, { recursive: true });
await writeFile(path.join(output, 'operations.json'), JSON.stringify({ passed, at: new Date().toISOString(),
  settings: { trials: policy.trials, statsSamples: policy.statsSamples, exportSamples: policy.exportSamples, clients: policy.clients,
    deadlineMs: policy.responseDeadlineMs, maxResponseBytes: LOOKUP_RESPONSE_MAX_BYTES, exportSpacingMs: policy.exportSpacingMs,
    readOnly: true, httpConcurrency: policy.httpConcurrency }, pair: pair ? { token: pair.token, label: pair.label } : null,
  actors, executorProfile, databaseIdentity, chartSnapshot, sourceIdentity, sourceIdentityAfter: after, sourceStable, records, identityChecks,
  error: failure instanceof Error ? failure.message : failure ? String(failure) : null }, null, 2) + '\n');
if (!passed) throw new Error('statistics/export qualification failed; see saved evidence');
