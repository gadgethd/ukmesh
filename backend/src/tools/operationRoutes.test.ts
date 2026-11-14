import assert from 'node:assert/strict';
import { mkdtemp, mkdir, readFile, rm, writeFile } from 'node:fs/promises';
import path from 'node:path';
import os from 'node:os';
import test from 'node:test';
import { withOperationServer } from './operationRoutes.js';

test('statistics and exports use the selected revision and close all owners after partial failure', async t => {
  const directory = await mkdtemp(path.join(os.tmpdir(), 'operations-source-'));
  t.after(() => rm(directory, { recursive: true, force: true }));
  for (const label of ['baseline', 'candidate', 'broken', 'writing', 'writable', 'stale']) {
    const source = path.join(directory, label), evidence = path.join(source, 'closed');
    const append = `import {appendFileSync} from 'node:fs'; const record = value => appendFileSync(${JSON.stringify(evidence)}, value+'\\n');`;
    const files: Record<string, string> = {
      'db/runtime.ts': `${append} export const pool={};
        export const query=async(sql)=>{if(sql.startsWith('INSERT'))throw Object.assign(new Error('read-only'),{code:'25006'});return {rows:[{value:'on'}]}};
        export const analyticsQuery=${label === 'writable' ? "async()=>({rows:[{value:'off'}]})" : 'query'};
        export async function closeDb(){record('runtime')}`,
      'api/bootstrap/caches.ts': `${append}
        export const STATS_CACHE_TTL_MS=${label === 'baseline' ? 100 : 200};
        export const CHARTS_CACHE_TTL_MS=1000,CHARTS_CACHE_STALE_TTL_MS=2000;
        export function createApiCaches(){return {statsCache:new Map(),chartsCache:new Map(),chartsInflight:new Map(),close(){record('caches')}}}`,
      'api/bootstrap/limiters.ts': `const pass=(_req,_res,next)=>next();
        export const EXPENSIVE_LIMITER=pass,STATS_CHARTS_LIMITER=pass,EXPORT_LIMITER=pass;
        export const createGlobalApiLimiter=()=>pass;`,
      'api/routes/stats.ts': `${append} export function registerStatsRoutes(router,deps){
        router.get('/stats',(_req,res)=>{${label === 'writing' ? "void deps.analyticsQuery('INSERT snapshot').catch(()=>{});" : ''}
          res.json({selected:${JSON.stringify(label)},ttl:deps.statsCacheTtlMs});});
        return {close(){record('stats')}}}`,
      'api/routes/exports.ts': `export function registerExportRoutes(router){
        ${label === 'broken' ? "throw new Error('export registration failed');" :
        `router.get('/v1/exports/nodes.csv',(_req,res)=>res.type('text/csv').send(${JSON.stringify(label)}));`}}`,
      'platform/query/networkFilters.ts': 'export const networkFilters=()=>({});',
      'privacy/responseGeneration.ts': 'export const readPublicResponseGeneration=async()=>1;',
      'privacy/authority.ts': 'export const readCurrentVisibilityGeneration=async()=>1;',
      'stats/maskDecodedPathNodes.ts': 'export const maskDecodedPathNodes=value=>value;',
      'stats/chartSnapshot.ts': `const generatedAt=new Date().toISOString();
        export const loadStoredChartSnapshot=async()=>({generated_at:generatedAt,visibility_generation:${label === 'stale' ? 2 : 1},payload:{snapshot:{generatedAt}}});
        export const validateChartSnapshotPayload=payload=>({payload,generatedAtMs:Date.parse(payload.snapshot.generatedAt)});`,
      'analysis/heavyWorkAdmission.ts': 'export const withHeavyWorkAdmission=({task})=>task();',
      'api/errors.ts': 'export const wrapAsyncHandlers=()=>{}; export const apiErrorMiddleware=(error,_req,res,_next)=>res.status(500).end();',
    };
    for (const [name, content] of Object.entries(files)) {
      await mkdir(path.dirname(path.join(source, name)), { recursive: true });
      await writeFile(path.join(source, name), content);
    }
    let usedPort = 0;
    if (label === 'stale') {
      await assert.rejects(withOperationServer(source, async () => assert.fail('stale chart authority cannot listen')), /fresh persisted chart snapshot/);
    } else if (label === 'writable') {
      await assert.rejects(withOperationServer(source, async () => assert.fail('writable analytics cannot listen')), /read-only operation pools/);
    } else if (label === 'writing') {
      await assert.rejects(withOperationServer(source, async port => {
        const response = await fetch(`http://127.0.0.1:${port}/api/stats`);
        assert.equal(response.status, 200);
      }), /attempted publication/);
    } else if (label === 'broken') {
      await assert.rejects(withOperationServer(source, async () => { assert.fail('partially registered routes cannot listen'); }), /export registration failed/);
    } else {
      await assert.rejects(withOperationServer(source, async port => {
        usedPort = port;
        const summary = await fetch(`http://127.0.0.1:${port}/api/stats`);
        assert.deepEqual(await summary.json(), { selected: label, ttl: label === 'baseline' ? 100 : 200 });
        const csv = await fetch(`http://127.0.0.1:${port}/api/v1/exports/nodes.csv`);
        assert.equal(await csv.text(), label);
        throw new Error('trial failed after both route owners worked');
      }), /trial failed after both route owners worked/);
      await assert.rejects(fetch(`http://127.0.0.1:${usedPort}/api/stats`));
    }
    assert.equal(await readFile(evidence, 'utf8'), ['writable', 'stale'].includes(label) ? 'runtime\n' : 'stats\ncaches\nruntime\n');
  }
});
