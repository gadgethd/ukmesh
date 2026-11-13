/** Mixed reads and real MQTT ingestion on an explicitly owned local stack. */
import { createHash } from 'node:crypto';
import { execFileSync } from 'node:child_process';
import { mkdir, readFile, writeFile } from 'node:fs/promises';
import path from 'node:path';
import pg from 'pg';
import WebSocket from 'ws';
import { assertLocalDatabaseUrl, option, readGzipCsv, requiredOption, rowObject } from './sampleData.js';
import { replaySampleMqtt } from './replaySampleMqtt.js';
import { CACHE_POLICY_REGISTRY } from '../cache/policyRegistry.js';
import { boundedCacheMetricLabel } from '../metrics/labels.js';

const args = process.argv.slice(2), endpoint = process.env['DATABASE_URL'] ?? '';
assertLocalDatabaseUrl(endpoint);
if (!new URL(endpoint).pathname.startsWith('/lookup_')) throw new Error('owned lookup database required');
const directory = path.resolve(requiredOption(args, 'data-dir'));
const output = path.resolve(requiredOption(args, 'output'));
const base = new URL(option(args, 'base-url') ?? 'http://127.0.0.1:3101');
const metricsUrl = new URL(option(args, 'metrics-url') ?? 'http://127.0.0.1:9093/metrics');
const broker = new URL(option(args, 'broker') ?? 'ws://127.0.0.1:9003');
for (const url of [base, metricsUrl, broker]) {
  if (!['localhost','127.0.0.1','[::1]'].includes(url.hostname)) throw new Error('owned loopback endpoints required');
}
const count = Number(option(args, 'limit') ?? 72000), rate = Number(option(args, 'rate') ?? 30);
const budgetMs = Number(option(args,'budget-ms') ?? 250);
if (!Number.isSafeInteger(count) || count < 30 || count > 108000 || rate !== 30) throw new Error('use 30 packets/s and 30..108000 unique packets');
if (!Number.isFinite(budgetMs)||budgetMs<=0) throw new Error('positive lookup budget required');
const manifest = JSON.parse(await readFile(path.join(directory, 'fixture.json'), 'utf8'));
if (!manifest.synthetic || manifest.count !== count) throw new Error('matching invented fixture required');
const table = await readGzipCsv(path.join(directory, 'packets.csv.gz'));
const hashes = table.rows.map(row => rowObject(table.headers,row)['packet_hash']!);
const expected = new Set(hashes);
if (hashes.length !== count || expected.size !== count) throw new Error('fixture must contain exactly the requested distinct hashes');
const receiver = '0102' + '11'.repeat(30);
const pool = new pg.Pool({ connectionString: endpoint, max: 1, statement_timeout: 30_000 });
const wait = (ms: number) => new Promise(resolve => setTimeout(resolve,ms));
const percentile = (values: number[], q: number) => [...values].sort((a,b) => a-b)[Math.ceil(values.length*q)-1] ?? null;
async function metrics() {
  const response = await fetch(metricsUrl, { signal: AbortSignal.timeout(15_000) });
  if (!response.ok) throw new Error(`metrics HTTP ${response.status}`);
  const text = await response.text();
  if (text.length > 2*1024*1024) throw new Error('metrics byte budget exceeded');
  return new Map(text.split('\n').flatMap(line => {
    const match = /^(meshcore_[a-zA-Z0-9_]+(?:\{[^}]*\})?) ([0-9.eE+\-]+)$/.exec(line);
    return match ? [[match[1]!,Number(match[2])]] : [];
  }));
}
async function counts() {
  return (await pool.query(`SELECT (SELECT count(*)::integer FROM mesh.packets) packets,
    (SELECT count(*)::integer FROM mesh.packet_paths) paths,
    (SELECT count(*)::integer FROM mesh.multibyte_path_facts) facts,
    (SELECT count(*)::integer FROM mesh.ingest_observation_ledger) ledger`)).rows[0]!;
}
const samples: {elapsedMs: number; values: Record<string,number>}[] = [];
const reads: {name: string; ms: number; status: number; bytes: number; entries: number|null}[] = [];
const seen = new Set<string>();
let ws: WebSocket | undefined, readWork: Promise<void> | undefined, timer: ReturnType<typeof setInterval> | undefined;
let failure: unknown;
try {
  for (let index=0; index<hashes.length; index+=3000) {
    const existing = await pool.query('SELECT 1 FROM mesh.packets WHERE packet_hash=ANY($1::text[]) LIMIT 1',[hashes.slice(index,index+3000)]);
    if (existing.rows.length) throw new Error('fixture hashes already present; generate a different --sequence-start');
  }
  const node = (await pool.query(`SELECT node_id,name FROM core.node_identity_nodes WHERE name NOT LIKE '%🚫%'
    AND role=2 AND lat BETWEEN 5 AND 90 AND network='ukmesh' ORDER BY advert_count DESC,node_id LIMIT 1`)).rows[0];
  const packet = (await pool.query(`SELECT packet_hash FROM mesh.packets WHERE visibility_ok IS TRUE AND is_private IS NOT TRUE
    ORDER BY time DESC,packet_hash LIMIT 1`)).rows[0];
  const observer = (await pool.query(`SELECT rx_node_id FROM mesh.packets WHERE src_node_id IS NOT NULL AND rx_node_id IS NOT NULL
    AND time>NOW()-INTERVAL '7 days'
    GROUP BY rx_node_id ORDER BY count(*) DESC LIMIT 1`)).rows[0];
  if (!node || !packet || !observer) throw new Error('retained public lookup fixture required');
  const routes = [
    ['map','/api/nodes/map?network=ukmesh'],
    ['search','/api/nodes/map?network=ukmesh&q='+encodeURIComponent(node.name)],
    ['missing-search','/api/nodes/map?network=ukmesh&q=zz_lookup_missing_zz'],
    ['observer-map','/api/nodes/map?network=ukmesh&observer='+observer.rx_node_id],
    ['history',`/api/nodes/${receiver}/history?network=ukmesh&hours=24`],
    ['packet',`/api/packets/${packet.packet_hash}?network=ukmesh`],
    ['feed','/api/packets/recent?network=ukmesh&fields=slim&limit=200'],
    ['links',`/api/nodes/${node.node_id}/links?network=ukmesh`],
  ];
  const before = await counts(), beforeMetrics = await metrics();
  const walBefore = (await pool.query('SELECT pg_current_wal_lsn()::text lsn')).rows[0].lsn;
  const socket = new URL('/ws?network=ukmesh',base); socket.protocol='ws:';
  ws = new WebSocket(socket,{origin:'http://127.0.0.1:5173'});
  let wsErrors = 0;
  ws.on('error',()=>{ wsErrors++; });
  ws.on('message',bytes=>{
    for (const line of bytes.toString().split('\n').filter(Boolean)) {
      const value=JSON.parse(line), hash=value.data?.packetHash;
      if (value.type==='packet' && expected.has(hash)) seen.add(hash);
    }
  });
  await new Promise<void>((resolve,reject)=>{
    const deadline=setTimeout(()=>reject(new Error('WebSocket did not open')),15_000);
    ws!.once('open',()=>{clearTimeout(deadline);resolve();});
    ws!.once('error',error=>{clearTimeout(deadline);reject(error);});
  });
  const started=performance.now();
  let lastBackground=0, metricsFailures=0;
  const readBurst=async()=>{
    const selected=routes.map(route=>[...route]);
    if (performance.now()-started-lastBackground>=60_000) {
      selected[selected.length-1]=['background-charts','/api/stats/charts?network=ukmesh'];
      lastBackground=performance.now()-started;
    }
    // Eight total HTTP slots, including background work; no overlapping bursts.
    await Promise.all(selected.map(async ([name,route])=>{
      const at=performance.now(); let status=0,bytes=0,entries: number|null=null;
      try {
        const response=await fetch(new URL(route!,base),{signal:AbortSignal.timeout(15_000)});
        status=response.status;
        const body=await response.arrayBuffer(); bytes=body.byteLength;
        if (bytes>8*1024*1024) throw new Error('response budget exceeded');
        const value=JSON.parse(Buffer.from(body).toString());
        entries=Array.isArray(value)?value.length:Array.isArray(value?.nodes)?value.nodes.length:null;
      } catch { status=0; }
      reads.push({name:name!,ms:performance.now()-at,status,bytes,entries});
    }));
    try {
      const values=await metrics();
      samples.push({elapsedMs:performance.now()-started,values:Object.fromEntries([...values].filter(([key])=>
        /resident_memory|heap_size_used|eventloop|queue_depth|mqtt_ingest_active|cache_entries|cache_bytes|mqtt_dedup_entries|mqtt_pre_resolve_history_entries|mqtt_post_persist_inflight/.test(key)))});
    } catch { metricsFailures++; }
  };
  timer=setInterval(()=>{ if (!readWork) readWork=readBurst().finally(()=>{readWork=undefined;}); },5000);
  let acknowledgedMs=0,drained=false;
  try {
    await replaySampleMqtt(['--data-dir',directory,'--limit',String(count),'--speed',String(rate),'--broker',broker.href,'--qos','1']);
    acknowledgedMs=performance.now()-started;
    const deadline=Date.now()+120_000;
    while (Date.now()<deadline) {
      const current=await metrics(), persisted=await counts();
      if (persisted.packets-before.packets===count && current.get('meshcore_packet_batch_queue_depth')===0
        && current.get('meshcore_mqtt_ingest_queue_depth')===0 && current.get('meshcore_mqtt_ingest_active')===0) {
        drained=true;break;
      }
      await wait(1000);
    }
  } catch (error) { failure=error; }
  finally { clearInterval(timer);timer=undefined;await readWork; }
  const after=await counts(), afterMetrics=await metrics();
  const elapsedMs=performance.now()-started;
  const walBytes=Number((await pool.query('SELECT pg_wal_lsn_diff(pg_current_wal_lsn(),$1)::text bytes',[walBefore])).rows[0].bytes);
  const deltas=Object.fromEntries(Object.entries(after).map(([key,value])=>[key,Number(value)-Number(before[key])]));
  const failures=reads.filter(row=>row.status!==200);
  const latency=[...new Set(reads.map(row=>row.name))].map(name=>{
    const rows=reads.filter(row=>row.name===name);
    return {name,requests:rows.length,failures:rows.filter(row=>row.status!==200).length,
      p50Ms:percentile(rows.map(row=>row.ms),.5),p95Ms:percentile(rows.map(row=>row.ms),.95),p99Ms:percentile(rows.map(row=>row.ms),.99)};
  });
  const badOutcomes=[...afterMetrics].filter(([key,value])=>/mqtt_ingest_outcomes_total/.test(key)
    && /failure|quarantined|invalid|rejected/.test(key) && value>(beforeMetrics.get(key)??0));
  const dropped=[...afterMetrics].filter(([key,value])=>/post_persist_dropped_total/.test(key) && value>(beforeMetrics.get(key)??0));
  const dedupMaximum=Math.max(0,...samples.flatMap(sample=>Object.entries(sample.values)
    .filter(([key])=>key.startsWith('meshcore_mqtt_dedup_entries')).map(([,value])=>value)));
  const cacheViolations: {key:string;value:number;limit:number}[]=[];
  for (const policy of CACHE_POLICY_REGISTRY.filter(policy=>policy.disposition==='bounded-cache')) {
    const label=boundedCacheMetricLabel(policy.name);
    for (const [metric,limit] of [['entries',policy.maxEntries],['bytes',policy.maxWeight]] as const) {
      if (limit===undefined) continue;
      const key=`meshcore_cache_${metric}{cache="${label}"}`;
      const value=Math.max(0,...samples.map(sample=>sample.values[key]??0));
      if (value>limit) cacheViolations.push({key,value,limit});
    }
  }
  // Compare the low-water marks after warmup, allowing GC/allocator variation.
  // A short smoke reports bounds; a full run must also show a stable plateau.
  const memoryKeys=[...new Set(samples.flatMap(sample=>Object.keys(sample.values)))]
    .filter(key=>/resident_memory_bytes$|heap_size_used_bytes$/.test(key));
  const memory=memoryKeys.map(key=>{
    const values=samples.map(sample=>sample.values[key]).filter((value):value is number=>value!==undefined);
    const middle=samples.filter(sample=>sample.elapsedMs>=elapsedMs*.5&&sample.elapsedMs<elapsedMs*.75)
      .map(sample=>sample.values[key]).filter((value):value is number=>value!==undefined);
    const final=samples.filter(sample=>sample.elapsedMs>=elapsedMs*.75)
      .map(sample=>sample.values[key]).filter((value):value is number=>value!==undefined);
    const middleLow=percentile(middle,.1),finalLow=percentile(final,.1),maximum=Math.max(0,...values);
    const growth=middleLow===null||finalLow===null?null:finalLow-middleLow;
    const plateau=count<72000||(growth!==null&&growth<=Math.max(64*1024*1024,(middleLow??0)*.2));
    return {key,maximum,middleLow,finalLow,growth,plateau,withinBound:maximum<=1024*1024*1024};
  });
  const memoryPassed=memory.length===2&&memory.every(value=>value.plateau&&value.withinBound);
  const passed=!failure&&drained&&Object.values(deltas).every(value=>value===count)
    && failures.length===0 && metricsFailures===0 && wsErrors===0 && seen.size===count
    && badOutcomes.length===0 && dropped.length===0 && dedupMaximum<=50_000
    && cacheViolations.length===0 && memoryPassed
    && latency.filter(row=>row.name!=='background-charts').every(row=>(row.p95Ms??Infinity)<=budgetMs);
  await mkdir(output,{recursive:true});
  await writeFile(path.join(output,'soak.json'),JSON.stringify({version:1,at:new Date().toISOString(),passed,
    commit:execFileSync('git',['rev-parse','HEAD'],{encoding:'utf8'}).trim(),
    diffSha256:createHash('sha256').update(execFileSync('git',['diff','HEAD'])).digest('hex'),
    settings:{count,rate,budgetMs,httpConcurrency:8,readIntervalMs:5000,backgroundIntervalMs:60000,fixture:manifest},
    elapsedMs,acknowledgedMs,drained,before,after,deltas,walBytes,walBytesPerPacket:walBytes/count,
    wsPackets:seen.size,wsErrors,metricsFailures,dedupMaximum,cacheViolations,memory,memoryPassed,latency,failures,badOutcomes,dropped,
    beforeMetrics:Object.fromEntries(beforeMetrics),afterMetrics:Object.fromEntries(afterMetrics),samples,
    error:failure instanceof Error?failure.message:failure?String(failure):null},null,2)+'\n');
  console.log(JSON.stringify({passed,elapsedMs,deltas,wsPackets:seen.size,httpFailures:failures.length,output}));
  if (!passed) throw new Error('mixed lookup/ingest qualification failed; see saved evidence');
} finally { if(timer)clearInterval(timer);await readWork;ws?.terminate();await pool.end(); }
