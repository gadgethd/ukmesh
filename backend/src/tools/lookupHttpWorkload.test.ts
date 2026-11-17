import assert from 'node:assert/strict';
import type { RequestHandler,Router } from 'express';
import test from 'node:test';
import { loadLookupHttpWorkload } from './lookupHttpWorkload.js';
import { createLookupRequestFleet } from './lookupHttpRequests.js';
import type { LookupRuntime } from './lookupBenchmarkSession.js';

test('shared fixture cases and selected routes retain identities, scopes, content and cleanup for either source',async t=>{
  const keys=['DATABASE_URL','MESHCORE_HTTP_DATABASE_URL','DATABASE_ROLE_ENFORCEMENT'] as const;
  const previous=Object.fromEntries(keys.map(key=>[key,process.env[key]]));
  t.after(()=>{for(const key of keys){if(previous[key]===undefined)delete process.env[key];else process.env[key]=previous[key];}});
  process.env['DATABASE_URL']=process.env['MESHCORE_HTTP_DATABASE_URL']='postgresql://fixture@127.0.0.1/lookup_fixture';
  process.env['DATABASE_ROLE_ENFORCEMENT']='false';
  for(const selected of ['baseline','candidate']){
    const id='1'.repeat(64),packet='2'.repeat(64),observer='3'.repeat(64),events:string[]=[];
    const visible=Array.from({length:1001},()=>({node_id:id}));
    const query=async(text:string)=>({rows:text.includes('current_setting')?[{current_user:'fixture',session_user:'fixture',database:'lookup_fixture',backend_pid:123,
        transaction_read_only:'on',default_transaction_read_only:'on',statement_timeout:'30s',search_path:'public',superuser:false,bypass_rls:false}]:
      text.includes('SELECT count(*)::text FROM core.nodes')?[{nodes:'3',observations:'20'}]:
      text.includes('FROM core.node_identity_nodes')?[{node_id:id,name:'Visible Name'}]:
      text.includes('SELECT packet_hash FROM mesh.packets')?[{packet_hash:packet}]:
      text.includes('SELECT p.rx_node_id')?[{rx_node_id:observer}]:[{region:'GB-SCT'}]});
    const noop:RequestHandler=(_req,_res,next)=>next();
    const modules:Record<string,unknown>={
      'repositories/nodes.ts':{createNodeRepository:()=>({listPublicMapRows:async()=>visible})},
      'repositories/nodeReads.ts':{},'repositories/packetReads.ts':{},
      'api/routes/nodes.ts':{registerNodeRoutes:(router:Router)=>{router.get('/nodes/map',(req,res)=>res.json({
        nodes:[{selected}],page:{snapshot:req.query.snapshot}}));return()=>{events.push('routes');};}},
      'api/routes/misc.ts':{registerMiscRoutes:()=>{}},
      'api/errors.ts':{wrapAsyncHandlers:()=>{},apiErrorMiddleware:noop},
      'platform/query/networkFilters.ts':{networkFilters:()=>({network:'ukmesh'})},
      'privacy/responseGeneration.ts':{readPublicResponseGeneration:async()=>1},
      'api/bootstrap/limiters.ts':{createGlobalApiLimiter:()=>noop,NODES_LIMITER:noop,PACKET_DETAIL_LIMITER:noop},
      'api/bootstrap/caches.ts':{NODE_LINKS_CACHE_TTL_MS:1,INFERRED_NODES_CACHE_TTL_MS:2,createApiCaches:()=>({
        nodeLinksCache:new Map(),nodeLinksInflight:new Map(),inferredNodesCache:new Map(),inferredNodesInflight:new Map(),
        close(){events.push('caches');}})},
    };
    const before=process.env['PUBLIC_FEATURE_REGION_SCOPING_ENABLED'];process.env['PUBLIC_FEATURE_REGION_SCOPING_ENABLED']='true';
    const fleet=createLookupRequestFleet();
    try{
      const workload=await loadLookupHttpWorkload(async file=>modules[file],{pool:{query},query} as unknown as LookupRuntime);
      const prepared=await workload.prepare('2026-11-14T00:00:00Z');
      assert.equal(prepared.databaseSetup.http.current_user,'fixture');
      assert.equal(prepared.databaseSetup.http.transaction_read_only,'on');
      assert.equal(prepared.cases.length,12);assert.equal(prepared.cases[9]!.expected,404);
      assert.ok(prepared.cases[1]!.route.includes('q=Visible%20Name'));
      assert.ok(prepared.cases[5]!.route.includes('observer='+observer));
      assert.ok(prepared.cases[10]!.route.includes('region=GB-SCT'));assert.ok(prepared.cases[11]!.route.includes('cursor='));
      await assert.rejects(workload.withTrial(async port=>{
        const result=await fleet.request(port,prepared.cases[0]!);
        assert.equal(result.entries,1);assert.equal(result.status,200);assert.match(result.digest,/^[a-f0-9]{64}$/);
        throw new Error('owned trial failure');
      }),/owned trial failure/);
      assert.deepEqual(events,['routes','caches']);
    }finally{await fleet.close();if(before===undefined)delete process.env['PUBLIC_FEATURE_REGION_SCOPING_ENABLED'];else process.env['PUBLIC_FEATURE_REGION_SCOPING_ENABLED']=before;}
  }
});
