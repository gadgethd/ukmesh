/** Compare retained data without exporting node names, messages or identities. */
import { createHash } from 'node:crypto';
import { mkdir, writeFile } from 'node:fs/promises';
import path from 'node:path';
import { pathToFileURL } from 'node:url';
import pg from 'pg';
import { createNodeRepository } from '../repositories/nodes.js';
import { publicIdentityNodeSourceSql } from '../nodes/identitySource.js';
import { PUBLIC_MAP_ALLOWED_FIELDS } from '../nodes/publicMap.js';
import { networkFilters } from '../platform/query/networkFilters.js';
import { combinedTopologyRows } from '../repositories/networkAnalysis.js';
import { assertLocalDatabaseUrl, requiredOption } from './sampleData.js';

const args=process.argv.slice(2), endpoint=process.env['DATABASE_URL']??'';
assertLocalDatabaseUrl(endpoint);
const baseline=path.resolve(requiredOption(args,'baseline-source-root'));
const output=path.resolve(requiredOption(args,'output'));
const oldModule=await import(pathToFileURL(path.join(baseline,'repositories/nodes.ts')).href);
const oldFiltersModule=await import(pathToFileURL(path.join(baseline,'platform/query/networkFilters.ts')).href);
const pool=new pg.Pool({connectionString:endpoint,max:1,statement_timeout:60_000});
const client=await pool.connect();
const cases: {name: string; rows: number; digest: string}[]=[];
try {
  await client.query('BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY');
  const query=<T extends pg.QueryResultRow>(sql:string,params?:unknown[])=>client.query<T>(sql,params);
  const projection=(await query(`WITH original AS MATERIALIZED (SELECT * FROM core.node_identity_nodes),
    candidate AS MATERIALIZED (SELECT * FROM ${publicIdentityNodeSourceSql()} n)
    SELECT (SELECT count(*)::integer FROM original) original,
      (SELECT count(*)::integer FROM candidate) candidate,
      (SELECT count(DISTINCT node_id)::integer FROM candidate) unique_candidate,
      (SELECT count(*)::integer FROM original o FULL JOIN candidate n USING(node_id)
        WHERE to_jsonb(o) IS DISTINCT FROM to_jsonb(n)) differences`)).rows[0]!;
  if (projection.original!==projection.candidate||projection.candidate!==projection.unique_candidate||projection.differences!==0) {
    throw new Error('full canonical projection differs from existing view');
  }
  const original=oldModule.createNodeRepository(query), candidate=createNodeRepository(query);
  const snapshot=(await query('SELECT NOW()::text AS time')).rows[0]!.time;
  const sample=await candidate.listPublicMapRows(['node_id','name'],networkFilters('ukmesh'),snapshot,null,20);
  if (!sample.length) throw new Error('public map fixture is empty');
  const observer=(await query(`SELECT rx_node_id FROM mesh.packets WHERE src_node_id IS NOT NULL
    AND rx_node_id IS NOT NULL AND time>NOW()-INTERVAL '7 days'
    GROUP BY rx_node_id ORDER BY count(*) DESC,rx_node_id LIMIT 1`)).rows[0]?.rx_node_id as string|undefined;
  const regions=(await query(`SELECT region FROM core.region_scope_members GROUP BY region ORDER BY count(*) DESC LIMIT 1`)).rows;
  const scopes=[{name:'ukmesh',network:'ukmesh'}, {name:'northeast',network:'northeast'}, {name:'test',network:'test'},
    ...(observer?[{name:'observer',network:'ukmesh',observer}]:[]),
    ...regions.map(row=>({name:'region',network:'ukmesh',region:row.region as string}))];
  const digest=(rows: unknown)=>createHash('sha256').update(JSON.stringify(rows)).digest('hex');
  for (const scope of scopes as {name:string;network:string;observer?:string;region?:string}[]) {
    const filters=networkFilters(scope.network,scope.observer,scope.region?{region:scope.region}:undefined);
    const beforeFilters=oldFiltersModule.networkFilters(scope.network,scope.observer,scope.region?{region:scope.region}:undefined);
    const terms=scope.region?[undefined]:[undefined,'zz_lookup_missing_zz','%','_','\\','café',
      String(sample[0]!['name']??'').slice(0,10)||'missing',String(sample[0]!['node_id']).slice(0,12)];
    for (const [termIndex,search] of terms.entries()) for (const [fieldIndex,fields] of [
      ['node_id','name','last_seen','is_online'], [...PUBLIC_MAP_ALLOWED_FIELDS],
    ].entries()) for (const [cursorIndex,cursor] of [null,String(sample[10]?.['node_id']??sample[0]!['node_id'])].entries()) {
      const before=await original.listPublicMapRows(fields,beforeFilters,snapshot,cursor,1000,search);
      const after=await candidate.listPublicMapRows(fields,filters,snapshot,cursor,1000,search);
      if (digest(before)!==digest(after)) throw new Error(`map parity failed: ${scope.name}/${termIndex}/${fieldIndex}/${cursorIndex}`);
      cases.push({name:`${scope.name}/${termIndex}/${fieldIndex}/${cursorIndex}`,rows:after.length,digest:digest(after)});
    }
  }
  for (const [index,node] of sample.slice(0,8).entries()) {
    const before=await original.listNodeLinks(String(node['node_id']),oldFiltersModule.networkFilters('ukmesh'));
    const after=await candidate.listNodeLinks(String(node['node_id']),networkFilters('ukmesh'));
    // The existing contract orders by observed_count; ties have no secondary
    // ordering. Compare complete peer records and independently retain that order.
    for (let at=1;at<after.length;at++) if(after[at-1]!.observed_count<after[at]!.observed_count) throw new Error('link ordering changed');
    const sorted=(rows: {peer_id:string}[])=>[...rows].sort((a,b)=>a.peer_id.localeCompare(b.peer_id));
    if(digest(sorted(before))!==digest(sorted(after))) throw new Error(`link parity failed: case ${index}`);
    cases.push({name:`links/${index}`,rows:after.length,digest:digest(sorted(after))});
  }
  if (args.includes('--topology')) {
    const oldTopology=await import(pathToFileURL(path.join(baseline,'repositories/networkAnalysis.ts')).href);
    const before=await oldTopology.combinedTopologyRows(query,oldFiltersModule.networkFilters('ukmesh'),100000);
    const after=await combinedTopologyRows(query,networkFilters('ukmesh'),100000);
    // All eligible links, without a tied-rank cutoff. Equal-ranked links have
    // never had a secondary ordering in the public contract.
    const sorted=(rows: {node_a_id:string;node_b_id:string}[])=>[...rows].sort((a,b)=>
      a.node_a_id.localeCompare(b.node_a_id)||a.node_b_id.localeCompare(b.node_b_id));
    if (digest(sorted(before.links))!==digest(sorted(after.links))) throw new Error('topology link parity failed');
    if (digest(before.standalone)!==digest(after.standalone)) throw new Error('topology standalone parity failed');
    cases.push({name:'topology',rows:after.links.length+after.standalone.length,digest:digest(after)});
  }
  await mkdir(output,{recursive:true});
  await writeFile(path.join(output,'parity.json'),JSON.stringify({version:1,passed:true,
    at:new Date().toISOString(),projection,scopeCount:scopes.length,caseCount:cases.length,
    note:'Shared read-only repeatable-read transaction, complete fields and independent baseline repository. Region search is covered by native canonical-view tests because the previous regional projection omitted history source IDs.',cases},null,2)+'\n');
  console.log(JSON.stringify({passed:true,projection,cases:cases.length,output}));
} finally {await client.query('ROLLBACK');client.release();await pool.end();}
