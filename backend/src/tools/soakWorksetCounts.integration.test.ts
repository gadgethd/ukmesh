import assert from 'node:assert/strict';
import test from 'node:test';
import pg from 'pg';
import { assertLocalDatabaseUrl } from './sampleData.js';
import { SOAK_HASH_AUDIT_SQL,SOAK_WORKSET_COUNTS_SQL } from './soakWorksetCounts.js';

const endpoint=process.env['TEST_INGEST_DATABASE_URL'];
const columns='"time" timestamptz,packet_hash text,network text,rx_node_id text,topic text,observation_id uuid';
const fixtures=`WITH fixture_packets AS (SELECT * FROM jsonb_to_recordset($2::jsonb) AS r(${columns})),
fixture_paths AS (SELECT * FROM jsonb_to_recordset($3::jsonb) AS r(${columns})),
fixture_facts AS (SELECT * FROM jsonb_to_recordset($4::jsonb) AS r(${columns})),
fixture_ledger AS (SELECT * FROM jsonb_to_recordset($5::jsonb) AS r(${columns})), `;
const native=(sql:string)=>fixtures+sql.replace(/^WITH /,'').replaceAll('mesh.packets','fixture_packets')
  .replaceAll('mesh.packet_paths','fixture_paths').replaceAll('mesh.multibyte_path_facts','fixture_facts')
  .replaceAll('mesh.ingest_observation_ledger','fixture_ledger');
type Row={time:string;packet_hash:string;network:string;rx_node_id:string|null;topic:string;observation_id:string|null};
const hash=(letter:string)=>letter.repeat(64);
const row=(id:number,h=hash('A'),rx='observer',network='ukmesh'):Row=>({
  time:'2026-11-15T00:00:00.000Z',packet_hash:h,network,rx_node_id:rx,topic:'meshcore/LHR/'+rx+'/packets',
  observation_id:`00000000-0000-4000-8000-${String(id).padStart(12,'0')}`});
const natural=(r:Row)=>JSON.stringify([new Date(r.time).toISOString(),r.packet_hash,r.network,r.rx_node_id??'',r.topic]);
const identity=(r:Row)=>JSON.stringify([r.observation_id,r.packet_hash,r.network]);

test('read-only VALUES prove packet-derived and independent hash counts across duplicates, children and orphans',{
  skip:endpoint?false:'TEST_INGEST_DATABASE_URL is not configured',
},async()=>{
  assertLocalDatabaseUrl(endpoint!);
  const url=new URL(endpoint!);
  if(url.port==='5433')assert.match(url.pathname,/^\/lean_(?:fixture|1x|10x|verify|soak)_/,'main local cluster requires owned fixture');
  const pool=new pg.Pool({connectionString:endpoint,max:1,statement_timeout:30000});
  const client=await pool.connect();
  try{
    await client.query('BEGIN READ ONLY');
    assert.equal((await client.query("SELECT current_setting('transaction_read_only') value")).rows[0].value,'on');
    const a=row(1),b=row(2,hash('A'),'second'),aux=row(3,hash('B'),'aux'),unrelated=row(4,hash('Z'));
    const orphan=row(9,hash('B'),'orphan');
    const cases=[
      {name:'empty',wanted:[],tables:[[],[],[],[]]},
      {name:'duplicate input hashes and multi-observer rows',wanted:[hash('A'),hash('A')],tables:[[a,b],[a,b],[a,b],[a,b]]},
      {name:'same-observer redelivery is one committed observation',wanted:[hash('A'),hash('A')],tables:[[a],[a],[a],[a]]},
      {name:'auxiliary observations have zero path/facts while all ledger rows remain',wanted:[hash('A'),hash('B')],tables:[[a,aux],[a],[a],[a,aux]]},
      {name:'unrelated rows cannot mask missing owned children',wanted:[hash('A')],tables:[[a,unrelated],[a,unrelated],[unrelated],[a,unrelated]]},
      {name:'wrong child hash or network fails parent-key identity',wanted:[hash('A')],tables:[[a],[a],[{...a,network:'other'}],[{...a,packet_hash:hash('Z')}]]},
      {name:'independent audit retains orphan children excluded from parent scope',wanted:[hash('A'),hash('B')],tables:[[a],[a,orphan],[a,orphan],[a,orphan]]},
      {name:'shared natural path is counted once across observation IDs',wanted:[hash('A')],tables:[[a,{...a,observation_id:b.observation_id}],[a],[a,{...a,observation_id:b.observation_id}],[a,{...a,observation_id:b.observation_id}]]},
      {name:'one observation identity shared by different parent times counts its children once',wanted:[hash('A')],tables:[
        [a,{...a,time:'2026-11-15T00:00:01.000Z'}],[a,{...a,time:'2026-11-15T00:00:01.000Z'}],[a],[a]]},
      {name:'same-hash orphan observations remain visible to the independent audit',wanted:[hash('A'),hash('A')],tables:[
        [a],[a,{...orphan,packet_hash:hash('A')}],[a,{...orphan,packet_hash:hash('A')}],[a,{...orphan,packet_hash:hash('A')}]]},
      {name:'nullable observer path identity remains stable',wanted:[hash('A')],tables:[[{...a,rx_node_id:null}],[{...a,rx_node_id:''}],[a],[a]]},
    ] as {name:string;wanted:string[];tables:Row[][]}[];
    for(const fixture of cases){
      const wanted=new Set(fixture.wanted),parents=fixture.tables[0]!.filter(r=>wanted.has(r.packet_hash));
      const paths=new Set(parents.map(natural)),ids=new Set(parents.filter(r=>r.observation_id!==null).map(identity));
      const expected={packets:parents.length,paths:fixture.tables[1]!.filter(r=>paths.has(natural(r))).length,
        facts:fixture.tables[2]!.filter(r=>ids.has(identity(r))).length,ledger:fixture.tables[3]!.filter(r=>ids.has(identity(r))).length};
      const independent=Object.fromEntries(['packets','paths','facts','ledger'].map((key,index)=>
        [key,fixture.tables[index]!.filter(r=>wanted.has(r.packet_hash)).length]));
      const params=[fixture.wanted,...fixture.tables.map(t=>JSON.stringify(t))];
      assert.deepEqual((await client.query(native(SOAK_WORKSET_COUNTS_SQL),params)).rows[0],expected,fixture.name);
      assert.deepEqual((await client.query(native(SOAK_HASH_AUDIT_SQL),params)).rows[0],independent,fixture.name);
      if(fixture.name.includes('orphan'))assert.notDeepEqual(expected,independent,'final orphan audit must remain independent');
    }
  }finally{await client.query('ROLLBACK');client.release();await pool.end();}
});
