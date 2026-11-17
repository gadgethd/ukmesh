import assert from 'node:assert/strict';
import test from 'node:test';
import { readFile } from 'node:fs/promises';
import path from 'node:path';
import { pathToFileURL } from 'node:url';
import pg from 'pg';
import { createDisposableDatabase } from '../db/fixtures/disposableDatabase.js';
import { bootstrapBaseSchemaFixture } from '../db/fixtures/baseSchema.js';
import { identityNodeSourceSql, publicIdentityNodeSourceSql, pagedIdentityNodeSourceSql } from '../nodes/identitySource.js';
import { mapVisiblePredicate, PUBLIC_MAP_ALLOWED_FIELDS } from '../nodes/publicMap.js';
import { nodeEffectiveLastSeenSql, nodeEffectiveOnlineSql } from '../nodes/presence.js';
import { networkFilters, nodeAliasArraySql, publicPacketPrivacySql } from '../platform/query/networkFilters.js';
import { convergePacketVisibility } from '../privacy/convergence.js';
import { createNodeRepository } from './nodes.js';
import { createDatabaseRuntime } from '../db/databaseRuntime.js';
import { mockRuntimeQuery } from '../db/fixtures/mockRuntimeQuery.js';
import { getViableLinks } from './topologyReads.js';

const endpoint = process.env['TEST_SAMPLE_DATABASE_URL'];

test('indexed node projections and search retain canonical view results for aliases, collisions and scopes', {
  skip: endpoint ? false : 'owned PostgreSQL fixture required', timeout: 180_000,
}, async t => {
  const fixture = await createDisposableDatabase(t, endpoint!, 'lookup_parity');
  const pool = new pg.Pool({ connectionString: fixture.url, max: 2 });
  fixture.addCleanup(() => pool.end());
  await bootstrapBaseSchemaFixture(pool);
  const runtime = createDatabaseRuntime({ DATABASE_URL: fixture.url, DATABASE_POOL_MAX: '1' });
  fixture.addCleanup(() => runtime.close());
  const query = runtime.query;
  const key = (hex: string) => hex.repeat(64);
  const snapshot = new Date().toISOString();
  for (const [id, nodeName, network, adverts, role, lat, lon] of [
    [key('1'), 'Canonical', 'ukmesh', 5, 2, 51, -1],
    [key('2'), 'Former 100%_\\ café', 'northeast', 20, 2, 52, -2],
    [key('3'), 'Source without canonical row', 'ukmesh', 0, 2, 53, -3],
    [key('a'), 'Lower case', 'ukmesh', 1, 2, 54, -4],
    [key('A'), 'Upper case', 'ukmesh', 2, 2, 55, -5],
    [' ' + key('a') + ' ', 'Whitespace legacy row', 'ukmesh', 3, 2, 56, -6],
    [key('6'), 'Chain source', 'ukmesh', 4, 2, 57, -7],
    [key('7'), 'Chain middle', 'ukmesh', 3, 2, 58, -8],
    [key('8'), 'Chain end', 'ukmesh', 2, 2, 59, -9],
    [key('9'), 'Private 🚫', 'ukmesh', 1, 2, 51, -2],
    [key('b'), 'Test fixture', 'test', 1, 2, 51, -3],
    [key('c'), null, 'ukmesh', 0, null, null, null],
    ['AAAB' + '1'.repeat(60), 'Ambiguous prefix one', 'ukmesh', 1, 2, 51, -1],
    ['AAAB' + '2'.repeat(60), 'Ambiguous prefix two', 'ukmesh', 1, 2, 52, -2],
    ['AAﬀb' + '1'.repeat(59), 'Legacy expanded key', 'ukmesh', 1, 2, 51, -1],
    ['AAFF' + 'C'.repeat(60), 'Expanded prefix peer', 'ukmesh', 1, 2, 52, -2],
  ]) {
    await pool.query(`INSERT INTO core.nodes(node_id,name,network,advert_count,role,lat,lon,last_seen,
      last_status_at,is_online,created_at) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$8,false,$8)`,
    [id, nodeName, network, adverts, role, lat, lon, snapshot]);
  }
  for (const [source, canonical] of [['2', '1'], ['3', '4'], ['6', '7'], ['7', '8']]) {
    await pool.query(`INSERT INTO core.node_identity_aliases(source_node_id,canonical_node_id,confidence,reason)
      VALUES($1,$2,'high','invented lookup parity fixture')`, [key(source!), key(canonical!)]);
  }
  await pool.query(`INSERT INTO core.nodes(node_id,name,network,role,lat,lon,last_seen)
    SELECT UPPER(md5(i::text)||md5('wide:'||i)), 'Wide fixture '||i, 'ukmesh',2,52,-1,$1
    FROM generate_series(1,650) i`, [snapshot]);
  await pool.query("UPDATE core.nodes SET name='Renamed representative' WHERE node_id=$1", [key('2')]);
  for (let index = 0; index < 24; index++) {
    const source = index < 20 ? key('3') : index === 20 ? key('A') : index === 21 ? key('6') : null;
    await pool.query(`INSERT INTO mesh.packets(packet_hash,rx_node_id,src_node_id,topic,topic_prefix,network,
      packet_type,path_hash_size_bytes,path_hashes) VALUES($1,$2,$3,'meshcore/ZZZ/fixture/packets','meshcore','ukmesh',3,1,$4)`,
    [String(index).padStart(64,'0'),key('2'),source,index === 20 ? ['99'] : []]);
  }
  const generation = (await pool.query('SELECT generation::text FROM ops.public_visibility_state WHERE singleton')).rows[0].generation;
  await convergePacketVisibility(pool, { apply: true, expectedGeneration: generation });
  const original = (await pool.query('SELECT * FROM core.node_identity_nodes ORDER BY node_id')).rows;
  const fast = (await pool.query(`SELECT * FROM ${publicIdentityNodeSourceSql()} n ORDER BY node_id`)).rows;
  assert.deepEqual(fast, original, 'every column, representative and aggregate matches the existing view');
  const scoped = (await pool.query(`SELECT * FROM ${identityNodeSourceSql('SELECT unnest($1::text[]) AS node_id')} n
    ORDER BY node_id`, [original.map(row => row.node_id).concat(key('F'))])).rows;
  assert.deepEqual(scoped, original, 'scoped hydration includes absent canonical rows and one-hop alias chains');
  await t.test('viable topology retains strict network/privacy scope across aliases and legacy keys', async inner => {
    const client = await pool.connect();
    try {
      await client.query('BEGIN');
      await client.query(`INSERT INTO core.node_links
        (node_a_id,node_b_id,observed_count,itm_viable,force_viable) VALUES
        ($1,$2,5,true,false),($1,$3,6,true,false),($1,$4,7,true,false),
        ($1,$5,8,true,false),($1,$6,9,false,true)`,
      [key('1'), key('A'), key('3'), key('9'), key('b'), key('c')]);
      await client.query('REFRESH MATERIALIZED VIEW core.node_identity_links');
      mockRuntimeQuery(inner, (sql, params) => client.query(sql, params));
      const links = await getViableLinks('ukmesh');
      assert.deepEqual(links.map(row => [row.node_a_id, row.node_b_id, String(row.observed_count)]).sort(), [
        [key('1'), key('4'), '6'], [key('1'), key('A'), '5'], [key('1'), key('C'), '9'],
      ]);
      for (const network of ['northeast', 'test', 'missing']) {
        assert.deepEqual(await getViableLinks(network), [], `both endpoints must satisfy ${network}`);
      }
    } finally {
      await client.query('ROLLBACK');
      client.release();
    }
  });
  const priorityIds = [key('1'), key('4'), key('7'), key('A')];
  const availableIds = new Set(original.map(row => row.node_id));
  const boundaryIds = [...new Set([...priorityIds.filter(id => availableIds.has(id)), ...availableIds])];
  for (const count of [127, 128, 129]) {
    const ids = boundaryIds.slice(0, count);
    assert.equal(ids.length, count);
    for (const projection of ['adaptive', 'bulk'] as const) {
      for (const rowHydration of ['indexed-points', 'ordered-scan'] as const) {
        for (const pageSize of [20, count + 1]) {
          const parameters = [ids, key('1'), pageSize];
          const source = pagedIdentityNodeSourceSql({
            where: 'n.node_id > $2', limit: '$3',
            candidates: { idsSql: 'SELECT unnest($1::text[]) AS node_id', projection, rowHydration },
          });
          const actual = await pool.query(`SELECT * FROM ${source} n ORDER BY node_id LIMIT $3`, parameters);
          const expected = await pool.query(`SELECT * FROM core.node_identity_nodes
            WHERE node_id=ANY($1::text[]) AND node_id>$2 ORDER BY node_id LIMIT $3`, parameters);
          assert.deepEqual(actual.rows, expected.rows,
            `complete canonical rows at ${count} candidates/${projection}/${rowHydration}/limit ${pageSize}`);
        }
      }
    }
  }

  await pool.query(`CREATE SCHEMA lookup_shadow;
    CREATE FUNCTION lookup_shadow.upper(text) RETURNS text LANGUAGE sql AS $$ SELECT 'wrong'::text $$`);
  const shadowClient = await pool.connect();
  try {
    await shadowClient.query('BEGIN');
    await shadowClient.query('SET LOCAL search_path=lookup_shadow,pg_catalog,public');
    const shadowed = await shadowClient.query('SELECT * FROM core.lookup_node_identity($1)',[key('1')]);
    assert.deepEqual(shadowed.rows,original.filter(row=>row.node_id===key('1')),
      'parsed function body keeps builtin/object resolution independent of the caller search_path');
  } finally { await shadowClient.query('ROLLBACK');shadowClient.release(); }
  for (const prefix of ['1111', '4444', 'AAAA', '7777', '8888', '9999', 'BBBB', 'FFFF', '111111', 'AAAAAA', 'AAAB', 'AAAB11', 'AAFF', 'AAFFB1']) {
    const actual = (await pool.query('SELECT * FROM core.lookup_node_identity_prefix($1) ORDER BY node_id', [prefix])).rows;
    assert.deepEqual(actual, original.filter(row => row.node_id.slice(0, prefix.length) === prefix),
      `prefix ${prefix} retains every canonical match and all representative fields`);
  }
  const beforeDecoder = (await readFile(new URL('../db/migrations/110_repair_decode_multibyte_path.sql', import.meta.url), 'utf8'))
    .replace('public.meshcore_decode_multibyte_path', 'public.lookup_decoder_before');
  await pool.query(beforeDecoder);
  for (const rx of [key('1'), key('2'), key('3'), key('6'), key('A'), null]) {
    for (const hops of [null, -1, 0, 1, 2, 5]) {
      for (const path of [null, [], ['1111', '4444'], ['AAAA', '7777'], ['7777', '8888', '1111'],
        ['1111', '1111'], ['FFFF', '4444'], ['9999', 'AAAA'], ['111111', 'AAAAAA'], ['AAAB', '1111'], ['AAAB11', 'AAAB22'], ['AAFF', '1111'], ['AAFFB1', '111111']]) {
        for (const width of [1, 2, 3]) {
          const params = [rx, hops, width, path];
          const before = await pool.query('SELECT * FROM public.lookup_decoder_before($1,$2,$3,$4)', params);
          const after = await pool.query('SELECT * FROM public.meshcore_decode_multibyte_path($1,$2,$3,$4)', params);
          assert.deepEqual(after.rows, before.rows, `decoder ${rx}/${hops}/${width}/${path}`);
        }
      }
    }
  }

  const repository = createNodeRepository(query);
  await pool.query("INSERT INTO core.region_scope_stats(region) VALUES ('lookup_test')");
  await pool.query(`INSERT INTO core.region_scope_members(region,node_id,packets)
    VALUES ('lookup_test',$1,1),('lookup_test',$2,1),('lookup_test',$3,1)`, [key('2'),key('3'),key('9')]);
  const fields = ['node_id', 'name', 'last_seen', 'is_online'];
  for (const network of ['ukmesh', 'northeast', 'test']) for (const observer of [undefined, key('2')])
  for (const region of network==='ukmesh'&&!observer?[undefined,'lookup_test']:[undefined]) {
    const filters = networkFilters(network, observer, region?{region}:undefined);
    for (const search of [undefined, 'Canonical', 'Former', '%', '_', '\\', '100%_', '%_\\', 'café', 'aaffb', 'A', 'missing', 'Wide']) {
      for (const cursor of [null, key('4')]) {
        const snapshotParam = filters.params.length + 1, cursorParam = snapshotParam + 1;
        const limitParam = cursorParam + 1, searchParam = limitParam + 1;
        // Preserve an independent oracle for the original observation-level
        // observer filter, including private-only sources and repeated rows.
        const originalScope = networkFilters(network,undefined,region?{region}:undefined).nodesAlias('n') + (observer ? ` AND (
          n.node_id=ANY(${nodeAliasArraySql('$2')}) OR n.node_id IN (
            SELECT COALESCE(a.canonical_node_id,UPPER(BTRIM(p.src_node_id))) FROM mesh.packets p
            LEFT JOIN core.node_identity_aliases a ON a.source_node_id=UPPER(BTRIM(p.src_node_id))
            WHERE p.rx_node_id=ANY(${nodeAliasArraySql('$2')}) AND p.time>NOW()-INTERVAL '7 days'
              AND p.src_node_id IS NOT NULL AND p.network ${network === 'ukmesh' ? '= ANY($1)' : '= $1'}
              AND ${publicPacketPrivacySql('p')}
          ))` : '');
        const reference = await pool.query(`SELECT n.node_id,n.name,
          ${nodeEffectiveLastSeenSql('n')}::text AS last_seen,
          ${nodeEffectiveOnlineSql('n', `$${snapshotParam}::timestamptz`)} AS is_online
          FROM core.node_identity_nodes n
          WHERE ${mapVisiblePredicate('n', `$${snapshotParam}::timestamptz`)}
          AND NOT EXISTS(SELECT 1 FROM core.node_map_suppressions s WHERE s.node_id=n.node_id)
          ${originalScope}
          AND ($${cursorParam}::text IS NULL OR n.node_id > $${cursorParam})
          ${search ? `AND (
            STRPOS(LOWER(COALESCE(n.name,'')),LOWER($${searchParam}))>0
            OR STRPOS(LOWER(n.node_id),LOWER($${searchParam}))>0
            OR STRPOS(LOWER(COALESCE(n.iata,'')),LOWER($${searchParam}))>0
            OR EXISTS(SELECT 1 FROM core.node_name_history h JOIN core.nodes s ON s.node_id=h.node_id
              WHERE h.node_id=ANY(n.identity_source_ids) AND h.name NOT LIKE '%🚫%'
              AND (s.name IS NULL OR s.name NOT LIKE '%🚫%')
              AND (s.network IS DISTINCT FROM 'test' OR n.network='test')
              AND STRPOS(LOWER(h.name),LOWER($${searchParam}))>0)
          )` : ''} ORDER BY n.node_id LIMIT $${limitParam}`,
        [...filters.params, snapshot, cursor, 4, ...(search ? [search] : [])]);
        assert.deepEqual(await repository.listPublicMapRows(fields, filters, snapshot, cursor, 3, search),
          reference.rows, `${network}/${observer ? 'observer' : 'all'}/${region??'all regions'}/${search ?? 'no search'}/${cursor ?? 'first page'}`);
      }
    }
  }

  // A private representative may be an alias outside the requested member
  // list. Hydration must still aggregate the whole identity before filtering.
  const privateAlias = 'DD' + 'D'.repeat(62);
  await pool.query(`INSERT INTO core.nodes(node_id,name,network,advert_count,role,lat,lon,last_seen)
    VALUES($1,'Private alias 🚫','ukmesh',100,2,51,-1,$2)`, [privateAlias,snapshot]);
  await pool.query(`INSERT INTO core.node_identity_aliases(source_node_id,canonical_node_id,confidence,reason)
    VALUES($1,$2,'high','owned private representative fixture')`, [privateAlias,key('1')]);
  assert.equal((await query('SELECT name FROM core.node_identity_nodes WHERE node_id=$1',[key('1')])).rows[0]?.['name'],
    'Private alias 🚫','the complete identity selects the private alias outside regional membership');
  const baselineRoot = process.env['TEST_LOOKUP_BASELINE_SOURCE_ROOT'];
  const baselineRepository = baselineRoot
    ? (await import(pathToFileURL(path.join(baselineRoot,'repositories/nodes.ts')).href)).createNodeRepository(query)
    : undefined;
  // Region memberships accept normalized public keys; legacy Unicode IDs
  // remain covered by the general identity/decoder cases above.
  const ids = [...new Set([key('4'),key('1'),key('9'),key('B'),key('A'),key('7'),...boundaryIds])]
    .filter(id=>/^[0-9A-F]{64}$/.test(id));
  let regionalCases = 0;
  for (const count of [0,1,127,128,129,500,ids.length]) {
    const region = `lookup_size_${count}`;
    const members = ids.slice(0,count).map(id => id===key('4') ? key('3') : id===key('1') ? key('2') : id===key('7') ? key('6') : id);
    await pool.query('INSERT INTO core.region_scope_stats(region) VALUES($1)',[region]);
    await pool.query(`INSERT INTO core.region_scope_members(region,node_id,packets)
      SELECT $1,unnest($2::text[]),1`,[region,members]);
    for (const network of ['ukmesh','northeast','test']) {
      const filters = networkFilters(network,undefined,{region});
      for (const requested of [fields,[...PUBLIC_MAP_ALLOWED_FIELDS]]) for (const cursor of [null,key('4')]) {
        for (const limit of [3,1000]) {
          const snapshotParameter=filters.params.length+1,cursorParameter=snapshotParameter+1,limitParameter=cursorParameter+1;
          const projection=requested.map(field=>field==='last_seen' ? `${nodeEffectiveLastSeenSql('n')}::text AS last_seen`
            : field==='is_online' ? `${nodeEffectiveOnlineSql('n',`$${snapshotParameter}::timestamptz`)} AS is_online` : `n.${field}`).join(',');
          const expected = (await query(`SELECT ${projection} FROM core.node_identity_nodes n
            WHERE ${mapVisiblePredicate('n',`$${snapshotParameter}::timestamptz`)}
              AND NOT EXISTS(SELECT 1 FROM core.node_map_suppressions s WHERE s.node_id=n.node_id)
              ${filters.nodesAlias('n')}
              AND ($${cursorParameter}::text IS NULL OR n.node_id>$${cursorParameter})
            ORDER BY n.node_id LIMIT $${limitParameter}`,[...filters.params,snapshot,cursor,limit+1])).rows;
          const actual = await repository.listPublicMapRows(requested,filters,snapshot,cursor,limit);
          assert.deepEqual(actual,expected,`region${count}/${network}/${requested.length}fields/${cursor??'first'}/${limit}`);
          assert.ok(actual.every(row=>row['node_id']!==key('1')),'a private alias cannot expose its canonical identity');
          if (network==='ukmesh' && cursor===null && limit===1000) {
            if (count===0) assert.equal(actual.length,0,'an empty region has no public nodes');
            else {
              assert.ok(actual.some(row=>row['node_id']===key('4')),'the public alias family without a canonical raw row remains visible');
              if (count===1) assert.equal(actual.length,1,'the sparse positive control is populated');
              else assert.ok(actual.length>=count-10,'boundary and dense cases retain their public population');
            }
          }
          if (network==='northeast') assert.equal(actual.length,0,'the only northeast family has a private representative');
          if (network==='test' && count<=1) assert.equal(actual.length,0,'a public-only region does not cross the test-network scope');
          if (baselineRepository) assert.deepEqual(actual,
            await baselineRepository.listPublicMapRows(requested,filters,snapshot,cursor,limit),
            `sealed baseline region${count}/${network}/${requested.length}fields/${cursor??'first'}/${limit}`);
          regionalCases++;
        }
      }
    }
  }
  t.diagnostic(`regional candidate parity: ${regionalCases} complete-field/scope/page cases; sealed baseline ${baselineRepository?'included':'independent canonical-view oracle'}`);
  const prepared = (await query(`SELECT name,(generic_plans+custom_plans)::int AS executions
    FROM pg_prepared_statements WHERE starts_with(name,'read_v1_')`)).rows;
  assert.ok(prepared.length > 0 && prepared.length <= 32, 'canonical-view parity exercises actual bounded runtime preparation');
  assert.ok(prepared.some(row => row['executions'] > 5), 'the same prepared shape observes subsequent privacy/identity changes');
});
