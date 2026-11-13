/** Expand a CLONE of the retained fixture. Never point this at the DEV stack.
 * Extra identities and text are invented. The source archive is never edited.
 */
import pg from 'pg';
import { prepareSampleDataset } from './prepareSampleDataset.js';
import { assertLocalDatabaseUrl } from './sampleData.js';

const endpoint = process.env['DATABASE_URL'] ?? '';
assertLocalDatabaseUrl(endpoint);
if (!/^\/lookup_10x$/.test(new URL(endpoint).pathname)
  || !process.argv.includes('--owned-fixture')) throw new Error('requires --owned-fixture and a separate lookup_10x database');
const pool = new pg.Pool({ connectionString: endpoint, max: 1, statement_timeout: 900_000 });
const client = await pool.connect();
try {
  const before = (await client.query(`SELECT (SELECT count(*)::integer FROM core.nodes) nodes,
    (SELECT count(*)::integer FROM mesh.packets) observations`)).rows[0];
  if (before.nodes !== 6176 || before.observations !== 157751) throw new Error('requires an unmodified clone of the retained fixture');
  await client.query('BEGIN');
  // As in sample:load --fast, the bulk fixture is uncertified until the normal
  // preparation/convergence completes. Node privacy triggers stay enabled.
  await client.query('UPDATE ops.packet_visibility_materialization_state SET visibility_generation=0 WHERE singleton');
  await client.query(`CREATE FUNCTION pg_temp.lookup_id(shard integer, value text) RETURNS text
    LANGUAGE sql IMMUTABLE STRICT AS $$ SELECT upper(md5('lookup-a:' || shard || ':' || value)
      || md5('lookup-b:' || shard || ':' || value)) $$`);
  for (const [name, table] of [['nodes', 'core.nodes'], ['links', 'core.node_links'],
    ['statuses', 'core.node_status_samples'], ['packets', 'mesh.packets']] as const) {
    await client.query(`CREATE TEMP TABLE lookup_base_${name} ON COMMIT DROP AS SELECT * FROM ${table}`);
  }
  // Resolve each old hop once, instead of searching all nodes for every copied
  // reception. Ambiguous/unknown prefixes remain explicitly invented evidence.
  await client.query(`CREATE TEMP TABLE lookup_prefixes ON COMMIT DROP AS
    SELECT hash, min(n.node_id) AS node_id FROM (
      SELECT DISTINCT unnest(path_hashes) hash FROM lookup_base_packets
    ) hashes LEFT JOIN lookup_base_nodes n ON upper(left(n.node_id,length(hash)))=upper(hash)
    GROUP BY hash`);
  await client.query('CREATE UNIQUE INDEX ON lookup_prefixes(hash)');
  for (let shard = 1; shard < 10; shard++) {
    // Six shards stress recent windows; four also exercise historical chunks.
    const days = shard < 6 ? 0 : (shard - 5) * 7;
    await client.query(`INSERT INTO core.nodes SELECT copied.* FROM lookup_base_nodes n
      CROSS JOIN LATERAL jsonb_populate_record(NULL::core.nodes,
      to_jsonb(n) || jsonb_build_object(
        'node_id', pg_temp.lookup_id($1,n.node_id), 'public_key',pg_temp.lookup_id($1,n.node_id),
        'name','X' || left(pg_temp.lookup_id($1,n.node_id),24) || ' Mesh',
        'hardware_model','Fixture', 'firmware_version','fixture')) copied`, [shard]);
    await client.query(`INSERT INTO core.node_links SELECT copied.* FROM lookup_base_links l
      CROSS JOIN LATERAL jsonb_populate_record(NULL::core.node_links,
      to_jsonb(l) || jsonb_build_object('node_a_id',pg_temp.lookup_id($1,l.node_a_id),
        'node_b_id',pg_temp.lookup_id($1,l.node_b_id))) copied`, [shard]);
    await client.query(`INSERT INTO core.node_status_samples SELECT copied.* FROM lookup_base_statuses s
      CROSS JOIN LATERAL jsonb_populate_record(NULL::core.node_status_samples,
      to_jsonb(s) || jsonb_build_object('node_id',pg_temp.lookup_id($1,s.node_id),
        'time',s.time - ($2::integer * interval '1 day'), 'stats','{}'::jsonb)) copied`, [shard, days]);
    await client.query('SET LOCAL session_replication_role = replica');
    await client.query(`INSERT INTO mesh.packets SELECT copied.* FROM lookup_base_packets p
      CROSS JOIN LATERAL jsonb_populate_record(NULL::mesh.packets,
      to_jsonb(p) || jsonb_build_object(
        'time',p.time - ($2::integer * interval '1 day'),
        'packet_hash',pg_temp.lookup_id($1,p.packet_hash),
        'packet_uid',CASE WHEN p.packet_uid IS NULL THEN NULL ELSE decode(pg_temp.lookup_id($1,encode(p.packet_uid,'hex')),'hex') END,
        'observation_id',md5('lookup:' || $1 || ':' || p.observation_id)::uuid,
        'src_node_id',pg_temp.lookup_id($1,p.src_node_id), 'rx_node_id',pg_temp.lookup_id($1,p.rx_node_id),
        'topic','meshcore/LHR/' || coalesce(pg_temp.lookup_id($1,p.rx_node_id),'fixture') || '/packets',
        'payload',jsonb_build_object('_summary','Invented lookup observation'), 'raw_hex',NULL,
        'path_hashes',CASE WHEN p.path_hashes IS NULL THEN NULL ELSE ARRAY(
          SELECT upper(left(pg_temp.lookup_id($1,coalesce(prefix.node_id,hop.hash)),length(hop.hash)))
          FROM unnest(p.path_hashes) WITH ORDINALITY hop(hash,ord)
          LEFT JOIN lookup_prefixes prefix ON prefix.hash=hop.hash ORDER BY hop.ord
        ) END)) copied`, [shard, days]);
    await client.query('SET LOCAL session_replication_role = origin');
    console.log(JSON.stringify({ shard, historicalDays: days, nodes: before.nodes, observations: before.observations }));
  }
  await client.query('COMMIT');
  await client.query('ANALYZE core.nodes; ANALYZE mesh.packets; ANALYZE core.node_links; ANALYZE core.node_status_samples');
} catch (error) { await client.query('ROLLBACK'); throw error; }
finally { client.release(); await pool.end(); }
await prepareSampleDataset(endpoint);
