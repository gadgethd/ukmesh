/** An invented published scope stresses regional reads independently of RF
 * classification. This is not evidence about the region classifier's accuracy.
 */
import pg from 'pg';
import { mapVisiblePredicate } from '../nodes/publicMap.js';
import { assertLocalDatabaseUrl } from './sampleData.js';

const endpoint=process.env['DATABASE_URL']??'';
assertLocalDatabaseUrl(endpoint);
if (!['/lookup_1x','/lookup_10x'].includes(new URL(endpoint).pathname)||!process.argv.includes('--owned-fixture')) {
  throw new Error('explicitly owned lookup fixture required');
}
const pool=new pg.Pool({connectionString:endpoint,max:1});
const client=await pool.connect();
try {
  await client.query('BEGIN');
  await client.query("INSERT INTO core.region_scope_stats(region) VALUES('lookup_qualification') ON CONFLICT DO NOTHING");
  await client.query("DELETE FROM core.region_scope_members WHERE region='lookup_qualification'");
  const inserted=await client.query(`INSERT INTO core.region_scope_members(region,node_id,packets)
    SELECT 'lookup_qualification',n.node_id,1 FROM core.node_identity_nodes n
    WHERE ${mapVisiblePredicate('n')} AND n.network IN ('ukmesh','northeast','teesside')`);
  await client.query('COMMIT');
  await client.query('ANALYZE core.region_scope_members');
  console.log(JSON.stringify({invented:true,scope:'lookup_qualification',members:inserted.rowCount,
    purpose:'Large published-member read fixture; raw RF classification is qualified separately.'}));
} catch(error) {await client.query('ROLLBACK');throw error;}
finally {client.release();await pool.end();}
