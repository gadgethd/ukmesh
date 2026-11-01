import assert from 'node:assert/strict';
import fs from 'node:fs';
import test from 'node:test';
import pg from 'pg';
const endpoint=process.env.TEST_INGEST_DATABASE_URL;
test('ledger compaction preserves complete history and rejects archived replays for current and prior SQL writers', {skip:endpoint?false:'owned PostgreSQL required',timeout:60000},async(t)=>{
  assert.ok(['localhost','127.0.0.1'].includes(new URL(endpoint!).hostname));
  const admin=new pg.Pool({connectionString:endpoint});const name=`archive_${process.pid}_${Date.now()}`;
  await admin.query(`CREATE DATABASE ${name}`);const url=new URL(endpoint!);url.pathname='/'+name;
  const pool=new pg.Pool({connectionString:url.toString(),max:3});
  t.after(async()=>{await pool.end();await admin.query(`DROP DATABASE ${name} WITH (FORCE)`);await admin.end();});
  await pool.query('CREATE EXTENSION IF NOT EXISTS timescaledb');
  await pool.query(fs.readFileSync(new URL('./migrations/059_ingest_observation_ledger.sql',import.meta.url),'utf8'));
  await pool.query(`INSERT INTO ingest_observation_ledger(observation_id,observed_at,packet_hash,network,created_at)
    SELECT gen_random_uuid(),NOW()-INTERVAL '90 days',repeat(md5(i::text),2),'ukmesh',NOW()-INTERVAL '90 days' FROM generate_series(1,20000) i`);
  const before=await pool.query("SELECT count(*)::int n,md5(string_agg(row(seq,observation_id,observed_at,packet_hash,network,created_at)::text,'' ORDER BY seq)) digest FROM ingest_observation_ledger");
  await pool.query('BEGIN');await pool.query(fs.readFileSync(new URL('./migrations/070_ingest_ledger_archive.sql',import.meta.url),'utf8'));await pool.query('COMMIT');
  const old=(await pool.query('SELECT * FROM ingest_observation_ledger ORDER BY seq LIMIT 1')).rows[0];
  // Keep a stale repeatable-read writer open across compaction.
  const stale=await pool.connect();await stale.query('BEGIN ISOLATION LEVEL REPEATABLE READ');await stale.query('SELECT count(*) FROM ingest_observation_ledger');
  try{
    await pool.query("CALL compact_ingest_observation_ledger(NULL,NULL)");
    await assert.rejects(stale.query('INSERT INTO ingest_observation_ledger(observation_id,observed_at,packet_hash,network) VALUES($1,NOW(),$2,$3) ON CONFLICT(observation_id) DO NOTHING',[old.observation_id,old.packet_hash,old.network]), /serialize|concurrent update/);
  }finally{await stale.query('ROLLBACK');stale.release();}
  for(let n=0;n<3;n++)await pool.query('CALL compact_ingest_observation_ledger(NULL,NULL)');
  assert.equal((await pool.query('SELECT count(*)::int n FROM ingest_observation_ledger')).rows[0].n,0);
  const after=await pool.query("SELECT count(*)::int n,md5(string_agg(row(seq,observation_id,observed_at,packet_hash,network,created_at)::text,'' ORDER BY seq)) digest FROM ingest_observation_history");
  assert.deepEqual(after.rows,before.rows);
  const replay=await pool.query('INSERT INTO ingest_observation_ledger(observation_id,observed_at,packet_hash,network) VALUES($1,NOW(),$2,$3) ON CONFLICT(observation_id) DO NOTHING RETURNING seq',[old.observation_id,old.packet_hash,old.network]);assert.equal(replay.rowCount,0);
  await pool.query("INSERT INTO ingest_observation_ledger(observation_id,observed_at,packet_hash,network) VALUES(gen_random_uuid(),NOW(),'new','ukmesh')");
  assert.equal((await pool.query('SELECT count(*)::int n FROM ingest_observation_history')).rows[0].n,20001);
  const bytesBefore=(await pool.query("SELECT sum(total_bytes)::bigint::text bytes FROM chunks_detailed_size('ingest_observation_archive')")).rows[0].bytes;
  await pool.query("SELECT compress_chunk(chunk) FROM show_chunks('ingest_observation_archive') chunk");
  const bytesAfter=(await pool.query("SELECT sum(total_bytes)::bigint::text bytes FROM chunks_detailed_size('ingest_observation_archive')")).rows[0].bytes;
  assert.ok(Number(bytesAfter)<Number(bytesBefore));
  assert.deepEqual((await pool.query("SELECT count(*)::int n,md5(string_agg(row(seq,observation_id,observed_at,packet_hash,network,created_at)::text,'' ORDER BY seq)) digest FROM ingest_observation_history WHERE packet_hash<>'new'")).rows,before.rows);
  console.log(JSON.stringify({rows:20000,archiveBytesBefore:bytesBefore,archiveBytesAfter:bytesAfter}));
});
