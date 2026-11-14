/** Concrete native probes; this driver only touches the selected owned fixture. */
import assert from 'node:assert/strict';
import {createRequire} from 'node:module';
import {readFile,writeFile} from 'node:fs/promises';
import {setTimeout as wait} from 'node:timers/promises';
import {buildFullStackTraffic,publishFullStackTraffic,readFullStackTraffic} from '../../../backend/src/tools/fullStackTraffic.ts';
import {assertOwnedLeanFixture} from '../../../backend/src/tools/ownedLookupFixture.ts';
import {assertDataLifecycleGate} from '../../../backend/src/db/dataLifecycle.ts';
const require = createRequire(new URL('../../../backend/package.json',import.meta.url));
const pg = require('pg'), mqtt = require('mqtt');
const input = JSON.parse(await readFile(process.argv[2],'utf8'));
const endpoint = process.env.DATABASE_URL;
await assertOwnedLeanFixture(endpoint);
const client = new pg.Client({connectionString:endpoint,statement_timeout:15000,connectionTimeoutMillis:5000,
  application_name:'ukmesh-owned-recovery'});
await client.connect();
let result;
try {
  if (input.action === 'lost-connections') {
    const rows = await client.query(`SELECT pg_terminate_backend(pid) AS killed FROM pg_stat_activity
      WHERE datname=current_database() AND usename='meshcore_api' AND backend_type='client backend' AND pid<>pg_backend_pid()`);
    assert.ok(rows.rows.length>0 && rows.rows.every(row=>row.killed),'no actual API session was terminated');
    result = {passed:true,terminatedApiSessions:rows.rows.length};
  } else if (input.action === 'lifecycle') {
    const before = await client.query('SELECT count(*)::text n FROM mesh.packets');
    for (const action of ['compression','retention']) {
      const env = {[`DATA_LIFECYCLE_${action.toUpperCase()}_ENABLED`]:'true',
        DATA_LIFECYCLE_COMPRESSION_TARGETS:'packets,packet_paths,node_status_samples,node_neighbor_samples',
        DATA_LIFECYCLE_RETENTION_TARGETS:'packets,node_status_samples,node_neighbor_samples'};
      assert.throws(()=>assertDataLifecycleGate({action,target:'packets',approval:`apply-data-lifecycle-${action}-packets`,env}),
        /DATA_LIFECYCLE_RESTORE_RECEIPT_PATH is required/);
    }
    assert.equal((await client.query('SELECT count(*)::text n FROM mesh.packets')).rows[0].n,before.rows[0].n);
    result = {passed:true,compressionAndRetentionRefusedWithoutVerifiedRestore:true,unchangedPacketCount:true};
  } else if (input.action === 'redeliver') {
    const plan = await readFullStackTraffic(input.manifest);
    const original = plan.events.find(event=>event.envelope.raw && event.offsetMs===100);
    assert.ok(original);
    const observer = plan.nodeIds[0];
    const publisher = mqtt.connect('ws://127.0.0.1:9002',{reconnectPeriod:0,connectTimeout:10000});
    try {
      await new Promise((resolve,reject)=>{publisher.once('connect',resolve);publisher.once('error',reject);});
      await publisher.publishAsync(original.topic,JSON.stringify({...original.envelope,timestamp:new Date().toISOString()}),{qos:1});
      await wait(1000);
      await publisher.publishAsync(`meshcore/LHR/${observer}/packets`,JSON.stringify({...original.envelope,
        origin_id:observer,timestamp:new Date(Date.now()-90000).toISOString()}),{qos:1});
    } finally {await publisher.endAsync(true);}
    result = {passed:true,duplicateDeliveries:1,delayedNewObserver:observer,delaySeconds:90};
  } else if (input.action === 'publish' || input.action === 'lock') {
    const plan = buildFullStackTraffic(input.seed,new Date().toISOString(),input.cycles??1,input.cycles?5000:1000);
    await writeFile(input.manifest,JSON.stringify(plan,null,2)+'\n',{flag:'wx',mode:0o600});
    let blocked=0;
    if (input.action === 'lock') await client.query("BEGIN;SELECT pg_advisory_xact_lock(hashtext('meshcore:packet-batch-writer'))");
    try {
      await publishFullStackTraffic(plan,'ws://127.0.0.1:9002');
      if (input.action === 'lock') {
        await wait(1500);
        const copies=await client.query('SELECT count(*)::int n FROM mesh.packets WHERE packet_hash=ANY($1::text[])',[plan.ukmeshHashes]);
        assert.equal(copies.rows[0].n,0,'writer ignored the real transaction lock');
        blocked=Number((await client.query(`SELECT count(*) n FROM pg_locks l JOIN pg_stat_activity a USING(pid)
          WHERE a.datname=current_database() AND l.locktype='advisory' AND NOT l.granted`)).rows[0].n);
        assert.ok(blocked>0,'no actual packet writer was waiting');
      }
    } finally {
      // Closing this session aborts its held transaction and releases the lock.
      if (input.action === 'lock') await client.end();
    }
    result={passed:true,brokerAcknowledgements:plan.events.length,blockedWriters:blocked,
      lockReleasedByLostSession:input.action==='lock'};
  } else throw new Error('unknown owned recovery action');
  await writeFile(input.result,JSON.stringify(result,null,2)+'\n',{flag:'wx',mode:0o600});
} finally {await client.end().catch(()=>{});}
