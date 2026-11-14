// Prove the credentialed public tail while the real SQL writer cannot commit.
import assert from 'node:assert/strict';
import {spawnSync} from 'node:child_process';
import {createHash, randomInt, randomUUID} from 'node:crypto';
import {readFileSync, writeFileSync} from 'node:fs';
import {createRequire} from 'node:module';
import path from 'node:path';
import {fileURLToPath} from 'node:url';
import {assertOwnedLeanFixture} from '../../backend/src/tools/ownedLookupFixture.ts';
import {generateSampleDataset} from '../../backend/src/tools/generateSampleDataset.ts';
import {packetReplayEnvelope} from '../../backend/src/tools/replaySampleMqtt.ts';
import {readGzipCsv, rowObject} from '../../backend/src/tools/sampleData.ts';
import {until, coreStatus, readProjection} from './corescope-proof-utils.mjs';

const root = fileURLToPath(new URL('../../', import.meta.url));
const require = createRequire(new URL('../../backend/package.json', import.meta.url));
const {Pool} = require('pg'), mqtt = require('mqtt');
const [settingsFile, output, stream] = process.argv.slice(2);
assert.equal(settingsFile, path.join(root, 'plans/lean-runtime/runtime/corescope-mode.json'));
const settings = JSON.parse(readFileSync(settingsFile));
const selected = JSON.parse(readFileSync(path.join(root, 'plans/lean-runtime/runtime/selected-fixture.json')));
assert.equal(settings.mode, 'public');
assert.equal(settings.fixture_token, selected.token);
const streamPrefix = 'meshcore:ingest:lean:' + selected.token + ':corescope:';
assert.ok(stream.startsWith(streamPrefix) && /^[a-f0-9]{12}$/.test(stream.slice(streamPrefix.length)));
assert.ok(path.resolve(output).startsWith(path.join(root, 'plans/lean-runtime/qualification/')));
const endpoint = 'postgresql://meshcore:meshcore@127.0.0.1:5433/' + selected.targets.ukmesh;
await assertOwnedLeanFixture(endpoint);
const pool = new Pool({connectionString: endpoint, max: 2, statement_timeout: 15_000,
  application_name: 'ukmesh-owned-core-ram-proof'});
const session = randomUUID();
const receivers = ['A1', 'B2'].map(prefix => prefix + createHash('sha256').update(session + prefix).digest('hex').slice(2).toUpperCase());
const checks = [];
let lock, privacyChanged = false;
const record = (name, evidence = {}) => checks.push({name, passed: true, ...evidence});
function restartPrivacyWorker() {
  const result = spawnSync('sg', ['docker', '-c', 'bash scripts/full-dev/control.sh restart privacy-remat-worker'],
    {cwd: root, encoding: 'utf8', timeout: 240_000, env: {PATH: process.env.PATH, HOME: process.env.HOME, LANG: 'C.UTF-8'}});
  assert.ifError(result.error);
  assert.equal(result.status, 0, 'owned privacy worker restart');
}
async function converge() {
  await until(async () => {
    const {rows: [state]} = await pool.query(`SELECT current.generation=ready.visibility_generation
      AND NOT EXISTS(SELECT 1 FROM ops.privacy_rematerialization_queue WHERE status IN ('pending','processing','failed')) AS ready
      FROM ops.public_visibility_state current JOIN ops.packet_visibility_materialization_state ready USING(singleton)
      WHERE current.singleton=true`);
    return state?.ready && await coreStatus() === 200;
  }, 'current public policy', 180_000);
}
const headers = {authorization: 'Bearer local-full-public-stream-token-0123456789abcdef'};
async function page(after = '0-0') {
  const response = await fetch('http://127.0.0.1:3100/internal/ingest/public/entries?limit=500&after=' + after,
    {headers, signal: AbortSignal.timeout(5000)});
  assert.equal(response.status, 200);
  return response.json();
}
const countCopies = ids => {
  const marks = ids.map(() => '?').join(',');
  return readProjection(settings.data_dir,
    `SELECT count(*) n FROM observations WHERE upstream_observation_id IN (${marks})`, ids)[0].n;
};
try {
  assert.ok((await pool.query('SELECT count(*)::int n FROM mesh.packets')).rows[0].n <= 10_000);
  for (const [index, receiver] of receivers.entries()) {
    await pool.query("INSERT INTO core.nodes(node_id,public_key,name,network) VALUES($1,$1,$2,'ukmesh')",
      [receiver, 'Core RAM owned ' + index]);
  }
  await pool.query("UPDATE core.nodes SET name=name||' 🚫' WHERE node_id=$1", [receivers[1]]);
  privacyChanged = true;
  restartPrivacyWorker();
  await converge();
  assert.equal((await pool.query('SELECT EXISTS(SELECT 1 FROM core.private_node_prefixes WHERE node_id=$1) AS private', [receivers[1]])).rows[0].private, true,
    'live private-prefix trigger must register the owned receiver');
  const directory = path.join(output, 'source');
  await generateSampleDataset(['--data-dir', directory, '--count', '100', '--sequence-start', String(randomInt(1_000_000_000, 2_000_000_000))]);
  const [packetTable, rawTable] = await Promise.all([
    readGzipCsv(path.join(directory, 'packets.csv.gz')), readGzipCsv(path.join(directory, 'raw_packets.csv.gz'))]);
  const packets = packetTable.rows.map(row => rowObject(packetTable.headers, row));
  const raw = rawTable.rows.map(row => rowObject(rawTable.headers, row));
  const hashes = packets.map(row => row.packet_hash);
  assert.equal((await pool.query('SELECT count(*)::int n FROM mesh.packets WHERE packet_hash=ANY($1::text[])', [hashes])).rows[0].n, 0);
  lock = await pool.connect();
  await lock.query('BEGIN');
  await lock.query("SELECT pg_advisory_xact_lock(hashtext('meshcore:packet-batch-writer'))");
  const client = await mqtt.connectAsync('ws://127.0.0.1:9002', {clientId: 'owned-core-ram-' + session, reconnectPeriod: 0});
  try {
    for (let index = 0; index < packets.length; index++) {
      const receiver = receivers[index < 80 ? 0 : 1];
      await client.publishAsync(`meshcore/LHR/${receiver}/packets`,
        JSON.stringify(packetReplayEnvelope({...packets[index], rx_node_id: receiver}, raw[index].raw_hex, 'observer')), {qos: 1});
    }
  } finally { await client.endAsync(); }
  // Broker ACKs precede the application's RAM admission. Count only the owned
  // stream, through the operator test process; Core still has no Redis access.
  await until(() => {
    const result = spawnSync('sg', ['docker', '-c', 'docker exec ukmesh-sample-redis redis-cli --raw XLEN ' + stream],
      {cwd: root, encoding: 'utf8', timeout: 5000, env: {PATH: process.env.PATH, HOME: process.env.HOME, LANG: 'C.UTF-8'}});
    assert.ifError(result.error);
    assert.equal(result.status, 0, 'owned stream admission oracle');
    return Number(result.stdout.trim()) === 100;
  }, 'all100 application RAM admissions');
  let published;
  await until(async () => { published = await page(); return published.entries.length === 80; }, '80 filtered public pending entries');
  const values = published.entries.map(entry => ({id: entry.id, value: JSON.parse(entry.value)}));
  assert.ok(values.every(entry => entry.value.d.rxNodeId === receivers[0] && entry.value.vo === 1 && entry.value.ip === 0));
  const ids = values.map(entry => entry.value.o);
  assert.equal(new Set(ids).size, 80);
  const lastPublic = values.at(-1).id;
  const [lastMs, lastSequence] = lastPublic.split('-').map(BigInt);
  const [nextMs, nextSequence] = published.nextCursor.split('-').map(BigInt);
  assert.ok(nextMs > lastMs || nextMs === lastMs && nextSequence > lastSequence,
    'cursor must advance past trailing private entries');
  const onlyPrivate = await page(lastPublic);
  assert.equal(onlyPrivate.entries.length, 0);
  assert.equal(onlyPrivate.nextCursor, published.nextCursor);
  await until(() => countCopies(ids) === 80, 'Core HTTP tail before SQL commit', 60_000);
  assert.equal((await pool.query('SELECT count(*)::int n FROM mesh.packets WHERE packet_hash=ANY($1::text[])', [hashes])).rows[0].n, 0);
  record('public-http-tail-before-postgresql-commit', {admitted: 100, publicCopies: 80, sqlRows: 0});
  record('private-only-page-advances-cursor', {publicEntries: 80, privateEntries: 20, cursorAdvanced: true});
  await lock.query('ROLLBACK'); lock.release(); lock = null;
  await until(async () => (await pool.query('SELECT count(*)::int n FROM mesh.packets WHERE packet_hash=ANY($1::text[])', [hashes])).rows[0].n === 100,
    'all RAM packets commit after lock release', 120_000);
  assert.equal((await pool.query('SELECT count(*)::int n FROM mesh.packets WHERE packet_hash=ANY($1::text[]) AND is_private IS TRUE AND visibility_ok IS FALSE',
    [hashes.slice(80)])).rows[0].n, 20);
  const {rows: [ledger]} = await pool.query('SELECT max(seq)::text seq FROM mesh.ingest_observation_ledger WHERE packet_hash=ANY($1::text[])', [hashes.slice(0, 80)]);
  await until(() => BigInt(JSON.parse(readFileSync(path.join(settings.data_dir, 'public-pg.json'))).lastSeq) >= BigInt(ledger.seq),
    'committed ledger overlap cursor', 60_000);
  assert.equal(countCopies(ids), 80);
  const privateCopies = readProjection(settings.data_dir,
    'SELECT count(*) n FROM public_projection_journal WHERE json_extract(record,\'$.rxNodeId\')=?', [receivers[1]])[0].n;
  assert.equal(privateCopies, 0);
  const duplicateIdentities = readProjection(settings.data_dir,
    'SELECT count(*) n FROM (SELECT upstream_observation_id FROM observations WHERE upstream_observation_id IS NOT NULL GROUP BY upstream_observation_id HAVING count(*)>1)')[0].n;
  assert.equal(duplicateIdentities, 0);
  record('postgresql-handoff-deduplicates-and-excludes-private', {committed: 100, publicCopies: 80, privateCopies, duplicateIdentities});
} catch (error) {
  checks.push({name: 'native-ram-proof', passed: false, error: error.message});
  process.exitCode = 1;
} finally {
  if (lock) { await lock.query('ROLLBACK'); lock.release(); }
  try {
    if (privacyChanged) {
      await pool.query("UPDATE core.nodes SET name='Core RAM owned released' WHERE node_id=$1", [receivers[1]]);
      restartPrivacyWorker();
      await converge();
    }
  } catch (error) { checks.push({name: 'privacy-restoration', passed: false, error: error.message}); process.exitCode = 1; }
  await pool.end();
  writeFileSync(path.join(output, 'corescope-public-ram.json'), JSON.stringify({passed: process.exitCode !== 1,
    at: new Date().toISOString(), fixture_token: selected.token, checks}, null, 2) + '\n', {mode: 0o600});
}
