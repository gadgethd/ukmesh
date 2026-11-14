// Native proof for the existing CoreScope pair on an explicitly owned fixture.
import assert from 'node:assert/strict';
import {spawnSync} from 'node:child_process';
import {createHash, randomInt, randomUUID} from 'node:crypto';
import {readFileSync, writeFileSync} from 'node:fs';
import {createRequire} from 'node:module';
import path from 'node:path';
import {fileURLToPath} from 'node:url';
import {until, coreStatus, readProjection} from './corescope-proof-utils.mjs';
import {assertOwnedLeanFixture} from '../../backend/src/tools/ownedLookupFixture.ts';
import {generateSampleDataset} from '../../backend/src/tools/generateSampleDataset.ts';
import {packetReplayEnvelope} from '../../backend/src/tools/replaySampleMqtt.ts';
import {readGzipCsv, rowObject} from '../../backend/src/tools/sampleData.ts';

const root = fileURLToPath(new URL('../../', import.meta.url));
const require = createRequire(new URL('../../backend/package.json', import.meta.url));
const {Pool} = require('pg');
const mqtt = require('mqtt');
const [settingsFile, output] = process.argv.slice(2);
assert.equal(settingsFile, path.join(root, 'plans/lean-runtime/runtime/corescope-mode.json'));
const settings = JSON.parse(readFileSync(settingsFile));
assert.equal(settings.mode, 'public');
const selected = JSON.parse(readFileSync(path.join(root, 'plans/lean-runtime/runtime/selected-fixture.json')));
assert.equal(selected.token, settings.fixture_token);
const database = selected.targets.ukmesh;
const endpoint = 'postgresql://meshcore:meshcore@127.0.0.1:5433/' + database;
await assertOwnedLeanFixture(endpoint);
assert.ok(path.resolve(output).startsWith(path.join(root, 'plans/lean-runtime/qualification/')));
const pool = new Pool({connectionString: endpoint, max: 1, statement_timeout: 15_000,
  application_name: 'ukmesh-owned-core-public-proof'});
const token = randomUUID();
const receiver = createHash('sha256').update('corescope-public:' + token).digest('hex').toUpperCase();
const nodeName = 'CoreScope owned public-source fixture';
const checks = [];
let nodeCreated = false;
let privacyChanged = false;
const record = (name, evidence = {}) => checks.push({name, passed: true, ...evidence});

function sqlite(query, args = []) {
  return readProjection(settings.data_dir, query, args);
}
function counts(ids) {
  const placeholders = ids.map(() => '?').join(',');
  return {
    journal: sqlite(`SELECT count(*) AS n FROM public_projection_journal WHERE observation_id IN (${placeholders})`, ids)[0].n,
    observations: sqlite(`SELECT count(*) AS n FROM observations WHERE upstream_observation_id IN (${placeholders})`, ids)[0].n,
    duplicateIdentities: sqlite('SELECT count(*) AS n FROM (SELECT upstream_observation_id FROM observations WHERE upstream_observation_id IS NOT NULL GROUP BY upstream_observation_id HAVING count(*)>1)')[0].n,
  };
}
function command(args) {
  const result = spawnSync(args[0], args.slice(1), {cwd: root, encoding: 'utf8', timeout: 240_000,
    env: {PATH: process.env.PATH, HOME: process.env.HOME, LANG: 'C.UTF-8'}});
  assert.ifError(result.error);
  if (result.status !== 0) throw new Error('Owned lifecycle command failed: ' + args[0]);
}
function restartPrivacyWorker() {
  command(['sg', 'docker', '-c', 'bash scripts/full-dev/control.sh restart privacy-remat-worker']);
}
async function converge() {
  await until(async () => {
    const {rows: [state]} = await pool.query(`SELECT current.generation=ready.visibility_generation
      AND NOT EXISTS(SELECT 1 FROM ops.privacy_rematerialization_queue WHERE status IN ('pending','processing','failed')) AS ready
      FROM ops.public_visibility_state current JOIN ops.packet_visibility_materialization_state ready USING(singleton)
      WHERE current.singleton=true`);
    return state?.ready === true;
  }, 'authoritative privacy convergence', 180_000);
  await until(async () => await coreStatus() === 200, 'CoreScope current public epoch', 60_000);
}
async function observationIds(hashes) {
  return (await pool.query('SELECT observation_id::text AS id FROM mesh.packets WHERE packet_hash=ANY($1::text[]) AND rx_node_id=$2 ORDER BY observation_id', [hashes, receiver])).rows.map(row => row.id);
}

try {
  assert.ok((await pool.query('SELECT count(*)::int AS n FROM mesh.packets')).rows[0].n <= 10_000,
    'native privacy faults require the small owned fixture');
  await pool.query('INSERT INTO core.nodes(node_id,public_key,name,network) VALUES($1,$1,$2,\'ukmesh\')', [receiver, nodeName]);
  nodeCreated = true;
  await converge();
  const directory = path.join(output, 'corescope-public-source');
  await generateSampleDataset(['--data-dir', directory, '--count', '2', '--sequence-start', String(randomInt(5_000_000, 1_000_000_000))]);
  const [packetTable, rawTable] = await Promise.all([
    readGzipCsv(path.join(directory, 'packets.csv.gz')), readGzipCsv(path.join(directory, 'raw_packets.csv.gz')),
  ]);
  const packets = packetTable.rows.map(row => rowObject(packetTable.headers, row));
  const raw = rawTable.rows.map(row => rowObject(rawTable.headers, row));
  const hashes = packets.map(row => row.packet_hash);
  assert.equal((await pool.query('SELECT count(*)::int AS n FROM mesh.packets WHERE packet_hash=ANY($1::text[])', [hashes])).rows[0].n, 0);
  const client = await mqtt.connectAsync('ws://127.0.0.1:9002', {clientId: 'owned-core-public-' + token, reconnectPeriod: 0});
  try {
    for (let index = 0; index < packets.length; index++) {
      const row = {...packets[index], rx_node_id: receiver};
      const envelope = packetReplayEnvelope(row, raw[index].raw_hex, 'observer');
      await client.publishAsync(`meshcore/LHR/${receiver}/packets`, JSON.stringify(envelope), {qos: 1});
    }
  } finally { await client.endAsync(); }
  let ids;
  await until(async () => { ids = await observationIds(hashes); return ids.length === 2; }, 'native UKMesh commits');
  await until(() => {
    const state = counts(ids);
    return state.journal === 2 && state.observations === 2 && state.duplicateIdentities === 0;
  }, 'CoreScope PG projection commits', 60_000);
  record('public-postgresql-backfill-and-follow', counts(ids));
  const cursorBefore = JSON.parse(readFileSync(path.join(settings.data_dir, 'public-pg.json')));
  assert.ok(cursorBefore.lastSeq > 0);
  command(['python3', 'scripts/full-dev/corescope-public.py', 'restart']);
  await converge();
  await until(() => counts(ids).observations === 2, 'cursor restart continuity');
  const cursorAfter = JSON.parse(readFileSync(path.join(settings.data_dir, 'public-pg.json')));
  assert.ok(cursorAfter.lastSeq >= cursorBefore.lastSeq);
  assert.deepEqual(counts(ids), {journal: 2, observations: 2, duplicateIdentities: 0});
  record('cursor-restart-and-observation-deduplication', {lastSeqBefore: cursorBefore.lastSeq, lastSeqAfter: cursorAfter.lastSeq});

  const {rows: [authority]} = await pool.query('SELECT generation::text FROM ops.public_visibility_state WHERE singleton=true');
  const wrongGeneration = BigInt(authority.generation) + 1_000_000n;
  await pool.query('UPDATE ops.packet_visibility_materialization_state SET visibility_generation=$1 WHERE singleton=true', [wrongGeneration.toString()]);
  try {
    assert.equal(await coreStatus(), 503);
    record('stale-authority-fails-closed');
  } finally {
    const restored = await pool.query(`UPDATE ops.packet_visibility_materialization_state ready
      SET visibility_generation=$1 FROM ops.public_visibility_state current
      WHERE ready.singleton=true AND current.singleton=true AND current.generation=$1
        AND ready.visibility_generation=$2`, [authority.generation, wrongGeneration.toString()]);
    assert.equal(restored.rowCount, 1, 'no concurrent policy change may be falsely certified');
  }
  await converge();
  await pool.query("UPDATE core.nodes SET name=$2 WHERE node_id=$1", [receiver, nodeName + ' 🚫']);
  privacyChanged = true;
  restartPrivacyWorker();
  await converge();
  await until(() => counts(ids).journal === 0 && counts(ids).observations === 0,
    'revocation removes replay journal and derived observations', 60_000);
  record('privacy-revocation-prunes-copied-and-derived-data', counts(ids));
  await pool.query('UPDATE core.nodes SET name=$2 WHERE node_id=$1', [receiver, nodeName]);
  privacyChanged = false;
  restartPrivacyWorker();
  await converge();
  await until(() => counts(ids).observations === 2 && counts(ids).journal === 2,
    'released public policy replays retained observations', 60_000);
  assert.equal(counts(ids).duplicateIdentities, 0);
  record('public-restoration-replays-once', counts(ids));
  writeFileSync(path.join(output, 'corescope-public-pg.json'), JSON.stringify({passed: true,
    at: new Date().toISOString(), fixture_token: selected.token, checks}, null, 2) + '\n', {mode: 0o600});
} catch (error) {
  writeFileSync(path.join(output, 'corescope-public-pg.json'), JSON.stringify({passed: false,
    at: new Date().toISOString(), fixture_token: selected.token, code: error.code ?? error.name,
    error: error.message, checks}, null, 2) + '\n', {mode: 0o600});
  process.exitCode = 1;
} finally {
  try {
    if (nodeCreated && privacyChanged) {
      await pool.query('UPDATE core.nodes SET name=$2 WHERE node_id=$1', [receiver, nodeName]);
      restartPrivacyWorker();
      await converge();
    }
  } finally { await pool.end(); }
}
