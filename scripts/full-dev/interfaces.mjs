// Real application flows on the selected, explicitly owned fixture.
import assert from 'node:assert/strict';
import {createHash, randomInt, randomUUID} from 'node:crypto';
import {readFileSync, writeFileSync} from 'node:fs';
import {mkdir} from 'node:fs/promises';
import {createRequire} from 'node:module';
import {execFile} from 'node:child_process';
import {promisify} from 'node:util';
import path from 'node:path';
import {fileURLToPath} from 'node:url';
import {assertOwnedLeanFixture} from '../../backend/src/tools/ownedLookupFixture.ts';
import {generateSampleDataset} from '../../backend/src/tools/generateSampleDataset.ts';
import {packetReplayEnvelope} from '../../backend/src/tools/replaySampleMqtt.ts';
import {readGzipCsv, rowObject} from '../../backend/src/tools/sampleData.ts';
import {browserLaunchOptions} from './browser-runtime.mjs';
import {startInterfaceReaders} from './interface-readers.mjs';

const root = fileURLToPath(new URL('../../', import.meta.url));
const require = createRequire(new URL('../../backend/package.json', import.meta.url));
const {Pool} = require('pg'), mqtt = require('mqtt');
const {chromium} = createRequire(new URL('../../frontend/package.json', import.meta.url))('playwright');
const [output] = process.argv.slice(2);
assert.ok(path.resolve(output).startsWith(path.join(root, 'plans/lean-runtime/qualification/')));
await mkdir(output, {recursive: true, mode: 0o700});
const selected = JSON.parse(readFileSync(path.join(root, 'plans/lean-runtime/runtime/selected-fixture.json')));
assert.match(selected.token, /^[a-z0-9_]{1,38}$/);
assert.equal(selected.targets.owner, 'lean_owner_' + selected.token);
const endpoint = 'postgresql://meshcore:meshcore@127.0.0.1:5433/' + selected.targets.ukmesh;
await assertOwnedLeanFixture(endpoint);
const ownerURL = 'postgresql://meshcore:meshcore@127.0.0.1:5433/' + selected.targets.owner;
const limits = {max: 1, connectionTimeoutMillis: 3000, statement_timeout: 15000, query_timeout: 20000};
const main = new Pool({...limits, connectionString: endpoint, application_name: 'owned-interface-provisioner'});
const ownerControl = new Pool({...limits, connectionString: ownerURL, application_name: 'owned-interface-authority-control'});
const marker = await ownerControl.query("SELECT shobj_description(oid,'pg_database') marker FROM pg_database WHERE datname=current_database()");
assert.equal(marker.rows[0]?.marker, 'ukmesh-owned-fixture:' + selected.token);
process.env.OWNER_DATABASE_URL = 'postgresql://meshcore_owner:local-full-dev-only@127.0.0.1:5433/' + selected.targets.owner;
process.env.DATABASE_URL = endpoint;
process.env.OWNER_DATABASE_POOL_MAX = '2';
process.env.OWNER_DATABASE_STATEMENT_TIMEOUT_MS = '15000';
const authority = await import('../../backend/src/owner/ownerAuth.ts');
const id = randomUUID().replaceAll('-', '');
const users = ['qual-owner-' + id.slice(0, 12), 'qual-other-' + id.slice(0, 12)];
const nodes = users.map(user => createHash('sha256').update(user).digest('hex').toUpperCase());
const destination = 'qual-' + id.slice(0, 12), destinationName = 'Owned qualification destination';
writeFileSync(path.join(output, 'owned-owner-identities.json'), JSON.stringify({users, nodes, destination}, null, 2) + '\n', {mode: 0o600});
const checks = [], browserErrors = [];
const record = (name, details = {}) => checks.push({name, passed: true, ...details});
let browser, readers;
const base = 'http://127.0.0.1:5173';
const browserAgent = 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/143.0.0.0 Safari/537.36';
function watch(context) {
  context.on('page', page => page.on('pageerror', error => browserErrors.push(error.message)));
  return context;
}
async function login(context, username) {
  const page = await context.newPage();
  await page.goto(base + '/login', {waitUntil: 'domcontentloaded'});
  await page.getByLabel('MQTT username', {exact: true}).fill(username);
  await page.getByLabel('MQTT password', {exact: true}).fill('local-owned-interface-only');
  const response = page.waitForResponse(value => new URL(value.url()).pathname === '/api/owner/login' && value.request().method() === 'POST');
  await page.getByRole('button', {name: 'Login', exact: true}).click();
  assert.equal((await response).status(), 200, 'real broker login and verified owner grant');
  return page;
}
async function post(context, body, csrf = true) {
  const response = await context.request.get(base + '/api/owner/csrf');
  const {csrfToken} = await response.json();
  return context.request.post(base + '/api/owner/packet-sharing', {data: body,
    headers: csrf ? {'X-CSRF-Token': csrfToken, origin: base} : {origin: base}});
}
try {
  for (let index = 0; index < users.length; index++) {
    await main.query(`INSERT INTO core.nodes(node_id,public_key,name,network,role,lat,lon,iata,last_seen)
      VALUES($1,$1,$2,'ukmesh',2,51.5,-0.1,'LHR',NOW())`, [nodes[index], 'Qualification owner ' + index]);
    await authority.upsertVerifiedOwnerGrant(users[index], nodes[index], {
      verificationMethod: 'operator-database', actor: 'owned-interface-qualification', reason: 'Synthetic local fixture interface proof'});
  }
  await main.query(`INSERT INTO core.owner_packet_share_destinations(destination_id,display_name,broker_url,
    topic_template,enabled,allowed_owner_username,allowed_node_id)
    VALUES($1,$2,'ws://ukmesh-sample-mqtt:9001',$3,TRUE,$4,$5)`,
    [destination, destinationName, 'qualification/owner/' + id + '/{packetHash}', users[0], nodes[0]]);
  readers = await startInterfaceReaders();
  browser = await chromium.launch(browserLaunchOptions());
  const context = watch(await browser.newContext({viewport: {width: 1440, height: 900}, serviceWorkers: 'block', userAgent: browserAgent}));
  const page = await login(context, users[0]);
  const control = page.getByRole('switch', {name: 'Share packets with ' + destinationName, exact: true});
  await control.waitFor({state: 'visible', timeout: 30000});
  let injectFailure = true;
  await page.route('**/api/owner/packet-sharing', async route => {
    if (route.request().method() === 'POST' && injectFailure) {
      injectFailure = false;
      await route.fulfill({status: 503, json: {error: 'Owned qualification interrupted this save'}});
    } else await route.continue();
  });
  await control.check();
  await page.getByText('Your changes have not been confirmed. Your selection is kept.', {exact: true}).waitFor();
  assert.equal(await control.isChecked(), true);
  assert.equal((await main.query('SELECT count(*)::int n FROM core.owner_packet_share_rules WHERE owner_username=$1 AND node_id=$2 AND destination_id=$3 AND enabled',
    [users[0], nodes[0], destination])).rows[0].n, 0);
  await page.screenshot({path: path.join(output, 'owner-failed-save-1440.png')});
  record('owner-failed-save-retains-unconfirmed-selection');
  const saved = page.waitForResponse(value => new URL(value.url()).pathname === '/api/owner/packet-sharing' && value.request().method() === 'POST');
  await page.getByRole('button', {name: 'Retry saving', exact: true}).click();
  assert.equal((await saved).status(), 200);
  assert.equal((await main.query('SELECT count(*)::int n FROM core.owner_packet_share_rules WHERE owner_username=$1 AND node_id=$2 AND destination_id=$3 AND enabled',
    [users[0], nodes[0], destination])).rows[0].n, 1);
  await page.getByText('Your changes have not been confirmed. Your selection is kept.', {exact: true}).waitFor({state: 'hidden'});
  assert.equal(await control.isChecked(), true);
  await page.screenshot({path: path.join(output, 'owner-confirmed-save-1440.png')});
  record('real-owner-save-commits-under-eight-readers');
  const body = {nodeId: nodes[0], enabled: true, destinationIds: [destination]};
  const anonymous = watch(await browser.newContext({userAgent: browserAgent}));
  assert.equal((await post(anonymous, body)).status(), 401);
  assert.equal((await post(context, body, false)).status(), 403);
  const other = watch(await browser.newContext());
  await login(other, users[1]);
  assert.equal((await other.request.get(base + '/api/owner/live?nodeId=' + nodes[0])).status(), 403);
  assert.equal((await post(other, body)).status(), 400);
  assert.equal((await post(other, {nodeId: nodes[1], enabled: true, destinationIds: [destination]})).status(), 409);
  record('owner-session-csrf-actor-and-destination-scope-enforced');
  await authority.revokeOwnerGrant(users[0], nodes[0], {actor: 'owned-interface-qualification', reason: 'Prove immediate fixture session revocation'});
  assert.equal((await context.request.get(base + '/api/owner/session')).status(), 401);
  record('revoked-owner-session-fails-closed');

  // One invented packet must arrive through both actual application proxies.
  const sockets = [];
  for (const url of [base + '/feed', 'http://127.0.0.1:8923/feed']) {
    const view = await anonymous.newPage();
    await view.goto(url, {waitUntil: 'domcontentloaded', timeout: 30000});
    await view.waitForFunction(() => !document.title.includes('Making sure'), {timeout: 30000});
    await view.evaluate(() => new Promise((resolve, reject) => {
      window.qualificationHashes = [];
      window.qualificationState = {opened: false, initial: false, types: [], closed: null};
      const socket = new WebSocket(location.origin.replace(/^http/, 'ws') + '/ws?network=ukmesh');
      const timer = setTimeout(() => {socket.close(); reject(new Error('Live initial state did not arrive'));}, 15000);
      socket.onopen = () => {window.qualificationState.opened = true;};
      socket.onerror = () => reject(new Error('Live socket failed'));
      socket.onclose = event => {window.qualificationState.closed = {code: event.code, reason: event.reason};};
      socket.onmessage = message => {for (const line of String(message.data).split('\n').filter(Boolean)) {
        const value = JSON.parse(line);
        if (!window.qualificationState.types.includes(value.type)) window.qualificationState.types.push(value.type);
        if (value.type === 'initial_state') {window.qualificationState.initial = true; clearTimeout(timer); resolve(true);}
        if (value.type === 'packet') window.qualificationHashes.push(value.data?.packetHash);
      }};
      window.qualificationSocket = socket;
    }));
    sockets.push(view);
  }
  const source = path.join(output, 'live-source');
  await generateSampleDataset(['--data-dir', source, '--count', '1', '--sequence-start', String(randomInt(2_000_000_000, 3_000_000_000))]);
  const [packets, raw] = await Promise.all([readGzipCsv(path.join(source, 'packets.csv.gz')), readGzipCsv(path.join(source, 'raw_packets.csv.gz'))]);
  const packet = rowObject(packets.headers, packets.rows[0]), wire = rowObject(raw.headers, raw.rows[0]);
  const mqttClient = await mqtt.connectAsync('ws://127.0.0.1:9002', {reconnectPeriod: 0});
  try {await mqttClient.publishAsync('meshcore/LHR/' + nodes[1] + '/packets',
    JSON.stringify(packetReplayEnvelope({...packet, rx_node_id: nodes[1]}, wire.raw_hex, 'observer')), {qos: 1});}
  finally {await mqttClient.endAsync();}
  for (const view of sockets) {
    await view.waitForFunction(hash => window.qualificationHashes.includes(hash) || window.qualificationState.closed,
      packet.packet_hash, {timeout: 30000});
    const state = await view.evaluate(hash => ({matched: window.qualificationHashes.includes(hash), ...window.qualificationState}), packet.packet_hash);
    assert.equal(state.matched, true, 'Live packet delivery at ' + new URL(view.url()).origin + ': ' + JSON.stringify(state));
  }
  record('direct-and-anubis-live-packet-delivery');
  assert.deepEqual(browserErrors, []);
  record('native-owner-and-live-browser-has-no-uncaught-errors');
  await browser.close(); browser = null;
  const browserOutput = path.join(output, 'views');
  await promisify(execFile)('node', [path.join(root, 'scripts/full-dev/browser.mjs'), browserOutput],
    {cwd: root, timeout: 300000, maxBuffer: 1024 * 1024,
      env: {PATH: process.env.PATH, HOME: process.env.HOME, LANG: 'C.UTF-8'}});
  const views = JSON.parse(readFileSync(path.join(browserOutput, 'results.json')));
  assert.equal(views.passed, true); assert.equal(views.cases.length, 12);
  record('application-urls-mobile-desktop-under-eight-readers', {views: views.cases});
} catch (error) {
  checks.push({name: 'actual-interface-flow', passed: false, error: error.message}); process.exitCode = 1;
} finally {
  const cleanup = async (name, operation) => {
    try {await operation();}
    catch (error) {checks.push({name: 'cleanup:' + name, passed: false, error: error.message});}
  };
  if (readers) await cleanup('eight-readers', async () => {
    const value = await readers.finish();
    checks.push({name: 'eight-interactive-readers', ...value});
  });
  if (browser) await cleanup('browser', () => browser.close());
  for (let index = 0; index < users.length; index++) {
    await cleanup('grant-' + index, () => authority.revokeOwnerGrant(users[index], nodes[index],
      {actor: 'owned-interface-qualification', reason: 'Finish owned fixture interface proof'}));
  }
  await cleanup('disable-destination', () => main.query('UPDATE core.owner_packet_share_destinations SET enabled=FALSE WHERE destination_id=$1', [destination]));
  await cleanup('rules', () => main.query('DELETE FROM core.owner_packet_share_rules WHERE owner_username=ANY($1::text[]) AND destination_id=$2', [users, destination]));
  await cleanup('destination', () => main.query('DELETE FROM core.owner_packet_share_destinations WHERE destination_id=$1', [destination]));
  await Promise.all([cleanup('main-pool', () => main.end()), cleanup('owner-control-pool', () => ownerControl.end()),
    cleanup('owner-authority-pool', () => authority.closeOwnerAuthDb())]);
  const passed = checks.every(value => value.passed);
  writeFileSync(path.join(output, 'interfaces-native.json'), JSON.stringify({passed, fixture_token: selected.token, checks}, null, 2) + '\n', {mode: 0o600});
  if (!passed) process.exitCode = 1;
}
