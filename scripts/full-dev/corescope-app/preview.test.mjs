import assert from 'node:assert/strict';
import { spawn, spawnSync } from 'node:child_process';
import { mkdtemp, mkdir, readFile, rm, symlink, writeFile } from 'node:fs/promises';
import { createServer, get } from 'node:http';
import { createRequire } from 'node:module';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import test from 'node:test';
import { setTimeout as delay } from 'node:timers/promises';
import { fileURLToPath } from 'node:url';

const require = createRequire(new URL('../../../frontend/package.json', import.meta.url));
const { WebSocket, WebSocketServer } = require('ws');

async function listen(server) {
  await new Promise((resolve, reject) => {
    server.once('error', reject);
    server.listen(0, '127.0.0.1', resolve);
  });
  return server.address().port;
}

test('isolated app edge serves the shell and credentials-free current projection only', {
  skip: !process.env.TEST_CORESCOPE_APP_EDGE,
  timeout: 45_000,
}, async t => {
  const temporary = await mkdtemp(join(tmpdir(), 'ukmesh-core-edge-'));
  t.after(() => rm(temporary, { recursive: true, force: true }));
  let readiness = 204;
  const requests = [];
  const core = createServer((request, response) => {
    requests.push([request.url, request.headers]);
    if (request.url === '/api/public-projection-ready') return response.writeHead(readiness).end();
    if (request.url === '/corescope-app-shell') return response.end('<title>CoreScope preview</title>');
    if (request.url?.startsWith('/readyz')) return response.writeHead(readiness === 204 ? 200 : 503).end();
    response.writeHead(200, { 'X-CoreScope-Privacy-Generation': '7', 'Set-Cookie': 'not-forwarded=1' });
    response.end(request.url);
  });
  const sockets = new WebSocketServer({ noServer: true });
  core.on('upgrade', (request, socket, head) => {
    requests.push([request.url, request.headers]);
    sockets.handleUpgrade(request, socket, head, ws => {
      ws.send('generation=7');
      ws.close();
    });
  });
  const ukmesh = createServer((request, response) => response.end(request.url));
  const corePort = await listen(core);
  const ukmeshPort = await listen(ukmesh);
  t.after(() => { core.close(); ukmesh.close(); sockets.close(); });
  const reservation = createServer();
  const edgePort = await listen(reservation);
  const feedReservation = createServer();
  const feedPort = await listen(feedReservation);
  await new Promise(resolve => reservation.close(resolve));
  await new Promise(resolve => feedReservation.close(resolve));

  const config = join(temporary, 'default.conf');
  const assets = join(temporary, 'public');
  const beaconAssets = join(temporary, 'beacon-feed');
  await mkdir(join(assets, 'icons'), { recursive: true });
  await writeFile(join(assets, 'map.js'), '/* preview map */');
  await symlink('/etc/passwd', join(assets, 'redirected.js'));
  await writeFile(join(assets, 'icons/phosphor-sprite.svg'), '<svg/>');
  await mkdir(join(beaconAssets, 'assets'), { recursive: true });
  await writeFile(join(beaconAssets, 'ukmesh-feed.html'), '<title>Beacon Channels preview</title>');
  await writeFile(join(beaconAssets, 'assets/channels-abcdef.js'), '/* Channels preview */');
  const coreConfig = include => `server {
    listen 127.0.0.1:${edgePort};
    proxy_read_timeout 10s;
    set $corescope_projection http://127.0.0.1:${corePort};
    set $ukmesh_app http://127.0.0.1:${ukmeshPort};
    set $ukmesh_backend http://127.0.0.1:${ukmeshPort};
    ${include ? 'include /etc/nginx/corescope-app/preview-locations.conf;' : 'location = / { return 200 "Legacy UKMesh map"; }'}
  }\n`;
  const feedConfig = include => `server {
    listen 127.0.0.1:${feedPort};
    proxy_read_timeout 10s;
    set $beacon_projection http://127.0.0.1:${corePort};
    ${include ? 'include /etc/nginx/ukmesh-ingress/beacon-feed-locations.conf;' : 'location = /feed { return 200 "Packet Feed default"; }'}
    location = /feed/packets { return 200 "Packet Feed"; }
    location / { return 404; }
  }\n`;
  await writeFile(config, coreConfig(true) + feedConfig(true));
  const dockerfile = await readFile(new URL('../../../Dockerfile.app', import.meta.url), 'utf8');
  const image = dockerfile.match(/^FROM (nginx:\S+)$/m)?.[1];
  assert.ok(image, 'use the main app image pin');
  const name = 'ukmesh-core-edge-' + process.pid;
  const edge = spawn('docker', ['run', '--rm', '--name', name, '--network', 'host', '--memory', '32m',
    '--memory-swap', '32m', '-v', `${config}:/etc/nginx/conf.d/default.conf:ro`,
    '-v', `${fileURLToPath(new URL('.', import.meta.url))}:/etc/nginx/corescope-app:ro`,
    '-v', `${fileURLToPath(new URL('../ingress', import.meta.url))}:/etc/nginx/ukmesh-ingress:ro`,
    '-v', `${assets}:/srv/corescope/public:ro`, '-v', `${beaconAssets}:/srv/beacon-feed:ro`,
    image], { stdio: ['ignore', 'pipe', 'pipe'] });
  let edgeLog = '';
  edge.stderr.on('data', data => { edgeLog += data.toString(); });
  edge.stdout.resume();
  t.after(() => { spawnSync('docker', ['rm', '-f', name], { timeout: 5_000, stdio: 'ignore' }); });

  const origin = `http://127.0.0.1:${edgePort}`;
  const read = (path, options) => fetch(origin + path, { signal: AbortSignal.timeout(4_000), ...options });
  for (let attempt = 0; attempt < 60; attempt++) {
    if (edge.exitCode != null) assert.fail(edgeLog);
    const shell = await read('/').catch(() => null);
    if (shell) {
      assert.equal(await shell.text(), '<title>CoreScope preview</title>');
      break;
    }
    assert.ok(attempt < 59, edgeLog || 'preview did not start');
    await delay(100);
  }

  assert.match(await (await read('/corescope/config.js')).text(), /\/corescope\/ws/);
  assert.equal(await (await read('/corescope/static/map.js')).text(), '/* preview map */');
  assert.equal((await read('/corescope/static/redirected.js')).status, 403);
  assert.equal(await (await read('/icons/phosphor-sprite.svg')).text(), '<svg/>');
  assert.equal(await (await read('/api/runtime-config')).text(), '/api/runtime-config');
  assert.equal(await (await read('/api/regions/summary?network=ukmesh')).text(), '/api/regions/summary?network=ukmesh');
  assert.equal(await (await read('/terrain-tiles/3/4/5.png')).text(), '/terrain-tiles/3/4/5.png');
  assert.equal(await (await read('/sw.js')).text(), '/sw.js');
  assert.equal(await (await read('/icons/icon-192.png')).text(), '/icons/icon-192.png');
  assert.equal(await (await read('/rf-coverage/meta.json')).text(), '/internal/rf-coverage-public/meta.json');
  const nodeTile = '/tiles/nodes/n111111111111111111111111/1-2.png';
  assert.equal(await (await read('/rf-coverage' + nodeTile)).text(), '/internal/rf-coverage-public' + nodeTile);
  const credentials = { Cookie: 'session=forbidden', Authorization: 'Bearer forbidden', Accept: 'application/json' };
  const nodes = await read('/corescope/api/nodes?q=a%2Bb', { headers: credentials });
  assert.equal(nodes.status, 200);
  assert.equal(nodes.headers.get('x-corescope-privacy-generation'), '7');
  assert.equal(nodes.headers.get('set-cookie'), null);
  assert.equal(await nodes.text(), '/api/nodes?q=a%2Bb');
  const head = await read('/corescope/api/nodes', { method: 'HEAD', headers: credentials });
  assert.equal(head.status, 200);
  assert.equal(await head.text(), '');
  assert.equal(head.headers.get('set-cookie'), null);
  assert.equal((await read('/corescope/api/nodes', { method: 'POST' })).status, 405);
  assert.equal((await read('/corescope/ws')).status, 426);
  for (const route of ['/_projection-ready', '/corescope/api/public-projection-ready', '/corescope/api/debug/affinity',
    '/corescope/api/backup', '/api/nodes', '/corescope/static/api/backup', '/corescope/static/../api/backup',
    '/rf-coverage/raw/meta.json', '/rf-coverage/tiles/nodes/unverified/1-2.png', '/internal/rf-coverage-public/meta.json']) {
    assert.equal((await read(route)).status, 404, route);
  }
  // Send a raw path to Nginx so URL normalization in Fetch cannot remove it first.
  const rawStatus = path => new Promise((resolve, reject) => {
    const request = get({ hostname: '127.0.0.1', port: edgePort, path }, response => {
      response.resume();
      resolve(response.statusCode);
    });
    request.on('error', reject);
  });
  assert.equal(await rawStatus('/corescope/static/%2e%2e/api/backup'), 404);

  const connect = () => new Promise((resolve, reject) => {
    const ws = new WebSocket(origin.replace(/^http/, 'ws') + '/corescope/ws?privacyGeneration=7', {
      origin, headers: credentials, handshakeTimeout: 4_000,
    });
    ws.once('message', event => resolve(event.toString()));
    ws.once('error', reject);
  });
  assert.equal(await connect(), 'generation=7');
  const socketHeaders = requests.find(([route]) => route === '/?privacyGeneration=7')?.[1];
  assert.equal(socketHeaders?.origin, origin);
  assert.equal(socketHeaders?.host, `127.0.0.1:${edgePort}`);
  assert.ok(requests.every(([, header]) => !header.authorization && !header.cookie));
  const actualReads = () => requests.filter(([route]) => route.startsWith('/api/nodes') || route.startsWith('/?'));
  assert.equal(actualReads().length, 3);
  for (const status of [403, 404]) {
    readiness = status;
    assert.equal((await read('/corescope/api/nodes')).status, 503);
    assert.equal((await read('/rf-coverage' + nodeTile)).status, 503);
    await assert.rejects(connect(), /Unexpected server response: 503/);
  }
  assert.equal(actualReads().length, 3, 'no data or upgrade reached the upstream during authority loss');

  const feedOrigin = `http://127.0.0.1:${feedPort}`;
  const feed = (path, options) => fetch(feedOrigin + path, { signal: AbortSignal.timeout(4_000), ...options });
  assert.equal((await feed('/beacon/readyz')).status, 503);
  readiness = 204;
  assert.equal((await feed('/beacon/readyz')).status, 200);
  const channels = await feed('/feed?region=UK');
  assert.match(channels.headers.get('content-type'), /^text\/html/);
  assert.match(await channels.text(), /Beacon Channels preview/);
  assert.equal(await (await feed('/feed/packets?type=3')).text(), 'Packet Feed');
  assert.equal(await (await feed('/beacon-feed/assets/channels-abcdef.js')).text(), '/* Channels preview */');
  const messages = await feed('/beacon/api/v1/channels/7/messages?cursor=234', { headers: credentials });
  assert.equal(messages.headers.get('set-cookie'), null);
  assert.equal(await messages.text(), '/api/v1/channels/7/messages?cursor=234');
  assert.equal((await feed('/beacon/api/v1/channels', { method: 'POST' })).status, 405);
  assert.equal((await feed('/beacon/api/v1/admin')).status, 404);
  assert.equal((await feed('/beacon/api/v1/channels/0/messages')).status, 404);
  assert.equal((await feed('/beacon/ws')).status, 426);
  const upgraded = new Promise((resolve, reject) => {
    const ws = new WebSocket(feedOrigin.replace(/^http/, 'ws') + '/beacon/ws', {
      origin: feedOrigin, headers: credentials, handshakeTimeout: 4_000,
    });
    ws.once('message', event => resolve(event.toString()));
    ws.once('error', reject);
  });
  assert.equal(await upgraded, 'generation=7');
  assert.ok(requests.every(([, header]) => !header.authorization && !header.cookie));

  // Change just the Feed selection on the fixture and verify the live app
  // and Packet Feed still resolve after Nginx loads the previous config.
  async function reload(mapCandidate, feedCandidate, route, expected) {
    await writeFile(config, coreConfig(mapCandidate) + feedConfig(feedCandidate));
    for (const args of [['nginx', '-t'], ['nginx', '-s', 'reload']]) {
      const command = spawnSync('docker', ['exec', name, ...args], { encoding: 'utf8', timeout: 5_000 });
      assert.equal(command.status, 0, command.stderr);
    }
    for (let attempt = 0; attempt < 30; attempt++) {
      if (await (await route({ headers: { connection: 'close' } })).text() === expected) return;
      await delay(100);
    }
    assert.fail(`prior route did not return ${expected}`);
  }
  await reload(true, false, options => feed('/feed', options), 'Packet Feed default');
  assert.equal((await feed('/beacon/readyz')).status, 404);
  assert.equal(await (await feed('/feed/packets')).text(), 'Packet Feed');
  assert.match(await (await read('/')).text(), /CoreScope preview/);
  await reload(false, true, options => read('/', options), 'Legacy UKMesh map');
  assert.match(await (await feed('/feed')).text(), /Beacon Channels preview/);
});
