import assert from 'node:assert/strict';
import { spawn, spawnSync } from 'node:child_process';
import { mkdtemp, mkdir, readFile, rm, writeFile } from 'node:fs/promises';
import { createServer } from 'node:http';
import { createRequire } from 'node:module';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import test from 'node:test';
import { setTimeout as delay } from 'node:timers/promises';
import { fileURLToPath } from 'node:url';

const require = createRequire(new URL('../../../backend/package.json', import.meta.url));
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
  await new Promise(resolve => reservation.close(resolve));

  const config = join(temporary, 'default.conf');
  const assets = join(temporary, 'public');
  await mkdir(join(assets, 'icons'), { recursive: true });
  await writeFile(join(assets, 'map.js'), '/* preview map */');
  await writeFile(join(assets, 'icons/phosphor-sprite.svg'), '<svg/>');
  await writeFile(config, `server {
    listen 127.0.0.1:${edgePort};
    set $corescope_projection http://127.0.0.1:${corePort};
    set $ukmesh_app http://127.0.0.1:${ukmeshPort};
    include /etc/nginx/corescope-app/preview-locations.conf;
  }\n`);
  const dockerfile = await readFile(new URL('../../../Dockerfile.app', import.meta.url), 'utf8');
  const image = dockerfile.match(/^FROM (nginx:\S+)$/m)?.[1];
  assert.ok(image, 'use the main app image pin');
  const name = 'ukmesh-core-edge-' + process.pid;
  const edge = spawn('docker', ['run', '--rm', '--name', name, '--network', 'host', '--memory', '32m',
    '--memory-swap', '32m', '-v', `${config}:/etc/nginx/conf.d/default.conf:ro`,
    '-v', `${fileURLToPath(new URL('.', import.meta.url))}:/etc/nginx/corescope-app:ro`,
    '-v', `${assets}:/srv/corescope/public:ro`, image], { stdio: ['ignore', 'pipe', 'pipe'] });
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
  assert.equal(await (await read('/icons/phosphor-sprite.svg')).text(), '<svg/>');
  assert.equal(await (await read('/api/runtime-config')).text(), '/api/runtime-config');
  assert.equal(await (await read('/rf-coverage/tiles/precision/1-2.png?v=7')).text(),
    '/rf-coverage/tiles/precision/1-2.png?v=7');
  const credentials = { Cookie: 'session=forbidden', Authorization: 'Bearer forbidden', Accept: 'application/json' };
  const nodes = await read('/corescope/api/nodes?q=a%2Bb', { headers: credentials });
  assert.equal(nodes.status, 200);
  assert.equal(nodes.headers.get('x-corescope-privacy-generation'), '7');
  assert.equal(nodes.headers.get('set-cookie'), null);
  assert.equal(await nodes.text(), '/api/nodes?q=a%2Bb');
  assert.equal((await read('/corescope/api/nodes', { method: 'POST' })).status, 405);
  assert.equal((await read('/corescope/ws')).status, 426);
  for (const route of ['/_projection-ready', '/corescope/api/public-projection-ready', '/corescope/api/debug/affinity',
    '/corescope/api/backup', '/api/nodes', '/corescope/static/api/backup', '/corescope/static/../api/backup']) {
    assert.equal((await read(route)).status, 404, route);
  }

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
  assert.equal(actualReads().length, 2);
  for (const status of [403, 404]) {
    readiness = status;
    assert.equal((await read('/corescope/api/nodes')).status, 503);
    await assert.rejects(connect(), /Unexpected server response: 503/);
  }
  assert.equal(actualReads().length, 2, 'no data or upgrade reached the upstream during authority loss');
});
