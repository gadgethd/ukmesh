import { createServer } from 'node:http';
import { readFile } from 'node:fs/promises';
import { createStatusReader } from './status.mjs';

const PORT = Number(process.env.PORT || 8080);
const TIMEOUT_MS = Number(process.env.PROBE_TIMEOUT_MS || 2500);
const page = await readFile(new URL('./index.html', import.meta.url));
const theme = await readFile('/theme/tokens.css').catch(error => {
  if (error.code !== 'ENOENT') throw error;
  return null;
});
const routes = JSON.parse(await readFile(new URL('./routes.json', import.meta.url), 'utf8'));
const targets = routes.filter((route) => route.access === 'app' || process.env.LAN_BIND_ADDRESS === '127.0.0.1');
const status = createStatusReader(targets, probe);

async function probe(url) {
  const started = performance.now();
  try {
    const response = await fetch(url, { redirect: 'manual', signal: AbortSignal.timeout(TIMEOUT_MS) });
    const status = response.status;
    await response.body?.cancel();
    return {
      status, up: status >= 200 && status < 400,
      ms: Math.round(performance.now() - started), error: null,
    };
  } catch (error) {
    return { status: null, up: false, ms: Math.round(performance.now() - started),
      error: error.name === 'TimeoutError' ? 'timeout' : 'unreachable' };
  }
}

const server = createServer(async (request, response) => {
  if (request.method !== 'GET') {
    response.writeHead(405, { allow: 'GET' }).end();
    return;
  }
  const path = request.url.split('?')[0];
  if (path === '/healthz') {
    response.writeHead(200, { 'content-type': 'text/plain' }).end('ok\n');
  } else if (path === '/api/status') {
    const body = JSON.stringify(await status());
    response.writeHead(200, { 'content-type': 'application/json', 'cache-control': 'no-store' }).end(body);
  } else if (path === '/' || path === '/index.html') {
    response.writeHead(200, { 'content-type': 'text/html; charset=utf-8', 'cache-control': 'no-cache' }).end(page);
  } else if (path === '/ukmesh-theme/tokens.css' && theme) {
    response.writeHead(200, { 'content-type': 'text/css; charset=utf-8', 'cache-control': 'no-cache' }).end(theme);
  } else {
    response.writeHead(404, { 'content-type': 'text/plain' }).end('not found\n');
  }
});

server.listen(PORT, '0.0.0.0', () => {
  console.log(`ukmesh-lan status site on :${PORT} (${targets.length} targets)`);
});
