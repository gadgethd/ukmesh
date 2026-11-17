/** Eight real loopback clients inside the backend namespace; no proxy headers. */
import http from 'node:http';
import { chmod } from 'node:fs/promises';
import { pathToFileURL } from 'node:url';

export function allowedRoute(value) {
  const url = new URL(value, 'http://owned');
  return url.origin === 'http://owned' && (
    ['/api/nodes/map', '/api/packets/recent', '/api/stats/charts'].includes(url.pathname)
    || /^\/api\/nodes\/[a-fA-F0-9]{64}\/(history|links)$/.test(url.pathname)
    || /^\/api\/packets\/[a-fA-F0-9]{64}$/.test(url.pathname));
}
export function createReader(upstreamPort = 3000) {
  const agents = Array.from({length: 8}, (_, index) => new http.Agent({keepAlive: true,
    maxSockets: 1, localAddress: `127.0.3.${index + 1}`}));
  let active = 0;
  const server = http.createServer((request, response) => {
    const index = Number(request.headers['x-owned-reader-slot']);
    if (request.method !== 'GET' || !Number.isInteger(index) || index < 0 || index > 7
      || !allowedRoute(request.url) || active >= 8) {
      response.writeHead(400); response.end(); return;
    }
    active++;
    const upstream = http.get({host: '127.0.0.1', port: upstreamPort, path: request.url,
      agent: agents[index], timeout: 15000, headers: {'user-agent': 'UKMesh-owned-soak-reader'}}, received => {
      let bytes = 0;
      response.writeHead(received.statusCode, {'content-type': received.headers['content-type'] ?? 'application/json'});
      received.on('data', chunk => { bytes += chunk.length; if (bytes > 8 * 1024 * 1024) received.destroy(); });
      received.on('error', () => response.destroy());
      received.pipe(response);
    });
    upstream.on('timeout', () => upstream.destroy());
    upstream.on('error', () => { if (!response.headersSent) response.writeHead(502); response.end(); });
    response.once('close', () => { active--; upstream.destroy(); });
  });
  server.on('close', () => agents.forEach(agent => agent.destroy()));
  return server;
}
if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  const socket = process.argv[2];
  if (socket !== '/reader/reader.sock') throw new Error('owned reader socket required');
  const server = createReader();
  server.listen(socket, async () => { await chmod(socket, 0o600); });
  process.on('SIGTERM', () => server.close());
}
