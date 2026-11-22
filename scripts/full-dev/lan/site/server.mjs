import { createServer } from 'node:http';
import { readFile } from 'node:fs/promises';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';

const HERE = dirname(fileURLToPath(import.meta.url));
const PORT = Number(process.env.PORT || 8080);
const TIMEOUT_MS = Number(process.env.PROBE_TIMEOUT_MS || 2500);

// Single source of truth for both the status probes and the link cards.
const TARGETS = [
  { id: 'ukmesh', name: 'MeshCore app', port: 8081, group: 'main',
    probe: 'http://app-ukmesh:8080/', note: 'Primary UKMesh application' },
  { id: 'corescope', name: 'CoreScope', port: 8082, group: 'companions',
    probe: 'http://corescope:3000/', note: 'Packet analytics, MQTT ingest' },
  { id: 'website', name: 'UKMesh website', port: 8083, group: 'main',
    probe: 'http://website-ukmesh:8080/', note: 'Marketing site' },
  { id: 'beacon-web', name: 'Beacon web', port: 8084, group: 'companions',
    probe: 'http://beacon-web:8080/', note: 'Beacon companion UI' },
  { id: 'grafana', name: 'Grafana', port: 8085, group: 'observability',
    probe: 'http://grafana:3000/api/health', note: 'Dashboards' },
  { id: 'prometheus', name: 'Prometheus', port: 8086, group: 'observability',
    probe: 'http://prometheus:9090/-/healthy', note: 'Metrics' },
  { id: 'website-dev', name: 'Website (dev)', port: 8087, group: 'main',
    probe: 'http://website-dev:8080/', note: 'Development website' },
  { id: 'beacon-api', name: 'Beacon API', port: 8088, group: 'companions',
    probe: 'http://beacon:8080/', note: 'Beacon service API' },
  { id: 'health', name: 'Mesh health check', port: 8089, group: 'observability',
    probe: 'http://mesh-health-check:3090/', note: 'Health checks' },
];

async function probe(target) {
  const started = performance.now();
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), TIMEOUT_MS);
  try {
    const response = await fetch(target.probe, {
      redirect: 'manual',
      signal: controller.signal,
      headers: { accept: '*/*', 'user-agent': 'ukmesh-lan-status/1' },
    });
    const ms = Math.round(performance.now() - started);
    return { id: target.id, up: response.status < 500, status: response.status, ms, error: null };
  } catch (error) {
    const ms = Math.round(performance.now() - started);
    return { id: target.id, up: false, status: null, ms,
             error: error.name === 'AbortError' ? 'timeout' : String(error.cause?.code || error.message) };
  } finally {
    clearTimeout(timer);
  }
}

async function payload() {
  const results = await Promise.all(TARGETS.map(probe));
  const byId = new Map(results.map((row) => [row.id, row]));
  return {
    at: new Date().toISOString(),
    targets: TARGETS.map((t) => ({
      id: t.id, name: t.name, port: t.port, group: t.group, note: t.note,
      ...byId.get(t.id),
    })),
  };
}

const server = createServer(async (request, response) => {
  const url = new URL(request.url, 'http://localhost');
  if (url.pathname === '/api/status') {
    const body = JSON.stringify(await payload());
    response.writeHead(200, { 'content-type': 'application/json', 'cache-control': 'no-store' });
    response.end(body);
    return;
  }
  if (url.pathname === '/' || url.pathname === '/index.html') {
    const body = await readFile(join(HERE, 'index.html'));
    response.writeHead(200, { 'content-type': 'text/html; charset=utf-8', 'cache-control': 'no-store' });
    response.end(body);
    return;
  }
  response.writeHead(404, { 'content-type': 'text/plain' });
  response.end('not found\n');
});

server.listen(PORT, '0.0.0.0', () => {
  console.log(`ukmesh-lan status site on :${PORT} (${TARGETS.length} targets)`);
});
