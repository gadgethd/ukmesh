import { initializeMetrics } from './metrics/runtime.js';
import http, { type Server } from 'node:http';
import { metricsRegistry } from './metrics/registry.js';

let server: Server | null = null;

export function startMetricsServer(port: number, host = '0.0.0.0'): Server {
  if (server) return server;
  initializeMetrics();
  server = http.createServer(async (req, res) => {
    if (req.method !== 'GET' || req.url !== '/metrics') {
      res.writeHead(404, { 'content-type': 'application/json' });
      res.end('{"error":"not found"}');
      return;
    }
    try {
      res.writeHead(200, { 'content-type': metricsRegistry.contentType });
      res.end(await metricsRegistry.metrics());
    } catch (error) {
      console.error('[metrics] scrape failed:', error instanceof Error ? error.message : error);
      res.writeHead(500, { 'content-type': 'application/json' });
      res.end('{"error":"metrics unavailable"}');
    }
  });
  server.listen(port, host, () => {
    console.log(`[metrics] listening on internal port ${port}`);
  });
  return server;
}

export async function closeMetricsServer(): Promise<void> {
  if (!server) return;
  const current = server;
  server = null;
  await new Promise<void>((resolve, reject) => {
    current.close((error) => error ? reject(error) : resolve());
  });
}
