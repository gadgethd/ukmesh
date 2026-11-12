import { Counter, Histogram } from 'prom-client';
import { metricsRegistry } from './registry.js';
import type { NextFunction, Request, Response } from 'express';

export const httpRequestsTotal = new Counter({
  name: 'meshcore_http_requests_total',
  help: 'HTTP requests by method and bounded status class.',
  labelNames: ['method', 'status_class'] as const,
  registers: [metricsRegistry],
});

export const httpRequestDuration = new Histogram({
  name: 'meshcore_http_request_duration_seconds',
  help: 'HTTP response latency by method and bounded status class.',
  labelNames: ['method', 'status_class'] as const,
  buckets: [0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30],
  registers: [metricsRegistry],
});

const measuredPublicRoutes = new Set([
  '/api/stats/charts', '/api/nodes/map', '/api/regions/summary', '/api/regions/repeaters',
  '/api/topology', '/api/spam/messages/status', '/api/runtime-config', '/api/packets/recent',
  '/api/observer-activity', '/api/stats',
]);

export const publicRouteDuration = new Histogram({
  name: 'meshcore_public_route_duration_seconds',
  help: 'Public response latency for a fixed allowlist of website routes; excludes user-supplied path/query values.',
  labelNames: ['route', 'status_class'] as const,
  buckets: [0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30],
  registers: [metricsRegistry],
});

const HTTP_METHODS = new Set(['GET', 'POST', 'PUT', 'PATCH', 'DELETE', 'OPTIONS', 'HEAD']);

function boundedHttpMethod(method: string): string {
  const normalized = method.toUpperCase();
  return HTTP_METHODS.has(normalized) ? normalized : 'OTHER';
}

function statusClass(statusCode: number): string {
  if (statusCode >= 100 && statusCode <= 599) return `${Math.floor(statusCode / 100)}xx`;
  return 'other';
}

export function observeHttpRequest(req: Request, res: Response, next: NextFunction): void {
  const startedAt = process.hrtime.bigint();
  let observed = false;
  const observe = () => {
    if (observed) return;
    observed = true;
    const labels = {
      method: boundedHttpMethod(req.method),
      status_class: statusClass(res.statusCode),
    };
    const durationSeconds = Number(process.hrtime.bigint() - startedAt) / 1e9;
    httpRequestsTotal.inc(labels);
    httpRequestDuration.observe(labels, durationSeconds);
    const route = req.originalUrl.split('?', 1)[0];
    if (req.method === 'GET' && measuredPublicRoutes.has(route)) {
      publicRouteDuration.observe({ route, status_class: labels.status_class }, durationSeconds);
    }
  };
  res.once('finish', observe);
  res.once('close', observe);
  next();
}
