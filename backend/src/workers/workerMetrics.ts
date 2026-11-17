import { startMetricsServer } from '../metricsServer.js';
import type { Server } from 'node:http';

export function startWorkerMetrics(): Server {
  const parsed = Number(process.env['METRICS_PORT'] ?? 9091);
  const port = Number.isSafeInteger(parsed) && parsed >= 1 && parsed <= 65_535
    ? parsed
    : 9091;
  return startMetricsServer(port, process.env['METRICS_HOST'] ?? '0.0.0.0');
}
