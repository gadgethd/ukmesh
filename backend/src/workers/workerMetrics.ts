import { startMetricsServer } from '../metricsServer.js';

export function startWorkerMetrics(): void {
  const parsed = Number(process.env['METRICS_PORT'] ?? 9091);
  const port = Number.isSafeInteger(parsed) && parsed >= 1 && parsed <= 65_535
    ? parsed
    : 9091;
  startMetricsServer(port);
}
