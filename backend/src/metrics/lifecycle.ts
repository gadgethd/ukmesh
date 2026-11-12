import { Counter } from 'prom-client';
import { metricsRegistry } from './registry.js';

export const gracefulShutdownTotal = new Counter({
  name: 'meshcore_graceful_shutdown_total',
  help: 'Graceful shutdown attempts by bounded outcome.',
  labelNames: ['outcome'] as const,
  registers: [metricsRegistry],
});
