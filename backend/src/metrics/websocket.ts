import { Counter, Gauge } from 'prom-client';
import { metricsRegistry } from './registry.js';

export const websocketClients = new Gauge({
  name: 'meshcore_websocket_clients',
  help: 'Currently connected WebSocket clients.',
  registers: [metricsRegistry],
});

export const publicLiveOutcomesTotal = new Counter({
  name: 'meshcore_public_live_outcomes_total',
  help: 'Public live events authorized or suppressed at a bounded delivery batch.',
  labelNames: ['phase', 'outcome'] as const,
  registers: [metricsRegistry],
});

export const websocketAdmissionsTotal = new Counter({
  name: 'meshcore_websocket_admissions_total',
  help: 'WebSocket handshake decisions by bounded outcome.',
  labelNames: ['outcome'] as const,
  registers: [metricsRegistry],
});
