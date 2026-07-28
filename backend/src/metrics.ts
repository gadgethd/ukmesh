import {
  Counter,
  Gauge,
  Histogram,
  Registry,
  collectDefaultMetrics,
} from 'prom-client';

export const metricsRegistry = new Registry();
collectDefaultMetrics({ register: metricsRegistry, prefix: 'meshcore_process_' });

export const mqttMessagesTotal = new Counter({
  name: 'meshcore_mqtt_messages_total',
  help: 'MQTT messages accepted by the ingest handler.',
  labelNames: ['network'] as const,
  registers: [metricsRegistry],
});

export const websocketClients = new Gauge({
  name: 'meshcore_websocket_clients',
  help: 'Currently connected WebSocket clients.',
  registers: [metricsRegistry],
});

export const statsRecomputeTotal = new Counter({
  name: 'meshcore_stats_recompute_total',
  help: 'Cold statistics recomputations.',
  labelNames: ['network', 'status'] as const,
  registers: [metricsRegistry],
});

export const statsRecomputeDuration = new Histogram({
  name: 'meshcore_stats_recompute_duration_seconds',
  help: 'Cold statistics recomputation duration.',
  labelNames: ['network', 'status'] as const,
  buckets: [0.05, 0.1, 0.25, 0.5, 1, 2, 5, 10, 30, 60, 120],
  registers: [metricsRegistry],
});

export const dbPoolTotal = new Gauge({
  name: 'meshcore_db_pool_connections',
  help: 'PostgreSQL pool connections by state.',
  labelNames: ['state'] as const,
  registers: [metricsRegistry],
});
