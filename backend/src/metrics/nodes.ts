import { Gauge } from 'prom-client';
import { metricsRegistry } from './registry.js';

export const mapSuppressedNodes = new Gauge({
  name: 'meshcore_map_suppressed_nodes',
  help: 'Stale co-located nodes excluded from the public map at the last refresh.',
  registers: [metricsRegistry],
});
