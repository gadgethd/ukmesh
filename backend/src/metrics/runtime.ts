import { collectDefaultMetrics } from 'prom-client';
import { metricsRegistry } from './registry.js';
import './ingest.js';
import './owner.js';
import './websocket.js';
import './nodes.js';
import './stats.js';
import './database.js';
import './queue.js';
import './http.js';
import './frontend.js';
import './cache.js';
import './analysis.js';
import './spam.js';
import './regions.js';
import './workers.js';
import './synthetic.js';
import './health.js';
import './privacy.js';
import './lifecycle.js';
import './labels.js';
import './ram.js';

let processMetricsInitialized = false;

/** Register the unchanged process inventory once, when the metrics runtime starts. */
export function initializeMetrics(): void {
  if (processMetricsInitialized) return;
  processMetricsInitialized = true;
  collectDefaultMetrics({ register: metricsRegistry, prefix: 'meshcore_process_' });
}
