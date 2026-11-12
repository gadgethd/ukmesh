import { Gauge } from 'prom-client';
import { metricsRegistry } from './registry.js';

export const linkQueueDepth = new Gauge({
  name: 'meshcore_link_queue_active_jobs',
  help: 'Queued and leased durable link jobs.',
  registers: [metricsRegistry],
});

export const linkQueueBytes = new Gauge({
  name: 'meshcore_link_queue_active_bytes',
  help: 'Serialized bytes retained by active durable link jobs.',
  registers: [metricsRegistry],
});

export const linkQueueDeadJobs = new Gauge({
  name: 'meshcore_link_queue_dead_jobs',
  help: 'Retained dead-letter link jobs.',
  registers: [metricsRegistry],
});

export const linkQueueDeadBytes = new Gauge({
  name: 'meshcore_link_queue_dead_bytes',
  help: 'Serialized bytes retained by dead-letter link jobs.',
  registers: [metricsRegistry],
});

export const linkQueueLeases = new Gauge({
  name: 'meshcore_link_queue_active_leases',
  help: 'Currently leased link jobs.',
  registers: [metricsRegistry],
});

export const linkQueueRetries = new Gauge({
  name: 'meshcore_link_queue_retries',
  help: 'Retry attempts retained in active and dead link jobs.',
  registers: [metricsRegistry],
});

export const linkQueueOldestAgeSeconds = new Gauge({
  name: 'meshcore_link_queue_oldest_age_seconds',
  help: 'Age of the oldest active link job.',
  registers: [metricsRegistry],
});
