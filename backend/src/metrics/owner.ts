import { Counter, Gauge } from 'prom-client';
import { metricsRegistry } from './registry.js';

export const ownerAclReloadTotal = new Counter({
  name: 'meshcore_owner_acl_reload_total',
  help: 'Authenticated Mosquitto ACL reload requests by bounded outcome.',
  labelNames: ['outcome'] as const,
  registers: [metricsRegistry],
});

export const webhookDeliveriesTotal = new Counter({
  name: 'meshcore_owner_webhook_deliveries_total',
  help: 'Owner webhook delivery attempts by bounded outcome.',
  labelNames: ['outcome'] as const,
  registers: [metricsRegistry],
});

export const packetShareOutcomesTotal = new Counter({
  name: 'meshcore_owner_packet_share_outcomes_total',
  help: 'Owner packet sharing decisions and delivery outcomes.',
  labelNames: ['outcome'] as const,
  registers: [metricsRegistry],
});

export const packetShareQueueDepth = new Gauge({
  name: 'meshcore_owner_packet_share_queue_depth',
  help: 'Owner packet sharing deliveries currently claimed for processing.',
  registers: [metricsRegistry],
});

export const publicFeedOutcomesTotal = new Counter({
  name: 'meshcore_public_feed_outcomes_total',
  help: 'Public MeshCore feed decisions and delivery outcomes.',
  labelNames: ['outcome'] as const,
  registers: [metricsRegistry],
});

export const publicFeedQueueDepth = new Gauge({
  name: 'meshcore_public_feed_queue_depth',
  help: 'Public MeshCore feed deliveries awaiting completion.',
  registers: [metricsRegistry],
});

export const publicFeedOldestAgeSeconds = new Gauge({
  name: 'meshcore_public_feed_oldest_age_seconds',
  help: 'Age of the oldest outstanding public feed delivery.',
  registers: [metricsRegistry],
});
