import { Counter } from 'prom-client';
import { metricsRegistry } from './registry.js';

export const observerRegistrationSubmissionsTotal = new Counter({
  name: 'meshcore_observer_registration_submissions_total',
  help: 'Observer registration submissions by bounded outcome.',
  labelNames: ['outcome'] as const,
  registers: [metricsRegistry],
});

export const observerRegistrationWebhookDeliveriesTotal = new Counter({
  name: 'meshcore_observer_registration_webhook_deliveries_total',
  help: 'Observer registration webhook delivery attempts by bounded outcome.',
  labelNames: ['outcome'] as const,
  registers: [metricsRegistry],
});
