import { Histogram } from 'prom-client';
import { metricsRegistry } from './registry.js';

export const frontendLcp = new Histogram({
  name: 'meshcore_frontend_lcp_seconds', help: 'Anonymous browser LCP samples for a fixed public route allowlist.',
  labelNames: ['route'] as const, buckets: [0.5, 1, 1.5, 2.5, 4, 6, 10, 30, 60], registers: [metricsRegistry],
});

export const frontendInp = new Histogram({
  name: 'meshcore_frontend_inp_seconds', help: 'Anonymous observed browser INP samples; absent without an interaction.',
  labelNames: ['route'] as const, buckets: [0.05, 0.1, 0.2, 0.5, 1, 2, 5, 10], registers: [metricsRegistry],
});

export const frontendCls = new Histogram({
  name: 'meshcore_frontend_cls', help: 'Anonymous browser CLS samples for a fixed public route allowlist.',
  labelNames: ['route'] as const, buckets: [0, 0.01, 0.05, 0.1, 0.25, 0.5, 1, 2, 10], registers: [metricsRegistry],
});
