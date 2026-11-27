import { Histogram } from 'prom-client';
import { metricsRegistry } from './registry.js';

export const pathResolutionStageDuration = new Histogram({
  name: 'meshcore_path_resolution_stage_seconds',
  help: 'Time spent waiting or running on the bounded path resolver worker pool.',
  labelNames: ['priority', 'stage', 'outcome'] as const,
  buckets: [0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 15, 30, 60, 120],
  registers: [metricsRegistry],
});
