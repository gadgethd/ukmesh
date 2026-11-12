import { createCollectorSlot } from './collector.js';
import { Counter, Gauge } from 'prom-client';
import { metricsRegistry } from './registry.js';

export const privacyFilterTotal = new Counter({
  name: 'meshcore_privacy_filter_total',
  help: 'Privacy-filter decisions by bounded operation and outcome.',
  labelNames: ['operation', 'outcome'] as const,
  registers: [metricsRegistry],
});

const collector = createCollectorSlot<(() => Promise<boolean>) | null>(null);

export function setPrivacyGenerationHealthCollector(read: (() => Promise<boolean>) | null): () => void {
  return collector.register(read);
}

export const privacyGenerationHealthy = new Gauge({
  name: 'meshcore_privacy_generation_healthy',
  help: 'One when desired, materialized and globally admissible privacy generations agree; zero on drift or an unavailable observation.',
  registers: [metricsRegistry],
  async collect() {
    this.set(0);
    const read = collector.read();
    if (read) {
      try { this.set(await read() ? 1 : 0); }
      catch { this.set(0); }
    }
  },
});
