import { Router } from 'express';
import { HEALTH_CACHE_TTL_MS } from '../bootstrap/caches.js';
import { HealthSnapshotCache } from '../../health/snapshot.js';
import { toPublicHealthOverview } from '../../health/projections.js';
import type { HealthRuntime } from '../../health/runtime.js';
import { getMqttRuntimeStatus } from '../../mqtt/client.js';
import { withMqttDeliveryHealth } from '../../health/mqttDelivery.js';

export function createHealthRoutes(healthRuntime: HealthRuntime) {
    const router = Router();
    const healthSnapshot = new HealthSnapshotCache(healthRuntime.getWorkerHealthOverview, Math.max(HEALTH_CACHE_TTL_MS * 5, 5 * 60_000));
    let timer: ReturnType<typeof setInterval> | undefined;
    let closed = false;
  router.get('/health', (_req, res) => {
    const current = healthSnapshot.read();
    if (!current.ready) {
      res.setHeader('Cache-Control', 'public, max-age=5');
      res.status(503).json({
        status: 'initializing',
        generatedAt: current.generatedAt == null
          ? null
          : new Date(current.generatedAt).toISOString(),
        lastError: current.lastError,
      });
      return;
    }
    res.setHeader('Cache-Control', 'public, max-age=30, stale-while-revalidate=60');
    res.json(withMqttDeliveryHealth(toPublicHealthOverview(current.data), getMqttRuntimeStatus().delivery));
  });


    return {
      router,
      start() {
        if (timer || closed) return;
        timer = setInterval(() => void healthSnapshot.refresh(), HEALTH_CACHE_TTL_MS);
        timer.unref();
        void healthSnapshot.refresh();
      },
      close() { closed = true; healthSnapshot.close(); if (timer) clearInterval(timer); timer = undefined; },
    };
}
