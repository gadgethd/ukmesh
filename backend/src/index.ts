import { closeDb } from './db/runtime.js';
import { closeOwnerAuthDb } from './owner/ownerAuth.js';
import { closeMetricsServer, startMetricsServer } from './metricsServer.js';
import { LifecycleCoordinator, LifecycleDeadlineError } from './lifecycle/coordinator.js';
import { gracefulShutdownTotal } from './metrics/lifecycle.js';

const ROLE = process.env['UKMESH_RUNTIME_ROLE'] ?? 'combined';
if (ROLE !== 'api' && ROLE !== 'ingest' && ROLE !== 'combined') throw new Error('Invalid UKMESH_RUNTIME_ROLE');
const SHUTDOWN_DEADLINE_MS = Math.min(
  30_000,
  Math.max(1_000, Number(process.env['SHUTDOWN_DEADLINE_MS'] ?? 30_000) || 30_000),
);
const lifecycle = new LifecycleCoordinator(SHUTDOWN_DEADLINE_MS);
let shutdownExitCode = 0;
let shutdownDeadlineAt = 0;
function shutdownTimeLeft(): number {
  return Math.max(1, (shutdownDeadlineAt || Date.now() + SHUTDOWN_DEADLINE_MS) - Date.now());
}
let forceCloseHttpConnections = () => {};

async function shutdown(reason: string, exitCode: number): Promise<void> {
  shutdownExitCode = Math.max(shutdownExitCode, exitCode);
  if (shutdownDeadlineAt === 0) shutdownDeadlineAt = Date.now() + SHUTDOWN_DEADLINE_MS;
  if (!lifecycle.isDraining) console.log(`[app] draining after ${reason}`);
  try {
    await lifecycle.drain(reason);
    gracefulShutdownTotal.inc({ outcome: 'success' });
    console.log('[app] graceful shutdown complete');
    process.exit(shutdownExitCode);
  } catch (error) {
    gracefulShutdownTotal.inc({
      outcome: error instanceof LifecycleDeadlineError ? 'deadline' : 'failure',
    });
    forceCloseHttpConnections();
    const diagnostics = lifecycle.snapshot();
    if (error instanceof LifecycleDeadlineError) {
      console.error('[app] graceful shutdown timed out', diagnostics);
    } else {
      console.error('[app] graceful shutdown failed:', error, diagnostics);
    }
    process.exit(1);
  }
}

process.on('SIGTERM', () => { void shutdown('SIGTERM', 0); });
process.on('SIGINT', () => { void shutdown('SIGINT', 0); });
process.on('unhandledRejection', (reason) => {
  console.error('[app] unhandled rejection:', reason);
  void shutdown('unhandledRejection', 1);
});
process.on('uncaughtException', (error) => {
  console.error('[app] uncaught exception:', error);
  void shutdown('uncaughtException', 1);
});

async function main() {
  lifecycle.register({ name: 'metrics-server', stage: 30, close: closeMetricsServer });
  lifecycle.register({ name: 'owner-auth-database', stage: 40, close: closeOwnerAuthDb });
  lifecycle.register({ name: 'application-database', stage: 40, close: closeDb });
  const metricsPort = Number(process.env['METRICS_PORT'] ?? 9091);
  startMetricsServer(metricsPort, process.env['METRICS_HOST'] ?? '0.0.0.0');
  if (ROLE === 'ingest') {
    const { startIngestWorker } = await import('./ingest/service.js');
    startIngestWorker({ lifecycle, shutdownTimeLeft });
    return;
  }
  const { startApiService } = await import('./api/service.js');
  await startApiService({ lifecycle, shutdownTimeLeft, combined: ROLE === 'combined',
    setForceClose: (close) => { forceCloseHttpConnections = close; } });
}

main().catch((err) => {
  console.error('[app] fatal startup error:', err);
  void shutdown('startup failure', 1);
});
