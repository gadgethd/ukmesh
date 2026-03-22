/**
 * Worker thread entry point for path-beta resolution.
 * Each worker maintains its own DB connection pool and context cache,
 * keeping the main event loop free during CPU-heavy path computation.
 */
import { parentPort } from 'worker_threads';
import { resolveBetaPathForPacketHash, resolveMultiObserverBetaPath } from './resolver.js';

if (!parentPort) throw new Error('resolveWorker must run as a worker thread');

type WorkerMessage = {
  id: number;
  type: 'resolve' | 'resolveMulti';
  packetHash: string;
  network: string;
  observer?: string;
  stickyMap?: Record<string, string>;
  stickyAgeFraction?: number;
};

parentPort.on('message', (msg: WorkerMessage) => {
  const stickyMap = msg.stickyMap ? new Map(Object.entries(msg.stickyMap)) : undefined;
  const run = msg.type === 'resolveMulti'
    ? resolveMultiObserverBetaPath(msg.packetHash, msg.network, stickyMap, msg.stickyAgeFraction)
    : resolveBetaPathForPacketHash(msg.packetHash, msg.network, msg.observer, stickyMap, msg.stickyAgeFraction);

  run
    .then((result) => { parentPort!.postMessage({ id: msg.id, ok: true, result: result ?? null }); })
    .catch((err: Error) => { parentPort!.postMessage({ id: msg.id, ok: false, error: err.message }); });
});
