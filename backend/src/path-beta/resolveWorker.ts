/**
 * Worker thread entry point for path-beta resolution.
 * Each worker maintains its own DB connection pool and context cache,
 * keeping the main event loop free during CPU-heavy path computation.
 */
import { parentPort } from 'worker_threads';
import { resolveBetaPathForPacketHash, resolveMultiObserverBetaPath, resolveLazyPathForPacketHash } from './resolver.js';
import type { HeldPathEntry } from './resolveCache.js';

if (!parentPort) throw new Error('resolveWorker must run as a worker thread');

type WorkerMessage = {
  id: number;
  type: 'resolve' | 'resolveMulti' | 'resolveLazy';
  packetHash: string;
  network: string;
  requiredVisibilityGeneration: number;
  observer?: string;
  stickyMap?: Record<string, string>;
  stickyAgeFraction?: number;
  heldPath?: HeldPathEntry;
};

parentPort.on('message', (msg: WorkerMessage) => {
  const stickyMap = msg.stickyMap ? new Map(Object.entries(msg.stickyMap)) : undefined;
  const options = { heldPath: msg.heldPath, requiredVisibilityGeneration: msg.requiredVisibilityGeneration };
  const run = msg.type === 'resolveLazy'
    ? resolveLazyPathForPacketHash(msg.packetHash, msg.network, options)
    : msg.type === 'resolveMulti'
      ? resolveMultiObserverBetaPath(msg.packetHash, msg.network, stickyMap, msg.stickyAgeFraction, options)
      : resolveBetaPathForPacketHash(
          msg.packetHash,
          msg.network,
          msg.observer,
          stickyMap,
          msg.stickyAgeFraction,
          options,
        );

  run
    .then((result) => {
      parentPort!.postMessage({ id: msg.id, ok: true, result: result ?? null });
    })
    .catch((err: Error) => { parentPort!.postMessage({ id: msg.id, ok: false, error: err.message }); });
});
