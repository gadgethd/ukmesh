/**
 * Worker thread entry point for path-beta resolution.
 * Each worker maintains its own DB connection pool and context cache,
 * keeping the main event loop free during CPU-heavy path computation.
 */
import { parentPort } from 'worker_threads';
import { query } from '../db/index.js';
import { lazyResolvePath } from '../path-lazy/lazyResolver.js';
import { resolveBetaPathForPacketHash, resolveMultiObserverBetaPath } from './resolver.js';
import { getHeldPath, setHeldPath, type HeldPathEntry } from './resolveCache.js';

if (!parentPort) throw new Error('resolveWorker must run as a worker thread');

type WorkerMessage = {
  id: number;
  type: 'resolve' | 'resolveMulti' | 'resolveLazy';
  packetHash: string;
  network: string;
  observer?: string;
  stickyMap?: Record<string, string>;
  stickyAgeFraction?: number;
  heldPath?: HeldPathEntry;
};

parentPort.on('message', (msg: WorkerMessage) => {
  const stickyMap = msg.stickyMap ? new Map(Object.entries(msg.stickyMap)) : undefined;
  const heldPath = msg.heldPath ?? getHeldPath(msg.packetHash, msg.network);
  const run = msg.type === 'resolveLazy'
    ? lazyResolvePath(msg.packetHash, msg.network, query)
    : msg.type === 'resolveMulti'
      ? resolveMultiObserverBetaPath(msg.packetHash, msg.network, stickyMap, msg.stickyAgeFraction, { heldPath })
      : resolveBetaPathForPacketHash(
          msg.packetHash,
          msg.network,
          msg.observer,
          stickyMap,
          msg.stickyAgeFraction,
          { heldPath },
        );

  run
    .then((result) => {
      if (result && msg.type !== 'resolveLazy' && 'canonicalPath' in result) {
        const path = result.canonicalPath
          .map((hop: { nodeId?: string | null }) => hop.nodeId ?? '')
          .filter(Boolean);
        if (path.length === result.canonicalPath.length && path.length > 0) {
          setHeldPath(msg.packetHash, msg.network, {
            path,
            resolvedAt: Date.now(),
            physical: true,
          });
        }
      }
      parentPort!.postMessage({ id: msg.id, ok: true, result: result ?? null });
    })
    .catch((err: Error) => { parentPort!.postMessage({ id: msg.id, ok: false, error: err.message }); });
});
