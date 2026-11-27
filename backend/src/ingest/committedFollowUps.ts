import { reactivateHistoricPathNodes } from '../repositories/pathEvidence.js';
import { postPersistDroppedTotal, postPersistInFlightGauge } from '../metrics/ingest.js';
import { makePathObservation, type PathObservation } from '../path-beta/observation.js';
import type { DatabaseQueryFn } from '../db/contracts.js';
import type { IngestEvents } from './events.js';

export type PostPersistFollowUpArgs = {
  finalHash: string;
  decodedPathHashSizeBytes: number | undefined;
  path: string[] | undefined;
  decodedRouteType: number | undefined;
  network: string;
  resolvedPacketType: number | undefined;
};

export function createCommittedFollowUps({ query, observePath, emitNodeUpsert }: {
  query: DatabaseQueryFn;
  observePath?: (observation: PathObservation) => void;
  emitNodeUpsert: IngestEvents['emitNodeUpsert'];
}) {
  const active = new Set<Promise<void>>();
  const maxInFlight = 64;
  let closed = false;

  function schedule(args: PostPersistFollowUpArgs): void {
    if (closed) return;

    // Path caches need every post-commit hint, even when historic node lookups
    // have filled their own capacity. A live publisher never delays PUBACK.
    if (observePath) {
      try { observePath(makePathObservation(args)); }
      catch (error) { console.error('[path] observation notice failed:', (error as Error).message); }
    }

    const { decodedPathHashSizeBytes, decodedRouteType, network, path } = args;
    if (decodedPathHashSizeBytes == null || decodedPathHashSizeBytes < 2 || !path?.length) return;
    if (active.size >= maxInFlight) {
      postPersistDroppedTotal.inc();
      return;
    }

    // Multibyte path evidence can reactivate a unique repeater with stored
    // coordinates; a full row restores it for clients that dropped it.
    const work = reactivateHistoricPathNodes(query, {
      pathHashes: path,
      sizeBytes: decodedPathHashSizeBytes,
      seenAt: new Date(),
      routeType: decodedRouteType,
      network,
    }).then((nodes) => {
      for (const node of nodes) emitNodeUpsert({ ...node });
    }).catch((error: unknown) => {
      console.error('[mqtt] path evidence error:', (error as Error).message);
    }).finally(() => {
      active.delete(work);
      postPersistInFlightGauge.set(active.size);
    });
    active.add(work);
    postPersistInFlightGauge.set(active.size);
  }

  return {
    schedule,
    async close() {
      closed = true;
      await Promise.allSettled([...active]);
    },
  };
}
export type CommittedFollowUps = ReturnType<typeof createCommittedFollowUps>;
