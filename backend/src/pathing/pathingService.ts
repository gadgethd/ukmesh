import type { PathingRepository } from './pathingRepository.js';

type ResolvePoolFn = {
  run<T>(
    job:
      | { type: 'resolve'; packetHash: string; network: string; observer?: string | null }
      | { type: 'resolveMulti'; packetHash: string; network: string },
  ): Promise<T | null>;
};

type PathHistoryCacheEntry = {
  ts: number;
  data: unknown;
};

type PathingServiceDeps = {
  pathHistoryCache: Map<string, PathHistoryCacheEntry>;
  pathHistoryCacheTtlMs: number;
  getResolveCache: (key: string) => unknown;
  setResolveCache: (key: string, value: unknown) => void;
  resolvePool: ResolvePoolFn;
  repository: PathingRepository;
};

export type PathingService = ReturnType<typeof createPathingService>;

export function createPathingService(deps: PathingServiceDeps) {
  const {
    pathHistoryCache,
    pathHistoryCacheTtlMs,
    getResolveCache,
    setResolveCache,
    resolvePool,
    repository,
  } = deps;

  async function resolvePacket(packetHash: string, network: string, observer?: string | null): Promise<unknown> {
    const cacheKey = `r|${packetHash}|${network}|${observer ?? ''}`;
    const cached = getResolveCache(cacheKey);
    if (cached) return cached;

    const resolved = await resolvePool.run<unknown>({
      type: 'resolve',
      packetHash,
      network,
      observer,
    });
    if (!resolved) {
      throw new Error('PACKET_NOT_FOUND');
    }

    setResolveCache(cacheKey, resolved);
    return resolved;
  }

  async function resolvePacketMulti(packetHash: string, network: string): Promise<unknown> {
    const cacheKey = `m|${packetHash}|${network}`;
    const cached = getResolveCache(cacheKey);
    if (cached) return cached;

    const resolved = await resolvePool.run<unknown>({
      type: 'resolveMulti',
      packetHash,
      network,
    });
    if (!resolved) {
      throw new Error('PACKET_NOT_FOUND');
    }

    setResolveCache(cacheKey, resolved);
    return resolved;
  }

  async function getPathHistory(scope: string): Promise<unknown> {
    const memoryCached = pathHistoryCache.get(scope);
    if (memoryCached && Date.now() - memoryCached.ts < pathHistoryCacheTtlMs) {
      return memoryCached.data;
    }

    const cached = await repository.fetchPathHistory(scope);
    let responseData: unknown;
    if (!cached) {
      responseData = {
        ok: true,
        scope,
        windowStart: null,
        updatedAt: null,
        packetCount: 0,
        resolvedPacketCount: 0,
        maxCount: 0,
        segments: [],
      };
    } else {
      const segments = Array.isArray(cached.segment_counts) ? cached.segment_counts : [];
      const maxCount = segments.reduce((max, segment) => Math.max(max, Number(segment.count ?? 0)), 0);
      responseData = {
        ok: true,
        scope,
        windowStart: cached.window_start,
        updatedAt: cached.updated_at,
        packetCount: cached.packet_count,
        resolvedPacketCount: cached.resolved_packet_count,
        maxCount,
        segments,
      };
    }

    pathHistoryCache.set(scope, { ts: Date.now(), data: responseData });
    return responseData;
  }

  async function getPathLearning(network: string, limit: number): Promise<unknown> {
    const {
      prefixRows,
      transitionRows,
      edgeRows,
      motifRows,
      calibrationRows,
    } = await repository.fetchPathLearning(network, limit);

    const calibration = calibrationRows.rows[0] ?? {
      evaluated_packets: 0,
      top1_accuracy: 0,
      mean_pred_confidence: 0,
      confidence_scale: 1,
      confidence_bias: 0,
      recommended_threshold: 0.5,
    };

    return {
      network,
      calibration,
      prefixPriors: prefixRows.rows,
      transitionPriors: transitionRows.rows,
      edgePriors: edgeRows.rows,
      motifPriors: motifRows.rows,
    };
  }

  return {
    resolvePacket,
    resolvePacketMulti,
    getPathHistory,
    getPathLearning,
  };
}
