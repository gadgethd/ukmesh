import type { PathingRepository } from './pathingRepository.js';
import type { HeldPathEntry } from '../path-beta/resolveCache.js';
import {
  toPublicBetaResultDto,
  toPublicMultiObserverDto,
  type PublicBetaResultDto,
  type PublicMultiObserverDto,
} from './pathingPublicDto.js';

type ResolvePoolFn = {
  run<T>(
    job:
      | { type: 'resolve'; packetHash: string; network: string; observer?: string | null; heldPath?: HeldPathEntry }
      | { type: 'resolveMulti'; packetHash: string; network: string; heldPath?: HeldPathEntry },
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
  getHeldPath: (packetHash: string, network: string) => HeldPathEntry | undefined;
  setHeldPath: (packetHash: string, network: string, value: HeldPathEntry) => void;
  resolvePool: ResolvePoolFn;
  repository: PathingRepository;
};

const resolveInflightSingle = new Map<string, Promise<PublicBetaResultDto>>();
const resolveInflightMulti = new Map<string, Promise<PublicMultiObserverDto>>();
const observerVariantsByPacket = new Map<string, Set<string>>();
const RESOLVE_UNIQUE_INFLIGHT_MAX = Math.min(
  256,
  Math.max(1, Number(process.env['PATH_RESOLVE_UNIQUE_INFLIGHT_MAX'] ?? 34) || 34),
);
const OBSERVER_VARIANTS_PER_PACKET_MAX = Math.min(
  16,
  Math.max(1, Number(process.env['PATH_RESOLVE_OBSERVER_VARIANTS_PER_PACKET_MAX'] ?? 4) || 4),
);

function reserveResolve(packetHash: string, network: string, observer?: string | null): () => void {
  if (resolveInflightSingle.size + resolveInflightMulti.size >= RESOLVE_UNIQUE_INFLIGHT_MAX) {
    throw new Error('PATH_RESOLVE_OVERLOADED');
  }
  if (!observer) return () => undefined;
  const packetKey = `${packetHash}|${network}`;
  const variants = observerVariantsByPacket.get(packetKey) ?? new Set<string>();
  if (!variants.has(observer) && variants.size >= OBSERVER_VARIANTS_PER_PACKET_MAX) {
    throw new Error('PATH_RESOLVE_OVERLOADED');
  }
  variants.add(observer);
  observerVariantsByPacket.set(packetKey, variants);
  return () => {
    variants.delete(observer);
    if (variants.size === 0) observerVariantsByPacket.delete(packetKey);
  };
}

type ResolvePayload = {
  mode?: 'resolved' | 'fallback' | 'none';
  confidence?: number | null;
  threshold?: number;
  permutationCount?: number;
  remainingHops?: number | null;
  debug?: { hopsRequested?: number; hopsUsed?: number };
  results?: ResolvePayload[];
} & Record<string, unknown>;

export function addPathExplanation(value: unknown): unknown {
  if (!value || typeof value !== 'object' || Array.isArray(value)) return value;
  const payload = value as ResolvePayload;
  if (Array.isArray(payload.results)) {
    const results = payload.results.map((result) => addPathExplanation(result) as ResolvePayload);
    const resolved = results.filter((result) => result.mode === 'resolved').length;
    return {
      ...payload,
      results,
      explanation: {
        evidenceLevel: resolved > 0 ? 'mixed' : 'low',
        summary: `${resolved} of ${results.length} observer views produced a resolved candidate path.`,
        reasons: ['Observer views are solved independently before their overlays are combined.'],
        alternativesConsidered: results.reduce((sum, result) => sum + Number(result.permutationCount ?? 0), 0),
      },
    };
  }
  if (!payload.mode) return value;
  const confidence = payload.confidence ?? null;
  const threshold = Number(payload.threshold ?? 0.45);
  const requested = Number(payload.debug?.hopsRequested ?? 0);
  const used = Number(payload.debug?.hopsUsed ?? 0);
  const reasons: string[] = [];
  if (payload.mode === 'resolved') {
    reasons.push('Hash-prefix candidates were ranked using observed topology, terrain viability, direction, and learned path priors.');
    if (confidence != null && confidence >= threshold) reasons.push('The calibrated confidence met the displayed-path threshold.');
    if ((payload.remainingHops ?? 0) > 0) reasons.push(`${payload.remainingHops} hop(s) remain below the confidence threshold.`);
  } else if (payload.mode === 'fallback') {
    reasons.push('No complete high-confidence candidate chain was found; the red path shows bounded fallback possibilities.');
  } else {
    reasons.push('Available packet, node, and link evidence was insufficient to draw a candidate path.');
  }
  if (requested > used) reasons.push(`${requested - used} requested hop hash(es) could not be used safely.`);
  return {
    ...payload,
    explanation: {
      evidenceLevel: confidence != null && confidence >= threshold ? 'high' : payload.mode === 'resolved' ? 'medium' : 'low',
      summary: payload.mode === 'resolved'
        ? `Resolved ${used} of ${requested} requested hops at ${Math.round((confidence ?? 0) * 100)}% calibrated confidence.`
        : payload.mode === 'fallback' ? 'Showing fallback path evidence.' : 'No defensible path was resolved.',
      reasons,
      alternativesConsidered: Number(payload.permutationCount ?? 0),
      limitations: ['Predictions are inferred from recent network evidence and are not proof that every relay handled this packet.'],
    },
  };
}

export function createPathingService(deps: PathingServiceDeps) {
  const {
    pathHistoryCache,
    pathHistoryCacheTtlMs,
    getResolveCache,
    setResolveCache,
    getHeldPath,
    setHeldPath,
    resolvePool,
    repository,
  } = deps;

  async function resolvePacket(packetHash: string, network: string, observer?: string | null): Promise<PublicBetaResultDto> {
    const visibilityGeneration = await repository.fetchVisibilityGeneration();
    const cacheKey = `r|${packetHash}|${network}|${observer ?? ''}|v${visibilityGeneration}`;
    const cached = getResolveCache(cacheKey);
    if (cached) return toPublicBetaResultDto(cached);
    const inflight = resolveInflightSingle.get(cacheKey);
    if (inflight) return inflight;
    const release = reserveResolve(packetHash, network, observer);

    const promise = (async () => {
      const resolved = await resolvePool.run<unknown>({
        type: 'resolve',
        packetHash,
        network,
        observer,
        heldPath: getHeldPath(packetHash, network),
      });
      if (!resolved) {
        throw new Error('PACKET_NOT_FOUND');
      }

      const projected = toPublicBetaResultDto(addPathExplanation(resolved));
      const heldNodes = projected.canonicalPath.map((hop) => hop.nodeId ?? '').filter(Boolean);
      if (heldNodes.length === projected.canonicalPath.length && heldNodes.length > 0) {
        setHeldPath(packetHash, network, { path: heldNodes, resolvedAt: Date.now(), physical: true });
      }
      setResolveCache(cacheKey, projected);
      return projected;
    })();
    resolveInflightSingle.set(cacheKey, promise);
    try {
      return await promise;
    } finally {
      release();
      if (resolveInflightSingle.get(cacheKey) === promise) {
        resolveInflightSingle.delete(cacheKey);
      }
    }
  }

  async function resolvePacketMulti(packetHash: string, network: string): Promise<PublicMultiObserverDto> {
    const visibilityGeneration = await repository.fetchVisibilityGeneration();
    const cacheKey = `m|${packetHash}|${network}|v${visibilityGeneration}`;
    const cached = getResolveCache(cacheKey);
    if (cached) return toPublicMultiObserverDto(cached);
    const inflight = resolveInflightMulti.get(cacheKey);
    if (inflight) return inflight;
    const release = reserveResolve(packetHash, network);

    const promise = (async () => {
      const resolved = await resolvePool.run<unknown>({
        type: 'resolveMulti',
        packetHash,
        network,
        heldPath: getHeldPath(packetHash, network),
      });
      if (!resolved) {
        throw new Error('PACKET_NOT_FOUND');
      }

      const projected = toPublicMultiObserverDto(addPathExplanation(resolved));
      const heldNodes = projected.canonicalPath.map((hop) => hop.nodeId ?? '').filter(Boolean);
      if (heldNodes.length === projected.canonicalPath.length && heldNodes.length > 0) {
        setHeldPath(packetHash, network, { path: heldNodes, resolvedAt: Date.now(), physical: true });
      }
      setResolveCache(cacheKey, projected);
      return projected;
    })();
    resolveInflightMulti.set(cacheKey, promise);
    try {
      return await promise;
    } finally {
      release();
      if (resolveInflightMulti.get(cacheKey) === promise) {
        resolveInflightMulti.delete(cacheKey);
      }
    }
  }

  async function getPathHistory(scope: string): Promise<unknown> {
    const visibilityGeneration = await repository.fetchVisibilityGeneration();
    const cacheKey = `${scope}|v${visibilityGeneration}`;
    const memoryCached = pathHistoryCache.get(cacheKey);
    if (memoryCached && Date.now() - memoryCached.ts < pathHistoryCacheTtlMs) {
      return memoryCached.data;
    }

    const cached = await repository.fetchPathHistory(scope, visibilityGeneration);
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

    pathHistoryCache.set(cacheKey, { ts: Date.now(), data: responseData });
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
