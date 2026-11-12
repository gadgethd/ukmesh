import { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import { useVisibilityPoll } from './useVisibilityPoll.js';
import { POLLING } from '../config/polling.js';
import { fetchJson } from '../utils/api.js';

export type RfCoverageTierName = 'standard' | 'precision';

export type RfCoverageBounds = {
  South: number;
  North: number;
  West: number;
  East: number;
};

export type RfCoverageTile = {
  image: string;
  bounds: RfCoverageBounds;
};

export type RfCoverageAssumptions = {
  tx_power_dbm: number;
  tx_antenna_gain_dbi: number;
  rx_antenna_gain_dbi: number;
  rx_sensitivity_dbm: number;
  fade_margin_db: number;
  antenna_height_m: number;
  rx_height_m: number;
  note: string;
};

export type RfCoverageTier = {
  tiles: RfCoverageTile[];
  frequency_mhz: number;
  max_search_range_km: number;
  dem_zoom_level: number;
  generated_at?: string;
  assumptions: RfCoverageAssumptions;
};

export type RfNodeCoverageState = 'pending' | 'available' | 'stale' | 'error';

export type RfNodeCoverage = {
  dataset_id: string;
  run_id: string;
  state: 'computing' | 'available' | 'failed';
  position_status: 'active' | 'degraded' | 'silent';
  last_heard?: string;
  lat: number;
  lon: number;
  requested_at: string;
  updated_at: string;
  fresh_until?: string;
  completed_tiles?: number;
  total_tiles?: number;
  standard?: RfCoverageTier;
  failure?: string;
};

export type RfCoverageMeta = {
  generated_at: string;
  source: string;
  version: string;
  complete: boolean;
  coverage?: Partial<Record<RfCoverageTierName, RfCoverageTier>>;
  node_coverage?: Record<string, RfNodeCoverage>;
  run?: {
    id: string;
    started_at: string;
    model: string;
    source_version: string;
    completed_tiles: number;
    total_tiles: number;
    tiers: Partial<Record<RfCoverageTierName, {
      state: 'pending' | 'computing' | 'available' | 'failed';
      completed_tiles: number;
      total_tiles: number;
      failure?: string;
    }>>;
    failure?: string;
  };
};

export type RfCoverageProgress = {
  run_id?: string;
  stage: string;
  done: number;
  total: number;
  percent: number;
  message: string;
  updated_at: string;
  eta_seconds?: number;
  backend?: 'cpu' | 'gpu' | 'remote_gpu' | string;
};

function isFiniteNumber(value: unknown): value is number {
  return typeof value === 'number' && Number.isFinite(value);
}

export function isValidRfCoverageTile(value: unknown): value is RfCoverageTile {
  if (!value || typeof value !== 'object') return false;
  const tile = value as Partial<RfCoverageTile>;
  const bounds = tile.bounds as Partial<RfCoverageBounds> | undefined;
  const validPath = typeof tile.image === 'string'
    && /^(?:tiles\/(?:standard|precision)\/[0-9]+-[0-9]+\.png|tiles\/nodes\/n[0-9a-f]{24}\/[0-9]+-[0-9]+\.png)$/.test(tile.image);
  return validPath
    && !!bounds
    && isFiniteNumber(bounds.South)
    && isFiniteNumber(bounds.North)
    && isFiniteNumber(bounds.West)
    && isFiniteNumber(bounds.East)
    && bounds.South < bounds.North
    && bounds.West < bounds.East;
}

export function rfNodeCoverageState(
  meta: RfCoverageMeta | null,
  publicKey: string | null | undefined,
  now = Date.now(),
): RfNodeCoverageState {
  if (!publicKey || !/^[0-9a-f]{64}$/i.test(publicKey)) return 'pending';
  const entry = meta?.node_coverage?.[publicKey.toLowerCase()];
  if (!entry || entry.state === 'computing') return 'pending';
  if (entry.state === 'failed') return 'error';
  const hasTiles = entry.standard?.tiles?.some(isValidRfCoverageTile) ?? false;
  if (!hasTiles) return 'pending';
  const freshUntil = entry.fresh_until ? Date.parse(entry.fresh_until) : Number.NaN;
  if (entry.position_status !== 'active' || !Number.isFinite(freshUntil) || freshUntil <= now) return 'stale';
  return 'available';
}

export function availableRfCoverageTiers(meta: RfCoverageMeta | null): RfCoverageTierName[] {
  if (!meta?.coverage) return [];
  return (['standard', 'precision'] as const).filter((tier) => {
    const product = meta.coverage?.[tier];
    return !!product && Array.isArray(product.tiles) && product.tiles.some(isValidRfCoverageTile);
  });
}

function fetchRfJson<T>(path: string, signal: AbortSignal): Promise<T> {
  return fetchJson<T>(path, { cache: 'no-store', credentials: 'same-origin', signal }, {
    timeoutMs: POLLING.ordinaryTimeout,
    maxBytes: 4 * 1024 * 1024,
  });
}

/**
 * Ask the calculator for one repeater's RF footprint. The request is
 * asynchronous: the server enqueues it, writes a `computing` entry into
 * meta.json, and the existing poll in useRfCoverage picks up the finished
 * tiles. Returns true when the request was accepted or was already queued.
 */
export async function requestRfNodeCoverage(publicKey: string): Promise<boolean> {
  if (!/^[0-9a-f]{64}$/i.test(publicKey)) return false;
  const response = await fetch(`/rf-coverage/nodes/${publicKey.toLowerCase()}/request`, {
    method: 'POST',
    credentials: 'same-origin',
    headers: { Accept: 'application/json' },
    signal: AbortSignal.timeout(POLLING.ordinaryTimeout),
  });
  await response.body?.cancel();
  return response.ok || response.status === 409;
}

export type RfPollingOptions = {
  layerVisible: boolean;
  nodeKey?: string | null;
  nodeRequestVisible?: boolean;
  discoverOnMount?: boolean;
};

export function rfCoveragePollingPolicy(enabled: boolean, options: RfPollingOptions,
  meta: RfCoverageMeta | null, discovered: boolean): { enabled: boolean; intervalMs: number } {
  const nodeState = options.nodeKey ? rfNodeCoverageState(meta, options.nodeKey) : null;
  const waitingForNode = (options.layerVisible || !!options.nodeRequestVisible) && !!options.nodeKey
    && nodeState !== 'available' && nodeState !== 'error';
  return {
    enabled: enabled && (options.layerVisible || waitingForNode || (!!options.discoverOnMount && !discovered)),
    intervalMs: waitingForNode || !meta?.complete ? POLLING.rfActive : POLLING.rfIdle,
  };
}

export function useRfCoverage(enabled: boolean, options: RfPollingOptions = { layerVisible: enabled }): {
  meta: RfCoverageMeta | null;
  progress: RfCoverageProgress | null;
  availableTiers: RfCoverageTierName[];
} {
  const [meta, setMeta] = useState<RfCoverageMeta | null>(null);
  const [progress, setProgress] = useState<RfCoverageProgress | null>(null);
  const [discovered, setDiscovered] = useState(false);
  const policy = rfCoveragePollingPolicy(enabled, options, meta, discovered);
  const intervalRef = useRef(policy.intervalMs);
  intervalRef.current = policy.intervalMs;
  const interval = useCallback(() => intervalRef.current, []);

  useEffect(() => {
    if (enabled) return;
    setMeta(null); setProgress(null); setDiscovered(false);
  }, [enabled]);

  useVisibilityPoll(async signal => {
    const stamp = Date.now();
    const [nextMeta, nextProgress] = await Promise.allSettled([
      fetchRfJson<RfCoverageMeta>(`/rf-coverage/meta.json?poll=${stamp}`, signal),
      fetchRfJson<RfCoverageProgress>(`/rf-coverage/progress.json?poll=${stamp}`, signal),
    ]);
    if (signal.aborted) return;
    // The scheduling policy reads this ref before React's next render too.
    if (nextMeta.status === 'fulfilled' && typeof nextMeta.value?.generated_at === 'string') {
      setMeta(nextMeta.value);
      intervalRef.current = rfCoveragePollingPolicy(enabled, options, nextMeta.value, true).intervalMs;
    }
    if (nextProgress.status === 'fulfilled' && typeof nextProgress.value?.stage === 'string') setProgress(nextProgress.value);
    setDiscovered(true);
    if (nextMeta.status === 'rejected') throw nextMeta.reason;
    if (nextProgress.status === 'rejected') throw nextProgress.reason;
  }, {
    enabled: policy.enabled,
    scopeKey: `rf:${options.layerVisible}:${options.nodeKey ?? 'network'}:${!!options.nodeRequestVisible}`,
    intervalMs: interval,
    timeoutMs: POLLING.ordinaryTimeout,
    maxBackoffMs: POLLING.rfIdle * 4,
  });

  const availableTiers = useMemo(() => availableRfCoverageTiers(meta), [meta]);
  return { meta, progress, availableTiers };
}
