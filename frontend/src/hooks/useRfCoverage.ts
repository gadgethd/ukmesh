import { useEffect, useMemo, useState } from 'react';

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

export type RfCoverageMeta = {
  generated_at: string;
  source: string;
  version: string;
  complete: boolean;
  coverage?: Partial<Record<RfCoverageTierName, RfCoverageTier>>;
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

const POLL_INTERVAL_MS = 3_000;

function isFiniteNumber(value: unknown): value is number {
  return typeof value === 'number' && Number.isFinite(value);
}

export function isValidRfCoverageTile(value: unknown): value is RfCoverageTile {
  if (!value || typeof value !== 'object') return false;
  const tile = value as Partial<RfCoverageTile>;
  const bounds = tile.bounds as Partial<RfCoverageBounds> | undefined;
  return typeof tile.image === 'string'
    && tile.image.length > 0
    && !tile.image.startsWith('/')
    && !tile.image.includes('..')
    && !!bounds
    && isFiniteNumber(bounds.South)
    && isFiniteNumber(bounds.North)
    && isFiniteNumber(bounds.West)
    && isFiniteNumber(bounds.East)
    && bounds.South < bounds.North
    && bounds.West < bounds.East;
}

export function availableRfCoverageTiers(meta: RfCoverageMeta | null): RfCoverageTierName[] {
  if (!meta?.coverage) return [];
  return (['standard', 'precision'] as const).filter((tier) => {
    const product = meta.coverage?.[tier];
    return !!product && Array.isArray(product.tiles) && product.tiles.some(isValidRfCoverageTile);
  });
}

async function fetchJson<T>(path: string, signal: AbortSignal): Promise<T> {
  const response = await fetch(path, {
    cache: 'no-store',
    credentials: 'same-origin',
    headers: { Accept: 'application/json' },
    signal,
  });
  if (!response.ok) throw new Error(`${path} returned ${response.status}`);
  return response.json() as Promise<T>;
}

export function useRfCoverage(enabled: boolean): {
  meta: RfCoverageMeta | null;
  progress: RfCoverageProgress | null;
  availableTiers: RfCoverageTierName[];
} {
  const [meta, setMeta] = useState<RfCoverageMeta | null>(null);
  const [progress, setProgress] = useState<RfCoverageProgress | null>(null);

  useEffect(() => {
    if (!enabled) {
      setMeta(null);
      setProgress(null);
      return undefined;
    }

    let stopped = false;
    let timer: number | null = null;
    let controller: AbortController | null = null;

    const poll = async () => {
      controller?.abort();
      controller = new AbortController();
      const signal = controller.signal;
      const stamp = Date.now();
      const [nextMeta, nextProgress] = await Promise.allSettled([
        fetchJson<RfCoverageMeta>(`/rf-coverage/meta.json?poll=${stamp}`, signal),
        fetchJson<RfCoverageProgress>(`/rf-coverage/progress.json?poll=${stamp}`, signal),
      ]);
      if (stopped || signal.aborted) return;
      if (nextMeta.status === 'fulfilled' && typeof nextMeta.value?.generated_at === 'string') {
        setMeta(nextMeta.value);
      }
      if (nextProgress.status === 'fulfilled' && typeof nextProgress.value?.stage === 'string') {
        setProgress(nextProgress.value);
      }
      timer = window.setTimeout(() => { void poll(); }, POLL_INTERVAL_MS);
    };

    void poll();
    return () => {
      stopped = true;
      controller?.abort();
      if (timer !== null) window.clearTimeout(timer);
    };
  }, [enabled]);

  const availableTiers = useMemo(() => availableRfCoverageTiers(meta), [meta]);
  return { meta, progress, availableTiers };
}
