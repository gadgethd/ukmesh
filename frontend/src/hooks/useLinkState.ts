import { useSyncExternalStore } from 'react';
import { linkKey, type LinkMetrics } from '../utils/pathing.js';

type LinkUpdate = {
  node_a_id: string;
  node_b_id: string;
  observed_count: number;
  multibyte_observed_count?: number;
  itm_viable: boolean | null;
  itm_path_loss_db?: number | null;
  count_a_to_b?: number;
  count_b_to_a?: number;
};

export type ViableLinkSnapshot = {
  node_a_id: string;
  node_b_id: string;
  observed_count: number;
  multibyte_observed_count?: number;
  itm_viable: boolean | null;
  itm_path_loss_db?: number | null;
  count_a_to_b?: number;
  count_b_to_a?: number;
};

type LinkState = {
  linkPairs: Set<string>;
  linkMetrics: Map<string, LinkMetrics>;
  viablePairsArr: [string, string][];
};

let state: LinkState = {
  linkPairs: new Set(),
  linkMetrics: new Map(),
  viablePairsArr: [],
};

const listeners = new Set<() => void>();

function emit(): void {
  for (const listener of listeners) listener();
}

function subscribe(listener: () => void): () => void {
  listeners.add(listener);
  return () => listeners.delete(listener);
}

function setState(next: LinkState): void {
  state = next;
  emit();
}

function getState(): LinkState {
  return state;
}

function applyInitialViablePairs(viablePairs?: [string, string][]): void {
  if (!viablePairs) return;

  const linkPairs = new Set(viablePairs.map(([a, b]) => linkKey(a, b)));
  const linkMetrics = new Map<string, LinkMetrics>();
  for (const [a, b] of viablePairs) {
    linkMetrics.set(linkKey(a, b), {
      observed_count: 0,
      multibyte_observed_count: 0,
      itm_viable: true,
    });
  }

  setState({
    linkPairs,
    linkMetrics,
    viablePairsArr: viablePairs,
  });
}

function applyInitialViableLinks(viableLinks?: ViableLinkSnapshot[]): void {
  if (!viableLinks || viableLinks.length === 0) return;

  const viablePairsArr = viableLinks.map((link) => [link.node_a_id, link.node_b_id] as [string, string]);
  const linkPairs = new Set(viablePairsArr.map(([a, b]) => linkKey(a, b)));
  const linkMetrics = new Map<string, LinkMetrics>();
  for (const link of viableLinks) {
    linkMetrics.set(linkKey(link.node_a_id, link.node_b_id), {
      observed_count: link.observed_count,
      multibyte_observed_count: link.multibyte_observed_count ?? 0,
      itm_viable: link.itm_viable,
      itm_path_loss_db: link.itm_path_loss_db ?? null,
      count_a_to_b: link.count_a_to_b,
      count_b_to_a: link.count_b_to_a,
    });
  }

  setState({
    linkPairs,
    linkMetrics,
    viablePairsArr,
  });
}

function applyLinkUpdate(update: LinkUpdate): void {
  applyLinkUpdateBatch([update]);
}

function applyLinkUpdateBatch(updates: LinkUpdate[]): void {
  if (updates.length === 0) return;

  const nextLinkMetrics = new Map(state.linkMetrics);
  const nextLinkPairs = new Set(state.linkPairs);
  const viablePairs = [...state.viablePairsArr];
  const viablePairKeys = new Set(viablePairs.map(([a, b]) => linkKey(a, b)));

  for (const update of updates) {
    const key = linkKey(update.node_a_id, update.node_b_id);
    const existing = nextLinkMetrics.get(key);
    nextLinkMetrics.set(key, {
      observed_count: Math.max(existing?.observed_count ?? 0, update.observed_count ?? 0),
      multibyte_observed_count: Math.max(existing?.multibyte_observed_count ?? 0, update.multibyte_observed_count ?? 0),
      itm_viable: update.itm_viable ?? existing?.itm_viable ?? null,
      itm_path_loss_db: update.itm_path_loss_db ?? existing?.itm_path_loss_db ?? null,
      count_a_to_b: update.count_a_to_b ?? existing?.count_a_to_b,
      count_b_to_a: update.count_b_to_a ?? existing?.count_b_to_a,
    });

    if (update.itm_viable) {
      nextLinkPairs.add(key);
      if (!viablePairKeys.has(key)) {
        viablePairKeys.add(key);
        viablePairs.push([update.node_a_id, update.node_b_id]);
      }
    }
  }

  setState({
    linkPairs: nextLinkPairs,
    linkMetrics: nextLinkMetrics,
    viablePairsArr: viablePairs,
  });
}

export const linkStateStore = {
  subscribe,
  getState,
  applyInitialViablePairs,
  applyInitialViableLinks,
  applyLinkUpdate,
  applyLinkUpdateBatch,
};

export function useLinkPairs(): Set<string> {
  return useSyncExternalStore(subscribe, () => state.linkPairs);
}

export function useLinkMetrics(): Map<string, LinkMetrics> {
  return useSyncExternalStore(subscribe, () => state.linkMetrics);
}

export function useViablePairsArr(): [string, string][] {
  return useSyncExternalStore(subscribe, () => state.viablePairsArr);
}
