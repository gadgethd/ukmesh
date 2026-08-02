import { linkKey, type LinkMetrics } from '../utils/pathing.js';
import { canonicalNodeId } from '../utils/nodeIds.js';

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
  scopeKey: string | null;
  epoch: number;
  linkPairs: Set<string>;
  linkMetrics: Map<string, LinkMetrics>;
  viablePairsArr: [string, string][];
};

let state: LinkState = {
  scopeKey: null,
  epoch: 0,
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

function acceptsEpoch(epoch: number | undefined): boolean {
  return epoch === undefined || epoch === state.epoch;
}

function canonicalPair(a: string, b: string): [string, string] | null {
  const nodeA = canonicalNodeId(a);
  const nodeB = canonicalNodeId(b);
  if (!nodeA || !nodeB || nodeA === nodeB) return null;
  return nodeA < nodeB ? [nodeA, nodeB] : [nodeB, nodeA];
}

function reset(scopeKey: string): number {
  const epoch = state.epoch + 1;
  setState({
    scopeKey,
    epoch,
    linkPairs: new Set(),
    linkMetrics: new Map(),
    viablePairsArr: [],
  });
  return epoch;
}

function applyInitialViablePairs(viablePairs: [string, string][] = [], epoch?: number): void {
  if (!acceptsEpoch(epoch)) return;
  const normalizedPairs = viablePairs
    .map(([a, b]) => canonicalPair(a, b))
    .filter((pair): pair is [string, string] => pair !== null);

  const linkPairs = new Set(normalizedPairs.map(([a, b]) => linkKey(a, b)));
  const linkMetrics = new Map<string, LinkMetrics>();
  for (const [a, b] of normalizedPairs) {
    linkMetrics.set(linkKey(a, b), {
      observed_count: 0,
      multibyte_observed_count: 0,
      itm_viable: true,
    });
  }

  setState({
    linkPairs,
    linkMetrics,
    viablePairsArr: normalizedPairs,
    scopeKey: state.scopeKey,
    epoch: state.epoch,
  });
}

function applyInitialViableLinks(viableLinks: ViableLinkSnapshot[] = [], epoch?: number): void {
  if (!acceptsEpoch(epoch)) return;
  const normalized = viableLinks
    .map((link) => {
      const pair = canonicalPair(link.node_a_id, link.node_b_id);
      return pair ? { link, pair } : null;
    })
    .filter((entry): entry is { link: ViableLinkSnapshot; pair: [string, string] } => entry !== null);
  const viablePairsArr = normalized
    .filter(({ link }) => link.itm_viable === true)
    .map(({ pair }) => pair);
  const linkPairs = new Set(viablePairsArr.map(([a, b]) => linkKey(a, b)));
  const linkMetrics = new Map<string, LinkMetrics>();
  for (const { link, pair } of normalized) {
    linkMetrics.set(linkKey(pair[0], pair[1]), {
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
    scopeKey: state.scopeKey,
    epoch: state.epoch,
  });
}

function applyLinkUpdate(update: LinkUpdate, epoch?: number): void {
  applyLinkUpdateBatch([update], epoch);
}

function applyLinkUpdateBatch(updates: LinkUpdate[], epoch?: number): void {
  if (!acceptsEpoch(epoch) || updates.length === 0) return;

  const nextLinkMetrics = new Map(state.linkMetrics);
  const nextLinkPairs = new Set(state.linkPairs);
  const viablePairs = [...state.viablePairsArr];
  const viablePairKeys = new Set(viablePairs.map(([a, b]) => linkKey(a, b)));

  for (const update of updates) {
    const pair = canonicalPair(update.node_a_id, update.node_b_id);
    if (!pair) continue;
    const key = linkKey(pair[0], pair[1]);
    const existing = nextLinkMetrics.get(key);
    const metrics: LinkMetrics = {
      observed_count: Math.max(existing?.observed_count ?? 0, update.observed_count ?? 0),
      multibyte_observed_count: Math.max(existing?.multibyte_observed_count ?? 0, update.multibyte_observed_count ?? 0),
      itm_viable: update.itm_viable ?? existing?.itm_viable ?? null,
      itm_path_loss_db: update.itm_path_loss_db ?? existing?.itm_path_loss_db ?? null,
      count_a_to_b: update.count_a_to_b ?? existing?.count_a_to_b,
      count_b_to_a: update.count_b_to_a ?? existing?.count_b_to_a,
    };

    if (update.itm_viable === false) {
      nextLinkMetrics.delete(key);
      nextLinkPairs.delete(key);
      viablePairKeys.delete(key);
      const index = viablePairs.findIndex(([a, b]) => linkKey(a, b) === key);
      if (index >= 0) viablePairs.splice(index, 1);
    } else {
      // null/unknown updates enrich metrics but deliberately preserve current
      // membership. A link is added only by an explicit true transition.
      nextLinkMetrics.set(key, metrics);
    }

    if (update.itm_viable === true) {
      nextLinkPairs.add(key);
      if (!viablePairKeys.has(key)) {
        viablePairKeys.add(key);
        viablePairs.push(pair);
      }
    }
  }

  setState({
    scopeKey: state.scopeKey,
    epoch: state.epoch,
    linkPairs: nextLinkPairs,
    linkMetrics: nextLinkMetrics,
    viablePairsArr: viablePairs,
  });
}

export const linkStateStore = {
  subscribe,
  getState,
  reset,
  applyInitialViablePairs,
  applyInitialViableLinks,
  applyLinkUpdate,
  applyLinkUpdateBatch,
};
