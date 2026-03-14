import { useCallback, useState } from 'react';
import { MIN_LINK_OBSERVATIONS, linkKey, type LinkMetrics } from '../utils/pathing.js';

type LinkUpdate = {
  node_a_id: string;
  node_b_id: string;
  observed_count: number;
  itm_viable: boolean | null;
  itm_path_loss_db?: number | null;
  count_a_to_b?: number;
  count_b_to_a?: number;
};

export type ViableLinkSnapshot = {
  node_a_id: string;
  node_b_id: string;
  observed_count: number;
  itm_viable: boolean | null;
  itm_path_loss_db?: number | null;
  count_a_to_b?: number;
  count_b_to_a?: number;
};

export function useLinkState() {
  const [linkPairs, setLinkPairs] = useState<Set<string>>(new Set());
  const [linkMetrics, setLinkMetrics] = useState<Map<string, LinkMetrics>>(new Map());
  const [viablePairsArr, setViablePairsArr] = useState<[string, string][]>([]);

  const applyInitialViablePairs = useCallback((viablePairs?: [string, string][]) => {
    if (!viablePairs) return;

    setLinkPairs(new Set(viablePairs.map(([a, b]) => linkKey(a, b))));
    setLinkMetrics(() => {
      const metrics = new Map<string, LinkMetrics>();
      for (const [a, b] of viablePairs) {
        metrics.set(linkKey(a, b), {
          observed_count: MIN_LINK_OBSERVATIONS,
          itm_viable: true,
        });
      }
      return metrics;
    });
    setViablePairsArr(viablePairs);
  }, []);

  const applyInitialViableLinks = useCallback((viableLinks?: ViableLinkSnapshot[]) => {
    if (!viableLinks || viableLinks.length === 0) return;

    const pairs = viableLinks.map((l) => [l.node_a_id, l.node_b_id] as [string, string]);
    setLinkPairs(new Set(pairs.map(([a, b]) => linkKey(a, b))));
    setViablePairsArr(pairs);
    setLinkMetrics(() => {
      const metrics = new Map<string, LinkMetrics>();
      for (const link of viableLinks) {
        metrics.set(linkKey(link.node_a_id, link.node_b_id), {
          observed_count: link.observed_count,
          itm_viable: link.itm_viable,
          itm_path_loss_db: link.itm_path_loss_db ?? null,
          count_a_to_b: link.count_a_to_b,
          count_b_to_a: link.count_b_to_a,
        });
      }
      return metrics;
    });
  }, []);

  const applyLinkUpdate = useCallback((update: LinkUpdate) => {
    const key = linkKey(update.node_a_id, update.node_b_id);
    setLinkMetrics((prev) => {
      const next = new Map(prev);
      const existing = next.get(key);
      next.set(key, {
        observed_count: Math.max(existing?.observed_count ?? 0, update.observed_count ?? 0),
        itm_viable: update.itm_viable ?? existing?.itm_viable ?? null,
        itm_path_loss_db: update.itm_path_loss_db ?? existing?.itm_path_loss_db ?? null,
        count_a_to_b: update.count_a_to_b ?? existing?.count_a_to_b,
        count_b_to_a: update.count_b_to_a ?? existing?.count_b_to_a,
      });
      return next;
    });

    if (update.itm_viable && update.observed_count >= MIN_LINK_OBSERVATIONS) {
      setLinkPairs((prev) => {
        if (prev.has(key)) return prev;
        const next = new Set(prev);
        next.add(key);
        return next;
      });
      setViablePairsArr((prev) => {
        if (prev.some(([a, b]) => linkKey(a, b) === key)) return prev;
        return [...prev, [update.node_a_id, update.node_b_id]];
      });
    }
  }, []);

  const applyLinkUpdateBatch = useCallback((updates: LinkUpdate[]) => {
    if (updates.length === 0) return;
    
    setLinkMetrics((prev) => {
      const next = new Map(prev);
      for (const update of updates) {
        const key = linkKey(update.node_a_id, update.node_b_id);
        const existing = next.get(key);
        next.set(key, {
          observed_count: Math.max(existing?.observed_count ?? 0, update.observed_count ?? 0),
          itm_viable: update.itm_viable ?? existing?.itm_viable ?? null,
          itm_path_loss_db: update.itm_path_loss_db ?? existing?.itm_path_loss_db ?? null,
          count_a_to_b: update.count_a_to_b ?? existing?.count_a_to_b,
          count_b_to_a: update.count_b_to_a ?? existing?.count_b_to_a,
        });
      }
      return next;
    });

    const newViable = updates.filter(u => u.itm_viable && u.observed_count >= MIN_LINK_OBSERVATIONS);
    if (newViable.length > 0) {
      setLinkPairs((prev) => {
        const next = new Set(prev);
        for (const update of newViable) {
          next.add(linkKey(update.node_a_id, update.node_b_id));
        }
        return next;
      });
      setViablePairsArr((prev) => {
        let changed = false;
        const added: [string, string][] = [];
        for (const update of newViable) {
          const key = linkKey(update.node_a_id, update.node_b_id);
          if (!prev.some(([a, b]) => linkKey(a, b) === key)) {
            added.push([update.node_a_id, update.node_b_id]);
            changed = true;
          }
        }
        return changed ? [...prev, ...added] : prev;
      });
    }
  }, []);

  return {
    linkPairs,
    linkMetrics,
    viablePairsArr,
    applyInitialViablePairs,
    applyInitialViableLinks,
    applyLinkUpdate,
    applyLinkUpdateBatch,
  };
}
