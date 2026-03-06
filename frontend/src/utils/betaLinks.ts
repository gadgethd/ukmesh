import { MIN_LINK_OBSERVATIONS, type LinkMetrics } from './pathing.js';

function clamp(value: number, min: number, max: number): number {
  return Math.max(min, Math.min(max, value));
}

export function directionalSupport(meta: LinkMetrics | undefined, fromId: string, toId: string): number {
  if (!meta || meta.count_a_to_b == null || meta.count_b_to_a == null) return 0.5;
  const a = fromId < toId ? fromId : toId;
  const forward = fromId === a ? meta.count_a_to_b : meta.count_b_to_a;
  const reverse = fromId === a ? meta.count_b_to_a : meta.count_a_to_b;
  const total = forward + reverse;
  if (total <= 0) return 0.5;
  return forward / total;
}

export function minimumDirectionalSupport(observed: number): number {
  return observed >= 50 ? 0.12 : 0.02;
}

export function confirmedLinkConfidence(
  meta: LinkMetrics | undefined,
  fromId: string,
  toId: string,
  boosts: {
    prefix: number;
    transition: number;
    motif: number;
    edge: number;
    ambiguity: number;
  },
): number {
  const observed = meta?.observed_count ?? MIN_LINK_OBSERVATIONS;
  const obsBoost = Math.min(0.18, Math.log10(1 + observed) * 0.12);
  const pathLoss = meta?.itm_path_loss_db;
  const plPenalty = pathLoss == null ? 0 : Math.min(0.12, Math.max(0, (pathLoss - 130) / 120));
  const dirBoost = (directionalSupport(meta, fromId, toId) - 0.5) * 0.12;
  const viableBoost = meta?.itm_viable === false ? -0.1 : 0.05;
  const conf = 0.66
    + obsBoost
    + dirBoost
    + viableBoost
    - plPenalty
    + boosts.prefix
    + boosts.transition
    + boosts.motif
    + boosts.edge
    + boosts.ambiguity;
  return clamp(conf, 0.45, 0.98);
}
