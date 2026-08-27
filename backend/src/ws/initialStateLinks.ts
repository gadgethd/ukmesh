type RecentViableLink = {
  last_observed: string;
  observed_count: number;
  multibyte_observed_count: number;
};

/** Select a stable, most-recent viable-link snapshot without mutating the
 * shared full cache used by other callers. Live link_update frames continue
 * filling and refreshing the client-side map after this bounded seed. */
export function selectInitialViableLinks<T extends RecentViableLink>(
  links: readonly T[],
  limit: number,
): T[] {
  if (!Number.isSafeInteger(limit) || limit < 1) throw new Error('INVALID_VIABLE_LINK_LIMIT');
  return links
    .map((link, index) => ({ link, index, observedAt: Date.parse(link.last_observed) }))
    .sort((left, right) => (
      (Number.isFinite(right.observedAt) ? right.observedAt : Number.NEGATIVE_INFINITY)
        - (Number.isFinite(left.observedAt) ? left.observedAt : Number.NEGATIVE_INFINITY)
      || right.link.multibyte_observed_count - left.link.multibyte_observed_count
      || right.link.observed_count - left.link.observed_count
      || left.index - right.index
    ))
    .slice(0, limit)
    .map(({ link }) => link);
}
