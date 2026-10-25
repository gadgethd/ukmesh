/**
 * Membership floor for the region-scope participation tables.
 *
 * A node only counts as a member of a region when it relayed at least
 * MEMBER_MIN_SHARE_PCT percent of that region's scoped packets. Nodes that
 * merely pick up the odd scoped packet (a handful out of thousands) are not
 * part of a region's relay pattern: including them diluted member lists and
 * stretched the map hulls. Calibrated against the 2026-10 live corpus: the
 * 10% floor removed ~36% of member rows and cleaned 7 of 24 hulls without
 * dropping any region's shape.
 */
export const MEMBER_MIN_SHARE_PCT = 10;

/**
 * True when `memberPackets` is at least MEMBER_MIN_SHARE_PCT% of
 * `regionPacketsTotal`. Integer-safe (no float division) and defensive about
 * degenerate inputs.
 */
export function meetsMemberShare(memberPackets: number, regionPacketsTotal: number): boolean {
  if (!Number.isFinite(memberPackets) || !Number.isFinite(regionPacketsTotal)) return false;
  if (regionPacketsTotal <= 0) return false;
  return memberPackets * 100 >= regionPacketsTotal * MEMBER_MIN_SHARE_PCT;
}
