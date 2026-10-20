/** Prefix offsets include each row's border box and a final total-height entry. */
export function rowOffsets(
  keys: readonly string[],
  heights: ReadonlyMap<string, number>,
  estimate = 76,
): number[] {
  const fallback = Number.isFinite(estimate) && estimate > 0 ? estimate : 76;
  const offsets = [0];
  for (const key of keys) {
    const measured = heights.get(key);
    const height = measured !== undefined && Number.isFinite(measured) && measured > 0
      ? measured
      : fallback;
    offsets.push(offsets[offsets.length - 1]! + height);
  }
  return offsets;
}

/** Return the row whose border box contains this content-space offset. */
export function rowAtOffset(offsets: readonly number[], position: number): number {
  const count = offsets.length - 1;
  if (count <= 0) return 0;
  let low = 0;
  let high = count;
  const target = Math.max(0, position);
  while (low < high) {
    const middle = Math.floor((low + high) / 2);
    if (offsets[middle + 1]! <= target) low = middle + 1;
    else high = middle;
  }
  return Math.min(low, count - 1);
}

export function measuredRowRange(
  offsets: readonly number[],
  scrollTop: number,
  height: number,
  overscan = 5,
) {
  const count = offsets.length - 1;
  if (count <= 0) return { start: 0, end: 0, top: 0, bottom: 0 };
  const extra = Math.max(0, Math.trunc(overscan));
  const start = Math.max(0, rowAtOffset(offsets, scrollTop) - extra);
  const end = Math.min(count, rowAtOffset(offsets, scrollTop + Math.max(0, height)) + 1 + extra);
  return { start, end, top: offsets[start]!, bottom: offsets[count]! - offsets[end]! };
}
