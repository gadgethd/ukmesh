/** Prefix offsets include each row's border box and a final total-height entry. */
export function rowOffsets(keys: readonly string[], heights: ReadonlyMap<string, number>, estimate = 76): number[] {
  const offsets = [0];
  for (const key of keys) offsets.push(offsets[offsets.length - 1]! + (heights.get(key) ?? estimate));
  return offsets;
}

export function rowAtOffset(offsets: readonly number[], position: number): number {
  const count = offsets.length - 1;
  if (count <= 0) return 0;
  let low = 0;
  let high = count;
  while (low < high) {
    const middle = Math.floor((low + high) / 2);
    if (offsets[middle + 1]! <= position) low = middle + 1;
    else high = middle;
  }
  return Math.min(low, count - 1);
}

export function measuredRowRange(offsets: readonly number[], scrollTop: number, height: number, overscan = 5) {
  const count = offsets.length - 1;
  const start = Math.max(0, rowAtOffset(offsets, scrollTop) - overscan);
  const end = Math.min(count, rowAtOffset(offsets, scrollTop + height) + 1 + overscan);
  return { start, end, top: offsets[start]!, bottom: offsets[count]! - offsets[end]! };
}
