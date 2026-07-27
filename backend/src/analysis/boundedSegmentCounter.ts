type CounterEntry = {
  key: string;
  count: number;
  error: number;
  heapIndex: number;
};

/**
 * Space-Saving heavy-hitter counter.
 *
 * Memory stays proportional to `capacity`, even for an unbounded number of
 * distinct keys. Returned counts are conservative lower bounds, so an item
 * cannot cross a publication threshold solely due to approximation error.
 */
export class BoundedSegmentCounter {
  private readonly entries = new Map<string, CounterEntry>();
  private readonly heap: CounterEntry[] = [];
  private replacements = 0;

  constructor(private readonly capacity: number) {
    if (!Number.isSafeInteger(capacity) || capacity < 1) {
      throw new Error('INVALID_SEGMENT_COUNTER_CAPACITY');
    }
  }

  observe(key: string): void {
    const existing = this.entries.get(key);
    if (existing) {
      existing.count += 1;
      this.siftDown(existing.heapIndex);
      return;
    }

    if (this.heap.length < this.capacity) {
      const entry: CounterEntry = {
        key,
        count: 1,
        error: 0,
        heapIndex: this.heap.length,
      };
      this.entries.set(key, entry);
      this.heap.push(entry);
      this.siftUp(entry.heapIndex);
      return;
    }

    const minimum = this.heap[0]!;
    this.entries.delete(minimum.key);
    minimum.key = key;
    minimum.error = minimum.count;
    minimum.count += 1;
    this.entries.set(key, minimum);
    this.replacements += 1;
    this.siftDown(0);
  }

  candidates(minimumCount: number): Array<{ key: string; count: number }> {
    return Array.from(this.entries.values())
      .map((entry) => ({
        key: entry.key,
        count: entry.count - entry.error,
      }))
      .filter((entry) => entry.count >= minimumCount)
      .sort((left, right) => right.count - left.count || left.key.localeCompare(right.key));
  }

  size(): number {
    return this.entries.size;
  }

  replacementCount(): number {
    return this.replacements;
  }

  private less(left: CounterEntry, right: CounterEntry): boolean {
    return left.count < right.count || (left.count === right.count && left.key < right.key);
  }

  private swap(leftIndex: number, rightIndex: number): void {
    const left = this.heap[leftIndex]!;
    const right = this.heap[rightIndex]!;
    this.heap[leftIndex] = right;
    this.heap[rightIndex] = left;
    left.heapIndex = rightIndex;
    right.heapIndex = leftIndex;
  }

  private siftUp(startIndex: number): void {
    let index = startIndex;
    while (index > 0) {
      const parent = Math.floor((index - 1) / 2);
      if (!this.less(this.heap[index]!, this.heap[parent]!)) break;
      this.swap(index, parent);
      index = parent;
    }
  }

  private siftDown(startIndex: number): void {
    let index = startIndex;
    while (true) {
      const left = index * 2 + 1;
      const right = left + 1;
      let smallest = index;
      if (left < this.heap.length && this.less(this.heap[left]!, this.heap[smallest]!)) {
        smallest = left;
      }
      if (right < this.heap.length && this.less(this.heap[right]!, this.heap[smallest]!)) {
        smallest = right;
      }
      if (smallest === index) return;
      this.swap(index, smallest);
      index = smallest;
    }
  }
}
