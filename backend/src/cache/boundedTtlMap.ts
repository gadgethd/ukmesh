type BoundedTtlMapOptions<K, V> = {
  maxEntries: number;
  maxWeight: number;
  ttlMs: number;
  weightOf?: (key: K, value: V) => number;
  now?: () => number;
};

function defaultWeight(value: unknown): number {
  try {
    return Buffer.byteLength(JSON.stringify(value));
  } catch {
    return 1;
  }
}

/** Map-compatible bounded LRU/TTL storage with physical cold-entry expiry. */
export class BoundedTtlMap<K, V> extends Map<K, V> {
  private readonly insertedAt = new Map<K, number>();
  private readonly weights = new Map<K, number>();
  private totalWeight = 0;
  private readonly timer: NodeJS.Timeout;
  private hits = 0;
  private misses = 0;
  private expiries = 0;
  private evictions = 0;
  private rejections = 0;

  constructor(private readonly options: BoundedTtlMapOptions<K, V>) {
    super();
    if (
      !Number.isFinite(options.maxEntries) || options.maxEntries < 1
      || !Number.isFinite(options.maxWeight) || options.maxWeight < 1
      || !Number.isFinite(options.ttlMs) || options.ttlMs < 1
    ) throw new Error('INVALID_BOUNDED_CACHE_CONFIG');
    this.timer = setInterval(() => this.sweep(), Math.min(options.ttlMs, 60_000));
    this.timer.unref();
  }

  override get(key: K): V | undefined {
    if (!super.has(key)) {
      this.misses += 1;
      return undefined;
    }
    const value = super.get(key);
    const now = this.options.now?.() ?? Date.now();
    if (now - (this.insertedAt.get(key) ?? 0) >= this.options.ttlMs) {
      this.expiries += 1;
      this.misses += 1;
      this.delete(key);
      return undefined;
    }
    this.hits += 1;
    super.delete(key);
    super.set(key, value as V);
    return value;
  }

  override set(key: K, value: V): this {
    const weight = Math.max(1, Math.trunc(this.options.weightOf?.(key, value) ?? defaultWeight(value)));
    if (weight > this.options.maxWeight) {
      this.rejections += 1;
      return this;
    }
    this.sweep();
    this.delete(key);
    super.set(key, value);
    this.insertedAt.set(key, this.options.now?.() ?? Date.now());
    this.weights.set(key, weight);
    this.totalWeight += weight;
    while (this.size > this.options.maxEntries || this.totalWeight > this.options.maxWeight) {
      const oldest = this.keys().next().value as K | undefined;
      if (oldest === undefined) break;
      this.evictions += 1;
      this.delete(oldest);
    }
    return this;
  }

  override delete(key: K): boolean {
    this.totalWeight = Math.max(0, this.totalWeight - (this.weights.get(key) ?? 0));
    this.weights.delete(key);
    this.insertedAt.delete(key);
    return super.delete(key);
  }

  override clear(): void {
    super.clear();
    this.insertedAt.clear();
    this.weights.clear();
    this.totalWeight = 0;
  }

  sweep(now = this.options.now?.() ?? Date.now()): void {
    for (const [key, timestamp] of this.insertedAt) {
      if (now - timestamp >= this.options.ttlMs) {
        this.expiries += 1;
        this.delete(key);
      }
    }
  }

  weight(): number {
    return this.totalWeight;
  }

  metrics(): {
    hits: number;
    misses: number;
    expiries: number;
    evictions: number;
    rejections: number;
    size: number;
    weight: number;
  } {
    return {
      hits: this.hits,
      misses: this.misses,
      expiries: this.expiries,
      evictions: this.evictions,
      rejections: this.rejections,
      size: this.size,
      weight: this.totalWeight,
    };
  }

  shutdown(): void {
    clearInterval(this.timer);
  }
}
