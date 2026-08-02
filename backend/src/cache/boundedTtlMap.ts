import {
  boundedCacheMetricLabel,
  cacheBytes,
  cacheEntries,
  cacheOperationsTotal,
} from '../metrics.js';

type BoundedTtlMapOptions<K, V> = {
  name?: string;
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
  private readonly metricName: string;

  constructor(private readonly options: BoundedTtlMapOptions<K, V>) {
    super();
    if (
      !Number.isFinite(options.maxEntries) || options.maxEntries < 1
      || !Number.isFinite(options.maxWeight) || options.maxWeight < 1
      || !Number.isFinite(options.ttlMs) || options.ttlMs < 1
    ) throw new Error('INVALID_BOUNDED_CACHE_CONFIG');
    this.metricName = boundedCacheMetricLabel(options.name);
    this.timer = setInterval(() => this.sweep(), Math.min(options.ttlMs, 60_000));
    this.timer.unref();
    this.syncMetricGauges();
  }

  private recordMetric(outcome: string): void {
    cacheOperationsTotal.inc({ cache: this.metricName, outcome });
  }

  private syncMetricGauges(): void {
    cacheEntries.set({ cache: this.metricName }, this.size);
    cacheBytes.set({ cache: this.metricName }, this.totalWeight);
  }

  override get(key: K): V | undefined {
    if (!super.has(key)) {
      this.misses += 1;
      this.recordMetric('miss');
      return undefined;
    }
    const value = super.get(key);
    const now = this.options.now?.() ?? Date.now();
    if (now - (this.insertedAt.get(key) ?? 0) >= this.options.ttlMs) {
      this.expiries += 1;
      this.misses += 1;
      this.recordMetric('expired');
      this.recordMetric('miss');
      this.delete(key);
      return undefined;
    }
    this.hits += 1;
    this.recordMetric('hit');
    super.delete(key);
    super.set(key, value as V);
    return value;
  }

  override set(key: K, value: V): this {
    const weight = Math.max(1, Math.trunc(this.options.weightOf?.(key, value) ?? defaultWeight(value)));
    if (weight > this.options.maxWeight) {
      this.rejections += 1;
      this.recordMetric('rejected');
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
      this.recordMetric('evicted');
      this.delete(oldest);
    }
    this.recordMetric('set');
    this.syncMetricGauges();
    return this;
  }

  override delete(key: K): boolean {
    this.totalWeight = Math.max(0, this.totalWeight - (this.weights.get(key) ?? 0));
    this.weights.delete(key);
    this.insertedAt.delete(key);
    const deleted = super.delete(key);
    if (deleted) {
      this.recordMetric('deleted');
      this.syncMetricGauges();
    }
    return deleted;
  }

  override clear(): void {
    super.clear();
    this.insertedAt.clear();
    this.weights.clear();
    this.totalWeight = 0;
    this.recordMetric('cleared');
    this.syncMetricGauges();
  }

  sweep(now = this.options.now?.() ?? Date.now()): void {
    for (const [key, timestamp] of this.insertedAt) {
      if (now - timestamp >= this.options.ttlMs) {
        this.expiries += 1;
        this.recordMetric('expired');
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
