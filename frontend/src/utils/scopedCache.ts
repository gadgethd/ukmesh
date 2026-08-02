export type ScopedCacheSnapshot = {
  name: string;
  entries: number;
  bytes: number;
  inflight: number;
  hits: number;
  misses: number;
  loads: number;
  evictions: number;
  expirations: number;
  rejectedLoads: number;
  maxEntries: number;
  maxBytes: number;
};

export type ScopedCacheOptions<Value> = {
  name: string;
  ttlMs: number;
  maxEntries: number;
  maxBytes: number;
  maxInflight?: number;
  estimateBytes?: (value: Value) => number;
  onEvict?: (value: Value, reason: 'capacity' | 'expired' | 'delete' | 'clear' | 'scope') => void;
  now?: () => number;
};

type Entry<Value> = {
  value: Value;
  scope: string;
  expiresAt: number;
  bytes: number;
};

function defaultEstimateBytes(value: unknown): number {
  try {
    return new TextEncoder().encode(JSON.stringify(value)).byteLength;
  } catch {
    return 1_024;
  }
}

function compositeKey(scope: string, key: string): string {
  return `${scope.length}:${scope}${key}`;
}

export class ScopedCache<Value> {
  readonly #options: Required<
    Pick<ScopedCacheOptions<Value>, 'name' | 'ttlMs' | 'maxEntries' | 'maxBytes' | 'maxInflight'>
  > & ScopedCacheOptions<Value>;
  readonly #entries = new Map<string, Entry<Value>>();
  readonly #inflight = new Map<string, Promise<Value>>();
  readonly #scopeGenerations = new Map<string, number>();
  #generation = 0;
  #bytes = 0;
  #hits = 0;
  #misses = 0;
  #loads = 0;
  #evictions = 0;
  #expirations = 0;
  #rejectedLoads = 0;

  constructor(options: ScopedCacheOptions<Value>) {
    if (!options.name.trim()) throw new Error('ScopedCache requires a name');
    if (!Number.isFinite(options.ttlMs) || options.ttlMs <= 0) throw new Error('ttlMs must be positive');
    if (!Number.isSafeInteger(options.maxEntries) || options.maxEntries <= 0) {
      throw new Error('maxEntries must be a positive integer');
    }
    if (!Number.isSafeInteger(options.maxBytes) || options.maxBytes <= 0) {
      throw new Error('maxBytes must be a positive integer');
    }
    const maxInflight = options.maxInflight ?? Math.min(options.maxEntries, 32);
    if (!Number.isSafeInteger(maxInflight) || maxInflight <= 0) {
      throw new Error('maxInflight must be a positive integer');
    }
    this.#options = { ...options, maxInflight };
  }

  #now(): number {
    return this.#options.now?.() ?? Date.now();
  }

  #remove(
    key: string,
    reason: 'capacity' | 'expired' | 'delete' | 'clear' | 'scope',
  ): boolean {
    const entry = this.#entries.get(key);
    if (!entry) return false;
    this.#entries.delete(key);
    this.#bytes -= entry.bytes;
    if (reason === 'capacity') this.#evictions += 1;
    if (reason === 'expired') this.#expirations += 1;
    this.#options.onEvict?.(entry.value, reason);
    return true;
  }

  #pruneExpired(now = this.#now()): void {
    for (const [key, entry] of this.#entries) {
      if (entry.expiresAt <= now) this.#remove(key, 'expired');
    }
  }

  #enforceBounds(): void {
    while (this.#entries.size > this.#options.maxEntries || this.#bytes > this.#options.maxBytes) {
      const oldestKey = this.#entries.keys().next().value as string | undefined;
      if (oldestKey === undefined) break;
      this.#remove(oldestKey, 'capacity');
    }
  }

  get(scope: string, key: string): Value | undefined {
    const id = compositeKey(scope, key);
    const entry = this.#entries.get(id);
    if (!entry) {
      this.#misses += 1;
      return undefined;
    }
    if (entry.expiresAt <= this.#now()) {
      this.#remove(id, 'expired');
      this.#misses += 1;
      return undefined;
    }
    // Map insertion order is the LRU order.
    this.#entries.delete(id);
    this.#entries.set(id, entry);
    this.#hits += 1;
    return entry.value;
  }

  peek(scope: string, key: string): Value | undefined {
    const id = compositeKey(scope, key);
    const entry = this.#entries.get(id);
    if (!entry || entry.expiresAt <= this.#now()) return undefined;
    return entry.value;
  }

  set(scope: string, key: string, value: Value, ttlMs = this.#options.ttlMs): void {
    if (!Number.isFinite(ttlMs) || ttlMs <= 0) throw new Error('entry ttlMs must be positive');
    this.#pruneExpired();
    const id = compositeKey(scope, key);
    this.#remove(id, 'delete');
    const estimate = this.#options.estimateBytes?.(value) ?? defaultEstimateBytes(value);
    const bytes = Math.max(1, Number.isFinite(estimate) ? Math.trunc(estimate) : 1);
    this.#entries.set(id, {
      value,
      scope,
      expiresAt: this.#now() + ttlMs,
      bytes,
    });
    this.#bytes += bytes;
    this.#enforceBounds();
  }

  delete(scope: string, key: string): boolean {
    return this.#remove(compositeKey(scope, key), 'delete');
  }

  invalidateScope(scope: string): number {
    this.#scopeGenerations.set(scope, (this.#scopeGenerations.get(scope) ?? 0) + 1);
    let removed = 0;
    for (const [key, entry] of this.#entries) {
      if (entry.scope === scope && this.#remove(key, 'scope')) removed += 1;
    }
    for (const key of this.#inflight.keys()) {
      if (key.startsWith(`${scope.length}:${scope}`)) this.#inflight.delete(key);
    }
    return removed;
  }

  clear(): void {
    this.#generation += 1;
    for (const key of [...this.#entries.keys()]) this.#remove(key, 'clear');
    this.#inflight.clear();
  }

  async getOrLoad(scope: string, key: string, loader: () => Promise<Value>): Promise<Value> {
    const cached = this.get(scope, key);
    if (cached !== undefined) return cached;
    const id = compositeKey(scope, key);
    const existing = this.#inflight.get(id);
    if (existing) return existing;
    if (this.#inflight.size >= this.#options.maxInflight) {
      this.#rejectedLoads += 1;
      throw new Error(`ScopedCache ${this.#options.name} inflight limit exceeded`);
    }
    this.#loads += 1;
    const generation = this.#generation;
    const scopeGeneration = this.#scopeGenerations.get(scope) ?? 0;
    const pending = loader()
      .then((value) => {
        if (
          this.#generation === generation
          && (this.#scopeGenerations.get(scope) ?? 0) === scopeGeneration
        ) {
          this.set(scope, key, value);
        }
        return value;
      })
      .finally(() => {
        if (this.#inflight.get(id) === pending) this.#inflight.delete(id);
      });
    this.#inflight.set(id, pending);
    return pending;
  }

  snapshot(): ScopedCacheSnapshot {
    this.#pruneExpired();
    return {
      name: this.#options.name,
      entries: this.#entries.size,
      bytes: this.#bytes,
      inflight: this.#inflight.size,
      hits: this.#hits,
      misses: this.#misses,
      loads: this.#loads,
      evictions: this.#evictions,
      expirations: this.#expirations,
      rejectedLoads: this.#rejectedLoads,
      maxEntries: this.#options.maxEntries,
      maxBytes: this.#options.maxBytes,
    };
  }
}
