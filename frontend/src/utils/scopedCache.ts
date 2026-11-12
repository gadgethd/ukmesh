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
  loadTimeoutMs?: number;
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

type Load<Value> = {
  scope: string;
  controller: AbortController;
  promise: Promise<Value>;
};

function consume<Value>(pending: Promise<Value>, signal?: AbortSignal): Promise<Value> {
  if (!signal) return pending;
  if (signal.aborted) return Promise.reject(signal.reason);
  return new Promise<Value>((resolve, reject) => {
    const aborted = () => reject(signal.reason);
    signal.addEventListener('abort', aborted, { once: true });
    void pending.then(resolve, reject).finally(() => signal.removeEventListener('abort', aborted));
  });
}

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
  readonly #inflight = new Map<string, Load<Value>>();
  readonly #active = new Set<Load<Value>>();
  #activeScope: string | null = null;
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
    if (options.loadTimeoutMs !== undefined && (!Number.isFinite(options.loadTimeoutMs) || options.loadTimeoutMs <= 0)) {
      throw new Error('loadTimeoutMs must be positive');
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
    let removed = 0;
    for (const [key, entry] of this.#entries) {
      if (entry.scope === scope && this.#remove(key, 'scope')) removed += 1;
    }
    for (const [key, load] of this.#inflight) {
      if (load.scope === scope) {
        this.#inflight.delete(key);
        load.controller.abort(new DOMException('Scope invalidated', 'AbortError'));
      }
    }
    return removed;
  }

  /** Called by a resource's session owner, never by a consumer's unmount cleanup. */
  activateScope(scope: string): void {
    if (this.#activeScope === scope) return;
    if (this.#activeScope !== null) this.invalidateScope(this.#activeScope);
    this.#activeScope = scope;
  }

  clear(): void {
    this.#activeScope = null;
    for (const key of [...this.#entries.keys()]) this.#remove(key, 'clear');
    for (const load of this.#inflight.values()) {
      load.controller.abort(new DOMException('Cache disposed', 'AbortError'));
    }
    this.#inflight.clear();
  }

  async getOrLoad(scope: string, key: string, loader: (signal: AbortSignal) => Promise<Value>, consumerSignal?: AbortSignal): Promise<Value> {
    if (consumerSignal?.aborted) throw consumerSignal.reason;
    const cached = this.get(scope, key);
    if (cached !== undefined) return cached;
    const id = compositeKey(scope, key);
    const existing = this.#inflight.get(id);
    if (existing) return consume(existing.promise, consumerSignal);
    if (this.#active.size >= this.#options.maxInflight) {
      this.#rejectedLoads += 1;
      throw new Error(`ScopedCache ${this.#options.name} inflight limit exceeded`);
    }
    this.#loads += 1;
    const controller = new AbortController();
    const load: Load<Value> = { scope, controller, promise: Promise.resolve(undefined as Value) };
    this.#active.add(load);
    this.#inflight.set(id, load);
    const timer = setTimeout(() => controller.abort(new DOMException('Cache load deadline exceeded', 'TimeoutError')),
      this.#options.loadTimeoutMs ?? 15_000);
    timer.unref?.();
    const stopTimer = () => clearTimeout(timer);
    controller.signal.addEventListener('abort', stopTimer, { once: true });
    let work: Promise<Value>;
    try { work = loader(controller.signal); } catch (error) { work = Promise.reject(error); }
    const pending = work
      .then((value) => {
        if (!controller.signal.aborted && this.#inflight.get(id) === load) {
          this.set(scope, key, value);
        }
        return value;
      })
      .finally(() => {
        stopTimer();
        controller.signal.removeEventListener('abort', stopTimer);
        this.#active.delete(load);
        if (this.#inflight.get(id) === load) this.#inflight.delete(id);
      });
    load.promise = consume(pending, controller.signal);
    return consume(load.promise, consumerSignal);
  }

  snapshot(): ScopedCacheSnapshot {
    this.#pruneExpired();
    return {
      name: this.#options.name,
      entries: this.#entries.size,
      bytes: this.#bytes,
      inflight: this.#active.size,
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
