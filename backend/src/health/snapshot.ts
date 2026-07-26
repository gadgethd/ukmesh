export type HealthSnapshotRead<T> =
  | { ready: true; generatedAt: number; data: T }
  | { ready: false; generatedAt: number | null; lastError: string | null };

export class HealthSnapshotCache<T> {
  private snapshot: { generatedAt: number; data: T } | null = null;
  private inflight: Promise<void> | null = null;
  private lastError: string | null = null;

  constructor(
    private readonly load: () => Promise<T>,
    private readonly hardTtlMs: number,
    private readonly now: () => number = Date.now,
  ) {}

  refresh(): Promise<void> {
    if (this.inflight) return this.inflight;
    const tracked = this.load()
      .then((data) => {
        this.snapshot = { generatedAt: this.now(), data };
        this.lastError = null;
      })
      .catch((err: unknown) => {
        this.lastError = err instanceof Error ? err.message : String(err);
      })
      .finally(() => {
        if (this.inflight === tracked) this.inflight = null;
      });
    this.inflight = tracked;
    return tracked;
  }

  read(): HealthSnapshotRead<T> {
    const current = this.snapshot;
    if (!current || this.now() - current.generatedAt > this.hardTtlMs) {
      return {
        ready: false,
        generatedAt: current?.generatedAt ?? null,
        lastError: this.lastError,
      };
    }
    return { ready: true, generatedAt: current.generatedAt, data: current.data };
  }
}
