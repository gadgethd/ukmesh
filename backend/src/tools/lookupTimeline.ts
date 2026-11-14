/** Native lifecycle evidence only. HTTP requests keep their existing clocks. */
type Scope = { phase: string; trial?: number; case?: string };
type Clock = { utc: () => string; monotonic: () => number };
type Boundary = Scope & { sequence: number; span: number; parent: number | null;
  edge: 'start' | 'finish'; at: string; offsetMs: number; outcome?: 'passed' | 'failed' };

export const LOOKUP_TIMELINE_MAX_EVENTS = 1024;

export class LookupTimeline {
  private readonly events: Boundary[] = [];
  private readonly open: number[] = [];
  private readonly origin: number;
  private nextSpan = 0;
  private complete = false;
  readonly startedAt: string;

  constructor(private readonly clock: Clock = {
    utc: () => new Date().toISOString(), monotonic: () => performance.now(),
  }) {
    this.startedAt = clock.utc();
    this.origin = clock.monotonic();
    if (!Number.isFinite(this.origin) || !Number.isFinite(Date.parse(this.startedAt))) {
      throw new Error('invalid native timeline clock');
    }
  }

  private stamp() {
    const at = this.clock.utc(), offsetMs = this.clock.monotonic() - this.origin;
    if (!Number.isFinite(Date.parse(at)) || !Number.isFinite(offsetMs) || offsetMs < 0
      || offsetMs < (this.events.at(-1)?.offsetMs ?? 0)) throw new Error('invalid native timeline clock');
    return { at, offsetMs };
  }

  private record(scope: Scope, span: number, parent: number | null, edge: Boundary['edge'], outcome?: Boundary['outcome']) {
    this.events.push({ ...scope, sequence: this.events.length, span, parent, edge, ...this.stamp(),
      ...(outcome ? { outcome } : {}) });
  }

  async span<T>(scope: Scope, work: () => Promise<T>): Promise<T> {
    // Reserve both edges before work. Nested spans cannot exhaust cleanup space.
    if (this.complete || this.events.length + this.open.length + 2 > LOOKUP_TIMELINE_MAX_EVENTS) {
      throw new Error('native timeline event budget exceeded or already finished');
    }
    const span = this.nextSpan++, parent = this.open.at(-1) ?? null;
    this.record(scope, span, parent, 'start'); this.open.push(span);
    let outcome: 'passed' | 'failed' = 'failed';
    try {
      const result = await work(); outcome = 'passed'; return result;
    } finally {
      if (this.open.pop() !== span) throw new Error('native timeline spans completed out of order');
      this.record(scope, span, parent, 'finish', outcome);
    }
  }

  finish(passed: boolean) {
    if (this.complete || this.open.length) throw new Error('native timeline is incomplete or already finished');
    const finished = this.stamp();
    this.complete = true;
    return { version: 1, startedAt: this.startedAt, originMonotonicMs: this.origin,
      finishedAt: finished.at, durationMs: finished.offsetMs,
      passed, maxEvents: LOOKUP_TIMELINE_MAX_EVENTS, events: this.events };
  }
}
