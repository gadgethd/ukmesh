/** Native lifecycle evidence only. HTTP requests keep their existing clocks. */
type Scope = { phase: string; trial?: number; case?: string };
type Clock = { utc: () => string; monotonic: () => number };
type Capture = { at: string; monotonicBeforeMs: number; monotonicAfterMs: number };
type Boundary = Scope & { sequence: number; span: number; parent: number | null;
  edge: 'start' | 'finish'; at: string; offsetMs: number; clock: Capture; outcome?: 'passed' | 'failed' };

export const LOOKUP_TIMELINE_MAX_EVENTS = 1024;

export class LookupTimeline {
  private readonly events: Boundary[] = [];
  private readonly open: number[] = [];
  private readonly origin: Capture;
  private nextSpan = 0;
  private complete = false;
  readonly startedAt: string;
  get originMonotonicMs(): number { return this.origin.monotonicAfterMs; }

  constructor(private readonly clock: Clock = {
    utc: () => new Date().toISOString(), monotonic: () => performance.now(),
  }) {
    this.origin = this.capture();
    this.startedAt = this.origin.at;
  }

  private capture(): Capture {
    const monotonicBeforeMs = this.clock.monotonic(), at = this.clock.utc(), monotonicAfterMs = this.clock.monotonic();
    if (!Number.isFinite(Date.parse(at)) || !Number.isFinite(monotonicBeforeMs) || !Number.isFinite(monotonicAfterMs)
      || monotonicBeforeMs < 0 || monotonicAfterMs < monotonicBeforeMs) throw new Error('invalid native timeline clock');
    return { at, monotonicBeforeMs, monotonicAfterMs };
  }

  private stamp() {
    const clock = this.capture();
    if (clock.monotonicBeforeMs < (this.events.at(-1)?.clock ?? this.origin).monotonicAfterMs) {
      throw new Error('invalid native timeline clock');
    }
    return { at: clock.at, offsetMs: clock.monotonicAfterMs - this.origin.monotonicAfterMs, clock };
  }

  private record(scope: Scope, span: number, parent: number | null, edge: Boundary['edge'], outcome?: Boundary['outcome']) {
    const boundary = { ...scope, sequence: this.events.length, span, parent, edge, ...this.stamp(),
      ...(outcome ? { outcome } : {}) };
    this.events.push(boundary);
    return boundary;
  }

  async span<T>(scope: Scope, work: (start: Boundary) => Promise<T>): Promise<T> {
    // Reserve both edges before work. Nested spans cannot exhaust cleanup space.
    if (this.complete || this.events.length + this.open.length + 2 > LOOKUP_TIMELINE_MAX_EVENTS) {
      throw new Error('native timeline event budget exceeded or already finished');
    }
    const span = this.nextSpan++, parent = this.open.at(-1) ?? null;
    const started = this.record(scope, span, parent, 'start'); this.open.push(span);
    let outcome: 'passed' | 'failed' = 'failed';
    try {
      const result = await work(started); outcome = 'passed'; return result;
    } finally {
      if (this.open.pop() !== span) throw new Error('native timeline spans completed out of order');
      this.record(scope, span, parent, 'finish', outcome);
    }
  }

  finish(passed: boolean) {
    if (this.complete || this.open.length) throw new Error('native timeline is incomplete or already finished');
    const finished = this.stamp();
    this.complete = true;
    return { version: 2, startedAt: this.startedAt, originMonotonicMs: this.origin.monotonicAfterMs, originClock: this.origin,
      finishedAt: finished.at, finishedClock: finished.clock, durationMs: finished.offsetMs,
      passed, maxEvents: LOOKUP_TIMELINE_MAX_EVENTS, events: this.events };
  }
}
