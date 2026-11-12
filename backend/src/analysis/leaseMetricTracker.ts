import { boundedAnalysisWorkloadLabel } from '../metrics/labels.js';

type Lease = { workload: string; expiresAt: number };
type DropReason = 'capacity' | 'expired';

/** Bounded local observations only; SQL leases remain authoritative. */
export class LeaseMetricTracker {
  private readonly runs = new Map<string, Lease>();
  private readonly counts = new Map<string, number>();
  private nextExpiryAt = Number.POSITIVE_INFINITY;

  constructor(
    private readonly capacity: number,
    private readonly publish: (workload: string, count: number) => void,
    private readonly dropped: (workload: string, reason: DropReason) => void,
    private readonly now: () => number = Date.now,
  ) {
    if (!Number.isSafeInteger(capacity) || capacity < 1) throw new Error('INVALID_LEASE_METRIC_CAPACITY');
  }

  get size(): number { return this.runs.size; }

  track(workload: string, runId: string, leaseExpiresAt: number, deadlineAt: number): void {
    const now = this.now();
    this.sweep(now);
    this.remove(runId);
    const expiresAt = Math.min(leaseExpiresAt, deadlineAt);
    if (!Number.isFinite(expiresAt) || expiresAt <= now) return;
    const label = boundedAnalysisWorkloadLabel(workload);
    if (this.runs.size >= this.capacity) {
      const oldest = this.runs.keys().next().value!;
      this.dropped(this.runs.get(oldest)!.workload, 'capacity');
      this.remove(oldest);
    }
    this.runs.set(runId, { workload: label, expiresAt });
    this.nextExpiryAt = Math.min(this.nextExpiryAt, expiresAt);
    const count = (this.counts.get(label) ?? 0) + 1;
    this.counts.set(label, count);
    this.publish(label, count);
  }

  remove(runId: string): void {
    const lease = this.runs.get(runId);
    if (!lease) return;
    this.runs.delete(runId);
    const count = (this.counts.get(lease.workload) ?? 1) - 1;
    if (count) this.counts.set(lease.workload, count);
    else this.counts.delete(lease.workload);
    this.publish(lease.workload, count);
    if (!this.runs.size) this.nextExpiryAt = Number.POSITIVE_INFINITY;
  }

  sweep(now = this.now()): void {
    if (now < this.nextExpiryAt) return;
    let next = Number.POSITIVE_INFINITY;
    for (const [id, lease] of this.runs) {
      if (lease.expiresAt <= now) {
        this.dropped(lease.workload, 'expired');
        this.remove(id);
      } else next = Math.min(next, lease.expiresAt);
    }
    this.nextExpiryAt = next;
  }
}
