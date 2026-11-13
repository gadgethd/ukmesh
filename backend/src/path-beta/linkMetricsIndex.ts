import { undirectedLinkKey } from '../path-shared/priorKeys.js';
import type { LinkMetrics } from './types.js';

type Numeric = number | string;
export type LinkMetricRow = {
  node_a_id: string;
  node_b_id: string;
  observed_count: Numeric | null;
  multibyte_observed_count: Numeric | null;
  itm_path_loss_db: Numeric | null;
  itm_viable: boolean | null;
  count_a_to_b: Numeric | null;
  count_b_to_a: Numeric | null;
  neighbor_report_count?: Numeric;
  neighbor_best_snr_db?: Numeric | null;
};

/** Read-only graph snapshot. IDs are interned once; edge values use packed
 * numeric columns rather than retaining two long IDs and an object per edge.
 * Lookup returns the same fields, nulls and directional counts as the old Map.
 */
export class LinkMetricsIndex {
  private readonly ids = new Map<string, number>();
  private readonly positions = new Map<number | string, number>();
  private readonly legacy = new Map<string, LinkMetrics>();
  private readonly values: Float64Array;
  private readonly flags: Uint8Array;
  private readonly neighborCounts: Float64Array | undefined;
  private readonly neighborSnr: Float64Array | undefined;
  private readonly stride: number;

  constructor(rows: readonly LinkMetricRow[]) {
    this.values = new Float64Array(rows.length * 5);
    this.flags = new Uint8Array(rows.length);
    this.neighborCounts = rows.some(row => row.neighbor_report_count !== undefined)
      ? new Float64Array(rows.length) : undefined;
    this.neighborSnr = rows.some(row => row.neighbor_best_snr_db !== undefined)
      ? new Float64Array(rows.length) : undefined;
    this.stride = rows.length * 2 + 1;
    const idIndex = (id: string) => {
      let index = this.ids.get(id);
      if (index === undefined) { index = this.ids.size; this.ids.set(id, index); }
      return index;
    };
    for (const [index, row] of rows.entries()) {
      const offset = index * 5;
      this.values[offset] = Number(row.observed_count ?? 0);
      this.values[offset + 1] = Number(row.multibyte_observed_count ?? 0);
      this.values[offset + 2] = Number(row.itm_path_loss_db ?? 0);
      this.values[offset + 3] = Number(row.count_a_to_b ?? 0);
      this.values[offset + 4] = Number(row.count_b_to_a ?? 0);
      this.flags[index] = (row.itm_path_loss_db == null ? 1 : 0)
        | (row.count_a_to_b == null ? 2 : 0) | (row.count_b_to_a == null ? 4 : 0)
        | (row.itm_viable === true ? 8 : row.itm_viable === false ? 16 : 0)
        | (row.neighbor_report_count !== undefined ? 32 : 0)
        | (row.neighbor_best_snr_db !== undefined ? 64 : 0)
        | (row.neighbor_best_snr_db === null ? 128 : 0);
      if (this.neighborCounts) this.neighborCounts[index] = Number(row.neighbor_report_count ?? 0);
      if (this.neighborSnr) this.neighborSnr[index] = Number(row.neighbor_best_snr_db ?? 0);
      if (row.node_a_id.includes(':') || row.node_b_id.includes(':')) {
        // Preserve the old string-key behaviour for nonstandard legacy IDs.
        this.legacy.set(undirectedLinkKey(row.node_a_id, row.node_b_id), this.metricsAt(index));
      } else {
        this.positions.set(this.pairKey(idIndex(row.node_a_id), idIndex(row.node_b_id)), index);
      }
    }
  }

  get size(): number { return this.positions.size + this.legacy.size; }

  private pairKey(a: number, b: number): number | string {
    const low = Math.min(a, b), high = Math.max(a, b);
    const key = low * this.stride + high;
    return Number.isSafeInteger(key) ? key : `${low}:${high}`;
  }

  get(key: string): LinkMetrics | undefined {
    const separator = key.indexOf(':');
    if (separator < 0) return undefined;
    if (separator !== key.lastIndexOf(':')) return this.legacy.get(key);
    const first = key.slice(0, separator), second = key.slice(separator + 1);
    // Match Map.get exactly: callers use the lexically ordered string key.
    if (first > second) return undefined;
    const a = this.ids.get(first), b = this.ids.get(second);
    if (a === undefined || b === undefined) return undefined;
    const index = this.positions.get(this.pairKey(a, b));
    return index === undefined ? undefined : this.metricsAt(index);
  }

  private metricsAt(index: number): LinkMetrics {
    const offset = index * 5, flags = this.flags[index]!;
    const metrics: LinkMetrics = {
      observed_count: this.values[offset]!, multibyte_observed_count: this.values[offset + 1]!,
      itm_path_loss_db: flags & 1 ? null : this.values[offset + 2]!,
      itm_viable: flags & 8 ? true : flags & 16 ? false : null,
      count_a_to_b: flags & 2 ? null : this.values[offset + 3]!,
      count_b_to_a: flags & 4 ? null : this.values[offset + 4]!,
    };
    if (flags & 32) metrics.neighbor_report_count = this.neighborCounts![index]!;
    if (flags & 64) metrics.neighbor_best_snr_db = flags & 128 ? null : this.neighborSnr![index]!;
    return metrics;
  }

  /** Cache accounting walks the actual retained backing stores, not an
   * expanded logical Map which would allocate objects just to weigh them. */
  retainedReferences(): readonly object[] {
    return [this.ids, this.positions, this.legacy, this.values, this.flags,
      ...(this.neighborCounts ? [this.neighborCounts] : []), ...(this.neighborSnr ? [this.neighborSnr] : [])];
  }
}
