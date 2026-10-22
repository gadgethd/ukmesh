import { spamAnalysisTruncationsTotal } from '../metrics.js';

/** Report only aggregate counts; never include message text or identities. */
export function reportSpamAnalysisTruncation(
  phase: 'candidates' | 'messages' | 'incidents',
  observed: number,
  limit: number,
): void {
  if (observed <= limit) return;
  spamAnalysisTruncationsTotal.labels(phase).inc();
  console.warn(
    `[spam-msg] truncated phase=${phase} limit=${limit} observed_at_least=${observed} omitted_at_least=${observed - limit}`,
  );
}
