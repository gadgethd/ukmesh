import type pg from 'pg';

export const FIXED_PREPARED_STATEMENTS = Object.freeze({
  packetBatchSingle: 'packet_batch_single_v1',
  packetBatchSingleIdempotent: 'packet_batch_single_idempotent_v1',
} as const);

export type FixedPreparedStatementName = typeof FIXED_PREPARED_STATEMENTS[keyof typeof FIXED_PREPARED_STATEMENTS];

const allowed = new Set<FixedPreparedStatementName>(Object.values(FIXED_PREPARED_STATEMENTS));

export function fixedPreparedStatement(
  name: FixedPreparedStatementName, text: string, values?: unknown[],
): pg.QueryConfig<unknown[]> {
  if (!allowed.has(name)) throw new Error('INVALID_FIXED_PREPARED_STATEMENT');
  return { name, text, values };
}
