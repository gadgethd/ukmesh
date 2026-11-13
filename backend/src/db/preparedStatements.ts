import type pg from 'pg';
import type { FixedPreparedStatementName } from './contracts.js';

const allowed = new Set<FixedPreparedStatementName>(['packet_batch_single_v1', 'packet_batch_single_idempotent_v1']);

export function fixedPreparedStatement(
  name: FixedPreparedStatementName, text: string, values?: unknown[],
): pg.QueryConfig<unknown[]> {
  if (!allowed.has(name)) throw new Error('INVALID_FIXED_PREPARED_STATEMENT');
  return { name, text, values };
}
