import { createHash } from 'node:crypto';
import type pg from 'pg';

export const FIXED_PREPARED_STATEMENTS = Object.freeze({
  packetBatchSingle: 'packet_batch_single_v1',
  packetBatchSingleIdempotent: 'packet_batch_single_idempotent_v1',
} as const);

export type FixedPreparedStatementName = typeof FIXED_PREPARED_STATEMENTS[keyof typeof FIXED_PREPARED_STATEMENTS];

/** Explicit opt-in; this caches statement definitions, never read results. */
export const BOUNDED_READ_PREPARATION = Object.freeze({ kind: 'bounded-read' } as const);
export type BoundedReadPreparationHint = typeof BOUNDED_READ_PREPARATION;
export type StatementHint = FixedPreparedStatementName | BoundedReadPreparationHint;

const allowed = new Set<FixedPreparedStatementName>(Object.values(FIXED_PREPARED_STATEMENTS));
const readStatements = new WeakMap<pg.PoolClient, Set<string>>();
const MAX_READ_STATEMENTS_PER_CLIENT = 32;

export function fixedPreparedStatement(
  name: FixedPreparedStatementName, text: string, values?: unknown[],
): pg.QueryConfig<unknown[]> {
  if (!allowed.has(name)) throw new Error('INVALID_FIXED_PREPARED_STATEMENT');
  return { name, text, values };
}

/** Resolve only after checkout: prepared definitions belong to this connection.
 * Overflow stays unnamed; eviction would not remove server/driver definitions.
 */
export function preparedStatementForClient(
  client: pg.PoolClient, hint: StatementHint, text: string, values?: unknown[],
): pg.QueryConfig<unknown[]> {
  if (typeof hint === 'string') return fixedPreparedStatement(hint, text, values);
  if (!hint || hint.kind !== 'bounded-read' || Object.keys(hint).length !== 1) {
    throw new Error('INVALID_STATEMENT_HINT');
  }
  const digest = createHash('sha256').update(text, 'utf8').digest('base64url');
  let admitted = readStatements.get(client);
  if (!admitted) {
    admitted = new Set();
    readStatements.set(client, admitted);
  }
  if (!admitted.has(digest)) {
    if (admitted.size >= MAX_READ_STATEMENTS_PER_CLIENT) return { text, values };
    // Reserve before dispatch, including failed statements and queued callers.
    admitted.add(digest);
  }
  return { name: `read_v1_${digest}`, text, values };
}
