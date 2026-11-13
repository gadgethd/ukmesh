import type pg from 'pg';

/** At most two singleton batch statements per connection; values are never cached. */
export type FixedPreparedStatementName = 'packet_batch_single_v1' | 'packet_batch_single_idempotent_v1';

export type DatabaseQueryFn = <T extends pg.QueryResultRow = pg.QueryResultRow>(
  text: string,
  params?: unknown[],
  signal?: AbortSignal,
  statement?: FixedPreparedStatementName,
) => Promise<{ rows: T[] }>;
