import type pg from 'pg';
import type { FixedPreparedStatementName } from './preparedStatements.js';

/** At most two singleton batch statements per connection; values are never cached. */
export type { FixedPreparedStatementName } from './preparedStatements.js';

export type DatabaseQueryFn = <T extends pg.QueryResultRow = pg.QueryResultRow>(
  text: string,
  params?: unknown[],
  signal?: AbortSignal,
  statement?: FixedPreparedStatementName,
) => Promise<{ rows: T[] }>;
