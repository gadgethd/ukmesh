import type pg from 'pg';
import type { StatementHint } from './preparedStatements.js';

/** Two fixed ingest definitions or an explicit bounded read preparation hint. */
export type { FixedPreparedStatementName, StatementHint } from './preparedStatements.js';

export type DatabaseQueryFn = <T extends pg.QueryResultRow = pg.QueryResultRow>(
  text: string,
  params?: unknown[],
  signal?: AbortSignal,
  statement?: StatementHint,
) => Promise<{ rows: T[] }>;
