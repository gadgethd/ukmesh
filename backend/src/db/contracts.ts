import type pg from 'pg';

export type DatabaseQueryFn = <T extends pg.QueryResultRow = pg.QueryResultRow>(
  text: string,
  params?: unknown[],
) => Promise<{ rows: T[] }>;
