import { createDatabaseRuntime } from './databaseRuntime.js';

/**
 * Intentional process-default composition for the existing CLI and repositories.
 * Independent execution imports databaseRuntime directly and supplies its env.
 * Creating these shared pools performs no queries, packet-batch setup or migrations.
 */
const database = createDatabaseRuntime(process.env);
export const {
  query, namedQuery, analyticsQuery, withTransaction, pool, analyticsPool,
  ingestPool, ingestQuery, withIngestTransaction, close: closeDb,
} = database;

export async function initDb(): Promise<void> {
  // Schema creation belongs to the migrate job, avoiding replica DDL races.
  console.warn('[db] startup schema initialisation is disabled; run the migrate job before starting services');
}
