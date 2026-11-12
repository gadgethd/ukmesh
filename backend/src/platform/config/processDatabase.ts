import { loadDatabaseConfig } from './database.js';

/** Intentional process configuration snapshot for CLI and legacy repository composition. */
export const databaseConfig = loadDatabaseConfig(process.env);
