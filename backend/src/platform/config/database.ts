import { boundedIntegerSetting } from './boundedNumber.js';

export function loadDatabaseConfig(env: NodeJS.ProcessEnv) {
  const databaseSchema = String(env['DATABASE_SCHEMA'] ?? '').trim();
  const skipSchemaInit = String(env['DATABASE_SKIP_SCHEMA_INIT'] ?? '').trim().toLowerCase();

  if (databaseSchema && !/^[a-z_][a-z0-9_]*$/i.test(databaseSchema)) {
    throw new Error(`Invalid DATABASE_SCHEMA: ${databaseSchema}`);
  }

  return {
    schema: databaseSchema,
    skipSchemaInit: skipSchemaInit === '1' || skipSchemaInit === 'true' || skipSchemaInit === 'yes',
    applicationName: String(env['DATABASE_APPLICATION_NAME'] ?? 'meshcore-backend').trim() || 'meshcore-backend',
    statementTimeoutMs: boundedIntegerSetting(
      'DATABASE_STATEMENT_TIMEOUT_MS',
      env['DATABASE_STATEMENT_TIMEOUT_MS'],
      30_000,
      0,
      3_600_000,
    ),
    connectionTimeoutMs: boundedIntegerSetting(
      'DATABASE_CONNECTION_TIMEOUT_MS',
      env['DATABASE_CONNECTION_TIMEOUT_MS'],
      5_000,
      100,
      300_000,
    ),
    poolMax: boundedIntegerSetting(
      'DATABASE_POOL_MAX',
      env['DATABASE_POOL_MAX'],
      8,
      1,
      100,
    ),
    idleTimeoutMs: 30_000,
  } as const;
}

export const databaseConfig = loadDatabaseConfig(process.env);
