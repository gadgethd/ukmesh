const databaseSchema = String(process.env['DATABASE_SCHEMA'] ?? '').trim();
const skipSchemaInit = String(process.env['DATABASE_SKIP_SCHEMA_INIT'] ?? '').trim().toLowerCase();

if (databaseSchema && !/^[a-z_][a-z0-9_]*$/i.test(databaseSchema)) {
  throw new Error(`Invalid DATABASE_SCHEMA: ${databaseSchema}`);
}

export const databaseConfig = {
  schema: databaseSchema,
  skipSchemaInit: skipSchemaInit === '1' || skipSchemaInit === 'true' || skipSchemaInit === 'yes',
  applicationName: String(process.env['DATABASE_APPLICATION_NAME'] ?? 'meshcore-backend').trim() || 'meshcore-backend',
  statementTimeoutMs: Number(process.env['DATABASE_STATEMENT_TIMEOUT_MS'] ?? 30_000),
  poolMax: Number(process.env['DATABASE_POOL_MAX'] ?? 8),
  idleTimeoutMs: 30_000,
  connectionTimeoutMs: 5_000,
} as const;
