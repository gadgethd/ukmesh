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

export function analyticsStatementTimeoutMs(configuredTimeoutMs: number): number {
  return configuredTimeoutMs === 0 ? 0 : Math.max(900_000, configuredTimeoutMs);
}

/** Optional per-analytics-session estimates; absent settings keep server defaults. */
export function analyticsPlannerOptions(env: NodeJS.ProcessEnv): string | undefined {
  const random = String(env['DATABASE_ANALYTICS_RANDOM_PAGE_COST'] ?? '').trim();
  const cache = String(env['DATABASE_ANALYTICS_EFFECTIVE_CACHE_SIZE'] ?? '').trim();
  if (random && (!/^(?:0|[1-9][0-9]*)(?:\.[0-9]+)?$/.test(random)
    || !Number.isFinite(Number(random)))) {
    throw new Error('DATABASE_ANALYTICS_RANDOM_PAGE_COST must be a nonnegative decimal');
  }
  if (cache) {
    const match = /^([1-9][0-9]*)(kB|MB|GB|TB)$/.exec(cache);
    const units: Record<string, bigint> = { kB: 1024n, MB: 1024n ** 2n, GB: 1024n ** 3n, TB: 1024n ** 4n };
    const bytes = match ? BigInt(match[1]!) * units[match[2]!]! : 0n;
    if (bytes < 8192n || bytes > 2_147_483_647n * 8192n) {
      throw new Error('DATABASE_ANALYTICS_EFFECTIVE_CACHE_SIZE must be a PostgreSQL cache estimate from 8kB to 2147483647 blocks');
    }
  }
  return [random && `-c random_page_cost=${random}`, cache && `-c effective_cache_size=${cache}`]
    .filter(Boolean).join(' ') || undefined;
}

/** Preserve pg URL-option precedence while sharing explicit analytics policy. */
export function analyticsConnectionOptions(env: NodeJS.ProcessEnv, schemaOptions?: string) {
  const plannerOptions = analyticsPlannerOptions(env);
  const options = [schemaOptions, plannerOptions].filter(Boolean).join(' ') || undefined;
  let connectionString = databaseUrlForRole('worker', env);
  if (plannerOptions && connectionString) {
    let endpoint: URL;
    try { endpoint = new URL(connectionString); }
    catch { throw new Error('Invalid worker database URL for analytics planner options'); }
    if (endpoint.searchParams.has('options')) {
      const previous = endpoint.searchParams.getAll('options').at(-1);
      endpoint.searchParams.set('options', [previous, plannerOptions].filter(Boolean).join(' '));
      connectionString = endpoint.href;
    }
  }
  return { connectionString, options };
}

export type DatabaseRole = 'http' | 'ingest' | 'worker' | 'owner' | 'migrate';

const roleDatabaseUrlEnv: Record<DatabaseRole, string> = {
  http: 'MESHCORE_HTTP_DATABASE_URL',
  ingest: 'MESHCORE_INGEST_DATABASE_URL',
  worker: 'MESHCORE_WORKER_DATABASE_URL',
  owner: 'OWNER_DATABASE_URL',
  migrate: 'MESHCORE_MIGRATE_DATABASE_URL',
};

function truthySetting(value: string | undefined): boolean {
  return ['1', 'true', 'yes', 'on'].includes(String(value ?? '').trim().toLowerCase());
}

/**
 * Resolve a role-specific DSN. Until the role split is rolled out, callers may
 * supply a legacy fallback. Once DATABASE_ROLE_ENFORCEMENT is enabled, a
 * missing role DSN is fatal even when a legacy URL exists.
 */
export function databaseUrlForRole(
  role: DatabaseRole,
  env: NodeJS.ProcessEnv = process.env,
  legacyFallback?: string,
): string | undefined {
  const envKey = roleDatabaseUrlEnv[role];
  const roleUrl = String(env[envKey] ?? '').trim();
  if (roleUrl) return roleUrl;

  if (truthySetting(env['DATABASE_ROLE_ENFORCEMENT'])) {
    throw new Error(`${envKey} is required when DATABASE_ROLE_ENFORCEMENT is enabled`);
  }

  const fallback = String(legacyFallback ?? env['DATABASE_URL'] ?? '').trim();
  return fallback || undefined;
}
