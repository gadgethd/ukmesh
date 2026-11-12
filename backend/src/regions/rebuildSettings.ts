import { boundedEnvInteger } from '../platform/config/boundedNumber.js';

export function regionRebuildSettings(env: NodeJS.ProcessEnv = process.env) {
  const read = (name: string, fallback: number, min: number, max: number) =>
    boundedEnvInteger(name, fallback, min, max, env[name]?.trim() || undefined);
  return {
    windowDays: read('REGION_SCOPE_WINDOW_DAYS', 92, 7, 365),
    maxPackets: read('REGION_SCOPE_MAX_PACKETS', 500_000, 1, 2_000_000),
    maxNodes: read('REGION_SCOPE_MAX_NODES', 100_000, 1, 1_000_000),
    statementTimeoutMs: read('REGION_SCOPE_STATEMENT_TIMEOUT_MS', 120_000, 1_000, 900_000),
    runTimeoutMs: read('REGION_SCOPE_RUN_TIMEOUT_MS', 180_000, 5_000, 900_000),
    pollMs: read('REGION_SCOPE_POLL_MS', 30_000, 1_000, 300_000),
    maxAgeMs: read('REGION_SCOPE_MAX_AGE_MS', 900_000, 60_000, 86_400_000),
    allowEmpty: env['REGION_SCOPE_ALLOW_EMPTY'] === '1',
  };
}
