import { pathToFileURL } from 'node:url';
import pg from 'pg';
import {
  convergePacketVisibility,
  type ConvergenceOptions,
  PrivacyConvergenceError,
  validateGeneration,
} from '../privacy/convergence.js';

export function parseConvergenceArgs(args: string[]): ConvergenceOptions {
  const options: ConvergenceOptions = {};
  const seen = new Set<string>();
  for (const arg of args) {
    const name = arg.split('=')[0];
    if (seen.has(name)) throw new PrivacyConvergenceError(`DUPLICATE_OPTION:${name}`);
    seen.add(name);
    if (arg === '--apply') options.apply = true;
    else if (arg === '--dry-run') options.apply = false;
    else if (arg.startsWith('--expected-generation=')) options.expectedGeneration = validateGeneration(arg.slice(22));
    else if (arg.startsWith('--statement-timeout-ms=')) options.statementTimeoutMs = Number(arg.slice(23));
    else throw new PrivacyConvergenceError(`UNKNOWN_OPTION:${name}`);
  }
  if (seen.has('--apply') && seen.has('--dry-run')) throw new PrivacyConvergenceError('CONFLICTING_MODES');
  if (options.apply) validateGeneration(options.expectedGeneration);
  if (options.statementTimeoutMs !== undefined && (!Number.isInteger(options.statementTimeoutMs)
    || options.statementTimeoutMs < 1_000 || options.statementTimeoutMs > 3_600_000)) {
    throw new PrivacyConvergenceError('STATEMENT_TIMEOUT_OUT_OF_RANGE: expected 1000..3600000 milliseconds');
  }
  return options;
}

export function convergenceErrorMessage(error: unknown): string {
  if (error instanceof PrivacyConvergenceError) return error.message;
  const code = error && typeof error === 'object' && 'code' in error ? error.code : null;
  return typeof code === 'string' && /^[0-9A-Z]{5}$/.test(code)
    ? `database operation failed (SQLSTATE ${code}); inspect server diagnostics`
    : 'database connection or operation failed; inspect local diagnostics';
}

async function main(): Promise<void> {
  if (process.argv.slice(2).some((arg) => arg === '--help')) {
    console.log('privacy:converge [--dry-run] | --apply --expected-generation=N [--statement-timeout-ms=900000]');
    console.log('Uses DATABASE_URL. Default is a read-only inventory; apply locks policy, queue and both packet stores for a full verified repair.');
    return;
  }
  const options = parseConvergenceArgs(process.argv.slice(2));
  if (!process.env['DATABASE_URL']) throw new PrivacyConvergenceError('DATABASE_URL_REQUIRED');
  const pool = new pg.Pool({
    connectionString: process.env['DATABASE_URL'], application_name: 'meshcore-privacy-convergence',
    max: 1, connectionTimeoutMillis: 5_000,
  });
  try { console.log(JSON.stringify(await convergePacketVisibility(pool, options), null, 2)); }
  finally { await pool.end(); }
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  main().catch((error: unknown) => {
    // Do not print connection options or server errors that might include data.
    console.error('[privacy-convergence] failed:', convergenceErrorMessage(error));
    process.exitCode = 1;
  });
}
