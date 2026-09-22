import { spawnSync } from 'node:child_process';
import { accessSync, constants, mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import {
  decideOwnerBaselineAction,
  parseRefreshValidationSummary,
  refreshValidationPassed,
} from '../owner/autoRebaseline.js';

const repoRoot = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '../../..');
const refreshScript = path.join(repoRoot, 'scripts/refresh-owner-baseline.sh');
const readyzUrl = process.env['OWNER_BASELINE_READYZ_URL'] ?? 'http://127.0.0.1:3000/readyz';
const checkOnly = process.argv.slice(2).includes('--check');
const invalidArgs = process.argv.slice(2).some((argument) => argument !== '--check');

function canExecute(filePath: string): boolean {
  try {
    accessSync(filePath, constants.X_OK);
    return true;
  } catch {
    return false;
  }
}

async function readReadiness(): Promise<{ status: number; payload: unknown }> {
  const response = await fetch(readyzUrl, {
    method: 'GET',
    redirect: 'error',
    signal: AbortSignal.timeout(10_000),
  });
  let payload: unknown;
  try {
    payload = await response.json();
  } catch {
    throw new Error(`readiness response was not JSON (HTTP ${response.status})`);
  }
  return { status: response.status, payload };
}

function runRefresh(args: string[], env: NodeJS.ProcessEnv) {
  return spawnSync(refreshScript, args, {
    cwd: repoRoot,
    env,
    encoding: 'utf8',
    maxBuffer: 8 * 1024 * 1024,
  });
}

async function main(): Promise<void> {
  if (invalidArgs || process.argv.slice(2).length > 1) {
    throw new Error('usage: autoRebaseline.js [--check]');
  }

  const readiness = await readReadiness();
  const decision = decideOwnerBaselineAction(readiness.status, readiness.payload);
  if (checkOnly) {
    process.stdout.write(`${JSON.stringify({
      mode: 'check',
      action: decision.action,
      reason: decision.reason,
      mismatches: decision.mismatches,
      refreshScriptAvailable: canExecute(refreshScript),
      scriptValidation: 'not-run-in-check-mode',
      applied: false,
    }, null, 2)}\n`);
    if (decision.action === 'abort') process.exitCode = 1;
    return;
  }

  if (decision.action === 'skip') {
    process.stdout.write(`[owner-baseline-auto] skipped: ${decision.reason}\n`);
    return;
  }
  if (decision.action === 'abort') throw new Error(decision.reason);
  if (!canExecute(refreshScript)) {
    throw new Error(`refresh script is missing or not executable: ${refreshScript}`);
  }

  const validationDirectory = mkdtempSync(path.join(tmpdir(), 'owner-baseline-validation-'));
  try {
    process.stdout.write('[owner-baseline-auto] owner ACL generation drift detected; validating candidate\n');
    const validation = runRefresh([], {
      ...process.env,
      OWNER_BASELINE_EXPORT_DIR: validationDirectory,
    });
    const summary = parseRefreshValidationSummary(validation.stdout ?? '');
    if (validation.error || !refreshValidationPassed(validation.status, summary)) {
      throw new Error('refresh script did not validate the candidate baseline as ok:true; apply aborted');
    }

    process.stdout.write('[owner-baseline-auto] candidate validated ok:true; applying and recreating backend\n');
    const applied = runRefresh(['--apply', '--recreate'], process.env);
    if (applied.error || applied.status !== 0) {
      throw new Error('refresh script apply/recreate failed; inspect the refresh script on the VPS');
    }
    process.stdout.write('[owner-baseline-auto] baseline applied and backend recreate completed\n');
  } finally {
    rmSync(validationDirectory, { recursive: true, force: true });
  }
}

main().catch((error: Error) => {
  console.error(`[owner-baseline-auto] abort: ${error.message}`);
  process.exitCode = 1;
});
